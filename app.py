from flask import Flask, request, jsonify
import asyncio
import os
import threading
import time
import hashlib
import hmac
from datetime import datetime
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson
import binascii
import aiohttp
import requests
import json
import like_pb2
import like_count_pb2
import uid_generator_pb2
import MajorLoginReq_pb2
import MajorLoginRes_pb2
from google.protobuf.message import DecodeError
import base64
import urllib3
from urllib.parse import urlparse

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
if hasattr(app, "json"):
    app.json.sort_keys = False
else:
    app.config["JSON_SORT_KEYS"] = False
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOKENS_FILE = os.path.join(BASE_DIR, "tokens.json")
UIDPASS_FILE = os.path.join(BASE_DIR, "uidpass.json")
TOKEN_API_URL = os.getenv("TOKEN_API_URL", "local").strip()
UPSTREAM_TIMEOUT_SECONDS = int(os.getenv("UPSTREAM_TIMEOUT_SECONDS", "20"))
TOKEN_RETRY_ATTEMPTS = int(os.getenv("TOKEN_RETRY_ATTEMPTS", "10"))
TOKEN_RETRY_DELAY_SECONDS = float(os.getenv("TOKEN_RETRY_DELAY_SECONDS", "0.7"))
FFINFO_API_URL = os.getenv("FFINFO_API_URL", "https://info.killersharmabot.online/player-info").strip()
FFINFO_API_KEY = os.getenv("FFINFO_API_KEY", "").strip()
AUTO_TOKEN_REFRESH_HOURS = float(os.getenv("AUTO_TOKEN_REFRESH_HOURS", "7"))
ENABLE_AUTO_TOKEN_REFRESH = os.getenv("ENABLE_AUTO_TOKEN_REFRESH", "true").strip().lower() in {"1", "true", "yes", "on"}
RELEASE_VERSION = os.getenv("RELEASE_VERSION", "OB53").strip() or "OB53"
MAIN_KEY = b"Yg&tc%DEuh6%Zc^8"
MAIN_IV = b"6oyZDr22E3ychjM%"
GUEST_TOKEN_GRANT_URLS = (
    "https://100067.connect.garena.com/api/v2/oauth/guest/token:grant",
    "https://100067.connect.garena.com/oauth/guest/token/grant",
    "https://ffmconnect.live.gop.garenanow.com/oauth/guest/token/grant",
)
MAJOR_LOGIN_URLS = (
    "https://loginbp.ggpolarbear.com/MajorLogin",
    "https://loginbp.ggblueshark.com/MajorLogin",
)
COMMON_MAJOR_LOGIN_URL = "https://loginbp.common.ggbluefox.com/MajorLogin"
COMMON_MAJOR_LOGIN_REGIONS = {"IND", "ME", "TH", "TW", "CIS", "SAC"}
GUEST_CLIENT_ID = "100067"
GUEST_CLIENT_SECRET = "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3"
GUEST_USER_AGENT = os.getenv("GUEST_USER_AGENT", "GarenaMSDK/4.0.19P9(SM-S908E; Android 11; en; IN)")
MAJOR_LOGIN_USER_AGENT = os.getenv(
    "MAJOR_LOGIN_USER_AGENT",
    "Dalvik/2.1.0 (Linux; U; Android 13; A063 Build/TKQ1.221220.001)",
)
FF_NICKNAME_KEY = b"1e5898ccb8dfdd921f9bdea848768b64a201"
FFINFO_DEFAULT_REGIONS = [
    region.strip().upper()
    for region in os.getenv("FFINFO_DEFAULT_REGIONS", "SG,IND,BD,BR,US,SAC,NA,EU,ME,TH,VN,ID,TW,RU").split(",")
    if region.strip()
]

def read_uidpass_records():
    uid = os.getenv("FREEFIRE_UID", "").strip()
    password = os.getenv("FREEFIRE_PASSWORD", "").strip()

    if uid and password:
        return [{"uid": uid, "password": password}]

    with open(UIDPASS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def response_brief(response, limit=160):
    if response is None:
        return "no response"
    try:
        body = response.text[:limit]
    except Exception:
        body = ""
    return f"HTTP {response.status_code}: {body}"


def ordered_major_login_urls(region=None):
    if str(region or "").upper() in COMMON_MAJOR_LOGIN_REGIONS:
        return (COMMON_MAJOR_LOGIN_URL, *MAJOR_LOGIN_URLS)
    return MAJOR_LOGIN_URLS


def extract_guest_token_credentials(token_data):
    data = token_data.get("data") if isinstance(token_data, dict) else {}
    if not isinstance(data, dict):
        data = {}
    access_token = token_data.get("access_token") or data.get("access_token")
    open_id = (
        token_data.get("open_id")
        or token_data.get("openId")
        or token_data.get("openid")
        or data.get("open_id")
        or data.get("openId")
        or data.get("openid")
    )
    return access_token, open_id


def grant_guest_access_token(uid, password):
    secret = GUEST_CLIENT_SECRET.encode()
    session = requests.Session()
    token_body = {
        "uid": str(uid),
        "password": str(password),
        "response_type": "token",
        "client_type": "2",
        "client_secret": GUEST_CLIENT_SECRET,
        "client_id": GUEST_CLIENT_ID,
    }
    last_error = ""

    for token_url in GUEST_TOKEN_GRANT_URLS:
        token_host = urlparse(token_url).netloc
        try:
            if "/api/v2/" in token_url:
                token_json = json.dumps(
                    {
                        "uid": str(uid),
                        "password": str(password),
                        "response_type": "token",
                        "client_type": 2,
                        "client_secret": GUEST_CLIENT_SECRET,
                        "client_id": int(GUEST_CLIENT_ID),
                    },
                    separators=(",", ":"),
                )
                response = session.post(
                    token_url,
                    data=token_json,
                    headers={
                        "Host": token_host,
                        "User-Agent": GUEST_USER_AGENT,
                        "Authorization": f"Signature {hmac.new(secret, token_json.encode(), hashlib.sha256).hexdigest()}",
                        "Content-Type": "application/json; charset=utf-8",
                        "Accept": "application/json",
                        "Connection": "Keep-Alive",
                        "Accept-Encoding": "gzip",
                    },
                    timeout=20,
                )
            else:
                response = session.post(
                    token_url,
                    data=token_body,
                    headers={
                        "Host": token_host,
                        "User-Agent": GUEST_USER_AGENT,
                        "Connection": "Keep-Alive",
                        "Accept-Encoding": "gzip",
                    },
                    timeout=20,
                )

            if response.status_code != 200:
                last_error = response_brief(response)
                continue
            token_data = response.json()
            access_token, open_id = extract_guest_token_credentials(token_data)
            if access_token and open_id:
                return str(access_token), str(open_id), token_data, ""
            last_error = "Token grant did not return access_token/open_id."
        except ValueError:
            last_error = "Token grant returned invalid JSON."
        except requests.exceptions.RequestException as e:
            last_error = str(e)

    return "", "", {}, last_error or "Token grant failed."


def aes_encrypt_payload(payload):
    cipher = AES.new(MAIN_KEY, AES.MODE_CBC, MAIN_IV)
    return cipher.encrypt(pad(payload, AES.block_size))


def build_major_login_payload(open_id, access_token, platform="4", language="en"):
    message = MajorLoginReq_pb2.MajorLogin(
        event_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        game_name="free fire",
        platform_id=1,
        client_version="1.123.1",
        system_software="Android OS 9 / API-28 (PQ3B.190801.10101846/G9650ZHU2ARC6)",
        system_hardware="Handheld",
        telecom_operator="Verizon",
        network_type="WIFI",
        screen_width=1920,
        screen_height=1080,
        screen_dpi="280",
        processor_details="ARM64 FP ASIMD AES VMH | 2865 | 4",
        memory=3003,
        gpu_renderer="Adreno (TM) 640",
        gpu_version="OpenGL ES 3.1 v1.46",
        unique_device_id="Google|34a7dcdf-a7d5-4cb6-8d7e-3b0e448a0c57",
        client_ip="223.191.51.89",
        language=language,
        open_id=open_id,
        open_id_type="4",
        device_type="Handheld",
        memory_available=MajorLoginReq_pb2.GameSecurity(version=55, hidden_value=81),
        access_token=access_token,
        platform_sdk_id=1,
        network_operator_a="Verizon",
        network_type_a="WIFI",
        client_using_version="7428b253defc164018c604a1ebbfebdf",
        external_storage_total=36235,
        external_storage_available=31335,
        internal_storage_total=2519,
        internal_storage_available=703,
        game_disk_storage_available=25010,
        game_disk_storage_total=26628,
        external_sdcard_avail_storage=32992,
        external_sdcard_total_storage=36235,
        login_by=3,
        library_path="/data/app/com.dts.freefireth-YPKM8jHEwAJlhpmhDhv5MQ==/lib/arm64",
        reg_avatar=1,
        library_token="5b892aaabd688e571f688053118a162b|/data/app/com.dts.freefireth-YPKM8jHEwAJlhpmhDhv5MQ==/base.apk",
        channel_type=3,
        cpu_type=2,
        cpu_architecture="64",
        client_version_code="2019118695",
        graphics_api="OpenGLES2",
        supported_astc_bitset=16383,
        login_open_id_type=4,
        analytics_detail=b"FwQVTgUPX1UaUllDDwcWCRBpWA0FUgsvA1snWlBaO1kFYg==",
        loading_time=13564,
        release_channel="android",
        extra_info="KqsHTymw5/5GB23YGniUYN2/q47GATrq7eFeRatf0NkwLKEMQ0PK5BKEk72dPflAxUlEBir6Vtey83XqF593qsl8hwY=",
        android_engine_init_flag=110009,
        if_push=1,
        is_vpn=1,
        origin_platform_type=str(platform or "4"),
        primary_platform_type=str(platform or "4"),
    )
    return aes_encrypt_payload(message.SerializeToString())


def decode_major_login_info(raw_bytes):
    message = MajorLoginRes_pb2.MajorLoginRes()
    message.ParseFromString(raw_bytes)
    return {
        "account_id": str(message.account_id or ""),
        "region": message.lock_region,
        "lock_region": message.lock_region,
        "noti_region": message.noti_region,
        "ip_region": message.ip_region,
        "agora_environment": message.agora_environment,
        "new_active_region": message.new_active_region,
        "token": message.token,
        "ttl": message.ttl,
        "server_url": message.server_url,
        "emulator_score": message.emulator_score,
        "tp_url": message.tp_url,
        "app_server_id": message.app_server_id,
        "ano_url": message.ano_url,
        "ip_city": message.ip_city,
        "ip_subdivision": message.ip_subdivision,
        "kts": message.kts,
    }


def decode_jwt_payload(token):
    try:
        payload = str(token or "").split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload).decode("utf-8"))
    except Exception:
        return {}


def decode_ff_nickname(encoded):
    try:
        raw = base64.b64decode(str(encoded or ""))
        decoded = bytearray()
        for index, byte in enumerate(raw):
            decoded.append(byte ^ FF_NICKNAME_KEY[index % len(FF_NICKNAME_KEY)])
        return decoded.decode("utf-8", errors="replace")
    except Exception:
        return ""


def format_ttl(seconds):
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        return seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours} hours, {minutes} mins, {secs} secs"


def convert_timestamps_to_human(data):
    if isinstance(data, dict):
        converted = {}
        for key, value in data.items():
            if isinstance(value, (int, float)) and 1000000000 < value < 3000000000:
                human_time = datetime.utcfromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S UTC")
                converted[key] = f"{value} ({human_time})"
            else:
                converted[key] = convert_timestamps_to_human(value)
        return converted
    if isinstance(data, list):
        return [convert_timestamps_to_human(item) for item in data]
    return data


def jwt_region_from_data(data):
    claims = decode_jwt_payload(data.get("token", ""))
    region = (
        data.get("lock_region")
        or data.get("noti_region")
        or claims.get("lock_region")
        or claims.get("noti_region")
    )
    return str(region or "").upper()


def create_jwt_token(uid, password, region=""):
    access_token, open_id, token_data, error = grant_guest_access_token(uid, password)
    if error:
        return None, error

    encrypted_payload = build_major_login_payload(open_id, access_token)
    last_error = ""
    for login_url in ordered_major_login_urls(region):
        host = urlparse(login_url).netloc
        try:
            response = requests.post(
                login_url,
                data=encrypted_payload,
                headers={
                    "Accept-Encoding": "gzip",
                    "Authorization": "Bearer",
                    "Connection": "Keep-Alive",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Content-Length": str(len(encrypted_payload)),
                    "Expect": "100-continue",
                    "Host": host,
                    "ReleaseVersion": RELEASE_VERSION,
                    "User-Agent": MAJOR_LOGIN_USER_AGENT,
                    "X-GA": "v1 1",
                    "X-Unity-Version": "2018.4.11f1",
                },
                timeout=20,
            )
            if response.status_code != 200 or not response.content:
                last_error = response_brief(response)
                continue
            login_info = decode_major_login_info(response.content)
            if login_info.get("token"):
                login_info["access_token"] = access_token
                login_info["open_id"] = open_id
                login_info["token_data"] = token_data
                return login_info, ""
            last_error = "MajorLogin response did not contain token."
        except requests.exceptions.RequestException as e:
            last_error = str(e)
        except Exception as e:
            last_error = f"MajorLogin decode failed: {e}"

    return None, last_error or "MajorLogin failed."


def fetch_token_with_retry(uid, password):
    last_error = None
    for attempt in range(1, TOKEN_RETRY_ATTEMPTS + 1):
        try:
            if TOKEN_API_URL.lower() in {"", "local", "self"}:
                data, error = create_jwt_token(uid, password)
                if error:
                    raise RuntimeError(error)
            else:
                response = requests.get(
                    TOKEN_API_URL,
                    params={"uid": uid, "password": password},
                    timeout=20
                )
                response.raise_for_status()
                data = response.json()
            token = data.get("token") if isinstance(data, dict) else None
            if token and isinstance(token, str):
                return token
            last_error = "token missing in response"
        except Exception as e:
            last_error = e

        if attempt < TOKEN_RETRY_ATTEMPTS:
            time.sleep(TOKEN_RETRY_DELAY_SECONDS)

    app.logger.error(f"Token fetch failed for UID {uid} after {TOKEN_RETRY_ATTEMPTS} attempts: {last_error}")
    return None

def refresh_tokens_from_uidpass():
    try:
        uidpass_list = read_uidpass_records()
        new_tokens = []
        failed_uids = []

        for item in uidpass_list:
            uid = str(item.get("uid", "")).strip()
            password = str(item.get("password", "")).strip()
            if not uid or not password:
                continue
            token = fetch_token_with_retry(uid, password)
            if token and isinstance(token, str):
                new_tokens.append({"uid": uid, "token": token})
            else:
                failed_uids.append(uid)

        if new_tokens:
            with open(TOKENS_FILE, "w", encoding="utf-8") as f:
                json.dump(new_tokens, f, ensure_ascii=False, indent=4)
            return True, len(new_tokens), len(uidpass_list), failed_uids

        return False, 0, len(uidpass_list), failed_uids
    except Exception as e:
        app.logger.error(f"Auto token refresh failed: {e}")
        return False, 0, 0, []


def load_tokens(auto_refresh=True):
    try:
        with open(TOKENS_FILE, "r", encoding="utf-8") as f:
            tokens = json.load(f)
        if isinstance(tokens, list) and tokens:
            return tokens
        raise ValueError("tokens.json is empty or invalid")
    except Exception as e:
        app.logger.error(f"Error loading tokens: {e}")
        if auto_refresh:
            ok, count, total, failed_uids = refresh_tokens_from_uidpass()
            if ok:
                app.logger.info(
                    f"Auto-refreshed tokens.json from uidpass.json. total={total} valid={count} failed={len(failed_uids)}"
                )
                return load_tokens(auto_refresh=False)
            app.logger.error(
                f"Token auto-refresh did not produce valid tokens. total={total} failed={len(failed_uids)}"
            )
        return None

def refresh_and_load_tokens():
    ok, count, total, failed_uids = refresh_tokens_from_uidpass()
    if not ok:
        app.logger.error(
            f"Token refresh failed. total={total} valid={count} failed={len(failed_uids)}"
        )
        return None
    app.logger.info(
        f"Refreshed tokens.json from uidpass.json. total={total} valid={count} failed={len(failed_uids)}"
    )
    return load_tokens(auto_refresh=False)

def auto_refresh_tokens_loop():
    interval_seconds = max(300, int(AUTO_TOKEN_REFRESH_HOURS * 60 * 60))
    while True:
        try:
            app.logger.info(f"Auto token refresh running. interval_hours={AUTO_TOKEN_REFRESH_HOURS}")
            refresh_and_load_tokens()
        except Exception as e:
            app.logger.error(f"Scheduled token refresh failed: {e}")
        time.sleep(interval_seconds)

if ENABLE_AUTO_TOKEN_REFRESH:
    threading.Thread(target=auto_refresh_tokens_loop, daemon=True).start()

def get_region_from_token(token):
    try:
        parsed_payload = decode_jwt_payload(token)
        return (
            parsed_payload.get('lock_region')
            or parsed_payload.get('noti_region')
            or ''
        ).upper()
    except Exception as e:
        app.logger.error(f"Error decoding token payload: {e}")
        return ""

def normalize_region(region):
    region = str(region or "").strip().upper()
    if region in {"EU", "EUR"}:
        return "EUROPE"
    return region

def ffinfo_api_region(region):
    region = str(region or "").strip().upper()
    if region == "EUROPE":
        return "eu"
    return region.lower()

def token_region(token_item):
    token = token_item.get("token", "") if isinstance(token_item, dict) else str(token_item)
    return normalize_region(get_region_from_token(token))

def select_tokens_for_region(tokens, server_name):
    requested_region = normalize_region(server_name)
    return [item for item in tokens if token_region(item) == requested_region]

def order_tokens_for_region(tokens, server_name):
    requested_region = normalize_region(server_name)
    matching = []
    others = []
    for item in tokens:
        if token_region(item) == requested_region:
            matching.append(item)
        else:
            others.append(item)
    return matching + others

def select_token_for_region(tokens, server_name):
    matching_tokens = select_tokens_for_region(tokens, server_name)
    if matching_tokens:
        return token_value(matching_tokens[0])
    return None

def first_token_value(tokens):
    if not tokens:
        return None
    return token_value(tokens[0])

def token_value(token_item):
    if isinstance(token_item, dict):
        return str(token_item.get("token", "")).strip()
    return str(token_item or "").strip()

def build_freefire_headers(token):
    return {
        'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
        'Connection': "Keep-Alive",
        'Accept-Encoding': "gzip",
        'Authorization': f"Bearer {token}",
        'Content-Type': "application/x-www-form-urlencoded",
        'Expect': "100-continue",
        'X-Unity-Version': "2018.4.11f1",
        'X-GA': "v1 1",
        'ReleaseVersion': RELEASE_VERSION,
    }

def like_result_summary(results):
    summary = {"total": 0, "success": 0, "failed": 0, "statuses": {}}
    for item in results or []:
        summary["total"] += 1
        if isinstance(item, Exception):
            summary["failed"] += 1
            key = item.__class__.__name__
            summary["statuses"][key] = summary["statuses"].get(key, 0) + 1
            continue
        if isinstance(item, str):
            summary["success"] += 1
            summary["statuses"]["200"] = summary["statuses"].get("200", 0) + 1
            continue
        if isinstance(item, int):
            summary["failed"] += 1
            status = str(item)
            summary["statuses"][status] = summary["statuses"].get(status, 0) + 1
            continue
        if item is None:
            summary["failed"] += 1
            summary["statuses"]["none"] = summary["statuses"].get("none", 0) + 1
            continue
        summary["failed"] += 1
        summary["statuses"]["unknown"] = summary["statuses"].get("unknown", 0) + 1
    return summary

def fetch_external_ffinfo(uid, server_name):
    if not FFINFO_API_URL:
        return None, "", {"error": "FFINFO_API_URL is disabled"}

    last_error = {"error": "No region returned data"}

    params = {"uid": uid}
    if FFINFO_API_KEY:
        params["key"] = FFINFO_API_KEY

    try:
        response = requests.get(
            FFINFO_API_URL,
            params=params,
            timeout=UPSTREAM_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            last_error = {
                "region": normalize_region(server_name),
                "status_code": response.status_code,
                "body": response.text[:180],
            }
            app.logger.info(
                f"FF info API returned {response.status_code}: {response.text[:180]}"
            )
            return None, "", last_error
        data = response.json()
        basic_info = data.get("basicInfo", {}) if isinstance(data, dict) else {}
        resolved_region = normalize_region(basic_info.get("region") or server_name)
        return data, resolved_region, None
    except Exception as e:
        last_error = {"region": normalize_region(server_name), "error": str(e)}
        app.logger.info(f"External FF info request failed: {e}")
    return None, "", last_error

def encrypt_message(plaintext):
    try:
        key = b'Yg&tc%DEuh6%Zc^8'
        iv = b'6oyZDr22E3ychjM%'
        cipher = AES.new(key, AES.MODE_CBC, iv)
        padded_message = pad(plaintext, AES.block_size)
        encrypted_message = cipher.encrypt(padded_message)
        return binascii.hexlify(encrypted_message).decode('utf-8')
    except Exception as e:
        app.logger.error(f"Error encrypting message: {e}")
        return None

def create_protobuf_message(user_id, region):
    try:
        message = like_pb2.like()
        message.uid = int(user_id)
        message.region = region
        return message.SerializeToString()
    except Exception as e:
        app.logger.error(f"Error creating protobuf message: {e}")
        return None

async def send_request(encrypted_uid, token, url):
    try:
        edata = bytes.fromhex(encrypted_uid)
        headers = build_freefire_headers(token)
        timeout = aiohttp.ClientTimeout(total=UPSTREAM_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, data=edata, headers=headers) as response:
                if response.status != 200:
                    app.logger.error(f"Request failed with status code: {response.status}")
                    return response.status
                return await response.text()
    except Exception as e:
        app.logger.error(f"Exception in send_request: {e}")
        return None

async def send_multiple_requests(uid, server_name, url, tokens=None):
    try:
        region = server_name
        protobuf_message = create_protobuf_message(uid, region)
        if protobuf_message is None:
            app.logger.error("Failed to create protobuf message.")
            return None
        encrypted_uid = encrypt_message(protobuf_message)
        if encrypted_uid is None:
            app.logger.error("Encryption failed.")
            return None
        tasks = []
        if tokens is None:
            tokens = load_tokens()
        if tokens is None:
            app.logger.error("Failed to load tokens.")
            return None
        tokens = order_tokens_for_region(tokens, server_name)
        if not tokens:
            app.logger.error("No tokens found.")
            return None
        tokens = [item for item in tokens if token_value(item)]
        if not tokens:
            app.logger.error("No usable token values found.")
            return None
        for item in tokens:
            token = token_value(item)
            tasks.append(send_request(encrypted_uid, token, url))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    except Exception as e:
        app.logger.error(f"Exception in send_multiple_requests: {e}")
        return None

def create_protobuf(uid):
    try:
        message = uid_generator_pb2.uid_generator()
        message.saturn_ = int(uid)
        message.garena = 1
        return message.SerializeToString()
    except Exception as e:
        app.logger.error(f"Error creating uid protobuf: {e}")
        return None

def enc(uid):
    protobuf_data = create_protobuf(uid)
    if protobuf_data is None:
        return None
    encrypted_uid = encrypt_message(protobuf_data)
    return encrypted_uid

def make_request(encrypt, server_name, token):
    try:
        if server_name == "IND":
            url = "https://client.ind.freefiremobile.com/GetPlayerPersonalShow"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            url = "https://client.us.freefiremobile.com/GetPlayerPersonalShow"
        else:
            url = "https://clientbp.ggpolarbear.com/GetPlayerPersonalShow"
        
        if not encrypt:
            app.logger.error("Invalid encrypt parameter (empty or None)")
            return None
            
        edata = bytes.fromhex(encrypt)
        headers = build_freefire_headers(token)
        response = requests.post(
            url,
            data=edata,
            headers=headers,
            verify=False,
            timeout=UPSTREAM_TIMEOUT_SECONDS,
        )
        
        if response.status_code != 200:
            app.logger.error(f"Server returned status {response.status_code}: {response.text[:100]}")
            return None
            
        hex_data = response.content.hex()
        binary = bytes.fromhex(hex_data)
        decode = decode_protobuf(binary)
        if decode is None:
            app.logger.error("Protobuf decoding returned None.")
            return None
        return decode
    except ValueError as e:
        app.logger.error(f"Error converting encrypt to hex: {e}")
        return None
    except Exception as e:
        app.logger.error(f"Error in make_request: {e}")
        return None

def decode_protobuf(binary):
    try:
        items = like_count_pb2.Info()
        items.ParseFromString(binary)
        return items
    except DecodeError as e:
        app.logger.error(f"Error decoding Protobuf data: {e}")
        return None
    except Exception as e:
        app.logger.error(f"Unexpected error during protobuf decoding: {e}")
        return None


@app.route('/token', methods=['GET'])
@app.route('/guest', methods=['GET'])
def handle_token():
    uid = str(request.args.get("uid", "")).strip()
    password = str(request.args.get("password", request.args.get("pw", ""))).strip()
    region = str(request.args.get("region", request.args.get("server_name", ""))).strip().upper()

    if not uid or not password:
        return jsonify({"status": "error", "message": "Missing parameters. Use /token?uid=xxx&password=xxx or /guest?uid=xxx&pw=xxx"}), 400
    if not uid.isdigit():
        return jsonify({"status": "error", "message": "uid must be numeric"}), 400

    data, error = create_jwt_token(uid, password, region=region)
    if error:
        app.logger.error(f"Local token API failed for UID {uid}: {error}")
        return jsonify({"status": "error", "message": error}), 502

    claims = decode_jwt_payload(data.get("token", ""))
    resolved_region = jwt_region_from_data(data)
    nickname = decode_ff_nickname(claims.get("nickname", "")) or data.get("nickname", "")

    token_data = data.get("token_data") if isinstance(data.get("token_data"), dict) else {}
    major_login = {
        "account_id": data.get("account_id") or uid,
        "nickname": nickname or "Unknown",
        "lock_region": data.get("lock_region", ""),
        "noti_region": data.get("noti_region", ""),
        "ip_region": data.get("ip_region", ""),
        "agora_environment": data.get("agora_environment", ""),
        "new_active_region": data.get("new_active_region", ""),
        "token": data.get("token", ""),
        "ttl": format_ttl(data.get("ttl", 0)),
        "server_url": data.get("server_url", ""),
        "emulator_score": data.get("emulator_score", 0),
        "tp_url": data.get("tp_url", ""),
        "app_server_id": data.get("app_server_id", 0),
        "ano_url": data.get("ano_url", ""),
        "ip_city": data.get("ip_city", ""),
        "ip_subdivision": data.get("ip_subdivision", ""),
        "kts": data.get("kts", 0),
    }
    major_login = {key: value for key, value in major_login.items() if value not in ("", None, 0)}

    return jsonify({
        "creator": "Crownx64Alone",
        "status": "success",
        "Guest_Auth": convert_timestamps_to_human(token_data),
        "MajorLogin": convert_timestamps_to_human(major_login),
        "uid": uid,
        "account_id": data.get("account_id") or uid,
        "nickname": nickname,
        "name": nickname,
        "region": resolved_region,
        "lock_region": data.get("lock_region") or claims.get("lock_region", ""),
        "noti_region": data.get("noti_region") or claims.get("noti_region", ""),
        "access_token": data.get("access_token", ""),
        "open_id": data.get("open_id", ""),
        "token": data.get("token", ""),
        "ttl": data.get("ttl", 0),
        "server_url": data.get("server_url", ""),
    })


@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "credit": "https://t.me/mean_un",
        "message": "Welcome to the Free Fire Like API",
        "status": "API is running",
        "endpoints": "/token?uid=<uid>&password=<password>, /like?uid=<uid> or /like?uid=<uid>&server_name=<server_name>",
        "example": "/like?uid=123456789 or /like?uid=123456789&server_name=bd"
})


@app.route('/like', methods=['GET'])
def handle_requests():
    uid = str(request.args.get("uid", "")).strip()
    if not uid:
        return jsonify({"error": "UID is required"}), 400
    if not uid.isdigit():
        return jsonify({"error": "UID must be numeric"}), 400

    try:
        tokens = load_tokens()
        if tokens is None or not tokens:
            return jsonify({"error": "Failed to load tokens."}), 500
        
        # Extract server_name (lock_region) from token if not provided
        server_name = str(request.args.get("server_name", "")).strip().upper()
        if server_name and not server_name.replace("_", "").isalpha():
            return jsonify({"error": "server_name must be a valid region code"}), 400
        if not server_name:
            token = token_value(tokens[0])
            server_name = get_region_from_token(token)
        
        if not server_name:
            return jsonify({"error": "server_name could not be determined from token or input"}), 400
        server_name = normalize_region(server_name)

        token = select_token_for_region(tokens, server_name)
        if token is None:
            app.logger.info(f"No token for {server_name}. Refreshing tokens and retrying selection once.")
            tokens = refresh_and_load_tokens()
            token = select_token_for_region(tokens or [], server_name)
            if token is None:
                available_regions = sorted({token_region(item) for item in (tokens or []) if token_region(item)})
                token = first_token_value(tokens or [])
                if token is None:
                    return jsonify({
                        "error": f"No valid token found for region {server_name}. Add a {server_name} UID/password in uidpass.json and refresh tokens.",
                        "available_regions": available_regions,
                    }), 500
                app.logger.info(
                    f"No exact token for {server_name}. Using fallback token from available regions: {available_regions}"
                )
        tokens_for_send = order_tokens_for_region(tokens, server_name)
        
        encrypted_uid = enc(uid)
        if encrypted_uid is None:
            return jsonify({"error": "Encryption of UID failed."}), 500

        # Get before likes count
        before = make_request(encrypted_uid, server_name, token)
        if before is None:
            app.logger.info("Player info failed before likes. Refreshing tokens and retrying once.")
            tokens = refresh_and_load_tokens()
            if tokens is None or not tokens:
                return jsonify({"error": "Token expired and auto-refresh failed. Check uidpass.json."}), 500
            token = select_token_for_region(tokens, server_name)
            if token is None:
                token = first_token_value(tokens)
                if token is None:
                    return jsonify({"error": f"Token refresh succeeded, but no token exists for region {server_name}."}), 500
            tokens_for_send = order_tokens_for_region(tokens, server_name)
            before = make_request(encrypted_uid, server_name, token)
            if before is None:
                return jsonify({"error": "Failed to retrieve player info. There are no valid token found! please update tokens.json with valid tokens"}), 500
        
        data_before = json.loads(MessageToJson(before))
        before_like = int(data_before.get('AccountInfo', {}).get('Likes', 0) or 0)
        app.logger.info(f"Likes before: {before_like}")

        # Determine URL based on server
        if server_name == "IND":
            url = "https://client.ind.freefiremobile.com/LikeProfile"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            url = "https://client.us.freefiremobile.com/LikeProfile"
        else:
            url = "https://clientbp.ggpolarbear.com/LikeProfile"

        # Send like requests
        requests_sent = asyncio.run(send_multiple_requests(uid, server_name, url, tokens=tokens_for_send))
        request_summary = like_result_summary(requests_sent)
        app.logger.info(f"Like request summary for uid={uid} region={server_name}: {request_summary}")
        if requests_sent is None:
            return jsonify({"error": "Failed to send like requests."}), 500

        # Get after likes count
        after = make_request(encrypted_uid, server_name, token)
        if after is None:
            app.logger.info("Player info failed after likes. Refreshing tokens and retrying once.")
            tokens = refresh_and_load_tokens()
            if tokens is None or not tokens:
                return jsonify({"error": "Token expired and auto-refresh failed after likes. Check uidpass.json."}), 500
            token = select_token_for_region(tokens, server_name)
            if token is None:
                token = first_token_value(tokens)
                if token is None:
                    return jsonify({"error": f"Token refresh succeeded, but no token exists for region {server_name}."}), 500
            after = make_request(encrypted_uid, server_name, token)
            if after is None:
                return jsonify({"error": "Failed to retrieve player info after likes."}), 500
        
        data_after = json.loads(MessageToJson(after))
        account_info = data_after.get('AccountInfo', {})
        after_like = int(account_info.get('Likes', 0) or 0)
        player_uid = str(account_info.get('UID', uid) or uid)  # Keep as string
        player_name = str(account_info.get('PlayerNickname', 'Unknown')).strip()
        
        like_given = after_like - before_like
        if like_given <= 0 and request_summary["success"] == 0:
            app.logger.info("No like requests succeeded. Refreshing tokens and retrying send once.")
            tokens = refresh_and_load_tokens()
            if tokens:
                token = select_token_for_region(tokens, server_name) or first_token_value(tokens)
                retry_results = asyncio.run(send_multiple_requests(uid, server_name, url, tokens=order_tokens_for_region(tokens, server_name)))
                retry_summary = like_result_summary(retry_results)
                app.logger.info(f"Like retry summary for uid={uid} region={server_name}: {retry_summary}")
                after_retry = make_request(encrypted_uid, server_name, token)
                if after_retry is not None:
                    data_after = json.loads(MessageToJson(after_retry))
                    account_info = data_after.get('AccountInfo', {})
                    after_like = int(account_info.get('Likes', 0) or 0)
                    player_uid = str(account_info.get('UID', uid) or uid)
                    player_name = str(account_info.get('PlayerNickname', player_name) or player_name).strip()
                    like_given = after_like - before_like
                    request_summary = retry_summary
        
        return jsonify({
            "credit": "https://t.me/mean_un",
            "LikesGivenByAPI": like_given,
            "LikesafterCommand": after_like,
            "LikesbeforeCommand": before_like,
            "PlayerNickname": player_name,
            "Region": server_name,
            "UID": player_uid,
            "status": 1 if like_given > 0 else 2
        })
    except Exception as e:
        app.logger.error(f"Error processing request: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/ffinfo', methods=['GET'])
def handle_ffinfo():
    uid = request.args.get("uid")
    if not uid:
        return jsonify({"error": "UID is required"}), 400

    try:
        server_name = request.args.get("server_name", "").upper()
        local_only = str(request.args.get("local", "")).strip().lower() in {"1", "true", "yes", "on"}

        external_error = None
        if not local_only:
            external_data, resolved_region, external_error = fetch_external_ffinfo(uid, normalize_region(server_name))
            if external_data:
                return jsonify({
                    "source": "external",
                    "Region": resolved_region,
                    "data": external_data,
                    "status": 1,
                })

        tokens = load_tokens()
        if tokens is None or not tokens:
            return jsonify({"error": "Failed to load tokens and full ffinfo API did not return data."}), 500

        if not server_name:
            token = token_value(tokens[0])
            server_name = get_region_from_token(token)
        if not server_name:
            return jsonify({"error": "server_name could not be determined from token or input"}), 400
        server_name = normalize_region(server_name)

        token = select_token_for_region(tokens, server_name)
        if token is None:
            app.logger.info(f"No token for {server_name}. Refreshing tokens and retrying selection once.")
            tokens = refresh_and_load_tokens()
            token = select_token_for_region(tokens or [], server_name)
            if token is None:
                available_regions = sorted({token_region(item) for item in (tokens or []) if token_region(item)})
                return jsonify({
                    "error": f"Full ffinfo API did not return data, and no local token exists for region {server_name}.",
                    "available_regions": available_regions,
                }), 500

        encrypted_uid = enc(uid)
        if encrypted_uid is None:
            return jsonify({"error": "Encryption of UID failed."}), 500

        player_info = make_request(encrypted_uid, server_name, token)
        if player_info is None:
            if local_only:
                return jsonify({"error": "Player not found.", "details": external_error}), 404
            app.logger.info("FF info failed. Refreshing tokens and retrying once.")
            tokens = refresh_and_load_tokens()
            if tokens is None or not tokens:
                return jsonify({"error": "Token expired and auto-refresh failed. Check uidpass.json."}), 500
            token = select_token_for_region(tokens, server_name)
            if token is None:
                return jsonify({"error": f"Token refresh succeeded, but no token exists for region {server_name}."}), 500
            player_info = make_request(encrypted_uid, server_name, token)
            if player_info is None:
                return jsonify({"error": "Player not found.", "details": external_error}), 404

        data = json.loads(MessageToJson(player_info))
        account_info = data.get('AccountInfo', {})
        return jsonify({
            "source": "local",
            "Region": server_name,
            "data": {
                "basicInfo": {
                    "accountId": str(account_info.get('UID', uid) or uid),
                    "nickname": str(account_info.get('PlayerNickname', 'Unknown')).strip(),
                    "liked": int(account_info.get('Likes', 0) or 0),
                    "region": server_name,
                }
            },
            "status": 1,
        })
    except Exception as e:
        app.logger.error(f"Error processing ffinfo request: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv("PORT", "5001"))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
