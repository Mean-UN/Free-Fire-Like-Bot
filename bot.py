import json
import logging
import os
import random
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, quote, urlparse
import hashlib
import html
import hmac

import requests
import telebot
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from flask import Flask, jsonify, request
from requests.adapters import HTTPAdapter
from telebot.types import CopyTextButton, InlineKeyboardButton, InlineKeyboardMarkup
from urllib3.util.retry import Retry

try:
    import MajorLoginRes_pb2
except Exception:
    MajorLoginRes_pb2 = None


def load_env_file(path=".env"):
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        pass


load_env_file()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5001").rstrip("/")
API_TIMEOUT_SECONDS = int(os.getenv("API_TIMEOUT_SECONDS", "90"))
AUTO_TOKEN_REFRESH_HOURS = float(os.getenv("AUTO_TOKEN_REFRESH_HOURS", "7"))
ENABLE_AUTO_TOKEN_REFRESH = os.getenv("ENABLE_AUTO_TOKEN_REFRESH", "true").strip().lower() in {"1", "true", "yes", "on"}
JWT_API_URL = os.getenv("JWT_API_URL", "https://xtytdtyj-jwt.up.railway.app/token").strip()
JWT_RETRY_ATTEMPTS = int(os.getenv("JWT_RETRY_ATTEMPTS", "10"))
JWT_RETRY_DELAY_SECONDS = float(os.getenv("JWT_RETRY_DELAY_SECONDS", "0.7"))
BIO_API_KEY = os.getenv("BIO_API_KEY", os.getenv("API_KEY", "")).strip()
BIO_API_BASE_URL = os.getenv("BIO_API_BASE_URL", "https://bio.ffutils.tech/api/update_bio").strip()
FFINFO_API_URL = os.getenv("FFINFO_API_URL", "https://info.killersharmabot.online/player-info").strip()
FFINFO_API_KEY = os.getenv("FFINFO_API_KEY", "").strip()
FFINFO_DEFAULT_REGIONS = [
    item.strip().upper()
    for item in os.getenv("FFINFO_DEFAULT_REGIONS", "SG,IND,BD,BR,US,SAC,NA,EU,ME,TH,VN,ID,TW,RU").split(",")
    if item.strip()
]

if not BOT_TOKEN:
    logger.error("BOT_TOKEN not found. Set BOT_TOKEN in environment variables.")
    sys.exit(1)

CHANNELS = [
    {"username": "@freefirelikebotcambodia", "name": "Free Fire Like Bot Cambodia"}
]
REQUIRED_CHANNELS = [item["username"] for item in CHANNELS]
GROUP_JOIN_LINK = "https://t.me/freefirelikebotcambodia"
OWNER_ID = 1126297297
OWNER_USERNAME = "@mean_un"
UIDPASS_FILE = "uidpass.json"
TOKEN_FILE = "tokens.json"
GUESTGEN_REGIONS = ("IND", "SG", "RU", "ID", "TW", "US", "VN", "TH", "ME", "PK", "CIS", "SAC", "BR", "BD")
GUESTGEN_REGION_LANG = {
    "IND": "hi",
    "SG": "en",
    "RU": "ru",
    "ID": "id",
    "TW": "zh",
    "US": "en",
    "VN": "vi",
    "TH": "th",
    "ME": "ar",
    "PK": "ur",
    "CIS": "ru",
    "SAC": "es",
    "BR": "pt",
    "BD": "bn",
}
GUESTGEN_USAGE = (
    "ℹ️ Usage: /guestgen <region> <name>\n"
    "Regions: IND, SG, RU, ID, TW, US, VN, TH, ME, PK, CIS, SAC, BR, BD\n"
    "Example: /guestgen SG ᴍᴇᴀɴXᴜɴ!"
)
if set(GUESTGEN_REGIONS) != set(GUESTGEN_REGION_LANG):
    missing_lang = sorted(set(GUESTGEN_REGIONS) - set(GUESTGEN_REGION_LANG))
    extra_lang = sorted(set(GUESTGEN_REGION_LANG) - set(GUESTGEN_REGIONS))
    raise RuntimeError(f"Guestgen region/language mismatch. missing={missing_lang} extra={extra_lang}")
RELEASE_VERSION = os.getenv("RELEASE_VERSION", "OB53").strip() or "OB53"
MAIN_KEY = b"Yg&tc%DEuh6%Zc^8"
MAIN_IV = b"6oyZDr22E3ychjM%"
GUEST_REGISTER_URL = "https://100067.connect.garena.com/api/v2/oauth/guest:register"
GUEST_TOKEN_GRANT_URLS = (
    "https://100067.connect.garena.com/api/v2/oauth/guest/token:grant",
    "https://100067.connect.garena.com/oauth/guest/token/grant",
    "https://ffmconnect.live.gop.garenanow.com/oauth/guest/token/grant",
)
MAJOR_REGISTER_URLS = (
    "https://loginbp.ggblueshark.com/MajorRegister",
    "https://loginbp.ggpolarbear.com/MajorRegister",
)
MAJOR_LOGIN_URLS = (
    "https://loginbp.ggpolarbear.com/MajorLogin",
    "https://loginbp.ggblueshark.com/MajorLogin",
)
COMMON_MAJOR_REGISTER_URL = "https://loginbp.common.ggbluefox.com/MajorRegister"
COMMON_MAJOR_LOGIN_URL = "https://loginbp.common.ggbluefox.com/MajorLogin"
COMMON_MAJOR_REGISTER_REGIONS = {"ME", "TH"}
COMMON_MAJOR_LOGIN_REGIONS = {"IND", "ME", "TH", "TW", "CIS", "SAC"}
GUESTGEN_HTTP_RETRIES = int(os.getenv("GUESTGEN_HTTP_RETRIES", "2"))
GUESTGEN_POOL_SIZE = int(os.getenv("GUESTGEN_POOL_SIZE", "10"))
GUESTGEN_ACCOUNT_ATTEMPTS = int(os.getenv("GUESTGEN_ACCOUNT_ATTEMPTS", "3"))
GUESTGEN_PROXY_ROTATION_ENABLED = os.getenv("GUESTGEN_PROXY_ROTATION_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
GUESTGEN_PROXY_ROTATE_EVERY = max(1, int(os.getenv("GUESTGEN_PROXY_ROTATE_EVERY", "1")))
GUESTGEN_TOR_PROXY_URL = os.getenv("GUESTGEN_TOR_PROXY_URL", "socks5h://127.0.0.1:9050").strip()
GUESTGEN_TOR_CONTROL_HOST = os.getenv("GUESTGEN_TOR_CONTROL_HOST", "127.0.0.1").strip()
GUESTGEN_TOR_CONTROL_PORT = int(os.getenv("GUESTGEN_TOR_CONTROL_PORT", "9051"))
GUESTGEN_TOR_CONTROL_PASSWORD = os.getenv("GUESTGEN_TOR_CONTROL_PASSWORD", "")
GUESTGEN_TOR_RENEW_WAIT_SECONDS = float(os.getenv("GUESTGEN_TOR_RENEW_WAIT_SECONDS", "3"))
GUESTGEN_MAJOR_LOGIN_USER_AGENTS = (
    "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_I005DA Build/PI)",
    "Dalvik/2.1.0 (Linux; U; Android 12; ASUS_Z01QD Build/SKQ1.210216.001)",
    "Dalvik/2.1.0 (Linux; U; Android 13; SM-G998B Build/TP1A.220624.014)",
    "Dalvik/2.1.0 (Linux; U; Android 13; M2012K11AG Build/TKQ1.220829.002)",
    "Dalvik/2.1.0 (Linux; U; Android 12; RMX3393 Build/SP1A.210812.016)",
)

bot = telebot.TeleBot(BOT_TOKEN)
app = Flask(__name__)

# In-memory daily usage tracker
like_tracker = {}
guestgen_proxy_lock = threading.Lock()
guestgen_proxy_use_count = 0
guestgen_proxy_disabled_until = 0


def reset_limits():
    """Reset in-memory usage counters daily at 00:00 UTC."""
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            next_reset = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            sleep_seconds = (next_reset - now_utc).total_seconds()
            time.sleep(max(1, sleep_seconds))
            like_tracker.clear()
            logger.info("Daily limits reset at 00:00 UTC.")
        except Exception as e:
            logger.error(f"Error in reset_limits thread: {e}")
            time.sleep(5)


def is_user_in_channel(user_id):
    try:
        if user_id == OWNER_ID:
            return True
        for channel in REQUIRED_CHANNELS:
            member = bot.get_chat_member(channel, user_id)
            if member.status not in ["member", "administrator", "creator"]:
                return False
        return True
    except Exception as e:
        logger.error(f"Join check failed: {e}")
        return False


def build_join_markup():
    markup = InlineKeyboardMarkup()
    for item in CHANNELS:
        username = item["username"].lstrip("@")
        group_name = item.get("name", item["username"])
        markup.add(InlineKeyboardButton(f"Join {group_name}", url=f"https://t.me/{username}"))
    return markup


def extract_access_token(raw):
    raw = str(raw or "").strip()
    if raw.startswith(("http://", "https://")):
        try:
            params = parse_qs(urlparse(raw).query)
            if "eat" in params and params["eat"]:
                return params["eat"][0]
            if "access_token" in params and params["access_token"]:
                return params["access_token"][0]
        except Exception:
            return None
        return None

    if raw and all(char in "abcdefghijklmnopqrstuvwxyz0123456789" for char in raw):
        return raw
    return None


def parse_protobuf_scalars(data):
    result = {}
    index = 0

    def read_varint(offset):
        value = 0
        shift = 0
        while offset < len(data):
            byte = data[offset]
            offset += 1
            value |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return value, offset
            shift += 7
        raise ValueError("Truncated varint")

    while index < len(data):
        key, index = read_varint(index)
        field = key >> 3
        wire_type = key & 7
        if wire_type == 0:
            value, index = read_varint(index)
        elif wire_type == 2:
            length, index = read_varint(index)
            value = data[index:index + length]
            index += length
        elif wire_type == 5:
            value = data[index:index + 4]
            index += 4
        elif wire_type == 1:
            value = data[index:index + 8]
            index += 8
        else:
            raise ValueError(f"Unsupported wire type {wire_type}")
        result.setdefault(field, []).append(value)
    return result


def decode_major_login_info(raw_bytes):
    if MajorLoginRes_pb2 is not None:
        try:
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
                "timestamp": message.kts,
                "kts": message.kts,
                "key": message.ak,
                "iv": message.aiv,
                "ak": message.ak,
                "aiv": message.aiv,
            }
        except Exception as e:
            logger.info(f"MajorLoginRes protobuf decode failed, using fallback parser: {e}")

    fields = parse_protobuf_scalars(raw_bytes)

    def text_field(field):
        values = fields.get(field) or []
        if not values:
            return ""
        value = values[-1]
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="ignore")
        return str(value)

    def int_field(field):
        values = fields.get(field) or []
        if not values:
            return 0
        value = values[-1]
        return value if isinstance(value, int) else 0

    return {
        "account_id": str(int_field(1) or ""),
        "region": text_field(2),
        "lock_region": text_field(2),
        "noti_region": text_field(3),
        "ip_region": text_field(4),
        "token": text_field(8),
        "ttl": int_field(9),
        "server_url": text_field(10),
        "emulator_score": int_field(12),
        "timestamp": int_field(21),
        "key": (fields.get(22) or [b""])[-1],
        "iv": (fields.get(23) or [b""])[-1],
        "ak": (fields.get(22) or [b""])[-1],
        "aiv": (fields.get(23) or [b""])[-1],
    }


def get_open_id_from_access_token(access_token):
    response = requests.get(
        "https://100067.connect.garena.com/oauth/token/inspect",
        params={"token": access_token},
        headers={
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "close",
            "Content-Type": "application/x-www-form-urlencoded",
            "Host": "100067.connect.garena.com",
            "User-Agent": "GarenaMSDK/4.0.19P4(G011A ;Android 9;en;US;)",
        },
        timeout=10,
    )
    try:
        data = response.json()
    except ValueError:
        return "", "", f"Token inspect returned invalid JSON: {response_brief(response)}"

    if response.status_code != 200 or data.get("error"):
        return "", "", data.get("error") or response_brief(response)

    open_id = data.get("open_id") or data.get("openId") or data.get("openid")
    platform = data.get("platform")
    if not open_id:
        return "", "", "Token inspect did not return open_id."
    return str(open_id), str(platform or "4"), ""


def get_bio_jwt(access_token):
    open_id, platform, error = get_open_id_from_access_token(access_token)
    if error:
        return None, error

    encrypted_payload = build_major_login_payload(open_id, access_token, platform=platform)
    last_error = ""
    for login_url in ordered_major_login_urls():
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
                    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; G011A Build/PI)",
                    "X-GA": "v1 1",
                    "X-Unity-Version": "2018.4.11f1",
                },
                timeout=15,
            )
            if response.status_code == 200 and response.content:
                info = decode_major_login_info(response.content)
                if info.get("token"):
                    return info, ""
                last_error = "MajorLogin response did not contain JWT token."
            else:
                last_error = response_brief(response)
        except requests.exceptions.RequestException as e:
            last_error = str(e)
        except Exception as e:
            last_error = f"MajorLogin decode failed: {e}"

    return None, last_error or "MajorLogin failed."


def update_bio_with_jwt(jwt_token, bio_text, server_url=""):
    payload = {
        2: 17,
        5: b"",
        6: b"",
        8: bio_text,
        9: 1,
        11: b"",
        12: b"",
    }
    encrypted_data = aes_encrypt_payload(encode_proto_payload(payload))
    urls = []
    if server_url:
        urls.append(f"{server_url.rstrip('/')}/UpdateSocialBasicInfo")
    urls.extend(
        [
            "https://clientbp.ggpolarbear.com/UpdateSocialBasicInfo",
            "https://clientbp.ggblueshark.com/UpdateSocialBasicInfo",
            "https://client.ind.freefiremobile.com/UpdateSocialBasicInfo",
            "https://client.us.freefiremobile.com/UpdateSocialBasicInfo",
        ]
    )

    last_error = ""
    tried = set()
    for url in urls:
        if url in tried:
            continue
        tried.add(url)
        host = urlparse(url).netloc
        try:
            response = requests.post(
                url,
                data=encrypted_data,
                headers={
                    "Expect": "100-continue",
                    "Authorization": f"Bearer {jwt_token}",
                    "X-Unity-Version": "2018.4.11f1",
                    "X-GA": "v1 1",
                    "ReleaseVersion": RELEASE_VERSION,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Content-Length": str(len(encrypted_data)),
                    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 11; SM-A305F Build/RP1A.200720.012)",
                    "Host": host,
                    "Connection": "Keep-Alive",
                    "Accept-Encoding": "gzip",
                },
                timeout=15,
            )
            if response.status_code == 200:
                return True, ""
            last_error = response_brief(response)
        except requests.exceptions.RequestException as e:
            last_error = str(e)

    return False, last_error or "No update response."


def call_bio_api(access_token, bio_text):
    if len(bio_text) >= 250:
        return {"status": "error", "message": "BIO must be less than 250 characters."}

    try:
        login_info, error = get_bio_jwt(access_token)
        if error:
            return {"status": "error", "message": error}

        ok, error = update_bio_with_jwt(login_info["token"], bio_text, login_info.get("server_url", ""))
        if not ok:
            return {"status": "error", "message": error}

        uid = login_info.get("account_id") or "N/A"
        region = login_info.get("region") or "N/A"
        nickname = fetch_bio_player_name(uid, region)

        return {
            "status": "success",
            "uid": uid,
            "nickname": nickname or "N/A",
            "region": region,
            "bio": bio_text,
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "message": "Bio update timed out. Please try again."}
    except requests.exceptions.ConnectionError:
        return {"status": "error", "message": "Could not connect to Free Fire bio server."}
    except Exception as e:
        logger.error(f"Bio update failed: {e}", exc_info=True)
        return {"status": "error", "message": "Bio update failed. Check bot logs."}


def fetch_bio_player_name(uid, region):
    if not uid or uid == "N/A":
        return ""

    responses = []
    response = call_ffinfo_api(region if region != "N/A" else "", uid)
    responses.append(response)
    if "error" in response:
        responses.append(call_direct_ffinfo_api(region if region != "N/A" else "", uid))

    for item in responses:
        if not isinstance(item, dict) or "error" in item:
            continue
        data = item.get("data") if isinstance(item.get("data"), dict) else item
        basic = pick(data, "basicInfo", "AccountInfo", default={})
        nickname = pick(basic, "nickname", "AccountName", "PlayerNickname", default="")
        if nickname and nickname != "N/A":
            return str(nickname).strip()
        nickname = pick(data, "nickname", "AccountName", "PlayerNickname", default="")
        if nickname and nickname != "N/A":
            return str(nickname).strip()
    return ""


def call_api(region, uid):
    url = f"{API_BASE_URL}/like"
    try:
        logger.info(f"Calling like API: url={url} region={region} uid={uid}")
        response = requests.get(
            url,
            params={"uid": uid, "server_name": region},
            timeout=API_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            try:
                payload = response.json()
                return {
                    "error": payload.get("error") or "Like request could not be completed.",
                    "available_regions": payload.get("available_regions"),
                    "status_code": response.status_code,
                }
            except ValueError:
                return {
                    "error": "Like request could not be completed.",
                    "status_code": response.status_code,
                }
        return response.json()
    except requests.exceptions.ConnectionError as e:
        logger.error(f"API connection failed: {e}")
        return {"error": f"Cannot connect to API at {API_BASE_URL}. Start app.py or fix API_BASE_URL."}
    except requests.exceptions.Timeout:
        logger.error(f"API timeout after {API_TIMEOUT_SECONDS}s for uid={uid} region={region}")
        return {"error": f"API timed out after {API_TIMEOUT_SECONDS}s. Please try again."}
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        return {"error": "API failed. Please try again later."}
    except ValueError:
        return {"error": "Invalid JSON response from API."}


def format_like_error(response, region, uid):
    message = str(response.get("error", ""))
    available_regions = response.get("available_regions") or []
    status_code = response.get("status_code")

    if "No valid token found for region" in message or available_regions:
        available = ", ".join(available_regions) if available_regions else "none"
        return (
            "⚠️ Likes are not available for this region right now.\n\n"
            f"🆔 UID: {uid}\n"
            f"🌐 Requested Region: {region}\n"
            f"✅ Available Token Regions: {available}\n\n"
            "Please try a supported region or ask the owner to add a valid account for this region."
        )

    if "token expired" in message.lower() or "valid token" in message.lower():
        return "⚠️ Like tokens are unavailable right now. Please try again later."

    if status_code and status_code >= 500:
        return "⚠️ The like service is temporarily unavailable. Please try again later."

    return f"❌ Unable to send likes.\n\nReason: {message or 'Unknown error'}"


def call_ffinfo_api(region, uid):
    url = f"{API_BASE_URL}/ffinfo"
    params = {"uid": uid}
    if region:
        params["server_name"] = region
    try:
        logger.info(f"Calling FF info API: url={url} region={region or 'AUTO'} uid={uid}")
        response = requests.get(url, params=params, timeout=API_TIMEOUT_SECONDS)
        if response.status_code != 200:
            try:
                payload = response.json()
                message = payload.get("error") or "Player information could not be fetched."
                return {
                    "error": message,
                    "details": payload.get("details"),
                    "status_code": response.status_code,
                }
            except ValueError:
                return {
                    "error": "Player information could not be fetched.",
                    "status_code": response.status_code,
                }
        return response.json()
    except requests.exceptions.ConnectionError as e:
        logger.error(f"FF info API connection failed: {e}")
        return {"error": f"Cannot connect to API at {API_BASE_URL}. Start app.py or fix API_BASE_URL."}
    except requests.exceptions.Timeout:
        logger.error(f"FF info API timeout after {API_TIMEOUT_SECONDS}s for uid={uid} region={region}")
        return {"error": f"API timed out after {API_TIMEOUT_SECONDS}s. Please try again."}
    except requests.exceptions.RequestException as e:
        logger.error(f"FF info API request failed: {e}")
        return {"error": "API failed. Please try again later."}
    except ValueError:
        return {"error": "Invalid JSON response from API."}


def call_direct_ffinfo_api(region, uid):
    if not FFINFO_API_URL:
        return {"error": "Direct player info API is disabled."}

    last_error = {"error": "Direct player information could not be fetched."}

    try:
        logger.info(f"Calling direct FF info API: url={FFINFO_API_URL} uid={uid}")
        params = {"uid": uid}
        if FFINFO_API_KEY:
            params["key"] = FFINFO_API_KEY
        response = requests.get(
            FFINFO_API_URL,
            params=params,
            timeout=API_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            return {
                "error": "Direct player information could not be fetched.",
                "details": {
                    "status_code": response.status_code,
                    "body": response.text[:180],
                },
                "status_code": response.status_code,
            }
        data = response.json()
        basic_info = data.get("basicInfo", {}) if isinstance(data, dict) else {}
        resolved_region = str(basic_info.get("region") or region or "").strip().upper()
        return {
            "source": "external-direct",
            "Region": resolved_region,
            "data": data,
            "status": 1,
        }
    except requests.exceptions.Timeout:
        logger.error(f"Direct FF info API timeout after {API_TIMEOUT_SECONDS}s for uid={uid}")
        last_error = {"error": f"Direct API timed out after {API_TIMEOUT_SECONDS}s. Please try again."}
    except requests.exceptions.RequestException as e:
        logger.error(f"Direct FF info API request failed: {e}")
        last_error = {"error": "Direct player info API failed. Please try again later."}
    except ValueError:
        last_error = {"error": "Invalid JSON response from direct player info API."}

    return last_error

def extract_ffinfo_region(response):
    if not isinstance(response, dict):
        return ""

    region = str(response.get("Region") or "").strip().upper()
    if region:
        return region

    data = response.get("data", response)
    if isinstance(data, dict) and "result" in data and isinstance(data["result"], dict):
        data = data["result"]
    if not isinstance(data, dict):
        return ""

    basic = pick(data, "basicInfo", "AccountInfo", default={})
    if isinstance(basic, dict):
        return str(pick(basic, "region", "AccountRegion", default="")).strip().upper()
    return ""


def resolve_uid_region(uid):
    response = call_ffinfo_api("", uid)
    if "error" in response and is_token_error(response.get("error", "")):
        ok, count, total, failed_uids = refresh_tokens_from_uidpass()
        if ok:
            logger.info(
                f"Auto-refreshed tokens before region lookup. total={total} valid={count} failed={len(failed_uids)}"
            )
            response = call_ffinfo_api("", uid)

    if "error" in response:
        return "", response.get("error", "Could not resolve UID region.")

    resolved_region = extract_ffinfo_region(response)
    if not resolved_region:
        return "", "Could not detect UID region."
    return resolved_region, ""


def format_ffinfo_error(response, region, uid):
    status_code = response.get("status_code")
    details = response.get("details") or {}
    detail_status = details.get("status_code") if isinstance(details, dict) else None
    requested_region = (region or "").upper()

    if status_code in {404, 502} or detail_status in {404, 500, 502}:
        if requested_region:
            return (
                "❌ Player profile not found\n\n"
                f"🆔 UID: {uid}\n"
                f"🌐 Region: {requested_region}\n\n"
                "Please check that the UID and region are correct, then try again."
            )
        return (
            "❌ Player profile not found\n\n"
            f"🆔 UID: {uid}\n\n"
            "I could not find this player in the supported regions. Try again with the region, for example:\n"
            f"/ffinfo sg {uid}"
        )

    if status_code == 429:
        return "⏳ The player info service is busy. Please try again in a few minutes."

    if status_code and status_code >= 500:
        return "⚠️ The player info service is temporarily unavailable. Please try again later."

    return f"❌ Unable to fetch player profile.\n\nReason: {response.get('error', 'Unknown error')}"


def pick(data, *paths, default="N/A"):
    for path in paths:
        value = data
        for key in path.split("."):
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                value = None
                break
        if value not in (None, ""):
            return value
    return default


def format_number(value):
    if value in (None, "", "N/A"):
        return "N/A"
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return str(value)


def format_unix_date(value):
    if value in (None, "", "N/A"):
        return "N/A"
    try:
        return datetime.fromtimestamp(int(value)).strftime("%d %b %Y")
    except (TypeError, ValueError, OSError):
        return str(value)


def clean_enum(value):
    text = str(value or "N/A")
    for prefix in ("Language_", "ExternalIconShowType_", "AccountPrivacy_", "Privacy_"):
        text = text.replace(prefix, "")
    return text.replace("_", " ")


def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def roman_number(value):
    numerals = {
        1: "I",
        2: "II",
        3: "III",
        4: "IV",
        5: "V",
        6: "VI",
        7: "VII",
        8: "VIII",
        9: "IX",
        10: "X",
    }
    return numerals.get(value, str(value))


def star_label(count):
    if count is None or count <= 0:
        return ""
    if count <= 5:
        return " " + ("★" * count)
    return f" ★{count}"


def br_heroic_stars(points):
    points = to_int(points)
    if points is None or points < 3200:
        return None
    return max(1, min(5, ((points - 3200) // 500) + 1))


def br_master_stars(points):
    points = to_int(points)
    if points is None:
        return None
    thresholds = (
        (10000, 5),
        (9000, 4),
        (8000, 3),
        (7100, 2),
        (6300, 1),
    )
    for minimum_points, stars in thresholds:
        if points >= minimum_points:
            return stars
    return None


def cs_visible_stars(rank, points):
    points = to_int(points)
    if points is None:
        return None
    return points


def cs_rank_name(rank, points):
    rank_code = to_int(rank)
    if rank_code is not None:
        if rank_code == 321:
            stars = to_int(points)
            if stars is not None and stars >= 100:
                return "Heroic"
            return "Diamond V"
        if rank_code >= 322:
            stars = to_int(points)
            if stars is not None and stars >= 127:
                return "Master"
            return "Heroic"
        if rank_code >= 315:
            return f"Diamond {roman_number(rank_code - 314)}"
        if rank_code >= 311:
            return f"Platinum {roman_number(rank_code - 310)}"
        if rank_code >= 307:
            return f"Gold {roman_number(rank_code - 306)}"
        if rank_code >= 304:
            return f"Silver {roman_number(rank_code - 303)}"
        if rank_code >= 301:
            return f"Bronze {roman_number(rank_code - 300)}"

    stars = to_int(points)
    if stars is None:
        return "N/A"

    tiers = (
        ("Bronze", 0, 3, 3),
        ("Silver", 9, 3, 4),
        ("Gold", 21, 4, 4),
        ("Platinum", 37, 4, 5),
        ("Diamond", 57, 4, 5),
    )
    for name, start, divisions, stars_per_division in tiers:
        tier_total = divisions * stars_per_division
        if start <= stars < start + tier_total:
            division = ((stars - start) // stars_per_division) + 1
            return f"{name} {roman_number(division)}"

    if stars < 127:
        return "Heroic"
    return "Master"


def rank_name(value, points=None, mode="br"):
    rank = to_int(value)
    rank_points = to_int(points)
    if rank is None:
        return str(value or "N/A")

    if mode == "cs":
        return cs_rank_name(rank, rank_points)

    if rank >= 326:
        stars = br_master_stars(rank_points)
        if rank_points is not None and rank_points >= 8000:
            return f"Elite Master{star_label(stars)}"
        if rank_points is not None and rank_points >= 6300:
            return f"Master{star_label(stars)}"
        return "Master"
    if rank >= 321:
        stars = br_heroic_stars(rank_points)
        if stars is None:
            stars = min(max(rank - 320, 1), 5)
        return f"Heroic{star_label(min(stars, 5))}"
    if rank >= 318:
        return "Master"
    if rank >= 315:
        return f"Diamond {roman_number(rank - 314)}"
    if rank >= 311:
        return f"Platinum {roman_number(rank - 310)}"
    if rank >= 307:
        return f"Gold {roman_number(rank - 306)}"
    if rank >= 304:
        return f"Silver {roman_number(rank - 303)}"
    if rank >= 301:
        return f"Bronze {roman_number(rank - 300)}"
    if rank >= 215:
        return f"Diamond {roman_number(rank - 214)}"
    if rank >= 211:
        return f"Platinum {roman_number(rank - 210)}"
    if rank >= 207:
        return f"Gold {roman_number(rank - 206)}"
    if rank >= 204:
        return f"Silver {roman_number(rank - 203)}"
    if rank >= 201:
        return f"Bronze {roman_number(rank - 200)}"
    return str(rank)


def format_percent(part, total):
    part_num = to_int(part)
    total_num = to_int(total)
    if part_num is None or total_num in (None, 0):
        return "N/A"
    return f"{round((part_num / total_num) * 100)}%"


def build_ffinfo_text(payload, requester):
    data = payload.get("data", payload)
    if isinstance(data, dict) and "result" in data and isinstance(data["result"], dict):
        data = data["result"]

    basic = pick(data, "basicInfo", "AccountInfo", default={})
    clan = pick(data, "clanBasicInfo", "GuildInfo", default={})
    captain = pick(data, "captainBasicInfo", default={})
    profile = pick(data, "profileInfo", "AccountProfileInfo", default={})
    social = pick(data, "socialInfo", "socialinfo", default={})
    pet = pick(data, "petInfo", default={})
    credit = pick(data, "creditScoreInfo", default={})

    nickname = pick(basic, "nickname", "AccountName", "PlayerNickname")
    uid = pick(basic, "accountId", "AccountId", "AccountID", "UID")
    region = pick(basic, "region", "AccountRegion", default=payload.get("Region", "N/A"))
    level = pick(basic, "level", "AccountLevel")
    exp = pick(basic, "exp", "AccountEXP")
    likes = pick(basic, "liked", "likes", "AccountLikes", "Likes")
    badges = pick(basic, "badgeCnt", "badgeCount", "AccountBPBadges", "badges")
    prime_level = pick(basic, "primeLevel.level", "primeInfo.primeLevel", default=0)
    title = pick(basic, "title", "Title", "AccountTitle", "TitleID")
    created = pick(basic, "createAt", "createdAt", "AccountCreateTime")
    last_login = pick(basic, "lastLoginAt", "lastLogin", "AccountLastLogin")
    br_rank = pick(basic, "rank", "BrRank", "maxRank", "BrMaxRank")
    br_points = pick(basic, "rankingPoints", "BrRankPoint")
    cs_rank = pick(basic, "csRank", "CsRank", "csMaxRank", "CsMaxRank")
    cs_points = pick(basic, "csRankingPoints", "CsRankPoint", "csRankPoint")
    cs_stars = cs_visible_stars(cs_rank, cs_points)
    clan_members = pick(clan, "memberNum", "GuildMember")
    clan_capacity = pick(clan, "capacity", "GuildCapacity")
    captain_uid = pick(clan, "captainId", "GuildOwner", "ownerId")
    captain_name = pick(captain, "nickname", "AccountName", default="N/A")
    captain_level = pick(captain, "level", "AccountLevel")
    captain_likes = pick(captain, "liked", "AccountLikes")
    captain_br_rank = pick(captain, "rank", "BrRank", "maxRank", "BrMaxRank")
    captain_br_points = pick(captain, "rankingPoints", "BrRankPoint")
    captain_cs_rank = pick(captain, "csRank", "CsRank", "csMaxRank", "CsMaxRank")
    captain_cs_points = pick(captain, "csRankingPoints", "CsRankPoint", "csRankPoint")
    captain_cs_stars = cs_visible_stars(captain_cs_rank, captain_cs_points)
    captain_last_login = pick(captain, "lastLoginAt", "AccountLastLogin")

    equipped_skills = pick(profile, "equipedSkills", "equippedSkills", "EquippedSkills", default=[])
    if isinstance(equipped_skills, list):
        equipped_skills = len(equipped_skills)

    credit_score = pick(credit, "creditScore", "score")
    credit_valid = pick(credit, "periodicSummaryEndTime", "validUntil")
    credit_status = "Good" if str(credit_score).isdigit() and int(credit_score) >= 90 else "N/A"

    requested_at = datetime.now().strftime("%d %b %Y %H:%M:%S")
    requester_name = str(requester.first_name or requester.username or requester.id)
    sections = [
        (
            "🎮 FREE FIRE PROFILE\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "👤 BASIC INFORMATION\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"🏷️ Nickname: {nickname}\n"
            f"🆔 UID: {uid}\n"
            f"🌐 Region: {str(region).upper()}\n"
            f"📈 Level: {format_number(level)}\n"
            f"✨ EXP: {format_number(exp)}\n"
            f"💙 Likes: {format_number(likes)}\n"
            f"🎖 Badges: {format_number(badges)}\n"
            f"💎 Prime Level: {format_number(prime_level)}\n"
            f"🏅 Title ID: {format_number(title)}\n"
            f"📆 Created: {format_unix_date(created)}\n"
            f"⏱ Last Login: {format_unix_date(last_login)}"
        ),
        (
            "🏆 BATTLE ROYALE\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"🥇 Rank: {rank_name(br_rank, br_points, mode='br')}\n"
            f"📊 Points: {format_number(br_points)}"
        ),
        (
            "⚔️ CLASH SQUAD\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"🥈 Rank: {rank_name(cs_rank, cs_points, mode='cs')}\n"
            f"⭐️Total Stars: {format_number(cs_points)}"
        ),
    ]

    has_clan = any(
        pick(clan, key, default=None) not in (None, "", "N/A")
        for key in ("clanId", "Guildid", "GuildID", "clanName", "GuildName")
    )
    if has_clan:
        sections.append(
            "🏰 CLAN INFORMATION\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"🏷️ Name: {pick(clan, 'clanName', 'GuildName')}\n"
            f"🆔 ID: {format_number(pick(clan, 'clanId', 'Guildid', 'GuildID'))}\n"
            f"📈 Level: {format_number(pick(clan, 'clanLevel', 'GuildLevel'))}\n"
            f"👥 Members: {format_number(clan_members)}/{format_number(clan_capacity)} ({format_percent(clan_members, clan_capacity)})\n"
            f"👑 Captain: {captain_name}\n"
            f"🆔 Captain UID: {format_number(captain_uid)}"
        )

    has_captain = has_clan and (
        captain_uid not in (None, "", "N/A")
        or any(
            pick(captain, key, default=None) not in (None, "", "N/A")
            for key in ("accountId", "AccountId", "nickname", "AccountName")
        )
    )
    if has_captain:
        sections.append(
            "👑 CAPTAIN INFORMATION\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"🏷️ Name: {captain_name}\n"
            f"🆔 UID: {format_number(pick(captain, 'accountId', 'AccountId', default=captain_uid))}\n"
            f"🌐 Region: {str(pick(captain, 'region', 'AccountRegion', default='N/A')).upper()}\n"
            f"📈 Level: {format_number(captain_level)}\n"
            f"💙 Likes: {format_number(captain_likes)}\n"
            f"🏆 BR Rank: {rank_name(captain_br_rank, captain_br_points, mode='br')}\n"
            f"📊 Points: {format_number(captain_br_points)}\n"
            f"⚔️ CS Rank: {rank_name(captain_cs_rank, captain_cs_points, mode='cs')}\n"
            f"⭐️Total Stars: {format_number(captain_cs_points)}\n"
            f"⏱ Last Login: {format_unix_date(captain_last_login)}"
        )

    sections.extend([
        (
            "🎨 PROFILE STYLE\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"🖼 Avatar ID: {format_number(pick(profile, 'avatarId', 'AccountAvatarId'))}\n"
            f"⚡ Skills Equipped: {format_number(equipped_skills)}"
        ),
        (
            "💬 SOCIAL\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"📝 Bio: {pick(social, 'signature', 'AccountSignature', default='No bio')}\n"
            f"🌐 Language: {clean_enum(pick(social, 'language', 'AccountLanguage'))}\n"
            f"🔐 Privacy: {clean_enum(pick(social, 'privacy', 'AccountPrivacy'))}"
        ),
        (
            "🐾 PET INFORMATION\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"🆔 ID: {format_number(pick(pet, 'id', 'PetId'))}\n"
            f"📈 Level: {format_number(pick(pet, 'level', 'PetLevel'))}\n"
            f"✨ EXP: {format_number(pick(pet, 'exp', 'PetEXP'))}\n"
            f"🎨 Skin ID: {format_number(pick(pet, 'skinId', 'SkinId'))}"
        ),
        (
            "✅ CREDIT SCORE\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"💯 Score: {format_number(credit_score)}/100\n"
            f"📌 Status: {credit_status}\n"
            f"⏳ Valid Until: {format_unix_date(credit_valid)}"
        ),
        (
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🙋 Requested by: {requester_name}\n"
            f"🕒 Time: {requested_at}"
        ),
    ])

    return "\n\n".join(sections)


def build_local_ffinfo_text(payload, requester):
    data = payload.get("data", payload)
    basic = pick(data, "basicInfo", "AccountInfo", default={})

    nickname = pick(basic, "nickname", "AccountName", "PlayerNickname")
    uid = pick(basic, "accountId", "AccountId", "AccountID", "UID")
    region = pick(basic, "region", "AccountRegion", default=payload.get("Region", "N/A"))
    likes = pick(basic, "liked", "likes", "AccountLikes", "Likes")
    requested_at = datetime.now().strftime("%d %b %Y %H:%M:%S")
    requester_name = str(requester.first_name or requester.username or requester.id)

    return (
        "🎮 FREE FIRE PROFILE\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👤 BASIC INFORMATION\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🏷️ Nickname: {nickname}\n"
        f"🆔 UID: {uid}\n"
        f"🌐 Region: {str(region).upper()}\n"
        f"💙 Likes: {format_number(likes)}\n\n"
        "ℹ️ Full profile data is unavailable from the fallback local decoder.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🙋 Requested by: {requester_name}\n"
        f"🕒 Time: {requested_at}"
    )


def get_user_limit(user_id):
    if user_id == OWNER_ID:
        return 999999999
    return 1


def load_json_file(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json_file(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def build_guest_name(base_name=""):
    base_name = "".join(char for char in str(base_name or "").strip() if char.isprintable())
    superscript_digits = "⁰¹²³⁴⁵⁶⁷⁸⁹"
    if not base_name:
        name_digits = "".join(superscript_digits[int(digit)] for digit in str(random.randint(1, 9999)))
        return f"0xMe{name_digits}"

    suffix = "".join(superscript_digits[int(digit)] for digit in str(random.randint(1, 999)))
    return f"{base_name[: max(1, 12 - len(suffix))]}{suffix}"


def is_guest_name_taken_response(response):
    if response is None:
        return False

    text = response.text[:500].lower()
    taken_markers = (
        "name already",
        "nickname already",
        "already exist",
        "already taken",
        "duplicate name",
        "name is taken",
        "nickname is taken",
    )
    return any(marker in text for marker in taken_markers)


def response_brief(response, limit=160):
    if response is None:
        return "no response"
    text = str(response.text or "").replace("\r", " ").replace("\n", " ").strip()
    return f"HTTP {response.status_code} - {text[:limit]}"


def response_json_or_error(response, label):
    try:
        return response.json(), ""
    except ValueError:
        return None, f"{label} returned invalid JSON: {response_brief(response)}"


def short_guestgen_warning(error):
    text = str(error or "").strip()
    if not text:
        return ""

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        try:
            data = json.loads(text[start:end + 1])
            message = data.get("msg") or data.get("message") or data.get("type")
            if message:
                return str(message).replace("_", " ")
        except ValueError:
            pass

    lowered = text.lower()
    if "account_not_found" in lowered or "account not found" in lowered:
        return "account not found"
    if "missing account_id" in lowered or "did not return account_id" in lowered:
        return "account id not returned"
    if text.startswith("MajorLogin check failed:"):
        text = text.split(":", 1)[1].strip()
    if " - " in text:
        text = text.rsplit(" - ", 1)[-1].strip()
    return text[:120]


def redact_proxy_url(proxy_url):
    if not proxy_url:
        return "direct"
    parsed = urlparse(proxy_url)
    if not parsed.hostname:
        return "proxy"
    host = parsed.hostname
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{host}{port}"


def renew_guestgen_tor_ip():
    try:
        import socket

        with socket.create_connection((GUESTGEN_TOR_CONTROL_HOST, GUESTGEN_TOR_CONTROL_PORT), timeout=10) as sock:
            password = GUESTGEN_TOR_CONTROL_PASSWORD.replace('"', '\\"')
            if password:
                sock.sendall(f'AUTHENTICATE "{password}"\r\n'.encode())
            else:
                sock.sendall(b'AUTHENTICATE ""\r\n')
            auth_response = sock.recv(1024)
            if not auth_response.startswith(b"250"):
                return False, f"Tor authenticate failed: {auth_response.decode(errors='ignore').strip()}"

            sock.sendall(b"SIGNAL NEWNYM\r\n")
            newnym_response = sock.recv(1024)
            if not newnym_response.startswith(b"250"):
                return False, f"Tor NEWNYM failed: {newnym_response.decode(errors='ignore').strip()}"

            sock.sendall(b"QUIT\r\n")

        time.sleep(max(0, GUESTGEN_TOR_RENEW_WAIT_SECONDS))
        return True, "Tor NEWNYM sent"
    except Exception as e:
        return False, str(e)


def is_guestgen_tor_proxy_ready():
    try:
        import socket

        parsed = urlparse(GUESTGEN_TOR_PROXY_URL)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 9050
        with socket.create_connection((host, port), timeout=1.5):
            return True
    except Exception:
        return False


def disable_guestgen_proxy_temporarily(reason, seconds=60):
    global guestgen_proxy_disabled_until
    guestgen_proxy_disabled_until = time.time() + max(1, seconds)
    logger.warning(f"/guestgen proxy disabled temporarily, using direct network: {reason}")


def is_socks_proxy_error(error):
    text = str(error).lower()
    return "socks" in text or "proxy" in text or "127.0.0.1:9050" in text


def get_guestgen_proxy():
    global guestgen_proxy_use_count
    if not GUESTGEN_PROXY_ROTATION_ENABLED or not GUESTGEN_TOR_PROXY_URL:
        return ""
    if time.time() < guestgen_proxy_disabled_until:
        return ""
    if not is_guestgen_tor_proxy_ready():
        disable_guestgen_proxy_temporarily("Tor SOCKS port is not reachable")
        return ""

    with guestgen_proxy_lock:
        guestgen_proxy_use_count += 1
        if guestgen_proxy_use_count >= GUESTGEN_PROXY_ROTATE_EVERY:
            guestgen_proxy_use_count = 0
            ok, detail = renew_guestgen_tor_ip()
            if ok:
                logger.info(f"/guestgen Tor rotation: {detail}")
            else:
                logger.warning(f"/guestgen Tor rotation failed: {detail}")
        return GUESTGEN_TOR_PROXY_URL


def ordered_major_register_urls(region):
    if str(region or "").upper() in COMMON_MAJOR_REGISTER_REGIONS:
        return (COMMON_MAJOR_REGISTER_URL, *MAJOR_REGISTER_URLS)
    return MAJOR_REGISTER_URLS


def ordered_major_login_urls(region=None):
    if str(region or "").upper() in COMMON_MAJOR_LOGIN_REGIONS:
        return (COMMON_MAJOR_LOGIN_URL, *MAJOR_LOGIN_URLS)
    return MAJOR_LOGIN_URLS


def configure_guestgen_session(session, proxy_url=""):
    retry = Retry(
        total=GUESTGEN_HTTP_RETRIES,
        connect=GUESTGEN_HTTP_RETRIES,
        read=GUESTGEN_HTTP_RETRIES,
        status=GUESTGEN_HTTP_RETRIES,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=None,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=GUESTGEN_POOL_SIZE,
        pool_maxsize=GUESTGEN_POOL_SIZE,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    if proxy_url:
        session.proxies.update({"http": proxy_url, "https": proxy_url})
    return session


def encode_varint(number):
    result = bytearray()
    while number:
        byte = number & 0x7F
        number >>= 7
        result.append(byte | (0x80 if number else 0))
    return bytes(result or b"\x00")


def encode_proto_field(field, value):
    if isinstance(value, bool):
        value = int(value)
    if isinstance(value, int):
        return encode_varint((field << 3) | 0) + encode_varint(value)
    if isinstance(value, dict):
        data = encode_proto_payload(value)
    else:
        data = value.encode() if isinstance(value, str) else value
    return encode_varint((field << 3) | 2) + encode_varint(len(data)) + data


def encode_proto_payload(payload):
    packet = bytearray()
    for field in sorted(payload):
        packet.extend(encode_proto_field(field, payload[field]))
    return bytes(packet)


def aes_encrypt_payload(payload):
    cipher = AES.new(MAIN_KEY, AES.MODE_CBC, MAIN_IV)
    return cipher.encrypt(pad(payload, AES.block_size))


def encode_open_id(value):
    key = [0, 0, 0, 2, 0, 1, 7, 0, 0, 0, 0, 0, 2, 0, 1, 7, 0, 0, 0, 0, 0, 2, 0, 1, 7, 0, 0, 0, 0, 0, 2, 0]
    return bytes(byte ^ key[index % len(key)] ^ 48 for index, byte in enumerate(value.encode()))


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


def build_major_login_payload(open_id, access_token, platform=None, language="en"):
    platform = str(platform or "4")
    language = str(language or "en")
    payload = {
        3: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        4: "free fire",
        5: 1,
        7: "1.123.1",
        8: "Android OS 9 / API-28 (PQ3B.190801.10101846/G9650ZHU2ARC6)",
        9: "Handheld",
        10: "Verizon",
        11: "WIFI",
        12: 1920,
        13: 1080,
        14: "280",
        15: "ARM64 FP ASIMD AES VMH | 2865 | 4",
        16: 3003,
        17: "Adreno (TM) 640",
        18: "OpenGL ES 3.1 v1.46",
        19: "Google|34a7dcdf-a7d5-4cb6-8d7e-3b0e448a0c57",
        20: "223.191.51.89",
        21: language,
        22: open_id,
        23: "4",
        24: "Handheld",
        25: {6: 55, 8: 81},
        29: access_token,
        30: 1,
        41: "Verizon",
        42: "WIFI",
        57: "7428b253defc164018c604a1ebbfebdf",
        60: 36235,
        61: 31335,
        62: 2519,
        63: 703,
        64: 25010,
        65: 26628,
        66: 32992,
        67: 36235,
        73: 3,
        74: "/data/app/com.dts.freefireth-YPKM8jHEwAJlhpmhDhv5MQ==/lib/arm64",
        76: 1,
        77: "5b892aaabd688e571f688053118a162b|/data/app/com.dts.freefireth-YPKM8jHEwAJlhpmhDhv5MQ==/base.apk",
        78: 3,
        79: 2,
        81: "64",
        83: "2019118695",
        86: "OpenGLES2",
        87: 16383,
        88: 4,
        89: b"FwQVTgUPX1UaUllDDwcWCRBpWA0FUgsvA1snWlBaO1kFYg==",
        92: 13564,
        93: "android",
        94: "KqsHTymw5/5GB23YGniUYN2/q47GATrq7eFeRatf0NkwLKEMQ0PK5BKEk72dPflAxUlEBir6Vtey83XqF593qsl8hwY=",
        95: 110009,
        97: 1,
        98: 1,
        99: platform,
        100: platform,
    }
    return aes_encrypt_payload(encode_proto_payload(payload))


def build_legacy_major_login_payload(open_id, access_token, language="en"):
    lang = str(language or "en").encode("ascii", errors="ignore") or b"en"
    payload_parts = [
        b'\x1a\x132025-08-30 05:19:21"\tfree fire(\x01:\x081.114.13B2Android OS 9 / API-28 (PI/rel.cjw.20220518.114133)J\x08HandheldR\nATM MobilsZ\x04WIFI`\xb6\nh\xee\x05r\x03300z\x1fARMv7 VFPv3 NEON VMH | 2400 | 2\x80\x01\xc9\x0f\x8a\x01\x0fAdreno (TM) 640\x92\x01\rOpenGL ES 3.2\x9a\x01+Google|dfa4ab4b-9dc4-454e-8065-e70c733fa53f\xa2\x01\x0e105.235.139.91\xaa\x01\x02',
        lang,
        b'\xb2\x01 1d8ec0240ede109973f3321b9354b44d\xba\x01\x014\xc2\x01\x08Handheld\xca\x01\x10Asus ASUS_I005DA\xea\x01@afcfbf13334be42036e4f742c80b956344bed760ac91b3aff9b607a610ab4390\xf0\x01\x01\xca\x02\nATM Mobils\xd2\x02\x04WIFI\xca\x03 7428b253defc164018c604a1ebbfebdf\xe0\x03\xa8\x81\x02\xe8\x03\xf6\xe5\x01\xf0\x03\xaf\x13\xf8\x03\x84\x07\x80\x04\xe7\xf0\x01\x88\x04\xa8\x81\x02\x90\x04\xe7\xf0\x01\x98\x04\xa8\x81\x02\xc8\x04\x01\xd2\x04=/data/app/com.dts.freefireth-PdeDnOilCSFn37p1AH_FLg==/lib/arm\xe0\x04\x01\xea\x04_2087f61c19f57f2af4e7feff0b24d9d9|/data/app/com.dts.freefireth-PdeDnOilCSFn37p1AH_FLg==/base.apk\xf0\x04\x03\xf8\x04\x01\x8a\x05\x0232\x9a\x05\n2019118692\xb2\x05\tOpenGLES2\xb8\x05\xff\x7f\xc0\x05\x04\xe0\x05\xf3F\xea\x05\x07android\xf2\x05pKqsHT5ZLWrYljNb5Vqh//yFRlaPHSO9NWSQsVvOmdhEEn7W+VHNUK+Q+fduA3ptNrGB0Ll0LRz3WW0jOwesLj6aiU7sZ40p8BfUE/FI/jzSTwRe2\xf8\x05\xfb\xe4\x06\x88\x06\x01\x90\x06\x01\x9a\x06\x014\xa2\x06\x014\xb2\x06"GQ@O\x00\x0e^\x00D\x06UA\x0ePM\r\x13hZ\x07T\x06\x0cm\\V\x0ejYV;\x0bU5',
    ]
    payload = b"".join(payload_parts)
    payload = payload.replace(b"afcfbf13334be42036e4f742c80b956344bed760ac91b3aff9b607a610ab4390", access_token.encode())
    payload = payload.replace(b"1d8ec0240ede109973f3321b9354b44d", open_id.encode())
    return aes_encrypt_payload(payload)


def get_guest_major_login_info(session, access_token, open_id, region="", language="en"):
    payload_builders = (
        ("protobuf", build_major_login_payload),
        ("legacy-template", build_legacy_major_login_payload),
    )
    last_error = ""
    for payload_name, payload_builder in payload_builders:
        encrypted_payload = payload_builder(open_id, access_token, language=language)
        for login_url in ordered_major_login_urls(region):
            host = urlparse(login_url).netloc
            try:
                response = session.post(
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
                        "User-Agent": random.choice(GUESTGEN_MAJOR_LOGIN_USER_AGENTS),
                        "X-GA": "v1 1",
                        "X-Unity-Version": "2018.4.11f1",
                    },
                    timeout=20,
                )
                if response.status_code == 200 and response.content:
                    login_info = decode_major_login_info(response.content)
                    if login_info.get("account_id") or login_info.get("token"):
                        login_info["major_login_payload"] = payload_name
                        return login_info, ""
                    last_error = f"MajorLogin {payload_name} response missing account_id/token."
                    continue
                last_error = f"MajorLogin {payload_name}: {response_brief(response)}"
            except requests.exceptions.RequestException as e:
                last_error = f"MajorLogin {payload_name}: {e}"
            except Exception as e:
                last_error = f"MajorLogin {payload_name} decode failed: {e}"
    return {}, last_error or "no response"


def register_guest_account(region, base_name=""):
    secret = b"2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3"
    client_id = "100067"
    user_agent = "GarenaMSDK/4.0.19P9(SM-S908E; Android 11; en; IN)"
    major_user_agent = "Dalvik/2.1.0 (Linux; U; Android 13; A063 Build/TKQ1.221220.001)"
    proxy_url = get_guestgen_proxy()
    proxy_label = redact_proxy_url(proxy_url)
    session = configure_guestgen_session(requests.Session(), proxy_url=proxy_url)

    try:
        if proxy_url:
            logger.info(f"/guestgen using proxy {proxy_label}")
        language = GUESTGEN_REGION_LANG.get(region.upper(), "en")
        password = str(random.randint(1000000000, 9999999999))
        password_hash = hashlib.sha256(password.encode()).hexdigest().upper()

        body = json.dumps(
            {"password": password_hash, "client_type": 2, "source": 2, "app_id": int(client_id)},
            separators=(",", ":"),
        )
        headers = {
            "Host": "100067.connect.garena.com",
            "User-Agent": user_agent,
            "Authorization": f"Signature {hmac.new(secret, body.encode(), hashlib.sha256).hexdigest()}",
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip",
        }

        response = session.post(
            GUEST_REGISTER_URL,
            data=body,
            headers=headers,
            timeout=15,
        )
        if response.status_code != 200:
            return None, None, None, {}, f"Guest register failed: {response_brief(response)}"

        register_data, json_error = response_json_or_error(response, "Guest register")
        if json_error:
            return None, None, None, {}, json_error
        uid = (register_data.get("data") or {}).get("uid") or register_data.get("uid")
        if not uid:
            return None, None, None, {}, "Guest register did not return UID."

        token_body = {
            "uid": str(uid),
            "password": password_hash,
            "response_type": "token",
            "client_type": "2",
            "client_secret": secret.decode(),
            "client_id": client_id,
        }
        response = None
        last_token_error = ""
        for token_url in GUEST_TOKEN_GRANT_URLS:
            token_host = urlparse(token_url).netloc
            try:
                if "/api/v2/" in token_url:
                    token_json = json.dumps(
                        {
                            "uid": str(uid),
                            "password": password_hash,
                            "response_type": "token",
                            "client_type": 2,
                            "client_secret": secret.decode(),
                            "client_id": int(client_id),
                        },
                        separators=(",", ":"),
                    )
                    response = session.post(
                        token_url,
                        data=token_json,
                        headers={
                            "Host": token_host,
                            "User-Agent": user_agent,
                            "Authorization": f"Signature {hmac.new(secret, token_json.encode(), hashlib.sha256).hexdigest()}",
                            "Content-Type": "application/json; charset=utf-8",
                            "Accept": "application/json",
                            "Connection": "Keep-Alive",
                            "Accept-Encoding": "gzip",
                        },
                        timeout=15,
                    )
                else:
                    response = session.post(
                        token_url,
                        data=token_body,
                        headers={
                            "Host": token_host,
                            "User-Agent": user_agent,
                            "Connection": "Keep-Alive",
                            "Accept-Encoding": "gzip",
                        },
                        timeout=15,
                    )
                if response.status_code == 200:
                    break
                last_token_error = response_brief(response)
            except requests.exceptions.RequestException as e:
                last_token_error = str(e)

        if response is None or response.status_code != 200:
            return None, None, None, {}, f"Token grant failed: {last_token_error or response_brief(response)}"

        token_data, json_error = response_json_or_error(response, "Token grant")
        if json_error:
            return None, None, None, {}, json_error
        access_token, open_id = extract_guest_token_credentials(token_data)
        if not access_token or not open_id:
            return None, None, None, {}, "Token grant did not return access token/open_id."

        major_headers = {
            "Authorization": f"Bearer {access_token}",
            "X-Unity-Version": "2018.4.11f1",
            "X-GA": "v1 1",
            "ReleaseVersion": RELEASE_VERSION,
            "Content-Type": "application/octet-stream",
            "User-Agent": major_user_agent,
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip",
            "Expect": "100-continue",
        }

        tried_guest_names = set()
        guest_name = None
        response = None
        last_major_error = ""
        for name_attempt in range(20):
            for _ in range(10):
                guest_name = build_guest_name(base_name)
                if guest_name not in tried_guest_names:
                    tried_guest_names.add(guest_name)
                    break

            payload = {
                1: guest_name,
                2: access_token,
                3: open_id,
                5: 102000007,
                6: 4,
                7: 1,
                13: 1,
                14: encode_open_id(open_id),
                15: language,
                16: 1,
                17: 1,
            }
            encrypted_data = aes_encrypt_payload(encode_proto_payload(payload))
            major_headers["Content-Length"] = str(len(encrypted_data))

            for major_url in ordered_major_register_urls(region):
                major_headers["Host"] = urlparse(major_url).netloc
                for auth_value in (f"Bearer {access_token}", "Bearer"):
                    major_headers["Authorization"] = auth_value
                    for attempt in range(3):
                        try:
                            response = session.post(
                                major_url,
                                data=encrypted_data,
                                headers=major_headers,
                                timeout=15,
                            )
                        except requests.exceptions.RequestException as e:
                            last_major_error = str(e)
                            response = None
                            break

                        if response.status_code not in {500, 502, 503, 504}:
                            break
                        last_major_error = response_brief(response)
                        time.sleep(2 * (attempt + 1))

                    if response is not None and (response.status_code == 200 or is_guest_name_taken_response(response)):
                        break

                if response is not None and (response.status_code == 200 or is_guest_name_taken_response(response)):
                    break

            if response is not None and (response.status_code == 200 or not is_guest_name_taken_response(response)):
                break

        if response is not None and response.status_code == 200:
            login_info, login_error = get_guest_major_login_info(session, access_token, open_id, region=region, language=language)
            if proxy_url:
                login_info["proxy"] = proxy_label
            if login_error:
                return str(uid), password_hash, guest_name, login_info, f"MajorLogin check failed: {login_error}"
            return str(uid), password_hash, guest_name, login_info, None
        return str(uid), password_hash, guest_name, {}, f"Major register failed: {response_brief(response) if response is not None else last_major_error or 'no response'}"
    except requests.exceptions.Timeout:
        return None, None, None, {}, "Guest generation timed out."
    except requests.exceptions.RequestException as e:
        if proxy_url and is_socks_proxy_error(e):
            disable_guestgen_proxy_temporarily(e)
        return None, None, None, {}, f"Guest generation request failed: {e}"
    except Exception as e:
        logger.error(f"Guest generation failed: {e}", exc_info=True)
        return None, None, None, {}, "Guest generation failed. Check bot logs."
    finally:
        session.close()


def register_guest_account_with_retry(region, base_name=""):
    last_result = (None, None, None, {}, "Guest generation did not start.")
    for attempt in range(1, max(1, GUESTGEN_ACCOUNT_ATTEMPTS) + 1):
        result = register_guest_account(region, base_name)
        uid, password, guest_name, login_info, error = result
        if uid and password:
            return result

        last_result = result
        if attempt < GUESTGEN_ACCOUNT_ATTEMPTS:
            time.sleep(0.7 * attempt)

    return last_result


def add_uidpass_entry(uid, password):
    records = load_json_file(UIDPASS_FILE, [])
    for item in records:
        if str(item.get("uid", "")).strip() == uid:
            return False, len(records)

    records.append({"uid": uid, "password": password})
    save_json_file(UIDPASS_FILE, records)
    return True, len(records)


def update_uidpass_entry(uid, password):
    records = load_json_file(UIDPASS_FILE, [])
    for item in records:
        if str(item.get("uid", "")).strip() == uid:
            item["password"] = password
            save_json_file(UIDPASS_FILE, records)
            return True, len(records)
    return False, len(records)


def upsert_token_entry(uid, token):
    tokens = load_json_file(TOKEN_FILE, [])
    if not isinstance(tokens, list):
        tokens = []

    updated = False
    for item in tokens:
        if isinstance(item, dict) and str(item.get("uid", "")).strip() == uid:
            item["token"] = token
            updated = True
            break

    if not updated:
        tokens.append({"uid": uid, "token": token})

    save_json_file(TOKEN_FILE, tokens)
    return len(tokens)


def check_jwt(uid, password, attempts=JWT_RETRY_ATTEMPTS, delay=JWT_RETRY_DELAY_SECONDS):
    last_error = ""
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                JWT_API_URL,
                params={"uid": uid, "password": password},
                timeout=20,
            )
            if response.status_code != 200:
                last_error = f"HTTP {response.status_code}: {response.text[:160]}"
            else:
                data = response.json()
                token = data.get("token")
                status = str(data.get("status", "")).strip().lower()
                if token and (not status or status == "success"):
                    return data, "", attempt
                last_error = data.get("error") or data.get("message") or "token missing in response"
        except ValueError:
            last_error = "Invalid JSON response from JWT API."
        except requests.exceptions.Timeout:
            last_error = "JWT API timed out."
        except requests.exceptions.RequestException as e:
            last_error = f"JWT API request failed: {e}"

        if attempt < attempts:
            time.sleep(delay)

    return None, last_error or "JWT check failed.", attempts


def short_secret(value, head=18, tail=12):
    text = str(value or "").strip()
    if not text:
        return "N/A"
    if len(text) <= head + tail + 3:
        return text
    return f"{text[:head]}...{text[-tail:]}"


def format_jwt_check_result(uid, data, attempts):
    access_token = data.get("access_token", "N/A")
    account_id = data.get("account_id", "N/A")
    region = data.get("region", "N/A")
    token = data.get("token", "N/A")

    return (
        "✅ JWT CHECK SUCCESS\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"👤 Account ID: {account_id}\n"
        f"🌐 Region: {region}\n"
        "\n"
        "🔑 Access Token\n"
        f"{access_token}\n\n"
        "🎟 JWT Token\n"
        f"{token}"
    )


def extract_jwt_account_id(data):
    if not isinstance(data, dict):
        return ""
    for key in ("account_id", "accountId", "account_uid", "accountUid", "uid"):
        value = str(data.get(key, "")).strip()
        if value and value.lower() != "none":
            return value
    return ""


def refresh_single_uidpass(uid, password):
    try:
        from update_tokens import fetch_token
    except Exception as e:
        return False, 0, f"import error: {e}"

    token = fetch_token(uid, password)
    if not token:
        return False, len(load_json_file(TOKEN_FILE, [])), "token fetch failed"

    total_tokens = upsert_token_entry(uid, token)
    return True, total_tokens, ""


def fetch_single_token(uid, password):
    try:
        from update_tokens import fetch_token
    except Exception as e:
        return None, f"import error: {e}"

    token = fetch_token(uid, password)
    if not token:
        return None, "token fetch failed"
    return token, ""


def notify_owner_token_cleanup(removed_duplicates, removed_failed, removed_invalid):
    if not removed_duplicates and not removed_failed and not removed_invalid:
        return

    def compact(items, limit=12):
        values = []
        for item in items:
            if not item:
                continue
            if isinstance(item, dict):
                uid = str(item.get("uid", "")).strip()
                password = str(item.get("password", "")).strip()
                values.append(f"{uid}:{password}" if password else uid)
            else:
                values.append(str(item))
        if len(values) > limit:
            return ", ".join(values[:limit]) + f", +{len(values) - limit} more"
        return ", ".join(values) if values else "None"

    text = (
        "UID/PASS token refresh report\n\n"
        f"Duplicate skipped: {len(removed_duplicates)}\n"
        f"Failed token: {len(removed_failed)}\n"
        f"Invalid skipped: {len(removed_invalid)}\n\n"
        f"Duplicate UID: {compact(removed_duplicates)}\n"
        f"Failed UID/PASS: {compact(removed_failed)}\n"
        f"Invalid UID: {compact(removed_invalid)}"
    )
    try:
        bot.send_message(OWNER_ID, text)
    except Exception as e:
        logger.error(f"Failed to notify owner about UID/PASS cleanup: {e}")


def refresh_tokens_from_uidpass():
    try:
        from update_tokens import fetch_token
    except Exception as e:
        return False, 0, 0, [f"import error: {e}"]

    uidpass_list = load_json_file(UIDPASS_FILE, [])
    original_total = len(uidpass_list)
    seen_uids = set()
    new_tokens = []
    failed_uids = []
    removed_duplicates = []
    removed_invalid = []

    for item in uidpass_list:
        uid = str(item.get("uid", "")).strip()
        password = str(item.get("password", "")).strip()
        if not uid or not password:
            removed_invalid.append(uid or "missing_uid")
            continue
        if uid in seen_uids:
            removed_duplicates.append(uid)
            continue
        seen_uids.add(uid)
        token = fetch_token(uid, password)
        if token:
            new_tokens.append({"uid": uid, "token": token})
        else:
            failed_uids.append({"uid": uid, "password": password})

    if new_tokens:
        save_json_file(TOKEN_FILE, new_tokens)
        if removed_duplicates or removed_invalid or failed_uids:
            notify_owner_token_cleanup(removed_duplicates, failed_uids, removed_invalid)
        return True, len(new_tokens), original_total, failed_uids

    if removed_duplicates or removed_invalid:
        notify_owner_token_cleanup(removed_duplicates, [], removed_invalid)

    return False, 0, original_total, failed_uids


def is_token_error(error_message):
    msg = str(error_message).lower()
    token_error_markers = [
        "failed to load tokens",
        "no valid token found",
        "valid token",
        "update tokens.json",
    ]
    return any(marker in msg for marker in token_error_markers)


def scheduled_token_refresh():
    interval_seconds = max(300, int(AUTO_TOKEN_REFRESH_HOURS * 60 * 60))
    while True:
        try:
            ok, count, total, failed_uids = refresh_tokens_from_uidpass()
            if ok:
                logger.info(
                    f"Scheduled token refresh complete. total={total} valid={count} failed={len(failed_uids)}"
                )
            else:
                logger.error(
                    f"Scheduled token refresh failed. total={total} valid={count} failed={len(failed_uids)}"
                )
        except Exception as e:
            logger.error(f"Scheduled token refresh crashed: {e}", exc_info=True)
        time.sleep(interval_seconds)


threading.Thread(target=reset_limits, daemon=True).start()
if ENABLE_AUTO_TOKEN_REFRESH:
    threading.Thread(target=scheduled_token_refresh, daemon=True).start()


@app.route("/")
def home():
    return jsonify({"status": "Bot is running", "bot": "Free Fire Likes Bot", "health": "OK"})


@app.route("/health")
def health():
    return jsonify({"status": "healthy"}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        json_str = request.get_data().decode("UTF-8")
        update = telebot.types.Update.de_json(json_str)
        bot.process_new_updates([update])
        return "", 200
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return "", 500


@bot.message_handler(commands=["start"])
def start_command(message):
    user_id = message.from_user.id
    if not is_user_in_channel(user_id):
        bot.reply_to(
            message,
            "Channel membership required.\nTo use this bot, you must join all our channels first.",
            reply_markup=build_join_markup(),
        )
        return

    if user_id not in like_tracker:
        like_tracker[user_id] = {"used": 0, "last_used": datetime.now(timezone.utc) - timedelta(days=1)}

    bot.reply_to(message, "You are verified. Use /like to send likes.")


@bot.message_handler(commands=["like"])
def handle_like(message):
    user_id = message.from_user.id
    args = message.text.split()

    if message.chat.type == "private" and user_id != OWNER_ID:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Join Official Group", url=GROUP_JOIN_LINK))
        bot.reply_to(
            message,
            "Sorry, this command is not allowed here.\n\nJoin our official group:",
            reply_markup=markup,
        )
        return

    if not is_user_in_channel(user_id):
        bot.reply_to(message, "You must join all our channels to use this command.", reply_markup=build_join_markup())
        return

    if len(args) != 3:
        bot.reply_to(message, "Format: /like server_name uid")
        return

    region, uid = args[1], args[2]
    if not region.isalpha() or not uid.isdigit():
        bot.reply_to(message, "Invalid input. Use: /like server_name uid")
        return

    threading.Thread(target=process_like, args=(message, region.upper(), uid), daemon=True).start()


@bot.message_handler(commands=["ffinfo"])
def handle_ffinfo(message):
    user_id = message.from_user.id
    args = message.text.split()

    if message.chat.type == "private" and user_id != OWNER_ID:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Join Official Group", url=GROUP_JOIN_LINK))
        bot.reply_to(
            message,
            "Sorry, this command is not allowed here.\n\nJoin our official group:",
            reply_markup=markup,
        )
        return

    if not is_user_in_channel(user_id):
        bot.reply_to(message, "You must join all our channels to use this command.", reply_markup=build_join_markup())
        return

    if len(args) == 2:
        region, uid = "", args[1]
    elif len(args) == 3:
        region, uid = args[1].upper(), args[2]
    else:
        bot.reply_to(message, "Format: /ffinfo uid\nOr: /ffinfo server_name uid")
        return

    if (region and not region.isalpha()) or not uid.isdigit():
        bot.reply_to(message, "Invalid input. Use: /ffinfo uid or /ffinfo server_name uid")
        return

    threading.Thread(target=process_ffinfo, args=(message, region, uid), daemon=True).start()


def process_ffinfo(message, region, uid):
    processing_msg = bot.reply_to(message, "Please wait... Fetching player info.")
    response = call_ffinfo_api(region, uid)
    auto_refreshed = False

    if "error" in response:
        status_code = response.get("status_code")
        error_message = str(response.get("error", ""))
        can_try_direct = (
            (status_code and status_code >= 500)
            or error_message.startswith("Cannot connect to API")
            or "timed out" in error_message.lower()
            or error_message in {"API failed. Please try again later.", "Invalid JSON response from API."}
        )
        if can_try_direct:
            direct_response = call_direct_ffinfo_api(region, uid)
            if "error" not in direct_response:
                response = direct_response

        if is_token_error(response.get("error", "")):
            ok, count, total, failed_uids = refresh_tokens_from_uidpass()
            auto_refreshed = ok
            if ok:
                logger.info(
                    f"Auto-refreshed tokens from uidpass.json. total={total} valid={count} failed={len(failed_uids)}"
                )
                response = call_ffinfo_api(region, uid)

        if "error" in response and auto_refreshed:
            response["error"] = f"{response['error']} (tokens auto-refreshed and retried once)"

        if "error" in response:
            text = format_ffinfo_error(response, region, uid)
            logger.error(f"FF info API error for uid={uid} region={region or 'AUTO'}: {response['error']}")
            try:
                bot.edit_message_text(text=text, chat_id=processing_msg.chat.id, message_id=processing_msg.message_id)
            except Exception:
                bot.reply_to(message, text)
            return

    try:
        if response.get("source") == "local":
            text = build_local_ffinfo_text(response, message.from_user)
        else:
            text = build_ffinfo_text(response, message.from_user)
        bot.edit_message_text(
            text=text,
            chat_id=processing_msg.chat.id,
            message_id=processing_msg.message_id,
        )
    except Exception as e:
        logger.error(f"Error in process_ffinfo: {e}", exc_info=True)
        error_msg = f"❌ Error: {str(e)[:100]}"
        try:
            bot.edit_message_text(
                text=error_msg,
                chat_id=processing_msg.chat.id,
                message_id=processing_msg.message_id,
            )
        except Exception:
            bot.reply_to(message, error_msg)


def process_like(message, region, uid):
    user_id = message.from_user.id
    now_utc = datetime.now(timezone.utc)
    usage = like_tracker.get(user_id, {"used": 0, "last_used": now_utc - timedelta(days=1)})

    if now_utc.date() > usage["last_used"].date():
        usage["used"] = 0

    max_limit = get_user_limit(user_id)
    if usage["used"] >= max_limit:
        bot.reply_to(message, "You have exceeded your daily request limit.")
        return

    processing_msg = bot.reply_to(message, "Please wait... Checking UID region.")
    requested_region = region
    resolved_region, region_error = resolve_uid_region(uid)
    if resolved_region:
        region = resolved_region
    elif region_error:
        logger.info(f"Could not auto-detect region for UID {uid}: {region_error}")

    try:
        bot.edit_message_text(
            text=f"Please wait... Sending likes to {region}.",
            chat_id=processing_msg.chat.id,
            message_id=processing_msg.message_id,
        )
    except Exception:
        pass

    response = call_api(region, uid)
    auto_refreshed = False

    if "error" in response:
        if is_token_error(response.get("error", "")):
            ok, count, total, failed_uids = refresh_tokens_from_uidpass()
            auto_refreshed = ok
            if ok:
                logger.info(
                    f"Auto-refreshed tokens from uidpass.json. total={total} valid={count} failed={len(failed_uids)}"
                )
                response = call_api(region, uid)

        if "error" in response and not resolved_region:
            fallback_region, fallback_error = resolve_uid_region(uid)
            if fallback_region and fallback_region != region:
                logger.info(f"Retrying like for UID {uid} with detected region {fallback_region} after {region} failed.")
                requested_region = region
                region = fallback_region
                response = call_api(region, uid)
            elif fallback_error:
                logger.info(f"Like fallback region lookup failed for UID {uid}: {fallback_error}")

        if "error" in response and auto_refreshed:
            response["error"] = f"{response['error']} (tokens auto-refreshed and retried once)"

        text = format_like_error(response, region, uid)
        logger.error(f"Like API error for uid={uid} region={region}: {response['error']}")
        try:
            bot.edit_message_text(text=text, chat_id=processing_msg.chat.id, message_id=processing_msg.message_id)
        except Exception:
            bot.reply_to(message, text)
        return

    if not isinstance(response, dict):
        try:
            bot.edit_message_text(
                text="Invalid API response. Please try again later.",
                chat_id=processing_msg.chat.id,
                message_id=processing_msg.message_id,
            )
        except Exception:
            bot.reply_to(message, "Invalid UID or unable to fetch data.")
        return

    try:
        # Log the full response for debugging
        logger.info(f"API Response for UID {uid}: {response}")
        
        # Extract player info with safe defaults
        player_uid = str(response.get("UID") or uid).strip()
        player_name = str(response.get("PlayerNickname") or "Unknown").strip()
        region_name = str(response.get("Region") or region).strip()
        likes_before = str(response.get("LikesbeforeCommand") or 0)
        likes_after = str(response.get("LikesafterCommand") or 0)
        likes_given = str(response.get("LikesGivenByAPI") or 0)

        status = int(response.get("status") or 0)
        if status == 1:
            usage["used"] += 1
            usage["last_used"] = now_utc
            like_tracker[user_id] = usage
            title = "✅ Request processed successfully"
        else:
            title = "UID already reached max likes for now."

        response_text = (
            f"{title}\n\n"
            f"👤 Name: {player_name}\n"
            f"🆔 UID: {player_uid}\n"
            f"🌍 Region: {region_name}\n"
            f"❤️ Likes Before: {likes_before}\n"
            f"➕ Likes Added: {likes_given}\n"
            f"⭐ Total Likes Now: {likes_after}\n"
            f"📊 Remaining Requests: {max_limit - usage['used']}\n"
            "\n🔗 Credit: @Mean_Un"
        )

        bot.edit_message_text(
            text=response_text,
            chat_id=processing_msg.chat.id,
            message_id=processing_msg.message_id,
        )
    except Exception as e:
        logger.error(f"Error in process_like: {e}", exc_info=True)
        error_msg = f"❌ Error: {str(e)[:100]}"
        try:
            bot.edit_message_text(
                text=error_msg,
                chat_id=processing_msg.chat.id,
                message_id=processing_msg.message_id,
            )
        except Exception:
            bot.reply_to(message, error_msg)


@bot.message_handler(commands=["bio"])
def handle_bio(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    try:
        bot.delete_message(chat_id, message.message_id)
    except Exception as e:
        logger.info(f"Could not delete /bio command message: {e}")

    if not is_user_in_channel(user_id):
        bot.send_message(chat_id, "You must join all our channels to use this command.", reply_markup=build_join_markup())
        return

    parts = message.text.strip().split(None, 2)
    if len(parts) < 3:
        bot.send_message(
            chat_id,
            "Format: /bio <access_token_or_link> <new bio>\n\n"
            "Accepted token formats:\n"
            "- Plain lowercase token\n"
            "- Kiosgamer link with ?eat=\n"
            "- Garena Help link with ?access_token=",
        )
        return

    token = extract_access_token(parts[1])
    bio_text = parts[2].strip()

    if token is None:
        bot.send_message(
            chat_id,
            "Invalid access token format.\n\n"
            "Send a plain lowercase token, a Kiosgamer link, or a Garena Help access_token link.",
        )
        return

    if not bio_text:
        bot.send_message(chat_id, "Bio text cannot be empty.")
        return

    processing_msg = bot.send_message(chat_id, "Please wait... Updating bio.")
    result = call_bio_api(token, bio_text)

    if str(result.get("status", "")).lower() == "success":
        nickname = result.get("nickname", "Unknown")
        uid = result.get("uid", "N/A")
        region = result.get("region", "N/A")
        new_bio = result.get("bio", bio_text)
        text = (
            "✅ Bio updated successfully\n\n"
            f"👤 Player: {nickname}\n"
            f"🆔 UID: {uid}\n"
            f"🌐 Region: {region}\n\n"
            f"📝 New Bio: {new_bio}\n\n"
            f"Support: {OWNER_USERNAME}"
        )
    else:
        error_msg = result.get("message", "Unknown error occurred.")
        text = f"❌ Failed to update bio\n\nReason: {error_msg}"

    try:
        bot.edit_message_text(text=text, chat_id=processing_msg.chat.id, message_id=processing_msg.message_id)
    except Exception:
        bot.send_message(chat_id, text)


@bot.message_handler(commands=["guestgen"])
def handle_guestgen(message):
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "This command is owner only.")
        return

    args = message.text.split()
    if len(args) < 3:
        bot.reply_to(message, GUESTGEN_USAGE)
        return

    region = args[1].strip().upper()
    if region not in GUESTGEN_REGIONS:
        bot.reply_to(message, f"{GUESTGEN_USAGE}\n\nInvalid region. Use one of: {', '.join(GUESTGEN_REGIONS)}")
        return

    base_name = " ".join(args[2:]).strip()
    threading.Thread(target=process_guestgen, args=(message, region, base_name), daemon=True).start()


def process_guestgen(message, region, base_name=""):
    processing_msg = bot.reply_to(
        message,
        f"Please wait... Generating guest account for {region}.",
    )
    uid, password, guest_name, login_info, error = register_guest_account_with_retry(region, base_name)
    account_id = str((login_info or {}).get("account_id") or "").strip()
    login_region = str((login_info or {}).get("lock_region") or (login_info or {}).get("region") or region).strip().upper()
    proxy_label = str((login_info or {}).get("proxy") or "").strip()
    proxy_line = f"Proxy: {html.escape(proxy_label)}\n" if proxy_label else ""
    safe_region = html.escape(login_region or region)
    safe_guest_name = html.escape(str(guest_name or "N/A"))
    safe_account_id = html.escape(account_id or "N/A")
    safe_uid = html.escape(str(uid or "N/A"))
    safe_password = html.escape(str(password or "N/A"))
    reply_markup = None
    if uid and password:
        copy_payload = json.dumps(
            {
                "guest_account_info": {
                    "com.garena.msdk.guest_password": str(password),
                    "com.garena.msdk.guest_uid": str(uid),
                }
            },
            separators=(",", ":"),
        )
        reply_markup = InlineKeyboardMarkup()
        reply_markup.add(InlineKeyboardButton("Copy guest_account_info", copy_text=CopyTextButton(copy_payload)))

    if uid and password and error:
        warning = short_guestgen_warning(error)
        warning_lines = [warning] if warning else []
        if not account_id and warning != "account not found":
            warning_lines.append("MajorLogin response did not return account_id")
        warning_text = html.escape("; ".join(warning_lines))
        text = (
            "Guest account generated\n\n"
            f"Region: {safe_region}\n"
            f"{proxy_line}"
            f"Name: {safe_guest_name}\n"
            f"Account ID: <code>{safe_account_id}</code>\n"
            f"UID: <code>{safe_uid}</code>\n"
            f"Password: <code>{safe_password}</code>\n\n"
            f"Warning: {warning_text}"
        )
    elif error:
        text = f"Guest generation failed\n\nRegion: {html.escape(region)}\nReason: {html.escape(str(error))}"
    else:
        warning_text = ""
        if not account_id:
            warning_text = "\n\nWarning: account id not returned"
        text = (
            "Guest account generated\n\n"
            f"Region: {safe_region}\n"
            f"{proxy_line}"
            f"Name: {safe_guest_name}\n"
            f"Account ID: <code>{safe_account_id}</code>\n"
            f"UID: <code>{safe_uid}</code>\n"
            f"Password: <code>{safe_password}</code>"
            f"{warning_text}"
        )

    try:
        bot.edit_message_text(
            text=text,
            chat_id=processing_msg.chat.id,
            message_id=processing_msg.message_id,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )
    except Exception:
        bot.reply_to(message, text, parse_mode="HTML", reply_markup=reply_markup)


def process_checkjwt(message, uid, password):
    processing_msg = bot.reply_to(
        message,
        f"⏳ Checking JWT for UID {uid}...\nRetries: {JWT_RETRY_ATTEMPTS} | Delay: {JWT_RETRY_DELAY_SECONDS}s",
    )
    data, error, attempts = check_jwt(uid, password)

    if data:
        text = format_jwt_check_result(uid, data, attempts)
    else:
        text = (
            "❌ JWT CHECK FAILED\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"🆔 UID: {uid}\n"
            f"🔁 Attempts: {attempts}/{JWT_RETRY_ATTEMPTS}\n"
            f"⏱ Delay: {JWT_RETRY_DELAY_SECONDS}s\n\n"
            f"Reason: {error}"
        )

    try:
        bot.edit_message_text(text=text, chat_id=processing_msg.chat.id, message_id=processing_msg.message_id)
    except Exception:
        bot.reply_to(message, text)


@bot.message_handler(commands=["remain", "uidpass", "adduidpass", "updateuidpass", "addremain", "checkjwt"])
def owner_commands(message):
    if message.from_user.id != OWNER_ID:
        return

    args = message.text.split()
    cmd = args[0].lower()

    if cmd == "/remain":
        lines = ["Remaining Daily Requests Per User:"]
        if not like_tracker:
            lines.append("No users have used the bot yet today.")
        else:
            for uid, usage in like_tracker.items():
                limit = get_user_limit(uid)
                used = usage.get("used", 0)
                limit_str = "Unlimited" if limit > 1000 else str(limit)
                lines.append(f"{uid} -> {used}/{limit_str}")
        bot.reply_to(message, "\n".join(lines))
        return

    if cmd == "/uidpass":
        records = load_json_file(UIDPASS_FILE, [])
        bot.reply_to(message, f"UID/PASS records: {len(records)}")
        return

    if cmd == "/checkjwt":
        if len(args) != 3:
            bot.reply_to(message, "Use: /checkjwt <uid> <password>")
            return

        uid = args[1].strip()
        password = args[2].strip()

        if not uid.isdigit():
            bot.reply_to(message, "UID must be numeric.")
            return

        threading.Thread(target=process_checkjwt, args=(message, uid, password), daemon=True).start()
        return

    if cmd == "/adduidpass":
        if len(args) != 3:
            bot.reply_to(message, "Use: /adduidpass <uid> <password>")
            return

        uid = args[1].strip()
        password = args[2].strip()

        if not uid.isdigit():
            bot.reply_to(message, "UID must be numeric.")
            return

        records = load_json_file(UIDPASS_FILE, [])
        if any(str(item.get("uid", "")).strip() == uid for item in records):
            bot.reply_to(message, f"DUPLICATE: UID {uid} already exists. No changes made. UID/PASS total: {len(records)}")
            return

        token, error = fetch_single_token(uid, password)
        if not token:
            bot.reply_to(message, f"WARN: UID {uid} not added. Error: {error}")
            return

        added, total = add_uidpass_entry(uid, password)
        if not added:
            bot.reply_to(message, f"DUPLICATE: UID {uid} already exists. No changes made. UID/PASS total: {total}")
            return

        count = upsert_token_entry(uid, token)
        bot.reply_to(
            message,
            f"OK: UID {uid} added. UID/PASS total: {total} | Tokens now: {count}",
        )
        return

    if cmd == "/updateuidpass":
        if len(args) != 3:
            bot.reply_to(message, "Use: /updateuidpass <uid> <new_password>")
            return

        uid = args[1].strip()
        password = args[2].strip()

        if not uid.isdigit():
            bot.reply_to(message, "UID must be numeric.")
            return

        records = load_json_file(UIDPASS_FILE, [])
        if not any(str(item.get("uid", "")).strip() == uid for item in records):
            bot.reply_to(message, f"NOT FOUND: UID {uid} does not exist in uidpass.json.")
            return

        token, error = fetch_single_token(uid, password)
        if not token:
            bot.reply_to(message, f"WARN: UID {uid} not updated. Error: {error}")
            return

        updated, total = update_uidpass_entry(uid, password)
        if not updated:
            bot.reply_to(message, f"NOT FOUND: UID {uid} does not exist in uidpass.json.")
            return

        count = upsert_token_entry(uid, token)
        bot.reply_to(
            message,
            f"OK: UID {uid} password updated. UID/PASS total: {total} | Tokens now: {count}",
        )
        return

    if cmd == "/addremain":
        if len(args) != 3:
            bot.reply_to(message, "Use: /addremain <userid> <n>\nExample: /addremain 123456789 5")
            return

        try:
            user_id = int(args[1])
            n = int(args[2])
        except ValueError:
            bot.reply_to(message, "User ID and N must be numbers.")
            return

        if n < 0:
            bot.reply_to(message, "N cannot be negative.")
            return

        # Update the user's daily limit
        if user_id not in like_tracker:
            like_tracker[user_id] = {"used": 0, "last_used": datetime.now(timezone.utc) - timedelta(days=1)}
        
        like_tracker[user_id]["used"] = max(0, get_user_limit(user_id) - n)
        bot.reply_to(message, f"✅ User {user_id}: Remaining requests set to {n}")
        return


@bot.message_handler(commands=["help"])
def help_command(message):
    user_id = message.from_user.id

    if user_id == OWNER_ID:
        help_text = (
            "Bot Commands:\n\n"
            "/like <region> <uid> - Send likes to Free Fire UID\n"
            "/ffinfo <uid> - Show Free Fire player info\n"
            "/ffinfo <region> <uid> - Show player info for a region\n"
            "/bio <token/link> <text> - Update Free Fire bio\n"
            "/start - Start or verify\n"
            "/help - Show this help menu\n\n"
            "Owner Commands:\n"
            "/remain - Show all users usage\n"
            "/uidpass - Show total UID/PASS records\n"
            "/checkjwt <uid> <password> - Check JWT API with retry\n"
            "/adduidpass <uid> <password> - Add UID/PASS and fetch only its token\n\n"
            "/updateuidpass <uid> <new_password> - Update password after token check\n"
            "/guestgen [region] [name] - Generate guest UID/PASS with auto-numbered name\n\n"
            f"Support: {OWNER_USERNAME}"
        )
        bot.reply_to(message, help_text)
        return

    # Allow help command even for users not in channel (but show join message)
    help_text = (
        "Bot Commands:\n\n"
        "/like <region> <uid> - Send likes to Free Fire UID\n"
        "/ffinfo <uid> - Show Free Fire player info\n"
        "/ffinfo <region> <uid> - Show player info for a region\n"
        "/bio <token/link> <text> - Update Free Fire bio\n"
        "/start - Start or verify\n"
        "/help - Show this help menu\n\n"
        f"Support: {OWNER_USERNAME}\n"
        "Join our channels for updates."
    )
    bot.reply_to(message, help_text)
    
    if not is_user_in_channel(user_id):
        bot.reply_to(message, "To use the /like command, you must join all our channels.", reply_markup=build_join_markup())


@bot.message_handler(func=lambda message: True, content_types=["text"])
def reply_all(message):
    if not message.text.startswith("/"):
        return

    known_commands = {
        "/start",
        "/like",
        "/ffinfo",
        "/bio",
        "/help",
        "/remain",
        "/uidpass",
        "/checkjwt",
        "/adduidpass",
        "/updateuidpass",
        "/addremain",
        "/guestgen",
    }
    command = message.text.split()[0].split("@")[0].lower()
    if command not in known_commands:
        bot.reply_to(message, "Unknown command. Use /help to see available commands.")


if __name__ == "__main__":
    mode = os.getenv("BOT_MODE", "polling").strip().lower()
    if mode == "webhook":
        port = int(os.getenv("PORT", "5000"))
        logger.info(f"Starting webhook mode on port {port}")
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
    else:
        logger.info("Starting polling mode")
        bot.remove_webhook()
        bot.infinity_polling(skip_pending=True, timeout=30, long_polling_timeout=30)
