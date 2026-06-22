import json
import logging
import os
import random
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from math import ceil
from urllib.parse import parse_qs, quote, urlparse
import hashlib
import html
import hmac
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
import telebot
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from flask import Flask, jsonify, request
from requests.adapters import HTTPAdapter
from telebot.types import CopyTextButton, InlineKeyboardButton, InlineKeyboardMarkup
from urllib3.util.retry import Retry

try:
    import MajorLoginReq_pb2
except Exception:
    MajorLoginReq_pb2 = None

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
FFINFO_LOOKUP_TIMEOUT_SECONDS = 15
AUTO_TOKEN_REFRESH_HOURS = float(os.getenv("AUTO_TOKEN_REFRESH_HOURS", "7"))
ENABLE_AUTO_TOKEN_REFRESH = os.getenv("ENABLE_AUTO_TOKEN_REFRESH", "true").strip().lower() in {"1", "true", "yes", "on"}
JWT_API_URL = os.getenv("JWT_API_URL", f"{API_BASE_URL}/token").strip()
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
ADMIN_IDS = {
    OWNER_ID,
    *{
        int(item.strip())
        for item in os.getenv("ADMIN_IDS", "").split(",")
        if item.strip().isdigit()
    },
}
AUTO_LIKE_GROUP_ID_RAW = os.getenv("AUTO_LIKE_GROUP_ID", os.getenv("TELEGRAM_GROUP_ID", "")).strip()
AUTO_LIKE_GROUP_ID = int(AUTO_LIKE_GROUP_ID_RAW) if AUTO_LIKE_GROUP_ID_RAW.lstrip("-").isdigit() else None
AUTO_LIKE_DB_FILE = os.getenv("AUTO_LIKE_DB_FILE", "autolikes.db")
AUTO_LIKE_GROUP_ID_ENV = AUTO_LIKE_GROUP_ID
try:
    AUTO_LIKE_TIMEZONE = ZoneInfo("Asia/Phnom_Penh")
except ZoneInfoNotFoundError:
    AUTO_LIKE_TIMEZONE = timezone(timedelta(hours=7), name="Asia/Phnom_Penh")
AUTO_LIKE_RUN_HOUR = int(os.getenv("AUTO_LIKE_RUN_HOUR", "9"))
AUTO_LIKE_RUN_MINUTE = int(os.getenv("AUTO_LIKE_RUN_MINUTE", "0"))
AUTO_LIKE_ORDER_DELAY_SECONDS = max(1, int(os.getenv("AUTO_LIKE_ORDER_DELAY_SECONDS", "30")))
AUTO_LIKE_VALID_SERVERS = {
    item.strip().upper()
    for item in os.getenv(
        "AUTO_LIKE_VALID_SERVERS",
        "BD,SG,IND,BR,US,SAC,NA,EU,ME,TH,VN,ID,TW,RU,PK,CIS",
    ).split(",")
    if item.strip()
}
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
GUESTGEN_DIRECT_REGIONS = {
    item.strip().upper()
    for item in os.getenv("GUESTGEN_DIRECT_REGIONS", "SG").split(",")
    if item.strip()
}
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


def call_ffinfo_api(region, uid, timeout_seconds=None, local_only=False):
    url = f"{API_BASE_URL}/ffinfo"
    params = {"uid": uid}
    if region:
        params["server_name"] = region
    if local_only:
        params["local"] = "1"
    timeout_seconds = timeout_seconds or API_TIMEOUT_SECONDS
    try:
        logger.info(f"Calling FF info API: url={url} region={region or 'AUTO'} uid={uid}")
        response = requests.get(url, params=params, timeout=timeout_seconds)
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
        logger.error(f"FF info API timeout after {timeout_seconds}s for uid={uid} region={region}")
        return {"error": f"API timed out after {timeout_seconds}s. Please try again."}
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
    response = call_ffinfo_api("", uid, timeout_seconds=FFINFO_LOOKUP_TIMEOUT_SECONDS)
    if "error" in response and is_token_error(response.get("error", "")):
        ok, count, total, failed_uids = refresh_tokens_from_uidpass()
        if ok:
            logger.info(
                f"Auto-refreshed tokens before region lookup. total={total} valid={count} failed={len(failed_uids)}"
            )
            response = call_ffinfo_api("", uid, timeout_seconds=FFINFO_LOOKUP_TIMEOUT_SECONDS)

    if "error" in response:
        return "", response.get("error", "Could not resolve UID region.")

    try:
        if is_player_not_found_error(json.dumps(response, ensure_ascii=False)):
            return "", "Player not found."
    except Exception:
        pass

    resolved_region = extract_ffinfo_region(response)
    if not resolved_region:
        return "", "Could not detect UID region."
    return resolved_region, ""


autolike_db_lock = threading.Lock()
autolike_run_lock = threading.Lock()


def is_admin_user(user_id):
    return int(user_id or 0) == OWNER_ID


def normalize_autolike_server(server):
    server = str(server or "").strip().upper()
    if server in {"EU", "EUR"}:
        server = "EUROPE"
    return server


def autolike_db():
    conn = sqlite3.connect(AUTO_LIKE_DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_autolike_db():
    with autolike_db_lock:
        with autolike_db() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS autolike_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    server TEXT NOT NULL,
                    uid TEXT NOT NULL,
                    player_name TEXT NOT NULL,
                    username TEXT NOT NULL,
                    telegram_user_id INTEGER NOT NULL,
                    likes_before_purchase INTEGER NOT NULL,
                    total_likes INTEGER NOT NULL,
                    purchase_date TEXT NOT NULL,
                    last_likes_before INTEGER NOT NULL,
                    likes_added_today INTEGER NOT NULL DEFAULT 0,
                    current_likes INTEGER NOT NULL,
                    total_delivered INTEGER NOT NULL DEFAULT 0,
                    remaining_likes INTEGER NOT NULL,
                    progress_percent REAL NOT NULL DEFAULT 0,
                    estimated_days INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_autolike_active_uid
                ON autolike_orders(uid)
                WHERE status = 'active'
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS autolike_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )


def row_to_dict(row):
    return dict(row) if row is not None else None


def get_autolike_setting(key, default=""):
    with autolike_db_lock:
        with autolike_db() as conn:
            row = conn.execute("SELECT value FROM autolike_settings WHERE key = ?", (key,)).fetchone()
            return str(row["value"]) if row else default


def set_autolike_setting(key, value):
    with autolike_db_lock:
        with autolike_db() as conn:
            conn.execute(
                """
                INSERT INTO autolike_settings(key, value)
                VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, str(value)),
            )


def delete_autolike_setting(key):
    with autolike_db_lock:
        with autolike_db() as conn:
            conn.execute("DELETE FROM autolike_settings WHERE key = ?", (key,))


def get_autolike_group_id():
    value = get_autolike_setting("group_id", "")
    if value.lstrip("-").isdigit():
        return int(value)
    return AUTO_LIKE_GROUP_ID_ENV


def autolike_now():
    return datetime.now(AUTO_LIKE_TIMEZONE)


def calc_autolike_fields(order, last_likes_before, current_likes):
    likes_before_purchase = int(order["likes_before_purchase"])
    total_likes = int(order["total_likes"])
    likes_added_today = max(0, int(current_likes) - int(last_likes_before))
    total_delivered = max(0, int(current_likes) - likes_before_purchase)
    remaining_likes = max(0, total_likes - total_delivered)
    progress_percent = min(100.0, round((total_delivered / total_likes) * 100, 2)) if total_likes > 0 else 100.0

    try:
        purchase_date = datetime.strptime(order["purchase_date"], "%Y-%m-%d").date()
        elapsed_days = max(1, (autolike_now().date() - purchase_date).days + 1)
    except Exception:
        elapsed_days = 1
    average_daily_likes = total_delivered / elapsed_days if elapsed_days else 0
    estimated_days = ceil(remaining_likes / average_daily_likes) if remaining_likes > 0 and average_daily_likes > 0 else 0
    status = "completed" if remaining_likes <= 0 else "active"
    return {
        "last_likes_before": int(last_likes_before),
        "likes_added_today": likes_added_today,
        "current_likes": int(current_likes),
        "total_delivered": total_delivered,
        "remaining_likes": remaining_likes,
        "progress_percent": progress_percent,
        "estimated_days": estimated_days,
        "status": status,
    }


def get_autolike_order(uid, active_only=False):
    query = "SELECT * FROM autolike_orders WHERE uid = ?"
    params = [str(uid)]
    if active_only:
        query += " AND status = 'active'"
    query += " ORDER BY id DESC LIMIT 1"
    with autolike_db_lock:
        with autolike_db() as conn:
            return row_to_dict(conn.execute(query, params).fetchone())


def get_active_autolike_orders():
    with autolike_db_lock:
        with autolike_db() as conn:
            return [row_to_dict(row) for row in conn.execute("SELECT * FROM autolike_orders WHERE status = 'active' ORDER BY id ASC")]


def get_user_autolike_orders(telegram_user_id):
    with autolike_db_lock:
        with autolike_db() as conn:
            return [
                row_to_dict(row)
                for row in conn.execute(
                    "SELECT * FROM autolike_orders WHERE telegram_user_id = ? AND status != 'removed' ORDER BY id ASC",
                    (int(telegram_user_id),),
                )
            ]


def update_autolike_order(uid, fields):
    assignments = ", ".join([f"{key} = ?" for key in fields])
    values = list(fields.values()) + [autolike_now().isoformat(timespec="seconds"), str(uid)]
    with autolike_db_lock:
        with autolike_db() as conn:
            conn.execute(
                f"UPDATE autolike_orders SET {assignments}, updated_at = ? WHERE uid = ? AND status = 'active'",
                values,
            )


def extract_player_snapshot(response):
    if not isinstance(response, dict) or "error" in response:
        return None, None, response.get("error", "Player information could not be fetched.") if isinstance(response, dict) else "Invalid player info response."

    data = response.get("data", response)
    if isinstance(data, dict) and isinstance(data.get("result"), dict):
        data = data["result"]
    if not isinstance(data, dict):
        return None, None, "Player information response is invalid."

    basic = pick(data, "basicInfo", "AccountInfo", default={})
    if not isinstance(basic, dict):
        basic = data

    player_name = str(pick(basic, "nickname", "AccountName", "PlayerNickname", default="Unknown")).strip() or "Unknown"
    likes = pick(basic, "liked", "likes", "AccountLikes", "Likes", default=None)
    try:
        return player_name, int(likes), ""
    except (TypeError, ValueError):
        return player_name, None, "Could not read current likes for this UID."


def fetch_autolike_player_snapshot(server, uid):
    response = call_ffinfo_api(server, uid)
    if "error" in response and is_token_error(response.get("error", "")):
        ok, count, total, failed_uids = refresh_tokens_from_uidpass()
        if ok:
            logger.info(
                f"Auto-refreshed tokens before AutoLike player lookup. total={total} valid={count} failed={len(failed_uids)}"
            )
            response = call_ffinfo_api(server, uid)
    if "error" in response:
        direct = call_direct_ffinfo_api(server, uid)
        if "error" not in direct:
            response = direct
    return extract_player_snapshot(response)


def autolike_daily_message(order):
    return (
        "✓ ʏᴏᴜʀ ᴅᴀɪʟʏ ᴀᴜᴛᴏʟɪᴋᴇ ᴜᴘᴅᴀᴛᴇ\n\n"
        "━━━━━━━━━━━━━━━\n"
        f"› ᴜɪᴅ : {order['uid']}\n"
        f"› ᴘʟᴀʏᴇʀ : {order['player_name']}\n\n"
        "› ᴘʀᴏɢʀᴇss\n"
        f"› ʟɪᴋᴇs ʙᴇғᴏʀᴇ : {order['last_likes_before']}\n"
        f"› ʟɪᴋᴇs ᴀᴅᴅᴇᴅ : {order['likes_added_today']}\n"
        f"› ᴄᴜʀʀᴇɴᴛ ʟɪᴋᴇs : {order['current_likes']}\n"
        f"› ᴛᴏᴛᴀʟ ᴅᴇʟɪᴠᴇʀᴇᴅ : {order['total_delivered']}/{order['total_likes']}\n\n"
        "› sᴛᴀᴛᴜs\n"
        f"› ᴘʀᴏɢʀᴇss : {order['progress_percent']}%\n"
        f"› ʀᴇᴍᴀɪɴɪɴɢ : {order['remaining_likes']}\n\n"
        "━━━━━━━━━━━━━━━\n"
        f"↳ ᴏᴡɴᴇʀ : {OWNER_USERNAME}"
    )


def autolike_details_line(order, number):
    return (
        f"#{number}\n"
        f"› ᴜɪᴅ : {order['uid']}\n"
        f"› ᴘʟᴀʏᴇʀ : {order['player_name']}\n"
        f"› ʟɪᴋᴇs ʙᴇғᴏʀᴇ ᴘᴜʀᴄʜᴀsᴇ : {order['likes_before_purchase']}\n"
        f"› ʟɪᴋᴇs ᴘᴜʀᴄʜᴀsᴇᴅ : {order['total_likes']}\n"
        f"› ᴘᴜʀᴄʜᴀsᴇ ᴅᴀᴛᴇ : {order['purchase_date']}\n"
        f"› ᴛᴏᴛᴀʟ ʟɪᴋᴇs ɢɪᴠᴇɴ ʙʏ ʙᴏᴛ : {order['total_delivered']}\n"
        f"› ᴘʀᴏɢʀᴇss : {order['progress_percent']}%\n"
        f"› ʀᴇᴍᴀɪɴɪɴɢ : {order['remaining_likes']}\n"
        f"› ᴇsᴛɪᴍᴀᴛᴇᴅ ᴛɪᴍᴇ : {order['estimated_days']} days"
    )


def send_autolike_update(order):
    text = autolike_daily_message(order)
    private_error = None
    try:
        bot.send_message(int(order["telegram_user_id"]), text)
    except Exception as e:
        private_error = e
        logger.warning(f"AutoLike private update failed for uid={order['uid']} user={order['telegram_user_id']}: {e}")

    group_id = get_autolike_group_id()
    if group_id:
        group_text = text
        if private_error:
            group_text += f"\n\nPrivate update failed for {order['username']} ({order['telegram_user_id']})."
        try:
            bot.send_message(group_id, group_text)
        except Exception as e:
            logger.error(f"AutoLike group update failed for uid={order['uid']}: {e}")


def process_autolike_order(order):
    uid = order["uid"]
    if order["status"] != "active":
        return

    last_likes_before = int(order["current_likes"])
    response = call_api(order["server"], uid)
    if "error" in response and is_token_error(response.get("error", "")):
        ok, count, total, failed_uids = refresh_tokens_from_uidpass()
        if ok:
            logger.info(f"AutoLike refreshed tokens. total={total} valid={count} failed={len(failed_uids)}")
            response = call_api(order["server"], uid)

    if "error" in response:
        logger.error(f"AutoLike API error for uid={uid}: {response['error']}")
        group_id = get_autolike_group_id()
        if group_id:
            bot.send_message(group_id, f"❌ AutoLike failed for UID {uid}\nReason: {response['error']}")
        return

    player_name = str(response.get("PlayerNickname") or order["player_name"] or "Unknown").strip()
    current_likes = int(response.get("LikesafterCommand") or order["current_likes"] or 0)
    last_likes_before = int(response.get("LikesbeforeCommand") or last_likes_before)
    fields = calc_autolike_fields(order, last_likes_before, current_likes)
    fields["player_name"] = player_name
    if fields["status"] == "completed":
        fields["completed_at"] = autolike_now().isoformat(timespec="seconds")
    update_autolike_order(uid, fields)
    updated = get_autolike_order(uid)
    send_autolike_update(updated)


def seconds_until_next_autolike_run():
    now = autolike_now()
    next_run = now.replace(hour=AUTO_LIKE_RUN_HOUR, minute=AUTO_LIKE_RUN_MINUTE, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(days=1)
    return max(1, (next_run - now).total_seconds())


def run_daily_autolikes_once():
    if not autolike_run_lock.acquire(blocking=False):
        logger.info("AutoLike daily run skipped because another run is active.")
        return
    try:
        orders = get_active_autolike_orders()
        logger.info(f"AutoLike daily run started. active_orders={len(orders)}")
        for index, order in enumerate(orders):
            if index:
                time.sleep(AUTO_LIKE_ORDER_DELAY_SECONDS)
            try:
                process_autolike_order(order)
            except Exception as e:
                logger.error(f"AutoLike order processing crashed for uid={order.get('uid')}: {e}", exc_info=True)
        logger.info("AutoLike daily run finished.")
    finally:
        autolike_run_lock.release()


def autolike_scheduler_loop():
    while True:
        try:
            sleep_seconds = seconds_until_next_autolike_run()
            logger.info(f"Next AutoLike run in {int(sleep_seconds)} seconds.")
            time.sleep(sleep_seconds)
            run_daily_autolikes_once()
        except Exception as e:
            logger.error(f"AutoLike scheduler error: {e}", exc_info=True)
            time.sleep(60)


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


def is_player_not_found_error(message):
    text = str(message or "").strip().lower()
    return any(
        marker in text
        for marker in (
            "player not found",
            "profile not found",
            "account not found",
            "br_account_not_found",
        )
    )


def like_token_value(token_item):
    if isinstance(token_item, dict):
        return str(token_item.get("token", "")).strip()
    return str(token_item or "").strip()


def token_item_region(token_item):
    claims = decode_jwt_payload(like_token_value(token_item))
    return str(claims.get("lock_region") or claims.get("noti_region") or "").strip().upper()


def available_local_token_regions():
    tokens = load_json_file(TOKEN_FILE, [])
    if not isinstance(tokens, list):
        return []
    regions = []
    for item in tokens:
        region = token_item_region(item)
        if region and region not in regions:
            regions.append(region)
    return regions


def player_not_found_text(uid, requested_region):
    return (
        "ᴘʟᴀʏᴇʀ ɴᴏᴛ ғᴏᴜɴᴅ\n\n"
        f"› ᴜɪᴅ : {uid}\n"
        f"› ʀᴇɢɪᴏɴ : {requested_region}\n\n"
        "Please check the UID and region, then try again."
    )


def wrong_region_text(uid, requested_region, correct_region):
    return (
        "ᴡʀᴏɴɢ ʀᴇɢɪᴏɴ\n\n"
        f"› ᴜɪᴅ : {uid}\n"
        f"› ʀᴇǫᴜᴇsᴛᴇᴅ : {requested_region}\n"
        f"› ᴄᴏʀʀᴇᴄᴛ : {correct_region}\n\n"
        f"Please use /like {str(correct_region).lower()} {uid}"
    )


def verify_uid_region_local(uid, requested_region):
    requested_region = str(requested_region or "").strip().upper()
    response = call_ffinfo_api(
        requested_region,
        uid,
        timeout_seconds=FFINFO_LOOKUP_TIMEOUT_SECONDS,
        local_only=True,
    )
    if "error" not in response:
        try:
            if not is_player_not_found_error(json.dumps(response, ensure_ascii=False)):
                return True, extract_ffinfo_region(response) or requested_region, ""
        except Exception:
            return True, extract_ffinfo_region(response) or requested_region, ""

    for region in available_local_token_regions():
        if region == requested_region:
            continue
        response = call_ffinfo_api(
            region,
            uid,
            timeout_seconds=FFINFO_LOOKUP_TIMEOUT_SECONDS,
            local_only=True,
        )
        if "error" in response:
            continue
        try:
            if is_player_not_found_error(json.dumps(response, ensure_ascii=False)):
                continue
        except Exception:
            pass
        return False, extract_ffinfo_region(response) or region, "wrong_region"

    return False, "", "not_found"


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


def compact_list(value, limit=4, default="N/A"):
    if value in (None, "", "N/A"):
        return default
    if not isinstance(value, list):
        return str(value)
    items = [str(item) for item in value if item not in (None, "", "N/A")]
    if not items:
        return default
    shown = items[:limit]
    suffix = f", +{len(items) - limit} more" if len(items) > limit else ""
    return ", ".join(shown) + suffix


def multiline_list(value, prefix="• ", limit=6, default="N/A"):
    if value in (None, "", "N/A"):
        return default
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",") if item.strip()]
    elif isinstance(value, list):
        items = [str(item).strip() for item in value if item not in (None, "", "N/A")]
    else:
        items = [str(value)]
    if not items:
        return default
    shown = items[:limit]
    lines = [f"{prefix}{item}" for item in shown]
    if len(items) > limit:
        lines.append(f"{prefix}+{len(items) - limit} more")
    return "\n".join(lines)


def compact_history(history, limit=5):
    if not isinstance(history, list) or not history:
        return "N/A"
    lines = []
    for item in history[:limit]:
        if not isinstance(item, dict):
            continue
        event_id = pick(item, "epEventId", "eventId", default="N/A")
        badge_count = pick(item, "badgeCnt", "badgeCount", default="N/A")
        owned = " | Pass" if item.get("ownedPass") else ""
        lines.append(f"S{event_id}: {format_number(badge_count)} badges{owned}")
    return "\n".join(lines) if lines else "N/A"


def is_missing_value(value):
    if value in (None, ""):
        return True
    text = str(value).strip()
    return text in {"", "N/A", "None", "0", "0/100"}


def info_line(label, value):
    if is_missing_value(value):
        return ""
    if not label:
        return f"{value}\n"
    if isinstance(value, str) and value.startswith("\n"):
        return f"{label}:\n{value.lstrip()}\n"
    return f"{label}: {value}\n"


def info_section(title, lines):
    body = "".join(line for line in lines if line)
    if not body.strip():
        return ""
    return f"{title}\n━━━━━━━━━━━━━━━━━━\n{body.rstrip()}"


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
    history = pick(data, "historyEpInfo", default=[])
    workshop_maps = pick(data, "workshop_maps", "workshopMaps", default=[])

    nickname = pick(basic, "nickname", "AccountName", "PlayerNickname")
    uid = pick(basic, "accountId", "AccountId", "AccountID", "UID")
    region = pick(basic, "region", "AccountRegion", default=payload.get("Region", "N/A"))
    level = pick(basic, "level", "AccountLevel")
    level_progress = pick(basic, "levelProgress", default="N/A")
    exp = pick(basic, "exp", "AccountEXP")
    exp_needed = pick(basic, "expNeeded", default="N/A")
    likes = pick(basic, "liked", "likes", "AccountLikes", "Likes")
    badges = pick(basic, "badgeCnt", "badgeCount", "AccountBPBadges", "badges")
    prime_level = pick(basic, "primeLevel.level", "primeInfo.primeLevel", default=0)
    title = pick(basic, "title", "Title", "AccountTitle", "TitleID")
    title_name = pick(basic, "titleName", default="N/A")
    banner_name = pick(basic, "bannerName", default="N/A")
    avatar_name = pick(basic, "headPicName", default="N/A")
    equipped_gun = pick(basic, "equippedGunName", default="N/A")
    equipped_animation = pick(basic, "equippedAnimationName", default="N/A")
    weapon_skins = pick(basic, "weaponSkinNames", default=[])
    release_version = pick(basic, "releaseVersion", default="N/A")
    ban_status = pick(basic, "banStatus", default="N/A")
    created = pick(basic, "createAt", "createdAt", "AccountCreateTime")
    last_login = pick(basic, "lastLoginAt", "lastLogin", "AccountLastLogin")
    br_rank = pick(basic, "rank", "BrRank", "maxRank", "BrMaxRank")
    br_points = pick(basic, "rankingPoints", "BrRankPoint")
    br_ranking_name = pick(basic, "rankingName", default="")
    cs_rank = pick(basic, "csRank", "CsRank", "csMaxRank", "CsMaxRank")
    cs_points = pick(basic, "csRankingPoints", "CsRankPoint", "csRankPoint")
    cs_ranking_name = pick(basic, "csRankingName", default="")
    cs_stars = cs_visible_stars(cs_rank, cs_points)
    clan_members = pick(clan, "memberNum", "GuildMember")
    clan_capacity = pick(clan, "capacity", "GuildCapacity")
    captain_uid = pick(clan, "captainId", "GuildOwner", "ownerId")
    captain_name = pick(captain, "nickname", "AccountName", default="N/A")
    captain_level = pick(captain, "level", "AccountLevel")
    captain_likes = pick(captain, "liked", "AccountLikes")
    captain_br_rank = pick(captain, "rank", "BrRank", "maxRank", "BrMaxRank")
    captain_br_points = pick(captain, "rankingPoints", "BrRankPoint")
    captain_br_ranking_name = pick(captain, "rankingName", default="")
    captain_cs_rank = pick(captain, "csRank", "CsRank", "csMaxRank", "CsMaxRank")
    captain_cs_points = pick(captain, "csRankingPoints", "CsRankPoint", "csRankPoint")
    captain_cs_ranking_name = pick(captain, "csRankingName", default="")
    captain_cs_stars = cs_visible_stars(captain_cs_rank, captain_cs_points)
    captain_last_login = pick(captain, "lastLoginAt", "AccountLastLogin")

    equipped_skills = pick(profile, "equipedSkills", "equippedSkills", "EquippedSkills", default=[])
    if isinstance(equipped_skills, list):
        equipped_skills = len(equipped_skills)
    clothes_names = pick(profile, "clothesNames", default=[])
    equipped_skills_names = pick(profile, "equippedSkillsNames", default="N/A")
    avatar_character = pick(profile, "avatarName", default="N/A")

    credit_score = pick(credit, "creditScore", "score")
    credit_valid = pick(credit, "periodicSummaryEndTime", "validUntil")
    credit_status = pick(credit, "rewardState", default="")
    if not credit_status:
        credit_status = "Good" if str(credit_score).isdigit() and int(credit_score) >= 90 else "N/A"

    requested_at = datetime.now().strftime("%d %b %Y %H:%M:%S")
    requester_name = str(requester.first_name or requester.username or requester.id)
    sections = [
        "🎮 FREE FIRE PROFILE\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        info_section("👤 BASIC INFORMATION", [
            info_line("🏷️ Nickname", nickname),
            info_line("🆔 UID", uid),
            info_line("🌐 Region", str(region).upper() if not is_missing_value(region) else ""),
            info_line("📈 Level", format_number(level)),
            info_line("📊 Level Progress", level_progress),
            info_line("✨ EXP", format_number(exp)),
            info_line("🎯 EXP Needed", format_number(exp_needed)),
            info_line("💙 Likes", format_number(likes)),
            info_line("🎖 Badges", format_number(badges)),
            info_line("💎 Prime Level", format_number(prime_level)),
            info_line("🏅 Title", title_name if not is_missing_value(title_name) else format_number(title)),
            info_line("🛡 Ban Status", ban_status),
            info_line("📦 Version", release_version),
            info_line("📆 Created", format_unix_date(created)),
            info_line("⏱ Last Login", format_unix_date(last_login)),
        ]),
        info_section("🏆 BATTLE ROYALE", [
            info_line("🥇 Rank", br_ranking_name or rank_name(br_rank, br_points, mode='br')),
            info_line("📊 Points", format_number(br_points)),
        ]),
        info_section("⚔️ CLASH SQUAD", [
            info_line("🥈 Rank", cs_ranking_name or rank_name(cs_rank, cs_points, mode='cs')),
            info_line("⭐️Total Stars", format_number(cs_points)),
        ]),
    ]

    has_clan = any(
        pick(clan, key, default=None) not in (None, "", "N/A")
        for key in ("clanId", "Guildid", "GuildID", "clanName", "GuildName")
    )
    if has_clan:
        sections.append(
            info_section("🏰 CLAN INFORMATION", [
                info_line("🏷️ Name", pick(clan, 'clanName', 'GuildName')),
                info_line("🆔 ID", format_number(pick(clan, 'clanId', 'Guildid', 'GuildID'))),
                info_line("📈 Level", format_number(pick(clan, 'clanLevel', 'GuildLevel'))),
                info_line("👥 Members", f"{format_number(clan_members)}/{format_number(clan_capacity)} ({format_percent(clan_members, clan_capacity)})"),
                info_line("👑 Captain", captain_name),
                info_line("🆔 Captain UID", format_number(captain_uid)),
            ])
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
            info_section("👑 CAPTAIN INFORMATION", [
                info_line("🏷️ Name", captain_name),
                info_line("🆔 UID", pick(captain, 'accountId', 'AccountId', default=captain_uid)),
                info_line("🌐 Region", str(pick(captain, 'region', 'AccountRegion', default='')).upper()),
                info_line("📈 Level", format_number(captain_level)),
                info_line("💙 Likes", format_number(captain_likes)),
                info_line("🏆 BR Rank", captain_br_ranking_name or rank_name(captain_br_rank, captain_br_points, mode='br')),
                info_line("📊 Points", format_number(captain_br_points)),
                info_line("⚔️ CS Rank", captain_cs_ranking_name or rank_name(captain_cs_rank, captain_cs_points, mode='cs')),
                info_line("⭐️Total Stars", format_number(captain_cs_points)),
                info_line("⏱ Last Login", format_unix_date(captain_last_login)),
            ])
        )

    sections.extend([
        info_section("🎨 PROFILE STYLE", [
            info_line("🧍 Character", avatar_character),
            info_line("🖼 Avatar", avatar_name),
            info_line("🏳 Banner", banner_name),
            info_line("🔫 Weapon", equipped_gun),
            info_line("🏇 Animation", equipped_animation),
            info_line("🧩 Skins", "\n" + multiline_list(weapon_skins, limit=4) if not is_missing_value(compact_list(weapon_skins, limit=4)) else ""),
            info_line("👕 Outfit", "\n" + multiline_list(clothes_names, limit=6) if not is_missing_value(compact_list(clothes_names, limit=6)) else ""),
            info_line("⚡ Skill Slots", format_number(equipped_skills)),
            info_line("🧠 Skills", "\n" + multiline_list(equipped_skills_names, limit=6) if not is_missing_value(equipped_skills_names) else ""),
        ]),
        info_section("💬 SOCIAL", [
            info_line("📝 Bio", pick(social, 'signature', 'AccountSignature', default='')),
            info_line("🚻 Gender", "Unknown" if is_missing_value(pick(social, 'gender', default='')) else clean_enum(pick(social, 'gender', default=''))),
            info_line("🌐 Language", clean_enum(pick(social, 'language', 'AccountLanguage', default=''))),
            info_line("🎮 Mode Prefer", clean_enum(pick(social, 'modePrefer', default=''))),
            info_line("🏆 Rank Show", clean_enum(pick(social, 'rankShow', default=''))),
            info_line("🔐 Privacy", clean_enum(pick(social, 'privacy', 'AccountPrivacy', default=''))),
        ]),
        info_section("🐾 PET INFORMATION", [
            info_line("🏷️ Name", pick(pet, 'petName', default='')),
            info_line("🆔 ID", format_number(pick(pet, 'id', 'PetId'))),
            info_line("📈 Level", format_number(pick(pet, 'level', 'PetLevel'))),
            info_line("✨ EXP", format_number(pick(pet, 'exp', 'PetEXP'))),
            info_line("", f"🎨 {pick(pet, 'skinName', default='') or format_number(pick(pet, 'skinId', 'SkinId'))}"),
            info_line("🧠 Skill", pick(pet, 'skillName', default='')),
        ]),
        info_section("🗺 CRAFTLAND", [
            info_line("🏷️ Name", pick(workshop_maps[0], 'Name', 'name', default='') if isinstance(workshop_maps, list) and workshop_maps else ''),
            info_line("🔑 Code", pick(workshop_maps[0], 'Code', 'code', default='') if isinstance(workshop_maps, list) and workshop_maps else ''),
        ]),
        info_section("✅ CREDIT SCORE", [
            info_line("💯 Score", f"{format_number(credit_score)}/100" if not is_missing_value(credit_score) else ""),
            info_line("📌 Status", credit_status),
            info_line("⏳ Valid Until", format_unix_date(credit_valid)),
        ]),
        (
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🙋 Requested by: {requester_name}\n"
            f"🕒 Time: {requested_at}"
        ),
    ])

    return "\n\n".join(section for section in sections if section and str(section).strip())


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


def get_guestgen_proxy(region=""):
    global guestgen_proxy_use_count
    if str(region or "").upper() in GUESTGEN_DIRECT_REGIONS:
        return ""
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
    if MajorLoginReq_pb2 is not None:
        try:
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
                origin_platform_type=platform,
                primary_platform_type=platform,
            )
            return aes_encrypt_payload(message.SerializeToString())
        except Exception as e:
            logger.info(f"MajorLoginReq protobuf encode failed, using fallback encoder: {e}")

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
    proxy_url = get_guestgen_proxy(region)
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


def remove_uidpass_entry(uid):
    records = load_json_file(UIDPASS_FILE, [])
    if not isinstance(records, list):
        records = []
    kept_records = [item for item in records if str(item.get("uid", "")).strip() != uid]
    removed_uidpass = len(kept_records) != len(records)
    if removed_uidpass:
        save_json_file(UIDPASS_FILE, kept_records)

    tokens = load_json_file(TOKEN_FILE, [])
    if not isinstance(tokens, list):
        tokens = []
    kept_tokens = [
        item for item in tokens
        if not (isinstance(item, dict) and str(item.get("uid", "")).strip() == uid)
    ]
    removed_token = len(kept_tokens) != len(tokens)
    if removed_token:
        save_json_file(TOKEN_FILE, kept_tokens)

    return removed_uidpass, removed_token, len(kept_records), len(kept_tokens)


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


def decode_jwt_payload(token):
    try:
        import base64

        payload = str(token or "").split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload).decode("utf-8"))
    except Exception:
        return {}


def nested_get(data, path, default=""):
    value = data
    for key in path.split("."):
        if isinstance(value, dict):
            value = value.get(key)
        else:
            return default
    return value if value not in (None, "") else default


def extract_jwt_check_tokens(data):
    major_login = data.get("MajorLogin") if isinstance(data.get("MajorLogin"), dict) else {}
    access_token = (
        data.get("access_token")
        or nested_get(data, "Guest_Auth.data.access_token")
        or ""
    )
    jwt_token = data.get("token") or major_login.get("token") or ""
    return str(access_token or ""), str(jwt_token or "")


def build_jwt_copy_markup(data):
    access_token, jwt_token = extract_jwt_check_tokens(data)
    markup = InlineKeyboardMarkup()
    if access_token:
        markup.add(InlineKeyboardButton("Copy Access Token", copy_text=CopyTextButton(access_token)))
    if jwt_token:
        markup.add(InlineKeyboardButton("Copy JWT Token", copy_text=CopyTextButton(jwt_token)))
    return markup if markup.keyboard else None


def format_jwt_check_result(uid, data, attempts):
    major_login = data.get("MajorLogin") if isinstance(data.get("MajorLogin"), dict) else {}
    access_token, token = extract_jwt_check_tokens(data)
    access_token = access_token or "N/A"
    account_id = (
        data.get("account_id")
        or major_login.get("account_id")
        or uid
        or "N/A"
    )
    token = token or "N/A"
    claims = decode_jwt_payload(token)
    name = (
        data.get("name")
        or data.get("nickname")
        or major_login.get("nickname")
        or data.get("player_name")
        or ""
    )
    region = (
        major_login.get("lock_region")
        or major_login.get("noti_region")
        or data.get("lock_region")
        or data.get("noti_region")
        or claims.get("lock_region")
        or claims.get("noti_region")
        or "N/A"
    )
    safe_account_id = html.escape(str(account_id))
    safe_name = html.escape(str(name))
    safe_region = html.escape(str(region))
    safe_access_token = html.escape(str(access_token))
    safe_token = html.escape(str(token))

    return (
        "✅ JWT CHECK SUCCESS\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"👤 Account ID: <code>{safe_account_id}</code>\n"
        + (f"🏷 Name: {safe_name}\n" if name else "")
        +
        f"🌐 Region: {safe_region}\n"
        "\n"
        "🔑 Access Token\n"
        f"<code>{safe_access_token}</code>\n\n"
        "🎟 JWT Token\n"
        f"<code>{safe_token}</code>"
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


@bot.message_handler(commands=["id", "chatid"])
def handle_id_lookup(message):
    user = message.from_user
    chat = message.chat
    username = f"@{user.username}" if getattr(user, "username", None) else "N/A"
    chat_label = "Group ID" if chat.type in {"group", "supergroup"} else "Chat ID"

    bot.reply_to(
        message,
        "Telegram IDs\n\n"
        f"User ID: {user.id}\n"
        f"Username: {username}\n"
        f"{chat_label}: {chat.id}\n"
        f"Chat Type: {chat.type}",
    )


@bot.message_handler(content_types=["new_chat_members", "left_chat_member"])
def auto_delete_group_service_messages(message):
    if message.chat.type not in {"group", "supergroup"}:
        return
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except Exception as e:
        logger.info(f"Could not delete group service message in chat {message.chat.id}: {e}")


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

    processing_msg = bot.reply_to(message, "Please wait... Checking region.")
    requested_region = region
    resolved_region = ""

    detected_region, region_error = resolve_uid_region(uid)
    if region_error:
        logger.info(f"Could not resolve region for UID {uid}: {region_error}")
        bot.edit_message_text(
            text=player_not_found_text(uid, requested_region),
            chat_id=processing_msg.chat.id,
            message_id=processing_msg.message_id,
        )
        return

    region = detected_region or requested_region
    resolved_region = region

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

        if "error" in response:
            profile_response = call_ffinfo_api(
                region,
                uid,
                timeout_seconds=FFINFO_LOOKUP_TIMEOUT_SECONDS,
                local_only=True,
            )
            profile_missing = "error" in profile_response
            if not profile_missing:
                try:
                    profile_missing = is_player_not_found_error(json.dumps(profile_response, ensure_ascii=False))
                except Exception:
                    profile_missing = False
            if profile_missing:
                logger.info(f"Local FF info check failed after like API error for uid={uid} region={region}: {profile_response.get('error')}")
                bot.edit_message_text(
                    text=(
                        "ᴘʟᴀʏᴇʀ ɴᴏᴛ ғᴏᴜɴᴅ\n\n"
                        f"› ᴜɪᴅ : {uid}\n"
                        f"› ʀᴇɢɪᴏɴ : {requested_region}\n\n"
                        "Please check the UID and region, then try again."
                    ),
                    chat_id=processing_msg.chat.id,
                    message_id=processing_msg.message_id,
                )
                return

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
            title = "✓ ʀᴇǫᴜᴇsᴛ ᴘʀᴏᴄᴇssᴇᴅ sᴜᴄᴄᴇssғᴜʟʟʏ"
        else:
            title = "ᴜɪᴅ ᴀʟʀᴇᴀᴅʏ ʀᴇᴀᴄʜᴇᴅ ᴍᴀx ʟɪᴋᴇs ғᴏʀ ɴᴏᴡ."

        remaining_requests = max_limit - usage["used"]
        if max_limit > 1000:
            remaining_requests = "∞"

        response_text = (
            f"{title}\n\n"
            f"› ɴᴀᴍᴇ : {player_name}\n"
            f"› ᴜɪᴅ : {player_uid}\n"
            f"› ʀᴇɢɪᴏɴ : {region_name}\n"
            f"› ʟɪᴋᴇs ʙᴇғᴏʀᴇ : {likes_before}\n"
            f"› ʟɪᴋᴇs ᴀᴅᴅᴇᴅ : {likes_given}\n"
            f"› ᴛᴏᴛᴀʟ ɴᴏᴡ : {likes_after}\n"
            f"› ʀᴇǫᴜᴇsᴛs : {remaining_requests}\n"
            f"\n↳ ᴏᴡɴᴇʀ : {OWNER_USERNAME}"
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


@bot.message_handler(commands=["autolike"])
def handle_autolike_add(message):
    if not is_admin_user(message.from_user.id):
        return

    args = message.text.split()
    if len(args) != 6:
        bot.reply_to(message, "Use: /autolike <server> <uid> <total_likes> <username> <telegram_user_id>")
        return

    server = normalize_autolike_server(args[1])
    uid = args[2].strip()
    username = args[4].strip()
    telegram_user_id = args[5].strip()

    if server not in AUTO_LIKE_VALID_SERVERS and server != "EUROPE":
        bot.reply_to(message, f"Invalid server. Supported: {', '.join(sorted(AUTO_LIKE_VALID_SERVERS))}")
        return
    if not uid.isdigit():
        bot.reply_to(message, "Invalid UID. UID must be numeric.")
        return
    try:
        total_likes = int(args[3])
        telegram_user_id_int = int(telegram_user_id)
    except ValueError:
        bot.reply_to(message, "Invalid format. total_likes and telegram_user_id must be numbers.")
        return
    if total_likes <= 0:
        bot.reply_to(message, "total_likes must be greater than 0.")
        return
    if not username.startswith("@"):
        bot.reply_to(message, "username must start with @.")
        return
    if get_autolike_order(uid, active_only=True):
        bot.reply_to(message, f"Duplicate active order: UID {uid} already has an active AutoLike order.")
        return

    processing_msg = bot.reply_to(message, "Adding AutoLike order... Fetching player profile.")
    player_name, current_likes, error = fetch_autolike_player_snapshot(server, uid)
    if error:
        bot.edit_message_text(
            text=f"Could not add AutoLike order for UID {uid}.\nReason: {error}",
            chat_id=processing_msg.chat.id,
            message_id=processing_msg.message_id,
        )
        return

    now = autolike_now()
    purchase_date = now.strftime("%Y-%m-%d")
    with autolike_db_lock:
        with autolike_db() as conn:
            conn.execute(
                """
                INSERT INTO autolike_orders (
                    server, uid, player_name, username, telegram_user_id,
                    likes_before_purchase, total_likes, purchase_date,
                    last_likes_before, likes_added_today, current_likes,
                    total_delivered, remaining_likes, progress_percent,
                    estimated_days, status, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, ?, 0, 0, 'active', ?, ?)
                """,
                (
                    server,
                    uid,
                    player_name,
                    username,
                    telegram_user_id_int,
                    current_likes,
                    total_likes,
                    purchase_date,
                    current_likes,
                    current_likes,
                    total_likes,
                    now.isoformat(timespec="seconds"),
                    now.isoformat(timespec="seconds"),
                ),
            )

    bot.edit_message_text(
        text=(
            "✓ ᴀᴜᴛᴏʟɪᴋᴇ ᴏʀᴅᴇʀ ᴀᴅᴅᴇᴅ\n\n"
            f"› ᴜɪᴅ : {uid}\n"
            f"› ᴘʟᴀʏᴇʀ : {player_name}\n"
            f"› sᴇʀᴠᴇʀ : {server}\n"
            f"› ʟɪᴋᴇs ʙᴇғᴏʀᴇ ᴘᴜʀᴄʜᴀsᴇ : {current_likes}\n"
            f"› ᴘᴜʀᴄʜᴀsᴇᴅ ʟɪᴋᴇs : {total_likes}\n"
            f"› ᴜsᴇʀ : {username} ({telegram_user_id_int})\n\n"
            f"↳ ᴏᴡɴᴇʀ : {OWNER_USERNAME}"
        ),
        chat_id=processing_msg.chat.id,
        message_id=processing_msg.message_id,
    )
    order = get_autolike_order(uid, active_only=True)
    if order:
        threading.Thread(target=process_autolike_order, args=(order,), daemon=True).start()


@bot.message_handler(commands=["remove"])
def handle_autolike_remove(message):
    if not is_admin_user(message.from_user.id):
        return
    args = message.text.split()
    if len(args) != 2 or not args[1].isdigit():
        bot.reply_to(message, "Use: /remove <uid>")
        return
    uid = args[1].strip()
    order = get_autolike_order(uid, active_only=True)
    if not order:
        bot.reply_to(message, f"No active AutoLike order found for UID {uid}.")
        return
    update_autolike_order(uid, {"status": "removed"})
    bot.reply_to(message, f"Removed AutoLike order for UID {uid}.")


@bot.message_handler(commands=["extend"])
def handle_autolike_extend(message):
    if not is_admin_user(message.from_user.id):
        return
    args = message.text.split()
    if len(args) != 3 or not args[1].isdigit():
        bot.reply_to(message, "Use: /extend <uid> <extra_likes>")
        return
    uid = args[1].strip()
    try:
        extra_likes = int(args[2])
    except ValueError:
        bot.reply_to(message, "extra_likes must be a number.")
        return
    if extra_likes <= 0:
        bot.reply_to(message, "extra_likes must be greater than 0.")
        return
    order = get_autolike_order(uid, active_only=True)
    if not order:
        bot.reply_to(message, f"No active AutoLike order found for UID {uid}.")
        return
    new_total = int(order["total_likes"]) + extra_likes
    fields = calc_autolike_fields({**order, "total_likes": new_total}, order["last_likes_before"], order["current_likes"])
    fields["total_likes"] = new_total
    update_autolike_order(uid, fields)
    updated = get_autolike_order(uid, active_only=True)
    bot.reply_to(message, f"Extended UID {uid} by {extra_likes} likes. New total: {updated['total_likes']}.")


@bot.message_handler(commands=["status"])
def handle_autolike_status(message):
    if not is_admin_user(message.from_user.id):
        return
    args = message.text.split()
    if len(args) != 2 or not args[1].isdigit():
        bot.reply_to(message, "Use: /status <uid>")
        return
    order = get_autolike_order(args[1].strip())
    if not order:
        bot.reply_to(message, f"No AutoLike order found for UID {args[1]}.")
        return
    bot.reply_to(message, autolike_details_line(order, 1))


@bot.message_handler(commands=["myautolikes"])
def handle_myautolikes(message):
    username = f"@{message.from_user.username}" if message.from_user.username else str(message.from_user.id)
    orders = get_user_autolike_orders(message.from_user.id)
    if not orders:
        bot.reply_to(message, f"Hello {username}\nYou have no AutoLike orders.")
        return
    parts = [f"Hello {username}\nYour AutoLike Details:"]
    for index, order in enumerate(orders, start=1):
        parts.append(autolike_details_line(order, index))
    bot.reply_to(message, "\n\n".join(parts))


@bot.message_handler(commands=["list"])
def handle_autolike_list(message):
    if not is_admin_user(message.from_user.id):
        return
    orders = get_active_autolike_orders()
    if not orders:
        bot.reply_to(message, "No active AutoLike orders.")
        return
    lines = ["Active AutoLike orders:"]
    for order in orders:
        lines.append(
            f"UID {order['uid']} | {order['server']} | {order['username']} | "
            f"{order['total_delivered']}/{order['total_likes']} ({order['progress_percent']}%) | "
            f"remaining {order['remaining_likes']}"
        )
    bot.reply_to(message, "\n".join(lines))


@bot.message_handler(commands=["setautogroup"])
def handle_set_autolike_group(message):
    if message.from_user.id != OWNER_ID:
        return

    args = message.text.split()
    if len(args) == 2:
        group_id_text = args[1].strip()
    elif message.chat.type in {"group", "supergroup"}:
        group_id_text = str(message.chat.id)
    else:
        bot.reply_to(message, "Use this in the group, or use: /setautogroup <group_id>")
        return

    if not group_id_text.lstrip("-").isdigit():
        bot.reply_to(message, "Invalid group ID.")
        return

    set_autolike_setting("group_id", group_id_text)
    bot.reply_to(
        message,
        "✓ ᴀᴜᴛᴏʟɪᴋᴇ ɢʀᴏᴜᴘ sᴇᴛ\n\n"
        f"› ɢʀᴏᴜᴘ ɪᴅ : {group_id_text}\n\n"
        f"↳ ᴏᴡɴᴇʀ : {OWNER_USERNAME}",
    )


@bot.message_handler(commands=["removeautogroup"])
def handle_remove_autolike_group(message):
    if message.from_user.id != OWNER_ID:
        return
    delete_autolike_setting("group_id")
    bot.reply_to(
        message,
        "✓ ᴀᴜᴛᴏʟɪᴋᴇ ɢʀᴏᴜᴘ ʀᴇᴍᴏᴠᴇᴅ\n\n"
        "› ᴅᴀɪʟʏ ɢʀᴏᴜᴘ ᴜᴘᴅᴀᴛᴇs : ᴏғғ",
    )


@bot.message_handler(commands=["autogroup"])
def handle_autolike_group_status(message):
    if message.from_user.id != OWNER_ID:
        return
    group_id = get_autolike_group_id()
    bot.reply_to(
        message,
        "ᴀᴜᴛᴏʟɪᴋᴇ ɢʀᴏᴜᴘ\n\n"
        f"› ɢʀᴏᴜᴘ ɪᴅ : {group_id or 'ɴᴏᴛ sᴇᴛ'}",
    )


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
        parse_mode = "HTML"
    else:
        text = (
            "❌ JWT CHECK FAILED\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"🆔 UID: {uid}\n"
            f"🔁 Attempts: {attempts}/{JWT_RETRY_ATTEMPTS}\n"
            f"⏱ Delay: {JWT_RETRY_DELAY_SECONDS}s\n\n"
            f"Reason: {error}"
        )
        parse_mode = None

    try:
        bot.edit_message_text(
            text=text,
            chat_id=processing_msg.chat.id,
            message_id=processing_msg.message_id,
            parse_mode=parse_mode,
        )
    except Exception:
        bot.reply_to(message, text, parse_mode=parse_mode)


@bot.message_handler(commands=["remain", "uidpass", "adduidpass", "updateuidpass", "removeuidpass", "addremain", "checkjwt"])
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

    if cmd == "/removeuidpass":
        if len(args) != 2:
            bot.reply_to(message, "Use: /removeuidpass <uid>")
            return

        uid = args[1].strip()
        if not uid.isdigit():
            bot.reply_to(message, "UID must be numeric.")
            return

        removed_uidpass, removed_token, uidpass_total, token_total = remove_uidpass_entry(uid)
        if not removed_uidpass and not removed_token:
            bot.reply_to(message, f"NOT FOUND: UID {uid} does not exist in uidpass.json or tokens.json.")
            return

        bot.reply_to(
            message,
            f"OK: UID {uid} removed. UID/PASS total: {uidpass_total} | Tokens now: {token_total}",
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
            "/myautolikes - Show your AutoLike orders\n"
            "/ffinfo [region] <uid> - Show Free Fire player info\n"
            "/bio <token/link> <text> - Update Free Fire bio\n"
            "/start - Start or verify\n"
            "/help - Show this help menu\n\n"
            "Owner Commands:\n"
            "/autolike <server> <uid> <total_likes> <username> <telegram_user_id> - Add AutoLike order\n"
            "/remove <uid> - Remove AutoLike order\n"
            "/extend <uid> <extra_likes> - Extend AutoLike order\n"
            "/status <uid> - Show AutoLike progress\n"
            "/list - Show active AutoLike orders\n"
            "/setautogroup [group_id] - Set AutoLike update group\n"
            "/removeautogroup - Remove AutoLike update group\n"
            "/autogroup - Show AutoLike update group\n"
            "/id or /chatid - Show your user ID and current chat/group ID\n"
            "/remain - Show all users usage\n"
            "/uidpass - Show total UID/PASS records\n"
            "/checkjwt <uid> <password> - Check JWT API with retry\n"
            "/adduidpass <uid> <password> - Add UID/PASS and fetch only its token\n\n"
            "/updateuidpass <uid> <new_password> - Update password after token check\n"
            "/removeuidpass <uid> - Remove UID/PASS and token\n"
            "/guestgen [region] [name] - Generate guest UID/PASS with auto-numbered name\n\n"
            f"Support: {OWNER_USERNAME}"
        )
        bot.reply_to(message, help_text)
        return

    # Allow help command even for users not in channel (but show join message)
    help_text = (
        "Bot Commands:\n\n"
        "/like <region> <uid> - Send likes to Free Fire UID\n"
        "/myautolikes - Show your AutoLike orders\n"
        "/ffinfo [region] <uid> - Show Free Fire player info\n"
        "/bio <token/link> <text> - Update Free Fire bio\n"
        "/id or /chatid - Show your user ID and current chat/group ID\n"
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
        "/id",
        "/chatid",
        "/like",
        "/autolike",
        "/remove",
        "/extend",
        "/status",
        "/myautolikes",
        "/list",
        "/setautogroup",
        "/removeautogroup",
        "/autogroup",
        "/ffinfo",
        "/bio",
        "/help",
        "/remain",
        "/uidpass",
        "/checkjwt",
        "/adduidpass",
        "/updateuidpass",
        "/removeuidpass",
        "/addremain",
        "/guestgen",
    }
    command = message.text.split()[0].split("@")[0].lower()
    if command not in known_commands:
        bot.reply_to(message, "Unknown command. Use /help to see available commands.")


if __name__ == "__main__":
    init_autolike_db()
    threading.Thread(target=autolike_scheduler_loop, daemon=True).start()
    mode = os.getenv("BOT_MODE", "polling").strip().lower()
    if mode == "webhook":
        port = int(os.getenv("PORT", "5000"))
        logger.info(f"Starting webhook mode on port {port}")
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
    else:
        logger.info("Starting polling mode")
        bot.remove_webhook()
        bot.infinity_polling(skip_pending=True, timeout=30, long_polling_timeout=30)
