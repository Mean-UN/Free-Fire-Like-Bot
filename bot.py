import json
import logging
import os
import random
import sys
import threading
import time
from datetime import datetime, timedelta
from urllib.parse import parse_qs, quote, urlparse
import hashlib
import hmac

import requests
import telebot
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from flask import Flask, jsonify, request
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup


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
GUESTGEN_REGIONS = {"IND", "SG", "RU", "ID", "TW", "US", "VN", "TH", "ME", "PK", "CIS", "BR", "BD"}
GUESTGEN_USAGE = "ℹ️ Usage: /guestgen <region> <name>\nExample: /guestgen SG ᴍᴇᴀɴXᴜɴ!"

bot = telebot.TeleBot(BOT_TOKEN)
app = Flask(__name__)

# In-memory daily usage tracker
like_tracker = {}


def reset_limits():
    """Reset in-memory usage counters daily at 00:00 UTC."""
    while True:
        try:
            now_utc = datetime.utcnow()
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


def call_bio_api(access_token, bio_text):
    try:
        url = f"{BIO_API_BASE_URL}?access_token={access_token}&bio={quote(bio_text, safe='')}"
        if BIO_API_KEY:
            url += f"&key={quote(BIO_API_KEY, safe='')}"
        response = requests.get(url, timeout=20)
        try:
            return response.json()
        except ValueError:
            return {"status": "error", "message": "Invalid response from bio service."}
    except requests.exceptions.Timeout:
        return {"status": "error", "message": "Bio service timed out. Please try again."}
    except requests.exceptions.ConnectionError:
        return {"status": "error", "message": "Could not connect to bio service."}
    except Exception as e:
        logger.error(f"Bio API failed: {e}", exc_info=True)
        return {"status": "error", "message": "Bio service failed. Please try again later."}


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

    suffix = "".join(superscript_digits[int(digit)] for digit in str(random.randint(10, 99)))
    return f"{base_name[: max(1, 12 - len(suffix))]}{suffix}"


def register_guest_account(region, base_name=""):
    secret = b"2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3"
    client_id = "100067"
    user_agent = "GarenaMSDK/4.0.19P9(SM-S908E; Android 11; en; IN)"
    session = requests.Session()

    def encode_open_id(value):
        key = [0, 0, 0, 2, 0, 1, 7, 0, 0, 0, 0, 0, 2, 0, 1, 7, 0, 0, 0, 0, 0, 2, 0, 1, 7, 0, 0, 0, 0, 0, 2, 0]
        return bytes(byte ^ key[index % len(key)] ^ 48 for index, byte in enumerate(value.encode()))

    def aes_encrypt_hex(hex_value):
        cipher = AES.new(b"Yg&tc%DEuh6%Zc^8", AES.MODE_CBC, b"6oyZDr22E3ychjM%")
        return cipher.encrypt(pad(bytes.fromhex(hex_value), 16)).hex()

    def encode_varint(number):
        result = bytearray()
        while number:
            byte = number & 0x7F
            number >>= 7
            result.append(byte | (0x80 if number else 0))
        return bytes(result)

    def encode_field(field, value):
        if isinstance(value, int):
            return encode_varint((field << 3) | 0) + encode_varint(value)
        data = value.encode() if isinstance(value, str) else value
        return encode_varint((field << 3) | 2) + encode_varint(len(data)) + data

    def encode_payload(payload):
        packet = bytearray()
        for field in sorted(payload):
            packet.extend(encode_field(field, payload[field]))
        return packet

    try:
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
            "Content-Type": "application/json",
        }

        response = session.post(
            "https://100067.connect.garena.com/api/v2/oauth/guest:register",
            data=body,
            headers=headers,
            timeout=15,
        )
        if response.status_code != 200:
            return None, None, None, f"Guest register failed: HTTP {response.status_code} - {response.text[:120]}"

        register_data = response.json()
        uid = (register_data.get("data") or {}).get("uid") or register_data.get("uid")
        if not uid:
            return None, None, None, "Guest register did not return UID."

        token_body = {
            "uid": str(uid),
            "password": password_hash,
            "response_type": "token",
            "client_type": "2",
            "client_secret": secret.decode(),
            "client_id": client_id,
        }
        response = session.post(
            "https://100067.connect.garena.com/oauth/guest/token/grant",
            data=token_body,
            headers={"Host": "100067.connect.garena.com", "User-Agent": user_agent},
            timeout=15,
        )
        if response.status_code != 200:
            return None, None, None, f"Token grant failed: HTTP {response.status_code} - {response.text[:120]}"

        token_data = response.json()
        access_token = token_data.get("access_token")
        open_id = token_data.get("open_id") or token_data.get("openId") or token_data.get("openid")
        if not access_token or not open_id:
            return None, None, None, "Token grant did not return access token/open_id."

        guest_name = build_guest_name(base_name)
        payload = {
            1: guest_name,
            2: access_token,
            3: open_id,
            5: 102000007,
            6: 4,
            7: 1,
            13: 1,
            14: encode_open_id(open_id),
            15: region,
            16: 1,
        }
        encrypted_data = bytes.fromhex(aes_encrypt_hex(encode_payload(payload).hex()))
        major_headers = {
            "Authorization": f"Bearer {access_token}",
            "X-Unity-Version": "2018.4.11f1",
            "X-GA": "v1 1",
            "ReleaseVersion": "OB53",
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(encrypted_data)),
            "User-Agent": user_agent,
            "Host": "loginbp.ggblueshark.com",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip",
        }
        response = None
        for attempt in range(3):
            response = session.post(
                "https://loginbp.ggblueshark.com/MajorRegister",
                data=encrypted_data,
                headers=major_headers,
                timeout=15,
            )
            if response.status_code != 503:
                break
            time.sleep(2 * (attempt + 1))

        if response.status_code == 200:
            return str(uid), password_hash, guest_name, None
        return str(uid), password_hash, guest_name, f"Major register failed: HTTP {response.status_code} - {response.text[:120]}"
    except requests.exceptions.Timeout:
        return None, None, None, "Guest generation timed out."
    except requests.exceptions.RequestException as e:
        return None, None, None, f"Guest generation request failed: {e}"
    except Exception as e:
        logger.error(f"Guest generation failed: {e}", exc_info=True)
        return None, None, None, "Guest generation failed. Check bot logs."
    finally:
        session.close()


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
        f"{token}\n\n"
        "Short Preview\n"
        f"Access: {short_secret(access_token)}\n"
        f"JWT: {short_secret(token)}"
    )


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
        like_tracker[user_id] = {"used": 0, "last_used": datetime.utcnow() - timedelta(days=1)}

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
    now_utc = datetime.utcnow()
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

    if not is_user_in_channel(user_id):
        bot.reply_to(message, "You must join all our channels to use this command.", reply_markup=build_join_markup())
        return

    parts = message.text.strip().split(None, 2)
    if len(parts) < 3:
        bot.reply_to(
            message,
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
        bot.reply_to(
            message,
            "Invalid access token format.\n\n"
            "Send a plain lowercase token, a Kiosgamer link, or a Garena Help access_token link.",
        )
        return

    if not bio_text:
        bot.reply_to(message, "Bio text cannot be empty.")
        return

    processing_msg = bot.reply_to(message, "Please wait... Updating bio.")
    result = call_bio_api(token, bio_text)

    if str(result.get("status", "")).lower() == "success":
        nickname = result.get("nickname", "Unknown")
        uid = result.get("uid", "N/A")
        platform = result.get("platform", "N/A")
        region = result.get("region", "N/A")
        new_bio = result.get("bio", bio_text)
        text = (
            "✅ Bio updated successfully\n\n"
            f"👤 Player: {nickname}\n"
            f"🆔 UID: {uid}\n"
            f"📱 Platform: {platform}\n"
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
        bot.reply_to(message, text)


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
        bot.reply_to(message, f"{GUESTGEN_USAGE}\n\nInvalid region. Use one of: {', '.join(sorted(GUESTGEN_REGIONS))}")
        return

    base_name = " ".join(args[2:]).strip()
    threading.Thread(target=process_guestgen, args=(message, region, base_name), daemon=True).start()


def process_guestgen(message, region, base_name=""):
    processing_msg = bot.reply_to(message, f"Please wait... Generating guest account for {region}.")
    uid, password, guest_name, error = register_guest_account(region, base_name)

    if uid and password and error:
        text = (
            "Guest account generated\n\n"
            f"Region: {region}\n"
            f"Name: {guest_name}\n"
            f"UID: {uid}\n"
            f"Password: {password}\n\n"
            f"Warning: {error}"
        )
    elif error:
        text = f"Guest generation failed\n\nRegion: {region}\nReason: {error}"
    else:
        text = (
            "Guest account generated\n\n"
            f"Region: {region}\n"
            f"Name: {guest_name}\n"
            f"UID: {uid}\n"
            f"Password: {password}"
        )

    try:
        bot.edit_message_text(text=text, chat_id=processing_msg.chat.id, message_id=processing_msg.message_id)
    except Exception:
        bot.reply_to(message, text)


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
            like_tracker[user_id] = {"used": 0, "last_used": datetime.utcnow() - timedelta(days=1)}
        
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
