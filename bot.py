import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timedelta

import requests
import telebot
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
            return {"error": f"API returned {response.status_code}: {response.text[:180]}"}
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


def call_ffinfo_api(region, uid):
    url = f"{API_BASE_URL}/ffinfo"
    params = {"uid": uid}
    if region:
        params["server_name"] = region
    try:
        logger.info(f"Calling FF info API: url={url} region={region or 'AUTO'} uid={uid}")
        response = requests.get(url, params=params, timeout=API_TIMEOUT_SECONDS)
        if response.status_code != 200:
            return {"error": f"API returned {response.status_code}: {response.text[:180]}"}
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


def rank_name(value):
    try:
        rank = int(value)
    except (TypeError, ValueError):
        return str(value or "N/A")

    if rank >= 322:
        return "Elite Heroic"
    if rank >= 312:
        return "Heroic"
    if rank >= 301:
        return "Master"
    if rank >= 215:
        return "Diamond"
    if rank >= 211:
        return "Platinum"
    if rank >= 201:
        return "Gold"
    if rank >= 101:
        return "Bronze"
    return str(rank)


def build_ffinfo_text(payload, requester):
    data = payload.get("data", payload)
    if isinstance(data, dict) and "result" in data and isinstance(data["result"], dict):
        data = data["result"]

    basic = pick(data, "basicInfo", "AccountInfo", default={})
    clan = pick(data, "clanBasicInfo", "GuildInfo", default={})
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
    title = pick(basic, "title", "Title", "AccountTitle", "TitleID")
    created = pick(basic, "createAt", "createdAt", "AccountCreateTime")
    last_login = pick(basic, "lastLoginAt", "lastLogin", "AccountLastLogin")
    br_rank = pick(basic, "rank", "BrRank", "maxRank", "BrMaxRank")
    br_points = pick(basic, "rankingPoints", "BrRankPoint")
    cs_rank = pick(basic, "csRank", "CsRank", "csMaxRank", "CsMaxRank")
    cs_points = pick(basic, "csRankingPoints", "CsRankPoint", "csRankPoint")

    equipped_skills = pick(profile, "equipedSkills", "equippedSkills", "EquippedSkills", default=[])
    if isinstance(equipped_skills, list):
        equipped_skills = len(equipped_skills)

    credit_score = pick(credit, "creditScore", "score")
    credit_valid = pick(credit, "periodicSummaryEndTime", "validUntil")
    credit_status = "Good" if str(credit_score).isdigit() and int(credit_score) >= 90 else "N/A"

    requested_at = datetime.now().strftime("%d %b %Y %H:%M:%S")
    requester_name = str(requester.first_name or requester.username or requester.id)

    return (
        "🎮 PLAYER INFORMATION 🎮\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📋 BASIC INFORMATION\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"👤 Nickname: {nickname}\n"
        f"🆔 UID: {uid}\n"
        f"🌍 Region: {str(region).upper()}\n"
        f"⭐ Level: {format_number(level)}\n"
        f"📊 EXP: {format_number(exp)}\n"
        f"❤️ Likes: {format_number(likes)}\n"
        f"🎖️ Badges: {format_number(badges)}\n"
        f"🏆 Title ID: {format_number(title)}\n"
        f"📅 Created: {format_unix_date(created)}\n"
        f"🕒 Last Login: {format_unix_date(last_login)}\n\n"
        "🏆 BATTLE ROYALE RANK\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Current Rank: {rank_name(br_rank)}\n"
        f"📈 Ranking Points: {format_number(br_points)}\n\n"
        "🔫 CLASH SQUAD RANK\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"⚡ Current Rank: {rank_name(cs_rank)}\n"
        f"📊 CS Points: {format_number(cs_points)}\n\n"
        "👥 CLAN INFORMATION\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🏰 Clan Name: {pick(clan, 'clanName', 'GuildName')}\n"
        f"🔢 Clan ID: {format_number(pick(clan, 'clanId', 'Guildid', 'GuildID'))}\n"
        f"⭐ Clan Level: {format_number(pick(clan, 'clanLevel', 'GuildLevel'))}\n"
        f"👤 Members: {format_number(pick(clan, 'memberNum', 'GuildMember'))}/{format_number(pick(clan, 'capacity', 'GuildCapacity'))}\n"
        f"👑 Captain UID: {format_number(pick(clan, 'captainId', 'GuildOwner', 'ownerId'))}\n\n"
        "🎭 PROFILE SETTINGS\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🖼️ Avatar ID: {format_number(pick(profile, 'avatarId', 'AccountAvatarId'))}\n"
        f"⚡ Equipped Skills: {format_number(equipped_skills)}\n\n"
        "💬 SOCIAL INFORMATION\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📝 Bio: {pick(social, 'signature', 'AccountSignature', default='No bio')}\n"
        f"🌐 Language: {clean_enum(pick(social, 'language', 'AccountLanguage'))}\n"
        f"🔒 Privacy: {clean_enum(pick(social, 'privacy', 'AccountPrivacy'))}\n\n"
        "🐾 PET INFORMATION\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Pet ID: {format_number(pick(pet, 'id', 'PetId'))}\n"
        f"⭐ Pet Level: {format_number(pick(pet, 'level', 'PetLevel'))}\n"
        f"📈 Pet EXP: {format_number(pick(pet, 'exp', 'PetEXP'))}\n"
        f"🎭 Skin ID: {format_number(pick(pet, 'skinId', 'SkinId'))}\n\n"
        "💳 CREDIT SCORE\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"⭐ Score: {format_number(credit_score)}/100\n"
        f"📊 Status: {credit_status}\n"
        f"⏰ Valid Until: {format_unix_date(credit_valid)}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Requested by: {requester_name}\n"
        f"🕒 {requested_at}"
    )


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
        "🎮 PLAYER INFORMATION 🎮\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📋 BASIC INFORMATION\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"👤 Nickname: {nickname}\n"
        f"🆔 UID: {uid}\n"
        f"🌍 Region: {str(region).upper()}\n"
        f"❤️ Likes: {format_number(likes)}\n\n"
        "ℹ️ Full rank, clan, pet, bio, and credit score fields need a full FF info API key.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Requested by: {requester_name}\n"
        f"🕒 {requested_at}"
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


def add_uidpass_entry(uid, password):
    records = load_json_file(UIDPASS_FILE, [])
    updated = False
    for item in records:
        if str(item.get("uid", "")).strip() == uid:
            item["password"] = password
            updated = True
            break
    if not updated:
        records.append({"uid": uid, "password": password})
    save_json_file(UIDPASS_FILE, records)
    return updated, len(records)


def refresh_tokens_from_uidpass():
    try:
        from update_tokens import fetch_token
    except Exception as e:
        return False, 0, 0, [f"import error: {e}"]

    uidpass_list = load_json_file(UIDPASS_FILE, [])
    new_tokens = []
    failed_uids = []

    for item in uidpass_list:
        uid = str(item.get("uid", "")).strip()
        password = str(item.get("password", "")).strip()
        if not uid or not password:
            continue
        token = fetch_token(uid, password)
        if token:
            new_tokens.append({"token": token})
        else:
            failed_uids.append(uid)

    if new_tokens:
        save_json_file(TOKEN_FILE, new_tokens)
        return True, len(new_tokens), len(uidpass_list), failed_uids

    return False, 0, len(uidpass_list), failed_uids


def is_token_error(error_message):
    msg = str(error_message).lower()
    token_error_markers = [
        "failed to load tokens",
        "no valid token found",
        "valid token",
        "update tokens.json",
    ]
    return any(marker in msg for marker in token_error_markers)


threading.Thread(target=reset_limits, daemon=True).start()


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

        text = f"API Error: {response['error']}"
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

    processing_msg = bot.reply_to(message, "Please wait... Sending likes.")
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

        if "error" in response and auto_refreshed:
            response["error"] = f"{response['error']} (tokens auto-refreshed and retried once)"

        text = f"API Error: {response['error']}"
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


@bot.message_handler(commands=["remain", "uidpass", "adduidpass", "addremain"])
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

    if cmd == "/adduidpass":
        if len(args) != 3:
            bot.reply_to(message, "Use: /adduidpass <uid> <password>")
            return

        uid = args[1].strip()
        password = args[2].strip()

        if not uid.isdigit():
            bot.reply_to(message, "UID must be numeric.")
            return

        updated, total = add_uidpass_entry(uid, password)
        ok, count, _, failed_uids = refresh_tokens_from_uidpass()
        action = "updated" if updated else "added"
        status = "OK" if ok else "WARN"
        bot.reply_to(
            message,
            f"{status}: UID {uid} {action}. UID/PASS total: {total} | Valid tokens now: {count} | Failed: {len(failed_uids)}",
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
        ok, count, _, failed_uids = refresh_tokens_from_uidpass()
        action = "updated" if updated else "added"
        status = "OK" if ok else "WARN"
        bot.reply_to(
            message,
            f"{status}: UID {uid} {action}. UID/PASS total: {total} | Valid tokens now: {count} | Failed: {len(failed_uids)}",
        )


@bot.message_handler(commands=["help"])
def help_command(message):
    user_id = message.from_user.id

    if user_id == OWNER_ID:
        help_text = (
            "Bot Commands:\n\n"
            "/like <region> <uid> - Send likes to Free Fire UID\n"
            "/ffinfo <uid> - Show Free Fire player info\n"
            "/ffinfo <region> <uid> - Show player info for a region\n"
            "/start - Start or verify\n"
            "/help - Show this help menu\n\n"
            "Owner Commands:\n"
            "/remain - Show all users usage\n"
            "/uidpass - Show total UID/PASS records\n"
            "/adduidpass <uid> <password> - Add/update UID/PASS and refresh tokens\n\n"
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

    known_commands = {"/start", "/like", "/ffinfo", "/help", "/remain", "/uidpass", "/adduidpass", "/addremain"}
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
