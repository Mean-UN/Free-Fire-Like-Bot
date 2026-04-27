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
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5000").rstrip("/")

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
        response = requests.get(url, params={"uid": uid, "server_name": region}, timeout=20)
        if response.status_code != 200:
            return {"error": f"API returned {response.status_code}: {response.text[:180]}"}
        return response.json()
    except requests.exceptions.RequestException:
        return {"error": "API failed. Please try again later."}
    except ValueError:
        return {"error": "Invalid JSON response from API."}


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
        try:
            bot.edit_message_text(text=text, chat_id=processing_msg.chat.id, message_id=processing_msg.message_id)
        except Exception:
            bot.reply_to(message, text)
        return

    if not isinstance(response, dict) or response.get("status") != 1:
        try:
            bot.edit_message_text(
                text="UID already reached max likes for now. Try another UID or after 24 hours.",
                chat_id=processing_msg.chat.id,
                message_id=processing_msg.message_id,
            )
        except Exception:
            bot.reply_to(message, "Invalid UID or unable to fetch data.")
        return

    try:
        # Validate response contains required fields
        if "UID" not in response or "PlayerNickname" not in response or "LikesGivenByAPI" not in response:
            error_msg = f"I can't decode your info. Response missing required fields: {response}"
            logger.error(error_msg)
            try:
                bot.edit_message_text(
                    text="Error: Failed to decode player information. Please try again.",
                    chat_id=processing_msg.chat.id,
                    message_id=processing_msg.message_id,
                )
            except Exception:
                bot.reply_to(message, "Error: Failed to decode player information. Please try again.")
            return
        
        player_uid = str(response.get("UID", uid)).strip()
        player_name = str(response.get("PlayerNickname", "N/A")).strip()
        region_name = str(response.get("Region", region)).strip()
        likes_before = str(response.get("LikesbeforeCommand", "N/A"))
        likes_after = str(response.get("LikesafterCommand", "N/A"))
        likes_given = str(response.get("LikesGivenByAPI", "N/A"))

        usage["used"] += 1
        usage["last_used"] = now_utc
        like_tracker[user_id] = usage

        response_text = (
            "Request processed successfully\n\n"
            f"Name: {player_name}\n"
            f"UID: {player_uid}\n"
            f"Region: {region_name}\n"
            f"Likes Before: {likes_before}\n"
            f"Likes Added: {likes_given}\n"
            f"Total Likes Now: {likes_after}\n"
            f"Remaining Requests: {max_limit - usage['used']}\n"
            "Credit: @Mean_Un"
        )

        bot.edit_message_text(
            text=response_text,
            chat_id=processing_msg.chat.id,
            message_id=processing_msg.message_id,
        )
    except Exception as e:
        logger.error(f"Error in process_like: {e}")
        try:
            bot.edit_message_text(
                text="Something went wrong. Please try again later.",
                chat_id=processing_msg.chat.id,
                message_id=processing_msg.message_id,
            )
        except Exception:
            bot.reply_to(message, "Something went wrong. Please try again later.")


@bot.message_handler(commands=["remain", "uidpass", "adduidpass"])
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


@bot.message_handler(commands=["help"])
def help_command(message):
    user_id = message.from_user.id

    if user_id == OWNER_ID:
        help_text = (
            "Bot Commands:\n\n"
            "/like <region> <uid> - Send likes to Free Fire UID\n"
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

    known_commands = {"/start", "/like", "/help", "/remain", "/uidpass", "/adduidpass"}
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
