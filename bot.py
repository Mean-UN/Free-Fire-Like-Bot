# -*- coding: utf-8 -*-
import json
import asyncio
import math
import os
import subprocess
import sys
from datetime import datetime, time as dt_time, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

load_dotenv(override=True)

API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5000")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}
TASKS_FILE = os.getenv("TASKS_FILE", "autolike_tasks.json")
UIDPASS_FILE = os.getenv("UIDPASS_FILE", "uidpass.json")
LIKE_TARGET_DEFAULT = int(os.getenv("LIKE_TARGET", "300"))
DAILY_LIMIT_TOTAL = int(os.getenv("DAILY_LIMIT_TOTAL", "1"))
CONTACT_HANDLE = os.getenv("CONTACT_HANDLE", "@Mean_Un")
USAGE_FILE = os.getenv("USAGE_FILE", "like_usage.json")
LIMITS_FILE = os.getenv("LIMITS_FILE", "like_limits.json")
try:
    CAMBODIA_TZ = ZoneInfo("Asia/Phnom_Penh")
except ZoneInfoNotFoundError:
    # Fallback when tzdata isn't installed on Windows
    CAMBODIA_TZ = timezone(timedelta(hours=7))
AUTO_LIKE_HOUR = int(os.getenv("AUTO_LIKE_HOUR", "7"))
AUTO_LIKE_MINUTE = int(os.getenv("AUTO_LIKE_MINUTE", "0"))


HELP_TEXT = (
    "🆘 /help\n"
    "Shows this help menu with all bot features\n\n"
    "for user\n\n"
    "❤️ /like <region> <uid>\n"
    "Send likes to Free Fire player profile\n"
    "Example: /like SG 1234567890\n"
    "• /myautolikes - Check your active autolike tasks\n\n"
    "for admin\n"
    "🚀 /autolike <region> <uid> <likes> <@username> <user_id>\n"
    "➕ /addremain <user_id> <total>\n"
    "➕ /adduidpass <uid> <password>\n"
    "👾 /rmsuperbd <uid>\n"
    "🦧 /listsuperlikesbd - View all active tasks"
)


def load_tasks() -> list:
    if not os.path.exists(TASKS_FILE):
        return []
    try:
        with open(TASKS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def save_tasks(tasks: list) -> None:
    with open(TASKS_FILE, "w", encoding="utf-8") as f:
        json.dump(tasks, f, ensure_ascii=False, indent=2)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def load_limits() -> dict:
    if not os.path.exists(LIMITS_FILE):
        return {}
    try:
        with open(LIMITS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def save_limits(limits: dict) -> None:
    with open(LIMITS_FILE, "w", encoding="utf-8") as f:
        json.dump(limits, f, ensure_ascii=False, indent=2)


def load_uidpass() -> list:
    if not os.path.exists(UIDPASS_FILE):
        return []
    try:
        with open(UIDPASS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def save_uidpass(entries: list) -> None:
    with open(UIDPASS_FILE, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)


def load_usage() -> dict:
    if not os.path.exists(USAGE_FILE):
        return {}
    try:
        with open(USAGE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def save_usage(usage: dict) -> None:
    with open(USAGE_FILE, "w", encoding="utf-8") as f:
        json.dump(usage, f, ensure_ascii=False, indent=2)


def today_key_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def today_key_cambodia() -> str:
    return datetime.now(CAMBODIA_TZ).strftime("%Y-%m-%d")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT)


async def like_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    requester_id = update.effective_user.id
    use_pool = False
    pool_remaining = 0
    if not is_admin(requester_id):
        limits = load_limits()
        pool_remaining = int(limits.get(str(requester_id), 0) or 0)
        if pool_remaining > 0:
            use_pool = True
        else:
            usage = load_usage()
            today = today_key_utc()
            used_today = int(usage.get(str(requester_id), {}).get(today, 0) or 0)
            if used_today >= 1:
                await update.message.reply_text(
                    "⏳ You've reached your daily like limit!\n\n🤝Purchase VIP For More Uses!",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Purchase VIP", url=f"https://t.me/{CONTACT_HANDLE.lstrip('@')}")]]
                    ),
                )
                return

    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "Usage: /like <region> <uid>\nExample: /like SG 1234567890"
        )
        return

    region = context.args[0].upper()
    uid = context.args[1]

    url = f"{API_BASE_URL.rstrip('/')}/like"
    try:
        resp = requests.get(url, params={"uid": uid, "server_name": region}, timeout=30)
        data = resp.json()
    except Exception as exc:
        await update.message.reply_text(f"Request failed: {exc}")
        return

    if resp.status_code != 200:
        await update.message.reply_text(f"API error: {data}")
        return

    if not is_admin(requester_id):
        if use_pool:
            limits[str(requester_id)] = max(pool_remaining - 1, 0)
            save_limits(limits)
        else:
            usage = load_usage()
            today = today_key_utc()
            user_rec = usage.get(str(requester_id), {})
            user_rec[today] = int(user_rec.get(today, 0) or 0) + 1
            usage[str(requester_id)] = user_rec
            save_usage(usage)

    likes_before = data.get("LikesbeforeCommand")
    likes_after = data.get("LikesafterCommand")
    likes_added = data.get("LikesGivenByAPI")
    player_name = data.get("PlayerNickname")
    player_uid = data.get("UID", uid)

    if not likes_added or int(likes_added) <= 0:
        await update.message.reply_text(
            "⚠️ Max Likes Reached!\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "This UID has received maximum likes from elsewhere!\n\n"
            "Try another UID or wait.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("Purchase VIP", url=f"https://t.me/{CONTACT_HANDLE.lstrip('@')}")]]
            ),
        )
        return

    message = (
        f"🦠𝙇𝙄𝙆𝙀𝙎 𝘿𝙀𝙋𝙇𝙊𝙔𝙀𝘿!🦠\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 UID: {player_uid}\n"
        f"👤 Name: {player_name}\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🧌 Day Start Likes: {likes_before}\n"
        f"💔 Likes Before: {likes_before}\n"
        f"❤️‍🔥 Likes Added: +{likes_added}\n"
        f"❤️‍🩹 Total Likes Now: {likes_after}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🌟 Gᴇᴛ 220 Lɪᴋᴇs Eᴠᴇʀʏᴅᴀʏ Aᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ! 🌟\n"
        "💫6600 Like Per Month \n"
        "   ✦ 3$/month\n"
        f"  |★| {CONTACT_HANDLE} \n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

    await update.message.reply_text(message)


async def run_autolike_job(_: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    tasks = load_tasks()
    if not tasks:
        return

    today = today_key_cambodia()
    updated = False

    for task in tasks:
        try:
            total = int(task.get("likes", 0) or 0)
        except (TypeError, ValueError):
            total = 0

        delivered = int(task.get("delivered", 0) or 0)
        if total <= 0 or delivered >= total:
            continue

        if task.get("last_run_date") == today:
            continue

        region = str(task.get("region", "")).upper()
        uid = str(task.get("uid", "")).strip()
        if not uid or not region:
            continue

        url = f"{API_BASE_URL.rstrip('/')}/like"
        try:
            resp = requests.get(
                url,
                params={"uid": uid, "server_name": region},
                timeout=30,
            )
            data = resp.json()
        except Exception as exc:
            task["last_error"] = f"{exc}"
            continue

        if resp.status_code != 200:
            task["last_error"] = f"API error: {data}"
            continue

        likes_before = data.get("LikesbeforeCommand")
        likes_after = data.get("LikesafterCommand")
        likes_added = data.get("LikesGivenByAPI") or 0
        try:
            added = int(likes_added)
        except (TypeError, ValueError):
            added = 0

        task["likes_before"] = likes_before
        task["current_likes"] = likes_after
        task["likes_added"] = added
        if data.get("PlayerNickname"):
            task["name"] = data.get("PlayerNickname")

        task["delivered"] = min(delivered + added, total)
        task["last_run_date"] = today
        task.pop("last_error", None)

        if added > 0 and task["delivered"] < total:
            remaining = total - task["delivered"]
            task["estimated_days"] = int(math.ceil(remaining / added))

        updated = True

    if updated:
        save_tasks(tasks)


async def daily_autolike_loop(application) -> None:
    while True:
        now = datetime.now(CAMBODIA_TZ)
        next_run = now.replace(
            hour=AUTO_LIKE_HOUR, minute=AUTO_LIKE_MINUTE, second=0, microsecond=0
        )
        if next_run <= now:
            next_run += timedelta(days=1)
        await asyncio.sleep((next_run - now).total_seconds())
        await run_autolike_job()


async def my_autolikes_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    tasks = [t for t in load_tasks() if int(t.get("user_id", 0)) == user_id]
    if not tasks:
        await update.message.reply_text("No active autolike tasks.")
        return

    messages = [format_autolike_update(t) for t in tasks]
    await update.message.reply_text("\n\n".join(messages))


async def autolike_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Admin only.")
        return

    if not context.args or len(context.args) < 5:
        await update.message.reply_text(
            "Usage: /autolike <region> <uid> <likes> <@username> <user_id>"
        )
        return

    region = context.args[0].upper()
    uid = context.args[1]
    likes = context.args[2]
    username = context.args[3].lstrip("@")
    target_user_id = context.args[4]

    task = {
        "region": region,
        "uid": uid,
        "likes": likes,
        "username": username,
        "user_id": target_user_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "delivered": 0,
    }

    tasks = load_tasks()
    tasks.append(task)
    save_tasks(tasks)

    await update.message.reply_text(
        f"Autolike task added for {username} (UID {uid}).\n"
        f"Contact {CONTACT_HANDLE}"
    )


async def rmsuper_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Admin only.")
        return

    if not context.args or len(context.args) < 1:
        await update.message.reply_text("Usage: /rmsuperbd <uid>")
        return

    uid = context.args[0]
    tasks = load_tasks()
    remaining = [t for t in tasks if str(t.get("uid")) != str(uid)]
    if len(remaining) == len(tasks):
        await update.message.reply_text("No task found for that UID.")
        return

    save_tasks(remaining)
    await update.message.reply_text(f"Removed tasks for UID {uid}.")


async def listsuper_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Admin only.")
        return

    tasks = load_tasks()
    if not tasks:
        await update.message.reply_text("No active tasks.")
        return

    messages = [format_autolike_update(t) for t in tasks]
    await update.message.reply_text("\n\n".join(messages))


async def addremain_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Admin only.")
        return

    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Usage: /addremain <user_id> <total>")
        return

    target_user_id = context.args[0]
    try:
        total = int(context.args[1])
    except ValueError:
        await update.message.reply_text("Total must be a number.")
        return

    limits = load_limits()
    limits[str(target_user_id)] = max(total, 0)
    save_limits(limits)
    await update.message.reply_text(
        f"Remaining likes set for user {target_user_id}: {max(total, 0)}"
    )


async def adduidpass_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Admin only.")
        return

    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Usage: /adduidpass <uid> <password>")
        return

    uid = context.args[0].strip()
    password = " ".join(context.args[1:]).strip()
    if not uid or not password:
        await update.message.reply_text("UID and password are required.")
        return

    entries = load_uidpass()
    entries.append({"uid": uid, "password": password})
    save_uidpass(entries)

    try:
        result = subprocess.run(
            [sys.executable, "update_tokens.py"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            await update.message.reply_text(
                "UID/password saved, but token refresh failed.\n"
                f"Exit code: {result.returncode}\n"
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
            return
    except Exception as exc:
        await update.message.reply_text(
            f"UID/password saved, but token refresh failed: {exc}"
        )
        return

    await update.message.reply_text("UID/password saved and tokens refreshed.")


def format_autolike_update(task: dict) -> str:
    uid = task.get("uid", "N/A")
    player = task.get("name") or task.get("username") or "N/A"
    likes_before = task.get("likes_before", "N/A")
    likes_added = task.get("likes_added", "N/A")
    current_likes = task.get("current_likes", "N/A")
    delivered = int(task.get("delivered", 0) or 0)
    total = int(task.get("likes", 0) or 0)
    remaining = max(total - delivered, 0)
    progress = (delivered / total * 100) if total > 0 else 0.0

    estimated_days = task.get("estimated_days")
    if estimated_days is None:
        estimated_days = "N/A"

    return (
        "📊 **Your Daily Autolike Update** 📊\n\n"
        "━━━━━━━━━━━━━━━\n"
        f"🆔 UID: {uid}\n"
        f"👤 Player: {player}\n\n"
        "📊 **Progress:**\n"
        f"🤡 Likes Before: {likes_before}\n"
        f"🚀 Likes Added: {likes_added}\n"
        f"🗿 Current Likes: {current_likes}\n"
        f"☠️Total Delivered: {delivered}/{total}\n\n"
        "📈 **Status:**\n"
        f"🌀 Progress: {progress:.1f}%\n"
        f"❤️‍🩹 Remaining Likes: {remaining}\n"
        f"📆 Estimated Days: {estimated_days}\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🎯 Keep sharing your UID with friends!\n"
        f"Contact {CONTACT_HANDLE}"
    )


async def post_init(application) -> None:
    if application.job_queue:
        application.job_queue.run_daily(
            run_autolike_job,
            time=dt_time(AUTO_LIKE_HOUR, AUTO_LIKE_MINUTE, tzinfo=CAMBODIA_TZ),
            name="daily_autolike",
        )
    else:
        application.create_task(daily_autolike_loop(application))


def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is not set.")

    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("like", like_cmd))
    app.add_handler(CommandHandler("myautolikes", my_autolikes_cmd))
    app.add_handler(CommandHandler("autolike", autolike_cmd))
    app.add_handler(CommandHandler("addremain", addremain_cmd))
    app.add_handler(CommandHandler("adduidpass", adduidpass_cmd))
    app.add_handler(CommandHandler("rmsuperbd", rmsuper_cmd))
    app.add_handler(CommandHandler("listsuperlikesbd", listsuper_cmd))
    app.run_polling()


if __name__ == "__main__":
    main()


