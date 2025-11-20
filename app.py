#!/usr/bin/env python3
"""
Telegram Word-Splitter Bot (Webhook-ready) without python-telegram-bot.

Optimizations made:
- Single Flask app instance (unchanged)
- Webhook handling is now asynchronous: webhook returns quickly while processing happens in background threads -> fast replies to Telegram
- Reused requests.Session for faster HTTP to Telegram
- DB migration to add sent_count to tasks and track progress so /status shows accurate remaining words
- enqueue and queue-size logic changed so the first task (when nothing is running) is not treated as "in queue"
- Removed estimated times and removed time stamps from user-facing commands/reports
- /stats now shows only the requesting user's stats for the last 12 hours
- /botinfo now reports user stats for the last 1 hour and sorts users by words splitted (desc)
- hourly owner report sorts users by words splitted (desc)
- /listusers no longer shows '@' before usernames
- Faster global worker loop polling interval
- Friendly and simple text replies
- Background scheduler jobs/health notifications simplified (no time strings)
- Sent count incremented as words are sent for correct status tracking
Run with: gunicorn app:app --bind 0.0.0.0:$PORT
"""
import os
import time
import json
import sqlite3
import threading
import traceback
import logging
from datetime import datetime, timedelta
from typing import List
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, abort, jsonify
import requests

# Configure logging to stdout
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Create Flask app once
app = Flask(__name__)

# Configuration via environment variables
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")  # full https URL for webhook
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))  # numeric Telegram user id
OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "justmemmy")
MAX_ALLOWED_USERS = int(os.environ.get("MAX_ALLOWED_USERS", "50"))
MAX_QUEUE_PER_USER = int(os.environ.get("MAX_QUEUE_PER_USER", "50"))
MAINTENANCE_START_HOUR_WAT = 3  # 03:00 WAT
MAINTENANCE_END_HOUR_WAT = 4    # 04:00 WAT
DB_PATH = os.environ.get("DB_PATH", "botdata.sqlite3")
REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))

if not TELEGRAM_TOKEN or not WEBHOOK_URL or not OWNER_ID:
    logger.warning("TELEGRAM_TOKEN, WEBHOOK_URL, OWNER_ID should be set in environment.")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None

# Reuse a requests.Session for faster connections
_session = requests.Session()

# DB helper
_db_lock = threading.Lock()


def init_db():
    with _db_lock, sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        # allowed users table
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS allowed_users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                added_at TEXT,
                is_admin INTEGER DEFAULT 0
            )
            """
        )
        # tasks: one row per submitted text task
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                text TEXT,
                words_json TEXT,
                total_words INTEGER,
                sent_count INTEGER DEFAULT 0,
                status TEXT,
                created_at TEXT,
                started_at TEXT,
                finished_at TEXT
            )
            """
        )
        # split logs (what words were split) for stats
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS split_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                words INTEGER,
                created_at TEXT
            )
            """
        )
        # bot messages we sent (to allow deletion)
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS sent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                sent_at TEXT,
                deleted INTEGER DEFAULT 0
            )
            """
        )
        conn.commit()

        # Migration: ensure sent_count column exists (for older DBs)
        c.execute("PRAGMA table_info(tasks)")
        cols = [r[1] for r in c.fetchall()]
        if "sent_count" not in cols:
            try:
                c.execute("ALTER TABLE tasks ADD COLUMN sent_count INTEGER DEFAULT 0")
                conn.commit()
                logger.info("Migrated tasks table: added sent_count")
            except Exception:
                # If alter fails for some reason, log and continue
                logger.exception("Failed to add sent_count column (maybe already present)")


def db_execute(query, params=(), fetch=False):
    with _db_lock, sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute(query, params)
        if fetch:
            rows = c.fetchall()
            return rows
        conn.commit()


# Initialize DB and ensure owner is allowed/admin
init_db()
try:
    res = db_execute("SELECT user_id FROM allowed_users WHERE user_id = ?", (OWNER_ID,), fetch=True)
    if not res:
        db_execute(
            "INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)",
            (OWNER_ID, OWNER_USERNAME, datetime.utcnow().isoformat(), 1),
        )
except Exception:
    logger.exception("Error ensuring owner in allowed_users")


# In-memory per-user locks to guarantee single active worker per user (persisted states in DB)
user_locks = {}
user_locks_lock = threading.Lock()


def get_user_lock(user_id):
    with user_locks_lock:
        if user_id not in user_locks:
            user_locks[user_id] = threading.Lock()
        return user_locks[user_id]


# Utilities
def split_text_into_words(text: str) -> List[str]:
    return [w for w in text.strip().split() if w]


def compute_interval(total_words: int) -> float:
    if total_words <= 150:
        return 0.4
    elif total_words <= 300:
        return 0.5
    else:
        return 0.6


def is_maintenance_now() -> bool:
    utc_now = datetime.utcnow()
    wat_now = utc_now + timedelta(hours=1)
    h = wat_now.hour
    if MAINTENANCE_START_HOUR_WAT < MAINTENANCE_END_HOUR_WAT:
        return MAINTENANCE_START_HOUR_WAT <= h < MAINTENANCE_END_HOUR_WAT
    return h >= MAINTENANCE_START_HOUR_WAT or h < MAINTENANCE_END_HOUR_WAT


# Telegram API helpers (requests)
def tg_call(method: str, payload: dict):
    if not TELEGRAM_API:
        logger.error("tg_call attempted but TELEGRAM_API not configured")
        return None
    url = f"{TELEGRAM_API}/{method}"
    try:
        resp = _session.post(url, json=payload, timeout=REQUESTS_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            logger.error("Telegram API error: %s", data)
        return data
    except Exception:
        logger.exception("tg_call failed")
        return None


def send_message(chat_id: int, text: str, parse_mode: str = "Markdown"):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
    data = tg_call("sendMessage", payload)
    if data and data.get("result"):
        mid = data["result"].get("message_id")
        if mid:
            record_sent_message(chat_id, mid)
        return data["result"]
    return None


def delete_message(chat_id: int, message_id: int):
    payload = {"chat_id": chat_id, "message_id": message_id}
    data = tg_call("deleteMessage", payload)
    if data and data.get("ok"):
        mark_message_deleted(chat_id, message_id)
    return data


def set_webhook():
    if not TELEGRAM_API or not WEBHOOK_URL:
        logger.warning("Cannot set webhook: TELEGRAM_TOKEN or WEBHOOK_URL not configured")
        return None
    try:
        resp = _session.post(f"{TELEGRAM_API}/setWebhook", json={"url": WEBHOOK_URL}, timeout=REQUESTS_TIMEOUT)
        resp.raise_for_status()
        logger.info("Webhook set response: %s", resp.text)
        return resp.json()
    except Exception:
        logger.exception("Failed to set webhook")
        return None


# Task management
def enqueue_task(user_id: int, username: str, text: str) -> dict:
    words = split_text_into_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    # Count only queued tasks. The first task (when nothing is running) should not be treated as "in queue".
    q = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)
    pending = q[0][0] if q else 0
    if pending >= MAX_QUEUE_PER_USER:
        return {"ok": False, "reason": "queue_full", "queue_size": pending}
    db_execute(
        "INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at, sent_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (user_id, username, text, json.dumps(words), total, "queued", datetime.utcnow().isoformat(), 0),
    )
    # queue_size from perspective of queued tasks
    return {"ok": True, "total_words": total, "queue_size": pending + 1}


def get_next_task_for_user(user_id: int):
    rows = db_execute(
        "SELECT id, words_json, total_words, text FROM tasks WHERE user_id = ? AND status = 'queued' ORDER BY id ASC LIMIT 1",
        (user_id,),
        fetch=True,
    )
    if not rows:
        return None
    r = rows[0]
    return {"id": r[0], "words": json.loads(r[1]), "total_words": r[2], "text": r[3]}


def set_task_status(task_id: int, status: str):
    if status == "running":
        db_execute("UPDATE tasks SET status = ?, started_at = ? WHERE id = ?", (status, datetime.utcnow().isoformat(), task_id))
    elif status in ("done", "cancelled"):
        db_execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", (status, datetime.utcnow().isoformat(), task_id))
    else:
        db_execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))


def mark_task_paused(task_id: int):
    set_task_status(task_id, "paused")


def mark_task_resumed(task_id: int):
    set_task_status(task_id, "running")


def mark_task_done(task_id: int):
    set_task_status(task_id, "done")


def cancel_active_task_for_user(user_id: int):
    rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status IN ('queued','running','paused')", (user_id,), fetch=True)
    count = 0
    for r in rows:
        db_execute("UPDATE tasks SET status = ? WHERE id = ?", ("cancelled", r[0]))
        count += 1
    return count


def record_split_log(user_id: int, username: str, words: int):
    db_execute("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (?, ?, ?, ?)",
               (user_id, username, words, datetime.utcnow().isoformat()))


def record_sent_message(chat_id: int, message_id: int):
    db_execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)",
               (chat_id, message_id, datetime.utcnow().isoformat()))


def mark_message_deleted(chat_id: int, message_id: int):
    db_execute("UPDATE sent_messages SET deleted = 1 WHERE chat_id = ? AND message_id = ?", (chat_id, message_id))


def get_messages_older_than(days=1):
    cutoff = datetime.utcnow() - timedelta(days=days)
    rows = db_execute("SELECT chat_id, message_id, sent_at FROM sent_messages WHERE deleted = 0", fetch=True)
    res = []
    for r in rows:
        try:
            sent_at = datetime.fromisoformat(r[2])
        except Exception:
            continue
        if sent_at < cutoff:
            res.append({"chat_id": r[0], "message_id": r[1]})
    return res


# Authorization helpers
def is_allowed(user_id: int) -> bool:
    rows = db_execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True)
    return bool(rows)


def is_admin(user_id: int) -> bool:
    rows = db_execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True)
    if not rows:
        return False
    return bool(rows[0][0])


# Background worker to process tasks for users
_worker_stop = threading.Event()


def process_user_queue(user_id: int, chat_id: int, username: str):
    lock = get_user_lock(user_id)
    if not lock.acquire(blocking=False):
        return
    try:
        while True:
            task = get_next_task_for_user(user_id)
            if not task:
                break
            task_id = task["id"]
            words = task["words"]
            total = task["total_words"]
            set_task_status(task_id, "running")
            # queued count after picking this task (how many additional tasks are waiting)
            qcount = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
            interval = compute_interval(total)
            # Friendly start message, no estimated time
            start_msg = f"🚀 Starting your split now. Words: {total}."
            if qcount:
                start_msg += f" There are {qcount} more task(s) waiting."
            send_message(chat_id, start_msg)
            i = 0
            # Ensure we capture any sent_count from DB if something resumed mid-way
            sent_info = db_execute("SELECT sent_count FROM tasks WHERE id = ?", (task_id,), fetch=True)
            sent = int(sent_info[0][0] or 0) if sent_info else 0
            i = sent
            while i < total:
                status_row = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
                if not status_row:
                    break
                status = status_row[0][0]
                if status == "paused":
                    send_message(chat_id, "⏸️ Task paused. Use /resume to continue.")
                    # wait until resumed or cancelled
                    while True:
                        time.sleep(0.5)
                        status_row = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
                        if not status_row:
                            break
                        status = status_row[0][0]
                        if status == "running":
                            send_message(chat_id, "▶️ Resuming your task.")
                            break
                        if status == "cancelled":
                            break
                    if status == "cancelled":
                        break
                if status == "cancelled":
                    break
                # Send the word
                send_message(chat_id, words[i])
                # Update sent_count in DB immediately for accurate /status reporting
                db_execute("UPDATE tasks SET sent_count = sent_count + 1 WHERE id = ?", (task_id,))
                i += 1
                # Sleep governing the dynamic delay
                time.sleep(interval)
            final_status_row = db_execute("SELECT status, sent_count, total_words FROM tasks WHERE id = ?", (task_id,), fetch=True)
            final_status = final_status_row[0][0] if final_status_row else "done"
            sent_count = int(final_status_row[0][1] or 0) if final_status_row else total
            total_words_final = int(final_status_row[0][2] or total) if final_status_row else total
            if final_status != "cancelled":
                mark_task_done(task_id)
                # Record how many words were actually sent (use sent_count)
                record_split_log(user_id, username, sent_count)
                send_message(chat_id, "✅ All done! Your text was split.")
            else:
                send_message(chat_id, "🛑 Task stopped.")
            qcount_after = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
            if qcount_after > 0:
                send_message(chat_id, "⏩ Next task will start soon.")
    finally:
        lock.release()


def global_worker_loop():
    # Poll frequently so tasks start fast
    while not _worker_stop.is_set():
        try:
            rows = db_execute("SELECT DISTINCT user_id, username FROM tasks WHERE status = 'queued' ORDER BY created_at ASC", fetch=True)
            for r in rows:
                user_id = r[0]
                username = r[1] or ""
                # For chat_id we use user_id (assumes private chats). If you use group chats change accordingly.
                t = threading.Thread(target=process_user_queue, args=(user_id, user_id, username), daemon=True)
                t.start()
            time.sleep(0.5)
        except Exception:
            traceback.print_exc()
            time.sleep(1)


# Scheduler jobs
scheduler = BackgroundScheduler()


def hourly_owner_stats():
    cutoff = datetime.utcnow() - timedelta(hours=1)
    rows = db_execute("SELECT user_id, username, SUM(words) as s FROM split_logs WHERE created_at >= ? GROUP BY user_id ORDER BY s DESC", (cutoff.isoformat(),), fetch=True)
    if not rows:
        send_message(OWNER_ID, "🕐 Last 1 hour: no splits.")
        return
    lines = []
    total_words = 0
    for r in rows:
        uid = r[0]
        uname = r[1] or ""
        wsum = int(r[2] or 0)
        total_words += wsum
        lines.append(f"{uid} - {wsum} words ({uname})" if uname else f"{uid} - {wsum} words")
    body = "🕐 Last 1 hour activity:\n" + "\n".join(lines) + f"\n\nTotal words: {total_words}"
    send_message(OWNER_ID, body)


def delete_old_bot_messages():
    msgs = get_messages_older_than(days=1)
    for m in msgs:
        try:
            delete_message(m["chat_id"], m["message_id"])
        except Exception:
            mark_message_deleted(m["chat_id"], m["message_id"])


def maintenance_hourly_health():
    # simple heartbeat without time
    send_message(OWNER_ID, "👑 Bot health: running.")


scheduler.add_job(hourly_owner_stats, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=10))
scheduler.add_job(delete_old_bot_messages, "interval", minutes=30, next_run_time=datetime.utcnow() + timedelta(seconds=20))
scheduler.add_job(maintenance_hourly_health, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=15))
scheduler.start()

_worker_thread = threading.Thread(target=global_worker_loop, daemon=True)
_worker_thread.start()


# Webhook: handle updates asynchronously for fast HTTP replies
def handle_update(update_json: dict):
    try:
        if "message" in update_json:
            msg = update_json["message"]
            user = msg.get("from", {})
            user_id = user.get("id")
            username = user.get("username") or user.get("first_name") or ""
            text = msg.get("text") or ""
            if text.startswith("/"):
                parts = text.split(None, 1)
                command = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                handle_command(user_id, username, command, args)
            else:
                handle_new_text(user_id, username, text)
    except Exception:
        logger.exception("Error handling webhook update")


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update_json = request.get_json(force=True)
    except Exception:
        return "no json", 400
    # Process asynchronously so Telegram gets a fast 200
    try:
        threading.Thread(target=handle_update, args=(update_json,), daemon=True).start()
    except Exception:
        logger.exception("Failed to spawn background handler")
    return jsonify({"ok": True})


# Command handlers (perform actions and send messages; no Flask Response returned now)
def handle_command(user_id: int, username: str, command: str, args: str):
    # Quick guard: missing user_id?
    if not user_id:
        return
    if not is_allowed(user_id) and command not in ("/start", "/help"):
        send_message(user_id, "❌ Sorry, you are not allowed to use this bot. The owner has been notified.")
        send_message(OWNER_ID, f"⚠️ Unallowed access attempt by {username or user_id} ({user_id}).")
        return

    if command == "/start":
        body = (
            f"👋 Hi {username or user_id}!\n"
            "I split your text into individual word messages.\n"
            "Commands: /start /example /pause /resume /status /stop /stats /about\n"
            "Owner-only: /adduser /removeuser /listusers /botinfo /broadcast\n"
            "Just send any text and I'll split it for you."
        )
        send_message(user_id, body)
        return

    if command == "/example":
        sample = "This is a demo split"
        send_message(user_id, "Running a short example...")
        res = enqueue_task(user_id, username, sample)
        running_exists = bool(db_execute("SELECT 1 FROM tasks WHERE user_id = ? AND status IN ('running','paused')", (user_id,), fetch=True))
        queued = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
        if running_exists:
            send_message(user_id, f"📝 Queued. You have {queued} task(s) waiting.")
        else:
            send_message(user_id, "🚀 Task queued and will start shortly.")
        return

    if command == "/pause":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'running' ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        if not rows:
            send_message(user_id, "❌ No active task to pause.")
            return
        task_id = rows[0][0]
        mark_task_paused(task_id)
        send_message(user_id, "⏸️ Paused. Use /resume to continue.")
        return

    if command == "/resume":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'paused' ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        if not rows:
            send_message(user_id, "❌ No paused task to resume.")
            return
        task_id = rows[0][0]
        mark_task_resumed(task_id)
        send_message(user_id, "▶️ Resuming your task now.")
        return

    if command == "/status":
        active = db_execute("SELECT id, status, total_words, sent_count FROM tasks WHERE user_id = ? AND status IN ('running','paused') ORDER BY started_at ASC LIMIT 1", (user_id,), fetch=True)
        queued = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
        if active:
            aid, status, total_words, sent_count = active[0]
            remaining = int(total_words or 0) - int(sent_count or 0)
            send_message(user_id, f"📊 Status: {status}\nRemaining words: {remaining}\nQueue size: {queued}")
        else:
            if queued > 0:
                send_message(user_id, f"📝 Waiting. Your first task is in line. Queue size: {queued}")
            else:
                send_message(user_id, "📊 You have no active or queued tasks.")
        return

    if command == "/stop":
        queued_rows = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)
        queued_count = queued_rows[0][0] if queued_rows else 0
        stopped = cancel_active_task_for_user(user_id)
        if stopped > 0:
            send_message(user_id, "🛑 Active task stopped. Your queued tasks have been cleared too.")
        elif queued_count > 0:
            db_execute("UPDATE tasks SET status = 'cancelled' WHERE user_id = ? AND status = 'queued'", (user_id,))
            send_message(user_id, f"🛑 Cleared {queued_count} queued task(s).")
        else:
            send_message(user_id, "ℹ️ You had no active or queued tasks.")
        return

    if command == "/stats":
        cutoff = datetime.utcnow() - timedelta(hours=12)
        rows = db_execute("SELECT SUM(words) FROM split_logs WHERE user_id = ? AND created_at >= ?", (user_id, cutoff.isoformat()), fetch=True)
        words = int(rows[0][0] or 0) if rows else 0
        send_message(user_id, f"🕰️ Your last 12 hours: {words} words split. Nice!")
        return

    if command == "/about":
        body = (
            "About:\nI split texts into single-word messages. Features: queueing, pause/resume, hourly owner stats, auto-delete old bot messages.\n"
            f"Developer: @{OWNER_USERNAME}"
        )
        send_message(user_id, body)
        return

    if command == "/adduser":
        if not is_admin(user_id):
            send_message(user_id, "❌ You are not allowed to use this.")
            return
        if not args:
            send_message(user_id, "Usage: /adduser <telegram_user_id> [username]")
            return
        parts = args.split()
        try:
            target_id = int(parts[0])
        except Exception:
            send_message(user_id, "Invalid user id. Must be numeric.")
            return
        uname = parts[1] if len(parts) > 1 else ""
        count = db_execute("SELECT COUNT(*) FROM allowed_users", fetch=True)[0][0]
        if count >= MAX_ALLOWED_USERS:
            send_message(user_id, f"Cannot add more users. Max: {MAX_ALLOWED_USERS}")
            return
        db_execute("INSERT OR REPLACE INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)",
                   (target_id, uname, datetime.utcnow().isoformat(), 0))
        send_message(user_id, f"✅ User {target_id} added.")
        send_message(target_id, "✅ You have been added. Send any text to start.")
        return

    if command == "/removeuser":
        if not is_admin(user_id):
            send_message(user_id, "❌ You are not allowed to use this.")
            return
        if not args:
            send_message(user_id, "Usage: /removeuser <telegram_user_id>")
            return
        try:
            target_id = int(args.split()[0])
        except Exception:
            send_message(user_id, "Invalid user id.")
            return
        db_execute("DELETE FROM allowed_users WHERE user_id = ?", (target_id,))
        send_message(user_id, f"✅ User {target_id} removed.")
        send_message(target_id, "❌ You have been removed. Contact the owner to regain access.")
        return

    if command == "/listusers":
        if not is_admin(user_id):
            send_message(user_id, "❌ You are not allowed to use this.")
            return
        rows = db_execute("SELECT user_id, username, is_admin, added_at FROM allowed_users", fetch=True)
        lines = []
        for r in rows:
            lines.append(f"{r[0]} {r[1] or ''} admin={bool(r[2])} added={r[3]}")
        body = "Allowed users:\n" + ("\n".join(lines) if lines else "(none)")
        send_message(user_id, body)
        return

    if command == "/botinfo":
        if user_id != OWNER_ID:
            send_message(user_id, "❌ Only the bot owner can use /botinfo")
            return
        total_allowed = db_execute("SELECT COUNT(*) FROM allowed_users", fetch=True)[0][0]
        active_tasks = db_execute("SELECT COUNT(*) FROM tasks WHERE status IN ('running','paused')", fetch=True)[0][0]
        queued_tasks = db_execute("SELECT COUNT(*) FROM tasks WHERE status = 'queued'", fetch=True)[0][0]
        cutoff = datetime.utcnow() - timedelta(hours=1)
        rows = db_execute("SELECT user_id, username, SUM(words) as s FROM split_logs WHERE created_at >= ? GROUP BY user_id ORDER BY s DESC", (cutoff.isoformat(),), fetch=True)
        peruser_lines = []
        for r in rows:
            peruser_lines.append(f"{r[0]} - {int(r[2] or 0)} words ({r[1] or ''})" if r[1] else f"{r[0]} - {int(r[2] or 0)} words")
        body = (
            f"Bot status: Online\n"
            f"Allowed users: {total_allowed}\n"
            f"Active tasks: {active_tasks}\n"
            f"Queued tasks: {queued_tasks}\n"
            f"User stats (last 1h):\n" + ("\n".join(peruser_lines) if peruser_lines else "No activity")
        )
        send_message(user_id, body)
        return

    if command == "/broadcast":
        if user_id != OWNER_ID:
            send_message(user_id, "❌ Only owner can broadcast")
            return
        if not args:
            send_message(user_id, "Usage: /broadcast <message>")
            return
        rows = db_execute("SELECT user_id FROM allowed_users", fetch=True)
        count = 0
        fails = 0
        for r in rows:
            try:
                send_message(r[0], f"📣 Broadcast from owner:\n\n{args}")
                count += 1
            except Exception:
                fails += 1
        send_message(user_id, f"Broadcast done. Success: {count}, Failed: {fails}")
        return

    send_message(user_id, "Unknown command.")


def get_queue_size(user_id):
    q = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)
    return q[0][0] if q else 0


def handle_new_text(user_id: int, username: str, text: str):
    if not is_allowed(user_id):
        send_message(user_id, "❌ Sorry, you are not allowed. The owner has been notified.")
        send_message(OWNER_ID, f"⚠️ Unallowed access attempt by {username or user_id} ({user_id}). Message: {text}")
        return
    if is_maintenance_now():
        send_message(user_id, "🛠️ Maintenance in progress. New tasks are blocked. Please try later.")
        send_message(OWNER_ID, f"🛠️ Maintenance attempt by {user_id}.")
        return
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        if res.get("reason") == "empty":
            send_message(user_id, "Empty text. Nothing to split.")
            return
        if res.get("reason") == "queue_full":
            send_message(user_id, f"❌ Your queue is full ({res['queue_size']}). Use /stop or wait.")
            return
    # Determine if there is an active task for this user
    running_exists = bool(db_execute("SELECT 1 FROM tasks WHERE user_id = ? AND status IN ('running','paused')", (user_id,), fetch=True))
    queued = get_queue_size(user_id)
    if running_exists:
        send_message(user_id, f"📝 Queued. You have {queued} task(s) waiting.")
    else:
        send_message(user_id, f"🚀 Task queued and will start shortly. Words: {res['total_words']}. Speed: {compute_interval(res['total_words'])}s per word")
    return


# Ensure the root GET exists and POST forwarded to webhook() fallback
@app.route("/", methods=["GET", "POST"])
def root_forward():
    if request.method == "POST":
        logger.info("Received POST at root; forwarding to /webhook")
        try:
            update_json = request.get_json(force=True)
            threading.Thread(target=handle_update, args=(update_json,), daemon=True).start()
            return jsonify({"ok": True})
        except Exception:
            logger.exception("Forwarding POST to webhook failed")
            return jsonify({"ok": False, "error": "forward failed"}), 200
    return "Word Splitter Bot is running.", 200


# Health endpoint for UptimeRobot (accept HEAD and GET, and both /health and /health/)
@app.route("/health", methods=["GET", "HEAD"])
@app.route("/health/", methods=["GET", "HEAD"])
def health():
    logger.info("Health check from %s method=%s", request.remote_addr, request.method)
    return jsonify({"ok": True}), 200


# Debug: list all registered routes (useful to verify /health exists)
@app.route("/debug/routes", methods=["GET"])
def debug_routes():
    try:
        rules = sorted(str(r) for r in app.url_map.iter_rules())
        return jsonify({"routes": rules}), 200
    except Exception:
        logger.exception("Failed to list routes")
        return jsonify({"ok": False, "error": "failed to list routes"}), 500


if __name__ == "__main__":
    # Optionally set webhook when running directly (local debug). On Render, gunicorn will import app and this block won't run.
    try:
        set_webhook()
    except Exception:
        logger.exception("Failed to set webhook at startup")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
