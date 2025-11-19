#!/usr/bin/env python3
"""
Telegram Word-Splitter Bot (Webhook-ready) without python-telegram-bot.

This version V4 features:
1. Direct Task Start from the Webhook thread (for the first task), ensuring the lock is held immediately.
2. Simplified global worker loop to reduce SQLite contention.
3. Improved lock management for faster command replies.
"""
import os
import time
import json
import sqlite3
import threading
import traceback
import logging
from datetime import datetime, timedelta
from typing import List, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, abort, jsonify
import requests

# Configure logging to stdout (Render will show these logs)
name = "word_splitter_bot"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(name)

# Create Flask app once
app = Flask(name)

# --- Configuration via environment variables ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "") 
OWNER_ID = int(os.environ.get("OWNER_ID", "0")) 
OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "justmemmy") 
MAX_ALLOWED_USERS = int(os.environ.get("MAX_ALLOWED_USERS", "50"))
MAX_QUEUE_PER_USER = int(os.environ.get("MAX_QUEUE_PER_USER", "50"))
MAINTENANCE_START_HOUR_WAT = 3 
MAINTENANCE_END_HOUR_WAT = 4 
DB_PATH = os.environ.get("DB_PATH", "botdata.sqlite3")
REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))

if not TELEGRAM_TOKEN or not WEBHOOK_URL or not OWNER_ID:
    logger.warning("TELEGRAM_TOKEN, WEBHOOK_URL, OWNER_ID should be set in environment.")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None

# --- DB helper ---

_db_lock = threading.Lock()

def init_db():
    with _db_lock, sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
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
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                text TEXT,
                words_json TEXT,
                total_words INTEGER,
                status TEXT,
                current_index INTEGER DEFAULT 0,
                created_at TEXT,
                started_at TEXT,
                finished_at TEXT
            )
            """
        )
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

def db_execute(query, params=(), fetch=False):
    # CRITICAL: This global lock is the source of command slowness. 
    # It must be used, but all calling functions must minimize their time here.
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

# In-memory per-user locks
user_locks = {}
user_locks_lock = threading.Lock()

def get_user_lock(user_id):
    """Returns the unique lock for a given user_id."""
    with user_locks_lock:
        if user_id not in user_locks:
            user_locks[user_id] = threading.Lock()
        return user_locks[user_id]

# --- Utilities and Task management (mostly unchanged) ---

def get_now_iso():
    return datetime.utcnow().isoformat()

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

def tg_call(method: str, payload: dict):
    if not TELEGRAM_API:
        logger.error("tg_call attempted but TELEGRAM_API not configured")
        return None
    url = f"{TELEGRAM_API}/{method}"
    try:
        resp = requests.post(url, json=payload, timeout=REQUESTS_TIMEOUT)
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

def set_task_status(task_id: int, status: str):
    if status == "running":
        db_execute("UPDATE tasks SET status = ?, started_at = ? WHERE id = ?", (status, get_now_iso(), task_id))
    elif status in ("done", "cancelled"):
        db_execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", (status, get_now_iso(), task_id))
    else:
        db_execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))

def get_next_task_id_for_user(user_id: int) -> Optional[int]:
    """Retrieves the ID of the next queued task for a user."""
    rows = db_execute(
        "SELECT id FROM tasks WHERE user_id = ? AND status = 'queued' ORDER BY id ASC LIMIT 1",
        (user_id,),
        fetch=True,
    )
    return rows[0][0] if rows else None

# (Other task management functions like enqueue_task, get_running_task_for_user, mark_task_paused, etc. remain as V3)

# ... (Insert original V3 definitions for: enqueue_task, get_next_task_for_user, get_running_task_for_user, 
# mark_task_paused, mark_task_resumed, mark_task_done, cancel_active_task_for_user, 
# record_split_log, record_sent_message, mark_message_deleted, get_messages_older_than,
# is_allowed, is_admin, get_queue_size. They were omitted here for brevity but are needed.)
def enqueue_task(user_id: int, username: str, text: str) -> dict:
    words = split_text_into_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    q = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status IN ('queued','running','paused')", (user_id,), fetch=True)
    pending = q[0][0] if q else 0
    if pending >= MAX_QUEUE_PER_USER:
        return {"ok": False, "reason": "queue_full", "queue_size": pending}
    
    db_execute(
        "INSERT INTO tasks (user_id, username, text, words_json, total_words, status, current_index, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (user_id, username, text, json.dumps(words), total, "queued", 0, get_now_iso()),
    )
    # Fetch the ID of the task just inserted (using ROWID if it's the latest for the user)
    task_id_row = db_execute("SELECT id FROM tasks WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user_id,), fetch=True)
    new_task_id = task_id_row[0][0] if task_id_row else None

    return {"ok": True, "total_words": total, "queue_size": pending + 1, "task_id": new_task_id, "words": words}

def get_next_task_for_user(user_id: int) -> Optional[dict]:
    rows = db_execute(
        "SELECT id, words_json, total_words, text, current_index FROM tasks WHERE user_id = ? AND status = 'queued' ORDER BY id ASC LIMIT 1",
        (user_id,),
        fetch=True,
    )
    if not rows:
        return None
    r = rows[0]
    return {"id": r[0], "words": json.loads(r[1]), "total_words": r[2], "text": r[3], "current_index": r[4]}

def get_running_task_for_user(user_id: int) -> Optional[dict]:
    rows = db_execute(
        "SELECT id, words_json, total_words, text, current_index, status FROM tasks WHERE user_id = ? AND status IN ('running', 'paused') ORDER BY id ASC LIMIT 1",
        (user_id,),
        fetch=True,
    )
    if not rows:
        return None
    r = rows[0]
    return {"id": r[0], "words": json.loads(r[1]), "total_words": r[2], "text": r[3], "current_index": r[4], "status": r[5]}

def mark_task_paused(task_id: int, current_index: int):
    db_execute("UPDATE tasks SET status = 'paused', current_index = ? WHERE id = ?", (current_index, task_id))

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
        (user_id, username, words, get_now_iso()))

def record_sent_message(chat_id: int, message_id: int):
    db_execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)",
        (chat_id, message_id, get_now_iso()))

def is_allowed(user_id: int) -> bool:
    rows = db_execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True)
    return bool(rows)

def is_admin(user_id: int) -> bool:
    rows = db_execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True)
    if not rows:
        return False
    return bool(rows[0][0])

def get_queue_size(user_id):
    q = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)
    return q[0][0] if q else 0

# --- Background Scheduler Logic ---

def release_user_lock_if_held(user_id: int):
    """Helper to safely release the lock if it's currently held."""
    lock = get_user_lock(user_id)
    if lock.locked():
        try:
            lock.release()
            # Immediately try to start the next task for this user
            threading.Thread(target=_start_next_queued_task_if_exists, args=(user_id, user_id, ""), daemon=True).start()
        except RuntimeError:
            pass 

def send_word_and_schedule_next(task_id: int, user_id: int, chat_id: int, words: List[str], index: int, interval: float, username: str):
    """Sends a single word and schedules the next word to be sent if the task is running."""
    
    lock = get_user_lock(user_id)
    
    # CRITICAL: We MUST be able to acquire the lock here, otherwise something is wrong.
    if not lock.acquire(blocking=False):
        logger.warning(f"Scheduler failed to acquire lock for task {task_id}. Releasing scheduler chain.")
        return
    
    try:
        # 1. Check current status
        status_row = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
        if not status_row:
            logger.warning(f"Task {task_id} disappeared during split.")
            return

        status = status_row[0][0]
        
        if status == "cancelled":
            set_task_status(task_id, "cancelled")
            send_message(chat_id, "🛑 Active Task Stopped! The current word transmission has been terminated.")
            return
        
        if status == "paused":
            mark_task_paused(task_id, index)
            send_message(chat_id, "⏸️ Task Paused! Waiting for /resume to continue.")
            return

        if status != "running":
            return
        
        # 2. Send the current word
        if index < len(words):
            send_message(chat_id, words[index])
            
            # Update the current index in DB
            db_execute("UPDATE tasks SET current_index = ? WHERE id = ?", (index + 1, task_id))
        else:
            return

        # 3. Determine next action
        next_index = index + 1
        
        if next_index < len(words):
            # Schedule the next word
            scheduler.add_job(
                send_word_and_schedule_next, 
                "date", 
                run_date=datetime.utcnow() + timedelta(seconds=interval), 
                kwargs={
                    "task_id": task_id, 
                    "user_id": user_id, 
                    "chat_id": chat_id, 
                    "words": words, 
                    "index": next_index, 
                    "interval": interval, 
                    "username": username
                },
                id=f"word_split_{task_id}_{next_index}", 
                replace_existing=True
            )
        else:
            # Task is complete
            mark_task_done(task_id)
            record_split_log(user_id, username, len(words))
            send_message(chat_id, "✅ All Done! Split finished successfully.")
            
            qcount_after = db_execute("SELECT COUNT() FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
            if qcount_after > 0:
                send_message(chat_id, "⏩ Next Task Starting Soon! Your next queued task is ready for processing.")
            
    except Exception:
        logger.exception(f"Fatal error in scheduled word transmission for task {task_id}")
        set_task_status(task_id, "cancelled")
        send_message(chat_id, "❌ Critical Error: Word transmission failed unexpectedly and has been stopped.")
    finally:
        # Crucial: Release the lock *only* if the task is finished/cancelled/paused
        status = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
        
        if status and status[0][0] in ('done', 'cancelled', 'paused'):
            if lock.locked():
                 lock.release()
                 # Try to start the next queued task if the current one is truly finished/cancelled
                 if status[0][0] != 'paused':
                    threading.Thread(target=_start_next_queued_task_if_exists, args=(user_id, user_id, username), daemon=True).start()
        else:
            # If the task is RUNNING, the lock must be released to allow the next scheduled job 
            # to acquire it in the future, but NOT NOW, as it's still being used.
            # *Wait, the scheduler handles the timing, not the lock.*
            # If status is 'running', the lock must be released immediately so the webhooks can run.
            # The next scheduled job will acquire it.
            if lock.locked():
                lock.release()
            

# --- Background worker to process tasks for users ---

def _start_next_queued_task_if_exists(user_id: int, chat_id: int, username: str):
    """
    Called by the global loop or after a task finishes/is cancelled.
    Acquires the lock, starts the task, and keeps the lock until the task finishes.
    """
    lock = get_user_lock(user_id)
    
    # 1. Acquire lock for this user to ensure only one task runs/schedules at a time
    if not lock.acquire(blocking=False):
        # Another task is currently running for this user (lock is held), so exit.
        return
    
    try:
        # 2. Get the next queued task
        task = get_next_task_for_user(user_id)
        if not task:
            # No more tasks for this user, release lock
            return 

        task_id = task["id"]
        words = task["words"]
        total = task["total_words"]
        set_task_status(task_id, "running") # Marks task as running
        
        qcount = db_execute("SELECT COUNT() FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]
        interval = compute_interval(total)
        est_seconds = interval * total
        est_str = str(timedelta(seconds=int(est_seconds)))

        start_msg = (
            f"🚀 Task Starting! (Waiting: {qcount})\n"
            f"Words: {total}. Time per word: {interval}s\n"
            f"⏳ Estimated total time: {est_str}"
        )
        send_message(chat_id, start_msg)
        
        # 3. Schedule the very first word immediately, which maintains the lock
        scheduler.add_job(
            send_word_and_schedule_next, 
            "date", 
            run_date=datetime.utcnow() + timedelta(seconds=0.1), 
            kwargs={
                "task_id": task_id, 
                "user_id": user_id, 
                "chat_id": chat_id, 
                "words": words, 
                "index": 0, 
                "interval": interval, 
                "username": username
            },
            id=f"word_split_{task_id}_0", 
            replace_existing=True
        )
        
    except Exception:
        logger.exception("Error in _start_next_queued_task_if_exists")
        # If an error occurred before starting the job, release the lock manually.
        if lock.locked():
             lock.release()
    finally:
        # The lock is either released on failure above, or held until 
        # the task finishes (released in send_word_and_schedule_next).
        pass

_worker_stop = threading.Event()

def global_worker_loop():
    """Simplified: Runs less often and only checks for queued users."""
    while not _worker_stop.is_set():
        try:
            # Look for any user with a queued task
            rows = db_execute("SELECT DISTINCT user_id, username FROM tasks WHERE status IN ('queued') ORDER BY created_at ASC", fetch=True)
            for r in rows:
                user_id = r[0]
                username = r[1] or ""
                # Attempt to start the task only if the lock is not currently held
                if not get_user_lock(user_id).locked():
                     t = threading.Thread(target=_start_next_queued_task_if_exists, args=(user_id, user_id, username), daemon=True)
                     t.start()
            time.sleep(5) # Increased sleep to 5 seconds to reduce DB pressure
        except Exception:
            traceback.print_exc()
            time.sleep(10)

# (Scheduler setup remains unchanged)

scheduler = BackgroundScheduler()
# ... (hourly_owner_stats, delete_old_bot_messages, maintenance_hourly_health definitions go here)

scheduler.add_job(lambda: send_message(OWNER_ID, "Health Check"), "interval", hours=1) 
scheduler.add_job(lambda: [send_message(OWNER_ID, f"🕐 Last 1 Hour Activity: {db_execute('SELECT SUM(words) FROM split_logs WHERE created_at >= ?', [(datetime.utcnow() - timedelta(hours=1)).isoformat()], fetch=True)[0][0] or 0} words") if db_execute('SELECT 1 FROM split_logs', fetch=True) else None] , "interval", hours=1)
scheduler.add_job(lambda: [tg_call("deleteMessage", {"chat_id": m["chat_id"], "message_id": m["message_id"]}) for m in get_messages_older_than(days=1)], "interval", minutes=30)
scheduler.start()


_worker_thread = threading.Thread(target=global_worker_loop, daemon=True)
_worker_thread.start()

# --- Flask webhook endpoint (POST only) ---

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update_json = request.get_json(force=True)
    except Exception:
        return "no json", 400

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
                # This function call (handle_command) still holds the webhook connection, 
                # so it must be fast.
                return handle_command(user_id, username, command, args) 
            else: 
                return handle_new_text(user_id, username, text) 
        return jsonify({"ok": True})
    except Exception: 
        logger.exception("Error handling webhook update") 
        return jsonify({"ok": True}) 

# --- Command handlers ---

def handle_command(user_id: int, username: str, command: str, args: str):
    # ... (Authorization and command logic remain, but are now running in the Flask thread. 
    # The commands need to be very quick, relying on DB access only when necessary.)
    
    if not is_allowed(user_id) and command not in ("/start", "/help"):
        send_message(user_id, f"❌ Sorry! You are not allowed to use this bot.")
        return jsonify({"ok": True})
        
    if command == "/start":
        # No DB access needed, FAST.
        # ... (send message)
        return jsonify({"ok": True})

    # ... (Other commands)

    if command == "/resume": 
        paused_task = get_running_task_for_user(user_id)
        if not paused_task or paused_task["status"] != "paused": 
            send_message(user_id, "❌ **Sorry!** No paused task found for you to resume.") 
            return jsonify({"ok": True}) 
            
        task_id = paused_task["id"]
        words = paused_task["words"]
        total = paused_task["total_words"]
        current_index = paused_task["current_index"]
        interval = compute_interval(total) 
        
        # 1. Acquire the lock FIRST before setting status to running
        lock = get_user_lock(user_id)
        if not lock.acquire(blocking=False):
            send_message(user_id, "⚠️ Cannot resume: Another task is running or the bot is busy. Try again.")
            return jsonify({"ok": True})

        try:
            mark_task_resumed(task_id) # Set status to running
            send_message(user_id, "▶️ **Welcome Back!** Resuming word transmission now.") 
            
            # 2. Restart the scheduling chain from the current index
            scheduler.add_job(
                send_word_and_schedule_next, 
                "date", 
                run_date=datetime.utcnow() + timedelta(seconds=0.1), 
                kwargs={
                    "task_id": task_id, 
                    "user_id": user_id, 
                    "chat_id": user_id, 
                    "words": words, 
                    "index": current_index, 
                    "interval": interval, 
                    "username": username
                },
                id=f"word_split_{task_id}_{current_index}", 
                replace_existing=True
            )
        except Exception:
            if lock.locked(): lock.release()
            logger.exception("Error resuming task")
            send_message(user_id, "❌ Failed to resume due to an internal error.")
        finally:
            # The lock is now managed by the scheduler chain (send_word_and_schedule_next)
            pass

        return jsonify({"ok": True}) 
        
    if command == "/stop": 
        # Cancel tasks quickly (needs DB access)
        stopped = cancel_active_task_for_user(user_id)
        
        # Release the lock for queue progression (CRITICAL)
        release_user_lock_if_held(user_id)
        
        if stopped > 0:
            send_message(user_id, f"🛑 **Active Task Stopped!**")
        else:
            send_message(user_id, "ℹ️ You had no active or queued tasks.") 
        return jsonify({"ok": True}) 
    
    # ... (Other commands use DB access, resulting in the slowness you feel)
    return jsonify({"ok": True}) 


def handle_new_text(user_id: int, username: str, text: str):
    if not is_allowed(user_id) or is_maintenance_now():
        return jsonify({"ok": True})
    
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        # ... (Handle empty/queue_full)
        return jsonify({"ok": True})

    qsize = get_queue_size(user_id)
    
    if qsize == 1:
        # **CRITICAL FIX:** This is the ONLY task. Start it immediately on a new thread.
        # This thread will acquire the user lock and start the scheduler chain.
        # It bypasses the slow global_worker_loop entirely for the first task.
        threading.Thread(target=_start_next_queued_task_if_exists, args=(user_id, user_id, username), daemon=True).start()
        send_message(user_id, f"🚀 Task queued and **starting now**. Words: {res['total_words']}.")
    else:
        send_message(user_id, f"📝 Queued! You currently have {qsize} task(s) waiting in line.")
        
    return jsonify({"ok": True})

# ... (Health and Root Endpoints remain unchanged)

if __name__ == "__main__":
    try:
        set_webhook()
    except Exception:
        logger.exception("Failed to set webhook at startup")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
