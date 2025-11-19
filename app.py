#!/usr/bin/env python3
"""
High Performance Telegram Word-Splitter Bot (Optimized for Fast SQLite and Telegram API Replies)
- Bulk DB queries per user loop (minimize lock contention)
- Connection pool for DB connections (reduces SQLite-open/close overhead for multi-thread/threaded servers)
- Status-check only at key moments—less chatty DB, more CPU for bot
- Accurate, fast dynamic delay using monotonic clock (always as fast as allowed & possible)
- No switch to Postgres, 100% SQLite
"""

import os
import time
import json
import threading
import queue
import logging
from datetime import datetime, timedelta
from typing import List
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
import requests
import sqlite3

# CONFIG
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bot")
app = Flask(__name__)

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

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None

# ---------- SQLite Connection Pool ----------
class SQLitePool:
    def __init__(self, db_path, size=5):
        self.q = queue.Queue(maxsize=size)
        for _ in range(size):
            conn = sqlite3.connect(db_path, check_same_thread=False, timeout=5)
            self.q.put(conn)
    def acquire(self):
        return self.q.get()
    def release(self, conn):
        self.q.put(conn)
    def closeall(self):
        while not self.q.empty():
            conn = self.q.get()
            try:
                conn.close()
            except Exception:
                pass

_db_pool = SQLitePool(DB_PATH, size=10)
_db_lock = threading.Lock()

def db_execute(query, params=(), fetch=False, many=False):
    conn = _db_pool.acquire()
    try:
        c = conn.cursor()
        if many:
            c.executemany(query, params)
        else:
            c.execute(query, params)
        if fetch:
            rows = c.fetchall()
            return rows
        conn.commit()
    finally:
        _db_pool.release(conn)

def init_db():
    conn = _db_pool.acquire()
    try:
        c = conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS allowed_users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                added_at TEXT,
                is_admin INTEGER DEFAULT 0
              )""")
        c.execute(
            """CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                text TEXT,
                words_json TEXT,
                total_words INTEGER,
                status TEXT,
                created_at TEXT,
                started_at TEXT,
                finished_at TEXT
            )""")
        c.execute(
            """CREATE TABLE IF NOT EXISTS split_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                words INTEGER,
                created_at TEXT
            )""")
        c.execute(
            """CREATE TABLE IF NOT EXISTS sent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                sent_at TEXT,
                deleted INTEGER DEFAULT 0
            )""")
        conn.commit()
    finally:
        _db_pool.release(conn)

init_db()
try:
    res = db_execute("SELECT user_id FROM allowed_users WHERE user_id = ?", (OWNER_ID,), fetch=True)
    if not res:
        db_execute(
            "INSERT INTO allowed_users (user_id, username, added_at, is_admin) VALUES (?, ?, ?, ?)",
            (OWNER_ID, OWNER_USERNAME, datetime.utcnow().isoformat(), 1)
        )
except Exception as e:
    logger.exception("Ensuring owner in allowed_users failed")

# Per-user lock structure for high-concurrency safety
user_locks = {}
user_locks_lock = threading.Lock()
def get_user_lock(user_id):
    with user_locks_lock:
        if user_id not in user_locks:
            user_locks[user_id] = threading.Lock()
        return user_locks[user_id]

# Helpers
def get_now_iso():
    return datetime.utcnow().isoformat()

def split_text_into_words(text: str) -> List[str]:
    return [w for w in text.strip().split() if w]

def compute_interval(total_words: int) -> float:
    # Pushing to absolute lowest value for high speed
    return 0.15 if total_words <= 100 else 0.22 if total_words <= 300 else 0.30 if total_words <= 1000 else 0.40

def is_maintenance_now() -> bool:
    utc_now = datetime.utcnow()
    wat_now = utc_now + timedelta(hours=1)
    h = wat_now.hour
    if MAINTENANCE_START_HOUR_WAT < MAINTENANCE_END_HOUR_WAT:
        return MAINTENANCE_START_HOUR_WAT <= h < MAINTENANCE_END_HOUR_WAT
    return h >= MAINTENANCE_START_HOUR_WAT or h < MAINTENANCE_END_HOUR_WAT

# Telegram API: OUTSIDE of DB lock!
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
            logger.error("Telegram API error for %s: %s", method, data)
        return data
    except Exception as e:
        logger.error("tg_call failed: %s", e)
        return None

def send_message(chat_id: int, text: str, parse_mode: str = "Markdown"):
    # Telegram can randomly choke on exotic Unicode; fallback to utf-8 replace
    try:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        data = tg_call("sendMessage", payload)
        if data and data.get("result"):
            mid = data["result"].get("message_id")
            if mid:
                record_sent_message(chat_id, mid)
            return data["result"]
    except Exception as e:
        logger.warning("Send message failed: %s", e)
    return None

def delete_message(chat_id: int, message_id: int):
    try:
        payload = {"chat_id": chat_id, "message_id": message_id}
        data = tg_call("deleteMessage", payload)
        if data and data.get("ok"):
            mark_message_deleted(chat_id, message_id)
        return data
    except Exception:
        return None

def set_webhook():
    if not TELEGRAM_API or not WEBHOOK_URL:
        logger.warning("Cannot set webhook: TELEGRAM_TOKEN or WEBHOOK_URL not configured")
        return None
    try:
        resp = requests.post(f"{TELEGRAM_API}/setWebhook", json={"url": WEBHOOK_URL}, timeout=REQUESTS_TIMEOUT)
        resp.raise_for_status()
        logger.info("Webhook set response: %s", resp.text)
        return resp.json()
    except Exception:
        logger.exception("Failed to set webhook")
        return None

# Queue logic
def enqueue_task(user_id: int, username: str, text: str) -> dict:
    words = split_text_into_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    q = db_execute(
        "SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status IN ('queued','running','paused')",
        (user_id,), fetch=True
    )
    pending = q[0][0] if q else 0
    if pending >= MAX_QUEUE_PER_USER:
        return {"ok": False, "reason": "queue_full", "queue_size": pending}
    db_execute(
        "INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (user_id, username, text, json.dumps(words), total, "queued", get_now_iso())
    )
    return {"ok": True, "total_words": total, "queue_size": pending + 1}

def get_next_task_for_user(user_id: int):
    rows = db_execute(
        "SELECT id, words_json, total_words, text FROM tasks WHERE user_id = ? AND status = 'queued' ORDER BY id ASC LIMIT 1",
        (user_id,), fetch=True
    )
    if not rows:
        return None
    r = rows[0]
    return {"id": r[0], "words": json.loads(r[1]), "total_words": r[2], "text": r[3]}

def set_task_status(task_id: int, status: str):
    if status == "running":
        db_execute("UPDATE tasks SET status = ?, started_at = ? WHERE id = ?", (status, get_now_iso(), task_id))
    elif status in ("done", "cancelled"):
        db_execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", (status, get_now_iso(), task_id))
    else:
        db_execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))

def mark_task_paused(task_id: int): set_task_status(task_id, "paused")
def mark_task_resumed(task_id: int): set_task_status(task_id, "running")
def mark_task_done(task_id: int): set_task_status(task_id, "done")

def cancel_active_task_for_user(user_id: int):
    rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status IN ('queued','running','paused')", (user_id,), fetch=True)
    ids = [r[0] for r in rows]
    if ids:
        db_execute("UPDATE tasks SET status = 'cancelled' WHERE id IN (%s)" % ','.join(['?']*len(ids)), ids)
    return len(ids)

def record_split_log(user_id: int, username: str, words: int):
    db_execute("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (?, ?, ?, ?)",
               (user_id, username, words, get_now_iso()))

def record_sent_message(chat_id: int, message_id: int):
    db_execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)",
               (chat_id, message_id, get_now_iso()))

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

def is_allowed(user_id: int) -> bool:
    return bool(db_execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True))

def is_admin(user_id: int) -> bool:
    rows = db_execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,), fetch=True)
    return bool(rows and rows[0][0])

# ------ OPTIMIZED PER-USER TASK WORKER ------
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
            interval = compute_interval(total)
            est_seconds = interval * total
            est_str = str(timedelta(seconds=int(est_seconds)))
            send_message(chat_id, f"🚀 Task Starting! Words: {total}. Time/word: {interval}s\n⏳ ETA: {est_str}")
            qcount = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)[0][0]

            i, next_send_time = 0, time.monotonic()
            status = "running"
            while i < total:
                # Batch-check status: every 10 words or stalled loop
                if i == 0 or i % 10 == 0:
                    srow = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
                    if srow: status = srow[0][0]
                if status == "paused":
                    send_message(chat_id, "⏸️ Task Paused! Waiting on /resume.")
                    # Paused: only DB poll, skip messaging
                    while True:
                        time.sleep(1)
                        srow = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
                        if srow and srow[0][0] == "running":
                            send_message(chat_id, "▶️ **Welcome Back!** Resuming word transmission.")
                            status = "running"
                            break
                        if not srow or srow[0][0] == "cancelled":
                            status = "cancelled"
                            break
                    if status == "cancelled":
                        break
                if status == "cancelled":
                    break
                now = time.monotonic()
                wait = next_send_time - now
                if wait > 0:
                    time.sleep(wait)
                send_message(chat_id, words[i])
                i += 1
                next_send_time += interval
            # Check final task status
            srow = db_execute("SELECT status FROM tasks WHERE id = ?", (task_id,), fetch=True)
            fstatus = srow[0][0] if srow else "done"
            if fstatus != "cancelled":
                mark_task_done(task_id)
                record_split_log(user_id, username, total)
                send_message(chat_id, "✅ **All Done!** Split finished successfully.")
            else:
                send_message(chat_id, "🛑 **Active Task Stopped!** The current word transmission was terminated.")
    finally:
        lock.release()

def global_worker_loop():
    processed = set()
    while not _worker_stop.is_set():
        try:
            # Only one thread per user ever
            rows = db_execute("SELECT DISTINCT user_id, username FROM tasks WHERE status = 'queued'", fetch=True)
            for r in rows:
                user_id, username = r[0], r[1] or ""
                if user_id not in processed:
                    t = threading.Thread(target=process_user_queue, args=(user_id, user_id, username), daemon=True)
                    t.start()
                    processed.add(user_id)
            time.sleep(1)
        except Exception:
            traceback.print_exc()
            time.sleep(2)

# ========== SCHEDULER ==========
scheduler = BackgroundScheduler()

def hourly_owner_stats():
    cutoff = datetime.utcnow() - timedelta(hours=1)
    rows = db_execute("SELECT user_id, username, SUM(words) FROM split_logs WHERE created_at >= ? GROUP BY user_id", (cutoff.isoformat(),), fetch=True)
    if not rows:
        send_message(OWNER_ID, "🕐 No splits in last 1 hour.")
        return
    total_words = sum(int(r[2] or 0) for r in rows)
    msg = "🕐 Hour Activity\n" + '\n'.join(f"{r[0]}: {r[2]} words" for r in rows) + f"\n\nTotal: {total_words} words"
    send_message(OWNER_ID, msg)

def delete_old_bot_messages():
    msgs = get_messages_older_than(days=1)
    if msgs:
        db_execute("UPDATE sent_messages SET deleted=1 WHERE id IN (%s)" % ','.join(['?']*len(msgs)), [m['message_id'] for m in msgs], many=True)

def maintenance_hourly_health():
    send_message(OWNER_ID, f"👑 Health OK {get_now_iso()}")

scheduler.add_job(hourly_owner_stats, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=10))
scheduler.add_job(maintenance_hourly_health, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=15))
scheduler.add_job(delete_old_bot_messages, "interval", minutes=30, next_run_time=datetime.utcnow() + timedelta(seconds=20))
scheduler.start()

_worker_thread = threading.Thread(target=global_worker_loop, daemon=True)
_worker_thread.start()

# ---------- Webhook & Flask ----------
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json(force=True)
        if "message" in update:
            msg = update["message"]
            user = msg.get("from", {})
            user_id = user.get("id")
            username = user.get("username") or user.get("first_name") or ""
            text = msg.get("text") or ""
            if text.startswith("/"):
                command, args = (text.split(None, 1) + [""])[:2]
                command = command.split("@")[0].lower()
                return handle_command(user_id, username, command, args)
            else:
                return handle_new_text(user_id, username, text)
    except Exception:
        logger.exception("Webhook error")
    return jsonify({"ok": True})

def get_queue_size(user_id):
    q = db_execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,), fetch=True)
    return q[0][0] if q else 0

def handle_command(user_id: int, username: str, command: str, args: str):
    if not is_allowed(user_id) and command not in ("/start", "/help"):
        send_message(user_id, "❌ **Sorry!** You are not allowed to use this bot. A request has been sent to the owner.")
        send_message(OWNER_ID, f"⚠️ Unallowed access by {username or user_id} ({user_id}): {args or '[no text]'}")
        return jsonify({"ok": True})
    if command == "/start":
        send_message(user_id, f"👋 Welcome @{username or user_id}!\nSend any text to split it into messages.\n/start /pause /resume /status /stop /about")
        return jsonify({"ok": True})
    if command == "/example":
        sample = "This is a demo split"
        send_message(user_id, "Running example split...")
        enqueue_task(user_id, username, sample)
        send_message(user_id, f"📝 **Queued!** You have {get_queue_size(user_id)} task(s) in queue.")
        return jsonify({"ok": True})
    if command == "/pause":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'running' LIMIT 1", (user_id,), fetch=True)
        if not rows: send_message(user_id, "❌ No active task to pause."); return jsonify({"ok": True})
        mark_task_paused(rows[0][0]); send_message(user_id, "⏸️ Paused!"); return jsonify({"ok": True})
    if command == "/resume":
        rows = db_execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'paused' LIMIT 1", (user_id,), fetch=True)
        if not rows: send_message(user_id, "❌ No paused task to resume."); return jsonify({"ok": True})
        mark_task_resumed(rows[0][0]); send_message(user_id, "▶️ Resumed!"); return jsonify({"ok": True})
    if command == "/stop":
        num = cancel_active_task_for_user(user_id)
        send_message(user_id, "🛑 All tasks stopped!")
        return jsonify({"ok": True})
    if command == "/status":
        rows = db_execute("SELECT id, status, total_words FROM tasks WHERE user_id = ? AND status IN ('running','paused')", (user_id,), fetch=True)
        queued = get_queue_size(user_id)
        inf = rows[0] if rows else None
        if inf:
            send_message(user_id, f"📊 Status: {inf[1]}, words: {inf[2]}, queue: {queued}")
        else:
            if queued:
                send_message(user_id, f"📝 Queue: {queued} tasks.")
            else:
                send_message(user_id, "📊 No active/queued tasks.")
        return jsonify({"ok": True})
    if command == "/about":
        send_message(user_id, "Word Splitter Bot. Fast, optimized, dynamic delay etc.")
        return jsonify({"ok": True})
    send_message(user_id, f"Unknown command: {command}")
    return jsonify({"ok": True})

def handle_new_text(user_id, username, text):
    if not is_allowed(user_id):
        send_message(user_id, "❌ Not allowed. Ask owner for access.")
        send_message(OWNER_ID, f"Attempt by @{username or user_id}")
        return jsonify({"ok": True})
    if is_maintenance_now():
        send_message(user_id, "🛠️ Maintenance ongoing.")
        return jsonify({"ok": True})
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        if res.get("reason") == "empty":
            send_message(user_id, "Can't split empty text.")
        elif res.get("reason") == "queue_full":
            send_message(user_id, "❌ Your queue is full. Use /stop to clear.")
        return jsonify({"ok": True})
    qsize = get_queue_size(user_id)
    send_message(user_id, f"📝 Task queued. You have {qsize} queued tasks.")
    return jsonify({"ok": True})

@app.route("/", methods=["GET", "POST"])
def root_forward():
    if request.method == "POST":
        return webhook()
    return "Word Splitter Bot running.", 200

@app.route("/health", methods=["GET", "HEAD"])
@app.route("/health/", methods=["GET", "HEAD"])
def health():
    return jsonify({"ok": True, "time": datetime.utcnow().isoformat()}), 200

@app.route("/debug/routes", methods=["GET"])
def debug_routes():
    try:
        rules = sorted(str(r) for r in app.url_map.iter_rules())
        return jsonify({"routes": rules}), 200
    except Exception:
        return jsonify({"ok": False, "error": "failed to list routes"}), 500

if __name__ == "__main__":
    try:
        set_webhook()
    except Exception:
        logger.exception("Webhook at startup failed")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
