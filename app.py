#!/usr/bin/env python3
"""
WordSplitter Telegram Bot (enhanced botinfo & hourly reports)

This is the running bot script with:
- Plain text messages (no parse_mode).
- Numeric IDs are sent as monospace (code) via `entities` so they are copyable.
- Owners always allowed; allowed_users row ensured on incoming messages.
- Increased emoji usage and much richer /botinfo output and hourly owner reports.
"""

import os
import time
import json
import sqlite3
import threading
import logging
import re
from datetime import datetime, timedelta
from typing import List, Dict
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
import requests

# Startup timestamp for uptime calculation
START_TS = datetime.utcnow()

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wordsplitter")

app = Flask(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
OWNER_IDS_RAW = os.environ.get("OWNER_IDS", "")      # comma/space separated IDs
ALLOWED_USERS_RAW = os.environ.get("ALLOWED_USERS", "")  # comma/space separated IDs
DB_PATH = os.environ.get("DB_PATH", "botdata.sqlite3")
MAX_QUEUE_PER_USER = int(os.environ.get("MAX_QUEUE_PER_USER", "5"))
MAX_MSG_PER_SECOND = float(os.environ.get("MAX_MSG_PER_SECOND", "50"))
REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None
_session = requests.Session()

def parse_id_list(raw: str) -> List[int]:
    if not raw:
        return []
    parts = re.split(r"[,\s]+", raw.strip())
    ids = []
    for p in parts:
        if not p:
            continue
        try:
            ids.append(int(p))
        except Exception:
            continue
    return ids

OWNER_IDS = parse_id_list(OWNER_IDS_RAW)
PRIMARY_OWNER = OWNER_IDS[0] if OWNER_IDS else None
ALLOWED_USERS = parse_id_list(ALLOWED_USERS_RAW)

NIGERIA_TZ_OFFSET = timedelta(hours=1)
def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
def utc_to_wat_ts(utc_ts: str) -> str:
    try:
        utc_dt = datetime.strptime(utc_ts, "%Y-%m-%d %H:%M:%S")
        wat_dt = utc_dt + NIGERIA_TZ_OFFSET
        return wat_dt.strftime("%Y-%m-%d %H:%M:%S WAT")
    except Exception:
        return f"{utc_ts} (UTC error)"

def format_uptime(start_ts: datetime) -> str:
    delta = datetime.utcnow() - start_ts
    days = delta.days
    hours, rem = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)

_is_maintenance = False
_maintenance_lock = threading.Lock()
def is_maintenance_time() -> bool:
    with _maintenance_lock:
        return _is_maintenance

_db_lock = threading.Lock()
def _ensure_db_parent(dirpath: str):
    try:
        if dirpath and not os.path.exists(dirpath):
            os.makedirs(dirpath, exist_ok=True)
    except Exception as e:
        logger.warning("Could not create DB parent directory %s: %s", dirpath, e)

def init_db():
    global DB_PATH
    parent = os.path.dirname(os.path.abspath(DB_PATH))
    if parent:
        _ensure_db_parent(parent)
    try:
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("""
            CREATE TABLE IF NOT EXISTS allowed_users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                added_at TEXT
            )""")
            c.execute("""
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
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS split_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                words INTEGER,
                created_at TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS sent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                sent_at TEXT,
                deleted INTEGER DEFAULT 0
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS suspended_users (
                user_id INTEGER PRIMARY KEY,
                suspended_until TEXT,
                reason TEXT,
                added_at TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS send_failures (
                user_id INTEGER PRIMARY KEY,
                failures INTEGER,
                last_failure_at TEXT
            )""")
            conn.commit()
        logger.info("DB initialized at %s", DB_PATH)
    except sqlite3.OperationalError:
        logger.exception("Failed to open DB at %s, falling back to in-memory DB", DB_PATH)
        DB_PATH = ":memory:"
        try:
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("""
                CREATE TABLE IF NOT EXISTS allowed_users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    added_at TEXT
                )""")
                c.execute("""
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
                )""")
                c.execute("""
                CREATE TABLE IF NOT EXISTS split_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    username TEXT,
                    words INTEGER,
                    created_at TEXT
                )""")
                c.execute("""
                CREATE TABLE IF NOT EXISTS sent_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    message_id INTEGER,
                    sent_at TEXT,
                    deleted INTEGER DEFAULT 0
                )""")
                c.execute("""
                CREATE TABLE IF NOT EXISTS suspended_users (
                    user_id INTEGER PRIMARY KEY,
                    suspended_until TEXT,
                    reason TEXT,
                    added_at TEXT
                )""")
                c.execute("""
                CREATE TABLE IF NOT EXISTS send_failures (
                    user_id INTEGER PRIMARY KEY,
                    failures INTEGER,
                    last_failure_at TEXT
                )""")
                conn.commit()
            logger.info("In-memory DB initialized")
        except Exception:
            logger.exception("Failed to initialize in-memory DB; DB operations may fail")
init_db()

# Ensure owners auto-added as allowed (never suspended)
for oid in OWNER_IDS:
    try:
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (oid,))
            exists = c.fetchone()
        if not exists:
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("INSERT OR REPLACE INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)", (oid, "", now_ts()))
                conn.commit()
    except Exception:
        logger.exception("Error ensuring owner in allowed_users")

# Ensure all ALLOWED_USERS auto-added as allowed at startup (skip owners)
for uid in ALLOWED_USERS:
    if uid in OWNER_IDS:
        continue
    try:
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (uid,))
            rows = c.fetchone()
        if not rows:
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("INSERT INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)",
                          (uid, "", now_ts()))
                conn.commit()
            try:
                if TELEGRAM_API:
                    _session.post(f"{TELEGRAM_API}/sendMessage", json={
                        "chat_id": uid, "text": "✅ You have been added. Send any text to start."
                    }, timeout=3)
            except Exception:
                pass
    except Exception:
        logger.exception("Auto-add allowed user error")

_token_bucket = {"tokens": MAX_MSG_PER_SECOND, "last": time.time(), "capacity": max(1.0, MAX_MSG_PER_SECOND), "lock": threading.Lock()}
def acquire_token(timeout=10.0):
    start = time.time()
    while True:
        with _token_bucket["lock"]:
            now = time.time()
            elapsed = now - _token_bucket["last"]
            if elapsed > 0:
                refill = elapsed * MAX_MSG_PER_SECOND
                _token_bucket["tokens"] = min(_token_bucket["capacity"], _token_bucket["tokens"] + refill)
                _token_bucket["last"] = now
            if _token_bucket["tokens"] >= 1:
                _token_bucket["tokens"] -= 1
                return True
        if time.time() - start >= timeout:
            return False
        time.sleep(0.02)

def parse_telegram_json(resp):
    try:
        return resp.json()
    except Exception:
        return None

def _build_code_entities_for_numbers(text: str):
    """
    Find plain numeric tokens (sequences of digits) and return a list of
    message entity dicts marking them as type 'code'. Offsets/lengths use
    UTF-16 code units as required by Telegram; for ASCII digits this is
    equivalent to Python string indices, so this is safe here.
    """
    entities = []
    for m in re.finditer(r"\b\d+\b", text):
        start = m.start()
        length = m.end() - m.start()
        entities.append({"type": "code", "offset": start, "length": length})
    return entities if entities else None

def increment_failure(user_id: int):
    try:
        # If owner failing, log but avoid notifying owners or cancelling their tasks to prevent recursion/loops.
        if user_id in OWNER_IDS:
            logger.warning("Send failure for owner %s; recording but not notifying owners to avoid recursion.", user_id)
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("SELECT failures FROM send_failures WHERE user_id = ?", (user_id,))
                row = c.fetchone()
                if not row:
                    c.execute("INSERT INTO send_failures (user_id, failures, last_failure_at) VALUES (?, ?, ?)",
                              (user_id, 1, now_ts()))
                else:
                    failures = int(row[0] or 0) + 1
                    c.execute("UPDATE send_failures SET failures = ?, last_failure_at = ? WHERE user_id = ?",
                              (failures, now_ts(), user_id))
                conn.commit()
            return

        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT failures FROM send_failures WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            if not row:
                c.execute("INSERT INTO send_failures (user_id, failures, last_failure_at) VALUES (?, ?, ?)",(user_id, 1, now_ts()))
                failures = 1
            else:
                failures = int(row[0] or 0) + 1
                c.execute("UPDATE send_failures SET failures = ?, last_failure_at = ? WHERE user_id = ?",(failures, now_ts(), user_id))
            conn.commit()
        if failures >= 6:
            notify_owners(f"⚠️ Repeated send failures for {user_id} ({failures}). Stopping their tasks. 🛑")
            cancel_active_task_for_user(user_id)
    except Exception:
        logger.exception("increment_failure error")

def reset_failures(user_id: int):
    try:
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("DELETE FROM send_failures WHERE user_id = ?", (user_id,))
            conn.commit()
    except Exception:
        pass

def send_message(chat_id: int, text: str):
    """
    Send plain text (no parse_mode). Numeric IDs inside the text are sent
    as monospace (code) via the 'entities' parameter so they are copyable.
    Emojis are included inline in text.
    """
    if not TELEGRAM_API:
        logger.error("No TELEGRAM_TOKEN; cannot send message.")
        return None
    acquire_token(timeout=5.0)
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    entities = _build_code_entities_for_numbers(text)
    if entities:
        payload["entities"] = entities
    try:
        resp = _session.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUESTS_TIMEOUT)
    except Exception:
        logger.exception("Network send error")
        increment_failure(chat_id)
        return None
    data = parse_telegram_json(resp)
    if data and data.get("ok"):
        try:
            mid = data["result"].get("message_id")
            if mid:
                with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                    c = conn.cursor()
                    c.execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)",(chat_id, mid, now_ts()))
                    conn.commit()
        except Exception:
            logger.exception("record sent message failed")
        reset_failures(chat_id)
        return data["result"]
    else:
        increment_failure(chat_id)
        return None

def broadcast_send_raw(chat_id: int, text: str):
    """
    Send a plain broadcast message; numeric IDs will be marked monospace (code).
    """
    if not TELEGRAM_API:
        return False, "no_token"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    entities = _build_code_entities_for_numbers(text)
    if entities:
        payload["entities"] = entities
    try:
        resp = _session.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUESTS_TIMEOUT)
    except Exception as e:
        logger.info("Broadcast network error to %s: %s", chat_id, e)
        return False, str(e)
    data = parse_telegram_json(resp)
    if data and data.get("ok"):
        try:
            mid = data["result"].get("message_id")
            if mid:
                with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                    c = conn.cursor()
                    c.execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)",
                              (chat_id, mid, now_ts()))
                    conn.commit()
        except Exception:
            pass
        return True, "ok"
    reason = data.get("description") if isinstance(data, dict) else "error"
    logger.info("Broadcast failed to %s: %s", chat_id, reason)
    return False, reason

def broadcast_to_all_allowed(text: str):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id FROM allowed_users")
        rows = c.fetchall()
    for r in rows:
        tid = r[0]
        if not is_suspended(tid):
            broadcast_send_raw(tid, text)

def cancel_all_tasks():
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE status IN ('queued','running','paused')", ("cancelled", now_ts()))
        conn.commit()
        count = c.rowcount
    return count

def start_maintenance():
    global _is_maintenance
    with _maintenance_lock:
        _is_maintenance = True
    stopped = cancel_all_tasks()
    broadcast_to_all_allowed("🛠️ Scheduled maintenance started (2:00 AM–3:00 AM). Tasks are temporarily blocked. Please try later. ⏳")
    notify_owners("🛠️ Automatic maintenance started. Bot tasks were blocked. 🔔")
    logger.info("Maintenance started. All tasks cancelled: %s", stopped)

def end_maintenance():
    global _is_maintenance
    with _maintenance_lock:
        _is_maintenance = False
    broadcast_to_all_allowed("🟢 Scheduled maintenance ended. Bot is now available for tasks. ✅")
    notify_owners("🟢 Automatic maintenance ended. Bot resumed. 👍")
    logger.info("Maintenance ended.")

def split_text_to_words(text: str) -> List[str]:
    return [w for w in text.strip().split() if w]

def enqueue_task(user_id: int, username: str, text: str):
    if is_maintenance_time():
        return {"ok": False, "reason": "maintenance"}
    words = split_text_to_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        pending = c.fetchone()[0]
        if pending >= MAX_QUEUE_PER_USER:
            return {"ok": False, "reason": "queue_full", "queue_size": pending}
    try:
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at, sent_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                      (user_id, username, text, json.dumps(words), total, "queued", now_ts(), 0))
            conn.commit()
    except Exception:
        logger.exception("enqueue_task db error")
        return {"ok": False, "reason": "db_error"}
    return {"ok": True, "total_words": total, "queue_size": pending + 1}

def get_next_task_for_user(user_id: int):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT id, words_json, total_words, text FROM tasks WHERE user_id = ? AND status = 'queued' ORDER BY id ASC LIMIT 1", (user_id,))
        r = c.fetchone()
    if not r:
        return None
    return {"id": r[0], "words": json.loads(r[1]) if r[1] else split_text_to_words(r[3]), "total_words": r[2], "text": r[3]}

def set_task_status(task_id: int, status: str):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        if status == "running":
            c.execute("UPDATE tasks SET status = ?, started_at = ? WHERE id = ?", (status, now_ts(), task_id))
        elif status in ("done", "cancelled"):
            c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", (status, now_ts(), task_id))
        else:
            c.execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))
        conn.commit()

def cancel_active_task_for_user(user_id: int):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM tasks WHERE user_id = ? AND status IN ('queued','running','paused')", (user_id,))
        rows = c.fetchall()
        count = 0
        for r in rows:
            tid = r[0]
            c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", ("cancelled", now_ts(), tid))
            count += 1
        conn.commit()
    notify_user_worker(user_id)
    return count

def record_split_log(user_id: int, username: str, count: int = 1):
    try:
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            # Save one record for each word sent, with username at send time
            for _ in range(count):
                c.execute("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (?, ?, ?, ?)", (user_id, username, 1, now_ts()))
            conn.commit()
    except Exception:
        logger.exception("record_split_log error")

def is_allowed(user_id: int) -> bool:
    # Owners are always allowed
    if user_id in OWNER_IDS:
        return True
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,))
        return bool(c.fetchone())

def suspend_user(target_id: int, seconds: int, reason: str = ""):
    until_utc_str = (datetime.utcnow() + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
    until_wat_str = utc_to_wat_ts(until_utc_str)
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO suspended_users (user_id, suspended_until, reason, added_at) VALUES (?, ?, ?, ?)",
                  (target_id, until_utc_str, reason, now_ts()))
        conn.commit()
    stopped = cancel_active_task_for_user(target_id)
    try:
        send_message(target_id, f"⛔ You have been suspended until {until_wat_str} by Owner ({PRIMARY_OWNER}).\nUser ID: {target_id} 🔒")
    except Exception:
        logger.exception("notify suspended user failed")
    notify_owners(f"⛔ User suspended: {target_id} suspended_until={until_wat_str} by={PRIMARY_OWNER} 👑")

def unsuspend_user(target_id: int) -> bool:
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (target_id,))
        r = c.fetchone()
        if not r:
            return False
        c.execute("DELETE FROM suspended_users WHERE user_id = ?", (target_id,))
        conn.commit()
    try:
        send_message(target_id, f"✅ You have been unsuspended by Owner ({PRIMARY_OWNER}).\nUser ID: {target_id} 🙌")
    except Exception:
        logger.exception("notify unsuspended failed")
    notify_owners(f"✅ Manual unsuspend: {target_id} by {PRIMARY_OWNER}.")
    return True

def list_suspended():
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, suspended_until, reason, added_at FROM suspended_users ORDER BY suspended_until ASC")
        return c.fetchall()

def is_suspended(user_id: int) -> bool:
    if user_id in OWNER_IDS:
        return False
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        if not r:
            return False
        try:
            until = datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S")
            return until > datetime.utcnow()
        except Exception:
            return False

def notify_owners(text: str):
    for oid in OWNER_IDS:
        try:
            send_message(oid, f"🔔 {text} 👑")
        except Exception:
            logger.exception("notify owner failed for %s", oid)

_user_workers_lock = threading.Lock()
_user_workers: Dict[int, Dict[str, object]] = {}

def notify_user_worker(user_id: int):
    with _user_workers_lock:
        info = _user_workers.get(user_id)
        if info and "wake" in info:
            try:
                info["wake"].set()
            except Exception:
                pass

def start_user_worker_if_needed(user_id: int):
    with _user_workers_lock:
        info = _user_workers.get(user_id)
        if info:
            thr = info.get("thread")
            if thr and thr.is_alive():
                return
        wake = threading.Event()
        stop = threading.Event()
        thr = threading.Thread(target=per_user_worker_loop, args=(user_id, wake, stop), daemon=True)
        _user_workers[user_id] = {"thread": thr, "wake": wake, "stop": stop}
        thr.start()
        logger.info("Started worker for user %s", user_id)

def stop_user_worker(user_id: int, join_timeout: float = 0.5):
    with _user_workers_lock:
        info = _user_workers.get(user_id)
        if not info:
            return
        try:
            info["stop"].set()
            info["wake"].set()
            thr = info.get("thread")
            if thr and thr.is_alive():
                thr.join(join_timeout)
        except Exception:
            logger.exception("Error stopping worker for %s", user_id)
        finally:
            _user_workers.pop(user_id, None)
            logger.info("Stopped worker for user %s", user_id)

def per_user_worker_loop(user_id: int, wake_event: threading.Event, stop_event: threading.Event):
    logger.info("Worker loop starting for user %s", user_id)
    try:
        while not stop_event.is_set():
            if is_maintenance_time():
                cancel_active_task_for_user(user_id)
                while is_maintenance_time() and not stop_event.is_set():
                    wake_event.wait(timeout=3.0)
                    wake_event.clear()
                continue
            if is_suspended(user_id):
                cancel_active_task_for_user(user_id)
                try:
                    send_message(user_id, f"⛔ You are suspended; stopping your task.\nUser ID: {user_id} 🔒")
                except Exception:
                    pass
                while is_suspended(user_id) and not stop_event.is_set():
                    wake_event.wait(timeout=5.0)
                    wake_event.clear()
                continue
            task = get_next_task_for_user(user_id)
            if not task:
                wake_event.wait(timeout=1.0)
                wake_event.clear()
                continue
            task_id = task["id"]
            words = task["words"]
            total = int(task["total_words"] or len(words))
            set_task_status(task_id, "running")
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("SELECT sent_count FROM tasks WHERE id = ?", (task_id,))
                sent_info = c.fetchone()
            sent = int(sent_info[0] or 0) if sent_info else 0
            interval = 0.4 if total <= 150 else (0.5 if total <= 300 else 0.6)
            est_seconds = int((total - sent) * interval)
            est_str = str(timedelta(seconds=est_seconds))
            try:
                send_message(user_id, f"🚀 Starting your split now. Words: {total}. Estimated time: {est_str} ✨\nTask ID: {task_id}")
            except Exception:
                pass
            i = sent
            while i < total and not stop_event.is_set():
                with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                    c = conn.cursor()
                    c.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                    row = c.fetchone()
                if not row:
                    break
                status = row[0]
                if status == "cancelled":
                    break
                if is_suspended(user_id):
                    try:
                        send_message(user_id, f"⛔ You are suspended; stopping your task.\nUser ID: {user_id} 🔒")
                    except Exception:
                        pass
                    set_task_status(task_id, "cancelled")
                    break
                if is_maintenance_time():
                    try:
                        send_message(user_id, f"🛠️ Your task stopped due to scheduled maintenance. Please try later. ⏳\nTask ID: {task_id}")
                    except Exception:
                        pass
                    set_task_status(task_id, "cancelled")
                    break
                if status == "paused":
                    try:
                        send_message(user_id, f"⏸️ Task paused… Hold tight! 🙏\nTask ID: {task_id}")
                    except Exception:
                        pass
                    while True:
                        wake_event.wait(timeout=0.7)
                        wake_event.clear()
                        if stop_event.is_set():
                            break
                        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn_check:
                            c_check = conn_check.cursor()
                            c_check.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                            row2 = c_check.fetchone()
                        if not row2:
                            break
                        new_status = row2[0]
                        if new_status == "cancelled" or is_suspended(user_id) or is_maintenance_time():
                            break
                        if new_status == "running":
                            try:
                                send_message(user_id, f"▶️ Resuming your task now. Let's go! 🚀\nTask ID: {task_id}")
                            except Exception:
                                pass
                            break
                    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                        c = conn.cursor()
                        c.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                        rr = c.fetchone()
                    if not rr or rr[0] in ("cancelled", "paused"):
                        break
                # Send word, record split log per word (for stats)
                uname_for_stat = ""
                with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                    c = conn.cursor()
                    c.execute("SELECT username FROM allowed_users WHERE user_id = ?", (user_id,))
                    r = c.fetchone()
                uname_for_stat = r[0] if r and r[0] else ""
                try:
                    send_message(user_id, words[i])
                    record_split_log(user_id, uname_for_stat or str(user_id), 1)
                except Exception:
                    # Even if send failed, record a log entry to keep username history consistent.
                    record_split_log(user_id, uname_for_stat or str(user_id), 1)
                i += 1
                try:
                    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                        c = conn.cursor()
                        c.execute("UPDATE tasks SET sent_count = ? WHERE id = ?", (i, task_id))
                        conn.commit()
                except Exception:
                    logger.exception("Failed to update sent_count for task %s", task_id)
                sleeper = 0.0
                target = interval
                start_sleep = time.time()
                while sleeper < target and not stop_event.is_set():
                    now = time.time()
                    remaining = max(0, target - sleeper)
                    time.sleep(min(0.07, remaining))
                    if wake_event.is_set():
                        wake_event.clear()
                        break
                    sleeper = now - start_sleep
                if is_maintenance_time() or is_suspended(user_id):
                    break
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("SELECT status, sent_count FROM tasks WHERE id = ?", (task_id,))
                r = c.fetchone()
            final_status = r[0] if r else "done"
            sent_final = int(r[1] or 0) if r and r[1] is not None else i
            if final_status not in ("cancelled", "paused"):
                set_task_status(task_id, "done")
                try:
                    send_message(user_id, f"✅ All done! 🎉\nTask ID: {task_id} 🏁")
                except Exception:
                    pass
            elif final_status == "cancelled":
                try:
                    send_message(user_id, f"🛑 Task stopped. If you need help, contact Owner. 🆘\nTask ID: {task_id}")
                except Exception:
                    pass
    except Exception:
        logger.exception("Worker error for user %s", user_id)
    finally:
        with _user_workers_lock:
            _user_workers.pop(user_id, None)
        logger.info("Worker loop exiting for user %s", user_id)

def fetch_display_username(user_id: int):
    # Always show most recent username for user if available, from logs or allowed_users.
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT username FROM split_logs WHERE user_id = ? ORDER BY created_at DESC LIMIT 1", (user_id,))
        r = c.fetchone()
        if r and r[0]:
            return r[0]
        c.execute("SELECT username FROM allowed_users WHERE user_id = ?", (user_id,))
        r2 = c.fetchone()
        if r2 and r2[0]:
            return r2[0]
    return ""

def compute_last_hour_stats():
    cutoff = datetime.utcnow() - timedelta(hours=1)
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("""
            SELECT user_id, username, COUNT(*) as s
            FROM split_logs
            WHERE created_at >= ?
            GROUP BY user_id, username
            ORDER BY s DESC
        """, (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))
        rows = c.fetchall()
    # Collapse multiple usernames to latest per user
    stat_map = {}
    for uid, uname, s in rows:
        stat_map[uid] = {"uname": uname, "words": stat_map.get(uid,{}).get("words",0)+int(s)}
    return [(k, v["uname"], v["words"]) for k, v in stat_map.items()]

def compute_last_12h_stats(user_id: int):
    cutoff = datetime.utcnow() - timedelta(hours=12)
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("""
            SELECT COUNT(*) FROM split_logs WHERE user_id = ? AND created_at >= ?
        """, (user_id, cutoff.strftime("%Y-%m-%d %H:%M:%S")))
        r = c.fetchone()
        return int(r[0] or 0)

def send_hourly_owner_stats():
    """
    Enhanced hourly report for owners:
    - Top senders in last hour
    - Total words sent in last hour
    - Active / queued tasks, suspended users, failure summary
    - Uptime and worker info
    """
    cutoff = datetime.utcnow() - timedelta(hours=1)
    rows = compute_last_hour_stats()  # list of (uid, uname, words)
    total_words = sum(r[2] for r in rows) if rows else 0
    top_lines = []
    for i, (uid, uname, w) in enumerate(rows[:10], start=1):
        uname_disp = uname or fetch_display_username(uid) or "(no username)"
        top_lines.append(f"{i}. {uid} ({uname_disp}) — {w} words")
    # gather other stats
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        # active tasks count
        c.execute("SELECT COUNT(*) FROM tasks WHERE status IN ('running','paused')")
        active_tasks = c.fetchone()[0] or 0
        # queued tasks count
        c.execute("SELECT COUNT(*) FROM tasks WHERE status = 'queued'")
        queued_tasks = c.fetchone()[0] or 0
        # per-user queued counts (top 8)
        c.execute("SELECT user_id, COUNT(*) as q FROM tasks WHERE status = 'queued' GROUP BY user_id ORDER BY q DESC LIMIT 8")
        qrows = c.fetchall()
        queued_user_lines = []
        for uid_q, qcount in qrows:
            uname_q = fetch_display_username(uid_q) or ""
            queued_user_lines.append(f"{uid_q} ({uname_q}) — {qcount}")
        # suspended count
        c.execute("SELECT COUNT(*) FROM suspended_users")
        suspended_count = c.fetchone()[0] or 0
        # failures summary
        c.execute("SELECT user_id, failures, last_failure_at FROM send_failures ORDER BY failures DESC LIMIT 8")
        fail_rows = c.fetchall()
        failure_lines = []
        total_failures = 0
        for fr in fail_rows:
            failure_lines.append(f"{fr[0]} — {fr[1]} failures (last: {fr[2]})")
            total_failures += int(fr[1] or 0)
        # tasks status breakdown
        c.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
        status_rows = c.fetchall()
        status_map = {r[0]: r[1] for r in status_rows}
        # tasks finished in last hour
        c.execute("SELECT COUNT(*), COALESCE(SUM(total_words),0) FROM tasks WHERE finished_at >= ? AND status = 'done'", (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))
        done_count_last_hour, done_words_last_hour = c.fetchone()
        done_count_last_hour = done_count_last_hour or 0
        done_words_last_hour = done_words_last_hour or 0

    # uptime and worker count
    uptime = format_uptime(START_TS)
    worker_count = len(_user_workers)

    # Build rich report
    lines = []
    lines.append("🕰️ HOURLY REPORT — last 1 hour")
    lines.append(f"⏱️ Uptime: {uptime}")
    lines.append(f"👥 Owners: {len(OWNER_IDS)} | Allowed users (DB): {count_allowed_users():,} | Suspended: {suspended_count}")
    lines.append(f"⚙️ Workers running: {worker_count} | Active tasks: {active_tasks} | Queued tasks: {queued_tasks}")
    lines.append(f"📊 Words sent (last 1h): {total_words}")
    lines.append(f"✅ Tasks done (last 1h): {done_count_last_hour} | Words in completed tasks: {done_words_last_hour}")
    lines.append("")
    lines.append("🔥 Top senders (by words) — last 1h:")
    if top_lines:
        lines.extend(top_lines)
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append("📝 Top queued users:")
    if queued_user_lines:
        lines.extend(queued_user_lines)
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append("⚠️ Send failures summary:")
    if failure_lines:
        lines.extend(failure_lines)
        lines.append(f"Total recorded failures (top rows): {total_failures}")
    else:
        lines.append("  (no recent failures)")
    lines.append("")
    lines.append("🔧 Task status breakdown:")
    for sname in ("running","paused","queued","done","cancelled"):
        lines.append(f"  {sname}: {status_map.get(sname,0)}")
    lines.append("")
    lines.append("📌 Notes: Use /botinfo for more details. ❤️")

    body = "\n".join(lines)
    for oid in OWNER_IDS:
        try:
            send_message(oid, body)
        except Exception:
            pass

def count_allowed_users() -> int:
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM allowed_users")
        return int(c.fetchone()[0] or 0)

def get_tasks_status_counts():
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
        rows = c.fetchall()
    return {r[0]: r[1] for r in rows}

def get_top_users_by_queued(limit=8):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, COUNT(*) as q FROM tasks WHERE status = 'queued' GROUP BY user_id ORDER BY q DESC LIMIT ?", (limit,))
        rows = c.fetchall()
    return rows

def get_send_failures_summary(limit=8):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, failures, last_failure_at FROM send_failures ORDER BY failures DESC LIMIT ?", (limit,))
        return c.fetchall()

# Schedule jobs
scheduler = BackgroundScheduler()
scheduler.add_job(send_hourly_owner_stats, "interval", hours=1, next_run_time=datetime.utcnow() + timedelta(seconds=10), timezone='UTC')
scheduler.add_job(lambda: check_and_lift(), "interval", minutes=1, next_run_time=datetime.utcnow() + timedelta(seconds=15), timezone='UTC')
scheduler.add_job(start_maintenance, 'cron', hour=1, minute=0, timezone='UTC')
scheduler.add_job(end_maintenance, 'cron', hour=2, minute=0, timezone='UTC')
scheduler.start()

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json(force=True)
    except Exception:
        return jsonify({"ok": False}), 400
    try:
        if "message" in update:
            msg = update["message"]
            user = msg.get("from", {})
            uid = user.get("id")
            username = user.get("username") or (user.get("first_name") or "")
            text = msg.get("text") or ""

            # Ensure an allowed_users row exists for this user (insert if missing), then update username
            try:
                with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                    c = conn.cursor()
                    c.execute("INSERT OR IGNORE INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)",
                              (uid, username or "", now_ts()))
                    c.execute("UPDATE allowed_users SET username = ? WHERE user_id = ?", (username or "", uid))
                    conn.commit()
            except Exception:
                logger.exception("webhook: ensure allowed_users row failed")

            if text.startswith("/"):
                parts = text.split(None, 1)
                cmd = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                return handle_command(uid, username, cmd, args)
            else:
                return handle_user_text(uid, username, text)
    except Exception:
        logger.exception("webhook handling error")
    return jsonify({"ok": True})

@app.route("/", methods=["GET"])
def root():
    return "WordSplitter running. 💚", 200

@app.route("/health", methods=["GET", "HEAD"])
def health():
    return jsonify({"ok": True, "ts": now_ts(), "uptime": format_uptime(START_TS)}), 200

def get_user_task_counts(user_id: int):
    with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status IN ('running','paused')", (user_id,))
        active = int(c.fetchone()[0] or 0)
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        queued = int(c.fetchone()[0] or 0)
    return active, queued

def handle_command(user_id: int, username: str, command: str, args: str):
    def is_owner(u): return u in OWNER_IDS

    # Ensure /start and /about are always functional for everyone
    if command == "/start":
        msg = (
            f"👋 Hi {username or user_id}! 😊\n"
            "I split your text into individual word messages.\n\n"
            "Owner commands:\n"
            " /adduser /listusers /listsuspended /botinfo /broadcast /suspend /unsuspend\n\n"
            "User commands:\n"
            " /start /example /pause /resume /status /stop /stats /about\n\n"
            "Just send any text and I'll split it for you. ✨🎉"
        )
        send_message(user_id, msg)
        return jsonify({"ok": True})

    if command == "/about":
        msg = (
            "ℹ️ About 🤖\n"
            "I split texts into single words.\n\n"
            "Features:\n"
            "queueing, pause/resume, scheduled maintenance (2AM–3AM),\n"
            "hourly owner stats, rate-limited sending. 💡\n\n"
            "Developer: Owner 👑"
        )
        send_message(user_id, msg)
        return jsonify({"ok": True})

    if command != "/start" and is_maintenance_time() and not is_owner(user_id):
        send_message(user_id, "🛠️ Scheduled maintenance in progress. Tasks are temporarily blocked. Please try later. ⏳")
        return jsonify({"ok": True})

    if user_id not in OWNER_IDS and not is_allowed(user_id):
        send_message(user_id, f"❌ Sorry, you are not allowed. Owner notified. 🔒\nUser ID: {user_id}")
        notify_owners(f"⚠️ Unallowed access attempt by {username or user_id} ({user_id}).")
        return jsonify({"ok": True})

    # many command handlers (unchanged) ...
    # we'll keep the rest of the handlers as previously implemented (example/pause/resume/status/stop/stats/adduser/listusers/listsuspended/botinfo/broadcast/suspend/unsuspend)
    # but we enrich /botinfo here.

    if command == "/example":
        sample = "\n".join([
            "996770061141", "996770064514", "996770071665", "996770073284",
            "996770075145", "996770075627", "996770075973", "996770076350",
            "996770076869", "996770077101"
        ])
        res = enqueue_task(user_id, username, sample)
        if not res["ok"]:
            if res.get("reason") == "maintenance":
                send_message(user_id, "🛠️ Scheduled maintenance in progress. Try later. ⏳")
                return jsonify({"ok": True})
            send_message(user_id, "😔 Could not queue demo. Try later. 🙏")
            return jsonify({"ok": True})
        start_user_worker_if_needed(user_id)
        notify_user_worker(user_id)
        active, queued = get_user_task_counts(user_id)
        if active:
            send_message(user_id, f"✅ Task added. Words: {res['total_words']}. 🎉\nQueue position: {queued} ⌛\nUser ID: {user_id}")
        else:
            send_message(user_id, f"✅ Task added. Words: {res['total_words']}. 🎉\nUser ID: {user_id}")
        return jsonify({"ok": True})

    if command == "/pause":
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'running' ORDER BY started_at ASC LIMIT 1", (user_id,))
            rows = c.fetchone()
        if not rows:
            send_message(user_id, f"❌ No active task to pause. 🤷\nUser ID: {user_id}")
            return jsonify({"ok": True})
        set_task_status(rows[0], "paused")
        notify_user_worker(user_id)
        send_message(user_id, f"⏸️ Paused. Use /resume to continue. 🙏\nUser ID: {user_id}")
        return jsonify({"ok": True})

    if command == "/resume":
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'paused' ORDER BY started_at ASC LIMIT 1", (user_id,))
            rows = c.fetchone()
        if not rows:
            send_message(user_id, f"❌ No paused task to resume. 🤔\nUser ID: {user_id}")
            return jsonify({"ok": True})
        set_task_status(rows[0], "running")
        notify_user_worker(user_id)
        send_message(user_id, f"▶️ Resuming your task now. 🚀\nUser ID: {user_id}")
        return jsonify({"ok": True})

    if command == "/status":
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT id, status, total_words, sent_count FROM tasks WHERE user_id = ? AND status IN ('running','paused') ORDER BY started_at ASC LIMIT 1", (user_id,))
            active = c.fetchone()
            c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
            queued = c.fetchone()[0]
        if active:
            aid, status, total, sent = active
            remaining = int(total or 0) - int(sent or 0)
            send_message(user_id, f"📊 Status: {status}\nRemaining words: {remaining}\nQueue size: {queued}\nTask ID: {aid} 🧾")
        elif queued > 0:
            send_message(user_id, f"📝 Waiting. Queue size: {queued} ⌛\nUser ID: {user_id}")
        else:
            send_message(user_id, f"📊 You have no active or queued tasks. ✅\nUser ID: {user_id}")
        return jsonify({"ok": True})

    if command == "/stop":
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
            queued = c.fetchone()[0]
        stopped = cancel_active_task_for_user(user_id)
        stop_user_worker(user_id)
        if stopped > 0 or queued > 0:
            send_message(user_id, f"🧹🛑 Active task stopped. Your queued tasks were cleared too. 👍\nUser ID: {user_id}")
        else:
            send_message(user_id, f"ℹ️ You had no active or queued tasks. 🤷\nUser ID: {user_id}")
        return jsonify({"ok": True})

    if command == "/stats":
        words = compute_last_12h_stats(user_id)
        send_message(user_id, f"📈 Your last 12 hours: {words} words split 📊\nUser ID: {user_id}")
        return jsonify({"ok": True})

    if command == "/adduser":
        if not is_owner(user_id):
            send_message(user_id, f"❌ Owner only. 🔐\nUser ID: {user_id}")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, f"ℹ️ Usage: /adduser <user_id> [username]\nUser ID: {user_id}")
            return jsonify({"ok": True})
        parts = re.split(r"[,\s]+", args.strip())
        added, already, invalid = [], [], []
        for p in parts:
            if not p:
                continue
            try:
                tid = int(p)
            except Exception:
                invalid.append(p)
                continue
            with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
                c = conn.cursor()
                c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (tid,))
                if c.fetchone():
                    already.append(tid)
                    continue
                c.execute("INSERT INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)", (tid, "", now_ts()))
                conn.commit()
            added.append(tid)
            try:
                send_message(tid, f"✅ You have been added. Send any text to start. 🙌\nUser ID: {tid}")
            except Exception:
                pass
        parts_msgs = []
        if added: parts_msgs.append("✅ Added: " + ", ".join(str(x) for x in added))
        if already: parts_msgs.append("ℹ️ Already present: " + ", ".join(str(x) for x in already))
        if invalid: parts_msgs.append("❌ Invalid: " + ", ".join(invalid))
        send_message(user_id, "; ".join(parts_msgs) if parts_msgs else "No changes")
        return jsonify({"ok": True})

    if command == "/listusers":
        if not is_owner(user_id):
            send_message(user_id, f"❌ Owner only. 🔐\nUser ID: {user_id}")
            return jsonify({"ok": True})
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT user_id, username, added_at FROM allowed_users ORDER BY added_at DESC")
            rows = c.fetchall()
        lines = []
        for r in rows:
            uid, uname, added_at_utc = r
            uname_s = f"({uname})" if uname else "(no username)"
            added_at_wat = utc_to_wat_ts(added_at_utc)
            lines.append(f"{uid} {uname_s} added={added_at_wat}")
        send_message(user_id, "👥 Allowed users:\n" + ("\n".join(lines) if lines else "(none)"))
        return jsonify({"ok": True})

    if command == "/listsuspended":
        if not is_owner(user_id):
            send_message(user_id, f"❌ Owner only. 🔐\nUser ID: {user_id}")
            return jsonify({"ok": True})
        for row in list_suspended()[:]:
            uid, until_utc, reason, added_at_utc = row
            until_dt = datetime.strptime(until_utc, "%Y-%m-%d %H:%M:%S")
            if until_dt <= datetime.utcnow():
                unsuspend_user(uid)
        rows = list_suspended()
        if not rows:
            send_message(user_id, "✅ No suspended users.")
            return jsonify({"ok": True})
        lines = []
        for r in rows:
            uid, until_utc, reason, added_at_utc = r
            until_wat = utc_to_wat_ts(until_utc)
            added_wat = utc_to_wat_ts(added_at_utc)
            uname = fetch_display_username(uid)
            uname_s = f"({uname})" if uname else ""
            lines.append(f"⛔ {uid} {uname_s} suspended_until={until_wat} by={PRIMARY_OWNER} reason={reason}")
        send_message(user_id, "⛔ Suspended users:\n" + "\n".join(lines))
        return jsonify({"ok": True})

    if command == "/botinfo":
        if not is_owner(user_id):
            send_message(user_id, f"❌ Owner only. 🔐\nUser ID: {user_id}")
            return jsonify({"ok": True})
        # Enhanced botinfo
        uptime = format_uptime(START_TS)
        worker_count = len(_user_workers)
        allowed_count = count_allowed_users()
        status_counts = get_tasks_status_counts()
        queued_top = get_top_users_by_queued(8)
        failures = get_send_failures_summary(8)
        # assemble lines
        blines = []
        blines.append("🛰️ BOT INFO — Detailed")
        blines.append(f"⏱️ Uptime: {uptime} | Workers: {worker_count} 🧵")
        blines.append(f"👥 Owners: {len(OWNER_IDS)} | Allowed (DB): {allowed_count} ✅")
        blines.append(f"⚠️ Suspended users: {list_suspended().__len__()} | Send-fail entries: {len(failures)}")
        blines.append("")
        blines.append("🔎 Task status snapshot:")
        for s in ("running","paused","queued","done","cancelled"):
            blines.append(f"  • {s}: {status_counts.get(s,0)}")
        blines.append("")
        blines.append("📝 Top queued users:")
        if queued_top:
            for uid_q, qcount in queued_top:
                uname_q = fetch_display_username(uid_q) or ""
                blines.append(f"  • {uid_q} ({uname_q}) — {qcount} queued")
        else:
            blines.append("  • (none)")
        blines.append("")
        blines.append("💥 Recent send-failures (top):")
        if failures:
            for uidf, fcount, last in failures:
                blines.append(f"  • {uidf} — {fcount} fails (last: {last})")
        else:
            blines.append("  • (none)")
        blines.append("")
        # Add top senders last hour
        last_hour = compute_last_hour_stats()
        blines.append("📈 Top senders (last 1h):")
        if last_hour:
            for i, (uidh, un, w) in enumerate(last_hour[:8], start=1):
                uname_h = un or fetch_display_username(uidh) or ""
                blines.append(f"  {i}. {uidh} ({uname_h}) — {w} words")
        else:
            blines.append("  • (none in last hour)")
        blines.append("")
        blines.append(f"🗂️ DB path: {DB_PATH}")
        blines.append(f"📌 Next maintenance: daily 02:00–03:00 WAT (UTC 01:00–02:00) 🛠️")
        blines.append("")
        blines.append("ℹ️ Use /listsuspended /listusers /botinfo /stats for more info. ❤️")
        send_message(user_id, "\n".join(blines))
        return jsonify({"ok": True})

    if command == "/broadcast":
        if not is_owner(user_id):
            send_message(user_id, f"❌ Owner only. 🔐\nUser ID: {user_id}")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, f"ℹ️ Usage: /broadcast <message>\nUser ID: {user_id}")
            return jsonify({"ok": True})
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT user_id FROM allowed_users")
            rows = c.fetchall()
        succeeded, failed = [], []
        header = f"📣 Broadcast from Owner:\n\n{args}"
        for r in rows:
            tid = r[0]
            ok, reason = broadcast_send_raw(tid, header)
            if ok:
                succeeded.append(tid)
            else:
                failed.append((tid, reason))
        summary = f"📣 Broadcast done. Success: {len(succeeded)}, Failed: {len(failed)}"
        send_message(user_id, summary)
        if failed:
            notify_owners("Broadcast failures: " + ", ".join(f"{x[0]}({x[1]})" for x in failed))
        return jsonify({"ok": True})

    if command == "/suspend":
        if not is_owner(user_id):
            send_message(user_id, f"❌ Owner only. 🔐\nUser ID: {user_id}")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, f"ℹ️ Usage: /suspend <telegram_user_id> [duration]\nUser ID: {user_id}")
            return jsonify({"ok": True})
        parts = args.split(None, 2)
        try:
            target = int(parts[0])
        except Exception:
            send_message(user_id, f"❌ Invalid user id. ❗\nUser ID: {user_id}")
            return jsonify({"ok": True})
        if len(parts) < 2:
            send_message(user_id, f"❌ Missing duration. ⌛\nUser ID: {user_id}")
            return jsonify({"ok": True})
        dur = parts[1]
        reason = parts[2] if len(parts) > 2 else ""
        m = re.match(r"^(\d+)(s|m|h|d)?$", dur)
        if not m:
            send_message(user_id, f"❌ Invalid duration format. ⌛\nUser ID: {user_id}")
            return jsonify({"ok": True})
        val, unit = int(m.group(1)), (m.group(2) or "s")
        mul = {"s":1, "m":60, "h":3600, "d":86400}.get(unit,1)
        seconds = val * mul
        suspend_user(target, seconds, reason)
        send_message(user_id, f"✅ User {target} suspended until {utc_to_wat_ts((datetime.utcnow() + timedelta(seconds=seconds)).strftime('%Y-%m-%d %H:%M:%S'))}. 🛡️")
        return jsonify({"ok": True})

    if command == "/unsuspend":
        if not is_owner(user_id):
            send_message(user_id, f"❌ Owner only. 🔐\nUser ID: {user_id}")
            return jsonify({"ok": True})
        if not args:
            send_message(user_id, f"ℹ️ Usage: /unsuspend <telegram_user_id>\nUser ID: {user_id}")
            return jsonify({"ok": True})
        try:
            target = int(args.split()[0])
        except Exception:
            send_message(user_id, f"❌ Invalid user id. ❗\nUser ID: {user_id}")
            return jsonify({"ok": True})
        ok = unsuspend_user(target)
        if ok:
            send_message(user_id, f"✅ User {target} unsuspended. 🙌")
        else:
            send_message(user_id, f"ℹ️ User {target} is not suspended. 🤷")
        return jsonify({"ok": True})

    send_message(user_id, f"🤔 Unknown command. ❓\nUser ID: {user_id}")
    return jsonify({"ok": True})

def handle_user_text(user_id: int, username: str, text: str):
    if is_maintenance_time():
        send_message(user_id, "🛠️ Scheduled maintenance in progress. Tasks are temporarily blocked. Please try later. ⏳")
        return jsonify({"ok": True})
    # Owners are always allowed; regular users must be in allowed_users
    if user_id not in OWNER_IDS and not is_allowed(user_id):
        send_message(user_id, f"❌ Sorry, you are not allowed. Owner notified. 🔒\nUser ID: {user_id}")
        notify_owners(f"⚠️ Unallowed access attempt by {user_id}.")
        return jsonify({"ok": True})
    if is_suspended(user_id):
        with _db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,))
            r = c.fetchone()
            until_utc = r[0] if r else "unknown"
            until_wat = utc_to_wat_ts(until_utc)
        send_message(user_id, f"⛔ You have been suspended until {until_wat} by Owner. 🔒\nUser ID: {user_id}")
        return jsonify({"ok": True})
    res = enqueue_task(user_id, username, text)
    if not res["ok"]:
        if res.get("reason") == "maintenance":
            send_message(user_id, "🛠️ Scheduled maintenance in progress. Try later. ⏳")
            return jsonify({"ok": True})
        if res["reason"] == "empty":
            send_message(user_id, f"⚠️ Empty text. Nothing to split. ✍️\nUser ID: {user_id}")
            return jsonify({"ok": True})
        if res["reason"] == "queue_full":
            send_message(user_id, f"❌ Your queue is full ({res['queue_size']}). Use /stop or wait. ⌛\nUser ID: {user_id}")
            return jsonify({"ok": True})
        send_message(user_id, f"😔 Could not queue task. Try later. 🙏\nUser ID: {user_id}")
        return jsonify({"ok": True})
    start_user_worker_if_needed(user_id)
    notify_user_worker(user_id)
    active, queued = get_user_task_counts(user_id)
    if active:
        send_message(user_id, f"✅ Task added. Words: {res['total_words']} 🔢.\nQueue position: {queued} ⌛\nUser ID: {user_id}")
    else:
        send_message(user_id, f"✅ Task added. Words: {res['total_words']} 🔢.\nUser ID: {user_id}")
    return jsonify({"ok": True})

def set_webhook():
    if not TELEGRAM_API or not WEBHOOK_URL:
        logger.info("Webhook not configured.")
        return
    try:
        _session.post(f"{TELEGRAM_API}/setWebhook", json={"url": WEBHOOK_URL}, timeout=REQUESTS_TIMEOUT)
    except Exception:
        logger.exception("set_webhook failed")

if __name__ == "__main__":
    try:
        set_webhook()
    except Exception:
        pass
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
