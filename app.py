#!/usr/bin/env python3
"""
Multi-Bot WordSplitter - Optimized Version
Maintains all original functionality with performance improvements
"""

import os
import time
import json
import sqlite3
import threading
import logging
import re
import signal
import ssl
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

# ===================== CONSTANTS =====================
NIGERIA_TZ_OFFSET = timedelta(hours=1)
LOG_RETENTION_DAYS = 30
FAILURE_NOTIFY_THRESHOLD = 6
PERMANENT_SUSPEND_DAYS = 365
REQUESTS_TIMEOUT = 10.0

# ===================== LOGGING =====================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("multibot_wordsplitter")

# ===================== CONFIGURATION =====================
def parse_id_list(raw: str) -> List[int]:
    """Parse comma/space separated ID list"""
    if not raw:
        return []
    return [int(p) for p in re.split(r"[,\s]+", raw.strip()) if p]

# Bot Configuration
BOTS_CONFIG = {
    "bot_a": {
        "name": "Bot A",
        "token": os.environ.get("TELEGRAM_TOKEN_A", ""),
        "webhook_url": os.environ.get("WEBHOOK_URL_A", ""),
        "owner_ids_raw": os.environ.get("OWNER_IDS_A", ""),
        "allowed_users_raw": os.environ.get("ALLOWED_USERS_A", ""),
        "owner_tag": "Owner (@justmemmy)",
        "db_path": os.environ.get("DB_PATH_A", "/tmp/botdata_a.sqlite3"),
        "interval_speed": "fast",
        "max_queue_per_user": 5,
        "max_msg_per_second": 50.0,
        "max_concurrent_workers": 25,
    },
    "bot_b": {
        "name": "Bot B",
        "token": os.environ.get("TELEGRAM_TOKEN_B", ""),
        "webhook_url": os.environ.get("WEBHOOK_URL_B", ""),
        "owner_ids_raw": os.environ.get("OWNER_IDS_B", ""),
        "allowed_users_raw": os.environ.get("ALLOWED_USERS_B", ""),
        "owner_tag": "Owner (@justmemmy)",
        "db_path": os.environ.get("DB_PATH_B", "/tmp/botdata_b.sqlite3"),
        "interval_speed": "fast",
        "max_queue_per_user": 5,
        "max_msg_per_second": 50.0,
        "max_concurrent_workers": 25,
    },
    "bot_c": {
        "name": "Bot C",
        "token": os.environ.get("TELEGRAM_TOKEN_C", ""),
        "webhook_url": os.environ.get("WEBHOOK_URL_C", ""),
        "owner_ids_raw": os.environ.get("OWNER_IDS_C", ""),
        "allowed_users_raw": os.environ.get("ALLOWED_USERS_C", ""),
        "owner_tag": "Owner (@justmemmy)",
        "db_path": os.environ.get("DB_PATH_C", "/tmp/botdata_c.sqlite3"),
        "interval_speed": "slow",
        "max_queue_per_user": 5,
        "max_msg_per_second": 50.0,
        "max_concurrent_workers": 25,
    }
}

# Initialize bot configurations
for bot_id, config in BOTS_CONFIG.items():
    config["owner_ids"] = parse_id_list(config["owner_ids_raw"])
    config["allowed_users"] = parse_id_list(config["allowed_users_raw"])
    config["primary_owner"] = config["owner_ids"][0] if config["owner_ids"] else None
    config["telegram_api"] = f"https://api.telegram.org/bot{config['token']}" if config['token'] else None
    
    # Parse numeric settings
    config["max_queue_per_user"] = int(os.environ.get(f"MAX_QUEUE_PER_USER_{bot_id.upper().replace('_', '')}", config["max_queue_per_user"]))
    config["max_msg_per_second"] = float(os.environ.get(f"MAX_MSG_PER_SECOND_{bot_id.upper().replace('_', '')}", config["max_msg_per_second"]))
    config["max_concurrent_workers"] = int(os.environ.get(f"MAX_CONCURRENT_WORKERS_{bot_id.upper().replace('_', '')}", config["max_concurrent_workers"]))

# ===================== GLOBAL STATES =====================
BOT_STATES = {}
for bot_id in BOTS_CONFIG:
    BOT_STATES[bot_id] = {
        "db_conn": None,
        "db_lock": threading.RLock(),
        "user_workers": {},
        "user_workers_lock": threading.Lock(),
        "owner_ops_state": {},
        "owner_ops_lock": threading.Lock(),
        "token_bucket": None,
        "active_workers_semaphore": None,
        "session": None,
        "session_created_at": 0,
        "session_request_count": 0,
        "worker_heartbeats": {},
        "worker_heartbeats_lock": threading.Lock(),
        "telegram_circuit_breaker": {"state": "closed", "failure_count": 0, "last_failure": 0, "next_check": 0}
    }

# ===================== FLASK APP =====================
app = Flask(__name__)

# ===================== UTILITY FUNCTIONS =====================
def format_datetime(dt: datetime) -> str:
    """Format datetime to readable string"""
    try:
        return dt.strftime("%b %d, %Y %-I:%M %p")
    except ValueError:
        return dt.strftime("%b %d, %Y %#I:%M %p")

def now_ts() -> str:
    """Current UTC timestamp for database"""
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def now_display() -> str:
    """Current UTC time in display format"""
    return format_datetime(datetime.utcnow())

def utc_to_wat_ts(utc_ts: str) -> str:
    """Convert UTC timestamp to WAT display format"""
    try:
        utc_dt = datetime.strptime(utc_ts, "%Y-%m-%d %H:%M:%S")
        wat_dt = utc_dt + NIGERIA_TZ_OFFSET
        return format_datetime(wat_dt) + " WAT"
    except Exception:
        return f"{utc_ts} (time error)"

def at_username(u: str) -> str:
    """Format username with @"""
    return u.lstrip("@") if u else ""

def label_for_self(bot_id: str, viewer_id: int, username: str) -> str:
    """Create label for self-reference"""
    config = BOTS_CONFIG[bot_id]
    if username:
        if viewer_id in config["owner_ids"]:
            return f"{at_username(username)} (ID: {viewer_id})"
        return f"{at_username(username)}"
    return f"(ID: {viewer_id})" if viewer_id in config["owner_ids"] else ""

def label_for_owner_view(bot_id: str, target_id: int, target_username: str) -> str:
    """Create label for owner view"""
    if target_username:
        return f"{at_username(target_username)} (ID: {target_id})"
    return str(target_id)

# ===================== DATABASE FUNCTIONS =====================
def get_db_connection(bot_id: str):
    """Get or create database connection with health check"""
    state = BOT_STATES[bot_id]
    
    # Return existing healthy connection
    if state["db_conn"]:
        try:
            with state["db_lock"]:
                c = state["db_conn"].cursor()
                c.execute("SELECT 1")
            return state["db_conn"]
        except Exception:
            # Connection is stale, close it
            try:
                state["db_conn"].close()
            except Exception:
                pass
            state["db_conn"] = None
    
    # Create new connection
    try:
        config = BOTS_CONFIG[bot_id]
        db_path = config["db_path"]
        
        # Ensure parent directory exists
        parent_dir = os.path.dirname(db_path)
        if parent_dir and not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
        
        conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-2000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=10000;")
        
        # Create schema if not exists
        c = conn.cursor()
        c.executescript("""
            CREATE TABLE IF NOT EXISTS allowed_users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                added_at TEXT
            );
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
                finished_at TEXT,
                last_activity TEXT,
                retry_count INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS split_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                words INTEGER,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS sent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                sent_at TEXT,
                deleted INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS suspended_users (
                user_id INTEGER PRIMARY KEY,
                suspended_until TEXT,
                reason TEXT,
                added_at TEXT
            );
            CREATE TABLE IF NOT EXISTS send_failures (
                user_id INTEGER PRIMARY KEY,
                failures INTEGER,
                last_failure_at TEXT,
                notified INTEGER DEFAULT 0,
                last_error_code INTEGER,
                last_error_desc TEXT
            );
        """)
        conn.commit()
        
        state["db_conn"] = conn
        logger.info("DB initialized for %s at %s", bot_id, db_path)
        return conn
        
    except Exception as e:
        logger.error("Failed to create DB connection for %s: %s", bot_id, e)
        return None

def check_db_health(bot_id: str) -> bool:
    """Check database health with reconnection"""
    return get_db_connection(bot_id) is not None

def ensure_owners_in_allowed(bot_id: str):
    """Ensure owners are in allowed_users table"""
    config = BOTS_CONFIG[bot_id]
    conn = get_db_connection(bot_id)
    if not conn:
        return
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            for oid in config["owner_ids"]:
                c.execute("INSERT OR IGNORE INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)",
                         (oid, "", now_ts()))
            conn.commit()
    except Exception:
        logger.exception("Error ensuring owners for %s", bot_id)

# Initialize databases
for bot_id in BOTS_CONFIG:
    get_db_connection(bot_id)
    ensure_owners_in_allowed(bot_id)

# ===================== SESSION MANAGEMENT =====================
def get_session(bot_id: str, force_new: bool = False):
    """Get or create requests session"""
    state = BOT_STATES[bot_id]
    current_time = time.time()
    session_age = current_time - state["session_created_at"]
    
    if state["session"] is None or force_new or session_age > 3600:
        if state["session"]:
            try:
                state["session"].close()
            except Exception:
                pass
        
        session = requests.Session()
        try:
            retry_strategy = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(
                max_retries=retry_strategy,
                pool_connections=BOTS_CONFIG[bot_id]["max_concurrent_workers"]*2,
                pool_maxsize=BOTS_CONFIG[bot_id]["max_concurrent_workers"]*2
            )
            session.mount("https://", adapter)
            session.mount("http://", adapter)
        except Exception:
            pass
        
        state["session"] = session
        state["session_created_at"] = current_time
        state["session_request_count"] = 0
    
    state["session_request_count"] += 1
    return state["session"]

# ===================== TOKEN BUCKET =====================
class TokenBucket:
    """Rate limiter for message sending"""
    def __init__(self, rate_per_sec: float):
        self.capacity = max(1.0, rate_per_sec)
        self.tokens = self.capacity
        self.rate = rate_per_sec
        self.last = time.monotonic()
        self.cond = threading.Condition()

    def acquire(self, timeout=10.0) -> bool:
        """Acquire a token with timeout"""
        end = time.monotonic() + timeout
        with self.cond:
            while True:
                now = time.monotonic()
                elapsed = now - self.last
                if elapsed > 0:
                    self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                    self.last = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return True
                remaining = end - now
                if remaining <= 0:
                    return False
                self.cond.wait(timeout=min(remaining, max(0.01, 1.0/max(1.0, self.rate))))

# Initialize token buckets
for bot_id in BOTS_CONFIG:
    BOT_STATES[bot_id]["token_bucket"] = TokenBucket(BOTS_CONFIG[bot_id]["max_msg_per_second"])
    BOT_STATES[bot_id]["active_workers_semaphore"] = threading.Semaphore(
        BOTS_CONFIG[bot_id]["max_concurrent_workers"]
    )

# ===================== TELEGRAM UTILITIES =====================
def _utf16_len(s: str) -> int:
    """Calculate UTF-16 length for Telegram entities"""
    return len(s.encode("utf-16-le")) // 2 if s else 0

def _build_entities_for_text(text: str):
    """Build entities for numeric words"""
    if not text:
        return None
    entities = []
    for m in re.finditer(r"\b\d+\b", text):
        utf16_offset = _utf16_len(text[:m.start()])
        utf16_length = _utf16_len(text[m.start():m.end()])
        entities.append({"type": "code", "offset": utf16_offset, "length": utf16_length})
    return entities if entities else None

def is_permanent_telegram_error(code: int, description: str = "") -> bool:
    """Check if Telegram error is permanent"""
    if code in (400, 403):
        return True
    desc = description.lower()
    return any(phrase in desc for phrase in [
        "bot was blocked", "chat not found", "user is deactivated", "forbidden"
    ])

# ===================== FAILURE HANDLING =====================
def record_failure(bot_id: str, user_id: int, inc: int = 1, error_code: int = None,
                   description: str = "", is_permanent: bool = False):
    """Record send failure for user"""
    conn = get_db_connection(bot_id)
    if not conn:
        return
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT failures FROM send_failures WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            
            if row:
                failures = row[0] + inc
                c.execute("""UPDATE send_failures SET failures=?, last_failure_at=?, 
                          last_error_code=?, last_error_desc=? WHERE user_id=?""",
                         (failures, now_ts(), error_code, description, user_id))
            else:
                failures = inc
                c.execute("""INSERT INTO send_failures 
                          (user_id, failures, last_failure_at, last_error_code, last_error_desc) 
                          VALUES (?, ?, ?, ?, ?)""",
                         (user_id, failures, now_ts(), error_code, description))
            
            if is_permanent or is_permanent_telegram_error(error_code or 0, description):
                mark_user_permanently_unreachable(bot_id, user_id, error_code, description)
                return
            
            if failures >= FAILURE_NOTIFY_THRESHOLD:
                c.execute("UPDATE send_failures SET notified=1 WHERE user_id=?", (user_id,))
                notify_owners(bot_id, f"⚠️ Repeated send failures for {user_id} ({failures}). Stopping tasks. 🛑")
                cancel_active_task_for_user(bot_id, user_id)
            
            conn.commit()
    except Exception:
        logger.exception("record_failure error for %s in %s", user_id, bot_id)

def reset_failures(bot_id: str, user_id: int):
    """Reset failure count for user"""
    conn = get_db_connection(bot_id)
    if not conn:
        return
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("DELETE FROM send_failures WHERE user_id = ?", (user_id,))
            conn.commit()
    except Exception:
        logger.exception("reset_failures failed for %s", user_id)

def mark_user_permanently_unreachable(bot_id: str, user_id: int, error_code: int = None, description: str = ""):
    """Mark user as permanently unreachable"""
    config = BOTS_CONFIG[bot_id]
    
    if user_id in config["owner_ids"]:
        notify_owners(bot_id, f"⚠️ Repeated send failures for owner {user_id}. Error: {error_code} {description}")
        return
    
    # Record permanent failure
    conn = get_db_connection(bot_id)
    if conn:
        try:
            with BOT_STATES[bot_id]["db_lock"]:
                c = conn.cursor()
                c.execute("""INSERT OR REPLACE INTO send_failures 
                          (user_id, failures, last_failure_at, notified, last_error_code, last_error_desc) 
                          VALUES (?, ?, ?, ?, ?, ?)""",
                         (user_id, 999, now_ts(), 1, error_code, description))
                conn.commit()
        except Exception:
            pass
    
    # Cancel tasks and suspend
    cancel_active_task_for_user(bot_id, user_id)
    suspend_user(bot_id, user_id, PERMANENT_SUSPEND_DAYS * 86400,
                 f"Permanent send failure: {error_code} {description}")
    
    notify_owners(bot_id, f"⚠️ Permanent send failure for {user_id} ({error_code}). Error: {description}")

# ===================== MESSAGE SENDING =====================
def send_message(bot_id: str, chat_id: int, text: str, reply_markup: Optional[Dict] = None):
    """Send message with rate limiting and retry logic"""
    config = BOTS_CONFIG[bot_id]
    if not config["telegram_api"]:
        return None
    
    # Check circuit breaker
    state = BOT_STATES[bot_id]
    cb = state["telegram_circuit_breaker"]
    if cb["state"] == "open" and time.time() < cb["next_check"]:
        logger.warning(f"Circuit breaker open for {bot_id}")
        return None
    elif cb["state"] == "open":
        cb["state"] = "half-open"
        cb["failure_count"] = 0
    
    # Prepare payload
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    entities = _build_entities_for_text(text)
    if entities:
        payload["entities"] = entities
    if reply_markup:
        payload["reply_markup"] = reply_markup
    
    # Acquire rate limit token
    if not state["token_bucket"].acquire(timeout=5.0):
        record_failure(bot_id, chat_id, inc=1, description="rate_limit_timeout")
        return None
    
    # Send with retry
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            session = get_session(bot_id, force_new=(attempt > 1))
            resp = session.post(f"{config['telegram_api']}/sendMessage",
                               json=payload, timeout=REQUESTS_TIMEOUT)
            data = resp.json() if resp.content else {}
            
            if data.get("ok"):
                # Record success
                mid = data["result"].get("message_id")
                if mid:
                    conn = get_db_connection(bot_id)
                    if conn:
                        try:
                            with state["db_lock"]:
                                c = conn.cursor()
                                c.execute("INSERT INTO sent_messages (chat_id, message_id, sent_at) VALUES (?, ?, ?)",
                                         (chat_id, mid, now_ts()))
                                conn.commit()
                        except Exception:
                            pass
                
                # Reset failures and circuit breaker
                reset_failures(bot_id, chat_id)
                if cb["state"] == "half-open":
                    cb["state"] = "closed"
                    cb["failure_count"] = 0
                return data["result"]
            
            # Handle errors
            error_code = data.get("error_code")
            description = data.get("description", "")
            
            if error_code == 429:
                retry_after = data.get("parameters", {}).get("retry_after", 1)
                time.sleep(max(0.5, retry_after))
                if attempt >= max_attempts:
                    record_failure(bot_id, chat_id, inc=1, error_code=error_code, description=description)
                continue
            
            if is_permanent_telegram_error(error_code or 0, description):
                record_failure(bot_id, chat_id, inc=1, error_code=error_code, description=description, is_permanent=True)
                return None
            
            if attempt >= max_attempts:
                record_failure(bot_id, chat_id, inc=1, error_code=error_code, description=description)
                break
            
            time.sleep(0.5 * (2 ** (attempt - 1)))
            
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError,
                requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
            if attempt >= max_attempts:
                record_failure(bot_id, chat_id, inc=1, description=f"{type(e).__name__}: {str(e)}")
                break
            time.sleep(0.5 * (2 ** (attempt - 1)))
    
    # Update circuit breaker on persistent failure
    cb["failure_count"] += 1
    if cb["failure_count"] >= 5:
        cb["state"] = "open"
        cb["next_check"] = time.time() + 60
        logger.warning(f"Circuit breaker opened for {bot_id}")
    
    return None

# ===================== TASK MANAGEMENT =====================
def split_text_to_words(text: str) -> List[str]:
    """Split text into words"""
    return [w for w in text.strip().split() if w]

def enqueue_task(bot_id: str, user_id: int, username: str, text: str):
    """Enqueue new task for user"""
    config = BOTS_CONFIG[bot_id]
    conn = get_db_connection(bot_id)
    if not conn:
        return {"ok": False, "reason": "db_unavailable"}
    
    words = split_text_to_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM tasks WHERE user_id=? AND status='queued'", (user_id,))
            pending = c.fetchone()[0]
            
            if pending >= config["max_queue_per_user"]:
                return {"ok": False, "reason": "queue_full", "queue_size": pending}
            
            c.execute("""INSERT INTO tasks (user_id, username, text, words_json, total_words, 
                      status, created_at, last_activity) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                     (user_id, username, text, json.dumps(words), total, "queued", now_ts(), now_ts()))
            conn.commit()
            
            return {"ok": True, "total_words": total, "queue_size": pending + 1}
            
    except Exception:
        return {"ok": False, "reason": "db_error"}

def get_next_task_for_user(bot_id: str, user_id: int):
    """Get next queued task for user"""
    conn = get_db_connection(bot_id)
    if not conn:
        return None
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("""SELECT id, words_json, total_words, text, retry_count 
                      FROM tasks WHERE user_id=? AND status='queued' ORDER BY id LIMIT 1""",
                     (user_id,))
            r = c.fetchone()
            
            if not r:
                return None
            
            return {
                "id": r[0],
                "words": json.loads(r[1]) if r[1] else split_text_to_words(r[3]),
                "total_words": r[2],
                "text": r[3],
                "retry_count": r[4]
            }
    except Exception:
        return None

def set_task_status(bot_id: str, task_id: int, status: str):
    """Update task status"""
    conn = get_db_connection(bot_id)
    if not conn:
        return
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            now = now_ts()
            
            if status == "running":
                c.execute("UPDATE tasks SET status=?, started_at=?, last_activity=? WHERE id=?",
                         (status, now, now, task_id))
            elif status in ("done", "cancelled"):
                c.execute("UPDATE tasks SET status=?, finished_at=?, last_activity=? WHERE id=?",
                         (status, now, now, task_id))
            else:
                c.execute("UPDATE tasks SET status=?, last_activity=? WHERE id=?",
                         (status, now, task_id))
            conn.commit()
    except Exception:
        logger.exception("set_task_status failed for task %s", task_id)

def update_task_activity(bot_id: str, task_id: int):
    """Update task activity timestamp"""
    conn = get_db_connection(bot_id)
    if not conn:
        return
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("UPDATE tasks SET last_activity=? WHERE id=?", (now_ts(), task_id))
            conn.commit()
    except Exception:
        pass

def cancel_active_task_for_user(bot_id: str, user_id: int) -> int:
    """Cancel all active tasks for user"""
    conn = get_db_connection(bot_id)
    if not conn:
        return 0
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT id FROM tasks WHERE user_id=? AND status IN ('queued','running','paused')",
                     (user_id,))
            rows = c.fetchall()
            
            for r in rows:
                c.execute("UPDATE tasks SET status='cancelled', finished_at=? WHERE id=?",
                         (now_ts(), r[0]))
            
            conn.commit()
            notify_user_worker(bot_id, user_id)
            return len(rows)
    except Exception:
        return 0

def record_split_log(bot_id: str, user_id: int, username: str, count: int = 1):
    """Record split log entry"""
    conn = get_db_connection(bot_id)
    if not conn:
        return
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            now = now_ts()
            entries = [(user_id, username, 1, now) for _ in range(count)]
            c.executemany("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (?, ?, ?, ?)", entries)
            conn.commit()
    except Exception:
        pass

# ===================== USER MANAGEMENT =====================
def is_allowed(bot_id: str, user_id: int) -> bool:
    """Check if user is allowed"""
    config = BOTS_CONFIG[bot_id]
    if user_id in config["owner_ids"]:
        return True
    
    conn = get_db_connection(bot_id)
    if not conn:
        return False
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT 1 FROM allowed_users WHERE user_id=?", (user_id,))
            return bool(c.fetchone())
    except Exception:
        return False

def suspend_user(bot_id: str, target_id: int, seconds: int, reason: str = ""):
    """Suspend user for specified duration"""
    config = BOTS_CONFIG[bot_id]
    conn = get_db_connection(bot_id)
    if not conn:
        return
    
    # Calculate suspension end time
    until_utc = (datetime.utcnow() + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
    until_wat = format_datetime(datetime.utcnow() + timedelta(seconds=seconds) + NIGERIA_TZ_OFFSET) + " WAT"
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("""INSERT OR REPLACE INTO suspended_users 
                      (user_id, suspended_until, reason, added_at) VALUES (?, ?, ?, ?)""",
                     (target_id, until_utc, reason, now_ts()))
            conn.commit()
    except Exception:
        pass
    
    # Cancel active tasks and notify
    cancel_active_task_for_user(bot_id, target_id)
    
    reason_text = f"\nReason: {reason}" if reason else ""
    send_message(bot_id, target_id, f"⛔ You have been suspended until {until_wat} by {config['owner_tag']}.{reason_text}")
    
    notify_owners(bot_id, f"🔒 User suspended: {label_for_owner_view(bot_id, target_id, fetch_display_username(bot_id, target_id))} until={until_wat} reason={reason}")

def unsuspend_user(bot_id: str, target_id: int) -> bool:
    """Unsuspend user"""
    conn = get_db_connection(bot_id)
    if not conn:
        return False
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT 1 FROM suspended_users WHERE user_id=?", (target_id,))
            if not c.fetchone():
                return False
            c.execute("DELETE FROM suspended_users WHERE user_id=?", (target_id,))
            conn.commit()
        
        send_message(bot_id, target_id, f"✅ You have been unsuspended by {BOTS_CONFIG[bot_id]['owner_tag']}.")
        notify_owners(bot_id, f"🔓 Manual unsuspend: {label_for_owner_view(bot_id, target_id, fetch_display_username(bot_id, target_id))}")
        return True
        
    except Exception:
        return False

def is_suspended(bot_id: str, user_id: int) -> bool:
    """Check if user is suspended"""
    config = BOTS_CONFIG[bot_id]
    if user_id in config["owner_ids"]:
        return False
    
    conn = get_db_connection(bot_id)
    if not conn:
        return False
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT suspended_until FROM suspended_users WHERE user_id=?", (user_id,))
            r = c.fetchone()
            
            if not r:
                return False
            
            until = datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S")
            return until > datetime.utcnow()
    except Exception:
        return False

def notify_owners(bot_id: str, text: str):
    """Notify all bot owners"""
    for oid in BOTS_CONFIG[bot_id]["owner_ids"]:
        try:
            send_message(bot_id, oid, text)
        except Exception:
            pass

# ===================== WORKER MANAGEMENT =====================
def update_worker_heartbeat(bot_id: str, user_id: int):
    """Update worker heartbeat"""
    state = BOT_STATES[bot_id]
    with state["worker_heartbeats_lock"]:
        state["worker_heartbeats"][user_id] = time.time()

def get_worker_heartbeat(bot_id: str, user_id: int) -> float:
    """Get worker heartbeat timestamp"""
    state = BOT_STATES[bot_id]
    with state["worker_heartbeats_lock"]:
        return state["worker_heartbeats"].get(user_id, 0)

def cleanup_stale_workers(bot_id: str):
    """Clean up stale workers"""
    state = BOT_STATES[bot_id]
    current_time = time.time()
    stale_threshold = 300  # 5 minutes
    
    with state["worker_heartbeats_lock"]:
        stale_users = [uid for uid, heartbeat in list(state["worker_heartbeats"].items())
                      if current_time - heartbeat > stale_threshold]
        
        for user_id in stale_users:
            state["worker_heartbeats"].pop(user_id, None)
            
            with state["user_workers_lock"]:
                if user_id in state["user_workers"]:
                    try:
                        info = state["user_workers"][user_id]
                        info.get("stop", threading.Event()).set()
                        info.get("wake", threading.Event()).set()
                    except Exception:
                        pass
                    state["user_workers"].pop(user_id, None)
                    logger.info("Cleaned stale worker for user %s in %s", user_id, bot_id)

def notify_user_worker(bot_id: str, user_id: int):
    """Wake up user worker"""
    state = BOT_STATES[bot_id]
    with state["user_workers_lock"]:
        info = state["user_workers"].get(user_id)
        if info and "wake" in info:
            try:
                info["wake"].set()
            except Exception:
                pass

def start_user_worker_if_needed(bot_id: str, user_id: int):
    """Start worker thread for user if not running"""
    state = BOT_STATES[bot_id]
    with state["user_workers_lock"]:
        info = state["user_workers"].get(user_id)
        if info and info.get("thread") and info["thread"].is_alive():
            update_worker_heartbeat(bot_id, user_id)
            return
        
        wake = threading.Event()
        stop = threading.Event()
        thr = threading.Thread(target=per_user_worker_loop,
                              args=(bot_id, user_id, wake, stop),
                              daemon=True)
        state["user_workers"][user_id] = {"thread": thr, "wake": wake, "stop": stop}
        thr.start()
        update_worker_heartbeat(bot_id, user_id)
        logger.info("Started worker for user %s in %s", user_id, bot_id)

def stop_user_worker(bot_id: str, user_id: int, join_timeout: float = 2.0):
    """Stop user worker thread"""
    state = BOT_STATES[bot_id]
    with state["user_workers_lock"]:
        info = state["user_workers"].pop(user_id, None)
        if not info:
            return
        
        try:
            info.get("stop", threading.Event()).set()
            info.get("wake", threading.Event()).set()
            thr = info.get("thread")
            if thr and thr.is_alive():
                thr.join(join_timeout)
        except Exception:
            pass
        
        with state["worker_heartbeats_lock"]:
            state["worker_heartbeats"].pop(user_id, None)
        
        logger.info("Stopped worker for user %s in %s", user_id, bot_id)

def check_stuck_tasks(bot_id: str):
    """Check for stuck tasks and reset them"""
    try:
        cutoff = (datetime.utcnow() - timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S")
        conn = get_db_connection(bot_id)
        if not conn:
            return
        
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT id, user_id, retry_count FROM tasks WHERE status='running' AND last_activity<?", (cutoff,))
            stuck_tasks = c.fetchall()
            
            for task_id, user_id, retry_count in stuck_tasks:
                if retry_count < 3:
                    c.execute("UPDATE tasks SET status='queued', retry_count=retry_count+1, last_activity=? WHERE id=?",
                             (now_ts(), task_id))
                    notify_user_worker(bot_id, user_id)
                else:
                    c.execute("UPDATE tasks SET status='cancelled', finished_at=? WHERE id=?",
                             (now_ts(), task_id))
                    send_message(bot_id, user_id, "🛑 Task cancelled after multiple failures.")
            
            if stuck_tasks:
                conn.commit()
                logger.info("Cleaned %s stuck tasks in %s", len(stuck_tasks), bot_id)
    except Exception:
        logger.exception("Error checking stuck tasks in %s", bot_id)

# ===================== WORKER LOOP FUNCTIONS =====================
def _process_task_words(bot_id: str, user_id: int, task_id: int, words: List[str], 
                        start_index: int, uname_for_stat: str) -> int:
    """Process and send words from task"""
    config = BOTS_CONFIG[bot_id]
    state = BOT_STATES[bot_id]
    i = start_index
    consecutive_errors = 0
    
    # Determine interval based on bot speed and word count
    total = len(words)
    if config["interval_speed"] == "fast":
        interval = 0.5 if total <= 150 else (0.6 if total <= 300 else 0.7)
    else:
        interval = 1.0 if total <= 150 else (1.1 if total <= 300 else 1.2)
    
    last_send_time = time.monotonic()
    last_activity_update = time.monotonic()
    last_heartbeat_update = time.monotonic()
    
    while i < total and not state["user_workers"].get(user_id, {}).get("stop", threading.Event()).is_set():
        # Update heartbeat and activity periodically
        current_time = time.monotonic()
        if current_time - last_heartbeat_update > 10:
            update_worker_heartbeat(bot_id, user_id)
            last_heartbeat_update = current_time
        
        if current_time - last_activity_update > 30:
            update_task_activity(bot_id, task_id)
            last_activity_update = current_time
        
        # Check if task was cancelled or user suspended
        if is_suspended(bot_id, user_id):
            break
        
        # Send word
        try:
            result = send_message(bot_id, user_id, words[i])
            if result:
                consecutive_errors = 0
            else:
                consecutive_errors += 1
                
                if consecutive_errors >= 10:
                    set_task_status(bot_id, task_id, "paused")
                    send_message(bot_id, user_id, "⚠️ Task paused due to sending errors. Will retry in 30s.")
                    time.sleep(30)
                    set_task_status(bot_id, task_id, "running")
                    consecutive_errors = 0
                    continue
        except Exception:
            consecutive_errors += 1
        
        # Record split log
        record_split_log(bot_id, user_id, uname_for_stat, 1)
        i += 1
        
        # Update progress in database
        conn = get_db_connection(bot_id)
        if conn:
            try:
                with state["db_lock"]:
                    c = conn.cursor()
                    c.execute("UPDATE tasks SET sent_count=?, last_activity=? WHERE id=?",
                             (i, now_ts(), task_id))
                    conn.commit()
            except Exception:
                pass
        
        # Rate limiting
        now = time.monotonic()
        elapsed = now - last_send_time
        if elapsed < interval:
            time.sleep(interval - elapsed)
        last_send_time = time.monotonic()
    
    return i

def per_user_worker_loop(bot_id: str, user_id: int, wake_event: threading.Event, stop_event: threading.Event):
    """Main worker loop for processing user tasks"""
    logger.info("Worker loop starting for user %s in %s", user_id, bot_id)
    config = BOTS_CONFIG[bot_id]
    state = BOT_STATES[bot_id]
    uname_for_stat = fetch_display_username(bot_id, user_id) or str(user_id)
    
    update_worker_heartbeat(bot_id, user_id)
    
    try:
        while not stop_event.is_set():
            update_worker_heartbeat(bot_id, user_id)
            
            # Check if suspended
            if is_suspended(bot_id, user_id):
                cancel_active_task_for_user(bot_id, user_id)
                send_message(bot_id, user_id, "⛔ You have been suspended; stopping tasks.")
                while is_suspended(bot_id, user_id) and not stop_event.is_set():
                    wake_event.wait(timeout=5.0)
                    wake_event.clear()
                    update_worker_heartbeat(bot_id, user_id)
                continue
            
            # Get next task
            task = get_next_task_for_user(bot_id, user_id)
            if not task:
                wake_event.wait(timeout=1.0)
                wake_event.clear()
                continue
            
            task_id = task["id"]
            words = task["words"]
            total = len(words)
            retry_count = task.get("retry_count", 0)
            
            # Set task as running
            set_task_status(bot_id, task_id, "running")
            
            # Get current progress
            conn = get_db_connection(bot_id)
            start_index = 0
            if conn:
                try:
                    with state["db_lock"]:
                        c = conn.cursor()
                        c.execute("SELECT sent_count FROM tasks WHERE id=?", (task_id,))
                        row = c.fetchone()
                        start_index = row[0] if row else 0
                except Exception:
                    pass
            
            # Acquire concurrency semaphore
            semaphore_acquired = False
            while not stop_event.is_set():
                acquired = state["active_workers_semaphore"].acquire(timeout=1.0)
                if acquired:
                    semaphore_acquired = True
                    break
                update_task_activity(bot_id, task_id)
                update_worker_heartbeat(bot_id, user_id)
            
            if not semaphore_acquired or stop_event.is_set():
                continue
            
            try:
                # Send start notification
                if retry_count > 0:
                    send_message(bot_id, user_id, f"🔄 Retrying task (attempt {retry_count + 1})...")
                
                est_seconds = int((total - start_index) * (0.5 if config["interval_speed"] == "fast" else 1.0))
                est_str = str(timedelta(seconds=est_seconds))
                send_message(bot_id, user_id, f"🚀 Starting split. Words: {total}. Estimated time: {est_str}")
                
                # Process words
                final_index = _process_task_words(bot_id, user_id, task_id, words, start_index, uname_for_stat)
                
                # Update final status
                if not is_suspended(bot_id, user_id) and not stop_event.is_set():
                    set_task_status(bot_id, task_id, "done")
                    send_message(bot_id, user_id, "✅ All done!")
                else:
                    set_task_status(bot_id, task_id, "cancelled")
                    send_message(bot_id, user_id, "🛑 Task stopped.")
                    
            finally:
                if semaphore_acquired:
                    state["active_workers_semaphore"].release()
    
    except Exception:
        logger.exception("Worker error for user %s in %s", user_id, bot_id)
    finally:
        # Cleanup
        with state["worker_heartbeats_lock"]:
            state["worker_heartbeats"].pop(user_id, None)
        with state["user_workers_lock"]:
            state["user_workers"].pop(user_id, None)
        logger.info("Worker loop exiting for user %s in %s", user_id, bot_id)

# ===================== STATISTICS FUNCTIONS =====================
def fetch_display_username(bot_id: str, user_id: int) -> str:
    """Fetch username for display"""
    conn = get_db_connection(bot_id)
    if not conn:
        return ""
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            # Try split_logs first
            c.execute("SELECT username FROM split_logs WHERE user_id=? ORDER BY created_at DESC LIMIT 1",
                     (user_id,))
            r = c.fetchone()
            if r and r[0]:
                return r[0]
            
            # Then allowed_users
            c.execute("SELECT username FROM allowed_users WHERE user_id=?", (user_id,))
            r = c.fetchone()
            return r[0] if r else ""
    except Exception:
        return ""

def compute_last_hour_stats(bot_id: str):
    """Compute statistics for last hour"""
    cutoff = datetime.utcnow() - timedelta(hours=1)
    conn = get_db_connection(bot_id)
    if not conn:
        return []
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("""SELECT user_id, username, COUNT(*) as count 
                      FROM split_logs WHERE created_at>=? 
                      GROUP BY user_id, username ORDER BY count DESC""",
                     (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))
            return c.fetchall()
    except Exception:
        return []

def compute_last_12h_stats(bot_id: str, user_id: int) -> int:
    """Compute user statistics for last 12 hours"""
    cutoff = datetime.utcnow() - timedelta(hours=12)
    conn = get_db_connection(bot_id)
    if not conn:
        return 0
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM split_logs WHERE user_id=? AND created_at>=?",
                     (user_id, cutoff.strftime("%Y-%m-%d %H:%M:%S")))
            r = c.fetchone()
            return r[0] if r else 0
    except Exception:
        return 0

def send_hourly_owner_stats(bot_id: str):
    """Send hourly statistics to owners"""
    rows = compute_last_hour_stats(bot_id)
    if not rows:
        msg = "📊 Hourly Report: no splits in the last hour."
    else:
        lines = []
        for uid, uname, w in rows:
            uname_display = at_username(uname) if uname else fetch_display_username(bot_id, uid)
            lines.append(f"{uid} ({uname_display}) - {w} words sent")
        msg = "📊 Report - last 1h:\n" + "\n".join(lines)
    
    for oid in BOTS_CONFIG[bot_id]["owner_ids"]:
        try:
            send_message(bot_id, oid, msg)
        except Exception:
            pass

def check_and_lift(bot_id: str):
    """Check and lift expired suspensions"""
    conn = get_db_connection(bot_id)
    if not conn:
        return
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT user_id, suspended_until FROM suspended_users")
            rows = c.fetchall()
        
        now = datetime.utcnow()
        for uid, until_str in rows:
            try:
                until = datetime.strptime(until_str, "%Y-%m-%d %H:%M:%S")
                if until <= now:
                    unsuspend_user(bot_id, uid)
            except Exception:
                pass
    except Exception:
        logger.exception("Error checking suspensions for %s", bot_id)

def prune_old_logs(bot_id: str):
    """Prune old log entries"""
    try:
        cutoff = (datetime.utcnow() - timedelta(days=LOG_RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        conn = get_db_connection(bot_id)
        if not conn:
            return
        
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("DELETE FROM split_logs WHERE created_at<?", (cutoff,))
            deleted1 = c.rowcount
            c.execute("DELETE FROM sent_messages WHERE sent_at<?", (cutoff,))
            deleted2 = c.rowcount
            conn.commit()
        
        if deleted1 or deleted2:
            logger.info("Pruned logs for %s: %s split logs, %s sent messages", bot_id, deleted1, deleted2)
    except Exception:
        logger.exception("Error pruning logs for %s", bot_id)

def cleanup_stale_resources(bot_id: str):
    """Clean up stale resources"""
    state = BOT_STATES[bot_id]
    
    # Clean up stale workers
    cleanup_stale_workers(bot_id)
    
    # Refresh old sessions
    current_time = time.time()
    session_age = current_time - state["session_created_at"]
    if session_age > 3600 and state["session"]:
        try:
            state["session"].close()
        except Exception:
            pass
        state["session"] = None
        state["session_created_at"] = 0
        state["session_request_count"] = 0

# ===================== SCHEDULER =====================
scheduler = BackgroundScheduler()

# Add jobs for each bot
for bot_id in BOTS_CONFIG:
    scheduler.add_job(
        lambda b=bot_id: send_hourly_owner_stats(b),
        "interval", hours=1, timezone='UTC',
        id=f"hourly_stats_{bot_id}"
    )
    scheduler.add_job(
        lambda b=bot_id: check_and_lift(b),
        "interval", minutes=1, timezone='UTC',
        id=f"check_suspended_{bot_id}"
    )
    scheduler.add_job(
        lambda b=bot_id: prune_old_logs(b),
        "interval", hours=24, timezone='UTC',
        id=f"prune_logs_{bot_id}"
    )
    scheduler.add_job(
        lambda b=bot_id: check_stuck_tasks(b),
        "interval", minutes=1, timezone='UTC',
        id=f"check_stuck_{bot_id}"
    )
    scheduler.add_job(
        lambda b=bot_id: cleanup_stale_resources(b),
        "interval", minutes=5, timezone='UTC',
        id=f"cleanup_resources_{bot_id}"
    )

scheduler.start()

# ===================== SHUTDOWN HANDLER =====================
def _graceful_shutdown(signum, frame):
    """Handle graceful shutdown"""
    logger.info("Graceful shutdown signal received (%s)", signum)
    
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    
    # Stop all workers
    for bot_id in BOTS_CONFIG:
        state = BOT_STATES[bot_id]
        with state["user_workers_lock"]:
            user_ids = list(state["user_workers"].keys())
        for uid in user_ids:
            stop_user_worker(bot_id, uid)
    
    # Close all connections
    for bot_id in BOTS_CONFIG:
        try:
            if BOT_STATES[bot_id]["db_conn"]:
                BOT_STATES[bot_id]["db_conn"].close()
        except Exception:
            pass
        
        try:
            if BOT_STATES[bot_id]["session"]:
                BOT_STATES[bot_id]["session"].close()
        except Exception:
            pass
    
    logger.info("Shutdown completed")
    import os
    os._exit(0)

signal.signal(signal.SIGTERM, _graceful_shutdown)
signal.signal(signal.SIGINT, _graceful_shutdown)

# ===================== OWNER OPERATIONS =====================
def get_owner_state(bot_id: str, user_id: int) -> Optional[Dict]:
    """Get owner operation state"""
    state = BOT_STATES[bot_id]
    with state["owner_ops_lock"]:
        return state["owner_ops_state"].get(user_id)

def set_owner_state(bot_id: str, user_id: int, state_dict: Dict):
    """Set owner operation state"""
    state = BOT_STATES[bot_id]
    with state["owner_ops_lock"]:
        state["owner_ops_state"][user_id] = state_dict

def clear_owner_state(bot_id: str, user_id: int):
    """Clear owner operation state"""
    state = BOT_STATES[bot_id]
    with state["owner_ops_lock"]:
        state["owner_ops_state"].pop(user_id, None)

def is_owner_in_operation(bot_id: str, user_id: int) -> bool:
    """Check if owner is in operation mode"""
    state = BOT_STATES[bot_id]
    with state["owner_ops_lock"]:
        return user_id in state["owner_ops_state"]

def get_user_tasks_preview(bot_id: str, user_id: int, hours: int, page: int = 0) -> Tuple[List[Dict], int, int]:
    """Get user tasks preview with pagination"""
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    conn = get_db_connection(bot_id)
    if not conn:
        return [], 0, 0
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("""SELECT id, text, created_at, total_words, sent_count
                      FROM tasks WHERE user_id=? AND created_at>=? 
                      ORDER BY created_at DESC""",
                     (user_id, cutoff.strftime("%Y-%m-%d %H:%M:%S")))
            rows = c.fetchall()
    except Exception:
        return [], 0, 0
    
    tasks = []
    for task_id, text, created_at, total_words, sent_count in rows:
        words = split_text_to_words(text)
        preview = " ".join(words[:2]) if len(words) >= 2 else words[0] if words else "(empty)"
        created_display = utc_to_wat_ts(created_at)
        
        tasks.append({
            "id": task_id,
            "preview": preview,
            "created_at": created_display,
            "total_words": total_words,
            "sent_count": sent_count
        })
    
    total_tasks = len(tasks)
    page_size = 20
    start_idx = page * page_size
    paginated_tasks = tasks[start_idx:start_idx + page_size]
    total_pages = (total_tasks + page_size - 1) // page_size
    
    return paginated_tasks, total_tasks, total_pages

def get_all_users_ordered(bot_id: str):
    """Get all allowed users ordered by addition date"""
    conn = get_db_connection(bot_id)
    if not conn:
        return []
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT user_id, username, added_at FROM allowed_users ORDER BY added_at DESC")
            return c.fetchall()
    except Exception:
        return []

def get_user_index(bot_id: str, user_id: int):
    """Get user index in ordered user list"""
    users = get_all_users_ordered(bot_id)
    for i, (uid, username, added_at) in enumerate(users):
        if uid == user_id:
            return i, users
    return -1, users

def parse_duration(duration_str: str) -> Tuple[Optional[int], str]:
    """Parse duration string like '1d2h30m' to seconds"""
    if not duration_str:
        return None, "Empty duration"
    
    pattern = r'(\d+)([dhms])'
    matches = re.findall(pattern, duration_str.lower())
    
    if not matches:
        return None, f"Invalid duration format: {duration_str}"
    
    total_seconds = 0
    parts = []
    multipliers = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
    labels = {'d': 'day', 'h': 'hour', 'm': 'minute', 's': 'second'}
    
    for value, unit in matches:
        try:
            num = int(value)
            if num <= 0:
                return None, f"Value must be positive: {value}{unit}"
            
            total_seconds += num * multipliers[unit]
            label = labels[unit]
            parts.append(f"{num} {label if num == 1 else label + 's'}")
                
        except (ValueError, KeyError):
            return None, f"Invalid: {value}{unit}"
    
    if total_seconds == 0:
        return None, "Duration cannot be zero"
    
    return total_seconds, ", ".join(parts)

def send_ownersets_menu(bot_id: str, owner_id: int):
    """Send owner menu"""
    config = BOTS_CONFIG[bot_id]
    menu_text = f"👑 Owner Menu {config['owner_tag']}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nSelect an operation:"
    
    keyboard = [
        [{"text": "📊 Bot Info", "callback_data": "owner_botinfo"}, {"text": "👥 List Users", "callback_data": "owner_listusers"}],
        [{"text": "🚫 List Suspended", "callback_data": "owner_listsuspended"}, {"text": "➕ Add User", "callback_data": "owner_adduser"}],
        [{"text": "⏸️ Suspend User", "callback_data": "owner_suspend"}, {"text": "▶️ Unsuspend User", "callback_data": "owner_unsuspend"}],
        [{"text": "🔍 Check All User Preview", "callback_data": "owner_checkallpreview"}]
    ]
    
    send_message(bot_id, owner_id, menu_text, {"inline_keyboard": keyboard})

# ===================== COMMAND HANDLING =====================
def get_user_task_counts(bot_id: str, user_id: int) -> Tuple[int, int]:
    """Get active and queued task counts for user"""
    conn = get_db_connection(bot_id)
    if not conn:
        return 0, 0
    
    try:
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM tasks WHERE user_id=? AND status IN ('running','paused')", (user_id,))
            active = c.fetchone()[0] or 0
            c.execute("SELECT COUNT(*) FROM tasks WHERE user_id=? AND status='queued'", (user_id,))
            queued = c.fetchone()[0] or 0
            return active, queued
    except Exception:
        return 0, 0

def handle_command(bot_id: str, user_id: int, username: str, command: str, args: str):
    """Handle user command"""
    config = BOTS_CONFIG[bot_id]
    
    # /start command
    if command == "/start":
        who = label_for_self(bot_id, user_id, username) or "there"
        msg = (
            f"👋 Hi {who}!\n\n"
            "I split your text into individual word messages. ✂️📤\n\n"
            f"{config['owner_tag']} command:\n"
            " /ownersets - Owner management menu\n\n"
            "User commands:\n"
            " /start /example /pause /resume /status /stop /stats /about\n\n"
            "Just send any text and I'll split it for you. 🚀"
        )
        send_message(bot_id, user_id, msg)
        return jsonify({"ok": True})
    
    # /about command
    if command == "/about":
        send_message(bot_id, user_id, "ℹ️ About:\nI split texts into single words. ✂️\n\nFeatures:\nqueueing, pause/resume,\nhourly owner stats, rate-limited sending. ⚖️")
        return jsonify({"ok": True})
    
    # Check if user is allowed
    if user_id not in config["owner_ids"] and not is_allowed(bot_id, user_id):
        send_message(bot_id, user_id, f"🚫 Sorry, you are not allowed. {config['owner_tag']} notified.\nYour ID: {user_id}")
        notify_owners(bot_id, f"🚨 Unallowed access attempt by {at_username(username) if username else user_id} (ID: {user_id}).")
        return jsonify({"ok": True})
    
    # /example command
    if command == "/example":
        sample = "\n".join([
            "996770061141", "996770064514", "996770071665", "996770073284",
            "996770075145", "996770075627", "996770075973", "996770076350",
            "996770076869", "996770077101"
        ])
        res = enqueue_task(bot_id, user_id, username, sample)
        if not res["ok"]:
            send_message(bot_id, user_id, "❗ Could not queue demo. Try later.")
        else:
            start_user_worker_if_needed(bot_id, user_id)
            notify_user_worker(bot_id, user_id)
            active, queued = get_user_task_counts(bot_id, user_id)
            if active:
                send_message(bot_id, user_id, f"✅ Task added. Words: {res['total_words']}.\nQueue position: {queued}")
            else:
                send_message(bot_id, user_id, f"✅ Task added. Words: {res['total_words']}.")
        return jsonify({"ok": True})
    
    # /pause command
    if command == "/pause":
        conn = get_db_connection(bot_id)
        if not conn:
            send_message(bot_id, user_id, "⚠️ Service temporarily unavailable.")
            return jsonify({"ok": True})
        
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT id FROM tasks WHERE user_id=? AND status='running' LIMIT 1", (user_id,))
            row = c.fetchone()
        
        if not row:
            send_message(bot_id, user_id, "ℹ️ No active task to pause.")
        else:
            set_task_status(bot_id, row[0], "paused")
            notify_user_worker(bot_id, user_id)
            send_message(bot_id, user_id, "⏸️ Paused. Use /resume to continue.")
        return jsonify({"ok": True})
    
    # /resume command
    if command == "/resume":
        conn = get_db_connection(bot_id)
        if not conn:
            send_message(bot_id, user_id, "⚠️ Service temporarily unavailable.")
            return jsonify({"ok": True})
        
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT id FROM tasks WHERE user_id=? AND status='paused' LIMIT 1", (user_id,))
            row = c.fetchone()
        
        if not row:
            send_message(bot_id, user_id, "ℹ️ No paused task to resume.")
        else:
            set_task_status(bot_id, row[0], "running")
            notify_user_worker(bot_id, user_id)
            send_message(bot_id, user_id, "▶️ Resuming your task now.")
        return jsonify({"ok": True})
    
    # /status command
    if command == "/status":
        active, queued = get_user_task_counts(bot_id, user_id)
        
        if active > 0:
            conn = get_db_connection(bot_id)
            if conn:
                with BOT_STATES[bot_id]["db_lock"]:
                    c = conn.cursor()
                    c.execute("SELECT status, total_words, sent_count FROM tasks WHERE user_id=? AND status IN ('running','paused') LIMIT 1", (user_id,))
                    row = c.fetchone()
                    if row:
                        status, total, sent = row
                        remaining = (total or 0) - (sent or 0)
                        send_message(bot_id, user_id, f"ℹ️ Status: {status}\nRemaining words: {remaining}\nQueue size: {queued}")
                        return jsonify({"ok": True})
        
        if queued > 0:
            send_message(bot_id, user_id, f"⏳ Waiting. Queue size: {queued}")
        else:
            send_message(bot_id, user_id, "✅ You have no active or queued tasks.")
        return jsonify({"ok": True})
    
    # /stop command
    if command == "/stop":
        active, queued = get_user_task_counts(bot_id, user_id)
        stopped = cancel_active_task_for_user(bot_id, user_id)
        stop_user_worker(bot_id, user_id)
        
        if stopped > 0 or queued > 0:
            send_message(bot_id, user_id, "🛑 Active task stopped. Your queued tasks were cleared too.")
        else:
            send_message(bot_id, user_id, "ℹ️ You had no active or queued tasks.")
        return jsonify({"ok": True})
    
    # /stats command
    if command == "/stats":
        words = compute_last_12h_stats(bot_id, user_id)
        send_message(bot_id, user_id, f"📊 Your last 12 hours: {words} words split")
        return jsonify({"ok": True})
    
    # Unknown command
    send_message(bot_id, user_id, "❓ Unknown command.")
    return jsonify({"ok": True})

def handle_user_text(bot_id: str, user_id: int, username: str, text: str):
    """Handle regular user text input"""
    config = BOTS_CONFIG[bot_id]
    
    # Block owner task processing during operations
    if user_id in config["owner_ids"] and is_owner_in_operation(bot_id, user_id):
        logger.warning(f"Owner {user_id} text while in operation state in {bot_id}")
        return jsonify({"ok": True})
    
    # Check if user is allowed
    if user_id not in config["owner_ids"] and not is_allowed(bot_id, user_id):
        send_message(bot_id, user_id, f"🚫 Sorry, you are not allowed. {config['owner_tag']} notified.\nYour ID: {user_id}")
        notify_owners(bot_id, f"🚨 Unallowed access attempt by {at_username(username) if username else user_id} (ID: {user_id}).")
        return jsonify({"ok": True})
    
    # Check if suspended
    if is_suspended(bot_id, user_id):
        conn = get_db_connection(bot_id)
        if conn:
            with BOT_STATES[bot_id]["db_lock"]:
                c = conn.cursor()
                c.execute("SELECT suspended_until FROM suspended_users WHERE user_id=?", (user_id,))
                r = c.fetchone()
                until_wat = utc_to_wat_ts(r[0]) if r else "unknown"
                send_message(bot_id, user_id, f"⛔ You have been suspended until {until_wat} by {config['owner_tag']}.")
        return jsonify({"ok": True})
    
    # Enqueue task
    res = enqueue_task(bot_id, user_id, username, text)
    if not res["ok"]:
        if res["reason"] == "empty":
            send_message(bot_id, user_id, "⚠️ Empty text. Nothing to split.")
        elif res["reason"] == "queue_full":
            send_message(bot_id, user_id, f"⏳ Your queue is full ({res['queue_size']}). Use /stop or wait.")
        else:
            send_message(bot_id, user_id, "❗ Could not queue task. Try later.")
        return jsonify({"ok": True})
    
    # Start worker and notify
    start_user_worker_if_needed(bot_id, user_id)
    notify_user_worker(bot_id, user_id)
    active, queued = get_user_task_counts(bot_id, user_id)
    
    if active:
        send_message(bot_id, user_id, f"✅ Task added. Words: {res['total_words']}.\nQueue position: {queued}")
    else:
        send_message(bot_id, user_id, f"✅ Task added. Words: {res['total_words']}.")
    
    return jsonify({"ok": True})

# ===================== WEBHOOK HANDLER =====================
def handle_webhook(bot_id: str):
    """Handle webhook updates for bot"""
    try:
        update = request.get_json(force=True)
    except Exception:
        return jsonify({"ok": False}), 400
    
    try:
        config = BOTS_CONFIG[bot_id]
        
        # Handle callback queries
        if "callback_query" in update:
            return _handle_callback_query(bot_id, update["callback_query"])
        
        # Handle regular messages
        if "message" in update:
            msg = update["message"]
            user = msg.get("from", {})
            uid = user.get("id")
            username = user.get("username") or user.get("first_name", "")
            text = msg.get("text") or ""
            
            # Update username in database
            if is_allowed(bot_id, uid) or uid in config["owner_ids"]:
                conn = get_db_connection(bot_id)
                if conn:
                    try:
                        with BOT_STATES[bot_id]["db_lock"]:
                            c = conn.cursor()
                            c.execute("UPDATE allowed_users SET username=? WHERE user_id=?", (username or "", uid))
                            conn.commit()
                    except Exception:
                        pass
            
            # Handle owner input mode
            if uid in config["owner_ids"] and is_owner_in_operation(bot_id, uid):
                return _handle_owner_input(bot_id, uid, text)
            
            # Handle commands
            if text.startswith("/"):
                parts = text.split(None, 1)
                cmd = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                
                clear_owner_state(bot_id, uid)
                
                if cmd == "/ownersets":
                    if uid not in config["owner_ids"]:
                        send_message(bot_id, uid, f"🚫 Owner only. {config['owner_tag']} notified.")
                        notify_owners(bot_id, f"🚨 Unallowed /ownersets attempt by {at_username(username)} (ID: {uid}).")
                        return jsonify({"ok": True})
                    send_ownersets_menu(bot_id, uid)
                    return jsonify({"ok": True})
                else:
                    return handle_command(bot_id, uid, username, cmd, args)
            else:
                # Handle regular text
                return handle_user_text(bot_id, uid, username, text)
    
    except Exception:
        logger.exception("webhook handling error for %s", bot_id)
    
    return jsonify({"ok": True})

def _handle_callback_query(bot_id: str, callback: Dict) -> Any:
    """Handle callback query from inline keyboard"""
    config = BOTS_CONFIG[bot_id]
    user = callback.get("from", {})
    uid = user.get("id")
    data = callback.get("data", "")
    
    # Check if user is owner
    if uid not in config["owner_ids"]:
        try:
            get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery",
                                    json={"callback_query_id": callback.get("id"), "text": "⛔ Owner only."},
                                    timeout=2)
        except Exception:
            pass
        return jsonify({"ok": True})
    
    # Handle different callback actions
    if data == "owner_close":
        try:
            get_session(bot_id).post(f"{config['telegram_api']}/deleteMessage",
                                    json={"chat_id": callback["message"]["chat"]["id"],
                                          "message_id": callback["message"]["message_id"]},
                                    timeout=2)
        except Exception:
            pass
        clear_owner_state(bot_id, uid)
        return jsonify({"ok": True})
    
    elif data == "owner_backtomenu":
        send_ownersets_menu(bot_id, uid)
        try:
            get_session(bot_id).post(f"{config['telegram_api']}/deleteMessage",
                                    json={"chat_id": callback["message"]["chat"]["id"],
                                          "message_id": callback["message"]["message_id"]},
                                    timeout=2)
        except Exception:
            pass
        return jsonify({"ok": True})
    
    elif data in ["owner_botinfo", "owner_listusers", "owner_listsuspended"]:
        return _handle_owner_menu_action(bot_id, uid, callback, data)
    
    elif data in ["owner_adduser", "owner_suspend", "owner_unsuspend", "owner_checkallpreview"]:
        operation = data.replace("owner_", "")
        set_owner_state(bot_id, uid, {"operation": operation, "step": 0})
        
        prompts = {
            "adduser": "👤 Please send the User ID to add (multiple IDs separated by spaces or commas):",
            "suspend": "⏸️ Please send:\n1. User ID\n2. Duration (e.g., 30s, 10m, 2h, 1d, 1d2h, 2h30m)\n3. Optional reason",
            "unsuspend": "▶️ Please send the User ID to unsuspend:",
            "checkallpreview": "⏰ How many hours back should I check? (e.g., 1, 6, 24, 168):"
        }
        
        cancel_keyboard = {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "owner_cancelinput"}]]}
        send_message(bot_id, uid, f"⚠️ {prompts[operation]}\n\nPlease send the requested information.", cancel_keyboard)
        return jsonify({"ok": True})
    
    elif data == "owner_cancelinput":
        clear_owner_state(bot_id, uid)
        try:
            get_session(bot_id).post(f"{config['telegram_api']}/deleteMessage",
                                    json={"chat_id": callback["message"]["chat"]["id"],
                                          "message_id": callback["message"]["message_id"]},
                                    timeout=2)
        except Exception:
            pass
        return jsonify({"ok": True})
    
    # Answer callback query
    try:
        get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery",
                                json={"callback_query_id": callback.get("id")},
                                timeout=2)
    except Exception:
        pass
    
    return jsonify({"ok": True})

def _handle_owner_menu_action(bot_id: str, owner_id: int, callback: Dict, action: str) -> Any:
    """Handle owner menu actions that display information"""
    config = BOTS_CONFIG[bot_id]
    
    if action == "owner_botinfo":
        # Get bot info
        conn = get_db_connection(bot_id)
        if not conn:
            try:
                get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery",
                                        json={"callback_query_id": callback.get("id"),
                                              "text": "⚠️ Database unavailable."},
                                        timeout=2)
            except Exception:
                pass
            return jsonify({"ok": True})
        
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM allowed_users")
            total_allowed = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM suspended_users")
            total_suspended = c.fetchone()[0]
            c.execute("SELECT user_id, username, SUM(total_words - IFNULL(sent_count,0)), COUNT(*) FROM tasks WHERE status IN ('running','paused') GROUP BY user_id")
            active_rows = c.fetchall()
            c.execute("SELECT COUNT(*) FROM tasks WHERE status='queued'")
            queued_tasks = c.fetchone()[0]
        
        # Format active users info
        lines_active = []
        for uid, uname, rem, ac in active_rows:
            uname_display = at_username(uname) if uname else fetch_display_username(bot_id, uid)
            lines_active.append(f"{uid} ({uname_display}) - {int(rem)} remaining - {int(ac)} active")
        
        # Get stats
        stats_rows = compute_last_hour_stats(bot_id)
        lines_stats = []
        for uid, uname, s in stats_rows:
            uname_display = at_username(uname) if uname else fetch_display_username(bot_id, uid)
            lines_stats.append(f"{uid} ({uname_display}) - {int(s)} words sent")
        
        body = (
            f"🤖 {config['name']} Status\n"
            f"👥 Allowed users: {total_allowed}\n"
            f"🚫 Suspended users: {total_suspended}\n"
            f"⚙️ Active tasks: {len(active_rows)}\n"
            f"📨 Queued tasks: {queued_tasks}\n\n"
            "Users with active tasks:\n" + ("\n".join(lines_active) if lines_active else "(none)") + "\n\n"
            "User stats (last 1h):\n" + ("\n".join(lines_stats) if lines_stats else "(none)")
        )
        
    elif action == "owner_listusers":
        conn = get_db_connection(bot_id)
        if not conn:
            try:
                get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery",
                                        json={"callback_query_id": callback.get("id"),
                                              "text": "⚠️ Database unavailable."},
                                        timeout=2)
            except Exception:
                pass
            return jsonify({"ok": True})
        
        with BOT_STATES[bot_id]["db_lock"]:
            c = conn.cursor()
            c.execute("SELECT user_id, username, added_at FROM allowed_users ORDER BY added_at DESC")
            rows = c.fetchall()
        
        lines = []
        for uid, uname, added_at in rows:
            uname_s = f"({at_username(uname)})" if uname else "(no username)"
            added_wat = utc_to_wat_ts(added_at)
            lines.append(f"{uid} {uname_s} added={added_wat}")
        
        body = "👥 Allowed users:\n" + ("\n".join(lines) if lines else "(none)")
    
    elif action == "owner_listsuspended":
        # Auto-unsuspend expired first
        conn = get_db_connection(bot_id)
        if conn:
            with BOT_STATES[bot_id]["db_lock"]:
                c = conn.cursor()
                c.execute("SELECT user_id, suspended_until FROM suspended_users")
                rows = c.fetchall()
                now = datetime.utcnow()
                for uid, until_utc in rows:
                    try:
                        until = datetime.strptime(until_utc, "%Y-%m-%d %H:%M:%S")
                        if until <= now:
                            unsuspend_user(bot_id, uid)
                    except Exception:
                        pass
        
        rows = []
        conn = get_db_connection(bot_id)
        if conn:
            with BOT_STATES[bot_id]["db_lock"]:
                c = conn.cursor()
                c.execute("SELECT user_id, suspended_until, reason FROM suspended_users")
                rows = c.fetchall()
        
        if not rows:
            body = "✅ No suspended users."
        else:
            lines = []
            for uid, until_utc, reason in rows:
                until_wat = utc_to_wat_ts(until_utc)
                uname = fetch_display_username(bot_id, uid)
                uname_s = f"({at_username(uname)})" if uname else ""
                lines.append(f"{uid} {uname_s} until={until_wat} reason={reason}")
            body = "🚫 Suspended users:\n" + "\n".join(lines)
    
    # Update menu message
    menu_text = f"👑 Owner Menu {config['owner_tag']}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{body}"
    keyboard = [
        [{"text": "📊 Bot Info", "callback_data": "owner_botinfo"}, {"text": "👥 List Users", "callback_data": "owner_listusers"}],
        [{"text": "🚫 List Suspended", "callback_data": "owner_listsuspended"}, {"text": "➕ Add User", "callback_data": "owner_adduser"}],
        [{"text": "⏸️ Suspend User", "callback_data": "owner_suspend"}, {"text": "▶️ Unsuspend User", "callback_data": "owner_unsuspend"}],
        [{"text": "🔍 Check All User Preview", "callback_data": "owner_checkallpreview"}]
    ]
    
    try:
        get_session(bot_id).post(f"{config['telegram_api']}/editMessageText",
                                json={"chat_id": callback["message"]["chat"]["id"],
                                      "message_id": callback["message"]["message_id"],
                                      "text": menu_text,
                                      "reply_markup": {"inline_keyboard": keyboard}},
                                timeout=2)
    except Exception:
        pass
    
    return jsonify({"ok": True})

def _handle_owner_input(bot_id: str, owner_id: int, text: str) -> Any:
    """Handle owner text input during operations"""
    owner_state = get_owner_state(bot_id, owner_id)
    if not owner_state:
        return jsonify({"ok": True})
    
    operation = owner_state.get("operation")
    
    if operation == "adduser":
        parts = re.split(r"[,\s]+", text.strip())
        added, already, invalid = [], [], []
        
        for p in parts:
            if not p:
                continue
            try:
                tid = int(p)
                conn = get_db_connection(bot_id)
                if conn:
                    with BOT_STATES[bot_id]["db_lock"]:
                        c = conn.cursor()
                        c.execute("SELECT 1 FROM allowed_users WHERE user_id=?", (tid,))
                        if c.fetchone():
                            already.append(tid)
                        else:
                            c.execute("INSERT INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)",
                                     (tid, "", now_ts()))
                            conn.commit()
                            added.append(tid)
                            send_message(bot_id, tid, "✅ You have been added. Send any text to start.")
            except ValueError:
                invalid.append(p)
        
        parts_msgs = []
        if added: parts_msgs.append("Added: " + ", ".join(str(x) for x in added))
        if already: parts_msgs.append("Already present: " + ", ".join(str(x) for x in already))
        if invalid: parts_msgs.append("Invalid: " + ", ".join(invalid))
        
        clear_owner_state(bot_id, owner_id)
        send_message(bot_id, owner_id, f"✅ {'; '.join(parts_msgs) if parts_msgs else 'No changes'}\n\nUse /ownersets again for menu.")
        return jsonify({"ok": True})
    
    elif operation == "suspend":
        parts = text.split(maxsplit=2)
        if len(parts) < 2:
            send_message(bot_id, owner_id, "⚠️ Please provide both User ID and duration.")
            return jsonify({"ok": True})
        
        try:
            target = int(parts[0])
        except ValueError:
            send_message(bot_id, owner_id, "❌ Invalid User ID.")
            return jsonify({"ok": True})
        
        dur = parts[1]
        reason = parts[2] if len(parts) > 2 else ""
        
        result = parse_duration(dur)
        if result[0] is None:
            send_message(bot_id, owner_id, f"❌ {result[1]}")
            return jsonify({"ok": True})
        
        seconds, formatted_duration = result
        suspend_user(bot_id, target, seconds, reason)
        
        until_dt = datetime.utcnow() + timedelta(seconds=seconds)
        until_wat = format_datetime(until_dt + NIGERIA_TZ_OFFSET) + " WAT"
        reason_part = f"\nReason: {reason}" if reason else ""
        
        clear_owner_state(bot_id, owner_id)
        send_message(bot_id, owner_id, f"✅ User {label_for_owner_view(bot_id, target, fetch_display_username(bot_id, target))} suspended for {formatted_duration} (until {until_wat}).{reason_part}\n\nUse /ownersets again for menu.")
        return jsonify({"ok": True})
    
    elif operation == "unsuspend":
        try:
            target = int(text.strip())
        except ValueError:
            send_message(bot_id, owner_id, "❌ Invalid User ID.")
            return jsonify({"ok": True})
        
        ok = unsuspend_user(bot_id, target)
        clear_owner_state(bot_id, owner_id)
        
        if ok:
            send_message(bot_id, owner_id, f"✅ User {label_for_owner_view(bot_id, target, fetch_display_username(bot_id, target))} unsuspended.\n\nUse /ownersets again for menu.")
        else:
            send_message(bot_id, owner_id, f"ℹ️ User {target} is not suspended.\n\nUse /ownersets again for menu.")
        return jsonify({"ok": True})
    
    elif operation == "checkallpreview":
        try:
            hours = int(text.strip())
            if hours <= 0:
                raise ValueError
        except ValueError:
            send_message(bot_id, owner_id, "❌ Please enter a valid positive number.")
            return jsonify({"ok": True})
        
        all_users = get_all_users_ordered(bot_id)
        if not all_users:
            clear_owner_state(bot_id, owner_id)
            send_message(bot_id, owner_id, "📋 No users found.")
            return jsonify({"ok": True})
        
        first_user_id, first_username, first_added_at = all_users[0]
        username_display = at_username(first_username) if first_username else "no username"
        added_wat = utc_to_wat_ts(first_added_at)
        
        tasks, total_tasks, total_pages = get_user_tasks_preview(bot_id, first_user_id, hours, 0)
        
        if not tasks:
            body = f"👤 User: {first_user_id} ({username_display})\nAdded: {added_wat}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📋 No tasks found in the last {hours} hours."
        else:
            lines = []
            for task in tasks:
                lines.append(f"🕒 {task['created_at']}\n📝 Preview: {task['preview']}\n📊 Progress: {task['sent_count']}/{task['total_words']} words")
            
            body = f"👤 User: {first_user_id} ({username_display})\nAdded: {added_wat}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n📋 Tasks (last {hours}h, page 1/{total_pages}):\n\n" + "\n\n".join(lines)
        
        keyboard = []
        
        # Task navigation
        if total_pages > 1:
            keyboard.append([{"text": "Next Page ➡️", "callback_data": f"owner_checkallpreview_{first_user_id}_1_{hours}"}])
        
        # User navigation
        user_nav = [{"text": f"User 1/{len(all_users)}", "callback_data": "owner_checkallpreview_noop"}]
        if len(all_users) > 1:
            next_user_id = all_users[1][0]
            user_nav.append({"text": "Next User ➡️", "callback_data": f"owner_checkallpreview_{next_user_id}_0_{hours}"})
        keyboard.append(user_nav)
        
        keyboard.append([{"text": "🔙 Back to Menu", "callback_data": "owner_backtomenu"}])
        
        clear_owner_state(bot_id, owner_id)
        send_message(bot_id, owner_id, body, {"inline_keyboard": keyboard})
        return jsonify({"ok": True})
    
    return jsonify({"ok": True})

# ===================== FLASK ROUTES =====================
@app.route("/", methods=["GET"])
def root():
    return "Multi-Bot WordSplitter running.", 200

@app.route("/health/a", methods=["GET", "HEAD"])
def health_a():
    db_ok = check_db_health("bot_a")
    return jsonify({
        "ok": True,
        "bot": "A",
        "ts": now_display(),
        "db_connected": db_ok,
        "workers": len(BOT_STATES["bot_a"]["user_workers"])
    }), 200

@app.route("/health/b", methods=["GET", "HEAD"])
def health_b():
    db_ok = check_db_health("bot_b")
    return jsonify({
        "ok": True,
        "bot": "B",
        "ts": now_display(),
        "db_connected": db_ok,
        "workers": len(BOT_STATES["bot_b"]["user_workers"])
    }), 200

@app.route("/health/c", methods=["GET", "HEAD"])
def health_c():
    db_ok = check_db_health("bot_c")
    return jsonify({
        "ok": True,
        "bot": "C",
        "ts": now_display(),
        "db_connected": db_ok,
        "workers": len(BOT_STATES["bot_c"]["user_workers"])
    }), 200

@app.route("/webhook/a", methods=["POST"])
def webhook_a():
    return handle_webhook("bot_a")

@app.route("/webhook/b", methods=["POST"])
def webhook_b():
    return handle_webhook("bot_b")

@app.route("/webhook/c", methods=["POST"])
def webhook_c():
    return handle_webhook("bot_c")

# ===================== MAIN =====================
def main():
    """Main entry point"""
    # Set webhooks for all bots
    for bot_id in BOTS_CONFIG:
        config = BOTS_CONFIG[bot_id]
        if config["telegram_api"] and config["webhook_url"]:
            try:
                webhook_url = f"{config['webhook_url'].rstrip('/')}/webhook/{bot_id.split('_')[-1].lower()}"
                get_session(bot_id).post(f"{config['telegram_api']}/setWebhook",
                                        json={"url": webhook_url},
                                        timeout=REQUESTS_TIMEOUT)
                logger.info("Webhook set for %s to %s", bot_id, webhook_url)
            except Exception:
                logger.exception("set_webhook failed for %s", bot_id)
    
    # Start Flask app
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()
