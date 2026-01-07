#!/usr/bin/env python3

import os
import time
import json
import sqlite3
import threading
import logging
import re
import signal
import math
import ssl
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wordsplitter_multi")

app = Flask(__name__)

# ============================================================================
# BOT CONFIGURATION - ADD NEW BOTS HERE
# ============================================================================
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

# CONFIGURE ALL BOTS HERE
BOT_CONFIGS = {
    "bot_a": {
        "token": os.environ.get("TELEGRAM_TOKEN_A", ""),
        "webhook_url": os.environ.get("WEBHOOK_URL_A", ""),
        "owner_ids": parse_id_list(os.environ.get("OWNER_IDS_A", "")),
        "allowed_users": parse_id_list(os.environ.get("ALLOWED_USERS_A", "")),
        "db_path": os.environ.get("DB_PATH_A", ""),
        "use_slow_interval": os.environ.get("USE_SLOW_INTERVAL_A", "false").lower() == "true",
        "owner_tag": os.environ.get("OWNER_TAG_A", "Owner (@justmemmy)"),
        # Per-bot overrides or use shared defaults
        "max_queue_per_user": int(os.environ.get("MAX_QUEUE_PER_USER_A", os.environ.get("MAX_QUEUE_PER_USER", "5"))),
        "max_msg_per_second": float(os.environ.get("MAX_MSG_PER_SECOND_A", os.environ.get("MAX_MSG_PER_SECOND", "50"))),
        "max_concurrent_workers": int(os.environ.get("MAX_CONCURRENT_WORKERS_A", os.environ.get("MAX_CONCURRENT_WORKERS", "25"))),
        "requests_timeout": float(os.environ.get("REQUESTS_TIMEOUT_A", os.environ.get("REQUESTS_TIMEOUT", "10"))),
        "log_retention_days": int(os.environ.get("LOG_RETENTION_DAYS_A", os.environ.get("LOG_RETENTION_DAYS", "30"))),
        "failure_notify_threshold": int(os.environ.get("FAILURE_NOTIFY_THRESHOLD_A", os.environ.get("FAILURE_NOTIFY_THRESHOLD", "6"))),
        "permanent_suspend_days": int(os.environ.get("PERMANENT_SUSPEND_DAYS_A", os.environ.get("PERMANENT_SUSPEND_DAYS", "365"))),
    },
    "bot_b": {
        "token": os.environ.get("TELEGRAM_TOKEN_B", ""),
        "webhook_url": os.environ.get("WEBHOOK_URL_B", ""),
        "owner_ids": parse_id_list(os.environ.get("OWNER_IDS_B", "")),
        "allowed_users": parse_id_list(os.environ.get("ALLOWED_USERS_B", "")),
        "db_path": os.environ.get("DB_PATH_B", ""),
        "use_slow_interval": os.environ.get("USE_SLOW_INTERVAL_B", "false").lower() == "true",
        "owner_tag": os.environ.get("OWNER_TAG_B", "Owner"),
        "max_queue_per_user": int(os.environ.get("MAX_QUEUE_PER_USER_B", os.environ.get("MAX_QUEUE_PER_USER", "5"))),
        "max_msg_per_second": float(os.environ.get("MAX_MSG_PER_SECOND_B", os.environ.get("MAX_MSG_PER_SECOND", "50"))),
        "max_concurrent_workers": int(os.environ.get("MAX_CONCURRENT_WORKERS_B", os.environ.get("MAX_CONCURRENT_WORKERS", "25"))),
        "requests_timeout": float(os.environ.get("REQUESTS_TIMEOUT_B", os.environ.get("REQUESTS_TIMEOUT", "10"))),
        "log_retention_days": int(os.environ.get("LOG_RETENTION_DAYS_B", os.environ.get("LOG_RETENTION_DAYS", "30"))),
        "failure_notify_threshold": int(os.environ.get("FAILURE_NOTIFY_THRESHOLD_B", os.environ.get("FAILURE_NOTIFY_THRESHOLD", "6"))),
        "permanent_suspend_days": int(os.environ.get("PERMANENT_SUSPEND_DAYS_B", os.environ.get("PERMANENT_SUSPEND_DAYS", "365"))),
    },
    "bot_c": {
        "token": os.environ.get("TELEGRAM_TOKEN_C", ""),
        "webhook_url": os.environ.get("WEBHOOK_URL_C", ""),
        "owner_ids": parse_id_list(os.environ.get("OWNER_IDS_C", "")),
        "allowed_users": parse_id_list(os.environ.get("ALLOWED_USERS_C", "")),
        "db_path": os.environ.get("DB_PATH_C", ""),
        "use_slow_interval": os.environ.get("USE_SLOW_INTERVAL_C", "true").lower() == "true",  # SLOW INTERVAL!
        "owner_tag": os.environ.get("OWNER_TAG_C", "Owner"),
        "max_queue_per_user": int(os.environ.get("MAX_QUEUE_PER_USER_C", os.environ.get("MAX_QUEUE_PER_USER", "5"))),
        "max_msg_per_second": float(os.environ.get("MAX_MSG_PER_SECOND_C", os.environ.get("MAX_MSG_PER_SECOND", "50"))),
        "max_concurrent_workers": int(os.environ.get("MAX_CONCURRENT_WORKERS_C", os.environ.get("MAX_CONCURRENT_WORKERS", "25"))),
        "requests_timeout": float(os.environ.get("REQUESTS_TIMEOUT_C", os.environ.get("REQUESTS_TIMEOUT", "10"))),
        "log_retention_days": int(os.environ.get("LOG_RETENTION_DAYS_C", os.environ.get("LOG_RETENTION_DAYS", "30"))),
        "failure_notify_threshold": int(os.environ.get("FAILURE_NOTIFY_THRESHOLD_C", os.environ.get("FAILURE_NOTIFY_THRESHOLD", "6"))),
        "permanent_suspend_days": int(os.environ.get("PERMANENT_SUSPEND_DAYS_C", os.environ.get("PERMANENT_SUSPEND_DAYS", "365"))),
    }
}

# Remove bots with no token
ACTIVE_BOTS = {bot_id: config for bot_id, config in BOT_CONFIGS.items() if config["token"]}
if not ACTIVE_BOTS:
    logger.warning("No bots configured with tokens!")

# ============================================================================
# TIME & UTILITY FUNCTIONS
# ============================================================================
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

def at_username(u: str) -> str:
    if not u:
        return ""
    return u.lstrip("@")

def label_for_self(bot_id: str, viewer_id: int, username: str) -> str:
    config = ACTIVE_BOTS[bot_id]
    if username:
        if viewer_id in config["owner_ids"]:
            return f"{at_username(username)} (ID: {viewer_id})"
        return f"{at_username(username)}"
    return f"(ID: {viewer_id})" if viewer_id in config["owner_ids"] else ""

def label_for_owner_view(bot_id: str, target_id: int, target_username: str) -> str:
    if target_username:
        return f"{at_username(target_username)} (ID: {target_id})"
    return str(target_id)

# ============================================================================
# DATABASE MANAGEMENT (PER-BOT) - FIXED INITIALIZATION
# ============================================================================
DB_CONNECTIONS: Dict[str, sqlite3.Connection] = {}
DB_LOCKS: Dict[str, threading.RLock] = {}

# Initialize ALL dictionaries for ALL configured bots first (FIX for KeyError)
for bot_id in BOT_CONFIGS.keys():
    DB_LOCKS[bot_id] = threading.RLock()
    DB_CONNECTIONS[bot_id] = None  # Will be initialized later if bot is active

def _ensure_db_parent(dirpath: str):
    try:
        if dirpath and not os.path.exists(dirpath):
            os.makedirs(dirpath, exist_ok=True)
    except Exception as e:
        logger.warning("Could not create DB parent directory %s: %s", dirpath, e)

def init_db_for_bot(bot_id: str):
    """Initialize database for a specific bot with improved path handling"""
    config = ACTIVE_BOTS[bot_id]
    db_path = config["db_path"]
    
    # Apply improved path handling from app.py for ALL bots
    if not db_path or db_path == "botdata.sqlite3":
        db_path = f"/tmp/botdata_{bot_id}.sqlite3"
        config["db_path"] = db_path
    
    # Ensure the directory exists
    parent = os.path.dirname(os.path.abspath(db_path))
    if parent:
        _ensure_db_parent(parent)
    
    # Also try to create the file if it doesn't exist (for permissions)
    try:
        if not os.path.exists(db_path):
            with open(db_path, 'w') as f:
                f.write('')  # Create empty file
            logger.info("Created DB file for %s: %s", bot_id, db_path)
    except Exception as e:
        logger.warning("Could not create DB file %s for %s: %s", db_path, bot_id, e)

    def _create_schema(conn):
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
            finished_at TEXT,
            last_activity TEXT,
            retry_count INTEGER DEFAULT 0
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
            last_failure_at TEXT,
            notified INTEGER DEFAULT 0,
            last_error_code INTEGER,
            last_error_desc TEXT
        )""")
        conn.commit()

    try:
        conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-2000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=30000;")
        _create_schema(conn)
        DB_CONNECTIONS[bot_id] = conn
        logger.info("DB initialized for %s at %s", bot_id, db_path)
    except Exception:
        logger.exception("Failed to open DB for %s at %s, falling back to in-memory DB", bot_id, db_path)
        try:
            conn = sqlite3.connect(":memory:", timeout=30, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA cache_size=-2000;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA busy_timeout=30000;")
            _create_schema(conn)
            DB_CONNECTIONS[bot_id] = conn
            logger.info("In-memory DB initialized for %s", bot_id)
        except Exception:
            DB_CONNECTIONS[bot_id] = None
            logger.exception("Failed to initialize in-memory DB for %s", bot_id)

def ensure_send_failures_columns_for_bot(bot_id: str):
    """Ensure migration for a specific bot's database"""
    if not DB_CONNECTIONS.get(bot_id):
        return
    
    try:
        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            c.execute("PRAGMA table_info(send_failures)")
            cols = [r[1] for r in c.fetchall()]
            to_add = []
            if "notified" not in cols:
                to_add.append("ALTER TABLE send_failures ADD COLUMN notified INTEGER DEFAULT 0")
            if "last_error_code" not in cols:
                to_add.append("ALTER TABLE send_failures ADD COLUMN last_error_code INTEGER")
            if "last_error_desc" not in cols:
                to_add.append("ALTER TABLE send_failures ADD COLUMN last_error_desc TEXT")
            for stmt in to_add:
                try:
                    c.execute(stmt)
                except Exception:
                    logger.debug("Migration statement failed for %s: %s", bot_id, stmt)
            DB_CONNECTIONS[bot_id].commit()
    except Exception:
        logger.exception("ensure_send_failures_columns failed for %s", bot_id)

def initialize_all_bots():
    """Initialize all active bots"""
    for bot_id in ACTIVE_BOTS.keys():
        init_db_for_bot(bot_id)
        if DB_CONNECTIONS.get(bot_id):
            ensure_send_failures_columns_for_bot(bot_id)
        
        # Ensure owners auto-added as allowed for this bot
        config = ACTIVE_BOTS[bot_id]
        for oid in config["owner_ids"]:
            try:
                with DB_LOCKS[bot_id]:
                    c = DB_CONNECTIONS[bot_id].cursor()
                    c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (oid,))
                    exists = c.fetchone()
                    if not exists:
                        c.execute("INSERT OR REPLACE INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)", 
                                 (oid, "", now_ts()))
                        DB_CONNECTIONS[bot_id].commit()
            except Exception:
                logger.exception("Error ensuring owner in allowed_users for %s", bot_id)
        
        # Ensure provided ALLOWED_USERS auto-added for this bot
        for uid in config["allowed_users"]:
            if uid in config["owner_ids"]:
                continue
            try:
                with DB_LOCKS[bot_id]:
                    c = DB_CONNECTIONS[bot_id].cursor()
                    c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (uid,))
                    rows = c.fetchone()
                    if not rows:
                        c.execute("INSERT INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)",
                                 (uid, "", now_ts()))
                        DB_CONNECTIONS[bot_id].commit()
                try:
                    telegram_api = f"https://api.telegram.org/bot{config['token']}"
                    _session.post(f"{telegram_api}/sendMessage", json={
                        "chat_id": uid, "text": f"✅ You have been added. Send any text to start."
                    }, timeout=3)
                except Exception:
                    pass
            except Exception:
                logger.exception("Auto-add allowed user error for %s", bot_id)

# ============================================================================
# BOT VALIDATION HELPER (FIX for KeyError)
# ============================================================================
def is_bot_valid(bot_id: str) -> bool:
    """Check if a bot is properly initialized before accessing its resources"""
    return (bot_id in ACTIVE_BOTS and 
            bot_id in DB_CONNECTIONS and 
            DB_CONNECTIONS[bot_id] is not None)

# ============================================================================
# REQUESTS SESSION CONFIGURATION (SHARED)
# ============================================================================
_session = requests.Session()

try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST", "GET"]
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=100,
        pool_maxsize=100,
        pool_block=False
    )
    
    _session.mount("https://", adapter)
    _session.mount("http://", adapter)
    
    _session.verify = False
    
    _session.headers.update({
        'User-Agent': 'Mozilla/5.0 (compatible; WordSplitterBot/1.0)',
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    })
    
except Exception as e:
    logger.warning("Could not configure advanced session settings: %s", e)
    try:
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
        _session.mount("https://", adapter)
        _session.mount("http://", adapter)
    except Exception:
        pass

# ============================================================================
# TOKEN BUCKET RATE LIMITING (PER-BOT)
# ============================================================================
class TokenBucket:
    def __init__(self, rate_per_sec: float):
        self.capacity = max(1.0, rate_per_sec)
        self.tokens = self.capacity
        self.rate = rate_per_sec
        self.last = time.monotonic()
        self.cond = threading.Condition()

    def acquire(self, timeout=10.0) -> bool:
        end = time.monotonic() + timeout
        with self.cond:
            while True:
                now = time.monotonic()
                elapsed = now - self.last
                if elapsed > 0:
                    refill = elapsed * self.rate
                    self.tokens = min(self.capacity, self.tokens + refill)
                    self.last = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return True
                remaining = end - time.monotonic()
                if remaining <= 0:
                    return False
                wait_time = min(remaining, max(0.01, (1.0 / max(1.0, self.rate))))
                self.cond.wait(timeout=wait_time)

    def notify_all(self):
        with self.cond:
            self.cond.notify_all()

TOKEN_BUCKETS: Dict[str, TokenBucket] = {}
for bot_id in ACTIVE_BOTS.keys():
    TOKEN_BUCKETS[bot_id] = TokenBucket(ACTIVE_BOTS[bot_id]["max_msg_per_second"])

def acquire_token(bot_id: str, timeout=10.0):
    if bot_id not in TOKEN_BUCKETS:
        logger.error(f"Token bucket not found for {bot_id}")
        return False
    return TOKEN_BUCKETS[bot_id].acquire(timeout=timeout)

# ============================================================================
# TELEGRAM MESSAGE SENDING (PER-BOT)
# ============================================================================
def get_telegram_api(bot_id: str) -> str:
    config = ACTIVE_BOTS[bot_id]
    return f"https://api.telegram.org/bot{config['token']}"

def parse_telegram_json(resp):
    try:
        return resp.json()
    except Exception:
        return None

def _utf16_len(s: str) -> int:
    if not s:
        return 0
    return len(s.encode("utf-16-le")) // 2

def _build_entities_for_text(text: str):
    if not text:
        return None
    entities = []
    for m in re.finditer(r"\b\d+\b", text):
        py_start = m.start()
        py_end = m.end()
        utf16_offset = _utf16_len(text[:py_start])
        utf16_length = _utf16_len(text[py_start:py_end])
        entities.append({"type": "code", "offset": utf16_offset, "length": utf16_length})
    return entities if entities else None

def is_permanent_telegram_error(code: int, description: str = "") -> bool:
    try:
        if code in (400, 403):
            return True
    except Exception:
        pass
    if description:
        desc = description.lower()
        if "bot was blocked" in desc or "chat not found" in desc or "user is deactivated" in desc or "forbidden" in desc:
            return True
    return False

def record_failure_for_bot(bot_id: str, user_id: int, inc: int = 1, error_code: int = None, description: str = "", is_permanent: bool = False):
    if not is_bot_valid(bot_id):
        logger.error(f"Cannot record failure for invalid bot: {bot_id}")
        return
    
    try:
        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            c.execute("SELECT failures, notified FROM send_failures WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            if not row:
                failures = inc
                notified = 0
                c.execute("INSERT INTO send_failures (user_id, failures, last_failure_at, notified, last_error_code, last_error_desc) VALUES (?, ?, ?, ?, ?, ?)",
                         (user_id, failures, now_ts(), 0, error_code, description))
            else:
                failures = int(row[0] or 0) + inc
                notified = int(row[1] or 0)
                c.execute("UPDATE send_failures SET failures = ?, last_failure_at = ?, last_error_code = ?, last_error_desc = ? WHERE user_id = ?",
                         (failures, now_ts(), error_code, description, user_id))
            DB_CONNECTIONS[bot_id].commit()

        config = ACTIVE_BOTS[bot_id]
        
        if is_permanent or is_permanent_telegram_error(error_code or 0, description):
            mark_user_permanently_unreachable_for_bot(bot_id, user_id, error_code, description)
            return

        if failures >= config["failure_notify_threshold"] and notified == 0:
            try:
                with DB_LOCKS[bot_id]:
                    c = DB_CONNECTIONS[bot_id].cursor()
                    c.execute("UPDATE send_failures SET notified = 1 WHERE user_id = ?", (user_id,))
                    DB_CONNECTIONS[bot_id].commit()
            except Exception:
                logger.exception("Failed to set notified flag for %s in %s", user_id, bot_id)
            notify_owners_for_bot(bot_id, f"⚠️ Repeated send failures for {user_id} ({failures}). Stopping their tasks. 🛑")
            cancel_active_task_for_user_for_bot(bot_id, user_id)
    except Exception:
        logger.exception("record_failure error for %s in %s", user_id, bot_id)

def reset_failures_for_bot(bot_id: str, user_id: int):
    if not is_bot_valid(bot_id):
        return
    
    try:
        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            c.execute("DELETE FROM send_failures WHERE user_id = ?", (user_id,))
            DB_CONNECTIONS[bot_id].commit()
    except Exception:
        logger.exception("reset_failures failed for %s in %s", user_id, bot_id)

def send_message(bot_id: str, chat_id: int, text: str, reply_markup: Optional[Dict] = None):
    if not is_bot_valid(bot_id):
        logger.error(f"Cannot send message from invalid bot: {bot_id}")
        return None
    
    config = ACTIVE_BOTS[bot_id]
    telegram_api = get_telegram_api(bot_id)
    
    if not telegram_api:
        logger.error("No token for %s; cannot send message.", bot_id)
        return None

    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    entities = _build_entities_for_text(text)
    if entities:
        payload["entities"] = entities
    if reply_markup:
        payload["reply_markup"] = reply_markup

    if not acquire_token(bot_id, timeout=5.0):
        logger.warning("Token acquire timed out for %s; dropping send to %s", bot_id, chat_id)
        record_failure_for_bot(bot_id, chat_id, inc=1, description="token_acquire_timeout")
        return None

    max_attempts = 3
    attempt = 0
    backoff_base = 0.5
    
    while attempt < max_attempts:
        attempt += 1
        try:
            resp = _session.post(f"{telegram_api}/sendMessage", json=payload, timeout=config["requests_timeout"])
        except requests.exceptions.SSLError as e:
            logger.warning("SSL send error from %s to %s (attempt %s): %s", bot_id, chat_id, attempt, e)
            if attempt >= max_attempts:
                logger.error("SSL error persists for %s to %s after %s attempts", bot_id, chat_id, max_attempts)
                return None
            time.sleep(backoff_base * (4 ** (attempt - 1)))
            continue
        except requests.exceptions.ConnectionError as e:
            logger.warning("Connection send error from %s to %s (attempt %s): %s", bot_id, chat_id, attempt, e)
            if attempt >= max_attempts:
                record_failure_for_bot(bot_id, chat_id, inc=1, description=f"connection_error: {str(e)}")
                return None
            time.sleep(backoff_base * (2 ** (attempt - 1)))
            continue
        except requests.exceptions.Timeout as e:
            logger.warning("Timeout send error from %s to %s (attempt %s): %s", bot_id, chat_id, attempt, e)
            if attempt >= max_attempts:
                record_failure_for_bot(bot_id, chat_id, inc=1, description=f"timeout: {str(e)}")
                return None
            time.sleep(backoff_base * (2 ** (attempt - 1)))
            continue
        except requests.exceptions.RequestException as e:
            logger.warning("Network send error from %s to %s (attempt %s): %s", bot_id, chat_id, attempt, e)
            if attempt >= max_attempts:
                record_failure_for_bot(bot_id, chat_id, inc=1, description=str(e))
                return None
            time.sleep(backoff_base * (2 ** (attempt - 1)))
            continue

        data = parse_telegram_json(resp)
        if not isinstance(data, dict):
            logger.warning("Unexpected non-json response for sendMessage from %s to %s", bot_id, chat_id)
            if attempt >= max_attempts:
                record_failure_for_bot(bot_id, chat_id, inc=1, description="non_json_response")
                return None
            time.sleep(backoff_base * (2 ** (attempt - 1)))
            continue

        if data.get("ok"):
            try:
                mid = data["result"].get("message_id")
                if mid:
                    with DB_LOCKS[bot_id]:
                        c = DB_CONNECTIONS[bot_id].cursor()
                        c.execute("INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (?, ?, ?, 0)", 
                                 (chat_id, mid, now_ts()))
                        DB_CONNECTIONS[bot_id].commit()
            except Exception:
                logger.exception("record sent message failed for %s", bot_id)
            reset_failures_for_bot(bot_id, chat_id)
            return data["result"]

        error_code = data.get("error_code")
        description = data.get("description", "")
        params = data.get("parameters") or {}
        
        if error_code == 429:
            retry_after = params.get("retry_after")
            if retry_after is None:
                retry_after = 1
            try:
                retry_after = int(retry_after)
            except Exception:
                retry_after = 1
            logger.info("Rate limited for %s to %s: retry_after=%s", bot_id, chat_id, retry_after)
            time.sleep(max(0.5, retry_after))
            if attempt >= max_attempts:
                record_failure_for_bot(bot_id, chat_id, inc=1, error_code=error_code, description=description)
                return None
            continue

        if is_permanent_telegram_error(error_code or 0, description):
            logger.info("Permanent error for %s to %s: %s %s", bot_id, chat_id, error_code, description)
            record_failure_for_bot(bot_id, chat_id, inc=1, error_code=error_code, description=description, is_permanent=True)
            return None

        logger.warning("Transient/send error for %s to %s: %s %s", bot_id, chat_id, error_code, description)
        if attempt >= max_attempts:
            record_failure_for_bot(bot_id, chat_id, inc=1, error_code=error_code, description=description)
            return None
        time.sleep(backoff_base * (2 ** (attempt - 1)))

def mark_user_permanently_unreachable_for_bot(bot_id: str, user_id: int, error_code: int = None, description: str = ""):
    if not is_bot_valid(bot_id):
        return
    
    try:
        config = ACTIVE_BOTS[bot_id]
        
        if user_id in config["owner_ids"]:
            with DB_LOCKS[bot_id]:
                c = DB_CONNECTIONS[bot_id].cursor()
                c.execute("INSERT OR REPLACE INTO send_failures (user_id, failures, last_failure_at, notified, last_error_code, last_error_desc) VALUES (?, ?, ?, ?, ?, ?)",
                         (user_id, config["failure_notify_threshold"], now_ts(), 1, error_code, description))
                DB_CONNECTIONS[bot_id].commit()
            notify_owners_for_bot(bot_id, f"⚠️ Repeated send failures for owner {user_id}. Please investigate. Error: {error_code} {description}")
            return

        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            c.execute("INSERT OR REPLACE INTO send_failures (user_id, failures, last_failure_at, notified, last_error_code, last_error_desc) VALUES (?, ?, ?, ?, ?, ?)",
                     (user_id, 999, now_ts(), 1, error_code, description))
            DB_CONNECTIONS[bot_id].commit()

        cancel_active_task_for_user_for_bot(bot_id, user_id)
        suspend_user_for_bot(bot_id, user_id, config["permanent_suspend_days"] * 24 * 3600, f"Permanent send failure: {error_code} {description}")

        notify_owners_for_bot(bot_id, f"⚠️ Repeated send failures for {user_id} ({error_code}). Stopping their tasks. 🛑 Error: {description}")
    except Exception:
        logger.exception("mark_user_permanently_unreachable failed for %s in %s", user_id, bot_id)

# ============================================================================
# TASK MANAGEMENT (PER-BOT)
# ============================================================================
def split_text_to_words(text: str) -> List[str]:
    return [w for w in text.strip().split() if w]

def get_message_interval(bot_id: str, total_words: int) -> float:
    config = ACTIVE_BOTS[bot_id]
    if config["use_slow_interval"]:
        # app (1).py intervals
        return 1.0 if total <= 150 else (1.1 if total <= 300 else 1.2)
    else:
        # app.py intervals
        return 0.5 if total <= 150 else (0.6 if total <= 300 else 0.7)

def enqueue_task_for_bot(bot_id: str, user_id: int, username: str, text: str):
    if not is_bot_valid(bot_id):
        return {"ok": False, "reason": "bot_not_initialized"}
    
    config = ACTIVE_BOTS[bot_id]
    words = split_text_to_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        pending = c.fetchone()[0]
        if pending >= config["max_queue_per_user"]:
            return {"ok": False, "reason": "queue_full", "queue_size": pending}
        try:
            c.execute("INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at, sent_count, last_activity, retry_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                     (user_id, username, text, json.dumps(words), total, "queued", now_ts(), 0, now_ts(), 0))
            DB_CONNECTIONS[bot_id].commit()
        except Exception:
            logger.exception("enqueue_task db error for %s", bot_id)
            return {"ok": False, "reason": "db_error"}
    return {"ok": True, "total_words": total, "queue_size": pending + 1}

def get_next_task_for_user_for_bot(bot_id: str, user_id: int):
    if not is_bot_valid(bot_id):
        return None
    
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT id, words_json, total_words, text, retry_count FROM tasks WHERE user_id = ? AND status = 'queued' ORDER BY id ASC LIMIT 1", (user_id,))
        r = c.fetchone()
    if not r:
        return None
    return {"id": r[0], "words": json.loads(r[1]) if r[1] else split_text_to_words(r[3]), "total_words": r[2], "text": r[3], "retry_count": r[4]}

def set_task_status_for_bot(bot_id: str, task_id: int, status: str):
    if not is_bot_valid(bot_id):
        return
    
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        if status == "running":
            c.execute("UPDATE tasks SET status = ?, started_at = ?, last_activity = ? WHERE id = ?", (status, now_ts(), now_ts(), task_id))
        elif status in ("done", "cancelled"):
            c.execute("UPDATE tasks SET status = ?, finished_at = ?, last_activity = ? WHERE id = ?", (status, now_ts(), now_ts(), task_id))
        else:
            c.execute("UPDATE tasks SET status = ?, last_activity = ? WHERE id = ?", (status, now_ts(), task_id))
        DB_CONNECTIONS[bot_id].commit()

def update_task_activity_for_bot(bot_id: str, task_id: int):
    if not is_bot_valid(bot_id):
        return
    
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("UPDATE tasks SET last_activity = ? WHERE id = ?", (now_ts(), task_id))
        DB_CONNECTIONS[bot_id].commit()

def increment_task_retry_for_bot(bot_id: str, task_id: int):
    if not is_bot_valid(bot_id):
        return
    
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("UPDATE tasks SET retry_count = retry_count + 1, last_activity = ? WHERE id = ?", (now_ts(), task_id))
        DB_CONNECTIONS[bot_id].commit()

def cancel_active_task_for_user_for_bot(bot_id: str, user_id: int):
    if not is_bot_valid(bot_id):
        return 0
    
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT id FROM tasks WHERE user_id = ? AND status IN ('queued','running','paused')", (user_id,))
        rows = c.fetchall()
        count = 0
        for r in rows:
            tid = r[0]
            c.execute("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", ("cancelled", now_ts(), tid))
            count += 1
        DB_CONNECTIONS[bot_id].commit()
    notify_user_worker_for_bot(bot_id, user_id)
    return count

def record_split_log_for_bot(bot_id: str, user_id: int, username: str, count: int = 1):
    if not is_bot_valid(bot_id):
        return
    
    try:
        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            now = now_ts()
            entries = [(user_id, username, 1, now) for _ in range(count)]
            c.executemany("INSERT INTO split_logs (user_id, username, words, created_at) VALUES (?, ?, ?, ?)", entries)
            DB_CONNECTIONS[bot_id].commit()
    except Exception:
        logger.exception("record_split_log error for %s", bot_id)

# ============================================================================
# USER MANAGEMENT (PER-BOT)
# ============================================================================
def is_allowed_for_bot(bot_id: str, user_id: int) -> bool:
    if not is_bot_valid(bot_id):
        return False
    
    config = ACTIVE_BOTS[bot_id]
    if user_id in config["owner_ids"]:
        return True
    
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,))
        return bool(c.fetchone())

def suspend_user_for_bot(bot_id: str, target_id: int, seconds: int, reason: str = ""):
    if not is_bot_valid(bot_id):
        return
    
    config = ACTIVE_BOTS[bot_id]
    until_utc_str = (datetime.utcnow() + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
    until_wat_str = utc_to_wat_ts(until_utc_str)
    try:
        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            c.execute("INSERT OR REPLACE INTO suspended_users (user_id, suspended_until, reason, added_at) VALUES (?, ?, ?, ?)",
                     (target_id, until_utc_str, reason, now_ts()))
            DB_CONNECTIONS[bot_id].commit()
    except Exception:
        logger.exception("suspend_user db error for %s", bot_id)
    stopped = cancel_active_task_for_user_for_bot(bot_id, target_id)
    try:
        reason_text = f"\nReason: {reason}" if reason else ""
        send_message(bot_id, target_id, f"⛔ You have been suspended until {until_wat_str} by {config['owner_tag']}.{reason_text}")
    except Exception:
        logger.exception("notify suspended user failed for %s", bot_id)
    notify_owners_for_bot(bot_id, f"🔒 User suspended: {label_for_owner_view(bot_id, target_id, fetch_display_username_for_bot(bot_id, target_id))} suspended_until={until_wat_str} reason={reason}")

def unsuspend_user_for_bot(bot_id: str, target_id: int) -> bool:
    if not is_bot_valid(bot_id):
        return False
    
    config = ACTIVE_BOTS[bot_id]
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (target_id,))
        r = c.fetchone()
        if not r:
            return False
        c.execute("DELETE FROM suspended_users WHERE user_id = ?", (target_id,))
        DB_CONNECTIONS[bot_id].commit()
    try:
        send_message(bot_id, target_id, f"✅ You have been unsuspended by {config['owner_tag']}.")
    except Exception:
        logger.exception("notify unsuspended failed for %s", bot_id)
    notify_owners_for_bot(bot_id, f"🔓 Manual unsuspend: {label_for_owner_view(bot_id, target_id, fetch_display_username_for_bot(bot_id, target_id))}")
    return True

def list_suspended_for_bot(bot_id: str):
    if not is_bot_valid(bot_id):
        return []
    
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT user_id, suspended_until, reason, added_at FROM suspended_users ORDER BY suspended_until ASC")
        return c.fetchall()

def is_suspended_for_bot(bot_id: str, user_id: int) -> bool:
    if not is_bot_valid(bot_id):
        return False
    
    config = ACTIVE_BOTS[bot_id]
    if user_id in config["owner_ids"]:
        return False
    
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,))
        r = c.fetchone()
    if not r:
        return False
    try:
        until = datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S")
        return until > datetime.utcnow()
    except Exception:
        return False

def notify_owners_for_bot(bot_id: str, text: str):
    if not is_bot_valid(bot_id):
        return
    
    config = ACTIVE_BOTS[bot_id]
    for oid in config["owner_ids"]:
        try:
            send_message(bot_id, oid, text)
        except Exception:
            logger.exception("notify owner failed for %s in %s", oid, bot_id)

# ============================================================================
# WORKER MANAGEMENT (PER-BOT) - FIXED INITIALIZATION
# ============================================================================
_user_workers: Dict[str, Dict[int, Dict[str, Any]]] = {}  # bot_id -> user_id -> worker info
_user_workers_locks: Dict[str, threading.Lock] = {}
_active_workers_semaphores: Dict[str, threading.Semaphore] = {}

# Initialize ALL dictionaries for ALL configured bots first (FIX for KeyError)
for bot_id in BOT_CONFIGS.keys():
    _user_workers[bot_id] = {}
    _user_workers_locks[bot_id] = threading.Lock()
    # Initialize with default value, will be updated for active bots
    _active_workers_semaphores[bot_id] = threading.Semaphore(25)  # Default

# Now update semaphores for ACTIVE bots with their actual config
for bot_id in ACTIVE_BOTS.keys():
    _active_workers_semaphores[bot_id] = threading.Semaphore(ACTIVE_BOTS[bot_id]["max_concurrent_workers"])

def notify_user_worker_for_bot(bot_id: str, user_id: int):
    if bot_id not in _user_workers_locks:
        return
    
    with _user_workers_locks[bot_id]:
        info = _user_workers[bot_id].get(user_id)
        if info and "wake" in info:
            try:
                info["wake"].set()
            except Exception:
                pass

def start_user_worker_if_needed_for_bot(bot_id: str, user_id: int):
    if not is_bot_valid(bot_id):
        return
    
    with _user_workers_locks[bot_id]:
        info = _user_workers[bot_id].get(user_id)
        if info:
            thr = info.get("thread")
            if thr and thr.is_alive():
                return
        wake = threading.Event()
        stop = threading.Event()
        thr = threading.Thread(target=per_user_worker_loop_for_bot, args=(bot_id, user_id, wake, stop), daemon=True)
        _user_workers[bot_id][user_id] = {"thread": thr, "wake": wake, "stop": stop}
        thr.start()
        logger.info("Started worker for user %s in %s", user_id, bot_id)

def stop_user_worker_for_bot(bot_id: str, user_id: int, join_timeout: float = 0.5):
    if bot_id not in _user_workers_locks:
        return
    
    with _user_workers_locks[bot_id]:
        info = _user_workers[bot_id].get(user_id)
        if not info:
            return
        try:
            info["stop"].set()
            info["wake"].set()
            thr = info.get("thread")
            if thr and thr.is_alive():
                thr.join(join_timeout)
        except Exception:
            logger.exception("Error stopping worker for %s in %s", user_id, bot_id)
        finally:
            _user_workers[bot_id].pop(user_id, None)
            logger.info("Stopped worker for user %s in %s", user_id, bot_id)

def stop_all_workers_for_bot(bot_id: str):
    if bot_id not in _user_workers_locks:
        return
    
    with _user_workers_locks[bot_id]:
        user_ids = list(_user_workers[bot_id].keys())
    for uid in user_ids:
        stop_user_worker_for_bot(bot_id, uid, join_timeout=1.0)

def per_user_worker_loop_for_bot(bot_id: str, user_id: int, wake_event: threading.Event, stop_event: threading.Event):
    if not is_bot_valid(bot_id):
        logger.error("Worker loop cannot start for invalid bot: %s", bot_id)
        return
    
    logger.info("Worker loop starting for user %s in %s", user_id, bot_id)
    acquired_semaphore = False
    try:
        uname_for_stat = fetch_display_username_for_bot(bot_id, user_id) or str(user_id)
        while not stop_event.is_set():
            if is_suspended_for_bot(bot_id, user_id):
                cancel_active_task_for_user_for_bot(bot_id, user_id)
                try:
                    send_message(bot_id, user_id, f"⛔ You have been suspended; stopping your task.")
                except Exception:
                    pass
                while is_suspended_for_bot(bot_id, user_id) and not stop_event.is_set():
                    wake_event.wait(timeout=5.0)
                    wake_event.clear()
                continue

            task = get_next_task_for_user_for_bot(bot_id, user_id)
            if not task:
                wake_event.wait(timeout=1.0)
                wake_event.clear()
                continue

            task_id = task["id"]
            words = task["words"]
            total = int(task["total_words"] or len(words))
            retry_count = task.get("retry_count", 0)

            with DB_LOCKS[bot_id]:
                c = DB_CONNECTIONS[bot_id].cursor()
                c.execute("SELECT sent_count, status FROM tasks WHERE id = ?", (task_id,))
                sent_info = c.fetchone()

            if not sent_info or sent_info[1] == "cancelled":
                continue

            update_task_activity_for_bot(bot_id, task_id)

            # Acquire concurrency semaphore for THIS bot
            while not stop_event.is_set():
                acquired = _active_workers_semaphores[bot_id].acquire(timeout=1.0)
                if acquired:
                    acquired_semaphore = True
                    break
                update_task_activity_for_bot(bot_id, task_id)
                with DB_LOCKS[bot_id]:
                    c = DB_CONNECTIONS[bot_id].cursor()
                    c.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                    row_check = c.fetchone()
                if not row_check or row_check[0] == "cancelled":
                    break

            with DB_LOCKS[bot_id]:
                c = DB_CONNECTIONS[bot_id].cursor()
                c.execute("SELECT sent_count, status FROM tasks WHERE id = ?", (task_id,))
                sent_info = c.fetchone()
            if not sent_info or sent_info[1] == "cancelled":
                if acquired_semaphore:
                    _active_workers_semaphores[bot_id].release()
                    acquired_semaphore = False
                continue

            sent = int(sent_info[0] or 0)
            set_task_status_for_bot(bot_id, task_id, "running")

            if retry_count > 0:
                try:
                    send_message(bot_id, user_id, f"🔄 Retrying your task (attempt {retry_count + 1})...")
                except Exception:
                    pass

            interval = get_message_interval(bot_id, total)
            est_seconds = int((total - sent) * interval)
            est_str = str(timedelta(seconds=est_seconds))
            try:
                send_message(bot_id, user_id, f"🚀 Starting your split now. Words: {total}. Estimated time: {est_str}")
            except Exception:
                pass

            i = sent
            last_send_time = time.monotonic()
            last_activity_update = time.monotonic()
            consecutive_errors = 0

            while i < total and not stop_event.is_set():
                if time.monotonic() - last_activity_update > 30:
                    update_task_activity_for_bot(bot_id, task_id)
                    last_activity_update = time.monotonic()
                
                with DB_LOCKS[bot_id]:
                    c = DB_CONNECTIONS[bot_id].cursor()
                    c.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                    row = c.fetchone()
                if not row:
                    break
                status = row[0]
                if status == "cancelled" or is_suspended_for_bot(bot_id, user_id):
                    break

                if status == "paused":
                    try:
                        send_message(bot_id, user_id, f"⏸️ Task paused…")
                    except Exception:
                        pass
                    while True:
                        wake_event.wait(timeout=0.7)
                        wake_event.clear()
                        if stop_event.is_set():
                            break
                        with DB_LOCKS[bot_id]:
                            c_check = DB_CONNECTIONS[bot_id].cursor()
                            c_check.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
                            row2 = c_check.fetchone()
                        if not row2 or row2[0] == "cancelled" or is_suspended_for_bot(bot_id, user_id):
                            break
                        if row2[0] == "running":
                            try:
                                send_message(bot_id, user_id, "▶️ Resuming your task now.")
                            except Exception:
                                pass
                            last_send_time = time.monotonic()
                            last_activity_update = time.monotonic()
                            break
                    if status == "cancelled" or is_suspended_for_bot(bot_id, user_id) or stop_event.is_set():
                        if is_suspended_for_bot(bot_id, user_id):
                            set_task_status_for_bot(bot_id, task_id, "cancelled")
                            try: 
                                send_message(bot_id, user_id, "⛔ You have been suspended; stopping your task.")
                            except Exception: 
                                pass
                        break

                try:
                    result = send_message(bot_id, user_id, words[i])
                    if result:
                        consecutive_errors = 0
                        record_split_log_for_bot(bot_id, user_id, uname_for_stat, 1)
                    else:
                        consecutive_errors += 1
                        logger.warning(f"Failed to send word {i+1} to user {user_id} in {bot_id} (consecutive errors: {consecutive_errors})")
                        
                        if consecutive_errors >= 10:
                            logger.error(f"Too many consecutive errors ({consecutive_errors}) for user {user_id} in {bot_id}. Pausing task.")
                            set_task_status_for_bot(bot_id, task_id, "paused")
                            try:
                                send_message(bot_id, user_id, f"⚠️ Task paused due to sending errors. Will retry in 30 seconds.")
                            except Exception:
                                pass
                            time.sleep(30)
                            set_task_status_for_bot(bot_id, task_id, "running")
                            consecutive_errors = 0
                            continue
                        
                        record_split_log_for_bot(bot_id, user_id, uname_for_stat, 1)
                except Exception as e:
                    logger.error(f"Exception sending word {i+1} to user {user_id} in {bot_id}: {e}")
                    consecutive_errors += 1
                    record_split_log_for_bot(bot_id, user_id, uname_for_stat, 1)

                i += 1

                try:
                    with DB_LOCKS[bot_id]:
                        c = DB_CONNECTIONS[bot_id].cursor()
                        c.execute("UPDATE tasks SET sent_count = ?, last_activity = ? WHERE id = ?", (i, now_ts(), task_id))
                        DB_CONNECTIONS[bot_id].commit()
                except Exception:
                    logger.exception("Failed to update sent_count for task %s in %s", task_id, bot_id)

                if wake_event.is_set():
                    wake_event.clear()
                    continue

                now = time.monotonic()
                elapsed = now - last_send_time
                remaining_time = interval - elapsed
                if remaining_time > 0:
                    time.sleep(remaining_time)
                last_send_time = time.monotonic()

                if is_suspended_for_bot(bot_id, user_id):
                    break

            with DB_LOCKS[bot_id]:
                c = DB_CONNECTIONS[bot_id].cursor()
                c.execute("SELECT status, sent_count FROM tasks WHERE id = ?", (task_id,))
                r = c.fetchone()

            final_status = r[0] if r else "done"
            if final_status not in ("cancelled", "paused"):
                set_task_status_for_bot(bot_id, task_id, "done")
                try:
                    send_message(bot_id, user_id, f"✅ All done!")
                except Exception:
                    pass
            elif final_status == "cancelled":
                try:
                    send_message(bot_id, user_id, f"🛑 Task stopped.")
                except Exception:
                    pass

            if acquired_semaphore:
                try:
                    _active_workers_semaphores[bot_id].release()
                except Exception:
                    pass
                acquired_semaphore = False

    except Exception:
        logger.exception("Worker error for user %s in %s", user_id, bot_id)
    finally:
        if acquired_semaphore:
            try:
                _active_workers_semaphores[bot_id].release()
            except Exception:
                pass
        with _user_workers_locks[bot_id]:
            _user_workers[bot_id].pop(user_id, None)
        logger.info("Worker loop exiting for user %s in %s", user_id, bot_id)

# ============================================================================
# STATISTICS & UTILITIES (PER-BOT) - WITH FIXED ERROR HANDLING
# ============================================================================
def fetch_display_username_for_bot(bot_id: str, user_id: int):
    if not is_bot_valid(bot_id):
        return ""
    
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT username FROM split_logs WHERE user_id = ? ORDER BY created_at DESC LIMIT 1", (user_id,))
        r = c.fetchone()
        if r and r[0]:
            return r[0]
        c.execute("SELECT username FROM allowed_users WHERE user_id = ?", (user_id,))
        r2 = c.fetchone()
        if r2 and r2[0]:
            return r2[0]
    return ""

def compute_last_hour_stats_for_bot(bot_id: str):
    # FIX: Check if bot is valid before accessing DB_LOCKS
    if not is_bot_valid(bot_id):
        logger.debug(f"Bot {bot_id} not valid, skipping stats")
        return []
    
    cutoff = datetime.utcnow() - timedelta(hours=1)
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("""
            SELECT user_id, username, COUNT(*) as s
            FROM split_logs
            WHERE created_at >= ?
            GROUP BY user_id, username
            ORDER BY s DESC
        """, (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))
        rows = c.fetchall()
    stat_map = {}
    for uid, uname, s in rows:
        stat_map[uid] = {"uname": uname, "words": stat_map.get(uid,{}).get("words",0)+int(s)}
    return [(k, v["uname"], v["words"]) for k, v in stat_map.items()]

def compute_last_12h_stats_for_bot(bot_id: str, user_id: int):
    if not is_bot_valid(bot_id):
        return 0
    
    cutoff = datetime.utcnow() - timedelta(hours=12)
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("""
            SELECT COUNT(*) FROM split_logs WHERE user_id = ? AND created_at >= ?
        """, (user_id, cutoff.strftime("%Y-%m-%d %H:%M:%S")))
        r = c.fetchone()
        return int(r[0] or 0)

def send_hourly_owner_stats_for_bot(bot_id: str):
    # FIX: Check if bot is valid
    if not is_bot_valid(bot_id):
        logger.debug(f"Bot {bot_id} not valid, skipping hourly stats")
        return
    
    rows = compute_last_hour_stats_for_bot(bot_id)
    config = ACTIVE_BOTS[bot_id]
    
    if not rows:
        msg = "📊 Hourly Report: no splits in the last hour."
        for oid in config["owner_ids"]:
            try:
                send_message(bot_id, oid, msg)
            except Exception:
                pass
        return
    
    lines = []
    for uid, uname, w in rows:
        uname_for_stat = at_username(uname) if uname else fetch_display_username_for_bot(bot_id, uid)
        lines.append(f"{uid} ({uname_for_stat}) - {w} words sent")
    body = "📊 Report - last 1h:\n" + "\n".join(lines)
    
    for oid in config["owner_ids"]:
        try:
            send_message(bot_id, oid, body)
        except Exception:
            pass

def check_and_lift_for_bot(bot_id: str):
    # FIX: Check if bot is valid
    if not is_bot_valid(bot_id):
        logger.debug(f"Bot {bot_id} not valid, skipping check_and_lift")
        return
    
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT user_id, suspended_until FROM suspended_users")
        rows = c.fetchall()
    now = datetime.utcnow()
    for r in rows:
        try:
            until = datetime.strptime(r[1], "%Y-%m-%d %H:%M:%S")
            if until <= now:
                uid = r[0]
                unsuspend_user_for_bot(bot_id, uid)
        except Exception:
            logger.exception("suspend parse error for %s in %s", r, bot_id)

def prune_old_logs_for_bot(bot_id: str):
    # FIX: Check if bot is valid
    if not is_bot_valid(bot_id):
        logger.debug(f"Bot {bot_id} not valid, skipping prune_old_logs")
        return
    
    try:
        config = ACTIVE_BOTS[bot_id]
        cutoff = (datetime.utcnow() - timedelta(days=config["log_retention_days"])).strftime("%Y-%m-%d %H:%M:%S")
        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            c.execute("DELETE FROM split_logs WHERE created_at < ?", (cutoff,))
            deleted1 = c.rowcount
            c.execute("DELETE FROM sent_messages WHERE sent_at < ?", (cutoff,))
            deleted2 = c.rowcount
            DB_CONNECTIONS[bot_id].commit()
        if deleted1 or deleted2:
            logger.info("Pruned logs for %s: split_logs=%s sent_messages=%s", bot_id, deleted1, deleted2)
    except Exception:
        logger.exception("prune_old_logs error for %s", bot_id)

def check_stuck_tasks_for_bot(bot_id: str):
    # FIX: Check if bot is valid
    if not is_bot_valid(bot_id):
        logger.debug(f"Bot {bot_id} not valid, skipping check_stuck_tasks")
        return
    
    try:
        cutoff = (datetime.utcnow() - timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S")
        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            c.execute("SELECT id, user_id, status, retry_count FROM tasks WHERE status = 'running' AND last_activity < ?", (cutoff,))
            stuck_tasks = c.fetchall()
            
            for task_id, user_id, status, retry_count in stuck_tasks:
                logger.warning(f"Stuck task detected in {bot_id}: task_id={task_id}, user_id={user_id}, status={status}, retry_count={retry_count}")
                
                if retry_count < 3:
                    c.execute("UPDATE tasks SET status = 'queued', retry_count = retry_count + 1, last_activity = ? WHERE id = ?", (now_ts(), task_id))
                    logger.info(f"Reset stuck task {task_id} to queued in {bot_id} (retry {retry_count + 1})")
                    notify_user_worker_for_bot(bot_id, user_id)
                else:
                    c.execute("UPDATE tasks SET status = 'cancelled', finished_at = ? WHERE id = ?", (now_ts(), task_id))
                    logger.info(f"Cancelled stuck task {task_id} in {bot_id} after {retry_count} retries")
                    try:
                        send_message(bot_id, user_id, f"🛑 Your task was cancelled after multiple failures. Please try again.")
                    except Exception:
                        pass
            
            if stuck_tasks:
                DB_CONNECTIONS[bot_id].commit()
                logger.info(f"Cleaned up {len(stuck_tasks)} stuck tasks in {bot_id}")
    except Exception:
        logger.exception("Error checking for stuck tasks in %s", bot_id)

def get_user_task_counts_for_bot(bot_id: str, user_id: int):
    if not is_bot_valid(bot_id):
        return 0, 0
    
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status IN ('running','paused')", (user_id,))
        active = int(c.fetchone()[0] or 0)
        c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
        queued = int(c.fetchone()[0] or 0)
    return active, queued

# ============================================================================
# SCHEDULER SETUP (PER-BOT JOBS) - FIXED: ONLY FOR ACTIVE BOTS
# ============================================================================
scheduler = BackgroundScheduler()

# CRITICAL FIX: Only schedule jobs for ACTIVE bots (bots with tokens)
for bot_id in ACTIVE_BOTS.keys():  # CHANGED FROM BOT_CONFIGS.keys()
    # Hourly stats for each bot
    scheduler.add_job(
        lambda b=bot_id: send_hourly_owner_stats_for_bot(b),
        "interval", hours=1, 
        next_run_time=datetime.utcnow() + timedelta(seconds=10),
        timezone='UTC',
        id=f"hourly_stats_{bot_id}"
    )
    
    # Check suspensions for each bot
    scheduler.add_job(
        lambda b=bot_id: check_and_lift_for_bot(b),
        "interval", minutes=1,
        next_run_time=datetime.utcnow() + timedelta(seconds=15),
        timezone='UTC',
        id=f"check_suspended_{bot_id}"
    )
    
    # Prune logs for each bot
    scheduler.add_job(
        lambda b=bot_id: prune_old_logs_for_bot(b),
        "interval", hours=24,
        next_run_time=datetime.utcnow() + timedelta(seconds=30),
        timezone='UTC',
        id=f"prune_logs_{bot_id}"
    )
    
    # Check stuck tasks for each bot
    scheduler.add_job(
        lambda b=bot_id: check_stuck_tasks_for_bot(b),
        "interval", minutes=1,
        next_run_time=datetime.utcnow() + timedelta(seconds=45),
        timezone='UTC',
        id=f"check_stuck_{bot_id}"
    )

scheduler.start()

# ============================================================================
# OWNER OPERATIONS (PER-BOT) - FIXED INITIALIZATION
# ============================================================================
_owner_ops_locks: Dict[str, threading.Lock] = {}
_owner_ops_states: Dict[str, Dict[int, Dict]] = {}

# Initialize ALL dictionaries for ALL configured bots first
for bot_id in BOT_CONFIGS.keys():
    _owner_ops_locks[bot_id] = threading.Lock()
    _owner_ops_states[bot_id] = {}

def get_owner_state_for_bot(bot_id: str, user_id: int) -> Optional[Dict]:
    with _owner_ops_locks[bot_id]:
        return _owner_ops_states[bot_id].get(user_id)

def set_owner_state_for_bot(bot_id: str, user_id: int, state: Dict):
    with _owner_ops_locks[bot_id]:
        _owner_ops_states[bot_id][user_id] = state

def clear_owner_state_for_bot(bot_id: str, user_id: int):
    with _owner_ops_locks[bot_id]:
        _owner_ops_states[bot_id].pop(user_id, None)

def is_owner_in_operation_for_bot(bot_id: str, user_id: int) -> bool:
    with _owner_ops_locks[bot_id]:
        return user_id in _owner_ops_states[bot_id]

def get_user_tasks_preview_for_bot(bot_id: str, user_id: int, hours: int, page: int = 0) -> Tuple[List[Dict], int, int]:
    if not is_bot_valid(bot_id):
        return [], 0, 0
    
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("""
            SELECT id, text, created_at, total_words, sent_count
            FROM tasks 
            WHERE user_id = ? AND created_at >= ?
            ORDER BY created_at DESC
        """, (user_id, cutoff.strftime("%Y-%m-%d %H:%M:%S")))
        rows = c.fetchall()
    
    tasks = []
    for r in rows:
        task_id, text, created_at, total_words, sent_count = r
        words = split_text_to_words(text)
        preview = " ".join(words[:2]) if len(words) >= 2 else words[0] if words else "(empty)"
        tasks.append({
            "id": task_id,
            "preview": preview,
            "created_at": utc_to_wat_ts(created_at),
            "total_words": total_words,
            "sent_count": sent_count
        })
    
    total_tasks = len(tasks)
    page_size = 20
    start_idx = page * page_size
    end_idx = start_idx + page_size
    paginated_tasks = tasks[start_idx:end_idx]
    
    total_pages = (total_tasks + page_size - 1) // page_size
    
    return paginated_tasks, total_tasks, total_pages

def get_all_users_ordered_for_bot(bot_id: str):
    if not is_bot_valid(bot_id):
        return []
    
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT user_id, username, added_at FROM allowed_users ORDER BY added_at DESC")
        return c.fetchall()

def get_user_index_for_bot(bot_id: str, user_id: int):
    users = get_all_users_ordered_for_bot(bot_id)
    for i, (uid, username, added_at) in enumerate(users):
        if uid == user_id:
            return i, users
    return -1, users

def parse_duration(duration_str: str) -> Tuple[int, str]:
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
            if num == 1:
                parts.append(f"{num} {label}")
            else:
                parts.append(f"{num} {label}s")
                
        except ValueError:
            return None, f"Invalid number: {value}{unit}"
        except KeyError:
            return None, f"Invalid unit: {unit}"
    
    if total_seconds == 0:
        return None, "Duration cannot be zero"
    
    formatted = ", ".join(parts)
    return total_seconds, formatted

def send_ownersets_menu_for_bot(bot_id: str, owner_id: int):
    if not is_bot_valid(bot_id):
        return
    
    config = ACTIVE_BOTS[bot_id]
    menu_text = f"👑 Owner Menu {config['owner_tag']}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nSelect an operation:"
    
    keyboard = [
        [{"text": "📊 Bot Info", "callback_data": f"owner_botinfo_{bot_id}"}, 
         {"text": "👥 List Users", "callback_data": f"owner_listusers_{bot_id}"}],
        [{"text": "🚫 List Suspended", "callback_data": f"owner_listsuspended_{bot_id}"}, 
         {"text": "➕ Add User", "callback_data": f"owner_adduser_{bot_id}"}],
        [{"text": "⏸️ Suspend User", "callback_data": f"owner_suspend_{bot_id}"}, 
         {"text": "▶️ Unsuspend User", "callback_data": f"owner_unsuspend_{bot_id}"}],
        [{"text": "🔍 Check All User Preview", "callback_data": f"owner_checkallpreview_{bot_id}"}]
    ]
    
    reply_markup = {"inline_keyboard": keyboard}
    send_message(bot_id, owner_id, menu_text, reply_markup)

# ============================================================================
# WEBHOOK HANDLERS (DYNAMIC PER BOT)
# ============================================================================
@app.route("/webhook/<bot_id>", methods=["POST"])
def webhook_handler(bot_id: str):
    if bot_id not in BOT_CONFIGS:
        logger.warning("Unknown bot ID in webhook: %s", bot_id)
        return jsonify({"ok": False, "error": "bot_not_found"}), 404
    
    try:
        update = request.get_json(force=True)
    except Exception:
        return jsonify({"ok": False}), 400
    
    try:
        # Handle callback queries
        if "callback_query" in update:
            return handle_callback_query(bot_id, update["callback_query"])
        
        # Handle regular messages
        if "message" in update:
            return handle_message(bot_id, update["message"])
            
    except Exception:
        logger.exception("webhook handling error for %s", bot_id)
    
    return jsonify({"ok": True})

def handle_callback_query(bot_id: str, callback):
    user = callback.get("from", {})
    uid = user.get("id")
    data = callback.get("data", "")
    
    config = ACTIVE_BOTS.get(bot_id)
    if not config or uid not in config["owner_ids"]:
        try:
            telegram_api = get_telegram_api(bot_id)
            if telegram_api:
                _session.post(f"{telegram_api}/answerCallbackQuery", json={
                    "callback_query_id": callback.get("id"),
                    "text": "⛔ Owner only."
                }, timeout=2)
        except Exception:
            pass
        return jsonify({"ok": True})
    
    # Extract action from callback data (format: "action_botid" or "action_botid_extra")
    parts = data.split("_")
    if len(parts) < 2:
        return jsonify({"ok": True})
    
    action = parts[0]
    
    if action == "owner_close":
        try:
            _session.post(f"{get_telegram_api(bot_id)}/deleteMessage", json={
                "chat_id": callback["message"]["chat"]["id"],
                "message_id": callback["message"]["message_id"]
            }, timeout=2)
        except Exception:
            pass
        try:
            _session.post(f"{get_telegram_api(bot_id)}/answerCallbackQuery", json={
                "callback_query_id": callback.get("id"),
                "text": "✅ Menu closed."
            }, timeout=2)
        except Exception:
            pass
        clear_owner_state_for_bot(bot_id, uid)
        return jsonify({"ok": True})
    
    elif action == "owner_botinfo":
        return handle_owner_botinfo(bot_id, callback, uid)
    
    elif action == "owner_listusers":
        return handle_owner_listusers(bot_id, callback, uid)
    
    elif action == "owner_listsuspended":
        return handle_owner_listsuspended(bot_id, callback, uid)
    
    elif action == "owner_backtomenu":
        send_ownersets_menu_for_bot(bot_id, uid)
        try:
            _session.post(f"{get_telegram_api(bot_id)}/deleteMessage", json={
                "chat_id": callback["message"]["chat"]["id"],
                "message_id": callback["message"]["message_id"]
            }, timeout=2)
        except Exception:
            pass
        try:
            _session.post(f"{get_telegram_api(bot_id)}/answerCallbackQuery", json={
                "callback_query_id": callback.get("id"),
                "text": "✅ Returning to menu."
            }, timeout=2)
        except Exception:
            pass
        return jsonify({"ok": True})
    
    elif action == "owner_checkallpreview":
        return handle_owner_checkallpreview(bot_id, callback, uid, parts)
    
    elif action in ["owner_adduser", "owner_suspend", "owner_unsuspend", "owner_checkallpreview"]:
        # Operations that require input
        operation = action.replace("owner_", "")
        set_owner_state_for_bot(bot_id, uid, {"operation": operation, "step": 0})
        
        prompts = {
            "adduser": "👤 Please send the User ID to add (multiple IDs separated by spaces/commas):",
            "suspend": "⏸️ Please send:\n1. User ID\n2. Duration (e.g., 30s, 10m, 2h, 1d, 1d2h, 2h30m, 1d2h3m5s)\n3. Optional reason\n\nExamples:\n• 123456789 30s Too many requests\n• 123456789 1d2h Spamming\n• 123456789 2h30m Violation",
            "unsuspend": "▶️ Please send the User ID to unsuspend:",
            "checkallpreview": "⏰ How many hours back should I check? (e.g., 1, 6, 24, 168):"
        }
        
        cancel_keyboard = {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": f"owner_cancelinput_{bot_id}"}]]}
        
        try:
            send_message(bot_id, uid, f"⚠️ {prompts[operation]}\n\nPlease send the requested information as a text message.", cancel_keyboard)
        except Exception:
            pass
        try:
            _session.post(f"{get_telegram_api(bot_id)}/answerCallbackQuery", json={
                "callback_query_id": callback.get("id"),
                "text": "ℹ️ Please check your new message."
            }, timeout=2)
        except Exception:
            pass
        return jsonify({"ok": True})
    
    elif action == "owner_cancelinput":
        clear_owner_state_for_bot(bot_id, uid)
        try:
            _session.post(f"{get_telegram_api(bot_id)}/deleteMessage", json={
                "chat_id": callback["message"]["chat"]["id"],
                "message_id": callback["message"]["message_id"]
            }, timeout=2)
        except Exception:
            pass
        try:
            _session.post(f"{get_telegram_api(bot_id)}/answerCallbackQuery", json={
                "callback_query_id": callback.get("id"),
                "text": "❌ Operation cancelled."
            }, timeout=2)
        except Exception:
            pass
        return jsonify({"ok": True})
    
    # Answer callback query
    try:
        _session.post(f"{get_telegram_api(bot_id)}/answerCallbackQuery", json={
            "callback_query_id": callback.get("id")
        }, timeout=2)
    except Exception:
        pass
    
    return jsonify({"ok": True})

def handle_owner_botinfo(bot_id: str, callback, uid: int):
    if not is_bot_valid(bot_id):
        return jsonify({"ok": True})
    
    config = ACTIVE_BOTS[bot_id]
    active_rows, queued_tasks = [], 0
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT user_id, username, SUM(total_words - IFNULL(sent_count,0)) as remaining, COUNT(*) as active_count FROM tasks WHERE status IN ('running','paused') GROUP BY user_id")
        active_rows = c.fetchall()
        c.execute("SELECT COUNT(*) FROM tasks WHERE status = 'queued'")
        queued_tasks = c.fetchone()[0]
    
    queued_counts = {}
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT user_id, COUNT(*) FROM tasks WHERE status = 'queued' GROUP BY user_id")
        for row in c.fetchall():
            queued_counts[row[0]] = row[1]
    
    stats_rows = compute_last_hour_stats_for_bot(bot_id)
    lines_active = []
    for r in active_rows:
        uid2, uname, rem, ac = r
        if not uname:
            uname = fetch_display_username_for_bot(bot_id, uid2)
        name = f" ({at_username(uname)})" if uname else ""
        queued_for_user = queued_counts.get(uid2, 0)
        lines_active.append(f"{uid2}{name} - {int(rem)} remaining - {int(ac)} active - {queued_for_user} queued")
    
    lines_stats = []
    for uid2, uname, s in stats_rows:
        uname_final = at_username(uname) if uname else fetch_display_username_for_bot(bot_id, uid2)
        lines_stats.append(f"{uid2} ({uname_final}) - {int(s)} words sent")
    
    total_allowed = 0
    total_suspended = 0
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT COUNT(*) FROM allowed_users")
        total_allowed = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM suspended_users")
        total_suspended = c.fetchone()[0]
    
    interval_type = "SLOW" if config["use_slow_interval"] else "FAST"
    body = (
        f"🤖 Bot Status\n"
        f"👥 Allowed users: {total_allowed}\n"
        f"🚫 Suspended users: {total_suspended}\n"
        f"⚙️ Active tasks: {len(active_rows)}\n"
        f"📨 Queued tasks: {queued_tasks}\n"
        f"⏱️ Interval: {interval_type}\n\n"
        "Users with active tasks:\n" + ("\n".join(lines_active) if lines_active else "(none)") + "\n\n"
        "User stats (last 1h):\n" + ("\n".join(lines_stats) if lines_stats else "(none)")
    )
    
    menu_text = f"👑 Owner Menu {config['owner_tag']}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{body}"
    keyboard = [
        [{"text": "📊 Bot Info", "callback_data": f"owner_botinfo_{bot_id}"}, 
         {"text": "👥 List Users", "callback_data": f"owner_listusers_{bot_id}"}],
        [{"text": "🚫 List Suspended", "callback_data": f"owner_listsuspended_{bot_id}"}, 
         {"text": "➕ Add User", "callback_data": f"owner_adduser_{bot_id}"}],
        [{"text": "⏸️ Suspend User", "callback_data": f"owner_suspend_{bot_id}"}, 
         {"text": "▶️ Unsuspend User", "callback_data": f"owner_unsuspend_{bot_id}"}],
        [{"text": "🔍 Check All User Preview", "callback_data": f"owner_checkallpreview_{bot_id}"}]
    ]
    
    try:
        _session.post(f"{get_telegram_api(bot_id)}/editMessageText", json={
            "chat_id": callback["message"]["chat"]["id"],
            "message_id": callback["message"]["message_id"],
            "text": menu_text,
            "reply_markup": {"inline_keyboard": keyboard}
        }, timeout=2)
    except Exception:
        pass
    try:
        _session.post(f"{get_telegram_api(bot_id)}/answerCallbackQuery", json={
            "callback_query_id": callback.get("id"),
            "text": "✅ Bot info loaded."
        }, timeout=2)
    except Exception:
        pass
    
    return jsonify({"ok": True})

def handle_owner_listusers(bot_id: str, callback, uid: int):
    if not is_bot_valid(bot_id):
        return jsonify({"ok": True})
    
    config = ACTIVE_BOTS[bot_id]
    with DB_LOCKS[bot_id]:
        c = DB_CONNECTIONS[bot_id].cursor()
        c.execute("SELECT user_id, username, added_at FROM allowed_users ORDER BY added_at DESC")
        rows = c.fetchall()
    
    lines = []
    for r in rows:
        uid2, uname, added_at_utc = r
        uname_s = f"({at_username(uname)})" if uname else "(no username)"
        added_at_wat = utc_to_wat_ts(added_at_utc)
        lines.append(f"{uid2} {uname_s} added={added_at_wat}")
    
    body = "👥 Allowed users:\n" + ("\n".join(lines) if lines else "(none)")
    
    menu_text = f"👑 Owner Menu {config['owner_tag']}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{body}"
    keyboard = [
        [{"text": "📊 Bot Info", "callback_data": f"owner_botinfo_{bot_id}"}, 
         {"text": "👥 List Users", "callback_data": f"owner_listusers_{bot_id}"}],
        [{"text": "🚫 List Suspended", "callback_data": f"owner_listsuspended_{bot_id}"}, 
         {"text": "➕ Add User", "callback_data": f"owner_adduser_{bot_id}"}],
        [{"text": "⏸️ Suspend User", "callback_data": f"owner_suspend_{bot_id}"}, 
         {"text": "▶️ Unsuspend User", "callback_data": f"owner_unsuspend_{bot_id}"}],
        [{"text": "🔍 Check All User Preview", "callback_data": f"owner_checkallpreview_{bot_id}"}]
    ]
    
    try:
        _session.post(f"{get_telegram_api(bot_id)}/editMessageText", json={
            "chat_id": callback["message"]["chat"]["id"],
            "message_id": callback["message"]["message_id"],
            "text": menu_text,
            "reply_markup": {"inline_keyboard": keyboard}
        }, timeout=2)
    except Exception:
        pass
    try:
        _session.post(f"{get_telegram_api(bot_id)}/answerCallbackQuery", json={
            "callback_query_id": callback.get("id"),
            "text": "✅ User list loaded."
        }, timeout=2)
    except Exception:
        pass
    
    return jsonify({"ok": True})

def handle_owner_listsuspended(bot_id: str, callback, uid: int):
    if not is_bot_valid(bot_id):
        return jsonify({"ok": True})
    
    config = ACTIVE_BOTS[bot_id]
    for row in list_suspended_for_bot(bot_id)[:]:
        uid2, until_utc, reason, added_at_utc = row
        until_dt = datetime.strptime(until_utc, "%Y-%m-%d %H:%M:%S")
        if until_dt <= datetime.utcnow():
            unsuspend_user_for_bot(bot_id, uid2)
    
    rows = list_suspended_for_bot(bot_id)
    if not rows:
        body = "✅ No suspended users."
    else:
        lines = []
        for r in rows:
            uid2, until_utc, reason, added_at_utc = r
            until_wat = utc_to_wat_ts(until_utc)
            added_wat = utc_to_wat_ts(added_at_utc)
            uname = fetch_display_username_for_bot(bot_id, uid2)
            uname_s = f"({at_username(uname)})" if uname else ""
            lines.append(f"{uid2} {uname_s} until={until_wat} reason={reason}")
        body = "🚫 Suspended users:\n" + "\n".join(lines)
    
    menu_text = f"👑 Owner Menu {config['owner_tag']}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{body}"
    keyboard = [
        [{"text": "📊 Bot Info", "callback_data": f"owner_botinfo_{bot_id}"}, 
         {"text": "👥 List Users", "callback_data": f"owner_listusers_{bot_id}"}],
        [{"text": "🚫 List Suspended", "callback_data": f"owner_listsuspended_{bot_id}"}, 
         {"text": "➕ Add User", "callback_data": f"owner_adduser_{bot_id}"}],
        [{"text": "⏸️ Suspend User", "callback_data": f"owner_suspend_{bot_id}"}, 
         {"text": "▶️ Unsuspend User", "callback_data": f"owner_unsuspend_{bot_id}"}],
        [{"text": "🔍 Check All User Preview", "callback_data": f"owner_checkallpreview_{bot_id}"}]
    ]
    
    try:
        _session.post(f"{get_telegram_api(bot_id)}/editMessageText", json={
            "chat_id": callback["message"]["chat"]["id"],
            "message_id": callback["message"]["message_id"],
            "text": menu_text,
            "reply_markup": {"inline_keyboard": keyboard}
        }, timeout=2)
    except Exception:
        pass
    try:
        _session.post(f"{get_telegram_api(bot_id)}/answerCallbackQuery", json={
            "callback_query_id": callback.get("id"),
            "text": "✅ Suspended list loaded."
        }, timeout=2)
    except Exception:
        pass
    
    return jsonify({"ok": True})

def handle_owner_checkallpreview(bot_id: str, callback, uid: int, parts):
    if not is_bot_valid(bot_id):
        return jsonify({"ok": True})
    
    if len(parts) == 5:  # owner_checkallpreview_userid_page_hours
        target_user = int(parts[2])
        page = int(parts[3])
        hours = int(parts[4])
        
        user_index, all_users = get_user_index_for_bot(bot_id, target_user)
        if user_index == -1:
            if all_users:
                target_user = all_users[0][0]
                user_index = 0
            else:
                try:
                    _session.post(f"{get_telegram_api(bot_id)}/editMessageText", json={
                        "chat_id": callback["message"]["chat"]["id"],
                        "message_id": callback["message"]["message_id"],
                        "text": "📋 No users found.",
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
        
        tasks, total_tasks, total_pages = get_user_tasks_preview_for_bot(bot_id, target_user, hours, page)
        
        user_info = all_users[user_index]
        user_id_info, username_info, added_at_info = user_info
        username_display = at_username(username_info) if username_info else "no username"
        added_wat = utc_to_wat_ts(added_at_info)
        
        if not tasks:
            body = f"👤 User: {user_id_info} ({username_display})\nAdded: {added_wat}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📋 No tasks found in the last {hours} hours."
        else:
            lines = []
            for task in tasks:
                lines.append(f"🕒 {task['created_at']}\n📝 Preview: {task['preview']}\n📊 Progress: {task['sent_count']}/{task['total_words']} words")
            
            body = f"👤 User: {user_id_info} ({username_display})\nAdded: {added_wat}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n📋 Tasks (last {hours}h, page {page+1}/{total_pages}):\n\n" + "\n\n".join(lines)
        
        keyboard = []
        
        task_nav = []
        if page > 0:
            task_nav.append({"text": "⬅️ Prev Page", "callback_data": f"owner_checkallpreview_{bot_id}_{target_user}_{page-1}_{hours}"})
        if page + 1 < total_pages:
            task_nav.append({"text": "Next Page ➡️", "callback_data": f"owner_checkallpreview_{bot_id}_{target_user}_{page+1}_{hours}"})
        if task_nav:
            keyboard.append(task_nav)
        
        user_nav = []
        if user_index > 0:
            prev_user_id = all_users[user_index-1][0]
            user_nav.append({"text": "⬅️ Prev User", "callback_data": f"owner_checkallpreview_{bot_id}_{prev_user_id}_0_{hours}"})
        
        user_nav.append({"text": f"User {user_index+1}/{len(all_users)}", "callback_data": f"owner_checkallpreview_noop_{bot_id}"})
        
        if user_index + 1 < len(all_users):
            next_user_id = all_users[user_index+1][0]
            user_nav.append({"text": "Next User ➡️", "callback_data": f"owner_checkallpreview_{bot_id}_{next_user_id}_0_{hours}"})
        
        if user_nav:
            keyboard.append(user_nav)
        
        keyboard.append([{"text": "🔙 Back to Menu", "callback_data": f"owner_backtomenu_{bot_id}"}])
        
        try:
            _session.post(f"{get_telegram_api(bot_id)}/editMessageText", json={
                "chat_id": callback["message"]["chat"]["id"],
                "message_id": callback["message"]["message_id"],
                "text": body,
                "reply_markup": {"inline_keyboard": keyboard}
            }, timeout=2)
        except Exception:
            pass
    
    return jsonify({"ok": True})

def handle_message(bot_id: str, msg):
    config = ACTIVE_BOTS.get(bot_id)
    if not config:
        return jsonify({"ok": True})
    
    user = msg.get("from", {})
    uid = user.get("id")
    username = user.get("username") or (user.get("first_name") or "")
    text = msg.get("text") or ""

    # Update username for this bot only
    try:
        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            c.execute("UPDATE allowed_users SET username = ? WHERE user_id = ?", (username or "", uid))
            DB_CONNECTIONS[bot_id].commit()
    except Exception:
        logger.exception("webhook: update allowed_users username failed for %s", bot_id)

    # Check if owner is in input mode for THIS bot
    if uid in config["owner_ids"] and is_owner_in_operation_for_bot(bot_id, uid):
        state = get_owner_state_for_bot(bot_id, uid)
        if state:
            operation = state.get("operation")
            
            if operation == "adduser":
                parts = re.split(r"[,\s]+", text.strip())
                added, already, invalid = [], [], []
                for p in parts:
                    if not p:
                        continue
                    try:
                        tid = int(p)
                    except Exception:
                        invalid.append(p)
                        continue
                    with DB_LOCKS[bot_id]:
                        c = DB_CONNECTIONS[bot_id].cursor()
                        c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (tid,))
                        if c.fetchone():
                            already.append(tid)
                            continue
                        c.execute("INSERT INTO allowed_users (user_id, username, added_at) VALUES (?, ?, ?)", (tid, "", now_ts()))
                        DB_CONNECTIONS[bot_id].commit()
                    added.append(tid)
                    try:
                        send_message(bot_id, tid, f"✅ You have been added. Send any text to start.")
                    except Exception:
                        pass
                parts_msgs = []
                if added: parts_msgs.append("Added: " + ", ".join(str(x) for x in added))
                if already: parts_msgs.append("Already present: " + ", ".join(str(x) for x in already))
                if invalid: parts_msgs.append("Invalid: " + ", ".join(invalid))
                result_msg = "✅ " + ("; ".join(parts_msgs) if parts_msgs else "No changes")
                
                clear_owner_state_for_bot(bot_id, uid)
                send_message(bot_id, uid, f"{result_msg}\n\nUse /ownersets again to access the menu. 😊")
                return jsonify({"ok": True})
            
            elif operation == "suspend":
                parts = text.split(maxsplit=2)
                if len(parts) < 2:
                    send_message(bot_id, uid, "⚠️ Please provide both User ID and duration. Example: 123456789 1d2h")
                    return jsonify({"ok": True})
                
                try:
                    target = int(parts[0])
                except Exception:
                    send_message(bot_id, uid, "❌ Invalid User ID. Please try again.")
                    return jsonify({"ok": True})
                
                dur = parts[1]
                reason = parts[2] if len(parts) > 2 else ""
                
                result = parse_duration(dur)
                if result[0] is None:
                    send_message(bot_id, uid, f"❌ {result[1]}\n\nValid examples: 30s, 10m, 2h, 1d, 1d2h, 2h30m, 1d2h3m5s")
                    return jsonify({"ok": True})
                
                seconds, formatted_duration = result
                
                suspend_user_for_bot(bot_id, target, seconds, reason)
                reason_part = f"\nReason: {reason}" if reason else ""
                until_wat = utc_to_wat_ts((datetime.utcnow() + timedelta(seconds=seconds)).strftime('%Y-%m-%d %H:%M:%S'))
                
                clear_owner_state_for_bot(bot_id, uid)
                send_message(bot_id, uid, f"✅ User {label_for_owner_view(bot_id, target, fetch_display_username_for_bot(bot_id, target))} suspended for {formatted_duration} (until {until_wat}).{reason_part}\n\nUse /ownersets again to access the menu. 😊")
                return jsonify({"ok": True})
            
            elif operation == "unsuspend":
                try:
                    target = int(text.strip())
                except Exception:
                    send_message(bot_id, uid, "❌ Invalid User ID. Please try again.")
                    return jsonify({"ok": True})
                
                ok = unsuspend_user_for_bot(bot_id, target)
                if ok:
                    result = f"✅ User {label_for_owner_view(bot_id, target, fetch_display_username_for_bot(bot_id, target))} unsuspended."
                else:
                    result = f"ℹ️ User {target} is not suspended."
                
                clear_owner_state_for_bot(bot_id, uid)
                send_message(bot_id, uid, f"{result}\n\nUse /ownersets again to access the menu. 😊")
                return jsonify({"ok": True})
            
            elif operation == "checkallpreview":
                try:
                    hours = int(text.strip())
                    if hours <= 0:
                        raise ValueError
                except Exception:
                    send_message(bot_id, uid, "❌ Please enter a valid positive number of hours.")
                    return jsonify({"ok": True})
                
                all_users = get_all_users_ordered_for_bot(bot_id)
                if not all_users:
                    clear_owner_state_for_bot(bot_id, uid)
                    send_message(bot_id, uid, "📋 No users found.")
                    return jsonify({"ok": True})
                
                first_user_id, first_username, first_added_at = all_users[0]
                username_display = at_username(first_username) if first_username else "no username"
                added_wat = utc_to_wat_ts(first_added_at)
                
                tasks, total_tasks, total_pages = get_user_tasks_preview_for_bot(bot_id, first_user_id, hours, 0)
                
                if not tasks:
                    body = f"👤 User: {first_user_id} ({username_display})\nAdded: {added_wat}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📋 No tasks found in the last {hours} hours."
                else:
                    lines = []
                    for task in tasks:
                        lines.append(f"🕒 {task['created_at']}\n📝 Preview: {task['preview']}\n📊 Progress: {task['sent_count']}/{task['total_words']} words")
                    
                    body = f"👤 User: {first_user_id} ({username_display})\nAdded: {added_wat}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n📋 Tasks (last {hours}h, page 1/{total_pages}):\n\n" + "\n\n".join(lines)
                
                keyboard = []
                
                task_nav = []
                if total_pages > 1:
                    task_nav.append({"text": "Next Page ➡️", "callback_data": f"owner_checkallpreview_{bot_id}_{first_user_id}_1_{hours}"})
                if task_nav:
                    keyboard.append(task_nav)
                
                user_nav = []
                user_nav.append({"text": f"User 1/{len(all_users)}", "callback_data": f"owner_checkallpreview_noop_{bot_id}"})
                
                if len(all_users) > 1:
                    next_user_id = all_users[1][0]
                    user_nav.append({"text": "Next User ➡️", "callback_data": f"owner_checkallpreview_{bot_id}_{next_user_id}_0_{hours}"})
                
                if user_nav:
                    keyboard.append(user_nav)
                
                keyboard.append([{"text": "🔙 Back to Menu", "callback_data": f"owner_backtomenu_{bot_id}"}])
                
                clear_owner_state_for_bot(bot_id, uid)
                send_message(bot_id, uid, body, {"inline_keyboard": keyboard})
                return jsonify({"ok": True})

    # Handle commands
    if text.startswith("/"):
        parts = text.split(None, 1)
        cmd = parts[0].split("@")[0].lower()
        args = parts[1] if len(parts) > 1 else ""
        
        # Clear any existing owner state for THIS bot when new command comes
        clear_owner_state_for_bot(bot_id, uid)
        
        # Handle /ownersets command
        if cmd == "/ownersets":
            if uid not in config["owner_ids"]:
                send_message(bot_id, uid, f"🚫 Owner only. {config['owner_tag']} notified.")
                notify_owners_for_bot(bot_id, f"🚨 Unallowed /ownersets attempt by {at_username(username) if username else uid} (ID: {uid}).")
                return jsonify({"ok": True})
            send_ownersets_menu_for_bot(bot_id, uid)
            return jsonify({"ok": True})
        else:
            return handle_command_for_bot(bot_id, uid, username, cmd, args)
    else:
        # Handle regular text input
        return handle_user_text_for_bot(bot_id, uid, username, text)
    
    return jsonify({"ok": True})

# ============================================================================
# COMMAND HANDLERS (PER-BOT) - FIXED: NO BOT ID IN MESSAGES
# ============================================================================
def handle_command_for_bot(bot_id: str, user_id: int, username: str, command: str, args: str):
    if not is_bot_valid(bot_id):
        return jsonify({"ok": True})
    
    config = ACTIVE_BOTS[bot_id]
    
    if command == "/start":
        who = label_for_self(bot_id, user_id, username) or "there"
        msg = (
            f"👋 Hi {who}!\n\n"
            f"I split your text into individual word messages. ✂️📤\n\n"
            f"{config['owner_tag']} command:\n"
            " /ownersets - Owner management menu\n\n"
            "User commands:\n"
            " /start /example /pause /resume /status /stop /stats /about\n\n"
            f"Just send any text and I'll split it for you. 🚀"
        )
        send_message(bot_id, user_id, msg)
        return jsonify({"ok": True})

    if command == "/about":
        interval_type = "SLOW (1.0-1.2s)" if config["use_slow_interval"] else "FAST (0.5-0.7s)"
        msg = (
            f"ℹ️ About:\n"
            "I split texts into single words. ✂️\n\n"
            f"Interval: {interval_type}\n"
            "Features:\n"
            "queueing, pause/resume,\n"
            "hourly owner stats, rate-limited sending. ⚖️"
        )
        send_message(bot_id, user_id, msg)
        return jsonify({"ok": True})

    if user_id not in config["owner_ids"] and not is_allowed_for_bot(bot_id, user_id):
        send_message(bot_id, user_id, f"🚫 Sorry, you are not allowed. {config['owner_tag']} notified.\nYour ID: {user_id}")
        notify_owners_for_bot(bot_id, f"🚨 Unallowed access attempt by {at_username(username) if username else user_id} (ID: {user_id}).")
        return jsonify({"ok": True})

    if command == "/example":
        sample = "\n".join([
            "996770061141", "996770064514", "996770071665", "996770073284",
            "996770075145", "996770075627", "996770075973", "996770076350",
            "996770076869", "996770077101"
        ])
        res = enqueue_task_for_bot(bot_id, user_id, username, sample)
        if not res["ok"]:
            send_message(bot_id, user_id, "❗ Could not queue demo. Try later.")
            return jsonify({"ok": True})
        start_user_worker_if_needed_for_bot(bot_id, user_id)
        notify_user_worker_for_bot(bot_id, user_id)
        active, queued = get_user_task_counts_for_bot(bot_id, user_id)
        if active:
            send_message(bot_id, user_id, f"✅ Task added. Words: {res['total_words']}.\nQueue position: {queued}")
        else:
            send_message(bot_id, user_id, f"✅ Task added. Words: {res['total_words']}.")
        return jsonify({"ok": True})

    if command == "/pause":
        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            c.execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'running' ORDER BY started_at ASC LIMIT 1", (user_id,))
            rows = c.fetchone()
        if not rows:
            send_message(bot_id, user_id, "ℹ️ No active task to pause.")
            return jsonify({"ok": True})
        set_task_status_for_bot(bot_id, rows[0], "paused")
        notify_user_worker_for_bot(bot_id, user_id)
        send_message(bot_id, user_id, "⏸️ Paused. Use /resume to continue.")
        return jsonify({"ok": True})

    if command == "/resume":
        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            c.execute("SELECT id FROM tasks WHERE user_id = ? AND status = 'paused' ORDER BY started_at ASC LIMIT 1", (user_id,))
            rows = c.fetchone()
        if not rows:
            send_message(bot_id, user_id, "ℹ️ No paused task to resume.")
            return jsonify({"ok": True})
        set_task_status_for_bot(bot_id, rows[0], "running")
        notify_user_worker_for_bot(bot_id, user_id)
        send_message(bot_id, user_id, "▶️ Resuming your task now.")
        return jsonify({"ok": True})

    if command == "/status":
        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            c.execute("SELECT id, status, total_words, sent_count FROM tasks WHERE user_id = ? AND status IN ('running','paused') ORDER BY started_at ASC LIMIT 1", (user_id,))
            active = c.fetchone()
            c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
            queued = c.fetchone()[0]
        if active:
            aid, status, total, sent = active
            remaining = int(total or 0) - int(sent or 0)
            send_message(bot_id, user_id, f"ℹ️ Status: {status}\nRemaining words: {remaining}\nQueue size: {queued}")
        elif queued > 0:
            send_message(bot_id, user_id, f"⏳ Waiting. Queue size: {queued}")
        else:
            send_message(bot_id, user_id, "✅ You have no active or queued tasks.")
        return jsonify({"ok": True})

    if command == "/stop":
        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            c.execute("SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status = 'queued'", (user_id,))
            queued = c.fetchone()[0]
        stopped = cancel_active_task_for_user_for_bot(bot_id, user_id)
        stop_user_worker_for_bot(bot_id, user_id)
        if stopped > 0 or queued > 0:
            send_message(bot_id, user_id, "🛑 Active task stopped. Your queued tasks were cleared too.")
        else:
            send_message(bot_id, user_id, "ℹ️ You had no active or queued tasks.")
        return jsonify({"ok": True})

    if command == "/stats":
        words = compute_last_12h_stats_for_bot(bot_id, user_id)
        send_message(bot_id, user_id, f"📊 Your last 12 hours: {words} words split")
        return jsonify({"ok": True})

    send_message(bot_id, user_id, "❓ Unknown command.")
    return jsonify({"ok": True})

def handle_user_text_for_bot(bot_id: str, user_id: int, username: str, text: str):
    if not is_bot_valid(bot_id):
        return jsonify({"ok": True})
    
    config = ACTIVE_BOTS[bot_id]
    
    # Block owner task processing if in operation mode for THIS bot
    if user_id in config["owner_ids"] and is_owner_in_operation_for_bot(bot_id, user_id):
        logger.warning(f"Owner {user_id} text reached handle_user_text while in operation state for {bot_id}.")
        return jsonify({"ok": True})
    
    if user_id not in config["owner_ids"] and not is_allowed_for_bot(bot_id, user_id):
        send_message(bot_id, user_id, f"🚫 Sorry, you are not allowed. {config['owner_tag']} notified.\nYour ID: {user_id}")
        notify_owners_for_bot(bot_id, f"🚨 Unallowed access attempt by {at_username(username) if username else user_id} (ID: {user_id}).")
        return jsonify({"ok": True})
    
    if is_suspended_for_bot(bot_id, user_id):
        with DB_LOCKS[bot_id]:
            c = DB_CONNECTIONS[bot_id].cursor()
            c.execute("SELECT suspended_until FROM suspended_users WHERE user_id = ?", (user_id,))
            r = c.fetchone()
            until_utc = r[0] if r else "unknown"
            until_wat = utc_to_wat_ts(until_utc)
        send_message(bot_id, user_id, f"⛔ You have been suspended until {until_wat} by {config['owner_tag']}.")
        return jsonify({"ok": True})
    
    res = enqueue_task_for_bot(bot_id, user_id, username, text)
    if not res["ok"]:
        if res["reason"] == "empty":
            send_message(bot_id, user_id, "⚠️ Empty text. Nothing to split.")
            return jsonify({"ok": True})
        if res["reason"] == "queue_full":
            send_message(bot_id, user_id, f"⏳ Your queue is full ({res['queue_size']}). Use /stop or wait.")
            return jsonify({"ok": True})
        send_message(bot_id, user_id, "❗ Could not queue task. Try later.")
        return jsonify({"ok": True})
    
    start_user_worker_if_needed_for_bot(bot_id, user_id)
    notify_user_worker_for_bot(bot_id, user_id)
    active, queued = get_user_task_counts_for_bot(bot_id, user_id)
    if active:
        send_message(bot_id, user_id, f"✅ Task added. Words: {res['total_words']}.\nQueue position: {queued}")
    else:
        send_message(bot_id, user_id, f"✅ Task added. Words: {res['total_words']}.")
    return jsonify({"ok": True})

# ============================================================================
# HEALTH ENDPOINTS (PER-BOT & OVERALL)
# ============================================================================
@app.route("/", methods=["GET"])
def root():
    bot_list = ", ".join(ACTIVE_BOTS.keys())
    return f"WordSplitter Multi-Bot running. Active bots: {bot_list}", 200

@app.route("/health/<bot_id>", methods=["GET", "HEAD"])
def health_bot(bot_id: str):
    if bot_id not in BOT_CONFIGS:
        return jsonify({"ok": False, "error": "bot_not_found"}), 404
    
    is_active = bot_id in ACTIVE_BOTS
    status = {
        "ok": is_active and is_bot_valid(bot_id),
        "bot_id": bot_id,
        "active": is_active,
        "timestamp": now_ts(),
        "configured": bot_id in BOT_CONFIGS,
        "db_connected": is_bot_valid(bot_id),
        "workers_active": len(_user_workers.get(bot_id, {})),
    }
    
    if is_active:
        status["config"] = {
            "use_slow_interval": ACTIVE_BOTS[bot_id]["use_slow_interval"],
            "max_queue_per_user": ACTIVE_BOTS[bot_id]["max_queue_per_user"],
            "max_msg_per_second": ACTIVE_BOTS[bot_id]["max_msg_per_second"],
            "max_concurrent_workers": ACTIVE_BOTS[bot_id]["max_concurrent_workers"],
        }
    
    return jsonify(status), 200 if status["ok"] else 503

@app.route("/health", methods=["GET", "HEAD"])
def health_all():
    overall_ok = True
    bots_status = {}
    
    for bot_id in BOT_CONFIGS.keys():
        is_active = bot_id in ACTIVE_BOTS
        bot_status = {
            "active": is_active,
            "configured": True,
            "db_connected": is_bot_valid(bot_id) if is_active else False,
            "workers_active": len(_user_workers.get(bot_id, {})),
        }
        
        if is_active:
            bot_status["use_slow_interval"] = ACTIVE_BOTS[bot_id]["use_slow_interval"]
        
        bots_status[bot_id] = bot_status
        
        if is_active and not is_bot_valid(bot_id):
            overall_ok = False
    
    status = {
        "ok": overall_ok,
        "timestamp": now_ts(),
        "total_bots": len(BOT_CONFIGS),
        "active_bots": len(ACTIVE_BOTS),
        "bots": bots_status
    }
    
    return jsonify(status), 200 if overall_ok else 503

# ============================================================================
# WEBHOOK SETUP (PER-BOT)
# ============================================================================
def set_all_webhooks():
    for bot_id, config in ACTIVE_BOTS.items():
        if config["token"] and config["webhook_url"]:
            api_url = f"https://api.telegram.org/bot{config['token']}/setWebhook"
            # Make sure webhook_url ends with /webhook/bot_id
            webhook_url = config["webhook_url"].rstrip("/") + f"/webhook/{bot_id}"
            try:
                logger.info("Setting webhook for %s to %s", bot_id, webhook_url)
                resp = _session.post(api_url, json={"url": webhook_url}, timeout=10)
                if resp.status_code == 200:
                    logger.info("Webhook set successfully for %s", bot_id)
                else:
                    logger.warning("Failed to set webhook for %s: %s", bot_id, resp.text)
            except Exception as e:
                logger.exception("set_webhook failed for %s: %s", bot_id, e)

# ============================================================================
# GRACEFUL SHUTDOWN
# ============================================================================
def _graceful_shutdown(signum, frame):
    logger.info("Graceful shutdown signal received (%s). Stopping scheduler and workers...", signum)
    
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    
    # Stop all workers for all bots
    for bot_id in ACTIVE_BOTS.keys():
        stop_all_workers_for_bot(bot_id)
    
    # Close all database connections
    for bot_id, conn in DB_CONNECTIONS.items():
        if conn:
            try:
                conn.close()
            except Exception:
                pass
    
    logger.info("Shutdown completed. Exiting.")
    try:
        import os
        os._exit(0)
    except Exception:
        pass

signal.signal(signal.SIGTERM, _graceful_shutdown)
signal.signal(signal.SIGINT, _graceful_shutdown)

# ============================================================================
# INITIALIZATION & MAIN
# ============================================================================
def main():
    # Initialize all bots
    logger.info("Initializing %d active bots: %s", len(ACTIVE_BOTS), ", ".join(ACTIVE_BOTS.keys()))
    initialize_all_bots()
    
    # Set webhooks for all bots
    try:
        set_all_webhooks()
    except Exception:
        logger.exception("Failed to set webhooks during initialization")
    
    # Start Flask app
    port = int(os.environ.get("PORT", "10000"))
    logger.info("Starting Flask app on port %d", port)
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()
