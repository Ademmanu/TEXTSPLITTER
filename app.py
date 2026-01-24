#!/usr/bin/env python3

import os
import time
import json
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
import psycopg2
from psycopg2 import pool

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("multibot_wordsplitter")

app = Flask(__name__)

# ===================== CONFIGURATION =====================

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

# Configuration for all three bots
BOTS_CONFIG = {
    "bot_a": {
        "name": "Bot A",
        "token": os.environ.get("TELEGRAM_TOKEN_A", ""),
        "webhook_url": os.environ.get("WEBHOOK_URL_A", ""),
        "owner_ids_raw": os.environ.get("OWNER_IDS_A", ""),
        "allowed_users_raw": os.environ.get("ALLOWED_USERS_A", ""),
        "owner_tag": "Owner (@justmemmy)",
        "interval_speed": "fast",
        "max_queue_per_user": int(os.environ.get("MAX_QUEUE_PER_USER_A", "5")),
        "max_msg_per_second": float(os.environ.get("MAX_MSG_PER_SECOND_A", "30")),
        "max_concurrent_workers": int(os.environ.get("MAX_CONCURRENT_WORKERS_A", "15")),
    },
    "bot_b": {
        "name": "Bot B",
        "token": os.environ.get("TELEGRAM_TOKEN_B", ""),
        "webhook_url": os.environ.get("WEBHOOK_URL_B", ""),
        "owner_ids_raw": os.environ.get("OWNER_IDS_B", ""),
        "allowed_users_raw": os.environ.get("ALLOWED_USERS_B", ""),
        "owner_tag": "Owner (@justmemmy)",
        "interval_speed": "fast",
        "max_queue_per_user": int(os.environ.get("MAX_QUEUE_PER_USER_B", "5")),
        "max_msg_per_second": float(os.environ.get("MAX_MSG_PER_SECOND_B", "30")),
        "max_concurrent_workers": int(os.environ.get("MAX_CONCURRENT_WORKERS_B", "15")),
    },
    "bot_c": {
        "name": "Bot C",
        "token": os.environ.get("TELEGRAM_TOKEN_C", ""),
        "webhook_url": os.environ.get("WEBHOOK_URL_C", ""),
        "owner_ids_raw": os.environ.get("OWNER_IDS_C", ""),
        "allowed_users_raw": os.environ.get("ALLOWED_USERS_C", ""),
        "owner_tag": "Owner (@justmemmy)",
        "interval_speed": "slow",
        "max_queue_per_user": int(os.environ.get("MAX_QUEUE_PER_USER_C", "5")),
        "max_msg_per_second": float(os.environ.get("MAX_MSG_PER_SECOND_C", "30")),
        "max_concurrent_workers": int(os.environ.get("MAX_CONCURRENT_WORKERS_C", "15")),
    }
}

# Shared settings
SHARED_SETTINGS = {
    "requests_timeout": float(os.environ.get("REQUESTS_TIMEOUT", "10")),
    "log_retention_days": int(os.environ.get("LOG_RETENTION_DAYS", "30")),
    "failure_notify_threshold": int(os.environ.get("FAILURE_NOTIFY_THRESHOLD", "6")),
    "permanent_suspend_days": int(os.environ.get("PERMANENT_SUSPEND_DAYS", "365")),
}

# Initialize bot-specific parsed lists
for bot_id in BOTS_CONFIG:
    config = BOTS_CONFIG[bot_id]
    config["owner_ids"] = parse_id_list(config["owner_ids_raw"])
    config["allowed_users"] = parse_id_list(config["allowed_users_raw"])
    config["primary_owner"] = config["owner_ids"][0] if config["owner_ids"] else None
    config["telegram_api"] = f"https://api.telegram.org/bot{config['token']}" if config['token'] else None

# ===================== POSTGRESQL DATABASE =====================

DB_POOL = None
DB_LOCK = threading.RLock()

def init_postgresql():
    """Initialize PostgreSQL connection pool for all bots"""
    global DB_POOL
    
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("No DATABASE_URL configured - PostgreSQL disabled")
        return
    
    try:
        # Create a connection pool
        DB_POOL = psycopg2.pool.SimpleConnectionPool(
            1,  # Minimum connections
            20, # Maximum connections
            database_url
        )
        
        # Test connection and create tables if needed
        conn = DB_POOL.getconn()
        try:
            with conn.cursor() as cur:
                # Create tables with bot_id column to separate data
                cur.execute("""
                CREATE TABLE IF NOT EXISTS allowed_users (
                    bot_id VARCHAR(10) NOT NULL,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (bot_id, user_id)
                )""")
                
                cur.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id SERIAL PRIMARY KEY,
                    bot_id VARCHAR(10) NOT NULL,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    text TEXT,
                    words_json TEXT,
                    total_words INTEGER,
                    sent_count INTEGER DEFAULT 0,
                    status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    retry_count INTEGER DEFAULT 0
                )""")
                
                cur.execute("""
                CREATE TABLE IF NOT EXISTS split_logs (
                    id SERIAL PRIMARY KEY,
                    bot_id VARCHAR(10) NOT NULL,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    words INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""")
                
                cur.execute("""
                CREATE TABLE IF NOT EXISTS sent_messages (
                    id SERIAL PRIMARY KEY,
                    bot_id VARCHAR(10) NOT NULL,
                    chat_id BIGINT,
                    message_id BIGINT,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    deleted INTEGER DEFAULT 0
                )""")
                
                cur.execute("""
                CREATE TABLE IF NOT EXISTS suspended_users (
                    bot_id VARCHAR(10) NOT NULL,
                    user_id BIGINT NOT NULL,
                    suspended_until TIMESTAMP,
                    reason TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (bot_id, user_id)
                )""")
                
                cur.execute("""
                CREATE TABLE IF NOT EXISTS send_failures (
                    bot_id VARCHAR(10) NOT NULL,
                    user_id BIGINT NOT NULL,
                    failures INTEGER,
                    last_failure_at TIMESTAMP,
                    notified INTEGER DEFAULT 0,
                    last_error_code INTEGER,
                    last_error_desc TEXT,
                    PRIMARY KEY (bot_id, user_id)
                )""")
                
                # Create indexes for better performance
                cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_bot_user_status ON tasks(bot_id, user_id, status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_bot_status ON tasks(bot_id, status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_split_logs_bot_created ON split_logs(bot_id, created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_split_logs_bot_user ON split_logs(bot_id, user_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_allowed_users_bot ON allowed_users(bot_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_bot_last_activity ON tasks(bot_id, last_activity)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_suspended_users_bot_until ON suspended_users(bot_id, suspended_until)")
                
                conn.commit()
                logger.info("PostgreSQL database initialized successfully for all bots")
        finally:
            DB_POOL.putconn(conn)
            
    except Exception as e:
        logger.exception("Failed to initialize PostgreSQL: %s", e)
        DB_POOL = None

def execute_sql(bot_id: str, query: str, params: tuple = (), fetchone: bool = False, fetchall: bool = False):
    """Execute SQL query for specific bot"""
    if not DB_POOL:
        return None
    
    conn = None
    try:
        conn = DB_POOL.getconn()
        with conn.cursor() as cur:
            # Add bot_id to queries that need it
            if 'where' in query.lower() and '%s' in query:
                # For queries with WHERE clause, inject bot_id
                query_parts = query.lower().split('where')
                if len(query_parts) > 1:
                    # Check if bot_id is already in query
                    if 'bot_id' not in query.lower():
                        # Insert bot_id condition at the beginning of WHERE clause
                        query = query_parts[0] + 'WHERE bot_id = %s AND ' + query_parts[1]
                        params = (bot_id,) + params
            
            cur.execute(query, params)
            
            if fetchone:
                result = cur.fetchone()
            elif fetchall:
                result = cur.fetchall()
            else:
                result = cur.rowcount
                conn.commit()
            
            return result
    except Exception as e:
        logger.exception("Database query failed for bot %s: %s", bot_id, e)
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            DB_POOL.putconn(conn)

def check_db_health():
    """Check if database connection is healthy"""
    if not DB_POOL:
        return False
    
    try:
        conn = DB_POOL.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        DB_POOL.putconn(conn)
        return True
    except Exception:
        logger.warning("Database health check failed")
        return False

# Initialize PostgreSQL database
init_postgresql()

# ===================== GLOBALS =====================

# Bot-specific global states
BOT_STATES = {
    bot_id: {
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
    }
    for bot_id in BOTS_CONFIG
}

# ===================== SHARED UTILITIES =====================

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
    config = BOTS_CONFIG[bot_id]
    if username:
        if viewer_id in config["owner_ids"]:
            return f"{at_username(username)} (ID: {viewer_id})"
        return f"{at_username(username)}"
    return f"(ID: {viewer_id})" if viewer_id in config["owner_ids"] else ""

def label_for_owner_view(bot_id: str, target_id: int, target_username: str) -> str:
    if target_username:
        return f"{at_username(target_username)} (ID: {target_id})"
    return str(target_id)

# Initialize token buckets for all bots
for bot_id in BOTS_CONFIG:
    BOT_STATES[bot_id]["token_bucket"] = TokenBucket(BOTS_CONFIG[bot_id]["max_msg_per_second"])
    BOT_STATES[bot_id]["active_workers_semaphore"] = threading.Semaphore(
        BOTS_CONFIG[bot_id]["max_concurrent_workers"]
    )

# ===================== DATABASE FUNCTIONS =====================

def is_allowed(bot_id: str, user_id: int) -> bool:
    """Check if user is allowed for specific bot"""
    if user_id in BOTS_CONFIG[bot_id]["owner_ids"]:
        return True
    
    result = execute_sql(bot_id,
        "SELECT 1 FROM allowed_users WHERE user_id = %s",
        (user_id,),
        fetchone=True
    )
    return bool(result)

def enqueue_task(bot_id: str, user_id: int, username: str, text: str):
    """Enqueue a task for specific bot"""
    words = split_text_to_words(text)
    total = len(words)
    if total == 0:
        return {"ok": False, "reason": "empty"}
    
    # Check queue limit
    result = execute_sql(bot_id,
        "SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'queued'",
        (user_id,),
        fetchone=True
    )
    
    if result and result[0] >= BOTS_CONFIG[bot_id]["max_queue_per_user"]:
        return {"ok": False, "reason": "queue_full", "queue_size": result[0]}
    
    # Insert task
    execute_sql(bot_id,
        """INSERT INTO tasks (user_id, username, text, words_json, total_words, status, created_at, last_activity) 
           VALUES (%s, %s, %s, %s, %s, 'queued', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
        (user_id, username, text, json.dumps(words), total)
    )
    
    # Get updated queue count
    result = execute_sql(bot_id,
        "SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'queued'",
        (user_id,),
        fetchone=True
    )
    
    return {"ok": True, "total_words": total, "queue_size": result[0] if result else 1}

def get_next_task_for_user(bot_id: str, user_id: int):
    """Get next task for specific bot and user"""
    result = execute_sql(bot_id,
        "SELECT id, words_json, total_words, text, retry_count FROM tasks WHERE user_id = %s AND status = 'queued' ORDER BY id ASC LIMIT 1",
        (user_id,),
        fetchone=True
    )
    
    if not result:
        return None
    
    return {
        "id": result[0], 
        "words": json.loads(result[1]) if result[1] else split_text_to_words(result[3]), 
        "total_words": result[2], 
        "text": result[3], 
        "retry_count": result[4]
    }

def set_task_status(bot_id: str, task_id: int, status: str):
    """Set task status for specific bot"""
    if status == "running":
        execute_sql(bot_id,
            "UPDATE tasks SET status = %s, started_at = CURRENT_TIMESTAMP, last_activity = CURRENT_TIMESTAMP WHERE id = %s",
            (status, task_id)
        )
    elif status in ("done", "cancelled"):
        execute_sql(bot_id,
            "UPDATE tasks SET status = %s, finished_at = CURRENT_TIMESTAMP, last_activity = CURRENT_TIMESTAMP WHERE id = %s",
            (status, task_id)
        )
    else:
        execute_sql(bot_id,
            "UPDATE tasks SET status = %s, last_activity = CURRENT_TIMESTAMP WHERE id = %s",
            (status, task_id)
        )

def update_task_activity(bot_id: str, task_id: int):
    """Update task last activity time"""
    execute_sql(bot_id,
        "UPDATE tasks SET last_activity = CURRENT_TIMESTAMP WHERE id = %s",
        (task_id,)
    )

def increment_task_retry(bot_id: str, task_id: int):
    """Increment task retry count"""
    execute_sql(bot_id,
        "UPDATE tasks SET retry_count = retry_count + 1, last_activity = CURRENT_TIMESTAMP WHERE id = %s",
        (task_id,)
    )

def cancel_active_task_for_user(bot_id: str, user_id: int):
    """Cancel all tasks for user in specific bot"""
    execute_sql(bot_id,
        "UPDATE tasks SET status = 'cancelled', finished_at = CURRENT_TIMESTAMP WHERE user_id = %s AND status IN ('queued','running','paused')",
        (user_id,)
    )
    return execute_sql(bot_id,
        "SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'cancelled'",
        (user_id,),
        fetchone=True
    )[0] or 0

def record_split_log(bot_id: str, user_id: int, username: str, count: int = 1):
    """Record split log for specific bot"""
    for _ in range(count):
        execute_sql(bot_id,
            "INSERT INTO split_logs (user_id, username, words, created_at) VALUES (%s, %s, %s, CURRENT_TIMESTAMP)",
            (user_id, username, 1)
        )

def is_suspended(bot_id: str, user_id: int) -> bool:
    """Check if user is suspended for specific bot"""
    if user_id in BOTS_CONFIG[bot_id]["owner_ids"]:
        return False
    
    result = execute_sql(bot_id,
        "SELECT suspended_until FROM suspended_users WHERE user_id = %s",
        (user_id,),
        fetchone=True
    )
    
    if not result:
        return False
    
    try:
        return result[0] > datetime.utcnow()
    except Exception:
        return False

def suspend_user(bot_id: str, target_id: int, seconds: int, reason: str = ""):
    """Suspend user for specific bot"""
    until_time = datetime.utcnow() + timedelta(seconds=seconds)
    
    execute_sql(bot_id,
        """INSERT INTO suspended_users (user_id, suspended_until, reason, added_at) 
           VALUES (%s, %s, %s, CURRENT_TIMESTAMP) 
           ON CONFLICT (user_id) DO UPDATE SET suspended_until = EXCLUDED.suspended_until, reason = EXCLUDED.reason""",
        (target_id, until_time, reason)
    )
    
    # Cancel active tasks
    cancel_active_task_for_user(bot_id, target_id)

def unsuspend_user(bot_id: str, target_id: int) -> bool:
    """Unsuspend user for specific bot"""
    result = execute_sql(bot_id,
        "DELETE FROM suspended_users WHERE user_id = %s RETURNING user_id",
        (target_id,),
        fetchone=True
    )
    return bool(result)

def list_suspended(bot_id: str):
    """List suspended users for specific bot"""
    return execute_sql(bot_id,
        "SELECT user_id, suspended_until, reason, added_at FROM suspended_users ORDER BY suspended_until ASC",
        fetchall=True
    ) or []

def record_failure(bot_id: str, user_id: int, inc: int = 1, error_code: int = None, 
                   description: str = "", is_permanent: bool = False):
    """Record failure for specific bot"""
    # Get current failure count
    result = execute_sql(bot_id,
        "SELECT failures, notified FROM send_failures WHERE user_id = %s",
        (user_id,),
        fetchone=True
    )
    
    if not result:
        failures = inc
        notified = 0
        execute_sql(bot_id,
            "INSERT INTO send_failures (user_id, failures, last_failure_at, notified, last_error_code, last_error_desc) VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s)",
            (user_id, failures, notified, error_code, description)
        )
    else:
        failures = (result[0] or 0) + inc
        notified = result[1] or 0
        execute_sql(bot_id,
            "UPDATE send_failures SET failures = %s, last_failure_at = CURRENT_TIMESTAMP, last_error_code = %s, last_error_desc = %s WHERE user_id = %s",
            (failures, error_code, description, user_id)
        )
    
    if is_permanent or is_permanent_telegram_error(error_code or 0, description):
        mark_user_permanently_unreachable(bot_id, user_id, error_code, description)
        return
    
    if failures >= SHARED_SETTINGS["failure_notify_threshold"] and notified == 0:
        execute_sql(bot_id,
            "UPDATE send_failures SET notified = 1 WHERE user_id = %s",
            (user_id,)
        )
        notify_owners(bot_id, f"⚠️ Repeated send failures for {user_id} ({failures}). Stopping their tasks. 🛑")
        cancel_active_task_for_user(bot_id, user_id)

def reset_failures(bot_id: str, user_id: int):
    """Reset failures for user in specific bot"""
    execute_sql(bot_id,
        "DELETE FROM send_failures WHERE user_id = %s",
        (user_id,)
    )

def mark_user_permanently_unreachable(bot_id: str, user_id: int, error_code: int = None, description: str = ""):
    """Mark user as permanently unreachable for specific bot"""
    if user_id in BOTS_CONFIG[bot_id]["owner_ids"]:
        execute_sql(bot_id,
            "INSERT INTO send_failures (user_id, failures, last_failure_at, notified, last_error_code, last_error_desc) VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET failures = EXCLUDED.failures",
            (user_id, SHARED_SETTINGS["failure_notify_threshold"], 1, error_code, description)
        )
        notify_owners(bot_id, f"⚠️ Repeated send failures for owner {user_id}. Please investigate. Error: {error_code} {description}")
        return
    
    execute_sql(bot_id,
        "INSERT INTO send_failures (user_id, failures, last_failure_at, notified, last_error_code, last_error_desc) VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET failures = EXCLUDED.failures",
        (user_id, 999, 1, error_code, description)
    )
    
    cancel_active_task_for_user(bot_id, user_id)
    suspend_user(bot_id, user_id, SHARED_SETTINGS["permanent_suspend_days"] * 24 * 3600, 
                 f"Permanent send failure: {error_code} {description}")
    
    notify_owners(bot_id, f"⚠️ Repeated send failures for {user_id} ({error_code}). Stopping their tasks. 🛑 Error: {description}")

# Initialize owners and allowed users in database
for bot_id in BOTS_CONFIG:
    config = BOTS_CONFIG[bot_id]
    
    # Ensure owners auto-added as allowed
    for oid in config["owner_ids"]:
        try:
            execute_sql(bot_id,
                "INSERT INTO allowed_users (user_id, username, added_at) VALUES (%s, %s, CURRENT_TIMESTAMP) ON CONFLICT (user_id) DO NOTHING",
                (oid, "")
            )
        except Exception:
            logger.exception("Error ensuring owner in allowed_users for %s", bot_id)
    
    # Ensure provided ALLOWED_USERS auto-added
    for uid in config["allowed_users"]:
        if uid in config["owner_ids"]:
            continue
        try:
            execute_sql(bot_id,
                "INSERT INTO allowed_users (user_id, username, added_at) VALUES (%s, %s, CURRENT_TIMESTAMP) ON CONFLICT (user_id) DO NOTHING",
                (uid, "")
            )
            # Notify user if possible
            try:
                if config["telegram_api"]:
                    get_session(bot_id).post(f"{config['telegram_api']}/sendMessage", json={
                        "chat_id": uid, "text": "✅ You have been added. Send any text to start."
                    }, timeout=3)
            except Exception:
                pass
        except Exception:
            logger.exception("Auto-add allowed user error for %s", bot_id)

# ===================== SESSION MANAGEMENT =====================

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

def acquire_token(bot_id: str, timeout=10.0):
    return BOT_STATES[bot_id]["token_bucket"].acquire(timeout=timeout)

def get_session(bot_id: str, force_new: bool = False):
    """Get or create a requests session for a bot with health checks"""
    state = BOT_STATES[bot_id]
    
    # Check if we need a new session
    current_time = time.time()
    session_age = current_time - state["session_created_at"]
    
    if (state["session"] is None or force_new or 
        session_age > 3600 or  # 1 hour max age
        state["session_request_count"] > 10000):  # Max requests per session
        
        if state["session"]:
            try:
                state["session"].close()
            except Exception:
                pass
        
        session = requests.Session()
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
                pool_connections=BOTS_CONFIG[bot_id]["max_concurrent_workers"]*2,
                pool_maxsize=max(20, BOTS_CONFIG[bot_id]["max_concurrent_workers"]*2),
                pool_block=False
            )
            
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            session.verify = False
            
            session.headers.update({
                'User-Agent': f'Mozilla/5.0 (compatible; WordSplitterBot/{bot_id}/1.0)',
                'Accept': 'application/json',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            })
            
        except Exception as e:
            logger.warning("Could not configure advanced session settings for %s: %s", bot_id, e)
            try:
                adapter = HTTPAdapter(
                    pool_connections=BOTS_CONFIG[bot_id]["max_concurrent_workers"]*2,
                    pool_maxsize=max(20, BOTS_CONFIG[bot_id]["max_concurrent_workers"]*2)
                )
                session.mount("https://", adapter)
                session.mount("http://", adapter)
            except Exception:
                pass
        
        state["session"] = session
        state["session_created_at"] = current_time
        state["session_request_count"] = 0
    
    # Increment request count
    state["session_request_count"] += 1
    
    return state["session"]

# ===================== TELEGRAM UTILITIES =====================

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

# ===================== MESSAGE SENDING =====================

def send_message(bot_id: str, chat_id: int, text: str, reply_markup: Optional[Dict] = None):
    """Send message using bot-specific token with improved resilience"""
    config = BOTS_CONFIG[bot_id]
    if not config["telegram_api"]:
        logger.error("No TELEGRAM_TOKEN for %s; cannot send message.", bot_id)
        return None

    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    entities = _build_entities_for_text(text)
    if entities:
        payload["entities"] = entities
    if reply_markup:
        payload["reply_markup"] = reply_markup

    if not acquire_token(bot_id, timeout=5.0):
        logger.warning("Token acquire timed out for %s; dropping send to %s", bot_id, chat_id)
        record_failure(bot_id, chat_id, inc=1, description="token_acquire_timeout")
        return None

    max_attempts = 3
    attempt = 0
    backoff_base = 0.5
    
    while attempt < max_attempts:
        attempt += 1
        try:
            # Get fresh session if needed
            session = get_session(bot_id, force_new=(attempt > 1))
            resp = session.post(f"{config['telegram_api']}/sendMessage", 
                                json=payload, 
                                timeout=SHARED_SETTINGS["requests_timeout"])
        except requests.exceptions.SSLError as e:
            logger.warning("SSL send error for %s to %s (attempt %s): %s", bot_id, chat_id, attempt, e)
            if attempt >= max_attempts:
                logger.error("SSL error persists for %s to %s after %s attempts", bot_id, chat_id, max_attempts)
                return None
            time.sleep(backoff_base * (4 ** (attempt - 1)))
            continue
        except requests.exceptions.ConnectionError as e:
            logger.warning("Connection send error for %s to %s (attempt %s): %s", bot_id, chat_id, attempt, e)
            if attempt >= max_attempts:
                record_failure(bot_id, chat_id, inc=1, description=f"connection_error: {str(e)}")
                return None
            time.sleep(backoff_base * (2 ** (attempt - 1)))
            continue
        except requests.exceptions.Timeout as e:
            logger.warning("Timeout send error for %s to %s (attempt %s): %s", bot_id, chat_id, attempt, e)
            if attempt >= max_attempts:
                record_failure(bot_id, chat_id, inc=1, description=f"timeout: {str(e)}")
                return None
            time.sleep(backoff_base * (2 ** (attempt - 1)))
            continue
        except requests.exceptions.RequestException as e:
            logger.warning("Network send error for %s to %s (attempt %s): %s", bot_id, chat_id, attempt, e)
            if attempt >= max_attempts:
                record_failure(bot_id, chat_id, inc=1, description=str(e))
                return None
            time.sleep(backoff_base * (2 ** (attempt - 1)))
            continue

        data = parse_telegram_json(resp)
        if not isinstance(data, dict):
            logger.warning("Unexpected non-json response for sendMessage from %s to %s", bot_id, chat_id)
            if attempt >= max_attempts:
                record_failure(bot_id, chat_id, inc=1, description="non_json_response")
                return None
            time.sleep(backoff_base * (2 ** (attempt - 1)))
            continue

        if data.get("ok"):
            try:
                mid = data["result"].get("message_id")
                if mid:
                    execute_sql(bot_id,
                        "INSERT INTO sent_messages (chat_id, message_id, sent_at, deleted) VALUES (%s, %s, CURRENT_TIMESTAMP, 0)",
                        (chat_id, mid)
                    )
            except Exception:
                logger.exception("record sent message failed for %s", bot_id)
            reset_failures(bot_id, chat_id)
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
                record_failure(bot_id, chat_id, inc=1, error_code=error_code, description=description)
                return None
            continue

        if is_permanent_telegram_error(error_code or 0, description):
            logger.info("Permanent error for %s to %s: %s %s", bot_id, chat_id, error_code, description)
            record_failure(bot_id, chat_id, inc=1, error_code=error_code, description=description, is_permanent=True)
            return None

        logger.warning("Transient/send error for %s to %s: %s %s", bot_id, chat_id, error_code, description)
        if attempt >= max_attempts:
            record_failure(bot_id, chat_id, inc=1, error_code=error_code, description=description)
            return None
        time.sleep(backoff_base * (2 ** (attempt - 1)))

def notify_owners(bot_id: str, text: str):
    """Notify all owners of a specific bot"""
    config = BOTS_CONFIG[bot_id]
    for oid in config["owner_ids"]:
        try:
            send_message(bot_id, oid, text)
        except Exception:
            logger.exception("notify owner failed for %s in %s", oid, bot_id)

# ===================== TEXT PROCESSING =====================

def split_text_to_words(text: str) -> List[str]:
    return [w for w in text.strip().split() if w]

# ===================== WORKER MANAGEMENT =====================

def update_worker_heartbeat(bot_id: str, user_id: int):
    """Update heartbeat for a worker"""
    state = BOT_STATES[bot_id]
    with state["worker_heartbeats_lock"]:
        state["worker_heartbeats"][user_id] = time.time()

def get_worker_heartbeat(bot_id: str, user_id: int) -> float:
    """Get last heartbeat timestamp for a worker"""
    state = BOT_STATES[bot_id]
    with state["worker_heartbeats_lock"]:
        return state["worker_heartbeats"].get(user_id, 0)

def cleanup_stale_workers(bot_id: str):
    """Clean up workers that haven't sent heartbeat in 5 minutes"""
    state = BOT_STATES[bot_id]
    current_time = time.time()
    stale_threshold = 300  # 5 minutes
    
    with state["worker_heartbeats_lock"]:
        stale_users = []
        for user_id, last_heartbeat in list(state["worker_heartbeats"].items()):
            if current_time - last_heartbeat > stale_threshold:
                stale_users.append(user_id)
        
        for user_id in stale_users:
            state["worker_heartbeats"].pop(user_id, None)
            
            # Also remove from user_workers if present
            with state["user_workers_lock"]:
                if user_id in state["user_workers"]:
                    info = state["user_workers"][user_id]
                    try:
                        info["stop"].set()
                        info["wake"].set()
                    except Exception:
                        pass
                    state["user_workers"].pop(user_id, None)
                    logger.info("Cleaned up stale worker for user %s in %s", user_id, bot_id)

def notify_user_worker(bot_id: str, user_id: int):
    state = BOT_STATES[bot_id]
    with state["user_workers_lock"]:
        info = state["user_workers"].get(user_id)
        if info and "wake" in info:
            try:
                info["wake"].set()
            except Exception:
                pass

def start_user_worker_if_needed(bot_id: str, user_id: int):
    state = BOT_STATES[bot_id]
    with state["user_workers_lock"]:
        info = state["user_workers"].get(user_id)
        if info:
            thr = info.get("thread")
            if thr and thr.is_alive():
                update_worker_heartbeat(bot_id, user_id)
                return
        wake = threading.Event()
        stop = threading.Event()
        thr = threading.Thread(target=per_user_worker_loop, args=(bot_id, user_id, wake, stop), daemon=True)
        state["user_workers"][user_id] = {"thread": thr, "wake": wake, "stop": stop}
        thr.start()
        update_worker_heartbeat(bot_id, user_id)
        logger.info("Started worker for user %s in %s", user_id, bot_id)

def stop_user_worker(bot_id: str, user_id: int, join_timeout: float = 2.0):
    """Stop a user worker with proper cleanup"""
    state = BOT_STATES[bot_id]
    with state["user_workers_lock"]:
        info = state["user_workers"].get(user_id)
        if not info:
            return
        
        # Mark as stopping
        try:
            info["stop"].set()
            info["wake"].set()
            
            # Wait for thread to finish with timeout
            thr = info.get("thread")
            if thr and thr.is_alive():
                thr.join(join_timeout)
                
                # Force cleanup if still alive
                if thr.is_alive():
                    logger.warning("Worker thread for user %s in %s didn't stop gracefully", user_id, bot_id)
        except Exception as e:
            logger.exception("Error stopping worker for %s in %s: %s", user_id, bot_id, e)
        finally:
            # Clean up heartbeat
            with state["worker_heartbeats_lock"]:
                state["worker_heartbeats"].pop(user_id, None)
            
            # Remove from workers dict
            state["user_workers"].pop(user_id, None)
            logger.info("Stopped worker for user %s in %s", user_id, bot_id)

def per_user_worker_loop(bot_id: str, user_id: int, wake_event: threading.Event, stop_event: threading.Event):
    """Worker loop with bot-specific interval speeds and improved resilience"""
    logger.info("Worker loop starting for user %s in %s", user_id, bot_id)
    config = BOTS_CONFIG[bot_id]
    state = BOT_STATES[bot_id]
    
    # Initial heartbeat
    update_worker_heartbeat(bot_id, user_id)
    
    acquired_semaphore = False
    try:
        uname_for_stat = fetch_display_username(bot_id, user_id) or str(user_id)
        while not stop_event.is_set():
            # Update heartbeat
            update_worker_heartbeat(bot_id, user_id)
            
            if is_suspended(bot_id, user_id):
                cancel_active_task_for_user(bot_id, user_id)
                try:
                    send_message(bot_id, user_id, f"⛔ You have been suspended; stopping your task.")
                except Exception:
                    pass
                while is_suspended(bot_id, user_id) and not stop_event.is_set():
                    wake_event.wait(timeout=5.0)
                    wake_event.clear()
                    update_worker_heartbeat(bot_id, user_id)
                continue

            task = get_next_task_for_user(bot_id, user_id)
            if not task:
                wake_event.wait(timeout=1.0)
                wake_event.clear()
                update_worker_heartbeat(bot_id, user_id)
                continue

            task_id = task["id"]
            words = task["words"]
            total = int(task["total_words"] or len(words))
            retry_count = task.get("retry_count", 0)

            result = execute_sql(bot_id,
                "SELECT sent_count, status FROM tasks WHERE id = %s",
                (task_id,),
                fetchone=True
            )

            if not result or result[1] == "cancelled":
                continue

            update_task_activity(bot_id, task_id)
            update_worker_heartbeat(bot_id, user_id)

            # Acquire concurrency semaphore
            semaphore_acquired = False
            while not stop_event.is_set():
                acquired = state["active_workers_semaphore"].acquire(timeout=1.0)
                if acquired:
                    acquired_semaphore = True
                    semaphore_acquired = True
                    break
                update_task_activity(bot_id, task_id)
                update_worker_heartbeat(bot_id, user_id)
                result = execute_sql(bot_id,
                    "SELECT status FROM tasks WHERE id = %s",
                    (task_id,),
                    fetchone=True
                )
                if not result or result[0] == "cancelled":
                    break

            if not semaphore_acquired:
                continue

            result = execute_sql(bot_id,
                "SELECT sent_count, status FROM tasks WHERE id = %s",
                (task_id,),
                fetchone=True
            )
            if not result or result[1] == "cancelled":
                if acquired_semaphore:
                    state["active_workers_semaphore"].release()
                    acquired_semaphore = False
                continue

            sent = int(result[0] or 0)
            set_task_status(bot_id, task_id, "running")
            update_worker_heartbeat(bot_id, user_id)

            if retry_count > 0:
                try:
                    send_message(bot_id, user_id, f"🔄 Retrying your task (attempt {retry_count + 1})...")
                except Exception:
                    pass

            # BOT-SPECIFIC INTERVAL SPEEDS
            # Bot A & B: fast intervals (0.5-0.7s)
            # Bot C: slow intervals (1.0-1.2s)
            if config["interval_speed"] == "fast":
                interval = 0.5 if total <= 150 else (0.6 if total <= 300 else 0.7)
            else:  # "slow" for Bot C
                interval = 1.0 if total <= 150 else (1.1 if total <= 300 else 1.2)
            
            est_seconds = int((total - sent) * interval)
            est_str = str(timedelta(seconds=est_seconds))
            try:
                send_message(bot_id, user_id, f"🚀 Starting your split now. Words: {total}. Estimated time: {est_str}")
            except Exception:
                pass

            i = sent
            last_send_time = time.monotonic()
            last_activity_update = time.monotonic()
            last_heartbeat_update = time.monotonic()
            consecutive_errors = 0

            while i < total and not stop_event.is_set():
                # Update heartbeat periodically
                current_time = time.monotonic()
                if current_time - last_heartbeat_update > 10:
                    update_worker_heartbeat(bot_id, user_id)
                    last_heartbeat_update = current_time
                
                if current_time - last_activity_update > 30:
                    update_task_activity(bot_id, task_id)
                    last_activity_update = current_time
                
                result = execute_sql(bot_id,
                    "SELECT status FROM tasks WHERE id = %s",
                    (task_id,),
                    fetchone=True
                )
                if not result:
                    break
                status = result[0]
                if status == "cancelled" or is_suspended(bot_id, user_id):
                    break

                if status == "paused":
                    try:
                        send_message(bot_id, user_id, f"⏸️ Task paused…")
                    except Exception:
                        pass
                    while True:
                        wake_event.wait(timeout=0.7)
                        wake_event.clear()
                        update_worker_heartbeat(bot_id, user_id)
                        if stop_event.is_set():
                            break
                        result = execute_sql(bot_id,
                            "SELECT status FROM tasks WHERE id = %s",
                            (task_id,),
                            fetchone=True
                        )
                        if not result or result[0] == "cancelled" or is_suspended(bot_id, user_id):
                            break
                        if result[0] == "running":
                            try:
                                send_message(bot_id, user_id, "▶️ Resuming your task now.")
                            except Exception:
                                pass
                            last_send_time = time.monotonic()
                            last_activity_update = time.monotonic()
                            last_heartbeat_update = time.monotonic()
                            break
                    if status == "cancelled" or is_suspended(bot_id, user_id) or stop_event.is_set():
                        if is_suspended(bot_id, user_id):
                            set_task_status(bot_id, task_id, "cancelled")
                            try: 
                                send_message(bot_id, user_id, "⛔ You have been suspended; stopping your task.")
                            except Exception: 
                                pass
                        break

                try:
                    result = send_message(bot_id, user_id, words[i])
                    if result:
                        consecutive_errors = 0
                        record_split_log(bot_id, user_id, uname_for_stat, 1)
                    else:
                        consecutive_errors += 1
                        logger.warning(f"Failed to send word {i+1} to user {user_id} in {bot_id} (consecutive errors: {consecutive_errors})")
                        
                        if consecutive_errors >= 10:
                            logger.error(f"Too many consecutive errors ({consecutive_errors}) for user {user_id} in {bot_id}. Pausing task.")
                            set_task_status(bot_id, task_id, "paused")
                            try:
                                send_message(bot_id, user_id, f"⚠️ Task paused due to sending errors. Will retry in 30 seconds.")
                            except Exception:
                                pass
                            time.sleep(30)
                            set_task_status(bot_id, task_id, "running")
                            consecutive_errors = 0
                            continue
                        
                        record_split_log(bot_id, user_id, uname_for_stat, 1)
                except Exception as e:
                    logger.error(f"Exception sending word {i+1} to user {user_id} in {bot_id}: {e}")
                    consecutive_errors += 1
                    record_split_log(bot_id, user_id, uname_for_stat, 1)

                i += 1

                try:
                    execute_sql(bot_id,
                        "UPDATE tasks SET sent_count = %s, last_activity = CURRENT_TIMESTAMP WHERE id = %s",
                        (i, task_id)
                    )
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

                if is_suspended(bot_id, user_id):
                    break

            result = execute_sql(bot_id,
                "SELECT status, sent_count FROM tasks WHERE id = %s",
                (task_id,),
                fetchone=True
            )

            final_status = result[0] if result else "done"
            if final_status not in ("cancelled", "paused"):
                set_task_status(bot_id, task_id, "done")
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
                    state["active_workers_semaphore"].release()
                except Exception:
                    pass
                acquired_semaphore = False

    except Exception:
        logger.exception("Worker error for user %s in %s", user_id, bot_id)
    finally:
        # Clean up heartbeat
        with state["worker_heartbeats_lock"]:
            state["worker_heartbeats"].pop(user_id, None)
        
        if acquired_semaphore:
            try:
                state["active_workers_semaphore"].release()
            except Exception:
                pass
        with state["user_workers_lock"]:
            state["user_workers"].pop(user_id, None)
        logger.info("Worker loop exiting for user %s in %s", user_id, bot_id)

# ===================== STATISTICS =====================

def fetch_display_username(bot_id: str, user_id: int):
    """Fetch display username for a user in specific bot"""
    result = execute_sql(bot_id,
        "SELECT username FROM split_logs WHERE user_id = %s ORDER BY created_at DESC LIMIT 1",
        (user_id,),
        fetchone=True
    )
    if result and result[0]:
        return result[0]
    
    result = execute_sql(bot_id,
        "SELECT username FROM allowed_users WHERE user_id = %s",
        (user_id,),
        fetchone=True
    )
    if result and result[0]:
        return result[0]
    
    return ""

def compute_last_hour_stats(bot_id: str):
    """Compute statistics for last hour for specific bot"""
    cutoff = datetime.utcnow() - timedelta(hours=1)
    result = execute_sql(bot_id,
        """SELECT user_id, username, COUNT(*) as s
           FROM split_logs
           WHERE created_at >= %s
           GROUP BY user_id, username
           ORDER BY s DESC""",
        (cutoff,),
        fetchall=True
    )
    
    if not result:
        return []
    
    stat_map = {}
    for uid, uname, s in result:
        stat_map[uid] = {"uname": uname, "words": stat_map.get(uid,{}).get("words",0)+int(s)}
    return [(k, v["uname"], v["words"]) for k, v in stat_map.items()]

def compute_last_12h_stats(bot_id: str, user_id: int):
    """Compute statistics for last 12 hours for specific bot and user"""
    cutoff = datetime.utcnow() - timedelta(hours=12)
    result = execute_sql(bot_id,
        "SELECT COUNT(*) FROM split_logs WHERE user_id = %s AND created_at >= %s",
        (user_id, cutoff),
        fetchone=True
    )
    return int(result[0] or 0) if result else 0

def get_user_task_counts(bot_id: str, user_id: int):
    """Get task counts for user in specific bot"""
    active_result = execute_sql(bot_id,
        "SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status IN ('running','paused')",
        (user_id,),
        fetchone=True
    )
    queued_result = execute_sql(bot_id,
        "SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'queued'",
        (user_id,),
        fetchone=True
    )
    
    active = int(active_result[0] or 0) if active_result else 0
    queued = int(queued_result[0] or 0) if queued_result else 0
    return active, queued

# ===================== SCHEDULED TASKS =====================

def send_hourly_owner_stats(bot_id: str):
    """Send hourly statistics to owners of specific bot"""
    rows = compute_last_hour_stats(bot_id)
    if not rows:
        msg = "📊 Hourly Report: no splits in the last hour."
        for oid in BOTS_CONFIG[bot_id]["owner_ids"]:
            try:
                send_message(bot_id, oid, msg)
            except Exception:
                pass
        return
    lines = []
    for uid, uname, w in rows:
        uname_for_stat = at_username(uname) if uname else fetch_display_username(bot_id, uid)
        lines.append(f"{uid} ({uname_for_stat}) - {w} words sent")
    body = "📊 Report - last 1h:\n" + "\n".join(lines)
    for oid in BOTS_CONFIG[bot_id]["owner_ids"]:
        try:
            send_message(bot_id, oid, body)
        except Exception:
            pass

def check_and_lift(bot_id: str):
    """Check and lift expired suspensions for specific bot"""
    rows = list_suspended(bot_id)
    now = datetime.utcnow()
    for row in rows:
        try:
            uid, until, reason, added_at = row
            if until <= now:
                unsuspend_user(bot_id, uid)
        except Exception:
            logger.exception("suspend parse error for %s in %s", row, bot_id)

def prune_old_logs(bot_id: str):
    """Prune old logs for specific bot"""
    try:
        cutoff = datetime.utcnow() - timedelta(days=SHARED_SETTINGS["log_retention_days"])
        
        deleted1 = execute_sql(bot_id,
            "DELETE FROM split_logs WHERE created_at < %s",
            (cutoff,)
        ) or 0
        
        deleted2 = execute_sql(bot_id,
            "DELETE FROM sent_messages WHERE sent_at < %s",
            (cutoff,)
        ) or 0
        
        if deleted1 or deleted2:
            logger.info("Pruned logs for %s: split_logs=%s sent_messages=%s", bot_id, deleted1, deleted2)
    except Exception:
        logger.exception("prune_old_logs error for %s", bot_id)

def check_stuck_tasks(bot_id: str):
    """Check for stuck tasks for specific bot"""
    try:
        cutoff = datetime.utcnow() - timedelta(minutes=2)
        
        stuck_tasks = execute_sql(bot_id,
            "SELECT id, user_id, status, retry_count FROM tasks WHERE status = 'running' AND last_activity < %s",
            (cutoff,),
            fetchall=True
        ) or []
        
        for task_id, user_id, status, retry_count in stuck_tasks:
            logger.warning(f"Stuck task detected in {bot_id}: task_id={task_id}, user_id={user_id}, status={status}, retry_count={retry_count}")
            
            if retry_count < 3:
                execute_sql(bot_id,
                    "UPDATE tasks SET status = 'queued', retry_count = retry_count + 1, last_activity = CURRENT_TIMESTAMP WHERE id = %s",
                    (task_id,)
                )
                logger.info(f"Reset stuck task {task_id} to queued in {bot_id} (retry {retry_count + 1})")
                notify_user_worker(bot_id, user_id)
            else:
                execute_sql(bot_id,
                    "UPDATE tasks SET status = 'cancelled', finished_at = CURRENT_TIMESTAMP WHERE id = %s",
                    (task_id,)
                )
                logger.info(f"Cancelled stuck task {task_id} in {bot_id} after {retry_count} retries")
                try:
                    send_message(bot_id, user_id, f"🛑 Your task was cancelled after multiple failures. Please try again.")
                except Exception:
                    pass
        
        if stuck_tasks:
            logger.info(f"Cleaned up {len(stuck_tasks)} stuck tasks in {bot_id}")
    except Exception:
        logger.exception("Error checking for stuck tasks in %s", bot_id)

def cleanup_stale_resources(bot_id: str):
    """Clean up stale workers and refresh sessions for specific bot"""
    state = BOT_STATES[bot_id]
    
    # Clean up stale workers
    cleanup_stale_workers(bot_id)
    
    # Refresh session if old
    current_time = time.time()
    session_age = current_time - state["session_created_at"]
    if session_age > 3600 and state["session"]:  # 1 hour
        try:
            state["session"].close()
        except Exception:
            pass
        state["session"] = None
        state["session_created_at"] = 0
        state["session_request_count"] = 0
        logger.info("Refreshed session for %s", bot_id)

# ===================== SCHEDULER =====================

scheduler = BackgroundScheduler()

# Add jobs for each bot
for bot_id in BOTS_CONFIG:
    scheduler.add_job(
        lambda b=bot_id: send_hourly_owner_stats(b),
        "interval", 
        hours=1, 
        next_run_time=datetime.utcnow() + timedelta(seconds=10),
        timezone='UTC',
        id=f"hourly_stats_{bot_id}"
    )
    scheduler.add_job(
        lambda b=bot_id: check_and_lift(b),
        "interval", 
        minutes=1,
        next_run_time=datetime.utcnow() + timedelta(seconds=15),
        timezone='UTC',
        id=f"check_suspended_{bot_id}"
    )
    scheduler.add_job(
        lambda b=bot_id: prune_old_logs(b),
        "interval", 
        hours=24,
        next_run_time=datetime.utcnow() + timedelta(seconds=30),
        timezone='UTC',
        id=f"prune_logs_{bot_id}"
    )
    scheduler.add_job(
        lambda b=bot_id: check_stuck_tasks(b),
        "interval", 
        minutes=1,
        next_run_time=datetime.utcnow() + timedelta(seconds=45),
        timezone='UTC',
        id=f"check_stuck_{bot_id}"
    )
    scheduler.add_job(
        lambda b=bot_id: cleanup_stale_resources(b),
        "interval",
        minutes=5,
        next_run_time=datetime.utcnow() + timedelta(seconds=60),
        timezone='UTC',
        id=f"cleanup_resources_{bot_id}"
    )

scheduler.start()

# ===================== SHUTDOWN HANDLER =====================

def _graceful_shutdown(signum, frame):
    logger.info("Graceful shutdown signal received (%s). Stopping scheduler and workers...", signum)
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    
    # Stop workers for all bots
    for bot_id in BOTS_CONFIG:
        state = BOT_STATES[bot_id]
        with state["user_workers_lock"]:
            keys = list(state["user_workers"].keys())
        for k in keys:
            stop_user_worker(bot_id, k, join_timeout=2.0)
    
    # Close database connection pool
    if DB_POOL:
        try:
            DB_POOL.closeall()
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

# ===================== OWNER OPERATIONS =====================

def get_owner_state(bot_id: str, user_id: int) -> Optional[Dict]:
    state = BOT_STATES[bot_id]
    with state["owner_ops_lock"]:
        return state["owner_ops_state"].get(user_id)

def set_owner_state(bot_id: str, user_id: int, state_dict: Dict):
    state = BOT_STATES[bot_id]
    with state["owner_ops_lock"]:
        state["owner_ops_state"][user_id] = state_dict

def clear_owner_state(bot_id: str, user_id: int):
    state = BOT_STATES[bot_id]
    with state["owner_ops_lock"]:
        state["owner_ops_state"].pop(user_id, None)

def is_owner_in_operation(bot_id: str, user_id: int) -> bool:
    state = BOT_STATES[bot_id]
    with state["owner_ops_lock"]:
        return user_id in state["owner_ops_state"]

def get_user_tasks_preview(bot_id: str, user_id: int, hours: int, page: int = 0) -> Tuple[List[Dict], int, int]:
    """Get user tasks preview for specific bot"""
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    
    result = execute_sql(bot_id,
        """SELECT id, text, created_at, total_words, sent_count
           FROM tasks 
           WHERE user_id = %s AND created_at >= %s
           ORDER BY created_at DESC""",
        (user_id, cutoff),
        fetchall=True
    ) or []
    
    tasks = []
    for r in result:
        task_id, text, created_at, total_words, sent_count = r
        words = split_text_to_words(text)
        preview = " ".join(words[:2]) if len(words) >= 2 else words[0] if words else "(empty)"
        tasks.append({
            "id": task_id,
            "preview": preview,
            "created_at": utc_to_wat_ts(created_at.strftime("%Y-%m-%d %H:%M:%S")),
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

def get_all_users_ordered(bot_id: str):
    """Get all users ordered by addition time for specific bot"""
    return execute_sql(bot_id,
        "SELECT user_id, username, added_at FROM allowed_users ORDER BY added_at DESC",
        fetchall=True
    ) or []

def get_user_index(bot_id: str, user_id: int):
    """Get user index in ordered list for specific bot"""
    users = get_all_users_ordered(bot_id)
    for i, (uid, username, added_at) in enumerate(users):
        if uid == user_id:
            return i, users
    return -1, users

def parse_duration(duration_str: str) -> Tuple[int, str]:
    """Parse duration string to seconds"""
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

def send_ownersets_menu(bot_id: str, owner_id: int):
    """Send owner menu for specific bot"""
    config = BOTS_CONFIG[bot_id]
    menu_text = f"👑 Owner Menu {config['owner_tag']}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nSelect an operation:"
    
    keyboard = [
        [{"text": "📊 Bot Info", "callback_data": "owner_botinfo"}, {"text": "👥 List Users", "callback_data": "owner_listusers"}],
        [{"text": "🚫 List Suspended", "callback_data": "owner_listsuspended"}, {"text": "➕ Add User", "callback_data": "owner_adduser"}],
        [{"text": "⏸️ Suspend User", "callback_data": "owner_suspend"}, {"text": "▶️ Unsuspend User", "callback_data": "owner_unsuspend"}],
        [{"text": "🔍 Check All User Preview", "callback_data": "owner_checkallpreview"}]
    ]
    
    reply_markup = {"inline_keyboard": keyboard}
    send_message(bot_id, owner_id, menu_text, reply_markup)

# ===================== COMMAND HANDLING =====================

def handle_command(bot_id: str, user_id: int, username: str, command: str, args: str):
    """Handle command for specific bot"""
    config = BOTS_CONFIG[bot_id]
    
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

    if command == "/about":
        msg = (
            "ℹ️ About:\n"
            "I split texts into single words. ✂️\n\n"
            "Features:\n"
            "queueing, pause/resume,\n"
            "hourly owner stats, rate-limited sending. ⚖️"
        )
        send_message(bot_id, user_id, msg)
        return jsonify({"ok": True})

    if user_id not in config["owner_ids"] and not is_allowed(bot_id, user_id):
        send_message(bot_id, user_id, f"🚫 Sorry, you are not allowed. {config['owner_tag']} notified.\nYour ID: {user_id}")
        notify_owners(bot_id, f"🚨 Unallowed access attempt by {at_username(username) if username else user_id} (ID: {user_id}).")
        return jsonify({"ok": True})

    if command == "/example":
        sample = "\n".join([
            "996770061141", "996770064514", "996770071665", "996770073284",
            "996770075145", "996770075627", "996770075973", "996770076350",
            "996770076869", "996770077101"
        ])
        res = enqueue_task(bot_id, user_id, username, sample)
        if not res["ok"]:
            send_message(bot_id, user_id, "❗ Could not queue demo. Try later.")
            return jsonify({"ok": True})
        start_user_worker_if_needed(bot_id, user_id)
        notify_user_worker(bot_id, user_id)
        active, queued = get_user_task_counts(bot_id, user_id)
        if active:
            send_message(bot_id, user_id, f"✅ Task added. Words: {res['total_words']}.\nQueue position: {queued}")
        else:
            send_message(bot_id, user_id, f"✅ Task added. Words: {res['total_words']}.")
        return jsonify({"ok": True})

    if command == "/pause":
        result = execute_sql(bot_id,
            "SELECT id FROM tasks WHERE user_id = %s AND status = 'running' ORDER BY started_at ASC LIMIT 1",
            (user_id,),
            fetchone=True
        )
        if not result:
            send_message(bot_id, user_id, "ℹ️ No active task to pause.")
            return jsonify({"ok": True})
        set_task_status(bot_id, result[0], "paused")
        notify_user_worker(bot_id, user_id)
        send_message(bot_id, user_id, "⏸️ Paused. Use /resume to continue.")
        return jsonify({"ok": True})

    if command == "/resume":
        result = execute_sql(bot_id,
            "SELECT id FROM tasks WHERE user_id = %s AND status = 'paused' ORDER BY started_at ASC LIMIT 1",
            (user_id,),
            fetchone=True
        )
        if not result:
            send_message(bot_id, user_id, "ℹ️ No paused task to resume.")
            return jsonify({"ok": True})
        set_task_status(bot_id, result[0], "running")
        notify_user_worker(bot_id, user_id)
        send_message(bot_id, user_id, "▶️ Resuming your task now.")
        return jsonify({"ok": True})

    if command == "/status":
        active_result = execute_sql(bot_id,
            "SELECT id, status, total_words, sent_count FROM tasks WHERE user_id = %s AND status IN ('running','paused') ORDER BY started_at ASC LIMIT 1",
            (user_id,),
            fetchone=True
        )
        queued_result = execute_sql(bot_id,
            "SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'queued'",
            (user_id,),
            fetchone=True
        )
        queued = queued_result[0] if queued_result else 0
        
        if active_result:
            aid, status, total, sent = active_result
            remaining = int(total or 0) - int(sent or 0)
            send_message(bot_id, user_id, f"ℹ️ Status: {status}\nRemaining words: {remaining}\nQueue size: {queued}")
        elif queued > 0:
            send_message(bot_id, user_id, f"⏳ Waiting. Queue size: {queued}")
        else:
            send_message(bot_id, user_id, "✅ You have no active or queued tasks.")
        return jsonify({"ok": True})

    if command == "/stop":
        queued_result = execute_sql(bot_id,
            "SELECT COUNT(*) FROM tasks WHERE user_id = %s AND status = 'queued'",
            (user_id,),
            fetchone=True
        )
        queued = queued_result[0] if queued_result else 0
        stopped = cancel_active_task_for_user(bot_id, user_id)
        stop_user_worker(bot_id, user_id)
        if stopped > 0 or queued > 0:
            send_message(bot_id, user_id, "🛑 Active task stopped. Your queued tasks were cleared too.")
        else:
            send_message(bot_id, user_id, "ℹ️ You had no active or queued tasks.")
        return jsonify({"ok": True})

    if command == "/stats":
        words = compute_last_12h_stats(bot_id, user_id)
        send_message(bot_id, user_id, f"📊 Your last 12 hours: {words} words split")
        return jsonify({"ok": True})

    send_message(bot_id, user_id, "❓ Unknown command.")
    return jsonify({"ok": True})

def handle_user_text(bot_id: str, user_id: int, username: str, text: str):
    """Handle user text for specific bot"""
    config = BOTS_CONFIG[bot_id]
    
    # BLOCK OWNER TASK PROCESSING
    if user_id in config["owner_ids"] and is_owner_in_operation(bot_id, user_id):
        logger.warning(f"Owner {user_id} text reached handle_user_text while in operation state in {bot_id}. Text: {text[:50]}...")
        return jsonify({"ok": True})
    
    if user_id not in config["owner_ids"] and not is_allowed(bot_id, user_id):
        send_message(bot_id, user_id, f"🚫 Sorry, you are not allowed. {config['owner_tag']} notified.\nYour ID: {user_id}")
        notify_owners(bot_id, f"🚨 Unallowed access attempt by {at_username(username) if username else user_id} (ID: {user_id}).")
        return jsonify({"ok": True})
    
    if is_suspended(bot_id, user_id):
        result = execute_sql(bot_id,
            "SELECT suspended_until FROM suspended_users WHERE user_id = %s",
            (user_id,),
            fetchone=True
        )
        if result:
            until_utc = result[0]
            until_wat = utc_to_wat_ts(until_utc.strftime("%Y-%m-%d %H:%M:%S")) if until_utc else "unknown"
            send_message(bot_id, user_id, f"⛔ You have been suspended until {until_wat} by {config['owner_tag']}.")
        return jsonify({"ok": True})
    
    res = enqueue_task(bot_id, user_id, username, text)
    if not res["ok"]:
        if res["reason"] == "empty":
            send_message(bot_id, user_id, "⚠️ Empty text. Nothing to split.")
            return jsonify({"ok": True})
        if res["reason"] == "queue_full":
            send_message(bot_id, user_id, f"⏳ Your queue is full ({res['queue_size']}). Use /stop or wait.")
            return jsonify({"ok": True})
        send_message(bot_id, user_id, "❗ Could not queue task. Try later.")
        return jsonify({"ok": True})
    
    start_user_worker_if_needed(bot_id, user_id)
    notify_user_worker(bot_id, user_id)
    active, queued = get_user_task_counts(bot_id, user_id)
    if active:
        send_message(bot_id, user_id, f"✅ Task added. Words: {res['total_words']}.\nQueue position: {queued}")
    else:
        send_message(bot_id, user_id, f"✅ Task added. Words: {res['total_words']}.")
    return jsonify({"ok": True})

# ===================== WEBHOOK HANDLERS =====================

def handle_webhook(bot_id: str):
    """Handle webhook updates for a specific bot"""
    try:
        update = request.get_json(force=True)
    except Exception:
        return jsonify({"ok": False}), 400
    
    try:
        config = BOTS_CONFIG[bot_id]
        state = BOT_STATES[bot_id]
        
        # Handle callback queries
        if "callback_query" in update:
            callback = update["callback_query"]
            user = callback.get("from", {})
            uid = user.get("id")
            data = callback.get("data", "")
            
            # Check if user is an owner for this bot
            if uid not in config["owner_ids"]:
                try:
                    get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "⛔ Owner only."
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
            
            # Handle callback data with bot-specific context
            if data == "owner_close":
                try:
                    get_session(bot_id).post(f"{config['telegram_api']}/deleteMessage", json={
                        "chat_id": callback["message"]["chat"]["id"],
                        "message_id": callback["message"]["message_id"]
                    }, timeout=2)
                except Exception:
                    pass
                try:
                    get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "✅ Menu closed."
                    }, timeout=2)
                except Exception:
                    pass
                clear_owner_state(bot_id, uid)
                return jsonify({"ok": True})
            
            elif data == "owner_botinfo":
                # Get bot-specific info
                active_rows = execute_sql(bot_id,
                    "SELECT user_id, username, SUM(total_words - COALESCE(sent_count,0)) as remaining, COUNT(*) as active_count FROM tasks WHERE status IN ('running','paused') GROUP BY user_id, username",
                    fetchall=True
                ) or []
                
                queued_tasks_result = execute_sql(bot_id,
                    "SELECT COUNT(*) FROM tasks WHERE status = 'queued'",
                    fetchone=True
                )
                queued_tasks = queued_tasks_result[0] if queued_tasks_result else 0
                
                queued_counts = {}
                queued_rows = execute_sql(bot_id,
                    "SELECT user_id, COUNT(*) FROM tasks WHERE status = 'queued' GROUP BY user_id",
                    fetchall=True
                ) or []
                for row in queued_rows:
                    queued_counts[row[0]] = row[1]
                
                stats_rows = compute_last_hour_stats(bot_id)
                lines_active = []
                for r in active_rows:
                    uid2, uname, rem, ac = r
                    if not uname:
                        uname = fetch_display_username(bot_id, uid2)
                    name = f" ({at_username(uname)})" if uname else ""
                    queued_for_user = queued_counts.get(uid2, 0)
                    lines_active.append(f"{uid2}{name} - {int(rem)} remaining - {int(ac)} active - {queued_for_user} queued")
                
                lines_stats = []
                for uid2, uname, s in stats_rows:
                    uname_final = at_username(uname) if uname else fetch_display_username(bot_id, uid2)
                    lines_stats.append(f"{uid2} ({uname_final}) - {int(s)} words sent")
                
                total_allowed_result = execute_sql(bot_id,
                    "SELECT COUNT(*) FROM allowed_users",
                    fetchone=True
                )
                total_allowed = total_allowed_result[0] if total_allowed_result else 0
                
                total_suspended_result = execute_sql(bot_id,
                    "SELECT COUNT(*) FROM suspended_users",
                    fetchone=True
                )
                total_suspended = total_suspended_result[0] if total_suspended_result else 0
                
                body = (
                    f"🤖 {config['name']} Status\n"
                    f"👥 Allowed users: {total_allowed}\n"
                    f"🚫 Suspended users: {total_suspended}\n"
                    f"⚙️ Active tasks: {len(active_rows)}\n"
                    f"📨 Queued tasks: {queued_tasks}\n\n"
                    "Users with active tasks:\n" + ("\n".join(lines_active) if lines_active else "(none)") + "\n\n"
                    "User stats (last 1h):\n" + ("\n".join(lines_stats) if lines_stats else "(none)")
                )
                
                menu_text = f"👑 Owner Menu {config['owner_tag']}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{body}"
                keyboard = [
                    [{"text": "📊 Bot Info", "callback_data": "owner_botinfo"}, {"text": "👥 List Users", "callback_data": "owner_listusers"}],
                    [{"text": "🚫 List Suspended", "callback_data": "owner_listsuspended"}, {"text": "➕ Add User", "callback_data": "owner_adduser"}],
                    [{"text": "⏸️ Suspend User", "callback_data": "owner_suspend"}, {"text": "▶️ Unsuspend User", "callback_data": "owner_unsuspend"}],
                    [{"text": "🔍 Check All User Preview", "callback_data": "owner_checkallpreview"}]
                ]
                
                try:
                    get_session(bot_id).post(f"{config['telegram_api']}/editMessageText", json={
                        "chat_id": callback["message"]["chat"]["id"],
                        "message_id": callback["message"]["message_id"],
                        "text": menu_text,
                        "reply_markup": {"inline_keyboard": keyboard}
                    }, timeout=2)
                except Exception:
                    pass
                try:
                    get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "✅ Bot info loaded."
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
            
            elif data == "owner_listusers":
                rows = execute_sql(bot_id,
                    "SELECT user_id, username, added_at FROM allowed_users ORDER BY added_at DESC",
                    fetchall=True
                ) or []
                
                lines = []
                for r in rows:
                    uid2, uname, added_at_utc = r
                    uname_s = f"({at_username(uname)})" if uname else "(no username)"
                    added_at_wat = utc_to_wat_ts(added_at_utc.strftime("%Y-%m-%d %H:%M:%S")) if added_at_utc else "unknown"
                    lines.append(f"{uid2} {uname_s} added={added_at_wat}")
                
                body = "👥 Allowed users:\n" + ("\n".join(lines) if lines else "(none)")
                menu_text = f"👑 Owner Menu {config['owner_tag']}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{body}"
                keyboard = [
                    [{"text": "📊 Bot Info", "callback_data": "owner_botinfo"}, {"text": "👥 List Users", "callback_data": "owner_listusers"}],
                    [{"text": "🚫 List Suspended", "callback_data": "owner_listsuspended"}, {"text": "➕ Add User", "callback_data": "owner_adduser"}],
                    [{"text": "⏸️ Suspend User", "callback_data": "owner_suspend"}, {"text": "▶️ Unsuspend User", "callback_data": "owner_unsuspend"}],
                    [{"text": "🔍 Check All User Preview", "callback_data": "owner_checkallpreview"}]
                ]
                
                try:
                    get_session(bot_id).post(f"{config['telegram_api']}/editMessageText", json={
                        "chat_id": callback["message"]["chat"]["id"],
                        "message_id": callback["message"]["message_id"],
                        "text": menu_text,
                        "reply_markup": {"inline_keyboard": keyboard}
                    }, timeout=2)
                except Exception:
                    pass
                try:
                    get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "✅ User list loaded."
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
            
            elif data == "owner_listsuspended":
                # Auto-unsuspend expired ones first
                for row in list_suspended(bot_id)[:]:
                    uid2, until_utc, reason, added_at_utc = row
                    if until_utc <= datetime.utcnow():
                        unsuspend_user(bot_id, uid2)
                
                rows = list_suspended(bot_id)
                if not rows:
                    body = "✅ No suspended users."
                else:
                    lines = []
                    for r in rows:
                        uid2, until_utc, reason, added_at_utc = r
                        until_wat = utc_to_wat_ts(until_utc.strftime("%Y-%m-%d %H:%M:%S")) if until_utc else "unknown"
                        added_wat = utc_to_wat_ts(added_at_utc.strftime("%Y-%m-%d %H:%M:%S")) if added_at_utc else "unknown"
                        uname = fetch_display_username(bot_id, uid2)
                        uname_s = f"({at_username(uname)})" if uname else ""
                        lines.append(f"{uid2} {uname_s} until={until_wat} reason={reason}")
                    body = "🚫 Suspended users:\n" + "\n".join(lines)
                
                menu_text = f"👑 Owner Menu {config['owner_tag']}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{body}"
                keyboard = [
                    [{"text": "📊 Bot Info", "callback_data": "owner_botinfo"}, {"text": "👥 List Users", "callback_data": "owner_listusers"}],
                    [{"text": "🚫 List Suspended", "callback_data": "owner_listsuspended"}, {"text": "➕ Add User", "callback_data": "owner_adduser"}],
                    [{"text": "⏸️ Suspend User", "callback_data": "owner_suspend"}, {"text": "▶️ Unsuspend User", "callback_data": "owner_unsuspend"}],
                    [{"text": "🔍 Check All User Preview", "callback_data": "owner_checkallpreview"}]
                ]
                
                try:
                    get_session(bot_id).post(f"{config['telegram_api']}/editMessageText", json={
                        "chat_id": callback["message"]["chat"]["id"],
                        "message_id": callback["message"]["message_id"],
                        "text": menu_text,
                        "reply_markup": {"inline_keyboard": keyboard}
                    }, timeout=2)
                except Exception:
                    pass
                try:
                    get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "✅ Suspended list loaded."
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
            
            elif data == "owner_backtomenu":
                send_ownersets_menu(bot_id, uid)
                try:
                    get_session(bot_id).post(f"{config['telegram_api']}/deleteMessage", json={
                        "chat_id": callback["message"]["chat"]["id"],
                        "message_id": callback["message"]["message_id"]
                    }, timeout=2)
                except Exception:
                    pass
                try:
                    get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "✅ Returning to menu."
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
            
            elif data.startswith("owner_checkallpreview_"):
                parts = data.split("_")
                
                if len(parts) == 5:
                    target_user = int(parts[2])
                    page = int(parts[3])
                    hours = int(parts[4])
                    
                    user_index, all_users = get_user_index(bot_id, target_user)
                    if user_index == -1:
                        if all_users:
                            target_user = all_users[0][0]
                            user_index = 0
                        else:
                            try:
                                get_session(bot_id).post(f"{config['telegram_api']}/editMessageText", json={
                                    "chat_id": callback["message"]["chat"]["id"],
                                    "message_id": callback["message"]["message_id"],
                                    "text": "📋 No users found.",
                                }, timeout=2)
                            except Exception:
                                pass
                            return jsonify({"ok": True})
                    
                    tasks, total_tasks, total_pages = get_user_tasks_preview(bot_id, target_user, hours, page)
                    user_info = all_users[user_index]
                    user_id_info, username_info, added_at_info = user_info
                    username_display = at_username(username_info) if username_info else "no username"
                    added_wat = utc_to_wat_ts(added_at_info.strftime("%Y-%m-%d %H:%M:%S")) if added_at_info else "unknown"
                    
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
                        task_nav.append({"text": "⬅️ Prev Page", "callback_data": f"owner_checkallpreview_{target_user}_{page-1}_{hours}"})
                    if page + 1 < total_pages:
                        task_nav.append({"text": "Next Page ➡️", "callback_data": f"owner_checkallpreview_{target_user}_{page+1}_{hours}"})
                    if task_nav:
                        keyboard.append(task_nav)
                    
                    user_nav = []
                    if user_index > 0:
                        prev_user_id = all_users[user_index-1][0]
                        user_nav.append({"text": "⬅️ Prev User", "callback_data": f"owner_checkallpreview_{prev_user_id}_0_{hours}"})
                    
                    user_nav.append({"text": f"User {user_index+1}/{len(all_users)}", "callback_data": "owner_checkallpreview_noop"})
                    
                    if user_index + 1 < len(all_users):
                        next_user_id = all_users[user_index+1][0]
                        user_nav.append({"text": "Next User ➡️", "callback_data": f"owner_checkallpreview_{next_user_id}_0_{hours}"})
                    
                    if user_nav:
                        keyboard.append(user_nav)
                    
                    keyboard.append([{"text": "🔙 Back to Menu", "callback_data": "owner_backtomenu"}])
                    
                    try:
                        get_session(bot_id).post(f"{config['telegram_api']}/editMessageText", json={
                            "chat_id": callback["message"]["chat"]["id"],
                            "message_id": callback["message"]["message_id"],
                            "text": body,
                            "reply_markup": {"inline_keyboard": keyboard}
                        }, timeout=2)
                    except Exception:
                        pass
                    
                elif data == "owner_checkallpreview_noop":
                    try:
                        get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery", json={
                            "callback_query_id": callback.get("id")
                        }, timeout=2)
                    except Exception:
                        pass
                    
                return jsonify({"ok": True})
            
            elif data in ["owner_adduser", "owner_suspend", "owner_unsuspend", "owner_checkallpreview"]:
                operation = data.replace("owner_", "")
                
                if operation == "checkallpreview":
                    set_owner_state(bot_id, uid, {"operation": operation, "step": 0})
                    cancel_keyboard = {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "owner_cancelinput"}]]}
                    
                    try:
                        send_message(bot_id, uid, "⏰ How many hours back should I check? (e.g., 1, 6, 24, 168):", cancel_keyboard)
                    except Exception:
                        pass
                    try:
                        get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery", json={
                            "callback_query_id": callback.get("id"),
                            "text": "ℹ️ Please check your new message."
                        }, timeout=2)
                    except Exception:
                        pass
                else:
                    set_owner_state(bot_id, uid, {"operation": operation, "step": 0})
                    
                    prompts = {
                        "adduser": "👤 Please send the User ID to add (you can add multiple IDs separated by spaces or commas):",
                        "suspend": "⏸️ Please send:\n1. User ID\n2. Duration (e.g., 30s, 10m, 2h, 1d, 1d2h, 2h30m, 1d2h3m5s)\n3. Optional reason\n\nExamples:\n• 123456789 30s Too many requests\n• 123456789 1d2h Spamming\n• 123456789 2h30m Violation",
                        "unsuspend": "▶️ Please send the User ID to unsuspend:",
                    }
                    
                    cancel_keyboard = {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "owner_cancelinput"}]]}
                    
                    try:
                        send_message(bot_id, uid, f"⚠️ {prompts[operation]}\n\nPlease send the requested information as a text message.", cancel_keyboard)
                    except Exception:
                        pass
                    try:
                        get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery", json={
                            "callback_query_id": callback.get("id"),
                            "text": "ℹ️ Please check your new message."
                        }, timeout=2)
                    except Exception:
                        pass
                return jsonify({"ok": True})
            
            elif data == "owner_cancelinput":
                clear_owner_state(bot_id, uid)
                try:
                    get_session(bot_id).post(f"{config['telegram_api']}/deleteMessage", json={
                        "chat_id": callback["message"]["chat"]["id"],
                        "message_id": callback["message"]["message_id"]
                    }, timeout=2)
                except Exception:
                    pass
                try:
                    get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery", json={
                        "callback_query_id": callback.get("id"),
                        "text": "❌ Operation cancelled."
                    }, timeout=2)
                except Exception:
                    pass
                return jsonify({"ok": True})
            
            # Answer callback query
            try:
                get_session(bot_id).post(f"{config['telegram_api']}/answerCallbackQuery", json={
                    "callback_query_id": callback.get("id")
                }, timeout=2)
            except Exception:
                pass
            
            return jsonify({"ok": True})
        
        # Handle regular messages
        if "message" in update:
            msg = update["message"]
            user = msg.get("from", {})
            uid = user.get("id")
            username = user.get("username") or (user.get("first_name") or "")
            text = msg.get("text") or ""

            # Update username only for existing/allowed users
            try:
                execute_sql(bot_id,
                    "UPDATE allowed_users SET username = %s WHERE user_id = %s",
                    (username or "", uid)
                )
            except Exception:
                logger.exception("webhook: update allowed_users username failed for %s", bot_id)

            # Check if owner is in input mode
            if uid in config["owner_ids"] and is_owner_in_operation(bot_id, uid):
                owner_state = get_owner_state(bot_id, uid)
                if owner_state:
                    operation = owner_state.get("operation")
                    step = owner_state.get("step", 0)
                    
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
                            
                            result = execute_sql(bot_id,
                                "SELECT 1 FROM allowed_users WHERE user_id = %s",
                                (tid,),
                                fetchone=True
                            )
                            if result:
                                already.append(tid)
                                continue
                            
                            execute_sql(bot_id,
                                "INSERT INTO allowed_users (user_id, username, added_at) VALUES (%s, %s, CURRENT_TIMESTAMP)",
                                (tid, "")
                            )
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
                        
                        clear_owner_state(bot_id, uid)
                        send_message(bot_id, uid, f"{result_msg}\n\nUse /ownersets again to access the menu. 😊")
                        return jsonify({"ok": True})
                    
                    elif operation == "suspend":
                        if step == 0:
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
                            suspend_user(bot_id, target, seconds, reason)
                            reason_part = f"\nReason: {reason}" if reason else ""
                            until_wat = utc_to_wat_ts((datetime.utcnow() + timedelta(seconds=seconds)).strftime('%Y-%m-%d %H:%M:%S'))
                            
                            clear_owner_state(bot_id, uid)
                            send_message(bot_id, uid, f"✅ User {label_for_owner_view(bot_id, target, fetch_display_username(bot_id, target))} suspended for {formatted_duration} (until {until_wat}).{reason_part}\n\nUse /ownersets again to access the menu. 😊")
                            return jsonify({"ok": True})
                    
                    elif operation == "unsuspend":
                        try:
                            target = int(text.strip())
                        except Exception:
                            send_message(bot_id, uid, "❌ Invalid User ID. Please try again.")
                            return jsonify({"ok": True})
                        
                        ok = unsuspend_user(bot_id, target)
                        if ok:
                            result = f"✅ User {label_for_owner_view(bot_id, target, fetch_display_username(bot_id, target))} unsuspended."
                        else:
                            result = f"ℹ️ User {target} is not suspended."
                        
                        clear_owner_state(bot_id, uid)
                        send_message(bot_id, uid, f"{result}\n\nUse /ownersets again to access the menu. 😊")
                        return jsonify({"ok": True})
                    
                    elif operation == "checkallpreview":
                        if step == 0:
                            try:
                                hours = int(text.strip())
                                if hours <= 0:
                                    raise ValueError
                            except Exception:
                                send_message(bot_id, uid, "❌ Please enter a valid positive number of hours.")
                                return jsonify({"ok": True})
                            
                            all_users = get_all_users_ordered(bot_id)
                            if not all_users:
                                clear_owner_state(bot_id, uid)
                                send_message(bot_id, uid, "📋 No users found.")
                                return jsonify({"ok": True})
                            
                            first_user_id, first_username, first_added_at = all_users[0]
                            username_display = at_username(first_username) if first_username else "no username"
                            added_wat = utc_to_wat_ts(first_added_at.strftime("%Y-%m-%d %H:%M:%S")) if first_added_at else "unknown"
                            
                            tasks, total_tasks, total_pages = get_user_tasks_preview(bot_id, first_user_id, hours, 0)
                            
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
                                task_nav.append({"text": "Next Page ➡️", "callback_data": f"owner_checkallpreview_{first_user_id}_1_{hours}"})
                            if task_nav:
                                keyboard.append(task_nav)
                            
                            user_nav = []
                            user_nav.append({"text": f"User 1/{len(all_users)}", "callback_data": "owner_checkallpreview_noop"})
                            
                            if len(all_users) > 1:
                                next_user_id = all_users[1][0]
                                user_nav.append({"text": "Next User ➡️", "callback_data": f"owner_checkallpreview_{next_user_id}_0_{hours}"})
                            
                            if user_nav:
                                keyboard.append(user_nav)
                            
                            keyboard.append([{"text": "🔙 Back to Menu", "callback_data": "owner_backtomenu"}])
                            
                            clear_owner_state(bot_id, uid)
                            send_message(bot_id, uid, body, {"inline_keyboard": keyboard})
                            return jsonify({"ok": True})
            
            # Handle commands
            if text.startswith("/"):
                parts = text.split(None, 1)
                cmd = parts[0].split("@")[0].lower()
                args = parts[1] if len(parts) > 1 else ""
                
                # Clear any existing owner state when new command comes
                clear_owner_state(bot_id, uid)
                
                if cmd == "/ownersets":
                    if uid not in config["owner_ids"]:
                        send_message(bot_id, uid, f"🚫 Owner only. {config['owner_tag']} notified.")
                        notify_owners(bot_id, f"🚨 Unallowed /ownersets attempt by {at_username(username) if username else uid} (ID: {uid}).")
                        return jsonify({"ok": True})
                    send_ownersets_menu(bot_id, uid)
                    return jsonify({"ok": True})
                else:
                    return handle_command(bot_id, uid, username, cmd, args)
            else:
                # Handle regular text input
                return handle_user_text(bot_id, uid, username, text)
    except Exception:
        logger.exception("webhook handling error for %s", bot_id)
    
    return jsonify({"ok": True})

# ===================== FLASK ROUTES =====================

@app.route("/", methods=["GET"])
def root():
    return "Multi-Bot WordSplitter running.", 200

# Separate health endpoints for each bot
@app.route("/health/a", methods=["GET", "HEAD"])
def health_a():
    db_ok = check_db_health()
    return jsonify({
        "ok": True, 
        "bot": "A", 
        "ts": now_ts(),
        "db_connected": db_ok,
        "workers": len(BOT_STATES["bot_a"]["user_workers"])
    }), 200

@app.route("/health/b", methods=["GET", "HEAD"])
def health_b():
    db_ok = check_db_health()
    return jsonify({
        "ok": True, 
        "bot": "B", 
        "ts": now_ts(),
        "db_connected": db_ok,
        "workers": len(BOT_STATES["bot_b"]["user_workers"])
    }), 200

@app.route("/health/c", methods=["GET", "HEAD"])
def health_c():
    db_ok = check_db_health()
    return jsonify({
        "ok": True, 
        "bot": "C", 
        "ts": now_ts(),
        "db_connected": db_ok,
        "workers": len(BOT_STATES["bot_c"]["user_workers"])
    }), 200

# Separate webhook endpoints for each bot
@app.route("/webhook/a", methods=["POST"])
def webhook_a():
    return handle_webhook("bot_a")

@app.route("/webhook/b", methods=["POST"])
def webhook_b():
    return handle_webhook("bot_b")

@app.route("/webhook/c", methods=["POST"])
def webhook_c():
    return handle_webhook("bot_c")

# ===================== WEBHOOK SETUP =====================

def set_webhook(bot_id: str):
    """Set webhook for specific bot"""
    config = BOTS_CONFIG[bot_id]
    if not config["telegram_api"] or not config["webhook_url"]:
        logger.info("Webhook not configured for %s", bot_id)
        return
    try:
        # Ensure the webhook URL is correct for this bot
        webhook_url = config["webhook_url"]
        if not webhook_url.endswith(f"/webhook/{bot_id.split('_')[-1].lower()}"):
            webhook_url = f"{webhook_url.rstrip('/')}/webhook/{bot_id.split('_')[-1].lower()}"
        
        get_session(bot_id).post(f"{config['telegram_api']}/setWebhook", 
                                json={"url": webhook_url}, 
                                timeout=SHARED_SETTINGS["requests_timeout"])
        logger.info("Webhook set for %s to %s", bot_id, webhook_url)
    except Exception:
        logger.exception("set_webhook failed for %s", bot_id)

# ===================== MAIN =====================

def main():
    # Set webhooks for all bots
    for bot_id in BOTS_CONFIG:
        set_webhook(bot_id)
    
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()
