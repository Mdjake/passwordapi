"""
🤖 Telegram Bot Bridge API v2.2.0 - WITH POLLING & EDIT DETECTION (FIXED)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Features:
  - Sends query, then POLLS for bot response
  - Does NOT rely on reply_to_msg_id (most bots don't reply, they just send)
  - Filters by message timestamp (only messages AFTER query was sent)
  - Waits for bot to EDIT message (final version)
  - Ignores "loading/processing" temporary messages
  - Returns ONLY the final edited response
  - Multi-user session support
  - Session priority: Environment Variable → SQLite Cloud
  - Rate limiting per user
  - Response caching
"""

import os
import asyncio
import logging
import time
import uuid
from typing import Optional, Dict
from datetime import datetime, timezone
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
import sqlitecloud

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
API_ID = int(os.getenv("TG_API_ID", "123456"))
API_HASH = os.getenv("TG_API_HASH", "your_hash")
SQL_CONN = os.getenv("SQLITE_CLOUD_SESSIONS", "")
TARGET_BOT = os.getenv("TARGET_BOT", "")
API_KEY = os.getenv("API_KEY", "")
RATE_LIMIT = int(os.getenv("RATE_LIMIT_PER_MIN", "30"))
RESPONSE_TIMEOUT = int(os.getenv("RESPONSE_TIMEOUT_SECONDS", "60"))
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL_SECONDS", "1.5"))
# Extra seconds to wait after finding a response, in case bot edits it
EDIT_WAIT = float(os.getenv("EDIT_WAIT_SECONDS", "2.0"))

# Validate required configs
if not SQL_CONN:
    raise ValueError("SQLITE_CLOUD_SESSIONS environment variable is required")
if not TARGET_BOT:
    raise ValueError("TARGET_BOT environment variable is required")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("BridgeAPI")

app = FastAPI(
    title="🤖 Telegram Bot Bridge API",
    description="Bridge between your API and Telegram Bot with polling & edit detection",
    version="2.2.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# Rate Limiter
# ─────────────────────────────────────────────
class RateLimiter:
    def __init__(self, limit: int = 30, window: int = 60):
        self._requests: dict[str, list[float]] = defaultdict(list)
        self.limit = limit
        self.window = window

    def is_allowed(self, identifier: str) -> tuple[bool, int]:
        now = time.time()
        cutoff = now - self.window
        self._requests[identifier] = [t for t in self._requests[identifier] if t > cutoff]
        remaining = self.limit - len(self._requests[identifier])
        if remaining <= 0:
            return False, 0
        self._requests[identifier].append(now)
        return True, remaining - 1

rate_limiter = RateLimiter(limit=RATE_LIMIT)

# ─────────────────────────────────────────────
# Response Cache
# ─────────────────────────────────────────────
class ResponseCache:
    def __init__(self, ttl: int = 300):
        self._store: dict[str, tuple[str, float]] = {}
        self.ttl = ttl
        self.hits = 0
        self.misses = 0

    def _make_key(self, query: str, user_id: str) -> str:
        return f"{user_id}:{query.lower().strip()}"

    def get(self, query: str, user_id: str) -> Optional[str]:
        key = self._make_key(query, user_id)
        if key in self._store:
            response, expires_at = self._store[key]
            if time.time() < expires_at:
                self.hits += 1
                return response
            del self._store[key]
        self.misses += 1
        return None

    def set(self, query: str, user_id: str, response: str):
        key = self._make_key(query, user_id)
        self._store[key] = (response, time.time() + self.ttl)

    def stats(self) -> dict:
        total = self.hits + self.misses
        return {
            "entries": len(self._store),
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": f"{(self.hits / total * 100):.1f}%" if total > 0 else "0%",
        }

cache = ResponseCache(ttl=CACHE_TTL)

# ─────────────────────────────────────────────
# Session Manager (SQLite Cloud)
# ─────────────────────────────────────────────
class SessionManager:
    def __init__(self, conn_str: str):
        self.conn_str = conn_str
        self._init_db()

    def _init_db(self):
        try:
            with sqlitecloud.connect(self.conn_str) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS sessions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT UNIQUE NOT NULL,
                        session_string TEXT NOT NULL,
                        phone_number TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("Session database initialized")
        except Exception as e:
            logger.error(f"Failed to init session DB: {e}")
            raise

    def get_session(self, user_id: str = "default") -> Optional[str]:
        try:
            with sqlitecloud.connect(self.conn_str) as conn:
                res = conn.execute(
                    "SELECT session_string FROM sessions WHERE user_id = ?",
                    (user_id,)
                ).fetchone()
                if res:
                    conn.execute(
                        "UPDATE sessions SET last_used = CURRENT_TIMESTAMP WHERE user_id = ?",
                        (user_id,)
                    )
                    return res[0]
                return None
        except Exception as e:
            logger.error(f"Failed to get session for {user_id}: {e}")
            return None

    def save_session(self, user_id: str, session_string: str, phone_number: str = None):
        try:
            with sqlitecloud.connect(self.conn_str) as conn:
                conn.execute(
                    """INSERT INTO sessions (user_id, session_string, phone_number)
                       VALUES (?, ?, ?)
                       ON CONFLICT(user_id) DO UPDATE SET
                       session_string = excluded.session_string,
                       last_used = CURRENT_TIMESTAMP""",
                    (user_id, session_string, phone_number)
                )
                logger.info(f"Session saved for user: {user_id}")
        except Exception as e:
            logger.error(f"Failed to save session for {user_id}: {e}")
            raise

    def delete_session(self, user_id: str):
        try:
            with sqlitecloud.connect(self.conn_str) as conn:
                conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
                logger.info(f"Session deleted for user: {user_id}")
        except Exception as e:
            logger.error(f"Failed to delete session for {user_id}: {e}")

session_mgr = SessionManager(SQL_CONN)

# ─────────────────────────────────────────────
# Telegram Client Manager with FIXED POLLING
# ─────────────────────────────────────────────
class TelegramClientManager:
    def __init__(self):
        self.clients: Dict[str, TelegramClient] = {}
        self._lock = asyncio.Lock()

    async def get_client(self, user_id: str = "default") -> Optional[TelegramClient]:
        async with self._lock:
            if user_id in self.clients:
                client = self.clients[user_id]
                if client.is_connected():
                    return client
                else:
                    await self._reconnect_client(user_id)
                    return self.clients.get(user_id)

            # Session priority: Env variable FIRST, then SQLite Cloud
            session_str = os.getenv(f"SESSION_STRING_{user_id.upper()}")

            if not session_str:
                session_str = session_mgr.get_session(user_id)

            if not session_str:
                logger.warning(f"No session found for user: {user_id}")
                return None

            return await self._create_client(user_id, session_str)

    async def _create_client(self, user_id: str, session_str: str) -> Optional[TelegramClient]:
        try:
            client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
            await client.connect()

            if not await client.is_user_authorized():
                logger.error(f"Session invalid for user: {user_id}")
                return None

            self.clients[user_id] = client
            logger.info(f"Client created for user: {user_id}")
            return client
        except Exception as e:
            logger.error(f"Failed to create client for {user_id}: {e}")
            return None

    async def _reconnect_client(self, user_id: str):
        if user_id in self.clients:
            try:
                await self.clients[user_id].disconnect()
            except Exception:
                pass
            del self.clients[user_id]
        await self.get_client(user_id)

    async def send_query_with_polling(
        self, user_id: str, query: str, timeout: int = RESPONSE_TIMEOUT
    ) -> dict:
        """
        Send query to bot and ONLY THEN start POLLING for response.

        KEY FIX: Does NOT filter by reply_to_msg_id — most bots send a new
        message rather than replying. Instead we filter by:
          - msg.out == False  (not our own message)
          - msg.date >= send_time  (arrived after we sent the query)

        Also waits EDIT_WAIT seconds after finding a candidate response
        then re-fetches to capture the bot's final edited version.
        """
        client = await self.get_client(user_id)
        if not client:
            raise HTTPException(
                status_code=503,
                detail=f"No active session for user: {user_id}"
            )

        # Record exact send time (timezone-aware UTC)
        send_time = datetime.now(timezone.utc)
        start_ts = time.time()

        # STEP 1: Send the query
        logger.info(f"📤 Sending query to {TARGET_BOT}: {query[:80]}")
        sent_msg = await client.send_message(TARGET_BOT, query)
        logger.info(f"📨 Message sent, ID: {sent_msg.id}")

        # STEP 2: Poll for the bot's response
        logger.info("🔍 Polling for bot response...")

        # msg_id -> last known text
        seen_messages: dict[int, str] = {}
        candidate_msg_id: Optional[int] = None
        final_response: Optional[str] = None

        LOADING_KEYWORDS = [
            "loading", "processing", "typing", "please wait",
            "⏳", "🔄", "generating", "thinking",
            "searching", "searching for data", "fetching", "looking up",
            "retrieving", "please hold", "working on it"
        ]

        def is_loading(text: str) -> bool:
            tl = text.lower()
            # Also treat very short messages with trailing dots as loading
            if text.strip() in ("...", "..", ".", "…."):
                return True
            return any(kw in tl for kw in LOADING_KEYWORDS)

        def looks_final(text: str) -> bool:
            """Heuristic: a real answer is usually non-trivial."""
            if len(text.strip()) < 10:
                return False
            if is_loading(text):
                return False
            return True

        while time.time() - start_ts < timeout:
            await asyncio.sleep(POLL_INTERVAL)

            try:
                async for msg in client.iter_messages(TARGET_BOT, limit=15):
                    # Skip messages we sent ourselves
                    if msg.out:
                        continue

                    # Skip messages that existed before we sent our query
                    msg_date = msg.date
                    if msg_date.tzinfo is None:
                        msg_date = msg_date.replace(tzinfo=timezone.utc)
                    if msg_date < send_time:
                        continue

                    msg_id = msg.id
                    # Prefer .text; fall back to .message (same field, alias)
                    msg_text = (msg.text or msg.message or "").strip()

                    if not msg_text:
                        continue  # Ignore media-only or empty messages

                    if msg_id not in seen_messages:
                        # Brand-new message from bot
                        seen_messages[msg_id] = msg_text
                        logger.info(
                            f"📥 New bot message (ID:{msg_id}): {msg_text[:120]}"
                        )

                        if is_loading(msg_text):
                            logger.info("⏳ Looks like a loading message, waiting...")
                            candidate_msg_id = msg_id  # track it for edits
                        elif looks_final(msg_text):
                            # Treat as candidate; still wait for possible edits
                            candidate_msg_id = msg_id
                            seen_messages[msg_id] = msg_text

                    else:
                        prev_text = seen_messages[msg_id]
                        if prev_text != msg_text:
                            # Bot edited this message
                            seen_messages[msg_id] = msg_text
                            logger.info(
                                f"✏️ Bot EDITED message (ID:{msg_id}): {msg_text[:120]}"
                            )
                            if looks_final(msg_text):
                                candidate_msg_id = msg_id

                # If we have a solid candidate, wait briefly then re-fetch
                if candidate_msg_id and looks_final(seen_messages.get(candidate_msg_id, "")):
                    logger.info(
                        f"⏸️ Candidate found (ID:{candidate_msg_id}), "
                        f"waiting {EDIT_WAIT}s for final edit..."
                    )
                    await asyncio.sleep(EDIT_WAIT)

                    # Re-fetch that specific message for its latest version
                    async for msg in client.iter_messages(TARGET_BOT, limit=20):
                        if msg.id == candidate_msg_id:
                            final_text = (msg.text or msg.message or "").strip()
                            if final_text:
                                final_response = final_text
                            break

                    if not final_response:
                        # Fallback to what we last saw in memory
                        final_response = seen_messages.get(candidate_msg_id)

                    if final_response:
                        logger.info(f"✅ Final response: {final_response[:120]}")
                        return {
                            "success": True,
                            "query": query,
                            "response": final_response,
                            "response_time": round(time.time() - start_ts, 2),
                            "method": "polling",
                        }

            except Exception as e:
                logger.error(f"Error during polling: {e}")
                continue

        # ── Timeout reached ──────────────────────────────────────────────
        # Return whatever we last saw, even if it's a loading stub
        last_seen = None
        if seen_messages:
            # Pick the most-recent non-loading message if possible
            for mid, txt in reversed(list(seen_messages.items())):
                if looks_final(txt):
                    last_seen = txt
                    break
            if not last_seen:
                last_seen = list(seen_messages.values())[-1]

        if last_seen:
            logger.warning("⚠️ Timeout — returning last captured content")
            return {
                "success": True,
                "query": query,
                "response": last_seen,
                "response_time": round(time.time() - start_ts, 2),
                "method": "polling_timeout",
                "warning": "Timeout reached; response may be incomplete",
            }

        raise HTTPException(
            status_code=408,
            detail="Bot did not respond within timeout"
        )

    async def cleanup(self):
        for user_id, client in self.clients.items():
            try:
                await client.disconnect()
            except Exception:
                pass
        self.clients.clear()


tg_manager = TelegramClientManager()

# ─────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    logger.info("🚀 Starting Telegram Bridge API v2.2.0 (FIXED POLLING)")
    logger.info(f"🎯 Target Bot: {TARGET_BOT}")
    logger.info(f"⏱️ Response Timeout: {RESPONSE_TIMEOUT}s")
    logger.info(f"🔄 Poll Interval: {POLL_INTERVAL}s")
    logger.info(f"✏️ Edit Wait: {EDIT_WAIT}s")
    logger.info("📋 Polling starts ONLY after query is sent")
    logger.info("✅ FIX: No reply_to_msg_id filter — works with any bot response style")


@app.on_event("shutdown")
async def shutdown():
    logger.info("🛑 Shutting down, cleaning up clients...")
    await tg_manager.cleanup()


@app.post("/query")
async def send_query_post(
    query: str,
    user_id: str = "default",
    use_cache: bool = True,
    timeout: int = RESPONSE_TIMEOUT,
    api_key: Optional[str] = Query(None),
):
    """
    Send a query to the Telegram bot and wait for the final response.

    Polling starts ONLY after the query is sent. Filters by timestamp —
    not reply_to_msg_id — so it works with bots that don't use replies.
    Waits for any edits before returning.
    """
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    if timeout < 5 or timeout > 300:
        timeout = RESPONSE_TIMEOUT

    allowed, remaining = rate_limiter.is_allowed(user_id)
    if not allowed:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    if use_cache:
        cached = cache.get(query, user_id)
        if cached:
            return {
                "success": True,
                "cached": True,
                "query": query,
                "response": cached,
                "rate_remaining": remaining,
            }

    result = await tg_manager.send_query_with_polling(user_id, query, timeout)
    result["cached"] = False
    result["rate_remaining"] = remaining

    if result.get("response"):
        cache.set(query, user_id, result["response"])

    return result


@app.get("/query")
async def send_query_get(
    query: str = Query(..., description="Your question"),
    user_id: str = "default",
    use_cache: bool = True,
    timeout: int = RESPONSE_TIMEOUT,
    api_key: Optional[str] = Query(None),
):
    """GET version — easy for browser testing."""
    return await send_query_post(query, user_id, use_cache, timeout, api_key)


@app.post("/session/register")
async def register_session(
    user_id: str,
    session_string: str,
    phone_number: Optional[str] = None,
    api_key: Optional[str] = Query(None),
):
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    session_mgr.save_session(user_id, session_string, phone_number)

    client = await tg_manager.get_client(user_id)
    if not client:
        raise HTTPException(status_code=400, detail="Invalid session string")

    return {"success": True, "message": f"Session registered for {user_id}"}


@app.delete("/session/{user_id}")
async def delete_session(
    user_id: str,
    api_key: Optional[str] = Query(None),
):
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    session_mgr.delete_session(user_id)
    if user_id in tg_manager.clients:
        await tg_manager.clients[user_id].disconnect()
        del tg_manager.clients[user_id]

    return {"success": True, "message": f"Session deleted for {user_id}"}


@app.get("/health")
async def health_check():
    clients_status = {
        uid: client.is_connected()
        for uid, client in tg_manager.clients.items()
    }
    return {
        "status": "online",
        "version": "2.2.0",
        "mode": "polling (starts only after query, timestamp-filtered)",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "clients_connected": len(tg_manager.clients),
        "clients_detail": clients_status,
        "cache_stats": cache.stats(),
        "target_bot": TARGET_BOT,
        "poll_interval": POLL_INTERVAL,
        "edit_wait": EDIT_WAIT,
        "timeout": RESPONSE_TIMEOUT,
    }


@app.get("/metrics")
async def get_metrics(api_key: Optional[str] = Query(None)):
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "active_clients": len(tg_manager.clients),
        "cache": cache.stats(),
        "rate_limiter": {
            "limit_per_minute": RATE_LIMIT,
            "active_users": len(rate_limiter._requests),
        },
        "config": {
            "poll_interval": POLL_INTERVAL,
            "edit_wait": EDIT_WAIT,
            "timeout": RESPONSE_TIMEOUT,
        },
    }


@app.get("/")
async def root():
    return {
        "api": "🤖 Telegram Bot Bridge API v2.2.0",
        "fix": "Removed reply_to_msg_id filter — uses timestamp-based filtering instead",
        "how_it_works": {
            "step_1": "Record exact UTC timestamp",
            "step_2": "Send query to bot",
            "step_3": "Poll for messages from bot (not our own, arrived AFTER send time)",
            "step_4": "Ignore loading/placeholder messages",
            "step_5": f"Wait {EDIT_WAIT}s after finding candidate response",
            "step_6": "Re-fetch message to capture final edited version",
            "step_7": "Return final response",
        },
        "endpoints": {
            "send_query_get":   "GET  /query?query=hello&user_id=default&api_key=YOUR_KEY",
            "send_query_post":  "POST /query?query=hello&user_id=default&api_key=YOUR_KEY",
            "register_session": "POST /session/register?user_id=me&session_string=...&api_key=YOUR_KEY",
            "delete_session":   "DELETE /session/{user_id}?api_key=YOUR_KEY",
            "health":           "GET  /health",
            "metrics":          "GET  /metrics?api_key=YOUR_KEY",
        },
        "docs": "/docs",
    }
