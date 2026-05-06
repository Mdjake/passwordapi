"""
🤖 Telegram Bot Bridge API v2.4.0 - WITH POLLING, EDIT DETECTION & RESPONSE PARSER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Features:
  - Sends query, then POLLS for bot response
  - Does NOT rely on reply_to_msg_id (most bots don't reply, they just send)
  - Filters by message timestamp (only messages AFTER query was sent)
  - Waits for bot to EDIT message (final version)
  - Ignores "loading/processing/searching" temporary messages
  - Extracts phone, display_phone, country, country_code, input from bot responses
  - Handles split "Code: +91 / Number: 7355348898" formats
  - Handles next-line values ("Number:\\n7355348898")
  - Returns clean structured JSON with all extracted fields
  - Multi-user session support
  - Session priority: Environment Variable → SQLite Cloud
  - Rate limiting per user
  - Response caching
"""

import os
import re
import asyncio
import logging
import time
import unicodedata
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
API_ID             = int(os.getenv("TG_API_ID", "123456"))
API_HASH           = os.getenv("TG_API_HASH", "your_hash")
SQL_CONN           = os.getenv("SQLITE_CLOUD_SESSIONS", "")
TARGET_BOT         = os.getenv("TARGET_BOT", "")
API_KEY            = os.getenv("API_KEY", "")
RATE_LIMIT         = int(os.getenv("RATE_LIMIT_PER_MIN", "30"))
RESPONSE_TIMEOUT   = int(os.getenv("RESPONSE_TIMEOUT_SECONDS", "60"))
CACHE_TTL          = int(os.getenv("CACHE_TTL_SECONDS", "300"))
POLL_INTERVAL      = float(os.getenv("POLL_INTERVAL_SECONDS", "1.5"))
EDIT_WAIT          = float(os.getenv("EDIT_WAIT_SECONDS", "2.0"))

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
    description="Bridge between your API and Telegram Bot with polling, edit detection & response parsing",
    version="2.4.0"
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
        self.limit  = limit
        self.window = window

    def is_allowed(self, identifier: str) -> tuple[bool, int]:
        now    = time.time()
        cutoff = now - self.window
        self._requests[identifier] = [
            t for t in self._requests[identifier] if t > cutoff
        ]
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
        self._store: dict[str, tuple[dict, float]] = {}
        self.ttl    = ttl
        self.hits   = 0
        self.misses = 0

    def _make_key(self, query: str, user_id: str) -> str:
        return f"{user_id}:{query.lower().strip()}"

    def get(self, query: str, user_id: str) -> Optional[dict]:
        key = self._make_key(query, user_id)
        if key in self._store:
            response, expires_at = self._store[key]
            if time.time() < expires_at:
                self.hits += 1
                return response
            del self._store[key]
        self.misses += 1
        return None

    def set(self, query: str, user_id: str, response: dict):
        key = self._make_key(query, user_id)
        self._store[key] = (response, time.time() + self.ttl)

    def stats(self) -> dict:
        total = self.hits + self.misses
        return {
            "entries":  len(self._store),
            "hits":     self.hits,
            "misses":   self.misses,
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
                        id             INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id        TEXT UNIQUE NOT NULL,
                        session_string TEXT NOT NULL,
                        phone_number   TEXT,
                        created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_used      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                           last_used      = CURRENT_TIMESTAMP""",
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
# Bot Response Parser  v2 — robust multi-format
# ─────────────────────────────────────────────
def _strip_decoration(text: str) -> str:
    """
    Remove all leading emoji, Telegram box-drawing chars, symbols,
    dashes, separators, and whitespace from a line.
    """
    return re.sub(
        r'^[\s'
        r'\U00010000-\U0010FFFF'   # supplementary planes (most emoji)
        r'\u2600-\u27BF'           # misc symbols, dingbats
        r'\u2500-\u257F'           # box-drawing (━ ─ │ etc.)
        r'\u25A0-\u25FF'           # geometric shapes
        r'\u2100-\u214F'           # letterlike symbols
        r'\uFE00-\uFE0F'           # variation selectors
        r'\-=*#•|>_~`]+'
        ,
        '',
        text,
        flags=re.UNICODE,
    ).strip()


def parse_bot_response(text: str) -> dict:
    """
    Parse a Telegram bot response into clean structured fields.

    Handles all these formats:
      📞 Phone: +91 7355348898
      🌍 Country: India
      Code: +91
      Number:
      7355348898              ← value on the very next line
      Input:
      6258915779
      📞Phone:+919709211448   ← no spaces

    Returns:
      {
        "phone":         "+917355348898",    # E.164-ish, no spaces
        "display_phone": "+91 7355348898",   # as-is from bot
        "country":       "India",
        "country_code":  "+91",
        "input":         "6258915779",
        "extra":         { ...any other key:value pairs... }
      }
    All fields default to None / {} when not found.
    """
    result: dict = {
        "phone":         None,
        "display_phone": None,
        "country":       None,
        "country_code":  None,
        "input":         None,
        "extra":         {},
    }

    lines = [l.rstrip() for l in text.splitlines()]
    i = 0

    while i < len(lines):
        clean = _strip_decoration(lines[i])

        # Skip blank lines and section headers without a colon
        if not clean or ':' not in clean:
            i += 1
            continue

        key_raw, _, val_raw = clean.partition(':')
        key = key_raw.strip().lower()
        val = val_raw.strip()

        # Value might be on the NEXT line ("Number:\n7355348898")
        if not val and i + 1 < len(lines):
            next_clean = _strip_decoration(lines[i + 1])
            if next_clean and ':' not in next_clean:
                val = next_clean
                i += 1   # consume the value line

        if not val:
            i += 1
            continue

        # ── Map to known fields ──────────────────────────────────────────
        if key in ('phone', 'mobile', 'number', 'no', 'ph'):
            result['display_phone'] = val
            # Normalise: keep leading +, strip spaces / dashes / dots / parens
            result['phone'] = re.sub(r'[\s\-().]', '', val)

        elif key in ('country', 'location', 'region'):
            result['country'] = val

        elif key in ('code', 'country code', 'dial code', 'isd'):
            result['country_code'] = val

        elif key in ('input', 'query', 'searched', 'search'):
            result['input'] = val

        else:
            # Preserve any other key:value the bot may send
            result['extra'][key] = val

        i += 1

    # ── Assemble phone from split Code + Number fields ───────────────────
    # Some bots send:  Code: +91   (separate line)   Number: 7355348898
    if not result['phone'] and result['country_code']:
        number = (
            result['extra'].pop('number', None)
            or result['extra'].pop('no', None)
        )
        if number:
            code    = result['country_code'].strip()
            num     = re.sub(r'[\s\-]', '', number)
            result['phone']         = code + num
            result['display_phone'] = f"{code} {number}"

    # Drop empty extra dict to keep response clean
    if not result['extra']:
        del result['extra']

    return result


# ─────────────────────────────────────────────
# Telegram Client Manager with POLLING
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
        self,
        user_id: str,
        query: str,
        timeout: int = RESPONSE_TIMEOUT,
    ) -> dict:
        """
        Send query to bot and ONLY THEN start POLLING for response.

        KEY DESIGN:
          - Does NOT filter by reply_to_msg_id (most bots send a new message)
          - Filters by:  msg.out == False  AND  msg.date >= send_time
          - Waits EDIT_WAIT seconds after finding a candidate, then re-fetches
            to capture the bot's final edited version.
          - Extracted fields are returned as clean top-level keys.
        """
        client = await self.get_client(user_id)
        if not client:
            raise HTTPException(
                status_code=503,
                detail=f"No active session for user: {user_id}",
            )

        # Record exact send time (timezone-aware UTC)
        send_time = datetime.now(timezone.utc)
        start_ts  = time.time()

        # STEP 1 — Send the query
        logger.info(f"📤 Sending query to {TARGET_BOT}: {query[:80]}")
        sent_msg = await client.send_message(TARGET_BOT, query)
        logger.info(f"📨 Message sent, ID: {sent_msg.id}")

        # STEP 2 — Poll for the bot's response
        logger.info("🔍 Polling for bot response...")

        seen_messages: dict[int, str]  = {}
        candidate_msg_id: Optional[int] = None
        final_response: Optional[str]   = None

        LOADING_KEYWORDS = [
            "loading", "processing", "typing", "please wait",
            "⏳", "🔄", "generating", "thinking",
            "searching", "searching for data", "fetching",
            "looking up", "retrieving", "please hold", "working on it",
        ]

        def is_loading(text: str) -> bool:
            tl = text.lower()
            if text.strip() in ("...", "..", ".", "…."):
                return True
            return any(kw in tl for kw in LOADING_KEYWORDS)

        def looks_final(text: str) -> bool:
            if len(text.strip()) < 10:
                return False
            if is_loading(text):
                return False
            return True

        while time.time() - start_ts < timeout:
            await asyncio.sleep(POLL_INTERVAL)

            try:
                async for msg in client.iter_messages(TARGET_BOT, limit=15):
                    # Skip our own outgoing messages
                    if msg.out:
                        continue

                    # Skip messages that existed before we sent the query
                    msg_date = msg.date
                    if msg_date.tzinfo is None:
                        msg_date = msg_date.replace(tzinfo=timezone.utc)
                    if msg_date < send_time:
                        continue

                    msg_id   = msg.id
                    msg_text = (msg.text or msg.message or "").strip()

                    if not msg_text:
                        continue

                    if msg_id not in seen_messages:
                        seen_messages[msg_id] = msg_text
                        logger.info(
                            f"📥 New bot message (ID:{msg_id}): {msg_text[:120]}"
                        )

                        if is_loading(msg_text):
                            # Placeholder — mark as candidate and wait for edit
                            logger.info("⏳ Loading message — waiting for edit...")
                            candidate_msg_id = msg_id

                        elif looks_final(msg_text):
                            # ✅ Clean first message — return immediately, no edit wait needed
                            logger.info(f"✅ Instant final response (ID:{msg_id}): {msg_text[:120]}")
                            parsed = parse_bot_response(msg_text)
                            return {
                                "success":       True,
                                "query":         query,
                                **parsed,
                                "Developer":  "i_AmAnanya",
                                "response_time": round(time.time() - start_ts, 2),
                                "method":        "polling",
                                "cached":        False
                            }

                    else:
                        prev_text = seen_messages[msg_id]
                        if prev_text != msg_text:
                            seen_messages[msg_id] = msg_text
                            logger.info(
                                f"✏️ Bot EDITED message (ID:{msg_id}): {msg_text[:120]}"
                            )
                            if looks_final(msg_text):
                                candidate_msg_id = msg_id

                # Candidate exists = was a loading msg that got edited into final form.
                # Wait EDIT_WAIT once more to ensure no further edits are coming.
                if candidate_msg_id and looks_final(
                    seen_messages.get(candidate_msg_id, "")
                ):
                    logger.info(
                        f"⏸️ Edited candidate (ID:{candidate_msg_id}), "
                        f"waiting {EDIT_WAIT}s to confirm no further edits..."
                    )
                    await asyncio.sleep(EDIT_WAIT)

                    # Re-fetch the latest version of the message
                    async for msg in client.iter_messages(TARGET_BOT, limit=20):
                        if msg.id == candidate_msg_id:
                            fetched = (msg.text or msg.message or "").strip()
                            if fetched:
                                final_response = fetched
                            break

                    if not final_response:
                        final_response = seen_messages.get(candidate_msg_id)

                    if final_response:
                        logger.info(f"✅ Final edited response: {final_response[:120]}")
                        parsed = parse_bot_response(final_response)
                        return {
                            "success":       True,
                            "query":         query,
                            **parsed,
                            "Deveoper":  "@i_AmAnanya",
                            "response_time": round(time.time() - start_ts, 2),
                            "method":        "polling_edited",
                            "cached":        False,
                        }

            except Exception as e:
                logger.error(f"Error during polling: {e}")
                continue

        # ── Timeout reached — return best captured content ────────────────
        last_seen = None
        if seen_messages:
            for mid, txt in reversed(list(seen_messages.items())):
                if looks_final(txt):
                    last_seen = txt
                    break
            if not last_seen:
                last_seen = list(seen_messages.values())[-1]

        if last_seen:
            logger.warning("⚠️ Timeout — returning last captured content")
            parsed = parse_bot_response(last_seen)
            return {
                "success":       True,
                "query":         query,
                **parsed,
                "raw_response":  last_seen,
                "response_time": round(time.time() - start_ts, 2),
                "method":        "polling_timeout",
                "cached":        False,
                "warning":       "Timeout reached; response may be incomplete",
            }

        raise HTTPException(
            status_code=408,
            detail="Bot did not respond within timeout",
        )

    async def cleanup(self):
        for uid, client in self.clients.items():
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
    logger.info("🚀 Starting Telegram Bridge API v2.4.0")
    logger.info(f"🎯 Target Bot:       {TARGET_BOT}")
    logger.info(f"⏱️  Response Timeout: {RESPONSE_TIMEOUT}s")
    logger.info(f"🔄 Poll Interval:    {POLL_INTERVAL}s")
    logger.info(f"✏️  Edit Wait:        {EDIT_WAIT}s")
    logger.info("✅ Timestamp-based filtering (no reply_to_msg_id)")
    logger.info("✅ Extracts phone, display_phone, country, country_code, input")
    logger.info("✅ Handles split Code+Number and next-line value formats")


@app.on_event("shutdown")
async def shutdown():
    logger.info("🛑 Shutting down, cleaning up clients...")
    await tg_manager.cleanup()


@app.post("/query")
async def send_query_post(
    query:     str,
    user_id:   str           = "default",
    use_cache: bool          = True,
    timeout:   int           = RESPONSE_TIMEOUT,
    api_key:   Optional[str] = Query(None),
):
    """
    Send a query to the Telegram bot and wait for the final response.

    Returns clean structured JSON:
      - phone         → E.164-normalised number ("+917355348898")
      - display_phone → as received from bot ("+91 7355348898")
      - country       → country name
      - country_code  → dial code ("+91")
      - input         → the input number the bot echoed back
      - raw_response  → full original bot message (for debugging)
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
            return {**cached, "cached": True, "rate_remaining": remaining}

    result = await tg_manager.send_query_with_polling(user_id, query, timeout)
    result["rate_remaining"] = remaining

    # Cache the full result
    cache.set(query, user_id, result)

    return result


@app.get("/query")
async def send_query_get(
    query:     str           = Query(..., description="Phone number or query to look up"),
    user_id:   str           = "default",
    use_cache: bool          = True,
    timeout:   int           = RESPONSE_TIMEOUT,
    api_key:   Optional[str] = Query(None),
):
    """GET version — easy for browser testing."""
    return await send_query_post(query, user_id, use_cache, timeout, api_key)


@app.post("/session/register")
async def register_session(
    user_id:        str,
    session_string: str,
    phone_number:   Optional[str] = None,
    api_key:        Optional[str] = Query(None),
):
    """Register a Telethon session string for a user."""
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
    """Delete a user's session."""
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    session_mgr.delete_session(user_id)
    if user_id in tg_manager.clients:
        await tg_manager.clients[user_id].disconnect()
        del tg_manager.clients[user_id]

    return {"success": True, "message": f"Session deleted for {user_id}"}


@app.get("/health")
async def health_check():
    """Public health check — no API key required."""
    clients_status = {
        uid: client.is_connected()
        for uid, client in tg_manager.clients.items()
    }
    return {
        "status":           "online",
        "version":          "2.4.0",
        "mode":             "polling (timestamp-filtered, edit-aware, structured-parser)",
        "timestamp":        datetime.utcnow().isoformat() + "Z",
        "clients_connected": len(tg_manager.clients),
        "clients_detail":   clients_status,
        "cache_stats":      cache.stats(),
        "target_bot":       TARGET_BOT,
        "poll_interval":    POLL_INTERVAL,
        "edit_wait":        EDIT_WAIT,
        "timeout":          RESPONSE_TIMEOUT,
    }


@app.get("/metrics")
async def get_metrics(api_key: Optional[str] = Query(None)):
    """Detailed runtime metrics."""
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return {
        "timestamp":      datetime.utcnow().isoformat() + "Z",
        "active_clients": len(tg_manager.clients),
        "cache":          cache.stats(),
        "rate_limiter": {
            "limit_per_minute": RATE_LIMIT,
            "active_users":     len(rate_limiter._requests),
        },
        "config": {
            "poll_interval": POLL_INTERVAL,
            "edit_wait":     EDIT_WAIT,
            "timeout":       RESPONSE_TIMEOUT,
        },
    }


@app.get("/")
async def root():
    return {
        "api":     "🤖 Telegram Bot Bridge API v2.4.0",
        "example_response": {
            "success":        True,
            "query":          "6258915779",
            "phone":          "+917355348898",
            "display_phone":  "+91 7355348898",
            "country":        "India",
            "country_code":   "+91",
            "input":          "6258915779",
            "raw_response":   "🔎 PHONE INFO\n\nInput:\n6258915779\n\nCountry: India\n\nCode: +91\n\nNumber:\n7355348898",
            "response_time":  3.42,
            "method":         "polling",
            "cached":         False,
            "rate_remaining": 29,
        },
        "endpoints": {
            "query_get":        "GET  /query?query=6258915779&user_id=default&api_key=YOUR_KEY",
            "query_post":       "POST /query?query=6258915779&user_id=default&api_key=YOUR_KEY",
            "register_session": "POST /session/register?user_id=me&session_string=...&api_key=YOUR_KEY",
            "delete_session":   "DELETE /session/{user_id}?api_key=YOUR_KEY",
            "health":           "GET  /health",
            "metrics":          "GET  /metrics?api_key=YOUR_KEY",
        },
        "docs": "/docs",
    }
