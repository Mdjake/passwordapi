"""
🤖 Telegram Bot Bridge API v2.1.0 - WITH POLLING & EDIT DETECTION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Features:
  - Sends query, then POLLS for bot response
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
from datetime import datetime
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
    version="2.1.0"
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
# Telegram Client Manager with POLLING (ONLY AFTER QUERY)
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
            except:
                pass
            del self.clients[user_id]
        await self.get_client(user_id)

    async def send_query_with_polling(self, user_id: str, query: str, timeout: int = RESPONSE_TIMEOUT) -> dict:
        """
        Send query to bot and ONLY THEN start POLLING for response.
        Waits for bot to EDIT message to final version.
        Ignores "loading" temporary messages.
        """
        client = await self.get_client(user_id)
        if not client:
            raise HTTPException(status_code=503, detail=f"No active session for user: {user_id}")

        start_time = time.time()
        
        # STEP 1: Send the query to bot
        logger.info(f"📤 Sending query to {TARGET_BOT}: {query}")
        sent_msg = await client.send_message(TARGET_BOT, query)
        logger.info(f"📨 Message sent, ID: {sent_msg.id}")
        
        # STEP 2: NOW start polling for response (only after query is sent)
        logger.info(f"🔍 Starting to poll for bot response...")
        
        # Track seen messages to avoid duplicates
        seen_messages = {}
        last_edited_content = None
        final_response = None
        
        # Poll for response (only runs after query is sent)
        while time.time() - start_time < timeout:
            await asyncio.sleep(POLL_INTERVAL)
            
            # Get recent messages from bot (last 10 messages)
            try:
                async for msg in client.iter_messages(TARGET_BOT, limit=10):
                    # Only consider messages that are replies to OUR sent message
                    if msg.reply_to_msg_id != sent_msg.id:
                        continue
                    
                    msg_id = msg.id
                    msg_text = msg.text or ""
                    is_edited = msg.edit_date is not None
                    
                    # Skip loading/processing messages
                    if any(keyword in msg_text.lower() for keyword in ['loading', 'processing', 'typing', '...', 'please wait']):
                        logger.info(f"⏳ Ignoring loading message: {msg_text[:50]}")
                        continue
                    
                    # First time seeing this message
                    if msg_id not in seen_messages:
                        seen_messages[msg_id] = msg_text
                        
                        if is_edited or len(msg_text) > 50:
                            logger.info(f"✏️ Bot sent/edited message (ID: {msg_id}): {msg_text[:100]}")
                            last_edited_content = msg_text
                        else:
                            logger.info(f"📥 Bot responded (ID: {msg_id}): {msg_text[:100]}")
                            
                            # If bot replied without editing, this might be final
                            if not is_edited and len(msg_text) > 20:
                                final_response = msg_text
                                break
                    
                    # Message content changed (edited)
                    elif seen_messages[msg_id] != msg_text:
                        seen_messages[msg_id] = msg_text
                        logger.info(f"✏️ Bot UPDATED message (ID: {msg_id}): {msg_text[:100]}")
                        last_edited_content = msg_text
                        
                        # If this looks like final response
                        if len(msg_text) > 30 and any(c in msg_text for c in ['.', ':', '\n', '-']):
                            final_response = msg_text
                            break
                
                # If we have a final response, return it immediately
                if final_response:
                    logger.info(f"✅ Final response captured: {final_response[:100]}")
                    return {
                        "success": True,
                        "query": query,
                        "response": final_response,
                        "response_time": round(time.time() - start_time, 2),
                        "method": "polling"
                    }
                
            except Exception as e:
                logger.error(f"Error polling messages: {e}")
                continue
        
        # Timeout reached - return last edited content if exists
        if last_edited_content:
            logger.info(f"⚠️ Timeout but returning last edited content")
            return {
                "success": True,
                "query": query,
                "response": last_edited_content,
                "response_time": round(time.time() - start_time, 2),
                "method": "polling_timeout",
                "warning": "Response may be incomplete (timeout)"
            }
        
        # No response at all
        raise HTTPException(status_code=408, detail="Bot did not respond within timeout")

    async def cleanup(self):
        for user_id, client in self.clients.items():
            try:
                await client.disconnect()
            except:
                pass
        self.clients.clear()

tg_manager = TelegramClientManager()

# ─────────────────────────────────────────────
# API KEY VERIFICATION
# ─────────────────────────────────────────────
async def verify_api_key(api_key: Optional[str] = Query(None)):
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key

# ─────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    logger.info(f"🚀 Starting Telegram Bridge API v2.1.0 (POLLING MODE)")
    logger.info(f"🎯 Target Bot: {TARGET_BOT}")
    logger.info(f"⏱️ Response Timeout: {RESPONSE_TIMEOUT}s")
    logger.info(f"🔄 Poll Interval: {POLL_INTERVAL}s")
    logger.info(f"📋 Polling starts ONLY after query is sent")

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
    api_key: Optional[str] = Query(None)
):
    """
    Send a query to Telegram bot and wait for response.
    Bot can take time to respond - this waits for the final edited version.
    Polling ONLY starts AFTER the query is sent.
    """
    # Verify API key
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    # Validate timeout
    if timeout < 5 or timeout > 120:
        timeout = RESPONSE_TIMEOUT
    
    # Rate limiting
    allowed, remaining = rate_limiter.is_allowed(user_id)
    if not allowed:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    
    # Check cache
    if use_cache:
        cached = cache.get(query, user_id)
        if cached:
            return {
                "success": True,
                "cached": True,
                "query": query,
                "response": cached,
                "rate_remaining": remaining
            }
    
    # Send to bot with polling (polling starts ONLY after query sent)
    result = await tg_manager.send_query_with_polling(user_id, query, timeout)
    result["cached"] = False
    result["rate_remaining"] = remaining
    
    # Cache response
    if result.get("response"):
        cache.set(query, user_id, result["response"])
    
    return result

@app.get("/query")
async def send_query_get(
    query: str = Query(..., description="Your question"),
    user_id: str = "default",
    use_cache: bool = True,
    timeout: int = RESPONSE_TIMEOUT,
    api_key: Optional[str] = Query(None)
):
    """GET version - easy for browser testing"""
    return await send_query_post(query, user_id, use_cache, timeout, api_key)

@app.post("/session/register")
async def register_session(
    user_id: str,
    session_string: str,
    phone_number: Optional[str] = None,
    api_key: Optional[str] = Query(None)
):
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    session_mgr.save_session(user_id, session_string, phone_number)
    
    # Test the session
    client = await tg_manager.get_client(user_id)
    if not client:
        raise HTTPException(status_code=400, detail="Invalid session string")
    
    return {"success": True, "message": f"Session registered for {user_id}"}

@app.delete("/session/{user_id}")
async def delete_session(
    user_id: str, 
    api_key: Optional[str] = Query(None)
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
    clients_status = {}
    for user_id, client in tg_manager.clients.items():
        clients_status[user_id] = client.is_connected()
    
    return {
        "status": "online",
        "version": "2.1.0",
        "mode": "polling (starts only after query)",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "clients_connected": len(tg_manager.clients),
        "clients_detail": clients_status,
        "cache_stats": cache.stats(),
        "target_bot": TARGET_BOT,
        "poll_interval": POLL_INTERVAL,
        "timeout": RESPONSE_TIMEOUT
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
            "active_users": len(rate_limiter._requests)
        },
        "config": {
            "poll_interval": POLL_INTERVAL,
            "timeout": RESPONSE_TIMEOUT
        }
    }

@app.get("/")
async def root():
    return {
        "api": "🤖 Telegram Bot Bridge API v2.1.0",
        "mode": "polling - starts ONLY AFTER query is sent",
        "how_it_works": {
            "step_1": "Send query to bot",
            "step_2": "THEN start polling for response (not before)",
            "step_3": "Ignore 'loading/processing' messages",
            "step_4": "Wait for bot to EDIT message to final version",
            "step_5": "Return the final response"
        },
        "endpoints": {
            "send_query": "GET /query?query=hello&user_id=default&api_key=YOUR_KEY",
            "register_session": "POST /session/register?user_id=me&session_string=...&api_key=YOUR_KEY",
            "health": "GET /health",
            "metrics": "GET /metrics?api_key=YOUR_KEY"
        },
        "docs": "/docs"
    }