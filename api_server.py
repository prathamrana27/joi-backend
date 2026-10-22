import os
import json
import uuid
import asyncio
import re
import sys
import tempfile
import base64
import hashlib
import hmac
import secrets
import time
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from urllib.parse import urlparse
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from fastapi import (
    FastAPI,
    UploadFile,
    File,
    Form,
    HTTPException,
    Depends,
    Header,
    WebSocket,
    WebSocketDisconnect,
    Query,
)
from fastapi.responses import StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import logging
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from openai import OpenAI
import redis
import httpx
from redis.exceptions import RedisError
from pymongo import MongoClient, DESCENDING
from pymongo.collection import Collection
from pymongo.errors import PyMongoError, DuplicateKeyError
from bson import ObjectId
from bson.errors import InvalidId
from google.oauth2 import id_token as google_id_token
from google.oauth2.credentials import Credentials as GoogleUserCredentials
from google.auth.transport import requests as google_auth_requests

def _resolve_env_file() -> Optional[Path]:
    override_path = str(os.getenv("JOI_ENV_PATH", "")).strip()
    candidates: List[Path] = []

    if override_path:
        candidates.append(Path(override_path).expanduser())

    if getattr(sys, "frozen", False):
        candidates.append(Path(sys.executable).resolve().parent / ".env")

    try:
        candidates.append(Path(__file__).resolve().parent / ".env")
    except Exception:
        pass

    candidates.append(Path.cwd() / ".env")

    for candidate in candidates:
        try:
            if candidate.is_file():
                return candidate
        except Exception:
            continue
    return None


_env_file_path = _resolve_env_file()
if _env_file_path:
    load_dotenv(dotenv_path=_env_file_path, override=False)
else:
    load_dotenv(override=False)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
# httpx logs every request URL at INFO, and some APIs put credentials in the URL
# (Telegram: /bot<token>/...). Keep request lines out of the logs.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger("api_server")

# ----------- LLM PROVIDER -----------
# Chat is served by the configured provider (OpenAI by default, local Ollama when
# JOI_LLM_PROVIDER=ollama). OpenAI is only hard-required when it is the chat
# provider; otherwise it stays optional and is used only for title / STT / TTS
# when a key happens to be present.
LLM_PROVIDER = str(os.getenv("JOI_LLM_PROVIDER", "openai")).strip().lower()
OPENAI_API_KEY = str(os.getenv("OPENAI_API_KEY", "")).strip()

if LLM_PROVIDER == "openai" and not OPENAI_API_KEY:
    logger.error("CRITICAL: OPENAI_API_KEY not found but JOI_LLM_PROVIDER=openai.")
    raise SystemExit(1)

# Optional OpenAI client for auxiliary features (chat title, transcription, TTS).
# When absent, those endpoints degrade gracefully instead of crashing the server.
title_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
logger.info("LLM provider selected: %s (openai aux features: %s)",
            LLM_PROVIDER, "on" if title_client else "off")

# Chat handler (provider-agnostic)
from llm_provider import astream_llm, warm_up as warm_up_llm, unload as unload_llm
# Voice handler (ElevenLabs / OpenAI)
import voice_provider

from tools import tool_registry
from prompts import system_prompt
from security import redact_secrets, wrap_untrusted_tool_output
from briefing import build_briefing
from telegram_channel import TelegramChannel, get_me as tg_get_me
import memory_learning
import routine_triggers
import wake_word
from tools.undo import extract_undo
import tools.google_auth as google_auth
from tools.google_auth import COMBINED_SCOPES as GOOGLE_COMBINED_SCOPES

MAX_CONSECUTIVE_TOOL_CALLS = 10
SESSIONS_LIST_CACHE_PREFIX = "sessions:list:"
SESSION_CACHE_PREFIX = "sessions:detail:"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "joi_local")
MONGO_SESSIONS_COLLECTION = os.getenv("MONGO_SESSIONS_COLLECTION", "sessions")
MONGO_FEEDBACK_COLLECTION = os.getenv("MONGO_FEEDBACK_COLLECTION", "message_feedback")
MONGO_USERS_COLLECTION = os.getenv("MONGO_USERS_COLLECTION", "users")
MONGO_AUTH_IDENTITIES_COLLECTION = os.getenv("MONGO_AUTH_IDENTITIES_COLLECTION", "auth_identities")
MONGO_MEMORY_COLLECTION = os.getenv("MONGO_MEMORY_COLLECTION", "user_memory")
MONGO_ROUTINES_COLLECTION = os.getenv("MONGO_ROUTINES_COLLECTION", "routines")
MONGO_REMINDERS_COLLECTION = os.getenv("MONGO_REMINDERS_COLLECTION", "reminders")
MONGO_JOBS_COLLECTION = os.getenv("MONGO_JOBS_COLLECTION", "jobs")
MONGO_NOTIFICATIONS_COLLECTION = os.getenv("MONGO_NOTIFICATIONS_COLLECTION", "notifications")
MONGO_AUDIT_COLLECTION = os.getenv("MONGO_AUDIT_COLLECTION", "audit_logs")
MONGO_PERMISSIONS_COLLECTION = os.getenv("MONGO_PERMISSIONS_COLLECTION", "user_permissions")
MONGO_APPROVALS_COLLECTION = os.getenv("MONGO_APPROVALS_COLLECTION", "tool_approvals")
MONGO_RAG_CHUNKS_COLLECTION = os.getenv("MONGO_RAG_CHUNKS_COLLECTION", "rag_chunks")

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
REDIS_SESSIONS_LIST_TTL = int(os.getenv("REDIS_SESSIONS_LIST_TTL", "20"))
REDIS_SESSION_TTL = int(os.getenv("REDIS_SESSION_TTL", "120"))

AUTH_JWT_SECRET = os.getenv("AUTH_JWT_SECRET", "dev-only-change-me")
AUTH_JWT_EXP_SECONDS = int(os.getenv("AUTH_JWT_EXP_SECONDS", "604800"))
AUTH_PASSWORD_PBKDF2_ITERATIONS = int(os.getenv("AUTH_PASSWORD_PBKDF2_ITERATIONS", "260000"))
GOOGLE_OAUTH_CLIENT_ID = str(os.getenv("GOOGLE_OAUTH_CLIENT_ID", "")).strip()
GOOGLE_OAUTH_CLIENT_SECRET = str(os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "")).strip()
GOOGLE_OAUTH_ALLOWED_CLIENT_IDS_RAW = str(
    os.getenv("GOOGLE_OAUTH_ALLOWED_CLIENT_IDS", "")
).strip()

# Slack per-user OAuth (each user connects their own workspace)
SLACK_CLIENT_ID = str(os.getenv("SLACK_CLIENT_ID", "")).strip()
SLACK_CLIENT_SECRET = str(os.getenv("SLACK_CLIENT_SECRET", "")).strip()
SLACK_REDIRECT_URI = str(
    os.getenv("SLACK_REDIRECT_URI", "http://127.0.0.1:8000/integrations/slack/callback")
).strip()
# Installed copies don't hold the Slack secret: they send the one-time code to JOI's
# token-exchange function (infra/slack_exchange), which holds it. Dev keeps the secret.
SLACK_EXCHANGE_URL = str(os.getenv("SLACK_EXCHANGE_URL", "")).strip()
SLACK_CONFIGURED = bool(SLACK_CLIENT_ID and (SLACK_CLIENT_SECRET or SLACK_EXCHANGE_URL))
SLACK_BOT_SCOPES = "chat:write,channels:read,channels:history,groups:read,users:read"

# Google per-user OAuth for API access (Gmail/Calendar/Drive/Docs/Sheets/Contacts).
# Reuses the web OAuth client; the redirect URI must be registered in Google Cloud.
GOOGLE_API_REDIRECT_URI = str(
    os.getenv("GOOGLE_API_REDIRECT_URI", "http://127.0.0.1:8000/integrations/google/callback")
).strip()
# Tools that require the user's Google credentials.
GOOGLE_TOOLS = {
    "email", "calendar", "drive_search", "drive_read", "docs_create", "docs_read",
    "sheets_read", "sheets_append", "meet_create", "contacts_search",
}

_WEAK_JWT_SECRETS = {"", "dev-only-change-me", "change-me", "secret"}
if AUTH_JWT_SECRET in _WEAK_JWT_SECRETS or len(AUTH_JWT_SECRET) < 32:
    # A weak/shared signing secret lets anyone forge auth tokens for any account.
    # Allow it only when explicitly opted in for local development.
    if str(os.getenv("JOI_ALLOW_DEV_SECRET", "")).strip() == "1":
        logger.warning(
            "AUTH_JWT_SECRET is weak/default. Running in dev mode (JOI_ALLOW_DEV_SECRET=1)."
        )
    else:
        logger.error(
            "CRITICAL: AUTH_JWT_SECRET is missing or too weak. "
            "Set a strong random AUTH_JWT_SECRET (>=32 chars) in .env, "
            "or set JOI_ALLOW_DEV_SECRET=1 for local development only."
        )
        raise SystemExit(1)

app = FastAPI(title="JOI - AI Assistant API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

background_scheduler_task: Optional[asyncio.Task] = None
background_scheduler_stop = asyncio.Event()


class NotificationHub:
    """Tracks live WebSocket connections per user for real-time push."""

    def __init__(self) -> None:
        self._conns: Dict[str, set] = {}

    async def connect(self, user_id: str, ws: WebSocket) -> None:
        self._conns.setdefault(user_id, set()).add(ws)

    def disconnect(self, user_id: str, ws: WebSocket) -> None:
        conns = self._conns.get(user_id)
        if conns:
            conns.discard(ws)
            if not conns:
                self._conns.pop(user_id, None)

    async def broadcast(self, user_id: str, payload: Dict[str, Any]) -> None:
        for ws in list(self._conns.get(user_id, set())):
            try:
                await ws.send_json(payload)
            except Exception:
                self.disconnect(user_id, ws)


notification_hub = NotificationHub()


def push_notification_realtime(user_id: str, payload: Dict[str, Any]) -> None:
    """Fire-and-forget realtime push; safe to call from the async event loop."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(notification_hub.broadcast(user_id, payload))
    except RuntimeError:
        pass  # no running loop (e.g. called from a worker thread)

try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    mongo_client.admin.command("ping")
    mongo_db = mongo_client[MONGO_DB_NAME]
    sessions_collection: Collection = mongo_db[MONGO_SESSIONS_COLLECTION]
    feedback_collection: Collection = mongo_db[MONGO_FEEDBACK_COLLECTION]
    users_collection: Collection = mongo_db[MONGO_USERS_COLLECTION]
    auth_identities_collection: Collection = mongo_db[MONGO_AUTH_IDENTITIES_COLLECTION]
    memory_collection: Collection = mongo_db[MONGO_MEMORY_COLLECTION]
    routines_collection: Collection = mongo_db[MONGO_ROUTINES_COLLECTION]
    reminders_collection: Collection = mongo_db[MONGO_REMINDERS_COLLECTION]
    jobs_collection: Collection = mongo_db[MONGO_JOBS_COLLECTION]
    notifications_collection: Collection = mongo_db[MONGO_NOTIFICATIONS_COLLECTION]
    audit_collection: Collection = mongo_db[MONGO_AUDIT_COLLECTION]
    permissions_collection: Collection = mongo_db[MONGO_PERMISSIONS_COLLECTION]
    approvals_collection: Collection = mongo_db[MONGO_APPROVALS_COLLECTION]
    rag_chunks_collection: Collection = mongo_db[MONGO_RAG_CHUNKS_COLLECTION]
    integrations_collection: Collection = mongo_db["integrations"]
    memory_suggestions_collection: Collection = mongo_db["memory_suggestions"]
    undo_collection: Collection = mongo_db["undo_actions"]
    sessions_collection.create_index("session_id", unique=True)
    sessions_collection.create_index([("owner_user_id", 1), ("updated_at", DESCENDING)])
    sessions_collection.create_index([("updated_at", DESCENDING)])
    try:
        feedback_collection.drop_index("conversation_id_1_message_id_1")
    except Exception:
        pass
    feedback_collection.create_index(
        [("owner_user_id", 1), ("conversation_id", 1), ("message_id", 1)],
        unique=True
    )
    feedback_collection.create_index([("owner_user_id", 1), ("updated_at", DESCENDING)])
    feedback_collection.create_index([("updated_at", DESCENDING)])
    users_collection.create_index("email_normalized", unique=True, sparse=True)
    users_collection.create_index([("updated_at", DESCENDING)])
    auth_identities_collection.create_index(
        [("provider", 1), ("provider_user_id", 1)],
        unique=True
    )
    auth_identities_collection.create_index([("user_id", 1)])
    memory_collection.create_index("owner_user_id", unique=True)
    routines_collection.create_index([("owner_user_id", 1), ("enabled", 1), ("next_run_at", 1)])
    reminders_collection.create_index([("owner_user_id", 1), ("status", 1), ("due_at", 1)])
    jobs_collection.create_index([("owner_user_id", 1), ("status", 1), ("run_at", 1)])
    notifications_collection.create_index([("owner_user_id", 1), ("created_at", -1)])
    notifications_collection.create_index([("owner_user_id", 1), ("read", 1), ("created_at", -1)])
    audit_collection.create_index([("owner_user_id", 1), ("created_at", -1)])
    permissions_collection.create_index("owner_user_id", unique=True)
    approvals_collection.create_index([("owner_user_id", 1), ("status", 1), ("created_at", -1)])
    approvals_collection.create_index([("owner_user_id", 1), ("approval_id", 1)], unique=True)
    try:
        rag_chunks_collection.create_index([("owner_user_id", 1), ("root_path", 1), ("file_path", 1)])
    except Exception:
        pass
    try:
        integrations_collection.create_index([("owner_user_id", 1), ("provider", 1)], unique=True)
        memory_suggestions_collection.create_index([("owner_user_id", 1), ("status", 1), ("created_at", -1)])
        undo_collection.create_index([("status", 1), ("execute_at", 1)])
        undo_collection.create_index("undo_id", unique=True)
    except Exception:
        pass
    try:
        rag_chunks_collection.create_index([("owner_user_id", 1), ("text", "text")])
    except Exception:
        pass
    logger.info("MongoDB connected: %s/%s", MONGO_DB_NAME, MONGO_SESSIONS_COLLECTION)
except Exception as exc:
    logger.error("CRITICAL: MongoDB connection failed (%s)", exc)
    raise SystemExit(1)

class _MemoryCache:
    """In-process stand-in for the few Redis calls JOI makes. Redis is only a cache
    here, so customer installs work without it (one desktop process, one user)."""

    def __init__(self) -> None:
        self._data: Dict[str, Tuple[float, str]] = {}

    def ping(self) -> bool:
        return True

    def get(self, key: str) -> Optional[str]:
        item = self._data.get(key)
        if not item:
            return None
        expires_at, value = item
        if expires_at < time.monotonic():
            self._data.pop(key, None)
            return None
        return value

    def setex(self, key: str, ttl_seconds: int, value: str) -> bool:
        if len(self._data) > 5000:
            now = time.monotonic()
            self._data = {k: v for k, v in self._data.items() if v[0] >= now}
        self._data[key] = (time.monotonic() + max(1, int(ttl_seconds)), value)
        return True

    def delete(self, *keys: str) -> int:
        return sum(1 for k in keys if self._data.pop(k, None) is not None)


redis_client: Any
if REDIS_URL:
    try:
        redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        logger.info("Redis connected: %s", REDIS_URL)
    except Exception as exc:
        logger.warning("Redis unavailable (%s); using the in-memory cache.", exc)
        redis_client = _MemoryCache()
else:
    logger.info("No REDIS_URL set; using the in-memory cache.")
    redis_client = _MemoryCache()


class ChatHistoryMessage(BaseModel):
    role: str = Field(..., description="system|user|assistant")
    content: str = Field(default="")


class SSEChatRequest(BaseModel):
    conversation_id: str = Field(..., min_length=1)
    message: str = Field(..., min_length=1)
    model: str = Field(default="openai")
    history: List[ChatHistoryMessage] = Field(default_factory=list)


class TitleRequest(BaseModel):
    history: List[ChatHistoryMessage] = Field(default_factory=list)
    fallback_text: str = Field(default="")


class TTSRequest(BaseModel):
    text: str = Field(..., min_length=1)
    model: str = Field(default=os.getenv("OPENAI_TTS_MODEL", "gpt-4o-mini-tts"))
    voice: str = Field(default=os.getenv("OPENAI_TTS_VOICE", "alloy"))


class MessageFeedbackRequest(BaseModel):
    conversation_id: str = Field(..., min_length=1)
    message_id: str = Field(..., min_length=1)
    feedback: str = Field(..., pattern="^(like|dislike|clear)$")
    message_text: str = Field(default="")
    model: str = Field(default="openai")


class AuthSignupRequest(BaseModel):
    first_name: str = Field(..., min_length=1, max_length=80)
    last_name: str = Field(..., min_length=1, max_length=80)
    email: str = Field(..., min_length=3, max_length=320)
    password: str = Field(..., min_length=8, max_length=256)
    confirm_password: str = Field(..., min_length=8, max_length=256)


class AuthLoginRequest(BaseModel):
    email: str = Field(..., min_length=3, max_length=320)
    password: str = Field(..., min_length=1, max_length=256)


class GoogleAuthRequest(BaseModel):
    credential: str = Field(..., min_length=20)


class GoogleDesktopAuthRequest(BaseModel):
    code: str = Field(..., min_length=10)
    code_verifier: str = Field(..., min_length=43, max_length=128)
    redirect_uri: str = Field(..., min_length=10, max_length=1024)


class MemoryUpsertRequest(BaseModel):
    preferences: str = Field(default="", max_length=8000)
    notes: str = Field(default="", max_length=16000)


class PermissionRuleUpsertRequest(BaseModel):
    tool_name: str = Field(..., min_length=2, max_length=80)
    mode: str = Field(..., pattern="^(allow|deny|require_approval)$")


class ChatResumeRequest(BaseModel):
    conversation_id: str = Field(..., min_length=1)
    approval_id: str = Field(..., min_length=1)


class ApprovalDecisionRequest(BaseModel):
    decision: str = Field(..., pattern="^(approved|denied)$")
    note: str = Field(default="", max_length=1000)


class RoutineCreateRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=120)
    prompt: str = Field(..., min_length=3, max_length=8000)
    interval_minutes: int = Field(default=1440, ge=5, le=10080)
    enabled: bool = Field(default=True)
    # When to run: on a schedule, or on an event (see routine_triggers.py).
    trigger_type: str = Field(default="schedule", pattern="^(schedule|email|meeting|battery)$")
    trigger_value: str = Field(default="", max_length=200)


class RoutineUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=2, max_length=120)
    prompt: Optional[str] = Field(default=None, min_length=3, max_length=8000)
    interval_minutes: Optional[int] = Field(default=None, ge=5, le=10080)
    enabled: Optional[bool] = None
    trigger_type: Optional[str] = Field(default=None, pattern="^(schedule|email|meeting|battery)$")
    trigger_value: Optional[str] = Field(default=None, max_length=200)


class ReminderCreateRequest(BaseModel):
    title: str = Field(..., min_length=2, max_length=140)
    message: str = Field(default="", max_length=2000)
    due_at: str = Field(..., min_length=5, max_length=64)


class ReminderUpdateRequest(BaseModel):
    title: Optional[str] = Field(default=None, min_length=2, max_length=140)
    message: Optional[str] = Field(default=None, max_length=2000)
    due_at: Optional[str] = Field(default=None, min_length=5, max_length=64)
    status: Optional[str] = Field(default=None, pattern="^(pending|done|cancelled)$")


class JobCreateRequest(BaseModel):
    job_type: str = Field(..., min_length=2, max_length=80)
    payload: Dict[str, Any] = Field(default_factory=dict)
    run_at: Optional[str] = Field(default=None, min_length=5, max_length=64)


class RagIndexRequest(BaseModel):
    root_path: str = Field(..., min_length=1, max_length=1024)
    max_files: int = Field(default=100, ge=1, le=1000)
    max_bytes_per_file: int = Field(default=500000, ge=1000, le=3000000)


class RagQueryRequest(BaseModel):
    query: str = Field(..., min_length=2, max_length=4000)
    top_k: int = Field(default=5, ge=1, le=15)


EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
GOOGLE_ID_TOKEN_REQUEST = google_auth_requests.Request()


def _normalize_email(email: str) -> str:
    return str(email or "").strip().lower()


def _validate_email_or_400(email: str) -> str:
    normalized = _normalize_email(email)
    if not normalized or not EMAIL_PATTERN.match(normalized):
        raise HTTPException(status_code=400, detail="Invalid email format.")
    return normalized


def _normalize_name(name: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(name or "").strip())
    return cleaned[:80]


def _display_name(first_name: str, last_name: str, email: str) -> str:
    merged = f"{first_name} {last_name}".strip()
    if merged:
        return merged[:160]
    return _normalize_email(email)[:160]


def _unix_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(f"{raw}{padding}")


def _jwt_encode(payload: Dict[str, Any]) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    header_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    signature = hmac.new(
        AUTH_JWT_SECRET.encode("utf-8"),
        signing_input,
        hashlib.sha256
    ).digest()
    signature_b64 = _b64url_encode(signature)
    return f"{header_b64}.{payload_b64}.{signature_b64}"


def _jwt_decode(token: str) -> Dict[str, Any]:
    parts = str(token or "").split(".")
    if len(parts) != 3:
        raise HTTPException(status_code=401, detail="Invalid auth token.")

    header_b64, payload_b64, signature_b64 = parts
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    expected_signature = hmac.new(
        AUTH_JWT_SECRET.encode("utf-8"),
        signing_input,
        hashlib.sha256
    ).digest()
    provided_signature = _b64url_decode(signature_b64)

    if not hmac.compare_digest(expected_signature, provided_signature):
        raise HTTPException(status_code=401, detail="Invalid auth token signature.")

    try:
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
    except (ValueError, json.JSONDecodeError):
        raise HTTPException(status_code=401, detail="Invalid auth token payload.")

    exp = int(payload.get("exp", 0))
    if exp <= _unix_ts():
        raise HTTPException(status_code=401, detail="Auth token expired.")

    return payload


def _hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    derived_key = hashlib.pbkdf2_hmac(
        "sha256",
        str(password).encode("utf-8"),
        salt.encode("utf-8"),
        AUTH_PASSWORD_PBKDF2_ITERATIONS
    )
    encoded_key = base64.b64encode(derived_key).decode("utf-8")
    return f"pbkdf2_sha256${AUTH_PASSWORD_PBKDF2_ITERATIONS}${salt}${encoded_key}"


def _verify_password(password: str, stored_hash: str) -> bool:
    try:
        algorithm, iterations_str, salt, encoded_key = str(stored_hash or "").split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        iterations = int(iterations_str)
        expected_key = base64.b64decode(encoded_key.encode("utf-8"))
    except (ValueError, TypeError):
        return False

    derived_key = hashlib.pbkdf2_hmac(
        "sha256",
        str(password).encode("utf-8"),
        salt.encode("utf-8"),
        iterations
    )
    return hmac.compare_digest(derived_key, expected_key)


def _serialize_user(user_doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(user_doc.get("_id")),
        "email": str(user_doc.get("email") or ""),
        "first_name": str(user_doc.get("first_name") or ""),
        "last_name": str(user_doc.get("last_name") or ""),
        "display_name": str(user_doc.get("display_name") or ""),
        "picture_url": str(user_doc.get("picture_url") or ""),
        "is_email_verified": bool(user_doc.get("is_email_verified", False)),
        "created_at": str(user_doc.get("created_at") or "")
    }


def _issue_auth_payload(user_doc: Dict[str, Any]) -> Dict[str, Any]:
    now_ts = _unix_ts()
    payload = {
        "sub": str(user_doc.get("_id")),
        "email": str(user_doc.get("email") or ""),
        "iat": now_ts,
        "exp": now_ts + AUTH_JWT_EXP_SECONDS
    }
    token = _jwt_encode(payload)
    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_in": AUTH_JWT_EXP_SECONDS,
        "user": _serialize_user(user_doc)
    }


def _fetch_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    try:
        oid = ObjectId(str(user_id))
    except (InvalidId, TypeError):
        return None
    return users_collection.find_one({"_id": oid})


def _extract_bearer_token(authorization_header: Optional[str]) -> str:
    raw = str(authorization_header or "").strip()
    if not raw:
        raise HTTPException(status_code=401, detail="Authorization header is required.")
    if not raw.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Authorization header must be Bearer token.")
    token = raw[7:].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Bearer token is missing.")
    return token


def _upsert_auth_identity(
    user_id: ObjectId,
    provider: str,
    provider_user_id: str,
    email: str
) -> None:
    now = _utc_now_iso()
    auth_identities_collection.update_one(
        {"provider": provider, "provider_user_id": provider_user_id},
        {
            "$set": {
                "user_id": user_id,
                "provider": provider,
                "provider_user_id": provider_user_id,
                "email_at_provider": str(email or ""),
                "last_login_at": now
            },
            "$setOnInsert": {
                "created_at": now
            }
        },
        upsert=True
    )


def _touch_user_login(user_id: ObjectId) -> None:
    users_collection.update_one(
        {"_id": user_id},
        {"$set": {"last_login_at": _utc_now_iso()}}
    )


def _verify_google_id_credential(credential: str) -> Dict[str, Any]:
    if not GOOGLE_OAUTH_CLIENT_ID and not GOOGLE_OAUTH_ALLOWED_CLIENT_IDS_RAW:
        raise HTTPException(status_code=500, detail="Google auth is not configured on the server.")

    allowed_client_ids = {
        c.strip()
        for c in (
            [GOOGLE_OAUTH_CLIENT_ID]
            + GOOGLE_OAUTH_ALLOWED_CLIENT_IDS_RAW.split(",")
        )
        if c and str(c).strip()
    }

    try:
        claims = google_id_token.verify_oauth2_token(
            credential,
            GOOGLE_ID_TOKEN_REQUEST,
            # Tolerate small clock drift between the user's PC and Google. Without this,
            # a freshly issued token is rejected as "used too early" if the PC clock is
            # even one second behind.
            clock_skew_in_seconds=60,
        )
    except ValueError as exc:
        logger.warning("Google credential rejected: %s", exc)
        raise HTTPException(
            status_code=401,
            detail="Google sign-in couldn't be verified. Please try again, and check that your PC's date and time are set automatically.",
        )

    issuer = str(claims.get("iss", ""))
    if issuer not in {"accounts.google.com", "https://accounts.google.com"}:
        raise HTTPException(status_code=401, detail="Invalid Google token issuer.")

    audience = claims.get("aud")
    audience_values = audience if isinstance(audience, list) else [str(audience or "").strip()]
    audience_values = [a for a in audience_values if a]
    if not audience_values or not any(a in allowed_client_ids for a in audience_values):
        raise HTTPException(
            status_code=401,
            detail=(
                "Google token audience mismatch. "
                f"received={audience_values} allowed={sorted(allowed_client_ids)}"
            )
        )

    google_sub = str(claims.get("sub") or "").strip()
    email = _normalize_email(claims.get("email"))
    if not google_sub or not email:
        raise HTTPException(status_code=401, detail="Google credential is missing required fields.")

    return claims


def _is_valid_google_loopback_redirect(redirect_uri: str) -> bool:
    parsed = urlparse(str(redirect_uri or "").strip())
    if parsed.scheme != "http":
        return False
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        return False
    if not parsed.path:
        return False
    if not parsed.port:
        return False
    return True


async def _exchange_google_auth_code_for_credential(
    code: str,
    code_verifier: str,
    redirect_uri: str
) -> str:
    if not GOOGLE_OAUTH_CLIENT_ID:
        raise HTTPException(status_code=500, detail="Google auth is not configured on the server.")

    if not _is_valid_google_loopback_redirect(redirect_uri):
        raise HTTPException(status_code=400, detail="Invalid Google redirect_uri for desktop flow.")

    token_payload = {
        "client_id": GOOGLE_OAUTH_CLIENT_ID,
        "grant_type": "authorization_code",
        "code": str(code or "").strip(),
        "code_verifier": str(code_verifier or "").strip(),
        "redirect_uri": str(redirect_uri or "").strip()
    }
    if GOOGLE_OAUTH_CLIENT_SECRET:
        token_payload["client_secret"] = GOOGLE_OAUTH_CLIENT_SECRET

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            token_response = await client.post(
                "https://oauth2.googleapis.com/token",
                data=token_payload
            )
    except Exception as exc:
        logger.exception("Google token exchange request failed: %s", exc)
        raise HTTPException(status_code=502, detail="Google token exchange failed.") from exc

    if token_response.status_code >= 400:
        detail = f"status={token_response.status_code}"
        try:
            err_payload = token_response.json()
            detail = str(
                err_payload.get("error_description")
                or err_payload.get("error")
                or detail
            )
        except Exception:
            pass
        logger.warning("Google token exchange rejected: %s", detail)
        raise HTTPException(status_code=401, detail=f"Google sign-in failed: {detail}")

    try:
        token_data = token_response.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="Google token response was not valid JSON.")

    credential = str(token_data.get("id_token") or "").strip()
    if not credential:
        raise HTTPException(status_code=401, detail="Google token response did not include id_token.")
    return credential


def _issue_auth_payload_from_google_claims(claims: Dict[str, Any]) -> Dict[str, Any]:
    google_sub = str(claims.get("sub") or "").strip()
    email_normalized = _validate_email_or_400(str(claims.get("email") or ""))
    first_name = _normalize_name(str(claims.get("given_name") or ""))
    last_name = _normalize_name(str(claims.get("family_name") or ""))
    picture_url = str(claims.get("picture") or "")[:2048]
    email_verified = bool(claims.get("email_verified", False))
    now = _utc_now_iso()

    try:
        identity = auth_identities_collection.find_one(
            {"provider": "google", "provider_user_id": google_sub}
        )
        user_doc = None
        if identity and identity.get("user_id"):
            user_doc = users_collection.find_one({"_id": identity["user_id"]})

        if not user_doc:
            user_doc = users_collection.find_one({"email_normalized": email_normalized})

        if user_doc:
            merged_first = str(user_doc.get("first_name") or "").strip() or first_name
            merged_last = str(user_doc.get("last_name") or "").strip() or last_name
            merged_picture = str(user_doc.get("picture_url") or "").strip() or picture_url
            users_collection.update_one(
                {"_id": user_doc["_id"]},
                {
                    "$set": {
                        "email": email_normalized,
                        "email_normalized": email_normalized,
                        "first_name": merged_first,
                        "last_name": merged_last,
                        "display_name": _display_name(merged_first, merged_last, email_normalized),
                        "picture_url": merged_picture,
                        "is_email_verified": bool(
                            user_doc.get("is_email_verified", False) or email_verified
                        ),
                        "updated_at": now
                    }
                }
            )
            user_id = user_doc["_id"]
        else:
            new_user = {
                "email": email_normalized,
                "email_normalized": email_normalized,
                "first_name": first_name,
                "last_name": last_name,
                "display_name": _display_name(first_name, last_name, email_normalized),
                "picture_url": picture_url,
                "is_email_verified": email_verified,
                "created_at": now,
                "updated_at": now,
                "last_login_at": now
            }
            insert_result = users_collection.insert_one(new_user)
            user_id = insert_result.inserted_id

        _upsert_auth_identity(user_id, "google", google_sub, email_normalized)
        _upsert_auth_identity(user_id, "email", email_normalized, email_normalized)
        _touch_user_login(user_id)
        fresh_user = users_collection.find_one({"_id": user_id})
        if not fresh_user:
            raise HTTPException(status_code=500, detail="Failed to load account after Google sign-in.")
        return _issue_auth_payload(fresh_user)
    except HTTPException:
        raise
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="Account conflict detected. Please sign in with email.")
    except PyMongoError as exc:
        logger.exception("Google auth failed for '%s': %s", email_normalized, exc)
        raise HTTPException(status_code=500, detail="Google sign-in failed.") from exc


async def require_auth_user(authorization: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    token = _extract_bearer_token(authorization)
    payload = _jwt_decode(token)
    user = _fetch_user_by_id(str(payload.get("sub", "")))
    if not user:
        raise HTTPException(status_code=401, detail="Authenticated user not found.")
    return user


def _sanitize_history(history: List[ChatHistoryMessage]) -> List[Dict[str, str]]:
    sanitized: List[Dict[str, str]] = []
    allowed_roles = {"system", "user", "assistant"}

    for msg in history:
        role = str(msg.role).strip().lower()
        content = str(msg.content or "")
        # Client-sent "system" turns are dropped: the server's own system prompt is the
        # only source of JOI's rules, so a modified client can't replace them.
        if role in allowed_roles and role != "system" and content.strip():
            sanitized.append({"role": role, "content": content})

    sanitized.insert(0, {"role": "system", "content": system_prompt})
    return sanitized


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _current_time_context_prompt() -> str:
    now_local = datetime.now().astimezone()
    now_utc = now_local.astimezone(timezone.utc)
    offset_raw = now_local.strftime("%z")
    offset_fmt = (
        f"{offset_raw[:3]}:{offset_raw[3:]}"
        if len(offset_raw) == 5
        else offset_raw
    )
    tz_name = now_local.tzname() or "local"

    return (
        "Authoritative current time context:\n"
        f"- Local datetime: {now_local.strftime('%Y-%m-%d %H:%M:%S')} ({tz_name}, UTC{offset_fmt})\n"
        f"- Local date: {now_local.strftime('%Y-%m-%d')}\n"
        f"- UTC datetime: {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        "Resolve relative terms such as today/tomorrow/yesterday/this week using the local date above. "
        "Do not treat stale source dates as current unless the user explicitly asks for historical data."
    )


# Tool names that default to require_approval. These MUST match the names the
# model emits in TOOL_CALL:: payloads (not the Python module names).
DANGEROUS_TOOLS = {
    # Existing outward-facing / write-capable tools
    "email", "whatsapp", "app", "calendar", "fs_write", "fs_mkdir",
    # New system-control tools
    "clipboard_write", "window_close", "open_path",
    "type_text", "press_keys", "mouse_click", "power", "shell",
    # Google Workspace write actions
    "docs_create", "sheets_append", "meet_create",
    # Slack
    "slack_send",
}
RAG_ALLOWED_SUFFIXES = {
    ".txt", ".md", ".py", ".json", ".csv", ".log", ".html", ".htm",
    ".js", ".ts", ".tsx", ".css", ".xml", ".yaml", ".yml", ".ini", ".cfg"
}


def _parse_iso_datetime(value: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError("datetime text is required")

    normalized = text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _doc_id_or_404(raw_id: str) -> ObjectId:
    try:
        return ObjectId(str(raw_id))
    except (InvalidId, TypeError):
        raise HTTPException(status_code=404, detail="Record not found.")


def _serialize_mongo_doc(doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not doc:
        return {}
    result = dict(doc)
    if "_id" in result:
        result["id"] = str(result["_id"])
        del result["_id"]
    return result


def _write_audit_log(
    owner_user_id: str,
    action: str,
    status: str,
    metadata: Optional[Dict[str, Any]] = None
) -> None:
    try:
        audit_collection.insert_one(
            {
                "owner_user_id": owner_user_id,
                "action": str(action or "").strip()[:120],
                "status": str(status or "").strip()[:40],
                "metadata": metadata or {},
                "created_at": _utc_now_iso()
            }
        )
    except Exception:
        pass


def _create_notification(
    owner_user_id: str,
    title: str,
    body: str,
    category: str = "system"
) -> None:
    try:
        doc = {
            "owner_user_id": owner_user_id,
            "title": str(title or "").strip()[:180],
            "body": str(body or "").strip()[:4000],
            "category": str(category or "system").strip()[:60],
            "read": False,
            "created_at": _utc_now_iso()
        }
        result = notifications_collection.insert_one(doc)
        # Real-time push to any connected clients.
        push_notification_realtime(
            owner_user_id,
            {
                "type": "notification",
                "id": str(result.inserted_id),
                "title": doc["title"],
                "body": doc["body"],
                "category": doc["category"],
                "created_at": doc["created_at"],
            },
        )
    except Exception:
        pass


def _get_permission_rules(owner_user_id: str) -> Dict[str, str]:
    doc = permissions_collection.find_one({"owner_user_id": owner_user_id}) or {}
    rules = doc.get("rules", {})
    if not isinstance(rules, dict):
        rules = {}

    merged = {tool_name: "require_approval" for tool_name in DANGEROUS_TOOLS}
    for key, value in rules.items():
        tool = str(key).strip()
        mode = str(value).strip()
        if tool and mode in {"allow", "deny", "require_approval"}:
            merged[tool] = mode
    return merged


def _tool_execution_mode(owner_user_id: str, tool_name: str) -> str:
    rules = _get_permission_rules(owner_user_id)
    return rules.get(str(tool_name).strip(), "allow")


_READ_ONLY_CALENDAR_ACTIONS = {"list_events", "list_tasklists", "list_tasks"}


def _is_read_only_call(tool_name: str, args: Dict[str, Any]) -> bool:
    """True for calls to normally-gated tools that only read the user's own data."""
    command = str((args or {}).get("command_string", ""))
    if tool_name == "email":
        reads = re.search(r"\b(read\s*:\s*true|read_full_id\s*:)", command, re.IGNORECASE)
        return bool(reads) and not re.search(r"\bto\s*:", command, re.IGNORECASE)
    if tool_name == "calendar":
        action = re.search(r"\baction\s*:\s*(\w+)", command, re.IGNORECASE)
        return bool(action) and action.group(1).lower() in _READ_ONLY_CALENDAR_ACTIONS
    return False


def _create_tool_approval(
    owner_user_id: str, tool_name: str, args: Dict[str, Any], session_id: str = ""
) -> Dict[str, Any]:
    now = _utc_now_iso()
    approval_id = f"apr_{uuid.uuid4().hex[:16]}"
    payload = {
        "approval_id": approval_id,
        "owner_user_id": owner_user_id,
        "session_id": str(session_id or ""),
        "tool_name": str(tool_name or "").strip(),
        "tool_args": args or {},
        "status": "pending",
        "decision_note": "",
        "created_at": now,
        "updated_at": now
    }
    approvals_collection.insert_one(payload)
    _write_audit_log(owner_user_id, "tool_approval_requested", "pending", {"approval_id": approval_id, "tool_name": tool_name})
    return payload


def _consume_matching_approved_tool_approval(
    owner_user_id: str,
    tool_name: str,
    args: Dict[str, Any]
) -> Optional[str]:
    try:
        candidate = approvals_collection.find_one(
            {
                "owner_user_id": owner_user_id,
                "tool_name": str(tool_name or "").strip(),
                "status": "approved",
                "consumed_at": {"$exists": False}
            },
            sort=[("updated_at", DESCENDING)]
        )
        if not candidate:
            return None

        expected = json.dumps(candidate.get("tool_args", {}), sort_keys=True, ensure_ascii=False)
        current = json.dumps(args or {}, sort_keys=True, ensure_ascii=False)
        if expected != current:
            return None

        approval_id = str(candidate.get("approval_id") or "")
        approvals_collection.update_one(
            {"_id": candidate.get("_id"), "status": "approved"},
            {
                "$set": {
                    "status": "used",
                    "consumed_at": _utc_now_iso(),
                    "updated_at": _utc_now_iso()
                }
            }
        )
        return approval_id or None
    except Exception:
        return None


def _read_user_memory(owner_user_id: str) -> Dict[str, str]:
    doc = memory_collection.find_one({"owner_user_id": owner_user_id}) or {}
    return {
        "preferences": str(doc.get("preferences") or ""),
        "notes": str(doc.get("notes") or "")
    }


def _home_resolved() -> Path:
    return Path.home().resolve()


def _is_path_allowed_for_rag(path: Path) -> bool:
    try:
        target = path.resolve()
        home = _home_resolved()
        return str(target).lower().startswith(str(home).lower())
    except Exception:
        return False


def _chunk_text(text: str, chunk_size: int = 900, overlap: int = 160) -> List[str]:
    normalized = str(text or "").replace("\r\n", "\n").strip()
    if not normalized:
        return []

    chunks: List[str] = []
    i = 0
    step = max(50, chunk_size - overlap)
    while i < len(normalized):
        chunk = normalized[i:i + chunk_size].strip()
        if chunk:
            chunks.append(chunk)
        i += step
    return chunks


def _rag_search(owner_user_id: str, query_text: str, top_k: int = 5) -> List[Dict[str, Any]]:
    query = str(query_text or "").strip()
    if not query:
        return []

    rows: List[Dict[str, Any]] = []
    try:
        cursor = rag_chunks_collection.find(
            {"owner_user_id": owner_user_id, "$text": {"$search": query}},
            {
                "_id": 0,
                "file_path": 1,
                "chunk_index": 1,
                "text": 1,
                "score": {"$meta": "textScore"}
            }
        ).sort([("score", {"$meta": "textScore"})]).limit(max(1, int(top_k)))
        rows = list(cursor)
    except Exception:
        rows = []

    if rows:
        return rows

    tokens = [t for t in re.split(r"\W+", query.lower()) if t]
    sample = list(
        rag_chunks_collection.find(
            {"owner_user_id": owner_user_id},
            {"_id": 0, "file_path": 1, "chunk_index": 1, "text": 1}
        ).limit(500)
    )
    scored: List[Dict[str, Any]] = []
    for row in sample:
        text = str(row.get("text") or "").lower()
        score = sum(1 for tok in tokens if tok in text)
        if score > 0:
            scored.append({**row, "score": score})
    scored.sort(key=lambda x: x.get("score", 0), reverse=True)
    return scored[: max(1, int(top_k))]


def _run_scheduler_tick() -> None:
    now = datetime.now(timezone.utc)
    now_iso = _as_iso(now)

    due_reminders = list(
        reminders_collection.find(
            {"status": "pending", "due_at": {"$lte": now_iso}}
        ).limit(25)
    )
    for reminder in due_reminders:
        rid = reminder.get("_id")
        owner = str(reminder.get("owner_user_id") or "")
        if not rid or not owner:
            continue
        reminders_collection.update_one(
            {"_id": rid, "status": "pending"},
            {"$set": {"status": "done", "updated_at": now_iso}}
        )
        _create_notification(
            owner,
            str(reminder.get("title") or "Reminder"),
            str(reminder.get("message") or ""),
            category="reminder"
        )
        _write_audit_log(owner, "reminder_fired", "ok", {"reminder_id": str(rid)})

    due_routines = list(
        routines_collection.find(
            # Only timer routines; "" (event-triggered or disabled) must never match <= now.
            {"enabled": True, "next_run_at": {"$ne": "", "$lte": now_iso}}
        ).limit(15)
    )
    for routine in due_routines:
        rid = routine.get("_id")
        owner = str(routine.get("owner_user_id") or "")
        interval = int(routine.get("interval_minutes") or 1440)
        if not rid or not owner:
            continue
        next_run = now + timedelta(minutes=interval)
        routines_collection.update_one(
            {"_id": rid},
            {"$set": {"last_run_at": now_iso, "next_run_at": _as_iso(next_run), "updated_at": now_iso}}
        )
        jobs_collection.insert_one(
            {
                "owner_user_id": owner,
                "job_type": "routine_execution",
                "payload": {
                    "routine_id": str(rid),
                    "name": str(routine.get("name") or "Routine"),
                    "prompt": str(routine.get("prompt") or "")
                },
                "status": "pending",
                "run_at": now_iso,
                "created_at": now_iso,
                "updated_at": now_iso
            }
        )
        _write_audit_log(owner, "routine_enqueued", "ok", {"routine_id": str(rid)})

    due_jobs = list(
        jobs_collection.find(
            # Routine jobs run the full agent loop, so the async _run_routine_jobs_tick owns them.
            {"status": "pending", "run_at": {"$lte": now_iso}, "job_type": {"$ne": "routine_execution"}}
        ).limit(30)
    )
    for job in due_jobs:
        jid = job.get("_id")
        owner = str(job.get("owner_user_id") or "")
        if not jid or not owner:
            continue
        jobs_collection.update_one({"_id": jid, "status": "pending"}, {"$set": {"status": "running", "updated_at": now_iso}})
        payload = job.get("payload", {}) if isinstance(job.get("payload"), dict) else {}
        job_type = str(job.get("job_type") or "")

        if job_type == "notify":
            _create_notification(
                owner,
                str(payload.get("title") or "Background job complete"),
                str(payload.get("body") or ""),
                category="job"
            )
        else:
            _create_notification(owner, "Background job complete", f"Job '{job_type}' finished.", category="job")

        jobs_collection.update_one({"_id": jid}, {"$set": {"status": "done", "updated_at": _utc_now_iso()}})
        _write_audit_log(owner, "job_completed", "ok", {"job_id": str(jid), "job_type": job_type})


async def _run_briefing_tick() -> None:
    """Fire daily briefings for users whose scheduled local time has arrived."""
    now_local = datetime.now().astimezone()
    today = now_local.strftime("%Y-%m-%d")
    current_hhmm = now_local.strftime("%H:%M")

    try:
        due_users = list(
            users_collection.find(
                {"briefing.enabled": True},
                {"_id": 1, "first_name": 1, "briefing": 1},
            ).limit(100)
        )
    except Exception:
        return

    for user in due_users:
        prefs = _briefing_prefs(user)
        if not prefs["enabled"]:
            continue
        if prefs["last_run_date"] == today:
            continue
        if current_hhmm < prefs["time"]:
            continue
        owner_id = str(user.get("_id"))
        try:
            text = await build_briefing(
                city=prefs["city"],
                first_name=str(user.get("first_name") or ""),
                google_creds=_get_google_credentials(owner_id),
            )
            _create_notification(owner_id, "Your daily briefing", text[:3900], category="briefing")
            users_collection.update_one({"_id": user["_id"]}, {"$set": {"briefing.last_run_date": today}})
            _write_audit_log(owner_id, "briefing_auto", "ok", {})
        except Exception as exc:
            logger.warning("briefing tick failed for %s: %s", owner_id, exc)


def _strip_tool_call_text(text: str) -> str:
    return "\n".join(
        ln for ln in str(text or "").splitlines() if not ln.strip().startswith("TOOL_CALL::")
    ).strip()


async def _run_routine(owner_id: str, routine_id: str, prompt: str) -> Tuple[str, bool, str]:
    """Run a routine's prompt through the full agent loop as the routine's owner, using
    their own connected accounts and permission rules. Each run is saved as its own chat
    so the user can read it (and approve a paused step from there).
    Returns (final_text, needs_approval, session_id)."""
    session_id = f"routine_{routine_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    history = _sanitize_history([])
    _add_turn_context(history, owner_id, prompt)
    history.append({"role": "user", "content": prompt})
    _upsert_session_history(session_id, "joi", history, owner_id)

    final_text, needs_approval = "", False
    async for event_name, data in _agent_events(owner_id, session_id, "joi", history, prompt):
        if event_name == "workflow_step_requires_approval":
            needs_approval = True
        elif event_name == "done":
            final_text = str((data or {}).get("message") or "")
        elif event_name == "error":
            raise RuntimeError(str((data or {}).get("message") or "agent error"))
    return _strip_tool_call_text(final_text), needs_approval, session_id


async def _run_routine_jobs_tick() -> None:
    """Execute due routine jobs one at a time (they share the single local model)."""
    jobs = list(
        jobs_collection.find(
            {"status": "pending", "job_type": "routine_execution", "run_at": {"$lte": _utc_now_iso()}}
        ).limit(3)
    )
    for job in jobs:
        jid = job.get("_id")
        owner = str(job.get("owner_user_id") or "")
        claimed = jobs_collection.update_one(
            {"_id": jid, "status": "pending"},
            {"$set": {"status": "running", "updated_at": _utc_now_iso()}},
        )
        if not owner or claimed.modified_count == 0:
            continue
        payload = job.get("payload", {}) if isinstance(job.get("payload"), dict) else {}
        name = str(payload.get("name") or "Routine")
        try:
            text, needs_approval, session_id = await _run_routine(
                owner, str(payload.get("routine_id") or jid), str(payload.get("prompt") or "")
            )
            if needs_approval:
                body = "Paused: one step needs your approval. Open the routine's chat in JOI to approve it."
            else:
                body = text or "Done."
            _create_notification(owner, f"Routine: {name}", body[:3900], category="routine")
            jobs_collection.update_one(
                {"_id": jid},
                {"$set": {"status": "done", "session_id": session_id, "updated_at": _utc_now_iso()}},
            )
            _write_audit_log(owner, "routine_executed", "ok", {"job_id": str(jid), "session_id": session_id})
        except Exception as exc:
            logger.warning("Routine job %s failed: %s", jid, exc)
            _create_notification(owner, f"Routine failed: {name}", "JOI couldn't finish this routine. Try running it again.", category="routine")
            jobs_collection.update_one(
                {"_id": jid}, {"$set": {"status": "failed", "error": str(exc)[:500], "updated_at": _utc_now_iso()}}
            )
            _write_audit_log(owner, "routine_executed", "error", {"job_id": str(jid)})


def _register_undo(owner_id: str, spec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Store an undo action for this user (from a tool's undo marker, see tools/undo.py)."""
    try:
        seconds = max(5, min(int(spec.get("seconds") or 30), 3600))
        now = datetime.now(timezone.utc)
        undo_id = f"undo_{uuid.uuid4().hex[:16]}"
        expires = _as_iso(now + timedelta(seconds=seconds))
        undo_collection.insert_one({
            "undo_id": undo_id,
            "owner_user_id": owner_id,
            "kind": str(spec.get("kind") or ""),
            "label": str(spec.get("label") or "Last action"),
            "payload": spec.get("payload") or {},
            "status": "pending",
            "created_at": _as_iso(now),
            "expires_at": expires,
            # Queued emails are actually sent when the undo window closes.
            "execute_at": expires if spec.get("kind") == "email_send" else "",
        })
        return {"undo_id": undo_id, "label": str(spec.get("label") or ""), "seconds": seconds, "kind": spec.get("kind")}
    except Exception as exc:
        logger.warning("Could not register undo: %s", exc)
        return None


def _gmail_service_for(owner_id: str):
    creds = _get_google_credentials(owner_id)
    if not creds:
        raise RuntimeError("Google is not connected.")
    from googleapiclient.discovery import build as gbuild
    return gbuild("gmail", "v1", credentials=creds, cache_discovery=False), creds


def _perform_undo(doc: Dict[str, Any]) -> str:
    kind, payload, owner = doc.get("kind"), doc.get("payload") or {}, str(doc.get("owner_user_id"))
    if kind == "email_send":
        # The message is still held here (the claim in undo_action stops the send tick).
        return "Email cancelled. It was not sent."
    if kind == "calendar_restore":
        creds = _get_google_credentials(owner)
        if not creds:
            raise RuntimeError("Google is not connected.")
        from googleapiclient.discovery import build as gbuild
        cal = gbuild("calendar", "v3", credentials=creds, cache_discovery=False)
        cal.events().insert(calendarId="primary", body=payload.get("event") or {}).execute()
        return "Event restored to your calendar."
    if kind == "file_restore":
        target = Path(payload.get("path") or "")
        if payload.get("existed") and payload.get("backup") and Path(payload["backup"]).exists():
            import shutil
            shutil.copy2(payload["backup"], target)
            Path(payload["backup"]).unlink(missing_ok=True)
            return "File restored to its previous version."
        if not payload.get("existed") and target.exists():
            target.unlink()
            return "New file removed."
        raise RuntimeError("Nothing to restore.")
    raise RuntimeError("This action can't be undone.")


@app.post("/undo/{undo_id}")
async def undo_action(undo_id: str, current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    # Claim it atomically so the email tick can't send it at the same moment.
    doc = undo_collection.find_one_and_update(
        {"undo_id": undo_id, "owner_user_id": owner_id, "status": "pending", "expires_at": {"$gt": _utc_now_iso()}},
        {"$set": {"status": "undoing"}},
    )
    if not doc:
        raise HTTPException(status_code=410, detail="Too late to undo this one.")
    try:
        message = await asyncio.to_thread(_perform_undo, doc)
    except Exception as exc:
        undo_collection.update_one({"_id": doc["_id"]}, {"$set": {"status": "failed", "error": str(exc)[:300]}})
        raise HTTPException(status_code=500, detail=f"Couldn't undo: {exc}")
    undo_collection.update_one(
        {"_id": doc["_id"]}, {"$set": {"status": "undone", "undone_at": _utc_now_iso()}, "$unset": {"payload.raw": ""}}
    )
    _write_audit_log(owner_id, "undo", "ok", {"kind": doc.get("kind")})
    return {"undone": True, "message": message}


async def _run_undo_tick() -> None:
    """Send queued emails whose undo window has closed; expire other undo records."""
    now_iso = _utc_now_iso()
    for _ in range(10):
        doc = undo_collection.find_one_and_update(
            {"kind": "email_send", "status": "pending", "execute_at": {"$ne": "", "$lte": now_iso}},
            {"$set": {"status": "sending"}},
        )
        if not doc:
            break
        try:
            gmail, _creds = await asyncio.to_thread(_gmail_service_for, str(doc.get("owner_user_id")))
            await asyncio.to_thread(
                lambda: gmail.users().messages().send(userId="me", body={"raw": doc["payload"].get("raw", "")}).execute()
            )
            undo_collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {"status": "executed", "executed_at": _utc_now_iso()}, "$unset": {"payload.raw": ""}},
            )
        except Exception as exc:
            logger.warning("Queued email %s failed to send: %s", doc.get("undo_id"), exc)
            undo_collection.update_one({"_id": doc["_id"]}, {"$set": {"status": "failed", "error": str(exc)[:300]}})
            _create_notification(
                str(doc.get("owner_user_id")), "Email not sent",
                f"{doc.get('label', 'An email')} couldn't be sent: {str(exc)[:160]}", category="email",
            )
    for doc in undo_collection.find({"kind": {"$ne": "email_send"}, "status": "pending", "expires_at": {"$lte": now_iso}}).limit(50):
        backup = (doc.get("payload") or {}).get("backup")
        if backup:
            Path(backup).unlink(missing_ok=True)
        undo_collection.update_one({"_id": doc["_id"]}, {"$set": {"status": "expired"}})


_trigger_last_check: Dict[str, float] = {"battery": 0.0, "google": 0.0}


def _enqueue_routine_job(routine: Dict[str, Any], context: str) -> None:
    now_iso = _utc_now_iso()
    owner = str(routine.get("owner_user_id") or "")
    prompt = str(routine.get("prompt") or "")
    jobs_collection.insert_one({
        "owner_user_id": owner,
        "job_type": "routine_execution",
        "payload": {
            "routine_id": str(routine.get("_id")),
            "name": str(routine.get("name") or "Routine"),
            "prompt": f"{prompt}\n\nWhat just happened: {context}",
        },
        "status": "pending",
        "run_at": now_iso,
        "created_at": now_iso,
        "updated_at": now_iso,
    })
    _write_audit_log(owner, "routine_triggered", "ok", {"routine_id": str(routine.get("_id")), "context": context[:200]})


async def _run_trigger_tick() -> None:
    """Check event triggers: battery every minute, Gmail and Calendar every two minutes."""
    now = time.monotonic()
    check_battery = now - _trigger_last_check["battery"] >= 60
    check_google = now - _trigger_last_check["google"] >= 120
    if not (check_battery or check_google):
        return
    types = (["battery"] if check_battery else []) + (["email", "meeting"] if check_google else [])
    if check_battery:
        _trigger_last_check["battery"] = now
    if check_google:
        _trigger_last_check["google"] = now
    for routine in routines_collection.find({"enabled": True, "trigger.type": {"$in": types}}).limit(50):
        trigger = routine.get("trigger") or {}
        state = dict(routine.get("trigger_state") or {})
        owner = str(routine.get("owner_user_id") or "")
        try:
            if trigger.get("type") == "battery":
                contexts, state = await asyncio.to_thread(routine_triggers.check_battery, trigger, state)
            else:
                creds = await asyncio.to_thread(_get_google_credentials, owner)
                if not creds:
                    continue
                if trigger.get("type") == "email":
                    contexts, state = await asyncio.to_thread(routine_triggers.check_email, trigger, state, creds)
                else:
                    contexts, state = await asyncio.to_thread(
                        routine_triggers.check_meeting, trigger, state, creds, datetime.now(timezone.utc)
                    )
        except Exception as exc:
            logger.warning("Trigger check failed for routine %s: %s", routine.get("_id"), exc)
            continue
        update: Dict[str, Any] = {"trigger_state": state}
        if contexts:
            update["last_run_at"] = _utc_now_iso()
        routines_collection.update_one({"_id": routine["_id"]}, {"$set": update})
        for context in contexts:
            _enqueue_routine_job(routine, context)


async def _background_scheduler_loop() -> None:
    while not background_scheduler_stop.is_set():
        try:
            _run_scheduler_tick()
        except Exception as exc:
            logger.warning("Background scheduler tick failed: %s", exc)
        try:
            await _run_briefing_tick()
        except Exception as exc:
            logger.warning("Briefing tick failed: %s", exc)
        try:
            await _run_undo_tick()
        except Exception as exc:
            logger.warning("Undo tick failed: %s", exc)
        try:
            await _run_trigger_tick()
        except Exception as exc:
            logger.warning("Trigger tick failed: %s", exc)
        try:
            await _run_routine_jobs_tick()
        except Exception as exc:
            logger.warning("Routine jobs tick failed: %s", exc)
        await asyncio.sleep(10)


def _fallback_title(text: str) -> str:
    cleaned = re.sub(r"[^\w\s]", " ", str(text or "").lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return "New chat"

    ignore = {
        "hey", "hi", "hello", "joi", "please", "basically", "actually", "just",
        "can", "could", "would", "you", "help", "with", "what", "how", "the",
        "a", "an", "to", "for", "me", "i", "my", "on", "in", "of"
    }
    words = [w for w in cleaned.split(" ") if w and w not in ignore]
    if not words:
        words = cleaned.split(" ")

    title = " ".join(words[:6]).strip()
    if not title:
        return "New chat"
    return title[:1].upper() + title[1:40]


JOI_NOTE_PREFIX = "<<JOI_NOTE>>"

# Requests about the user's own accounts or computer. Answering these without a tool
# result means the model invented the data (it once listed fake "John Doe" emails).
_PERSONAL_DATA_RE = re.compile(
    r"\b(e-?mails?|mails?|inbox|gmail|unread|calendar|meetings?|events?|schedule|"
    r"files?|folders?|drive|docs?|sheets?|slack|channels?|contacts?|whatsapp|"
    r"cpu|ram|memory|battery|disk|storage|windows?)\b",
    re.IGNORECASE,
)


_EMAIL_ADDR_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")


_DRAFT_INTENT_RE = re.compile(r"\b(draft|compose|write|prepare)\b", re.IGNORECASE)
_SEND_INTENT_RE = re.compile(r"\bsend\b|\bmail it\b|\bemail it\b", re.IGNORECASE)


def _latest_user_text(history: List[Dict[str, str]]) -> str:
    for m in reversed(history):
        content = str(m.get("content", ""))
        if m.get("role") == "user" and not content.lstrip().startswith(("<<TOOL_RESULT", JOI_NOTE_PREFIX)):
            return content
    return ""


def _unverified_recipient_error(tool_name: str, args: Dict[str, Any], history: List[Dict[str, str]]) -> str:
    """Block email sends the user didn't ask for: a "draft" request, or recipients the model
    made up. A recipient must appear somewhere in the conversation (typed by the user or
    returned by a tool such as contacts_search)."""
    if tool_name != "email":
        return ""
    match = re.search(r"\bto:\s*([^;]*)", str(args.get("command_string", "")), re.IGNORECASE)
    if not match:
        return ""
    asked = _latest_user_text(history)
    if _DRAFT_INTENT_RE.search(asked) and not _SEND_INTENT_RE.search(asked):
        return (
            "Error: not sent. The user asked for a draft, not for sending. Show the draft in the chat "
            "and ask whether they want it sent."
        )
    known = " ".join(
        str(m.get("content", "")) for m in history
        if m.get("role") == "user" and "Error: not sent." not in str(m.get("content", ""))
    ).lower()
    recipients = [r.strip() for r in re.split(r"[,\s]+", match.group(1)) if r.strip()]
    # Every recipient must be a real address the user (or a lookup tool) supplied;
    # placeholders like "me" or "boss" are rejected too, not just invented addresses.
    unknown = [r for r in recipients if not _EMAIL_ADDR_RE.fullmatch(r) or r.lower() not in known]
    if not recipients:
        unknown = ["(none)"]
    if not unknown:
        return ""
    return (
        f"Error: not sent. The recipient {', '.join(unknown)} is not an email address the user gave. "
        "Do not guess addresses. Show the draft to the user in the chat and ask who it should go to "
        "(or look the person up with contacts_search)."
    )


def _drop_repeated_question(history: List[Dict[str, str]], message: str) -> List[Dict[str, str]]:
    """When the user asks (nearly) the same thing again, they want a fresh answer: drop the
    earlier copy and the reply that followed, or a small model just repeats that old reply
    (it once kept saying "Google isn't connected" after the user had connected it)."""
    words = lambda s: " ".join(re.sub(r"[^\w\s]", " ", str(s).lower()).split())
    target = words(message)
    if not target:
        return history
    out: List[Dict[str, str]] = []
    skipping = False
    for m in history:
        if m.get("role") == "user":
            skipping = SequenceMatcher(None, words(m.get("content", "")), target).ratio() >= 0.85
            if skipping:
                continue
        elif m.get("role") == "assistant" and skipping:
            continue
        out.append(m)
    return out


def _joi_note(text: str) -> Dict[str, str]:
    """An agent-loop instruction to the model, hidden from the visible transcript."""
    return {"role": "user", "content": f"{JOI_NOTE_PREFIX} {text}"}


def _extract_visible_messages(history: List[Dict[str, str]]) -> List[Dict[str, str]]:
    visible = []
    for msg in history:
        role = str(msg.get("role", "")).strip().lower()
        content = str(msg.get("content", ""))
        # Tool results and agent-loop corrections are internal model context,
        # not something the user wrote.
        if content.lstrip().startswith(("<<TOOL_RESULT", JOI_NOTE_PREFIX)):
            continue
        if role in {"user", "assistant"} and content.strip():
            visible.append({"role": role, "content": content})
    return visible


def _session_title_from_messages(messages: List[Dict[str, str]]) -> str:
    first_user = next((m["content"] for m in messages if m.get("role") == "user"), "")
    return _fallback_title(first_user)


def _session_preview_from_messages(messages: List[Dict[str, str]]) -> str:
    for msg in reversed(messages):
        if msg.get("role") == "user" and msg.get("content"):
            return str(msg["content"])[:120]
    return ""


def _sessions_list_cache_key(user_id: str) -> str:
    return f"{SESSIONS_LIST_CACHE_PREFIX}{user_id}"


def _session_cache_key(user_id: str, session_id: str) -> str:
    return f"{SESSION_CACHE_PREFIX}{user_id}:{session_id}"


def _cache_get_json(cache_key: str) -> Optional[Dict[str, Any]]:
    try:
        cached = redis_client.get(cache_key)
        if not cached:
            return None
        parsed = json.loads(cached)
        if isinstance(parsed, dict):
            return parsed
    except (RedisError, json.JSONDecodeError) as exc:
        logger.warning("Redis read failed for key '%s': %s", cache_key, exc)
    return None


def _cache_set_json(cache_key: str, payload: Dict[str, Any], ttl_seconds: int) -> None:
    try:
        redis_client.setex(
            cache_key,
            max(1, int(ttl_seconds)),
            json.dumps(payload, ensure_ascii=False)
        )
    except (RedisError, TypeError, ValueError) as exc:
        logger.warning("Redis write failed for key '%s': %s", cache_key, exc)


def _invalidate_session_cache(user_id: str, session_id: str) -> None:
    keys = [_sessions_list_cache_key(user_id), _session_cache_key(user_id, session_id)]
    try:
        redis_client.delete(*keys)
    except RedisError as exc:
        logger.warning("Redis delete failed for session '%s': %s", session_id, exc)


def _upsert_session_history(
    session_id: str,
    model: str,
    history: List[Dict[str, str]],
    owner_user_id: str
) -> None:
    sid = str(session_id).strip()
    owner_id = str(owner_user_id).strip()
    if not sid or not owner_id:
        return

    now = _utc_now_iso()
    visible_messages = _extract_visible_messages(history)
    preview = _session_preview_from_messages(visible_messages)

    try:
        existing = sessions_collection.find_one(
            {"session_id": sid},
            {"_id": 0, "title": 1, "owner_user_id": 1}
        )
        existing_owner = str((existing or {}).get("owner_user_id") or "").strip()
        if existing_owner and existing_owner != owner_id:
            logger.warning(
                "Session ownership mismatch for session '%s'. owner=%s attempted_by=%s",
                sid,
                existing_owner,
                owner_id
            )
            return

        existing_title = str((existing or {}).get("title") or "").strip()
        title = (
            existing_title
            if existing_title and existing_title != "New chat"
            else _session_title_from_messages(visible_messages)
        )

        sessions_collection.update_one(
            {"session_id": sid},
            {
                "$set": {
                    "session_id": sid,
                    "owner_user_id": owner_id,
                    "model": model or "openai",
                    "updated_at": now,
                    "title": title,
                    "messages": visible_messages,
                    "message_count": len(visible_messages),
                    "preview": preview
                },
                "$setOnInsert": {
                    "created_at": now
                }
            },
            upsert=True
        )
    except PyMongoError as exc:
        logger.exception("Mongo upsert failed for session '%s': %s", sid, exc)
        raise RuntimeError(f"Session persistence failed for '{sid}'") from exc

    _invalidate_session_cache(owner_id, sid)


def _session_summary(record: Dict[str, Any]) -> Dict[str, Any]:
    messages = record.get("messages", [])
    preview = str(record.get("preview") or "")
    if not preview:
        preview = _session_preview_from_messages(messages)

    return {
        "session_id": record.get("session_id"),
        "title": record.get("title", "New chat"),
        "model": record.get("model", "openai"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "message_count": int(record.get("message_count", len(messages))),
        "preview": preview
    }


@app.get("/auth/config")
async def auth_config():
    return {
        "google_enabled": bool(GOOGLE_OAUTH_CLIENT_ID),
        "google_client_id": GOOGLE_OAUTH_CLIENT_ID
    }


@app.post("/auth/signup")
async def auth_signup(payload: AuthSignupRequest):
    email_normalized = _validate_email_or_400(payload.email)
    first_name = _normalize_name(payload.first_name)
    last_name = _normalize_name(payload.last_name)
    password = str(payload.password)
    confirm_password = str(payload.confirm_password)

    if not first_name or not last_name:
        raise HTTPException(status_code=400, detail="First name and last name are required.")
    if password != confirm_password:
        raise HTTPException(status_code=400, detail="Password and confirm password do not match.")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters.")

    now = _utc_now_iso()
    try:
        existing_user = users_collection.find_one({"email_normalized": email_normalized})
        if existing_user and existing_user.get("password_hash"):
            raise HTTPException(status_code=409, detail="Account already exists. Please sign in.")

        if existing_user:
            users_collection.update_one(
                {"_id": existing_user["_id"]},
                {
                    "$set": {
                        "email": email_normalized,
                        "email_normalized": email_normalized,
                        "first_name": first_name,
                        "last_name": last_name,
                        "display_name": _display_name(first_name, last_name, email_normalized),
                        "password_hash": _hash_password(password),
                        "is_email_verified": True,
                        "updated_at": now
                    }
                }
            )
            user_doc = users_collection.find_one({"_id": existing_user["_id"]})
        else:
            user_doc = {
                "email": email_normalized,
                "email_normalized": email_normalized,
                "first_name": first_name,
                "last_name": last_name,
                "display_name": _display_name(first_name, last_name, email_normalized),
                "password_hash": _hash_password(password),
                "picture_url": "",
                "is_email_verified": True,
                "created_at": now,
                "updated_at": now,
                "last_login_at": now
            }
            insert_result = users_collection.insert_one(user_doc)
            user_doc["_id"] = insert_result.inserted_id

        user_id = user_doc["_id"]
        _upsert_auth_identity(user_id, "email", email_normalized, email_normalized)
        _touch_user_login(user_id)
        fresh_user = users_collection.find_one({"_id": user_id}) or user_doc
        return _issue_auth_payload(fresh_user)
    except HTTPException:
        raise
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="Account already exists. Please sign in.")
    except PyMongoError as exc:
        logger.exception("Signup failed for email '%s': %s", email_normalized, exc)
        raise HTTPException(status_code=500, detail="Failed to create account.") from exc


@app.post("/auth/login")
async def auth_login(payload: AuthLoginRequest):
    email_normalized = _validate_email_or_400(payload.email)
    password = str(payload.password or "")
    if not password:
        raise HTTPException(status_code=400, detail="Password is required.")

    try:
        user_doc = users_collection.find_one({"email_normalized": email_normalized})
    except PyMongoError as exc:
        logger.exception("Login lookup failed for email '%s': %s", email_normalized, exc)
        raise HTTPException(status_code=500, detail="Failed to sign in.") from exc

    if not user_doc:
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    password_hash = str(user_doc.get("password_hash") or "")
    if not password_hash:
        raise HTTPException(
            status_code=400,
            detail="This account uses Google sign-in. Continue with Google or create a password."
        )
    if not _verify_password(password, password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    user_id = user_doc["_id"]
    _upsert_auth_identity(user_id, "email", email_normalized, email_normalized)
    _touch_user_login(user_id)
    fresh_user = users_collection.find_one({"_id": user_id}) or user_doc
    return _issue_auth_payload(fresh_user)


@app.post("/auth/google")
async def auth_google(payload: GoogleAuthRequest):
    claims = _verify_google_id_credential(payload.credential)
    return _issue_auth_payload_from_google_claims(claims)


@app.post("/auth/google/desktop")
async def auth_google_desktop(payload: GoogleDesktopAuthRequest):
    credential = await _exchange_google_auth_code_for_credential(
        payload.code,
        payload.code_verifier,
        payload.redirect_uri
    )
    claims = _verify_google_id_credential(credential)
    return _issue_auth_payload_from_google_claims(claims)


@app.get("/auth/me")
async def auth_me(current_user: Dict[str, Any] = Depends(require_auth_user)):
    return {"user": _serialize_user(current_user)}


@app.get("/memory")
async def get_memory(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    memory = _read_user_memory(owner_id)
    return {"memory": memory}


@app.put("/memory")
async def upsert_memory(payload: MemoryUpsertRequest, current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    now = _utc_now_iso()
    memory_collection.update_one(
        {"owner_user_id": owner_id},
        {
            "$set": {
                "owner_user_id": owner_id,
                "preferences": str(payload.preferences or "")[:8000],
                "notes": str(payload.notes or "")[:16000],
                "updated_at": now
            },
            "$setOnInsert": {"created_at": now}
        },
        upsert=True
    )
    _write_audit_log(owner_id, "memory_upsert", "ok", {})
    return {"saved": True, "memory": _read_user_memory(owner_id)}


async def _learn_from_message(owner_id: str, message: str) -> None:
    try:
        facts = await memory_learning.extract_facts(system_prompt, message)
        if not facts:
            return
        memory = _read_user_memory(owner_id)
        pending = [
            str(d.get("text") or "")
            for d in memory_suggestions_collection.find({"owner_user_id": owner_id, "status": "pending"}, {"text": 1})
        ]
        fresh = memory_learning.only_new(facts, memory["preferences"] + "\n" + memory["notes"], pending)
        now = _utc_now_iso()
        for fact in fresh:
            memory_suggestions_collection.insert_one({
                "suggestion_id": f"mem_{uuid.uuid4().hex[:12]}",
                "owner_user_id": owner_id,
                "text": fact,
                "status": "pending",
                "created_at": now,
            })
        if fresh:
            logger.info("Memory: %d new suggestion(s) for %s", len(fresh), owner_id)
    except Exception as exc:
        logger.warning("Memory suggestion failed: %s", exc)


@app.get("/memory/suggestions")
async def list_memory_suggestions(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    rows = list(
        memory_suggestions_collection.find({"owner_user_id": owner_id, "status": "pending"}, {"_id": 0})
        .sort("created_at", DESCENDING).limit(20)
    )
    return {"count": len(rows), "suggestions": rows}


@app.post("/memory/suggestions/{suggestion_id}/{decision}")
async def decide_memory_suggestion(
    suggestion_id: str, decision: str, current_user: Dict[str, Any] = Depends(require_auth_user)
):
    owner_id = str(current_user.get("_id"))
    if decision not in {"accept", "dismiss"}:
        raise HTTPException(status_code=400, detail="Decision must be accept or dismiss.")
    doc = memory_suggestions_collection.find_one(
        {"owner_user_id": owner_id, "suggestion_id": suggestion_id, "status": "pending"}
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Suggestion not found.")
    now = _utc_now_iso()
    if decision == "accept":
        memory = _read_user_memory(owner_id)
        notes = (memory["notes"].rstrip() + "\n- " + str(doc.get("text") or "")).strip()[:16000]
        memory_collection.update_one(
            {"owner_user_id": owner_id},
            {"$set": {"owner_user_id": owner_id, "notes": notes, "updated_at": now}, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
    memory_suggestions_collection.update_one(
        {"_id": doc["_id"]}, {"$set": {"status": "accepted" if decision == "accept" else "dismissed", "decided_at": now}}
    )
    _write_audit_log(owner_id, f"memory_suggestion_{decision}", "ok", {})
    return {"saved": True, "memory": _read_user_memory(owner_id)}


@app.delete("/memory")
async def clear_memory(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    memory_collection.delete_one({"owner_user_id": owner_id})
    _write_audit_log(owner_id, "memory_delete", "ok", {})
    return {"deleted": True}


@app.get("/permissions")
async def get_permissions(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    return {"rules": _get_permission_rules(owner_id)}


@app.put("/permissions")
async def upsert_permission_rule(
    payload: PermissionRuleUpsertRequest,
    current_user: Dict[str, Any] = Depends(require_auth_user)
):
    owner_id = str(current_user.get("_id"))
    now = _utc_now_iso()
    tool_name = str(payload.tool_name).strip()
    permissions_collection.update_one(
        {"owner_user_id": owner_id},
        {
            "$set": {
                f"rules.{tool_name}": payload.mode,
                "owner_user_id": owner_id,
                "updated_at": now
            },
            "$setOnInsert": {"created_at": now}
        },
        upsert=True
    )
    _write_audit_log(owner_id, "permission_upsert", "ok", {"tool_name": tool_name, "mode": payload.mode})
    return {"saved": True, "rules": _get_permission_rules(owner_id)}


@app.get("/approvals/pending")
async def list_pending_approvals(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    rows = list(
        approvals_collection.find(
            {"owner_user_id": owner_id, "status": "pending"},
            {"_id": 0}
        ).sort("created_at", DESCENDING).limit(50)
    )
    return {"count": len(rows), "approvals": rows}


@app.post("/approvals/{approval_id}/decision")
async def decide_approval(
    approval_id: str,
    payload: ApprovalDecisionRequest,
    current_user: Dict[str, Any] = Depends(require_auth_user)
):
    owner_id = str(current_user.get("_id"))
    now = _utc_now_iso()
    result = approvals_collection.update_one(
        {
            "owner_user_id": owner_id,
            "approval_id": str(approval_id).strip(),
            "status": "pending"
        },
        {
            "$set": {
                "status": payload.decision,
                "decision_note": str(payload.note or "")[:1000],
                "updated_at": now
            }
        }
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Pending approval not found.")

    _write_audit_log(owner_id, "approval_decision", "ok", {"approval_id": approval_id, "decision": payload.decision})
    return {"saved": True, "approval_id": approval_id, "decision": payload.decision}


@app.get("/routines")
async def list_routines(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    rows = list(
        routines_collection.find({"owner_user_id": owner_id}).sort("created_at", DESCENDING)
    )
    return {"count": len(rows), "routines": [_serialize_mongo_doc(r) for r in rows]}


@app.post("/routines")
async def create_routine(payload: RoutineCreateRequest, current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    error = routine_triggers.validate(payload.trigger_type, payload.trigger_value)
    if error:
        raise HTTPException(status_code=400, detail=error)
    now = datetime.now(timezone.utc)
    next_run = now + timedelta(minutes=int(payload.interval_minutes))
    on_schedule = payload.trigger_type == "schedule"
    doc = {
        "owner_user_id": owner_id,
        "name": str(payload.name).strip(),
        "prompt": str(payload.prompt).strip(),
        "interval_minutes": int(payload.interval_minutes),
        "enabled": bool(payload.enabled),
        "trigger": {"type": payload.trigger_type, "value": payload.trigger_value.strip()},
        "trigger_state": {},
        "created_at": _as_iso(now),
        "updated_at": _as_iso(now),
        "last_run_at": "",
        # Event-triggered routines never run on the timer.
        "next_run_at": _as_iso(next_run) if payload.enabled and on_schedule else ""
    }
    inserted = routines_collection.insert_one(doc)
    _write_audit_log(owner_id, "routine_create", "ok", {"routine_id": str(inserted.inserted_id)})
    return {"created": True, "routine": _serialize_mongo_doc({**doc, "_id": inserted.inserted_id})}


@app.patch("/routines/{routine_id}")
async def update_routine(
    routine_id: str,
    payload: RoutineUpdateRequest,
    current_user: Dict[str, Any] = Depends(require_auth_user)
):
    owner_id = str(current_user.get("_id"))
    rid = _doc_id_or_404(routine_id)
    existing = routines_collection.find_one({"_id": rid, "owner_user_id": owner_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Routine not found.")

    update_fields: Dict[str, Any] = {"updated_at": _utc_now_iso()}
    if payload.name is not None:
        update_fields["name"] = str(payload.name).strip()
    if payload.prompt is not None:
        update_fields["prompt"] = str(payload.prompt).strip()
    if payload.interval_minutes is not None:
        update_fields["interval_minutes"] = int(payload.interval_minutes)
    trigger = dict(existing.get("trigger") or {"type": "schedule", "value": ""})
    if payload.trigger_type is not None:
        value = payload.trigger_value if payload.trigger_value is not None else ""
        error = routine_triggers.validate(payload.trigger_type, value)
        if error:
            raise HTTPException(status_code=400, detail=error)
        trigger = {"type": payload.trigger_type, "value": value.strip()}
        update_fields["trigger"] = trigger
        update_fields["trigger_state"] = {}
    enabled = bool(payload.enabled) if payload.enabled is not None else bool(existing.get("enabled"))
    if payload.enabled is not None:
        update_fields["enabled"] = enabled
    if trigger.get("type", "schedule") == "schedule" and enabled:
        interval = int(update_fields.get("interval_minutes") or existing.get("interval_minutes") or 1440)
        if payload.enabled is not None or payload.trigger_type is not None or payload.interval_minutes is not None:
            update_fields["next_run_at"] = _as_iso(datetime.now(timezone.utc) + timedelta(minutes=interval))
    else:
        update_fields["next_run_at"] = ""

    routines_collection.update_one({"_id": rid, "owner_user_id": owner_id}, {"$set": update_fields})
    updated = routines_collection.find_one({"_id": rid, "owner_user_id": owner_id})
    _write_audit_log(owner_id, "routine_update", "ok", {"routine_id": routine_id})
    return {"updated": True, "routine": _serialize_mongo_doc(updated)}


@app.delete("/routines/{routine_id}")
async def delete_routine(routine_id: str, current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    rid = _doc_id_or_404(routine_id)
    result = routines_collection.delete_one({"_id": rid, "owner_user_id": owner_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Routine not found.")
    _write_audit_log(owner_id, "routine_delete", "ok", {"routine_id": routine_id})
    return {"deleted": True}


@app.post("/routines/{routine_id}/run")
async def run_routine_now(routine_id: str, current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    rid = _doc_id_or_404(routine_id)
    routine = routines_collection.find_one({"_id": rid, "owner_user_id": owner_id})
    if not routine:
        raise HTTPException(status_code=404, detail="Routine not found.")

    now = _utc_now_iso()
    inserted = jobs_collection.insert_one(
        {
            "owner_user_id": owner_id,
            "job_type": "routine_execution",
            "payload": {
                "routine_id": routine_id,
                "name": str(routine.get("name") or "Routine"),
                "prompt": str(routine.get("prompt") or "")
            },
            "status": "pending",
            "run_at": now,
            "created_at": now,
            "updated_at": now
        }
    )
    _write_audit_log(owner_id, "routine_run_now", "ok", {"routine_id": routine_id, "job_id": str(inserted.inserted_id)})
    return {"queued": True, "job_id": str(inserted.inserted_id)}


@app.get("/reminders")
async def list_reminders(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    rows = list(reminders_collection.find({"owner_user_id": owner_id}).sort("due_at", DESCENDING).limit(300))
    return {"count": len(rows), "reminders": [_serialize_mongo_doc(r) for r in rows]}


@app.post("/reminders")
async def create_reminder(payload: ReminderCreateRequest, current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    try:
        due_at = _as_iso(_parse_iso_datetime(payload.due_at))
    except ValueError:
        raise HTTPException(status_code=400, detail="due_at must be valid ISO datetime.")

    now = _utc_now_iso()
    doc = {
        "owner_user_id": owner_id,
        "title": str(payload.title).strip(),
        "message": str(payload.message or "").strip(),
        "due_at": due_at,
        "status": "pending",
        "created_at": now,
        "updated_at": now
    }
    inserted = reminders_collection.insert_one(doc)
    _write_audit_log(owner_id, "reminder_create", "ok", {"reminder_id": str(inserted.inserted_id)})
    return {"created": True, "reminder": _serialize_mongo_doc({**doc, "_id": inserted.inserted_id})}


@app.patch("/reminders/{reminder_id}")
async def update_reminder(
    reminder_id: str,
    payload: ReminderUpdateRequest,
    current_user: Dict[str, Any] = Depends(require_auth_user)
):
    owner_id = str(current_user.get("_id"))
    rid = _doc_id_or_404(reminder_id)
    existing = reminders_collection.find_one({"_id": rid, "owner_user_id": owner_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Reminder not found.")

    update_fields: Dict[str, Any] = {"updated_at": _utc_now_iso()}
    if payload.title is not None:
        update_fields["title"] = str(payload.title).strip()
    if payload.message is not None:
        update_fields["message"] = str(payload.message).strip()
    if payload.due_at is not None:
        try:
            update_fields["due_at"] = _as_iso(_parse_iso_datetime(payload.due_at))
        except ValueError:
            raise HTTPException(status_code=400, detail="due_at must be valid ISO datetime.")
    if payload.status is not None:
        update_fields["status"] = payload.status

    reminders_collection.update_one({"_id": rid, "owner_user_id": owner_id}, {"$set": update_fields})
    updated = reminders_collection.find_one({"_id": rid, "owner_user_id": owner_id})
    _write_audit_log(owner_id, "reminder_update", "ok", {"reminder_id": reminder_id})
    return {"updated": True, "reminder": _serialize_mongo_doc(updated)}


@app.delete("/reminders/{reminder_id}")
async def delete_reminder(reminder_id: str, current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    rid = _doc_id_or_404(reminder_id)
    result = reminders_collection.delete_one({"_id": rid, "owner_user_id": owner_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Reminder not found.")
    _write_audit_log(owner_id, "reminder_delete", "ok", {"reminder_id": reminder_id})
    return {"deleted": True}


@app.post("/jobs")
async def create_job(payload: JobCreateRequest, current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    if payload.run_at:
        try:
            run_at = _as_iso(_parse_iso_datetime(payload.run_at))
        except ValueError:
            raise HTTPException(status_code=400, detail="run_at must be valid ISO datetime.")
    else:
        run_at = _utc_now_iso()

    now = _utc_now_iso()
    doc = {
        "owner_user_id": owner_id,
        "job_type": str(payload.job_type).strip(),
        "payload": payload.payload or {},
        "status": "pending",
        "run_at": run_at,
        "created_at": now,
        "updated_at": now
    }
    inserted = jobs_collection.insert_one(doc)
    _write_audit_log(owner_id, "job_create", "ok", {"job_id": str(inserted.inserted_id), "job_type": payload.job_type})
    return {"queued": True, "job": _serialize_mongo_doc({**doc, "_id": inserted.inserted_id})}


@app.get("/jobs")
async def list_jobs(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    rows = list(jobs_collection.find({"owner_user_id": owner_id}).sort("created_at", DESCENDING).limit(300))
    return {"count": len(rows), "jobs": [_serialize_mongo_doc(r) for r in rows]}


@app.get("/notifications")
async def list_notifications(
    unread_only: bool = False,
    current_user: Dict[str, Any] = Depends(require_auth_user)
):
    owner_id = str(current_user.get("_id"))
    query = {"owner_user_id": owner_id}
    if unread_only:
        query["read"] = False
    rows = list(notifications_collection.find(query).sort("created_at", DESCENDING).limit(200))
    return {"count": len(rows), "notifications": [_serialize_mongo_doc(n) for n in rows]}


@app.post("/notifications/{notification_id}/read")
async def mark_notification_read(notification_id: str, current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    nid = _doc_id_or_404(notification_id)
    result = notifications_collection.update_one(
        {"_id": nid, "owner_user_id": owner_id},
        {"$set": {"read": True}}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Notification not found.")
    return {"saved": True}


@app.get("/audit")
async def list_audit_logs(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    rows = list(audit_collection.find({"owner_user_id": owner_id}).sort("created_at", DESCENDING).limit(500))
    return {"count": len(rows), "logs": [_serialize_mongo_doc(r) for r in rows]}


@app.post("/rag/index")
async def rag_index(payload: RagIndexRequest, current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    root = Path(str(payload.root_path)).expanduser()
    if not _is_path_allowed_for_rag(root):
        raise HTTPException(status_code=403, detail="RAG indexing is restricted to your home directory.")
    if not root.exists():
        raise HTTPException(status_code=404, detail="Path does not exist.")

    file_paths: List[Path] = []
    if root.is_file():
        file_paths = [root]
    else:
        for candidate in root.rglob("*"):
            if candidate.is_file() and candidate.suffix.lower() in RAG_ALLOWED_SUFFIXES:
                file_paths.append(candidate)
                if len(file_paths) >= int(payload.max_files):
                    break

    root_resolved = str(root.resolve())
    rag_chunks_collection.delete_many({"owner_user_id": owner_id, "root_path": root_resolved})

    now = _utc_now_iso()
    to_insert: List[Dict[str, Any]] = []
    indexed_files = 0
    skipped_files = 0

    for fpath in file_paths:
        try:
            blob = fpath.read_bytes()
            blob = blob[: int(payload.max_bytes_per_file)]
            text = blob.decode("utf-8", errors="ignore")
            chunks = _chunk_text(text)
            if not chunks:
                skipped_files += 1
                continue

            indexed_files += 1
            resolved_file = str(fpath.resolve())
            for idx, chunk in enumerate(chunks[:80]):
                to_insert.append(
                    {
                        "owner_user_id": owner_id,
                        "root_path": root_resolved,
                        "file_path": resolved_file,
                        "chunk_index": idx,
                        "text": chunk,
                        "created_at": now,
                        "updated_at": now
                    }
                )
        except Exception:
            skipped_files += 1

    if to_insert:
        rag_chunks_collection.insert_many(to_insert, ordered=False)

    _write_audit_log(
        owner_id,
        "rag_index",
        "ok",
        {"root_path": root_resolved, "indexed_files": indexed_files, "skipped_files": skipped_files, "chunks": len(to_insert)}
    )
    return {
        "indexed_files": indexed_files,
        "skipped_files": skipped_files,
        "chunk_count": len(to_insert),
        "root_path": root_resolved
    }


@app.post("/rag/query")
async def rag_query(payload: RagQueryRequest, current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    query_text = str(payload.query or "").strip()
    if not query_text:
        raise HTTPException(status_code=400, detail="query is required.")

    rows = _rag_search(owner_id, query_text, top_k=int(payload.top_k))

    results = []
    for row in rows:
        results.append(
            {
                "file_path": str(row.get("file_path") or ""),
                "chunk_index": int(row.get("chunk_index") or 0),
                "score": float(row.get("score") or 0.0),
                "snippet": str(row.get("text") or "")[:1200]
            }
        )
    return {"count": len(results), "results": results}


@app.get("/assistant/context")
async def assistant_context_snapshot(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    pending_approvals = approvals_collection.count_documents({"owner_user_id": owner_id, "status": "pending"})
    unread_notifications = notifications_collection.count_documents({"owner_user_id": owner_id, "read": False})
    routines_enabled = routines_collection.count_documents({"owner_user_id": owner_id, "enabled": True})
    return {
        "pending_approvals": int(pending_approvals),
        "unread_notifications": int(unread_notifications),
        "enabled_routines": int(routines_enabled),
        "memory": _read_user_memory(owner_id)
    }


def _briefing_prefs(user_doc: Dict[str, Any]) -> Dict[str, Any]:
    raw = user_doc.get("briefing") if isinstance(user_doc.get("briefing"), dict) else {}
    return {
        "enabled": bool(raw.get("enabled", False)),
        "time": str(raw.get("time") or "08:00"),
        "city": str(raw.get("city") or ""),
        "last_run_date": str(raw.get("last_run_date") or ""),
    }


class BriefingPrefsRequest(BaseModel):
    enabled: Optional[bool] = None
    time: Optional[str] = Field(default=None, pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    city: Optional[str] = Field(default=None, max_length=120)


class WakeWordRequest(BaseModel):
    enabled: bool


_main_loop: Optional[asyncio.AbstractEventLoop] = None


def _on_wake(owner_id: str) -> None:
    # Called from the listener thread: hand the push to the server's event loop.
    if _main_loop:
        asyncio.run_coroutine_threadsafe(notification_hub.broadcast(owner_id, {"type": "wake"}), _main_loop)


def _wake_state(owner_id: str) -> Dict[str, Any]:
    mine = wake_word.listener.owner_id == str(owner_id)
    return {
        "enabled": bool((users_collection.find_one({"_id": owner_id}, {"wake_word": 1}) or {}).get("wake_word", {}).get("enabled")),
        "status": wake_word.listener.status if mine else "off",
        "error": wake_word.listener.error if mine else "",
        "model_ready": wake_word.model_ready(),
    }


@app.get("/wake-word")
async def get_wake_word(current_user: Dict[str, Any] = Depends(require_auth_user)):
    return _wake_state(current_user["_id"])


@app.put("/wake-word")
async def set_wake_word(payload: WakeWordRequest, current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = current_user["_id"]
    users_collection.update_one(
        {"_id": owner_id}, {"$set": {"wake_word": {"enabled": payload.enabled, "updated_at": _utc_now_iso()}}}
    )
    if payload.enabled:
        # One microphone per machine: the person who turned it on most recently gets it.
        users_collection.update_many({"_id": {"$ne": owner_id}, "wake_word.enabled": True}, {"$set": {"wake_word.enabled": False}})
        wake_word.listener.start(str(owner_id), _on_wake)
    elif wake_word.listener.owner_id == str(owner_id):
        await asyncio.to_thread(wake_word.listener.stop)
    return _wake_state(owner_id)


@app.get("/briefing")
async def get_briefing_prefs(current_user: Dict[str, Any] = Depends(require_auth_user)):
    return {"briefing": _briefing_prefs(current_user)}


@app.put("/briefing")
async def set_briefing_prefs(
    payload: BriefingPrefsRequest,
    current_user: Dict[str, Any] = Depends(require_auth_user),
):
    owner_id = current_user["_id"]
    current = _briefing_prefs(current_user)
    update = {}
    if payload.enabled is not None:
        update["briefing.enabled"] = bool(payload.enabled)
    if payload.time is not None:
        update["briefing.time"] = payload.time
    if payload.city is not None:
        update["briefing.city"] = payload.city.strip()
    if update:
        users_collection.update_one({"_id": owner_id}, {"$set": update})
    fresh = users_collection.find_one({"_id": owner_id}) or current_user
    return {"briefing": _briefing_prefs(fresh)}


@app.post("/briefing/run")
async def run_briefing_now(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    prefs = _briefing_prefs(current_user)
    text = await build_briefing(
        city=prefs.get("city", ""),
        first_name=str(current_user.get("first_name") or ""),
        google_creds=_get_google_credentials(owner_id),
    )
    _create_notification(owner_id, "Your daily briefing", text[:3900], category="briefing")
    _write_audit_log(owner_id, "briefing_run", "ok", {})
    return {"briefing": text}


def _sign_oauth_state(user_id: str, provider: str) -> str:
    now_ts = _unix_ts()
    return _jwt_encode({"sub": str(user_id), "provider": provider, "kind": "oauth_state",
                        "iat": now_ts, "exp": now_ts + 600})


def _verify_oauth_state(state: str, provider: str) -> str:
    payload = _jwt_decode(str(state or ""))
    if payload.get("kind") != "oauth_state" or payload.get("provider") != provider:
        raise HTTPException(status_code=400, detail="Invalid OAuth state.")
    return str(payload.get("sub") or "")


def _get_slack_token(owner_id: str) -> str:
    try:
        doc = integrations_collection.find_one({"owner_user_id": owner_id, "provider": "slack"})
        return str((doc or {}).get("access_token") or "")
    except Exception:
        return ""


@app.get("/integrations/slack/status")
async def slack_status(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    doc = integrations_collection.find_one({"owner_user_id": owner_id, "provider": "slack"}) or {}
    return {
        "configured": SLACK_CONFIGURED,
        "connected": bool(doc.get("access_token")),
        "team": str(doc.get("team_name") or ""),
    }


@app.get("/integrations/slack/connect")
async def slack_connect(current_user: Dict[str, Any] = Depends(require_auth_user)):
    if not SLACK_CONFIGURED:
        raise HTTPException(status_code=400, detail="Slack isn't available in this version of JOI yet.")
    owner_id = str(current_user.get("_id"))
    state = _sign_oauth_state(owner_id, "slack")
    from urllib.parse import urlencode
    url = "https://slack.com/oauth/v2/authorize?" + urlencode({
        "client_id": SLACK_CLIENT_ID,
        "scope": SLACK_BOT_SCOPES,
        "redirect_uri": SLACK_REDIRECT_URI,
        "state": state,
    })
    return {"authorize_url": url}


def _oauth_result_page(msg: str, ok: bool = False) -> Response:
    """Branded page shown in the system browser after a connector OAuth round-trip.
    On success it hands the user back to the desktop app via the joi:// protocol."""
    import html as _html
    safe = _html.escape(str(msg))  # messages can contain provider error text
    back = (
        "<a class='btn' href='joi://connected'>Return to JOI</a>"
        "<p class='hint'>Not switching automatically? Click the button, or close this tab.</p>"
        "<script>setTimeout(function(){location.href='joi://connected'},500);"
        "setTimeout(function(){try{window.close()}catch(e){}},1500);</script>"
        if ok else "<p class='hint'>You can close this tab and try again from JOI.</p>"
    )
    page = f"""<!doctype html><html><head><meta charset="utf-8"><title>JOI</title><style>
body{{margin:0;min-height:100vh;display:grid;place-items:center;font-family:Inter,"Segoe UI",system-ui,sans-serif;
background:radial-gradient(900px 600px at 85% -10%,rgba(217,119,87,.14),transparent 60%),#faf7f2;color:#17150f}}
.card{{width:min(440px,90vw);padding:40px 36px;border-radius:26px;background:rgba(255,255,255,.8);
border:1px solid rgba(23,21,15,.1);box-shadow:0 24px 50px -20px rgba(28,26,23,.22);text-align:center}}
h1{{margin:20px 0 8px;font-family:Georgia,serif;font-size:26px}}
.btn{{display:inline-block;margin-top:22px;padding:12px 26px;border-radius:999px;color:#fff;text-decoration:none;font-weight:600;
background:linear-gradient(135deg,#e68a67,#d97757 55%,#c45f3d);box-shadow:0 10px 30px -10px rgba(217,119,87,.55)}}
.hint{{margin-top:14px;font-size:13px;color:#857d70}}
</style></head><body><div class="card">
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" width="56" height="56"><defs><linearGradient id="t" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#ec9a76"/><stop offset=".55" stop-color="#d97757"/><stop offset="1" stop-color="#b4502f"/></linearGradient></defs><rect width="512" height="512" rx="120" fill="url(#t)"/><circle cx="300" cy="130" r="40" fill="#fdf6ee"/><path d="M300 208V318C300 384 262 408 206 396" fill="none" stroke="#fdf6ee" stroke-width="66" stroke-linecap="round"/></svg>
<h1>{safe}</h1>{back}</div></body></html>"""
    return Response(content=page, media_type="text/html")


@app.get("/integrations/slack/callback")
async def slack_callback(code: str = Query(default=""), state: str = Query(default="")):
    _page = _oauth_result_page
    try:
        owner_id = _verify_oauth_state(state, "slack")
    except HTTPException:
        return _page("Slack connection failed: invalid state.")
    if not code:
        return _page("Slack connection cancelled.")
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            if SLACK_CLIENT_SECRET:
                resp = await client.post("https://slack.com/api/oauth.v2.access", data={
                    "client_id": SLACK_CLIENT_ID,
                    "client_secret": SLACK_CLIENT_SECRET,
                    "code": code,
                    "redirect_uri": SLACK_REDIRECT_URI,
                })
            else:
                resp = await client.post(SLACK_EXCHANGE_URL, json={"code": code, "redirect_uri": SLACK_REDIRECT_URI})
        data = resp.json()
    except Exception as exc:
        logger.exception("Slack token exchange failed: %s", exc)
        return _page("Slack connection failed.")
    if not data.get("ok"):
        return _page(f"Slack connection failed: {data.get('error', 'unknown')}.")

    now = _utc_now_iso()
    integrations_collection.update_one(
        {"owner_user_id": owner_id, "provider": "slack"},
        {"$set": {
            "owner_user_id": owner_id, "provider": "slack",
            "access_token": data.get("access_token", ""),
            "team_name": (data.get("team") or {}).get("name", ""),
            "team_id": (data.get("team") or {}).get("id", ""),
            "bot_user_id": data.get("bot_user_id", ""),
            "scope": data.get("scope", ""),
            "updated_at": now,
        }, "$setOnInsert": {"created_at": now}},
        upsert=True,
    )
    _write_audit_log(owner_id, "slack_connected", "ok", {"team": (data.get("team") or {}).get("name", "")})
    return _page("Slack connected", ok=True)


@app.post("/integrations/slack/disconnect")
async def slack_disconnect(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    integrations_collection.delete_one({"owner_user_id": owner_id, "provider": "slack"})
    _write_audit_log(owner_id, "slack_disconnected", "ok", {})
    return {"disconnected": True}


# ---------------------------------------------------------------------------
#  Telegram: reach JOI from your phone through your own bot (see telegram_channel.py)
# ---------------------------------------------------------------------------

PAIRING_TTL_SECONDS = 15 * 60


class TelegramConnectRequest(BaseModel):
    bot_token: str = Field(..., min_length=20, max_length=200)


def _telegram_load(owner_id: str) -> Optional[Dict[str, Any]]:
    return integrations_collection.find_one({"owner_user_id": owner_id, "provider": "telegram"})


def _telegram_save(owner_id: str, fields: Dict[str, Any]) -> None:
    integrations_collection.update_one(
        {"owner_user_id": owner_id, "provider": "telegram"},
        {"$set": {**fields, "updated_at": _utc_now_iso()}},
    )


def _new_pairing_code() -> Tuple[str, float]:
    return f"{secrets.randbelow(1_000_000):06d}", time.time() + PAIRING_TTL_SECONDS


async def _handle_phone_message(owner_id: str, chat_id: int, text: str) -> str:
    """Answer a message from the user's paired phone using the full agent, with every
    sensitive action held for approval on the PC (remote=True)."""
    session_id = f"telegram_{owner_id}_{chat_id}"
    record = sessions_collection.find_one({"session_id": session_id, "owner_user_id": owner_id}, {"messages": 1}) or {}
    prior = [
        {"role": m.get("role"), "content": str(m.get("content") or "")}
        for m in (record.get("messages") or [])[-12:]
        if m.get("role") in {"user", "assistant"} and str(m.get("content") or "").strip()
    ]
    history = [{"role": "system", "content": system_prompt}] + prior
    _add_turn_context(history, owner_id, text)
    history.append({"role": "user", "content": text})
    _write_audit_log(owner_id, "phone_message", "ok", {"channel": "telegram"})

    final_text, approval_tool = "", ""
    async for event_name, data in _agent_events(owner_id, session_id, "joi", history, text, remote=True):
        if event_name == "workflow_step_requires_approval":
            approval_tool = str((data or {}).get("tool") or "")
        elif event_name == "done":
            final_text = str((data or {}).get("message") or "")
    if approval_tool:
        label = approval_tool.replace("_", " ").capitalize()
        return (
            f"I need your OK on your PC before I use {label}. Open JOI there: this conversation "
            "is in your chat list with an Approve button (you'll get a notification too)."
        )
    return _strip_tool_call_text(final_text) or "Done."


telegram_channel = TelegramChannel(_telegram_load, _telegram_save, _handle_phone_message)


def _telegram_status_payload(doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not doc:
        return {"configured": True, "connected": False}
    paired = doc.get("chat_id") is not None
    code_valid = bool(doc.get("pairing_code")) and time.time() < float(doc.get("pairing_expires") or 0)
    return {
        "configured": True,
        "connected": True,
        "bot": f"@{doc.get('bot_username')}" if doc.get("bot_username") else "",
        "paired": paired,
        "pairing_code": "" if paired or not code_valid else doc.get("pairing_code"),
    }


@app.get("/integrations/telegram/status")
async def telegram_status(current_user: Dict[str, Any] = Depends(require_auth_user)):
    return _telegram_status_payload(_telegram_load(str(current_user.get("_id"))))


@app.post("/integrations/telegram/connect")
async def telegram_connect(payload: TelegramConnectRequest, current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    token = payload.bot_token.strip()
    if not re.fullmatch(r"\d{5,15}:[A-Za-z0-9_-]{30,}", token):
        raise HTTPException(status_code=400, detail="That doesn't look like a Telegram bot token.")
    try:
        bot = await tg_get_me(token)
    except Exception:
        raise HTTPException(status_code=400, detail="Telegram didn't accept that token. Check it and try again.")

    code, expires = _new_pairing_code()
    integrations_collection.update_one(
        {"owner_user_id": owner_id, "provider": "telegram"},
        {
            "$set": {
                "owner_user_id": owner_id,
                "provider": "telegram",
                "access_token": token,
                "bot_username": str(bot.get("username") or ""),
                "chat_id": None,
                "pairing_code": code,
                "pairing_expires": expires,
                "last_update_id": 0,
                "updated_at": _utc_now_iso(),
            },
            "$setOnInsert": {"created_at": _utc_now_iso()},
        },
        upsert=True,
    )
    telegram_channel.ensure(owner_id)
    _write_audit_log(owner_id, "telegram_connected", "ok", {"bot": bot.get("username", "")})
    return _telegram_status_payload(_telegram_load(owner_id))


@app.post("/integrations/telegram/pairing")
async def telegram_new_pairing_code(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    if not _telegram_load(owner_id):
        raise HTTPException(status_code=404, detail="Telegram isn't connected.")
    code, expires = _new_pairing_code()
    _telegram_save(owner_id, {"pairing_code": code, "pairing_expires": expires, "chat_id": None})
    telegram_channel.ensure(owner_id)
    return _telegram_status_payload(_telegram_load(owner_id))


@app.post("/integrations/telegram/disconnect")
async def telegram_disconnect(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    telegram_channel.stop(owner_id)
    integrations_collection.delete_one({"owner_user_id": owner_id, "provider": "telegram"})
    _write_audit_log(owner_id, "telegram_disconnected", "ok", {})
    return {"disconnected": True}


# ---- Google per-user OAuth (access + refresh tokens, auto-refresh) ----
def _get_google_credentials(owner_id: str) -> Optional[GoogleUserCredentials]:
    """Build the user's Google credentials from stored tokens, refreshing (and
    persisting the new access token) when expired. Returns None if not connected."""
    doc = integrations_collection.find_one({"owner_user_id": owner_id, "provider": "google"})
    if not doc or not doc.get("refresh_token"):
        return None

    expiry = None
    if doc.get("token_expiry"):
        try:
            expiry = datetime.fromisoformat(doc["token_expiry"])
            if expiry.tzinfo is not None:
                expiry = expiry.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            expiry = None

    creds = GoogleUserCredentials(
        token=doc.get("access_token") or None,
        refresh_token=doc.get("refresh_token"),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_OAUTH_CLIENT_ID,
        client_secret=GOOGLE_OAUTH_CLIENT_SECRET,
        scopes=GOOGLE_COMBINED_SCOPES,
        expiry=expiry,
    )
    try:
        if not creds.valid and creds.refresh_token:
            creds.refresh(google_auth_requests.Request())
            integrations_collection.update_one(
                {"owner_user_id": owner_id, "provider": "google"},
                {"$set": {
                    "access_token": creds.token,
                    "token_expiry": creds.expiry.isoformat() if creds.expiry else "",
                    "updated_at": _utc_now_iso(),
                }},
            )
    except Exception as exc:
        logger.warning("Google token refresh failed for %s: %s", owner_id, exc)
        return None
    return creds


@app.get("/integrations/google/status")
async def google_status(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    doc = integrations_collection.find_one({"owner_user_id": owner_id, "provider": "google"}) or {}
    return {
        "configured": bool(GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET),
        "connected": bool(doc.get("refresh_token")),
        "email": str(doc.get("email") or ""),
    }


@app.get("/integrations/google/connect")
async def google_connect(current_user: Dict[str, Any] = Depends(require_auth_user)):
    if not (GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET):
        raise HTTPException(status_code=400, detail="Google is not configured on the server.")
    owner_id = str(current_user.get("_id"))
    state = _sign_oauth_state(owner_id, "google")
    from urllib.parse import urlencode
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode({
        "client_id": GOOGLE_OAUTH_CLIENT_ID,
        "redirect_uri": GOOGLE_API_REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(GOOGLE_COMBINED_SCOPES),
        "access_type": "offline",     # request a refresh token
        "prompt": "consent",          # force refresh token on every (re)connect
        "include_granted_scopes": "true",
        "state": state,
    })
    return {"authorize_url": url}


@app.get("/integrations/google/callback")
async def google_callback(code: str = Query(default=""), state: str = Query(default="")):
    _page = _oauth_result_page
    try:
        owner_id = _verify_oauth_state(state, "google")
    except HTTPException:
        return _page("Google connection failed: invalid state.")
    if not code:
        return _page("Google connection cancelled.")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            tok = await client.post("https://oauth2.googleapis.com/token", data={
                "client_id": GOOGLE_OAUTH_CLIENT_ID,
                "client_secret": GOOGLE_OAUTH_CLIENT_SECRET,
                "code": code,
                "redirect_uri": GOOGLE_API_REDIRECT_URI,
                "grant_type": "authorization_code",
            })
            data = tok.json()
            if "access_token" not in data:
                return _page(f"Google connection failed: {data.get('error_description') or data.get('error', 'unknown')}.")
            email = ""
            try:
                ui = await client.get(
                    "https://www.googleapis.com/oauth2/v3/userinfo",
                    headers={"Authorization": f"Bearer {data['access_token']}"},
                )
                email = str(ui.json().get("email") or "")
            except Exception:
                pass
    except Exception as exc:
        logger.exception("Google token exchange failed: %s", exc)
        return _page("Google connection failed.")

    refresh_token = data.get("refresh_token", "")
    if not refresh_token:
        # Keep any previously stored refresh token if Google didn't return a new one.
        prev = integrations_collection.find_one({"owner_user_id": owner_id, "provider": "google"}) or {}
        refresh_token = prev.get("refresh_token", "")

    now = _utc_now_iso()
    expiry_iso = _as_iso(datetime.now(timezone.utc) + timedelta(seconds=int(data.get("expires_in", 3600))))
    integrations_collection.update_one(
        {"owner_user_id": owner_id, "provider": "google"},
        {"$set": {
            "owner_user_id": owner_id, "provider": "google",
            "access_token": data.get("access_token", ""),
            "refresh_token": refresh_token,
            "token_expiry": expiry_iso,
            "scope": data.get("scope", ""),
            "email": email,
            "updated_at": now,
        }, "$setOnInsert": {"created_at": now}},
        upsert=True,
    )
    _write_audit_log(owner_id, "google_connected", "ok", {"email": email})
    return _page("Google connected", ok=True)


@app.post("/integrations/google/disconnect")
async def google_disconnect(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    integrations_collection.delete_one({"owner_user_id": owner_id, "provider": "google"})
    _write_audit_log(owner_id, "google_disconnected", "ok", {})
    return {"disconnected": True}


@app.get("/sessions")
async def list_sessions(current_user: Dict[str, Any] = Depends(require_auth_user)):
    owner_id = str(current_user.get("_id"))
    cache_key = _sessions_list_cache_key(owner_id)
    cached = _cache_get_json(cache_key)
    if cached is not None:
        return cached

    try:
        records = list(
            sessions_collection.find(
                {"owner_user_id": owner_id},
                {"_id": 0}
            ).sort("updated_at", DESCENDING)
        )
    except PyMongoError as exc:
        logger.exception("Mongo list sessions failed for user '%s': %s", owner_id, exc)
        raise HTTPException(status_code=500, detail="Failed to list sessions.") from exc

    payload = {
        "count": len(records),
        "sessions": [_session_summary(s) for s in records]
    }
    _cache_set_json(cache_key, payload, REDIS_SESSIONS_LIST_TTL)
    return payload


@app.get("/sessions/{session_id}")
async def get_session(
    session_id: str,
    current_user: Dict[str, Any] = Depends(require_auth_user)
):
    sid = str(session_id).strip()
    if not sid:
        raise HTTPException(status_code=400, detail="session_id is required.")

    owner_id = str(current_user.get("_id"))
    cache_key = _session_cache_key(owner_id, sid)
    cached = _cache_get_json(cache_key)
    if cached is not None:
        return cached

    try:
        record = sessions_collection.find_one(
            {"session_id": sid, "owner_user_id": owner_id},
            {"_id": 0}
        )
    except PyMongoError as exc:
        logger.exception("Mongo get session failed for '%s': %s", sid, exc)
        raise HTTPException(status_code=500, detail="Failed to load session.") from exc

    if not record:
        raise HTTPException(status_code=404, detail=f"Session '{sid}' not found.")

    payload = {
        "session_id": record.get("session_id"),
        "title": record.get("title", "New chat"),
        "model": record.get("model", "openai"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "messages": record.get("messages", [])
    }
    _cache_set_json(cache_key, payload, REDIS_SESSION_TTL)
    return payload


@app.delete("/sessions/{session_id}")
async def delete_session(
    session_id: str,
    current_user: Dict[str, Any] = Depends(require_auth_user)
):
    sid = str(session_id).strip()
    if not sid:
        raise HTTPException(status_code=400, detail="session_id is required.")

    owner_id = str(current_user.get("_id"))
    try:
        existing = sessions_collection.find_one(
            {"session_id": sid, "owner_user_id": owner_id},
            {"_id": 0, "title": 1}
        )
        if not existing:
            raise HTTPException(status_code=404, detail=f"Session '{sid}' not found.")
        sessions_collection.delete_one({"session_id": sid, "owner_user_id": owner_id})
    except HTTPException:
        raise
    except PyMongoError as exc:
        logger.exception("Mongo delete failed for '%s': %s", sid, exc)
        raise HTTPException(status_code=500, detail="Failed to delete session.") from exc

    _invalidate_session_cache(owner_id, sid)
    return {
        "deleted": True,
        "session_id": sid,
        "title": str(existing.get("title") or "New chat")
    }


@app.post("/chat/title")
async def generate_chat_title(
    payload: TitleRequest,
    _current_user: Dict[str, Any] = Depends(require_auth_user)
):
    sanitized_history = _sanitize_history(payload.history)

    compact_messages = []
    for msg in sanitized_history:
        if msg["role"] in {"user", "assistant"}:
            compact_messages.append(f"{msg['role']}: {msg['content']}")
    compact_context = "\n".join(compact_messages[-6:]).strip()

    if not compact_context:
        return {"title": _fallback_title(payload.fallback_text)}

    # Without an OpenAI client (e.g. local-only Ollama build), use the heuristic title.
    if title_client is None:
        return {"title": _fallback_title(payload.fallback_text or compact_context)}

    prompt = (
        "Generate a short conversation title (max 6 words) for this chat. "
        "It must be user-friendly and specific. Return only the title text."
    )

    try:
        response = title_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You generate concise, clear chat titles."},
                {"role": "user", "content": f"{prompt}\n\n{compact_context}"}
            ],
            temperature=0.2,
            max_tokens=20
        )
        raw_title = (response.choices[0].message.content or "").strip().strip('"').strip("'")
        title = re.sub(r"\s+", " ", raw_title)
        if not title:
            title = _fallback_title(payload.fallback_text or compact_context)
        return {"title": title[:48]}
    except Exception as exc:
        logger.warning("Title generation failed, using fallback: %s", exc)
        return {"title": _fallback_title(payload.fallback_text or compact_context)}


@app.post("/feedback")
async def submit_message_feedback(
    payload: MessageFeedbackRequest,
    current_user: Dict[str, Any] = Depends(require_auth_user)
):
    conversation_id = str(payload.conversation_id).strip()
    message_id = str(payload.message_id).strip()
    feedback = str(payload.feedback).strip().lower()
    owner_id = str(current_user.get("_id"))

    if not conversation_id:
        raise HTTPException(status_code=400, detail="conversation_id is required.")
    if not message_id:
        raise HTTPException(status_code=400, detail="message_id is required.")
    if feedback not in {"like", "dislike", "clear"}:
        raise HTTPException(status_code=400, detail="feedback must be like, dislike, or clear.")

    try:
        if feedback == "clear":
            feedback_collection.delete_one(
                {
                    "owner_user_id": owner_id,
                    "conversation_id": conversation_id,
                    "message_id": message_id
                }
            )
            return {
                "saved": True,
                "conversation_id": conversation_id,
                "message_id": message_id,
                "feedback": "clear"
            }

        now = _utc_now_iso()
        feedback_collection.update_one(
            {
                "owner_user_id": owner_id,
                "conversation_id": conversation_id,
                "message_id": message_id
            },
            {
                "$set": {
                    "owner_user_id": owner_id,
                    "conversation_id": conversation_id,
                    "message_id": message_id,
                    "feedback": feedback,
                    "message_text": str(payload.message_text or "")[:4000],
                    "model": str(payload.model or "openai"),
                    "updated_at": now
                },
                "$setOnInsert": {
                    "created_at": now
                }
            },
            upsert=True
        )
        return {
            "saved": True,
            "conversation_id": conversation_id,
            "message_id": message_id,
            "feedback": feedback
        }
    except PyMongoError as exc:
        logger.exception("Mongo feedback upsert failed: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to save feedback.") from exc


@app.post("/audio/transcribe")
async def transcribe_audio(
    file: UploadFile = File(...),
    model: str = Form(default=os.getenv("OPENAI_TRANSCRIBE_MODEL", "whisper-1")),
    _current_user: Dict[str, Any] = Depends(require_auth_user)
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Audio filename is required.")

    blob = await file.read()
    if not blob:
        raise HTTPException(status_code=400, detail="Empty audio payload.")

    # Fallback chain: ElevenLabs -> local Whisper -> OpenAI. Each is tried only if
    # configured/available; errors are collected so we can report what was tried.
    attempts: List[str] = []

    if voice_provider.eleven_enabled():
        try:
            text = await voice_provider.eleven_stt(blob, file.filename)
            if text:
                return {"text": text, "model_used": os.getenv("ELEVENLABS_STT_MODEL", "scribe_v1")}
            attempts.append("elevenlabs: no speech detected")
        except Exception as exc:
            logger.warning("ElevenLabs STT failed, trying fallback: %s", exc)
            attempts.append(f"elevenlabs: {exc}")

    if voice_provider.local_stt_available():
        try:
            text = await voice_provider.local_stt(blob, file.filename)
            if text:
                return {"text": text, "model_used": f"whisper-local:{os.getenv('JOI_LOCAL_WHISPER_MODEL', 'base')}"}
            attempts.append("local-whisper: no speech detected")
        except Exception as exc:
            logger.warning("Local Whisper STT failed, trying fallback: %s", exc)
            attempts.append(f"local-whisper: {exc}")

    if title_client is not None:
        suffix = os.path.splitext(file.filename)[1] or ".webm"
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(blob)
                temp_path = tmp.name
            used_model = model
            with open(temp_path, "rb") as audio_file:
                try:
                    transcript = title_client.audio.transcriptions.create(model=model, file=audio_file)
                except Exception:
                    if model != "whisper-1":
                        used_model = "whisper-1"
                        audio_file.seek(0)
                        transcript = title_client.audio.transcriptions.create(model="whisper-1", file=audio_file)
                    else:
                        raise
            text = str(getattr(transcript, "text", "") or "").strip()
            if text:
                return {"text": text, "model_used": used_model}
            attempts.append("openai: no speech detected")
        except Exception as exc:
            logger.warning("OpenAI STT failed: %s", exc)
            attempts.append(f"openai: {exc}")
        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass

    if not attempts:
        raise HTTPException(
            status_code=503,
            detail="No speech-to-text provider available. Set ELEVENLABS_API_KEY, install faster-whisper, or set an OpenAI key."
        )
    raise HTTPException(status_code=502, detail="Transcription failed. Tried: " + "; ".join(attempts))


@app.post("/audio/speak")
async def speak_text(
    payload: TTSRequest,
    _current_user: Dict[str, Any] = Depends(require_auth_user)
):
    provider = voice_provider.active_provider(title_client is not None)
    if provider == "none":
        raise HTTPException(
            status_code=503,
            detail="Text-to-speech needs a voice provider. Set ELEVENLABS_API_KEY or an OpenAI key."
        )
    text = str(payload.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Text is required for TTS.")

    # Keep synthesis payload bounded for latency and cost.
    if len(text) > 4000:
        text = text[:4000]

    if provider == "elevenlabs":
        try:
            audio_bytes = await voice_provider.eleven_tts(text, str(payload.voice or ""))
        except Exception as exc:
            logger.exception("ElevenLabs TTS failed: %s", exc)
            raise HTTPException(status_code=502, detail=f"TTS failed: {exc}")
        if not audio_bytes:
            raise HTTPException(status_code=500, detail="TTS generated empty audio.")
        return Response(
            content=audio_bytes,
            media_type="audio/mpeg",
            headers={"Cache-Control": "no-store"},
        )

    temp_path = None
    try:
        speech = title_client.audio.speech.create(
            model=payload.model,
            voice=payload.voice,
            input=text
        )

        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
            temp_path = tmp.name
            speech.stream_to_file(temp_path)

        with open(temp_path, "rb") as f:
            audio_bytes = f.read()

        if not audio_bytes:
            raise HTTPException(status_code=500, detail="TTS generated empty audio.")

        return Response(
            content=audio_bytes,
            media_type="audio/mpeg",
            headers={
                "Cache-Control": "no-store"
            }
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("TTS generation failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"TTS failed: {exc}")
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass


def _sse_event(event_name: str, payload: Any) -> str:
    return f"event: {event_name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _normalize_tool_args(tool_name: str, tool_args: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(tool_args or {})

    if tool_name != "search":
        return normalized

    raw_query = str(normalized.get("query", "")).strip()
    if not raw_query:
        return normalized

    now = datetime.now().astimezone()
    current_year = str(now.year)
    current_month_year = now.strftime("%B %Y")
    current_full_date = now.strftime("%Y-%m-%d")
    current_human_date = now.strftime("%B %d, %Y")
    month_pattern = (
        r"\b(?:january|february|march|april|may|june|july|august|"
        r"september|october|november|december)\s+(?:19|20)\d{2}\b"
    )

    # Remove stale month-year/year mentions and append current date context.
    fresh_query = re.sub(month_pattern, "", raw_query, flags=re.IGNORECASE)
    fresh_query = re.sub(r"\b(19|20)\d{2}\b", "", fresh_query)
    fresh_query = re.sub(r"\s{2,}", " ", fresh_query).strip(" ,.-")

    relative_time_request = bool(
        re.search(r"\b(today|latest|current|now|tonight|this\s+week)\b", raw_query, flags=re.IGNORECASE)
    )
    if relative_time_request:
        fresh_query = f"{fresh_query} {current_human_date} {current_full_date}".strip()
    else:
        fresh_query = f"{fresh_query} {current_month_year}".strip()

    fresh_query = fresh_query.replace(f"{current_year} {current_month_year}", current_month_year)

    normalized["query"] = fresh_query
    return normalized


async def _agent_events(
    owner_id: str,
    session_id: str,
    model: str,
    history: List[Dict[str, str]],
    user_message: str,
    seeded_calls: Optional[List[Tuple[str, Dict[str, Any]]]] = None,
    remote: bool = False,
):
    """The JOI agent loop: generate, run tools, feed results back, repeat.

    Yields (event_name, data) tuples. Used by live chat (wrapped as SSE), by
    approval-resume (`seeded_calls` runs an already-approved tool call first) and by
    background routines (consumed headlessly). Mutates and persists `history`.
    """
    tool_count = 0
    final_response = ""
    workflow_active = False
    workflow_steps_executed = 0
    failed_call_signatures: set = set()
    invalid_call_corrected = False
    grounding_checked = False

    try:
        yield ("status", {"message": "SSE stream connected"})

        while tool_count < MAX_CONSECUTIVE_TOOL_CALLS:
            accumulated_response = ""
            chunk_index = 0

            if seeded_calls:
                # Resume path: run the call the user just approved before asking the model again.
                tool_calls, seeded_calls = seeded_calls, None
            else:
                async for chunk, accumulated in astream_llm(history.copy()):
                    accumulated_response = accumulated
                    if chunk:
                        yield ("ai_chunk", {"chunk": chunk, "index": chunk_index})
                        chunk_index += 1

                history.append({"role": "assistant", "content": accumulated_response})
                final_response = accumulated_response
                _upsert_session_history(session_id, model, history, owner_id)

                tool_calls = tool_registry.extract_tool_calls(accumulated_response)
            if not tool_calls and "TOOL_CALL::" in accumulated_response and not invalid_call_corrected:
                # The model tried a tool that doesn't exist (or wrote unparseable JSON).
                # The UI hides tool-call text, so without a retry the user sees an empty
                # reply. Correct it once with the real tool names.
                invalid_call_corrected = True
                history.append(_joi_note(
                    "That tool call was invalid: the tool does not exist or the JSON was malformed. "
                    f"Available tools: {', '.join(sorted(tool_registry.tools))}. "
                    "Either call one of these exactly as shown in your instructions, "
                    "or answer the user's previous message in plain text."
                ))
                yield ("ai_chunk", {"chunk": "\n\n", "index": chunk_index})
                continue
            if (
                not tool_calls
                and workflow_steps_executed == 0
                and not grounding_checked
                and _PERSONAL_DATA_RE.search(user_message)
            ):
                # The reply is about the user's own data but no tool ran this turn, so any
                # specifics in it are invented. Discard it and ask once for a grounded answer.
                grounding_checked = True
                history.pop()
                history.append(_joi_note(
                    "Before answering: you have not called any tool in this turn. Never invent "
                    "emails, events, files, messages, contacts or numbers, and never repeat earlier "
                    "replies in this chat as if they were still true: things may have changed since "
                    "(for example an account may now be connected). If the answer needs the user's real "
                    "data, call the right tool now. If a tool can't be used (for example the account "
                    "isn't connected), say so plainly. If the user only wanted writing or general "
                    "help, give that answer."
                ))
                yield ("ai_reset", {})
                continue
            if not tool_calls:
                if workflow_active:
                    yield (
                        "workflow_completed",
                        {
                            "message": "Workflow completed successfully.",
                            "steps_executed": workflow_steps_executed
                        }
                    )
                    workflow_active = False
                break

            if not workflow_active:
                workflow_active = True
                yield (
                    "workflow_started",
                    {
                        "message": "Agent workflow started.",
                        "initial_step_count": len(tool_calls)
                    }
                )

            approval_blocked = False
            for idx, (tool_name, tool_args) in enumerate(tool_calls, start=1):
                effective_tool_args = _normalize_tool_args(tool_name, tool_args)
                tool_mode = _tool_execution_mode(owner_id, tool_name)
                read_only = _is_read_only_call(tool_name, effective_tool_args)
                if tool_mode == "require_approval" and read_only:
                    # Approval guards actions (send, create, delete); reading the user's own
                    # inbox or calendar doesn't need a prompt every time. "deny" still blocks.
                    tool_mode = "allow"
                elif remote and tool_mode == "allow" and tool_name in DANGEROUS_TOOLS and not read_only:
                    # Requests from the phone always wait for approval on the PC, even for
                    # tools the user lets run freely when sitting at the computer.
                    tool_mode = "require_approval"

                workflow_steps_executed += 1
                yield (
                    "workflow_step_started",
                    {
                        "step_index": workflow_steps_executed,
                        "tool": tool_name,
                        "args": effective_tool_args
                    }
                )

                yield (
                    "tool_status",
                    {"message": f"Executing tool call {idx}/{len(tool_calls)}: {tool_name}"}
                )

                recipient_error = _unverified_recipient_error(tool_name, effective_tool_args, history)
                if recipient_error:
                    # Tool results (incl. contacts_search) are stored as "user" turns, so a
                    # looked-up address counts as known. Feed the refusal back and let the
                    # model show the draft / ask, without creating a pointless approval.
                    history.append({
                        "role": "user",
                        "content": wrap_untrusted_tool_output(tool_name, recipient_error)
                    })
                    yield (
                        "workflow_step_completed",
                        {"step_index": workflow_steps_executed, "tool": tool_name, "result": recipient_error}
                    )
                    continue

                if tool_mode == "deny":
                    block_text = f"Execution denied by security policy for tool '{tool_name}'."
                    yield (
                        "workflow_step_blocked",
                        {
                            "step_index": workflow_steps_executed,
                            "tool": tool_name,
                            "reason": block_text
                        }
                    )
                    history.append({"role": "assistant", "content": block_text})
                    final_response = block_text
                    _write_audit_log(owner_id, "tool_execution_denied", "blocked", {"tool_name": tool_name})
                    approval_blocked = True
                    break

                if tool_mode == "require_approval":
                    consumed_approval = _consume_matching_approved_tool_approval(
                        owner_id,
                        tool_name,
                        effective_tool_args
                    )
                    if consumed_approval:
                        yield (
                            "tool_status",
                            {
                                "message": (
                                    f"Using approved authorization {consumed_approval} "
                                    f"for tool {tool_name}."
                                )
                            }
                        )
                    else:
                        approval = _create_tool_approval(owner_id, tool_name, effective_tool_args, session_id)
                        approval_id = str(approval.get("approval_id") or "")
                        approval_text = (
                            f"Tool '{tool_name}' requires approval before execution. "
                            f"Approval ID: {approval_id}"
                        )
                        yield (
                            "workflow_step_requires_approval",
                            {
                                "step_index": workflow_steps_executed,
                                "tool": tool_name,
                                "args": effective_tool_args,
                                "approval_id": approval_id,
                                "message": approval_text
                            }
                        )
                        _create_notification(
                            owner_id,
                            "Approval required",
                            f"Approve tool '{tool_name}' to continue workflow.",
                            category="approval"
                        )
                        history.append({"role": "assistant", "content": approval_text})
                        final_response = approval_text
                        approval_blocked = True
                        break

                # Inject the current user's own integration credentials so each
                # customer acts as themselves. Kept out of SSE/history so nothing leaks.
                exec_args = dict(effective_tool_args)
                if tool_name.startswith("slack_"):
                    exec_args["_token"] = _get_slack_token(owner_id)
                _google_creds_set = False
                if tool_name in GOOGLE_TOOLS:
                    _g_creds = _get_google_credentials(owner_id)
                    if _g_creds:
                        google_auth.set_user_credentials(_g_creds)
                        _google_creds_set = True
                call_signature = f"{tool_name}:{json.dumps(effective_tool_args, sort_keys=True, default=str)}"
                if call_signature in failed_call_signatures:
                    # The model keeps retrying an identical call that already failed;
                    # don't run it again (that loop wasted ~17s per reply).
                    if _google_creds_set:
                        google_auth.clear_user_credentials()
                    result = (
                        "Error: this exact tool call already failed in this conversation turn. "
                        "Do not retry it. Explain the problem to the user and suggest what they can do."
                    )
                else:
                    try:
                        result = await tool_registry.execute(tool_name, exec_args)
                    finally:
                        if _google_creds_set:
                            google_auth.clear_user_credentials()
                    if str(result).lstrip().startswith("Error"):
                        failed_call_signatures.add(call_signature)
                result, undo_spec = extract_undo(str(result))
                undo_event = _register_undo(owner_id, undo_spec) if undo_spec else None
                # Secrets are stripped and the payload is fenced as untrusted data
                # so embedded instructions can't hijack the agent.
                safe_result = redact_secrets(str(result))
                # Fed back as a "user" turn: local models return an empty reply when the
                # result follows as a second consecutive assistant turn. The content is
                # still fenced as untrusted data, so it isn't treated as the user's request.
                history.append({
                    "role": "user",
                    "content": wrap_untrusted_tool_output(tool_name, result)
                })
                # Tool results are persisted by the next assistant-turn write and
                # the guaranteed write in `finally`; skip the per-tool write to
                # avoid serializing the full history on every tool call.
                _write_audit_log(owner_id, "tool_execution", "ok", {"tool_name": tool_name})

                yield (
                    "tool_result",
                    {"tool": tool_name, "args": effective_tool_args, "result": safe_result}
                )
                yield (
                    "workflow_step_completed",
                    {
                        "step_index": workflow_steps_executed,
                        "tool": tool_name,
                        "result": safe_result
                    }
                )
                if undo_event:
                    yield ("undo_available", undo_event)

            if approval_blocked:
                if workflow_active:
                    yield (
                        "workflow_failed",
                        {"message": "Workflow paused: approval required or blocked by policy."}
                    )
                    workflow_active = False
                break

            tool_count += 1

        if tool_count >= MAX_CONSECUTIVE_TOOL_CALLS:
            yield ("warning", {"message": "Max tool call limit reached"})
            if workflow_active:
                yield (
                    "workflow_failed",
                    {"message": "Workflow stopped: max tool call limit reached."}
                )

        yield (
            "done",
            {
                "conversation_id": session_id,
                "message": final_response,
                "workflow_steps_executed": workflow_steps_executed
            }
        )
    except Exception as exc:
        logger.exception("SSE stream failed for %s: %s", session_id, exc)
        if workflow_active:
            yield (
                "workflow_failed",
                {"message": f"Workflow failed: {exc}"}
            )
        yield ("error", {"message": f"Server error: {exc}"})
    finally:
        _upsert_session_history(session_id, model, history, owner_id)
        yield ("end", {})


@app.post("/chat/stream")
async def chat_stream(
    payload: SSEChatRequest,
    current_user: Dict[str, Any] = Depends(require_auth_user)
):
    session_id = str(payload.conversation_id).strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="conversation_id is required.")

    owner_id = str(current_user.get("_id"))
    history = _drop_repeated_question(_sanitize_history(payload.history), payload.message)
    _add_turn_context(history, owner_id, payload.message)
    history.append({"role": "user", "content": payload.message.strip()})
    _write_audit_log(owner_id, "chat_message", "ok", {"session_id": session_id})
    _upsert_session_history(session_id, payload.model, history, owner_id)

    async def events():
        async for event_name, data in _agent_events(owner_id, session_id, payload.model, history, payload.message):
            yield event_name, data
        # After the answer is out, look for lasting facts worth suggesting (memory that learns).
        if memory_learning.worth_checking(payload.message):
            asyncio.create_task(_learn_from_message(owner_id, payload.message))

    return _sse_response(events())


@app.post("/chat/resume")
async def chat_resume(
    payload: ChatResumeRequest,
    current_user: Dict[str, Any] = Depends(require_auth_user)
):
    """Run a tool call the user just approved, then let JOI finish the task, streaming
    into the same conversation, so the user doesn't have to ask again."""
    owner_id = str(current_user.get("_id"))
    session_id = str(payload.conversation_id).strip()
    approval = approvals_collection.find_one({
        "owner_user_id": owner_id,
        "approval_id": str(payload.approval_id).strip(),
        "status": "approved",
        "consumed_at": {"$exists": False},
    })
    if not approval:
        raise HTTPException(status_code=404, detail="This approval isn't waiting to run.")
    record = sessions_collection.find_one(
        {"session_id": session_id, "owner_user_id": owner_id}, {"_id": 0, "messages": 1, "model": 1}
    )
    if not record:
        raise HTTPException(status_code=404, detail="Conversation not found.")

    prior = [
        {"role": m.get("role"), "content": str(m.get("content") or "")}
        for m in record.get("messages", [])
        if m.get("role") in {"user", "assistant"} and str(m.get("content") or "").strip()
    ]
    last_user = next((m["content"] for m in reversed(prior) if m["role"] == "user"), "")
    history = [{"role": "system", "content": system_prompt}] + prior
    _add_turn_context(history, owner_id, last_user)
    seeded = [(str(approval.get("tool_name") or ""), dict(approval.get("tool_args") or {}))]
    _write_audit_log(owner_id, "chat_resume", "ok", {"session_id": session_id, "approval_id": payload.approval_id})

    model = str(record.get("model") or "joi")
    return _sse_response(
        _agent_events(owner_id, session_id, model, history, last_user, seeded_calls=seeded)
    )


def _sse_response(events) -> StreamingResponse:
    async def stream():
        async for event_name, data in events:
            yield _sse_event(event_name, data)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


def _add_turn_context(history: List[Dict[str, str]], owner_id: str, query: str) -> None:
    """Append the per-turn system context: current time, the user's stored memory,
    and knowledge-base excerpts relevant to `query`."""
    history.append({"role": "system", "content": _current_time_context_prompt()})
    memory = _read_user_memory(owner_id)
    memory_segments = []
    if memory.get("preferences"):
        memory_segments.append(f"User preferences:\n{memory['preferences'][:1800]}")
    if memory.get("notes"):
        memory_segments.append(f"User notes:\n{memory['notes'][:2200]}")
    if memory_segments:
        history.append(
            {
                "role": "system",
                "content": "Use stored user memory when helpful:\n\n" + "\n\n".join(memory_segments)
            }
        )

    rag_hits = _rag_search(owner_id, query, top_k=3) if query else []
    if rag_hits:
        rag_context_lines = []
        for hit in rag_hits:
            rag_context_lines.append(
                f"Source: {str(hit.get('file_path') or '')} (chunk {int(hit.get('chunk_index') or 0)})\n"
                f"{str(hit.get('text') or '')[:1000]}"
            )
        history.append(
            {
                "role": "system",
                "content": "Relevant local knowledge base excerpts:\n\n" + "\n\n---\n\n".join(rag_context_lines)
            }
        )


@app.websocket("/ws/notifications")
async def ws_notifications(websocket: WebSocket, token: str = Query(default="")):
    # Browsers can't set Authorization on a WS handshake, so the JWT comes as a
    # query param. Validate before accepting.
    try:
        payload = _jwt_decode(str(token or ""))
        user = _fetch_user_by_id(str(payload.get("sub", "")))
    except Exception:
        await websocket.close(code=1008)
        return
    if not user:
        await websocket.close(code=1008)
        return

    user_id = str(user["_id"])
    await websocket.accept()
    await notification_hub.connect(user_id, websocket)
    try:
        # Greet + deliver current unread count so the client can seed its badge.
        unread = notifications_collection.count_documents({"owner_user_id": user_id, "read": False})
        await websocket.send_json({"type": "hello", "unread": int(unread)})
        while True:
            # Keep the socket open; ignore any client pings.
            await websocket.receive_text()
    except WebSocketDisconnect:
        notification_hub.disconnect(user_id, websocket)
    except Exception:
        notification_hub.disconnect(user_id, websocket)
        try:
            await websocket.close()
        except Exception:
            pass


@app.on_event("startup")
async def _startup_background_workers():
    global background_scheduler_task
    if background_scheduler_task and not background_scheduler_task.done():
        return
    background_scheduler_stop.clear()
    background_scheduler_task = asyncio.create_task(_background_scheduler_loop())
    logger.info("Background scheduler started.")
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    try:
        doc = users_collection.find_one({"wake_word.enabled": True}, {"_id": 1})
        if doc:
            wake_word.listener.start(str(doc["_id"]), _on_wake)
    except Exception as exc:
        logger.warning("Could not start the wake word: %s", exc)
    # Pre-load the local model and process the system prompt in the background, so the
    # user's first message doesn't wait for a cold start. Kept referenced so it isn't GC'd.
    global _llm_warmup_task
    _llm_warmup_task = asyncio.create_task(warm_up_llm(system_prompt))
    # Resume phone channels for everyone who connected Telegram.
    try:
        for doc in integrations_collection.find({"provider": "telegram"}, {"owner_user_id": 1}):
            telegram_channel.ensure(str(doc.get("owner_user_id")))
    except Exception as exc:
        logger.warning("Could not start Telegram channels: %s", exc)


_llm_warmup_task: Optional[asyncio.Task] = None


@app.on_event("shutdown")
async def _shutdown_background_workers():
    global background_scheduler_task
    wake_word.listener.stop()
    background_scheduler_stop.set()
    if background_scheduler_task:
        try:
            await asyncio.wait_for(background_scheduler_task, timeout=3.0)
        except Exception:
            pass
        background_scheduler_task = None
    logger.info("Background scheduler stopped.")
    telegram_channel.stop_all()
    await unload_llm()


@app.get("/health")
async def health():
    mongo_ok = False
    redis_ok = False

    try:
        mongo_client.admin.command("ping")
        mongo_ok = True
    except Exception:
        mongo_ok = False

    try:
        redis_client.ping()
        redis_ok = True
    except Exception:
        redis_ok = False

    return {
        "status": "ok" if (mongo_ok and redis_ok) else "degraded",
        "models": {
            "openai": "OPENAI_API_KEY" in os.environ,
            # "gemini": "GEMINI_API_KEY" in os.environ
        },
        "storage": {
            "mongo": mongo_ok,
            "redis": redis_ok
        },
        "workers": {
            "scheduler_running": bool(background_scheduler_task and not background_scheduler_task.done())
        }
    }


def _serve() -> None:
    import uvicorn
    # Bind to loopback only. This backend is a local companion process for the
    # desktop app; exposing it on 0.0.0.0 would let any host on the LAN reach the
    # tool engine. Override with JOI_BIND_HOST only for intentional server deploys.
    bind_host = os.getenv("JOI_BIND_HOST", "127.0.0.1")
    bind_port = int(os.getenv("JOI_BIND_PORT", "8000"))
    uvicorn.run(app, host=bind_host, port=bind_port)


if __name__ == "__main__":
    if "--reload" in sys.argv:
        # Development only: restart automatically when a backend .py file is saved.
        # Not uvicorn's own reload: on Windows it stops the worker with a console-wide
        # Ctrl+C that also kills the reloader. watchfiles terminates just the child.
        from watchfiles import PythonFilter, run_process

        backend_dir = Path(__file__).resolve().parent
        os.chdir(backend_dir)
        ignore = [str(backend_dir / d) for d in ("venv", ".build-venv", "build", "dist", "__pycache__")]
        print("Auto-reload on: the backend restarts when you save a .py file.")
        run_process(backend_dir, target=_serve, watch_filter=PythonFilter(ignore_paths=ignore))
    else:
        _serve()
