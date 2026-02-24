import os
import json
import uuid
import asyncio
import re
import tempfile
import base64
import hashlib
import hmac
import secrets
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
from typing import Dict, List, Any, Optional
from pathlib import Path
from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    UploadFile,
    File,
    Form,
    HTTPException,
    Depends,
    Header
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
from google.auth.transport import requests as google_auth_requests

# Load env
load_dotenv()

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("api_server")

# ----------- OPENAI DEFAULT -----------
if "OPENAI_API_KEY" not in os.environ:
    logger.error("CRITICAL: OPENAI_API_KEY not found.")
    raise SystemExit(1)

logger.info("OpenAI API key loaded")
title_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# ----------- GEMINI (COMMENTED BACKUP) -----------
"""
import google.generativeai as genai

try:
    genai.configure(api_key=os.environ["GEMINI_API_KEY"])
    logger.info("Gemini initialized successfully")
except KeyError:
    logger.warning("Gemini key not found")
"""

# Import chatbot handlers
from openai_chatbot import send_to_openai
# from gemini_chatbot import send_to_gemini

from tools import tool_registry
from prompts import system_prompt

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

if AUTH_JWT_SECRET == "dev-only-change-me":
    logger.warning("AUTH_JWT_SECRET is using default dev value. Set AUTH_JWT_SECRET in .env.")

app = FastAPI(title="JOI - AI Assistant API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

active_connections: Dict[str, Dict[str, Any]] = {}
background_scheduler_task: Optional[asyncio.Task] = None
background_scheduler_stop = asyncio.Event()

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
        rag_chunks_collection.create_index([("owner_user_id", 1), ("text", "text")])
    except Exception:
        pass
    logger.info("MongoDB connected: %s/%s", MONGO_DB_NAME, MONGO_SESSIONS_COLLECTION)
except Exception as exc:
    logger.error("CRITICAL: MongoDB connection failed (%s)", exc)
    raise SystemExit(1)

try:
    redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    logger.info("Redis connected: %s", REDIS_URL)
except Exception as exc:
    logger.error("CRITICAL: Redis connection failed (%s)", exc)
    raise SystemExit(1)


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


class ApprovalDecisionRequest(BaseModel):
    decision: str = Field(..., pattern="^(approved|denied)$")
    note: str = Field(default="", max_length=1000)


class RoutineCreateRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=120)
    prompt: str = Field(..., min_length=3, max_length=8000)
    interval_minutes: int = Field(default=1440, ge=5, le=10080)
    enabled: bool = Field(default=True)


class RoutineUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=2, max_length=120)
    prompt: Optional[str] = Field(default=None, min_length=3, max_length=8000)
    interval_minutes: Optional[int] = Field(default=None, ge=5, le=10080)
    enabled: Optional[bool] = None


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
            GOOGLE_ID_TOKEN_REQUEST
        )
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=f"Invalid Google credential: {exc}")

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
        if role in allowed_roles and content.strip():
            sanitized.append({"role": role, "content": content})

    if not sanitized or sanitized[0]["role"] != "system":
        sanitized.insert(0, {"role": "system", "content": system_prompt})

    return sanitized


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


DANGEROUS_TOOLS = {"email_sender", "file_system", "app_control", "calendar_tasks"}
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
        notifications_collection.insert_one(
            {
                "owner_user_id": owner_user_id,
                "title": str(title or "").strip()[:180],
                "body": str(body or "").strip()[:4000],
                "category": str(category or "system").strip()[:60],
                "read": False,
                "created_at": _utc_now_iso()
            }
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


def _create_tool_approval(owner_user_id: str, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    now = _utc_now_iso()
    approval_id = f"apr_{uuid.uuid4().hex[:16]}"
    payload = {
        "approval_id": approval_id,
        "owner_user_id": owner_user_id,
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
            {"enabled": True, "next_run_at": {"$lte": now_iso}}
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
            {"status": "pending", "run_at": {"$lte": now_iso}}
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

        if job_type == "routine_execution":
            routine_name = str(payload.get("name") or "Routine")
            prompt = str(payload.get("prompt") or "")
            preview = prompt[:220] if prompt else "Routine executed."
            _create_notification(owner, f"Routine executed: {routine_name}", preview, category="routine")
        elif job_type == "notify":
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


async def _background_scheduler_loop() -> None:
    while not background_scheduler_stop.is_set():
        try:
            _run_scheduler_tick()
        except Exception as exc:
            logger.warning("Background scheduler tick failed: %s", exc)
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


def _extract_visible_messages(history: List[Dict[str, str]]) -> List[Dict[str, str]]:
    visible = []
    for msg in history:
        role = str(msg.get("role", "")).strip().lower()
        content = str(msg.get("content", ""))
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
    now = datetime.now(timezone.utc)
    next_run = now + timedelta(minutes=int(payload.interval_minutes))
    doc = {
        "owner_user_id": owner_id,
        "name": str(payload.name).strip(),
        "prompt": str(payload.prompt).strip(),
        "interval_minutes": int(payload.interval_minutes),
        "enabled": bool(payload.enabled),
        "created_at": _as_iso(now),
        "updated_at": _as_iso(now),
        "last_run_at": "",
        "next_run_at": _as_iso(next_run) if payload.enabled else ""
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
    if payload.enabled is not None:
        update_fields["enabled"] = bool(payload.enabled)
        if payload.enabled:
            update_fields["next_run_at"] = _as_iso(datetime.now(timezone.utc) + timedelta(minutes=int(existing.get("interval_minutes") or 1440)))
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

    suffix = os.path.splitext(file.filename)[1] or ".webm"
    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(blob)
            temp_path = tmp.name

        used_model = model
        with open(temp_path, "rb") as audio_file:
            try:
                transcript = title_client.audio.transcriptions.create(
                    model=model,
                    file=audio_file
                )
            except Exception:
                if model != "whisper-1":
                    used_model = "whisper-1"
                    audio_file.seek(0)
                    transcript = title_client.audio.transcriptions.create(
                        model="whisper-1",
                        file=audio_file
                    )
                else:
                    raise

        text = str(getattr(transcript, "text", "") or "").strip()
        if not text:
            raise HTTPException(status_code=422, detail="No speech text detected.")

        return {"text": text, "model_used": used_model}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Audio transcription failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Transcription failed: {exc}")
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass


@app.post("/audio/speak")
async def speak_text(
    payload: TTSRequest,
    _current_user: Dict[str, Any] = Depends(require_auth_user)
):
    text = str(payload.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Text is required for TTS.")

    # Keep synthesis payload bounded for latency and cost.
    if len(text) > 4000:
        text = text[:4000]

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

    now = datetime.now()
    current_year = str(now.year)
    current_month_year = now.strftime("%B %Y")
    month_pattern = (
        r"\b(?:january|february|march|april|may|june|july|august|"
        r"september|october|november|december)\s+(?:19|20)\d{2}\b"
    )

    # Remove stale month-year/year mentions and append exactly one current month-year.
    fresh_query = re.sub(month_pattern, "", raw_query, flags=re.IGNORECASE)
    fresh_query = re.sub(r"\b(19|20)\d{2}\b", "", fresh_query)
    fresh_query = re.sub(r"\s{2,}", " ", fresh_query).strip(" ,.-")
    fresh_query = f"{fresh_query} {current_month_year}".strip()
    fresh_query = fresh_query.replace(f"{current_year} {current_month_year}", current_month_year)

    normalized["query"] = fresh_query
    return normalized


@app.post("/chat/stream")
async def chat_stream(
    payload: SSEChatRequest,
    current_user: Dict[str, Any] = Depends(require_auth_user)
):
    session_id = str(payload.conversation_id).strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="conversation_id is required.")

    owner_id = str(current_user.get("_id"))
    history = _sanitize_history(payload.history)
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

    rag_hits = _rag_search(owner_id, payload.message, top_k=3)
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

    history.append({"role": "user", "content": payload.message.strip()})
    _write_audit_log(owner_id, "chat_message", "ok", {"session_id": session_id})
    _upsert_session_history(session_id, payload.model, history, owner_id)

    async def stream():
        tool_count = 0
        final_response = ""
        workflow_active = False
        workflow_steps_executed = 0

        try:
            if payload.model.lower() != "openai":
                yield _sse_event(
                    "warning",
                    {"message": f"Model '{payload.model}' is not available. Using openai."}
                )

            yield _sse_event("status", {"message": "SSE stream connected"})

            while tool_count < MAX_CONSECUTIVE_TOOL_CALLS:
                accumulated_response = ""
                chunk_index = 0

                for chunk, accumulated in send_to_openai(history.copy(), stream=True):
                    accumulated_response = accumulated
                    if chunk:
                        yield _sse_event("ai_chunk", {"chunk": chunk, "index": chunk_index})
                        chunk_index += 1
                        await asyncio.sleep(0)

                history.append({"role": "assistant", "content": accumulated_response})
                final_response = accumulated_response
                _upsert_session_history(session_id, payload.model, history, owner_id)

                tool_calls = tool_registry.extract_tool_calls(accumulated_response)
                if not tool_calls:
                    if workflow_active:
                        yield _sse_event(
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
                    yield _sse_event(
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

                    workflow_steps_executed += 1
                    yield _sse_event(
                        "workflow_step_started",
                        {
                            "step_index": workflow_steps_executed,
                            "tool": tool_name,
                            "args": effective_tool_args
                        }
                    )

                    yield _sse_event(
                        "tool_status",
                        {"message": f"Executing tool call {idx}/{len(tool_calls)}: {tool_name}"}
                    )

                    if tool_mode == "deny":
                        block_text = f"Execution denied by security policy for tool '{tool_name}'."
                        yield _sse_event(
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
                            yield _sse_event(
                                "tool_status",
                                {
                                    "message": (
                                        f"Using approved authorization {consumed_approval} "
                                        f"for tool {tool_name}."
                                    )
                                }
                            )
                        else:
                            approval = _create_tool_approval(owner_id, tool_name, effective_tool_args)
                            approval_id = str(approval.get("approval_id") or "")
                            approval_text = (
                                f"Tool '{tool_name}' requires approval before execution. "
                                f"Approval ID: {approval_id}"
                            )
                            yield _sse_event(
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

                    result = await tool_registry.execute(tool_name, effective_tool_args)
                    history.append({
                        "role": "assistant",
                        "content": f"{tool_name} result:\n{result}"
                    })
                    _upsert_session_history(session_id, payload.model, history, owner_id)
                    _write_audit_log(owner_id, "tool_execution", "ok", {"tool_name": tool_name})

                    yield _sse_event(
                        "tool_result",
                        {"tool": tool_name, "args": effective_tool_args, "result": result}
                    )
                    yield _sse_event(
                        "workflow_step_completed",
                        {
                            "step_index": workflow_steps_executed,
                            "tool": tool_name,
                            "result": result
                        }
                    )

                if approval_blocked:
                    if workflow_active:
                        yield _sse_event(
                            "workflow_failed",
                            {"message": "Workflow paused: approval required or blocked by policy."}
                        )
                        workflow_active = False
                    break

                tool_count += 1

            if tool_count >= MAX_CONSECUTIVE_TOOL_CALLS:
                yield _sse_event("warning", {"message": "Max tool call limit reached"})
                if workflow_active:
                    yield _sse_event(
                        "workflow_failed",
                        {"message": "Workflow stopped: max tool call limit reached."}
                    )

            yield _sse_event(
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
                yield _sse_event(
                    "workflow_failed",
                    {"message": f"Workflow failed: {exc}"}
                )
            yield _sse_event("error", {"message": f"Server error: {exc}"})
        finally:
            _upsert_session_history(session_id, payload.model, history, owner_id)
            yield _sse_event("end", {})

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


async def handle_chat_start(websocket: WebSocket, connection_id: str):
    active_connections[connection_id] = {
        "history": [{"role": "system", "content": system_prompt}],
        "model_type": "openai"
    }
    await websocket.send_json({"type": "status", "payload": "New chat started (OpenAI)"})


async def handle_load_chat(
    websocket: WebSocket,
    connection_id: str,
    history: List[Dict[str, Any]],
    model_type: str = "openai"
):
    if not isinstance(history, list):
        await websocket.send_json({"type": "error", "payload": "Invalid chat history payload"})
        return

    sanitized_history: List[Dict[str, str]] = []
    for msg in history:
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role", "")).strip().lower()
        content = str(msg.get("content", ""))
        if role in {"system", "user", "assistant"} and content:
            sanitized_history.append({"role": role, "content": content})

    if not sanitized_history or sanitized_history[0].get("role") != "system":
        sanitized_history.insert(0, {"role": "system", "content": system_prompt})

    active_connections[connection_id] = {
        "history": sanitized_history,
        "model_type": "openai"
    }
    await websocket.send_json({"type": "status", "payload": "Chat history loaded"})


async def handle_user_message(websocket: WebSocket, connection_id: str, message: str):
    if connection_id not in active_connections:
        await handle_chat_start(websocket, connection_id)

    history = active_connections[connection_id]["history"]
    history.append({"role": "user", "content": message})

    await process_message_with_model(websocket, connection_id)


async def process_tool_calls(websocket, ai_response, history):
    tool_calls = tool_registry.extract_tool_calls(ai_response)
    if not tool_calls:
        return False

    for idx, (tool_name, tool_args) in enumerate(tool_calls, start=1):
        await websocket.send_json({
            "type": "tool_status",
            "payload": f"Executing tool call {idx}/{len(tool_calls)}: {tool_name}"
        })

        result = await tool_registry.execute(tool_name, tool_args)

        # OpenAI chat.completions does not accept role="tool" unless using tool_calls.
        # Store tool output as assistant context instead.
        history.append({
            "role": "assistant",
            "content": f"{tool_name} result:\n{result}"
        })

        await websocket.send_json({
            "type": "tool_result",
            "payload": {"tool": tool_name, "args": tool_args, "result": result}
        })

    return True


async def process_message_with_model(websocket, connection_id):
    history = active_connections[connection_id]["history"]
    tool_count = 0

    while tool_count < MAX_CONSECUTIVE_TOOL_CALLS:
        accumulated_response = ""

        for chunk, accumulated in send_to_openai(history.copy(), stream=True):
            accumulated_response = accumulated
            if chunk:
                await websocket.send_json({"type": "ai_chunk", "payload": chunk})

        await websocket.send_json({"type": "stream_end", "payload": ""})

        history.append({"role": "assistant", "content": accumulated_response})

        has_tool = await process_tool_calls(websocket, accumulated_response, history)

        if not has_tool:
            break

        tool_count += 1

    if tool_count >= MAX_CONSECUTIVE_TOOL_CALLS:
        await websocket.send_json({
            "type": "warning",
            "payload": "Max tool call limit reached"
        })


@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await websocket.accept()
    connection_id = f"{client_id}_{uuid.uuid4().hex[:6]}"
    logger.info(f"Connected: {connection_id}")

    try:
        while True:
            data = await websocket.receive_json()
            message_type = data.get("type")

            if message_type == "start_chat":
                await handle_chat_start(websocket, connection_id)

            elif message_type == "load_chat":
                await handle_load_chat(
                    websocket,
                    connection_id,
                    data.get("history", []),
                    data.get("model", "openai")
                )

            elif message_type == "user_message":
                await handle_user_message(
                    websocket,
                    connection_id,
                    data.get("payload", "")
                )

            else:
                await websocket.send_json({
                    "type": "error",
                    "payload": f"Unknown message type: {message_type}"
                })

    except WebSocketDisconnect:
        logger.info(f"Disconnected: {connection_id}")
        active_connections.pop(connection_id, None)
    except Exception as exc:
        logger.exception("Unhandled websocket error for %s: %s", connection_id, exc)
        try:
            await websocket.send_json({"type": "error", "payload": f"Server error: {exc}"})
        except Exception:
            pass
    finally:
        active_connections.pop(connection_id, None)


@app.on_event("startup")
async def _startup_background_workers():
    global background_scheduler_task
    if background_scheduler_task and not background_scheduler_task.done():
        return
    background_scheduler_stop.clear()
    background_scheduler_task = asyncio.create_task(_background_scheduler_loop())
    logger.info("Background scheduler started.")


@app.on_event("shutdown")
async def _shutdown_background_workers():
    global background_scheduler_task
    background_scheduler_stop.set()
    if background_scheduler_task:
        try:
            await asyncio.wait_for(background_scheduler_task, timeout=3.0)
        except Exception:
            pass
        background_scheduler_task = None
    logger.info("Background scheduler stopped.")


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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
