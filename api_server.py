import os
import json
import uuid
import asyncio
import re
import tempfile
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import logging
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from openai import OpenAI
import redis
from redis.exceptions import RedisError
from pymongo import MongoClient, DESCENDING
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

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
SESSIONS_LIST_CACHE_KEY = "sessions:list"
SESSION_CACHE_PREFIX = "sessions:detail:"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "joi_local")
MONGO_SESSIONS_COLLECTION = os.getenv("MONGO_SESSIONS_COLLECTION", "sessions")
MONGO_FEEDBACK_COLLECTION = os.getenv("MONGO_FEEDBACK_COLLECTION", "message_feedback")

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
REDIS_SESSIONS_LIST_TTL = int(os.getenv("REDIS_SESSIONS_LIST_TTL", "20"))
REDIS_SESSION_TTL = int(os.getenv("REDIS_SESSION_TTL", "120"))

app = FastAPI(title="JOI - AI Assistant API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

active_connections: Dict[str, Dict[str, Any]] = {}

try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    mongo_client.admin.command("ping")
    mongo_db = mongo_client[MONGO_DB_NAME]
    sessions_collection: Collection = mongo_db[MONGO_SESSIONS_COLLECTION]
    feedback_collection: Collection = mongo_db[MONGO_FEEDBACK_COLLECTION]
    sessions_collection.create_index("session_id", unique=True)
    sessions_collection.create_index([("updated_at", DESCENDING)])
    feedback_collection.create_index(
        [("conversation_id", 1), ("message_id", 1)],
        unique=True
    )
    feedback_collection.create_index([("updated_at", DESCENDING)])
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


def _session_cache_key(session_id: str) -> str:
    return f"{SESSION_CACHE_PREFIX}{session_id}"


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


def _invalidate_session_cache(session_id: str) -> None:
    keys = [SESSIONS_LIST_CACHE_KEY, _session_cache_key(session_id)]
    try:
        redis_client.delete(*keys)
    except RedisError as exc:
        logger.warning("Redis delete failed for session '%s': %s", session_id, exc)


def _upsert_session_history(session_id: str, model: str, history: List[Dict[str, str]]) -> None:
    sid = str(session_id).strip()
    if not sid:
        return

    now = _utc_now_iso()
    visible_messages = _extract_visible_messages(history)
    preview = _session_preview_from_messages(visible_messages)

    try:
        existing = sessions_collection.find_one(
            {"session_id": sid},
            {"_id": 0, "title": 1}
        )
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

    _invalidate_session_cache(sid)


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


@app.get("/sessions")
async def list_sessions():
    cached = _cache_get_json(SESSIONS_LIST_CACHE_KEY)
    if cached is not None:
        return cached

    try:
        records = list(
            sessions_collection.find({}, {"_id": 0}).sort("updated_at", DESCENDING)
        )
    except PyMongoError as exc:
        logger.exception("Mongo list sessions failed: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to list sessions.") from exc

    payload = {
        "count": len(records),
        "sessions": [_session_summary(s) for s in records]
    }
    _cache_set_json(SESSIONS_LIST_CACHE_KEY, payload, REDIS_SESSIONS_LIST_TTL)
    return payload


@app.get("/sessions/{session_id}")
async def get_session(session_id: str):
    sid = str(session_id).strip()
    if not sid:
        raise HTTPException(status_code=400, detail="session_id is required.")

    cached = _cache_get_json(_session_cache_key(sid))
    if cached is not None:
        return cached

    try:
        record = sessions_collection.find_one({"session_id": sid}, {"_id": 0})
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
    _cache_set_json(_session_cache_key(sid), payload, REDIS_SESSION_TTL)
    return payload


@app.delete("/sessions/{session_id}")
async def delete_session(session_id: str):
    sid = str(session_id).strip()
    if not sid:
        raise HTTPException(status_code=400, detail="session_id is required.")

    try:
        existing = sessions_collection.find_one({"session_id": sid}, {"_id": 0, "title": 1})
        if not existing:
            raise HTTPException(status_code=404, detail=f"Session '{sid}' not found.")
        sessions_collection.delete_one({"session_id": sid})
    except HTTPException:
        raise
    except PyMongoError as exc:
        logger.exception("Mongo delete failed for '%s': %s", sid, exc)
        raise HTTPException(status_code=500, detail="Failed to delete session.") from exc

    _invalidate_session_cache(sid)
    return {
        "deleted": True,
        "session_id": sid,
        "title": str(existing.get("title") or "New chat")
    }


@app.post("/chat/title")
async def generate_chat_title(payload: TitleRequest):
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
async def submit_message_feedback(payload: MessageFeedbackRequest):
    conversation_id = str(payload.conversation_id).strip()
    message_id = str(payload.message_id).strip()
    feedback = str(payload.feedback).strip().lower()

    if not conversation_id:
        raise HTTPException(status_code=400, detail="conversation_id is required.")
    if not message_id:
        raise HTTPException(status_code=400, detail="message_id is required.")
    if feedback not in {"like", "dislike", "clear"}:
        raise HTTPException(status_code=400, detail="feedback must be like, dislike, or clear.")

    try:
        if feedback == "clear":
            feedback_collection.delete_one(
                {"conversation_id": conversation_id, "message_id": message_id}
            )
            return {
                "saved": True,
                "conversation_id": conversation_id,
                "message_id": message_id,
                "feedback": "clear"
            }

        now = _utc_now_iso()
        feedback_collection.update_one(
            {"conversation_id": conversation_id, "message_id": message_id},
            {
                "$set": {
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
    model: str = Form(default=os.getenv("OPENAI_TRANSCRIBE_MODEL", "whisper-1"))
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
async def speak_text(payload: TTSRequest):
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
async def chat_stream(payload: SSEChatRequest):
    session_id = str(payload.conversation_id).strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="conversation_id is required.")

    history = _sanitize_history(payload.history)
    history.append({"role": "user", "content": payload.message.strip()})
    _upsert_session_history(session_id, payload.model, history)

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
                _upsert_session_history(session_id, payload.model, history)

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

                for idx, (tool_name, tool_args) in enumerate(tool_calls, start=1):
                    effective_tool_args = _normalize_tool_args(tool_name, tool_args)

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

                    result = await tool_registry.execute(tool_name, effective_tool_args)
                    history.append({
                        "role": "assistant",
                        "content": f"{tool_name} result:\n{result}"
                    })
                    _upsert_session_history(session_id, payload.model, history)

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
            _upsert_session_history(session_id, payload.model, history)
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
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
