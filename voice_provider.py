"""Pluggable voice backend (speech-to-text + text-to-speech).

Lets JOI use ElevenLabs instead of OpenAI for voice, so the app can run with no
OpenAI key at all (chat on Ollama + voice on ElevenLabs).

Configuration (environment variables):
    JOI_VOICE_PROVIDER     "elevenlabs" | "openai" | "" (auto: elevenlabs if key present)
    ELEVENLABS_API_KEY     ElevenLabs API key
    ELEVENLABS_VOICE_ID    default "21m00Tcm4TlvDq8ikWAM" (Rachel)
    ELEVENLABS_TTS_MODEL   default "eleven_multilingual_v2"
    ELEVENLABS_STT_MODEL   default "scribe_v1"
"""

import os
import logging

import httpx

logger = logging.getLogger("voice_provider")

ELEVEN_API_BASE = "https://api.elevenlabs.io/v1"


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def _eleven_key() -> str:
    return _env("ELEVENLABS_API_KEY")


def eleven_enabled() -> bool:
    configured = _env("JOI_VOICE_PROVIDER").lower()
    return bool(_eleven_key()) and configured in ("", "elevenlabs")


# ---- Local Whisper (optional, free, offline STT fallback) -----------------
_local_whisper_model = None


def local_stt_available() -> bool:
    try:
        import faster_whisper  # noqa: F401
        return True
    except Exception:
        return False


def _get_local_whisper():
    global _local_whisper_model
    if _local_whisper_model is not None:
        return _local_whisper_model
    from faster_whisper import WhisperModel
    size = _env("JOI_LOCAL_WHISPER_MODEL", "base")
    # int8 on CPU keeps it light; first use downloads the model (~150MB for base).
    _local_whisper_model = WhisperModel(size, device="cpu", compute_type="int8")
    return _local_whisper_model


async def local_stt(audio_bytes: bytes, filename: str = "audio.webm") -> str:
    """Transcribe locally with faster-whisper (no network, no key)."""
    import asyncio
    import os
    import tempfile

    def _run() -> str:
        model = _get_local_whisper()
        suffix = os.path.splitext(filename)[1] or ".webm"
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(audio_bytes)
                tmp_path = tmp.name
            segments, _info = model.transcribe(tmp_path)
            return " ".join(seg.text.strip() for seg in segments).strip()
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    return await asyncio.to_thread(_run)


def active_provider(openai_available: bool) -> str:
    """Resolve which voice backend to use. Returns 'elevenlabs' | 'openai' | 'none'."""
    configured = _env("JOI_VOICE_PROVIDER").lower()
    if configured == "elevenlabs" or (not configured and _eleven_key()):
        return "elevenlabs" if _eleven_key() else ("openai" if openai_available else "none")
    if configured == "openai":
        return "openai" if openai_available else "none"
    # No explicit config and no ElevenLabs key: fall back to OpenAI if it exists.
    return "openai" if openai_available else "none"


async def eleven_tts(text: str, voice_id: str = "") -> bytes:
    """Synthesize speech with ElevenLabs. Returns MP3 bytes."""
    key = _eleven_key()
    if not key:
        raise RuntimeError("ELEVENLABS_API_KEY is not configured.")
    voice = voice_id or _env("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")
    model = _env("ELEVENLABS_TTS_MODEL", "eleven_multilingual_v2")

    url = f"{ELEVEN_API_BASE}/text-to-speech/{voice}"
    headers = {
        "xi-api-key": key,
        "Content-Type": "application/json",
        "Accept": "audio/mpeg",
    }
    body = {
        "text": text,
        "model_id": model,
        "voice_settings": {"stability": 0.5, "similarity_boost": 0.75},
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(url, headers=headers, json=body)
        if resp.status_code >= 400:
            raise RuntimeError(f"ElevenLabs TTS failed ({resp.status_code}): {resp.text[:300]}")
        return resp.content


async def eleven_stt(audio_bytes: bytes, filename: str = "audio.webm") -> str:
    """Transcribe speech with ElevenLabs Scribe. Returns the recognized text."""
    key = _eleven_key()
    if not key:
        raise RuntimeError("ELEVENLABS_API_KEY is not configured.")
    model = _env("ELEVENLABS_STT_MODEL", "scribe_v1")

    url = f"{ELEVEN_API_BASE}/speech-to-text"
    headers = {"xi-api-key": key}
    files = {"file": (filename or "audio.webm", audio_bytes, "application/octet-stream")}
    data = {"model_id": model}
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(url, headers=headers, data=data, files=files)
        if resp.status_code >= 400:
            raise RuntimeError(f"ElevenLabs STT failed ({resp.status_code}): {resp.text[:300]}")
        payload = resp.json()
    return str(payload.get("text") or "").strip()
