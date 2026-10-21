"""Pluggable LLM backend for JOI.

The chat loop depends on one async generator, ``astream_llm(messages)``, which
yields ``(chunk, accumulated)`` tuples. This module decides which engine answers
(OpenAI, local Ollama, or any OpenAI-compatible endpoint) from environment
configuration, so the model can be swapped without touching ``api_server.py``.

It is async on purpose: a blocking stream would freeze the whole backend (other
requests, notification sockets) for as long as the model is thinking.

Configuration (environment variables):
    JOI_LLM_PROVIDER      "openai" (default) | "ollama" | "openai_compatible"
    JOI_OPENAI_MODEL      default "gpt-4o"
    JOI_OLLAMA_BASE_URL   default "http://127.0.0.1:11434" (a trailing /v1 is tolerated)
    JOI_OLLAMA_MODEL      default "qwen2.5:3b"
    JOI_OLLAMA_NUM_CTX    context window, default "8192" — must exceed the system
                          prompt, or Ollama silently drops part of it
    JOI_OLLAMA_KEEP_ALIVE how long the model stays loaded after a request, default "30m"
    JOI_LLM_BASE_URL / JOI_LLM_API_KEY / JOI_LLM_MODEL   for "openai_compatible"
    JOI_LLM_TEMPERATURE   default "0.6"
    JOI_LLM_MAX_TOKENS    max reply length, default "1024"

Ollama uses its native /api/chat (not the OpenAI-compatible /v1) because only the
native API accepts per-request context size and keep-alive.
"""

import os
import json
import logging

import httpx
from openai import AsyncOpenAI

logger = logging.getLogger("llm_provider")


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


PROVIDER = _env("JOI_LLM_PROVIDER", "openai").lower()
TEMPERATURE = float(_env("JOI_LLM_TEMPERATURE", "0.6") or "0.6")
MAX_TOKENS = int(_env("JOI_LLM_MAX_TOKENS", "1024") or "1024")

OLLAMA_BASE_URL = _env("JOI_OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
if OLLAMA_BASE_URL.endswith("/v1"):
    OLLAMA_BASE_URL = OLLAMA_BASE_URL[:-3]
OLLAMA_MODEL = _env("JOI_OLLAMA_MODEL", "qwen2.5:3b")
OLLAMA_NUM_CTX = int(_env("JOI_OLLAMA_NUM_CTX", "8192") or "8192")
_keep_alive_raw = _env("JOI_OLLAMA_KEEP_ALIVE", "30m") or "30m"
# Ollama wants a number for seconds/forever (-1) and a duration string like "30m" otherwise.
OLLAMA_KEEP_ALIVE = int(_keep_alive_raw) if _keep_alive_raw.lstrip("-").isdigit() else _keep_alive_raw

_openai_client: "AsyncOpenAI | None" = None
_openai_model: str = ""


def _ollama_options() -> dict:
    return {"num_ctx": OLLAMA_NUM_CTX, "num_predict": MAX_TOKENS, "temperature": TEMPERATURE}


def _get_openai_client() -> "tuple[AsyncOpenAI, str]":
    """Create (and cache) the client for the OpenAI / OpenAI-compatible providers."""
    global _openai_client, _openai_model
    if _openai_client is not None:
        return _openai_client, _openai_model

    if PROVIDER == "openai_compatible":
        base_url = _env("JOI_LLM_BASE_URL")
        if not base_url:
            raise RuntimeError("JOI_LLM_BASE_URL is required for openai_compatible provider.")
        _openai_client = AsyncOpenAI(base_url=base_url, api_key=_env("JOI_LLM_API_KEY", "none") or "none")
        _openai_model = _env("JOI_LLM_MODEL", "qwen2.5:3b")
    else:
        api_key = _env("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is required for the openai provider.")
        _openai_client = AsyncOpenAI(api_key=api_key)
        _openai_model = _env("JOI_OPENAI_MODEL", "gpt-4o")
    logger.info("LLM provider: %s (model=%s)", PROVIDER, _openai_model)
    return _openai_client, _openai_model


async def _astream_ollama(messages):
    body = {
        "model": OLLAMA_MODEL,
        "messages": messages,
        "stream": True,
        "keep_alive": OLLAMA_KEEP_ALIVE,
        "options": _ollama_options(),
    }
    accumulated = ""
    # No read timeout: a cold model load can legitimately take a while.
    timeout = httpx.Timeout(connect=10.0, read=None, write=30.0, pool=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        async with client.stream("POST", f"{OLLAMA_BASE_URL}/api/chat", json=body) as resp:
            if resp.status_code >= 400:
                detail = (await resp.aread()).decode("utf-8", "ignore")[:300]
                raise RuntimeError(f"Ollama returned {resp.status_code}: {detail}")
            async for line in resp.aiter_lines():
                if not line:
                    continue
                data = json.loads(line)
                if data.get("error"):
                    raise RuntimeError(data["error"])
                piece = (data.get("message") or {}).get("content") or ""
                if piece:
                    accumulated += piece
                    yield piece, accumulated
                if data.get("done"):
                    break


async def _astream_openai(messages):
    client, model = _get_openai_client()
    accumulated = ""
    stream = await client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
        stream=True,
    )
    async for chunk in stream:
        if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
            piece = chunk.choices[0].delta.content
            accumulated += piece
            yield piece, accumulated


async def astream_llm(messages):
    """Stream a chat completion from the configured provider.

    Yields ``(chunk_text, accumulated_text)``. On error, yields a single
    ``(error_message, error_message)`` tuple so the caller surfaces it in-band.
    """
    try:
        source = _astream_ollama(messages) if PROVIDER == "ollama" else _astream_openai(messages)
        async for piece, accumulated in source:
            yield piece, accumulated
    except Exception as exc:  # noqa: BLE001 - surfaced to the stream deliberately
        error_message = f"JOI couldn't reach its language model ({exc})."
        logger.error("LLM provider '%s' failed: %s", PROVIDER, exc)
        yield error_message, error_message


async def unload() -> None:
    """Free the local model's GPU memory (called when JOI shuts down)."""
    if PROVIDER != "ollama":
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(f"{OLLAMA_BASE_URL}/api/generate", json={"model": OLLAMA_MODEL, "keep_alive": 0})
        logger.info("LLM unloaded from GPU: %s", OLLAMA_MODEL)
    except Exception as exc:  # noqa: BLE001 - best-effort on shutdown
        logger.warning("LLM unload skipped: %s", exc)


async def warm_up(system_prompt: str) -> None:
    """Load the local model and pre-process the system prompt so the first user
    message doesn't pay the cold-start cost. Ollama reuses the processed prompt
    for later requests that start with the same system message."""
    if PROVIDER != "ollama":
        return
    body = {
        "model": OLLAMA_MODEL,
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": "hi"}],
        "stream": False,
        "keep_alive": OLLAMA_KEEP_ALIVE,
        "options": {**_ollama_options(), "num_predict": 1},
    }
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(connect=10.0, read=None, write=30.0, pool=10.0)) as client:
            resp = await client.post(f"{OLLAMA_BASE_URL}/api/chat", json=body)
        data = resp.json()
        logger.info(
            "LLM warm-up done: model=%s prompt_tokens=%s load=%.1fs prompt_eval=%.1fs",
            OLLAMA_MODEL,
            data.get("prompt_eval_count"),
            (data.get("load_duration") or 0) / 1e9,
            (data.get("prompt_eval_duration") or 0) / 1e9,
        )
    except Exception as exc:  # noqa: BLE001 - warm-up is best-effort
        logger.warning("LLM warm-up skipped: %s", exc)
