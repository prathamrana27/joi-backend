"""Reach JOI from your phone through your own Telegram bot.

Each user creates a bot with @BotFather and gives JOI its token. JOI long-polls Telegram
from the user's own PC (no public server needed), so it works anywhere the PC is online.

Safety:
  - Pairing: JOI only listens to the one Telegram chat that sent the 6-digit pairing code.
    Messages from anyone else are ignored without a reply.
  - Stale messages (sent while the PC was off) are not acted on; JOI asks to resend.
  - The agent runs with remote=True, so sensitive actions always wait for approval on the PC.
"""

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Dict, Optional

import httpx

logger = logging.getLogger("telegram_channel")

TELEGRAM_API = "https://api.telegram.org"
POLL_TIMEOUT_S = 25
STALE_AFTER_S = 10 * 60
MAX_MESSAGE_CHARS = 4000  # Telegram's limit is 4096


async def tg_call(token: str, method: str, http_timeout: float = 15.0, **params: Any) -> Any:
    async with httpx.AsyncClient(timeout=http_timeout) as client:
        res = await client.post(f"{TELEGRAM_API}/bot{token}/{method}", json=params)
    data = res.json()
    if not data.get("ok"):
        raise RuntimeError(str(data.get("description") or f"Telegram {method} failed"))
    return data.get("result") or {}


async def get_me(token: str) -> Dict[str, Any]:
    return await tg_call(token, "getMe")


async def send_text(token: str, chat_id: int, text: str) -> None:
    text = text.strip() or "…"
    for start in range(0, len(text), MAX_MESSAGE_CHARS):
        await tg_call(token, "sendMessage", chat_id=chat_id, text=text[start:start + MAX_MESSAGE_CHARS])


Handler = Callable[[str, int, str], Awaitable[str]]


class TelegramChannel:
    """One long-poll task per connected user."""

    def __init__(
        self,
        load: Callable[[str], Optional[Dict[str, Any]]],
        save: Callable[[str, Dict[str, Any]], None],
        handle_message: Handler,
    ) -> None:
        self._load = load
        self._save = save
        self._handle = handle_message
        self._tasks: Dict[str, asyncio.Task] = {}

    def ensure(self, owner_id: str) -> None:
        task = self._tasks.get(owner_id)
        if task and not task.done():
            return
        self._tasks[owner_id] = asyncio.create_task(self._poll(owner_id))

    def stop(self, owner_id: str) -> None:
        task = self._tasks.pop(owner_id, None)
        if task:
            task.cancel()

    def stop_all(self) -> None:
        for owner_id in list(self._tasks):
            self.stop(owner_id)

    async def _poll(self, owner_id: str) -> None:
        backoff = 2
        while True:
            integ = self._load(owner_id)
            if not integ or not integ.get("access_token"):
                return  # disconnected
            token = str(integ["access_token"])
            try:
                updates = await self._get_updates(token, int(integ.get("last_update_id") or 0))
                backoff = 2
                if not updates:
                    # Long polling normally waits server-side; never spin if it returns early.
                    await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Telegram poll failed for %s: %s", owner_id, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            for update in updates:
                update_id = int(update.get("update_id") or 0)
                # Record progress first so a crash mid-handling never replays a command.
                self._save(owner_id, {"last_update_id": update_id})
                try:
                    await self._on_update(owner_id, token, update)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("Telegram message handling failed for %s: %s", owner_id, exc)

    async def _get_updates(self, token: str, last_update_id: int) -> list:
        # Long poll: Telegram holds the request open up to POLL_TIMEOUT_S waiting for messages.
        result = await tg_call(
            token,
            "getUpdates",
            http_timeout=POLL_TIMEOUT_S + 10,
            offset=last_update_id + 1,
            timeout=POLL_TIMEOUT_S,
            allowed_updates=["message"],
        )
        return result if isinstance(result, list) else []

    async def _on_update(self, owner_id: str, token: str, update: Dict[str, Any]) -> None:
        message = update.get("message") or {}
        text = str(message.get("text") or "").strip()
        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        if not text or chat_id is None or chat.get("type") != "private":
            return

        integ = self._load(owner_id) or {}
        paired_chat = integ.get("chat_id")

        if paired_chat is None:
            code = str(integ.get("pairing_code") or "")
            expires = float(integ.get("pairing_expires") or 0)
            if code and code in text and time.time() < expires:
                self._save(owner_id, {"chat_id": chat_id, "pairing_code": "", "paired_at": time.time()})
                await send_text(token, chat_id, "Paired with JOI. Message me here any time and I'll handle it on your PC.")
            # Anyone else gets silence: no hint that this bot belongs to a JOI install.
            return

        if chat_id != paired_chat:
            return

        sent_at = float(message.get("date") or 0)
        if sent_at and time.time() - sent_at > STALE_AFTER_S:
            await send_text(token, chat_id, "Your PC was offline when you sent that, so I didn't act on it. Please send it again.")
            return

        if text in ("/start", "/help"):
            await send_text(token, chat_id, "Ask me anything, like you would on your PC. Sensitive actions wait for your approval in the JOI app.")
            return

        try:
            await tg_call(token, "sendChatAction", chat_id=chat_id, action="typing")
        except Exception:
            pass
        reply = await self._handle(owner_id, chat_id, text)
        await send_text(token, chat_id, reply)
