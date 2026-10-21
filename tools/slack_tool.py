"""Slack tools: send messages, list channels, read recent messages.

Requires a Slack bot token in the SLACK_BOT_TOKEN env var (starts with xoxb-).
Create one at https://api.slack.com/apps → OAuth & Permissions → Bot Token
Scopes: chat:write, channels:read, channels:history, groups:read, users:read.
"""

import logging
from typing import Dict, Any

import httpx

logger = logging.getLogger(__name__)

SLACK_API = "https://slack.com/api"


def _token(args: Dict[str, Any] | None = None) -> str:
    # Multi-tenant: each user's own OAuth token is injected per request by the
    # chat loop. There is no shared/global token.
    return str((args or {}).get("_token", "")).strip()


def _bearer(args: Dict[str, Any] | None = None) -> Dict[str, str]:
    return {"Authorization": f"Bearer {_token(args)}"}


def _headers(args: Dict[str, Any] | None = None) -> Dict[str, str]:
    return {"Authorization": f"Bearer {_token(args)}", "Content-Type": "application/json; charset=utf-8"}


def _resolve_channel(client: httpx.Client, channel: str, args: Dict[str, Any] | None = None) -> str:
    """Accept a channel ID (C…/G…/D…) or a #name / name and return an ID."""
    ch = channel.strip().lstrip("#")
    if ch[:1] in ("C", "G", "D") and ch.isupper():
        return ch
    resp = client.get(
        f"{SLACK_API}/conversations.list",
        headers=_bearer(args),
        params={"limit": 1000, "types": "public_channel,private_channel"},
    ).json()
    for c in resp.get("channels", []):
        if c.get("name") == ch:
            return c.get("id")
    return channel  # fall back; Slack may still accept a name


_NOT_CONNECTED = "Error: Slack isn't connected. Connect your Slack account in Settings → Connectors."


def slack_send(args: Dict[str, Any]) -> str:
    if not _token(args):
        return _NOT_CONNECTED
    channel = str(args.get("channel", "")).strip()
    message = str(args.get("message", ""))
    if not channel or not message:
        return "Error: Provide 'channel' (e.g. #general) and 'message'."
    try:
        with httpx.Client(timeout=15) as client:
            cid = _resolve_channel(client, channel, args)
            r = client.post(
                f"{SLACK_API}/chat.postMessage",
                headers=_headers(args),
                json={"channel": cid, "text": message},
            ).json()
        if not r.get("ok"):
            return f"Error: Slack rejected the message: {r.get('error', 'unknown')}."
        return f"Sent message to {channel}."
    except Exception as e:
        return f"Error sending Slack message: {e}"


def slack_list_channels(args: Dict[str, Any]) -> str:
    if not _token(args):
        return _NOT_CONNECTED
    try:
        with httpx.Client(timeout=15) as client:
            r = client.get(
                f"{SLACK_API}/conversations.list",
                headers=_bearer(args),
                params={"limit": 200, "types": "public_channel,private_channel"},
            ).json()
        if not r.get("ok"):
            return f"Error: {r.get('error', 'unknown')}."
        chans = [f"#{c.get('name')} (id={c.get('id')})" for c in r.get("channels", [])]
        return "Slack channels:\n" + "\n".join(chans) if chans else "No channels found."
    except Exception as e:
        return f"Error listing Slack channels: {e}"


def slack_read(args: Dict[str, Any]) -> str:
    if not _token(args):
        return _NOT_CONNECTED
    channel = str(args.get("channel", "")).strip()
    limit = max(1, min(int(args.get("limit", 10) or 10), 50))
    if not channel:
        return "Error: Provide 'channel' to read."
    try:
        with httpx.Client(timeout=15) as client:
            cid = _resolve_channel(client, channel, args)
            r = client.get(
                f"{SLACK_API}/conversations.history",
                headers=_bearer(args),
                params={"channel": cid, "limit": limit},
            ).json()
        if not r.get("ok"):
            return f"Error: {r.get('error', 'unknown')}."
        msgs = r.get("messages", [])
        if not msgs:
            return f"No recent messages in {channel}."
        lines = [f"- {m.get('user','?')}: {m.get('text','')}" for m in msgs]
        return f"Recent messages in {channel}:\n" + "\n".join(lines)
    except Exception as e:
        return f"Error reading Slack: {e}"
