"""JSON-Schema tool definitions for native tool calling (Ollama / OpenAI `tools=[...]`).

Mirrors the TOOL_CALL:: examples in prompts.py. Keep the two in sync when a tool's
arguments change.
"""

from typing import Any, Dict, List


def _tool(name: str, description: str, properties: Dict[str, Any] | None = None, required: List[str] | None = None) -> Dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties or {},
                "required": required or [],
            },
        },
    }


S = {"type": "string"}
I = {"type": "integer"}

TOOL_SCHEMAS: List[Dict[str, Any]] = [
    # General
    _tool("search", "Search the web and read the top results.", {"query": S}, ["query"]),
    _tool("sysinfo", "CPU, RAM, disk, uptime and time ('basic') or network details ('network').",
          {"param": {"type": "string", "enum": ["basic", "network"]}}, ["param"]),
    _tool("app", "Open an installed application by name.", {"app_name": S}, ["app_name"]),
    # Workspace files
    _tool("fs_list", "List files in the JOI workspace folder.", {"relative_path": S}, ["relative_path"]),
    _tool("fs_read", "Read a text file in the JOI workspace.", {"relative_path": S}, ["relative_path"]),
    _tool("fs_write", "Write a text file in the JOI workspace.", {"relative_path": S, "content": S}, ["relative_path", "content"]),
    _tool("fs_mkdir", "Create a folder in the JOI workspace.", {"relative_path": S}, ["relative_path"]),
    _tool("fs_find", "Find files in the JOI workspace by glob pattern.", {"start_path": S, "pattern": S}, ["pattern"]),
    # Email
    _tool(
        "email",
        "Gmail. command_string is key:value pairs separated by semicolons. "
        "Read: 'read:true; query:is:unread; limit:5'. Open one: 'read_full_id:<id>'. "
        "Send (only when asked to send, to an address the user gave): 'to:a@b.com; subject:..; body:..'.",
        {"command_string": S},
        ["command_string"],
    ),
    # Calendar & tasks
    _tool(
        "calendar",
        "Google Calendar & Tasks. command_string is key:value pairs separated by semicolons. Actions: "
        "list_events (days, limit), create_event (summary, start, end as ISO with timezone), update_event, "
        "delete_event (event_id), list_tasklists, list_tasks, create_task (tasklist_id, title, due), "
        "update_task, delete_task. Example: 'action:list_events; days:1; limit:10'.",
        {"command_string": S},
        ["command_string"],
    ),
    # Messaging
    _tool("slack_send", "Send a Slack message to a channel.", {"channel": S, "message": S}, ["channel", "message"]),
    _tool("slack_read", "Read recent messages from a Slack channel.", {"channel": S, "limit": I}, ["channel"]),
    _tool("slack_channels", "List Slack channels."),
    _tool("whatsapp", "Send a WhatsApp message to a contact.", {"contact": S, "message": S}, ["contact", "message"]),
    # Google Workspace
    _tool("drive_search", "Search Google Drive files.", {"query": S, "limit": I}, ["query"]),
    _tool("drive_read", "Read a Google Drive file.", {"file_id": S}, ["file_id"]),
    _tool("docs_create", "Create a Google Doc.", {"title": S, "content": S}, ["title"]),
    _tool("docs_read", "Read a Google Doc.", {"doc_id": S}, ["doc_id"]),
    _tool("sheets_read", "Read a range from a Google Sheet.", {"spreadsheet_id": S, "range": S}, ["spreadsheet_id", "range"]),
    _tool("sheets_append", "Append a row to a Google Sheet.",
          {"spreadsheet_id": S, "range": S, "values": {"type": "array", "items": S}}, ["spreadsheet_id", "values"]),
    _tool("meet_create", "Create a Google Meet link with a calendar event (only when a Meet link is wanted).",
          {"summary": S, "start": S, "end": S}, ["summary", "start", "end"]),
    _tool("contacts_search", "Look up a contact's email address by name.", {"name": S}, ["name"]),
    # Computer control
    _tool("media", "Media keys.", {"action": {"type": "string", "enum": ["playpause", "next", "prev", "mute", "volup", "voldown"]}}, ["action"]),
    _tool("brightness", "Set screen brightness 0-100.", {"level": I}, ["level"]),
    _tool("clipboard_read", "Read the clipboard."),
    _tool("clipboard_write", "Copy text to the clipboard.", {"text": S}, ["text"]),
    _tool("screenshot", "Take a screenshot."),
    _tool("window_list", "List open windows."),
    _tool("window_focus", "Bring a window to the front by title.", {"title": S}, ["title"]),
    _tool("window_close", "Close a window by title.", {"title": S}, ["title"]),
    _tool("open_path", "Open a URL or a file/folder path.", {"url": S, "path": S}),
    _tool("type_text", "Type text as keystrokes.", {"text": S}, ["text"]),
    _tool("press_keys", "Press a key combination like 'ctrl+c'.", {"keys": S}, ["keys"]),
    _tool("mouse_click", "Click the mouse.", {"button": S, "x": I, "y": I}),
    _tool("power", "Lock, sleep, restart or shut down the PC.",
          {"action": {"type": "string", "enum": ["lock", "sleep", "restart", "shutdown", "cancel"]}}, ["action"]),
    _tool("shell", "Run a shell command (only when the user explicitly asks).",
          {"command": S, "shell": {"type": "string", "enum": ["powershell", "cmd"]}}, ["command"]),
]

TOOL_NAMES = [t["function"]["name"] for t in TOOL_SCHEMAS]
