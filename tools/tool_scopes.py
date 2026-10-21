"""Where each tool must run in the hybrid architecture.

LOCAL  -> executes on the user's desktop agent (their machine). Secret-free.
CLOUD  -> executes on the broker with the user's per-user OAuth token / server keys.

The orchestrator uses this to decide: run a CLOUD tool in-process, or dispatch a
LOCAL tool to the connected desktop agent. In today's single-process dev mode
everything runs in one place, but classifying now keeps the eventual split
mechanical and prevents accidentally running system control on a shared server.
"""

LOCAL_TOOLS = {
    "app",
    "sysinfo",
    "fs_list", "fs_read", "fs_write", "fs_mkdir", "fs_find",
    "clipboard_read", "clipboard_write",
    "screenshot",
    "media", "brightness",
    "window_list", "window_focus", "window_close",
    "open_path",
    "type_text", "press_keys", "mouse_click",
    "power",
    "shell",
    "whatsapp",
}

CLOUD_TOOLS = {
    "search",
    "email",
    "calendar",
    "drive_search", "drive_read",
    "docs_create", "docs_read",
    "sheets_read", "sheets_append",
    "meet_create",
    "contacts_search",
    "slack_send", "slack_channels", "slack_read",
}


def tool_scope(name: str) -> str:
    """Return 'local', 'cloud', or 'unknown' for a tool name."""
    if name in LOCAL_TOOLS:
        return "local"
    if name in CLOUD_TOOLS:
        return "cloud"
    return "unknown"


def is_local_tool(name: str) -> bool:
    return name in LOCAL_TOOLS
