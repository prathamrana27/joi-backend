from .app_control import open_app
from .web_search import search_and_crawl
from .system_info import system_info
from .email_sender import send_email
from .calendar_tasks import manage_calendar_tasks
from .whatsapp_automation import send_whatsapp_message  # <-- ADD THIS LINE

from .file_system import (
    list_directory,
    read_file,
    write_file,
    create_directory,
    find_files
)
from .system_control import (
    clipboard_read,
    clipboard_write,
    screenshot,
    media_control,
    set_brightness,
    window_list,
    window_focus,
    window_close,
    open_path,
    type_text,
    press_keys,
    mouse_click,
    power,
    run_shell,
)
from .google_workspace import (
    drive_search,
    drive_read,
    docs_create,
    docs_read,
    sheets_read,
    sheets_append,
    meet_create,
    contacts_search,
)
from .slack_tool import slack_send, slack_list_channels, slack_read
from .tool_registry import ToolRegistry

tool_registry = ToolRegistry()

tool_registry.register(
    name="app",
    function=open_app
)

tool_registry.register(
    name="search",
    function=search_and_crawl
)

# Register System Info
tool_registry.register(
    name="sysinfo",
    function=system_info
)

# Register File System Tools
tool_registry.register(
    name="fs_list",
    function=list_directory
)
tool_registry.register(
    name="fs_read",
    function=read_file
)
tool_registry.register(
    name="fs_write",
    function=write_file
)
tool_registry.register(
    name="fs_mkdir",
    function=create_directory
)
tool_registry.register(
    name="fs_find",
    function=find_files
)

# Register Email Tool
tool_registry.register(
    name="email",
    function=send_email
)

# Register WhatsApp Tool
tool_registry.register(                # <-- ADD THIS BLOCK
    name="whatsapp",
    function=send_whatsapp_message
)

# Register the new Calendar/Tasks tool
tool_registry.register(
    name="calendar", # Tool name the AI will use
    function=manage_calendar_tasks
)

# ---- System control tools ----
tool_registry.register(name="clipboard_read", function=clipboard_read)
tool_registry.register(name="clipboard_write", function=clipboard_write)
tool_registry.register(name="screenshot", function=screenshot)
tool_registry.register(name="media", function=media_control)
tool_registry.register(name="brightness", function=set_brightness)
tool_registry.register(name="window_list", function=window_list)
tool_registry.register(name="window_focus", function=window_focus)
tool_registry.register(name="window_close", function=window_close)
tool_registry.register(name="open_path", function=open_path)
tool_registry.register(name="type_text", function=type_text)
tool_registry.register(name="press_keys", function=press_keys)
tool_registry.register(name="mouse_click", function=mouse_click)
tool_registry.register(name="power", function=power)
tool_registry.register(name="shell", function=run_shell)

# ---- Google Workspace tools ----
tool_registry.register(name="drive_search", function=drive_search)
tool_registry.register(name="drive_read", function=drive_read)
tool_registry.register(name="docs_create", function=docs_create)
tool_registry.register(name="docs_read", function=docs_read)
tool_registry.register(name="sheets_read", function=sheets_read)
tool_registry.register(name="sheets_append", function=sheets_append)
tool_registry.register(name="meet_create", function=meet_create)
tool_registry.register(name="contacts_search", function=contacts_search)

# ---- Slack tools ----
tool_registry.register(name="slack_send", function=slack_send)
tool_registry.register(name="slack_channels", function=slack_list_channels)
tool_registry.register(name="slack_read", function=slack_read)

__all__ = ['tool_registry']