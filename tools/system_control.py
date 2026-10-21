"""System-control tools: clipboard, screenshots, media/volume/brightness,
window & app management, open paths, desktop automation, power, and shell.

Windows-focused. Uses PowerShell for OS integration (no extra deps) and
pyautogui for keyboard/mouse. Every handler returns a short status string.

Dangerous handlers (shell, keyboard/mouse automation, power, closing windows)
are gated by the per-user permission system in api_server (require_approval by
default) — this module just performs the action it is asked to.
"""

import os
import time
import shutil
import logging
import subprocess
import webbrowser
from typing import Dict, Any

logger = logging.getLogger(__name__)

try:
    import pyautogui
    pyautogui.FAILSAFE = False
    _HAS_GUI = True
except Exception:  # pragma: no cover - headless/import failure
    _HAS_GUI = False


def _run_powershell(script: str, timeout: int = 15) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script],
        capture_output=True, text=True, timeout=timeout,
    )


# ---------------------------------------------------------------- clipboard
def clipboard_read(args: Dict[str, Any]) -> str:
    try:
        r = _run_powershell("Get-Clipboard -Raw")
        text = (r.stdout or "").rstrip("\n")
        return f"Clipboard contents:\n{text}" if text else "Clipboard is empty."
    except Exception as e:
        return f"Error reading clipboard: {e}"


def clipboard_write(args: Dict[str, Any]) -> str:
    text = str(args.get("text", ""))
    if not text:
        return "Error: Missing 'text' to copy."
    try:
        # Pass via stdin to avoid quoting issues.
        p = subprocess.run(
            ["powershell", "-NoProfile", "-Command", "$input | Set-Clipboard"],
            input=text, capture_output=True, text=True, timeout=10,
        )
        if p.returncode != 0:
            return f"Error setting clipboard: {p.stderr.strip()}"
        return "Copied text to clipboard."
    except Exception as e:
        return f"Error writing clipboard: {e}"


# ---------------------------------------------------------------- screenshot
def screenshot(args: Dict[str, Any]) -> str:
    try:
        from .file_system import AI_WORKSPACE_DIR
        if AI_WORKSPACE_DIR is None:
            return "Error: Workspace directory not available for saving the screenshot."
        if not _HAS_GUI:
            return "Error: Screenshot support (pyautogui/Pillow) is not available."
        name = f"screenshot_{int(time.time())}.png"
        path = AI_WORKSPACE_DIR / name
        img = pyautogui.screenshot()
        img.save(str(path))
        return f"Saved screenshot to workspace: {name}"
    except Exception as e:
        return f"Error taking screenshot: {e}"


# ---------------------------------------------------------------- media / volume / brightness
_MEDIA_KEYS = {
    "playpause": "playpause", "play": "playpause", "pause": "playpause",
    "next": "nexttrack", "prev": "prevtrack", "previous": "prevtrack",
    "stop": "stop", "mute": "volumemute",
    "volup": "volumeup", "voldown": "volumedown",
}


def media_control(args: Dict[str, Any]) -> str:
    action = str(args.get("action", "")).strip().lower()
    key = _MEDIA_KEYS.get(action)
    if not key:
        return f"Error: Unknown media action '{action}'. Use: {', '.join(sorted(_MEDIA_KEYS))}."
    if not _HAS_GUI:
        return "Error: Media control requires pyautogui."
    try:
        repeat = 1
        if action in ("volup", "voldown"):
            repeat = max(1, min(int(args.get("steps", 5)), 50))
        for _ in range(repeat):
            pyautogui.press(key)
        return f"Media action '{action}' sent."
    except Exception as e:
        return f"Error with media action: {e}"


def set_brightness(args: Dict[str, Any]) -> str:
    try:
        level = int(args.get("level", -1))
    except (TypeError, ValueError):
        return "Error: 'level' must be an integer 0-100."
    if not 0 <= level <= 100:
        return "Error: 'level' must be between 0 and 100."
    try:
        r = _run_powershell(
            f"(Get-WmiObject -Namespace root/WMI -Class WmiMonitorBrightnessMethods)."
            f"WmiSetBrightness(1,{level})"
        )
        if r.returncode != 0:
            return f"Error setting brightness (may be unsupported on this display): {r.stderr.strip()}"
        return f"Brightness set to {level}%."
    except Exception as e:
        return f"Error setting brightness: {e}"


# ---------------------------------------------------------------- windows / apps / paths
def window_list(args: Dict[str, Any]) -> str:
    try:
        r = _run_powershell(
            "Get-Process | Where-Object {$_.MainWindowTitle} | "
            "Select-Object -First 40 ProcessName,MainWindowTitle | Format-Table -AutoSize | Out-String"
        )
        out = (r.stdout or "").strip()
        return f"Open windows:\n{out}" if out else "No visible windows found."
    except Exception as e:
        return f"Error listing windows: {e}"


def window_focus(args: Dict[str, Any]) -> str:
    title = str(args.get("title", "")).strip()
    if not title:
        return "Error: Missing 'title' of the window to focus."
    try:
        safe = title.replace("'", "''")
        r = _run_powershell(
            f"$w = New-Object -ComObject WScript.Shell; "
            f"$ok = $w.AppActivate('{safe}'); Write-Output $ok"
        )
        return f"Focused window matching '{title}'." if "True" in (r.stdout or "") \
            else f"Could not find a window matching '{title}'."
    except Exception as e:
        return f"Error focusing window: {e}"


def window_close(args: Dict[str, Any]) -> str:
    title = str(args.get("title", "")).strip()
    if not title:
        return "Error: Missing 'title' of the window to close."
    if not _HAS_GUI:
        return "Error: Closing windows requires pyautogui."
    try:
        focus_res = window_focus({"title": title})
        if "Could not find" in focus_res:
            return focus_res
        time.sleep(0.4)
        pyautogui.hotkey("alt", "f4")
        return f"Sent close (Alt+F4) to window '{title}'."
    except Exception as e:
        return f"Error closing window: {e}"


def open_path(args: Dict[str, Any]) -> str:
    target = str(args.get("path") or args.get("url") or "").strip()
    if not target:
        return "Error: Missing 'path' or 'url' to open."
    try:
        if target.lower().startswith(("http://", "https://")):
            webbrowser.open(target)
            return f"Opened URL: {target}"
        expanded = os.path.expandvars(os.path.expanduser(target))
        if not os.path.exists(expanded):
            return f"Error: Path does not exist: {target}"
        os.startfile(expanded)  # noqa: S606 - intentional OS open
        return f"Opened: {target}"
    except Exception as e:
        return f"Error opening '{target}': {e}"


# ---------------------------------------------------------------- desktop automation
def type_text(args: Dict[str, Any]) -> str:
    text = str(args.get("text", ""))
    if not text:
        return "Error: Missing 'text' to type."
    if not _HAS_GUI:
        return "Error: Typing requires pyautogui."
    try:
        time.sleep(0.3)
        pyautogui.typewrite(text, interval=0.02)
        return f"Typed {len(text)} characters."
    except Exception as e:
        return f"Error typing text: {e}"


def press_keys(args: Dict[str, Any]) -> str:
    keys = args.get("keys")
    if isinstance(keys, str):
        keys = [k.strip() for k in keys.split("+") if k.strip()]
    if not keys or not isinstance(keys, list):
        return "Error: Provide 'keys' as a list or '+'-separated string (e.g. 'ctrl+c')."
    if not _HAS_GUI:
        return "Error: Key presses require pyautogui."
    try:
        pyautogui.hotkey(*[str(k).lower() for k in keys])
        return f"Pressed: {'+'.join(map(str, keys))}."
    except Exception as e:
        return f"Error pressing keys: {e}"


def mouse_click(args: Dict[str, Any]) -> str:
    if not _HAS_GUI:
        return "Error: Mouse control requires pyautogui."
    try:
        button = str(args.get("button", "left")).lower()
        if "x" in args and "y" in args:
            pyautogui.click(int(args["x"]), int(args["y"]), button=button)
            return f"Clicked {button} at ({args['x']}, {args['y']})."
        pyautogui.click(button=button)
        return f"Clicked {button} at current position."
    except Exception as e:
        return f"Error clicking: {e}"


# ---------------------------------------------------------------- power
def power(args: Dict[str, Any]) -> str:
    action = str(args.get("action", "")).strip().lower()
    cmds = {
        "lock": ["rundll32.exe", "user32.dll,LockWorkStation"],
        "sleep": ["rundll32.exe", "powrprof.dll,SetSuspendState", "0,1,0"],
        "restart": ["shutdown", "/r", "/t", "10"],
        "shutdown": ["shutdown", "/s", "/t", "10"],
        "cancel": ["shutdown", "/a"],
    }
    cmd = cmds.get(action)
    if not cmd:
        return f"Error: Unknown power action '{action}'. Use: {', '.join(cmds)}."
    try:
        subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if action in ("restart", "shutdown"):
            return f"System {action} scheduled in 10s. Say 'power cancel' to abort."
        return f"Power action '{action}' executed."
    except Exception as e:
        return f"Error with power action: {e}"


# ---------------------------------------------------------------- shell
def run_shell(args: Dict[str, Any]) -> str:
    command = str(args.get("command", "")).strip()
    if not command:
        return "Error: Missing 'command' to run."
    shell = str(args.get("shell", "powershell")).lower()
    try:
        if shell == "cmd":
            exe = ["cmd", "/c", command]
        else:
            exe = ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command]
        p = subprocess.run(exe, capture_output=True, text=True, timeout=int(args.get("timeout", 30)))
        out = (p.stdout or "").strip()
        err = (p.stderr or "").strip()
        parts = [f"exit_code: {p.returncode}"]
        if out:
            parts.append(f"stdout:\n{out[:6000]}")
        if err:
            parts.append(f"stderr:\n{err[:2000]}")
        return "\n".join(parts)
    except subprocess.TimeoutExpired:
        return "Error: Command timed out."
    except Exception as e:
        return f"Error running command: {e}"
