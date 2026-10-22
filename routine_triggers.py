"""Event triggers for routines: run a routine when something happens, not only on a timer.

Trigger types (routine["trigger"]):
  {"type": "schedule"}                         every interval_minutes (original behaviour)
  {"type": "email",   "value": "priya@x.com"}  a new email from this sender (address or name)
  {"type": "meeting", "value": "15"}           N minutes before each calendar event
  {"type": "battery", "value": "20"}           battery falls to N% while unplugged

Each check returns (contexts, new_state). A context is a one-line description of what just
happened; the caller runs the routine once per context. State keeps it to once per event and
never fires for things that already existed when the routine was created.
"""

import datetime
from typing import Any, Dict, List, Optional, Tuple

import psutil
from googleapiclient.discovery import build

TRIGGER_TYPES = ("schedule", "email", "meeting", "battery")
Result = Tuple[List[str], Dict[str, Any]]


def validate(trigger_type: str, value: str) -> Optional[str]:
    """Return an error message, or None if the trigger is valid."""
    value = (value or "").strip()
    if trigger_type == "email" and not value:
        return "Enter the sender's email address or name."
    if trigger_type == "meeting" and not (value.isdigit() and 1 <= int(value) <= 240):
        return "Minutes before the meeting must be between 1 and 240."
    if trigger_type == "battery" and not (value.isdigit() and 5 <= int(value) <= 95):
        return "Battery level must be between 5 and 95%."
    return None


def describe(trigger: Dict[str, Any], interval_minutes: int) -> str:
    t, v = trigger.get("type", "schedule"), trigger.get("value", "")
    if t == "email":
        return f"when an email from {v} arrives"
    if t == "meeting":
        return f"{v} min before each meeting"
    if t == "battery":
        return f"when battery drops to {v}%"
    return f"every {interval_minutes} min"


def check_battery(trigger: Dict[str, Any], state: Dict[str, Any]) -> Result:
    battery = psutil.sensors_battery()
    if battery is None:  # desktop PC without a battery
        return [], state
    below = int(trigger.get("value") or 20)
    percent, plugged = round(battery.percent), bool(battery.power_plugged)
    latched = bool(state.get("latched"))
    if plugged or percent > below + 5:
        return [], {**state, "latched": False}  # re-arm once charged or plugged in
    if percent <= below and not latched:
        return [f"Battery is at {percent}% and not charging."], {**state, "latched": True}
    return [], state


def check_email(trigger: Dict[str, Any], state: Dict[str, Any], creds) -> Result:
    sender = str(trigger.get("value") or "").replace('"', "").strip()
    gmail = build("gmail", "v1", credentials=creds, cache_discovery=False)
    found = gmail.users().messages().list(userId="me", q=f'from:("{sender}") newer_than:2d', maxResults=10).execute()
    ids = [m["id"] for m in found.get("messages", [])]
    if "seen" not in state:  # first check: remember what's there, don't fire for old mail
        return [], {**state, "seen": ids[:200]}
    seen = set(state["seen"])
    new_ids = [i for i in ids if i not in seen]
    contexts = []
    for mid in new_ids[:3]:
        meta = gmail.users().messages().get(
            userId="me", id=mid, format="metadata", metadataHeaders=["From", "Subject"]
        ).execute()
        headers = {h["name"]: h["value"] for h in meta.get("payload", {}).get("headers", [])}
        contexts.append(f'New email from {headers.get("From", sender)}: "{headers.get("Subject", "(no subject)")}"')
    return contexts, {**state, "seen": (new_ids + state["seen"])[:200]}


def check_meeting(trigger: Dict[str, Any], state: Dict[str, Any], creds, now: datetime.datetime) -> Result:
    minutes = int(trigger.get("value") or 15)
    cal = build("calendar", "v3", credentials=creds, cache_discovery=False)
    events = cal.events().list(
        calendarId="primary",
        timeMin=now.isoformat(),
        timeMax=(now + datetime.timedelta(minutes=minutes)).isoformat(),
        singleEvents=True,
        orderBy="startTime",
        maxResults=10,
    ).execute().get("items", [])
    notified = list(state.get("notified", []))
    contexts = []
    for ev in events:
        start = ev.get("start", {}).get("dateTime")  # skip all-day events
        if not start or ev.get("id") in notified:
            continue
        when = datetime.datetime.fromisoformat(start.replace("Z", "+00:00")).astimezone().strftime("%H:%M")
        contexts.append(f'Meeting "{ev.get("summary", "Untitled")}" starts at {when}.')
        notified.insert(0, ev.get("id"))
    return contexts, {**state, "notified": notified[:100]}
