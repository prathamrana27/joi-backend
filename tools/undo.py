"""Undo markers: how a tool says "this action can be undone, and here's how".

A tool appends undo_marker(...) to its result. The agent loop strips the marker with
extract_undo() (the model never sees it), stores an undo record for the signed-in user,
and shows an Undo button in the chat. See api_server: _register_undo / POST /undo/{id}.
"""

import json
import re
from typing import Any, Dict, Optional, Tuple

_MARKER_RE = re.compile(r"\s*\[\[UNDO:(\{.*\})\]\]\s*$", re.DOTALL)


def undo_marker(kind: str, label: str, seconds: int, payload: Dict[str, Any]) -> str:
    spec = {"kind": kind, "label": label[:120], "seconds": int(seconds), "payload": payload}
    return "\n[[UNDO:" + json.dumps(spec, ensure_ascii=False) + "]]"


def extract_undo(result: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    match = _MARKER_RE.search(result or "")
    if not match:
        return result, None
    try:
        spec = json.loads(match.group(1))
    except json.JSONDecodeError:
        return result[: match.start()], None
    return result[: match.start()], spec
