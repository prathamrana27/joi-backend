"""Unified Google authentication for ALL Google tools.

Historically each tool (Gmail / Calendar / Tasks) kept its own token file with
its own narrow scopes, which caused constant re-auth loops ("token missing
required scopes"). This module centralizes auth into ONE token
(`token_google.pickle`) granted the full combined scope set, so the user
authorizes once and every Google tool shares it.

Re-authorize anytime by running:   python -m tools.google_auth
"""

import os
import pickle
import logging
import contextvars
from pathlib import Path
from typing import Optional

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

logger = logging.getLogger(__name__)

# Per-request, per-user credentials. The broker sets this (from the user's stored
# OAuth tokens) before running a Google tool, so each customer uses their OWN
# Google account. When unset, we fall back to the shared local token (dev mode).
_user_creds_var: contextvars.ContextVar = contextvars.ContextVar("user_google_creds", default=None)


def set_user_credentials(creds: Optional[Credentials]) -> None:
    _user_creds_var.set(creds)


def clear_user_credentials() -> None:
    _user_creds_var.set(None)

# One combined scope set covering every Google tool JOI uses.
COMBINED_SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/calendar",        # covers readonly + events
    "https://www.googleapis.com/auth/tasks",           # covers tasks.readonly
    "https://www.googleapis.com/auth/drive",           # search / read / upload
    "https://www.googleapis.com/auth/documents",       # Google Docs
    "https://www.googleapis.com/auth/spreadsheets",    # Google Sheets
    "https://www.googleapis.com/auth/contacts.readonly",  # name -> email
]

TOKEN_FILENAME = "token_google.pickle"

_cached_creds: Optional[Credentials] = None


def _resolve_tools_dir() -> Path:
    override = str(os.getenv("JOI_GOOGLE_TOOLS_DIR", "")).strip()
    if override:
        candidate = Path(override).expanduser()
        candidate.mkdir(parents=True, exist_ok=True)
        return candidate.resolve()
    return Path(__file__).parent.resolve()


def _has_all_scopes(creds: Credentials) -> bool:
    granted = set(creds.scopes or [])
    return all(s in granted for s in COMBINED_SCOPES)


NOT_CONNECTED_MESSAGE = (
    "Google account not connected. Ask the user to connect Google in "
    "Settings → Connectors, then try again."
)


def _shared_token_enabled() -> bool:
    # Developer-only escape hatch. In the product every customer must use their own
    # Google account, so the shared on-disk token is off unless explicitly enabled.
    return str(os.getenv("JOI_GOOGLE_SHARED_TOKEN", "")).strip().lower() in {"1", "true", "yes"}


def get_credentials(force: bool = False, allow_interactive: bool = False) -> Credentials:
    """Return valid combined-scope credentials.

    Priority: the current request's per-user credentials (multi-tenant / product
    mode). Without them, fail fast with a "connect Google" error, unless the
    developer-only shared token is enabled (JOI_GOOGLE_SHARED_TOKEN=1).
    The interactive browser flow only runs from the CLI (`python -m tools.google_auth`):
    inside the server it would block the request forever waiting for a sign-in.
    """
    # Multi-tenant path: use the per-user creds the broker injected for this request.
    user_creds = _user_creds_var.get()
    if user_creds is not None:
        if user_creds.expired and user_creds.refresh_token:
            try:
                user_creds.refresh(Request())
            except Exception as exc:
                raise RuntimeError(f"Google token refresh failed; reconnect Google. ({exc})")
        return user_creds

    if not allow_interactive and not _shared_token_enabled():
        raise RuntimeError(NOT_CONNECTED_MESSAGE)

    global _cached_creds
    if _cached_creds and _cached_creds.valid and not force:
        return _cached_creds

    tools_dir = _resolve_tools_dir()
    token_path = tools_dir / TOKEN_FILENAME
    credentials_path = tools_dir / "credentials.json"

    creds: Optional[Credentials] = None
    if token_path.exists() and not force:
        try:
            with open(token_path, "rb") as fh:
                loaded = pickle.load(fh)
            if isinstance(loaded, Credentials):
                creds = loaded
        except Exception as exc:
            logger.warning("Failed to load %s: %s", TOKEN_FILENAME, exc)

    # Usable as-is?
    if creds and creds.valid and _has_all_scopes(creds) and not force:
        _cached_creds = creds
        return creds

    # Try a silent refresh if we only lack validity (not scopes).
    if creds and creds.expired and creds.refresh_token and _has_all_scopes(creds) and not force:
        try:
            creds.refresh(Request())
            with open(token_path, "wb") as fh:
                pickle.dump(creds, fh)
            _cached_creds = creds
            return creds
        except Exception as exc:
            logger.warning("Token refresh failed, will re-authenticate: %s", exc)

    if not allow_interactive:
        raise RuntimeError(
            "Google account needs (re)authorization with the full scope set. "
            "Run: python -m tools.google_auth"
        )

    if not credentials_path.exists():
        raise RuntimeError(f"credentials.json not found at {credentials_path}")

    flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), COMBINED_SCOPES)
    creds = flow.run_local_server(port=0)
    with open(token_path, "wb") as fh:
        pickle.dump(creds, fh)
    _cached_creds = creds
    logger.info("Google account authorized with combined scopes.")
    return creds


def get_service(service_name: str, api_version: str):
    """Build an authenticated Google API service client."""
    creds = get_credentials()
    return build(service_name, api_version, credentials=creds, cache_discovery=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Authorizing JOI with your Google account (all scopes)…")
    get_credentials(force=True, allow_interactive=True)
    print(f"Done. Saved {TOKEN_FILENAME}. All Google tools now share this authorization.")
