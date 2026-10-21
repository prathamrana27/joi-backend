"""Google Workspace tools: Drive, Docs, Sheets, Meet, Contacts.

All share the unified auth in google_auth (one token, all scopes).
Each handler returns a human-readable string (or an error string).
"""

import logging
from typing import Dict, Any

from googleapiclient.errors import HttpError

from .google_auth import get_service

logger = logging.getLogger(__name__)


def _err(prefix: str, exc: Exception) -> str:
    if isinstance(exc, HttpError):
        return f"Error: {prefix}: {getattr(exc, 'reason', str(exc))}"
    msg = str(exc)
    if "re-authoriz" in msg.lower() or "scope" in msg.lower() or "credentials.json" in msg.lower():
        return f"Error: {prefix}: Google needs to be reconnected. Ask the user to reconnect Google in Settings → Connectors."
    return f"Error: {prefix}: {msg}"


# ---------------------------------------------------------------- Drive
def drive_search(args: Dict[str, Any]) -> str:
    query = str(args.get("query", "")).strip()
    limit = max(1, min(int(args.get("limit", 10) or 10), 25))
    try:
        svc = get_service("drive", "v3")
        q = f"name contains '{query}' and trashed = false" if query else "trashed = false"
        res = svc.files().list(
            q=q, pageSize=limit, orderBy="modifiedTime desc",
            fields="files(id,name,mimeType,modifiedTime,webViewLink)",
        ).execute()
        files = res.get("files", [])
        if not files:
            return f"No Drive files found for '{query}'."
        lines = [f"- {f['name']}  [{f.get('mimeType','').split('.')[-1]}]  id={f['id']}" for f in files]
        return f"Drive files matching '{query}':\n" + "\n".join(lines)
    except Exception as exc:
        return _err("Drive search failed", exc)


def drive_read(args: Dict[str, Any]) -> str:
    file_id = str(args.get("file_id", "")).strip()
    if not file_id:
        return "Error: Missing 'file_id' (use drive_search to find it)."
    try:
        svc = get_service("drive", "v3")
        meta = svc.files().get(fileId=file_id, fields="name,mimeType").execute()
        mime = meta.get("mimeType", "")
        name = meta.get("name", "file")
        if mime == "application/vnd.google-apps.document":
            data = svc.files().export(fileId=file_id, mimeType="text/plain").execute()
            text = data.decode("utf-8", "ignore") if isinstance(data, bytes) else str(data)
        elif mime.startswith("text/") or mime in ("application/json",):
            data = svc.files().get_media(fileId=file_id).execute()
            text = data.decode("utf-8", "ignore") if isinstance(data, bytes) else str(data)
        else:
            return f"'{name}' is a {mime} file; only Google Docs / text files can be read as text."
        return f"Content of '{name}':\n{text[:8000]}"
    except Exception as exc:
        return _err("Drive read failed", exc)


# ---------------------------------------------------------------- Docs
def docs_create(args: Dict[str, Any]) -> str:
    title = str(args.get("title", "")).strip() or "Untitled document"
    content = str(args.get("content", ""))
    try:
        docs = get_service("docs", "v1")
        doc = docs.documents().create(body={"title": title}).execute()
        doc_id = doc.get("documentId")
        if content:
            docs.documents().batchUpdate(
                documentId=doc_id,
                body={"requests": [{"insertText": {"location": {"index": 1}, "text": content}}]},
            ).execute()
        return f"Created Google Doc '{title}': https://docs.google.com/document/d/{doc_id}/edit"
    except Exception as exc:
        return _err("Docs create failed", exc)


def docs_read(args: Dict[str, Any]) -> str:
    doc_id = str(args.get("doc_id") or args.get("document_id") or "").strip()
    if not doc_id:
        return "Error: Missing 'doc_id'."
    try:
        docs = get_service("docs", "v1")
        doc = docs.documents().get(documentId=doc_id).execute()
        parts = []
        for el in doc.get("body", {}).get("content", []):
            for run in el.get("paragraph", {}).get("elements", []):
                parts.append(run.get("textRun", {}).get("content", ""))
        text = "".join(parts).strip()
        return f"Content of '{doc.get('title','document')}':\n{text[:8000]}" if text else "Document is empty."
    except Exception as exc:
        return _err("Docs read failed", exc)


# ---------------------------------------------------------------- Sheets
def sheets_read(args: Dict[str, Any]) -> str:
    sid = str(args.get("spreadsheet_id", "")).strip()
    rng = str(args.get("range", "A1:Z50")).strip()
    if not sid:
        return "Error: Missing 'spreadsheet_id'."
    try:
        sheets = get_service("sheets", "v4")
        res = sheets.spreadsheets().values().get(spreadsheetId=sid, range=rng).execute()
        rows = res.get("values", [])
        if not rows:
            return f"No data in range '{rng}'."
        return f"Sheet data ({rng}):\n" + "\n".join(" | ".join(map(str, r)) for r in rows[:50])
    except Exception as exc:
        return _err("Sheets read failed", exc)


def sheets_append(args: Dict[str, Any]) -> str:
    sid = str(args.get("spreadsheet_id", "")).strip()
    rng = str(args.get("range", "A1")).strip()
    values = args.get("values")
    if not sid:
        return "Error: Missing 'spreadsheet_id'."
    if isinstance(values, str):
        values = [v.strip() for v in values.split(",")]
    if not isinstance(values, list) or not values:
        return "Error: Provide 'values' as a list or comma-separated string (one row)."
    row = values if isinstance(values[0], list) else [values]
    try:
        sheets = get_service("sheets", "v4")
        sheets.spreadsheets().values().append(
            spreadsheetId=sid, range=rng, valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS", body={"values": row},
        ).execute()
        return f"Appended {len(row)} row(s) to the sheet."
    except Exception as exc:
        return _err("Sheets append failed", exc)


# ---------------------------------------------------------------- Meet
def meet_create(args: Dict[str, Any]) -> str:
    summary = str(args.get("summary", "")).strip() or "Meeting"
    start = str(args.get("start", "")).strip()
    end = str(args.get("end", "")).strip()
    if not start or not end:
        return "Error: Provide ISO 'start' and 'end' datetimes (e.g. 2026-10-21T15:00:00+05:30)."
    try:
        cal = get_service("calendar", "v3")
        event = {
            "summary": summary,
            "description": str(args.get("description", "")),
            "start": {"dateTime": start},
            "end": {"dateTime": end},
            "conferenceData": {
                "createRequest": {
                    "requestId": f"joi-{abs(hash(summary + start)) % 10**10}",
                    "conferenceSolutionKey": {"type": "hangoutsMeet"},
                }
            },
        }
        created = cal.events().insert(
            calendarId="primary", body=event, conferenceDataVersion=1,
        ).execute()
        link = created.get("hangoutLink") or "(link pending)"
        return f"Created '{summary}' with Google Meet: {link}\nEvent: {created.get('htmlLink','')}"
    except Exception as exc:
        return _err("Meet create failed", exc)


# ---------------------------------------------------------------- Contacts
def contacts_search(args: Dict[str, Any]) -> str:
    name = str(args.get("name", "")).strip().lower()
    if not name:
        return "Error: Missing 'name' to look up."
    try:
        people = get_service("people", "v1")
        res = people.people().connections().list(
            resourceName="people/me", pageSize=500,
            personFields="names,emailAddresses",
        ).execute()
        matches = []
        for p in res.get("connections", []):
            full = " ".join(n.get("displayName", "") for n in p.get("names", []))
            if name in full.lower():
                emails = [e.get("value") for e in p.get("emailAddresses", []) if e.get("value")]
                if emails:
                    matches.append(f"{full}: {', '.join(emails)}")
        if not matches:
            return f"No contacts found matching '{name}'."
        return "Matching contacts:\n" + "\n".join(matches[:10])
    except Exception as exc:
        return _err("Contacts search failed", exc)
