import os
import re
import base64
import pickle
import asyncio
import logging
import html2text
from typing import Dict, Any, List, Tuple
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pathlib import Path

from .file_system import _resolve_and_validate_path, AI_WORKSPACE_DIR
from .undo import undo_marker

UNDO_SEND_SECONDS = 30
# Larger messages (big attachments) skip the undo window rather than sit in the database.
MAX_HELD_RAW_CHARS = 8_000_000

logger = logging.getLogger(__name__)

# Gmail API scopes
SCOPES = ['https://www.googleapis.com/auth/gmail.send',
          'https://www.googleapis.com/auth/gmail.readonly']


def _resolve_tools_dir() -> Path:
    override = str(os.getenv("JOI_GOOGLE_TOOLS_DIR", "")).strip()
    if override:
        candidate = Path(override).expanduser()
        candidate.mkdir(parents=True, exist_ok=True)
        return candidate.resolve()
    return Path(__file__).parent.resolve()


def _get_gmail_service_sync():
    """Get the Gmail API service via the unified Google auth (shared token)."""
    try:
        from .google_auth import get_service
        return get_service("gmail", "v1"), None
    except Exception as e:
        logger.error(f"Gmail service unavailable: {e}")
        return None, f"Error: {e}"


def parse_email_command(command: str) -> Dict[str, Any]:
    """Parse the email command string (key:value;) into components."""
    email_data = {
        "to": [], "cc": [], "bcc": [], "subject": "", "body": "",
        "raw_attachments": [],
        "read": False, "query": "", "limit": 5, "read_full_id": ""
    }
    pattern = r"(\w+)\s*:\s*((?:[^;']*(?:'(?:\\.|[^'])*')?)*)"
    matches = re.findall(pattern, command)

    for key, value in matches:
        key = key.strip().lower()
        value = value.strip().strip("'")

        if not value: continue

        if key == 'to':
            email_data['to'] = [email.strip() for email in value.split(',')]
        elif key == 'cc':
            email_data['cc'] = [email.strip() for email in value.split(',')]
        elif key == 'bcc':
            email_data['bcc'] = [email.strip() for email in value.split(',')]
        elif key == 'subject':
            email_data['subject'] = value
        elif key == 'body':
            email_data['body'] = value
        elif key == 'attach':
            email_data['raw_attachments'] = [path.strip() for path in value.split(',')]
        elif key == 'read' and value.lower() == 'true':
            email_data['read'] = True
        elif key == 'query':
            email_data['query'] = value
        elif key == 'limit' and value.isdigit():
            email_data['limit'] = int(value)
        elif key == 'read_full_id':
            email_data['read_full_id'] = value

    return email_data


def create_message_with_attachments(
        sender: str,
        to: List[str],
        cc: List[str],
        bcc: List[str],
        subject: str,
        body: str,
        validated_attachments: List[Path]
) -> Dict[str, Any]:
    """Create a message with attachments using validated paths for the Gmail API."""
    message = MIMEMultipart()
    message['from'] = sender
    message['to'] = ', '.join(to)
    if cc: message['cc'] = ', '.join(cc)
    if bcc: message['bcc'] = ', '.join(bcc)
    message['subject'] = subject

    message.attach(MIMEText(body, 'plain', 'utf-8'))

    for file_path_obj in validated_attachments:
        try:
            with open(file_path_obj, 'rb') as file:
                part = MIMEApplication(file.read(), Name=file_path_obj.name)
            part['Content-Disposition'] = f'attachment; filename="{file_path_obj.name}"'
            message.attach(part)
            logger.info(f"Attached file: {file_path_obj.name}")
        except FileNotFoundError:
            logger.error(f"Attachment file not found during creation (unexpected): {file_path_obj}")
            raise Exception(f"Error attaching file {file_path_obj.name}: File not found (unexpected).")
        except Exception as e:
            logger.error(f"Error reading or attaching file {file_path_obj}: {str(e)}", exc_info=True)
            raise Exception(f"Error attaching file {file_path_obj.name}: {str(e)}")

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return {'raw': raw_message}


MAX_BODY_CHARS = 4000
# Zero-width and filler characters marketing emails use to pad previews.
_INVISIBLE_RE = re.compile("[​-‏͏­⁠-⁤﻿ ]")


def _html_to_text(html: str) -> str:
    """Readable text from an HTML email: no CSS, scripts or head, tidy whitespace."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["style", "script", "head", "title", "meta", "noscript"]):
        tag.decompose()
    for a in soup.find_all("a"):
        href = a.get("href", "")
        label = a.get_text(" ", strip=True)
        # Keep real links readable, drop tracking-pixel style anchors with no text.
        if label and href.startswith("http") and len(href) < 120:
            a.replace_with(f"{label} ({href})")
    text = soup.get_text("\n")
    return _tidy_text(text)


# A CSS rule: "selector { prop: value; … }", possibly spread over several lines.
_CSS_BLOCK_RE = re.compile(r"[^\n{}]*\{(?=[^{}]*[\w-]+\s*:[^{}]*;)[^{}]*\}")
_CSS_LINE_RE = re.compile(r"^\s*[}{]\s*$|^\s*@(media|font-face|import)\b")


def _tidy_text(text: str) -> str:
    text = _INVISIBLE_RE.sub(" ", text)
    # Some senders' "plain text" part is full of leftover CSS. Remove rule blocks (repeat
    # for blocks nested in @media), then stray braces and @-rule lines.
    for _ in range(3):
        stripped = _CSS_BLOCK_RE.sub("", text)
        if stripped == text:
            break
        text = stripped
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.splitlines() if not _CSS_LINE_RE.search(ln)]
    out, blank = [], 0
    for ln in lines:
        if not ln:
            blank += 1
            if blank > 1:
                continue
        else:
            blank = 0
        out.append(ln)
    text = "\n".join(out).strip()
    if len(text) > MAX_BODY_CHARS:
        text = text[:MAX_BODY_CHARS].rstrip() + "\n\n[…email continues; ask to see more if needed]"
    return text


def _parse_message_payload(payload: Dict[str, Any]) -> Tuple[str, str]:
    """Parse the message payload to extract the body content.

    Returns:
        Tuple[str, str]: A tuple containing (plain_text_content, content_type)
    """
    body_content = ""
    content_type = ""

    # Check if this part has a body
    if 'body' in payload and 'data' in payload['body'] and payload['body']['data']:
        body_data = payload['body']['data']
        try:
            decoded_data = base64.urlsafe_b64decode(body_data).decode('utf-8', errors='replace')
            content_type = payload.get('mimeType', '')
            # Single-part HTML emails used to come back as raw HTML/CSS.
            body_content = _html_to_text(decoded_data) if 'html' in content_type else _tidy_text(decoded_data)
        except Exception as e:
            logger.warning(f"Error decoding message body: {e}")
            return "", ""

    # If this is a multipart message, recursively process parts
    elif 'parts' in payload and isinstance(payload['parts'], list):
        # First try to find plain text
        for part in payload['parts']:
            part_mime = part.get('mimeType', '')
            if part_mime == 'text/plain':
                if 'body' in part and 'data' in part['body']:
                    try:
                        decoded_data = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='replace')
                        return _tidy_text(decoded_data), 'text/plain'
                    except Exception as e:
                        logger.warning(f"Error decoding plain text part: {e}")

        # If no plain text, try HTML
        for part in payload['parts']:
            part_mime = part.get('mimeType', '')
            if part_mime == 'text/html':
                if 'body' in part and 'data' in part['body']:
                    try:
                        decoded_data = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='replace')
                        return _html_to_text(decoded_data), 'text/html'
                    except Exception as e:
                        logger.warning(f"Error processing HTML part: {e}")

        # If still no content, recursively check nested parts
        for part in payload['parts']:
            if 'parts' in part:
                content, mime = _parse_message_payload(part)
                if content:
                    return content, mime

    return body_content, content_type


def _read_full_email_sync(service, message_id: str) -> str:
    """Read the full content of a specific email by ID."""
    try:
        # Get the full message
        msg = service.users().messages().get(userId='me', id=message_id, format='full').execute()

        # Extract headers
        headers = msg.get('payload', {}).get('headers', [])
        subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), 'No Subject')
        sender = next((h['value'] for h in headers if h['name'].lower() == 'from'), 'Unknown Sender')
        date = next((h['value'] for h in headers if h['name'].lower() == 'date'), 'Unknown Date')
        to = next((h['value'] for h in headers if h['name'].lower() == 'to'), 'Unknown Recipient')

        # Extract body content
        payload = msg.get('payload', {})
        body_content, content_type = _parse_message_payload(payload)

        if not body_content:
            body_content = "[Could not extract email body content]"

        # Format the output
        output = f"Subject: {subject}\nFrom: {sender}\nTo: {to}\nDate: {date}\n"
        if content_type:
            output += f"Content-Type: {content_type}\n"
        output += f"\n{body_content}"

        return output

    except Exception as e:
        logger.error(f"Error reading full email with ID {message_id}: {str(e)}", exc_info=True)
        return f"Error reading full email: {str(e)}"


# --- Email Reading ---
def _read_emails_sync(service, query: str = "", limit: int = 5) -> str:
    """Synchronous function to read emails from Gmail based on query.
    If no query is provided, returns the latest emails regardless of read/unread status.
    """
    try:
        # If no query provided, list latest emails
        if not query:
            query = ""

        results = service.users().messages().list(userId='me', q=query, maxResults=limit).execute()
        messages = results.get('messages', [])

        if not messages:
            return f"No emails found{' matching query: ' + query if query else ''}"

        email_summaries = []
        fetch_limit = min(len(messages), limit, 10)
        logger.info(
            f"Found {len(messages)} emails{' matching query: ' + query if query else ''}, fetching details for {fetch_limit}.")

        for message_info in messages[:fetch_limit]:
            try:
                msg_id = message_info['id']
                msg = service.users().messages().get(userId='me', id=msg_id, format='metadata',
                                                     metadataHeaders=['Subject', 'From', 'Date']).execute()
                headers = msg.get('payload', {}).get('headers', [])
                subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'].lower() == 'from'), 'Unknown Sender')
                date = next((h['value'] for h in headers if h['name'].lower() == 'date'), 'Unknown Date')
                snippet = msg.get('snippet', 'No preview available.')
                snippet = snippet.replace('\r\n', ' ').replace('\n', ' ').strip()
                snippet = (snippet[:150] + '...') if len(snippet) > 150 else snippet
                email_summaries.append(
                    f"ID: {msg_id}\nFrom: {sender}\nDate: {date}\nSubject: {subject}\nPreview: {snippet}\n")
            except Exception as detail_err:
                logger.warning(f"Could not fetch/parse details for message ID {message_info.get('id')}: {detail_err}")
                email_summaries.append(f"[Could not fetch details for one email]\n")

        query_text = f" matching query '{query}'" if query else ""
        output = f"Found {len(messages)} email(s){query_text}. Summaries for the first {len(email_summaries)}:\n\n" + "\n---\n".join(
            email_summaries)
        if len(messages) > len(email_summaries):
            output += f"\n[Note: More emails found but only {len(email_summaries)} summaries are shown.]"
        output += "\n\nTo read a full email, use 'read_full_id:<message_id>' in your command."
        return output

    except Exception as e:
        logger.error(f"Error reading emails{' with query ' + query if query else ''}: {str(e)}", exc_info=True)
        return f"Error reading emails: {str(e)}"


# --- Main Tool Function ---
async def send_email(args: Dict[str, Any]) -> str:
    """
    (Async) Sends or reads emails using the Gmail API based on a command string.

    Args:
        args (Dict[str, Any]): A dictionary containing the arguments.
                               Expected key: 'command_string' with the key:value; format.

    Returns:
        str: Result of the email operation (success message or error).
    """
    loop = asyncio.get_running_loop()

    command = args.get("command_string")
    if not command or not isinstance(command, str):
        return "Error: Missing or invalid 'command_string' argument for email tool."

    try:
        email_data = parse_email_command(command)
        # to_thread (unlike run_in_executor) copies contextvars, so the worker thread
        # sees the per-user Google credentials the broker set for this request.
        service, error = await asyncio.to_thread(_get_gmail_service_sync)
        if not service:
            return error

        # Check if reading a full email by ID
        if email_data['read_full_id']:
            message_id = email_data['read_full_id']
            logger.info(f"Reading full email with ID: {message_id}")
            result = await loop.run_in_executor(None, lambda: _read_full_email_sync(service, message_id))
            return result

        # Check if reading email summaries
        if email_data['read']:
            query = email_data['query']
            limit = email_data['limit']
            logger.info(f"Reading emails with query: '{query}', limit: {limit}")
            result = await loop.run_in_executor(None, lambda: _read_emails_sync(service, query, limit))
            return result

        # If no read parameters, check if we should list latest emails
        if not email_data['to']:
            # No recipients and not reading (e.g. "is Gmail connected?"): confirm the
            # connection and list the latest emails. Never sends.
            limit = email_data['limit']
            logger.info(f"Listing latest {limit} emails")
            try:
                profile = await loop.run_in_executor(None, lambda: service.users().getProfile(userId='me').execute())
                status = f"Gmail is connected ({profile.get('emailAddress', '')}).\n\n"
            except Exception:
                status = ""
            result = await loop.run_in_executor(None, lambda: _read_emails_sync(service, "", limit))
            return status + result

        # Otherwise, proceed with sending an email
        validated_attachment_paths: List[Path] = []
        if email_data['raw_attachments']:
            if AI_WORKSPACE_DIR is None:
                return "Error: Cannot process attachments because AI Workspace directory is not available."
            for rel_path_str in email_data['raw_attachments']:
                validated_path = _resolve_and_validate_path(rel_path_str, AI_WORKSPACE_DIR)
                if not validated_path:
                    return f"Error: Attachment path '{rel_path_str}' is invalid or outside the allowed ai_workspace."
                if not validated_path.is_file():
                    return f"Error: Attachment path '{rel_path_str}' does not point to a file in the ai_workspace."
                validated_attachment_paths.append(validated_path)
                logger.info(f"Attachment path validated: {validated_path}")

        try:
            user_info = await loop.run_in_executor(None, lambda: service.users().getProfile(userId='me').execute())
            sender_email = user_info['emailAddress']
        except Exception as e:
            logger.error(f"Failed to get user profile (sender email): {e}", exc_info=True)
            return f"Error: Could not retrieve sender email address from Gmail profile: {str(e)}"

        try:
            logger.info(f"Preparing email to: {email_data['to']} with {len(validated_attachment_paths)} attachments.")
            message_body = await loop.run_in_executor(
                None,
                lambda: create_message_with_attachments(
                    sender_email, email_data['to'], email_data['cc'], email_data['bcc'],
                    email_data['subject'], email_data['body'], validated_attachment_paths
                )
            )
        except Exception as e:
            logger.error(f"Error during email creation or sending: {e}", exc_info=True)
            return f"Error creating or sending email: {str(e)}"

        recipient_count = len(email_data['to'])
        cc_count = len(email_data['cc'])
        attachment_count = len(validated_attachment_paths)
        success_msg = (
            f"Email from {sender_email} to {recipient_count} recipient(s) is queued and sends in "
            f"{UNDO_SEND_SECONDS} seconds"
        )
        if cc_count > 0: success_msg += f", with {cc_count} CC recipient(s)"
        if attachment_count > 0: success_msg += f" and {attachment_count} attachment(s) from the workspace"
        success_msg += ". The user can undo it until then."
        label = f"Email to {', '.join(email_data['to'])[:60]}"
        raw = message_body.get('raw', '')
        if len(raw) > MAX_HELD_RAW_CHARS:
            # Too big to hold for the undo window: send now, as before.
            try:
                sent = await asyncio.to_thread(
                    lambda: service.users().messages().send(userId='me', body=message_body).execute()
                )
            except Exception as e:
                logger.error(f"Error sending email: {e}", exc_info=True)
                return f"Error creating or sending email: {str(e)}"
            return success_msg.split(" is queued")[0] + f" was sent. Message ID: {sent.get('id')}."
        # Undo window: the composed message is held by the backend and sent with
        # messages.send (the gmail.send scope) after UNDO_SEND_SECONDS unless the user
        # presses Undo. See api_server._run_undo_tick.
        logger.info("Email composed and held for the undo window.")
        return success_msg + undo_marker("email_send", label, UNDO_SEND_SECONDS, {"raw": raw})

    except Exception as e:
        logger.error(f"Unexpected error in send_email tool: {str(e)}", exc_info=True)
        return f"Error processing email command: {str(e)}"
