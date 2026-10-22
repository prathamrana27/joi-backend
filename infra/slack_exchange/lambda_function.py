"""JOI Slack token exchange (AWS Lambda, Python 3.12, Function URL).

Slack's client secret must stay private, so installed copies of JOI never get it.
When a user connects Slack, JOI receives a one-time code on its local callback and sends
it here; this function swaps it for the workspace token with the secret and returns
Slack's reply. Only used at connect time: Slack bot tokens don't expire (keep token
rotation off in the Slack app), so connected users never depend on this function.

Environment variables (set in the Lambda console, never in code):
  SLACK_CLIENT_ID       from Slack app → Basic Information
  SLACK_CLIENT_SECRET   from Slack app → Basic Information
  SLACK_REDIRECT_URI    http://127.0.0.1:8000/integrations/slack/callback
"""

import base64
import json
import os
import urllib.parse
import urllib.request

SLACK_TOKEN_URL = "https://slack.com/api/oauth.v2.access"


def _reply(status, payload):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
        "body": json.dumps(payload),
    }


def lambda_handler(event, _context):
    method = (event.get("requestContext") or {}).get("http", {}).get("method", "")
    if method != "POST":
        return _reply(405, {"ok": False, "error": "method_not_allowed"})

    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8", "replace")
    try:
        data = json.loads(body)
    except (ValueError, TypeError):
        return _reply(400, {"ok": False, "error": "invalid_json"})

    code = str(data.get("code") or "")
    redirect_uri = str(data.get("redirect_uri") or "")
    if not (10 <= len(code) <= 300):
        return _reply(400, {"ok": False, "error": "invalid_code"})
    # Only JOI's own local callback; anything else is not a JOI install.
    if redirect_uri != os.environ["SLACK_REDIRECT_URI"]:
        return _reply(400, {"ok": False, "error": "invalid_redirect_uri"})

    form = urllib.parse.urlencode({
        "client_id": os.environ["SLACK_CLIENT_ID"],
        "client_secret": os.environ["SLACK_CLIENT_SECRET"],
        "code": code,
        "redirect_uri": redirect_uri,
    }).encode()
    req = urllib.request.Request(SLACK_TOKEN_URL, data=form, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            slack = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return _reply(502, {"ok": False, "error": "slack_unreachable"})
    # Slack's reply goes back to the JOI install that holds the one-time code.
    return _reply(200, slack)
