"""JOI Slack connect helper (AWS Lambda, Python 3.12, Function URL).

Slack's client secret must stay private, and Slack only accepts https redirect URLs, so
installed copies of JOI can neither hold the secret nor receive Slack's redirect
directly. This function does both jobs:

  GET  /slack/callback  Slack's redirect target. Passes the one-time code and state on
                        to the JOI app on the user's own PC (JOI_LOCAL_CALLBACK). Stores
                        nothing; the code is useless without the secret.
  POST /                JOI sends the code here; it is swapped for the workspace token
                        with the secret and Slack's reply is returned.

Only used while connecting: Slack bot tokens don't expire (keep token rotation off in
the Slack app), so connected users never depend on this function.

Environment variables (set in the Lambda console, never in code):
  SLACK_CLIENT_ID       from Slack app → Basic Information
  SLACK_CLIENT_SECRET   from Slack app → Basic Information
  SLACK_REDIRECT_URI    this function's URL + "slack/callback" (also set in the Slack app)
  JOI_LOCAL_CALLBACK    http://127.0.0.1:8000/integrations/slack/callback
"""

import base64
import json
import os
import re
import urllib.parse
import urllib.request

SLACK_TOKEN_URL = "https://slack.com/api/oauth.v2.access"
# Codes, states and Slack error names are short and URL-safe; anything else is dropped.
_SAFE = re.compile(r"^[A-Za-z0-9._\-]{1,600}$")


def _reply(status, payload):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
        "body": json.dumps(payload),
    }


def _bounce_to_joi(event):
    params = event.get("queryStringParameters") or {}
    passed = {k: params[k] for k in ("code", "state", "error") if params.get(k) and _SAFE.match(params[k])}
    target = os.environ["JOI_LOCAL_CALLBACK"] + "?" + urllib.parse.urlencode(passed)
    return {
        "statusCode": 302,
        "headers": {"Location": target, "Cache-Control": "no-store", "Referrer-Policy": "no-referrer"},
        "body": "",
    }


def _exchange(event):
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


def lambda_handler(event, _context):
    http = (event.get("requestContext") or {}).get("http", {})
    method, path = http.get("method", ""), event.get("rawPath") or http.get("path") or "/"
    if method == "GET" and path.rstrip("/") == "/slack/callback":
        return _bounce_to_joi(event)
    if method == "POST" and path in ("", "/"):
        return _exchange(event)
    return _reply(405, {"ok": False, "error": "method_not_allowed"})
