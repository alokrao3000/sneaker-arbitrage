"""
One-time interactive StockX OAuth login — captures the initial refresh_token.

Prereqs (once, at https://developer.stockx.com):
  1. Create an app; note its client ID, client secret, and API key.
  2. Register the redirect URI shown below (STOCKX_REDIRECT_URI, default
     http://localhost:8017/stockx/callback) on the app.
  3. Put STOCKX_CLIENT_ID / STOCKX_CLIENT_SECRET / STOCKX_API_KEY in .env
     (.env is git-ignored — never commit these).

Then:  python scripts/stockx_auth.py

Opens the StockX login page, catches the redirect on a temporary local HTTP
server, verifies the CSRF state, exchanges the code for tokens, and persists
the refresh token to the stockx_oauth_tokens table (the app refreshes access
tokens from there automatically; rotations are persisted too). It also prints
the refresh token once so you can optionally mirror it to STOCKX_REFRESH_TOKEN
in .env as a backup against DB resets.
"""
import secrets
import sys
import threading
import webbrowser
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings                                    # noqa: E402
from app.database import init_db, SessionLocal, StockXOAuthToken   # noqa: E402
from app.scrapers.stockx_api import (                              # noqa: E402
    STOCKX_AUTHORIZE_URL, STOCKX_TOKEN_URL, STOCKX_AUDIENCE,
)

_result: dict = {}


class _CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        expected_path = urlparse(settings.stockx_redirect_uri).path or "/"
        if parsed.path != expected_path:
            self.send_response(404)
            self.end_headers()
            return
        qs = parse_qs(parsed.query)
        _result["code"] = (qs.get("code") or [None])[0]
        _result["state"] = (qs.get("state") or [None])[0]
        _result["error"] = (qs.get("error_description") or qs.get("error") or [None])[0]
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<h2>StockX login captured &mdash; you can close this tab.</h2>")
        threading.Thread(target=self.server.shutdown, daemon=True).start()

    def log_message(self, *args):   # silence per-request stderr noise
        pass


def main():
    if not (settings.stockx_client_id and settings.stockx_client_secret):
        sys.exit("STOCKX_CLIENT_ID / STOCKX_CLIENT_SECRET missing from .env — see this script's docstring.")

    state = secrets.token_urlsafe(24)   # CSRF guard, verified on the redirect
    authorize_url = STOCKX_AUTHORIZE_URL + "?" + urlencode({
        "response_type": "code",
        "client_id": settings.stockx_client_id,
        "redirect_uri": settings.stockx_redirect_uri,
        "scope": "offline_access openid",
        "audience": STOCKX_AUDIENCE,
        "state": state,
    })

    redirect = urlparse(settings.stockx_redirect_uri)
    server = HTTPServer((redirect.hostname or "localhost", redirect.port or 80), _CallbackHandler)

    print(f"Listening on {settings.stockx_redirect_uri}")
    print("Opening StockX login page (copy the URL below into a browser if it doesn't open):\n")
    print(f"  {authorize_url}\n")
    webbrowser.open(authorize_url)
    server.serve_forever()   # shuts down after the callback lands

    if _result.get("error"):
        sys.exit(f"StockX returned an error: {_result['error']}")
    if _result.get("state") != state:
        sys.exit("CSRF state mismatch — aborting without exchanging the code.")
    code = _result.get("code")
    if not code:
        sys.exit("No authorization code received.")

    print("Exchanging authorization code for tokens …")
    resp = httpx.post(STOCKX_TOKEN_URL, data={
        "grant_type": "authorization_code",
        "client_id": settings.stockx_client_id,
        "client_secret": settings.stockx_client_secret,
        "code": code,
        "redirect_uri": settings.stockx_redirect_uri,
    }, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    refresh_token = payload.get("refresh_token")
    if not refresh_token:
        sys.exit(f"Token response had no refresh_token (got keys: {list(payload)}). "
                 "Is the 'offline_access' scope enabled for your app?")

    init_db()
    session = SessionLocal()
    try:
        row = session.get(StockXOAuthToken, 1) or StockXOAuthToken(id=1)
        row.refresh_token = refresh_token
        row.access_token = payload.get("access_token")
        if payload.get("expires_in"):
            row.access_token_expires_at = datetime.utcnow() + timedelta(seconds=int(payload["expires_in"]))
        session.merge(row)
        session.commit()
    finally:
        session.close()

    print("\n✓ Refresh token stored in the stockx_oauth_tokens table — the app will")
    print("  refresh access tokens automatically from here on.")
    print("\nOptional backup — add to .env (git-ignored), NOT to any committed file:")
    print(f"  STOCKX_REFRESH_TOKEN={refresh_token}")


if __name__ == "__main__":
    main()
