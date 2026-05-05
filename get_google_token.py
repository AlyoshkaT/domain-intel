#!/usr/bin/env python3
"""
Run once locally to get Google OAuth refresh token for Railway.
Usage:
  pip install google-auth-oauthlib
  python get_google_token.py
"""
import json
import webbrowser
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

print("Paste your OAuth client credentials JSON (from GCP Console → APIs → Credentials → OAuth 2.0 Client ID → Desktop app → Download JSON):")
print("Paste JSON, then press Enter twice:")

lines = []
while True:
    line = input()
    if line == "" and lines:
        break
    lines.append(line)

client_config = json.loads("\n".join(lines))

# Support both downloaded file format and direct client_id/secret
if "installed" not in client_config and "web" not in client_config:
    client_config = {"installed": client_config}

flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
creds = flow.run_local_server(port=0)

token_json = json.dumps({
    "type": "authorized_user",
    "client_id": creds.client_id,
    "client_secret": creds.client_secret,
    "refresh_token": creds.refresh_token,
})

print("\n" + "="*60)
print("✅ Copy this value to Railway as GOOGLE_OAUTH_TOKEN_JSON:")
print("="*60)
print(token_json)
print("="*60)
