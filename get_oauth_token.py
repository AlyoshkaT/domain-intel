"""
One-time OAuth token generator.
Run: python get_oauth_token.py
Opens browser → log in as tovstonog.a@gmail.com → saves token to .env
"""
import json
import os
import re
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CLIENT_FILE = "oauth_client.json"

if not os.path.exists(CLIENT_FILE):
    print(f"ERROR: {CLIENT_FILE} not found.")
    print("Download OAuth 2.0 Client ID JSON from GCP Console and save as oauth_client.json")
    exit(1)

print("Opening browser — log in as tovstonog.a@gmail.com ...")
flow = InstalledAppFlow.from_client_secrets_file(CLIENT_FILE, SCOPES)
creds = flow.run_local_server(port=8765, prompt="consent")

token_data = json.loads(creds.to_json())
# Keep only what sheets_client.py needs
token_json = json.dumps({
    "refresh_token": token_data["refresh_token"],
    "client_id":     token_data["client_id"],
    "client_secret": token_data["client_secret"],
    "token_uri":     token_data.get("token_uri", "https://oauth2.googleapis.com/token"),
})

# Update .env
env_path = ".env"
with open(env_path) as f:
    content = f.read()

line = f"GOOGLE_OAUTH_TOKEN_JSON={token_json}"

if "GOOGLE_OAUTH_TOKEN_JSON" in content:
    # Anchor to line start; use [ \t]* (not \s*) so we never consume the
    # preceding newline and glue the var onto the previous line.
    content = re.sub(r"(?m)^[ \t]*#?[ \t]*GOOGLE_OAUTH_TOKEN_JSON=.*$", line, content)
else:
    if content and not content.endswith("\n"):
        content += "\n"
    content += f"{line}\n"

with open(env_path, "w") as f:
    f.write(content)

print("\nDone! GOOGLE_OAUTH_TOKEN_JSON written to .env")
print("Restart the server and try Sheets export again.")
