"""
Shared Google credentials helper.
Priority:
  1. GOOGLE_OAUTH_TOKEN_JSON  — OAuth user credentials (personal account, owns the files)
  2. GOOGLE_SHEETS_CREDENTIALS_JSON / GOOGLE_CREDENTIALS_JSON — service account
  3. File path fallback (local dev)
"""
import json
import logging
import os

logger = logging.getLogger(__name__)

SCOPES_READ  = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SCOPES_WRITE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_creds(scopes: list[str]):
    # 1. OAuth user credentials (no SA quota issues, files owned by real user)
    oauth_json = os.getenv("GOOGLE_OAUTH_TOKEN_JSON", "").strip()
    if oauth_json:
        try:
            from google.oauth2.credentials import Credentials
            info = json.loads(oauth_json)
            creds = Credentials(
                token=None,
                refresh_token=info["refresh_token"],
                token_uri=info.get("token_uri", "https://oauth2.googleapis.com/token"),
                client_id=info["client_id"],
                client_secret=info["client_secret"],
                scopes=scopes,
            )
            return creds
        except Exception as e:
            logger.warning(f"Failed to load OAuth creds: {e}")

    # 2. Service account from ENV
    for env_var in ("GOOGLE_SHEETS_CREDENTIALS_JSON", "GOOGLE_CREDENTIALS_JSON"):
        json_str = os.getenv(env_var, "").strip()
        if json_str:
            try:
                from google.oauth2 import service_account
                info = json.loads(json_str)
                return service_account.Credentials.from_service_account_info(info, scopes=scopes)
            except Exception as e:
                logger.warning(f"Failed to load creds from ENV {env_var}: {e}")

    # 3. File fallback (local dev)
    from config.settings import GOOGLE_SHEETS_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS
    for path in [GOOGLE_SHEETS_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS]:
        if path and os.path.exists(path):
            from google.oauth2 import service_account
            return service_account.Credentials.from_service_account_file(path, scopes=scopes)

    raise ValueError(
        "No Google credentials found. Set GOOGLE_OAUTH_TOKEN_JSON (recommended) "
        "or GOOGLE_SHEETS_CREDENTIALS_JSON."
    )


def get_service_account_email() -> str:
    """Return the service account email, or empty string for OAuth."""
    oauth_json = os.getenv("GOOGLE_OAUTH_TOKEN_JSON", "").strip()
    if oauth_json:
        return ""  # OAuth user, no SA email
    for env_var in ("GOOGLE_SHEETS_CREDENTIALS_JSON", "GOOGLE_CREDENTIALS_JSON"):
        json_str = os.getenv(env_var, "").strip()
        if json_str:
            try:
                return json.loads(json_str).get("client_email", "")
            except Exception:
                pass
    from config.settings import GOOGLE_SHEETS_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS
    for path in [GOOGLE_SHEETS_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS]:
        if path and os.path.exists(path):
            try:
                with open(path) as f:
                    return json.load(f).get("client_email", "")
            except Exception:
                pass
    return ""


def sheets_client(write: bool = False):
    from googleapiclient.discovery import build
    scopes = SCOPES_WRITE if write else SCOPES_READ
    creds = _get_creds(scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def drive_client():
    from googleapiclient.discovery import build
    creds = _get_creds(SCOPES_WRITE)
    return build("drive", "v3", credentials=creds, cache_discovery=False)
