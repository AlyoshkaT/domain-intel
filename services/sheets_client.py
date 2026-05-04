"""
Shared Google Sheets credentials helper.
Supports ENV-based JSON (Railway) and file-based (local dev).
"""
import json
import logging
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

SCOPES_READ  = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SCOPES_WRITE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_creds(scopes: list[str]):
    """Load Google credentials from ENV (Railway) or file (local dev)."""
    # 1. Try GOOGLE_SHEETS_CREDENTIALS_JSON (dedicated sheets key on Railway)
    for env_var in ("GOOGLE_SHEETS_CREDENTIALS_JSON", "GOOGLE_CREDENTIALS_JSON"):
        json_str = os.getenv(env_var, "").strip()
        if json_str:
            try:
                info = json.loads(json_str)
                return service_account.Credentials.from_service_account_info(info, scopes=scopes)
            except Exception as e:
                logger.warning(f"Failed to load creds from ENV {env_var}: {e}")

    # 2. Fallback to file (local dev)
    from config.settings import GOOGLE_SHEETS_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS
    for path in [GOOGLE_SHEETS_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS]:
        if path and os.path.exists(path):
            return service_account.Credentials.from_service_account_file(path, scopes=scopes)

    raise ValueError("No Google Sheets credentials available. Set GOOGLE_SHEETS_CREDENTIALS_JSON env var.")


def sheets_client(write: bool = False):
    """Return Google Sheets API client."""
    scopes = SCOPES_WRITE if write else SCOPES_READ
    creds = _get_creds(scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def drive_client():
    """Return Google Drive API client."""
    creds = _get_creds(SCOPES_WRITE)
    return build("drive", "v3", credentials=creds, cache_discovery=False)
