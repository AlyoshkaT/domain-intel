"""
Google Sheets export service
Creates a new Sheet for each job with results.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build

from config.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Column order for export (matches UI table)
EXPORT_COLUMNS = [
    ("domain",               "Domain"),
    ("sw_visits",            "Traffic"),
    ("cms_list",             "CMS"),
    ("osearch_group",        "oSearch Group"),
    ("osearch",              "oSearch"),
    ("ems_list",             "EMS"),
    ("ai_category",          "AI Category"),
    ("ai_is_ecommerce",      "AI Ecomm"),
    ("ai_industry",          "AI Industry"),
    ("bw_vertical",          "Industry BW"),
    ("sw_category",          "Category SW"),
    ("sw_subcategory",       "Subcategory SW"),
    ("sw_description",       "Description"),
    ("sw_title",             "Title"),
    ("company_name",         "Company"),
    ("sw_primary_region",    "Region"),
    ("sw_primary_region_pct","Region %"),
    ("status",               "Status"),
    ("error_detail",         "Error"),
]


def _get_creds():
    from config.settings import GOOGLE_SHEETS_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS
    import os
    creds_path = GOOGLE_SHEETS_CREDENTIALS or GOOGLE_APPLICATION_CREDENTIALS
    return service_account.Credentials.from_service_account_file(
        creds_path, scopes=SCOPES
    )

def _sheets_client():
    return build("sheets", "v4", credentials=_get_creds(), cache_discovery=False)

def _drive_client():
    return build("drive", "v3", credentials=_get_creds(), cache_discovery=False)

def export_job_to_sheets(job_id: str, filename: str, results: list[dict]) -> Optional[str]:
    """
    Create a new Google Sheet with job results.
    Returns the Sheet URL or None on error.
    """
    if not GOOGLE_APPLICATION_CREDENTIALS:
        logger.warning("GOOGLE_APPLICATION_CREDENTIALS not set — skipping Sheets export")
        return None

    try:
        sheets = _sheets_client()
        drive = _drive_client()

        # Create spreadsheet
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        title = f"Domain Intel — {filename} — {ts}"

        import os
        folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

        spreadsheet = sheets.spreadsheets().create(body={
            "properties": {"title": title},
            "sheets": [{"properties": {"title": "Results"}}]
        }).execute()

        sheet_id = spreadsheet["spreadsheetId"]

        # Move to shared folder if configured
        if folder_id:
            drive.files().update(
                fileId=sheet_id,
                addParents=folder_id,
                removeParents="root",
                fields="id, parents"
            ).execute()

        sheet_id = spreadsheet["spreadsheetId"]
        sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"

        # Build rows
        headers = [col[1] for col in EXPORT_COLUMNS]
        rows = [headers]

        for r in results:
            row = []
            for key, _ in EXPORT_COLUMNS:
                val = r.get(key)
                if val is None:
                    row.append("")
                elif isinstance(val, float) and key == "sw_visits":
                    row.append(int(val))
                else:
                    row.append(str(val) if val is not None else "")
            rows.append(row)

        # Write data
        sheets.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range="Results!A1",
            valueInputOption="RAW",
            body={"values": rows}
        ).execute()

        # Format header row
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [
                # Bold header
                {
                    "repeatCell": {
                        "range": {"sheetId": 0, "startRowIndex": 0, "endRowIndex": 1},
                        "cell": {"userEnteredFormat": {
                            "textFormat": {"bold": True},
                            "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.98},
                        }},
                        "fields": "userEnteredFormat(textFormat,backgroundColor)"
                    }
                },
                # Freeze header
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": 0,
                            "gridProperties": {"frozenRowCount": 1}
                        },
                        "fields": "gridProperties.frozenRowCount"
                    }
                },
                # Auto resize columns
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": 0,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": len(EXPORT_COLUMNS)
                        }
                    }
                }
            ]}
        ).execute()

        # Make sheet accessible by link
        drive.permissions().create(
            fileId=sheet_id,
            body={"type": "anyone", "role": "reader"}
        ).execute()

        logger.info(f"Sheets export done: {sheet_url}")
        return sheet_url

    except Exception as e:
        logger.error(f"Sheets export error for job {job_id}: {e}")
        return None
