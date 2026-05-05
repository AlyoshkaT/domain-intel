"""
Google Sheets export service.
Supports: job results export, Explorer filtered results export.
"""
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from services.sheets_client import sheets_client, drive_client

logger = logging.getLogger(__name__)

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


def _create_sheet(title: str, results: list[dict], columns: list[tuple] = None) -> Optional[str]:
    """Create a new Google Sheet inside GOOGLE_DRIVE_FOLDER_ID. Returns URL or raises."""
    cols = columns or EXPORT_COLUMNS

    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        raise ValueError(
            "GOOGLE_DRIVE_FOLDER_ID not set. "
            "Create a Google Drive folder, share it with the service account (Editor), "
            "and set GOOGLE_DRIVE_FOLDER_ID to the folder ID from its URL."
        )

    try:
        dr = drive_client()
        sh = sheets_client(write=True).spreadsheets()

        # Create spreadsheet directly inside the shared folder via Drive API.
        # This avoids the 403 that spreadsheets.create() raises when the
        # service account has no My Drive access — folder Editor is enough.
        file = dr.files().create(
            body={
                "name": title,
                "mimeType": "application/vnd.google-apps.spreadsheet",
                "parents": [folder_id],
            },
            fields="id"
        ).execute()

        sheet_id = file["id"]
        sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"

        # Build rows
        headers = [col[1] for col in cols]
        rows = [headers]
        for r in results:
            row = []
            for key, _ in cols:
                val = r.get(key)
                if val is None:
                    row.append("")
                elif isinstance(val, float) and key == "sw_visits":
                    row.append(int(val))
                else:
                    row.append(str(val))
            rows.append(row)

        sh.values().update(
            spreadsheetId=sheet_id,
            range="Results!A1",
            valueInputOption="RAW",
            body={"values": rows}
        ).execute()

        sh.batchUpdate(spreadsheetId=sheet_id, body={"requests": [
            {"repeatCell": {
                "range": {"sheetId": 0, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {
                    "textFormat": {"bold": True},
                    "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.98},
                }},
                "fields": "userEnteredFormat(textFormat,backgroundColor)"
            }},
            {"updateSheetProperties": {
                "properties": {"sheetId": 0, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }},
            {"autoResizeDimensions": {
                "dimensions": {"sheetId": 0, "dimension": "COLUMNS",
                               "startIndex": 0, "endIndex": len(cols)}
            }},
        ]}).execute()

        # Make sheet readable by anyone with the link
        dr.permissions().create(
            fileId=sheet_id,
            body={"type": "anyone", "role": "reader"}
        ).execute()

        logger.info(f"Sheet created: {sheet_url} ({len(results)} rows)")
        return sheet_url

    except Exception as e:
        logger.error(f"Sheets export error: {e}")
        raise


def export_job_to_sheets(job_id: str, filename: str, results: list[dict]) -> Optional[str]:
    """Export job results to a new Google Sheet."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    title = f"Domain Intel — {filename} — {ts}"
    return _create_sheet(title, results)


def export_explorer_to_sheets(label: str, results: list[dict]) -> Optional[str]:
    """Export Explorer filtered results to a new Google Sheet."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    title = f"Domain Intel — Explorer — {label} — {ts}"

    # Explorer columns (no status/error)
    cols = [c for c in EXPORT_COLUMNS if c[0] not in ("status", "error_detail")]
    return _create_sheet(title, results, columns=cols)
