"""
Google Sheets export service.
Supports: job results export, Explorer filtered results export.

Two modes (checked in order):
  1. GOOGLE_EXPORT_SHEET_ID — append a new tab to one existing spreadsheet.
     Setup: create a Google Sheet manually, share with SA as Editor,
     set GOOGLE_EXPORT_SHEET_ID to its ID.
     No Drive API, no quota issues.

  2. GOOGLE_DRIVE_FOLDER_ID — create a new file inside a shared Drive folder.
     Requires Drive API enabled and folder shared with SA as Editor.
"""
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from services.sheets_client import sheets_client

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


def _build_rows(results: list[dict], cols: list[tuple]) -> list[list]:
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
    return rows


def _export_as_tab(sheet_id: str, tab_title: str, rows: list[list]) -> str:
    """Add a new tab to an existing spreadsheet and write data. Returns URL with #gid anchor."""
    sh = sheets_client(write=True).spreadsheets()

    # Add new sheet tab
    resp = sh.batchUpdate(spreadsheetId=sheet_id, body={"requests": [
        {"addSheet": {"properties": {"title": tab_title}}}
    ]}).execute()
    tab_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    # Write data
    sh.values().update(
        spreadsheetId=sheet_id,
        range=f"'{tab_title}'!A1",
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()

    # Format: bold header, freeze, autosize
    n_cols = len(rows[0]) if rows else 1
    sh.batchUpdate(spreadsheetId=sheet_id, body={"requests": [
        {"repeatCell": {
            "range": {"sheetId": tab_id, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {"userEnteredFormat": {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.98},
            }},
            "fields": "userEnteredFormat(textFormat,backgroundColor)"
        }},
        {"updateSheetProperties": {
            "properties": {"sheetId": tab_id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount"
        }},
        {"autoResizeDimensions": {
            "dimensions": {"sheetId": tab_id, "dimension": "COLUMNS",
                           "startIndex": 0, "endIndex": n_cols}
        }},
    ]}).execute()

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}#gid={tab_id}"
    logger.info(f"Tab created: {url} ({len(rows)-1} rows)")
    return url


def _export_as_new_file(folder_id: str, title: str, rows: list[list]) -> str:
    """Create a new spreadsheet in the shared Drive folder. Returns URL."""
    from services.sheets_client import drive_client
    dr = drive_client()
    sh = sheets_client(write=True).spreadsheets()

    file = dr.files().create(
        body={
            "name": title,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [folder_id],
        },
        fields="id",
        supportsAllDrives=True,
    ).execute()
    sheet_id = file["id"]
    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"

    n_cols = len(rows[0]) if rows else 1
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
                           "startIndex": 0, "endIndex": n_cols}
        }},
    ]}).execute()

    dr.permissions().create(
        fileId=sheet_id,
        body={"type": "anyone", "role": "reader"},
        supportsAllDrives=True,
    ).execute()

    logger.info(f"Sheet created: {sheet_url} ({len(rows)-1} rows)")
    return sheet_url


def _create_sheet(title: str, tab_title: str, results: list[dict],
                  columns: list[tuple] = None) -> str:
    """Route to tab-mode or new-file-mode depending on env vars."""
    cols = columns or EXPORT_COLUMNS
    rows = _build_rows(results, cols)

    sheet_id = os.getenv("GOOGLE_EXPORT_SHEET_ID", "").strip()
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()

    if sheet_id:
        return _export_as_tab(sheet_id, tab_title, rows)

    if folder_id:
        return _export_as_new_file(folder_id, title, rows)

    raise ValueError(
        "Google Sheets export not configured. "
        "Set GOOGLE_EXPORT_SHEET_ID (recommended): create a Google Sheet, "
        "share with the service account as Editor, paste its ID. "
        "Or set GOOGLE_DRIVE_FOLDER_ID: shared Drive folder ID."
    )


def export_job_to_sheets(job_id: str, filename: str, results: list[dict]) -> Optional[str]:
    """Export job results to Google Sheets."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    title = f"Domain Intel — {filename} — {ts}"
    tab_title = f"{filename[:25]} {ts[11:]}"  # short tab name
    try:
        return _create_sheet(title, tab_title, results)
    except Exception as e:
        logger.error(f"Sheets export error: {e}")
        raise


def export_explorer_to_sheets(label: str, results: list[dict]) -> Optional[str]:
    """Export Explorer filtered results to Google Sheets."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    title = f"Domain Intel — Explorer — {label} — {ts}"
    tab_title = f"Explorer {ts[5:16]}"  # e.g. "Explorer 05-05 07:30"
    cols = [c for c in EXPORT_COLUMNS if c[0] not in ("status", "error_detail")]
    try:
        return _create_sheet(title, tab_title, results, columns=cols)
    except Exception as e:
        logger.error(f"Sheets export error: {e}")
        raise
