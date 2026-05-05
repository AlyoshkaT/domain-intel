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
    """Create a new Google Sheet with results. Returns URL or None."""
    cols = columns or EXPORT_COLUMNS
    try:
        sh = sheets_client(write=True).spreadsheets()
        dr = drive_client()
        folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

        spreadsheet = sh.create(body={
            "properties": {"title": title},
            "sheets": [{"properties": {"title": "Results"}}]
        }).execute()

        sheet_id = spreadsheet["spreadsheetId"]
        sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"

        if folder_id:
            dr.files().update(
                fileId=sheet_id,
                addParents=folder_id,
                removeParents="root",
                fields="id, parents"
            ).execute()

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
