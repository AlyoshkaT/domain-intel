"""
Technology Catalog service
Reads CMS / OSearch / EMS from Google Sheets and stores in BigQuery.
Used to match BuiltWith raw technologies against known catalog entries.
"""
import logging
from typing import Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery as bq

from config.settings import GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_SHEETS_CATALOG_ID
from core.bigquery import client, table_ref

logger = logging.getLogger(__name__)

CATALOG_TABLE = "technology_catalog"

CATALOG_SCHEMA = [
    bq.SchemaField("sheet", "STRING"),       # cms / osearch / ems
    bq.SchemaField("technology", "STRING"),  # назва технології
    bq.SchemaField("group_name", "STRING"),  # група (тільки для osearch)
]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _sheets_client():
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def ensure_catalog_table():
    """Create catalog table in BQ if not exists."""
    bq_client = client()
    table = bq.Table(table_ref(CATALOG_TABLE), schema=CATALOG_SCHEMA)
    try:
        bq_client.get_table(table)
    except Exception:
        bq_client.create_table(table)
        logger.info(f"Created table {CATALOG_TABLE}")


def sync_catalog() -> dict:
    """
    Read CMS / OSearch / EMS sheets and save to BQ.
    Returns counts per sheet.
    """
    if not GOOGLE_SHEETS_CATALOG_ID:
        raise ValueError("GOOGLE_SHEETS_CATALOG_ID not set in .env")
    if not GOOGLE_APPLICATION_CREDENTIALS:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS not set in .env")

    ensure_catalog_table()
    sheets = _sheets_client().spreadsheets()
    rows_to_insert = []

    # ── CMS sheet ─────────────────────────────────────────────────────────────
    cms_data = sheets.values().get(
        spreadsheetId=GOOGLE_SHEETS_CATALOG_ID,
        range="CMS!A2:A"
    ).execute().get("values", [])

    cms_count = 0
    for row in cms_data:
        if row and row[0].strip():
            rows_to_insert.append({
                "sheet": "cms",
                "technology": row[0].strip(),
                "group_name": "",
            })
            cms_count += 1

    # ── OSearch sheet ─────────────────────────────────────────────────────────
    osearch_data = sheets.values().get(
        spreadsheetId=GOOGLE_SHEETS_CATALOG_ID,
        range="OSearch!A2:B"
    ).execute().get("values", [])

    osearch_count = 0
    for row in osearch_data:
        if row and row[0].strip():
            rows_to_insert.append({
                "sheet": "osearch",
                "technology": row[0].strip(),
                "group_name": row[1].strip() if len(row) > 1 else "",
            })
            osearch_count += 1

    # ── EMS sheet ─────────────────────────────────────────────────────────────
    ems_data = sheets.values().get(
        spreadsheetId=GOOGLE_SHEETS_CATALOG_ID,
        range="EMS!A2:A"
    ).execute().get("values", [])

    ems_count = 0
    for row in ems_data:
        if row and row[0].strip():
            rows_to_insert.append({
                "sheet": "ems",
                "technology": row[0].strip(),
                "group_name": "",
            })
            ems_count += 1

    # ── Save to BQ ────────────────────────────────────────────────────────────
    bq_client = client()

    # Clear existing catalog first
    bq_client.query(
        f"DELETE FROM `{table_ref(CATALOG_TABLE)}` WHERE TRUE"
    ).result()

    # Insert new rows in batches of 500
    batch_size = 500
    for i in range(0, len(rows_to_insert), batch_size):
        batch = rows_to_insert[i:i + batch_size]
        errors = bq_client.insert_rows_json(table_ref(CATALOG_TABLE), batch)
        if errors:
            logger.error(f"Catalog insert errors: {errors}")

    counts = {"cms": cms_count, "osearch": osearch_count, "ems": ems_count}
    logger.info(f"Catalog synced: {counts}")
    return counts


def get_catalog() -> dict:
    """
    Load catalog from BQ into memory.
    Returns:
      {
        "cms": ["Shopify", "WordPress", ...],
        "osearch": [{"technology": "Algolia Search", "group": "Algolia"}, ...],
        "ems": ["Klaviyo", "Mailchimp", ...],
      }
    """
    bq_client = client()
    try:
        rows = list(bq_client.query(
            f"SELECT sheet, technology, group_name FROM `{table_ref(CATALOG_TABLE)}`"
        ).result())
    except Exception as e:
        logger.error(f"Catalog load error: {e}")
        return {"cms": [], "osearch": [], "ems": []}

    cms = []
    osearch = []
    ems = []

    for row in rows:
        if row["sheet"] == "cms":
            cms.append(row["technology"])
        elif row["sheet"] == "osearch":
            osearch.append({
                "technology": row["technology"],
                "group": row["group_name"] or ""
            })
        elif row["sheet"] == "ems":
            ems.append(row["technology"])

    return {"cms": cms, "osearch": osearch, "ems": ems}


def match_technologies(bw_data: dict, catalog: dict) -> dict:
    """
    Match BuiltWith technologies against catalog.
    Uses LastDetected to pick the most recent active technology.
    """
    # Збираємо всі технології з датами LastDetected
    techs_with_dates = []
    try:
        for path in bw_data.get("Results", [])[0].get("Result", {}).get("Paths", []):
            for t in path.get("Technologies", []):
                name = t.get("Name", "")
                last = t.get("LastDetected", 0)
                if name:
                    techs_with_dates.append((name, last))
    except Exception:
        return {"cms_list": "", "osearch": "", "osearch_group": "", "ems_list": ""}

    if not techs_with_dates:
        return {"cms_list": "", "osearch": "", "osearch_group": "", "ems_list": ""}

    # Індекс: name.lower() -> (name, LastDetected)
    bw_index = {}
    for name, last in techs_with_dates:
        key = name.lower()
        if key not in bw_index or last > bw_index[key][1]:
            bw_index[key] = (name, last)

    # CMS — знаходимо всі співпадіння, беремо з найновішим LastDetected
    cms_match = ""
    cms_last = 0
    for cms in catalog.get("cms", []):
        key = cms.lower()
        if key in bw_index and bw_index[key][1] >= cms_last:
            cms_match = bw_index[key][0]
            cms_last = bw_index[key][1]

    # OSearch
    osearch_match = ""
    osearch_group = ""
    osearch_last = 0
    for entry in catalog.get("osearch", []):
        tech = entry["technology"]
        key = tech.lower()
        if key in bw_index and bw_index[key][1] >= osearch_last:
            osearch_match = bw_index[key][0]
            osearch_group = entry.get("group", "")
            osearch_last = bw_index[key][1]

    # EMS
    ems_match = ""
    ems_last = 0
    for ems in catalog.get("ems", []):
        key = ems.lower()
        if key in bw_index and bw_index[key][1] >= ems_last:
            ems_match = bw_index[key][0]
            ems_last = bw_index[key][1]

    return {
        "cms_list": cms_match,
        "osearch": osearch_match,
        "osearch_group": osearch_group,
        "ems_list": ems_match,
    }