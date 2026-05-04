"""
Technology Catalog service
Reads CMS / OSearch / EMS from Google Sheets and stores in BigQuery.
Supports write-back: appending new technologies to the sheet.
"""
import logging
from typing import Optional
from google.cloud import bigquery as bq

from config.settings import GOOGLE_SHEETS_CATALOG_ID
from core.bigquery import client, table_ref
from services.sheets_client import sheets_client

logger = logging.getLogger(__name__)

CATALOG_TABLE = "technology_catalog"

CATALOG_SCHEMA = [
    bq.SchemaField("sheet", "STRING"),       # cms / osearch / ems
    bq.SchemaField("technology", "STRING"),  # назва технології
    bq.SchemaField("group_name", "STRING"),  # група (тільки для osearch)
]

# Sheet tab name → BQ sheet value
SHEET_TABS = {
    "cms":     ("CMS",     "A2:A"),
    "osearch": ("OSearch", "A2:B"),
    "ems":     ("EMS",     "A2:A"),
}


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
        raise ValueError("GOOGLE_SHEETS_CATALOG_ID not set")

    ensure_catalog_table()
    sh = sheets_client(write=False).spreadsheets()
    rows_to_insert = []

    counts = {}

    # CMS
    cms_data = sh.values().get(
        spreadsheetId=GOOGLE_SHEETS_CATALOG_ID, range="CMS!A2:A"
    ).execute().get("values", [])
    cms_count = 0
    for row in cms_data:
        if row and row[0].strip():
            rows_to_insert.append({"sheet": "cms", "technology": row[0].strip(), "group_name": ""})
            cms_count += 1
    counts["cms"] = cms_count

    # OSearch
    osearch_data = sh.values().get(
        spreadsheetId=GOOGLE_SHEETS_CATALOG_ID, range="OSearch!A2:B"
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
    counts["osearch"] = osearch_count

    # EMS
    ems_data = sh.values().get(
        spreadsheetId=GOOGLE_SHEETS_CATALOG_ID, range="EMS!A2:A"
    ).execute().get("values", [])
    ems_count = 0
    for row in ems_data:
        if row and row[0].strip():
            rows_to_insert.append({"sheet": "ems", "technology": row[0].strip(), "group_name": ""})
            ems_count += 1
    counts["ems"] = ems_count

    # Save to BQ — use load job (WRITE_TRUNCATE) instead of streaming insert,
    # so DML DELETE works immediately (streaming buffer blocks DML DELETE).
    bq_client = client()
    tbl_ref = table_ref(CATALOG_TABLE)
    job_cfg = bq.LoadJobConfig(
        schema=[
            bq.SchemaField("sheet", "STRING"),
            bq.SchemaField("technology", "STRING"),
            bq.SchemaField("group_name", "STRING"),
        ],
        write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
        source_format=bq.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    import json as _json
    ndjson = "\n".join(_json.dumps(r) for r in rows_to_insert).encode()
    import io as _io
    job = bq_client.load_table_from_file(
        _io.BytesIO(ndjson), tbl_ref, job_config=job_cfg
    )
    job.result()  # wait for completion
    if job.errors:
        logger.error(f"Catalog load job errors: {job.errors}")

    logger.info(f"Catalog synced: {counts}")
    return counts


def add_technology(sheet: str, technology: str, group_name: str = "") -> bool:
    """
    Append a new technology to the Google Sheet and BQ catalog.
    sheet: 'cms' | 'osearch' | 'ems'
    Returns True on success.
    """
    if not GOOGLE_SHEETS_CATALOG_ID:
        raise ValueError("GOOGLE_SHEETS_CATALOG_ID not set")
    if sheet not in SHEET_TABS:
        raise ValueError(f"Unknown sheet: {sheet}. Must be: cms, osearch, ems")

    tab_name = SHEET_TABS[sheet][0]

    # 1. Check not already in BQ
    bq_client = client()
    rows = list(bq_client.query(f"""
        SELECT technology FROM `{table_ref(CATALOG_TABLE)}`
        WHERE sheet = '{sheet}' AND LOWER(technology) = LOWER('{technology.replace("'", "''")}')
        LIMIT 1
    """).result())
    if rows:
        logger.info(f"Technology already exists: {sheet}/{technology}")
        return False

    # 2. Append to Google Sheet
    sh = sheets_client(write=True).spreadsheets()
    row_values = [technology, group_name] if sheet == "osearch" else [technology]
    sh.values().append(
        spreadsheetId=GOOGLE_SHEETS_CATALOG_ID,
        range=f"{tab_name}!A:A",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row_values]}
    ).execute()
    logger.info(f"Added to GSheet {tab_name}: {technology}")

    # 3. Add to BQ catalog via DML INSERT (not streaming — so DELETE works immediately)
    try:
        bq_client.query(
            f"INSERT INTO `{table_ref(CATALOG_TABLE)}` (sheet, technology, group_name) "
            f"VALUES (@sheet, @tech, @grp)",
            job_config=bq.QueryJobConfig(
                query_parameters=[
                    bq.ScalarQueryParameter("sheet", "STRING", sheet),
                    bq.ScalarQueryParameter("tech", "STRING", technology),
                    bq.ScalarQueryParameter("grp", "STRING", group_name),
                ]
            )
        ).result()
    except Exception as e:
        logger.error(f"BQ catalog insert error: {e}")
        return False

    return True


def get_catalog() -> dict:
    """Load catalog from BQ into memory."""
    bq_client = client()
    try:
        rows = list(bq_client.query(
            f"SELECT sheet, technology, group_name FROM `{table_ref(CATALOG_TABLE)}`"
        ).result())
    except Exception as e:
        logger.error(f"Catalog load error: {e}")
        return {"cms": [], "osearch": [], "ems": []}

    cms, osearch, ems = [], [], []
    for row in rows:
        if row["sheet"] == "cms":
            cms.append(row["technology"])
        elif row["sheet"] == "osearch":
            osearch.append({"technology": row["technology"], "group": row["group_name"] or ""})
        elif row["sheet"] == "ems":
            ems.append(row["technology"])

    return {"cms": cms, "osearch": osearch, "ems": ems}


def match_technologies(bw_data: dict, catalog: dict) -> dict:
    """Match BuiltWith technologies against catalog."""
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

    bw_index = {}
    for name, last in techs_with_dates:
        key = name.lower()
        if key not in bw_index or last > bw_index[key][1]:
            bw_index[key] = (name, last)

    cms_match, cms_last = "", 0
    for cms in catalog.get("cms", []):
        key = cms.lower()
        if key in bw_index and bw_index[key][1] >= cms_last:
            cms_match = bw_index[key][0]; cms_last = bw_index[key][1]

    osearch_match, osearch_group, osearch_last = "", "", 0
    for entry in catalog.get("osearch", []):
        key = entry["technology"].lower()
        if key in bw_index and bw_index[key][1] >= osearch_last:
            osearch_match = bw_index[key][0]
            osearch_group = entry.get("group", "")
            osearch_last = bw_index[key][1]

    ems_match, ems_last = "", 0
    for ems in catalog.get("ems", []):
        key = ems.lower()
        if key in bw_index and bw_index[key][1] >= ems_last:
            ems_match = bw_index[key][0]; ems_last = bw_index[key][1]

    return {
        "cms_list": cms_match,
        "osearch": osearch_match,
        "osearch_group": osearch_group,
        "ems_list": ems_match,
    }
