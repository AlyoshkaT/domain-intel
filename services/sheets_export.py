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

def traffic_rank(visits) -> str:
    """Categorise sw_visits into a sortable rank label (mirrors the Sheets IFs formula)."""
    try:
        v = int(visits or 0)
    except (TypeError, ValueError):
        return ""
    if v <= 0:        return ""
    if v > 1_000_000: return "a.1M+"
    if v > 200_000:   return "b.200k+"
    if v > 100_000:   return "c.100k+"
    if v > 50_000:    return "d.50k+"
    if v > 30_000:    return "e.30k+"
    if v > 10_000:    return "f.Small"
    return "g.micro"


# Column definitions: (result_key, header_label)
# Keys starting with "_" are computed — handled specially in _build_rows.
EXPORT_COLUMNS = [
    ("domain",               "Domain"),
    ("sw_visits",            "Traffic"),
    ("_traffic_rank",        "Traffic_Rank"),      # computed from sw_visits
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


# ── Analytics pivot helpers ────────────────────────────────────────────────────

RANK_COLS = ["a.1M+", "b.200k+", "c.100k+", "d.50k+", "e.30k+", "f.Small", "g.micro"]

# 4 pivot dimensions shown in the Analytics sheet
ANALYTICS_PIVOTS = [
    ("ems_list",      "EMS"),
    ("cms_list",      "CMS"),
    ("ai_category",   "AI Category"),
    ("osearch_group", "oSearch Group"),
]


def _build_pivot_rows(results: list[dict], field: str, field_label: str) -> list[list]:
    """
    Build one pivot table: field_values (rows) × Traffic_Rank (columns) → count of domains.
    Empty field values are excluded from individual rows but counted in the Total row.
    """
    from collections import defaultdict
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    col_totals: dict[str, int] = defaultdict(int)

    for r in results:
        val = (r.get(field) or "").strip()
        if not val:
            continue
        rank = traffic_rank(r.get("sw_visits"))
        counts[val][rank] += 1
        col_totals[rank] += 1

    grand_total = sum(col_totals.values())
    row_totals = {v: sum(rc.values()) for v, rc in counts.items()}
    sorted_vals = sorted(counts, key=lambda v: -row_totals[v])

    def _rank_cells(rank_counts: dict) -> list:
        return [rank_counts.get(r) or "" for r in RANK_COLS]

    header   = [field_label, "Domain"] + RANK_COLS + ["Grand Total"]
    total_row = [" Total",  ""] + _rank_cells(col_totals) + [grand_total or ""]
    data_rows = [
        [f"{v} Total", ""] + _rank_cells(counts[v]) + [row_totals[v]]
        for v in sorted_vals
    ]
    return [header, total_row] + data_rows


def build_analytics_rows(results: list[dict]) -> tuple[list[list], list[int]]:
    """
    Stack all 4 pivot tables with 2 blank rows between them.
    Returns (rows, header_row_indices) so callers can bold the header rows.
    """
    all_rows: list[list] = []
    header_indices: list[int] = []
    for field, label in ANALYTICS_PIVOTS:
        if all_rows:
            all_rows += [[], []]
        header_indices.append(len(all_rows))
        all_rows += _build_pivot_rows(results, field, label)
    return all_rows, header_indices


def _col_letter(n: int) -> str:
    """0-based column index → Sheets column letter (0→A, 26→AA, ...)."""
    result = ""
    n += 1
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(ord("A") + rem) + result
    return result


def _create_pivot_tables(sh, sheet_id: str,
                          data_tab_id: int, analytics_tab_id: int,
                          n_data_rows: int, data_tab_name: str = "Data",
                          analytics_tab_name: str = "Analytics",
                          n_cols: int | None = None):
    """
    Create 4 real interactive Google Sheets pivot tables in the Analytics sheet.

    Layout (horizontal, 2-column gap between each):
      [AI Category] .. [CMS] .. [EMS] .. [oSearch Group]

    Each block = 10 cols (label + Domain + 7 ranks + Grand Total) + 2 gap = 12 cols.

    Row 0 (row 1 in Sheets): "Grand Total" label + VLOOKUP formulas for each rank column
    Row 1 (row 2): empty separator
    Row 2 (row 3): pivot table starts here
      — Google Sheets renders 2 header rows → data starts at row 5 (1-based)
    """
    col_idx    = {label: i for i, (_, label) in enumerate(EXPORT_COLUMNS)}
    domain_col = col_idx.get("Domain", 0)
    rank_col   = col_idx.get("Traffic_Rank", 2)
    end_col    = n_cols if n_cols is not None else len(EXPORT_COLUMNS)

    source = {
        "sheetId":          data_tab_id,
        "startRowIndex":    0,
        "startColumnIndex": 0,
        "endRowIndex":      n_data_rows + 1,
        "endColumnIndex":   end_col,
    }

    # Order matches what user wants: AI Category, CMS, EMS, oSearch Group
    PIVOTS = [
        ("AI Category",   col_idx.get("AI Category", 7)),
        ("CMS",           col_idx.get("CMS", 3)),
        ("EMS",           col_idx.get("EMS", 6)),
        ("oSearch Group", col_idx.get("oSearch Group", 4)),
    ]

    BLOCK_COLS  = 12   # 10 pivot cols + 2 gap
    PIVOT_COLS  = 10   # label + Domain + 7 ranks + Grand Total
    PIVOT_ROW   = 2    # 0-based row index (A3 in Sheets) — rows 0,1 reserved for headers
    RANK_COUNT  = 7    # a.1M+ … g.micro
    # Google Sheets pivot with column grouping renders 2 header rows.
    # Pivot anchored at row index 2 → header rows at 2,3 → data starts at row 5 (1-based)
    DATA_ROW_1B = PIVOT_ROW + 3   # = 5 (1-based)

    # ── Step 1: expand Analytics sheet columns ────────────────────────────────
    # A new sheet has only 26 columns (A–Z). Our pivots need columns up to AT (col 45).
    # If the sheet doesn't have enough columns the API silently ignores the columnIndex
    # and stacks all pivots vertically at column 0. So we expand first.
    needed_cols = len(PIVOTS) * BLOCK_COLS + 4   # e.g. 52
    try:
        meta = sh.get(spreadsheetId=sheet_id,
                      fields="sheets(properties(sheetId,gridProperties))").execute()
        current_cols = next(
            (s["properties"]["gridProperties"]["columnCount"]
             for s in meta.get("sheets", [])
             if s["properties"]["sheetId"] == analytics_tab_id),
            26,
        )
        if current_cols < needed_cols:
            sh.batchUpdate(spreadsheetId=sheet_id, body={"requests": [{
                "appendDimension": {
                    "sheetId": analytics_tab_id,
                    "dimension": "COLUMNS",
                    "length": needed_cols - current_cols,
                }
            }]}).execute()
            logger.info(f"Analytics sheet expanded to {needed_cols} columns")
    except Exception as e:
        logger.warning(f"Could not expand Analytics sheet columns: {e}")

    # ── Step 2: place each pivot in ONE batchUpdate (separate updateCells per pivot) ─
    def _pivot_spec(dim_col: int) -> dict:
        return {"pivotTable": {
            "source": source,
            "rows": [{"sourceColumnOffset": dim_col,
                       "showTotals": True, "sortOrder": "DESCENDING"}],
            "columns": [{"sourceColumnOffset": rank_col,
                          "showTotals": True, "sortOrder": "ASCENDING"}],
            "values": [{"summarizeFunction": "COUNTA",
                         "sourceColumnOffset": domain_col, "name": "Domains"}],
            "valueLayout": "HORIZONTAL",
        }}

    requests = [
        {
            "updateCells": {
                "rows": [{"values": [_pivot_spec(dim_col)]}],
                "start": {
                    "sheetId":     analytics_tab_id,
                    "rowIndex":    PIVOT_ROW,
                    "columnIndex": i * BLOCK_COLS,
                },
                "fields": "pivotTable",
            }
        }
        for i, (_label, dim_col) in enumerate(PIVOTS)
    ]

    try:
        sh.batchUpdate(spreadsheetId=sheet_id, body={"requests": requests}).execute()
        logger.info(
            f"4 pivot tables placed at columns "
            f"{[i*BLOCK_COLS for i in range(len(PIVOTS))]} in {sheet_id}"
        )
    except Exception as e:
        logger.error(f"Pivot creation failed: {e}", exc_info=True)
        raise

    # ── Step 3: Grand Total summary row at row 1 (A1, M1, Y1, AK1) ─────────────
    # Each pivot counts only domains with a NON-EMPTY value for its dimension,
    # so Grand Totals differ per pivot (e.g. EMS=1265, CMS=1264, ...).
    # Formula: COUNTIFS(rank_col, rank_val, dim_col, "<>") — matches pivot exactly.
    RANK_VALUES = ["a.1M+", "b.200k+", "c.100k+", "d.50k+", "e.30k+", "f.Small", "g.micro"]
    rank_col_letter = _col_letter(rank_col)   # "C" — Traffic_Rank at col index 2
    safe_name = data_tab_name.replace("'", "''")

    formula_data = []
    for i, (_label, dim_col) in enumerate(PIVOTS):
        sc         = i * BLOCK_COLS
        lbl_col    = _col_letter(sc)                    # A / M / Y / AK
        last_f_col = _col_letter(sc + 1 + RANK_COUNT)  # I / U / AG / AS
        dim_letter = _col_letter(dim_col)               # H / D / G / E

        row = ["Grand Total", ""]   # col+0: label, col+1: blank (Domain col)
        for rv in RANK_VALUES:
            # Count domains where Traffic_Rank = rv AND this dimension is not empty
            row.append(
                f"=COUNTIFS("
                f"'{safe_name}'!${rank_col_letter}:${rank_col_letter},\"{rv}\","
                f"'{safe_name}'!${dim_letter}:${dim_letter},\"<>\")"
            )

        safe_analytics = analytics_tab_name.replace("'", "''")
        formula_data.append({"range": f"'{safe_analytics}'!{lbl_col}1:{last_f_col}1", "values": [row]})

    try:
        sh.values().batchUpdate(
            spreadsheetId=sheet_id,
            body={"valueInputOption": "USER_ENTERED", "data": formula_data},
        ).execute()
        logger.info(f"VLOOKUP headers written at row 1 for {len(PIVOTS)} pivots")
    except Exception as e:
        logger.error(f"Formula write failed: {e}", exc_info=True)
        raise


def _cell_value(row: dict, key: str):
    """Resolve a column key to a cell value, handling computed fields."""
    if key == "_traffic_rank":
        return traffic_rank(row.get("sw_visits"))
    val = row.get(key)
    if val is None:
        return ""
    if key == "sw_visits":
        try:
            return int(val)
        except (TypeError, ValueError):
            return val
    return str(val)


def _build_rows(results: list[dict], cols: list[tuple]) -> list[list]:
    headers = [col[1] for col in cols]
    return [headers] + [[_cell_value(r, key) for key, _ in cols] for r in results]


def results_to_dataframe(results: list[dict], cols: list[tuple] | None = None):
    """Build a pandas DataFrame from results using EXPORT_COLUMNS order."""
    import pandas as pd
    cols = cols or EXPORT_COLUMNS
    headers = [c[1] for c in cols]
    data = [[_cell_value(r, key) for key, _ in cols] for r in results]
    return pd.DataFrame(data, columns=headers)


def _write_data_tab(sh, sheet_id: str, tab_id: int, tab_title: str, rows: list[list]):
    """Write data rows and apply standard header formatting to one tab."""
    n_cols = len(rows[0]) if rows else 1
    sh.values().update(
        spreadsheetId=sheet_id,
        range=f"'{tab_title}'!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()
    sh.batchUpdate(spreadsheetId=sheet_id, body={"requests": [
        {"repeatCell": {
            "range": {"sheetId": tab_id, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {"userEnteredFormat": {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.98},
            }},
            "fields": "userEnteredFormat(textFormat,backgroundColor)",
        }},
        {"updateSheetProperties": {
            "properties": {"sheetId": tab_id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount",
        }},
        {"autoResizeDimensions": {
            "dimensions": {"sheetId": tab_id, "dimension": "COLUMNS",
                           "startIndex": 0, "endIndex": n_cols},
        }},
    ]}).execute()


def _add_tab(sh, sheet_id: str, title: str) -> int:
    """Add a new tab and return its sheetId."""
    resp = sh.batchUpdate(spreadsheetId=sheet_id, body={"requests": [
        {"addSheet": {"properties": {"title": title}}}
    ]}).execute()
    return resp["replies"][0]["addSheet"]["properties"]["sheetId"]


def _export_as_tab(sheet_id: str, tab_title: str, rows: list[list],
                   analytics: bool = False) -> str:
    """Add tab(s) to an existing spreadsheet. Returns URL with #gid anchor."""
    sh = sheets_client(write=True).spreadsheets()
    tab_id = _add_tab(sh, sheet_id, tab_title)
    n_cols = len(rows[0]) if rows else len(EXPORT_COLUMNS)
    _write_data_tab(sh, sheet_id, tab_id, tab_title, rows)

    if analytics:
        try:
            analytics_tab_title = f"{tab_title} Analytics"
            a_id = _add_tab(sh, sheet_id, analytics_tab_title)
            _create_pivot_tables(sh, sheet_id, tab_id, a_id, len(rows) - 1,
                                 data_tab_name=tab_title, analytics_tab_name=analytics_tab_title, n_cols=n_cols)
        except Exception as e:
            logger.error(f"Analytics tab creation failed (data tab still created): {e}", exc_info=True)

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}#gid={tab_id}"
    logger.info(f"Tab created: {url} ({len(rows)-1} rows)")
    return url


def _cleanup_old_drive_files(dr, folder_id: str, keep_days: int = 7) -> None:
    """Delete files older than keep_days from the Drive folder (only files owned by SA)."""
    try:
        from datetime import datetime, timezone, timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).isoformat()
        result = dr.files().list(
            q=f"'{folder_id}' in parents and createdTime < '{cutoff}'",
            fields="files(id, name, createdTime)",
            pageSize=50,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        for f in result.get("files", []):
            try:
                dr.files().delete(fileId=f["id"], supportsAllDrives=True).execute()
                logger.info(f"Auto-deleted old export: {f['name']}")
            except Exception as e:
                logger.debug(f"Could not delete {f['name']}: {e}")
    except Exception as e:
        logger.debug(f"Cleanup skipped: {e}")


def _export_as_new_file(folder_id: str, title: str, rows: list[list],
                        analytics: bool = False) -> str:
    """Create a new spreadsheet in Drive. Returns URL."""
    from services.sheets_client import drive_client
    dr = drive_client()
    sh = sheets_client(write=True).spreadsheets()

    # Clean up old exports before creating a new one
    if folder_id:
        _cleanup_old_drive_files(dr, folder_id)

    body: dict = {"name": title, "mimeType": "application/vnd.google-apps.spreadsheet"}
    if folder_id:
        body["parents"] = [folder_id]
    file = dr.files().create(body=body, fields="id", supportsAllDrives=True).execute()
    sheet_id = file["id"]
    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"

    # Rename default sheet to "Data"
    sh.batchUpdate(spreadsheetId=sheet_id, body={"requests": [
        {"updateSheetProperties": {
            "properties": {"sheetId": 0, "title": "Data"},
            "fields": "title",
        }}
    ]}).execute()
    n_cols = len(rows[0]) if rows else len(EXPORT_COLUMNS)
    _write_data_tab(sh, sheet_id, 0, "Data", rows)

    if analytics:
        try:
            a_id = _add_tab(sh, sheet_id, "Analytics")
            _create_pivot_tables(sh, sheet_id, 0, a_id, len(rows) - 1,
                                 data_tab_name="Data", analytics_tab_name="Analytics", n_cols=n_cols)
        except Exception as e:
            logger.error(f"Analytics tab creation failed (data file still created): {e}", exc_info=True)

    dr.permissions().create(
        fileId=sheet_id,
        body={"type": "anyone", "role": "reader"},
        supportsAllDrives=True,
    ).execute()

    # Transfer ownership to the real user so files don't count against service-account quota
    owner_email = os.getenv("GOOGLE_DRIVE_OWNER_EMAIL", "").strip()
    if owner_email:
        try:
            dr.permissions().create(
                fileId=sheet_id,
                body={"type": "user", "role": "owner", "emailAddress": owner_email},
                transferOwnership=True,
                supportsAllDrives=True,
            ).execute()
            logger.info(f"Ownership transferred to {owner_email}")
        except Exception as e:
            logger.warning(f"Could not transfer ownership to {owner_email}: {e}")

    logger.info(f"Sheet created: {sheet_url} ({len(rows)-1} rows)")
    return sheet_url


def _folder_id_from_url(url_or_id: str) -> str:
    """Extract folder ID from a Google Drive URL or return as-is if already an ID."""
    import re
    if not url_or_id:
        return ""
    # https://drive.google.com/drive/folders/<ID>
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", url_or_id)
    if m:
        return m.group(1)
    return url_or_id.strip()


def _create_sheet(title: str, tab_title: str, results: list[dict],
                  columns: list[tuple] = None,
                  folder_id: str = "",
                  analytics: bool = False) -> str:
    """Route to tab-mode or new-file-mode. analytics=True adds a second Analytics sheet."""
    cols = columns or EXPORT_COLUMNS
    rows = _build_rows(results, cols)

    # Tab mode: append to one existing sheet (no Drive quota needed)
    sheet_id = os.getenv("GOOGLE_EXPORT_SHEET_ID", "").strip()
    if sheet_id and not folder_id:
        return _export_as_tab(sheet_id, tab_title, rows, analytics)

    # New-file mode: use provided folder_id, env var, or OAuth root Drive
    effective_folder = _folder_id_from_url(folder_id) or os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
    using_oauth = bool(os.getenv("GOOGLE_OAUTH_TOKEN_JSON", "").strip())

    if effective_folder or using_oauth:
        return _export_as_new_file(effective_folder, title, rows, analytics)

    raise ValueError(
        "Google Sheets export not configured. "
        "Set GOOGLE_OAUTH_TOKEN_JSON (run get_google_token.py) "
        "or GOOGLE_EXPORT_SHEET_ID (existing sheet ID)."
    )


def export_job_to_sheets(job_id: str, filename: str, results: list[dict],
                         folder_id: str = "", analytics: bool = False) -> Optional[str]:
    """Export job results to Google Sheets. analytics=True adds a pivot-table Analytics sheet."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    title = f"Domain Intel — {filename} — {ts}"
    tab_title = f"{filename[:25]} {ts[11:]}"
    try:
        return _create_sheet(title, tab_title, results, folder_id=folder_id, analytics=analytics)
    except Exception as e:
        logger.error(f"Sheets export error: {e}")
        raise


def export_explorer_to_sheets(label: str, results: list[dict],
                               folder_id: str = "", analytics: bool = False) -> Optional[str]:
    """Export Explorer results to Google Sheets. analytics=True adds Analytics sheet."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    title = f"Domain Intel — Explorer — {label} — {ts}"
    tab_title = f"Explorer {ts[5:16]}"
    cols = [c for c in EXPORT_COLUMNS if c[0] not in ("status", "error_detail")]
    try:
        return _create_sheet(title, tab_title, results, columns=cols,
                             folder_id=folder_id, analytics=analytics)
    except Exception as e:
        logger.error(f"Sheets export error: {e}")
        raise
