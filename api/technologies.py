"""
Technologies API — aggregate BW technology data with time series.
"""
import json
import logging
import re
from datetime import datetime, timezone
from fastapi import APIRouter
from google.cloud import bigquery as bq

from core.bigquery import corp_client
from config.settings import CORP_PROJECT_ID, CORP_DATASET

router = APIRouter(prefix="/api/technologies")
logger = logging.getLogger(__name__)
BW_TABLE = f"`{CORP_PROJECT_ID}.{CORP_DATASET}.builtwith_raw_data`"


def _strip_version(name: str) -> str:
    name = re.sub(r'\s+v?\d+[\d\.x]*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+\d+[\d\.x]*$', '', name)
    return name.strip()


def _ts_to_ym(ts_ms: int) -> str:
    try:
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m")
    except Exception:
        return ""


@router.post("/aggregate")
async def aggregate_technologies(body: dict):
    date_from = body.get("date_from", "")
    date_to = body.get("date_to", "")
    show_unknown = body.get("show_unknown", False)
    granularity = body.get("granularity", "month")
    filter_domains = body.get("domains", [])
    logger.info(f"Technologies request: domains={len(filter_domains)}, from={date_from}, to={date_to}")
    logger.info(f"Technologies request: domains={len(filter_domains)}, from={date_from}, to={date_to}")

    try:
        from services.technology_catalog import get_catalog
        catalog = get_catalog()
        known: dict[str, str] = {}
        for t in catalog.get("cms", []):
            name = t if isinstance(t, str) else t.get("name", "")
            if name: known[name.lower()] = name
        for t in catalog.get("ems", []):
            name = t if isinstance(t, str) else t.get("name", "")
            if name: known[name.lower()] = name
        for t in catalog.get("osearch", []):
            name = t if isinstance(t, str) else t.get("name", "")
            if name: known[name.lower()] = name

        corp = corp_client()
        if filter_domains:
            domain_list = ", ".join(f"'{d}'" for d in filter_domains[:10000])
            domain_where = f"WHERE LOWER(REGEXP_REPLACE(domain, r'^www\\.', '')) IN ({domain_list})"
        else:
            domain_where = ""

        rows = list(corp.query(f"""
            SELECT domain, response_json FROM {BW_TABLE}
            {domain_where}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
            LIMIT 50000
        """).result())

        from_ts = 0
        to_ts = 9999999999999
        if date_from:
            try: from_ts = int(datetime.strptime(date_from + "-01", "%Y-%m-%d").timestamp() * 1000)
            except: pass
        if date_to:
            try: to_ts = int(datetime.strptime(date_to + "-01", "%Y-%m-%d").timestamp() * 1000)
            except: pass

        tech_timeline: dict[str, dict[str, set]] = {}
        tech_domains: dict[str, list] = {}
        unknown_techs: dict[str, int] = {}

        for row in rows:
            domain = row["domain"]
            rj = row["response_json"]
            data = rj if isinstance(rj, dict) else (json.loads(rj) if rj else None)
            if not data:
                continue
            try:
                results = data.get("Results", [])
                if not results:
                    continue
                paths = results[0].get("Result", {}).get("Paths", [])
                for path in paths:
                    for tech in path.get("Technologies", []):
                        raw_name = tech.get("Name", "")
                        if not raw_name:
                            continue
                        name_clean = _strip_version(raw_name)
                        name_lower = name_clean.lower()
                        first_ms = tech.get("FirstDetected", 0)
                        last_ms = tech.get("LastDetected", 0)
                        if last_ms and last_ms < from_ts: continue
                        if first_ms and first_ms > to_ts: continue

                        canonical = known.get(name_lower)
                        if not canonical:
                            unknown_techs[name_clean] = unknown_techs.get(name_clean, 0) + 1
                            if not show_unknown:
                                continue
                            canonical = name_clean

                        if canonical not in tech_timeline:
                            tech_timeline[canonical] = {}

                        if first_ms and last_ms:
                            f_dt = datetime.fromtimestamp(first_ms / 1000, tz=timezone.utc).replace(day=1)
                            l_dt = datetime.fromtimestamp(last_ms / 1000, tz=timezone.utc).replace(day=1)
                            cur = f_dt
                            while cur <= l_dt:
                                if granularity == "year":
                                    period = cur.strftime("%Y")
                                elif granularity == "quarter":
                                    q = (cur.month - 1) // 3 + 1
                                    period = f"{cur.year}-Q{q}"
                                else:
                                    period = cur.strftime("%Y-%m")
                                if period not in tech_timeline[canonical]:
                                    tech_timeline[canonical][period] = set()
                                tech_timeline[canonical][period].add(domain)
                                if granularity == "year":
                                    cur = cur.replace(year=cur.year + 1)
                                elif granularity == "quarter":
                                    m = cur.month + 3
                                    cur = cur.replace(year=cur.year + (1 if m > 12 else 0), month=m - 12 if m > 12 else m)
                                else:
                                    m = cur.month + 1
                                    cur = cur.replace(year=cur.year + (1 if m > 12 else 0), month=1 if m > 12 else m)

                        if canonical not in tech_domains:
                            tech_domains[canonical] = []
                        if len(tech_domains[canonical]) < 100:
                            tech_domains[canonical].append({
                                "domain": domain, "name": canonical,
                                "description": tech.get("Description", ""),
                                "link": tech.get("Link", ""),
                                "tag": tech.get("Tag", ""),
                                "first_detected": _ts_to_ym(first_ms) if first_ms else "",
                                "last_detected": _ts_to_ym(last_ms) if last_ms else "",
                            })
            except Exception as e:
                logger.warning(f"Parse error {domain}: {e}")

        all_periods = sorted(set(p for periods in tech_timeline.values() for p in periods))
        series = sorted([{
            "name": name,
            "data": [len(periods.get(p, set())) for p in all_periods],
            "total": sum(len(v) for v in periods.values()),
        } for name, periods in tech_timeline.items()], key=lambda x: -x["total"])

        table_rows = []
        for rows_list in tech_domains.values():
            table_rows.extend(rows_list)
        table_rows.sort(key=lambda x: x.get("last_detected", ""), reverse=True)

        return {
            "periods": all_periods,
            "series": series[:50],
            "table": table_rows[:1000],
            "unknown_count": len(unknown_techs),
            "unknown_top": sorted(unknown_techs.items(), key=lambda x: -x[1])[:20],
            "total_domains": len(rows),
        }
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return {"error": str(e), "periods": [], "series": [], "table": []}


@router.post("/catalog/add")
async def add_technology_endpoint(body: dict):
    """Add a new technology to Google Sheet + BQ catalog."""
    from fastapi import HTTPException
    from services.technology_catalog import add_technology
    sheet = body.get("sheet", "").strip()
    technology = body.get("technology", "").strip()
    group_name = body.get("group_name", "").strip()
    if not sheet or not technology:
        raise HTTPException(status_code=400, detail="sheet and technology are required")
    try:
        added = add_technology(sheet, technology, group_name)
        return {"added": added, "technology": technology, "sheet": sheet}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/export/xlsx")
async def export_technologies_xlsx(body: dict):
    import io
    import pandas as pd
    from fastapi import HTTPException
    from fastapi.responses import StreamingResponse
    rows = body.get("rows", [])
    if not rows:
        raise HTTPException(status_code=400, detail="No rows to export")
    df = pd.DataFrame(rows)
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Technologies")
    stream.seek(0)
    filename = f"technologies_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
