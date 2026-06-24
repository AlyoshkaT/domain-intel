"""
Technologies API — aggregate BW technology data with time series.
Reads from privateBQ.bw_parsed.technologies_json — no corpBQ calls needed.
"""
import json
import logging
import re
import time
from datetime import datetime, timezone
from fastapi import APIRouter, Request
from google.cloud import bigquery as bq

from core.bigquery import client, table_ref, _bq_touch, _bq_op, track_bq_call, BW_PARSED_TABLE, _bq_qcfg

router = APIRouter(prefix="/api/technologies")
logger = logging.getLogger(__name__)


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
def aggregate_technologies(body: dict):
    date_from = body.get("date_from", "")
    date_to = body.get("date_to", "")
    show_unknown = body.get("show_unknown", False)
    granularity = body.get("granularity", "month")
    filter_domains = body.get("domains", [])
    logger.info(f"Technologies request: domains={len(filter_domains)}, from={date_from}, to={date_to}")

    try:
        from services.technology_catalog import get_catalog
        catalog = get_catalog()
        # catalog entries are dicts {technology, group, class} (or legacy strings).
        # Map every known tech name → its display name so catalog techs are recognised
        # (and not dumped into "unknown"). Group is used as the canonical label when set
        # so variants collapse onto one line (e.g. "Klaviyo for Shopify" → "Klaviyo").
        known: dict[str, str] = {}
        for sheet in ("cms", "ems", "osearch"):
            for t in catalog.get(sheet, []):
                if isinstance(t, str):
                    tech, grp = t, ""
                else:
                    tech, grp = t.get("technology", ""), t.get("group", "")
                if tech:
                    known[tech.lower()] = grp or tech

        _bq_touch("priv_r")
        bq_client = client()

        # Build domain filter
        if filter_domains:
            domain_list = ", ".join(f"'{d}'" for d in filter_domains[:10000])
            domain_where = f"WHERE LOWER(REGEXP_REPLACE(domain, r'^www\\.', '')) IN ({domain_list})"
        else:
            domain_where = ""

        # Read only domain + technologies_json from privateBQ bw_parsed.
        # This is orders of magnitude cheaper than reading full JSON blobs from corpBQ.
        with _bq_op("priv_r"):
            rows = list(bq_client.query(f"""
                SELECT domain, technologies_json
                FROM (
                    SELECT domain, technologies_json,
                           ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) AS rn
                    FROM `{table_ref(BW_PARSED_TABLE)}`
                    {domain_where}
                ) WHERE rn = 1
                LIMIT 50000
            """, job_config=_bq_qcfg()).result())
        track_bq_call("priv_bw")

        from_ts = 0
        to_ts = 9999999999999
        from_dt_clip: datetime | None = None
        to_dt_clip:   datetime | None = None
        if date_from:
            try:
                from_dt_clip = datetime.strptime(date_from + "-01", "%Y-%m-%d").replace(tzinfo=timezone.utc, day=1)
                from_ts = int(from_dt_clip.timestamp() * 1000)
            except: pass
        if date_to:
            try:
                to_dt_clip = datetime.strptime(date_to + "-01", "%Y-%m-%d").replace(tzinfo=timezone.utc, day=1)
                to_ts = int(to_dt_clip.timestamp() * 1000)
            except: pass

        tech_timeline: dict[str, dict[str, set]] = {}
        tech_domains: dict[str, list] = {}
        unknown_techs: dict[str, int] = {}
        known_canon: set[str] = set()   # canonicals that come from the catalog

        for idx, row in enumerate(rows):
            if idx % 2000 == 0:
                time.sleep(0)  # yield GIL so event loop can serve bq_activity polls
            domain = row["domain"]
            tj = row["technologies_json"]
            if not tj:
                continue
            try:
                techs = json.loads(tj) if isinstance(tj, str) else tj
                if not isinstance(techs, list):
                    continue
                for tech in techs:
                    # Compact format: {"n": name, "t": tag, "f": first_detected_ms, "l": last_detected_ms}
                    raw_name = tech.get("n", "")
                    if not raw_name:
                        continue
                    name_clean = _strip_version(raw_name)
                    name_lower = name_clean.lower()
                    first_ms = tech.get("f") or 0
                    last_ms = tech.get("l") or 0
                    if last_ms and last_ms < from_ts: continue
                    if first_ms and first_ms > to_ts: continue

                    canonical = known.get(name_lower)
                    if not canonical:
                        unknown_techs[name_clean] = unknown_techs.get(name_clean, 0) + 1
                        if not show_unknown:
                            continue
                        canonical = name_clean
                    else:
                        known_canon.add(canonical)

                    if canonical not in tech_timeline:
                        tech_timeline[canonical] = {}

                    if first_ms and last_ms:
                        f_dt = datetime.fromtimestamp(first_ms / 1000, tz=timezone.utc).replace(day=1)
                        l_dt = datetime.fromtimestamp(last_ms / 1000, tz=timezone.utc).replace(day=1)
                        # Clip timeline to selected date range so chart X-axis matches the filter
                        cur = max(f_dt, from_dt_clip) if from_dt_clip else f_dt
                        end_dt = min(l_dt, to_dt_clip) if to_dt_clip else l_dt
                        while cur <= end_dt:
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
                            "description": "",
                            "link": "",
                            "tag": tech.get("t", ""),
                            "first_detected": _ts_to_ym(first_ms) if first_ms else "",
                            "last_detected": _ts_to_ym(last_ms) if last_ms else "",
                        })
            except Exception as e:
                logger.warning(f"Parse error {domain}: {e}")

        all_periods = sorted(set(p for periods in tech_timeline.values() for p in periods))
        # Catalog (known) techs first, then by reach — so the top-50 cap never drops a
        # catalog technology in favour of a more frequent "unknown" one.
        series = sorted([{
            "name": name,
            "data": [len(periods.get(p, set())) for p in all_periods],
            "total": len(set(d for v in periods.values() for d in v)),
            "known": name in known_canon,
        } for name, periods in tech_timeline.items()], key=lambda x: (not x["known"], -x["total"]))

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
def add_technology_endpoint(body: dict):
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
async def export_technologies_xlsx(request: Request, body: dict):
    import io
    import pandas as pd
    from fastapi import HTTPException
    from fastapi.responses import StreamingResponse
    rows = body.get("rows", [])
    if not rows:
        raise HTTPException(status_code=400, detail="No rows to export")
    try:
        from core.bigquery import log_activity
        log_activity(getattr(request.state, "username", "unknown"), "tech_export_xlsx", {"row_count": len(rows)})
    except Exception:
        pass
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
