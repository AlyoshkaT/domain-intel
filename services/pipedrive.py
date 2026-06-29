"""
Pipedrive CRM connector — relationship status per domain.

Model (see memory project_pipedrive_model):
  Each WON deal = one payment for a period. won_time = payment date,
  value+currency = amount. The custom field "Domen" links a deal to a domain.
  There are no subscription objects, so payment history = won deals grouped by
  won_time month.

Two privateBQ tables:
  pipedrive_deals_raw  — one row per deal (WRITE_TRUNCATE each sync).
  pipedrive_status     — one row per domain: current Pipedrive status, the
                         Paid -1/-2/-3 calendar-month flags, Status FACT and a
                         derived risk label (Alarm / Churn).

Status FACT (from the user's table):
  any payment in the last 3 calendar months  -> Won
  else: Open -> Open, Won -> Lost, Lost -> Lost
Risk label:
  Alarm  — no payment in last 2 months, last payment ~3 months ago.
  Churn  — paid within the last year but not in the last 3 months.
"""
import io
import json
import logging
import re
import time
from collections import defaultdict
from datetime import date, datetime

import httpx
from google.cloud import bigquery

from config.settings import PIPEDRIVE_API_TOKEN, PIPEDRIVE_COMPANY_DOMAIN, REQUEST_TIMEOUT
from core.bigquery import client, table_ref

logger = logging.getLogger(__name__)

DEALS_RAW_TABLE = "pipedrive_deals_raw"
STATUS_TABLE = "pipedrive_status"

# Custom deal field keys (discovered from /dealFields).
F_DOMEN = "008b81ed34c02301397301892241ef26029fbd62"
F_TARIFF = "73cec5f72f2013cfc8479d276920416ba66561da"

_PAGE = 500  # Pipedrive max page size for /deals


def _base() -> str:
    c = (PIPEDRIVE_COMPANY_DOMAIN or "").strip()
    return f"https://{c}.pipedrive.com/api/v1" if c else "https://api.pipedrive.com/v1"


def normalize_domain(raw: str | None) -> str:
    """Lower-case bare domain: strip scheme, path, query, leading www, trailing dot."""
    if not raw:
        return ""
    s = str(raw).strip().lower()
    s = re.sub(r"^[a-z]+://", "", s)        # scheme
    s = s.split("/")[0].split("?")[0]       # path / query
    s = s.split("@")[-1]                     # stray email-ish prefix
    if s.startswith("www."):
        s = s[4:]
    return s.strip().strip(".")


# ── Fetch ───────────────────────────────────────────────────────────────────

def _fetch_all_deals() -> list[dict]:
    """Paginate every non-deleted deal. Returns raw deal dicts."""
    if not PIPEDRIVE_API_TOKEN:
        raise RuntimeError("PIPEDRIVE_API_TOKEN not configured")
    out: list[dict] = []
    start = 0
    with httpx.Client(timeout=max(REQUEST_TIMEOUT, 30)) as cli:
        while True:
            r = cli.get(f"{_base()}/deals", params={
                "api_token": PIPEDRIVE_API_TOKEN,
                "status": "all_not_deleted",
                "start": start, "limit": _PAGE,
                "sort": "id ASC",
            })
            r.raise_for_status()
            body = r.json()
            out.extend(body.get("data") or [])
            pg = (((body.get("additional_data") or {}).get("pagination")) or {})
            if not pg.get("more_items_in_collection"):
                break
            start = pg.get("next_start") or (start + _PAGE)
            time.sleep(0.15)  # be gentle with the API
    logger.info(f"pipedrive: fetched {len(out)} deals")
    return out


def _deal_row(d: dict) -> dict:
    org = d.get("org_id")
    org_name = org.get("name") if isinstance(org, dict) else None
    org_id = org.get("value") if isinstance(org, dict) else org
    won = d.get("won_time") or None
    return {
        "deal_id": d.get("id"),
        "domain": normalize_domain(d.get(F_DOMEN) or org_name),
        "domen_raw": d.get(F_DOMEN) or "",
        "title": d.get("title") or "",
        "status": d.get("status") or "",
        "value": float(d.get("value") or 0),
        "currency": d.get("currency") or "",
        "won_time": won[:10] if won else None,
        "add_time": (d.get("add_time") or "")[:10] or None,
        "tariff": str(d.get(F_TARIFF) or ""),
        "org_id": org_id,
        "org_name": org_name or "",
    }


# ── Status computation ──────────────────────────────────────────────────────

def _month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _prev_months(today: date, n: int) -> list[str]:
    """The n previous calendar months (most recent first), excluding the current."""
    out, y, m = [], today.year, today.month
    for _ in range(n):
        m -= 1
        if m == 0:
            m = 12
            y -= 1
        out.append(f"{y:04d}-{m:02d}")
    return out


def _compute_status(rows: list[dict], today: date | None = None) -> list[dict]:
    today = today or date.today()
    # Window of 3 calendar months INCLUDING the current one:
    #   m1 = current month, m2 = previous, m3 = two months ago.
    # A payment in the current month must count as active (Won) — excluding it
    # would mislabel the most recent payers as Lost.
    m1 = _month_key(today)
    m2, m3 = _prev_months(today, 2)
    last12 = set(_prev_months(today, 12)) | {m1}

    by_domain: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if r["domain"]:
            by_domain[r["domain"]].append(r)

    result = []
    for domain, deals in by_domain.items():
        won = [d for d in deals if d["status"] == "won" and d["won_time"]]
        paid_months = {d["won_time"][:7] for d in won}
        p1, p2, p3 = m1 in paid_months, m2 in paid_months, m3 in paid_months
        paid_3mo = p1 or p2 or p3

        # current Pipedrive status = status of the most recent deal (by add/won time)
        latest = max(deals, key=lambda d: (d["won_time"] or d["add_time"] or ""))
        pd_status = latest["status"] or "open"

        if paid_3mo:
            fact = "Won"
        elif pd_status == "open":
            fact = "Open"
        else:  # won (lapsed) or lost
            fact = "Lost"

        risk = ""
        if (not p1) and (not p2) and p3:
            risk = "Alarm"
        elif (not paid_3mo) and (paid_months & last12):
            risk = "Churn"

        last_paid = max((d["won_time"] for d in won), default=None)
        total_paid = sum(d["value"] for d in won)
        result.append({
            "domain": domain,
            "status_pipedrive": pd_status,
            "paid_m1": p1, "paid_m2": p2, "paid_m3": p3,
            "status_fact": fact,
            "risk": risk,
            "last_paid_at": last_paid,
            "won_deals": len(won),
            "total_deals": len(deals),
            "total_paid_value": round(total_paid, 2),
            "currency": latest["currency"] or "",
            "org_name": latest["org_name"] or "",
            "computed_at": today.isoformat(),
        })
    return result


# ── Load into BQ ────────────────────────────────────────────────────────────

_DEALS_SCHEMA = [
    bigquery.SchemaField("deal_id", "INTEGER"),
    bigquery.SchemaField("domain", "STRING"),
    bigquery.SchemaField("domen_raw", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("value", "FLOAT"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("won_time", "DATE"),
    bigquery.SchemaField("add_time", "DATE"),
    bigquery.SchemaField("tariff", "STRING"),
    bigquery.SchemaField("org_id", "INTEGER"),
    bigquery.SchemaField("org_name", "STRING"),
]

_STATUS_SCHEMA = [
    bigquery.SchemaField("domain", "STRING"),
    bigquery.SchemaField("status_pipedrive", "STRING"),
    bigquery.SchemaField("paid_m1", "BOOLEAN"),
    bigquery.SchemaField("paid_m2", "BOOLEAN"),
    bigquery.SchemaField("paid_m3", "BOOLEAN"),
    bigquery.SchemaField("status_fact", "STRING"),
    bigquery.SchemaField("risk", "STRING"),
    bigquery.SchemaField("last_paid_at", "DATE"),
    bigquery.SchemaField("won_deals", "INTEGER"),
    bigquery.SchemaField("total_deals", "INTEGER"),
    bigquery.SchemaField("total_paid_value", "FLOAT"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("org_name", "STRING"),
    bigquery.SchemaField("computed_at", "DATE"),
]


def _load(table: str, schema: list, rows: list[dict]) -> None:
    bq = client()
    cfg = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    data = "\n".join(json.dumps(r) for r in rows).encode() or b"{}"
    bq.load_table_from_file(io.BytesIO(data), table_ref(table), job_config=cfg).result()


def sync_pipedrive() -> dict:
    """Full sync: fetch all deals → pipedrive_deals_raw, compute → pipedrive_status."""
    t0 = time.time()
    deals = _fetch_all_deals()
    rows = [_deal_row(d) for d in deals]
    status = _compute_status(rows)
    _load(DEALS_RAW_TABLE, _DEALS_SCHEMA, rows)
    _load(STATUS_TABLE, _STATUS_SCHEMA, status)
    elapsed = round(time.time() - t0, 1)
    no_domain = sum(1 for r in rows if not r["domain"])
    logger.info(f"sync_pipedrive: {len(rows)} deals, {len(status)} domains, "
                f"{no_domain} deals w/o domain, {elapsed}s")
    return {"status": "ok", "deals": len(rows), "domains": len(status),
            "deals_without_domain": no_domain, "elapsed": elapsed}


def get_status_rows() -> list[dict]:
    """Read pipedrive_status for the dashboard. Empty list if not synced yet."""
    bq = client()
    try:
        rows = bq.query(
            f"SELECT * FROM `{table_ref(STATUS_TABLE)}` ORDER BY status_fact, domain"
        ).result()
    except Exception:
        return []
    out = []
    for r in rows:
        d = dict(r)
        for k, v in d.items():
            if isinstance(v, (date, datetime)):
                d[k] = v.isoformat()
        out.append(d)
    return out


def get_status_for_domains(domains: list[str]) -> dict[str, dict]:
    """domain -> status row, for joining onto Explorer profiles. Cheap clustered-ish read."""
    domains = sorted({normalize_domain(d) for d in (domains or []) if d})
    if not domains:
        return {}
    bq = client()
    try:
        rows = bq.query(
            f"SELECT * FROM `{table_ref(STATUS_TABLE)}` WHERE domain IN UNNEST(@d)",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("d", "STRING", domains)]),
        ).result()
    except Exception:
        return {}
    out = {}
    for r in rows:
        d = dict(r)
        for k, v in d.items():
            if isinstance(v, (date, datetime)):
                d[k] = v.isoformat()
        out[d["domain"]] = d
    return out
