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
    lost = d.get("lost_time") or None
    return {
        "deal_id": d.get("id"),
        "domain": normalize_domain(d.get(F_DOMEN) or org_name),
        "domen_raw": d.get(F_DOMEN) or "",
        "title": d.get("title") or "",
        "status": d.get("status") or "",
        "value": float(d.get("value") or 0),
        "currency": d.get("currency") or "",
        "won_time": won[:10] if won else None,
        "lost_time": lost[:10] if lost else None,
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

    as_of_iso = today.isoformat()
    result = []
    for domain, deals in by_domain.items():
        # Only consider deals that exist as-of the evaluation date (won/lost up to today).
        won = [d for d in deals if d["status"] == "won" and d["won_time"] and d["won_time"] <= as_of_iso]
        open_deals = [d for d in deals if d["status"] == "open"]
        lost_deals = [d for d in deals if d["status"] == "lost"]

        paid_months = {d["won_time"][:7] for d in won}
        p1, p2, p3 = m1 in paid_months, m2 in paid_months, m3 in paid_months
        paid_3mo = p1 or p2 or p3

        # A domain can hold several deals at once (main service won, add-on open,
        # add-on lost). Status FACT is payment-driven: any recent payment → the
        # relationship is alive (Won), regardless of how many side deals are
        # open/lost. When there is no recent payment we fall back to the deal mix:
        # an open deal means an active opportunity (Open), otherwise Lost.
        if paid_3mo:
            fact = "Won"
        elif open_deals:
            fact = "Open"
        else:
            fact = "Lost"

        # Aggregate Pipedrive-side status as a count mix, e.g. "won:1 open:1 lost:1".
        mix = []
        if won: mix.append(f"won:{len(won)}")
        if open_deals: mix.append(f"open:{len(open_deals)}")
        if lost_deals: mix.append(f"lost:{len(lost_deals)}")
        pd_status = " ".join(mix) or "—"

        # Combined deal status: any won → won, else any open → open, else lost.
        # (A single won deal makes the combined verdict "won" regardless of how
        # many lost/open deals sit alongside it.)
        if won:
            deals_status = "won"
        elif open_deals:
            deals_status = "open"
        else:
            deals_status = "lost"

        # The "main" deal whose number we surface: highest priority (won>open>lost),
        # then most recent. Used for the DEALS № column / Pipedrive deep-link.
        _prio = {"won": 3, "open": 2, "lost": 1}
        main_deal = max(deals, key=lambda d: (
            _prio.get(d["status"], 0),
            d["won_time"] or d["lost_time"] or d["add_time"] or ""))
        main_deal_id = main_deal["deal_id"]

        risk = ""
        if (not p1) and (not p2) and p3:
            risk = "Alarm"
        elif (not paid_3mo) and (paid_months & last12):
            risk = "Churn"

        last_paid = max((d["won_time"] for d in won), default=None)
        total_paid = sum(d["value"] for d in won)
        currency = (won[0]["currency"] if won else (deals[0]["currency"] if deals else "")) or ""
        org_name = next((d["org_name"] for d in deals if d["org_name"]), "")

        # Per-deal breakdown so the dashboard can expand a domain with several deals.
        deals_detail = sorted(
            ({"deal_id": d["deal_id"], "title": d["title"], "status": d["status"],
              "value": d["value"], "currency": d["currency"], "won_time": d["won_time"],
              "lost_time": d["lost_time"], "tariff": d["tariff"]} for d in deals),
            key=lambda x: (x["won_time"] or x["lost_time"] or "", x["title"]), reverse=True)

        result.append({
            "domain": domain,
            "status_pipedrive": pd_status,
            "deals_status": deals_status,
            "main_deal_id": main_deal_id,
            "paid_m1": p1, "paid_m2": p2, "paid_m3": p3,
            "status_fact": fact,
            "risk": risk,
            "last_paid_at": last_paid,
            "won_deals": len(won),
            "open_deals": len(open_deals),
            "lost_deals": len(lost_deals),
            "total_deals": len(deals),
            "total_paid_value": round(total_paid, 2),
            "currency": currency,
            "org_name": org_name,
            "deals_json": json.dumps(deals_detail, ensure_ascii=False),
            "computed_at": as_of_iso,
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
    bigquery.SchemaField("lost_time", "DATE"),
    bigquery.SchemaField("add_time", "DATE"),
    bigquery.SchemaField("tariff", "STRING"),
    bigquery.SchemaField("org_id", "INTEGER"),
    bigquery.SchemaField("org_name", "STRING"),
]

_STATUS_SCHEMA = [
    bigquery.SchemaField("domain", "STRING"),
    bigquery.SchemaField("status_pipedrive", "STRING"),
    bigquery.SchemaField("deals_status", "STRING"),
    bigquery.SchemaField("main_deal_id", "INTEGER"),
    bigquery.SchemaField("paid_m1", "BOOLEAN"),
    bigquery.SchemaField("paid_m2", "BOOLEAN"),
    bigquery.SchemaField("paid_m3", "BOOLEAN"),
    bigquery.SchemaField("status_fact", "STRING"),
    bigquery.SchemaField("risk", "STRING"),
    bigquery.SchemaField("last_paid_at", "DATE"),
    bigquery.SchemaField("won_deals", "INTEGER"),
    bigquery.SchemaField("open_deals", "INTEGER"),
    bigquery.SchemaField("lost_deals", "INTEGER"),
    bigquery.SchemaField("total_deals", "INTEGER"),
    bigquery.SchemaField("total_paid_value", "FLOAT"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("org_name", "STRING"),
    bigquery.SchemaField("deals_json", "STRING"),
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


def _read_raw_deals() -> list[dict]:
    """Read pipedrive_deals_raw back into the dict shape _compute_status expects."""
    bq = client()
    rows = bq.query(
        f"""SELECT deal_id, domain, status, value, currency, title, tariff, org_name,
                   CAST(won_time AS STRING)  AS won_time,
                   CAST(lost_time AS STRING) AS lost_time,
                   CAST(add_time AS STRING)  AS add_time
            FROM `{table_ref(DEALS_RAW_TABLE)}`"""
    ).result()
    return [dict(r) for r in rows]


def get_status_rows(as_of: str | None = None) -> list[dict]:
    """Relationship-status rows for the dashboard.

    as_of=None → the precomputed pipedrive_status table (current day).
    as_of=YYYY-MM-DD → recompute on the fly from raw deals as of that date, so
    the user can see the relationship status at the end of any chosen period.
    Empty list if nothing is synced yet.
    """
    bq = client()
    if as_of:
        try:
            ad = date.fromisoformat(as_of)
            return _compute_status(_read_raw_deals(), ad)
        except Exception:
            return []
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


def get_timeseries(date_from: str, date_to: str) -> list[dict]:
    """Monthly deal-event counts over [date_from, date_to] for the trend chart:
      won  = distinct domains with a won payment that month (won_time),
      lost = distinct domains with a deal lost that month (lost_time),
      open = distinct domains with a deal created that month (add_time).
    Cheap GROUP BY over the raw table. Empty list if not synced yet."""
    bq = client()
    q = f"""
        WITH ev AS (
          SELECT domain, 'won'  AS kind, won_time  AS d FROM `{table_ref(DEALS_RAW_TABLE)}` WHERE won_time  IS NOT NULL
          UNION ALL
          SELECT domain, 'lost' AS kind, lost_time AS d FROM `{table_ref(DEALS_RAW_TABLE)}` WHERE lost_time IS NOT NULL
          UNION ALL
          SELECT domain, 'open' AS kind, add_time  AS d FROM `{table_ref(DEALS_RAW_TABLE)}` WHERE status = 'open' AND add_time IS NOT NULL
        )
        SELECT FORMAT_DATE('%Y-%m', d) AS month,
               COUNT(DISTINCT IF(kind='won',  domain, NULL)) AS won,
               COUNT(DISTINCT IF(kind='open', domain, NULL)) AS open,
               COUNT(DISTINCT IF(kind='lost', domain, NULL)) AS lost
        FROM ev
        WHERE domain != '' AND d BETWEEN @df AND @dt
        GROUP BY month ORDER BY month
    """
    try:
        rows = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("df", "DATE", date_from),
            bigquery.ScalarQueryParameter("dt", "DATE", date_to),
        ])).result()
    except Exception:
        return []
    return [{"month": r["month"], "won": r["won"], "open": r["open"], "lost": r["lost"]} for r in rows]


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
