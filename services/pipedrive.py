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

from config.settings import PIPEDRIVE_API_TOKEN, PIPEDRIVE_COMPANY_DOMAIN, REQUEST_TIMEOUT, PIPEDRIVE_TIMEOUT
from core.bigquery import client, table_ref

logger = logging.getLogger(__name__)

DEALS_RAW_TABLE = "pipedrive_deals_raw"
STATUS_TABLE = "pipedrive_status"
MRR_TABLE = "pipedrive_mrr"  # domain → latest MRR (derived from monthly; kept for sort/compat)
MRR_MONTHLY_TABLE = "pipedrive_mrr_monthly"  # domain → month → MRR, FULL history from corpBQ

# Custom deal field keys (discovered from /dealFields).
F_DOMEN = "008b81ed34c02301397301892241ef26029fbd62"
F_TARIFF = "73cec5f72f2013cfc8479d276920416ba66561da"

_PAGE = 500  # Pipedrive max page size for /deals

# code(str) → human label for the Tariff plan (set) field, loaded once per sync
# from /dealFields (free, Pipedrive API — no BigQuery cost).
_TARIFF_LABELS: dict[str, str] = {}


def _load_tariff_labels() -> dict[str, str]:
    """Fetch the Tariff-plan field options from Pipedrive once; cache in-process."""
    global _TARIFF_LABELS
    try:
        with httpx.Client(timeout=PIPEDRIVE_TIMEOUT) as cli:
            r = cli.get(f"{_base()}/dealFields", params={"api_token": PIPEDRIVE_API_TOKEN, "limit": 500})
        for f in (r.json().get("data") or []):
            if f.get("key") == F_TARIFF:
                _TARIFF_LABELS = {str(o["id"]): o["label"] for o in (f.get("options") or [])}
                break
    except Exception as e:
        logger.warning(f"tariff labels load failed: {e}")
    return _TARIFF_LABELS


def _tariff_label(codes: str) -> str:
    """Map a comma-separated option-id string ("198,204") → "Name1, Name2"."""
    if not codes:
        return ""
    if not _TARIFF_LABELS:
        _load_tariff_labels()
    return ", ".join(_TARIFF_LABELS.get(c.strip(), c.strip()) for c in str(codes).split(",") if c.strip())


def _base() -> str:
    c = (PIPEDRIVE_COMPANY_DOMAIN or "").strip()
    return f"https://{c}.pipedrive.com/api/v1" if c else "https://api.pipedrive.com/v1"


# Free-mail / public domains that leak in via the org_name fallback — not real clients.
_PUBLIC_DOMAINS = {
    "gmail.com", "mail.ru", "yandex.ru", "ukr.net", "bk.ru", "i.ua", "list.ru", "ya.ru",
    "yahoo.com", "inbox.ru", "icloud.com", "rambler.ru", "hotmail.com", "outlook.com",
    "yandex.ua", "meta.ua", "protonmail.com", "mail.ua", "bigmir.net", "ua.fm",
    "yandex.com", "inbox.lv", "tut.by", "yandex.by", "mail.com", "me.com", "aol.com",
    "gmail.ru", "email.ua", "proton.me", "googlemail.com",
}
_DOMAIN_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$")


def normalize_domain(raw: str | None) -> str:
    """Lower-case bare domain: strip scheme, path, query, leading www, trailing dot.
    Returns "" for junk (free-mail, non-domain text like "немає"/"-", malformed)."""
    if not raw:
        return ""
    s = str(raw).strip().lower()
    s = re.sub(r"^[a-z]+://", "", s)        # scheme
    s = s.split("/")[0].split("?")[0]       # path / query
    s = s.split("@")[-1]                     # stray email-ish prefix
    if s.startswith("www."):
        s = s[4:]
    s = s.strip().strip(".")
    # Drop noise: free-mail providers and anything that isn't a valid hostname.
    if s in _PUBLIC_DOMAINS or not _DOMAIN_RE.match(s):
        return ""
    return s


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
    owner = d.get("user_id")  # deal owner = the manager responsible for the client
    manager = owner.get("name") if isinstance(owner, dict) else None
    won = d.get("won_time") or None
    lost = d.get("lost_time") or None
    # Last contact (excluding payment): newest of last activity, in/out emails,
    # stage change — i.e. last touch via call/meeting/email/contract step.
    contact = max((str(d.get(k))[:10] for k in (
        "last_activity_date", "last_incoming_mail_time", "last_outgoing_mail_time",
        "stage_change_time") if d.get(k)), default=None)
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
        "last_contact": contact,
        "tariff": str(d.get(F_TARIFF) or ""),
        "tariff_label": _tariff_label(d.get(F_TARIFF) or ""),
        "org_id": org_id,
        "org_name": org_name or "",
        "manager": manager or "",
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


def _compute_status(rows: list[dict], today: date | None = None,
                    mrr_monthly: dict | None = None) -> list[dict]:
    """Relationship status per domain. FACT and RISK are MRR-driven (real monthly
    MRR from corpBQ in `mrr_monthly` = {domain: {"YYYY-MM": mrr}}). Rows cover the
    UNION of Pipedrive-deal domains and MRR domains — a paying customer with no
    Pipedrive deal still appears (deal columns empty). Pipedrive data (manager,
    tariff, deal mix, last contact) is reference only and never drives FACT."""
    today = today or date.today()
    # 3-month window INCLUDING the current month (m1=current, m2=prev, m3=two ago).
    m1 = _month_key(today)
    m2, m3 = _prev_months(today, 2)
    last12 = set(_prev_months(today, 12)) | {m1}
    recent12 = [m1] + _prev_months(today, 11)   # current + 11 previous, recent-first
    mrr_monthly = mrr_monthly or {}

    by_domain: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if r["domain"]:
            by_domain[r["domain"]].append(r)

    as_of_iso = today.isoformat()
    _prio = {"won": 3, "open": 2, "lost": 1}
    result = []
    # Union: every domain that has a Pipedrive deal OR real MRR history.
    for domain in set(by_domain) | set(mrr_monthly):
        deals = by_domain.get(domain, [])

        # ── MRR side (drives FACT / RISK) — months up to the eval month only ──
        # ch_by_month = {"YYYY-MM": {channel: mrr}}; the total per month (channels
        # summed) drives FACT/RISK exactly as before — channel split is additive.
        ch_by_month = mrr_monthly.get(domain) or {}
        series: dict[str, float] = {}
        for mo, chd in ch_by_month.items():
            if mo <= m1:
                tot = sum(v for v in chd.values() if v and v > 0)
                if tot > 0:
                    series[mo] = tot
        if not deals and not series:
            continue   # e.g. only future-dated MRR and no deal → nothing to show
        p1, p2, p3 = (m1 in series), (m2 in series), (m3 in series)
        paid_3mo = p1 or p2 or p3
        in_12 = bool(series.keys() & last12)
        has_any_mrr = bool(series)

        if paid_3mo:
            fact = "Won"          # MRR in any of the last 3 months → alive
        elif has_any_mrr:
            fact = "Lost"         # had MRR before, none in the last 3 months
        else:
            fact = "—"            # no (past/current) MRR for this domain in corpBQ

        risk = ""
        if (not p1) and (not p2) and p3:
            risk = "Alarm"        # paid 3 months ago, missed the last two
        elif (not paid_3mo) and in_12:
            risk = "Churn"        # paid within the last year, nothing in 3 months

        latest_mrr_month = max(series) if series else None
        last_paid = (latest_mrr_month + "-01") if latest_mrr_month else None
        mrr_latest = round(series[latest_mrr_month], 2) if latest_mrr_month else None
        # recent-12 months with the per-channel split (`ch`) for the money chart's
        # channel toggles; `mrr` stays the monthly total for the Σ column + mrr3.
        def _ch(mo: str) -> dict:
            return {c: round(v, 2) for c, v in (ch_by_month.get(mo) or {}).items() if v and v > 0}
        mrr_recent = [{"month": mo, "mrr": round(series.get(mo, 0.0), 2), "ch": _ch(mo)} for mo in recent12]
        # Per-channel breakdown of the latest month → the table's channel columns
        # (sums back to mrr_latest / the Σ column).
        mrr_channels = _ch(latest_mrr_month) if latest_mrr_month else {}

        # ── Pipedrive side (reference only; empty when the domain has no deals) ──
        won = [d for d in deals if d["status"] == "won" and d["won_time"] and d["won_time"] <= as_of_iso]
        open_deals = [d for d in deals if d["status"] == "open"]
        lost_deals = [d for d in deals if d["status"] == "lost"]

        mix = []
        if won: mix.append(f"won:{len(won)}")
        if open_deals: mix.append(f"open:{len(open_deals)}")
        if lost_deals: mix.append(f"lost:{len(lost_deals)}")
        pd_status = " ".join(mix) or "—"
        deals_status = ("won" if won else "open" if open_deals
                        else "lost" if lost_deals else "")

        if deals:
            # "Main" deal: highest priority (won>open>lost), then most recent.
            main_deal = max(deals, key=lambda d: (
                _prio.get(d["status"], 0),
                d["won_time"] or d["lost_time"] or d["add_time"] or ""))
            main_deal_id = main_deal["deal_id"]
            manager = main_deal.get("manager") or next(
                (d["manager"] for d in deals if d.get("manager")), "")
            tariff = main_deal.get("tariff_label") or next(
                (d.get("tariff_label") for d in deals if d.get("tariff_label")), "")
        else:
            main_deal_id, manager, tariff = None, "", ""

        last_contact = max((d["last_contact"] for d in deals
                            if d.get("last_contact") and d["last_contact"] <= as_of_iso), default=None)
        # Lifetime paid from won deals, by currency (supplementary, not the MRR metric).
        paid_by_cur: dict[str, float] = defaultdict(float)
        for d in won:
            paid_by_cur[(d["currency"] or "?")] += d["value"]
        if paid_by_cur:
            currency = max(paid_by_cur, key=paid_by_cur.get)
            total_paid = paid_by_cur[currency]
        else:
            currency = (deals[0]["currency"] if deals else "") or ""
            total_paid = 0.0
        paid_breakdown = ", ".join(f"{round(v):,} {c}"
                                   for c, v in sorted(paid_by_cur.items(), key=lambda x: -x[1]))
        org_name = next((d["org_name"] for d in deals if d["org_name"]), "")
        deals_detail = sorted(
            ({"deal_id": d["deal_id"], "title": d["title"], "status": d["status"],
              "value": d["value"], "currency": d["currency"], "won_time": d["won_time"],
              "lost_time": d["lost_time"], "add_time": d["add_time"], "tariff": d["tariff"]} for d in deals),
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
            "last_contact_at": last_contact,
            "won_deals": len(won),
            "open_deals": len(open_deals),
            "lost_deals": len(lost_deals),
            "total_deals": len(deals),
            "total_paid_value": round(total_paid, 2),
            "currency": currency,
            "paid_breakdown": paid_breakdown,
            "org_name": org_name,
            "manager": manager,
            "tariff": tariff,
            "mrr": mrr_latest,
            "mrr_monthly": json.dumps(mrr_recent, ensure_ascii=False),
            "mrr_channels": json.dumps(mrr_channels, ensure_ascii=False),
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
    bigquery.SchemaField("last_contact", "DATE"),
    bigquery.SchemaField("tariff", "STRING"),
    bigquery.SchemaField("tariff_label", "STRING"),
    bigquery.SchemaField("org_id", "INTEGER"),
    bigquery.SchemaField("org_name", "STRING"),
    bigquery.SchemaField("manager", "STRING"),
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
    bigquery.SchemaField("last_contact_at", "DATE"),
    bigquery.SchemaField("won_deals", "INTEGER"),
    bigquery.SchemaField("open_deals", "INTEGER"),
    bigquery.SchemaField("lost_deals", "INTEGER"),
    bigquery.SchemaField("total_deals", "INTEGER"),
    bigquery.SchemaField("total_paid_value", "FLOAT"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("paid_breakdown", "STRING"),
    bigquery.SchemaField("org_name", "STRING"),
    bigquery.SchemaField("manager", "STRING"),
    bigquery.SchemaField("tariff", "STRING"),
    bigquery.SchemaField("mrr", "FLOAT"),
    bigquery.SchemaField("mrr_monthly", "STRING"),   # JSON: recent months [{month, mrr, ch:{chan:val}}]
    bigquery.SchemaField("mrr_channels", "STRING"),  # JSON: latest month per-channel {chan: val}
    bigquery.SchemaField("deals_json", "STRING"),
    bigquery.SchemaField("computed_at", "DATE"),
]

_MRR_MONTHLY_SCHEMA = [
    bigquery.SchemaField("domain", "STRING"),
    bigquery.SchemaField("month", "STRING"),    # "YYYY-MM"
    bigquery.SchemaField("channel", "STRING"),  # corp Media: Email / Viber / ExtraFeatures / …
    bigquery.SchemaField("mrr", "FLOAT"),       # SUM(MRRUSD) for that domain·month·channel
]

# corp source that still carries the per-channel `Media` dimension (the
# mrr_by_month_from_mv VIEW already SUM()s it away). Summing all channels for a
# domain·month reproduces that view's figure exactly (verified on apteka.net.ua).
MRR_SOURCE_TABLE = "es_live.mv_Income_Report_MRR"


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
    _load_tariff_labels()  # resolve tariff codes → names once (free, Pipedrive API)
    deals = _fetch_all_deals()
    rows = [_deal_row(d) for d in deals]
    status = _compute_status(rows, mrr_monthly=_get_mrr_monthly_map())
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
        f"""SELECT deal_id, domain, status, value, currency, title, tariff, tariff_label,
                   org_name, manager,
                   CAST(won_time AS STRING)  AS won_time,
                   CAST(lost_time AS STRING) AS lost_time,
                   CAST(add_time AS STRING)  AS add_time,
                   CAST(last_contact AS STRING) AS last_contact
            FROM `{table_ref(DEALS_RAW_TABLE)}`"""
    ).result()
    return [dict(r) for r in rows]


def get_status_rows(as_of: str | None = None, date_from: str | None = None) -> list[dict]:
    """Relationship-status rows for the dashboard.

    as_of=None and date_from=None → the precomputed pipedrive_status table (today).
    as_of=YYYY-MM-DD → recompute status from raw as of that date (period end).
    date_from=YYYY-MM-DD → keep only domains with a deal event (won/lost/created)
    inside [date_from, as_of], so the table truly rebuilds for the chosen period.
    Empty list if nothing is synced yet.
    """
    bq = client()
    if as_of or date_from:
        try:
            ad = date.fromisoformat(as_of) if as_of else date.today()
            raw = _read_raw_deals()
            rows = _compute_status(raw, ad, mrr_monthly=_get_mrr_monthly_map())
            if date_from:
                end = ad.isoformat()
                # A domain is "active in the period" if any of its deals has an
                # event date (won/lost/created) within [date_from, end].
                active = {r["domain"] for r in raw if r["domain"] and any(
                    d and date_from <= d <= end
                    for d in (r["won_time"], r["lost_time"], r["add_time"]))}
                rows = [r for r in rows if r["domain"] in active]
            return rows
        except Exception:
            logger.exception("get_status_rows recompute failed")
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


def get_timeseries(date_from: str, date_to: str, manager: str | None = None) -> list[dict]:
    """Monthly deal-event counts over [date_from, date_to] for the trend chart:
      won  = distinct domains with a won payment that month (won_time),
      lost = distinct domains with a deal lost that month (lost_time),
      open = distinct domains with a deal created that month (add_time).
    Optional manager filter restricts to that owner's deals.
    Cheap GROUP BY over the raw table. Empty list if not synced yet."""
    bq = client()
    mclause = "AND manager = @mgr" if manager else ""
    q = f"""
        WITH ev AS (
          SELECT domain, 'won'  AS kind, won_time  AS d FROM `{table_ref(DEALS_RAW_TABLE)}` WHERE won_time  IS NOT NULL {mclause}
          UNION ALL
          SELECT domain, 'lost' AS kind, lost_time AS d FROM `{table_ref(DEALS_RAW_TABLE)}` WHERE lost_time IS NOT NULL {mclause}
          UNION ALL
          SELECT domain, 'open' AS kind, add_time  AS d FROM `{table_ref(DEALS_RAW_TABLE)}` WHERE status = 'open' AND add_time IS NOT NULL {mclause}
        )
        SELECT FORMAT_DATE('%Y-%m', d) AS month,
               COUNT(DISTINCT IF(kind='won',  domain, NULL)) AS won,
               COUNT(DISTINCT IF(kind='open', domain, NULL)) AS open,
               COUNT(DISTINCT IF(kind='lost', domain, NULL)) AS lost
        FROM ev
        WHERE domain != '' AND d BETWEEN @df AND @dt
        GROUP BY month ORDER BY month
    """
    params = [bigquery.ScalarQueryParameter("df", "DATE", date_from),
              bigquery.ScalarQueryParameter("dt", "DATE", date_to)]
    if manager:
        params.append(bigquery.ScalarQueryParameter("mgr", "STRING", manager))
    try:
        rows = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    except Exception:
        return []
    return [{"month": r["month"], "won": r["won"], "open": r["open"], "lost": r["lost"]} for r in rows]


def get_mrr_timeseries(date_from: str, date_to: str,
                       domains: list[str] | None = None) -> list[dict]:
    """Monthly SUM of real MRR over [date_from, date_to] (YYYY-MM), optionally limited
    to `domains` (the dashboard's currently-filtered set) — the money chart. Reads the
    private pipedrive_mrr_monthly table (cheap)."""
    bq = client()
    params = [bigquery.ScalarQueryParameter("df", "STRING", date_from[:7]),
              bigquery.ScalarQueryParameter("dt", "STRING", date_to[:7])]
    where = "WHERE month BETWEEN @df AND @dt"
    if domains:
        doms = sorted({normalize_domain(d) for d in domains if d})
        if doms:
            where += " AND domain IN UNNEST(@doms)"
            params.append(bigquery.ArrayQueryParameter("doms", "STRING", doms))
    q = f"""SELECT month, ROUND(SUM(mrr), 2) AS mrr, COUNT(DISTINCT domain) AS domains
            FROM `{table_ref(MRR_MONTHLY_TABLE)}` {where}
            GROUP BY month ORDER BY month"""
    try:
        rows = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    except Exception:
        logger.exception("get_mrr_timeseries failed")
        return []
    return [{"month": r["month"], "mrr": r["mrr"], "domains": r["domains"]} for r in rows]


# ── MRR from corpBQ (separate source + schedule) ────────────────────────────
# MRR is NOT in Pipedrive (the deal.mrr field is empty); the accurate figures live
# in corpBQ, tied to another CRM. We pull domain→MRR there ONCE per scheduled MRR
# sync (~700 MB corp scan) into a tiny private table; the dashboard only ever reads
# the private table, so day-to-day BQ traffic does not grow.

MRR_FREQ_KEY = "pipedrive_mrr_frequency"


def get_mrr_frequency() -> str:
    from core.bigquery import get_setting
    try:
        f = get_setting(MRR_FREQ_KEY, "off")
        return f if f in {"off", "monthly", "weekly", "daily"} else "off"
    except Exception:
        return "off"


def set_mrr_frequency(freq: str) -> dict:
    from core.bigquery import set_setting
    if freq not in {"off", "monthly", "weekly", "daily"}:
        raise ValueError(f"bad mrr frequency: {freq}")
    set_setting(MRR_FREQ_KEY, freq)
    return {"status": "ok", "frequency": freq}


def sync_mrr_from_corp() -> dict:
    """Pull the FULL monthly MRR history (domain × month × MRR) from corpBQ into the
    private `pipedrive_mrr_monthly` table, derive the latest MRR per domain into
    `pipedrive_mrr` (sort/compat), then fold MRR into pipedrive_status.
    One corp scan (~610 MB) per call — schedule sparsely."""
    import time as _t
    from core.bigquery import corp_client, CORP_PROJECT_ID, CORP_DATASET
    t0 = _t.time()
    cds = f"{CORP_PROJECT_ID}.{CORP_DATASET}"
    # Per-channel (Media) monthly MRR. We read the live income source (which still
    # has Media) instead of the mrr_by_month_from_mv view (which SUM()s it away).
    # SUM(MRRUSD) over all channels of a domain·month == the view's figure.
    sql = f"""
        SELECT LOWER(p.lookup_domain_actual) AS domain,
               FORMAT_DATE('%Y-%m', DATE(m.DateDistributed)) AS month,
               m.Media AS channel,
               SUM(m.MRRUSD) AS mrr
        FROM `{cds}.primary_domains_orgs_mrr` p
        JOIN `{CORP_PROJECT_ID}.{MRR_SOURCE_TABLE}` m
          ON m.OrganisationID = p.best_org_id
        WHERE p.lookup_domain_actual IS NOT NULL AND m.MRRUSD > 0 AND m.Media IS NOT NULL
        GROUP BY domain, month, channel
    """
    job = corp_client().query(sql)
    monthly = [{"domain": normalize_domain(r["domain"]), "month": r["month"],
                "channel": r["channel"], "mrr": float(r["mrr"])}
               for r in job.result() if normalize_domain(r["domain"])]
    _load(MRR_MONTHLY_TABLE, _MRR_MONTHLY_SCHEMA, monthly)
    # Derive latest total MRR per domain (channels summed, most recent month) for sort/compat.
    month_total: dict[tuple[str, str], float] = defaultdict(float)
    for r in monthly:
        month_total[(r["domain"], r["month"])] += r["mrr"]
    latest: dict[str, tuple[str, float]] = {}
    for (d, mo), v in month_total.items():
        if d not in latest or mo > latest[d][0]:
            latest[d] = (mo, v)
    flat = [{"domain": d, "mrr": round(v, 2)} for d, (mo, v) in latest.items()]
    _load(MRR_TABLE, [bigquery.SchemaField("domain", "STRING"), bigquery.SchemaField("mrr", "FLOAT")], flat)
    # Fold the fresh MRR into the precomputed status table (cheap: reads private raw).
    try:
        mm = _get_mrr_monthly_map(fresh=monthly)
        _load(STATUS_TABLE, _STATUS_SCHEMA, _compute_status(_read_raw_deals(), mrr_monthly=mm))
    except Exception:
        logger.exception("MRR fold into status failed")
    mb = round((job.total_bytes_billed or 0) / 1e6)
    logger.info(f"sync_mrr_from_corp: {len(monthly)} domain-months, {len(flat)} domains, "
                f"{mb} MB corp, {round(_t.time()-t0,1)}s")
    return {"status": "ok", "domain_months": len(monthly), "domains": len(flat),
            "mb_billed_corp": mb, "elapsed": round(_t.time() - t0, 1)}


def _get_mrr_map(fresh: list[dict] | None = None) -> dict:
    """domain → MRR from the private pipedrive_mrr table (or a freshly-built list)."""
    if fresh is not None:
        return {r["domain"]: r["mrr"] for r in fresh if r.get("domain")}
    bq = client()
    try:
        rows = bq.query(f"SELECT domain, mrr FROM `{table_ref(MRR_TABLE)}`").result()
        return {r["domain"]: r["mrr"] for r in rows if r["domain"]}
    except Exception:
        return {}


def _get_mrr_monthly_map(fresh: list[dict] | None = None) -> dict:
    """domain → {"YYYY-MM": {channel: mrr}} from pipedrive_mrr_monthly (or a freshly-
    built list). Per-channel; _compute_status sums channels for the FACT/RISK total."""
    out: dict[str, dict] = defaultdict(lambda: defaultdict(dict))
    rows = fresh
    if rows is None:
        try:
            rows = list(client().query(
                f"SELECT domain, month, channel, mrr FROM `{table_ref(MRR_MONTHLY_TABLE)}`").result())
        except Exception:
            return {}
    for r in rows:   # dict (fresh) or BQ Row — both support r["col"]
        d = r["domain"]
        if not d:
            continue
        out[d][r["month"]][r["channel"] or "?"] = r["mrr"]
    return {d: {mo: dict(chs) for mo, chs in months.items()} for d, months in out.items()}


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


# ── Sync frequency (off / monthly / weekly / daily / online) ────────────────

SYNC_FREQ_KEY = "pipedrive_sync_frequency"
WEBHOOK_ID_KEY = "pipedrive_webhook_id"
WEBHOOK_SECRET_KEY = "pipedrive_webhook_secret"
VALID_FREQ = {"off", "monthly", "weekly", "daily", "online"}


def get_webhook_secret() -> str:
    """Shared secret embedded in the webhook URL (?token=) to authenticate callbacks."""
    from core.bigquery import get_setting, set_setting
    s = get_setting(WEBHOOK_SECRET_KEY, "")
    if not s:
        import secrets as _secrets
        s = _secrets.token_urlsafe(24)
        set_setting(WEBHOOK_SECRET_KEY, s)
    return s


def get_sync_frequency() -> str:
    from core.bigquery import get_setting
    try:
        f = get_setting(SYNC_FREQ_KEY, "off")
        return f if f in VALID_FREQ else "off"
    except Exception:
        return "off"


def set_sync_frequency(freq: str, base_url: str | None = None) -> dict:
    """Persist the frequency. For 'online' register a Pipedrive webhook against
    base_url (the public app origin); leaving 'online' removes it."""
    from core.bigquery import set_setting
    if freq not in VALID_FREQ:
        raise ValueError(f"bad frequency: {freq}")
    prev = get_sync_frequency()
    set_setting(SYNC_FREQ_KEY, freq)
    hook = None
    if freq == "online":
        hook = register_webhook(base_url) if base_url else {"status": "no_base_url"}
    elif prev == "online":
        unregister_webhook()
    return {"status": "ok", "frequency": freq, "webhook": hook}


# ── Online mode: Pipedrive webhooks + incremental update ────────────────────

def register_webhook(base_url: str) -> dict:
    """Create a Pipedrive webhook for all deal events → <base_url>/api/pipedrive/webhook.
    Stores the webhook id so we can delete it later. Removes a stale one first."""
    from core.bigquery import set_setting
    unregister_webhook()  # avoid duplicates
    url = base_url.rstrip("/") + "/api/pipedrive/webhook?token=" + get_webhook_secret()
    with httpx.Client(timeout=PIPEDRIVE_TIMEOUT) as cli:
        r = cli.post(f"{_base()}/webhooks", params={"api_token": PIPEDRIVE_API_TOKEN}, json={
            "subscription_url": url,
            "event_action": "*",
            "event_object": "deal",
        })
    if r.status_code >= 300:
        logger.warning(f"pipedrive webhook register failed: {r.status_code} {r.text[:200]}")
        return {"status": "error", "code": r.status_code, "detail": r.text[:200]}
    wid = (((r.json() or {}).get("data")) or {}).get("id")
    if wid:
        set_setting(WEBHOOK_ID_KEY, str(wid))
    logger.info(f"pipedrive webhook registered id={wid} → {url}")
    return {"status": "ok", "id": wid, "url": url}


def unregister_webhook() -> dict:
    from core.bigquery import get_setting, set_setting
    wid = get_setting(WEBHOOK_ID_KEY, "")
    if not wid:
        return {"status": "none"}
    try:
        with httpx.Client(timeout=PIPEDRIVE_TIMEOUT) as cli:
            cli.delete(f"{_base()}/webhooks/{wid}", params={"api_token": PIPEDRIVE_API_TOKEN})
    except Exception as e:
        logger.debug(f"webhook delete: {e}")
    set_setting(WEBHOOK_ID_KEY, "")
    return {"status": "deleted", "id": wid}


def fetch_deal(deal_id: int) -> dict | None:
    with httpx.Client(timeout=PIPEDRIVE_TIMEOUT) as cli:
        r = cli.get(f"{_base()}/deals/{deal_id}", params={"api_token": PIPEDRIVE_API_TOKEN})
    if r.status_code >= 300:
        return None
    return (r.json() or {}).get("data")


def _recompute_domains(domains: list[str]) -> None:
    """Recompute pipedrive_status for the given domains from raw (delete + insert)."""
    domains = sorted({d for d in domains if d})
    if not domains:
        return
    bq = client()
    raw = [r for r in _read_raw_deals() if r["domain"] in domains]
    new_rows = _compute_status(raw, mrr_monthly=_get_mrr_monthly_map())
    bq.query(f"DELETE FROM `{table_ref(STATUS_TABLE)}` WHERE domain IN UNNEST(@d)",
             job_config=bigquery.QueryJobConfig(query_parameters=[
                 bigquery.ArrayQueryParameter("d", "STRING", domains)])).result()
    if new_rows:
        bq.insert_rows_json(table_ref(STATUS_TABLE), new_rows)


def apply_webhook_event(payload: dict) -> dict:
    """Incrementally apply one Pipedrive deal webhook event to BQ.
    Upserts the changed deal in pipedrive_deals_raw and recomputes its domain(s)."""
    bq = client()
    meta = payload.get("meta") or {}
    current = payload.get("current")
    previous = payload.get("previous")
    deal_id = (current or previous or {}).get("id") or meta.get("id")
    if not deal_id:
        return {"status": "ignored", "reason": "no deal id"}

    affected = set()
    # Remove any existing raw row for this deal.
    bq.query(f"DELETE FROM `{table_ref(DEALS_RAW_TABLE)}` WHERE deal_id = @id",
             job_config=bigquery.QueryJobConfig(query_parameters=[
                 bigquery.ScalarQueryParameter("id", "INT64", int(deal_id))])).result()
    if previous:
        prev_org = previous.get("org_id")
        prev_org_name = prev_org.get("name") if isinstance(prev_org, dict) else None
        affected.add(normalize_domain(previous.get(F_DOMEN) or prev_org_name))

    if current:  # created/updated → insert fresh row
        row = _deal_row(current)
        bq.insert_rows_json(table_ref(DEALS_RAW_TABLE), [row])
        affected.add(row["domain"])

    affected.discard("")
    _recompute_domains(list(affected))
    logger.info(f"pipedrive webhook: deal {deal_id} → domains {affected or '∅'}")
    return {"status": "ok", "deal_id": deal_id, "domains": sorted(affected)}
