"""
Partnership (agency) connector — which clients belong to which agency, and
each client's monthly per-channel MRR.

Membership source (corp, readable):
  es_analysis.t_partnership_commissions — partner ↔ organisation. This is the
  materialised copy of the Drive-backed partnership registry; it covers clients
  that had billable MRR within a commission window. A few non-billing clients
  may be missing because the master Google Sheet is not shared with our service
  account (v_partnership_clients / table_partnership_clients → 403 Drive).

MRR source (corp):
  es_live.mv_Income_Report_MRR — SUM(MRRUSD) per organisation·month·channel,
  each MRR row counted ONCE. We deliberately do NOT take MRR from
  t_partnership_commissions: that table repeats an MRR row per commission `type`
  (Acquisition / Service), which would double-count a client's revenue.

Two private BQ tables (WRITE_TRUNCATE each sync):
  partnership_members      — one row per (partner, org): name, domain, types.
  partnership_mrr_monthly  — one row per (org, month, channel): SUM(MRRUSD) $.

The dashboard reads only these two small private tables; the corp scan
(~570 MB, billed to the corp owner) happens solely during a sync.
"""
import io
import json
import logging
import re
import time
from collections import Counter, defaultdict

from google.cloud import bigquery

from core.bigquery import client, table_ref

logger = logging.getLogger(__name__)

MEMBERS_TABLE = "partnership_members"
MRR_MONTHLY_TABLE = "partnership_mrr_monthly"
FREQ_KEY = "partnership_sync_frequency"
SOURCE_KEY = "partnership_source"   # "sheet" (full) | "commissions" (billed only)

# How many trailing months of MRR to keep (≈12 full + the current partial month).
_MONTHS_WINDOW = 13

_MEMBERS_SCHEMA = [
    bigquery.SchemaField("partner", "STRING"),          # agency name
    bigquery.SchemaField("organisation_id", "INTEGER"),
    bigquery.SchemaField("organisation", "STRING"),
    bigquery.SchemaField("types", "STRING"),            # "Acquisition,Service"
    bigquery.SchemaField("domain", "STRING"),           # may be empty
]

_MRR_MONTHLY_SCHEMA = [
    bigquery.SchemaField("organisation_id", "INTEGER"),
    bigquery.SchemaField("month", "STRING"),            # "YYYY-MM"
    bigquery.SchemaField("channel", "STRING"),          # corp Media: Email / ExtraFeatures / …
    bigquery.SchemaField("mrr", "FLOAT"),               # SUM(MRRUSD) for that org·month·channel
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


def get_frequency() -> str:
    from core.bigquery import get_setting
    try:
        f = get_setting(FREQ_KEY, "off")
        return f if f in {"off", "monthly", "weekly", "daily"} else "off"
    except Exception:
        return "off"


def set_frequency(freq: str) -> dict:
    from core.bigquery import set_setting
    if freq not in {"off", "monthly", "weekly", "daily"}:
        raise ValueError(f"bad partnership frequency: {freq}")
    set_setting(FREQ_KEY, freq)
    return {"status": "ok", "frequency": freq}


# Membership source preference: the master registry Google Sheet (full list,
# incl. clients with no MRR yet) first; fall back to the commissions table
# (billed clients only) when the Sheet is not shared with the corp SA yet.
_MEMBERS_SHEET = "table_partnership_clients"      # Drive-backed external table
_MEMBERS_FALLBACK = "t_partnership_commissions"   # readable, billed clients only


def _fetch_membership(cc, cds: str) -> tuple[list[dict], str, int]:
    """Return (rows, source, mb). Each row: partner, organisation_id, organisation,
    types. Prefers the full master Sheet; silently falls back when it 403s."""
    def _agg(table: str):
        q = f"""
            SELECT partner,
                   SAFE_CAST(organisation_id AS INT64) AS organisation_id,
                   ANY_VALUE(organisation) AS organisation,
                   STRING_AGG(DISTINCT type ORDER BY type) AS types
            FROM `{cds}.{table}`
            WHERE partner IS NOT NULL AND organisation_id IS NOT NULL
            GROUP BY partner, organisation_id
        """
        job = cc.query(q)
        rows = [{"partner": r["partner"], "organisation_id": int(r["organisation_id"]),
                 "organisation": r["organisation"] or "", "types": r["types"] or ""}
                for r in job.result() if r["organisation_id"] is not None]
        return rows, round((job.total_bytes_billed or 0) / 1e6)

    try:
        rows, mb = _agg(_MEMBERS_SHEET)
        logger.info(f"partnership membership from master Sheet: {len(rows)} rows")
        return rows, "sheet", mb
    except Exception as e:
        logger.warning(f"master Sheet not readable ({str(e)[:80]}…) — falling back to commissions table")
        rows, mb = _agg(_MEMBERS_FALLBACK)
        return rows, "commissions", mb


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _enrich_with_hierarchy(cc, corp_project: str, mem_rows: list[dict]) -> tuple[list[dict], int]:
    """Native corpBQ fallback-enrichment: the agency↔client link also lives in the
    organisation hierarchy — a client's `Parent_OrganisationID` points at the agency
    org (e.g. RTMuse = org 106395 'RTMuseAgecy', Remrkt = 108976). Where a partner's
    registry clients share a parent org whose NAME matches the partner (so we skip
    generic buckets like 'Trial'/'SPRINGO'/'Yespo parent'), we pull ALL that parent's
    children — adding non-billing clients the billed registry misses (e.g. Biopell).
    Returns (added_rows, mb_billed)."""
    OI = f"{corp_project}.es_live.jasper_report_OrganisationsInfo"
    mb = 0
    by_partner: dict[str, set] = defaultdict(set)
    for m in mem_rows:
        by_partner[m["partner"]].add(m["organisation_id"])
    client_ids = sorted({i for s in by_partner.values() for i in s})
    if not client_ids:
        return [], 0

    # parents of the registry clients
    j = cc.query(f"SELECT OrganisationID, Parent_OrganisationID FROM `{OI}` "
                 f"WHERE OrganisationID IN ({','.join(map(str, client_ids))})")
    parent_of = {int(r["OrganisationID"]): r["Parent_OrganisationID"] for r in j.result()}
    mb += round((j.total_bytes_billed or 0) / 1e6)
    parent_ids = sorted({p for p in parent_of.values() if p is not None})
    if not parent_ids:
        return [], mb

    # names of those parent orgs
    j = cc.query(f"SELECT OrganisationID, Organisation FROM `{OI}` "
                 f"WHERE OrganisationID IN ({','.join(map(str, parent_ids))})")
    parent_name = {int(r["OrganisationID"]): (r["Organisation"] or "") for r in j.result()}
    mb += round((j.total_bytes_billed or 0) / 1e6)

    # accept a parent as the agency's org only when its name matches the partner name
    agency_org: dict[int, str] = {}   # parent_org_id → partner name
    for partner, ids in by_partner.items():
        cnt = Counter(parent_of.get(i) for i in ids if parent_of.get(i) is not None)
        if not cnt:
            continue
        dom = cnt.most_common(1)[0][0]
        pn, pp = _norm(parent_name.get(int(dom), "")), _norm(partner)
        if pp and pn and (pp in pn or pn in pp):
            agency_org[int(dom)] = partner
    if not agency_org:
        return [], mb

    # all children of the accepted agency orgs → full client lists
    j = cc.query(f"SELECT OrganisationID, Organisation, Parent_OrganisationID FROM `{OI}` "
                 f"WHERE Parent_OrganisationID IN ({','.join(map(str, agency_org))})")
    existing = {(m["partner"], m["organisation_id"]) for m in mem_rows}
    added = []
    for r in j.result():
        oid = int(r["OrganisationID"])
        partner = agency_org.get(int(r["Parent_OrganisationID"]))
        if not partner or (partner, oid) in existing:
            continue
        existing.add((partner, oid))
        added.append({"partner": partner, "organisation_id": oid,
                      "organisation": r["Organisation"] or "", "types": ""})
    mb += round((j.total_bytes_billed or 0) / 1e6)
    logger.info(f"partnership hierarchy enrichment: +{len(added)} clients from "
                f"{len(agency_org)} agency orgs")
    return added, mb


def sync_partnership() -> dict:
    """Pull agency↔client membership + trailing per-channel monthly MRR from corpBQ
    into the two private tables. One ~570 MB corp scan per call — schedule sparsely."""
    from core.bigquery import corp_client, CORP_PROJECT_ID, CORP_DATASET
    t0 = time.time()
    cds = f"{CORP_PROJECT_ID}.{CORP_DATASET}"
    cc = corp_client()
    mb = 0

    # 1) Membership: partner ↔ org (+ types), full Sheet if shared, else billed clients.
    mem_rows, source, mb_mem = _fetch_membership(cc, cds)
    mb += mb_mem
    # Native enrichment from the org hierarchy (adds non-billing clients). Only when we
    # fell back to the billed registry — a readable Sheet is already the full master.
    if source == "commissions":
        added, mb_h = _enrich_with_hierarchy(cc, CORP_PROJECT_ID, mem_rows)
        if added:
            mem_rows = mem_rows + added
            source = "commissions+hierarchy"
        mb += mb_h
    org_ids = sorted({m["organisation_id"] for m in mem_rows})

    # 1b) Representative domain per org (for linking / opening the site). An org
    # can have several domains (e.g. posylka.de + posylka.com); pick the one that
    # matches the org name when possible, otherwise the first alphabetically so
    # the choice is stable across syncs (ANY_VALUE used to pick arbitrarily and
    # could disagree with the displayed name).
    dom_map: dict[int, list[str]] = {}
    if org_ids:
        idlist = ",".join(str(i) for i in org_ids)
        djob = cc.query(f"""
            SELECT best_org_id,
                   ARRAY_AGG(DISTINCT LOWER(lookup_domain_actual) ORDER BY LOWER(lookup_domain_actual)) AS domains
            FROM `{cds}.primary_domains_orgs_mrr`
            WHERE lookup_domain_actual IS NOT NULL AND best_org_id IN ({idlist})
            GROUP BY best_org_id
        """)
        for r in djob.result():
            if r["best_org_id"] is not None:
                dom_map[int(r["best_org_id"])] = list(r["domains"] or [])
        mb += round((djob.total_bytes_billed or 0) / 1e6)

    def _pick_domain(name: str, doms: list[str]) -> str:
        if not doms:
            return ""
        n = _norm(name)
        for d in doms:
            if _norm(d) == n:          # org name itself is one of the domains
                return d
        return doms[0]                 # deterministic (alphabetical) fallback

    members = [{**m, "domain": _pick_domain(m["organisation"], dom_map.get(m["organisation_id"], []))}
               for m in mem_rows]

    # 2) Per-org per-channel monthly MRR (each MRR row counted once), trailing window.
    monthly: list[dict] = []
    if org_ids:
        idlist = ",".join(str(i) for i in org_ids)
        mrr_sql = f"""
            SELECT m.OrganisationID AS organisation_id,
                   FORMAT_DATE('%Y-%m', DATE(m.DateDistributed)) AS month,
                   m.Media AS channel,
                   SUM(m.MRRUSD) AS mrr
            FROM `{CORP_PROJECT_ID}.es_live.mv_Income_Report_MRR` m
            WHERE m.OrganisationID IN ({idlist})
              AND m.MRRUSD > 0 AND m.Media IS NOT NULL
              AND DATE(m.DateDistributed) >= DATE_SUB(CURRENT_DATE(), INTERVAL {_MONTHS_WINDOW - 1} MONTH)
            GROUP BY organisation_id, month, channel
        """
        job2 = corp_client().query(mrr_sql)
        monthly = [{"organisation_id": int(r["organisation_id"]), "month": r["month"],
                    "channel": r["channel"], "mrr": float(r["mrr"])} for r in job2.result()]
        mb += round((job2.total_bytes_billed or 0) / 1e6)

    _load(MEMBERS_TABLE, _MEMBERS_SCHEMA, members)
    _load(MRR_MONTHLY_TABLE, _MRR_MONTHLY_SCHEMA, monthly)
    try:
        from core.bigquery import set_setting
        set_setting(SOURCE_KEY, source)  # "sheet" (full registry) or "commissions" (billed only)
    except Exception:
        pass

    agencies = len({m["partner"] for m in members})
    elapsed = round(time.time() - t0, 1)
    logger.info(f"sync_partnership: {len(members)} clients, {agencies} agencies, "
                f"{len(monthly)} org-month-channels, source={source}, {mb} MB corp, {elapsed}s")
    return {"status": "ok", "clients": len(members), "agencies": agencies,
            "org_month_channels": len(monthly), "source": source,
            "mb_billed_corp": mb, "elapsed": elapsed}


def get_partnership_rows() -> dict:
    """Build the dashboard payload from the two private tables.

    Returns {"clients": [...], "months": [...], "generated": bool}. Each client:
      partner, organisation_id, organisation, domain, types,
      mrr (latest-month total, all channels),
      mrr_channels (JSON {channel: val} for the latest month),
      mrr_monthly  (JSON recent-first [{month, mrr, ch:{channel: val}}]).
    """
    bq = client()
    try:
        mem_rows = list(bq.query(
            f"SELECT partner, organisation_id, organisation, types, domain "
            f"FROM `{table_ref(MEMBERS_TABLE)}`").result())
    except Exception:
        return {"clients": [], "months": [], "generated": False}
    try:
        mrr_rows = list(bq.query(
            f"SELECT organisation_id, month, channel, mrr "
            f"FROM `{table_ref(MRR_MONTHLY_TABLE)}`").result())
    except Exception:
        mrr_rows = []

    # org → {month: {channel: mrr}}
    by_org: dict[int, dict] = defaultdict(lambda: defaultdict(dict))
    all_months: set[str] = set()
    for r in mrr_rows:
        oid = int(r["organisation_id"])
        by_org[oid][r["month"]][r["channel"] or "?"] = float(r["mrr"])
        all_months.add(r["month"])
    months_sorted = sorted(all_months)

    clients = []
    for m in mem_rows:
        oid = int(m["organisation_id"])
        chd = by_org.get(oid) or {}
        # recent-first monthly list with per-channel split + the month total
        recent = sorted(chd.keys(), reverse=True)
        mrr_monthly = [{
            "month": mo,
            "mrr": round(sum(v for v in chd[mo].values() if v and v > 0), 2),
            "ch": {c: round(v, 2) for c, v in chd[mo].items() if v and v > 0},
        } for mo in recent]
        latest = recent[0] if recent else None
        clients.append({
            "partner": m["partner"],
            "organisation_id": oid,
            "organisation": m["organisation"] or "",
            "domain": m["domain"] or "",
            "types": m["types"] or "",
            "mrr": round(sum(v for v in chd.get(latest, {}).values() if v and v > 0), 2) if latest else 0.0,
            "mrr_channels": json.dumps(
                {c: round(v, 2) for c, v in (chd.get(latest, {}) or {}).items() if v and v > 0},
                ensure_ascii=False) if latest else "{}",
            "mrr_monthly": json.dumps(mrr_monthly, ensure_ascii=False),
        })

    clients.sort(key=lambda c: (c["partner"] or "", -(c["mrr"] or 0)))
    try:
        from core.bigquery import get_setting
        source = get_setting(SOURCE_KEY, "commissions")
    except Exception:
        source = "commissions"
    return {"clients": clients, "months": months_sorted, "source": source, "generated": True}
