"""
Technology search index — derived from bw_parsed.

Two tables let Explorer search ANY word across raw BuiltWith technologies cheaply
(instead of scanning the ~822 MB bw_parsed JSON on every search):

  tech_dictionary  — distinct tech name + domain_count (~23K rows, ~1-2 MB).
                     Powers autocomplete + "discover new technologies".
  domain_techs     — (domain, tech) pairs, CLUSTERED BY tech (~216 MB).
                     `WHERE tech IN (...)` prunes to a few clusters → cheap domain lookup.

Both are rebuilt from bw_parsed (full rebuild) or batch-updated for one job's domains.
"""
import logging
import time

from core.bigquery import client, table_ref, BW_PARSED_TABLE, GCP_PROJECT_ID, BIGQUERY_DATASET

logger = logging.getLogger(__name__)

TECH_DICTIONARY_TABLE = "tech_dictionary"
DOMAIN_TECHS_TABLE = "domain_techs"
TECH_DESCRIPTIONS_TABLE = "tech_descriptions"  # name → description + link (per-tech, from corp)


def _bw() -> str:
    return f"`{table_ref(BW_PARSED_TABLE)}`"


def _domain_techs() -> str:
    return f"`{table_ref(DOMAIN_TECHS_TABLE)}`"


def _dictionary() -> str:
    return f"`{table_ref(TECH_DICTIONARY_TABLE)}`"


def rebuild_tech_index() -> dict:
    """Full rebuild of both tables from bw_parsed. Returns row counts + bytes billed."""
    t0 = time.time()
    bq = client()
    billed = 0

    # 1) domain_techs: distinct (domain, tech) from latest bw row per domain
    sql_dt = f"""
        CREATE OR REPLACE TABLE {_domain_techs()}
        CLUSTER BY tech AS
        WITH latest AS (
            SELECT domain, JSON_EXTRACT_ARRAY(technologies_json) AS arr
            FROM {_bw()}
            WHERE technologies_json IS NOT NULL AND technologies_json != ''
            QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
        )
        SELECT DISTINCT domain, JSON_VALUE(e, '$.n') AS tech
        FROM latest, UNNEST(latest.arr) AS e
        WHERE JSON_VALUE(e, '$.n') IS NOT NULL AND JSON_VALUE(e, '$.n') != ''
    """
    job = bq.query(sql_dt); job.result(); billed += job.total_bytes_billed or 0

    # 2) tech_dictionary: distinct tech + domain_count (+ lowercased for search)
    sql_dict = f"""
        CREATE OR REPLACE TABLE {_dictionary()} AS
        SELECT tech, LOWER(tech) AS tech_lc, COUNT(DISTINCT domain) AS domain_count
        FROM {_domain_techs()}
        GROUP BY tech
    """
    job = bq.query(sql_dict); job.result(); billed += job.total_bytes_billed or 0

    pairs = list(bq.query(f"SELECT COUNT(*) c FROM {_domain_techs()}").result())[0]["c"]
    techs = list(bq.query(f"SELECT COUNT(*) c FROM {_dictionary()}").result())[0]["c"]
    elapsed = round(time.time() - t0, 1)
    logger.info(f"rebuild_tech_index: {pairs} pairs, {techs} techs, {billed/1e6:.0f} MB, {elapsed}s")
    return {"status": "ok", "pairs": pairs, "techs": techs,
            "mb_billed": round(billed / 1e6), "elapsed": elapsed}


def _tables_exist() -> bool:
    bq = client()
    try:
        bq.get_table(table_ref(DOMAIN_TECHS_TABLE))
        bq.get_table(table_ref(TECH_DICTIONARY_TABLE))
        return True
    except Exception:
        return False


def _rebuild_dictionary() -> None:
    """Recompute the dictionary from domain_techs (cheap GROUP BY)."""
    client().query(f"""
        CREATE OR REPLACE TABLE {_dictionary()} AS
        SELECT tech, LOWER(tech) AS tech_lc, COUNT(DISTINCT domain) AS domain_count
        FROM {_domain_techs()}
        GROUP BY tech
    """).result()


def update_tech_index_for_domains(domains: list[str]) -> dict:
    """Batch-refresh the index for one job's domains (delete + reinsert from bw_parsed),
    then rebuild the dictionary. Falls back to a full rebuild if tables don't exist yet.
    """
    domains = sorted({d.strip().lower() for d in (domains or []) if d and d.strip()})
    if not domains:
        return {"status": "skip", "reason": "no domains"}
    if not _tables_exist():
        return rebuild_tech_index()

    from google.cloud import bigquery
    bq = client()
    t0 = time.time()
    # Chunk the domain list to stay well under BQ's 10k query-param limit.
    CHUNK = 5000
    for i in range(0, len(domains), CHUNK):
        chunk = domains[i:i + CHUNK]
        params = [bigquery.ArrayQueryParameter("d", "STRING", chunk)]
        cfg = bigquery.QueryJobConfig(query_parameters=params)
        bq.query(f"DELETE FROM {_domain_techs()} WHERE domain IN UNNEST(@d)", job_config=cfg).result()
        bq.query(f"""
            INSERT INTO {_domain_techs()} (domain, tech)
            WITH latest AS (
                SELECT domain, JSON_EXTRACT_ARRAY(technologies_json) AS arr
                FROM {_bw()}
                WHERE domain IN UNNEST(@d)
                  AND technologies_json IS NOT NULL AND technologies_json != ''
                QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
            )
            SELECT DISTINCT domain, JSON_VALUE(e, '$.n') AS tech
            FROM latest, UNNEST(latest.arr) AS e
            WHERE JSON_VALUE(e, '$.n') IS NOT NULL AND JSON_VALUE(e, '$.n') != ''
        """, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

    _rebuild_dictionary()
    elapsed = round(time.time() - t0, 1)
    logger.info(f"update_tech_index: {len(domains)} domains refreshed in {elapsed}s")
    return {"status": "ok", "domains": len(domains), "elapsed": elapsed}


def refresh_tech_descriptions() -> dict:
    """Rebuild the per-technology description+link dictionary from corp BuiltWith raw.
    Description/Link are stable per technology (1 distinct value each), so we store one
    row per name instead of bloating technologies_json. Scans corp once (~2 GB)."""
    import time as _t
    from core.bigquery import corp_client, CORP_PROJECT_ID, CORP_DATASET
    t0 = _t.time()
    cbq = corp_client()
    q = f"""
        SELECT name,
               ANY_VALUE(description) AS description,
               ANY_VALUE(link) AS link,
               ARRAY_TO_STRING(ARRAY_AGG(DISTINCT cat IGNORE NULLS ORDER BY cat), ', ') AS categories
        FROM (
            SELECT JSON_VALUE(tech, '$.Name')        AS name,
                   JSON_VALUE(tech, '$.Description') AS description,
                   JSON_VALUE(tech, '$.Link')        AS link,
                   cat
            FROM `{CORP_PROJECT_ID}.{CORP_DATASET}.builtwith_raw_data`,
                 UNNEST(JSON_QUERY_ARRAY(response_json, '$.Results[0].Result.Paths')) AS path,
                 UNNEST(JSON_QUERY_ARRAY(path, '$.Technologies')) AS tech
                 LEFT JOIN UNNEST(JSON_VALUE_ARRAY(tech, '$.Categories')) AS cat
            WHERE JSON_VALUE(tech, '$.Name') IS NOT NULL
        )
        WHERE name IS NOT NULL
        GROUP BY name
    """
    job = cbq.query(q)
    rows = [{"name": r["name"], "description": r["description"] or "", "link": r["link"] or "",
             "categories": r["categories"] or ""}
            for r in job.result()]

    import io as _io, json as _json
    from google.cloud import bigquery
    bq = client()
    schema = [bigquery.SchemaField("name", "STRING"),
              bigquery.SchemaField("description", "STRING"),
              bigquery.SchemaField("link", "STRING"),
              bigquery.SchemaField("categories", "STRING")]
    cfg = bigquery.LoadJobConfig(schema=schema,
                                 write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                 source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
    data = "\n".join(_json.dumps(r) for r in rows).encode()
    bq.load_table_from_file(_io.BytesIO(data), table_ref(TECH_DESCRIPTIONS_TABLE), job_config=cfg).result()
    elapsed = round(_t.time() - t0, 1)
    logger.info(f"refresh_tech_descriptions: {len(rows)} techs, {(job.total_bytes_billed or 0)/1e6:.0f} MB corp, {elapsed}s")
    return {"status": "ok", "techs": len(rows),
            "mb_billed_corp": round((job.total_bytes_billed or 0) / 1e6), "elapsed": elapsed}


def get_tech_descriptions() -> dict:
    """name(lower) → {description, link}. Empty dict if the table doesn't exist yet."""
    bq = client()
    try:
        rows = bq.query(f"SELECT name, description, link, "
                        f"IFNULL(categories, '') AS categories FROM {_q(TECH_DESCRIPTIONS_TABLE)}").result()
    except Exception:
        return {}
    return {r["name"].lower(): {"description": r["description"] or "", "link": r["link"] or "",
                                "categories": r["categories"] or ""}
            for r in rows if r["name"]}


def _q(table: str) -> str:
    return f"`{table_ref(table)}`"


def search_tech(q: str, limit: int = 50) -> list[dict]:
    """Autocomplete: dictionary rows whose tech name contains `q` (case-insensitive)."""
    q = (q or "").strip().lower()
    if len(q) < 2:
        return []
    from google.cloud import bigquery
    bq = client()
    rows = bq.query(
        f"""SELECT tech, domain_count FROM {_dictionary()}
            WHERE tech_lc LIKE @pat
            ORDER BY domain_count DESC, tech LIMIT @lim""",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("pat", "STRING", f"%{q}%"),
            bigquery.ScalarQueryParameter("lim", "INT64", limit),
        ]),
    ).result()
    return [{"tech": r["tech"], "domain_count": r["domain_count"]} for r in rows]


def domains_for_techs(techs: list[str]) -> list[str]:
    """Domains that have ANY of the given exact tech names (clustered lookup → cheap)."""
    techs = [t for t in (techs or []) if t and t.strip()]
    if not techs:
        return []
    from google.cloud import bigquery
    bq = client()
    rows = bq.query(
        f"SELECT DISTINCT domain FROM {_domain_techs()} WHERE tech IN UNNEST(@techs)",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ArrayQueryParameter("techs", "STRING", techs),
        ]),
    ).result()
    return [r["domain"] for r in rows]
