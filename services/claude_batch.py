"""
Claude Batch API path for AI classification — the "Safe / thrifty" mode.

Live classify_domain() posts one request per domain at full price. Batch mode
packs many requests into a single Message Batch that Anthropic processes
asynchronously (typically <1h, max 24h) at **50% of the token price**.

Because results arrive later, we persist a small registry in privateBQ so a
scheduler can poll in-flight batches and apply results whenever they finish:
  ai_batches       — one row per submitted batch (id, status, counts, applied).
  ai_batch_items   — custom_id → domain map (Batch custom_ids can't contain dots).
On completion each result is written to corpBQ claude_responses + privateBQ
ai_parsed via the same save path as the live flow.
"""
import logging
from datetime import datetime, timezone

from google.cloud import bigquery

from config.settings import ANTHROPIC_API_KEY
from core.bigquery import client, table_ref
from services.claude_ai import (
    AI_MODEL, AI_MAX_TOKENS, build_classification_prompt, parse_classification_text,
)

logger = logging.getLogger(__name__)

BATCHES_TABLE = "ai_batches"
BATCH_ITEMS_TABLE = "ai_batch_items"

_BATCHES_SCHEMA = [
    bigquery.SchemaField("batch_id", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("count", "INTEGER"),
    bigquery.SchemaField("applied", "BOOLEAN"),
    bigquery.SchemaField("submitted_at", "TIMESTAMP"),
    bigquery.SchemaField("job_id", "STRING"),
]
_ITEMS_SCHEMA = [
    bigquery.SchemaField("batch_id", "STRING"),
    bigquery.SchemaField("custom_id", "STRING"),
    bigquery.SchemaField("domain", "STRING"),
]


def _anthropic():
    import anthropic
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def _append(table: str, schema: list, rows: list[dict]):
    """Append via a load job (not streaming) so later UPDATE/DELETE isn't blocked
    by the streaming buffer."""
    import io, json as _json
    bq = client()
    cfg = bigquery.LoadJobConfig(
        schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
    data = "\n".join(_json.dumps(r) for r in rows).encode()
    bq.load_table_from_file(io.BytesIO(data), table_ref(table), job_config=cfg).result()


def _ensure_tables():
    bq = client()
    for name, schema in ((BATCHES_TABLE, _BATCHES_SCHEMA), (BATCH_ITEMS_TABLE, _ITEMS_SCHEMA)):
        ref = f"{table_ref(name)}"
        try:
            bq.get_table(ref)
        except Exception:
            bq.create_table(bigquery.Table(ref, schema=schema))


# Anthropic caps one Message Batch at 100,000 requests OR 256 MB (whichever first).
# Chunk well under both so ANY job size submits without the caller minding the limit.
_BATCH_MAX_BYTES = 180 * 1024 * 1024  # 180 MB safety margin under the 256 MB cap


def submit_classification_batch(items: list[dict], job_id: str = "") -> dict:
    """items: [{domain, sw_title, sw_description, sw_category, bw_cms, bw_ecommerce,
    homepage_text}]. Auto-splits into as many Message Batches as needed (each under
    the API's count/size caps) and registers every one. Returns {batches, count}."""
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request
    from config.settings import AI_BATCH_CHUNK

    items = [it for it in items if it.get("domain")]
    if not items:
        return {"status": "empty"}
    _ensure_tables()

    batch_ids: list[str] = []
    total = 0
    cur_reqs: list = []
    cur_rows: list[dict] = []
    cur_bytes = 0

    def _flush():
        nonlocal cur_reqs, cur_rows, cur_bytes
        if not cur_reqs:
            return
        batch = _anthropic().messages.batches.create(requests=cur_reqs)
        bid = batch.id
        for r in cur_rows:
            r["batch_id"] = bid
        _append(BATCH_ITEMS_TABLE, _ITEMS_SCHEMA, cur_rows)
        _append(BATCHES_TABLE, _BATCHES_SCHEMA, [{
            "batch_id": bid, "status": batch.processing_status, "count": len(cur_reqs),
            "applied": False, "submitted_at": datetime.now(timezone.utc).isoformat(),
            "job_id": job_id or None}])
        batch_ids.append(bid)
        logger.info(f"AI batch {len(batch_ids)} submitted: {bid} "
                    f"({len(cur_reqs)} domains, job={job_id})")
        cur_reqs, cur_rows, cur_bytes = [], [], 0

    for i, it in enumerate(items):
        cid = f"d{i}"  # Batch custom_id must be [A-Za-z0-9_-]; unique within its batch
        prompt = build_classification_prompt(
            it["domain"], it.get("sw_title", ""), it.get("sw_description", ""),
            it.get("sw_category", ""), it.get("bw_cms", ""), it.get("bw_ecommerce", ""),
            it.get("homepage_text", ""))
        pb = len(prompt.encode("utf-8"))
        # Start a new batch before this item would breach either cap
        if cur_reqs and (len(cur_reqs) >= AI_BATCH_CHUNK or cur_bytes + pb > _BATCH_MAX_BYTES):
            _flush()
        cur_reqs.append(Request(custom_id=cid, params=MessageCreateParamsNonStreaming(
            model=AI_MODEL, max_tokens=AI_MAX_TOKENS,
            thinking={"type": "disabled"},  # no reasoning needed; keep budget for JSON (Haiku 5.5 thinks by default)
            messages=[{"role": "user", "content": prompt}])))
        cur_rows.append({"batch_id": None, "custom_id": cid, "domain": it["domain"]})
        cur_bytes += pb
        total += 1

    _flush()
    logger.info(f"AI classification: {total} domains submitted across {len(batch_ids)} batch(es), job={job_id}")
    return {"status": "ok", "batches": len(batch_ids), "batch_ids": batch_ids, "count": total}


def _domain_map(batch_id: str) -> dict:
    bq = client()
    rows = bq.query(
        f"SELECT custom_id, domain FROM `{table_ref(BATCH_ITEMS_TABLE)}` WHERE batch_id=@b",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("b", "STRING", batch_id)])).result()
    return {r["custom_id"]: r["domain"] for r in rows}


def apply_batch(batch_id: str) -> dict:
    """If the batch has ended, fetch results and write them to corp+private.
    Returns counts. No-op (with status) if still processing."""
    cli = _anthropic()
    batch = cli.messages.batches.retrieve(batch_id)
    if batch.processing_status != "ended":
        _update_batch_status(batch_id, batch.processing_status)
        return {"status": batch.processing_status, "applied": 0}

    dmap = _domain_map(batch_id)
    ok = err = 0
    for res in cli.messages.batches.results(batch_id):
        domain = dmap.get(res.custom_id)
        if not domain:
            continue
        if res.result.type != "succeeded":
            err += 1
            continue
        text = next((b.text for b in res.result.message.content if b.type == "text"), "")
        parsed = parse_classification_text(text)
        if not parsed:
            err += 1
            continue
        try:
            # Do NOT write to corp claude_responses (owner's cache). Private only.
            from core.bigquery import save_ai_parsed
            save_ai_parsed(domain, {
                "ai_category": parsed["ai_category"],
                "ai_is_ecommerce": parsed["ai_is_ecommerce"],
                "ai_industry": parsed["ai_industry"],
                "ai_region": parsed.get("ai_region", "")})
            ok += 1
        except Exception:
            logger.exception(f"apply_batch save failed for {domain}")
            err += 1
    _mark_applied(batch_id)
    logger.info(f"AI batch {batch_id} applied: {ok} ok, {err} err")
    return {"status": "applied", "applied": ok, "errors": err}


def _update_batch_status(batch_id: str, status: str):
    client().query(
        f"UPDATE `{table_ref(BATCHES_TABLE)}` SET status=@s WHERE batch_id=@b",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("s", "STRING", status),
            bigquery.ScalarQueryParameter("b", "STRING", batch_id)])).result()


def _mark_applied(batch_id: str):
    client().query(
        f"UPDATE `{table_ref(BATCHES_TABLE)}` SET status='ended', applied=TRUE WHERE batch_id=@b",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("b", "STRING", batch_id)])).result()


def poll_pending_batches() -> dict:
    """Scheduler entry point: apply any submitted-but-not-applied batches that ended."""
    _ensure_tables()
    bq = client()
    try:
        rows = list(bq.query(
            f"SELECT batch_id FROM `{table_ref(BATCHES_TABLE)}` WHERE applied=FALSE").result())
    except Exception:
        return {"pending": 0}
    applied = 0
    for r in rows:
        try:
            out = apply_batch(r["batch_id"])
            if out.get("status") == "applied":
                applied += 1
        except Exception:
            logger.exception(f"poll_pending_batches: {r['batch_id']}")
    return {"pending": len(rows), "applied_now": applied}
