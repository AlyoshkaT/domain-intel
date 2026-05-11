"""
Redirects API router — exposes domain_redirects table.
"""
from fastapi import APIRouter, Request
from google.cloud import bigquery

from core.bigquery import client, table_ref
from services.redirect_resolver import REDIRECTS_TABLE

router = APIRouter()


@router.get("/api/redirects")
async def get_redirects(
    search: str = "",
    type: str = "",
    job_id: str = "",
    date_from: str = "",
    date_to: str = "",
    limit: int = 2000,
):
    bq = client()
    conditions = []
    params: list[bigquery.ScalarQueryParameter] = []

    if search:
        conditions.append(
            "(LOWER(original) LIKE @search OR LOWER(resolved) LIKE @search)"
        )
        params.append(bigquery.ScalarQueryParameter("search", "STRING", f"%{search.lower()}%"))
    if type:
        conditions.append("type = @type")
        params.append(bigquery.ScalarQueryParameter("type", "STRING", type))
    if job_id:
        conditions.append("job_id = @job_id")
        params.append(bigquery.ScalarQueryParameter("job_id", "STRING", job_id))
    if date_from:
        conditions.append("detected_at >= @date_from")
        params.append(bigquery.ScalarQueryParameter("date_from", "TIMESTAMP", date_from + "T00:00:00"))
    if date_to:
        conditions.append("detected_at <= @date_to")
        params.append(bigquery.ScalarQueryParameter("date_to", "TIMESTAMP", date_to + "T23:59:59"))

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    safe_limit = min(max(1, limit), 10000)

    query = f"""
        SELECT original, resolved, type,
               FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', detected_at) AS detected_at,
               job_id
        FROM `{table_ref(REDIRECTS_TABLE)}`
        {where}
        ORDER BY detected_at DESC
        LIMIT {safe_limit}
    """
    try:
        rows = list(bq.query(
            query,
            job_config=bigquery.QueryJobConfig(query_parameters=params) if params else None,
        ).result())
        data = [dict(r) for r in rows]
        return {"redirects": data, "total": len(data)}
    except Exception as e:
        return {"redirects": [], "total": 0, "error": str(e)}


@router.get("/api/redirects/jobs")
async def get_redirect_jobs():
    """Return distinct job_ids that have redirect records (for filter dropdown)."""
    bq = client()
    try:
        rows = list(bq.query(f"""
            SELECT DISTINCT job_id,
                   MIN(detected_at) AS first_seen
            FROM `{table_ref(REDIRECTS_TABLE)}`
            WHERE job_id IS NOT NULL AND job_id != ''
            GROUP BY job_id
            ORDER BY first_seen DESC
            LIMIT 200
        """).result())
        return {"jobs": [{"job_id": r["job_id"], "first_seen": str(r["first_seen"])} for r in rows]}
    except Exception as e:
        return {"jobs": [], "error": str(e)}
