"""
Company-standard traffic buckets — single source of truth.

Scale from es_analysis.market_analysis, effective 2026-09-03. Used everywhere
(BQ column, exports, dashboard, filters) so our analyses line up with the rest of
the company. Labels must match CHAR-FOR-CHAR (incl. the numeric sort prefix and the
dot in "2.5M") or Data Studio filters break.

`traffic` = SimilarWeb visits (Engagments.Visits) = our `sw_visits`.

Company rule (lower bound inclusive):
    >= 2 500 000 → '05 VIP >2.5M'
    >=   150 000 → '04 Large >150k'
    >=    25 000 → '03 Medium >25k'
    >=    10 000 → '02 Small >10k'
    <     10 000 → '01 Micro <10k'
    NULL         → 'Unknown'

OUR adaptation (confirmed with the user 2026-10-19): a domain with sw_visits = 0
means SimilarWeb returned no real traffic — we treat 0 the SAME as NULL → 'Unknown'
(not Micro), matching the "not in SimilarWeb → Unknown" intent. Only sw_visits > 0
is bucketed by value.
"""

VIP     = "05 VIP >2.5M"
LARGE   = "04 Large >150k"
MEDIUM  = "03 Medium >25k"
SMALL   = "02 Small >10k"
MICRO   = "01 Micro <10k"
UNKNOWN = "Unknown"

# Ascending display order (numeric prefixes sort first, Unknown last).
BUCKET_ORDER = [MICRO, SMALL, MEDIUM, LARGE, VIP, UNKNOWN]


def traffic_bucket(visits) -> str:
    """Map a visits value to its company bucket label. None/0/≤0 → 'Unknown'."""
    try:
        v = float(visits) if visits is not None else None
    except (TypeError, ValueError):
        v = None
    if v is None or v <= 0:
        return UNKNOWN
    if v >= 2_500_000:
        return VIP
    if v >= 150_000:
        return LARGE
    if v >= 25_000:
        return MEDIUM
    if v >= 10_000:
        return SMALL
    return MICRO


def bucket_sql(col: str = "sw_visits") -> str:
    """BigQuery CASE expression yielding the bucket label from a visits column.
    Same rule as traffic_bucket() — keep the two in lockstep."""
    return (
        f"CASE "
        f"WHEN {col} IS NULL OR {col} <= 0 THEN '{UNKNOWN}' "
        f"WHEN {col} >= 2500000 THEN '{VIP}' "
        f"WHEN {col} >= 150000 THEN '{LARGE}' "
        f"WHEN {col} >= 25000 THEN '{MEDIUM}' "
        f"WHEN {col} >= 10000 THEN '{SMALL}' "
        f"ELSE '{MICRO}' END"
    )
