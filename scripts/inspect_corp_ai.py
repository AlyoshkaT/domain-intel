"""
Inspect corpBQ claude_responses table structure and sample data.
Run: python scripts/inspect_corp_ai.py
"""
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.bigquery import corp_client
from config.settings import CORP_PROJECT_ID, CORP_DATASET

TABLE = f"`{CORP_PROJECT_ID}.{CORP_DATASET}.claude_responses`"


def main():
    bq = corp_client()

    # 1. Total count
    cnt = list(bq.query(f"SELECT COUNT(*) as total FROM {TABLE}").result())
    print(f"\n=== claude_responses: {cnt[0]['total']:,} rows ===\n")

    # 2. Sample 5 rows — full structure
    rows = list(bq.query(f"""
        SELECT domain, fetched_at, response_json, input_hash
        FROM {TABLE}
        ORDER BY fetched_at DESC
        LIMIT 5
    """).result())

    print("--- 5 latest rows ---")
    for r in rows:
        rj = r["response_json"]
        data = rj if isinstance(rj, dict) else json.loads(rj) if rj else {}
        print(f"\nDomain:     {r['domain']}")
        print(f"Fetched at: {r['fetched_at']}")
        print(f"Input hash: {r.get('input_hash', '(none)')}")
        print(f"JSON keys:  {list(data.keys())}")
        for k, v in data.items():
            print(f"  {k}: {repr(v)} ({type(v).__name__})")

    # 3. Value distribution for key fields
    print("\n--- is_ecommerce distribution ---")
    for row in bq.query(f"""
        SELECT JSON_VALUE(response_json, '$.is_ecommerce') as val, COUNT(*) as cnt
        FROM {TABLE}
        GROUP BY val ORDER BY cnt DESC
    """).result():
        print(f"  {row['val']!r}: {row['cnt']:,}")

    print("\n--- top 10 categories ---")
    for row in bq.query(f"""
        SELECT JSON_VALUE(response_json, '$.category') as val, COUNT(*) as cnt
        FROM {TABLE}
        GROUP BY val ORDER BY cnt DESC LIMIT 10
    """).result():
        print(f"  {row['val']!r}: {row['cnt']:,}")

    print("\n--- top 10 subcategories ---")
    for row in bq.query(f"""
        SELECT JSON_VALUE(response_json, '$.subcategory') as val, COUNT(*) as cnt
        FROM {TABLE}
        GROUP BY val ORDER BY cnt DESC LIMIT 10
    """).result():
        print(f"  {row['val']!r}: {row['cnt']:,}")

    print("\n--- extra JSON keys (if any unexpected fields) ---")
    for row in bq.query(f"""
        SELECT DISTINCT k
        FROM {TABLE}, UNNEST(JSON_KEYS(response_json)) k
        ORDER BY k
    """).result():
        print(f"  {row['k']}")


if __name__ == "__main__":
    main()
