#!/usr/bin/env python3
"""Extract a defined set of columns from a SimilarWeb cached JSON snapshot.

Reads `data/similarweb_rozetka.json` (created by `save_cached_local.py`) or calls
`core.bigquery.get_cached` if the JSON is missing and the environment supports it.

Outputs:
- `data/similarweb_rozetka_columns.json` (single JSON object with requested keys)
- `data/similarweb_rozetka_columns.csv` (single-row CSV with headers)

Usage:
    python3 scripts/extract_similarweb_columns.py

You can edit `COLUMNS` below to match the exact fields you want.
"""
import os
import sys
import json
import csv

# ensure project root is on sys.path so `core` package is importable if needed
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
INPUT_JSON = os.path.join(DATA_DIR, "similarweb_rozetka.json")
OUT_JSON = os.path.join(DATA_DIR, "similarweb_rozetka_columns.json")
OUT_CSV = os.path.join(DATA_DIR, "similarweb_rozetka_columns.csv")

# The column keys we want to keep (must be valid JSON keys in output)
COLUMNS = [
    "Domain",
    "Traffic_sm",
    "CMS_LIST",
    "WhatCMS",
    "oSearch_Group",
    "oSearch",
    "EMS_LIST",
    "Claude_AI_Category_17",
    "Claude_AI_Ecomm",
    "Claude_AI_Industry",
    "Industry_parse",
    "Industry_bw",
    "Category_sw",
    "Subcategory_sw",
    "Description_sw",
    "Title_sw",
    "Primary Region",
    "Region %",
    "CompanyName",
    "oSearch_parse",
]


def load_data():
    if os.path.exists(INPUT_JSON):
        with open(INPUT_JSON, "r", encoding="utf-8") as f:
            return json.load(f)

    # Fallback: try to call get_cached if available in this environment
    try:
        from core.bigquery import get_cached
        print("No local JSON found — calling get_cached()")
        return get_cached("similarweb_raw_data", "rozetka.com.ua")
    except Exception as e:
        raise RuntimeError("No input data found and get_cached failed: " + str(e))


def pick(d: dict, key: str):
    """Best-effort extractor for requested columns.

    Tries common locations and nested fields based on the column name.
    """
    # direct key (case-sensitive)
    if key in d:
        return d[key]

    # helper to traverse nested dict paths
    def get_path(obj, *parts):
        cur = obj
        for p in parts:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(p)
            if cur is None:
                return None
        return cur

    # Traffic
    if key == "Traffic_sm":
        return get_path(d, "AiTrafficDetails", "TotalVisits") or get_path(d, "Traffic", "TotalVisits") or d.get("TotalVisits") or d.get("total_visits")

    # Primary region and percent
    if key in ("Primary Region", "Region %"):
        candidates = (
            get_path(d, "AiTrafficDetails", "TopRegions"),
            get_path(d, "Traffic", "TopRegions"),
            get_path(d, "top_country_shares"),
            get_path(d, "TopRegions"),
            get_path(d, "AiTrafficDetails", "TopCountries"),
        )
        for c in candidates:
            if isinstance(c, list) and c:
                first = c[0]
                if not isinstance(first, dict):
                    continue
                name = first.get("Name") or first.get("name") or first.get("Country")
                val = first.get("Value") or first.get("Share") or first.get("Percent")
                if key == "Primary Region" and name:
                    return name
                if key == "Region %" and val is not None:
                    return val

    # textual and company fields
    if key in ("Description_sw", "Title_sw", "CompanyName"):
        return d.get("Description") or d.get("description") or d.get("sw_description") or d.get("Title") or d.get("title") or d.get("company_name") or d.get("CompanyName")

    # common aliases / case-insensitive match at top level
    lowers = {k.lower(): v for k, v in d.items()}
    lk = key.lower()
    if lk in lowers:
        return lowers[lk]

    # try one-level nested dicts for a match (case-insensitive)
    for v in d.values():
        if isinstance(v, dict):
            for kk, vv in v.items():
                if kk.lower() == lk:
                    return vv

    return None


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    raw = load_data()

    # If raw contains wrapped by domain key, try to unwrap
    if isinstance(raw, dict) and len(raw) > 0 and any(k in raw for k in ("domain", "AiTrafficDetails", "Traffic", "Category")):
        # raw appears to be the object we want
        item = raw
    elif isinstance(raw, dict) and len(raw) == 1:
        # single-key dict, maybe {"rozetka.com.ua": {...}}
        item = list(raw.values())[0]
    else:
        # unexpected shape: try first element if list
        if isinstance(raw, list) and len(raw) > 0 and isinstance(raw[0], dict):
            item = raw[0]
        else:
            raise RuntimeError("Unsupported input JSON structure — expected object or list of objects")

    out = {}
    for col in COLUMNS:
        out[col] = pick(item, col)

    # ensure Domain key exists (use the capitalized column name)
    if not out.get("Domain"):
        out["Domain"] = item.get("domain") or item.get("Domain") or "rozetka.com.ua"

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("Wrote", OUT_JSON)

    # write CSV one row
    with open(OUT_CSV, "w", encoding="utf-8", newline='') as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        # convert any lists to pipe-separated string
        row = {k: ("|".join(v) if isinstance(v, list) else v) for k, v in out.items()}
        w.writerow(row)
    print("Wrote", OUT_CSV)


if __name__ == "__main__":
    main()
