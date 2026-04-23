#!/usr/bin/env python3
"""Save cached SimilarWeb JSON snapshot to data/similarweb_rozetka.json
Run locally where your project dependencies are installed.
Usage:
    python3 scripts/save_cached_local.py
"""
import os
import sys
import json
# ensure project root is on sys.path so `core` package is importable
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from core.bigquery import get_cached

OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
os.makedirs(OUT_DIR, exist_ok=True)

def main():
    data = get_cached('similarweb_raw_data', 'rozetka.com.ua')
    out_path = os.path.join(OUT_DIR, 'similarweb_rozetka.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print('Wrote', out_path)

if __name__ == '__main__':
    main()
