#!/usr/bin/env python3
"""
Manual sync script: python sync_profiles.py
Builds domain_profiles table from corpBQ + our BQ.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

from services.domain_profiles import sync_domain_profiles

if __name__ == "__main__":
    print("=" * 60)
    print("Domain Profiles Sync")
    print("=" * 60)
    result = sync_domain_profiles()
    if "error" in result:
        print(f"\n❌ Error: {result['error']}")
        sys.exit(1)
    else:
        print(f"\n✅ Done! {result['total']} domains synced.")
