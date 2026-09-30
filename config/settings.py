"""
Central configuration — the SINGLE source of truth for every tunable in the app.

Rules:
  • Every env-configurable value lives HERE (read via os.getenv with a default).
  • No other module calls os.getenv for these — they import from this file.
  • Secrets stay in your private .env; see .env.example for the documented list.

Grouped: credentials/APIs → BigQuery → concurrency → HTTP timeouts →
retries/pauses → scheduler → cache. Change a default here or override in .env.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# BigQuery — projects, datasets, credentials
# ─────────────────────────────────────────────────────────────────────────────
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "esoteric-parsec-147012")
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "EU")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "es_analysis")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

CORP_PROJECT_ID = os.getenv("CORP_PROJECT_ID", "esoteric-parsec-147012")
CORP_DATASET = os.getenv("CORP_DATASET", "es_analysis")
GOOGLE_CORP_CREDENTIALS = os.getenv("GOOGLE_CORP_CREDENTIALS", "")

# BQ tables — raw API cache
BQ_BUILTWITH_CACHE = os.getenv("BUILTWITH_RAW_TABLE", "builtwith_raw_data")
BQ_SIMILARWEB_CACHE = os.getenv("SIMILARWEB_RAW_TABLE", "similarweb_raw_data")
# BQ tables — app state
BQ_JOBS_TABLE = "analysis_jobs"
BQ_RESULTS_TABLE = "analysis_results"

# Max bytes BigQuery may bill per query (cost guard). Reset daily by scheduler.
BQ_MAX_BYTES_BILLED_GB = int(os.getenv("BQ_MAX_BYTES_BILLED_GB", "10"))

# ─────────────────────────────────────────────────────────────────────────────
# External APIs — keys
# ─────────────────────────────────────────────────────────────────────────────
SIMILARWEB_RAPIDAPI_KEY = os.getenv("SIMILARWEB_RAPIDAPI_KEY", "")
BUILTWITH_API_KEY = os.getenv("BUILTWITH_API_KEY", "")
BUILTWITH_RAPIDAPI_KEY = os.getenv("BUILTWITH_RAPIDAPI_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# Pipedrive CRM (relationship-status sync)
PIPEDRIVE_API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN", "")
PIPEDRIVE_COMPANY_DOMAIN = os.getenv("PIPEDRIVE_COMPANY_DOMAIN", "")

# Google Sheets / Drive
GOOGLE_SHEETS_CATALOG_ID = os.getenv("GOOGLE_SHEETS_CATALOG_ID", "")
GOOGLE_SHEETS_CREDENTIALS = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "")

# ─────────────────────────────────────────────────────────────────────────────
# Concurrency — the main speed levers (used in processing/batch.py, limits.py)
# ─────────────────────────────────────────────────────────────────────────────
# How many domains flow through the pipeline at once. Biggest throughput lever.
# ~20 is the sweet spot; higher starts triggering SimilarWeb 429s (measured).
BATCH_CONCURRENCY = int(os.getenv("BATCH_CONCURRENCY", "20"))

# Per-service API caps (independent lanes). Default to BATCH_CONCURRENCY.
# SW is capped lower on purpose — RapidAPI throttles bursts with 429.
SW_CONCURRENCY = int(os.getenv("SW_CONCURRENCY", "6"))                          # SimilarWeb
BW_CONCURRENCY = int(os.getenv("BW_CONCURRENCY", str(BATCH_CONCURRENCY)))       # BuiltWith
AI_CONCURRENCY = int(os.getenv("AI_CONCURRENCY", str(BATCH_CONCURRENCY)))       # Claude

# Jobs with ≤ this many domains are "priority" — they pre-empt big jobs' new
# API calls so a small urgent run finishes fast. Used in processing/limits.py.
PRIORITY_MAX_DOMAINS = int(os.getenv("PRIORITY_MAX_DOMAINS", "10"))

# ─────────────────────────────────────────────────────────────────────────────
# Job lifecycle — crash recovery & cross-process safety (processing/batch.py)
# ─────────────────────────────────────────────────────────────────────────────
# Jobs live in one process's memory but their status/counters are shared in
# BigQuery. To stop the SAME job from running in two processes at once (the
# double-execution / double-BQ-cost bug), the owning process holds a "lease":
# every JOB_HEARTBEAT_SECONDS it re-writes updated_at + the live processed/failed
# counts. This (1) proves the job is still owned by a live process, and (2) keeps
# analysis_jobs' counters fresh so ANOTHER process reading the job sees a number
# close to reality (no more local=9k vs web=21k divergence). The same heartbeat
# reads the job's status back: if another process set it to cancelled/completed
# (Cancel / Force complete), this worker stops itself — cross-process stop.
JOB_HEARTBEAT_SECONDS = int(os.getenv("JOB_HEARTBEAT_SECONDS", "45"))
# On startup, only auto-resume a running/pending job whose lease (updated_at) is
# at least this old — i.e. its owner is truly gone. A fresh lease means another
# live process still owns it → don't resume (that was the double-run). Trade-off:
# a job interrupted by a FAST restart (gap < this) isn't auto-resumed — use the
# Resume button in the UI. Must be comfortably larger than JOB_HEARTBEAT_SECONDS.
JOB_STALE_RESUME_MINUTES = int(os.getenv("JOB_STALE_RESUME_MINUTES", "5"))

# ─────────────────────────────────────────────────────────────────────────────
# HTTP timeouts (seconds) — per outbound call
# ─────────────────────────────────────────────────────────────────────────────
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))        # SimilarWeb API (services/similarweb.py)
REDIRECT_TIMEOUT = int(os.getenv("REDIRECT_TIMEOUT", "3"))       # domain redirect probe (services/redirect_resolver.py)
HOMEPAGE_TIMEOUT = int(os.getenv("HOMEPAGE_TIMEOUT", "10"))      # homepage text fetch for AI (services/claude_ai.py)
AI_API_TIMEOUT = int(os.getenv("AI_API_TIMEOUT", "30"))          # Anthropic messages call (services/claude_ai.py)
PIPEDRIVE_TIMEOUT = int(os.getenv("PIPEDRIVE_TIMEOUT", "30"))    # Pipedrive REST (services/pipedrive.py)
BUILTWITH_WHOAMI_TIMEOUT = int(os.getenv("BUILTWITH_WHOAMI_TIMEOUT", "10"))  # BW credit check (services/credits.py)

# ─────────────────────────────────────────────────────────────────────────────
# Retries & pauses
# ─────────────────────────────────────────────────────────────────────────────
SW_MAX_RETRIES = int(os.getenv("SW_MAX_RETRIES", "5"))          # SimilarWeb 429 retry attempts
RATE_LIMIT_WAIT = int(os.getenv("RATE_LIMIT_WAIT", "10"))       # base backoff (s) after a 429
DELAY_BETWEEN_DOMAINS = float(os.getenv("DELAY_BETWEEN_DOMAINS", "0"))     # extra pause per domain (s), 0 = off
DELAY_BETWEEN_API_CALLS = int(os.getenv("DELAY_BETWEEN_API_CALLS", "300")) # pause between SW→BW→AI calls (ms)

# ─────────────────────────────────────────────────────────────────────────────
# Scheduler — daily auto-sync hours (UTC). Used in api/scheduler.py
# ─────────────────────────────────────────────────────────────────────────────
SYNC_HOUR_UTC = int(os.getenv("SYNC_HOUR_UTC", "4"))                    # domain_profiles full sync
PARSED_SYNC_HOUR_UTC = int(os.getenv("PARSED_SYNC_HOUR_UTC", "3"))      # corpBQ raw → privateBQ parsed
PIPEDRIVE_SYNC_HOUR_UTC = int(os.getenv("PIPEDRIVE_SYNC_HOUR_UTC", "2"))  # Pipedrive relationship status
PIPEDRIVE_MRR_HOUR_UTC = int(os.getenv("PIPEDRIVE_MRR_HOUR_UTC", "1"))    # Pipedrive MRR pull from corpBQ
RESET_BQ_LIMIT_HOUR_UTC = int(os.getenv("RESET_BQ_LIMIT_HOUR_UTC", "0"))  # reset BQ byte guard
BQ_LIMIT_DAILY_RESET_GB = int(os.getenv("BQ_LIMIT_DAILY_RESET_GB", "25")) # target for that reset

# ─────────────────────────────────────────────────────────────────────────────
# Cache
# ─────────────────────────────────────────────────────────────────────────────
CACHE_TTL_DAYS = int(os.getenv("CACHE_TTL_DAYS", "90"))         # raw API cache freshness window
