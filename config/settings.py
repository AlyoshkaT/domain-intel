import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# BigQuery
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "esoteric-parsec-147012")
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "EU")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "es_analysis")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# BQ Tables - Cache
BQ_BUILTWITH_CACHE = os.getenv("BUILTWITH_RAW_TABLE", "builtwith_raw_data")
BQ_SIMILARWEB_CACHE = os.getenv("SIMILARWEB_RAW_TABLE", "similarweb_raw_data")

# BQ Tables - App
BQ_JOBS_TABLE = "analysis_jobs"
BQ_RESULTS_TABLE = "analysis_results"

# External APIs
SIMILARWEB_RAPIDAPI_KEY = os.getenv("SIMILARWEB_RAPIDAPI_KEY", "")
BUILTWITH_API_KEY = os.getenv("BUILTWITH_API_KEY", "")
BUILTWITH_RAPIDAPI_KEY = os.getenv("BUILTWITH_RAPIDAPI_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# Rate limits
DELAY_BETWEEN_DOMAINS = float(os.getenv("DELAY_BETWEEN_DOMAINS", "0"))
DELAY_BETWEEN_API_CALLS = int(os.getenv("DELAY_BETWEEN_API_CALLS", "300"))  # ms
RATE_LIMIT_WAIT = int(os.getenv("RATE_LIMIT_WAIT", "10"))  # seconds after 429
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))

# Cache TTL
CACHE_TTL_DAYS = 90

# Batch processing
BATCH_CONCURRENCY = int(os.getenv("BATCH_CONCURRENCY", "5"))

GOOGLE_SHEETS_CATALOG_ID = os.getenv('GOOGLE_SHEETS_CATALOG_ID', '')
GOOGLE_CORP_CREDENTIALS = os.getenv('GOOGLE_CORP_CREDENTIALS', '')
CORP_PROJECT_ID = os.getenv('CORP_PROJECT_ID', 'esoteric-parsec-147012')
CORP_DATASET = os.getenv('CORP_DATASET', 'es_analysis')
GOOGLE_SHEETS_CREDENTIALS = os.getenv('GOOGLE_SHEETS_CREDENTIALS', '')
