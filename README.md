# Domain Intel

Інструмент масового аналізу доменів. Python + FastAPI + BigQuery.

## Встановлення

```bash
# 1. Python залежності
pip install -r requirements.txt

# 2. Frontend
cd frontend && npm install && npm run build && cd ..

# 3. Налаштування
cp .env.example .env
# Відредагуйте .env — вставте API ключі та шлях до credentials.json
```

## Запуск

```bash
# Development (бекенд)
uvicorn api.main:app --reload --port 8000

# Development (фронтенд — окремий термінал)
cd frontend && npm run dev

# Production (після npm run build у frontend/)
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

## Структура

```
config/settings.py     — всі налаштування з .env
core/bigquery.py       — BQ клієнт, кеш, jobs, results
services/
  similarweb.py        — SimilarWeb API
  builtwith.py         — BuiltWith API
  whatcms.py           — WhatCMS API
  claude_ai.py         — Claude Haiku класифікація
processing/
  pipeline.py          — обробка одного домену
  batch.py             — паралельна обробка + background jobs
api/main.py            — FastAPI endpoints
frontend/              — React UI
```

## BigQuery таблиці

Існуючі (не змінюються):
- `builtwith_cache` — кеш BuiltWith
- `similarweb_cache` — кеш SimilarWeb
- `whatcms_cache` — кеш WhatCMS

Нові (створюються автоматично при першому запуску):
- `analysis_jobs` — job-и та їх статуси
- `analysis_results` — оброблені результати
