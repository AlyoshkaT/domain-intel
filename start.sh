#!/bin/bash
# start.sh — запуск бекенду (production або dev)

set -e

MODE=${1:-dev}
PORT=${PORT:-8000}

echo "🚀 Domain Intel — запуск ($MODE)"

# Перевірка .env
if [ ! -f .env ]; then
  echo "⚠️  .env не знайдено. Копіюємо з .env.example..."
  cp .env.example .env
  echo "✏️  Відредагуйте .env та запустіть знову"
  exit 1
fi


if [ "$MODE" = "dev" ]; then
  echo "📡 Backend: http://localhost:$PORT"
  echo "   (фронтенд запускайте окремо: cd frontend && npm run dev)"
  uvicorn api.main:app --reload --port $PORT --host 0.0.0.0
else
  echo "📡 Production: http://0.0.0.0:$PORT"
  uvicorn api.main:app --port $PORT --host 0.0.0.0 --workers 1
fi
