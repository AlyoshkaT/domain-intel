#!/bin/bash
# setup.sh — одноразове встановлення всіх залежностей

set -e
echo "⚙️  Domain Intel — встановлення"

# Python deps
echo ""
echo "📦 Python залежності..."
pip install -r requirements.txt

# Frontend deps + build
echo ""
echo "📦 Frontend залежності..."
cd frontend
npm install

echo ""
echo "🔨 Frontend build..."
npm run build
cd ..

echo ""
echo "✅ Готово!"
echo ""
echo "Наступні кроки:"
echo "  1. cp .env.example .env"
echo "  2. Відредагуйте .env — вставте API ключі"
echo "  3. Покладіть Google credentials JSON у корінь проекту як credentials.json"
echo "  4. bash start.sh prod"
