#!/usr/bin/env bash
# Restart the Domain Intel API server safely.
#   ./restart.sh        — graceful kill, then start
#   ./restart.sh -9     — force kill (use if the old process is hung)
set -u

cd "$(dirname "$0")" || exit 1

PORT=8000
LOG=/tmp/domain-intel.log
PATTERN="uvicorn api.main:app"

# 1. Stop any running instance
if pgrep -f "$PATTERN" >/dev/null; then
  echo "Stopping old server..."
  pkill ${1:-} -f "$PATTERN"
  # wait up to ~10s for it to actually exit
  for _ in $(seq 1 10); do
    pgrep -f "$PATTERN" >/dev/null || break
    sleep 1
  done
  if pgrep -f "$PATTERN" >/dev/null; then
    echo "Still alive — forcing kill (-9)"
    pkill -9 -f "$PATTERN"; sleep 1
  fi
else
  echo "No running server found."
fi

# 2. Start a fresh instance
echo "Starting server..."
nohup uvicorn api.main:app --port "$PORT" --host 0.0.0.0 > "$LOG" 2>&1 &
NEW_PID=$!

# 3. Wait for health
for _ in $(seq 1 20); do
  code=$(curl -s -m 4 -o /dev/null -w "%{http_code}" "http://localhost:$PORT/api/health" 2>/dev/null)
  if [ "$code" = "200" ]; then
    echo "✅ UP — http://localhost:$PORT  (PID $NEW_PID, log: $LOG)"
    exit 0
  fi
  sleep 1
done

echo "❌ Server did not become healthy in time. Last log lines:"
tail -20 "$LOG"
exit 1
