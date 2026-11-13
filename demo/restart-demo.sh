#!/usr/bin/env bash
# Demonstrates recovery after the collector restarts mid-flight.
#
# 1. Submits a slow export.
# 2. Restarts the collector container while the portal is still generating.
# 3. Shows the job being resumed purely from SQLite and completing.
set -euo pipefail

COLLECTOR="${COLLECTOR:-http://localhost:8001}"
PORTAL="${PORTAL:-http://localhost:8000}"
COMPOSE="${COMPOSE:-docker compose}"

c() { curl -sS "$@"; }

job=$(c -X POST "$COLLECTOR/jobs" -H 'Content-Type: application/json' \
  -d '{"portal_url":"'"$PORTAL"'/","batch_no":"DEMO-RESTART","options":{"slow":true}}')
job_id=$(echo "$job" | jq -r '.id')
echo "submitted job $job_id; state now:"
c "$COLLECTOR/jobs/$job_id" | jq -c '{status, portal_task_id}'

echo "restarting collector..."
$COMPOSE restart collector

# Wait for the API to come back.
until c "$COLLECTOR/health" >/dev/null 2>&1; do sleep 1; done
c "$COLLECTOR/health" | jq .

for _ in $(seq 1 30); do
  state=$(c "$COLLECTOR/jobs/$job_id")
  status=$(echo "$state" | jq -r '.status')
  echo "$state" | jq -c '{status, portal_task_id, download_ready}'
  [ "$status" = "COMPLETED" ] && break
  [ "$status" = "MANUAL_REVIEW" ] && { echo "unexpected review"; exit 1; }
  sleep 1
done

c -o /tmp/demo-restart.csv "$COLLECTOR/jobs/$job_id/download"
echo "downloaded $(wc -c < /tmp/demo-restart.csv) bytes"
