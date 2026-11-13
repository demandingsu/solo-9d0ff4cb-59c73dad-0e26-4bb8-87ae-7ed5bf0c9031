#!/usr/bin/env bash
# End-to-end demonstration of the LIMS export-collection API.
#
# Assumes docker compose is running (see README.md):
#   docker compose up --build
#
# Scenarios:
#   1. Slow generation: progress polling and verified download
#   2. Accept-then-disconnect: reconciliation via the portal task table,
#      no resubmission
#   3. Duplicate portal tasks: MANUAL_REVIEW, no resubmission
#   4. Service restart: durable recovery from SQLite
set -euo pipefail

COLLECTOR="${COLLECTOR:-http://localhost:8001}"
PORTAL="${PORTAL:-http://localhost:8000}"

c() { curl -sS "$@"; }
j() { jq -r "$@"; }

wait_terminal() {
  local job_id=$1 timeout=${2:-60}
  local deadline=$(( $(date +%s) + timeout ))
  while :; do
    local job
    job=$(c "$COLLECTOR/jobs/$job_id")
    local status
    status=$(echo "$job" | j '.status')
    case "$status" in
      COMPLETED|MANUAL_REVIEW|FAILED)
        echo "$job"; return 0 ;;
    esac
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "TIMEOUT waiting for job $job_id; last state: $job" >&2
      return 1
    fi
    sleep 1
  done
}

echo "== sanity =="
c "$COLLECTOR/health" | jq .
c "$PORTAL/tasks" | jq 'length' | xargs echo "portal task rows:"

echo
echo "== scenario 1: slow generation =="
job=$(c -X POST "$COLLECTOR/jobs" -H 'Content-Type: application/json' \
  -d '{"portal_url":"'"$PORTAL"'/","batch_no":"DEMO-SLOW","options":{"slow":true}}')
job_id=$(echo "$job" | j '.id')
echo "created job $job_id; polling..."
for _ in 1 2 3; do
  sleep 1
  c "$COLLECTOR/jobs/$job_id" | jq -c '{status, portal_task_id, download_ready}'
done
final=$(wait_terminal "$job_id")
echo "$final" | jq '{status, result_size, result_sha256}'
c -o /tmp/demo-slow.csv "$COLLECTOR/jobs/$job_id/download"
echo "downloaded: $(wc -c < /tmp/demo-slow.csv) bytes, sha256=$(sha256sum /tmp/demo-slow.csv | cut -d' ' -f1)"

echo
echo "== duplicate call shares the same job =="
again=$(c -X POST "$COLLECTOR/jobs" -H 'Content-Type: application/json' \
  -d '{"portal_url":"'"$PORTAL"'","batch_no":"DEMO-SLOW"}')
echo "reused=$(echo "$again" | j '.reused') same_id=$([ "$(echo "$again" | j '.id')" = "$job_id" ] && echo yes || echo no)"

echo
echo "== scenario 2: accept then disconnect (no resubmission) =="
job=$(c -X POST "$COLLECTOR/jobs" -H 'Content-Type: application/json' \
  -d '{"portal_url":"'"$PORTAL"'/","batch_no":"DEMO-DISC","options":{"disconnect":true}}')
job_id=$(echo "$job" | j '.id')
final=$(wait_terminal "$job_id")
echo "$final" | jq '{status, portal_task_id, download_ready}'
echo "attempt log:"
c "$COLLECTOR/jobs/$job_id/attempts" | jq -c '.[] | {phase, outcome}'
echo "portal-side rows for the batch (expect 1):"
c "$PORTAL/tasks?batch_no=DEMO-DISC" | jq 'length'

echo
echo "== scenario 3: duplicate portal tasks -> manual review =="
job=$(c -X POST "$COLLECTOR/jobs" -H 'Content-Type: application/json' \
  -d '{"portal_url":"'"$PORTAL"'/","batch_no":"DEMO-DUP","options":{"disconnect":true,"duplicate":true}}')
job_id=$(echo "$job" | j '.id')
final=$(wait_terminal "$job_id")
echo "$final" | jq '{status, review_code, review_reason}'
echo "download while blocked:"
c -o /dev/null -w "HTTP %{http_code} (409 expected)\n" "$COLLECTOR/jobs/$job_id/download"

echo
echo "== scenario 4: restart the collector and verify recovery =="
echo "run these commands yourself to see durable recovery:"
echo "  docker compose restart collector"
echo "  curl -s $COLLECTOR/health | jq .   # resumed_on_startup >= 1"
echo
echo "Demo finished."
