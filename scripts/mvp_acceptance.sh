#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/.docker/docker-compose.yaml}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgres}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-geo_line_ranker_mvp_acceptance}"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/geo-line-ranker-mvp-acceptance.XXXXXX")"
LOG_DIR="$TMP_DIR/logs"
RAW_DIR="$TMP_DIR/raw"
mkdir -p "$LOG_DIR" "$RAW_DIR"

API_LOG="$LOG_DIR/api.log"
WORKER_LOG="$LOG_DIR/worker.log"
HOME_JSON="$TMP_DIR/home.json"
SEARCH_JSON="$TMP_DIR/search.json"
READY_JSON="$TMP_DIR/ready.json"
UPDATED_CSV="$TMP_DIR/events.updated.csv"

API_PID=""
WORKER_PID=""

note() {
  printf '[mvp] %s\n' "$*"
}

pick_free_port() {
  python3 - <<'PY'
import socket

with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

cleanup() {
  local status=$?

  if [[ -n "$API_PID" ]] && kill -0 "$API_PID" >/dev/null 2>&1; then
    kill "$API_PID" >/dev/null 2>&1 || true
    wait "$API_PID" || true
  fi

  if [[ -n "$WORKER_PID" ]] && kill -0 "$WORKER_PID" >/dev/null 2>&1; then
    kill "$WORKER_PID" >/dev/null 2>&1 || true
    wait "$WORKER_PID" || true
  fi

  if (( status != 0 )); then
    if [[ -f "$API_LOG" ]]; then
      note "api log tail"
      tail -n 80 "$API_LOG" || true
    fi
    if [[ -f "$WORKER_LOG" ]]; then
      note "worker log tail"
      tail -n 80 "$WORKER_LOG" || true
    fi
  fi

  docker compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$TMP_DIR"
  exit "$status"
}

trap cleanup EXIT INT TERM

sql() {
  docker compose -f "$COMPOSE_FILE" exec -T "$POSTGRES_SERVICE" \
    psql -U postgres -d geo_line_ranker -v ON_ERROR_STOP=1 -Atqc "$1"
}

wait_for_http_json() {
  local url="$1"
  local output_file="$2"
  local timeout_secs="${3:-90}"
  local deadline=$((SECONDS + timeout_secs))

  while (( SECONDS < deadline )); do
    if curl -fsS "$url" >"$output_file" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done

  printf 'timed out waiting for %s\n' "$url" >&2
  return 1
}

wait_for_jobs() {
  local min_succeeded="$1"
  local baseline="$2"
  local timeout_secs="${3:-90}"
  local deadline=$((SECONDS + timeout_secs))

  while (( SECONDS < deadline )); do
    local pending
    local succeeded
    pending="$(sql "SELECT COUNT(*) FROM job_queue WHERE id > ${baseline} AND status IN ('queued', 'running')")"
    succeeded="$(sql "SELECT COUNT(*) FROM job_queue WHERE id > ${baseline} AND status = 'succeeded'")"
    if [[ "$pending" == "0" ]] && (( succeeded >= min_succeeded )); then
      return 0
    fi
    sleep 1
  done

  printf 'timed out waiting for worker jobs after baseline %s\n' "$baseline" >&2
  return 1
}

retry_command() {
  local attempts="$1"
  local delay_secs="$2"
  shift 2

  local attempt=1
  while true; do
    if "$@"; then
      return 0
    fi

    if (( attempt >= attempts )); then
      return 1
    fi

    note "retrying (${attempt}/${attempts}) after transient failure: $*"
    attempt=$((attempt + 1))
    sleep "$delay_secs"
  done
}

cd "$ROOT_DIR"

set -a
# shellcheck disable=SC1091
source "$ROOT_DIR/.env.example"
set +a

POSTGRES_HOST_PORT="${POSTGRES_HOST_PORT:-$(pick_free_port)}"
REDIS_HOST_PORT="${REDIS_HOST_PORT:-$(pick_free_port)}"
APP_PORT="${APP_PORT:-$(pick_free_port)}"
APP_URL="http://127.0.0.1:${APP_PORT}"

export POSTGRES_HOST_PORT
export POSTGRES_SERVICE="postgres"
export POSTGRES_DB="geo_line_ranker"
export POSTGRES_USER="postgres"
export REDIS_HOST_PORT
export APP_BIND_ADDR="127.0.0.1:${APP_PORT}"
export DATABASE_URL="postgres://postgres:postgres@127.0.0.1:${POSTGRES_HOST_PORT}/geo_line_ranker"
export REDIS_URL="redis://127.0.0.1:${REDIS_HOST_PORT}"
export RANKING_CONFIG_DIR="$ROOT_DIR/configs/ranking"
export FIXTURE_DIR="$ROOT_DIR/storage/fixtures/minimal"
export RAW_STORAGE_DIR="$RAW_DIR"
export CANDIDATE_RETRIEVAL_MODE="sql_only"

note "starting minimal postgres and redis"
docker compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
docker compose -f "$COMPOSE_FILE" up -d postgres redis
"$ROOT_DIR/scripts/wait_for_postgres.sh"

note "bootstrapping schema, seed, and snapshots"
retry_command 5 2 cargo run -p cli -- migrate
retry_command 5 2 cargo run -p cli -- seed example
retry_command 5 2 cargo run -p cli -- snapshot refresh

note "starting worker and api"
cargo run -p worker -- serve >"$WORKER_LOG" 2>&1 &
WORKER_PID=$!
cargo run -p api -- serve >"$API_LOG" 2>&1 &
API_PID=$!

wait_for_http_json "$APP_URL/readyz" "$READY_JSON"

note "case 1/6 bootstrap readiness"
python3 - "$READY_JSON" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
assert payload["status"] == "ready", payload
assert payload["database"] == "reachable", payload
assert payload["cache"] == "reachable", payload
assert payload["opensearch"] == "disabled", payload
PY

note "case 2/6 placement behavior"
curl -fsS -X POST "$APP_URL/v1/recommendations" \
  -H "content-type: application/json" \
  -d '{"target_station_id":"st_tamachi","placement":"home","limit":3}' >"$HOME_JSON"
curl -fsS -X POST "$APP_URL/v1/recommendations" \
  -H "content-type: application/json" \
  -d '{"target_station_id":"st_tamachi","placement":"search","limit":3}' >"$SEARCH_JSON"
python3 - "$HOME_JSON" "$SEARCH_JSON" <<'PY'
import json
import sys

home = json.load(open(sys.argv[1], encoding="utf-8"))
search = json.load(open(sys.argv[2], encoding="utf-8"))

home_ids = [item["content_id"] for item in home["items"]]
search_ids = [item["content_id"] for item in search["items"]]

assert home["profile_version"] == search["profile_version"], (home, search)
assert home_ids, home
assert search_ids, search
assert home_ids != search_ids, (home_ids, search_ids)
PY

note "case 3/6 tracking to worker pipeline"
JOB_BASELINE="$(sql "SELECT COALESCE(MAX(id), 0) FROM job_queue")"
curl -fsS -X POST "$APP_URL/v1/track" \
  -H "content-type: application/json" \
  -d '{"user_id":"mvp-user-1","event_kind":"school_save","school_id":"school_garden"}' >/dev/null
curl -fsS -X POST "$APP_URL/v1/track" \
  -H "content-type: application/json" \
  -d '{"user_id":"mvp-user-1","event_kind":"search_execute","target_station_id":"st_tamachi"}' >/dev/null
wait_for_jobs 4 "$JOB_BASELINE"
[[ "$(sql "SELECT COUNT(*) FROM user_affinity_snapshots WHERE user_id = 'mvp-user-1'")" -ge 1 ]]
[[ "$(sql "SELECT COUNT(*) FROM popularity_snapshots WHERE search_execute_count > 0")" -ge 1 ]]
[[ "$(sql "SELECT COUNT(*) FROM job_queue WHERE id > ${JOB_BASELINE} AND status = 'failed'")" == "0" ]]

note "case 4/6 snapshot replay"
cargo run -p cli -- snapshot refresh
[[ "$(sql "SELECT COUNT(*) FROM popularity_snapshots")" -ge 1 ]]
[[ "$(sql "SELECT COUNT(*) FROM area_affinity_snapshots")" -ge 1 ]]

note "case 5/6 event csv import audit"
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
LATEST_IMPORT_RUN_ID="$(sql "SELECT id FROM import_runs WHERE source_id = 'event-csv' ORDER BY id DESC LIMIT 1")"
LATEST_STAGED_PATH="$(sql "SELECT staged_path FROM import_run_files WHERE import_run_id = ${LATEST_IMPORT_RUN_ID} ORDER BY id DESC LIMIT 1")"
[[ "$(sql "SELECT status FROM import_runs WHERE id = ${LATEST_IMPORT_RUN_ID}")" == "succeeded" ]]
[[ -n "$LATEST_STAGED_PATH" ]]
[[ -f "$LATEST_STAGED_PATH" ]]
[[ "$(sql "SELECT COUNT(*) FROM events WHERE source_type = 'event_csv' AND source_key = 'event-csv' AND is_active = TRUE")" == "4" ]]

note "case 6/6 event csv replacement semantics"
cat >"$UPDATED_CSV" <<'CSV'
event_id,school_id,title,event_category,is_open_day,is_featured,priority_weight,starts_at,placement_tags
event_seaside_open,school_seaside,Seaside Open Campus Summer Refresh,open_campus,true,false,0.9,2026-07-12T10:00:00+09:00,home|detail
CSV
cargo run -p cli -- import event-csv --file "$UPDATED_CSV"
[[ "$(sql "SELECT title FROM events WHERE id = 'event_seaside_open'")" == "Seaside Open Campus Summer Refresh" ]]
[[ "$(sql "SELECT is_active FROM events WHERE id = 'event_garden_lab'")" == "f" ]]
[[ "$(sql "SELECT COUNT(*) FROM events WHERE source_type = 'event_csv' AND source_key = 'event-csv' AND is_active = TRUE")" == "1" ]]

note "public MVP acceptance passed"
