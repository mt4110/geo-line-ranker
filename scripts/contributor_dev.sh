#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=scripts/contributor_env.sh
source "$ROOT_DIR/scripts/contributor_env.sh"

cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  cp .env.example .env
fi

require_sql_only_contributor_mode \
  "dev" \
  "use the manual api/worker recipes or optional full-mode runbook for non-baseline development" \
  "unset the shell override or use the manual api/worker recipes for optional modes"

API_PID=""
WORKER_PID=""

note() {
  printf '[dev] %s\n' "$*"
}

cleanup() {
  local status=$?

  if [[ -n "$API_PID" ]] && kill -0 "$API_PID" >/dev/null 2>&1; then
    kill "$API_PID" >/dev/null 2>&1 || true
    wait "$API_PID" >/dev/null 2>&1 || true
  fi

  if [[ -n "$WORKER_PID" ]] && kill -0 "$WORKER_PID" >/dev/null 2>&1; then
    kill "$WORKER_PID" >/dev/null 2>&1 || true
    wait "$WORKER_PID" >/dev/null 2>&1 || true
  fi

  exit "$status"
}

trap cleanup EXIT INT TERM

note "starting worker and API; run just setup first if the database is empty"
cargo run -p worker -- serve &
WORKER_PID=$!

cargo run -p api -- serve &
API_PID=$!

note "API will serve Swagger UI at http://127.0.0.1:4000/swagger-ui"
note "press Ctrl-C to stop both processes"

while true; do
  if ! kill -0 "$WORKER_PID" >/dev/null 2>&1; then
    wait "$WORKER_PID"
    exit $?
  fi

  if ! kill -0 "$API_PID" >/dev/null 2>&1; then
    wait "$API_PID"
    exit $?
  fi

  sleep 1
done
