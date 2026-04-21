#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/.docker/docker-compose.yaml}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-geo_line_ranker}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
TIMEOUT_SECS="${WAIT_FOR_POSTGRES_TIMEOUT_SECS:-90}"

deadline=$((SECONDS + TIMEOUT_SECS))

while (( SECONDS < deadline )); do
  if docker compose -f "$COMPOSE_FILE" exec -T "$POSTGRES_SERVICE" \
    pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
    state="$(
      docker compose -f "$COMPOSE_FILE" exec -T "$POSTGRES_SERVICE" \
        psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atqc \
        "SELECT CASE WHEN pg_is_in_recovery() THEN 'recovery' ELSE 'ready' END" \
        2>/dev/null || true
    )"
    if [[ "$state" == "ready" ]]; then
      printf 'postgres is ready: service=%s db=%s\n' "$POSTGRES_SERVICE" "$POSTGRES_DB"
      exit 0
    fi
  fi

  sleep 1
done

printf 'timed out waiting for postgres to leave recovery mode: service=%s db=%s timeout=%ss\n' \
  "$POSTGRES_SERVICE" "$POSTGRES_DB" "$TIMEOUT_SECS" >&2
exit 1
