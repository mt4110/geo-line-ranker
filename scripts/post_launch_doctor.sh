#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_ENV_FILE="$ROOT_DIR/.env"
EXAMPLE_ENV_FILE="$ROOT_DIR/.env.example"
ENV_FILE_FROM_USER="${ENV_FILE:-}"
ENV_FILE=""
ENV_LOADED="false"
WARNINGS=0

note() {
  printf '\n[doctor] %s\n' "$*"
}

info() {
  printf '  %s\n' "$*"
}

warn() {
  WARNINGS=$((WARNINGS + 1))
  printf '  warning: %s\n' "$*" >&2
}

have_command() {
  command -v "$1" >/dev/null 2>&1
}

redact_url() {
  printf '%s' "$1" | sed -E 's#(://)[^/@]*@#\1***@#'
}

display_path() {
  if [[ -z "$1" ]]; then
    printf 'none'
  else
    printf '%s' "${1#$ROOT_DIR/}"
  fi
}

runtime_env_present() {
  [[ -n "${DATABASE_URL:-}" ||
    -n "${REDIS_URL:-}" ||
    -n "${APP_BIND_ADDR:-}" ||
    -n "${CANDIDATE_RETRIEVAL_MODE:-}" ]]
}

derive_app_url() {
  if [[ -n "${APP_URL:-}" ]]; then
    printf '%s' "$APP_URL"
    return
  fi

  if [[ -n "${API_URL:-}" ]]; then
    printf '%s' "$API_URL"
    return
  fi

  local bind_addr="${APP_BIND_ADDR:-127.0.0.1:4000}"
  bind_addr="${bind_addr#http://}"
  bind_addr="${bind_addr#https://}"

  case "$bind_addr" in
    0.0.0.0:*) bind_addr="127.0.0.1:${bind_addr#*:}" ;;
    \[\:\:\]:*|:::*) bind_addr="127.0.0.1:${bind_addr##*:}" ;;
  esac

  printf 'http://%s' "$bind_addr"
}

psql_ready() {
  direct_psql_ready || docker_psql_ready
}

direct_psql_ready() {
  [[ -n "${DATABASE_URL:-}" ]] && have_command psql
}

docker_compose_ready() {
  have_command docker && docker compose version >/dev/null 2>&1
}

docker_psql_ready() {
  docker_compose_ready
}

direct_redis_ready() {
  [[ -n "${REDIS_URL:-}" ]] && have_command redis-cli
}

docker_redis_ready() {
  docker_compose_ready
}

run_psql() {
  if direct_psql_ready; then
    psql "$DATABASE_URL" "$@"
    return
  fi

  if docker_psql_ready; then
    docker compose -f "$COMPOSE_FILE" exec -T "$POSTGRES_SERVICE" \
      psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"
    return
  fi

  return 127
}

run_redis() {
  if direct_redis_ready; then
    redis-cli -u "$REDIS_URL" "$@"
    return
  fi

  if docker_redis_ready; then
    docker compose -f "$COMPOSE_FILE" exec -T redis redis-cli "$@"
    return
  fi

  return 127
}

sample_recommendation_cache_keys() {
  local sample_count=0
  local _

  while IFS= read -r _; do
    sample_count=$((sample_count + 1))
    if (( sample_count >= 1000 )); then
      break
    fi
  done < <(run_redis --scan --pattern 'geo-line-ranker:recommendations:*' 2>/dev/null || true)

  printf '%s' "$sample_count"
}

table_exists() {
  local table_name="$1"
  local table_exists_output

  psql_ready || return 1

  if ! table_exists_output="$(
    run_psql \
      -v ON_ERROR_STOP=1 \
      -P pager=off \
      -Atqc "SET default_transaction_read_only = on; SELECT to_regclass('public.${table_name}') IS NOT NULL;" \
      2>/dev/null
  )"; then
    return 1
  fi

  [[ "$table_exists_output" == "t" ]]
}

run_sql() {
  local title="$1"
  local query="$2"

  note "$title"
  if ! psql_ready; then
    warn "no PostgreSQL client path is available; set DATABASE_URL with psql installed or run the compose postgres service"
    return
  fi

  if ! run_psql \
    -v ON_ERROR_STOP=1 \
    -P pager=off \
    -q \
    -c "SET default_transaction_read_only = on; ${query}"; then
    warn "query failed; continue with the remaining read-only checks"
  fi
}

run_sql_if_tables() {
  local title="$1"
  local tables="$2"
  local query="$3"

  for table_name in $tables; do
    if ! table_exists "$table_name"; then
      note "$title"
      warn "table '${table_name}' is missing or unreachable; skipping"
      return
    fi
  done

  run_sql "$title" "$query"
}

cd "$ROOT_DIR"

if [[ -n "$ENV_FILE_FROM_USER" ]]; then
  ENV_FILE="$ENV_FILE_FROM_USER"
elif [[ -f "$DEFAULT_ENV_FILE" ]]; then
  ENV_FILE="$DEFAULT_ENV_FILE"
elif ! runtime_env_present && [[ -f "$EXAMPLE_ENV_FILE" ]]; then
  ENV_FILE="$EXAMPLE_ENV_FILE"
fi

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
  ENV_LOADED="true"
elif [[ -n "$ENV_FILE_FROM_USER" ]]; then
  warn "ENV_FILE was set but not found: $ENV_FILE_FROM_USER"
fi

COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/.docker/docker-compose.yaml}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-geo_line_ranker}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"

APP_READYZ_URL="$(derive_app_url)/readyz"

note "read-only post-launch doctor"
info "root_dir=$ROOT_DIR"
info "env_file=$(display_path "$ENV_FILE")"
info "env_loaded=$ENV_LOADED"
info "candidate_retrieval_mode=${CANDIDATE_RETRIEVAL_MODE:-unset}"
info "ranking_config_dir=${RANKING_CONFIG_DIR:-unset}"
info "raw_storage_dir=${RAW_STORAGE_DIR:-unset}"
if [[ -n "${DATABASE_URL:-}" ]]; then
  info "database_url=$(redact_url "$DATABASE_URL")"
else
  info "database_url=unset"
fi
if [[ -n "${REDIS_URL:-}" ]]; then
  info "redis_url=$(redact_url "$REDIS_URL")"
else
  info "redis_url=unset"
fi

note "tool availability"
for command_name in cargo curl docker psql redis-cli rg; do
  if have_command "$command_name"; then
    info "$command_name=available"
  else
    info "$command_name=missing"
  fi
done
if direct_psql_ready; then
  info "postgres_client=psql DATABASE_URL"
elif docker_psql_ready; then
  info "postgres_client=docker compose exec ${POSTGRES_SERVICE}"
else
  info "postgres_client=unavailable"
fi
if direct_redis_ready; then
  info "redis_client=redis-cli REDIS_URL"
elif docker_redis_ready; then
  info "redis_client=docker compose exec redis"
else
  info "redis_client=unavailable"
fi

note "api readiness"
if have_command curl; then
  if ! curl -fsS "$APP_READYZ_URL"; then
    warn "readyz request failed: $APP_READYZ_URL"
  else
    printf '\n'
  fi
else
  warn "curl is not installed; skipping readyz"
fi

run_sql "database readiness" "
SELECT
    current_database() AS database_name,
    current_user AS database_user,
    pg_is_in_recovery() AS in_recovery,
    (SELECT extversion FROM pg_extension WHERE extname = 'postgis') AS postgis_version;
"

run_sql_if_tables "core row counts" "schools stations school_station_links events" "
SELECT 'schools' AS relation, COUNT(*) AS row_count FROM schools
UNION ALL
SELECT 'stations', COUNT(*) FROM stations
UNION ALL
SELECT 'school_station_links', COUNT(*) FROM school_station_links
UNION ALL
SELECT 'events_total', COUNT(*) FROM events
UNION ALL
SELECT 'events_active', COUNT(*) FROM events WHERE is_active = TRUE
ORDER BY relation;
"

run_sql_if_tables "snapshot coverage" "schools popularity_snapshots area_affinity_snapshots user_affinity_snapshots" "
SELECT
    (SELECT COUNT(*) FROM schools) AS school_count,
    (SELECT COUNT(*) FROM popularity_snapshots) AS popularity_snapshot_count,
    (SELECT MAX(refreshed_at) FROM popularity_snapshots) AS latest_popularity_refresh,
    (SELECT COUNT(*) FROM area_affinity_snapshots) AS area_snapshot_count,
    (SELECT MAX(refreshed_at) FROM area_affinity_snapshots) AS latest_area_refresh,
    (SELECT COUNT(*) FROM user_affinity_snapshots) AS user_affinity_rows,
    (SELECT MAX(refreshed_at) FROM user_affinity_snapshots) AS latest_user_affinity_refresh;
"

run_sql_if_tables "job queue pressure" "job_queue" "
SELECT
    job_type,
    status,
    COUNT(*) AS job_count,
    MIN(run_after) AS oldest_run_after,
    MAX(updated_at) AS latest_update
FROM job_queue
GROUP BY job_type, status
ORDER BY job_type ASC, status ASC;
"

run_sql_if_tables "running or failed jobs" "job_queue" "
SELECT id, job_type, status, attempts, max_attempts, locked_by, locked_at, run_after, last_error
FROM job_queue
WHERE status IN ('running', 'failed')
ORDER BY
    CASE status WHEN 'running' THEN 0 ELSE 1 END,
    COALESCE(locked_at, run_after) ASC,
    id ASC
LIMIT 20;
"

run_sql_if_tables "recent event-csv imports" "import_runs import_run_files import_reports" "
SELECT id, source_id, status, total_rows, started_at, completed_at
FROM import_runs
WHERE source_id = 'event-csv'
ORDER BY id DESC
LIMIT 10;

SELECT import_run_id, logical_name, checksum_sha256, row_count, status
FROM import_run_files
ORDER BY id DESC
LIMIT 10;

SELECT import_run_id, level, code, message, row_count
FROM import_reports
ORDER BY id DESC
LIMIT 10;
"

run_sql_if_tables "recent event-csv rows" "events" "
SELECT id, school_id, title, event_category, is_active, source_key, updated_at
FROM events
WHERE source_type = 'event_csv'
ORDER BY updated_at DESC, id ASC
LIMIT 20;
"

run_sql_if_tables "recent crawl state" "crawl_runs crawl_fetch_logs crawl_parse_reports" "
SELECT id, source_id, parser_key, status, fetched_targets, parsed_rows, imported_rows, started_at, completed_at
FROM crawl_runs
ORDER BY id DESC
LIMIT 10;

SELECT crawl_run_id, logical_name, target_url, fetch_status, http_status, content_changed, fetched_at
FROM crawl_fetch_logs
ORDER BY id DESC
LIMIT 10;

SELECT crawl_run_id, logical_name, level, code, message, parsed_rows
FROM crawl_parse_reports
ORDER BY id DESC
LIMIT 10;
"

note "redis cache"
if ! direct_redis_ready && ! docker_redis_ready; then
  warn "no Redis client path is available; set REDIS_URL with redis-cli installed or run the compose redis service"
else
  if run_redis PING >/dev/null 2>&1; then
    info "redis=reachable"
    cache_sample_count="$(sample_recommendation_cache_keys)"
    info "recommendation_cache_keys_sampled=${cache_sample_count:-0}"
  else
    warn "redis ping failed"
  fi
fi

note "optional full-mode"
if [[ "${CANDIDATE_RETRIEVAL_MODE:-sql_only}" == "full" ]]; then
  if [[ -z "${OPENSEARCH_URL:-}" ]]; then
    warn "OPENSEARCH_URL is not set while CANDIDATE_RETRIEVAL_MODE=full"
  elif have_command curl; then
    if ! curl -fsS "${OPENSEARCH_URL}/_cluster/health?pretty=false"; then
      warn "OpenSearch health request failed"
    else
      printf '\n'
    fi
    if [[ -n "${OPENSEARCH_INDEX_NAME:-}" ]]; then
      if ! curl -fsS "${OPENSEARCH_URL}/${OPENSEARCH_INDEX_NAME}/_count"; then
        warn "OpenSearch index count request failed: ${OPENSEARCH_INDEX_NAME}"
      else
        printf '\n'
      fi
    fi
  else
    warn "curl is not installed; skipping OpenSearch checks"
  fi
else
  info "skipped; full-mode remains outside the public MVP gate"
fi

note "crawler manifest maturity"
if have_command rg; then
  rg -n 'source_maturity:' configs/crawler/sources || true
elif have_command grep; then
  grep -R -n 'source_maturity:' configs/crawler/sources || true
else
  warn "rg/grep unavailable; skipping manifest maturity scan"
fi

note "completion"
if (( WARNINGS > 0 )); then
  info "completed with warnings=${WARNINGS}"
else
  info "completed without warnings"
fi
info "no destructive operation was executed"
