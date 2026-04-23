#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_ENV_FILE="$ROOT_DIR/.env"
EXAMPLE_ENV_FILE="$ROOT_DIR/.env.example"
ENV_FILE_FROM_USER="${ENV_FILE:-}"
ENV_FILE=""
ENV_LOADED="false"
WARNINGS=0
REVIEW_ITEMS=0

note() {
  printf '\n[data-quality] %s\n' "$*"
}

info() {
  printf '  %s\n' "$*"
}

warn() {
  WARNINGS=$((WARNINGS + 1))
  printf '  warning: %s\n' "$*" >&2
}

review() {
  REVIEW_ITEMS=$((REVIEW_ITEMS + 1))
  printf '  review: %s\n' "$*"
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

direct_psql_ready() {
  [[ -n "${DATABASE_URL:-}" ]] && have_command psql
}

docker_compose_ready() {
  have_command docker && docker compose version >/dev/null 2>&1
}

docker_psql_ready() {
  docker_compose_ready
}

psql_ready() {
  direct_psql_ready || docker_psql_ready
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

tables_ready() {
  local tables="$1"
  local table_name

  for table_name in $tables; do
    if ! table_exists "$table_name"; then
      warn "table '${table_name}' is missing or unreachable; skipping"
      return 1
    fi
  done

  return 0
}

sql_scalar() {
  local query="$1"

  run_psql \
    -v ON_ERROR_STOP=1 \
    -P pager=off \
    -Atqc "SET default_transaction_read_only = on; ${query}" \
    2>/dev/null | tr -d '[:space:]'
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

  note "$title"
  if ! tables_ready "$tables"; then
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

guardrail_count() {
  local title="$1"
  local tables="$2"
  local count_query="$3"
  local sample_query="$4"
  local count

  note "$title"
  if ! tables_ready "$tables"; then
    return
  fi

  if ! count="$(sql_scalar "$count_query")"; then
    warn "count query failed"
    return
  fi

  if [[ ! "$count" =~ ^[0-9]+$ ]]; then
    warn "count query returned a non-numeric value: ${count:-empty}"
    return
  fi

  if (( count == 0 )); then
    info "ok"
    return
  fi

  review "count=${count}"
  if [[ -n "$sample_query" ]]; then
    run_sql "sample: $title" "$sample_query"
  fi
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
FAIL_ON_WARNING="${DATA_QUALITY_FAIL_ON_WARNING:-false}"

note "read-only data quality doctor"
info "root_dir=$ROOT_DIR"
info "env_file=$(display_path "$ENV_FILE")"
info "env_loaded=$ENV_LOADED"
info "candidate_retrieval_mode=${CANDIDATE_RETRIEVAL_MODE:-unset}"
info "fail_on_warning=$FAIL_ON_WARNING"
if [[ -n "${DATABASE_URL:-}" ]]; then
  info "database_url=$(redact_url "$DATABASE_URL")"
else
  info "database_url=unset"
fi

note "tool availability"
for command_name in docker psql; do
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

run_sql "database readiness" "
SELECT
    current_database() AS database_name,
    current_user AS database_user,
    pg_is_in_recovery() AS in_recovery,
    (SELECT extversion FROM pg_extension WHERE extname = 'postgis') AS postgis_version;
"

run_sql_if_tables "core data shape summary" "schools stations school_station_links events" "
SELECT 'schools' AS relation, COUNT(*) AS row_count FROM schools
UNION ALL
SELECT 'stations', COUNT(*) FROM stations
UNION ALL
SELECT 'school_station_links', COUNT(*) FROM school_station_links
UNION ALL
SELECT 'events_total', COUNT(*) FROM events
UNION ALL
SELECT 'events_active', COUNT(*) FROM events WHERE is_active = TRUE
UNION ALL
SELECT 'event_csv_active', COUNT(*) FROM events WHERE source_type = 'event_csv' AND is_active = TRUE
ORDER BY relation;
"

guardrail_count "schools without active events" "schools events" "
SELECT COUNT(*)
FROM schools AS school
WHERE NOT EXISTS (
    SELECT 1
    FROM events AS event
    WHERE event.school_id = school.id
      AND event.is_active = TRUE
);
" "
SELECT
    school.id,
    school.name,
    school.area,
    COUNT(event.id) FILTER (WHERE event.is_active = TRUE) AS active_events,
    COUNT(event.id) FILTER (WHERE event.is_active = FALSE) AS inactive_events
FROM schools AS school
LEFT JOIN events AS event ON event.school_id = school.id
GROUP BY school.id, school.name, school.area
HAVING COUNT(event.id) FILTER (WHERE event.is_active = TRUE) = 0
ORDER BY school.id ASC
LIMIT 20;
"

guardrail_count "schools without station links" "schools school_station_links" "
SELECT COUNT(*)
FROM schools AS school
WHERE NOT EXISTS (
    SELECT 1
    FROM school_station_links AS link
    WHERE link.school_id = school.id
);
" "
SELECT school.id, school.name, school.area
FROM schools AS school
WHERE NOT EXISTS (
    SELECT 1
    FROM school_station_links AS link
    WHERE link.school_id = school.id
)
ORDER BY school.id ASC
LIMIT 20;
"

guardrail_count "invalid station link metrics" "school_station_links" "
SELECT COUNT(*)
FROM school_station_links
WHERE walking_minutes <= 0
   OR distance_meters < 0
   OR hop_distance < 0;
" "
SELECT school_id, station_id, walking_minutes, distance_meters, hop_distance, line_name
FROM school_station_links
WHERE walking_minutes <= 0
   OR distance_meters < 0
   OR hop_distance < 0
ORDER BY school_id ASC, station_id ASC
LIMIT 20;
"

guardrail_count "missing popularity snapshots" "schools popularity_snapshots" "
SELECT COUNT(*)
FROM schools AS school
LEFT JOIN popularity_snapshots AS snapshot ON snapshot.school_id = school.id
WHERE snapshot.school_id IS NULL;
" "
SELECT school.id, school.name
FROM schools AS school
LEFT JOIN popularity_snapshots AS snapshot ON snapshot.school_id = school.id
WHERE snapshot.school_id IS NULL
ORDER BY school.id ASC
LIMIT 20;
"

guardrail_count "missing area snapshots" "schools area_affinity_snapshots" "
SELECT COUNT(*)
FROM (SELECT DISTINCT area FROM schools) AS school_area
LEFT JOIN area_affinity_snapshots AS snapshot ON snapshot.area = school_area.area
WHERE snapshot.area IS NULL;
" "
SELECT school_area.area
FROM (SELECT DISTINCT area FROM schools) AS school_area
LEFT JOIN area_affinity_snapshots AS snapshot ON snapshot.area = school_area.area
WHERE snapshot.area IS NULL
ORDER BY school_area.area ASC
LIMIT 20;
"

guardrail_count "popularity snapshots older than latest user event" "user_events popularity_snapshots" "
SELECT COUNT(*)
FROM popularity_snapshots AS snapshot
CROSS JOIN (SELECT MAX(created_at) AS latest_user_event_at FROM user_events) AS latest
WHERE latest.latest_user_event_at IS NOT NULL
  AND snapshot.refreshed_at < latest.latest_user_event_at;
" "
SELECT
    (SELECT MAX(created_at) FROM user_events) AS latest_user_event_at,
    MIN(refreshed_at) AS oldest_popularity_refresh,
    MAX(refreshed_at) AS latest_popularity_refresh
FROM popularity_snapshots;
"

guardrail_count "area snapshots older than latest user event" "user_events area_affinity_snapshots" "
SELECT COUNT(*)
FROM area_affinity_snapshots AS snapshot
CROSS JOIN (SELECT MAX(created_at) AS latest_user_event_at FROM user_events) AS latest
WHERE latest.latest_user_event_at IS NOT NULL
  AND snapshot.refreshed_at < latest.latest_user_event_at;
" "
SELECT
    (SELECT MAX(created_at) FROM user_events) AS latest_user_event_at,
    MIN(refreshed_at) AS oldest_area_refresh,
    MAX(refreshed_at) AS latest_area_refresh
FROM area_affinity_snapshots;
"

guardrail_count "non-seed events without source keys" "events" "
SELECT COUNT(*)
FROM events
WHERE source_type <> 'seed'
  AND (source_key IS NULL OR source_key = '');
" "
SELECT id, school_id, title, source_type, source_key, is_active, updated_at
FROM events
WHERE source_type <> 'seed'
  AND (source_key IS NULL OR source_key = '')
ORDER BY updated_at DESC, id ASC
LIMIT 20;
"

guardrail_count "duplicate active event natural keys" "events" "
SELECT COUNT(*)
FROM (
    SELECT
        source_type,
        COALESCE(source_key, '') AS source_key,
        school_id,
        title,
        starts_at,
        COUNT(*) AS duplicate_count
    FROM events
    WHERE is_active = TRUE
    GROUP BY source_type, COALESCE(source_key, ''), school_id, title, starts_at
    HAVING COUNT(*) > 1
) AS duplicates;
" "
SELECT
    source_type,
    COALESCE(source_key, '') AS source_key,
    school_id,
    title,
    starts_at,
    COUNT(*) AS duplicate_count
FROM events
WHERE is_active = TRUE
GROUP BY source_type, COALESCE(source_key, ''), school_id, title, starts_at
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, source_type ASC, source_key ASC, school_id ASC
LIMIT 20;
"

guardrail_count "source manifest identity mismatches" "source_manifests" "
SELECT COUNT(*)
FROM source_manifests
WHERE COALESCE(manifest_json ->> 'source_id', '') <> source_id;
" "
SELECT
    manifest_path,
    source_id,
    manifest_json ->> 'source_id' AS manifest_json_source_id,
    updated_at
FROM source_manifests
WHERE COALESCE(manifest_json ->> 'source_id', '') <> source_id
ORDER BY updated_at DESC, manifest_path ASC
LIMIT 20;
"

guardrail_count "event-csv import succeeded but has no active rows" "events import_runs" "
SELECT CASE
    WHEN EXISTS (
        SELECT 1
        FROM import_runs
        WHERE source_id = 'event-csv'
          AND status = 'succeeded'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM events
        WHERE source_type = 'event_csv'
          AND source_key = 'event-csv'
          AND is_active = TRUE
    )
    THEN 1
    ELSE 0
END;
" "
SELECT
    (SELECT id FROM import_runs WHERE source_id = 'event-csv' ORDER BY id DESC LIMIT 1) AS latest_import_run_id,
    (SELECT COUNT(*) FROM events WHERE source_type = 'event_csv' AND source_key = 'event-csv') AS event_csv_rows,
    (SELECT COUNT(*) FROM events WHERE source_type = 'event_csv' AND source_key = 'event-csv' AND is_active = TRUE) AS active_event_csv_rows;
"

guardrail_count "event-csv latest import has heavy deactivation" "import_runs import_reports" "
WITH latest_run AS (
    SELECT id, total_rows
    FROM import_runs
    WHERE source_id = 'event-csv'
      AND status = 'succeeded'
    ORDER BY id DESC
    LIMIT 1
),
deactivated AS (
    SELECT COALESCE(SUM(row_count), 0) AS row_count
    FROM import_reports
    WHERE import_run_id = (SELECT id FROM latest_run)
      AND code = 'event_csv_deactivated_stale_rows'
)
SELECT CASE
    WHEN (SELECT id FROM latest_run) IS NULL THEN 0
    WHEN (SELECT row_count FROM deactivated) > GREATEST(COALESCE((SELECT total_rows FROM latest_run), 0), 1) THEN 1
    ELSE 0
END;
" "
WITH latest_run AS (
    SELECT id, total_rows, started_at, completed_at
    FROM import_runs
    WHERE source_id = 'event-csv'
      AND status = 'succeeded'
    ORDER BY id DESC
    LIMIT 1
),
deactivated AS (
    SELECT COALESCE(SUM(row_count), 0) AS row_count
    FROM import_reports
    WHERE import_run_id = (SELECT id FROM latest_run)
      AND code = 'event_csv_deactivated_stale_rows'
)
SELECT
    latest_run.id,
    latest_run.total_rows,
    deactivated.row_count AS deactivated_rows,
    latest_run.started_at,
    latest_run.completed_at
FROM latest_run
CROSS JOIN deactivated;
"

guardrail_count "recent event-csv warning reports" "import_runs import_reports" "
SELECT COUNT(*)
FROM import_reports AS report
JOIN import_runs AS import_run ON import_run.id = report.import_run_id
WHERE import_run.source_id = 'event-csv'
  AND report.level IN ('warn', 'error')
  AND report.created_at >= NOW() - INTERVAL '7 days';
" "
SELECT
    import_run.id AS import_run_id,
    report.level,
    report.code,
    report.message,
    report.row_count,
    report.created_at
FROM import_reports AS report
JOIN import_runs AS import_run ON import_run.id = report.import_run_id
WHERE import_run.source_id = 'event-csv'
  AND report.level IN ('warn', 'error')
  AND report.created_at >= NOW() - INTERVAL '7 days'
ORDER BY report.created_at DESC, report.id DESC
LIMIT 20;
"

guardrail_count "failed jobs" "job_queue" "
SELECT COUNT(*)
FROM job_queue
WHERE status = 'failed';
" "
SELECT id, job_type, attempts, max_attempts, run_after, locked_at, last_error, updated_at
FROM job_queue
WHERE status = 'failed'
ORDER BY updated_at ASC, id ASC
LIMIT 20;
"

guardrail_count "stale running jobs" "job_queue" "
SELECT COUNT(*)
FROM job_queue
WHERE status = 'running'
  AND locked_at < NOW() - INTERVAL '15 minutes';
" "
SELECT
    id,
    job_type,
    attempts,
    max_attempts,
    locked_by,
    locked_at,
    NOW() - locked_at AS locked_for,
    last_error
FROM job_queue
WHERE status = 'running'
  AND locked_at < NOW() - INTERVAL '15 minutes'
ORDER BY locked_at ASC, id ASC
LIMIT 20;
"

guardrail_count "overdue queued jobs" "job_queue" "
SELECT COUNT(*)
FROM job_queue
WHERE status = 'queued'
  AND run_after < NOW() - INTERVAL '15 minutes';
" "
SELECT id, job_type, attempts, max_attempts, run_after, NOW() - run_after AS overdue_for, last_error
FROM job_queue
WHERE status = 'queued'
  AND run_after < NOW() - INTERVAL '15 minutes'
ORDER BY run_after ASC, id ASC
LIMIT 20;
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

note "completion"
info "review_items=${REVIEW_ITEMS}"
info "warnings=${WARNINGS}"
info "no destructive operation was executed"
if (( WARNINGS > 0 )) && [[ "$FAIL_ON_WARNING" == "true" || "$FAIL_ON_WARNING" == "1" ]]; then
  printf '  failure: strict mode failed because warnings were emitted\n' >&2
  exit 1
fi
