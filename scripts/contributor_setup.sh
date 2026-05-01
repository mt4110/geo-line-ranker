#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=scripts/contributor_env.sh
source "$ROOT_DIR/scripts/contributor_env.sh"

cd "$ROOT_DIR"

run() {
  printf '[setup] %s\n' "$*"
  "$@"
}

if [[ ! -f .env ]]; then
  run cp .env.example .env
else
  printf '[setup] using existing .env\n'
fi

require_sql_only_contributor_mode \
  "setup" \
  "update .env before running the first-run baseline" \
  "update .env or unset the shell override before running the first-run baseline"

cat <<'EOF'
[setup] contributor baseline
[setup] fixed path: sql_only + event-csv + PostgreSQL/PostGIS + Redis
[setup] outside this entrypoint: live crawler, full mode, OpenSearch, managed infrastructure
EOF

run docker compose -f .docker/docker-compose.yaml up -d postgres redis
run ./scripts/wait_for_postgres.sh
run cargo run -p cli -- migrate
run cargo run -p cli -- seed example
run cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
