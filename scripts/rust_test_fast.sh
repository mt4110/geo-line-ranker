#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

run() {
  printf '[rust-test-fast] %s\n' "$*"
  "$@"
}

profile="${NEXTEST_PROFILE:-default}"
partition_args=()
if [[ -n "${NEXTEST_PARTITION:-}" ]]; then
  partition_args=(--partition "$NEXTEST_PARTITION")
fi

packages=(
  -p config
  -p context
  -p domain
  -p generic-csv
  -p geo
  -p jp-postal
  -p jp-rail
  -p jp-school
  -p ranking
  -p storage
  -p test-support
)

if [[ "${RUN_NEXTEST:-1}" != "0" ]]; then
  ./scripts/ensure_nextest.sh
  run cargo nextest run --profile "$profile" "${partition_args[@]}" "${packages[@]}"
fi

if [[ "${RUN_DOCTESTS:-1}" != "0" ]]; then
  run cargo test --doc "${packages[@]}"
fi
