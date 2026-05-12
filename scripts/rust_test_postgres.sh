#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

run() {
  printf '[rust-test-postgres] %s\n' "$*"
  "$@"
}

profile="${NEXTEST_PROFILE:-postgres}"

packages=(
  -p api
  -p cli
  -p crawler
  -p storage-postgres
  -p worker
)

if [[ "${RUN_NEXTEST:-1}" != "0" ]]; then
  ./scripts/ensure_nextest.sh
  run cargo nextest run --profile "$profile" "${packages[@]}"
fi

if [[ "${RUN_DOCTESTS:-1}" != "0" ]]; then
  run cargo test --doc "${packages[@]}"
fi
