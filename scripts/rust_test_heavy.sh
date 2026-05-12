#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

run() {
  printf '[rust-test-heavy] %s\n' "$*"
  "$@"
}

profile="${NEXTEST_PROFILE:-default}"
partition_args=()
if [[ -n "${NEXTEST_PARTITION:-}" ]]; then
  partition_args=(--partition "$NEXTEST_PARTITION")
fi

packages=(
  -p api-contracts
  -p cache
  -p compatibility-tests
  -p crawler-core
  -p generic-http
  -p observability
  -p openapi
  -p worker-core
)

if [[ "${RUN_NEXTEST:-1}" != "0" ]]; then
  ./scripts/ensure_nextest.sh
  run cargo nextest run --profile "$profile" "${partition_args[@]}" "${packages[@]}"
  run cargo nextest run --profile "$profile" "${partition_args[@]}" -p storage-opensearch --no-default-features
fi

if [[ "${RUN_DOCTESTS:-1}" != "0" ]]; then
  run cargo test --doc -p storage-opensearch --no-default-features
fi
