#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

run() {
  printf '[ci-local] %s\n' "$*"
  "$@"
}

cat <<'EOF'
[ci-local] selected local mirror for separated contributor and CI checks
[ci-local] Includes Rust quality/nextest fast lane, SQL-only smoke, OpenAPI drift, docs links, TS SDK build, and frontend smoke.
[ci-local] It does not run or redefine the fixed public-MVP gate; use just mvp-acceptance for that.
EOF

run cargo fmt --all --check
run cargo clippy --workspace --all-targets --all-features -- -D warnings
run ./scripts/rust_test_fast.sh
run ./scripts/contributor_smoke.sh
run ./scripts/openapi_drift_check.sh
run ./scripts/docs_check.sh
run ./scripts/ts_sdk_check.sh
run ./scripts/frontend_smoke.sh
