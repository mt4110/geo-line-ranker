#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

./scripts/rust_test_fast.sh
./scripts/rust_test_heavy.sh
./scripts/rust_test_postgres.sh
