#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

git_value() {
  local fallback="$1"
  shift

  if command -v git >/dev/null 2>&1; then
    git "$@" 2>/dev/null || printf '%s' "$fallback"
    return
  fi

  printf '%s' "$fallback"
}

cd "$ROOT_DIR"

BRANCH="$(git_value unknown rev-parse --abbrev-ref HEAD)"
COMMIT="$(git_value unknown rev-parse --short HEAD)"

cat <<EOF
[release-readiness] public MVP release candidate command plan

Repository:
  root: $ROOT_DIR
  branch: $BRANCH
  commit: $COMMIT

Fixed public MVP boundary:
  candidate retrieval: sql_only
  operational content path: event-csv
  write store: PostgreSQL/PostGIS
  cache: Redis
  outside gate: live crawler, full mode, OpenSearch, managed infrastructure

Public references:
  docs/MVP_ACCEPTANCE.md
  docs/OPERATIONS.md
  docs/TESTING.md

Required local validation:
  cargo fmt --all --check
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  cargo test --workspace
  just mvp-acceptance
  git diff --check

Required release evidence:
  DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
  Treat review items as evidence unless they affect the fixed sql_only +
  event-csv gate or hide whether that gate is meaningful.

If just is unavailable:
  ./scripts/mvp_acceptance.sh
  DATA_QUALITY_FAIL_ON_WARNING=true ./scripts/data_quality_doctor.sh

CI evidence to compare with local validation:
  rust-quality
  rust-unit-tests
  rust-postgres-tests
  mvp-acceptance
  data-quality-doctor
  docs
  spellcheck

Release notes baseline:
  Public MVP runs on sql_only candidate retrieval with event-csv operational
  content, PostgreSQL/PostGIS as the reference write store, and Redis as cache
  only. Rail/station freshness is the latest available MLIT N02 snapshot.

This script is read-only. It does not run validation or change local services.
EOF
