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
[post-mvp-hardening] maintenance command plan

Repository:
  root: $ROOT_DIR
  branch: $BRANCH
  commit: $COMMIT

Fixed public MVP boundary:
  candidate retrieval: sql_only
  operational content path: event-csv
  write store: PostgreSQL/PostGIS
  cache: Redis
  fixed gate: just mvp-acceptance
  outside gate: crawler graduation, full mode, OpenSearch, managed infrastructure

Public references:
  docs/MVP_ACCEPTANCE.md
  docs/OPERATIONS.md
  docs/TESTING.md

Local validation and evidence:
  cargo fmt --all --check
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  cargo test --workspace
  just mvp-acceptance
  DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
  git diff --check

If just is unavailable:
  ./scripts/mvp_acceptance.sh
  DATA_QUALITY_FAIL_ON_WARNING=true ./scripts/data_quality_doctor.sh

Follow-up record fields:
  source:
  evidence command:
  evidence excerpt or SQL row:
  affected public-MVP boundary:
  classification:
  reason:
  next action:
  owner:
  recheck date:
  issue or PR:

This script is read-only. It does not run validation or change local services.
EOF
