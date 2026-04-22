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
[post-mvp-hardening] command plan

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

Primary guide:
  docs/POST_MVP_HARDENING.md

Evidence chain:
  docs/PUBLIC_MVP_RELEASE_READINESS.md
  docs/POST_LAUNCH_RUNBOOK.md
  docs/OPERATOR_FEEDBACK_LOOP.md
  docs/PHASE11_REGRESSION_EVIDENCE.md

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

Doctor classification:
  blocker: affects the fixed sql_only + event-csv behavior, or hides whether
    just mvp-acceptance is meaningful
  accepted risk: visible, bounded, owned, and safe to carry temporarily
  follow-up: actionable, but outside the current public-MVP operating gate

Optional evidence outside the public MVP gate:
  crawler graduation: crawler doctor, dry-run, health, policy review, rollback path
  full-mode evaluation: SQL-only/full-mode comparison, projection sync, OpenSearch health
  managed infrastructure: explicit review only; not a fixed-gate requirement

This script is read-only. It does not run validation or change local services.
EOF
