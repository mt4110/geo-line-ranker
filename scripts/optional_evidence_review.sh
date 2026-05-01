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
[optional-evidence-review] operating handoff command plan

Repository:
  root: $ROOT_DIR
  branch: $BRANCH
  commit: $COMMIT

Operating handoff guide:
  docs/OPTIONAL_EVIDENCE_HANDOFF.md

Read-only handoff flow:
  intake -> triage -> recheck audit -> closeout ledger -> closeout integrity -> lifecycle index -> inventory report

Fixed public MVP boundary:
  candidate retrieval: sql_only
  operational content path: event-csv
  write store: PostgreSQL/PostGIS
  cache: Redis
  fixed gate: just mvp-acceptance
  outside gate: live crawler operation, full mode, OpenSearch, managed infrastructure

Review intent:
  Use docs/OPTIONAL_EVIDENCE_HANDOFF.md and this read-only plan when crawler,
  full-mode, OpenSearch, managed infrastructure, data-quality, or local review
  artifact evidence needs a follow-up or closeout decision without widening the
  fixed public MVP gate.

Minimum record fields:
  evidence source:
  evidence command:
  owner:
  recheck command or source:
  public-MVP boundary impact:
  public API impact:
  next action:
  issue or PR:

Inventory boundary:
  optional evidence records: handoff support only
  lifecycle index rows: review inventory only
  inventory reports: safe summary only
  findings: edit candidates for source records, linked records, closeout ledgers, or integrity notes
  labels: record aids only, not required gates or repository setup prerequisites
  raw artifacts: keep in artifact storage; do not print raw diff or review bodies in first-pass inventory

Validation when files change:
  cargo fmt --all --check
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  cargo test --workspace
  cargo run -p cli -- config lint
  cargo run -p cli -- source-manifest lint
  cargo run -p cli -- fixtures doctor --path storage/fixtures/minimal
  cargo run -p cli -- fixtures doctor --path storage/fixtures/demo_jp
  cargo run -p crawler -- manifest lint
  just mvp-acceptance
  DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
  git diff --check

If just is unavailable:
  ./scripts/mvp_acceptance.sh
  DATA_QUALITY_FAIL_ON_WARNING=true ./scripts/data_quality_doctor.sh

This script is read-only. It does not run validation or change local services.
EOF
