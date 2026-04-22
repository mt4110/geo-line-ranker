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
[optional-evidence-review] graduation checklist

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
  outside gate: crawler graduation, live crawler operation, full mode,
    OpenSearch, managed infrastructure

Primary guides:
  docs/POST_MVP_HARDENING.md
  docs/OPTIONAL_EVIDENCE_GRADUATION.md

Decision ladder:
  1. confirm this evidence does not expand just mvp-acceptance
  2. confirm the packet is reproducible
  3. keep informational notes as optional evidence only
  4. open a follow-up for one reproducible improvement
  5. prepare crawler graduation only when the packet is complete and quiet
  6. request explicit review before changing public profile, API shape,
     crawler maturity, full-mode/OpenSearch production role, managed infra,
     or final-ranking owner

Crawler graduation packet:
  manifest fields: source_id, source_maturity, parser_key, expected_shape,
    targets, default school_id, allowlist domains
  policy evidence: terms URL, robots URL, review date, live_fetch_enabled
  commands:
    cargo run -p crawler -- doctor --manifest <manifest>
    cargo run -p crawler -- dry-run --manifest <manifest>
    cargo run -p crawler -- health --manifest <manifest>
  required shape:
    promotion_gate ready, or accepted review notes with owner/recheck date
    no robots, policy, parser, missing-school, or recent-run blockers
    rollback path to parser_only or policy_blocked plus event-csv repair

Full-mode automation candidate packet:
  compare the same request payloads in sql_only and full mode
  capture projection sync state and OpenSearch index health
  command:
    cargo test -p compatibility-tests --test sql_only_vs_full
  required shape:
    SQL-only remains the public MVP baseline
    OpenSearch remains candidate retrieval only
    automation reduces repeated operator work or catches a specific regression
    any required full-mode/OpenSearch production role goes to explicit review

Managed infrastructure:
  explicit review only
  do not add hosting, managed databases, managed cache, managed OpenSearch,
  production IaC, or cloud resources to the fixed gate from a hardening PR

Local validation and evidence when files change:
  cargo fmt --all --check
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  cargo test --workspace
  just mvp-acceptance
  DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
  git diff --check

If just is unavailable:
  ./scripts/mvp_acceptance.sh
  DATA_QUALITY_FAIL_ON_WARNING=true ./scripts/data_quality_doctor.sh

This script is read-only. It does not run validation or change local services.
EOF
