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
[optional-evidence-review] intake workflow checklist

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
  docs/OPTIONAL_EVIDENCE_INTAKE.md
  docs/POST_MVP_HARDENING.md
  docs/OPTIONAL_EVIDENCE_GRADUATION.md
  docs/OPTIONAL_EVIDENCE_PACKETS.md

Intake workflow:
  1. capture the evidence source and confirm it is reproducible enough to route
  2. confirm this evidence does not expand just mvp-acceptance
  3. choose one evidence type and one packet or review record
  4. paste the minimal intake header into the issue, PR, or review note
  5. choose one decision lane before implementation starts
  6. record owner, recheck date, public API shape status, and rollback path

Packet selection:
  strict data-quality doctor review item:
    use docs/POST_MVP_HARDENING.md doctor review item unless it points to
    crawler, full-mode, OpenSearch, or managed infrastructure evidence
  crawler source, policy, robots, parser, dry-run, health, or maturity:
    docs/OPTIONAL_EVIDENCE_PACKETS.md#crawler-graduation-packet
  SQL-only/full-mode comparison, projection sync, or OpenSearch health:
    docs/OPTIONAL_EVIDENCE_PACKETS.md#full-mode-automation-candidate-packet
  hosting, managed services, networking, secrets, observability, backup,
  cost, or production IaC:
    docs/OPTIONAL_EVIDENCE_PACKETS.md#managed-infrastructure-explicit-review-packet

Decision ladder:
  1. confirm this evidence does not expand just mvp-acceptance
  2. confirm the packet is reproducible
  3. keep informational notes as optional evidence only
  4. open a follow-up for one reproducible improvement
  5. prepare crawler graduation only when the packet is complete and quiet
  6. request explicit review before changing public profile, API shape,
     crawler maturity outside the crawler graduation lane, full-mode/OpenSearch
     production role, managed infra, or final-ranking owner

Packet templates to paste into issues, PRs, or review notes:
  crawler graduation:
    docs/OPTIONAL_EVIDENCE_PACKETS.md#crawler-graduation-packet
  full-mode automation candidate:
    docs/OPTIONAL_EVIDENCE_PACKETS.md#full-mode-automation-candidate-packet
  managed infrastructure explicit review:
    docs/OPTIONAL_EVIDENCE_PACKETS.md#managed-infrastructure-explicit-review-packet

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
  capture the automation reason and SQL-only rollback path
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
  use the managed infrastructure explicit review packet for hosting, managed
    DB, cache, OpenSearch, IaC, cost, and rollback evidence

Minimal intake header:
  docs/OPTIONAL_EVIDENCE_INTAKE.md#minimal-intake-header
  fields: evidence type, source, packet used, decision lane, owner, recheck,
    fixed public MVP boundary, public API shape, strict doctor evidence

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
