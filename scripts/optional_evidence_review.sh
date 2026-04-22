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
[optional-evidence-review] intake, triage, recheck audit, closeout ledger, and stale hygiene checklist

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
  docs/OPTIONAL_EVIDENCE_TRIAGE.md
  docs/OPTIONAL_EVIDENCE_RECHECK_AUDIT.md
  docs/OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md
  docs/POST_MVP_HARDENING.md
  docs/OPTIONAL_EVIDENCE_GRADUATION.md
  docs/OPTIONAL_EVIDENCE_PACKETS.md

GitHub templates:
  .github/ISSUE_TEMPLATE/optional_evidence.md
  .github/pull_request_template.md
  templates are records, not acceptance tests or new gates

Intake workflow:
  1. capture the evidence source and confirm it is reproducible enough to route
  2. confirm this evidence does not expand just mvp-acceptance
  3. choose one evidence type and one packet or review record
  4. paste the minimal intake header into the issue, PR, or review note
  5. choose one decision lane before implementation starts
  6. record owner, recheck date, public API shape status, and rollback path

Triage after an issue, PR, or review note exists:
  guide: docs/OPTIONAL_EVIDENCE_TRIAGE.md
  1. confirm the minimal intake header is present or linked
  2. confirm the fixed public MVP boundary is unchanged
  3. choose one primary source label and one primary decision lane
  4. record owner, recheck date, recheck command, and close condition
  5. use needs-recheck only when the record must be revisited later
  6. record the closeout ledger decision history after stale hygiene
  7. close only when the lane-specific close condition is satisfied

Recheck audit and stale hygiene:
  guide: docs/OPTIONAL_EVIDENCE_RECHECK_AUDIT.md
  use when a recheck date has arrived, needs-recheck is present, or an open
    optional evidence record has no clear next action
  stale recheck means:
    an open record whose recheck date arrived, whose needs-recheck marker has
      no current result, or whose next action is unclear after intake
  stale recheck classes:
    recheck:on-time
    recheck:overdue
    recheck:blocked
    recheck:split-needed
    recheck:closed
  stale hygiene decisions:
    close: evidence is recorded, no action remains, and the lane close
      condition is met
    keep-open: still waiting for a named owner plus a dated evidence source or
      external decision
    split: mixed evidence, lanes, owners, commands, or close conditions
    follow-up: scoped issue or PR needed without public-MVP profile expansion
    explicit-review: boundary, API shape, managed infrastructure,
      full-mode/OpenSearch production role, crawler maturity outside the
      graduation lane, or final-ranking ownership risk
  overdue records must name:
    owner
    next action
    close condition
  optional GitHub read-only inventory when labels are available:
    gh issue list --state open --label optional-evidence --json number,title,labels,assignees,updatedAt,url
    gh pr list --state open --label optional-evidence --json number,title,labels,assignees,updatedAt,url
  if labels are unavailable:
    search issue, PR, or review-note bodies for optional evidence,
      needs-recheck, recheck date, and lane names

Closeout ledger and decision history:
  guide: docs/OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md
  use after stale hygiene chooses close, keep-open, split, follow-up, or
    explicit-review
  required closeout fields:
    closeout date
    owner
    original evidence source
    stale class
    stale hygiene decision
    final lane
    result summary
    fixed public MVP boundary unchanged
    public API shape unchanged
    labels or written label equivalents as record aids only
    linked split, follow-up, or explicit review
    next recheck date or reason none is needed
    primary closeout status
    repeat or escalation marker
  primary closeout status aids, not gates:
    closeout:closed
    closeout:kept-open
    closeout:split
    closeout:follow-up-opened
    closeout:explicit-review-linked
  repeat or escalation aids, record alongside the primary closeout status:
    closeout:repeated-stale
  link requirements:
    split: link every split record before closing the original
    follow-up: link the issue or PR with one root cause, owner, validation plan,
      and recheck command
    explicit-review: link the review record with decision authority, rollback,
      cost or risk owner when relevant, and decision status
  repeated stale or repeated keep-open:
    route to explicit-review for boundary, API, infra, public profile,
      full-mode/OpenSearch production role, crawler maturity outside the
      graduation lane, or final-ranking ownership risk
    route to split when mixed evidence, lanes, owners, commands, or close
      conditions are blocking progress
    route to follow-up when scoped work can answer the question without
      changing the public MVP profile
    allow one final keep-open only for a dated external event with owner,
      narrow evidence source, and reason no split, follow-up, or explicit
      review is needed yet

Suggested label aids, not gates:
  optional-evidence
  lane:follow-up
  lane:crawler-graduation
  lane:explicit-review
  lane:optional-only
  source:doctor
  source:crawler
  source:full-mode
  source:managed-infra
  needs-recheck

Decision lane close conditions:
  optional evidence only:
    close when the evidence source, fixed boundary check, and reason for no
    implementation are recorded, with no recheck pending
  follow-up:
    close when the linked issue or PR has one root cause, one recheck command,
    and no public-MVP profile expansion
  crawler graduation:
    close when the packet is complete, source policy and robots are current,
    blockers are clear or accepted, rollback is recorded, and live crawler
    operation stays outside the fixed gate
  explicit review required:
    close when the decision authority records approved, deferred, or rejected;
    move any approved implementation into a separate issue or PR

Packet selection:
  strict data-quality doctor review item:
    use docs/POST_MVP_HARDENING.md doctor review item unless it points to
    crawler, full-mode, OpenSearch, or managed infrastructure evidence
    review_items are evidence and still require human classification
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
  minimal GitHub issue record:
    .github/ISSUE_TEMPLATE/optional_evidence.md
  PR fixed-boundary checks:
    .github/pull_request_template.md
  triage, labels, recheck, and close:
    docs/OPTIONAL_EVIDENCE_TRIAGE.md
  recheck audit and stale hygiene:
    docs/OPTIONAL_EVIDENCE_RECHECK_AUDIT.md
  closeout ledger and decision history:
    docs/OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md
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

Recheck commands:
  doctor:
    DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
  crawler:
    cargo run -p crawler -- doctor --manifest <manifest>
    cargo run -p crawler -- dry-run --manifest <manifest>
    cargo run -p crawler -- health --manifest <manifest>
  full-mode comparison:
    cargo test -p compatibility-tests --test sql_only_vs_full
  full-mode or OpenSearch optional evidence only:
    docker compose -f .docker/docker-compose.full.yaml up -d postgres redis opensearch
    cargo run -p cli -- index rebuild
  fixed boundary:
    just mvp-acceptance
  managed infrastructure:
    inspect the explicit review record; do not provision from triage

Minimal intake header:
  docs/OPTIONAL_EVIDENCE_INTAKE.md#minimal-intake-header
  fields: evidence type, source, packet used, decision lane, owner, recheck,
    fixed public MVP boundary, public API shape, strict doctor evidence

Recheck result template:
  docs/OPTIONAL_EVIDENCE_TRIAGE.md#recheck-result-template
  fields: date, owner, labels, lane, recheck command or source, result,
    fixed boundary, public API shape, next recheck date, issue or PR

Recheck audit templates:
  docs/OPTIONAL_EVIDENCE_RECHECK_AUDIT.md#audit-note-template
  docs/OPTIONAL_EVIDENCE_RECHECK_AUDIT.md#closeout-comment-template
  fields: stale class, stale hygiene decision, owner, overdue date,
    recheck command or source, next action, close condition, fixed boundary,
    public API shape, linked follow-up, split record, or explicit review

Closeout ledger template:
  docs/OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md#closeout-record-template
  fields: closeout date, owner, original evidence source, stale class,
    stale hygiene decision, primary closeout status, repeat or escalation
    marker, final lane, result summary, fixed boundary, public API shape,
    label status as record aid only, linked split, follow-up, or explicit
    review, next recheck date or reason none is needed

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
