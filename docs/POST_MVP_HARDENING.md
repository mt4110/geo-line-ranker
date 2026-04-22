# Post-MVP Hardening

Use this guide after the public MVP release gate is healthy and the work shifts
from release readiness to continuous operation. It connects the release
readiness evidence bundle to post-launch triage, data-quality review, and
follow-up planning without widening the fixed public MVP gate.

## Fixed Boundary

The public MVP operating gate stays fixed to:

- Candidate retrieval: `sql_only`
- Operational content path: `event-csv`
- Write store: PostgreSQL/PostGIS
- Cache: Redis
- Acceptance gate: [MVP_ACCEPTANCE.md](MVP_ACCEPTANCE.md)

Keep these outside the fixed gate unless an explicit review changes the public
MVP profile:

- live crawler operation
- crawler source graduation
- `full` mode and OpenSearch-backed retrieval
- managed infrastructure
- ML, embeddings, vector search, or frontend final-ranking logic

Rail and station freshness claims must say "latest available MLIT N02
snapshot".

## Evidence Flow

Use one chain for post-MVP hardening evidence:

1. Confirm the release baseline with
   [PUBLIC_MVP_RELEASE_READINESS.md](PUBLIC_MVP_RELEASE_READINESS.md).
2. Capture first-response state with
   [POST_LAUNCH_RUNBOOK.md](POST_LAUNCH_RUNBOOK.md).
3. Classify findings with
   [OPERATOR_FEEDBACK_LOOP.md](OPERATOR_FEEDBACK_LOOP.md).
4. Attach regression evidence with
   [PHASE11_REGRESSION_EVIDENCE.md](PHASE11_REGRESSION_EVIDENCE.md) when a
   follow-up PR changes operator feedback, imports, snapshots, jobs, or data
   quality guardrails.

Strict data-quality doctor output remains required release and post-MVP
evidence:

```bash
DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
```

Warnings or command failures block the evidence pass. Doctor `review_items`
are not automatic failures; classify them as:

- Blocker: affects the fixed `sql_only` + `event-csv` public-MVP behavior or
  hides whether `just mvp-acceptance` is meaningful.
- Accepted risk: visible, bounded, and safe to carry with an owner and review
  date.
- Follow-up: actionable but not blocking the current public-MVP operating
  profile.

## Command Plan

Print the post-MVP hardening command plan from the repository root:

```bash
just post-mvp-hardening
```

Without `just`:

```bash
./scripts/post_mvp_hardening.sh
```

Then run the local validation and evidence commands when preparing a follow-up
PR, a hardening review, or a release train:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
just mvp-acceptance
DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
git diff --check
```

If `just` is unavailable:

```bash
./scripts/mvp_acceptance.sh
DATA_QUALITY_FAIL_ON_WARNING=true ./scripts/data_quality_doctor.sh
```

## Optional Evidence Review

Optional evidence is useful for deciding what to improve next, but it must stay
outside the public MVP gate.

Crawler graduation evidence should include:

- source policy and robots or terms review
- manifest `source_maturity` and expected parser shape
- `crawler doctor`, `crawler dry-run`, and `crawler health` output
- latest staged checksum or crawl run ids when relevant
- rollback path to `event-csv` if public content is affected
- decision: do not promote, promote with accepted risk, or promote after a
  scoped follow-up

Full-mode evaluation evidence should include:

- SQL-only response samples and full-mode response samples for the same inputs
- projection sync state and OpenSearch index health
- compatibility test output, when applicable
- operator notes explaining whether the comparison reveals a product need
- decision: keep manual comparison, add a follow-up, or request explicit review
  for a public profile change

Managed infrastructure evidence should stay in explicit review. Do not add
hosting, managed databases, managed cache, managed OpenSearch, or new cloud
production resources as fixed-gate requirements in a hardening PR.

## Review Record

Use this shape in a PR body, issue, or hardening review note:

```text
Post-MVP hardening review:
Commit SHA:
Date:
Operator:

Public MVP boundary:
- sql_only candidate retrieval
- event-csv operational content
- PostgreSQL/PostGIS write store
- Redis cache
- crawler graduation, full mode, OpenSearch, and managed infrastructure outside gate

Required validation:
- cargo fmt --all --check:
- cargo clippy --workspace --all-targets --all-features -- -D warnings:
- cargo test --workspace:
- just mvp-acceptance:
- DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor:
- git diff --check:

Doctor classification:
- Blocker:
- Accepted risk:
- Follow-up:

Optional evidence outside gate:
- Crawler graduation:
- Full-mode evaluation:
- Managed infrastructure review:

Decision:
- Ship / hold / follow-up only:
- Owner:
- Recheck date:
```
