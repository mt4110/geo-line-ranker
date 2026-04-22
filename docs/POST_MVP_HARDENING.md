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

Use one chain for post-MVP hardening evidence, Phase 14 evidence review, and
Phase 15 optional evidence graduation:

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
5. Use [OPTIONAL_EVIDENCE_GRADUATION.md](OPTIONAL_EVIDENCE_GRADUATION.md)
   when crawler graduation, full-mode automation, or managed infrastructure
   evidence needs a graduation, follow-up, or explicit-review decision.
6. Record the final decision in the review shape below before closing the
   hardening review, issue, or PR.

Strict data-quality doctor output remains required release and post-MVP
evidence:

```bash
DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
```

Warnings or command failures block the evidence pass. Doctor `review_items`
are not automatic failures; classify every item before opening issue or PR
work.

## Evidence Review Loop

Use the same loop for doctor output, post-launch findings, release-candidate
residual risks, and optional crawler or full-mode evidence:

1. Capture the command output, SQL sample, response body, import checksum, or
   operator note that triggered the review.
2. Classify the finding into exactly one primary decision lane.
3. Route it to the smallest next action: no action, accepted risk record,
   issue, PR, or explicit review.
4. Verify with the same command or query that exposed the finding, plus the
   local validation set when code, fixtures, docs, or config changed.
5. Record the decision, owner, and recheck date.

### Decision Lanes

| Lane | Use when | Required action |
| --- | --- | --- |
| Blocker | Required validation fails, strict data-quality warnings are present, public-MVP `sql_only` + `event-csv` behavior is broken, or the finding hides whether `just mvp-acceptance` is meaningful. | Fix before shipping, closing the incident, or accepting the hardening review. Rerun the failed check and the fixed gate. |
| Accepted risk | The behavior is visible, bounded, understood, and safe to carry temporarily without changing the public MVP profile. | Record owner, reason, expiry or recheck date, and the evidence proving the risk is bounded. |
| Follow-up | The finding is actionable but does not block the current public-MVP operating profile. | Open a scoped issue or PR with one invariant, one root cause, and the recheck command. |
| Optional evidence only | Crawler graduation or full-mode comparison evidence does not affect the fixed gate and does not justify a product or operator change yet. | Keep the note in the review record and label it outside the public MVP gate. |
| Explicit review required | The next step would change the public MVP profile, public API shape, crawler maturity, full-mode/OpenSearch role, managed infrastructure, or final-ranking ownership. | Stop implementation work until the review decision is recorded. Public API changes must update `schemas/openapi.json` and `API_SPEC.md` in the same change. |

### Review Item Template

Use this shape for every nonzero doctor `review_items` summary:

```text
Doctor review item:
- Source:
- Evidence command:
- Evidence excerpt or SQL row:
- Affected public-MVP boundary:
- Classification: blocker / accepted risk / follow-up / optional evidence only / explicit review required
- Reason:
- Next action:
- Owner:
- Recheck date:
- Issue or PR:
```

If the classification is blocker, keep the issue or PR focused on restoring the
fixed gate. If it is accepted risk, the owner and recheck date are required. If
it is optional evidence only, do not add it to `just mvp-acceptance`.

## Routing Criteria

Open an issue when the evidence shows one reproducible invariant violation or
operator pain point, but the root cause or fix is not ready. Include the exact
command, SQL row, request payload, response body, import run id, staged path, or
checksum that makes the finding reproducible.

Open a PR when the root cause and verification path are clear. Keep the PR to
one root cause, and include the recheck command plus regression evidence when
the change touches operator feedback, imports, snapshots, jobs, or data quality
guardrails.

Request explicit review before changing any of these:

- public MVP profile or fixed gate membership
- public API shape
- crawler manifest graduation to `source_maturity: live_ready`
- `full` mode, OpenSearch, or projection sync as required production behavior
- managed production infrastructure
- ML, embeddings, vector search, or frontend final-ranking logic

Keep the result as optional evidence only when it is informative but does not
change the fixed gate, expose a reproducible invariant violation, or justify an
operator/product decision.

## Command Plan

Print the post-MVP hardening command plan from the repository root:

```bash
just post-mvp-hardening
```

Without `just`:

```bash
./scripts/post_mvp_hardening.sh
```

Print the optional evidence graduation checklist when a crawler, full-mode, or
managed infrastructure packet needs routing:

```bash
just optional-evidence-review
```

Without `just`:

```bash
./scripts/optional_evidence_review.sh
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
outside the public MVP gate. Use
[OPTIONAL_EVIDENCE_GRADUATION.md](OPTIONAL_EVIDENCE_GRADUATION.md) for the
Phase 15 decision ladder and evidence packet details.

At this level, route each optional packet to one of four outcomes:

- Optional evidence only: informative, not actionable yet, and outside the
  public MVP gate.
- Follow-up: one reproducible improvement that does not change the public MVP
  profile.
- Crawler graduation: source-specific evidence is complete and promotion has
  no blockers, with rollback recorded.
- Explicit review required: the next step would change public profile, public
  API shape, crawler maturity, full-mode/OpenSearch production role, managed
  infrastructure, or final-ranking ownership.

Crawler graduation evidence must include policy review, manifest fields,
`crawler doctor`, `crawler dry-run`, `crawler health`, fixture or staged
checksum context when relevant, and a rollback path to `parser_only` or
`policy_blocked` plus `event-csv` repair.

Full-mode automation candidate evidence must include SQL-only and full-mode
samples for the same inputs, projection sync state, OpenSearch health,
compatibility test output when applicable, and a reason manual comparison is no
longer enough.

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
- Optional evidence only:
- Explicit review required:

Review items:
- Source:
  Evidence:
  Classification:
  Reason:
  Next action:
  Owner:
  Recheck date:
  Issue or PR:

Optional evidence outside gate:
- Crawler graduation:
- Full-mode evaluation:
- Managed infrastructure review:

Decision:
- Ship / hold / follow-up only:
- Owner:
- Recheck date:
```
