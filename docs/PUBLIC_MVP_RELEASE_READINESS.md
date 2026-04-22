# Public MVP Release Readiness

Use this guide when preparing, cutting, or validating a release candidate for
the April 30, 2026 public MVP. It keeps the release decision tied to the same
small operating profile every time.

## Release Boundary

The public MVP release gate is fixed to:

- Candidate retrieval: `sql_only`
- Operational content path: `event-csv`
- Write store: PostgreSQL/PostGIS
- Cache: Redis
- Required binaries: CLI, worker, API
- Acceptance gate: [MVP_ACCEPTANCE.md](MVP_ACCEPTANCE.md)

Keep these outside the release gate unless the public MVP profile changes
through explicit review:

- live crawler operation
- `full` mode and OpenSearch-backed candidate retrieval
- managed infrastructure
- ML, embeddings, vector search, or frontend final-ranking logic

Data freshness claims for rail and station sources must say "latest available
MLIT N02 snapshot".

## Decision Model

Release blockers:

- any failure in `cargo fmt --all --check`
- any failure in `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- any failure in `cargo test --workspace`
- any failure in `just mvp-acceptance`
- any failed or warning-emitting `DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor` evidence pass
- a red CI job for the release candidate branch
- a public API change without matching `schemas/openapi.json` and `API_SPEC.md`
- release notes that make live crawler, full mode, OpenSearch, or managed
  infrastructure sound required for the MVP gate

Required release evidence:

- `DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor` output
- `GET /readyz` response from the candidate environment when available
- latest `event-csv` import run id, staged path, and checksum when validating
  operator content
- residual risks and their release decision

The data quality doctor is required evidence capture, not a seventh MVP
acceptance case. Run it with `DATA_QUALITY_FAIL_ON_WARNING=true` for release
readiness so doctor warnings or command failures block the candidate. Doctor
review items become blockers only when they affect the fixed `sql_only` +
`event-csv` behavior or hide whether `just mvp-acceptance` is meaningful.

Optional evidence:

- SQL-only vs full-mode comparison
- crawler doctor, dry-run, health, or promotion evidence

Optional evidence must be labeled outside the public MVP gate.

## Command Plan

Print the release readiness command plan from the repository root:

```bash
just release-readiness
```

Without `just`:

```bash
./scripts/release_readiness.sh
```

Then run the required local validation commands:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
just mvp-acceptance
git diff --check
```

Then capture the required release evidence:

```bash
DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
```

If `just` is not installed, use:

```bash
./scripts/mvp_acceptance.sh
DATA_QUALITY_FAIL_ON_WARNING=true ./scripts/data_quality_doctor.sh
```

## Before Cutting a Release Candidate

1. Start from latest `main`.
2. Confirm the candidate branch or tag points at the intended commit.
3. Confirm no public API change is included, or update `schemas/openapi.json`
   and `API_SPEC.md` in the same change.
4. Run the command plan and required local validation.
5. Open the release candidate evidence bundle below.

## Evidence Bundle

Capture this in the PR body, release candidate issue, or release notes draft.

```text
Release candidate:
Commit SHA:
Date:
Operator:

Public MVP boundary:
- sql_only candidate retrieval
- event-csv operational content
- PostgreSQL/PostGIS write store
- Redis cache
- live crawler, full mode, OpenSearch, and managed infrastructure outside gate

Required local validation:
- cargo fmt --all --check:
- cargo clippy --workspace --all-targets --all-features -- -D warnings:
- cargo test --workspace:
- just mvp-acceptance:
- git diff --check:

CI evidence:
- rust-quality:
- rust-unit-tests:
- rust-postgres-tests:
- mvp-acceptance:
- data-quality-doctor:
- docs:
- spellcheck:

Data quality doctor:
- command:
- warnings:
- review item summary:
- classification:

Release notes baseline:
- Public MVP runs on sql_only candidate retrieval.
- Operational event updates use event-csv import with checksum staging.
- PostgreSQL/PostGIS is the reference write store.
- Redis is cache only.
- Rail/station freshness is the latest available MLIT N02 snapshot.

Residual risks:
- Risk:
  Evidence:
  Decision:
  Follow-up:

Optional evidence outside gate:
- Crawler:
- Full mode / OpenSearch:
```

## Cutting and Verifying

Use the fixed acceptance result as the release decision anchor. After the
candidate is pushed, compare CI with the local command list above. The names do
not have to be identical, but the intent should match:

- `rust-quality` covers formatting and clippy.
- `rust-unit-tests` plus `rust-postgres-tests` cover workspace tests across DB-free
  and PostgreSQL-backed packages.
- `mvp-acceptance` runs the six-case public MVP gate.
- `data-quality-doctor` runs the read-only guardrails in strict warning mode.
- `docs` verifies generated OpenAPI remains unchanged.
- `spellcheck` checks documentation spelling.

Do not waive a failed MVP acceptance case. Fix the cause, rerun the full gate,
and update the evidence bundle.

## Post-release Handoff

After the public MVP is released, keep the same boundary for first-response
operations:

- Use [POST_MVP_HARDENING.md](POST_MVP_HARDENING.md) to connect release
  evidence to recurring post-MVP review.
- Start with [POST_LAUNCH_RUNBOOK.md](POST_LAUNCH_RUNBOOK.md).
- Feed findings into [OPERATOR_FEEDBACK_LOOP.md](OPERATOR_FEEDBACK_LOOP.md).
- Use [PHASE11_REGRESSION_EVIDENCE.md](PHASE11_REGRESSION_EVIDENCE.md) for
  follow-up PR evidence when the fix touches operator feedback or data quality
  guardrails.

Crawler graduation, full-mode automation, and managed infrastructure remain
optional evidence or separate review decisions after the MVP release candidate
is healthy. They do not become public-MVP gate requirements through post-release
handoff.
