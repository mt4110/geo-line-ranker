# Operator Feedback Loop

This guide turns a post-launch finding into reproducible improvement work. It
starts after the first read-only triage pass in [POST_LAUNCH_RUNBOOK.md](POST_LAUNCH_RUNBOOK.md).
During release candidate validation, the same findings should be summarized in
[PUBLIC_MVP_RELEASE_READINESS.md](PUBLIC_MVP_RELEASE_READINESS.md) evidence
before deciding whether they block the public MVP. After release, summarize the
same classification in [POST_MVP_HARDENING.md](POST_MVP_HARDENING.md) evidence
so recurring reviews can separate blockers, accepted risks, follow-ups,
optional evidence only, and explicit review decisions.

## Public MVP Boundary

Keep incident recovery inside the public MVP unless an explicit review changes
the operating profile.

- Candidate retrieval stays `sql_only`.
- Operational content repair stays on `event-csv`.
- PostgreSQL/PostGIS remains the reference write store.
- Redis remains cache only.
- OpenSearch full-mode comparison and crawler graduation remain optional
  evaluation paths, not MVP gates.
- Rail and station freshness claims should say "latest available MLIT N02
  snapshot".

Do not make live crawling, OpenSearch, managed infrastructure, ML, embeddings,
or vector search required in order to close a public-MVP incident.

## Feedback Bundle

Capture one bundle before changing data, config, or code:

- incident timestamp, environment name, branch or commit SHA, and operator name
- sanitized `.env` keys, with secrets removed
- `./scripts/post_launch_doctor.sh` output
- `./scripts/data_quality_doctor.sh` output when PostgreSQL is reachable
- `/readyz` response
- API and worker log tails for the incident window
- latest `event-csv` import run id, status, staged path, and checksum
- relevant `import_reports` rows, especially warning or deactivation rows
- queue pressure by `job_type` and `status`
- relevant `job_queue.id` and `job_attempts` rows
- snapshot counts and latest `refreshed_at` values
- affected request payloads and response bodies, with user identifiers removed
- crawler run ids only when the incident involves optional crawler output
- full-mode comparison output only when SQL-only health is already established

The goal is to replace "something looks wrong" with "this SQL row, this
snapshot timestamp, this job, this staged CSV, and this response changed."

## Classify First

Use the bundle to pick one primary lane before opening follow-up work:

| Lane | Evidence to inspect | Typical next action |
| --- | --- | --- |
| Readiness | `/readyz`, database reachability, Redis ping | Fix environment or dependency health, then rerun the same read-only checks. |
| Event CSV input | latest `import_runs`, `import_run_files`, `import_reports`, staged checksum | Re-import the last known complete CSV or fix importer validation. |
| Data quality | active event coverage, station links, source identity, snapshot staleness | Open a scoped data repair or importer bug with the exact SQL evidence. |
| Worker and cache | `job_queue`, `job_attempts`, Redis cache sample, worker logs | Fix dependency, retry or make due through CLI commands, then rerun worker drain. |
| Ranking behavior | request payload, response body, snapshot timestamps, ranking config version | Add or adjust deterministic ranking tests before changing ranking logic. |
| Optional crawler | `crawler doctor`, `crawler health`, crawl audit rows | Keep findings out of the MVP gate; repair through event-csv if public content is affected. |
| Optional full mode | SQL-only response, full-mode response, projection sync state | Treat as comparison evidence unless the public profile is explicitly changed. |

If more than one lane appears involved, open separate issues unless the same
root cause clearly explains all of them.

For recurring post-MVP review, map the lane to the decision vocabulary in
[POST_MVP_HARDENING.md](POST_MVP_HARDENING.md): blocker, accepted risk,
follow-up, optional evidence only, or explicit review required.

## Issue and PR Granularity

Prefer small follow-up units:

- One issue should describe one invariant, one observed violation, and the
  exact read-only query or command that revealed it.
- One PR should fix one root cause and include the regression evidence needed to
  show the invariant is now stable.
- Do not combine public-MVP recovery with crawler graduation or full-mode
  automation in the same PR.
- Do not manually edit production tables as the first fix. Prefer replay,
  importer fixes, snapshot refresh, or job retry paths that preserve audit
  history.
- Public API changes must update `schemas/openapi.json` and `API_SPEC.md` in
  the same PR.

Good issue shape:

```text
Invariant: every public-MVP school used in ranking has at least one station link.
Observed: school_id=... has link_count=0 in data-quality doctor output.
Evidence: doctor timestamp, SQL sample row, commit SHA, fixture/import source.
Next action: fix derivation/import source, rerun data-quality doctor and MVP gate.
```

## Data Quality Recheck

Run the read-only guardrail pass when PostgreSQL is available:

```bash
just data-quality-doctor
```

Without `just`:

```bash
./scripts/data_quality_doctor.sh
```

Review items are not destructive and are not automatically public-MVP blockers.
Treat them as prompts for human classification. A review item becomes a blocker
when it affects the fixed `sql_only` + `event-csv` public-MVP behavior or hides
whether `just mvp-acceptance` is meaningful.

Classify every nonzero `review_items` summary as one of:

- Blocker: must be fixed before shipping or before closing the incident.
- Accepted risk: safe to carry temporarily with an owner and review date.
- Follow-up: actionable, but not blocking the current public-MVP operating
  profile.
- Optional evidence only: informative crawler or full-mode evidence that does
  not change the fixed gate or require implementation.
- Explicit review required: would change the public MVP profile, public API
  shape, crawler maturity, full-mode/OpenSearch role, managed infrastructure,
  or final-ranking ownership.

Use the review item template in
[POST_MVP_HARDENING.md](POST_MVP_HARDENING.md) when a `review_items` summary
needs a durable owner, recheck date, issue, PR, or explicit review record.

## Closing the Loop

Before closing an incident or follow-up PR, capture:

- the command or SQL evidence that failed before
- the code, config, fixture, or documented procedure changed
- the exact recheck commands and their results
- any remaining optional crawler or full-mode notes, labeled as outside the MVP
  gate

Use [PHASE11_REGRESSION_EVIDENCE.md](PHASE11_REGRESSION_EVIDENCE.md) for the PR
evidence checklist.

When the finding came from a release candidate, also update the release
readiness evidence with the final classification: blocker fixed, accepted
residual risk, follow-up, optional crawler/full-mode note outside the MVP gate,
or explicit review required.
