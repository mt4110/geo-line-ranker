# MVP Acceptance

This document defines the fixed public-MVP launch gate for this repository.

## Decision Rule

The public MVP is ready for release only when this gate passes on the release candidate branch and in CI.

The gate is intentionally stable for the April 30, 2026 launch window. Prefer
tightening implementation and operations around these checks over adding
broader product surface to the gate.

## Scope

Public MVP acceptance is intentionally narrower than the full repository smoke surface.

- Canonical runtime mode: `sql_only`
- Canonical operational input: `event-csv`
- Included components: PostgreSQL/PostGIS, Redis, CLI, worker, API
- Excluded from the public-MVP release gate: live crawler, `full` mode, OpenSearch-backed retrieval

Why the exclusion is deliberate:

- some crawl sources still require manual review before production use
- `source_maturity` is not uniformly `live_ready`
- the launch gate should rely on the deterministic path we can operate today without policy ambiguity

For the surrounding operator guidance, see [OPERATIONS.md](OPERATIONS.md) and
[DATA_LICENSES.md](DATA_LICENSES.md).

Non-negotiable constraints:

- PostgreSQL/PostGIS is the reference write path.
- SQLite remains a read-only artifact/export target only.
- Redis remains cache only.
- OpenSearch remains optional full-mode candidate retrieval.
- Crawling remains optional and must not be required for the public-MVP path.
- Data freshness wording must use "latest available MLIT N02 snapshot" where rail/station source freshness is discussed.

## Fixed Cases

Public MVP acceptance is fixed to these six cases.

1. Bootstrap readiness
   Minimal services start, PostgreSQL leaves recovery mode, `migrate`, `seed example`, and `snapshot refresh` complete, and `GET /readyz` reports PostgreSQL as reachable with OpenSearch disabled.
2. Placement behavior
   `POST /v1/recommendations` returns `200` for both `home` and `search`, keeps the same `profile_version`, and produces different ordered items for the same target station.
3. Tracking to worker pipeline
   `POST /v1/track` accepts one school-affecting event and one `search_execute` event, the worker drains the queued jobs, and popularity/user-affinity snapshots update.
4. Snapshot replay
   `cargo run -p cli -- snapshot refresh` is rerunnable against the seeded MVP state and leaves snapshot tables populated.
5. Event CSV import audit
   `cargo run -p cli -- import event-csv --file ...` stages the raw file, records audit rows, and activates the imported event set.
6. Event CSV replacement semantics
   Re-importing the same logical `event-csv` source updates surviving rows and marks missing rows `is_active = false`.

## Failure Policy

Any failed case is a release blocker. Fix the cause and rerun the full gate; do not waive individual cases for launch.

The runner tears down its Docker Compose project at exit, uses temporary host ports, and prints API/worker log tails on failure so the next action is visible without digging through stale local state.

## Launch Checklist

Before marking the public MVP ready:

- `cargo fmt --all --check` passes
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes
- `cargo test --workspace` passes
- `just mvp-acceptance` passes
- `DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor` evidence is captured and classified
- `git diff --check` passes
- CI is green for the release candidate branch
- release notes describe `sql_only` as the public-MVP baseline
- optional crawler and full-mode notes remain clearly outside the public-MVP gate
- any public API change includes `schemas/openapi.json` and `API_SPEC.md` updates in the same change

`just data-quality-doctor` is required release evidence capture. Run it with
`DATA_QUALITY_FAIL_ON_WARNING=true` for release readiness so warnings fail the
evidence step. It does not change the six fixed cases in this document, and
review items become blockers only when they affect the fixed `sql_only` +
`event-csv` public-MVP behavior or hide whether this gate is meaningful.

## One-Shot Run

Run the full public-MVP acceptance gate from the repository root:

```bash
just mvp-acceptance
```

If `just` is not installed, run the script directly:

```bash
./scripts/mvp_acceptance.sh
```

The runner:

- uses `.env.example` as the public-MVP baseline
- starts only the minimal PostgreSQL/Redis stack
- waits until PostgreSQL is queryable and no longer in recovery
- forces `CANDIDATE_RETRIEVAL_MODE=sql_only`
- boots the local worker and API from the current workspace
- executes the six fixed cases above
- exits successfully only after printing `public MVP acceptance passed`

## Manual Bootstrap

If you want the same MVP baseline without the full acceptance assertions:

```bash
just mvp-up
just mvp-bootstrap
```

Without `just`, the equivalent commands are:

```bash
docker compose -f .docker/docker-compose.yaml up -d postgres redis
./scripts/wait_for_postgres.sh
cargo run -p cli -- migrate
cargo run -p cli -- seed example
cargo run -p cli -- snapshot refresh
```

`mvp-bootstrap` intentionally keeps the baseline narrow:

- `cargo run -p cli -- migrate`
- `cargo run -p cli -- seed example`
- `cargo run -p cli -- snapshot refresh`

Operational `event-csv` import remains a separate acceptance case so replacement semantics stay visible.
