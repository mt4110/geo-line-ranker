# MVP Acceptance

This document defines the fixed public-MVP acceptance gate for this repository.

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

For the surrounding operator guidance, see [OPERATIONS.md](OPERATIONS.md) and [DATA_LICENSES.md](DATA_LICENSES.md).

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
- boots the local worker and API from the current workspace
- executes the six fixed cases above

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
