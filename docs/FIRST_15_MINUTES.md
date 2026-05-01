# First 15 Minutes

This guide is the first-run map for a new contributor or operator. It explains
what to read, what to start, what success looks like, and where to look next
without changing the public API, ranking semantics, database schema, or
OpenAPI.

## Fixed Path

Keep the first run narrow:

- `sql_only` candidate retrieval
- `event-csv` operational content import
- PostgreSQL/PostGIS as the reference write store
- Redis as cache only
- committed default sample from `storage/fixtures/minimal/`

Do not add OpenSearch, `full` mode, live crawler operation, or managed
infrastructure to the first-run path or fixed public-MVP gate. Those paths are
useful later, but they need separate review evidence.

## Minute 0-3: Read The Map

Start with these docs in order:

1. [Project README](../README.md): project shape, shortest local path, and
   first success state.
2. This guide: the 15-minute reading and inspection path.
3. [Quickstart](QUICKSTART.md): the command-by-command local runbook.
4. [MVP Acceptance](MVP_ACCEPTANCE.md): the fixed six-case public-MVP gate.
5. [Optional Evidence Handoff](OPTIONAL_EVIDENCE_HANDOFF.md): review inventory
   for optional evidence.

The mental model is simple: README orients you, this guide tells you what to
notice first, Quickstart gets the system running, and MVP Acceptance defines the
fixed gate.

## Minute 3-8: Start The Baseline

Run the narrow local path from the repository root:

```bash
cp .env.example .env
docker compose -f .docker/docker-compose.yaml up -d postgres redis
cargo run -p cli -- migrate
cargo run -p cli -- seed example
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
```

Then start the runtime:

```bash
# terminal A
cargo run -p worker -- serve

# terminal B
cargo run -p api -- serve
```

The first Rust build may take longer than 15 minutes on a cold machine. That is
not a product failure. The first-run success state is still the same once the
build finishes.

## Minute 8-12: Inspect The Default Sample

The default sample is intentionally small enough to inspect by eye before you
trust the API response.

| File | What To Check |
|---|---|
| `storage/fixtures/minimal/fixture_manifest.yaml` | Manifest version, row counts, and checksums. When fixture rows change, update and verify this manifest. |
| `storage/fixtures/minimal/stations.csv` | 6 stations across JR Yamanote Line, Tokyo Metro Marunouchi Line, and Tokyo Metro Yurakucho Line. Use `st_tamachi` for the first station-first request. |
| `storage/fixtures/minimal/schools.csv` | 10 schools with area, prefecture, school type, and group ids. These rows are the local entities the ranking result can point at. |
| `storage/fixtures/minimal/school_station_links.csv` | Walking minutes, distance, hop distance, and line name. These links make station-first and line-first behavior visible. |
| `storage/fixtures/minimal/events.csv` | 5 seeded fixture events used by the local dataset before operational import. |
| `storage/fixtures/minimal/user_events.ndjson` | 2 initial behavior rows for snapshot and tracking checks. |
| `examples/import/events.sample.csv` | 4 operational `event-csv` rows. This tests import staging, audit rows, active event updates, and replacement semantics. It is not a crawler path. |

Useful quick checks:

```bash
sed -n '1,12p' storage/fixtures/minimal/stations.csv
sed -n '1,14p' storage/fixtures/minimal/schools.csv
sed -n '1,12p' storage/fixtures/minimal/school_station_links.csv
sed -n '1,10p' examples/import/events.sample.csv
cargo run -p cli -- fixtures doctor --path storage/fixtures/minimal
```

Look for three small paths:

- Station-first: `target_station_id = st_tamachi`
- Line-first: `JR Yamanote Line`
- Area-first: `Minato` or `Tokyo`

## Minute 12-15: Confirm First Success

Open Swagger UI:

[http://127.0.0.1:4000/swagger-ui](http://127.0.0.1:4000/swagger-ui)

Make one recommendation request:

```bash
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d '{"target_station_id":"st_tamachi","placement":"home","limit":3}'
```

Success means:

- HTTP `200`
- non-empty `items`
- item-level `content_kind`, `school_id`, `score`, and explanation fields
- top-level `fallback_stage`, `candidate_counts`, `context`,
  `profile_version`, and `algorithm_version`
- event items include `event_id` and `event_title`

After that, use [Quickstart](QUICKSTART.md) sections 8-10 to compare
placements, track one user event, and choose the next document.

## Fixed vs Optional

| Path | Use It For | Fixed Public-MVP Gate? |
|---|---|---|
| `sql_only` + `event-csv` + PostgreSQL/PostGIS + Redis | First run, fixed acceptance, release candidate baseline | Yes |
| `just mvp-acceptance` | Fixed six-case public-MVP gate | Yes |
| `DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor` | Strict release and post-MVP evidence capture | Evidence, not one of the six fixed cases |
| JP demo imports | Adapter and source parser development | No |
| allowlist crawler | Reviewed source experiments and parser work | No |
| `full` mode + OpenSearch | Optional candidate-retrieval evaluation | No |
| managed infrastructure | Explicitly reviewed production planning | No |
| `just optional-evidence-review` | Read-only handoff for optional evidence | No |

If an optional path finds a real release risk, route it into normal issue or PR
work. Do not silently add the optional path to the fixed gate.

## Touch Map

Use this small map before changing files:

| Change Type | Start Here | Also Check |
|---|---|---|
| First-run or contributor docs | `README.md`, `docs/README.md`, `docs/QUICKSTART.md`, this guide | Keep fixed vs optional wording aligned. |
| Default sample rows | `storage/fixtures/minimal/*`, `storage/fixtures/minimal/fixture_manifest.yaml` | Run fixture doctor; update docs if row counts or inspection anchors change. |
| Operational event CSV sample | `examples/import/events.sample.csv` | Keep `event-csv` audit and replacement semantics visible in Quickstart and MVP Acceptance. |
| Ranking scores, fallback, or reasons | `crates/ranking`, `configs/ranking`, `docs/REASON_CATALOG.md` | Add or adjust ranking tests and golden expectations. Do not change public response shape accidentally. |
| Public API fields | `apps/api`, `crates/api-contracts`, `crates/openapi`, `API_SPEC.md` | Update `schemas/openapi.json` and API docs in the same change. |
| Database write path or migrations | `crates/storage-postgres`, `storage/migrations/postgres` | Keep PostgreSQL/PostGIS as reference write store; update operations and tests. |
| Worker or tracking flow | `apps/worker`, `crates/worker-core`, `POST /v1/track` handling | Check queue recovery docs and snapshot/cache behavior. |
| Profile packs or fixture ownership | `configs/profiles`, `docs/PROFILE_PACKS.md`, `examples/*/README.md` | Keep `local-discovery-generic` and `school-event-jp` boundaries clear. |
| Crawler manifests or parsers | `apps/crawler`, `configs/crawler`, `storage/fixtures/crawler` | Treat as optional evidence unless explicitly reviewed for graduation. |
| OpenSearch or `full` mode | projection/index code and operations docs | Keep it outside the fixed gate unless the boundary changes through explicit review. |

When in doubt, make the smallest change that preserves deterministic output for
the same input, config, and data.
