# Local Contributing Guide

This guide is the contributor runbook for the JP station conversion flow that task notes may call `station_converter_ja`.

There is no standalone `station_converter_ja/` directory in this repository today. In the current workspace, that work is spread across the JP connectors, CLI import commands, fixtures, source manifests, and PostgreSQL storage layer.

## Read This First

- Start with [`AGENTS.md`](../AGENTS.md).
- If this is your first local run, follow
  [First 15 Minutes](FIRST_15_MINUTES.md) before this longer contributor
  runbook.
- If you are deciding which document owns a topic, use the
  [Documentation Index](README.md) audience and task map.
- Keep changes small and reviewable.
- Do not change runtime behavior when the task is documentation-only.

## Contributor Lanes

| Change type | Start with | Then validate with |
|---|---|---|
| Docs-only routing or onboarding | [Documentation Index](README.md), [First 15 Minutes](FIRST_15_MINUTES.md) | `git diff --check`, docs link/self-review |
| Ranking behavior, reasons, or fallback | [Reason Catalog](REASON_CATALOG.md), [Architecture](ARCHITECTURE.md) | ranking tests, default validation |
| Public API shape | [API Spec](../API_SPEC.md), [Versioning](VERSIONING.md) | OpenAPI/API docs update, default validation |
| Config, profile, or manifest contract | [Versioning](VERSIONING.md), [Deprecation Policy](DEPRECATION_POLICY.md) | config lint, profile validate/list/inspect, source-manifest lint, crawler manifest lint, fixture doctor |
| TypeScript SDK or example frontend | [Testing](TESTING.md) | `just ts-sdk-check`, `just frontend-smoke`, `just openapi-drift` when API shape is involved |
| Profile pack or fixture ownership | [Profile Packs](PROFILE_PACKS.md) | `cargo run -p cli -- profile validate`, `cargo run -p cli -- config lint`, fixture doctor |
| Connector, source manifest, or crawler source | [Data Sources](DATA_SOURCES.md), [Data Licenses](DATA_LICENSES.md) | source-manifest lint, crawler manifest lint, fixture doctor |

## Repository Layout

- `apps/api`: HTTP endpoints and Swagger UI wiring.
- `apps/cli`: `migrate`, `seed`, JP import, fixture generation, and OpenAPI dump commands.
- `apps/worker`: background jobs.
- `apps/crawler`: allowlist crawl tooling.
- `crates/connectors/jp-rail`: JP rail/station parsing logic.
- `crates/connectors/jp-postal`: JP postal parsing logic.
- `crates/connectors/jp-school`: JP school code and geodata parsing logic.
- `crates/storage-postgres`: reference write-path persistence and import audit tables.
- `storage/fixtures/demo_jp`: committed JP fixture CSV files.
- `storage/sources/jp_rail`, `storage/sources/jp_postal`, `storage/sources/jp_school`, `storage/sources/jp_school_geo`: source manifests for local import verification.
- `storage/migrations/postgres`: PostgreSQL/PostGIS migrations.
- `schemas/openapi.json`: generated OpenAPI artifact.
- `packages/ts-sdk`: TypeScript SDK package.
- `examples/frontend-next`: small Next.js example frontend.
- `docs`: architecture, testing, operations, quickstart, and contributor docs.

## Exact Build, Lint, and Test Commands

Run these from the repository root:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo run -p cli -- config lint
cargo run -p cli -- source-manifest lint
cargo run -p cli -- fixtures doctor --path storage/fixtures/minimal
cargo run -p cli -- fixtures doctor --path storage/fixtures/demo_jp
cargo run -p crawler -- manifest lint
```

For profile-pack or profile CLI changes, also run:

```bash
cargo run -p cli -- profile list
cargo run -p cli -- profile validate
cargo run -p cli -- profile inspect --profile-id local-discovery-generic
```

Convenience commands are also available:

```bash
just fmt
just lint
just test
just smoke
just docs
just ts-sdk-check
just frontend-smoke
just openapi-drift
just ci-local
```

`just ci-local` is intentionally heavier than the first-pass contributor
commands. It mirrors selected separated local and CI concerns without changing
the fixed public-MVP gate.

Node and frontend checks:

```bash
just ts-sdk-check
just frontend-smoke
```

If the public API shape changes, run the OpenAPI drift check and update both
`schemas/openapi.json` and `API_SPEC.md` in the same change:

```bash
just openapi-drift
```

## DB-Specific Verification

### PostgreSQL / PostGIS reference path

```bash
docker compose -f .docker/docker-compose.yaml up -d postgres redis
cargo run -p cli -- migrate
cargo run -p cli -- seed example
```

### JP import verification on PostgreSQL

```bash
cargo run -p cli -- fixtures generate-demo-jp
cargo run -p cli -- fixtures doctor --path storage/fixtures/demo_jp
cargo run -p cli -- import jp-rail --manifest storage/sources/jp_rail/example.yaml
cargo run -p cli -- import jp-postal --manifest storage/sources/jp_postal/example.yaml
cargo run -p cli -- import jp-school-codes --manifest storage/sources/jp_school/example.yaml
cargo run -p cli -- import jp-school-geodata --manifest storage/sources/jp_school_geo/example.yaml
cargo run -p cli -- derive school-station-links
```

### Full-mode verification

```bash
docker compose -f .docker/docker-compose.full.yaml up -d postgres redis opensearch
cargo run -p cli -- migrate
cargo run -p cli -- seed example
cargo run -p cli -- index rebuild
```

### MySQL status

MySQL remains optional and experimental in this repository. There is no committed MySQL verification command set or CI job in the current tree. If a change adds or modifies MySQL write-path behavior, the same PR must add exact local commands, CI coverage, and documentation before any readiness claim is made. See [MySQL Compatibility](MYSQL_COMPATIBILITY.md).

## Data Source and Storage Rules

- MLIT N02 is the canonical rail/station source.
- MLIT N05 may be used only as an optional non-commercial overlay.
- N05 must not replace N02 as the canonical baseline.
- PostgreSQL and MySQL are write-database targets when supported; PostgreSQL/PostGIS remains the reference implementation.
- SQLite is a read-only artifact/export format and must not become the primary write path.
- Redis is cache only.
- Do not describe data as real-time. Freshness wording must be precise: "latest available MLIT N02 snapshot".

## Artifact Rules

- Generated artifacts must ship with a manifest and checksum output.
- Provenance metadata should be explicit enough to trace source URL, source version, source checksum, generation time, and tool/version context.

## API and Infra Rules

- Public API changes must update generated OpenAPI in `schemas/openapi.json`.
- Public API changes must also update `API_SPEC.md` in the same change. If the file does not exist yet, add it as part of that PR instead of leaving the API contract implicit.
- Do not add cloud production resources, managed infrastructure, or production IaC without explicit review.

## Data Freshness Language

Use exact, audit-friendly wording in docs, PRs, and generated outputs:

- Good: `latest available MLIT N02 snapshot`
- Bad: `latest railway data`
- Bad: `real-time station data`
