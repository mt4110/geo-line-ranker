# AGENTS.md

## Repository mission
This repository is an OSS for deterministic geo-first / line-first recommendations.
No AI/ML/embeddings/vector search.

## Repository layout
- `apps/api`: Axum HTTP API.
- `apps/cli`: local operations, import, fixture, and OpenAPI dump commands.
- `apps/worker`: async job runner for snapshot and projection work.
- `apps/crawler`: allowlist-only fetch / parse / doctor flow.
- `crates/ranking`, `crates/domain`, `crates/geo`: deterministic ranking and domain logic.
- `crates/storage-postgres`: reference write-path storage implementation.
- `crates/connectors/jp-rail`, `crates/connectors/jp-postal`, `crates/connectors/jp-school`: JP source parsers and import adapters.
- `storage/fixtures`: committed local fixtures.
- `storage/sources`: source manifests for JP demo imports.
- `storage/migrations/postgres`: PostgreSQL/PostGIS schema.
- `configs`: ranking and crawler manifests.
- `docs`: contributor, ops, architecture, and quickstart docs.

## station_converter_ja note
- This repository does not currently have a dedicated `station_converter_ja/` package.
- In this codebase, station conversion work lives across `crates/connectors/jp-rail`, `apps/cli`, `storage/sources/jp_rail`, `storage/fixtures/demo_jp`, and `crates/storage-postgres`.

## Core rules
- Keep PostgreSQL/PostGIS as the reference implementation.
- Keep MySQL adapter optional and experimental.
- If MySQL support is present, treat PostgreSQL and MySQL as write databases; do not turn SQLite into a primary write store.
- SQLite is a read-only artifact/export target only.
- Keep Redis as cache only.
- Keep OpenSearch only for candidate retrieval in later phases.
- Keep SQL-only minimal mode working.
- Never move final ranking logic to the frontend.
- Never make crawling mandatory for the system to work.
- Treat MLIT N02 as the canonical rail/station source.
- Treat MLIT N05 only as an optional non-commercial overlay and never as a replacement for N02.
- Do not add cloud production resources or managed infrastructure without explicit review.
- Generated artifacts must include a manifest and checksum output.
- Public API changes must update generated OpenAPI (`schemas/openapi.json`) and `API_SPEC.md` in the same change.
- Be precise about freshness claims: say "latest available MLIT N02 snapshot", never "real-time railway data".

## Engineering rules
- Prefer small, reviewable changes.
- Add docs when behavior changes.
- Add tests for ranking behavior changes.
- Preserve deterministic outputs for the same input, config, and data.

## Validation rules
- Run formatting, lint, and tests before reporting completion.
- If a command cannot run, explain why and still prepare the command list.
- Keep example fixtures and quickstart working.
- Default workspace validation commands:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  - `cargo test --workspace`
- PostgreSQL reference verification commands:
  - `docker compose -f .docker/docker-compose.yaml up -d postgres redis`
  - `cargo run -p cli -- migrate`
  - `cargo run -p cli -- seed example`
- JP station import verification commands:
  - `cargo run -p cli -- fixtures generate-demo-jp`
  - `cargo run -p cli -- import jp-rail --manifest storage/sources/jp_rail/example.yaml`
  - `cargo run -p cli -- import jp-postal --manifest storage/sources/jp_postal/example.yaml`
  - `cargo run -p cli -- import jp-school-codes --manifest storage/sources/jp_school/example.yaml`
  - `cargo run -p cli -- import jp-school-geodata --manifest storage/sources/jp_school_geo/example.yaml`
  - `cargo run -p cli -- derive school-station-links`
- Full-mode verification commands:
  - `docker compose -f .docker/docker-compose.full.yaml up -d postgres redis opensearch`
  - `cargo run -p cli -- index rebuild`
- There is no committed MySQL verification flow in this workspace today. Any change that introduces or touches MySQL write support must add exact local and CI verification commands in the same PR.

See `docs/CONTRIBUTING_LOCAL.md` for the longer local contributor runbook.
