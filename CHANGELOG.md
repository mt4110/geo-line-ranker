# Changelog

## Unreleased

- Initialized the Rust workspace for deterministic geo/line recommendations.
- Added PostgreSQL/PostGIS schema, example fixtures, and CLI migrate/seed commands.
- Added the minimal recommendation API with health probes, readiness probe, and Swagger UI.
- Added a small TypeScript SDK scaffold and a Next.js example frontend.
- Added Japanese data adapters for rail, postal, school codes, and school geodata.
- Added source manifest, checksum staging, import audit tables, and school-station derivation.
- Added `POST /v1/track` with append-only event storage and DB-backed worker job enqueueing.
- Added popularity, user affinity, and area affinity snapshots plus worker refresh logic.
- Added optional Redis caching for recommendation responses with namespace invalidation.
- Added SQL-only / OpenSearch full mode switching for candidate retrieval.
- Added OpenSearch projection mapping, CLI rebuild/sync commands, and worker projection sync jobs.
- Added recommendation trace payloads and compatibility tests that compare SQL-only vs full mode.
- Added automatic `.env` loading in `api`, `worker`, and `cli` through shared config bootstrap.
- Updated quickstart and testing docs to include cache-enabled full-mode smoke steps.
- Added Phase 5 placement profiles for `home`, `search`, `detail`, and `mypage`.
- Added mixed school/event ranking with same-school cap, same-group cap, and per-kind ratio control.
- Added `profile_version` plus mixed content metadata to recommendation responses and the TypeScript SDK.
- Added idempotent `import event-csv --file ...` with checksum staging, import audit, and stale-row deactivation.
- Updated fixtures, ranking examples, schema artifacts, and docs for placement-aware ranking.
- Added optional Phase 6 allowlist crawl with parser registry, raw HTML staging, and differential checksum fetch.
- Added crawl audit tables for run status, fetch logs, parse reports, and dedupe reports.
- Added `apps/crawler`, `crates/crawler-core`, and `crates/connectors/generic-http`.
- Added crawl manifest `source_maturity`, parser `expected_shape`, shape-aware doctor checks, and `crawler scaffold-domain`.
- Added public MVP release readiness guidance for release candidate evidence, fixed SQL-only acceptance, data quality review, and release notes handoff.
- Added the Aoyama Gakuin Junior High school-tour crawl manifest, fixture scaffold, parser, and live-ready integration coverage.
- Upgraded `crawler scaffold-domain` to infer better defaults from source metadata and emit shape-aware fixture / promotion guidance.
- Expanded `crawler --help` and subcommand help so routine crawler workflows can be discovered directly from the CLI.
