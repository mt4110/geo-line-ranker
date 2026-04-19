# Architecture

## Phase 6 boundaries

- PostgreSQL/PostGIS remains the source of truth.
- Placement profiles are config-driven and loaded at startup.
- Redis stays cache only and can be disabled without changing correctness.
- OpenSearch remains optional and candidate-retrieval-only.
- Allowlist crawl is optional and does not gate API, worker, or CSV import availability.
- Final ranking, explanation, mixed content selection, and diversity control stay in Rust.
- Operational event CSV is staged under `.storage/raw/` and imported idempotently into PostgreSQL.
- Crawl fetches stage raw HTML under `.storage/raw/`, keep parser choice in a registry, and write fetch / parse / dedupe audit tables in PostgreSQL.
- `search_execute` is still intentionally outside snapshot weights at this phase boundary.

## Runtime view

```mermaid
flowchart LR
    FE["Example Frontend"] --> API["apps/api"]
    SW["Swagger UI"] --> API
    API --> RC["crates/ranking"]
    API --> CA["crates/cache"]
    API --> SP["crates/storage-postgres"]
    API --> OS["crates/storage-opensearch"]
    API --> OBS["crates/observability"]
    RC --> DM["crates/domain"]
    RC --> CFG["crates/config"]
    RC --> GEO["crates/geo"]
    SP --> PG["PostgreSQL / PostGIS"]
    OS --> OP["OpenSearch"]
    API --> RD["Redis (optional cache)"]
    WRK["apps/worker"] --> WC["crates/worker-core"]
    WC --> SP
    WC --> OS
    WC --> CA
    CLI["apps/cli"] --> PG
    CR["apps/crawler"] --> CC["crates/crawler-core"]
    CR --> GH["crates/connectors/generic-http"]
    CC --> SP
    GH --> RAW
    CLI --> RAW[".storage/raw"]
    CLI --> FX["storage/fixtures/minimal"]
    CLI --> MF["storage/sources/*.yaml"]
    CLI --> CSV["examples/import/events.sample.csv"]
    CR --> CM["configs/crawler/sources/*.yaml"]
```

## Recommendation flow

1. `POST /v1/recommendations` receives a target station plus placement.
2. The API builds a cache key from request payload, profile version, algorithm version, and retrieval mode.
3. Candidate links come from PostgreSQL (`sql_only`) or OpenSearch (`full`).
4. PostgreSQL loads school rows, active event rows, station rows, and snapshot rows for the candidate slice.
5. `crates/ranking` scores school candidates and event candidates from the same slice.
6. Placement config applies mixed-ranking boosts and diversity hard caps.
7. The response returns mixed items, explanation text, profile version, and algorithm version.

## Mixed ranking model

- `school`
  One best station-linked item per school.
- `event`
  Active event rows that belong to candidate schools and are visible to the requested placement.
- `article`
  Reserved in config and schema, but runtime validation still rejects it until article candidates are implemented.

Per-placement config currently controls:

- neighbor expansion tolerance
- same-line neighbor bonus
- per-content-kind score boosts
- featured event bonus
- event priority weight
- same school cap
- same group cap
- per-content-kind max ratio

## Diversity model

Selection happens after scoring.

- `same_school_cap`
  Limits repeated items from the same school across school and event content.
- `same_group_cap`
  Limits repeated items from the same school group.
- `content_kind_max_ratio`
  Limits how much of the final list can be occupied by one content kind.

The ranker may return fewer than the requested limit when the hard caps would otherwise be violated.

## Import model

- JP importers still use source manifests plus normalized tables.
- Operational event CSV uses direct file import through `cargo run -p cli -- import event-csv --file ...`.
- Raw CSV is checksum-staged under `.storage/raw/event-csv/...`.
- The importer upserts current rows and marks stale rows from the same source inactive.
- Import success and failure are recorded in `import_runs`, `import_run_files`, and `import_reports`.
- Allowlist crawl uses `cargo run -p crawler -- fetch|parse --manifest ...`.
- Fetch writes raw HTML into `.storage/raw/<source_id>/<checksum>/...`.
- Parse uses the registry-selected parser, records parse failures explicitly, dedupes deterministic event IDs, and imports rows into `events` as `source_type = 'crawl'`.
- Crawl success and failure are recorded in `crawl_runs`, `crawl_fetch_logs`, `crawl_parse_reports`, and `crawl_dedupe_reports`.

## Phase 6 crate map

- `crates/domain`
  Placement enum, content-kind enum, mixed recommendation item types, and ranking query/result shapes.
- `crates/config`
  Placement profile loading, strict config parsing, and startup validation.
- `crates/ranking`
  Mixed school/event scoring, explanation synthesis, and diversity selection.
- `crates/connectors/generic-csv`
  Checksum staging plus direct CSV staging for operational event import.
- `crates/connectors/generic-http`
  Allowlist URL validation, robots evaluation, HTTP fetch, and raw HTML staging.
- `crates/crawler-core`
  Crawl manifest loading, parser registry, HTML extraction, deterministic event IDs, and dedupe logic.
- `crates/storage-postgres`
  Placement-aware dataset loading, event CSV import, crawl audit persistence, and fixture seeding.
- `apps/api`
  Placement-aware recommendation endpoint and updated response contract.
- `apps/cli`
  `import event-csv`, existing JP imports, and projection commands.
- `apps/crawler`
  `fetch`, `parse`, `run`, and `serve` commands for optional allowlist crawl.
