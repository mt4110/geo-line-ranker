# geo-line-ranker

Deterministic geo-first and line-first recommendation engine for local discovery.
PostgreSQL/PostGIS is the reference store, ranking stays inside Rust, Redis is optional cache only, OpenSearch is optional candidate retrieval for full mode, and allowlist crawl remains an optional side path. No AI, ML, embeddings, or vector search.

## What is included

- Rust workspace with `api`, `cli`, `worker`, and `crawler`
- SQL-only minimal mode backed by PostgreSQL/PostGIS
- OpenSearch full mode for candidate retrieval with runtime mode switching
- `POST /v1/recommendations` with placement-aware mixed school/event ranking
- `POST /v1/track` for append-only behavior logging
- Placement profiles for `home`, `search`, `detail`, and `mypage`
- Diversity hard caps for same school, same group, and content-kind ratio
- `article` remains a reserved schema/config slot and is intentionally rejected at runtime until implemented
- DB-backed worker queue with retryable snapshot refresh and cache invalidation jobs
- Worker queue recovery CLI through `jobs list`, `jobs inspect`, `jobs retry`, `jobs due`, and `jobs enqueue`
- DB to OpenSearch projection sync through CLI and worker jobs
- Optional Redis cache for recommendation responses
- Operational `event-csv` import with checksum staging and audit trail
- Optional allowlist crawler with parser registry, raw HTML staging, differential checksum fetch, and audited fetch / parse / dedupe reports
- Source maturity labels plus parser expected-shape metadata on crawl manifests
- Parser health summary command for recent crawl runs, fetch outcomes, parse levels, latest parser errors, and `logical_name` red flags per manifest
- Read-only post-launch doctor and data quality doctor for incident triage
- Release readiness command plan for public MVP release candidate decisions
- `crawler scaffold-domain` for manifest / fixture / guide scaffolding when adding a new crawl source, now with inferred defaults and shape-aware guidance
- First real-domain crawl example for the University of Tokyo public events JSON feed
- Second real-domain crawl example for the Shibaura Institute of Technology Junior High admissions event page
- Third real-domain crawl example for the Hachioji Gakuen Hachioji Junior High admissions schedule page
- Fourth real-domain crawl example for the Nihon University Junior High information session page
- Fifth real-domain crawl example for the Aoyama Gakuin Junior High school tour page
- Local fixture seeding for a small rail-aware mixed ranking dataset
- Japanese source adapters for rail, postal, school codes, and school geodata
- Swagger UI and a small Next.js example frontend

## Current behavior notes

- `search_execute` persists through `POST /v1/track`, refreshes popularity / area snapshot weights through station-linked schools, and now uses config-driven calibration.
- `cargo run -p cli -- snapshot refresh` reapplies the current tracking config, invalidates recommendation cache, and syncs the full-mode projection when enabled.
- Public MVP acceptance remains SQL-only and deterministic; live crawling and full-mode retrieval stay optional side paths.
- Release candidate decisions use `just release-readiness` to review the flow, `just mvp-acceptance` as the fixed gate, and `DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor` as required evidence capture.

## Quickstart

For the first 15 minutes, read
[docs/FIRST_15_MINUTES.md](docs/FIRST_15_MINUTES.md). The canonical local
runbook lives in [docs/QUICKSTART.md](docs/QUICKSTART.md). The first run should
stay on the public-MVP path: SQL-only candidate retrieval, `event-csv`
operational content, PostgreSQL/PostGIS, and Redis. OpenSearch, full mode, live
crawler operation, and managed infrastructure are optional follow-on paths, not
fixed gates.

Contributor entrypoints:

```bash
just setup
just dev
just smoke
just docs
just eval
just ts-sdk-check
just frontend-smoke
just openapi-drift
just ci-local
```

Docs, SDK, frontend, OpenAPI drift, and `ci-local` checks are contributor and
CI tooling. `just ci-local` is a selected local mirror, not a replacement for
all GitHub Actions jobs. These checks do not expand the fixed public-MVP gate.

Shortest local path:

```bash
cp .env.example .env
docker compose -f .docker/docker-compose.yaml up -d postgres redis
cargo run -p cli -- migrate
cargo run -p cli -- seed example
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv

# terminal A
cargo run -p worker -- serve

# terminal B
cargo run -p api -- serve
```

First success means Swagger UI opens, `POST /v1/recommendations` returns
non-empty `items` with score and explanation fields, `POST /v1/track` accepts a
behavior event, and the `event-csv` import leaves an audit trail in PostgreSQL.

For crawler commands, full mode, projection sync, worker job recovery, and
post-launch doctor routines, use [docs/QUICKSTART.md](docs/QUICKSTART.md) and
[docs/OPERATIONS.md](docs/OPERATIONS.md) after the baseline path works.

Sample recommendation request:

```bash
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d '{"target_station_id":"st_tamachi","placement":"home","limit":3}'
```

Sample tracking event:

```bash
curl -X POST http://127.0.0.1:4000/v1/track \
  -H "content-type: application/json" \
  -d '{"user_id":"demo-user-1","event_kind":"school_view","school_id":"school_seaside"}'
```

## Example response shape

This is a field-shape example. Scores and event titles can vary with fixtures,
imports, and config.

```json
{
  "items": [
    {
      "content_kind": "event",
      "content_id": "event_seaside_open",
      "school_id": "school_seaside",
      "school_name": "Seaside High",
      "event_id": "event_seaside_open",
      "event_title": "Seaside Open Campus Spring",
      "primary_station_id": "st_tamachi",
      "primary_station_name": "Tamachi",
      "line_name": "JR Yamanote Line",
      "score": 6.41,
      "explanation": "沿線一致 と 注目イベント が効き、同一路線のイベント候補として上位になりました。",
      "score_breakdown": [
        {
          "feature": "line_match_bonus",
          "reason_code": "geo.line_match",
          "value": 1.25,
          "reason": "JR Yamanote Line 沿線の候補です。"
        }
      ],
      "fallback_stage": "same_line"
    }
  ],
  "explanation": "ホームでは JR Yamanote Line 沿線の候補群 を母集団にし、沿線一致 と 注目イベント を効かせて決定論的に順位付けしました。 多様性上限で同一学校1件を抑制し、3件の表示枠に整えています。",
  "score_breakdown": [],
  "fallback_stage": "same_line",
  "candidate_counts": {
    "strict_station": 0,
    "same_line": 5,
    "same_city": 2
  },
  "context": {
    "context_source": "request_line",
    "confidence": 0.95,
    "privacy_level": "coarse_area",
    "warnings": []
  },
  "profile_version": "phase5-profile-version",
  "algorithm_version": "phase8-policy-diversity-v1"
}
```

## Docs

- [Japanese README](README.md)
- [Documentation Index](docs/README.md): audience and task map
- [Non-engineer Friendly Design Docs](docs/design_document/README_JA.md)
- [Contributor Rules](AGENTS.md)
- [First 15 Minutes](docs/FIRST_15_MINUTES.md)
- [Local Contributing Guide](docs/CONTRIBUTING_LOCAL.md)
- [Quickstart](docs/QUICKSTART.md)
- [MVP Acceptance](docs/MVP_ACCEPTANCE.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Operations](docs/OPERATIONS.md)
- [Testing](docs/TESTING.md)
- [Data Sources](docs/DATA_SOURCES.md)
- [Data Licenses](docs/DATA_LICENSES.md)
