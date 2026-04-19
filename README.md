# geo-line-ranker

Deterministic geo-first and line-first recommendation engine for local discovery.  
PostgreSQL/PostGIS is the reference store, ranking stays inside Rust, Redis is optional cache only, OpenSearch is optional candidate retrieval for full mode, and allowlist crawl remains an optional side path.

## What is in Phase 6

- Rust workspace with `api`, `cli`, `worker`, and `crawler`
- SQL-only minimal mode backed by PostgreSQL/PostGIS
- OpenSearch full mode for candidate retrieval with runtime mode switching
- `POST /v1/recommendations` with placement-aware mixed school/event ranking
- `POST /v1/track` for append-only behavior logging
- Placement profiles for `home`, `search`, `detail`, and `mypage`
- Diversity hard caps for same school, same group, and content-kind ratio
- `article` remains a reserved schema/config slot and is intentionally rejected at runtime until implemented
- DB-backed worker queue with retryable snapshot refresh and cache invalidation jobs
- DB to OpenSearch projection sync through CLI and worker jobs
- Optional Redis cache for recommendation responses
- Operational `event-csv` import with checksum staging and audit trail
- Optional allowlist crawler with parser registry, raw HTML staging, differential checksum fetch, and audited fetch / parse / dedupe reports
- Source maturity labels plus parser expected-shape metadata on crawl manifests
- Parser health summary command for recent crawl runs, fetch outcomes, parse levels, latest parser errors, and `logical_name` red flags per manifest
- `crawler scaffold-domain` for manifest / fixture / guide scaffolding when adding a new crawl source, now with inferred defaults and shape-aware guidance
- First real-domain crawl example for the University of Tokyo public events JSON feed
- Second real-domain crawl example for the Shibaura Institute of Technology Junior High admissions event page
- Third real-domain crawl example for the Hachioji Gakuen Hachioji Junior High admissions schedule page
- Fourth real-domain crawl example for the Nihon University Junior High information session page
- Fifth real-domain crawl example for the Aoyama Gakuin Junior High school tour page
- Local fixture seeding for a small rail-aware mixed ranking dataset
- Japanese source adapters for rail, postal, school codes, and school geodata
- Swagger UI and a small Next.js example frontend

## Phase 6 note

- No major blockers remain in this phase.
- `search_execute` already persists through `POST /v1/track`.
- `search_execute` still does not feed snapshot weights.
- That remains a later handoff note, not a blocker for placement profiles, mixed ranking, or allowlist crawl.

## Quickstart

```bash
cp .env.example .env
docker compose -f .docker/docker-compose.yaml up -d postgres redis
cargo run -p cli -- migrate
cargo run -p cli -- seed example
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
cargo run -p crawler -- fetch --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- health --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- scaffold-domain --source-id sample-domain --source-name "Sample Domain Events" --school-id school_sample --parser-key sample_parser_v1 --expected-shape html_monthly_dl_pairs --target-url https://example.com/events
cargo run -p crawler -- fetch --manifest configs/crawler/sources/utokyo_events.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/utokyo_events.yaml
cargo run -p crawler -- fetch --manifest configs/crawler/sources/shibaura_junior_events.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/shibaura_junior_events.yaml
cargo run -p crawler -- fetch --manifest configs/crawler/sources/hachioji_junior_events.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/hachioji_junior_events.yaml
cargo run -p crawler -- fetch --manifest configs/crawler/sources/nihon_university_junior_events.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/nihon_university_junior_events.yaml
cargo run -p crawler -- fetch --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml
cargo run -p worker -- serve
cargo run -p api -- serve
```

`cargo run -p cli -- seed example` now seeds `school_utokyo`, `school_keio`, `school_shibaura_it_junior`, `school_hachioji_gakuen_junior`, `school_nihon_university_junior`, and `school_aoyama_gakuin_junior`, so all committed real-domain parser fixtures can import rows once fetched content is available in the local setup.

`configs/crawler/sources/keio_events.yaml` is parser-ready but explicitly blocked for live fetch. On April 19, 2026, `https://www.keio.ac.jp/robots.txt` returned HTTP 404, so the manifest now records `blocked_policy` instead of attempting an unsafe live fetch until Keio publishes an official robots URL or crawler policy changes.
`crawler -- serve` now auto-runs only manifests whose `source_maturity` is `live_ready`; `parser_only` and `policy_blocked` sources stay visible in doctor/health without generating poll-loop noise.
If you skip the seed or use a custom fixture, the matching `schools.id` rows still need to exist before crawl imports land in `events`.

Send a sample recommendation request:

```bash
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d '{"target_station_id":"st_tamachi","placement":"home","limit":3}'
```

Send a sample tracking event:

```bash
curl -X POST http://127.0.0.1:4000/v1/track \
  -H "content-type: application/json" \
  -d '{"user_id":"demo-user-1","event_kind":"school_view","school_id":"school_seaside"}'
```

## Full mode quickstart

```bash
perl -0pi -e 's/^CANDIDATE_RETRIEVAL_MODE=.*/CANDIDATE_RETRIEVAL_MODE=full/' .env
docker compose -f .docker/docker-compose.full.yaml up -d postgres redis opensearch
cargo run -p cli -- migrate
cargo run -p cli -- seed example
cargo run -p cli -- index rebuild
cargo run -p worker -- serve
cargo run -p api -- serve
```

Refresh the projection without rebuilding the index:

```bash
cargo run -p cli -- projection sync
```

## Example response shape

```json
{
  "items": [
    {
      "content_kind": "event",
      "content_id": "event_seaside_open",
      "school_id": "school_seaside",
      "school_name": "Seaside High",
      "event_id": "event_seaside_open",
      "event_title": "Seaside Open Campus",
      "primary_station_id": "st_tamachi",
      "primary_station_name": "Tamachi",
      "line_name": "JR Yamanote Line",
      "score": 6.41,
      "explanation": "直結条件 と 注目イベント が効き、直結条件のイベント候補として上位になりました。",
      "score_breakdown": [
        {
          "feature": "direct_station_bonus",
          "value": 3.0,
          "reason": "Tamachi に直結しています。"
        }
      ]
    }
  ],
  "explanation": "ホームでは Tamachi 直結の候補群 を母集団にし、直結条件 と 注目イベント を効かせて決定論的に順位付けしました。",
  "fallback_stage": "strict",
  "profile_version": "phase5-profile-version",
  "algorithm_version": "phase5-placement-mixed-ranking-v1"
}
```

## Docs

- [Japanese README](README_JA.md)
- [Quickstart](docs/QUICKSTART.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Operations](docs/OPERATIONS.md)
- [Testing](docs/TESTING.md)
- [Data Sources](docs/DATA_SOURCES.md)
- [Data Licenses](docs/DATA_LICENSES.md)
