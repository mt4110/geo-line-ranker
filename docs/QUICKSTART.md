# Quickstart

This is the canonical local runbook for the repository. `README.md` stays as
the shorter project overview.

If you are new to the project, read
[FIRST_15_MINUTES.md](FIRST_15_MINUTES.md) first for the reading order, default
sample inspection points, fixed-vs-optional boundary, and touch map.
[docs/README.md](README.md) is the audience and task map when you need to
choose a next document rather than run commands.

First-time operators should start with the SQL-only public-MVP path:
PostgreSQL/PostGIS, Redis, the committed default sample, and the operational
`event-csv` import. For the fixed release gate, use
[MVP_ACCEPTANCE.md](MVP_ACCEPTANCE.md). Optional JP demo import, crawler,
full-mode, OpenSearch, managed infrastructure, data-quality, and local review
evidence remain outside that gate; use
[OPTIONAL_EVIDENCE_HANDOFF.md](OPTIONAL_EVIDENCE_HANDOFF.md) when those items
need human review inventory or handoff support.

## 1. Set environment

Use `.env.example` as the baseline.

```bash
cp .env.example .env
```

The `api`, `worker`, `cli`, and `crawler` binaries automatically read `.env` from the repository root.
By default the runtime selects the `local-discovery-generic` profile pack, which
resolves `configs/ranking` and `storage/fixtures/minimal` from
`configs/profiles/local-discovery-generic/profile.yaml`.

## 2. Start minimal services

```bash
docker compose -f .docker/docker-compose.yaml up -d postgres redis
```

## 3. Apply schema

```bash
cargo run -p cli -- migrate
```

## 4. Seed the default sample

```bash
cargo run -p cli -- seed example
```

Fixture files live in `storage/fixtures/minimal/`. This default sample is small
on purpose:

- 6 stations across JR Yamanote Line, Tokyo Metro Marunouchi Line, and Tokyo
  Metro Yurakucho Line
- 10 school rows, including the local demo schools used by the first requests
- 5 fixture event rows for the seeded local dataset
- 7 school-station links with walking minutes, distance, hop distance, and line
  name
- 2 user event rows for initial behavior/snapshot checks

Use `st_tamachi` to inspect the station-first path, `JR Yamanote Line` for the
line-first path, and `Minato` / `Tokyo` for the area-first path. The first
successful recommendation should show a non-empty `items` array with stable
scores, explanations, fallback stage, and candidate counts.

Verify the committed fixture manifest and checksums when fixture files change:

```bash
cargo run -p cli -- fixtures doctor --path storage/fixtures/minimal
```

The minimal fixture is owned by the `local-discovery-generic` profile pack in
`configs/profiles/local-discovery-generic/`.

## 5. Import operational event CSV

```bash
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
```

The file has 4 event rows and exercises the public-MVP operational input path:
checksum staging, import audit rows, active event updates, and replacement
semantics. It is not a crawler path.

## 6. Run the worker and API

```bash
# terminal A
cargo run -p worker -- serve

# terminal B
cargo run -p api -- serve
```

Open Swagger UI:

[http://127.0.0.1:4000/swagger-ui](http://127.0.0.1:4000/swagger-ui)

## 7. Make the first recommendation request

```bash
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d '{"target_station_id":"st_tamachi","placement":"home","limit":3}'
```

Success looks like:

- HTTP `200`
- a non-empty `items` array
- each item has a `content_kind`, `school_id`, `score`, and explanation fields
- event items also include `event_id` and `event_title`
- the top-level response includes `fallback_stage`, `candidate_counts`,
  `context`, `profile_version`, and `algorithm_version`

This confirms that the default sample is loaded, the SQL-only ranking path is
working, and the API can explain why a result was selected.

## 8. Compare placements

```bash
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d '{"target_station_id":"st_tamachi","placement":"home","limit":3}'

curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d '{"target_station_id":"st_tamachi","placement":"search","limit":3}'
```

`home` should surface event-heavy mixes more aggressively.  
`search` should keep school items closer to the front.

## 9. Track user events

```bash
curl -X POST http://127.0.0.1:4000/v1/track \
  -H "content-type: application/json" \
  -d '{"user_id":"demo-user-1","event_kind":"school_view","school_id":"school_seaside"}'
```

The API stores the event in `user_events` and enqueues snapshot refresh jobs into `job_queue`.
Inspect queued worker jobs from the CLI:

```bash
cargo run -p cli -- jobs list --limit 20
```

For recovery commands such as `jobs inspect`, `jobs retry`, `jobs due`, and
`jobs enqueue`, use [docs/OPERATIONS.md](OPERATIONS.md).

If you tune `configs/ranking/tracking.default.yaml`, restart the API and worker, then reapply the current snapshot weights explicitly:

```bash
cargo run -p cli -- snapshot refresh
```

## 10. Where to go next

- New contributors: use [CONTRIBUTING_LOCAL.md](CONTRIBUTING_LOCAL.md) for
  change boundaries and validation, then [TESTING.md](TESTING.md) for local and
  CI checks.
- Operators: use [MVP_ACCEPTANCE.md](MVP_ACCEPTANCE.md) for the fixed six-case
  public-MVP gate, then [OPERATIONS.md](OPERATIONS.md) for worker recovery,
  replay evaluation, post-launch doctor, and data-quality doctor routines.
- Profile authors: use [PROFILE_PACKS.md](PROFILE_PACKS.md) before changing a
  profile manifest, fixture ownership, reason catalog, or source mapping.
- Connector authors: use [DATA_SOURCES.md](DATA_SOURCES.md) and
  [DATA_LICENSES.md](DATA_LICENSES.md) before changing source manifests,
  adapters, crawler manifests, or upstream-data handling.
- Maintainers: use [docs/README.md](README.md) as the audience and task map
  when deciding which document should own a new explanation.
- Use [OPTIONAL_EVIDENCE_HANDOFF.md](OPTIONAL_EVIDENCE_HANDOFF.md) before
  turning optional crawler, full-mode, OpenSearch, managed infrastructure,
  data-quality, or local review findings into follow-up work.

## 11. Optional JP demo import

Run this after the first SQL-only sample works, not as a prerequisite for the
default local path:

```bash
cargo run -p cli -- fixtures generate-demo-jp
cargo run -p cli -- fixtures doctor --path storage/fixtures/demo_jp
cargo run -p cli -- import jp-rail --manifest storage/sources/jp_rail/example.yaml
cargo run -p cli -- import jp-postal --manifest storage/sources/jp_postal/example.yaml
cargo run -p cli -- import jp-school-codes --manifest storage/sources/jp_school/example.yaml
cargo run -p cli -- import jp-school-geodata --manifest storage/sources/jp_school_geo/example.yaml
cargo run -p cli -- derive school-station-links
```

The JP adapter fixture path is owned by the `school-event-jp` reference profile
pack in `configs/profiles/school-event-jp/`.

## 12. Optional allowlist crawl

Crawler flows are optional operator workflows. They are useful for reviewed
source experiments, but they are not required for the first local success state
or the fixed public-MVP gate.

```bash
cargo run -p crawler -- fetch --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- doctor --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- dry-run --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- scaffold-domain --source-id sample-domain --source-name "Sample Domain Events" --school-id school_sample --parser-key sample_parser_v1 --expected-shape html_monthly_dl_pairs --target-url https://example.com/events
```

Real-domain example:

```bash
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
```

`cargo run -p cli -- seed example` seeds `school_utokyo`, `school_keio`,
`school_shibaura_it_junior`, `school_hachioji_gakuen_junior`,
`school_nihon_university_junior`, and `school_aoyama_gakuin_junior`, so the
committed real-domain parser fixtures can import rows in the default local
setup once raw content has been fetched.

`configs/crawler/sources/keio_events.yaml` is intentionally blocked for live
fetch. As of April 19, 2026, `https://www.keio.ac.jp/robots.txt` returned HTTP
404, so only local test fixtures or pre-fetched raw HTML should be used with
the Keio parser until the official robots path is confirmed.
`configs/crawler/sources/nihon_university_junior_events.yaml` is live-fetch
enabled. As of April 19, 2026,
`https://www.yokohama.hs.nihon-u.ac.jp/robots.txt` resolves successfully but
redirects back to HTML, so treat health output and future doctor warnings as
part of normal operator review.
`configs/crawler/sources/aoyama-junior-school-tour.yaml` is live-fetch enabled.
As of April 19, 2026, `https://www.jh.aoyama.ed.jp/robots.txt` returned
`text/plain` and does not explicitly block `/admission/explanation.html`.
`crawler -- serve` only auto-runs manifests marked `source_maturity:
live_ready`.
If you seed a different fixture set, keep the matching `schools.id` rows in
place before running crawl parse.

## 13. Optional full mode

```bash
perl -0pi -e 's/^CANDIDATE_RETRIEVAL_MODE=.*/CANDIDATE_RETRIEVAL_MODE=full/' .env
docker compose -f .docker/docker-compose.full.yaml up -d postgres redis opensearch
cargo run -p cli -- migrate
cargo run -p cli -- seed example
cargo run -p cli -- index rebuild
```

Refresh the projection after import or fixture changes:

```bash
cargo run -p cli -- projection sync
```

Operational detail such as readiness semantics, cache behavior, crawl health, and source-maturity handling lives in [docs/OPERATIONS.md](OPERATIONS.md).
