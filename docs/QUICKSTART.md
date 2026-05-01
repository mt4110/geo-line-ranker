# Quickstart

This is the canonical local runbook for the repository. `README.md` stays as the shorter project overview.

For the fixed public-MVP release gate, use [MVP_ACCEPTANCE.md](MVP_ACCEPTANCE.md). Quickstart remains broader and still includes optional JP demo import, crawler, and full-mode steps that are outside the initial public-MVP acceptance scope.
For optional crawler, full-mode, OpenSearch, managed infrastructure, data-quality,
or local review evidence handoff, use
[OPTIONAL_EVIDENCE_HANDOFF.md](OPTIONAL_EVIDENCE_HANDOFF.md); that flow is
review inventory only and does not expand the Quickstart or public-MVP gate.

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

## 4. Seed the demo fixture

```bash
cargo run -p cli -- seed example
```

Fixture files live in `storage/fixtures/minimal/`.
Verify the committed fixture manifest and checksums when fixture files change:

```bash
cargo run -p cli -- fixtures doctor --path storage/fixtures/minimal
```

The minimal fixture is owned by the `local-discovery-generic` profile pack in
`configs/profiles/local-discovery-generic/`.

## 5. Optional Phase 2 JP demo import

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

## 6. Import operational event CSV

```bash
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
```

## 7. Optional allowlist crawl

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

`cargo run -p cli -- seed example` now seeds `school_utokyo`, `school_keio`, `school_shibaura_it_junior`, `school_hachioji_gakuen_junior`, `school_nihon_university_junior`, and `school_aoyama_gakuin_junior`, so the committed real-domain parser fixtures can import rows in the default local setup once raw content has been fetched.

`configs/crawler/sources/keio_events.yaml` is intentionally blocked for live fetch. As of April 19, 2026, `https://www.keio.ac.jp/robots.txt` returned HTTP 404, so only local test fixtures or pre-fetched raw HTML should be used with the Keio parser until the official robots path is confirmed.
`configs/crawler/sources/nihon_university_junior_events.yaml` is live-fetch enabled. As of April 19, 2026, `https://www.yokohama.hs.nihon-u.ac.jp/robots.txt` resolves successfully but redirects back to HTML, so treat health output and future doctor warnings as part of normal operator review.
`configs/crawler/sources/aoyama-junior-school-tour.yaml` is live-fetch enabled. As of April 19, 2026, `https://www.jh.aoyama.ed.jp/robots.txt` returned `text/plain` and does not explicitly block `/admission/explanation.html`.
`crawler -- serve` only auto-runs manifests marked `source_maturity: live_ready`.
If you seed a different fixture set, keep the matching `schools.id` rows in place before running crawl parse.

## 8. Run the worker and API

```bash
cargo run -p worker -- serve
cargo run -p api -- serve
```

Open Swagger UI:

[http://127.0.0.1:4000/swagger-ui](http://127.0.0.1:4000/swagger-ui)

## 9. Compare placements

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

## 10. Track user events

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

## 11. Run full mode

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
