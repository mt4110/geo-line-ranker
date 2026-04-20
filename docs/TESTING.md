# Testing

## Default validation

Run the full Rust validation set from the repository root:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

## Integration prerequisites

Phase 5 integration checks use PostgreSQL and Redis. The SQL-only vs full-mode compatibility test still uses a mock OpenSearch endpoint and does not require Docker.

```bash
docker compose -f .docker/docker-compose.yaml up -d postgres redis
cargo run -p cli -- migrate
cargo run -p cli -- seed example
```

## What gets covered

- ranking unit tests:
  strict mode, neighbor fallback, placement mix differences, same-school cap, popularity, and user-affinity debug details
- API integration test:
  `POST /v1/track` persists an append-only event and enqueues DB-backed jobs
- worker integration test:
  snapshot jobs refresh snapshot tables and invalidate Redis cache entries
- importer integration test:
  Phase 2 JP import path still works after Phase 5 additions
- crawler integration test:
  allowlist fetch, parser registry selection, and crawl-to-events import flow work when PostgreSQL is reachable
- compatibility integration test:
  SQL-only and full mode return the same recommendation ordering for the shared demo cases

## Manual smoke checks

1. Start the worker:

   ```bash
   cargo run -p worker -- serve
   ```

2. Start the API:

   ```bash
   cargo run -p api -- serve
   ```

3. Compare placement responses:

   ```bash
   curl -X POST http://127.0.0.1:4000/v1/recommendations \
     -H "content-type: application/json" \
     -d '{"target_station_id":"st_tamachi","placement":"home","limit":3}'

   curl -X POST http://127.0.0.1:4000/v1/recommendations \
     -H "content-type: application/json" \
     -d '{"target_station_id":"st_tamachi","placement":"search","limit":3}'
   ```

   Confirm the item mix or ordering changes while `profile_version` stays stable for the same config set.

4. Send one or more tracking events:

   ```bash
   curl -X POST http://127.0.0.1:4000/v1/track \
     -H "content-type: application/json" \
     -d '{"user_id":"manual-user-1","event_kind":"school_save","school_id":"school_garden"}'
   ```

5. Confirm the queue drains and snapshot rows update:

   ```sql
   SELECT job_type, status, attempts
   FROM job_queue
   ORDER BY id DESC
   LIMIT 10;

   SELECT school_id, popularity_score
   FROM popularity_snapshots
   ORDER BY popularity_score DESC, school_id ASC;
   ```

6. Import the operational event CSV and confirm active rows update:

   ```bash
   cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
   ```

   ```sql
   SELECT id, title, placement_tags, is_active, source_key
   FROM events
   ORDER BY updated_at DESC, id ASC
   LIMIT 20;
   ```

7. Run the optional crawler and inspect audit tables:

   ```bash
   cargo run -p crawler -- fetch --manifest configs/crawler/sources/custom_example.yaml
   cargo run -p crawler -- parse --manifest configs/crawler/sources/custom_example.yaml
   cargo run -p crawler -- health --manifest configs/crawler/sources/custom_example.yaml
   cargo run -p crawler -- doctor --manifest configs/crawler/sources/custom_example.yaml
   cargo run -p crawler -- dry-run --manifest configs/crawler/sources/custom_example.yaml
   cargo run -p crawler -- scaffold-domain --source-id sample-domain --source-name "Sample Domain Events" --school-id school_sample --parser-key sample_parser_v1 --expected-shape html_monthly_dl_pairs --target-url https://example.com/events
   ```

   Real-domain smoke option:

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

   ```sql
   SELECT status, fetched_targets, parsed_rows, imported_rows
   FROM crawl_runs
   ORDER BY id DESC
   LIMIT 5;
   ```

   The default `seed example` fixture now includes `school_utokyo`, `school_keio`, `school_shibaura_it_junior`, `school_hachioji_gakuen_junior`, `school_nihon_university_junior`, and `school_aoyama_gakuin_junior`, so the committed real-domain parser fixtures can import rows in the normal local setup once fetchable raw content exists.

   `configs/crawler/sources/keio_events.yaml` remains parser-ready but live fetch is now blocked by manifest policy because `https://www.keio.ac.jp/robots.txt` returned HTTP 404 on April 19, 2026. Keep Keio validation on local fixture HTML until the official robots path is confirmed.
   `configs/crawler/sources/nihon_university_junior_events.yaml` is live-fetch enabled, but its current robots URL resolves to HTML rather than plain-text robots content. Health output now includes reason totals so operators can separate policy blocks, robots blocks, and parse errors at a glance.
   `configs/crawler/sources/aoyama-junior-school-tour.yaml` is live-fetch enabled, and its current robots URL returned plain-text rules on April 19, 2026. The parser expands the `2026年 8月 29・30日` fair row into two deterministic events, so smoke checks should expect 10 imported rows from the committed fixture.
   `crawler -- serve` auto-runs only `source_maturity: live_ready` manifests, so `parser_only` and `policy_blocked` sources should be exercised through `doctor`, `dry-run`, fixture-backed tests, or explicit `fetch/parse`.
   If you use a custom fixture set, keep those `schools.id` rows in place before running crawl parse.

## Full-mode cache smoke

1. Prepare `.env` for full mode:

   ```bash
   cp .env.example .env
   perl -0pi -e 's/^CANDIDATE_RETRIEVAL_MODE=.*/CANDIDATE_RETRIEVAL_MODE=full/' .env
   ```

2. Start PostgreSQL, Redis, and OpenSearch:

   ```bash
   docker compose -f .docker/docker-compose.full.yaml up -d postgres redis opensearch
   cargo run -p cli -- migrate
   cargo run -p cli -- seed example
   cargo run -p cli -- index rebuild
   ```

3. Warm the cache with one placement and inspect keys:

   ```bash
   curl -X POST http://127.0.0.1:4000/v1/recommendations \
     -H "content-type: application/json" \
     -d '{"target_station_id":"st_tamachi","placement":"home","limit":3}'

   docker exec docker-redis-1 redis-cli --scan --pattern 'geo-line-ranker:recommendations:*'
   ```

   The request payload hash now changes across placements, so `home` and `search` should not share the same cache entry.
