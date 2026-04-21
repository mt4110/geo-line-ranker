# Operations

## Services

- API: `cargo run -p api -- serve`
- Worker: `cargo run -p worker -- serve`
- CLI: `cargo run -p cli -- --help`
- Crawler: `cargo run -p crawler -- --help`
- PostgreSQL/PostGIS: source of truth for recommendations, tracking, imports, and jobs
- Redis: optional cache for recommendation responses
- OpenSearch: optional full-mode candidate projection and retrieval

## Public MVP profile

The initial public-MVP operating profile is intentionally narrow:

- candidate retrieval mode: `sql_only`
- operational content path: `event-csv`
- release gate: [MVP_ACCEPTANCE.md](MVP_ACCEPTANCE.md)

Live crawler flows and `full` mode stay supported as operator workflows, but they are not part of the public-MVP acceptance gate. Some crawl sources still carry manual-review expectations before production use, so the first public release should stay on the deterministic SQL-only path.

The binaries automatically read `.env` from the repository root when present.

## Readiness checks

The API exposes:

- `GET /healthz`
- `GET /readyz`

`/readyz` reports database reachability, cache status, and OpenSearch readiness. In `sql_only` mode the OpenSearch field is `disabled`; in `full` mode readiness stays red until the configured candidate index is reachable. Cache degradation only removes the fast path.

## Placement profile rollout

Placement configs live in `configs/ranking/placement.*.yaml`.

- `placement.home.yaml`
- `placement.search.yaml`
- `placement.detail.yaml`
- `placement.mypage.yaml`

Startup validation is strict. Unknown keys, missing placement files, invalid ratios, or impossible caps fail the process before the API starts serving.

## Event CSV import

Import operational events:

```bash
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
```

Operational notes:

- raw input is checksum-staged under `.storage/raw/event-csv/...`
- import audit is written to `import_runs`, `import_run_files`, and `import_reports`
- `starts_at` accepts ISO-8601 date (`YYYY-MM-DD`) or RFC3339 timestamp and stays as text so CSV imports and crawl imports keep the same deterministic representation
- repeated import of the same logical `event-csv` source is idempotent, even when the CSV file path changes
- rows no longer present in the same logical `event-csv` source are marked `is_active = false`

Inspect recent event imports:

```sql
SELECT id, source_id, status, total_rows, started_at, completed_at
FROM import_runs
WHERE source_id = 'event-csv'
ORDER BY id DESC
LIMIT 20;

SELECT import_run_id, logical_name, checksum_sha256, row_count, status
FROM import_run_files
ORDER BY id DESC
LIMIT 20;

SELECT import_run_id, level, code, message, row_count
FROM import_reports
ORDER BY id DESC
LIMIT 20;
```

Inspect active imported events:

```sql
SELECT id, school_id, title, event_category, placement_tags, is_active, source_key
FROM events
ORDER BY updated_at DESC, id ASC
LIMIT 20;
```

## Allowlist crawl

Fetch and parse one crawl manifest:

```bash
cargo run -p crawler -- fetch --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- health --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- doctor --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- dry-run --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- scaffold-domain --source-id sample-domain --source-name "Sample Domain Events" --school-id school_sample --parser-key sample_parser_v1 --expected-shape html_monthly_dl_pairs --target-url https://example.com/events

# real-domain example
cargo run -p crawler -- fetch --manifest configs/crawler/sources/utokyo_events.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/utokyo_events.yaml

# second real-domain example
cargo run -p crawler -- fetch --manifest configs/crawler/sources/shibaura_junior_events.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/shibaura_junior_events.yaml

# third real-domain example
cargo run -p crawler -- fetch --manifest configs/crawler/sources/hachioji_junior_events.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/hachioji_junior_events.yaml

# fourth real-domain example
cargo run -p crawler -- fetch --manifest configs/crawler/sources/nihon_university_junior_events.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/nihon_university_junior_events.yaml

# fifth real-domain example
cargo run -p crawler -- fetch --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml
```

Operational notes:

- crawler input is manifest-driven and allowlist-only
- each manifest can declare `source_maturity` (`live_ready`, `policy_blocked`, `parser_only`) and `expected_shape`
- raw HTML is checksum-staged under `.storage/raw/<source_id>/...`
- crawler failures do not stop the API, worker, or CSV importer
- parser selection is registry-driven per manifest `parser_key`
- crawl output lands in `events` with `source_type = 'crawl'`
- `crawler health` summarizes recent run state without needing ad hoc SQL first
- `crawler doctor` checks robots/terms redirects, content types, parser registration, school presence, and target `expected_shape` before you chase a live failure
- `crawler dry-run` reuses the latest fetched raw content to show predicted `parsed / deduped / imported / inactive` counts without mutating core events
- `crawler -- serve` auto-polls only `source_maturity = live_ready`; `parser_only` and `policy_blocked` stay visible in doctor/health without repeated failed fetch attempts
- `utokyo_events.yaml` reads the public `events.json` feed and keeps only the newest 60 dated items per parse for bounded deterministic imports
- `keio_events.yaml` reads the public event listing pages and extracts card metadata including range end date, venue, and registration flag
- `shibaura_junior_events.yaml` reads a public HTML admissions page and expands one bullet into multiple rows when a section lists several dates in the same month
- `hachioji_junior_events.yaml` reads public admissions schedule tables and expands month/day rows into deterministic event rows across the academic year boundary
- `nihon_university_junior_events.yaml` reads `h3.ttl + dl.text_box` schedule blocks, prefers detail PDF dates when available, and separates `detail_url`, `apply_url`, and `official_url` inside parser details
- `aoyama-junior-school-tour.yaml` reads the public school-tour page, extracts internal `section.explan1` rows plus external `section.explan3` fair rows, and expands `29・30日` date ranges into separate deterministic events
- the default `seed example` fixture now includes `school_utokyo`, `school_keio`, `school_shibaura_it_junior`, `school_hachioji_gakuen_junior`, `school_nihon_university_junior`, and `school_aoyama_gakuin_junior`, so all committed real-domain parser fixtures can import rows without extra school setup
- as of 2026-04-19, `https://www.keio.ac.jp/robots.txt` returned HTTP 404, so `keio_events.yaml` is parser-ready but intentionally records `blocked_policy` instead of attempting live fetch
- as of 2026-04-19, `https://www.yokohama.hs.nihon-u.ac.jp/robots.txt` resolves successfully but redirects to HTML content, so keep that source under visible operator review even when live fetch remains enabled
- as of 2026-04-19, `https://www.jh.aoyama.ed.jp/robots.txt` returned HTTP 200 with `text/plain`, and the published rules do not explicitly disallow `/admission/explanation.html`
- custom fixtures still need matching `schools.id` rows before crawl imports can land in `events`

Typical health output includes:

- `source_maturity` plus parser `expected_shape`
- total and recent run counts
- aggregated fetch statuses such as `fetched`, `not_modified`, `fetch_failed`, `blocked_robots`, and `blocked_policy`
- aggregated parse levels such as `info` and `error`
- reason totals such as `blocked_policy`, `blocked_robots`, `fetch_failed`, and `latest_parse_error:*`
- recent reason trend over the shown runs so you can see whether a failure pattern is new or recurring
- the latest parser error per run when one exists
- current manifest `logical_name` red flags such as `latest_fetch_failed`, `latest_blocked_robots`, `latest_blocked_policy`, `missing_from_latest_run`, and `latest_parse_error:*`

Inspect recent crawl runs:

```sql
SELECT id, source_id, parser_key, status, fetched_targets, parsed_rows, imported_rows
FROM crawl_runs
ORDER BY id DESC
LIMIT 20;

SELECT crawl_run_id, logical_name, target_url, fetch_status, checksum_sha256, content_changed
FROM crawl_fetch_logs
ORDER BY id DESC
LIMIT 20;

SELECT crawl_run_id, logical_name, level, code, message, parsed_rows
FROM crawl_parse_reports
ORDER BY id DESC
LIMIT 20;

SELECT crawl_run_id, dedupe_key, kept_event_id, dropped_event_id, reason
FROM crawl_dedupe_reports
ORDER BY id DESC
LIMIT 20;
```

## Job queue and snapshots

`job_queue` remains the worker source of truth.

Important job types:

- `refresh_popularity_snapshot`
- `refresh_user_affinity_snapshot`
- `invalidate_recommendation_cache`
- `sync_candidate_projection`

Inspect recent jobs:

```sql
SELECT id, job_type, status, attempts, max_attempts, last_error, run_after, completed_at
FROM job_queue
ORDER BY id DESC
LIMIT 20;
```

### Search-signal calibration

`configs/ranking/tracking.default.yaml` now owns the `search_execute` calibration knobs:

- `search_execute_school_signal_weight`
- `search_execute_area_signal_weight`

Keep them weaker than explicit school actions such as `school_view` and `school_save`. The default baseline is conservative:

- school signal: `0.4`
- area signal: `0.2`

When you change either value, reapply snapshots with the current config:

```bash
cargo run -p cli -- snapshot refresh
```

Operational notes:

- restart the API and worker after editing ranking config so the live processes pick up the new `profile_version` and tracking weights
- the command recalculates `popularity_snapshots` and `area_affinity_snapshots` from PostgreSQL using the current tracking config
- recommendation cache is invalidated when Redis is configured
- full mode also runs projection sync, so OpenSearch sees the updated popularity ordering without a separate command
- config-only tuning changes `profile_version`; keep `ALGORITHM_VERSION` for code-path changes rather than everyday weight nudges

Useful inspection queries while tuning:

```sql
SELECT target_station_id, COUNT(*) AS search_execute_count
FROM user_events
WHERE event_type = 'search_execute'
GROUP BY target_station_id
ORDER BY search_execute_count DESC, target_station_id ASC
LIMIT 20;

SELECT school_id, popularity_score, total_events, search_execute_count, refreshed_at
FROM popularity_snapshots
ORDER BY popularity_score DESC, school_id ASC
LIMIT 20;

SELECT area, affinity_score, event_count, search_execute_count, refreshed_at
FROM area_affinity_snapshots
ORDER BY affinity_score DESC, area ASC
LIMIT 20;
```

Inspect current snapshot rows:

```sql
SELECT school_id, popularity_score, total_events, search_execute_count, refreshed_at
FROM popularity_snapshots
ORDER BY popularity_score DESC, school_id ASC
LIMIT 20;

SELECT user_id, school_id, affinity_score, event_count, refreshed_at
FROM user_affinity_snapshots
ORDER BY refreshed_at DESC, user_id ASC, affinity_score DESC
LIMIT 20;

SELECT area, affinity_score, event_count, search_execute_count, refreshed_at
FROM area_affinity_snapshots
ORDER BY affinity_score DESC, area ASC
LIMIT 20;
```

## Cache invalidation

Recommendation cache keys include:

- profile version
- algorithm version
- retrieval mode
- candidate limit
- fallback `neighbor_distance_cap_meters`
- serialized request hash

Placement remains part of the serialized request payload, so cache entries stay separated per placement. Changing the retrieval window or fallback neighbor-distance cap also produces a different cache key, so rollout changes do not wait for TTL expiry before taking effect.
