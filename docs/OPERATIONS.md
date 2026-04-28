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
- release readiness command plan: `just release-readiness`
- fixed release gate: [MVP_ACCEPTANCE.md](MVP_ACCEPTANCE.md)

Live crawler flows and `full` mode stay supported as operator workflows, but they are not part of the public-MVP acceptance gate. Some crawl sources still carry manual-review expectations before production use, so the first public release should stay on the deterministic SQL-only path.

The binaries automatically read `.env` from the repository root when present.
`POSTGRES_POOL_MAX_SIZE` controls the per-process PostgreSQL connection pool
for API, worker, crawler, and CLI processes, including helper-backed import,
crawl, and migration paths. Keep it below the database server's available
connection budget after reserving room for migrations, manual `psql`, and
maintenance jobs.

For post-launch incident triage, start with the read-only doctor:

```bash
just post-launch-doctor
```

Without `just`:

```bash
./scripts/post_launch_doctor.sh
```

When PostgreSQL is reachable, add the data quality pass and review its output
before opening implementation work:

```bash
just data-quality-doctor
```

Without `just`:

```bash
./scripts/data_quality_doctor.sh
```

## Release readiness routine

Use the release readiness command plan before cutting or validating a release
candidate:

```bash
just release-readiness
```

Without `just`:

```bash
./scripts/release_readiness.sh
```

Then run the fixed public-MVP gate:

```bash
just mvp-acceptance
```

Without `just`:

```bash
./scripts/mvp_acceptance.sh
```

This gate deliberately starts only PostgreSQL/PostGIS and Redis, forces `CANDIDATE_RETRIEVAL_MODE=sql_only`, and exercises the CLI, worker, API, snapshots, tracking jobs, and `event-csv` import semantics. Do not add live crawler or OpenSearch requirements to this gate unless the public-MVP operating profile changes through explicit review.

For release candidate evidence, capture the local validation results
(`cargo fmt --all --check`,
`cargo clippy --workspace --all-targets --all-features -- -D warnings`,
`cargo test --workspace`, config/source/crawler manifest lint, fixture doctor,
and `git diff --check`), CI status, release notes,
and the required `DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor`
evidence. The data quality doctor is required evidence capture for release
readiness; strict mode makes doctor warnings fail the evidence step, but it
does not add cases to `just mvp-acceptance`.

## Maintenance Command Plans

After the release gate is healthy, print the post-MVP maintenance command plan:

```bash
just post-mvp-hardening
```

Without `just`:

```bash
./scripts/post_mvp_hardening.sh
```

For optional crawler, full-mode, OpenSearch, or managed infrastructure review,
print the read-only evidence command plan:

```bash
just optional-evidence-review
```

Without `just`:

```bash
./scripts/optional_evidence_review.sh
```

## Readiness checks

The API exposes:

- `GET /healthz`
- `GET /readyz`

`/readyz` reports database reachability, cache status, and OpenSearch readiness. In `sql_only` mode the OpenSearch field is `disabled`; in `full` mode readiness stays red until the configured candidate index is reachable. Cache degradation only removes the fast path.

## Docker Runtime Notes

The committed Dockerfiles build release binaries in a Rust builder stage, then
run slim runtime images as a non-root user. The compose files set
`no-new-privileges`, drop Linux capabilities for app containers, and mount
`/tmp` as writable tmpfs while keeping the container filesystem read-only.

These compose files are local/demo operational references. Do not treat them as
managed production infrastructure, and do not add cloud resources to the fixed
release gate without explicit review.

## Placement profile rollout

Placement configs live in `configs/ranking/placement.*.yaml`.

- `placement.home.yaml`
- `placement.search.yaml`
- `placement.detail.yaml`
- `placement.mypage.yaml`

Startup validation is strict. Unknown keys, missing placement files, invalid ratios, or impossible caps fail the process before the API starts serving.

When diversity caps remove otherwise high-scoring candidates, the top-level recommendation explanation calls out the affected cap family, such as same-school, same-group, or content-kind caps. This is intentionally result-level rather than a score component: the candidates are still scored deterministically first, then the final display list is shaped by policy.

Score component reason codes are cataloged in
[REASON_CATALOG.md](REASON_CATALOG.md). When adding a ranking component, add its
feature, stable reason code, and public label before using it in explanation
text.

## Replay evaluation

Replay recent persisted recommendation traces against the current SQL-only
ranking path:

```bash
cargo run -p cli -- replay evaluate --limit 20
```

Fail the command when any trace differs from the current deterministic output:

```bash
cargo run -p cli -- replay evaluate --limit 20 --fail-on-mismatch
```

Replay evaluation compares fallback stage and item order. Use it after ranking
profile changes, explanation integrity changes, event CSV imports, and snapshot
refreshes. Full-mode/OpenSearch comparison remains a separate compatibility
flow; replay evaluation intentionally exercises the SQL-only reference path.

## Event CSV import

Import operational events:

```bash
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
```

Operational notes:

- raw input is checksum-staged under `.storage/raw/event-csv/...`
- import audit is written to `import_runs`, `import_run_files`, and `import_reports`
- `starts_at` accepts ISO-8601 date (`YYYY-MM-DD`) or RFC3339 timestamp and is stored as `TIMESTAMPTZ` after import
- repeated import of the same logical `event-csv` source is idempotent, even when the CSV file path changes
- rows no longer present in the same logical `event-csv` source are marked `is_active = false`

Inspect recent event imports:

```sql
SELECT id, source_id, status, total_rows, started_at, completed_at
FROM import_runs
WHERE source_id = 'event-csv'
ORDER BY id DESC
LIMIT 20;

SELECT
    import_file.import_run_id,
    import_file.logical_name,
    import_file.checksum_sha256,
    import_file.row_count,
    import_file.status
FROM import_run_files AS import_file
JOIN import_runs AS import_run ON import_run.id = import_file.import_run_id
WHERE import_run.source_id = 'event-csv'
ORDER BY import_file.id DESC
LIMIT 20;

SELECT
    import_report.import_run_id,
    import_report.level,
    import_report.code,
    import_report.message,
    import_report.row_count
FROM import_reports AS import_report
JOIN import_runs AS import_run ON import_run.id = import_report.import_run_id
WHERE import_run.source_id = 'event-csv'
ORDER BY import_report.id DESC
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
- target `fixture_path` entries let `crawler doctor` verify parser shape for
  parser-only or policy-blocked sources without turning live crawl into a
  release requirement
- crawl output lands in `events` with `source_type = 'crawl'`
- `crawler health` summarizes recent run state without needing ad hoc SQL first
- `crawler doctor` checks robots/terms redirects, content types, parser registration, school presence, and target `expected_shape` before you chase a live failure
- `crawler dry-run` reuses the latest fetched raw content to show predicted `parsed / deduped / imported / inactive` counts without mutating core events
- `crawler doctor`, `crawler dry-run`, and `crawler health` print a `promotion_gate` line so operators can separate `ready`, `review`, and `blocked` states before marking a source `live_ready`
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
- `promotion_gate`, including blocker and review reasons for live promotion decisions
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

```bash
cargo run -p cli -- jobs list --limit 20
```

The SQL equivalent is:

```sql
SELECT id, job_type, status, attempts, max_attempts, last_error, run_after, completed_at
FROM job_queue
ORDER BY id DESC
LIMIT 20;
```

Inspect queue pressure by type:

```sql
SELECT
    job_type,
    status,
    COUNT(*) AS job_count,
    MIN(run_after) AS oldest_run_after,
    MAX(updated_at) AS latest_update
FROM job_queue
GROUP BY job_type, status
ORDER BY job_type ASC, status ASC;
```

Inspect a job and its attempts before changing anything:

```bash
cargo run -p cli -- jobs inspect --id 123
```

The SQL equivalent is:

```sql
SELECT id, job_type, payload, status, attempts, max_attempts, locked_by, locked_at,
       last_error, run_after, completed_at, created_at, updated_at
FROM job_queue
WHERE id = 123;

SELECT attempt_number, status, error_message, started_at, finished_at
FROM job_attempts
WHERE job_id = 123
ORDER BY attempt_number ASC;
```

Inspect running jobs that may have been orphaned by a stopped worker:

```sql
SELECT id, job_type, attempts, max_attempts, locked_by, locked_at,
       NOW() - locked_at AS locked_for, last_error
FROM job_queue
WHERE status = 'running'
ORDER BY locked_at ASC, id ASC;
```

Running jobs are recovered by the next worker claim after the stale lock window.
The current stale lock window is 15 minutes. Start a bounded worker drain to
trigger recovery and process due work:

Inspect running or retryable jobs across the public-MVP queue:

```sql
SELECT id, job_type, status, attempts, max_attempts, locked_by, locked_at, run_after, last_error
FROM job_queue
WHERE status IN ('queued', 'running', 'failed')
ORDER BY
  CASE status WHEN 'running' THEN 0 WHEN 'queued' THEN 1 ELSE 2 END,
  run_after ASC,
  id ASC
LIMIT 50;
```

```bash
cargo run -p worker -- run-once --max-jobs 50
```

Recovery behavior:

- queued jobs whose `run_after` has passed are claimable by the worker
- jobs whose latest attempt failed are requeued automatically until `attempts >= max_attempts`
- running jobs with locks older than the stale-lock timeout are reclaimed by the next worker claim when attempts remain
- exhausted stale jobs are marked `failed` with the last error preserved

If a transient dependency failure is fixed and a job is already `failed`, queue
one more attempt without losing the attempt history:

```bash
cargo run -p cli -- jobs retry --id 123
```

The SQL equivalent is:

```sql
UPDATE job_queue
SET status = 'queued',
    max_attempts = GREATEST(max_attempts, attempts + 1),
    run_after = NOW(),
    locked_at = NULL,
    locked_by = NULL,
    completed_at = NULL,
    last_error = NULL,
    updated_at = NOW()
WHERE id = 123
  AND status = 'failed';
```

If a retry is deliberately delayed but the dependency is now healthy, make only
that queued job due:

```bash
cargo run -p cli -- jobs due --id 123
```

The SQL equivalent is:

```sql
UPDATE job_queue
SET run_after = NOW(),
    updated_at = NOW()
WHERE id = 123
  AND status = 'queued'
  AND run_after > NOW();
```

Do not mark jobs `succeeded` manually. That hides missing side effects such as
snapshot rows, cache deletion, or projection documents. Either fix the
dependency and queue another attempt, or leave the failed row as the audit trail.

In `sql_only` mode, `sync_candidate_projection` should not be required. If that
job appears after a mode switch, either run recovery under the intended
full-mode environment or leave OpenSearch recovery outside the public-MVP gate.

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
- full-mode candidate retrieval sorts direct-station candidates before same-line neighbors, then by school-station distance, walking minutes, school id, and station id to match the SQL-only candidate slice before ranking
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

Inspect snapshot coverage and freshness:

```sql
SELECT
    (SELECT COUNT(*) FROM schools) AS school_count,
    (SELECT COUNT(*) FROM popularity_snapshots) AS popularity_snapshot_count,
    (SELECT COUNT(*) FROM area_affinity_snapshots) AS area_snapshot_count,
    (SELECT MAX(refreshed_at) FROM popularity_snapshots) AS latest_popularity_refresh,
    (SELECT MAX(refreshed_at) FROM area_affinity_snapshots) AS latest_area_refresh;

SELECT COUNT(*) AS user_affinity_rows,
       MAX(refreshed_at) AS latest_user_affinity_refresh
FROM user_affinity_snapshots;
```

Snapshot recovery order:

1. Confirm tracking input exists:

   ```sql
   SELECT event_type, COUNT(*) AS event_count
   FROM user_events
   GROUP BY event_type
   ORDER BY event_type ASC;
   ```

2. Rebuild global popularity and area snapshots with the current ranking config:

   ```bash
   cargo run -p cli -- snapshot refresh
   ```

3. If user-affinity snapshots need a full rebuild, enqueue a worker job with an
   empty payload and drain it:

   ```bash
   cargo run -p cli -- jobs enqueue \
     --job-type refresh_user_affinity_snapshot \
     --payload '{}'
   ```

   ```bash
   cargo run -p worker -- run-once --max-jobs 10
   ```

4. If only one user's affinity snapshot is stale, keep the recovery scoped:

   ```bash
   cargo run -p cli -- jobs enqueue \
     --job-type refresh_user_affinity_snapshot \
     --payload '{"user_id":"manual-user-1"}'
   ```

After a manual user-affinity rebuild, invalidate recommendation cache if Redis is
configured. User-affinity is read at recommendation time, so cached personalized
responses can otherwise stay stale until TTL expiry. Projection sync is not
needed for user-affinity-only recovery because OpenSearch projection documents
do not include user-affinity fields.

## Cache invalidation

Recommendation cache keys include:

- profile version
- algorithm version
- retrieval mode
- candidate limit
- fallback `neighbor_distance_cap_meters`
- serialized request hash

Placement remains part of the serialized request payload, so cache entries stay separated per placement. Changing the retrieval window or fallback neighbor-distance cap also produces a different cache key, so rollout changes do not wait for TTL expiry before taking effect.

Inspect Redis cache keys:

```bash
redis-cli -u "$REDIS_URL" --scan --pattern 'geo-line-ranker:recommendations:*' | head -20
```

Invalidate recommendation cache through the same worker path the API uses:

```bash
cargo run -p cli -- jobs enqueue \
  --job-type invalidate_recommendation_cache \
  --payload '{"scope":"recommendations"}'
```

```bash
cargo run -p worker -- run-once --max-jobs 10
```

When you are already rebuilding global snapshots, prefer:

```bash
cargo run -p cli -- snapshot refresh
```

That command recalculates global snapshots, invalidates recommendation cache
when Redis is configured, and runs projection sync in full mode.
In SQL-only mode, projection counts in the command summary should stay `0`.

Because Redis is cache only, cache loss must not affect correctness; it should
only remove the fast path until responses are warmed again.

## Projection sync

OpenSearch remains optional candidate retrieval for `full` mode. Projection sync
is only required for `CANDIDATE_RETRIEVAL_MODE=full`, and SQL-only mode does not
need OpenSearch for correctness. Keep these commands outside the public-MVP gate
unless the operating profile changes.

Start the optional full-mode services before rebuilding or syncing projection:

```bash
docker compose -f .docker/docker-compose.full.yaml up -d postgres redis opensearch
```

Inspect the PostgreSQL source row count:

```sql
SELECT COUNT(*) AS projection_source_rows
FROM school_station_links AS link
INNER JOIN schools AS school
  ON school.id = link.school_id
INNER JOIN stations AS station
  ON station.id = link.station_id;
```

Inspect the OpenSearch index:

```bash
curl -s "$OPENSEARCH_URL/$OPENSEARCH_INDEX_NAME/_count"
```

Sync the projection after import, fixture, or snapshot changes:

```bash
cargo run -p cli -- projection sync
```

If the index mapping is missing or the document count is clearly wrong, rebuild
the index from PostgreSQL:

```bash
cargo run -p cli -- index rebuild
```

Worker recovery for projection jobs:

- Only run `sync_candidate_projection` jobs with worker environment set to full
  mode. In SQL-only mode the worker intentionally fails that job because no
  projection sync client is configured.
- After OpenSearch is reachable again, queue one more attempt for the failed job
  using the job recovery SQL above, then drain due work:

  ```bash
  cargo run -p cli -- jobs retry --id 123
  ```

  ```bash
  cargo run -p worker -- run-once --max-jobs 10
  ```

- If freshness matters more than preserving the exact queued job, run projection
  sync directly and leave the failed worker row as the audit trail:

  ```bash
  cargo run -p cli -- projection sync
  ```

When `CANDIDATE_RETRIEVAL_MODE=sql_only`, `/readyz` should report OpenSearch as
`disabled` and projection sync is not part of launch readiness.
