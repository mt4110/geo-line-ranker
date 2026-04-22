# Post-launch Runbook

This runbook is the first stop when the public MVP is already deployed or being
validated after release. Keep the operating profile narrow unless an explicit
review changes it.

For release candidate preparation and the evidence bundle used before launch,
start with
[PUBLIC_MVP_RELEASE_READINESS.md](PUBLIC_MVP_RELEASE_READINESS.md). After
release, use this runbook for first response and feed follow-up work into
[OPERATOR_FEEDBACK_LOOP.md](OPERATOR_FEEDBACK_LOOP.md). For recurring
post-MVP evidence review, use
[POST_MVP_HARDENING.md](POST_MVP_HARDENING.md).

## Public MVP profile

- Candidate retrieval: `sql_only`
- Operational content path: `event-csv`
- Write store: PostgreSQL/PostGIS
- Cache: Redis only
- Optional workflows: OpenSearch full-mode comparison and allowlist crawling
- Release readiness flow:
  [PUBLIC_MVP_RELEASE_READINESS.md](PUBLIC_MVP_RELEASE_READINESS.md)
- Release gate: [MVP_ACCEPTANCE.md](MVP_ACCEPTANCE.md)

Do not add OpenSearch, live crawling, managed infrastructure, ML, embeddings, or
vector search to the MVP gate as part of incident recovery. Rail/station
freshness should be described as the "latest available MLIT N02 snapshot", not
as real-time railway data.

## First 10 minutes

1. Capture read-only state:

   ```bash
   ./scripts/post_launch_doctor.sh
   ./scripts/data_quality_doctor.sh
   ```

2. Check API readiness:

   ```bash
   curl -fsS http://127.0.0.1:4000/readyz
   ```

3. Classify the failure before changing anything:

   - API not ready: inspect database reachability first, then cache status.
   - Empty or stale recommendations: inspect snapshot coverage and recent event
     imports.
   - Personalized ranking drift: inspect `user_events`,
     `user_affinity_snapshots`, and pending snapshot jobs.
   - Worker backlog: inspect `job_queue` by status and job type.
   - Content mismatch after CSV import: inspect the latest `event-csv`
     `import_runs`, `import_run_files`, and `import_reports`.

4. Preserve the incident bundle:

   - doctor output
   - `/readyz` response
   - API and worker log tails
   - current `.env` keys with secrets removed
   - latest `event-csv` import run id and staged checksum
   - queue pressure grouped by job type and status
   - snapshot row counts and latest refresh timestamps
   - any crawl run ids only if the incident involves optional crawler output

5. Move follow-up work into the feedback loop:

   - use [OPERATOR_FEEDBACK_LOOP.md](OPERATOR_FEEDBACK_LOOP.md) to classify
     the finding
   - use [POST_MVP_HARDENING.md](POST_MVP_HARDENING.md) when the finding needs
     a blocker, accepted risk, follow-up, optional evidence only, or explicit
     review decision record
   - if the finding appears during release candidate validation, record the
     decision in the release readiness evidence bundle
   - keep the issue or PR scoped to one invariant and one root cause
   - use [PHASE11_REGRESSION_EVIDENCE.md](PHASE11_REGRESSION_EVIDENCE.md)
     for recheck evidence

## SQL-only readiness

`sql_only` is the correctness baseline. In this mode `/readyz` should report
OpenSearch as `disabled`; Redis may remove the fast path, but must not change
ranking correctness.

Minimal bootstrap remains:

```bash
docker compose -f .docker/docker-compose.yaml up -d postgres redis
cargo run -p cli -- migrate
cargo run -p cli -- seed example
cargo run -p cli -- snapshot refresh
```

Re-run the fixed acceptance gate before release decisions:

```bash
just mvp-acceptance
```

Without `just`:

```bash
./scripts/mvp_acceptance.sh
```

Run the Phase 11 read-only data quality pass when PostgreSQL is available:

```bash
just data-quality-doctor
```

Without `just`:

```bash
./scripts/data_quality_doctor.sh
```

## Event CSV replay

Use event-csv as the operational content repair path:

```bash
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
```

Then inspect:

```sql
SELECT id, source_id, status, total_rows, started_at, completed_at
FROM import_runs
WHERE source_id = 'event-csv'
ORDER BY id DESC
LIMIT 10;

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
LIMIT 10;

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
LIMIT 10;

SELECT id, school_id, title, event_category, is_active, source_key
FROM events
WHERE source_type = 'event_csv'
ORDER BY updated_at DESC, id ASC
LIMIT 20;
```

Replacement semantics are intentional: rows missing from the latest logical
`event-csv` source become inactive. If too much content disappeared, re-import
the last known complete CSV rather than manually editing `events`.

## Snapshot refresh

Rebuild global snapshots after tracking-weight changes, import repair, or
stale recommendation evidence:

```bash
cargo run -p cli -- snapshot refresh
```

Inspect coverage:

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

`snapshot refresh` invalidates recommendation cache when Redis is configured.
In `sql_only`, projection sync counts should remain zero.

## Job retry

List and inspect before retrying:

```bash
cargo run -p cli -- jobs list --limit 50
cargo run -p cli -- jobs inspect --id 123
```

Drain due work with a bounded worker run:

```bash
cargo run -p worker -- run-once --max-jobs 50
```

Retry a failed job only after the dependency is healthy:

```bash
cargo run -p cli -- jobs retry --id 123
```

Make a delayed queued job due when the delay is no longer needed:

```bash
cargo run -p cli -- jobs due --id 123
```

Do not manually mark jobs `succeeded`; that hides missing side effects and
breaks the audit trail.

## Cache invalidation

Redis is cache only. If cached recommendation responses are suspected stale,
invalidate through the same worker path the API uses:

```bash
cargo run -p cli -- jobs enqueue \
  --job-type invalidate_recommendation_cache \
  --payload '{"scope":"recommendations"}'

cargo run -p worker -- run-once --max-jobs 10
```

If snapshots are being rebuilt anyway, prefer `cargo run -p cli -- snapshot
refresh`.

## Optional full-mode comparison

Full mode is an evaluation path, not an MVP gate. Use it to compare retrieval
behavior after SQL-only health is established:

```bash
docker compose -f .docker/docker-compose.full.yaml up -d postgres redis opensearch
cargo run -p cli -- migrate
cargo run -p cli -- seed example
cargo run -p cli -- index rebuild
cargo run -p cli -- projection sync
```

Then compare against SQL-only outputs with the existing compatibility tests and
operator spot checks. Keep any findings as comparison notes unless the public
operating profile is explicitly changed. Use
[OPTIONAL_EVIDENCE_INTAKE.md](OPTIONAL_EVIDENCE_INTAKE.md) before turning
repeated full-mode comparison into an automation follow-up, confirm the lane in
[OPTIONAL_EVIDENCE_GRADUATION.md](OPTIONAL_EVIDENCE_GRADUATION.md), then record
the comparison with the packet template in
[OPTIONAL_EVIDENCE_PACKETS.md](OPTIONAL_EVIDENCE_PACKETS.md).

```bash
cargo test -p compatibility-tests --test sql_only_vs_full
```

## Crawler graduation checklist

Crawling remains optional. Move a manifest to `source_maturity: live_ready` only
after intake in [OPTIONAL_EVIDENCE_INTAKE.md](OPTIONAL_EVIDENCE_INTAKE.md), the
evidence packet in [OPTIONAL_EVIDENCE_PACKETS.md](OPTIONAL_EVIDENCE_PACKETS.md)
satisfies the graduation criteria in
[OPTIONAL_EVIDENCE_GRADUATION.md](OPTIONAL_EVIDENCE_GRADUATION.md), and all of
these are true:

- source policy and intended use are documented for the target domain
- robots and terms checks are current and do not block the target path
- parser output is deterministic, bounded, and mapped to the expected shape
- `crawler doctor` has no blockers and only accepted review notes
- `crawler dry-run` shows plausible parsed, deduped, imported, and inactive
  counts without mutating core events
- recent `crawler health` output shows no recurring fetch or parser failures
- matching `schools.id` rows exist before import
- rollback is clear: change the manifest back to `parser_only` or
  `policy_blocked`, then repair operational content through `event-csv`

Never make crawling mandatory for the public MVP path.
