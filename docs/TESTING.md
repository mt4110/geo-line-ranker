# Testing

## Default validation

Run the full Rust validation set from the repository root:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo run -p cli -- config lint
cargo run -p cli -- source-manifest lint
cargo run -p cli -- fixtures doctor --path storage/fixtures/minimal
cargo run -p cli -- fixtures doctor --path storage/fixtures/demo_jp
cargo run -p crawler -- manifest lint
```

`config lint` covers both active ranking config and committed profile pack
manifests under `configs/profiles`.

When the local PostgreSQL container is memory constrained, the workspace test
can also be run with serialized Rust test execution:

```bash
RUST_TEST_THREADS=1 cargo test --workspace
```

## CI test matrix

Pull request CI keeps the static checks and test execution separate:

- `rust-quality`: formatting, clippy, config lint, manifest lint, and fixture
  doctor checks for the full workspace.
- `rust-unit-tests`: DB-free packages and the mock OpenSearch compatibility
  tests.
- `rust-postgres-tests`: PostgreSQL/Redis-backed shards for `api`, `cli`,
  `crawler`, and `worker` plus `storage-postgres`.
- `mvp-acceptance`: the public MVP acceptance flow.
- `data-quality-doctor`: the read-only data quality evidence pass.

Each `rust-postgres-tests` shard gets its own GitHub Actions PostgreSQL and
Redis services, and runs with `RUST_TEST_THREADS=1` inside the shard. This keeps
PR validation deterministic while avoiding one shared PostgreSQL instance being
hit by the entire workspace at once.

## Integration prerequisites

Phase 5 integration checks use PostgreSQL and Redis. The SQL-only vs full-mode compatibility test still uses a mock OpenSearch endpoint and does not require Docker.

```bash
docker compose -f .docker/docker-compose.yaml up -d postgres redis
cargo run -p cli -- migrate
cargo run -p cli -- seed example
```

The manual smoke sections below are intentionally broader than the public-MVP release gate. For the fixed six-case acceptance flow that excludes live crawler and `full` mode, use [MVP_ACCEPTANCE.md](MVP_ACCEPTANCE.md).

## Release Validation

Print the release readiness command plan first:

```bash
just release-readiness
```

Then run the local release candidate validation set:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo run -p cli -- config lint
cargo run -p cli -- source-manifest lint
cargo run -p cli -- fixtures doctor --path storage/fixtures/minimal
cargo run -p cli -- fixtures doctor --path storage/fixtures/demo_jp
cargo run -p crawler -- manifest lint
just mvp-acceptance
git diff --check
```

Then capture the required release evidence:

```bash
DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
```

`just mvp-acceptance` remains the six-case public-MVP release gate.
`just data-quality-doctor` is required evidence capture for operator review,
not an additional acceptance-gate case. Run it with
`DATA_QUALITY_FAIL_ON_WARNING=true` for release readiness so doctor warnings
fail the evidence step. It does not add live crawler, full mode, OpenSearch, or
managed infrastructure to the release gate.

For data quality review changes, run the read-only doctor against a
bootstrapped PostgreSQL database:

```bash
just mvp-up
just mvp-bootstrap
just data-quality-doctor
just mvp-down
```

The doctor prints review items for human classification and does not mutate
PostgreSQL, Redis, OpenSearch, staged raw files, or crawler state.

CI also runs this as a separate `data-quality-doctor` job. Keep it separate
from `mvp-acceptance` so data-quality evidence improves operator review
without expanding the public-MVP release gate. The CI job fails on doctor
warnings, while review items stay as human-classified evidence.

For later maintenance review, print the command plan:

```bash
just post-mvp-hardening
```

Without `just`:

```bash
./scripts/post_mvp_hardening.sh
```

For optional crawler, full-mode, OpenSearch, or managed infrastructure review,
start with the read-only
[Optional Evidence Handoff](OPTIONAL_EVIDENCE_HANDOFF.md), then print the
evidence command plan:

```bash
just optional-evidence-review
```

Without `just`:

```bash
./scripts/optional_evidence_review.sh
```

The handoff and command plan stay outside the release gate. Optional evidence
records, lifecycle index rows, inventory reports, and findings are review
inventory only; they do not add required labels, CI gates, live crawler checks,
OpenSearch checks, `full` mode checks, or managed infrastructure checks to the
fixed public-MVP validation path.

Use the maintenance command plans before follow-up PRs to keep the same
validation set visible:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo run -p cli -- config lint
cargo run -p cli -- source-manifest lint
cargo run -p cli -- fixtures doctor --path storage/fixtures/minimal
cargo run -p cli -- fixtures doctor --path storage/fixtures/demo_jp
cargo run -p crawler -- manifest lint
just mvp-acceptance
DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
git diff --check
```

Crawler graduation and full-mode evaluation remain outside the fixed
public-MVP gate. Evidence review command plans do not add crawler, full mode,
OpenSearch, or managed infrastructure to local or CI release gates.

## Local review evaluation

The self-hosted local review workflow still runs only on the repository's
explicit review label for non-draft PRs. The offline evaluation harness below
does not call network services; it checks artifact capture and deterministic
failure handling for local review trials. The operational workflow reuses the
same artifact writer after checking out the trusted base commit, so it does not
execute PR-head code while saving review evidence.

Run the built-in deterministic scenarios:

```bash
just local-review-eval
```

Without `just`:

```bash
python3 scripts/local_review_eval.py --self-test
```

Capture a specific PR diff and review output:

```bash
python3 scripts/local_review_eval.py \
  --diff /path/to/pr.diff \
  --review /path/to/review.md \
  --out-dir .storage/local_review_eval/manual \
  --force
```

The output directory contains `pr.diff`, `review.md`, `findings.jsonl`,
`manifest.json`, and `checksums.txt`. The manifest omits wall-clock timestamps
and absolute input paths so repeated runs with the same inputs and `--run-id`
produce the same artifact bytes.

Check the failure path without failing the surrounding shell command:

```bash
python3 scripts/local_review_eval.py \
  --scenario failure \
  --expect-failure \
  --out-dir .storage/local_review_eval/failure \
  --force
```

Failure artifacts include `error.json`, `manifest.json`, and `checksums.txt`.
This remains evaluation-only evidence and does not add a new public-MVP gate.

Inspect a saved artifact directory before opening the raw diff or review:

```bash
python3 scripts/local_review_eval.py \
  --inspect .storage/local_review/pr-123-456-1
```

With `just`:

```bash
just local-review-inspect .storage/local_review/pr-123-456-1
```

For human triage, add the safe next-step guide:

```bash
python3 scripts/local_review_eval.py \
  --inspect .storage/local_review/pr-123-456-1 \
  --triage
```

With `just`:

```bash
just local-review-triage .storage/local_review/pr-123-456-1
```

List saved artifact directories before choosing one to open:

```bash
python3 scripts/local_review_eval.py \
  --inventory .storage/local_review
```

With `just`:

```bash
just local-review-inventory .storage/local_review
```

Inspection is read-only. It checks that `manifest.json` has the expected
schema, status, artifact records, and deterministic summary; verifies every
recorded sha256 in `checksums.txt`; rejects absolute or parent-directory
artifact paths; rejects unexpected entries in the artifact directory; validates
`findings.jsonl` as JSON Lines; and checks `error.json` shape for failed or
skipped runs. The report prints only file names, sizes, checksum prefixes,
status, metadata keys, and error summary; it does not print the raw `pr.diff`
or `review.md` contents. The `--triage` view keeps the same safety boundary and
adds a short read order for `findings.jsonl`, `error.json`, manifest metadata,
and workflow logs before anyone opens raw artifacts.

The `--inventory` view scans only direct children of the supplied artifact root,
verifies each artifact directory with the same inspection checks, and prints a
small index with state, finding count, diff byte count, run id, safe first files,
metadata keys, and raw artifact file names when present. This is read-only
human handoff support: it is not an acceptance gate, release gate, CI gate,
required label setup, or replacement for the source artifact records. It does
not print raw `pr.diff` or `review.md` contents. If any root entry is not a
directory, is a symlink, or fails artifact inspection, the inventory still shows
the verified directories and exits non-zero with summarized invalid entry
reasons so untrusted manifest values are not echoed into the first-pass index.

The operational workflow stores local review artifacts under
`.storage/local_review/pr-<number>-<run-id>-<run-attempt>` and uploads that
directory as a GitHub Actions artifact. Completed runs include the bounded
`pr.diff`, raw `review.md`, parsed `findings.jsonl`, `manifest.json`, and
`checksums.txt`. Failed or skipped runs include `error.json`; if a failure
happens after review output was produced, the workflow keeps that `review.md`
and parsed findings while marking the run failed. Skipped oversized diffs omit
`pr.diff` rather than bypassing `MAX_DIFF_BYTES`.

When using the writer from workflow automation, pass `--artifact-root` together
with `--force` so forced cleanup stays confined to the local review artifact
tree:

```bash
python3 scripts/local_review_eval.py \
  --diff /path/to/pr.diff \
  --review /path/to/review.md \
  --out-dir .storage/local_review/pr-123-456-1 \
  --artifact-root .storage/local_review \
  --run-id pr-123-456-1 \
  --no-synthetic-inputs \
  --force
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

   curl -X POST http://127.0.0.1:4000/v1/track \
     -H "content-type: application/json" \
     -d '{"user_id":"manual-user-1","event_kind":"search_execute","target_station_id":"st_tamachi"}'
   ```

5. Confirm the queue drains and snapshot rows update:

   ```bash
   cargo run -p cli -- jobs list --limit 10
   ```

   ```sql
   SELECT job_type, status, attempts
   FROM job_queue
   ORDER BY id DESC
   LIMIT 10;

   SELECT school_id, popularity_score, total_events, search_execute_count
   FROM popularity_snapshots
   ORDER BY popularity_score DESC, school_id ASC;

   SELECT area, affinity_score, event_count, search_execute_count
   FROM area_affinity_snapshots
   ORDER BY affinity_score DESC, area ASC;
   ```

6. Tune `configs/ranking/tracking.default.yaml` if needed, restart the API and worker, then force a recalculation with the current config:

   ```bash
   cargo run -p cli -- snapshot refresh
   ```

7. Import the operational event CSV and confirm active rows update:

   ```bash
   cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
   ```

   ```sql
   SELECT id, title, placement_tags, is_active, source_key
   FROM events
   ORDER BY updated_at DESC, id ASC
   LIMIT 20;
   ```

8. Run the optional crawler and inspect audit tables:

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
   Full-mode retrieval should keep the same candidate-slice ordering as SQL-only mode before ranking: direct station first, then distance, walking minutes, school id, and station id.
