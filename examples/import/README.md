# Import examples

Generate or refresh the small Japanese demo fixture:

```bash
cargo run -p cli -- fixtures generate-demo-jp
```

Run the Phase 2 import path end to end:

```bash
cargo run -p cli -- import jp-rail --manifest storage/sources/jp_rail/example.yaml
cargo run -p cli -- import jp-postal --manifest storage/sources/jp_postal/example.yaml
cargo run -p cli -- import jp-school-codes --manifest storage/sources/jp_school/example.yaml
cargo run -p cli -- import jp-school-geodata --manifest storage/sources/jp_school_geo/example.yaml
cargo run -p cli -- derive school-station-links
```

Import the Phase 5 operational event CSV:

```bash
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
```

Run the optional Phase 6 allowlist crawler:

```bash
cargo run -p crawler -- fetch --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/custom_example.yaml
```

Inspect the audit trail:

```sql
SELECT id, source_id, status, total_rows, started_at, completed_at
FROM import_runs
ORDER BY id DESC;

SELECT import_run_id, logical_name, checksum_sha256, row_count, status
FROM import_run_files
ORDER BY id DESC;

SELECT id, source_id, parser_key, status, fetched_targets, parsed_rows, imported_rows
FROM crawl_runs
ORDER BY id DESC;
```
