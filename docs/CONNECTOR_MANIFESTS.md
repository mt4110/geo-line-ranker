# Connector Manifest Schemas

Connector manifests are stable local contracts for profile-owned source
references. They describe how a profile points at committed source evidence and
which deterministic importer or crawler command may handle that evidence. They
are not plugin packages, remote connector code, dynamic loading hooks, or an
arbitrary field-mapping runtime.

The current stable contract version reported by doctor commands is
`local_stable_connector_manifest_schema_v1`.

## Stable Matrix

| Profile connector type | Source class | Manifest kind | Manifest schema version | `source_id` scope | `field_mapping` scope | Doctor lint | Runtime boundary |
|---|---|---|---|---|---|---|---|
| `source_manifest` | `csv_import` | `import_source` | `1` | Required in manifest; optional profile override must match | Not supported | `source_manifest_lint` | `import profile-source` delegates to the existing source manifest import path |
| `csv_import` | `csv_import` | `csv_file` | None; raw CSV file | Required in profile connector | `event_v1` required for runtime | `file_reference` | Event CSV one-shot import |
| `ndjson_import` | `ndjson_import` | `ndjson_file` | None; raw NDJSON file | Required in profile connector | `event_v1` required for runtime | `file_reference` | Event NDJSON one-shot import |
| `archive_source` | `archive_import` | `archive_source` | `1` | Required in manifest; optional profile override must match | `event_v1` required for runtime | `archive_source_lint` | Local archive unpack, then event CSV/NDJSON import |
| `crawler_manifest` | `html_crawl` | `crawler_source` | `1` | Required in manifest; optional profile override must match | Not supported | `crawler_manifest_lint` | Crawler commands only; no profile-source import execution |

All rows are local-reference only. Dynamic loading is disabled. Live fetch is
not enabled by the profile boundary. `crawler_manifest` also carries an
allowlist-required safety flag because crawl execution must go through crawler
policy checks.

## Validation

Profile validation resolves connector paths and checks the stable matrix:

- YAML-backed connector refs must declare the expected `schema_version` and
  `kind`.
- CSV and NDJSON refs must point directly at files with matching extensions.
- YAML-backed `source_id` values come from the referenced manifest; a profile
  override is allowed only when it matches that value exactly.
- File import `source_id` values are declared by the profile connector.
- `field_mapping` refs may be parsed as portable identifiers, but only
  `event_v1` is executable by the current import runtime.
- Archive manifests must declare a same-directory archive path, checksum, and
  bounded archive entries. The current runtime imports exactly one `events`
  logical entry after unpacking.

`doctor profile-pack` reports the stable schema contract version and every
schema row. `doctor ingest-quality` reports the same schema contract plus actual
coverage counts for source classes, manifest kinds, manifest schema versions,
field mappings, archive shape, crawler shape, safety flags, and the run-lineage
fields available on `import_runs` / `crawl_runs`. Both are
DB-free by default and make no live crawl requests; the profile-pack doctor with
`--persist` additionally writes profile registry evidence to PostgreSQL.

```bash
cargo run -p cli -- doctor profile-pack
cargo run -p cli -- doctor ingest-quality
```

## Run Lineage

`import profile-source --source-id ...` upserts the selected profile manifest
into `profile_pack_manifest_lineage`, then records the import run with nullable
lineage columns: `profile_id`, `profile_manifest_lineage_id`,
`connector_type`, `source_class`, `manifest_kind`, `manifest_schema_version`,
`field_mapping`, and `lineage_evidence`. Existing explicit import commands keep
those columns empty.

`crawl_runs` carries the same nullable columns for future profile-aware crawler
entry points, but crawler execution still goes through the existing crawler
commands and allowlist policy. This is evidence plumbing only; it does not add
dynamic loading, arbitrary mapping execution, or live feed behavior.

## Deferred

The stable schema contract intentionally does not implement dynamic connector
loading, arbitrary field mapping execution, arbitrary content-kind execution,
live feed fetching, or MySQL write support.
