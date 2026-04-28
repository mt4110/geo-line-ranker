# Versioning

This project keeps versioned boundaries explicit so deterministic behavior can
be changed deliberately and reviewed locally.

## Public API

The HTTP API contract is documented in `API_SPEC.md` and generated in
`schemas/openapi.json`.

Public API changes must update both files in the same change. Public response
field removals and reason-code renames follow `docs/DEPRECATION_POLICY.md`.

## Ranking Config

Active ranking config files in `configs/ranking` use `schema_version` plus
the file-specific `kind` from the table below. Example:

```yaml
schema_version: 1
kind: ranking_schools
```

The supported ranking config kinds are:

| Kind | Active files |
|---|---|
| `ranking_schools` | `schools.default.yaml` |
| `ranking_events` | `events.default.yaml` |
| `ranking_fallback` | `fallback.default.yaml` |
| `ranking_tracking` | `tracking.default.yaml` |
| `ranking_placement` | `placement.home.yaml`, `placement.search.yaml`, `placement.detail.yaml`, `placement.mypage.yaml` |

Ranking config loading is strict:

- `schema_version` must be supported by the current binary.
- `kind` must match the expected active file role.
- Unknown keys fail config loading.
- Existing ranking formulas and fallback semantics are not changed by config
  metadata.

Run the active ranking config lint locally:

```bash
cargo run -p cli -- config lint
```

Use a custom directory when testing migrations:

```bash
cargo run -p cli -- config lint --path /path/to/ranking-config
```

`configs/ranking/fallback.v020.yaml` is a legacy reference artifact, not part of
the active runtime profile set.

## Source Manifests

Import source manifests in `storage/sources` use:

```yaml
schema_version: 1
kind: import_source
manifest_version: 1
```

Crawler source manifests in `configs/crawler/sources` use:

```yaml
schema_version: 1
kind: crawler_source
manifest_version: 1
```

`schema_version` and `kind` identify the document shape. `manifest_version`
remains the source-authored manifest revision recorded in import and crawler
audit tables. Committed manifests must declare these fields explicitly so lint
can catch partially migrated files.

Import source lint also checks that committed manifest file paths resolve to
local CSV files. Crawler manifest lint checks parser registration,
`expected_shape` compatibility, and any declared target fixture paths without
fetching live content.

Run import source manifest lint locally:

```bash
cargo run -p cli -- source-manifest lint
```

Run crawler manifest lint locally:

```bash
cargo run -p crawler -- manifest lint
```

## Fixture Sets

Committed fixture directories carry `fixture_manifest.yaml`:

```yaml
schema_version: 1
kind: fixture_set
manifest_version: 1
```

The fixture manifest records each fixture file's relative path, format,
checksum, and row count. `manifest_version` is the source-authored revision for
the fixture set; bump it when the fixture contract changes.

Run fixture doctor locally:

```bash
cargo run -p cli -- fixtures doctor --path storage/fixtures/minimal
cargo run -p cli -- fixtures doctor --path storage/fixtures/demo_jp
```

## Reason Codes

Reason codes are a public explanation surface. New codes may be added, but
renames must either keep an alias or follow the deprecation policy. The current
catalog lives in `docs/REASON_CATALOG.md`.

## Generated Artifacts

Generated artifacts must carry enough manifest and checksum information to
identify their inputs. A deterministic rerun with the same input, config, and
data should produce the same output.
