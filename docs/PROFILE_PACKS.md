# Profile Packs

Profile packs describe which deterministic recommendation profile owns a demo
path, source mapping, reason labels, and operating assumptions. They are local
manifests, not runtime plugins.

## Current Profiles

| Profile | Manifest | Purpose |
|---|---|---|
| `local-discovery-generic` | `configs/profiles/local-discovery-generic/profile.yaml` | Small SQL-only demo path backed by `storage/fixtures/minimal`. |
| `school-event-jp` | `configs/profiles/school-event-jp/profile.yaml` | Maintained JP school/event reference profile backed by JP adapter fixtures and event CSV examples. |

## Contract

Each `profile.yaml` declares:

- `profile_id`: stable profile identifier.
- `supported_content_kinds`: content kinds the profile intentionally exposes.
- `context_inputs`: accepted context entry points such as station, line, area,
  or user profile.
- `fallback_policy`: profile-side fallback policy name.
- `ranking_config_dir`: active ranking config used by the profile.
- `reason_catalog`: profile-owned reason labels and core/profile layering.
- `fixtures`: committed fixture sets that exercise the profile.
- `source_manifests`, `event_csv_examples`, and `optional_crawler_manifests`:
  source mapping references owned by the profile.
- `article_support`: `reserved` until article candidates are explicitly
  implemented by a profile.

The linter checks schema version, kind, duplicate IDs, path syntax, referenced
files, fixture manifest identity, the active ranking config, and the profile
reason catalog.

```bash
cargo run -p cli -- config lint
```

## Runtime Selection

API, worker, and CLI commands select `local-discovery-generic` by default:

```bash
PROFILE_ID=local-discovery-generic
PROFILE_PACKS_DIR=configs/profiles
```

`PROFILE_ID` resolves `configs/profiles/<profile_id>/profile.yaml`, then uses
that manifest's `ranking_config_dir` and selected fixture path as runtime
defaults. `RANKING_CONFIG_DIR` and `FIXTURE_DIR` remain explicit overrides for
local experiments and compatibility with older runbooks.
`PROFILE_FIXTURE_SET_ID` is optional; when omitted, the first fixture declared
by the selected profile is used.

## Boundary

Core ranking remains deterministic Rust over canonical records. A connector
produces canonical inputs such as stations, schools, events, and school-station
links; the profile pack explains why that source belongs to a profile and which
fixture path proves it locally.

`school-event-jp` stays the best-maintained reference profile, but JP source
manifests and crawler examples live behind that profile boundary. The
`local-discovery-generic` profile keeps the default demo path small and does not
require JP adapters, live crawler access, full mode, OpenSearch, or managed
infrastructure.

## Fixture Link

Committed fixture manifests may include `profile_id`:

```yaml
schema_version: 1
kind: fixture_set
manifest_version: 2
fixture_set_id: minimal
profile_id: local-discovery-generic
```

`fixtures doctor` validates the profile id syntax when present while keeping
legacy fixture manifests readable.
