# Profile Packs

Profile packs describe which deterministic recommendation profile owns a demo
path, source mapping, reason labels, and operating assumptions. They are local
manifests, not runtime plugins.

For the broader docs map, start with [Documentation Index](README.md). Profile
authors should use this document before changing profile manifests, fixture
ownership, source mappings, or profile-owned reason labels. For the first local
run, use [First 15 Minutes](FIRST_15_MINUTES.md) and
[Quickstart](QUICKSTART.md) first.

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

Use the profile CLI when you only need the profile-pack surface:

```bash
cargo run -p cli -- profile list
cargo run -p cli -- profile validate
cargo run -p cli -- profile inspect --profile-id local-discovery-generic
```

`profile list`, `profile validate`, and `profile inspect` reuse the same
profile-pack contract checks without changing the meaning of `config lint`,
which remains the combined active ranking config and profile-pack lint command.

## Runtime Selection

API, worker, and ranking/fixture-consuming CLI commands select
`local-discovery-generic` by default:

```bash
PROFILE_ID=local-discovery-generic
PROFILE_PACKS_DIR=configs/profiles
```

Runtime selection uses `PROFILE_PACKS_DIR` together with `PROFILE_ID`. When
`PROFILE_PACKS_DIR` is a directory, a local profile registry discovers
`profile.yaml` manifests and selects the manifest whose `profile_id` matches
`PROFILE_ID`; the conventional committed layout remains
`configs/profiles/<profile_id>/profile.yaml`. If `PROFILE_PACKS_DIR` points
directly to a `profile.yaml` file, that manifest is used directly. The registry
is only local manifest discovery and selection. It is not a plugin ABI, dynamic
loader, marketplace, or remote package source.

The selected manifest then provides the runtime defaults for
`ranking_config_dir` and the selected fixture path. Setting either
`RANKING_CONFIG_DIR` or `FIXTURE_DIR` keeps legacy path mode: the explicit
directory is used, the other directory falls back to its built-in default, and
startup does not require profile pack IO. `PROFILE_FIXTURE_SET_ID` is optional;
when omitted, the first fixture declared by the selected profile is used.
Profiles may omit fixtures for ranking-only runtimes. Commands that consume
fixtures require either a selected profile fixture or an explicit `FIXTURE_DIR`.

CLI commands that do not consume ranking configs or fixtures, such as
`migrate`, explicit-manifest imports, `derive`, `index`, `projection`, and
`jobs`, avoid profile pack IO. Crawler commands also avoid profile pack IO
because crawler manifests carry their own source inputs.

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
