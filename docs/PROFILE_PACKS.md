# Profile Packs

Profile packs describe which deterministic recommendation profile owns a demo
path, source mapping, reason labels, and operating assumptions. They are local
manifests, not runtime plugins.

For the broader docs map, start with [Documentation Index](README.md).
Profile-pack authoring starts here: use this document before changing profile
manifests, fixture ownership, source mappings, or profile-owned reason labels.
For the first local run, use [First 15 Minutes](FIRST_15_MINUTES.md) and
[Quickstart](QUICKSTART.md) first.

## Current Profiles

| Profile | Compatibility | Manifest | Purpose |
|---|---|---|---|
| `local-discovery-generic` | `stable` | `configs/profiles/local-discovery-generic/profile.yaml` | Small SQL-only demo path backed by `storage/fixtures/minimal`. |
| `school-event-jp` | `reference` | `configs/profiles/school-event-jp/profile.yaml` | Maintained JP school/event reference profile backed by JP adapter fixtures and event CSV examples. |

## Authoring Flow

Use this order when adding or adjusting a profile pack:

1. Choose the closest baseline. Start from `local-discovery-generic` for a
   small SQL-only local discovery path, or compare against `school-event-jp`
   when the profile owns JP source manifests and event CSV examples.
2. Edit the local profile manifests under `configs/profiles/<profile_id>/`.
   A profile pack is a manifest contract, not a runtime plugin, marketplace
   package, dynamic loader, or remote code source.
3. Wire only the committed local evidence the profile owns: fixture manifests,
   source manifests, event CSV examples, optional crawler manifests, and
   profile-owned reason labels. Keep crawler output optional and reviewed.
4. Validate locally with the narrow profile CLI before widening the check:

   ```bash
   cargo run -p cli -- profile validate
   cargo run -p cli -- profile inspect --profile-id <profile_id>
   cargo run -p cli -- config lint
   ```

5. Update example README files only when their role, request samples, or data
   inputs change. Update versioning or testing docs only when the profile
   contract or validation workflow changes.

Validation failures should be deterministic and local. Do not add OpenSearch,
full mode, live crawler operation, managed infrastructure, runtime plugin ABI,
or marketplace assumptions to the profile-author starting path.

## Contract

Each `profile.yaml` declares:

- `schema_version`: profile pack document schema. The current generic boundary
  contract is `2`.
- `profile_id`: stable profile identifier.
- `compatibility_level`: profile support level, one of `reference`,
  `stable`, `experimental`, or `deprecated`.
- `default_locale`: selected reason-catalog locale when `reason_catalog`
  declares more than one locale file.
- `content_kinds`: profile-defined content kind registry. These are stable
  string identifiers, not a fixed core enum.
- `supported_content_kinds`: content kind refs the profile intentionally
  exposes. Each entry must exist in `content_kinds`.
- `context_inputs`: accepted context entry points such as station, line, area,
  or user profile.
- `placements`: placement surfaces the profile supports. The current runtime
  validates that the profile includes `home`, `search`, `detail`, and `mypage`
  because the active ranking config still requires those four placement files.
- `fallback_policy`: profile-side fallback policy name.
- `ranking_config_dir`: active ranking config used by the profile.
- `reason_catalog`: profile-owned reason labels and core/profile layering.
  The manifest accepts either a legacy single path or `locale_files`; when
  multiple locale files are declared, `default_locale` selects the runtime
  catalog path.
- `fixtures`: committed fixture sets that exercise the profile.
- `connectors`: optional normalized local connector manifest references. These
  are validated local references, not dynamic runtime connector loading.
- `evaluation`: optional committed evaluation references such as the golden
  scenario pack and an optional pairwise pack.
- `source_manifests`, `event_csv_examples`, and `optional_crawler_manifests`:
  source mapping references owned by the profile. These remain readable for
  current reference-profile docs and can coexist with `connectors`.
- `article_support`: `reserved` until article candidates are explicitly
  implemented by a profile.

For the current runtime, `article_support` must remain `reserved`. A profile
cannot expose `article` in `supported_content_kinds`, and the referenced
placement configs cannot mention `article` in enabled content kinds, score
boosts, or content-kind diversity ratios. The first article implementation
should update this contract together with the article read model, fixture
coverage, ranking tests, and any public API/OpenAPI docs required by the shape
change.

The linter checks schema version, kind, duplicate IDs, content-kind registry
syntax, supported content-kind refs, ranking-config content-kind refs,
placement declarations, path syntax, referenced files, fixture manifest
identity, compatibility level, the active ranking config, all declared reason
catalog locale files, connector manifest refs, and evaluation refs. For legacy
schema-2 manifests that omit `content_kinds`, the validator treats
`supported_content_kinds` as the inline registry; new profile packs should
declare `content_kinds` explicitly.

The manifest spec draft also sketches a nested fallback config object, richer
connector types, and per-profile evaluation packs. This repository has adopted
the local-reference contract above and the `eval golden --profile-id <id>`
execution path for `evaluation.scenario_pack`. When a profile declares
`evaluation.pairwise_pack`, the same golden runner loads those pairwise
expectations into the selected scenario run and reports the pack path in the
summary metadata. Pairwise packs use `schema_version: 1`,
`kind: replay_pairwise_pack`, and `expectations` entries keyed by
`scenario_id`. It has adopted profile-defined content-kind identifiers in
manifests, while the current ranking runtime still emits the implemented
school/event response shape. It has not adopted dynamic connector loading,
locale-specific explanation rendering, or a replacement ranking engine.

Compatibility levels are profile-pack contract labels, not storage parity
claims:

- `reference`: the canonical maintained profile for a domain or source family.
- `stable`: supported local contract for committed fixtures and authoring flows.
- `experimental`: allowed to change while evidence and docs settle.
- `deprecated`: kept readable while users migrate to another profile.

The current storage truth remains unchanged: PostgreSQL/PostGIS is the
reference implementation, OpenSearch and Redis are optional, MySQL remains
experimental until contract tests prove parity, and SQLite is an artifact/export
target only.

Storage compatibility has its own operator-facing status report:

```bash
cargo run -p cli -- doctor storage-compatibility
```

Use that command when reviewing storage/cache/index support levels. Use
`profile validate`, `doctor profile-pack`, or `doctor ranking-config` when
reviewing profile compatibility levels.

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
`profile inspect` also prints the selected runtime paths for ranking config,
reason catalog, and fixture set so local refs can be checked without changing
runtime semantics.

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
`ranking_config_dir`, the selected fixture path, and the profile-owned
`reason_catalog` path. Runtime selection resolves and validates the reason
catalog as a local file so later runtime code can use the selected profile's
local references without re-parsing the manifest. It does not change ranking
explanation semantics.

`eval golden` has a narrower profile-aware evaluation boundary. Without
`--profile-id` or `PROFILE_ID`, it keeps the historical
`configs/evaluation/scenarios` default. With a selected profile, it replays the
manifest's `evaluation.scenario_pack`, uses the profile's `ranking_config_dir`
unless `RANKING_CONFIG_DIR` or `--ranking-config-dir` overrides it, and includes
`profile_id`, `scenario_source`, and `scenario_path` in text and JSON summaries:

```bash
cargo run -p cli -- eval golden --profile-id school-event-jp
```

`--profiles-path` only chooses where the selected profile is loaded from; pair
it with `--profile-id` or `PROFILE_ID`.

`--scenario-path` is an explicit what-if override. It still keeps the selected
profile id and ranking config resolution, but it bypasses the manifest
`evaluation.scenario_pack` and does not load the manifest
`evaluation.pairwise_pack`. Use the profile manifest path for the complete
profile-owned evaluation contract.

Setting either `RANKING_CONFIG_DIR` or `FIXTURE_DIR` keeps legacy path mode:
the explicit directory is used, the other directory falls back to its built-in
default, and startup does not require profile pack IO. In that mode, the
profile-owned `reason_catalog` reference is also skipped and left unresolved
because no runtime profile is selected; runtime continues to use the built-in
ranking reason-code behavior instead of deriving reason labels from a manifest.
`PROFILE_FIXTURE_SET_ID` is optional; when omitted, the first fixture declared
by the selected profile is used. Profiles may omit fixtures for ranking-only
runtimes. Commands that consume fixtures require either a selected profile
fixture or an explicit `FIXTURE_DIR`.

CLI commands that do not consume ranking configs or fixtures, such as
`migrate`, explicit-manifest imports, `derive`, `index`, `projection`, and
`jobs`, avoid profile pack IO. Crawler commands also avoid profile pack IO
because crawler manifests carry their own source inputs.

## Boundary

Core ranking remains deterministic Rust over canonical records. A connector
produces canonical inputs such as stations, schools, events, and school-station
links; the profile pack explains why that source belongs to a profile and which
fixture path proves it locally. The domain crate now also exposes a minimal
generic boundary (`Entity`, `Occurrence`, `Candidate`,
`FeatureContribution`, and `ProfilePolicy`) so reference-profile records can be
mapped toward platform-level concepts without rewriting the current ranking
path in one step.

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
