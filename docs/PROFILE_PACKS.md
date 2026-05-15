# Profile Packs

Profile packs describe which deterministic recommendation profile owns a demo
path, source mapping, reason labels, and operating assumptions. They are local
manifests, not runtime plugins.

For the broader docs map, start with [Documentation Index](README.md).
Profile-pack authoring starts here: use this document before changing profile
manifests, fixture ownership, source mappings, or profile-owned reason labels.
For the first local run, use [First 15 Minutes](FIRST_15_MINUTES.md) and
[Quickstart](QUICKSTART.md) first. For the connector schema matrix shared by
profile validation and doctor output, use
[Connector Manifest Schemas](CONNECTOR_MANIFESTS.md).

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
  string identifiers, not a fixed core enum. A profile may declare future or
  domain-specific identifiers here before the current runtime can execute them.
- `supported_content_kinds`: content kind refs the profile intentionally
  exposes to the active runtime. Each entry must exist in `content_kinds`.
  Today only `school` and `event` are executable; other declared kinds must
  remain registry-only until their read model, ranking behavior, and response
  shape are implemented.
- `context_inputs`: accepted context entry points such as station, line, area,
  or user profile.
- `placements`: placement surfaces the profile supports. The current runtime
  validates that the profile includes `home`, `search`, `detail`, and `mypage`
  because the active ranking config still requires those four placement files.
- `fallback_policy`: profile-side fallback policy. The current manifest accepts
  either a legacy policy name or a nested `config_file` reference. The nested
  form is resolved and validated as a local runtime file reference; ranking
  still uses the active `ranking_config_dir` fallback weights.
- `ranking_config_dir`: active ranking config used by the profile.
- `reason_catalog`: profile-owned reason labels and core/profile layering.
  The manifest accepts either a legacy single path or `locale_files`; when
  multiple locale files are declared, `default_locale` selects the runtime
  catalog path. The selected catalog overlays labels used in runtime
  explanations while preserving the stable public `reason_code` values.
- `fixtures`: committed fixture sets that exercise the profile.
- `connectors`: optional registry-facing local connector references. Supported
  connector types are `source_manifest`, `csv_import`, `ndjson_import`,
  `archive_source`, and `crawler_manifest`. File and archive import connectors
  must declare a portable `field_mapping` ref. The manifest contract can parse
  future mapping refs, but the current import runtime executes only the
  deterministic `field_mapping: event_v1` mapping. Validation resolves each
  manifest path, derives source class, manifest kind, profile compatibility,
  field mapping, and safety metadata, and keeps the references local. This is
  not dynamic runtime connector loading or arbitrary mapping execution.
  `doctor profile-pack` and `doctor ingest-quality` report the stable connector
  schema contract version, currently
  `local_stable_connector_manifest_schema_v1`, plus each supported connector
  type's source class, manifest kind, manifest schema version, source-id scope,
  field-mapping boundary, lint path, runtime boundary, and safety flags.
- `evaluation`: optional committed evaluation references such as the golden
  scenario pack and an optional pairwise pack.
- `source_manifests`, `event_csv_examples`, and `optional_crawler_manifests`:
  source mapping references owned by the profile. These remain readable for
  current reference-profile docs and can coexist with `connectors`.
- `article_support`: `reserved` until article candidates are explicitly
  implemented by a profile.

For the current runtime, `article_support` must remain `reserved`. A profile
may declare `article` in `content_kinds` as a registry-only future kind, but it
cannot expose `article` in `supported_content_kinds`, and the referenced
placement configs cannot mention `article` in enabled content kinds, score
boosts, or content-kind diversity ratios. `profile validate`, `profile
inspect`, and `doctor profile-pack` report the runtime-executable content kinds
and registry-only kinds. The first article implementation should update this
contract together with the article read model, fixture coverage, ranking tests,
and any public API/OpenAPI docs required by the shape change.

The linter checks schema version, kind, duplicate IDs, content-kind registry
syntax, supported content-kind refs, runtime-executable content-kind boundary,
ranking-config content-kind refs,
placement declarations, path syntax, referenced files, fixture manifest
identity, compatibility level, the active ranking config, all declared reason
catalog locale files, connector manifest refs, connector type / manifest-kind
consistency, file-import field mapping refs, current import-runtime executable
mapping boundaries, and evaluation refs.
`source_manifest` refs must point to YAML with `kind: import_source`,
`archive_source` refs must point to YAML with `kind: archive_source`,
`crawler_manifest` refs must point to YAML with `kind: crawler_source`,
`csv_import` refs must point to a CSV file, and `ndjson_import` refs must point
to an NDJSON file. CSV/NDJSON file import refs must declare both a profile
`source_id` and a portable field mapping ref. Archive import refs must point to
a local archive manifest whose archive path stays beside the manifest, whose
checksum is checked by `doctor ingest-quality`, whose listed CSV/NDJSON entries
stay inside the archive, and whose unpacked files stay within bounded size
limits. Connector `source_id` values use the same portable lowercase letters,
digits, and hyphens rule as `profile_id`; `field_mapping` refs use lowercase
letters, digits, underscores, and hyphens, with no leading or trailing
separator. Only `field_mapping: event_v1` is executable today. Unsupported
mapping refs fail `profile validate`, `doctor profile-pack`, and
`import profile-source` instead of being treated as partial support.
YAML-backed connector refs must also declare `schema_version: 1`; CSV and
NDJSON refs are raw files and therefore report `manifest_schema_version: none`.
Optional `source_id` values on YAML-backed connectors must match the referenced
manifest's `source_id`. For legacy schema-2 manifests that omit
`content_kinds`, the validator treats `supported_content_kinds` as the inline
registry; new profile packs should declare `content_kinds` explicitly.

The manifest spec draft also sketches richer connector families and per-profile
evaluation packs. This repository has adopted the local-reference contract
above, a small source connector registry metadata surface, `csv_import` /
`ndjson_import` event file mapping refs, local archive-source manifests with an
explicit `event_v1` runtime boundary, the `import profile-source --source-id
<id>` path for profile-declared one-shot imports, selected-locale reason label
rendering, nested fallback config path validation, and the `eval golden
--profile-id <id>` execution path for `evaluation.scenario_pack`. When a
profile declares `evaluation.pairwise_pack`, the same golden runner loads those
pairwise expectations into the selected scenario run and reports the pack path
in the summary metadata. Pairwise packs use `schema_version: 1`, `kind:
replay_pairwise_pack`, and `expectations` entries keyed by `scenario_id`. It has
adopted profile-defined content-kind identifiers in manifests, while the
current ranking runtime still emits the implemented school/event response
shape. Non-executable identifiers are valid as registry entries, but exposing
one through `supported_content_kinds` is a validation/runtime error instead of
silent partial support. It has not adopted dynamic connector loading,
arbitrary field mapping execution, or a replacement ranking engine.

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

Run a profile-declared one-shot import by source id when you want the profile
manifest to choose the local source path and deterministic field mapping:

```bash
cargo run -p cli -- import profile-source --source-id event-ndjson
cargo run -p cli -- import profile-source --profile-id school-event-jp --source-id jp-rail
```

`profile list`, `profile validate`, and `profile inspect` reuse the same
profile-pack contract checks without changing the meaning of `config lint`,
which remains the combined active ranking config and profile-pack lint command.
`profile inspect` also prints the selected runtime paths for ranking config,
reason catalog, and fixture set so local refs can be checked without changing
runtime semantics.

`profile validate --persist` and `doctor profile-pack --persist` keep the same
validation behavior, then write the validated profile registry boundary to
PostgreSQL: `profile_registry`, `profile_pack_manifest_lineage`, and
`profile_compatibility_status`. Persistence is opt-in, requires migrated
PostgreSQL and `DATABASE_URL`, and does not make profile packs mandatory for
DB-free authoring commands.

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
`ranking_config_dir`, the selected fixture path, the optional nested fallback
config file path, and the profile-owned `reason_catalog` path. Runtime selection
resolves and validates these local file references. The selected reason catalog
is loaded by the API and profile-aware replay/evaluation paths so item and
top-level explanations render the profile's labels. Existing core score
features must keep their stable `reason_code`; a profile catalog can override
labels, not rewrite public reason-code identity.

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

`eval golden --persist` records the completed run in PostgreSQL
`evaluation_runs` and `evaluation_run_cases`. When a profile is selected, the
same run also upserts the profile manifest lineage and compatibility status so
the evaluation row can point at the manifest checksum that was tested. This is
historical evidence only; ranking inputs, weights, candidate retrieval, and
crawler behavior are unchanged.

Setting either `RANKING_CONFIG_DIR` or `FIXTURE_DIR` keeps legacy path mode:
the explicit directory is used, the other directory falls back to its built-in
default, and startup does not require profile pack IO. In that mode, the
profile-owned `reason_catalog` reference is also skipped and left unresolved
because no runtime profile is selected; runtime continues to use the built-in
reason label catalog instead of deriving labels from a manifest.
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
fixture path proves it locally. The domain crate exposes a minimal generic
boundary (`Entity`, `Occurrence`, `Candidate`, `FeatureContribution`, and
`ProfilePolicy`) plus a canonical ingest output boundary
(`CanonicalIngestOutput`, `CanonicalIngestRecord`, location context, and
lineage). These types let connector/profile/import responsibilities line up
without rewriting the current ranking path in one step.

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
