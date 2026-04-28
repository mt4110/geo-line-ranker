# v0.2.1 to v0.2.5 Design Plan

This plan scopes the next patch-sized work before the v0.3.0 context-first
platform work. The underlying design notes name v0.2.1 as hardening and v0.2.2
as profile-pack foundation. This public plan splits that work into smaller
GitHub-friendly slices through v0.2.5.

This is a planning surface, not a compatibility promise. Each implementation PR
still owns its exact code, docs, fixtures, and validation evidence.

## Goals

- Make the repository easier to change safely without changing ranking
  semantics.
- Keep the SQL-only PostgreSQL/PostGIS path first-class and deterministic.
- Add clear contributor, CI, config, and docs contracts before broadening the
  engine surface.
- Establish profile-pack boundaries so school/event JP remains a reference
  profile rather than core schema.
- Keep public API changes additive, explicit, and synchronized with OpenAPI and
  `API_SPEC.md` when needed.

## Non-Goals

- No ranking formula changes.
- No fallback ladder semantic changes.
- No new required public endpoints.
- No AI/ML, embeddings, or vector search.
- No final ranking logic in the frontend.
- No MySQL parity claim.
- No SQLite primary write store or Redis durable state.
- No dynamic plugin ABI, marketplace, or runtime code loading.
- No mandatory crawler path.
- No managed production infrastructure.
- No v0.3.0 context resolver implementation in this batch.
- No v0.3.1 evaluation platform or v0.4.0 compatibility registry work in this
  batch.

## Release Slices

| Slice | Theme | Primary outcome | Public API policy |
|---|---|---|---|
| v0.2.1 | Module boundary hardening | Large modules are split into reviewable files with behavior preserved. | No public API changes |
| v0.2.2 | Contributor workflow and CI | Local commands, docs routing, Node/TS/example build checks, docs checks, and OpenAPI drift checks are visible. | No public API changes |
| v0.2.3 | Config and versioning contract | Config files are versioned, can be linted, and are documented with a deprecation policy. | No public API changes |
| v0.2.4 | Profile-pack contract | The extension boundary, manifest shape, reason layering, and validation commands are documented and scaffolded. | Additive only |
| v0.2.5 | Reference profile separation | School/event JP starts moving behind the profile boundary, with a generic demo path kept small and deterministic. | Additive only |

## PR Breakdown

### v0.2.1: Module Boundary Hardening

Candidate PRs:

- Split `apps/crawler/src/lib.rs` into command, manifest, report, and shared
  modules.
- Split `crates/storage-postgres/src/lib.rs` into pool, migrations, and
  repository modules.
- Split `crates/ranking/src/lib.rs` into planning, scoring, fallback,
  diversity, explanation, profile, and feature modules.
- Split `apps/api/src/lib.rs` and `apps/cli/src/lib.rs` only after the higher
  risk library splits are stable.

Exit signals:

- Existing behavior for `/v1/recommendations`, `/v1/track`, replay evaluation,
  SQL-only quickstart, and fixture seeding is unchanged.
- Mechanical splits avoid public type churn unless a smaller internal module
  boundary requires it.
- No generated OpenAPI diff unless the PR explicitly explains why.

### v0.2.2: Contributor Workflow and CI

Candidate PRs:

- Add or normalize `just setup`, `just dev`, `just smoke`, `just docs`,
  `just eval`, and `just ci-local`.
- Add a contributor touch map that tells maintainers which docs, tests, and
  examples to update for common change types.
- Add CI coverage for the TypeScript SDK build and example frontend build.
- Add OpenAPI drift checking to primary CI.
- Add docs link checking or a documented local substitute if the first pass must
  be advisory.

Exit signals:

- A new contributor can find setup, smoke, test, and docs entry points from
  `README.md` or `docs/README.md`.
- CI shows Rust, Node/TS, OpenAPI drift, and docs checks as separate concerns.
- SQL-only minimal mode remains the baseline path.

### v0.2.3: Config and Versioning Contract

Candidate PRs:

- Add `schema_version` and `kind` to ranking, fallback, placement, tracking, and
  crawler manifests where applicable.
- Add a `cli config lint` command or equivalent command grouping for local and
  CI use.
- Add strict unknown-key validation after current configs are updated.
- Add `docs/VERSIONING.md` and `docs/DEPRECATION_POLICY.md`.
- Document reason-code stability and alias requirements for future renames.

Exit signals:

- Config loading failures are explicit and actionable.
- Existing configs pass the lint command.
- Public docs explain API field removal, config key removal, and reason-code
  rename policy.

### v0.2.4: Profile-Pack Contract

Candidate PRs:

- Add `docs/PROFILE_PACKS.md` with the public profile-pack concepts:
  `profile_id`, supported content kinds, context inputs, fallback policy, reason
  catalog, connector references, and evaluation hooks.
- Add a manifest schema draft for `profile_pack`.
- Add `cli profile list`, `cli profile validate`, and `cli profile inspect`
  scaffolding if it can be done without pulling school/event logic into core.
- Define reason catalog layering between core reason codes and profile-local
  templates.

Exit signals:

- Profile-pack authoring has a single documented starting point.
- Validation failures are deterministic and local.
- No runtime dynamic loading or marketplace assumptions are introduced.

### v0.2.5: Reference Profile Separation

Candidate PRs:

- Introduce a `school-event-jp` reference profile directory or equivalent
  internal boundary without changing public recommendation behavior.
- Add a small `local-discovery-generic` example path only where it demonstrates
  that core is not school-specific.
- Move profile-specific reasons and placement policy toward profile-owned files.
- Document connector manifest expectations and profile-side mapping.
- Keep article support reserved unless a profile explicitly implements it.

Exit signals:

- School/event JP remains the best maintained reference profile, but core does
  not gain new school-specific assumptions.
- Generic demo fixtures run without live crawler access.
- Connector output can be described as canonical records plus profile-side
  mapping.

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Mechanical file splits accidentally change behavior. | Keep split PRs narrow, run the full Rust validation set, and avoid semantic edits in the same PR. |
| Strict config validation breaks local startup unexpectedly. | Update existing configs first, add a lint path, then enable stricter runtime behavior with clear errors. |
| Profile-pack work becomes too abstract. | Start from the existing school/event JP behavior and one minimal generic demo, not a large plugin framework. |
| Public API fields drift from generated OpenAPI. | Treat OpenAPI as the source of truth and make drift checking a release gate. |
| Optional systems become accidental requirements. | Keep Redis cache-only, OpenSearch candidate-retrieval-only, and crawler allowlist-only and optional in every slice. |
| MySQL support is overstated. | Keep MySQL documented as optional and experimental until local and CI verification commands exist. |

## Verification Policy

Default validation for every code PR:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
git diff --check
```

Reference PostgreSQL path when storage, import, API, worker, or CLI behavior is
touched:

```bash
docker compose -f .docker/docker-compose.yaml up -d postgres redis
cargo run -p cli -- migrate
cargo run -p cli -- seed example
```

JP source verification when JP rail, postal, school, geodata, or station-link
behavior is touched:

```bash
cargo run -p cli -- fixtures generate-demo-jp
cargo run -p cli -- import jp-rail --manifest storage/sources/jp_rail/example.yaml
cargo run -p cli -- import jp-postal --manifest storage/sources/jp_postal/example.yaml
cargo run -p cli -- import jp-school-codes --manifest storage/sources/jp_school/example.yaml
cargo run -p cli -- import jp-school-geodata --manifest storage/sources/jp_school_geo/example.yaml
cargo run -p cli -- derive school-station-links
```

Node and frontend checks once v0.2.2 CI work lands:

```bash
(cd packages/ts-sdk && npm install && npm run build)
(cd examples/frontend-next && npm install && npm run build)
```

Public API changes must update both generated OpenAPI and `API_SPEC.md` in the
same PR. Freshness language must stay precise: say "latest available MLIT N02
snapshot", not broad live-data wording.
