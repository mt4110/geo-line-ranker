# Deprecation Policy

This policy covers public API fields, config keys, source manifest keys, and
reason codes.

## Principles

- Prefer additive changes.
- Fail explicitly instead of silently changing ranking behavior.
- Keep SQL-only PostgreSQL/PostGIS behavior first-class.
- Do not claim freshness beyond the latest available source snapshot.
- Keep removal and rename migrations documented in the same change that starts
  the deprecation.

## Public API

Public API removals require:

- A documented replacement or a clear statement that no replacement exists.
- An update to `API_SPEC.md`.
- A synchronized `schemas/openapi.json` update.
- A migration note in the release or PR description.

Additive fields are allowed, but they still need generated OpenAPI and
`API_SPEC.md` updates.

## Config And Manifest Keys

Config and manifest schemas are strict at load time. Unknown keys fail so typos
do not become silent behavior.

When a key is renamed:

- Add the replacement first when backward compatibility is practical.
- Update committed configs and manifests in the same change.
- Document the old key, new key, and removal target.
- Keep errors actionable when an unsupported schema or key is used.

When a key is removed without compatibility:

- Bump or reject the relevant `schema_version`.
- Explain the migration path in docs.
- Keep the change separate from ranking formula or fallback semantic changes.

## Reason Codes

Reason codes are part of the explanation contract. A rename must either:

- Keep the old code as an alias, or
- Document the old code, new code, and migration window before removal.

Reason-code removals should be rare. If a score component disappears because the
underlying feature is removed, document the behavior change and update
`docs/REASON_CATALOG.md`.

## Current v0.2.3 Contract

- Active ranking config files require `schema_version: 1` and a matching
  `kind`.
- Import and crawler manifests carry `schema_version: 1` plus `kind`, while
  keeping `manifest_version` for audit history.
- Local lint commands cover the active config and manifest sets:

```bash
cargo run -p cli -- config lint
cargo run -p cli -- source-manifest lint
cargo run -p crawler -- manifest lint
```
