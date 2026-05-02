# Local Discovery Generic

This profile is the smallest deterministic demo path. It uses the committed
`storage/fixtures/minimal` dataset and no JP source adapters, live crawler
fetches, full mode, OpenSearch, or managed services.

For profile-pack authoring, start with
[Profile Packs](../../docs/PROFILE_PACKS.md). This example is the small
SQL-only baseline, not a separate authoring runbook.

## Profile Manifest

- `configs/profiles/local-discovery-generic/profile.yaml`
- `configs/profiles/local-discovery-generic/reasons.yaml`
- `storage/fixtures/minimal/fixture_manifest.yaml`

The current storage schema still names school and event records, but this path
keeps source assumptions generic: local fixture rows in, SQL-only ranking out.

## Quickstart

```bash
docker compose -f .docker/docker-compose.yaml up -d postgres redis
cargo run -p cli -- migrate
cargo run -p cli -- seed example
cargo run -p cli -- config lint
cargo run -p cli -- fixtures doctor --path storage/fixtures/minimal
```

Then run the API and try the request samples in this directory.

## Request Samples

- `requests/station.request.json`: station-first local discovery
- `requests/line.request.json`: line-first local discovery
- `requests/area.request.json`: area-first local discovery

## Guardrail

This profile should stay small and deterministic. Do not add live crawler,
OpenSearch, or JP adapter requirements to this generic demo path.
