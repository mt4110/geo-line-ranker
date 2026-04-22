# School Event JP Reference

This profile shows how to use `geo-line-ranker` for school and event discovery without hardcoding school-specific rules into the generic ranking core.

## Data Inputs

Use the existing demo fixtures and source manifests:

- `storage/fixtures/demo_jp/jp_rail_stations.csv`
- `storage/fixtures/demo_jp/jp_postal_codes.csv`
- `storage/fixtures/demo_jp/jp_school_codes.csv`
- `storage/fixtures/demo_jp/jp_school_geodata.csv`
- `examples/import/events.sample.csv`
- `storage/sources/jp_rail/example.yaml`
- `storage/sources/jp_postal/example.yaml`
- `storage/sources/jp_school/example.yaml`
- `storage/sources/jp_school_geo/example.yaml`

## Quickstart

```bash
cargo run -p cli -- fixtures generate-demo-jp
cargo run -p cli -- import jp-rail --manifest storage/sources/jp_rail/example.yaml
cargo run -p cli -- import jp-postal --manifest storage/sources/jp_postal/example.yaml
cargo run -p cli -- import jp-school-codes --manifest storage/sources/jp_school/example.yaml
cargo run -p cli -- import jp-school-geodata --manifest storage/sources/jp_school_geo/example.yaml
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
cargo run -p cli -- derive school-station-links
cargo run -p cli -- snapshot refresh
```

Then run the API and try the request samples in this directory.

## Request Samples

- `requests/area.request.json`: area-first recommendation
- `requests/line.request.json`: line-first recommendation
- `requests/station.request.json`: station compatibility request
- `requests/new-user-fallback.request.json`: new user context fallback
- `requests/event-placement.request.json`: event-oriented placement

## Guardrail

The expected behavior is context-first and deterministic. A strong Hokkaido context must not prioritize Okinawa merely because an Okinawa school has high global popularity.
