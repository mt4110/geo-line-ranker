# v0.2.0 Design Notes

v0.2.0 moves `geo-line-ranker` from a station-id demo shape toward a context-first recommendation foundation for school and event discovery.

The runtime boundaries remain the same:

- final ranking stays in Rust
- PostgreSQL/PostGIS remains the reference implementation
- Redis is cache only
- OpenSearch is optional candidate retrieval only
- crawling stays optional and allowlist-bound
- AI, ML, embeddings, and vector search are not part of the system

## Implementation Scope

The v0.2.0 foundation adds:

- request `context` for station, line, and coarse area inputs
- `RankingContext` with source, confidence, privacy level, and warnings
- context resolution traces that do not store raw address or raw user id
- a fallback ladder: `strict_station`, `same_line`, `same_city`, `same_prefecture`, `neighbor_area`, `safe_global_popular`
- `candidate_counts` by fallback stage
- RFC3339 validation for tracking timestamps
- PostgreSQL migrations for `TIMESTAMPTZ`, areas, lines, user profile contexts, context traces, and source lineage
- crawler fetch hardening for redirect, response size, content type, and local/private host policy

## API Shape

`POST /v1/recommendations` keeps `target_station_id` compatibility and also accepts:

```json
{
  "context": {
    "line_name": "JR Yamanote Line",
    "area": {
      "prefecture_name": "Tokyo",
      "city_name": "Minato"
    }
  },
  "placement": "search",
  "limit": 3
}
```

`POST /v1/track` accepts `occurred_at` only as RFC3339 when present. Missing `occurred_at` remains backward compatible and is recorded as the current UTC time.

## Privacy

Ranking context stores coarse fields only: station id, line id/name, prefecture/city, and derived area identifiers. Raw addresses, names, emails, phone numbers, precise device GPS, and external account payloads must not be written into recommendation or context traces.

## Verification

Default validation remains:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```
