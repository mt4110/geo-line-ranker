# MySQL Compatibility

PostgreSQL/PostGIS is the reference implementation. MySQL remains optional and
experimental unless a future change adds a complete write-path adapter and
verification flow.

## Current Boundary

- PostgreSQL/PostGIS is required for the committed API, worker, migrations,
  import audit, trace, and job queue flows.
- MySQL is not part of the fixed public-MVP gate.
- SQLite must remain a read-only artifact/export target only.
- Redis remains cache only.
- OpenSearch remains optional candidate retrieval only in `full` mode.

## Before Adding MySQL Write Support

Any PR that introduces or changes MySQL write behavior must include all of the
following in the same change:

- exact local setup and migration commands
- CI coverage or an explicit CI-equivalent command
- compatibility notes for geo distance, timestamp, JSON, and transaction
  semantics
- rollback guidance back to PostgreSQL/PostGIS
- proof that SQL-only minimal mode still works without MySQL

## Compatibility Checklist

- Geography: document how PostGIS `geography(Point, 4326)` behavior is mapped.
- Time: preserve `TIMESTAMPTZ` semantics and UTC comparisons.
- JSON: preserve `JSONB` payload behavior used by traces, jobs, and import
  reports.
- Locks: preserve worker job claiming semantics equivalent to
  `FOR UPDATE SKIP LOCKED`.
- Ranking: final ranking remains in Rust and never moves to the database
  adapter or frontend.

Until those requirements exist in-tree, MySQL docs should describe status and
constraints only, not readiness.
