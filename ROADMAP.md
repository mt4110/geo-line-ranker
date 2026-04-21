# Roadmap

## Current

- Phase 10: post-launch operator hardening
  - keep the public MVP fixed to `sql_only`, `event-csv`, PostgreSQL/PostGIS, and Redis
  - add incident-friendly diagnosis and recovery guidance without expanding the MVP gate
  - make snapshot refresh, job retry, cache invalidation, and event-csv replay easier to inspect under pressure
  - evaluate optional full-mode retrieval only as an operator comparison path, not as launch acceptance
  - clarify crawler manifest graduation criteria while keeping crawling optional

## Phase 10 Exit Gates

- Post-launch runbook gives one-page guidance for sql_only incidents, event-csv replay, snapshot refresh, job retry, cache invalidation, and `/readyz`.
- A read-only doctor command can collect environment, readiness, snapshot, queue, import, and crawl state without mutating PostgreSQL, Redis, OpenSearch, or staged raw files.
- Optional full-mode comparison remains documented as evaluation only and is not added to `just mvp-acceptance`.
- Crawler manifest graduation is documented with explicit source-policy, robots/terms, parser-health, and rollback checks before `source_maturity = live_ready`.
- `just mvp-acceptance` continues to pass locally and in CI against PostgreSQL/PostGIS and Redis only.
- Public API shape is unchanged. If that changes, `schemas/openapi.json` and `API_SPEC.md` must be updated in the same change.
- Freshness language stays precise: use "latest available MLIT N02 snapshot", not real-time railway wording.
- No cloud production resources, managed infrastructure, ML/embeddings/vector search, mandatory crawling, or frontend final-ranking changes are introduced.

## Recently Completed

- Phase 9: production-readiness hardening for the April 30 public MVP launch
  - SQL-only minimal mode is the public-MVP release baseline
  - OpenSearch/full-mode and allowlist crawler flows stay optional operator workflows
  - public-MVP release gates, recovery runbooks, CI coverage, and launch checklist docs are tightened
  - PostgreSQL-heavy integration tests share a cross-binary lock through `crates/test-support`
- Phase 8: policy, diversity, crawler promotion, and operator recovery hardening
  - diversity caps now surface result-level impact in recommendation explanations
  - full-mode candidate retrieval is aligned with SQL-only ordering before ranking
  - crawler doctor, dry-run, and health outputs include a promotion gate for live-ready source decisions
  - CLI job operations cover list, inspect, retry, make due, and enqueue flows for operator recovery

## Next

- Later hardening after Phase 10
  - promote additional crawler manifests only after Phase 10 graduation evidence is available
  - consider broader full-mode automation only if operator comparisons show a clear need
  - add production hosting or managed infrastructure only through explicit review
