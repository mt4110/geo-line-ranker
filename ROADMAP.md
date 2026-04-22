# Roadmap

## Current

- Phase 11: operator feedback loop and data quality guardrails
  - keep the public MVP fixed to `sql_only`, `event-csv`, PostgreSQL/PostGIS, and Redis
  - turn post-launch doctor evidence into reproducible issue, PR, and recheck work
  - add read-only data quality guardrails for event-csv, schools, stations, links, snapshots, and jobs
  - keep optional full-mode retrieval and crawler graduation as evaluation evidence only
  - preserve the narrow public-MVP gate while improving operator handoff quality

## Phase 11 Exit Gates

- Operator feedback guidance documents the evidence bundle, incident classification, issue/PR granularity, and recheck loop after a post-launch finding.
- A read-only data quality doctor surfaces review items across event-csv imports, school event coverage, station link coverage, snapshots, logical sources, and queue pressure without mutating PostgreSQL, Redis, OpenSearch, or staged raw files.
- Regression evidence guidance explains how PRs show that operator quality did not regress.
- Optional full-mode comparison remains documented as evaluation only and is not added to `just mvp-acceptance`.
- Crawler manifest graduation remains an operator decision path; crawling is not made mandatory for launch or incident recovery.
- `just mvp-acceptance` continues to pass locally and in CI against PostgreSQL/PostGIS and Redis only.
- Public API shape is unchanged. If that changes, `schemas/openapi.json` and `API_SPEC.md` must be updated in the same change.
- Freshness language stays precise: use "latest available MLIT N02 snapshot", not real-time railway wording.
- No cloud production resources, managed infrastructure, ML/embeddings/vector search, mandatory crawling, or frontend final-ranking changes are introduced.

## Recently Completed

- Phase 10: post-launch operator hardening
  - post-launch runbook gives one-page guidance for sql_only incidents, event-csv replay, snapshot refresh, job retry, cache invalidation, and `/readyz`
  - read-only doctor command collects environment, readiness, snapshot, queue, import, and crawl state without mutating PostgreSQL, Redis, OpenSearch, or staged raw files
  - optional full-mode comparison remains documented as evaluation only and is not added to `just mvp-acceptance`
  - crawler manifest graduation is documented with explicit source-policy, robots/terms, parser-health, and rollback checks before `source_maturity = live_ready`
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

- Later hardening after Phase 11
  - promote additional crawler manifests only after Phase 11 feedback and graduation evidence is available
  - consider broader full-mode automation only if operator comparisons show a clear need
  - add production hosting or managed infrastructure only through explicit review
