# Roadmap

## Current

- Phase 13: post-MVP hardening
  - move from "can we publish?" to "can we keep improving safely after publishing?"
  - connect release readiness evidence to post-launch triage, data quality review, and follow-up decisions
  - keep the public MVP gate fixed to `sql_only`, `event-csv`, PostgreSQL/PostGIS, and Redis
  - keep crawler graduation, full-mode evaluation, and managed infrastructure outside the fixed gate as optional evidence or explicit review paths
  - make doctor `review_items` easy to classify as blocker, accepted risk, or follow-up

## Phase 13 Exit Gates

- `ROADMAP.md` names Phase 13 as current and moves Phase 12 into Recently Completed.
- Phase 12 readiness docs point clearly into post-MVP hardening, post-launch triage, and the operator feedback loop.
- The public MVP gate remains `sql_only` + `event-csv` + PostgreSQL/PostGIS + Redis.
- `just mvp-acceptance` remains the fixed six-case public-MVP gate.
- `DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor` remains strict release and post-MVP evidence; doctor `review_items` are classified by humans as blocker, accepted risk, or follow-up.
- Crawler graduation evidence and full-mode evaluation are documented outside the public MVP gate.
- Managed infrastructure, OpenSearch, live crawler operation, ML/embeddings/vector search, and frontend final-ranking changes are not fixed-gate requirements.
- CI and local validation commands stay visibly aligned in docs and command-plan scripts.
- Public API shape is unchanged. If that changes, `schemas/openapi.json` and `API_SPEC.md` must be updated in the same change.
- Freshness language stays precise: use "latest available MLIT N02 snapshot", not real-time railway wording.

## Recently Completed

- Phase 12: public MVP release readiness
  - release readiness guidance keeps the April 30, 2026 public MVP release gate fixed to `sql_only`, `event-csv`, PostgreSQL/PostGIS, and Redis
  - release candidate evidence is repeatable across CI results, local validation, strict data quality doctor output, and residual risk review
  - the Phase 11 operator feedback loop is connected to release candidate and post-release decisions
  - live crawler, full-mode retrieval, OpenSearch, and managed infrastructure remain outside the release gate
  - release notes and operator handoff docs make the `sql_only` public MVP baseline visible
- Phase 11: operator feedback loop and data quality guardrails
  - operator feedback guidance documents the evidence bundle, incident classification, issue/PR granularity, and recheck loop after a post-launch finding
  - read-only data quality doctor surfaces review items across event-csv imports, school event coverage, station link coverage, snapshots, logical sources, and queue pressure without mutating PostgreSQL, Redis, OpenSearch, or staged raw files
  - regression evidence guidance explains how PRs show that operator quality did not regress
  - optional full-mode comparison remains documented as evaluation only and is not added to `just mvp-acceptance`
  - crawler manifest graduation remains an operator decision path; crawling is not made mandatory for launch or incident recovery
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

- Later hardening after Phase 13
  - promote additional crawler manifests only after post-MVP graduation evidence is reviewed
  - consider broader full-mode automation only if operator comparisons show a clear need
  - add production hosting or managed infrastructure only through explicit review
