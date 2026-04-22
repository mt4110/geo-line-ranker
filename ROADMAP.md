# Roadmap

## Current

- Phase 12: public MVP release readiness
  - keep the April 30 public MVP release gate fixed to `sql_only`, `event-csv`, PostgreSQL/PostGIS, and Redis
  - make release candidate evidence repeatable across CI results, local validation, data quality doctor output, and residual risk review
  - connect the Phase 11 operator feedback loop to release candidate and post-release decisions
  - keep live crawler, full-mode retrieval, OpenSearch, and managed infrastructure outside the release gate
  - make the `sql_only` public MVP baseline visible in release notes and operator handoff docs

## Phase 12 Exit Gates

- `ROADMAP.md` names Phase 12 as current and moves Phase 11 into Recently Completed.
- Release readiness guidance gives one path from release candidate prep to evidence capture, release notes, and post-launch follow-up.
- `MVP_ACCEPTANCE.md`, `OPERATIONS.md`, `POST_LAUNCH_RUNBOOK.md`, and `TESTING.md` point to the Phase 12 readiness flow.
- `just mvp-acceptance` remains fixed to the six public-MVP cases and only requires PostgreSQL/PostGIS plus Redis.
- `just data-quality-doctor` is release candidate evidence for human classification; it does not expand the public-MVP acceptance gate.
- Live crawler, full mode, OpenSearch, and managed infrastructure remain optional evidence or future review items, not release blockers for the fixed MVP profile.
- Release notes and handoff docs explicitly describe the public MVP baseline as `sql_only` plus `event-csv`.
- CI and local validation commands are documented with matching names and outcomes.
- Public API shape is unchanged. If that changes, `schemas/openapi.json` and `API_SPEC.md` must be updated in the same change.
- Freshness language stays precise: use "latest available MLIT N02 snapshot", not real-time railway wording.
- No cloud production resources, managed infrastructure, ML/embeddings/vector search, mandatory crawling, or frontend final-ranking changes are introduced.

## Recently Completed

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

- Later hardening after Phase 12
  - promote additional crawler manifests only after release readiness and graduation evidence is available
  - consider broader full-mode automation only if operator comparisons show a clear need
  - add production hosting or managed infrastructure only through explicit review
