# Roadmap

## Current

- Phase 9: production-readiness hardening for the April 30 public MVP launch
  - make SQL-only minimal mode the public-MVP release baseline
  - keep OpenSearch/full-mode and allowlist crawler flows optional operator workflows
  - tighten release gates, recovery runbooks, CI coverage, and launch checklist docs
  - build on Phase 8's validated policy, diversity, crawler-promotion, and job-recovery tooling without widening launch scope

## Phase 9 Exit Gates

- `just mvp-acceptance` passes locally and in CI against PostgreSQL/PostGIS and Redis only.
- SQL-only quickstart remains runnable from a clean checkout using `.env.example`, `migrate`, `seed example`, and `snapshot refresh`.
- `docs/MVP_ACCEPTANCE.md` is the fixed launch decision gate for the public MVP.
- `docs/OPERATIONS.md` contains ready-to-run examples for job recovery, snapshot refresh, cache invalidation, and optional full-mode projection sync.
- Public API shape is unchanged. If that changes, `schemas/openapi.json` and `API_SPEC.md` must be updated in the same change.
- Freshness language stays precise: use "latest available MLIT N02 snapshot", not real-time railway wording.
- No cloud production resources, managed infrastructure, ML/embeddings/vector search, mandatory crawling, or frontend final-ranking changes are introduced.

## Recently Completed

- Phase 8: policy, diversity, crawler promotion, and operator recovery hardening
  - diversity caps now surface result-level impact in recommendation explanations
  - full-mode candidate retrieval is aligned with SQL-only ordering before ranking
  - crawler doctor, dry-run, and health outputs include a promotion gate for live-ready source decisions
  - CLI job operations cover list, inspect, retry, make due, and enqueue flows for operator recovery

## Next

- Post-launch hardening after the public MVP
  - evaluate optional full-mode retrieval under real operator needs
  - graduate additional crawler manifests only after source policy and parser health are clear
  - add production hosting or managed infrastructure only through explicit review
