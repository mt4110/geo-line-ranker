# Roadmap

## Current

- Phase 8: complete and ready for PR review
  - diversity caps now surface their result-level impact in recommendation explanations
  - full-mode candidate retrieval is aligned with SQL-only ordering before ranking
  - crawler doctor, dry-run, and health outputs now include a promotion gate for live-ready source decisions
  - CLI job operations now cover list, inspect, retry, make due, and enqueue flows for operator recovery
  - CI and docs now cover the policy/diversity, crawler promotion, and worker recovery paths added in this phase

## Next

- Phase 9: production-readiness hardening after the Phase 8 PR
  - keep snapshot refresh, cache invalidation, projection sync, and worker jobs easy to inspect and recover through validated runbooks
  - broaden CI coverage only where it protects deterministic SQL-only behavior, full-mode parity, or operator recovery
