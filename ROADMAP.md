# Roadmap

## Current

- Phase 7: config-driven search signal calibration and operator hardening on top of the merged baseline
  - `search_execute` now feeds snapshot weights through station-linked schools
  - snapshot recalculation, cache invalidation, and projection sync now have an operator entry point

## Next

- Phase 8: policy, diversity, and operator hardening after Phase 7 search signal rollout
  - keep SQL-only and full-mode behavior aligned while tuning explanations, policy, and diversity caps
  - tighten projection, cache, and crawler runbooks around the now-merged `main` line
