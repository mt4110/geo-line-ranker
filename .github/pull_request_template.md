## Summary

-

## Fixed Public MVP Boundary

This PR template is a review aid, not a new acceptance gate.

- [ ] `sql_only` remains the public MVP candidate retrieval path.
- [ ] `event-csv` remains the operational content path.
- [ ] PostgreSQL/PostGIS remains the write store.
- [ ] Redis remains the cache.
- [ ] `just mvp-acceptance` remains the six-case fixed gate.
- [ ] Crawler graduation, live crawler operation, full mode, OpenSearch, and
      managed infrastructure are not added to the fixed gate.
- [ ] Managed infrastructure, if discussed, remains explicit review only.
- [ ] Strict data-quality doctor output remains evidence, and `review_items`
      are classified by humans before issue or PR work.
- [ ] Public API shape is unchanged, or `schemas/openapi.json` and
      `API_SPEC.md` are updated in this PR.

## Validation

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `just test`
- [ ] `just test-cli-core` when CLI command surface or feature gates changed
- [ ] `just test-heavy` when storage-opensearch or mock compatibility behavior changed
- [ ] `just test-db` when PostgreSQL/Redis-backed behavior changed
- [ ] `cargo run -p cli -- config lint`
- [ ] `cargo run -p cli -- source-manifest lint`
- [ ] `cargo run -p cli -- fixtures doctor --path storage/fixtures/minimal`
- [ ] `cargo run -p cli -- fixtures doctor --path storage/fixtures/demo_jp`
- [ ] `cargo run -p crawler -- manifest lint`
- [ ] `cargo run -p cli -- eval golden`
- [ ] `just mvp-acceptance`
- [ ] `DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor`
- [ ] `git diff --check`
