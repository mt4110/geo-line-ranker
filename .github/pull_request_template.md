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

## Optional Evidence Intake

- [ ] Not applicable.
- [ ] Optional evidence issue is linked.
- [ ] Minimal intake header from
      [OPTIONAL_EVIDENCE_INTAKE.md](docs/OPTIONAL_EVIDENCE_INTAKE.md#minimal-intake-header)
      is pasted below or in the linked issue.
- [ ] Triage lane, optional labels, owner, recheck command, and close condition
      from [OPTIONAL_EVIDENCE_TRIAGE.md](docs/OPTIONAL_EVIDENCE_TRIAGE.md)
      are recorded below or in the linked issue.
- [ ] Matching packet from
      [OPTIONAL_EVIDENCE_PACKETS.md](docs/OPTIONAL_EVIDENCE_PACKETS.md)
      is pasted when the evidence type needs one.

```text
Optional evidence intake:
- Evidence type:
- Evidence source:
- Packet template used:
- Decision lane:
- Owner:
- Recheck date:
- Recheck command:
- Close condition:
- Issue or PR:
```

## Validation

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace`
- [ ] `just mvp-acceptance`
- [ ] `DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor`
- [ ] `git diff --check`
