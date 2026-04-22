---
name: Optional evidence intake
about: Route optional evidence without expanding the fixed public MVP gate.
title: "[Optional evidence] "
---

This issue records optional evidence and routing. It is not an acceptance test
and does not add crawler graduation, live crawler operation, full mode,
OpenSearch, or managed infrastructure to the fixed public MVP gate.

Use with:

- [Optional evidence intake](docs/OPTIONAL_EVIDENCE_INTAKE.md)
- [Optional evidence packets](docs/OPTIONAL_EVIDENCE_PACKETS.md)

## Minimal Intake Header

```text
Optional evidence intake:
- Evidence type: doctor review item / crawler graduation / full-mode automation candidate / managed infrastructure explicit review / mixed
- Evidence source:
- Packet template used:
- Decision lane: optional evidence only / follow-up / crawler graduation / explicit review required
- Owner:
- Recheck date:
- Issue or PR:

Fixed public MVP boundary unchanged:
- sql_only candidate retrieval:
- event-csv operational content path:
- PostgreSQL/PostGIS write store:
- Redis cache:
- just mvp-acceptance remains six cases:
- crawler, full mode, OpenSearch, and managed infrastructure outside fixed gate:
- public API shape unchanged:

Strict doctor evidence:
- DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor:
- review_items classified before issue or PR work:
```

## Packet Template To Use

- [ ] Strict data-quality doctor review item:
      use the doctor review item in
      [Post-MVP hardening](docs/POST_MVP_HARDENING.md#review-item-template)
- [ ] Crawler graduation:
      paste the
      [Crawler Graduation Packet](docs/OPTIONAL_EVIDENCE_PACKETS.md#crawler-graduation-packet)
- [ ] Full-mode automation candidate:
      paste the
      [Full-Mode Automation Candidate Packet](docs/OPTIONAL_EVIDENCE_PACKETS.md#full-mode-automation-candidate-packet)
- [ ] Managed infrastructure explicit review:
      paste the
      [Managed Infrastructure Explicit Review Packet](docs/OPTIONAL_EVIDENCE_PACKETS.md#managed-infrastructure-explicit-review-packet)
- [ ] Mixed or unclear:
      split into one primary evidence type and one decision lane before
      implementation starts

## Fixed Boundary Check

- [ ] `sql_only` remains the public MVP candidate retrieval path.
- [ ] `event-csv` remains the operational content path.
- [ ] PostgreSQL/PostGIS remains the write store.
- [ ] Redis remains the cache.
- [ ] `just mvp-acceptance` remains the six-case fixed gate.
- [ ] Crawler graduation or packet completion does not add live crawler
      operation to the fixed gate.
- [ ] Full-mode automation evidence does not add full mode or OpenSearch to the
      fixed gate.
- [ ] Managed infrastructure remains explicit review only.
- [ ] Strict data-quality doctor output remains evidence, and `review_items`
      are classified by humans before issue or PR work.
- [ ] Public API shape is unchanged, or `schemas/openapi.json` and
      `API_SPEC.md` are updated in the same change.

## Recheck

- Recheck command or evidence source:
- Owner:
- Recheck date:
- Rollback path, if relevant:
