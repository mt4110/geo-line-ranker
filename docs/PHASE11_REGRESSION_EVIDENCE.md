# Phase 11 Regression Evidence

Use this checklist when a PR changes operator feedback, runbooks, imports,
snapshots, jobs, or data quality checks.

For release candidate readiness, use
[PUBLIC_MVP_RELEASE_READINESS.md](PUBLIC_MVP_RELEASE_READINESS.md) as the
top-level evidence bundle and use this file only for the operator feedback or
data quality regression portion.

## Required Validation

Run the stable workspace checks:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
just mvp-acceptance
```

When the PR touches Phase 11 guardrails or operator docs, also run the read-only
data quality pass against a bootstrapped PostgreSQL database:

```bash
just mvp-up
just mvp-bootstrap
just data-quality-doctor
just mvp-down
```

The data quality doctor prints review items instead of repairing data. Include
the output summary in the PR when it explains a follow-up issue, but do not add
OpenSearch, live crawling, or managed infrastructure to the public-MVP gate.

CI runs the same read-only data quality doctor as a separate job. That job is
operator-quality evidence; it fails on doctor warnings such as unreachable
PostgreSQL or failed SQL, while nonzero review items remain classification
signals. It does not change the fixed `just mvp-acceptance` release gate.

## PR Evidence

Summarize the evidence in the PR body:

- public-MVP boundary stayed `sql_only` + `event-csv` + PostgreSQL/PostGIS +
  Redis
- operator feedback or data quality invariant changed
- read-only command or SQL that reveals the invariant
- before/after result, or why the change is documentation-only
- required validation commands and outcomes
- optional full-mode or crawler notes, clearly labeled outside the MVP gate

Use [OPTIONAL_EVIDENCE_INTAKE.md](OPTIONAL_EVIDENCE_INTAKE.md) when those
optional notes need packet and lane selection, confirm the lane in
[OPTIONAL_EVIDENCE_GRADUATION.md](OPTIONAL_EVIDENCE_GRADUATION.md), then attach
the matching packet from
[OPTIONAL_EVIDENCE_PACKETS.md](OPTIONAL_EVIDENCE_PACKETS.md).

## Review Guardrails

Ask for review before merging when:

- a crawler source is promoted toward `source_maturity = live_ready`
- full-mode comparison becomes automated beyond manual evaluation
- incident recovery requires a new write path
- ranking output changes for the same input, config, and data
- public API shape changes and requires `schemas/openapi.json` plus
  `API_SPEC.md`

Avoid mixing those decisions into a small operator feedback PR. Keep the work
reviewable and rerunnable.
