# Optional Evidence Packets

Start with
[OPTIONAL_EVIDENCE_INTAKE.md](OPTIONAL_EVIDENCE_INTAKE.md) when evidence first
arrives. Use these templates after intake has chosen the evidence type and
[OPTIONAL_EVIDENCE_GRADUATION.md](OPTIONAL_EVIDENCE_GRADUATION.md) has routed
optional crawler, full-mode, OpenSearch, or infrastructure evidence to a
decision lane. They are designed to be pasted into an issue, PR body, or
review note without turning the evidence into a fixed public MVP gate.

These packets are records, not acceptance tests. They do not add live crawler
operation, `full` mode, OpenSearch, or managed infrastructure to
`just mvp-acceptance`.

## Fixed Boundary

Every packet must preserve this public MVP boundary:

- Candidate retrieval: `sql_only`
- Operational content path: `event-csv`
- Write store: PostgreSQL/PostGIS
- Cache: Redis
- Acceptance gate: `just mvp-acceptance`

Strict data-quality doctor output remains evidence:

```bash
DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
```

Doctor warnings fail the evidence pass. Doctor `review_items` are still
classified by humans before issue or PR work starts.

If a packet proposes a public API shape change, update `schemas/openapi.json`
and `API_SPEC.md` in the same change. Otherwise, record that public API shape
is unchanged.

For the shortest issue, PR, or review-note preface, paste the minimal intake
header from
[OPTIONAL_EVIDENCE_INTAKE.md#minimal-intake-header](OPTIONAL_EVIDENCE_INTAKE.md#minimal-intake-header)
before the packet.

## Crawler Graduation Packet

Use this packet before moving a crawler source toward
`source_maturity: live_ready` or keeping a live source in live operation.
Crawler graduation remains outside the fixed public MVP gate.

```text
Optional evidence packet: crawler graduation

Source and scope:
- source_id:
- manifest path:
- source_maturity before:
- source_maturity requested:
- parser_key:
- expected_shape:
- target URLs:
- default school_id values:
- operator:
- review date:

Fixed public MVP boundary unchanged:
- sql_only candidate retrieval:
- event-csv operational content path:
- PostgreSQL/PostGIS write store:
- Redis cache:
- just mvp-acceptance remains six cases:
- live crawler operation outside the fixed gate:
- public API shape unchanged:

Policy and allowlist evidence:
- policy review date:
- terms URL:
- terms result:
- robots URL:
- robots result:
- allowlist.allowed_domains:
- allowlist.live_fetch_enabled:
- source policy notes:
- promotion_gate:

Parser and source evidence:
- parser registration confirmed:
- expected_shape matches target or fixture:
- fixture path or staged raw checksum:
- recent fetch checksum, if live content was inspected:
- parser test coverage:
- deterministic mapping notes:

Doctor evidence:
- command: cargo run -p crawler -- doctor --manifest <manifest>
- output excerpt:
- promotion_gate ready / review notes:
- blockers:

Dry-run evidence:
- command: cargo run -p crawler -- dry-run --manifest <manifest>
- parsed count:
- deduped count:
- imported count:
- inactive count:
- missing-school count:
- zero-row source accepted: yes / no / not applicable
- dry-run notes:

Health evidence:
- command: cargo run -p crawler -- health --manifest <manifest>
- fetch failures:
- robots failures:
- policy failures:
- parser failures:
- logical-name failures:
- recent failed runs:
- health summary:

Decision:
- optional evidence only / follow-up / crawler graduation / explicit review required:
- reason:
- owner:
- recheck date:
- issue or PR:

Rollback:
- rollback owner:
- rollback path to parser_only or policy_blocked:
- event-csv repair path if public content is affected:
- rollback verification command:
- rollback recheck date:
```

## Full-Mode Automation Candidate Packet

Use this packet when repeated SQL-only versus full-mode comparisons may justify
automation. Full mode and OpenSearch remain evaluation paths unless an explicit
review changes the public operating profile.

```text
Optional evidence packet: full-mode automation candidate

Scope:
- operator:
- review date:
- request corpus or sanitized payload source:
- environments compared:
- related issue or PR:

Fixed public MVP boundary unchanged:
- SQL-only remains the public MVP baseline:
- event-csv remains the operational content path:
- PostgreSQL/PostGIS remains the write store:
- Redis remains the cache:
- just mvp-acceptance remains six cases:
- full mode and OpenSearch outside the fixed gate:
- OpenSearch candidate retrieval only:
- final ranking remains in Rust:
- public API shape unchanged:

SQL-only comparison:
- command or request sample:
- response sample:
- ordering summary:
- missing candidate summary:
- latency notes:

Full-mode comparison:
- command or request sample:
- response sample:
- ordering summary:
- missing candidate summary:
- latency notes:

Projection and OpenSearch evidence:
- projection sync command:
- projection sync output:
- OpenSearch index health command:
- OpenSearch index health output:
- drift, lag, or stale index notes:

Compatibility evidence:
- command: cargo test -p compatibility-tests --test sql_only_vs_full
- output:
- fixture or payload reproducibility notes:

Automation candidate reason:
- repeated operator work this would remove:
- specific full-mode regression this would catch:
- why manual comparison is no longer enough:
- proposed automation scope:
- CI and local validation commands unchanged:

Decision:
- optional evidence only / follow-up / explicit review required:
- reason:
- owner:
- recheck date:
- issue or PR:

Rollback:
- rollback owner:
- return path to SQL-only candidate retrieval:
- automation disable path:
- OpenSearch dependency removal or bypass path:
- rollback verification command:
- rollback recheck date:
```

## Managed Infrastructure Explicit Review Packet

Use this packet when evidence discusses hosting, managed PostgreSQL/PostGIS,
managed Redis, managed OpenSearch, networking, secrets, observability, backup,
cost, or production IaC. Managed infrastructure is explicit review only and
does not graduate through a hardening PR.

```text
Optional evidence packet: managed infrastructure explicit review

Scope:
- operator:
- review date:
- proposed infrastructure:
- managed service category: hosting / database / cache / OpenSearch / networking / secrets / observability / backup / IaC / other
- related issue or review:

Fixed public MVP boundary unchanged:
- sql_only candidate retrieval:
- event-csv operational content path:
- PostgreSQL/PostGIS write store:
- Redis cache:
- just mvp-acceptance remains six cases:
- managed infrastructure outside the fixed gate:
- no production cloud resources added by this hardening PR:
- public API shape unchanged:

Explicit review evidence:
- review owner:
- decision authority:
- approval record link:
- production need:
- alternatives considered:
- reason local Docker or self-managed operation is insufficient:
- compatibility with SQL-only minimal mode:

Operational evidence:
- hosting plan:
- managed database plan:
- managed cache plan:
- managed OpenSearch plan:
- networking and access controls:
- secrets handling:
- observability:
- backup and restore:
- incident response owner:
- data retention notes:

Cost and lifecycle evidence:
- expected monthly cost:
- cost owner:
- budget approval:
- scaling assumptions:
- decommission plan:
- IaC scope:
- local development impact:

Decision:
- explicit review approved / explicit review rejected / deferred:
- reason:
- owner:
- recheck date:
- issue or PR:

Rollback:
- rollback owner:
- return path to local or self-managed services:
- managed service disable path:
- data export or restore path:
- cost stop condition:
- rollback verification command:
- rollback recheck date:
```

## Required Validation When Files Change

Keep local validation and CI expectations aligned:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
just mvp-acceptance
DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
git diff --check
```

If `just` is unavailable:

```bash
./scripts/mvp_acceptance.sh
DATA_QUALITY_FAIL_ON_WARNING=true ./scripts/data_quality_doctor.sh
```
