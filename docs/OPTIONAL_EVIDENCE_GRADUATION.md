# Optional Evidence Graduation

Use this guide after the Phase 14 evidence review loop has classified crawler,
full-mode, OpenSearch, or infrastructure notes. Its job is to decide whether
optional evidence stays informational, becomes a scoped follow-up, graduates a
crawler source, or needs explicit review.

This guide does not widen the public MVP gate.

## Fixed Boundary

The public MVP gate stays fixed to:

- Candidate retrieval: `sql_only`
- Operational content path: `event-csv`
- Write store: PostgreSQL/PostGIS
- Cache: Redis
- Acceptance gate: [MVP_ACCEPTANCE.md](MVP_ACCEPTANCE.md)

Keep these outside the fixed gate:

- crawler graduation and live crawler operation
- `full` mode, OpenSearch, and projection sync
- managed infrastructure
- ML, embeddings, vector search, or frontend final-ranking logic

Strict data-quality doctor output remains evidence:

```bash
DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
```

Warnings fail the evidence pass. Doctor `review_items` still need human
classification before issue or PR work starts.

## Decision Ladder

Use this ladder for every optional evidence packet:

1. Confirm `just mvp-acceptance` remains the fixed six-case gate and is not
   expanded by this evidence.
2. Confirm the evidence packet is complete enough to reproduce the finding.
3. Keep it as optional evidence only when it is informative but does not
   change an operator decision, product behavior, or source maturity.
4. Open a follow-up when the evidence shows one reproducible improvement that
   does not change the public MVP profile.
5. Prepare crawler graduation only when the crawler packet is complete and the
   source has no promotion blockers.
6. Request explicit review before changing public gate membership, public API
   shape, crawler maturity, full-mode/OpenSearch production role, managed
   infrastructure, or final-ranking ownership.

Do not combine public-MVP recovery with crawler graduation, full-mode
automation, or managed infrastructure work in the same PR.

## Crawler Graduation

Crawler graduation means moving a manifest toward `source_maturity: live_ready`
or keeping an already-live source in live operation. It remains
outside the fixed public MVP gate, and the review record must say so.

### Evidence Packet

Attach these items before approving graduation:

- manifest path, `source_id`, `source_maturity`, `parser_key`,
  `expected_shape`, target URLs, and default `school_id`
- source policy review with date, terms URL, robots URL, allowed domains, and
  whether live fetch is permitted
- `cargo run -p crawler -- doctor --manifest <manifest>` output with
  `promotion_gate: ready` or documented review notes
- a recent fetch or staged raw checksum when live content was inspected
- `cargo run -p crawler -- dry-run --manifest <manifest>` output with
  plausible parsed, deduped, imported, inactive, and missing-school counts
- `cargo run -p crawler -- health --manifest <manifest>` output showing no
  recurring fetch, robots, policy, parser, or logical-name failures
- fixture-backed parser coverage for the expected shape, when the parser or
  manifest mapping changed
- rollback path to `parser_only` or `policy_blocked`, plus the `event-csv`
  repair path if public content is affected

### Graduation Conditions

Graduation is eligible only when all of these are true:

- the source is allowlist-only and target URLs stay inside
  `allowlist.allowed_domains`
- robots and terms checks are current and do not block the target path
- `allowlist.live_fetch_enabled` and `source_maturity` agree operationally
- the parser is registered and its `expected_shape` matches the live target or
  committed fixture
- the parser output is deterministic, bounded, and mapped to the intended
  event fields
- every target has a valid school id in the tested environment
- dry-run output shows nonzero parsed and imported rows, unless the review
  explicitly accepts a seasonal zero-row source
- health output has no recent failed runs or reason totals such as
  `blocked_robots`, `blocked_policy`, or `fetch_failed`
- the rollback owner and recheck date are recorded

If any blocker remains, classify the packet as follow-up or explicit review
required. Do not change `source_maturity` in the same PR as unrelated public
MVP recovery.

## Full-Mode Automation Candidate

Full mode is evaluation unless an explicit review changes the public operating
profile. A packet can become an automation candidate only after SQL-only health
is already established.

### Evidence Packet

Attach these items:

- SQL-only response samples and full-mode response samples for the same
  request payloads
- projection sync command output and OpenSearch index health
- `cargo test -p compatibility-tests --test sql_only_vs_full` output, when
  applicable
- observed difference summary: ordering, missing candidates, latency, or
  operator effort
- explanation of why manual comparison is no longer enough
- rollback path that returns to SQL-only candidate retrieval without changing
  public API shape

### Candidate Conditions

Full-mode automation is eligible for a follow-up only when all of these are
true:

- `just mvp-acceptance` and strict data-quality doctor evidence are healthy
- the comparison is reproducible from committed fixtures, commands, or
  sanitized request samples
- SQL-only remains the public MVP baseline
- full-mode output does not change final ranking ownership or move final
  ranking logic out of Rust
- OpenSearch stays candidate retrieval only
- the automation would reduce repeated operator work or catch a specific
  full-mode regression
- local and CI validation commands remain visible and unchanged for the fixed
  gate

If the next step would make full mode required, make OpenSearch part of the
public gate, change readiness semantics, or change public API shape, stop and
request explicit review.

## Managed Infrastructure

Managed infrastructure is explicit review only. Evidence can describe hosting,
managed databases, managed cache, managed OpenSearch, networking, secrets,
observability, backup, or cost assumptions, but it cannot graduate through a
hardening PR.

Do not add production cloud resources, managed services, production IaC, or
new service dependencies to the fixed public MVP gate without an explicit
review record.

## Review Record

Use this shape in an issue, PR, or hardening review note:

```text
Optional evidence graduation:
Source:
Date:
Operator:

Fixed public MVP boundary unchanged:
- sql_only candidate retrieval:
- event-csv operational content:
- PostgreSQL/PostGIS write store:
- Redis cache:
- just mvp-acceptance remains six cases:

Evidence packet:
- Crawler graduation:
- Full-mode automation candidate:
- Managed infrastructure:

Decision:
- optional evidence only / follow-up / crawler graduation / explicit review required:
- Reason:
- Owner:
- Recheck date:
- Issue or PR:
- Rollback path:

Required validation when files change:
- cargo fmt --all --check:
- cargo clippy --workspace --all-targets --all-features -- -D warnings:
- cargo test --workspace:
- just mvp-acceptance:
- DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor:
- git diff --check:
```
