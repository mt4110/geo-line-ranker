# Optional Evidence Intake

Use this workflow when an operator receives crawler, full-mode, OpenSearch,
managed infrastructure, or strict doctor evidence and needs to decide where it
belongs before filling a packet.

Intake answers three questions:

- Which packet or review record should be used?
- Which decision lane should own the next step?
- Has this evidence stayed outside the fixed public MVP gate?

This workflow is read-only. It does not run validation, change source
maturity, enable full mode, require OpenSearch, provision managed
infrastructure, or change public API shape.

## Fixed Boundary

Every intake record must preserve this public MVP boundary:

- Candidate retrieval: `sql_only`
- Operational content path: `event-csv`
- Write store: PostgreSQL/PostGIS
- Cache: Redis
- Acceptance gate: `just mvp-acceptance`

Keep these outside the fixed gate:

- crawler graduation and live crawler operation
- `full` mode, OpenSearch, and projection sync
- managed infrastructure
- ML, embeddings, vector search, or frontend final-ranking logic

Strict data-quality doctor output remains evidence:

```bash
DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
```

Doctor warnings fail the evidence pass. Doctor `review_items` are classified
by humans before issue or PR work starts. If a review item points to crawler,
full-mode, OpenSearch, or managed infrastructure evidence, use this intake
workflow to choose the optional packet. Otherwise, classify it through the
post-MVP hardening review lanes.

Public API changes should be avoided in optional evidence intake. If a change
does alter public API shape, update `schemas/openapi.json` and `API_SPEC.md`
in the same change.

## Intake Steps

1. Capture the evidence source: command output, issue link, PR comment, review
   note, SQL row, request payload, response sample, manifest path, checksum, or
   operator observation.
2. Confirm the evidence does not expand `just mvp-acceptance` beyond the fixed
   six cases.
3. Choose the evidence type from the table below.
4. Paste the minimal intake header into the issue, PR body, or review note.
5. Paste the matching packet template when the evidence type needs one.
6. Choose exactly one decision lane for the next step.
7. Record owner, recheck date, rollback path when relevant, and the validation
   commands used if files changed.

## Evidence Type To Packet

| Evidence type | Use this record | Intake notes |
| --- | --- | --- |
| Strict data-quality doctor review item | Post-MVP hardening doctor review item | Keep strict doctor as evidence. If it affects the fixed `sql_only` + `event-csv` baseline, handle that before optional evidence. |
| Crawler source, policy, robots, parser, dry-run, health, or source maturity evidence | [Crawler Graduation Packet](OPTIONAL_EVIDENCE_PACKETS.md#crawler-graduation-packet) | Packet completion can support crawler graduation, but it never adds live crawler operation to the fixed gate. |
| SQL-only versus full-mode comparison, projection sync, OpenSearch health, or repeated manual comparison work | [Full-Mode Automation Candidate Packet](OPTIONAL_EVIDENCE_PACKETS.md#full-mode-automation-candidate-packet) | Full-mode automation can become a follow-up candidate only. It does not make full mode or OpenSearch part of `just mvp-acceptance`. |
| Hosting, managed PostgreSQL/PostGIS, managed Redis, managed OpenSearch, networking, secrets, observability, backup, cost, or production IaC | [Managed Infrastructure Explicit Review Packet](OPTIONAL_EVIDENCE_PACKETS.md#managed-infrastructure-explicit-review-packet) | Managed infrastructure is explicit review only. Do not add cloud resources or managed services from a hardening PR. |
| Mixed or unclear evidence | Minimal intake header first | Split the evidence until each record has one primary type and one decision lane. |

## Decision Lane Selection

Choose the narrowest lane that fits the next action:

- Optional evidence only: the evidence is informative, reproducible enough to
  keep, and does not justify a product, operator, source maturity, public API,
  or infrastructure change.
- Follow-up: the evidence shows one reproducible improvement that keeps the
  public MVP profile unchanged. Use this for full-mode automation candidates
  that only add comparison coverage or operator tooling.
- Crawler graduation: the crawler packet is complete, source-specific
  blockers are clear, policy and rollback are recorded, and the next change is
  limited to crawler source maturity or live-source operation. This lane still
  keeps live crawling outside the fixed gate.
- Explicit review required: the next step would change fixed gate membership,
  public API shape, the public operating profile, managed infrastructure,
  full-mode or OpenSearch production role, or final-ranking ownership.

When more than one lane seems possible, use the stricter lane. Managed
infrastructure always goes to explicit review required. Crawler graduation
must not be combined with public-MVP recovery, full-mode automation, or managed
infrastructure work in the same PR.

## Minimal Intake Header

Paste this header before the packet template in an issue, PR, or review note:

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

## Read-Only Command Output

Print the intake checklist from the repository root:

```bash
just optional-evidence-review
```

Without `just`:

```bash
./scripts/optional_evidence_review.sh
```

The output points back to this workflow, the decision ladder in
[OPTIONAL_EVIDENCE_GRADUATION.md](OPTIONAL_EVIDENCE_GRADUATION.md), and the
packet templates in [OPTIONAL_EVIDENCE_PACKETS.md](OPTIONAL_EVIDENCE_PACKETS.md).
It remains read-only and does not run validation or change local services.

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
