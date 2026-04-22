# Optional Evidence Triage

Use this guide after an optional evidence issue, PR, or review note exists.
Its job is to make the next label, lane, recheck, and close condition obvious
without widening the fixed public MVP gate.

When a recheck date has arrived, a record has `needs-recheck`, or open optional
evidence may have gone stale, use
`docs/OPTIONAL_EVIDENCE_RECHECK_AUDIT.md` to classify the record and route the
next action. After a stale hygiene decision is chosen, use
`docs/OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md` to record why the record closed,
stayed open, split, opened a follow-up, or linked explicit review.
After the closeout record exists, use
`docs/OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md` to check required fields, link
reachability, repeated-stale routing, and primary status consistency.
When operators need to review records across intake, triage, recheck, closeout,
and integrity, use `docs/OPTIONAL_EVIDENCE_LIFECYCLE_INDEX.md` as a read-only
lifecycle index and review inventory.
When those lifecycle rows need to be shared as a handoff, use
`docs/OPTIONAL_EVIDENCE_LIFECYCLE_INVENTORY_REPORT.md` for the read-only
inventory report and review snapshot.

This guide is a triage loop, not an acceptance test. It does not create GitHub
labels, run validation by itself, change source maturity, require OpenSearch,
enable full mode, provision managed infrastructure, or change public API shape.

## Fixed Boundary

Every triage record must preserve this public MVP boundary:

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
by humans before issue or PR work starts.

Public API changes should stay out of optional evidence triage. If a change
does alter public API shape, update `schemas/openapi.json` and create or
update `API_SPEC.md` in the same change and route the record to explicit
review required.

## Triage Loop

Use this loop for every optional evidence issue, PR, or review note:

1. Confirm the minimal intake header is present or linked.
2. Confirm the fixed public MVP boundary is unchanged.
3. Choose one primary source label and one primary decision lane.
4. Record the owner, recheck date, recheck command, and close condition.
5. Add `needs-recheck` only when the record must be revisited later.
6. On or after the recheck date, use
   `docs/OPTIONAL_EVIDENCE_RECHECK_AUDIT.md` to classify stale records, then
   rerun the lane-specific command or inspect the linked evidence source.
7. Record the stale hygiene decision in
   `docs/OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md` before closing, keeping open,
   splitting, opening follow-up, or linking explicit review.
8. Check `docs/OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md` so closeout records do
   not leave orphaned links or marker-only next actions.
9. For multi-record review, update or paste a lifecycle index row from
   `docs/OPTIONAL_EVIDENCE_LIFECYCLE_INDEX.md` so the current state, owner,
   stale status, links, and integrity result are visible.
10. When rows need to be shared, prepare a report snapshot with
    `docs/OPTIONAL_EVIDENCE_LIFECYCLE_INVENTORY_REPORT.md` so orphan, stale,
    and unclear-owner findings point back to source-record edits.
11. Close only when the lane close condition below is satisfied.

If one record needs multiple lanes, split it before implementation starts.
When more than one lane seems plausible, choose the stricter lane.

## Label Set

Labels are record aids only. GitHub label creation is not required for the
fixed public MVP gate, local validation, or PR merge. If labels are not
available in the repository, write the intended labels in the issue, PR, or
review note body.

| Label | Use when | Notes |
| --- | --- | --- |
| `optional-evidence` | The record is part of the optional evidence workflow. | Add to every optional evidence issue, PR, or review note when labels are available. |
| `lane:follow-up` | The next action is one scoped improvement that does not change the public MVP profile. | Use for full-mode automation candidates that only add comparison coverage or operator tooling. |
| `lane:crawler-graduation` | A crawler packet is complete enough for source-specific graduation review. | Packet completion still does not add live crawler operation to the fixed gate. |
| `lane:explicit-review` | The next action could change public profile, API shape, managed infrastructure, full-mode/OpenSearch production role, or final-ranking ownership. | Managed infrastructure always uses this lane. |
| `lane:optional-only` | The evidence is useful to retain but does not justify implementation work yet. | Close when recorded and no recheck is needed. |
| `source:doctor` | The evidence came from strict data-quality doctor output. | Doctor `review_items` still require human classification. |
| `source:crawler` | The evidence concerns crawler policy, robots, parser, dry-run, health, or source maturity. | Use with `lane:crawler-graduation`, `lane:follow-up`, or `lane:optional-only` as appropriate. |
| `source:full-mode` | The evidence concerns SQL-only versus full-mode comparison, projection sync, or OpenSearch health. | Full mode and OpenSearch remain outside the fixed gate. |
| `source:managed-infra` | The evidence concerns hosting, managed services, networking, secrets, backup, observability, cost, or production IaC. | Always pair with `lane:explicit-review`. |
| `needs-recheck` | A future date or condition must be checked before close. | Remove or mark complete after the recheck result is recorded. |

## Decision Lanes

| Lane | Owner | Recheck | Close only when |
| --- | --- | --- | --- |
| Optional evidence only | The operator or reviewer who received the evidence. | Optional. Use the original evidence source, or `just optional-evidence-review` to confirm routing. | The evidence source, fixed boundary check, and reason for no implementation are recorded; no public API, source maturity, full-mode/OpenSearch, or managed-infra change is pending; `needs-recheck` is absent or completed. |
| Follow-up | The issue or PR owner for the scoped improvement. | Rerun the command, request, SQL sample, or review step that exposed the finding. If files changed, run the validation set. | The linked issue or PR is opened or resolved with one root cause and one recheck command; the fixed public MVP boundary is unchanged; full-mode automation, if present, remains optional comparison coverage. |
| Crawler graduation | The source owner or parser owner. | Rerun crawler doctor, dry-run, and health for the manifest, plus parser tests when parser behavior changed. | The crawler packet is complete, source policy and robots checks are current, promotion blockers are clear or explicitly accepted, rollback owner and path are recorded, and live crawler operation is still outside the fixed gate. |
| Explicit review required | The decision authority named in the record. | Inspect the explicit review record and rerun validation only after an approved implementation PR exists. | The explicit review decision is recorded as approved, deferred, or rejected; managed infrastructure remains explicit review only; any approved follow-up is moved into a separate issue or PR with its own validation plan. |

## Recheck Commands

Use the narrowest command that proves the record can move forward or close.

| Source | Recheck command or evidence | Keep in mind |
| --- | --- | --- |
| Doctor | `DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor` | Warnings fail the evidence pass. Classify `review_items` by humans before opening implementation work. |
| Crawler | `cargo run -p crawler -- doctor --manifest <manifest>` | Use before crawler graduation or source-maturity work. |
| Crawler | `cargo run -p crawler -- dry-run --manifest <manifest>` | Record parsed, deduped, imported, inactive, and missing-school counts. |
| Crawler | `cargo run -p crawler -- health --manifest <manifest>` | Confirm no recurring fetch, robots, policy, parser, logical-name, or recent-run blockers. |
| Full mode | `cargo test -p compatibility-tests --test sql_only_vs_full` | Keeps SQL-only and full-mode comparison reproducible without changing the fixed gate. |
| Full mode | `docker compose -f .docker/docker-compose.full.yaml up -d postgres redis opensearch` then `cargo run -p cli -- index rebuild` | Use only for optional full-mode or OpenSearch evidence. This does not add OpenSearch to `just mvp-acceptance`. |
| Managed infrastructure | Link to the explicit review record, approval owner, cost owner, and rollback plan. | Do not provision cloud resources or managed services from triage. |
| Fixed boundary | `just mvp-acceptance` | This remains the fixed six-case gate. Do not add optional evidence cases to it. |

When files change, run the full validation set before reporting completion:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
just mvp-acceptance
DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
git diff --check
```

## Recheck Audit

Use `docs/OPTIONAL_EVIDENCE_RECHECK_AUDIT.md` when an optional evidence issue,
PR, or review note has an arrived recheck date, a `needs-recheck` marker, or no
clear next action after intake. The audit guide defines these stale recheck
classes:

- `recheck:on-time`
- `recheck:overdue`
- `recheck:blocked`
- `recheck:split-needed`
- `recheck:closed`

It also defines stale hygiene decisions: `close`, `keep-open`, `split`,
`follow-up`, and `explicit-review`. These decisions route records back to a
named owner, next action, and close condition. They are not new gates, and they
do not add crawler graduation, full mode, OpenSearch, managed infrastructure,
or public API changes to the fixed public MVP boundary.

After choosing one of those decisions, use
[OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md](OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md) to
record the decision history. Split, follow-up, and explicit-review decisions
must link their new records before the original record closes. Repeated stale
or repeated keep-open records must route to explicit review, split, or
follow-up unless one final dated external wait is recorded. Then use
[OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md](OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md)
to confirm required-field completeness, link reachability, orphan prevention,
and primary closeout status consistency.
For review inventory across multiple records, use
[OPTIONAL_EVIDENCE_LIFECYCLE_INDEX.md](OPTIONAL_EVIDENCE_LIFECYCLE_INDEX.md)
to map the same flow into lifecycle states such as `intake-recorded`,
`triaged`, `recheck-scheduled`, `recheck-overdue`, `closeout-recorded`,
`integrity-complete`, `follow-up-linked`, `explicit-review-linked`, and
`closed`.
For a shareable snapshot of those rows, use
[OPTIONAL_EVIDENCE_LIFECYCLE_INVENTORY_REPORT.md](OPTIONAL_EVIDENCE_LIFECYCLE_INVENTORY_REPORT.md)
to summarize lifecycle-state counts and record findings without turning them
into gate failures.

## Recheck Result Template

Paste this into the issue, PR, or review note when the recheck date arrives:

```text
Optional evidence recheck:
- Date:
- Owner:
- Source labels:
- Decision lane:
- Recheck command or evidence source:
- Result: close / keep-open / split / follow-up / explicit-review
- Fixed public MVP boundary unchanged:
- Public API shape unchanged:
- Next recheck date, if any:
- Issue or PR:
- Primary closeout status:
- Repeat/escalation marker, if any:
```

## Close Checklist

Before closing an optional evidence issue, PR, or review note, confirm:

- the minimal intake header is present or linked
- exactly one primary decision lane is recorded
- owner and recheck date are recorded, or the record says no recheck is needed
- the lane-specific close condition is satisfied
- the closeout ledger record explains the decision history
- the closeout integrity check confirms required fields, linked records,
  repeated-stale routing, and status/marker consistency
- lifecycle index state is clear when the record is part of a review inventory
- report snapshot findings, if any, point to source-record, linked-record,
  closeout, or integrity-note edits rather than gate failure
- labels or written label equivalents are recorded as aids only
- `just mvp-acceptance` remains the fixed six-case gate
- crawler graduation remains outside the fixed gate even when packet complete
- full-mode automation does not add `full` mode or OpenSearch to the fixed gate
- managed infrastructure remains explicit review only
- strict doctor `review_items` are classified by humans
- public API shape is unchanged, or `schemas/openapi.json` and
  `API_SPEC.md` were created or updated in the same explicit-review follow-up
