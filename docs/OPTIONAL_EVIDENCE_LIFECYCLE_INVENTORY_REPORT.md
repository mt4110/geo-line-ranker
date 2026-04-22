# Optional Evidence Lifecycle Inventory Report

Use this guide when lifecycle index rows need to be shared as an inventory
report or review snapshot. The report turns the read-only lifecycle index into
a compact review artifact that shows what exists, what state each record is in,
and which source records may need edits.

The report is review inventory only. It is not an acceptance gate, release
gate, required label setup, validation result, automation requirement, or
replacement source of truth. The source record remains the issue, PR, review
note, packet, closeout ledger entry, or integrity note linked by each row.

This workflow is read-only. It does not create GitHub labels, run validation by
itself, change source maturity, enable full mode, require OpenSearch, provision
managed infrastructure, or change public API shape.

## Fixed Boundary

Every inventory report must preserve this public MVP boundary:

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

Doctor warnings fail the evidence pass. Doctor `review_items` are classified by
humans before issue or PR work starts.

Public API shape should not change as part of inventory reporting. If a report
finding points to work that would change public API shape, route it to explicit
review before implementation and update `schemas/openapi.json` plus
`API_SPEC.md` only in the approved implementation change.

## Purpose

Use the report to answer these review questions quickly:

- What optional evidence records were included in this snapshot?
- How many records are in each lifecycle state?
- Which records are open, closed, linked to follow-up, or waiting for explicit
  review?
- Which records have orphan, stale, or unclear-owner findings?
- Which source record, linked record, closeout ledger entry, or integrity note
  should be edited next?

The report should reduce review cost. It should not create a second place to
resolve the work. When a finding is recorded, fix the source record or linked
record, then update the lifecycle index row and report snapshot if a new review
snapshot is needed.

## Scope

Build the report from lifecycle index rows that summarize:

- optional evidence issues
- optional evidence PRs
- review notes that include the optional evidence intake header
- crawler graduation, full-mode automation candidate, managed infrastructure,
  and strict doctor evidence packets
- recheck audit notes
- closeout ledger entries
- closeout integrity notes

Do not include:

- fixed `just mvp-acceptance` cases unless they are linked from an optional
  evidence record
- crawler graduation as a fixed gate, even when the packet is complete
- full-mode automation, `full` mode, or OpenSearch as fixed-gate requirements
- managed infrastructure unless an explicit review record owns it
- public API shape changes unless an explicit review and approved
  implementation record own them

## Non-Goals

The report is not:

- a pass or fail result
- a validation command
- a CI gate
- a label creation checklist
- a release-readiness substitute
- a generated source of truth
- a place to approve crawler graduation, full-mode production use,
  OpenSearch production reliance, managed infrastructure, or public API changes

Labels remain record aids only. If labels are missing, written fields in the
source record are enough for the lifecycle index and report.

## Report Shape

Use four sections when a shareable snapshot is needed:

1. Header: identifies the snapshot, scope, fixed boundary, and label status.
2. Summary: aggregates lifecycle state, lane, source type, and finding counts.
3. Rows: lists the lifecycle index rows included in the snapshot.
4. Findings: records review findings that point to source-record edits.

Keep the report compact enough to paste into an issue, PR, review note, or
handoff document. If the report is saved as a file, keep links back to the
source records so the report does not become a detached tracker.

## Header Format

Each report header should include these fields:

| Field | Minimum value |
| --- | --- |
| Snapshot date | Calendar date when the inventory was reviewed. |
| Reviewer | Named person or role preparing the snapshot. |
| Scope | Included issue, PR, review-note, packet, closeout, or integrity surfaces. |
| Collection method | Manual review, GitHub query, local search, or linked lifecycle index source. |
| Repository reference | Branch and commit when collected from a local checkout, or `not applicable` for GitHub-only review. |
| Fixed public MVP boundary | Must say unchanged: `sql_only`, `event-csv`, PostgreSQL/PostGIS, Redis, `just mvp-acceptance`. |
| Outside fixed gate | Must say crawler graduation, live crawler operation, full mode, OpenSearch, and managed infrastructure remain outside the fixed gate. |
| Strict doctor status | Must say doctor output is evidence and `review_items` are human-classified. |
| Public API shape | Must say unchanged, or link explicit review before implementation. |
| Label status | Must say labels or written label equivalents are record aids only, not gates. |

## Row Format

Each report row should copy or link one lifecycle index row. Include these
minimum fields:

| Field | Minimum value |
| --- | --- |
| Row id | Short stable id for this snapshot, such as `R1`. |
| Record link | Issue, PR, review note, packet, closeout record, or local reference. |
| Lifecycle state | One of the lifecycle states from [OPTIONAL_EVIDENCE_LIFECYCLE_INDEX.md](OPTIONAL_EVIDENCE_LIFECYCLE_INDEX.md). |
| Owner | Named person or role responsible for the current decision or next action. |
| Lane | Optional evidence only, follow-up, crawler graduation, or explicit review required. |
| Source type | Doctor, crawler, full-mode/OpenSearch, managed infrastructure, mixed, or other recorded evidence type. |
| Stale class | `recheck:on-time`, `recheck:overdue`, `recheck:blocked`, `recheck:split-needed`, `recheck:closed`, or none before audit. |
| Stale hygiene decision | `close`, `keep-open`, `split`, `follow-up`, `explicit-review`, or none before closeout. |
| Primary closeout status | `closeout:closed`, `closeout:kept-open`, `closeout:split`, `closeout:follow-up-opened`, `closeout:explicit-review-linked`, or none before closeout. |
| Repeat or escalation marker | `none`, `closeout:repeated-stale`, `repeated keep-open`, `final allowed keep-open`, or none before closeout. |
| Linked split/follow-up/explicit review | Reachable linked records, or `none` plus the reason no linked record is needed. |
| Next recheck date or reason none | Date, condition, or reason no recheck remains. |
| Integrity result | `complete`, `needs closeout edit`, `needs linked-record edit`, `route to explicit review`, or none before integrity. |
| Finding refs | Finding ids for this row, or `none`. |

Do not make label presence a report row field. Labels and written label
equivalents are record aids only, not gates.

## Summary Format

Each summary should include these minimum aggregate items:

| Field | Minimum value |
| --- | --- |
| Total rows | Count of lifecycle index rows in the snapshot. |
| Rows by lifecycle state | Counts for every lifecycle state, including zero counts. |
| Rows by lane | Counts for optional evidence only, follow-up, crawler graduation, and explicit review required. |
| Rows by source type | Counts for doctor, crawler, full-mode/OpenSearch, managed infrastructure, mixed, and other. |
| Rows needing source-record edits | Count of findings whose proposed edit target is the source record. |
| Rows needing linked-record edits | Count of findings whose proposed edit target is a linked split, follow-up, or explicit-review record. |
| Rows needing closeout or integrity edits | Count of findings whose proposed edit target is a closeout ledger entry or integrity note. |
| Findings by type | Counts for orphan candidate, stale candidate, and unclear owner. |
| Explicit review links | Count of rows that already link explicit review and count that need one. |
| Boundary status | Confirm fixed public MVP boundary unchanged. |
| Public API status | Confirm unchanged, or list explicit-review-linked rows only. |
| Label status | Confirm labels were aids only and not used as gates. |

If a count is zero, write `0`. Zero counts are useful because they show the
reviewer checked the category instead of skipping it.

## Lifecycle State Aggregates

Use these aggregate items when summarizing rows by lifecycle state:

| State | Count answers |
| --- | --- |
| `intake-recorded` | Records with intake present but triage fields incomplete. |
| `triaged` | Records with one lane, owner, recheck source, and close condition. |
| `recheck-scheduled` | Records waiting for a future recheck date or condition. |
| `recheck-overdue` | Records whose recheck date passed or whose next action is unclear. |
| `closeout-recorded` | Records with closeout ledger history but no complete integrity result yet. |
| `integrity-complete` | Records whose closeout integrity note is complete. |
| `follow-up-linked` | Records whose next action is owned by a reachable follow-up issue or PR. |
| `explicit-review-linked` | Records whose next decision is owned by a reachable explicit review record. |
| `closed` | Records where no action remains on the original record. |

The summary should make open work visible without implying failure. For
example, `recheck-overdue: 2` means two records need review attention; it does
not mean release validation failed.

## Finding Format

Findings record review attention, not pass/fail status. Use one finding for
each specific source-record edit that would make the row clearer.

| Field | Minimum value |
| --- | --- |
| Finding id | Short stable id for this snapshot, such as `F1`. |
| Type | `orphan candidate`, `stale candidate`, or `unclear owner`. |
| Row id | Report row that triggered the finding. |
| Source record | Issue, PR, review note, packet, closeout record, or integrity note that should be edited. |
| Linked record | Linked split, follow-up, explicit review, or `none`. |
| Evidence | Short reason the finding was recorded. |
| Proposed edit target | `source record`, `linked record`, `closeout ledger`, or `integrity note`. |
| Proposed edit | The smallest text, link, owner, recheck date, close condition, or routing note needed. |
| Owner | Named person or role who can make or review the edit. |
| Boundary/API note | Confirm no fixed-boundary or public API change, or route to explicit review. |
| Finding status | `open`, `resolved in source record`, `resolved in linked record`, `resolved in closeout`, `resolved in integrity`, or `routed to explicit review`. |

Do not use findings as gate failures. A finding is a pointer to improve the
record trail. The corrective action is to edit the source record, linked
record, closeout ledger, or integrity note, then refresh the lifecycle index row
or prepare a new report snapshot.

## Finding Types

Use the read-only inventory conditions from
[OPTIONAL_EVIDENCE_LIFECYCLE_INDEX.md](OPTIONAL_EVIDENCE_LIFECYCLE_INDEX.md)
to classify findings.

### Orphan Candidate

Record this when a row suggests work could be hidden or detached from its
owner. Common examples:

- `closeout:closed` appears while the body implies split, follow-up, or
  explicit review with no reachable link.
- `closeout:kept-open` lacks owner, next recheck date or condition, evidence
  source, or waiting reason.
- `closeout:split`, `closeout:follow-up-opened`, or
  `closeout:explicit-review-linked` lacks a reachable target or the target does
  not own the next action.
- repeated stale or repeated keep-open appears without a route to split,
  follow-up, explicit review, or one final dated external wait.

The finding should name the missing link, owner, next action, or close
condition to add.

### Stale Candidate

Record this when a row needs a fresh recheck decision. Common examples:

- the recheck date or condition arrived and no result is recorded
- `needs-recheck` or a written equivalent has no dated result
- a previous `keep-open` date passed without new evidence or a new decision
- no next recheck date exists and no reason says no recheck remains
- `recheck:overdue`, `recheck:blocked`, or `recheck:split-needed` has no stale
  hygiene decision

The finding should name the owner, narrow recheck source, and next decision
needed.

### Unclear Owner

Record this when a reviewer cannot tell who owns the next decision. Common
examples:

- the owner field is blank, generic, or only implied by an assignee
- multiple owners are named without one current decision owner
- source record owner and linked follow-up or review owner disagree without a
  handoff note
- managed infrastructure lacks decision authority, cost owner, or risk owner
- explicit review lacks decision authority or current decision status

The finding should name the owner field or handoff note to add.

## Snapshot Template

Paste this into an issue, PR, review note, or handoff document:

```text
Optional evidence lifecycle inventory report:
- Snapshot date:
- Reviewer:
- Scope:
- Collection method:
- Repository reference:
- Fixed public MVP boundary unchanged:
- Outside fixed gate: crawler graduation / live crawler operation / full mode / OpenSearch / managed infrastructure
- Strict doctor evidence: DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor; review_items human-classified
- Public API shape unchanged:
- Labels or written label equivalents were record aids only:

Summary:
- Total rows:
- Rows by lifecycle state:
  - intake-recorded:
  - triaged:
  - recheck-scheduled:
  - recheck-overdue:
  - closeout-recorded:
  - integrity-complete:
  - follow-up-linked:
  - explicit-review-linked:
  - closed:
- Rows by lane:
- Rows by source type:
- Rows needing source-record edits:
- Rows needing linked-record edits:
- Rows needing closeout or integrity edits:
- Findings by type:
  - orphan candidate:
  - stale candidate:
  - unclear owner:
- Explicit review links:
- Boundary status:
- Public API status:
- Label status:

Rows:
- Row id:
  Record:
  Lifecycle state:
  Owner:
  Lane:
  Source type:
  Stale class:
  Stale hygiene decision:
  Primary closeout status:
  Repeat or escalation marker:
  Linked split/follow-up/explicit review:
  Next recheck date or reason none is needed:
  Integrity result:
  Finding refs:

Findings:
- Finding id:
  Type: orphan candidate / stale candidate / unclear owner
  Row id:
  Source record:
  Linked record:
  Evidence:
  Proposed edit target: source record / linked record / closeout ledger / integrity note
  Proposed edit:
  Owner:
  Boundary/API note:
  Finding status: open / resolved in source record / resolved in linked record / resolved in closeout / resolved in integrity / routed to explicit review
```

## Review Checklist

Before sharing a report snapshot, confirm:

1. Every row links back to its source record or local reference.
2. Every row has one lifecycle state.
3. Every open row has an owner, lane, source type, and next recheck date or
   reason none is needed.
4. Every finding points to a proposed edit target.
5. Orphan, stale, and unclear-owner findings are written as review findings,
   not gate failures.
6. Crawler graduation remains outside the fixed gate even when packet complete.
7. Full-mode automation does not add `full` mode or OpenSearch to the fixed
   gate.
8. Managed infrastructure remains explicit review only.
9. Strict doctor `review_items` remain human-classified evidence.
10. Labels are treated as record aids only.
11. Public API shape is unchanged, or explicit review owns a separate approved
    implementation change.
