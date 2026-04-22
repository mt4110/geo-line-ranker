# Optional Evidence Lifecycle Index

Use this guide when optional evidence records need a review inventory across
intake, triage, recheck audit, closeout ledger, and closeout integrity. Its job
is to make record state, owner, stale status, linked actions, and integrity
result easy to scan without changing the underlying issue, PR, review note, or
packet.

The lifecycle index is exploration and review support only. It is not an
acceptance gate, release gate, required label setup, automation requirement, or
replacement source of truth. The source record remains the issue, PR, review
note, packet, closeout ledger entry, or integrity note that the index links.
When index rows need to be shared as a compact handoff, use
[OPTIONAL_EVIDENCE_LIFECYCLE_INVENTORY_REPORT.md](OPTIONAL_EVIDENCE_LIFECYCLE_INVENTORY_REPORT.md)
to prepare a read-only inventory report or review snapshot.

This workflow is read-only. It does not create GitHub labels, run validation by
itself, change source maturity, enable full mode, require OpenSearch, provision
managed infrastructure, or change public API shape.

## Fixed Boundary

Every lifecycle index row must preserve this public MVP boundary:

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

Public API shape should not change as part of lifecycle indexing. If an indexed
record would change public API shape, route it to explicit review before
implementation and update `schemas/openapi.json` plus create or update
`API_SPEC.md` only in the approved implementation change.

## Purpose

Use the lifecycle index to answer these review questions quickly:

- Which optional evidence records exist, and where is each source record?
- Which records are still waiting for an owner, recheck, follow-up, or explicit
  review?
- Which records are closed, and which closeout or integrity note proves that?
- Which records look orphaned, stale, or unclear before a reviewer spends time
  reopening each source record?

The index should reduce search cost. It should not create another place where
operators must resolve the work. When the index finds missing information, edit
the source record, linked record, closeout ledger entry, or integrity note.

## Scope

Include records from:

- optional evidence issues
- optional evidence PRs
- review notes that include the optional evidence intake header
- crawler graduation, full-mode automation candidate, managed infrastructure,
  and strict doctor evidence packets
- recheck audit notes
- closeout ledger entries
- closeout integrity notes

Do not include:

- fixed `just mvp-acceptance` cases as lifecycle index rows unless they are
  linked from an optional evidence record
- crawler graduation as a fixed gate, even when the packet is complete
- full-mode automation, `full` mode, or OpenSearch as fixed-gate requirements
- managed infrastructure unless an explicit review record owns it
- public API shape changes unless an explicit review and approved
  implementation record own them

## Lifecycle States

Use one current lifecycle state per row. Choose the most action-relevant state:
`recheck-overdue` beats `recheck-scheduled`, linked action states beat
`closeout-recorded`, and `closed` is terminal only when integrity and closeout
requirements are satisfied.

| State | Use when | Required evidence |
| --- | --- | --- |
| `intake-recorded` | The minimal intake header exists in an issue, PR, or review note, but triage fields are not complete yet. | Evidence type, source, packet choice or reason none, decision lane, owner or owner gap, recheck status, fixed boundary answer, public API answer. |
| `triaged` | The record has one primary lane, owner, recheck command or evidence source, and close condition. | Triage note or body fields from [OPTIONAL_EVIDENCE_TRIAGE.md](OPTIONAL_EVIDENCE_TRIAGE.md). |
| `recheck-scheduled` | A future recheck date or condition is recorded and the record is not yet overdue. | Owner, next recheck date or condition, recheck command or evidence source, close condition. |
| `recheck-overdue` | The recheck date has passed, `needs-recheck` has no current result, or the next action is unclear. | Recheck audit class from [OPTIONAL_EVIDENCE_RECHECK_AUDIT.md](OPTIONAL_EVIDENCE_RECHECK_AUDIT.md), or a written reason that the record needs audit. |
| `closeout-recorded` | A closeout ledger entry records `close`, `keep-open`, `split`, `follow-up`, or `explicit-review`. | Closeout ledger fields from [OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md](OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md). |
| `integrity-complete` | Closeout integrity confirms required fields, reachable links, repeat-marker routing, boundary status, and public API status. | Integrity note with result `complete` from [OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md](OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md). |
| `follow-up-linked` | The closeout decision opened or points to a scoped issue or PR that owns the next action. | Reachable follow-up link with one root cause, owner, validation plan, and recheck command. |
| `explicit-review-linked` | The closeout decision points to a review record for boundary, public API, public profile, managed infrastructure, full-mode/OpenSearch, crawler maturity, or final-ranking ownership risk. | Reachable explicit review link with decision authority, scope, rollback path, cost or risk owner when relevant, and decision status. |
| `closed` | No action or recheck remains on the original record and closeout integrity is complete. | Primary closeout status `closeout:closed` with repeat marker `none`, or split/follow-up/explicit-review link proving the original is no longer the action holder. |

States describe the record lifecycle; labels are only optional aids. If labels
are missing, written fields in the record body are enough for the lifecycle
index.

## Minimum Index Fields

Each lifecycle index row should include these fields:

| Field | Minimum value |
| --- | --- |
| Record link | Issue, PR, review note, packet, or closeout record URL or local reference. |
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

Do not make label presence a minimum field. Labels and written label equivalents
are record aids only, not gates.

## Read-Only Inventory Conditions

Use these conditions to find records that need source-record edits. The
inventory itself should not run validation or change services.

### Orphan Candidates

Flag a record as orphan-prone when any of these are true:

- `closeout:closed` appears, but the body implies a split, follow-up, or
  explicit review without a reachable link.
- `closeout:kept-open` has no owner, next recheck date or condition, narrow
  evidence source, or waiting reason.
- `closeout:split` exists, but any split target is missing or lacks one lane,
  owner, close condition, and recheck command or evidence source.
- `closeout:follow-up-opened` exists, but the linked issue or PR lacks one root
  cause, owner, validation plan, or recheck command.
- `closeout:explicit-review-linked` exists, but the linked review lacks
  decision authority, scope, rollback path, cost or risk owner when relevant,
  or current decision status.
- `closeout:repeated-stale`, `repeated keep-open`, or `final allowed keep-open`
  appears as a marker without a route to split, follow-up, explicit review, or
  one final dated external wait.

### Stale Candidates

Flag a record as stale when any of these are true:

- The recheck date or condition has arrived and no current result is recorded.
- `needs-recheck` or a written equivalent appears without a dated result.
- A previous `keep-open` date has passed without new evidence or a new decision.
- The record has no next recheck date and no reason that no recheck remains.
- The record has `recheck:overdue`, `recheck:blocked`, or
  `recheck:split-needed` without a stale hygiene decision.

### Unclear Owner Candidates

Flag a record as unclear-owner when any of these are true:

- The owner field is blank, generic, or only implied by an assignee.
- Multiple owners are named without one current decision owner.
- The source record owner and linked follow-up or review owner disagree without
  a handoff note.
- A managed infrastructure record lacks decision authority, cost owner, or risk
  owner.
- An explicit review record lacks decision authority or current decision
  status.

When a row matches any condition, update the source record, linked record,
closeout ledger entry, or integrity note before treating the row as complete.
If the missing action would change public profile, public API shape, managed
infrastructure, full-mode/OpenSearch production role, crawler maturity outside
the graduation lane, or final-ranking ownership, route it to explicit review.

## Read-Only Collection

Print the local checklist:

```bash
just optional-evidence-review
```

If GitHub CLI access is available, these reads can help collect open records:

```bash
gh issue list --state open --label optional-evidence \
  --json number,title,labels,assignees,updatedAt,url
gh pr list --state open --label optional-evidence \
  --json number,title,labels,assignees,updatedAt,url
```

If labels are unavailable, search issue, PR, or review-note bodies for
`optional evidence`, `recheck date`, `needs-recheck`, lane names,
`Optional evidence closeout ledger`, and `Optional evidence closeout integrity`.
The absence of labels is not a gate failure.

Do not run lane-specific validation commands while collecting inventory. Record
the narrow command or evidence source that the owner should use next.

## Inventory Report and Review Snapshot

Use the lifecycle index row template below for per-record review. When several
rows need to be shared, summarized, or handed off, prepare an inventory report
with
[OPTIONAL_EVIDENCE_LIFECYCLE_INVENTORY_REPORT.md](OPTIONAL_EVIDENCE_LIFECYCLE_INVENTORY_REPORT.md).

The report summarizes lifecycle-state counts and records orphan, stale, and
unclear-owner candidates as findings. Those findings are not failures. They are
edit candidates for the source record, linked record, closeout ledger entry, or
integrity note.

## Row Template

Paste this into a review note, issue comment, or PR comment when a compact
inventory row is useful:

```text
Optional evidence lifecycle index:
- Record:
- Lifecycle state: intake-recorded / triaged / recheck-scheduled / recheck-overdue / closeout-recorded / integrity-complete / follow-up-linked / explicit-review-linked / closed
- Owner:
- Lane: optional evidence only / follow-up / crawler graduation / explicit review required
- Source type: doctor / crawler / full-mode or OpenSearch / managed infrastructure / mixed / other
- Stale class: none / recheck:on-time / recheck:overdue / recheck:blocked / recheck:split-needed / recheck:closed
- Stale hygiene decision: none / close / keep-open / split / follow-up / explicit-review
- Primary closeout status: none / closeout:closed / closeout:kept-open / closeout:split / closeout:follow-up-opened / closeout:explicit-review-linked
- Repeat or escalation marker: none / closeout:repeated-stale / repeated keep-open / final allowed keep-open
- Linked split, follow-up, or explicit review:
- Next recheck date or reason none is needed:
- Integrity result: none / complete / needs closeout edit / needs linked-record edit / route to explicit review
- Inventory finding: none / orphan candidate / stale candidate / unclear owner
- Fixed public MVP boundary unchanged:
- Public API shape unchanged:
- Labels or written label equivalents were record aids only:
```

## Lifecycle Checklist

Before relying on the index as review inventory, confirm:

1. Every row links back to its source record.
2. Every current action has a named owner.
3. Every open row has a lane, source type, close condition, and recheck date or
   reason none is needed.
4. Every closeout row has primary status and repeat or escalation marker
   recorded separately.
5. Every split, follow-up, and explicit-review row links reachable target
   records.
6. Every closed row has integrity result `complete` or a linked action proving
   the original is no longer the action holder.
7. Labels are treated as record aids only; written equivalents are enough.
8. Crawler graduation remains outside the fixed gate even when packet complete.
9. Full-mode automation does not add `full` mode or OpenSearch to the fixed
   gate.
10. Managed infrastructure remains explicit review only.
11. Strict doctor `review_items` are human-classified before issue or PR work.
12. Public API shape is unchanged, or explicit review owns a separate approved
    implementation change.
