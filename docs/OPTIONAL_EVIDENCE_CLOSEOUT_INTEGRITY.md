# Optional Evidence Closeout Integrity

Use this guide after the
[optional evidence closeout ledger](OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md)
records why an optional evidence issue, PR, or review note was closed, kept
open, split, routed to follow-up, or routed to explicit review. Its job is to
check that the record is complete, reachable, and not orphaned from the action
it names.

After this pass, use
[OPTIONAL_EVIDENCE_LIFECYCLE_INDEX.md](OPTIONAL_EVIDENCE_LIFECYCLE_INDEX.md)
when records need a read-only lifecycle index or review inventory across
intake, triage, recheck audit, closeout ledger, and closeout integrity.
When those index rows need a shared handoff, use
[OPTIONAL_EVIDENCE_LIFECYCLE_INVENTORY_REPORT.md](OPTIONAL_EVIDENCE_LIFECYCLE_INVENTORY_REPORT.md)
to prepare a read-only inventory report and review snapshot.

This is an integrity and orphan-prevention pass, not a new acceptance gate. It
does not create GitHub labels, run validation by itself, change source maturity,
enable full mode, require OpenSearch, provision managed infrastructure, or
change public API shape.

## Fixed Boundary

Every integrity check must preserve this public MVP boundary:

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

Public API shape should not change as part of closeout integrity. If an
integrity check finds a record whose next action would change public API shape,
route it to explicit review before implementation and update
`schemas/openapi.json` plus `API_SPEC.md` only in the approved implementation
change.

## When to Run

Run a closeout integrity pass when:

- a closeout ledger record is added or edited
- an optional evidence record is about to close
- `split`, `follow-up`, or `explicit-review` appears in the stale hygiene
  decision
- `closeout:repeated-stale`, `repeated keep-open`, or `final allowed keep-open`
  appears in the record body or label aids
- a reviewer cannot tell which issue, PR, review note, packet, command, or
  decision authority now owns the action
- a lifecycle index row needs an `integrity-complete`, `follow-up-linked`,
  `explicit-review-linked`, or `closed` state before inventory review

The pass is read-only unless it finds missing record text. If work is missing,
edit the closeout record or linked record before changing code, services, source
maturity, or public API shape.

## Closeout Record Completeness

Every closeout ledger record is complete only when it includes all required
fields from [OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md](OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md):

| Field | Integrity condition |
| --- | --- |
| Closeout date | Calendar date is present. |
| Owner | Named person or role owns the decision and any next action. |
| Record | The issue, PR, review note, or original evidence record is named or linked. |
| Original evidence source | The source evidence, command output, packet, doctor result, issue, PR, or review note is named or linked. |
| Stale class | One of `recheck:on-time`, `recheck:overdue`, `recheck:blocked`, `recheck:split-needed`, or `recheck:closed`. |
| Stale hygiene decision | One of `close`, `keep-open`, `split`, `follow-up`, or `explicit-review`. |
| Primary closeout status | One of `closeout:closed`, `closeout:kept-open`, `closeout:split`, `closeout:follow-up-opened`, or `closeout:explicit-review-linked`. |
| Repeat or escalation marker | `none`, `closeout:repeated-stale`, `repeated keep-open`, or `final allowed keep-open` is recorded alongside the primary status. |
| Final lane | Optional evidence only, follow-up, crawler graduation, or explicit review required. |
| Result summary | Explains what changed, what was learned, or why no work remains. |
| Fixed public MVP boundary unchanged | Says yes, or links explicit review before implementation. |
| Public API shape unchanged | Says yes, or links explicit review before implementation. |
| Label status | Says labels or written label equivalents are record aids only. |
| Linked split, follow-up, or explicit review | Required for `split`, `follow-up`, and `explicit-review`; otherwise says none and why none is needed. |
| Next recheck date or reason none is needed | Required for `keep-open`; terminal records explain why no recheck remains. |

A record with a primary status but missing owner, evidence source, boundary
answer, public API answer, link requirement, or recheck answer is incomplete.
Do not treat it as closed until the missing field is written.

## Link Integrity

`split`, `follow-up`, and `explicit-review` are the decisions most likely to
orphan work. The original record can stop carrying action only when these link
conditions are satisfied.

| Stale hygiene decision | Required link condition |
| --- | --- |
| `split` | Link every split record. Each split record has one lane, one owner, one close condition, and one recheck command or evidence source. The original record says it is an index, not the action holder. |
| `follow-up` | Link the issue or PR. The linked record has one root cause, one owner, one validation plan, and one recheck command. |
| `explicit-review` | Link the explicit review record. The linked record names the decision authority, decision scope, rollback path, cost or risk owner when relevant, and current decision status. |

A link is not enough by itself. The target must be reachable by the next
reviewer, must describe the next action, and must not send the reviewer back to
the original record for ownership. If the target cannot be opened or does not
name an owner and close condition, the closeout record is still orphan-prone.

## Orphan Prevention

Use this pass to find records that look closed but still hide work:

- `closeout:closed` with an implied split, follow-up, or explicit review but no
  reachable link
- `closeout:kept-open` without owner, next recheck date or condition, narrow
  evidence source, or waiting reason
- `closeout:split` where the original record still carries unresolved action
  after splits were created
- `closeout:follow-up-opened` where the linked issue or PR lacks root cause,
  owner, validation plan, or recheck command
- `closeout:explicit-review-linked` where the review record lacks authority,
  scope, rollback path, risk or cost owner when relevant, or decision status
- repeated stale or repeated keep-open recorded only as a marker, with no route
  to explicit review, split, follow-up, or final dated external wait

When one of these appears, update the record before closing or moving on. If
the missing action would change public profile, public API shape, managed
infrastructure, full-mode/OpenSearch production role, crawler maturity outside
the graduation lane, or final-ranking ownership, route to explicit review.

## Status and Marker Consistency

Primary closeout status tells the current decision. Repeat or escalation marker
tells whether the record has already failed passive waiting. They are related,
but one never replaces the other.

| Primary closeout status | Allowed repeat or escalation marker | Integrity condition |
| --- | --- | --- |
| `closeout:closed` | `none` | No next action remains, no recheck remains, fixed boundary is unchanged, and public API shape is unchanged. |
| `closeout:kept-open` | `none` or `final allowed keep-open` | Owner, next recheck date or condition, evidence source, and waiting reason are present. `final allowed keep-open` also explains the dated external event and why no split, follow-up, or explicit review is needed yet. |
| `closeout:split` | `none`, `closeout:repeated-stale`, or `repeated keep-open` | Every split target is linked and owns the next action. Repeat markers are acceptable only when split is the route out of stale waiting. |
| `closeout:follow-up-opened` | `none`, `closeout:repeated-stale`, or `repeated keep-open` | The linked issue or PR owns scoped work that can answer the question without changing the public MVP profile. |
| `closeout:explicit-review-linked` | `none`, `closeout:repeated-stale`, or `repeated keep-open` | The linked explicit review owns the decision authority for boundary, API, public profile, managed infrastructure, full-mode/OpenSearch, crawler maturity, or final-ranking ownership risk. |

`closeout:repeated-stale` and `repeated keep-open` must not appear with
`closeout:closed` in an integrity-complete record. If the repeated condition
has been resolved and no action remains, keep the history in the result summary
and set the current repeat or escalation marker to `none`.

`final allowed keep-open` belongs only with `closeout:kept-open`. If that final
date or condition passes stale again, the next closeout decision must route to
`split`, `follow-up`, or `explicit-review`.

## Body Markers and Label Aids

Labels are record aids only. They are not required for the fixed public MVP
gate, local validation, PR merge, recheck audit, closeout ledger, or integrity
pass.

If labels are unavailable, write the primary closeout status and repeat or
escalation marker in the issue, PR, or review note body. Body-recorded markers
are authoritative enough for the integrity pass:

- `closeout:closed`
- `closeout:kept-open`
- `closeout:split`
- `closeout:follow-up-opened`
- `closeout:explicit-review-linked`
- `closeout:repeated-stale`
- `repeated keep-open`
- `final allowed keep-open`

Do not fail integrity because a GitHub label is missing. Fail it because the
record body does not say the status, marker, owner, evidence source, link, next
action, boundary answer, or public API answer.

## Boundary-Specific Integrity Checks

Crawler graduation can be packet complete and still remain outside the fixed
gate. An integrity pass may confirm that the packet is ready, deferred, split,
or routed to explicit review, but it must not add live crawler operation to
`just mvp-acceptance`.

Full-mode automation can be useful and still remain outside the fixed gate. An
integrity pass may confirm a follow-up for comparison coverage or operator
tooling, but it must not add `full` mode or OpenSearch to the fixed six-case
gate.

Managed infrastructure stays explicit review only. An integrity pass may close
or defer only when the explicit review record is linked and its current decision
status is recorded.

Strict data-quality doctor `review_items` stay human-classified evidence. An
integrity pass should confirm the human classification is named before
implementation work opens or closes.

## Integrity Note Template

Paste this under a closeout ledger record when checking that it is complete:

```text
Optional evidence closeout integrity:
- Date:
- Reviewer:
- Record:
- Closeout ledger record present:
- Required fields complete:
- Primary closeout status:
- Repeat or escalation marker:
- Status and marker are consistent:
- Split links reachable or none needed:
- Follow-up link reachable or none needed:
- Explicit review link reachable or none needed:
- Original record is no longer the action holder, or still has owner/recheck:
- Repeated stale / repeated keep-open next action:
- Fixed public MVP boundary unchanged:
- Public API shape unchanged:
- Labels or written label equivalents were record aids only:
- Integrity result: complete / needs closeout edit / needs linked-record edit / route to explicit review
```

## Integrity Checklist

Before accepting a closeout record as complete, confirm:

1. The closeout ledger record exists where the next reviewer will look first.
2. All required closeout fields are present.
3. The primary closeout status matches the stale hygiene decision.
4. Repeat or escalation marker is recorded separately from primary status.
5. `split`, `follow-up`, and `explicit-review` records link reachable targets.
6. Linked targets own their next action with owner, scope, close condition, and
   recheck or decision source.
7. Repeated stale or repeated keep-open has a route to explicit review, split,
   follow-up, or one final dated external wait.
8. Labels are treated as record aids only; body-recorded equivalents are enough.
9. Crawler graduation, full mode, OpenSearch, managed infrastructure, and public
   API changes remain outside the fixed public MVP gate unless explicit review
   approves a separate implementation change.
10. Strict doctor `review_items` are human-classified before implementation
    work starts.
11. The lifecycle index can point to this integrity result as review inventory
    only; it does not become a gate or replace the source records.
12. The inventory report can summarize this integrity result and any orphan,
    stale, or unclear-owner finding as an edit candidate only; it does not
    become a gate or replace the source records.
