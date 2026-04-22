# Optional Evidence Closeout Ledger

Use this guide after optional evidence triage or recheck audit has returned a
record to a decision. Its job is to leave decision history that can be read
later without guessing why the record was closed, kept open, split, routed to a
follow-up, or routed to explicit review.

The ledger is a record format, not a new acceptance gate. It does not create
GitHub labels, run validation by itself, change source maturity, enable full
mode, require OpenSearch, provision managed infrastructure, or change public API
shape.

## Fixed Boundary

Every closeout record must preserve this public MVP boundary:

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

Public API shape should not change as part of closeout bookkeeping. If a
decision would change public API shape, link an explicit review record and
update `schemas/openapi.json` plus `API_SPEC.md` only in the approved
implementation change.

## Ledger Location

Write the closeout record where the next reviewer will look first:

- the issue or PR comment for the optional evidence record
- the review note that carried the intake header
- the original record plus links to split, follow-up, or explicit-review records

Do not create a separate tracking surface unless the issue, PR, or review note
cannot hold the record. The closeout ledger should reduce search cost, not add a
parallel source of truth.

## Closeout Labels

Labels are record aids only. GitHub label creation is not required for the fixed
public MVP gate, local validation, PR merge, recheck audit, or closeout ledger.
If labels are unavailable, write the same status in the record body.

Suggested closeout statuses:

- `closeout:closed`
- `closeout:kept-open`
- `closeout:split`
- `closeout:follow-up-opened`
- `closeout:explicit-review-linked`
- `closeout:repeated-stale`

`closeout:repeated-stale` is a warning status, not a terminal lane. It means the
record has come back stale enough times that it must be routed to explicit
review, split into smaller records, or moved into a scoped follow-up.

## Required Closeout Record Fields

Every closeout record must include these fields:

| Field | Required value |
| --- | --- |
| Closeout date | Calendar date when the decision was recorded. |
| Owner | Named person or role responsible for the decision and any next action. |
| Original evidence source | Link, command output, issue, PR, review note, packet, or doctor result that started the record. |
| Stale class | One of `recheck:on-time`, `recheck:overdue`, `recheck:blocked`, `recheck:split-needed`, or `recheck:closed`. |
| Stale hygiene decision | One of `close`, `keep-open`, `split`, `follow-up`, or `explicit-review`. |
| Final lane | Optional evidence only, follow-up, crawler graduation, or explicit review required. |
| Result summary | Short explanation of what changed, what was learned, or why no work remains. |
| Fixed public MVP boundary unchanged | Must say yes, or link explicit review before implementation. |
| Public API shape unchanged | Must say yes, or link explicit review before implementation. |
| Label status | State that labels or written label equivalents are record aids only. |
| Linked split, follow-up, or explicit review | Required for `split`, `follow-up`, and `explicit-review`; otherwise say none and why none is needed. |
| Next recheck date or reason none is needed | Required for `keep-open`; for terminal records, explain why no recheck remains. |
| Repeat status | State first closeout, repeated stale, repeated keep-open, or final allowed keep-open. |

## Stale Hygiene to Closeout Mapping

Use the stale hygiene decision from
[OPTIONAL_EVIDENCE_RECHECK_AUDIT.md](OPTIONAL_EVIDENCE_RECHECK_AUDIT.md), then
record the matching closeout status.

| Stale hygiene decision | Closeout status | Closeout record condition |
| --- | --- | --- |
| `close` | `closeout:closed` | Recheck result is recorded, lane close condition is satisfied, no action remains, fixed boundary is unchanged, public API shape is unchanged, and no unlinked follow-up or explicit review exists. |
| `keep-open` | `closeout:kept-open` | Owner, next recheck date or condition, narrow evidence source, waiting reason, and repeat status are recorded. This is not a final close. |
| `split` | `closeout:split` | Every split record is linked, each split has one lane and owner, and the original record no longer carries an unresolved action. |
| `follow-up` | `closeout:follow-up-opened` | A linked issue or PR exists with one root cause, one owner, one validation plan, and one recheck command. |
| `explicit-review` | `closeout:explicit-review-linked` | A linked explicit review record names the decision authority, decision scope, rollback path, cost or risk owner when relevant, and current decision status. |

## Decision Conditions

### Close

Use `close` only when the evidence has a recorded result and the lane close
condition is satisfied. The closeout record must explain why no next recheck is
needed.

Do not close if a split, follow-up, or explicit-review action is implied but not
linked. Do not close if the fixed public MVP boundary or public API shape would
change.

### Keep Open

Use `keep-open` only when waiting is concrete: a named owner, next recheck date
or condition, narrow evidence source, and reason to wait are all present.

`keep-open` is not a place to park unclear design work. If the record has no
new evidence, no dated external decision, or no owner who can unblock it, route
it to `split`, `follow-up`, or `explicit-review` instead.

### Split

Use `split` when one record mixes lanes, owners, root causes, commands, or
decision authorities. The original record can close only after every split
record is linked and the original clearly says it is no longer the action
holder.

Each split record should have one lane, one owner, one close condition, and one
recheck command or evidence source.

### Follow-Up

Use `follow-up` when one scoped implementation or documentation task is needed
without changing the public MVP profile. The closeout record must link the issue
or PR before the original record closes.

The linked follow-up must name one root cause, one owner, one validation plan,
and one recheck command. Full-mode automation candidates can be follow-ups only
when they remain optional comparison or operator tooling and do not add `full`
mode or OpenSearch to the fixed gate.

### Explicit Review

Use `explicit-review` when the next action could change public profile, public
API shape, managed infrastructure, full-mode or OpenSearch production role,
crawler maturity outside the crawler graduation lane, or final-ranking
ownership.

The closeout record must link the explicit review record before the original
record closes. Managed infrastructure is always explicit review only. Approved
implementation work moves into a separate issue or PR with its own validation
plan.

## Repeated Stale and Repeated Keep-Open

Repeated stale means the same record has returned to stale audit without a
current result, or the next recheck date passed after a previous keep-open.
Repeated keep-open means the same record receives two consecutive `keep-open`
decisions, or receives a new recheck date without new evidence.

Repeated stale or repeated keep-open records must not stay in passive waiting.
Choose one next action:

- Route to `explicit-review` when the blocker is decision authority, public API,
  public profile, managed infrastructure, full-mode/OpenSearch production role,
  crawler maturity outside the crawler graduation lane, or final-ranking
  ownership.
- Route to `split` when mixed evidence, lanes, owners, commands, or close
  conditions are making the record hard to resolve.
- Route to `follow-up` when a scoped implementation or documentation task would
  answer the question without changing the public MVP profile.

One final `keep-open` is allowed only when the record names a dated external
event, the owner who will inspect it, the narrow evidence source, and why no
split, follow-up, or explicit review is needed yet. If that date passes stale
again, route the record to `explicit-review` or `split`.

## Boundary-Specific Notes

Crawler graduation can become packet complete and still remain outside the
fixed gate. A closeout record can say the packet is ready, deferred, or split,
but it must not add live crawler operation to `just mvp-acceptance`.

Full-mode automation can become useful and still remain outside the fixed gate.
A closeout record can open a follow-up for comparison coverage or operator
tooling, but it must not add `full` mode or OpenSearch to the fixed six-case
gate.

Managed infrastructure stays explicit review only. Do not close a managed
infrastructure record as follow-up or closed unless the explicit review record
is linked and the closeout summary explains the decision status.

Strict data-quality doctor `review_items` stay human-classified evidence. A
closeout record should name the human classification before opening or closing
implementation work.

## Closeout Record Template

Paste this into the issue, PR, or review note when recording the final or
current decision:

```text
Optional evidence closeout ledger:
- Closeout date:
- Owner:
- Record:
- Original evidence source:
- Stale class: recheck:on-time / recheck:overdue / recheck:blocked / recheck:split-needed / recheck:closed
- Stale hygiene decision: close / keep-open / split / follow-up / explicit-review
- Closeout status: closeout:closed / closeout:kept-open / closeout:split / closeout:follow-up-opened / closeout:explicit-review-linked / closeout:repeated-stale
- Final lane:
- Result summary:
- Fixed public MVP boundary unchanged:
- Public API shape unchanged:
- Labels or written label equivalents were record aids only:
- Linked split, follow-up, or explicit review:
- Next recheck date or reason none is needed:
- Repeat status:
```

## Closeout Checklist

Before closing or keeping an optional evidence record open, confirm:

1. The original evidence source is linked or named.
2. The stale class and stale hygiene decision are recorded.
3. The final lane and owner are recorded.
4. The fixed public MVP boundary is unchanged.
5. Public API shape is unchanged, or explicit review is linked before
   implementation.
6. Labels are treated as record aids only.
7. `split`, `follow-up`, and `explicit-review` decisions have links before the
   original record closes.
8. `keep-open` has a next recheck date or condition and a reason it is still
   waiting.
9. Repeated stale or repeated keep-open is routed to explicit review, split, or
   follow-up unless one final dated external wait is recorded.
10. Crawler graduation, full mode, OpenSearch, managed infrastructure, and
    public API changes remain outside the fixed public MVP gate.
