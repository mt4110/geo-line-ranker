# Optional Evidence Recheck Audit

Use this guide after optional evidence records have a recheck date, a
`needs-recheck` marker, or an open follow-up that may have gone stale. Its job
is to find records that are still open, classify their recheck state, and route
them back to a close, keep-open, split, follow-up, or explicit-review decision
without widening the fixed public MVP gate.

After choosing the stale hygiene decision, write the decision history with
[OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md](OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md).
The closeout ledger records why the record closed, stayed open, split, opened a
follow-up, or linked explicit review.
Then use
[OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md](OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md)
to check that the closeout record is complete, linked records are reachable,
and repeat markers have an owner and next action.

This is a read-only audit and stale hygiene guide. It does not create GitHub
labels, run validation by itself, change source maturity, enable full mode,
require OpenSearch, provision managed infrastructure, or change public API
shape.

## Fixed Boundary

Every audit decision must preserve this public MVP boundary:

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

Public API changes should not be introduced by recheck audit. If a stale record
would change public API shape, route it to explicit review required and update
`schemas/openapi.json` plus create or update `API_SPEC.md` only if that
document is part of the approved implementation change.

## Audit Inputs

Start from the open optional evidence surface:

- optional evidence issues
- optional evidence PRs
- review notes that include the optional evidence intake header
- records with `needs-recheck`, a written label equivalent, or a recheck date
- linked crawler, full-mode, managed infrastructure, or doctor evidence packets

Labels are record aids only. GitHub label creation is not required for the
fixed public MVP gate, local validation, PR merge, or this audit. If labels are
not available, inspect the issue, PR, or review note body for the same written
fields.

## Read-Only Audit Commands

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

If labels are not configured, use the repository issue and PR search UI, or
search bodies for `optional evidence`, `needs-recheck`, `recheck date`, and the
lane names. The absence of labels is not a gate failure.

Do not run the lane-specific validation commands during the inventory step.
Record the narrow command or evidence source that the owner must rerun next.

## Stale Recheck Classes

Use one class per record. If one record needs more than one class or lane, split
it before implementation starts.

Stale recheck is the umbrella state for any open record whose recheck date has
arrived, whose `needs-recheck` marker has no current result, or whose next
action is unclear after intake. Classify stale rechecks into the specific
classes below before choosing a stale hygiene decision.

| Class | Use when | Owner | Next action | Close condition |
| --- | --- | --- | --- | --- |
| `recheck:on-time` | The recheck date has not arrived and owner, command or evidence source, lane, and close condition are present. | The recorded lane owner. | Keep open until the named date or condition. Update only if ownership or the evidence source changes. | Close later under the lane close condition after the recheck result is recorded. |
| `recheck:overdue` | The recheck date has passed and no current result is recorded, but the command or evidence source is still available. | The recorded lane owner, or the issue or PR assignee if the owner is missing. | Rerun or inspect the narrow recheck source, record the result, choose a stale hygiene decision, and set a new date only when more waiting is justified. | Close when the result satisfies the lane close condition and no boundary, public API, source-maturity, full-mode/OpenSearch, or managed-infra change is pending. |
| `recheck:blocked` | The date has passed or the record cannot move because owner, evidence, access, command, explicit-review decision, or source context is missing. | The current assignee until a named owner is recorded. | Record the blocker, name the owner needed to unblock it, route to explicit review if the blocker is boundary/API/infra/profile risk, or split if mixed evidence caused the block. | Close only after the blocker is resolved and the lane close condition is satisfied, or after the explicit review record says deferred or rejected. |
| `recheck:split-needed` | The record mixes multiple lanes, owners, root causes, commands, or decision authorities. | The triage owner. | Split into scoped records before implementation. Keep only the original evidence index in the old record. | Close the original record after every split record is linked and the original no longer carries an unresolved action. |
| `recheck:closed` | The recheck result is recorded and the lane close condition is satisfied. | The closing owner. | Remove or mark `needs-recheck` complete when labels are available, add the closeout note, and close the record. | The closeout note confirms fixed boundary unchanged, public API shape unchanged, and any follow-up or explicit review is linked. |

`recheck:overdue` is not a failure by itself. It is a sign that the record must
return to an owner, command or evidence source, and close condition instead of
remaining open without a decision.

## Stale Hygiene Decisions

After classifying an overdue or blocked record, choose exactly one decision:

| Decision | Use when | Required record |
| --- | --- | --- |
| `close` | Evidence is recorded, no action remains, and the lane close condition is met. | Closeout ledger record with fixed boundary unchanged, public API shape unchanged, and a reason no next recheck is needed. |
| `keep-open` | The record is still waiting for a named owner plus a dated evidence source or external decision. | Closeout ledger record with owner, next recheck date or condition, narrow recheck source, waiting reason, and repeat or escalation marker. |
| `split` | The record mixes evidence, lanes, owners, commands, or close conditions. | Closeout ledger record with links to every split record and the reason the original is no longer the action holder. |
| `follow-up` | A scoped implementation or documentation task is needed without changing the public MVP profile. | Closeout ledger record with a linked issue or PR that has one root cause, one owner, one validation plan, and one recheck command. |
| `explicit-review` | The next action could change public profile, API shape, managed infrastructure, full-mode/OpenSearch production role, crawler maturity outside the graduation lane, or final-ranking ownership. | Closeout ledger record with a linked explicit review naming the decision authority, rollback path, cost or risk owner when relevant, and decision status. |

Crawler graduation can become packet-complete and still remain outside the
fixed gate. Full-mode automation can become useful and still not add `full`
mode or OpenSearch to `just mvp-acceptance`. Managed infrastructure always
stays explicit review only.

## Decision History

Use [OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md](OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md)
after every stale hygiene decision. The ledger defines the required closeout
record fields for `close`, `keep-open`, `split`, `follow-up`, and
`explicit-review`, plus primary closeout status aids:

- `closeout:closed`
- `closeout:kept-open`
- `closeout:split`
- `closeout:follow-up-opened`
- `closeout:explicit-review-linked`

It also defines repeat or escalation markers that can be recorded alongside the
primary closeout status. Suggested label aid:

- `closeout:repeated-stale`

Body-recorded markers:

- `repeated keep-open`
- `final allowed keep-open`

Labels are still record aids only. If labels are unavailable, write the
primary closeout status and any repeat or escalation marker in the record body.

Do not close `split`, `follow-up`, or `explicit-review` decisions without the
linked split records, linked issue or PR, or linked explicit review record.
Repeated stale or repeated keep-open records must return to explicit review,
split, or follow-up unless one final dated external wait is recorded.
Use [OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md](OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md)
after the ledger entry to confirm those links and repeat-marker routes are not
orphaned.

## Lane-Specific Overdue Handling

| Lane | Overdue owner | Next action | Close only when |
| --- | --- | --- | --- |
| Optional evidence only | The operator or reviewer who received the evidence. | Inspect the original evidence source, confirm why no implementation is needed, and remove or mark complete the recheck marker when labels are available. | The evidence source, fixed boundary check, and reason for no implementation are recorded; no public API, source maturity, full-mode/OpenSearch, or managed-infra change is pending. |
| Follow-up | The issue or PR owner for the scoped improvement. | Rerun the command, request, SQL sample, or review step that exposed the finding. If implementation files changed, run the validation set before reporting completion. | The linked issue or PR is opened or resolved with one root cause, one owner, one validation plan, and one recheck command; the fixed public MVP boundary is unchanged. |
| Crawler graduation | The source owner or parser owner. | Rerun crawler doctor, dry-run, and health for the manifest, plus parser tests when parser behavior changed. | The crawler packet is current, source policy and robots checks are current, promotion blockers are clear or explicitly accepted, rollback owner and path are recorded, and live crawler operation remains outside the fixed gate. |
| Explicit review required | The decision authority named in the record. | Inspect the explicit review record. Do not provision managed services, change public API shape, or promote full-mode/OpenSearch production roles from the audit itself. | The explicit review decision is recorded as approved, deferred, or rejected; any approved implementation moves into a separate issue or PR with its own validation plan. |

## Recheck Audit Checklist

Use this checklist for each open optional evidence record:

1. Confirm the minimal intake header is present or linked.
2. Confirm the fixed public MVP boundary is unchanged.
3. Confirm public API shape is unchanged, or route to explicit review required.
4. Confirm the primary lane, owner, recheck date, recheck source, and close
   condition are recorded.
5. Classify the record as `recheck:on-time`, `recheck:overdue`,
   `recheck:blocked`, `recheck:split-needed`, or `recheck:closed`.
6. For overdue or blocked records, choose exactly one stale hygiene decision.
7. Record the owner, next action, and close condition.
8. Keep labels as aids only; written label equivalents are enough.
9. Keep strict doctor `review_items` as human-classified evidence.
10. Keep crawler graduation, full-mode/OpenSearch, and managed infrastructure
    outside the fixed gate.
11. After writing closeout history, confirm closeout integrity and orphan
    prevention with
    [OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md](OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md).

## Audit Note Template

Paste this into the issue, PR, or review note when auditing a stale recheck:

```text
Optional evidence recheck audit:
- Date:
- Auditor:
- Record:
- Stale class: recheck:on-time / recheck:overdue / recheck:blocked / recheck:split-needed / recheck:closed
- Stale hygiene decision: close / keep-open / split / follow-up / explicit-review
- Decision lane:
- Owner:
- Recheck overdue since, if any:
- Recheck command or evidence source:
- Next action:
- Close condition:
- Fixed public MVP boundary unchanged:
- Public API shape unchanged:
- Next recheck date or condition, if any:
- Linked follow-up, split record, or explicit review:
```

## Closeout Comment Template

Paste this before closing or keeping an overdue or stale record open. The full
ledger guidance lives in
[OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md](OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md).

```text
Optional evidence closeout ledger:
- Closeout date:
- Owner:
- Record:
- Original evidence source:
- Stale class: recheck:on-time / recheck:overdue / recheck:blocked / recheck:split-needed / recheck:closed
- Stale hygiene decision: close / keep-open / split / follow-up / explicit-review
- Primary closeout status: closeout:closed / closeout:kept-open / closeout:split / closeout:follow-up-opened / closeout:explicit-review-linked
- Repeat or escalation marker: none / closeout:repeated-stale / repeated keep-open / final allowed keep-open
- Final lane:
- Result summary:
- Fixed public MVP boundary unchanged:
- Public API shape unchanged:
- Labels or written label equivalents were record aids only:
- Linked split, follow-up, or explicit review:
- Next recheck date or reason none is needed:
```

## Validation When Files Change

When a stale hygiene decision leads to code, docs, fixtures, scripts, or
template changes, run the full validation set before reporting completion:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
just mvp-acceptance
DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
git diff --check
```

The audit inventory itself remains read-only. Validation belongs to the
implementation or documentation change that follows from the audit decision.
