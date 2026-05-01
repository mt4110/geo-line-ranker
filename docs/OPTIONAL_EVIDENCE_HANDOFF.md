# Optional Evidence Operating Handoff

This is the read-only operating handoff guide for optional evidence closeout.
Use it when crawler, full-mode, OpenSearch, managed infrastructure, data-quality,
or local review artifact evidence needs a human decision without widening the
fixed public-MVP gate.

## Fixed Boundary

- Public-MVP candidate retrieval stays `sql_only`.
- Public-MVP operational content stays `event-csv`.
- PostgreSQL/PostGIS remains the write-store reference implementation.
- Redis remains cache only.
- `just mvp-acceptance` remains the fixed six-case public-MVP gate.
- `DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor` remains strict
  release and post-MVP evidence. Doctor `review_items` are classified by
  humans before issue or PR work starts.
- OpenSearch, `full` mode, live crawler operation, and managed infrastructure
  stay outside the fixed gate unless the public-MVP boundary changes through
  explicit review.
- Optional evidence records, lifecycle index rows, inventory reports, and
  findings are review inventory and handoff support only.
- Labels are record aids only. They are not required gates, CI prerequisites, or
  repository setup requirements.
- This handoff does not change public API shape. Public API changes still require
  `schemas/openapi.json` and `API_SPEC.md` updates in the same change.

## Operating Flow

| Moment | Use this record | Operator action | Output |
|---|---|---|---|
| Intake | Optional evidence intake record | Capture the evidence source, command, owner, recheck command or source, public-MVP boundary impact, public API impact, next action, and issue or PR pointer. Keep raw diffs and review bodies in their artifact location; refer to manifests and checksums instead of pasting raw content into public docs. | A bounded source record for one evidence item. |
| Triage | Optional evidence triage record | Classify the item as close, follow-up, recheck, split, or explicit review. Labels may help search and grouping, but absence of a label is not a failure. | A human decision lane with an owner and next step. |
| Recheck audit | Optional evidence recheck audit | For stale or changed evidence, record what was rechecked and whether the prior lane still holds. Recheck commands are evidence capture, not acceptance-gate cases. | A current audit note or a route back to triage. |
| Closeout ledger | Optional evidence closeout ledger | Record why the item was closed, kept open, split, routed to follow-up, or routed to explicit review. Keep the decision separate from repeat or escalation markers. | Durable decision history. |
| Closeout integrity | Optional evidence closeout integrity note | Check that closeout links are reachable, owners are clear, and repeat markers do not contradict the primary closeout status. | Integrity notes or edit candidates for the source record, linked record, or ledger. |
| Lifecycle index | Optional evidence lifecycle index row | Add or update one read-only index row per evidence item with state, owner, source pointer, recheck pointer, closeout pointer, and integrity pointer. | Review inventory that can be scanned without replacing the source record. |
| Inventory report | Optional evidence inventory report | Share a safe snapshot of states, counts, and findings. Do not print raw `pr.diff`, raw review output, secrets, or unrelated artifact bodies. | Handoff summary for humans, not a source of truth. |

## Findings

Treat orphan, stale, and unclear-owner findings as edit candidates, not gate
failures:

- Orphan finding: link the item to a source record, linked record, closeout
  ledger entry, or integrity note.
- Stale finding: update the recheck audit or route the item back through triage.
- Unclear-owner finding: assign or correct the owner in the source record,
  lifecycle row, or closeout ledger.

If a finding suggests a real product or release risk, open follow-up work through
the normal issue or PR path. Do not convert the finding itself into an
acceptance gate, release gate, CI gate, or required label setup.

## Command Plan

Print the read-only optional evidence handoff plan:

```bash
just optional-evidence-review
```

Without `just`:

```bash
./scripts/optional_evidence_review.sh
```

The command prints this handoff boundary and validation command list for
operators. It does not run validation, mutate services, create labels, or change
repository configuration.
