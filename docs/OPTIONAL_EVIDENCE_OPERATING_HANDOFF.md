# Optional Evidence Operating Handoff

Use this guide as the final operating map for the optional evidence workflow
introduced across Phase 17 through Phase 24. Its job is to tell an operator
which document to open next, how the record should move toward closeout, and
what must remain outside the fixed public MVP gate.

This handoff is review and operating support only. It is not an acceptance gate,
release gate, required label setup, validation result, automation requirement,
or replacement source of truth.

## Fixed Public MVP Boundary

This boundary is immutable for the public MVP:

- Candidate retrieval: `sql_only`
- Operational content path: `event-csv`
- Write store: PostgreSQL/PostGIS
- Cache: Redis
- Fixed gate: `just mvp-acceptance`

Keep these outside the fixed gate:

- crawler graduation and live crawler operation, even when a packet is complete
- `full` mode, OpenSearch, projection sync, and full-mode automation candidates
- managed infrastructure, hosting, production IaC, and managed services
- ML, embeddings, vector search, or frontend final-ranking logic

Strict data-quality doctor output remains evidence:

```bash
DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor
```

Doctor warnings fail the evidence pass. Doctor `review_items` are classified by
humans before issue or PR work starts.

Public API shape should not change in this workflow. If a finding points to
work that would change public API shape, route it to explicit review before
implementation and update `schemas/openapi.json` plus create or update
`API_SPEC.md` only in the approved implementation change.

## Final Operating Flow

Follow the flow in this order. Skip a step only when the source record already
contains the required fields from that step.

| Moment | Use when | Open this doc | Record produced |
| --- | --- | --- | --- |
| Intake | New crawler, full-mode, OpenSearch, managed infrastructure, strict doctor, or mixed evidence arrives. | [OPTIONAL_EVIDENCE_INTAKE.md](OPTIONAL_EVIDENCE_INTAKE.md) | Minimal intake header with evidence type, packet choice, lane, owner, recheck date, fixed boundary, public API status, and strict doctor status. |
| Triage | An issue, PR, or review note exists and needs lane, owner, recheck command, or close condition. | [OPTIONAL_EVIDENCE_TRIAGE.md](OPTIONAL_EVIDENCE_TRIAGE.md) | One primary lane, owner, recheck command or evidence source, close condition, and label or written-label aids. |
| Recheck audit | A recheck date arrived, `needs-recheck` is present, or the next action is unclear. | [OPTIONAL_EVIDENCE_RECHECK_AUDIT.md](OPTIONAL_EVIDENCE_RECHECK_AUDIT.md) | Stale class and one stale hygiene decision: `close`, `keep-open`, `split`, `follow-up`, or `explicit-review`. |
| Closeout ledger | A stale hygiene decision needs durable decision history. | [OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md](OPTIONAL_EVIDENCE_CLOSEOUT_LEDGER.md) | Closeout record with primary status, repeat or escalation marker, final lane, result summary, linked records, and next recheck answer. |
| Closeout integrity | A closeout record is ready to close, keep open, split, hand off, or move action elsewhere. | [OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md](OPTIONAL_EVIDENCE_CLOSEOUT_INTEGRITY.md) | Integrity result: `complete`, `needs closeout edit`, `needs linked-record edit`, or `route to explicit review`. |
| Lifecycle index | Multiple optional evidence records need one read-only shelf view. | [OPTIONAL_EVIDENCE_LIFECYCLE_INDEX.md](OPTIONAL_EVIDENCE_LIFECYCLE_INDEX.md) | Lifecycle row with state, owner, lane, stale status, closeout status, linked action, next recheck answer, and integrity result. |
| Inventory report | Rows need a shareable review snapshot or handoff summary. | [OPTIONAL_EVIDENCE_LIFECYCLE_INVENTORY_REPORT.md](OPTIONAL_EVIDENCE_LIFECYCLE_INVENTORY_REPORT.md) | Report header, summary counts, rows, and orphan/stale/unclear-owner findings. |

## Closeout Rules

- Keep the source record as the source of truth. The issue, PR, review note,
  packet, closeout ledger entry, or integrity note owns the durable record.
- Use lifecycle index rows and inventory reports only to reduce search cost.
  They do not approve, reject, fail, or replace the source records.
- Treat orphan, stale, and unclear-owner findings as review findings, not gate
  failures.
- Resolve findings by editing the source record, linked record, closeout
  ledger, or integrity note. Route to explicit review only when the edit would
  affect public profile, public API shape, managed infrastructure,
  full-mode/OpenSearch production role, crawler maturity outside the crawler
  graduation lane, or final-ranking ownership.
- Keep labels and written label equivalents as record aids only. Missing GitHub
  labels are not gate failures.

## Handoff Checklist

Before handing off or closing an optional evidence review set, confirm:

1. Every open record has a source link, one owner, one lane, one close
   condition, and a next recheck date or reason none is needed.
2. Every stale record has a recheck audit class and one stale hygiene decision.
3. Every `close`, `keep-open`, `split`, `follow-up`, or `explicit-review`
   decision has a closeout ledger record.
4. Every split, follow-up, or explicit-review decision has a reachable linked
   target before the original record stops carrying action.
5. Closeout integrity is complete, or its result names the exact source-record,
   linked-record, closeout, or integrity edit still needed.
6. Lifecycle index rows and inventory report findings remain review inventory
   and handoff support only.
7. The fixed public MVP boundary remains `sql_only` + `event-csv` +
   PostgreSQL/PostGIS + Redis, with `just mvp-acceptance` as the fixed gate.
8. Crawler graduation, full-mode automation, OpenSearch, and managed
   infrastructure remain outside the fixed gate.
9. Managed infrastructure remains explicit review only.
10. Public API shape is unchanged, or a separate explicit review owns the
    approved implementation work.

## Phase 26 And Later

After this operating handoff, the next improvement area is human-facing
onboarding polish:

- README orientation
- Quickstart command flow
- default sample clarity
- first-run guidance for what to run, what success looks like, and where to go
  next

Those improvements should make the repository easier to use without changing
the fixed public MVP boundary or turning optional evidence into a gate.
