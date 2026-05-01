# Documentation Index

Start here when choosing what to read next. This page is the routing map by
audience and task. For the actual first-run sequence, use
[First 15 Minutes](FIRST_15_MINUTES.md).

## What Each Entry Point Does

| Entry point | Use it for |
|---|---|
| [Project README](../README.md) | First impression, project principles, shortest local path, and the first success state. |
| [First 15 Minutes](FIRST_15_MINUTES.md) | The first reading path, baseline commands, default sample inspection, fixed-vs-optional boundary, and touch map. |
| This index | Choosing the next document by audience or by task after the first-run guide. |
| [Quickstart](QUICKSTART.md) | Command-by-command local runbook for the SQL-only public-MVP path and optional follow-on paths. |

## Choose By Audience

| Audience | Read first | Read next |
|---|---|---|
| New contributor | [Project README](../README.md), then [First 15 Minutes](FIRST_15_MINUTES.md) | [Quickstart](QUICKSTART.md), [Local Contributing Guide](CONTRIBUTING_LOCAL.md), [Testing](TESTING.md) |
| Operator | [First 15 Minutes](FIRST_15_MINUTES.md), then [Quickstart](QUICKSTART.md) | [MVP Acceptance](MVP_ACCEPTANCE.md), [Operations](OPERATIONS.md), [Optional Evidence Handoff](OPTIONAL_EVIDENCE_HANDOFF.md) |
| Maintainer | [Contributor Rules](../AGENTS.md), then [Architecture](ARCHITECTURE.md) | [API Spec](../API_SPEC.md), [Versioning](VERSIONING.md), [Deprecation Policy](DEPRECATION_POLICY.md), [Testing](TESTING.md), [v0.2.1 to v0.2.5 Design Plan](V0_2_1_TO_V0_2_5_DESIGN_PLAN.md) |
| Profile author | [Profile Packs](PROFILE_PACKS.md), then [Local Discovery Generic](../examples/local-discovery-generic/README.md) | [School Event JP Reference](../examples/school-event-jp/README.md), [Architecture](ARCHITECTURE.md), [Reason Catalog](REASON_CATALOG.md), [Versioning](VERSIONING.md) |
| Connector author | [Data Sources](DATA_SOURCES.md), then [Data Licenses](DATA_LICENSES.md) | [Quickstart](QUICKSTART.md), [Operations](OPERATIONS.md), [Local Contributing Guide](CONTRIBUTING_LOCAL.md), [Optional Evidence Handoff](OPTIONAL_EVIDENCE_HANDOFF.md) |

## Choose By Task

| Task | Start here | Then check |
|---|---|---|
| Run the baseline locally | [First 15 Minutes](FIRST_15_MINUTES.md) | [Quickstart](QUICKSTART.md), [MVP Acceptance](MVP_ACCEPTANCE.md) |
| Make a bounded code change | [Local Contributing Guide](CONTRIBUTING_LOCAL.md) | [Testing](TESTING.md), [Architecture](ARCHITECTURE.md) |
| Change ranking scores, reasons, or fallback behavior | [Reason Catalog](REASON_CATALOG.md) | [Architecture](ARCHITECTURE.md), [Testing](TESTING.md), [Versioning](VERSIONING.md) |
| Change public API fields | [API Spec](../API_SPEC.md) | [Versioning](VERSIONING.md), [Deprecation Policy](DEPRECATION_POLICY.md), [Testing](TESTING.md) |
| Run or review the fixed public-MVP gate | [MVP Acceptance](MVP_ACCEPTANCE.md) | [Quickstart](QUICKSTART.md), [Operations](OPERATIONS.md), [Testing](TESTING.md) |
| Review optional crawler, full-mode, OpenSearch, or managed infrastructure evidence | [Optional Evidence Handoff](OPTIONAL_EVIDENCE_HANDOFF.md) | [Operations](OPERATIONS.md), [Data Sources](DATA_SOURCES.md), [Testing](TESTING.md) |
| Add or adjust a profile pack | [Profile Packs](PROFILE_PACKS.md) | [Local Discovery Generic](../examples/local-discovery-generic/README.md), [School Event JP Reference](../examples/school-event-jp/README.md), [Versioning](VERSIONING.md) |
| Add or adjust a connector or source manifest | [Data Sources](DATA_SOURCES.md) | [Data Licenses](DATA_LICENSES.md), [Operations](OPERATIONS.md), [Quickstart](QUICKSTART.md) |
| Understand the system without implementation detail first | [Non-engineer Friendly Design Docs](design_document/README_JA.md) | [Architecture](ARCHITECTURE.md), [Project README](../README.md) |

## Fixed Boundary

The fixed public-MVP path remains:

- `sql_only` candidate retrieval
- `event-csv` operational content import
- PostgreSQL/PostGIS as the reference write store
- Redis as cache only

[MVP Acceptance](MVP_ACCEPTANCE.md) defines the fixed six-case public-MVP gate.
OpenSearch, `full` mode, live crawler operation, JP demo imports, and managed
infrastructure are optional paths. They can provide useful review evidence, but
do not add them to the fixed gate without explicit review.

## Reference List

### Build And Run

- [Quickstart](QUICKSTART.md): first-time SQL-only loop, default sample, and
  demo commands.
- [Local Contributing Guide](CONTRIBUTING_LOCAL.md): validation commands,
  database modes, and contributor workflow.
- [Testing](TESTING.md): unit, integration, compatibility, release-gate, and
  local review checks.
- [Contributor Rules](../AGENTS.md): repository mission, boundaries, and
  validation expectations.

### Product And Contracts

- [API Spec](../API_SPEC.md): human-readable API boundary and response fields.
- [Architecture](ARCHITECTURE.md): service boundaries, deterministic ranking,
  and storage roles.
- [Profile Packs](PROFILE_PACKS.md): reference profile manifests, fixture
  ownership, and source mapping boundaries.
- [Reason Catalog](REASON_CATALOG.md): stable score reason codes used by
  explanations.
- [Versioning](VERSIONING.md): config, manifest, public API, reason-code, and
  artifact versioning rules.
- [Deprecation Policy](DEPRECATION_POLICY.md): removal and rename policy for
  API fields, config keys, manifests, and reason codes.

### Operations And Evidence

- [Operations](OPERATIONS.md): service operation, profiles, replay evaluation,
  worker recovery, imports, crawler notes, and Docker notes.
- [MVP Acceptance](MVP_ACCEPTANCE.md): fixed SQL-only public-MVP gate.
- [Optional Evidence Handoff](OPTIONAL_EVIDENCE_HANDOFF.md): read-only
  intake-to-inventory handoff for optional evidence review and closeout.

### Data And Compatibility

- [Data Sources](DATA_SOURCES.md): source families and crawler examples.
- [Data Licenses](DATA_LICENSES.md): fixture and source-license notes.
- [MySQL Compatibility](MYSQL_COMPATIBILITY.md): current experimental boundary.

### Planning And Design

- [v0.2.1 to v0.2.5 Design Plan](V0_2_1_TO_V0_2_5_DESIGN_PLAN.md):
  patch-sized goals, non-goals, PR breakdown, risks, and verification policy
  before v0.3.0.
- [Non-engineer Friendly Design Docs](design_document/README_JA.md): Japanese
  product and system overview for readers who do not need implementation detail
  first.
