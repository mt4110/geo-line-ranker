# Documentation Index

Start here when choosing what to read next.

## Build And Run

- [Quickstart](QUICKSTART.md): local SQL-only loop and demo commands.
- [Local Contributing Guide](CONTRIBUTING_LOCAL.md): validation commands, database modes, and contributor workflow.
- [Testing](TESTING.md): unit, integration, compatibility, and release-gate checks.

## Product And API

- [API Spec](../API_SPEC.md): human-readable API boundary and response fields.
- [Architecture](ARCHITECTURE.md): service boundaries, deterministic ranking, and storage roles.
- [v0.2.0 Design Notes](v0.2.0/README.md): context-first recommendation foundation.
- [Reason Catalog](REASON_CATALOG.md): stable score reason codes used by explanations.

## Operations

- [Operations](OPERATIONS.md): service operation, profiles, replay evaluation, and Docker notes.
- [Post-launch Runbook](POST_LAUNCH_RUNBOOK.md): first checks after release.
- [Operator Feedback Loop](OPERATOR_FEEDBACK_LOOP.md): how to classify observed issues.
- [Public MVP Release Readiness](PUBLIC_MVP_RELEASE_READINESS.md): release-candidate evidence.
- [MVP Acceptance](MVP_ACCEPTANCE.md): fixed SQL-only public-MVP gate.

## Data And Compatibility

- [Data Sources](DATA_SOURCES.md): source families and crawler examples.
- [Data Licenses](DATA_LICENSES.md): fixture and source-license notes.
- [MySQL Compatibility](MYSQL_COMPATIBILITY.md): current experimental boundary.

## Optional Evidence

The `OPTIONAL_EVIDENCE_*` documents are review packets for crawler graduation,
full-mode comparison, managed infrastructure, and post-MVP hardening evidence.
They do not change the fixed public-MVP gate by themselves.
