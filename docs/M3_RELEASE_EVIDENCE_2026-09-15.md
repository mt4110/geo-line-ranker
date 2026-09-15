# M3 Release Evidence: 2026-09-15

This note captures the M3 release-candidate evidence after the M1/M2 portfolio
demo pack was merged. It is CI-backed evidence plus local lightweight checks;
it does not claim that every local command in [Testing](TESTING.md) was run on
this workstation.

## Scope

- Branch checked: `main`
- Commit checked: `573c8cbb057f294c342c555005ed9f63f009cec0`
- Merge source: [PR #94](https://github.com/mt4110/geo-line-ranker/pull/94)
- Fixed public-MVP boundary: `sql_only` candidate retrieval, `event-csv`
  operational content, PostgreSQL/PostGIS as reference write store, Redis as
  cache only
- Outside this gate: live crawler, `full` mode, OpenSearch, managed
  infrastructure, tag/release publication

## Local Evidence

Run from the repository root on `main` at `573c8cb`:

| Command | Result |
|---|---|
| `just release-readiness` | Passed. Printed the public-MVP release candidate command plan for branch `main`, commit `573c8cb`; this command is read-only and does not execute validation. |
| `cargo fmt --all --check` | Passed. |
| `./scripts/docs_check.sh` | Passed. Checked 41 markdown files and local links. |
| `./scripts/spellcheck.sh` | Passed. Checked 281 files with 0 issues. |
| `git diff --check` | Passed. |

The full local release validation set in [Testing](TESTING.md) was not completed
on this workstation. Commands that require compiling/linking Rust binaries were
blocked locally by the macOS Xcode license state during this work. Treat
the GitHub Actions jobs below as the executable validation evidence for this
candidate, not as proof that local validation was fully executed.

`just release-readiness` confirmed the release notes baseline:
public MVP runs on SQL-only candidate retrieval with `event-csv`, PostgreSQL /
PostGIS, and Redis cache only; correctness evidence comes from golden replay
scenarios, fixed MVP acceptance, and data-quality evidence. Rail/station
freshness wording remains "latest available MLIT N02 snapshot".

## CI Evidence

Main branch GitHub Actions for merge commit `573c8cb` all passed:

| Workflow / job | Evidence |
|---|---|
| `ci` workflow | [run 34929146462](https://github.com/mt4110/geo-line-ranker/actions/runs/34929146462), success |
| `mvp-acceptance` | [job 104253499138](https://github.com/mt4110/geo-line-ranker/actions/runs/34929146462/job/104253499138), success |
| `data-quality-doctor` | [job 104253499135](https://github.com/mt4110/geo-line-ranker/actions/runs/34929146462/job/104253499135), success |
| `rust-quality` | [job 104253499031](https://github.com/mt4110/geo-line-ranker/actions/runs/34929146462/job/104253499031), success |
| `openapi-drift` | [job 104253499151](https://github.com/mt4110/geo-line-ranker/actions/runs/34929146462/job/104253499151), success |
| `node-and-frontend` | [job 104253499298](https://github.com/mt4110/geo-line-ranker/actions/runs/34929146462/job/104253499298), success |
| `rust-unit-tests` | `slice:1/2` and `slice:2/2`, success |
| `rust-postgres-tests` | `api`, `cli`, `crawler`, and `worker-storage`, success |
| `rust-fast-doctests` | success |
| `rust-heavy-tests` | success |
| `docs` workflow | [run 34929146340](https://github.com/mt4110/geo-line-ranker/actions/runs/34929146340), success |
| `spellcheck` workflow | [run 34929146390](https://github.com/mt4110/geo-line-ranker/actions/runs/34929146390), success |

The `mvp-acceptance` job executed the fixed public MVP acceptance flow. The
`data-quality-doctor` job bootstrapped the data-quality baseline and ran the
read-only doctor in CI strict evidence mode.

## Release Validation Status

| Requirement area from `docs/TESTING.md` | Status |
|---|---|
| Format, Clippy, config/source/fixture/scenario lint, and local-review self-test | Covered by CI `rust-quality` on merge commit `573c8cb`. |
| Unit, heavy, doctest, and PostgreSQL-backed Rust tests | Covered by the CI Rust test jobs listed above. |
| OpenAPI drift, TypeScript SDK, and frontend smoke | Covered by CI `openapi-drift` and `node-and-frontend`. |
| Fixed public-MVP acceptance | Covered by CI `mvp-acceptance`. |
| Strict data-quality evidence | Covered by CI `data-quality-doctor`. |
| Full local release validation command set | Incomplete on this workstation; see Local Evidence. |

## Decision

M3 CI-backed evidence is captured for the current public-MVP boundary, with the
local full validation set explicitly marked incomplete. This is not a v0.3.0
tag or public release by itself; tagging, release notes publication, GitHub
description/topics, and portfolio polish remain M4 work.

## Local Notes

A locally generated, untracked `flake.lock` exists in this checkout from an
earlier `nix develop` attempt. It is not part of this evidence and is not
included in the repository change.
