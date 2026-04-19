# Data Licenses

This repository ships only small demo fixtures for the Phase 2 adapter flow.

## Repository fixtures

- Files under `storage/fixtures/demo_jp/` are synthetic, commit-safe demo inputs.
- They are intended for tests, quickstart, and import smoke checks.

## Upstream source handling

- MEXT school codes
- National Land Numerical Information school and rail datasets
- Japan Post postal code CSV

Before using upstream data in any non-demo environment, confirm the publisher's current terms, attribution requirements, redistribution limits, and update cadence.

## Raw data policy

- Do not commit upstream raw dumps into the repository.
- Stage raw files under `.storage/raw/` during import runs.
- Keep manifests in git, but keep large source payloads outside git.
