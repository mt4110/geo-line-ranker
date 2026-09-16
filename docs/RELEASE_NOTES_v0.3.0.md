# v0.3.0 Release Notes Draft

This is the draft publication text for the portfolio-ready release. Do not tag
or publish from this file alone; M5 still needs explicit approval for the tag,
GitHub Release, and repository metadata update.

## Summary

`geo-line-ranker` is a deterministic geo-first / line-first recommendation
engine for school events and local discovery. It ranks schools and events from
station, line, or coarse area context, returns explainable reason codes, and
keeps final ranking in Rust without AI, ML, embeddings, or vector search.

## What This Release Shows

- A fixed first-run path for PostgreSQL/PostGIS, Redis, SQL-only candidate
  retrieval, and operational `event-csv` import.
- A portfolio demo around `st_tamachi`, `JR Yamanote Line`, and `Minato` that
  shows station-first, line-first, and area-first recommendation behavior.
- Stable public response evidence: non-empty `items`, `fallback_stage`,
  `candidate_counts`, `candidate_plan_trace`, profile version, algorithm
  version, and score reason codes.
- Reference profile packs for `local-discovery-generic` and `school-event-jp`.
- Release-candidate evidence for the public-MVP gate, including CI-backed
  `mvp-acceptance` and strict `data-quality-doctor` checks.
- Container build/runtime polish for the committed API, worker, and crawler
  Dockerfiles.

## Demo Path

Start here:

1. [First 15 Minutes](FIRST_15_MINUTES.md)
2. [Portfolio Demo](PORTFOLIO_DEMO.md)
3. [M3 Release Evidence 2026-09-15](M3_RELEASE_EVIDENCE_2026-09-15.md)

The first visible success state is:

- Swagger UI opens at `http://127.0.0.1:4000/swagger-ui`.
- `POST /v1/recommendations` returns HTTP 200.
- The response contains non-empty `items`, a visible fallback stage, candidate
  counts, and reason codes such as `geo.direct_station`, `geo.line_match`, or
  event reasons.

## Boundaries

- PostgreSQL/PostGIS remains the reference write store.
- Redis is cache only.
- OpenSearch is optional full-mode candidate retrieval only.
- Crawling is optional and allowlist-only.
- The fixed public-MVP path does not require managed infrastructure.
- SQLite is not a primary write store.
- Final ranking does not move to the frontend.

## Suggested GitHub Metadata

Description:

```text
Deterministic geo-first and line-first recommendation engine for explainable school-event discovery.
```

Topics:

```text
rust, postgresql, postgis, redis, geo, ranking, recommendations, openapi, japan, portfolio
```

## Publication Checklist

- Confirm the M4 PR is merged into `main`.
- Confirm `main` CI is green after merge.
- Confirm the release evidence still references the intended `main` commit.
- Create tag `v0.3.0`.
- Publish GitHub Release notes from this draft after final review.
- Update repository description and topics.
