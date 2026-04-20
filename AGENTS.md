# AGENTS.md

## Repository mission
This repository is an OSS for deterministic geo-first / line-first recommendations.
No AI/ML/embeddings/vector search.

## Core rules
- Keep PostgreSQL/PostGIS as the reference implementation.
- Keep MySQL adapter optional and experimental.
- Keep Redis as cache only.
- Keep OpenSearch only for candidate retrieval in later phases.
- Keep SQL-only minimal mode working.
- Never move final ranking logic to the frontend.
- Never make crawling mandatory for the system to work.

## Engineering rules
- Prefer small, reviewable changes.
- Add docs when behavior changes.
- Add tests for ranking behavior changes.
- Preserve deterministic outputs for the same input, config, and data.

## Validation rules
- Run formatting, lint, and tests before reporting completion.
- If a command cannot run, explain why and still prepare the command list.
- Keep example fixtures and quickstart working.

