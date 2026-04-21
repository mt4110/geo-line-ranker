# 10. Prompt Template: 非エンジニア向け設計ドキュメントを追加する

以下は、このドキュメント群を別の assistant や automation に追加依頼するときの雛形です。

---

## Prompt

You are working in the `mt4110/geo-line-ranker` repository.

Goal: add a non-engineer-friendly Japanese design documentation section under `docs/design_document/`.

Do not change Rust code, database schema, API behavior, Docker settings, or existing configuration.  
This task is documentation-only.

### Repository context

`geo-line-ranker` is a deterministic, non-ML, geo-first and line-first recommendation engine.

Current repository already contains:

- `README.md`
- `README_EN.md`
- `docs/ARCHITECTURE.md`
- `docs/QUICKSTART.md`
- `docs/OPERATIONS.md`
- `docs/DATA_SOURCES.md`
- `docs/DATA_LICENSES.md`
- `docs/TESTING.md`
- Rust apps: `api`, `worker`, `crawler`, `cli`
- PostgreSQL/PostGIS as source of truth
- Redis as optional cache only
- OpenSearch as optional candidate retrieval in full mode
- allowlist crawler as optional side path
- Swagger UI and Next.js example frontend

### Required docs to add

Create the following files:

```text
docs/design_document/
├── README_JA.md
├── 00_EXECUTIVE_SUMMARY_JA.md
├── 01_REQUIREMENTS_JA.md
├── 02_SYSTEM_OVERVIEW_JA.md
├── 03_RECOMMENDATION_LOGIC_JA.md
├── 04_DATA_FLOW_PRIVACY_JA.md
├── 05_CRAWLING_AND_IMPORT_POLICY_JA.md
├── 06_OPERATIONS_GUIDE_JA.md
├── 07_NON_ENGINEER_QUICKSTART_JA.md
├── 08_GLOSSARY_JA.md
├── 09_FAQ_JA.md
└── 10_DOCS_PROMPT_TEMPLATE.md
```

### Documentation style

- Japanese main text.
- Simple language for PMs, planners, operators, data owners, legal/security reviewers, and non-engineers.
- Avoid overloading readers with Rust crate internals.
- Explain technical terms when they first appear.
- Keep the central principles clear:
  - no AI / no ML recommendation
  - deterministic rule-based scoring
  - geo and rail/line conditions are mandatory anchors
  - PostgreSQL/PostGIS is source of truth
  - Redis is cache only
  - OpenSearch is optional candidate retrieval only
  - crawler is optional and allowlist-based
  - recommendations must be explainable
  - fallback must avoid extremely distant candidates

### README link update

Update `docs/README` only if such a file exists.  
Otherwise, update the Docs section in root `README.md` and `README_EN.md` to include:

- `docs/design_document/README_JA.md` as "Non-engineer friendly design docs / 非エンジニア向け設計ドキュメント"

Do not remove existing links.

### Validation

Run or at least ensure the following:

```bash
ls docs/design_document
```

If markdown lint is configured, run it.  
If no markdown lint is configured, check that all relative links are valid by inspection.

### Non-goals

Do not:

- modify application behavior
- add new crates
- add new dependencies
- rewrite existing technical docs
- change crawler policy
- change license text
- change CI
- rename files outside this docs addition

### Expected result

A reader who is not an engineer should be able to understand:

1. What `geo-line-ranker` is.
2. Why it avoids AI/ML.
3. How recommendation results are decided.
4. What data is used.
5. How privacy and data source safety are handled.
6. How crawler/import differ.
7. How to run a minimal local demo.
8. What operational checks matter.

Commit message suggestion:

```text
docs: add non-engineer friendly design documents
```

---

## Review checklist

- [ ] New files exist under `docs/design_document/`
- [ ] Root README docs section links to the new docs
- [ ] Japanese text is readable for non-engineers
- [ ] No application code changed
- [ ] No DB schema changed
- [ ] No crawler behavior changed
- [ ] No dependency changed
- [ ] Terms like Redis, OpenSearch, fallback, placement, crawler are explained
- [ ] Non-AI / rule-based / explainable principles are stated repeatedly enough to avoid ambiguity
