# Data Sources

This repository includes adapter paths for four Japanese public data families
and an optional allowlist crawler path for supplemental event data.

Connector authors should start here, then read [Data Licenses](DATA_LICENSES.md)
before changing source manifests, adapters, crawler manifests, or upstream-data
handling. Use [Documentation Index](README.md) when choosing a broader audience
or task path, and [Optional Evidence Handoff](OPTIONAL_EVIDENCE_HANDOFF.md)
when optional crawler or full-mode evidence needs review without widening the
fixed public-MVP gate.

## Covered sources

- MEXT school codes
- National Land Numerical Information school geodata
- National Land Numerical Information rail data
- Japan Post postal code CSV
- Optional allowlist crawl sources under `configs/crawler/sources/`

## What is committed

- Small demo fixtures under `storage/fixtures/demo_jp/`
- Fixture manifests for fixture set directories such as
  `storage/fixtures/minimal/fixture_manifest.yaml` and
  `storage/fixtures/demo_jp/fixture_manifest.yaml`
- Example manifests under `storage/sources/*/example.yaml`
- Example crawl manifests under `configs/crawler/sources/*.yaml`
- Generated crawl scaffold notes under `docs/crawler_scaffolds/` when you use `crawler scaffold-domain`
- A real-domain crawl manifest for the University of Tokyo public events feed under `configs/crawler/sources/utokyo_events.yaml`
- A policy-blocked real-domain crawl manifest for the Keio University public events listing under `configs/crawler/sources/keio_events.yaml`
- A real-domain crawl manifest for the Shibaura Institute of Technology Junior High admissions page under `configs/crawler/sources/shibaura_junior_events.yaml`
- A real-domain crawl manifest for the Hachioji Gakuen Hachioji Junior High admissions schedule page under `configs/crawler/sources/hachioji_junior_events.yaml`
- A real-domain crawl manifest for the Nihon University Junior High information session page under `configs/crawler/sources/nihon_university_junior_events.yaml`
- A real-domain crawl manifest for the Aoyama Gakuin Junior High school tour page under `configs/crawler/sources/aoyama-junior-school-tour.yaml`
- No raw production dumps
- No raw production HTML

## Import model

1. Read a manifest.
2. Copy each source file into `.storage/raw/`.
3. Record checksum, parser version, and row counts.
4. Upsert normalized tables.
5. Derive `school_station_links` from normalized school and station geodata.

Crawler model:

1. Read an allowlist crawl manifest.
2. Validate allowed domains plus explicit robots / terms / user-agent / rate-limit policy.
3. Fetch raw HTML into `.storage/raw/`.
4. Record checksum, fetch status, parse reports, and dedupe reports.
5. Import deduped event rows into the same core `events` path used by operational CSV.

Profile connector registry model:

1. A profile declares local `connectors` entries.
2. `profile validate`, `profile inspect`, and `doctor profile-pack` resolve
   each entry without loading connector code dynamically.
3. `source_manifest` entries must point to `kind: import_source` YAML and are
   classified as `csv_import`.
4. `csv_import` entries point directly to CSV files, declare their profile
   source id plus a portable `field_mapping` ref, and are classified as
   `csv_import`.
5. `ndjson_import` entries point directly to NDJSON files, declare their
   profile source id plus a portable `field_mapping` ref, and are classified as
   `ndjson_import`.
6. `crawler_manifest` entries must point to `kind: crawler_source` YAML and
   are classified as `html_crawl`; registry metadata marks them as
   allowlist-required and live-fetch disabled by default at the profile
   boundary.
7. `cargo run -p cli -- import profile-source --source-id <id>` resolves the
   selected profile connector and runs the matching one-shot importer for JP
   source manifests, event CSV, or event NDJSON. The current file-import runtime
   executes only `field_mapping: event_v1`; other valid mapping refs fail
   validation, doctor, and import rather than becoming silent half-support.
   Crawler manifests still use the crawler commands.
8. `cargo run -p cli -- doctor ingest-quality` is DB-free coverage evidence for
   this registry. It reuses profile validation, lints declared source manifests
   and crawler manifests, and reports source-class, manifest-kind,
   runtime-executable mapping, source-manifest file, crawler target, and
   safety-boundary counts without importing data or making live crawl requests.

Connector and import `source_id` values must be portable path segments: lowercase
letters, digits, and hyphens, with no leading or trailing hyphen.
Profile file-import `field_mapping` refs must use lowercase letters, digits,
underscores, and hyphens, with no leading or trailing separator.

The current `event_v1` file mapping accepts event rows with `event_id`,
`school_id`, `title`, optional event metadata, pipe-delimited or array
`placement_tags`, and an optional object `details` payload. `details: null` is
normalized to an empty object; scalar or array `details` values are rejected.

## Notes

- The committed demo fixtures mimic the adapter shape and stay small on purpose.
- Fixture manifests record per-file checksums and row counts; use
  `cargo run -p cli -- fixtures doctor --path ...` before trusting a changed
  fixture set.
- Production usage should point manifests at separately managed source files.
- Provenance for each run lives in `source_manifests`, `import_runs`, `import_run_files`, and `import_reports`.
- Crawl provenance for allowlist sources lives in `source_manifests`, `crawl_runs`, `crawl_fetch_logs`, `crawl_parse_reports`, and `crawl_dedupe_reports`.
- Crawl manifests can declare `source_maturity` and `expected_shape` so operators can separate `live_ready`, `policy_blocked`, and `parser_only` sources while keeping doctor checks shape-aware.
- Crawl targets may declare `fixture_path` so `crawler doctor` and manifest lint
  can verify parser shape against committed local HTML/JSON without making live
  fetch mandatory.
- `configs/crawler/sources/utokyo_events.yaml` points at the public `events.json` feed and keeps parser output bounded to the newest 60 dated items per run.
- The U-Tokyo manifest uses `school_id: school_utokyo`; production import still requires a matching row in `schools`.
- `configs/crawler/sources/keio_events.yaml` reads the public event listing pages and extracts deterministic card rows with start date, optional end date, venue, registration flag, and detail URL.
- The Keio manifest uses `school_id: school_keio`; production import still requires a matching row in `schools`.
- As of 2026-04-19, `https://www.keio.ac.jp/robots.txt` returned HTTP 404, so the committed Keio manifest is parser-ready but explicitly records `blocked_policy` instead of attempting live fetch.
- `crawler -- serve` only auto-runs manifests with `source_maturity: live_ready`.
- `configs/crawler/sources/shibaura_junior_events.yaml` reads the public admissions event page and expands dated list items into multiple deterministic event rows when one bullet lists several dates.
- The Shibaura manifest uses `school_id: school_shibaura_it_junior`; production import still requires a matching row in `schools`.
- `configs/crawler/sources/hachioji_junior_events.yaml` reads the public admissions schedule tables and expands the listed month/day rows into deterministic event rows, including January dates that roll into the next calendar year inside the same academic year page.
- The Hachioji manifest uses `school_id: school_hachioji_gakuen_junior`; production import still requires a matching row in `schools`.
- `configs/crawler/sources/nihon_university_junior_events.yaml` reads the public information-session page and expands `h3.ttl + dl.text_box` pairs into deterministic event rows while keeping `detail_url`, `apply_url`, `official_url`, and `raw_schedule` separated in parser details.
- The Nihon manifest uses `school_id: school_nihon_university_junior`; production import still requires a matching row in `schools`.
- As of 2026-04-19, `https://www.yokohama.hs.nihon-u.ac.jp/robots.txt` resolves but redirects to HTML content rather than a plain-text robots file, so health output should stay part of the operator review loop for this source.
- `configs/crawler/sources/aoyama-junior-school-tour.yaml` reads the public school-tour page and combines `section.explan1` internal schedule rows with `section.explan3` external fair rows while keeping `raw_date`, `time_text`, `venue`, `organizer`, and `detail_url` in parser details.
- The Aoyama manifest uses `school_id: school_aoyama_gakuin_junior`; production import still requires a matching row in `schools`.
- As of 2026-04-19, `https://www.jh.aoyama.ed.jp/robots.txt` returned `text/plain` and does not explicitly disallow `/admission/explanation.html`.
- The committed minimal fixture now seeds all six school IDs so local `seed example` flows can exercise real-domain crawl import without manual school inserts first.
