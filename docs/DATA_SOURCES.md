# Data Sources

Phase 2 adds adapter paths for four Japanese public data families. Phase 6 adds an optional allowlist crawler path for supplemental event data.

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
