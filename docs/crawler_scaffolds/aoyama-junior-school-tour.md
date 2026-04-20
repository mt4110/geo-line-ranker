# Crawler Scaffold: Aoyama Gakuin Junior High admissions school tours

## Snapshot

- source_id: `aoyama-junior-school-tour`
- source_maturity: `live_ready`
- parser_key: `aoyama_junior_school_tour_v1`
- expected_shape: `html_school_tour_blocks`
- school_id: `school_aoyama_gakuin_junior`
- logical_name: `school_tour_page`
- target_url: `https://www.jh.aoyama.ed.jp/admission/explanation.html`
- manifest: `configs/crawler/sources/aoyama-junior-school-tour.yaml`
- fixture: `storage/fixtures/crawler/aoyama_junior_school_tour.html`

## Verification

- 2026-04-19: `https://www.jh.aoyama.ed.jp/robots.txt` returned HTTP 200 with `text/plain`.
- The school-tour target path is not explicitly disallowed by the published robots rules at that date.
- Privacy policy is published at `https://www.aoyamagakuin.jp/practice/compliance/privacypolicy/`.
- Parser fixture and integration coverage live in `crates/crawler-core/src/lib.rs` and `apps/crawler/src/lib.rs`.

## Doctor Checklist

- `robots.txt` resolves and is plain text, not HTML.
- `terms_url` resolves without auth or soft blocks.
- `expected_shape` matches the live target or the committed fixture.
- `school_aoyama_gakuin_junior` exists in `schools` for the environment you test against.
- `source_maturity` and `live_fetch_enabled` still say the same thing operationally.

## Suggested Commands

```bash
cargo run -p crawler -- doctor --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml
cargo run -p crawler -- fetch --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml
cargo run -p crawler -- dry-run --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml
cargo run -p crawler -- health --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml
```
