# geo-line-ranker

地理・路線主軸で候補を返す、決定論的なルールベース推薦エンジンです。  
Phase 6 では PostgreSQL/PostGIS を基準実装に据えたまま、allowlist crawler と parser registry を追加しています。

## Phase 6 の中身

- `api` / `cli` / `worker` / `crawler` を含む Rust workspace
- PostgreSQL/PostGIS ベースの SQL-only minimal mode
- OpenSearch を候補取得だけに使う full mode
- `home` / `search` / `detail` / `mypage` の placement profile
- school / event の mixed ranking
- same school cap / same group cap / content-kind ratio による多様性制御
- `article` は schema / config 上の将来枠として残しつつ、runtime ではまだ受け付けないようにしています
- `POST /v1/recommendations` で `profile_version` と mixed item を返却
- append-only な行動ログを受ける `POST /v1/track`
- DB テーブル正本の worker queue と retry
- recommendation response 向けの optional Redis cache
- `import event-csv --file ...` による運用 CSV の idempotent import
- allowlist manifest / robots / terms / user-agent / rate limit を明示した optional crawler
- raw HTML staging と differential checksum fetch
- `crawl_runs` / `crawl_fetch_logs` / `crawl_parse_reports` / `crawl_dedupe_reports` による crawl audit
- crawl manifest の `source_maturity` と `expected_shape`
- manifest ごとの parser health summary コマンドと `logical_name` 単位の赤信号
- 新規 source 追加時の `crawler scaffold-domain`。`logical_name` / `event_category` / fixture guide を shape-aware に自動補助
- 東京大学の公開イベント JSON feed を読む実ドメイン manifest / parser 例
- 芝浦工業大学附属中の入試説明会ページを読む実ドメイン manifest / parser 例
- 八王子学園八王子中の説明会日程ページを読む実ドメイン manifest / parser 例
- 日本大学中学校の説明会ページを読む実ドメイン manifest / parser 例
- 青山学院中等部の学校説明会ページを読む実ドメイン manifest / parser 例
- source manifest / checksum / parser version / report を残す import audit
- 小さな fixture データと Next.js example frontend

## Phase 6 の前提

- 重大な未解決はありません。
- `POST /v1/track` で `search_execute` も保存できます。
- `search_execute` の snapshot weight 反映は後続フェーズの持ち越しです。
- これは placement profile / mixed ranking / crawler の blocker ではありません。

## 最短起動

```bash
cp .env.example .env
docker compose -f .docker/docker-compose.yaml up -d postgres redis
cargo run -p cli -- migrate
cargo run -p cli -- seed example
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
cargo run -p crawler -- fetch --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- health --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- scaffold-domain --source-id sample-domain --source-name "Sample Domain Events" --school-id school_sample --parser-key sample_parser_v1 --expected-shape html_monthly_dl_pairs --target-url https://example.com/events
cargo run -p crawler -- fetch --manifest configs/crawler/sources/utokyo_events.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/utokyo_events.yaml
cargo run -p crawler -- fetch --manifest configs/crawler/sources/shibaura_junior_events.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/shibaura_junior_events.yaml
cargo run -p crawler -- fetch --manifest configs/crawler/sources/hachioji_junior_events.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/hachioji_junior_events.yaml
cargo run -p crawler -- fetch --manifest configs/crawler/sources/nihon_university_junior_events.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/nihon_university_junior_events.yaml
cargo run -p crawler -- fetch --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml
cargo run -p worker -- serve
cargo run -p api -- serve
```

`cargo run -p cli -- seed example` で `school_utokyo`、`school_keio`、`school_shibaura_it_junior`、`school_hachioji_gakuen_junior`、`school_nihon_university_junior`、`school_aoyama_gakuin_junior` も seed されるようにしたため、取得済み HTML / JSON があれば、コミット済みの実ドメイン parser は標準のローカル起動手順で import まで進めます。

`configs/crawler/sources/keio_events.yaml` は parser 側は利用可能ですが、live fetch は manifest policy で明示的に止めています。2026-04-19 時点で `https://www.keio.ac.jp/robots.txt` が HTTP 404 を返したため、Keio manifest は公式 robots URL が確認できるまで `blocked_policy` を記録する運用です。
`crawler -- serve` は `source_maturity=live_ready` の manifest だけを自動実行します。`parser_only` と `policy_blocked` は doctor / health で見えるまま残しつつ、poll loop のノイズを増やさない方針です。
seed を省略するか custom fixture を使う場合は、対応する `schools.id` が無いと crawl import は `events` に着地しません。

サンプルリクエスト:

```bash
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d '{"target_station_id":"st_tamachi","placement":"home","limit":3}'
```

```bash
curl -X POST http://127.0.0.1:4000/v1/track \
  -H "content-type: application/json" \
  -d '{"user_id":"demo-user-1","event_kind":"school_view","school_id":"school_seaside"}'
```

詳しい手順は [docs/QUICKSTART.md](docs/QUICKSTART.md) を参照してください。

## ドキュメント

- [非エンジニア向け設計ドキュメント](docs/design_document/README_JA.md)
- [Quickstart](docs/QUICKSTART.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Operations](docs/OPERATIONS.md)
- [Testing](docs/TESTING.md)
