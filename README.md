# geo-line-ranker

地域探索向けの、地理優先・路線優先の決定論的推薦エンジンです。
PostgreSQL/PostGIS を基準ストアにし、最終ランキングは Rust 内に閉じます。Redis は任意の cache、OpenSearch は full mode の候補取得だけに使い、allowlist crawler は任意の補助経路として扱います。AI / ML / embeddings / vector search は使いません。

## Phase 8 時点の中身

- `api` / `cli` / `worker` / `crawler` を含む Rust workspace
- PostgreSQL/PostGIS ベースの SQL-only minimal mode
- OpenSearch を候補取得に使う full mode と、実行時の retrieval mode 切り替え
- placement を考慮した school / event mixed ranking を返す `POST /v1/recommendations`
- append-only な行動ログを受ける `POST /v1/track`
- `home` / `search` / `detail` / `mypage` の placement profile
- same school / same group / content-kind ratio の diversity hard cap
- `article` は schema / config 上の将来枠として残しつつ、実装されるまでは runtime で明示的に拒否
- retry 可能な snapshot refresh / cache invalidation job を扱う DB-backed worker queue
- `jobs list` / `jobs inspect` / `jobs retry` / `jobs due` / `jobs enqueue` による worker queue recovery CLI
- CLI と worker job による PostgreSQL から OpenSearch への projection sync
- recommendation response 向けの任意 Redis cache
- checksum staging と audit trail を伴う運用 `event-csv` import
- parser registry、raw HTML staging、差分 checksum fetch、fetch / parse / dedupe audit report を持つ任意 allowlist crawler
- crawl manifest の source maturity label と parser expected-shape metadata
- 直近 crawl run、fetch outcome、parse level、最新 parser error、manifest ごとの `logical_name` red flag を見る parser health summary
- 新規 crawl source 追加時の manifest / fixture / guide を補助する `crawler scaffold-domain`
- 東京大学の公開 events JSON feed を読む実ドメイン crawl example
- 芝浦工業大学附属中学校の入試説明会ページを読む実ドメイン crawl example
- 八王子学園八王子中学校の説明会日程ページを読む実ドメイン crawl example
- 日本大学中学校の説明会ページを読む実ドメイン crawl example
- 青山学院中等部の学校説明会ページを読む実ドメイン crawl example
- 小さな路線-aware mixed ranking dataset の local fixture seeding
- rail / postal / school codes / school geodata 向けの日本ソース adapter
- Swagger UI と小さな Next.js example frontend

## 現在の挙動メモ

- `search_execute` は `POST /v1/track` から保存され、駅に紐づく学校経由で popularity / area snapshot weight を更新します。weight は config で調整します。
- `cargo run -p cli -- snapshot refresh` は現在の tracking config を再適用し、recommendation cache を invalidation し、full mode が有効なら projection も同期します。
- 公開 MVP acceptance は SQL-only かつ決定論的です。live crawling と full-mode retrieval は任意の補助経路として扱います。

## Quickstart

正式なローカル手順は [docs/QUICKSTART.md](docs/QUICKSTART.md) にあります。

最小の SQL-only loop:

```bash
cp .env.example .env
docker compose -f .docker/docker-compose.yaml up -d postgres redis
cargo run -p cli -- migrate
cargo run -p cli -- seed example
cargo run -p worker -- serve
cargo run -p api -- serve
```

次に試すと便利なコマンド:

```bash
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
cargo run -p crawler -- doctor --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- fetch --manifest configs/crawler/sources/custom_example.yaml
cargo run -p crawler -- parse --manifest configs/crawler/sources/custom_example.yaml
cargo run -p cli -- jobs list --limit 20
```

demo fixture には、コミット済みの実ドメイン crawl school も含まれています。`crawler -- serve` は `source_maturity = live_ready` の manifest だけを自動実行します。full mode、projection sync、実ドメイン crawler manifest、worker job recovery については [docs/QUICKSTART.md](docs/QUICKSTART.md) と [docs/OPERATIONS.md](docs/OPERATIONS.md) を参照してください。

recommendation request の例:

```bash
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d '{"target_station_id":"st_tamachi","placement":"home","limit":3}'
```

tracking event の例:

```bash
curl -X POST http://127.0.0.1:4000/v1/track \
  -H "content-type: application/json" \
  -d '{"user_id":"demo-user-1","event_kind":"school_view","school_id":"school_seaside"}'
```

## Response 例

```json
{
  "items": [
    {
      "content_kind": "event",
      "content_id": "event_seaside_open",
      "school_id": "school_seaside",
      "school_name": "Seaside High",
      "event_id": "event_seaside_open",
      "event_title": "Seaside Open Campus",
      "primary_station_id": "st_tamachi",
      "primary_station_name": "Tamachi",
      "line_name": "JR Yamanote Line",
      "score": 6.41,
      "explanation": "直結条件 と 注目イベント が効き、直結条件のイベント候補として上位になりました。",
      "score_breakdown": [
        {
          "feature": "direct_station_bonus",
          "value": 3.0,
          "reason": "Tamachi に直結しています。"
        }
      ]
    }
  ],
  "explanation": "ホームでは Tamachi 直結の候補群 を母集団にし、直結条件 と 注目イベント を効かせて決定論的に順位付けしました。 多様性上限で同一学校1件を抑制し、3件の表示枠に整えています。",
  "score_breakdown": [],
  "fallback_stage": "strict",
  "profile_version": "phase5-profile-version",
  "algorithm_version": "phase8-policy-diversity-v1"
}
```

## ドキュメント

- [English README](README_EN.md)
- [非エンジニア向け設計ドキュメント](docs/design_document/README_JA.md)
- [Contributor Rules](AGENTS.md)
- [Local Contributing Guide](docs/CONTRIBUTING_LOCAL.md)
- [Quickstart](docs/QUICKSTART.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Operations](docs/OPERATIONS.md)
- [Testing](docs/TESTING.md)
- [Data Sources](docs/DATA_SOURCES.md)
- [Data Licenses](docs/DATA_LICENSES.md)
