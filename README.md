# geo-line-ranker

地域探索向けの、地理優先・路線優先の決定論的推薦エンジンです。
PostgreSQL/PostGIS を基準ストアにし、最終ランキングは Rust 内に閉じます。Redis は任意の cache、OpenSearch は full mode の候補取得だけに使い、allowlist crawler は任意の補助経路として扱います。AI / ML / embeddings / vector search は使いません。

## 現在の中身

- `api` / `cli` / `worker` / `crawler` を含む Rust workspace
- PostgreSQL/PostGIS ベースの SQL-only minimal mode
- OpenSearch を候補取得に使う full mode と、実行時の retrieval mode 切り替え
- station / line / coarse area context と placement を考慮した school / event mixed ranking を返す `POST /v1/recommendations`
- append-only な行動ログを受ける `POST /v1/track`
- `home` / `search` / `detail` / `mypage` の placement profile
- same school / same group / content-kind ratio の diversity hard cap
- `article` は schema / config 上の将来枠として残しつつ、実装されるまでは runtime で明示的に拒否
- retry 可能な snapshot refresh / cache invalidation job を扱う DB-backed worker queue
- `jobs list` / `jobs inspect` / `jobs retry` / `jobs due` / `jobs enqueue` による worker queue recovery CLI
- CLI と worker job による PostgreSQL から OpenSearch への projection sync
- recommendation response 向けの任意 Redis cache
- stable reason code を含む score breakdown と Reason Catalog
- `local-discovery-generic` と `school-event-jp` の reference profile manifest
- 直近 recommendation trace を現在の SQL-only 経路で検証する replay evaluation CLI
- checksum staging と audit trail を伴う運用 `event-csv` import
- parser registry、raw HTML staging、差分 checksum fetch、fetch / parse / dedupe audit report を持つ任意 allowlist crawler
- crawl manifest の source maturity label と parser expected-shape metadata
- 直近 crawl run、fetch outcome、parse level、最新 parser error、manifest ごとの `logical_name` red flag を見る parser health summary
- 公開後の状態確認に使う read-only post-launch doctor と data quality doctor
- 公開 MVP release candidate の判定に使う release readiness command plan
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
- release candidate の判断は `just release-readiness` で手順を確認し、`just mvp-acceptance` を固定ゲート、`DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor` を必須証跡として扱います。

## Quickstart

最初の 15 分の読み方は [docs/FIRST_15_MINUTES.md](docs/FIRST_15_MINUTES.md)、
正式なローカル手順は [docs/QUICKSTART.md](docs/QUICKSTART.md) にあります。初回は SQL-only + `event-csv` + PostgreSQL/PostGIS + Redis の public-MVP path に絞ると、いちばん迷いません。OpenSearch、full mode、live crawler、managed infrastructure は任意の運用・検証経路で、固定 gate ではありません。

最短 local path:

```bash
cp .env.example .env
docker compose -f .docker/docker-compose.yaml up -d postgres redis
cargo run -p cli -- migrate
cargo run -p cli -- seed example
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv

# terminal A
cargo run -p worker -- serve

# terminal B
cargo run -p api -- serve
```

最初に見る成功状態:

- [Swagger UI](http://127.0.0.1:4000/swagger-ui) が開く
- `POST /v1/recommendations` が `items`、`score`、`explanation`、`fallback_stage`、`candidate_counts` を返す
- `POST /v1/track` が行動ログを受け取り、worker job を確認できる
- `event-csv` import の audit trail が PostgreSQL に残る

default sample は `storage/fixtures/minimal/` にあります。6駅、10学校、5 fixture events、7 school-station links、2 user events を含み、`st_tamachi` / `JR Yamanote Line` / `Minato` で station-first、line-first、area-first の推薦を小さく確認できます。`examples/import/events.sample.csv` は public-MVP の運用入力で、4件の event rows と import audit を確認するためのサンプルです。

recommendation request の例:

```bash
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d '{"context":{"area":{"city_name":"Minato"},"line_name":"JR Yamanote Line"},"placement":"home","limit":3}'
```

tracking event の例:

```bash
curl -X POST http://127.0.0.1:4000/v1/track \
  -H "content-type: application/json" \
  -d '{"user_id":"demo-user-1","event_kind":"school_view","school_id":"school_seaside"}'
```

次に読む docs:

- [First 15 Minutes](docs/FIRST_15_MINUTES.md): 初回 contributor / operator 向けの読む順、起動順、sample inspection、touch map
- [Quickstart](docs/QUICKSTART.md): 初回運用者向けの順序つき local runbook
- [MVP Acceptance](docs/MVP_ACCEPTANCE.md): 固定 public-MVP gate
- [Operations](docs/OPERATIONS.md): worker jobs、replay、doctor、運用確認
- [Optional Evidence Handoff](docs/OPTIONAL_EVIDENCE_HANDOFF.md): crawler/full/OpenSearch/managed infrastructure などの任意証跡の handoff
- [Documentation Index](docs/README.md): audience / task 別の読み先

## Response 例

フィールド形の例です。`score` や event title は fixture / import / config の状態で変わります。

```json
{
  "items": [
    {
      "content_kind": "event",
      "content_id": "event_seaside_open",
      "school_id": "school_seaside",
      "school_name": "Seaside High",
      "event_id": "event_seaside_open",
      "event_title": "Seaside Open Campus Spring",
      "primary_station_id": "st_tamachi",
      "primary_station_name": "Tamachi",
      "line_name": "JR Yamanote Line",
      "score": 6.41,
      "explanation": "沿線一致 と 注目イベント が効き、同一路線のイベント候補として上位になりました。",
      "score_breakdown": [
        {
          "feature": "line_match_bonus",
          "reason_code": "geo.line_match",
          "value": 1.25,
          "reason": "JR Yamanote Line 沿線の候補です。"
        }
      ],
      "fallback_stage": "same_line"
    }
  ],
  "explanation": "ホームでは JR Yamanote Line 沿線の候補群 を母集団にし、沿線一致 と 注目イベント を効かせて決定論的に順位付けしました。 多様性上限で同一学校1件を抑制し、3件の表示枠に整えています。",
  "score_breakdown": [],
  "fallback_stage": "same_line",
  "candidate_counts": {
    "strict_station": 0,
    "same_line": 5,
    "same_city": 2
  },
  "context": {
    "context_source": "request_line",
    "confidence": 0.95,
    "privacy_level": "coarse_area",
    "warnings": []
  },
  "profile_version": "phase5-profile-version",
  "algorithm_version": "phase8-policy-diversity-v1"
}
```

## ドキュメント

- [English README](README_EN.md)
- [Documentation Index](docs/README.md)
- [Profile Packs](docs/PROFILE_PACKS.md)
- [Local Discovery Generic](examples/local-discovery-generic/README.md)
- [School Event JP Reference](examples/school-event-jp/README.md)
- [Reason Catalog](docs/REASON_CATALOG.md)
- [v0.2.1 to v0.2.5 Design Plan](docs/V0_2_1_TO_V0_2_5_DESIGN_PLAN.md)
- [非エンジニア向け設計ドキュメント](docs/design_document/README_JA.md)
- [Contributor Rules](AGENTS.md)
- [First 15 Minutes](docs/FIRST_15_MINUTES.md)
- [Local Contributing Guide](docs/CONTRIBUTING_LOCAL.md)
- [Quickstart](docs/QUICKSTART.md)
- [MVP Acceptance](docs/MVP_ACCEPTANCE.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Operations](docs/OPERATIONS.md)
- [Testing](docs/TESTING.md)
- [Data Sources](docs/DATA_SOURCES.md)
- [Data Licenses](docs/DATA_LICENSES.md)
