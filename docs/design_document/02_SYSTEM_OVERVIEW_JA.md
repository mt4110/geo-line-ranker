# 02. システム概要: 何がどんな役割を持つか

## 全体像

`geo-line-ranker` は、複数の部品で構成されています。

```text
利用者・フロントエンド
        ↓
API
        ↓
ランキング処理
        ↓
データベース / キャッシュ / 検索エンジン
        ↑
CLI・Worker・Crawler によるデータ投入と更新
```

各部品は役割を分けています。  
この分離が重要です。全部をひとつに混ぜると、あとで変更できない黒い箱になります。

## 部品ごとの役割

| 部品 | 役割 | 非エンジニア向け説明 |
|---|---|---|
| `apps/api` | 推薦APIを返す | フロントから呼ばれる窓口 |
| `apps/worker` | 非同期ジョブを処理 | 裏側で集計や更新を行う係 |
| `apps/crawler` | 許可されたWeb情報を取得・解析 | イベントページなどを安全に取り込む係 |
| `apps/cli` | 手動操作・初期化・インポート | 管理者がコマンドで使う道具 |
| `crates/ranking` | スコア計算と並び替え | おすすめ順を決める心臓部 |
| `crates/storage-postgres` | PostgreSQLとの接続 | 正本データを読み書きする係 |
| `crates/cache` | Redisキャッシュ | 速く返すための一時保管 |
| `crates/crawler-core` | crawl設定・parser・重複排除 | 取り込み処理の中核 |
| `examples/frontend-next` | デモ画面 | 実際の見え方を確認するサンプル |

## データの正本

正しいデータの正本は **PostgreSQL/PostGIS** です。

PostgreSQL には、次のようなデータが入ります。

- 学校
- イベント
- 駅
- 路線
- 学校と駅の関係
- ユーザー行動ログ
- 集計済みスナップショット
- import / crawl の監査ログ

Redis や OpenSearch は補助です。

| 仕組み | 役割 | 正本か |
|---|---|---|
| PostgreSQL/PostGIS | データの保存・検索・監査 | はい |
| Redis | API応答を速くするキャッシュ | いいえ |
| OpenSearch | full mode の候補抽出 | いいえ |
| `.storage/raw` | 一時的な取込元ファイル | いいえ |

## なぜ Redis を正本にしないか

Redis は高速ですが、キャッシュ向きです。  
データの正しさや履歴管理を Redis に任せると、障害時に「何が正しかったのか」が分からなくなります。

このプロジェクトでは、Redis が消えても正しい推薦が返せる状態を守ります。

## なぜ OpenSearch に全部任せないか

OpenSearch は検索や候補抽出に強いです。  
しかし、最終的な推薦理由・多様性制御・ルールバージョン管理をすべて OpenSearch に入れると、検証が難しくなります。

そのため、

- 候補を探すところ: PostgreSQL または OpenSearch
- 最終的に並べるところ: Rust の `crates/ranking`

という分担にしています。

## minimal mode と full mode

### minimal mode

ローカルで簡単に動かすモードです。

```text
PostgreSQL/PostGIS + Redis(optional) + Rust API
```

OpenSearch は使いません。  
OSSとして試しやすく、まずはこのモードを基準にします。
公開MVPの確認では、この SQL-only path を `event-csv`、PostgreSQL/PostGIS、Redis で起動します。Redis はあくまで cache-only で、正本にはしません。

### full mode

大きなデータや検索補助が必要なときのモードです。

```text
PostgreSQL/PostGIS + Redis + OpenSearch + Rust API
```

OpenSearch は候補抽出を助けます。  
それでも最終スコアリングは Rust 側で行います。

## APIの主な役割

### `POST /v1/recommendations`

おすすめ候補を返します。

例:

```json
{
  "target_station_id": "st_tamachi",
  "placement": "home",
  "limit": 3
}
```

返すもの:

- 推薦アイテム
- 推薦理由
- スコア内訳
- フォールバック段階
- algorithm version
- profile version

### `POST /v1/track`

ユーザー行動を記録します。

例:

```json
{
  "user_id": "demo-user-1",
  "event_kind": "school_view",
  "school_id": "school_seaside"
}
```

この行動は後でランキングに使えるように、DBに保存し、必要に応じて worker が集計します。

## Worker の役割

Worker は、ユーザーがAPIを待っている間にやらなくていい処理を担当します。

例:

- 行動ログからユーザースナップショットを更新する
- キャッシュを消す
- OpenSearch への同期を行う
- import や crawl の後処理を行う

これにより、API は速く返せます。

## Crawler の役割

Crawler は、許可されたWebページだけを対象にして、イベント情報などを取り込みます。

重要なのは、crawler は必須ではないということです。  
CSV import や fixture だけでも動きます。

## CLI の役割

CLI は、開発者や運用者が手動で使う入口です。

例:

- DB migration
- seed data投入
- CSV import
- JP data import
- OpenSearch index rebuild
- crawl fetch / parse
- parser health確認

## まとめ

このシステムは、次の考え方で設計されています。

- 正しいデータは PostgreSQL に置く
- 速さは Redis で補助する
- 大量候補の検索は OpenSearch で補助する
- 最終判断は Rust のランキングロジックで行う
- クローリングは任意で、安全な allowlist 方式にする
- すべてローカルで試せるようにする
