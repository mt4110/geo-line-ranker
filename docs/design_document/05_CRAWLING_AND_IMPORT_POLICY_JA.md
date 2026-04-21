# 05. クローリングとインポート方針

## 基本方針

`geo-line-ranker` では、データ投入方法を複数用意します。

優先順位は次の通りです。

1. 公式CSV・オープンデータ
2. 管理者が投入するCSV
3. 提携先API
4. 許可されたWebページの allowlist crawl
5. 合成データ・fixture

クローリングは便利ですが、最初から主役にしません。  
主役は、再現可能で権利関係が明確なデータ取り込みです。

## なぜ無制限クローリングをしないか

無制限クローリングにはリスクがあります。

- 利用規約違反
- robots.txt との不整合
- 過剰アクセス
- HTML変更による壊れやすさ
- データ出所の説明困難
- OSSとしての信頼低下

そのため、このプロジェクトの crawler は **allowlist方式**にします。

## allowlist方式とは

事前に設定したソースだけを取りに行く方式です。

manifest に以下を記録します。

- source id
- source name
- target URL
- parser key
- school id
- source maturity
- expected shape
- crawl interval
- robots / policy status

例:

```yaml
source_id: sample-domain
source_name: Sample Domain Events
school_id: school_sample
target_url: https://example.com/events
parser_key: sample_parser_v1
expected_shape: html_monthly_dl_pairs
source_maturity: parser_only
```

## source maturity

ソースの成熟度をラベルで管理します。

| ラベル | 意味 |
|---|---|
| `live_ready` | 自動取得してよい |
| `parser_only` | parser検証用。自動取得しない |
| `policy_blocked` | policy上 live fetch しない |
| `experimental` | 検証中。運用には使わない |

このラベルにより、運用で誤って危険なソースを自動巡回しないようにします。

## fetch と parse を分ける理由

Crawler は次の2段階に分けます。

### fetch

WebページやAPIレスポンスを取得し、raw data として保存します。

```text
URL → .storage/raw/<source_id>/<checksum>/...
```

### parse

保存した raw data を解析し、イベントなどの正規化データに変換します。

```text
raw data → parsed event → PostgreSQL
```

分ける理由:

- 取得失敗と解析失敗を分けて調査できる
- 同じraw dataでparserを何度も試せる
- HTML変更時に差分を確認できる
- 過剰アクセスを避けられる

## CSV import

イベント情報などは、まずCSV importで取り込めるようにします。

CSV import の利点:

- 非エンジニアでもデータを準備しやすい
- 取得元の権利が明確にしやすい
- テストが簡単
- crawlerより壊れにくい

運用では、最初にCSV、必要に応じてcrawlerへ進むのが安全です。

## parser registry

HTMLやJSONの形はサイトごとに違います。  
そのため、parser は registry で管理します。

例:

| parser key | 対象 |
|---|---|
| `utokyo_events_v1` | 東京大学イベントJSON |
| `shibaura_junior_events_v1` | 芝浦工大附属イベントページ |
| `sample_parser_v1` | サンプル |

parser key を明示することで、「どのparserで処理したか」を後から追えます。

## dedupe

同じイベントが複数回取り込まれることがあります。

重複排除では、安定したIDを作ります。

材料例:

- source id
- school id
- event title
- start date
- URL

これにより、同じイベントを何度取り込んでも二重登録されにくくなります。

## robots / policy

クローリングでは次を確認します。

- robots.txt
- 利用規約
- 公式APIの有無
- アクセス頻度
- 取得対象ページの公開性
- 負荷をかけない設計

不明な場合は、live fetch しない方が安全です。  
`policy_blocked` や `parser_only` として扱います。

## 監査ログ

crawler は次を記録します。

- fetch 成功・失敗
- HTTP status
- checksum
- parse 成功・失敗
- parser error
- dedupe 結果
- import 件数
- source maturity

これにより、運用者は「なぜこのイベントが入ったのか」「なぜ入らなかったのか」を確認できます。

## 非エンジニア向けの運用ルール

新しいデータソースを追加したい場合は、次の順で判断します。

1. 公式CSVやAPIがあるか確認する
2. 手動CSVで足りるか確認する
3. crawl が本当に必要か確認する
4. robots / policy を確認する
5. parser_only でローカル検証する
6. 問題なければ live_ready にする

焦ってlive crawlに進まないこと。  
データ収集の近道は、よく見ると崖の入口だったりします。

## まとめ

クローリングは最後の手段ではありませんが、最初の手段でもありません。  
このOSSでは、**安全に、記録を残して、再現可能に取り込む**ことを優先します。
