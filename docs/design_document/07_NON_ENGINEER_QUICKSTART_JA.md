# 07. 非エンジニア向け Quickstart

## 目的

この手順は、`geo-line-ranker` をローカルで動かして、実際に推薦APIが返るところまで確認するためのものです。

開発者向けの細かい説明は `docs/QUICKSTART.md` にあります。  
この文書では「何をしているのか」を優先して説明します。

## 事前に必要なもの

開発者に以下が入っている環境を用意してもらってください。

- Docker
- Rust / cargo
- Git
- 必要なら Nix

非エンジニアが自分で全部入れる必要はありません。  
まずは「このコマンドを順に実行すると何が起きるか」を理解できれば十分です。

## 1. 設定ファイルを作る

```bash
cp .env.example .env
```

これは、アプリがどのDBやRedisを見るかを決める設定ファイルを作る作業です。

## 2. DBとRedisを起動する

```bash
docker compose -f .docker/docker-compose.yaml up -d postgres redis
```

ここでは、推薦に必要なデータベースとキャッシュを起動します。

- PostgreSQL: 正しいデータを保存する場所
- Redis: APIを速く返すための一時保存場所

## 3. DBのテーブルを作る

```bash
cargo run -p cli -- migrate
```

学校、イベント、駅、行動ログなどを保存するためのテーブルを作ります。

## 4. サンプルデータを入れる

```bash
cargo run -p cli -- seed example
```

ローカルで試すための学校・駅・イベントのサンプルデータを入れます。

## 5. イベントCSVを取り込む

```bash
cargo run -p cli -- import event-csv --file examples/import/events.sample.csv
```

サンプルのイベントCSVを取り込みます。

## 6. Workerを起動する

```bash
cargo run -p worker -- serve
```

Worker は、裏側で集計や更新を行う係です。

## 7. APIを起動する

```bash
cargo run -p api -- serve
```

推薦APIを起動します。

## 8. Swagger UIを開く

ブラウザで次を開きます。

```text
http://127.0.0.1:4000/swagger-ui
```

Swagger UI は、APIをブラウザから試せる画面です。

## 9. 推薦を試す

ターミナルで次を実行します。

```bash
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d '{"target_station_id":"st_tamachi","placement":"home","limit":3}'
```

期待する結果:

- 学校またはイベントの候補が返る
- `score` が付いている
- `explanation` がある
- `fallback_stage` がある

## 10. 行動ログを送る

```bash
curl -X POST http://127.0.0.1:4000/v1/track \
  -H "content-type: application/json" \
  -d '{"user_id":"demo-user-1","event_kind":"school_view","school_id":"school_seaside"}'
```

これは「ユーザーが学校を見た」という行動を記録します。

## よくある失敗

### APIが起動しない

確認:

- PostgreSQL が起動しているか
- `.env` があるか
- migrate が済んでいるか

### 推薦が空になる

確認:

- seed example を実行したか
- station id が正しいか
- event CSV を取り込んだか
- placement が正しいか

### 結果が変わらない

確認:

- Redis cache が効いていないか
- Worker が動いているか
- 行動ログが反映されるまでの処理が完了しているか

## full mode を試す場合

OpenSearch を使う full mode は、最初は後回しで構いません。  
まず minimal mode で動かし、仕組みを理解してから試します。

```bash
docker compose -f .docker/docker-compose.full.yaml up -d postgres redis opensearch
cargo run -p cli -- index rebuild
```

## まとめ

最初に見るべき成功状態は、これだけです。

- Swagger UI が開ける
- `/v1/recommendations` が返る
- 推薦理由が返る
- `/v1/track` で行動ログが保存される

この4つが動けば、`geo-line-ranker` の最小体験は成立しています。
