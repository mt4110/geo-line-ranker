# 学校住所座標化 RunBook（外部APIなし / Rustバックエンド向け）

このRunBookは、**学校住所はあるが緯度経度がない**データを、`geo-line-ranker` に安全に載せるための恒久運用手順です。  
目的は **zip納品後も同じ手順で再現できること** です。

## 0. スコープ（v4.0）

- この文書の完了条件は **「学校レコメンドをローカル運用で再現できること」** です。
- 範囲内: PostgreSQL/PostGIS + Redis、`/v1/recommendations` と `/v1/track` の確認、学校データ更新の手動/半手動運用。
- 範囲外: GUI製品化、本番監視運用、自動更新パイプラインの完全自動化。

## 1. 最短起動導線（納品先向け）

リポジトリ root で実行（`just dev` は常駐するため、先に `just smoke` を実行）:

```bash
just setup
just smoke

# terminal A（常駐）
just dev
```

最低受け入れ確認（別ターミナル）:

```bash
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d '{"target_station_id":"st_tamachi","placement":"home","limit":3}'

curl -X POST http://127.0.0.1:4000/v1/track \
  -H "content-type: application/json" \
  -d '{"user_id":"runbook-user","event_kind":"school_view","school_id":"school_seaside"}'
```

## 2. データ契約（この形式に合わせる）

### 2.1 学校コードCSV（`jp-school-codes`）

ヘッダー:

```csv
school_code,name,prefecture_name,city_name,school_type
```

### 2.2 学校座標CSV（`jp-school-geodata`）

ヘッダー:

```csv
school_code,name,prefecture_name,city_name,address,school_type,latitude,longitude
```

`school_id` は import 時に `jp_school_<school_code>` として生成されます。

## 3. 外部APIなし座標化フロー（推奨）

### 3.1 参照データを投入

```bash
cargo run -p cli -- import jp-rail --manifest storage/sources/jp_rail/example.yaml
cargo run -p cli -- import jp-school-codes --manifest storage/sources/jp_school/example.yaml
cargo run -p cli -- import jp-school-geodata --manifest storage/sources/jp_school_geo/example.yaml
cargo run -p cli -- import jp-postal --manifest storage/sources/jp_postal/example.yaml
```

### 3.2 自前学校DBをステージング

例（`psql`）:

```sql
CREATE TABLE IF NOT EXISTS staging_school_input (
  school_code TEXT,
  name TEXT NOT NULL,
  prefecture_name TEXT NOT NULL,
  city_name TEXT NOT NULL,
  address TEXT NOT NULL,
  school_type TEXT NOT NULL
);
```

`school_code` を持つ場合は最優先で使います。無い場合は `name + address` 突合を使います。

### 3.3 座標を埋める（優先順）

1. `school_code` 完全一致で `jp_school_geodata` から付与  
2. 未解決のみ `name + prefecture_name + city_name + address` 正規化突合  
3. それでも未解決は `manual_lat_lon.csv` で手補完（運用事故防止のため明示管理）

サンプル（1. のみ）:

```sql
DROP TABLE IF EXISTS staging_school_geocoded;
CREATE TABLE staging_school_geocoded AS
SELECT
  s.school_code,
  s.name,
  s.prefecture_name,
  s.city_name,
  s.address,
  s.school_type,
  g.latitude,
  g.longitude
FROM staging_school_input s
JOIN jp_school_geodata g
  ON s.school_code = g.school_code;
```

未解決確認:

```sql
SELECT COUNT(*) AS unresolved
FROM staging_school_input s
LEFT JOIN staging_school_geocoded g
  ON s.school_code = g.school_code
WHERE g.school_code IS NULL;
```

### 3.4 重要な注意（`jp-postal` の扱い）

- `jp_postal_codes` は **郵便番号・地名辞書** であり、緯度経度列は持ちません。
- そのため、このリポジトリ標準機能だけで「郵便番号重心の自動座標補完」は完結しません。
- 郵便番号重心を使う場合は、別途作成した `postal_centroids`（社内管理テーブルや事前生成CSV）を JOIN して補完してください。

### 3.5 生成CSVを固定化して再取り込み

`staging_school_geocoded` から `school_geodata.csv` を出力し、manifest を作成して import します。  
運用時はこのCSVを更新するだけにすると、再現性を維持できます。

```bash
cargo run -p cli -- import jp-school-geodata --manifest <your_manifest>.yaml
cargo run -p cli -- derive school-station-links
```

## 4. 受け入れ確認（固定）

1. `/v1/recommendations` が `200` かつ `items` 非空  
2. `/v1/track` が `202`（`status: accepted` を返す）  
3. `cargo run -p cli -- jobs list --limit 20` で worker キューが確認できる

## 5. トラブル時の最短確認（4点）

1. **APIが返らない**: `just dev` の API/worker 両プロセスが起動しているか  
2. **DB未起動**: `docker compose -f .docker/docker-compose.yaml up -d postgres redis`  
3. **migration未実行**: `cargo run -p cli -- migrate`  
4. **worker未起動**: `cargo run -p worker -- serve`

## 6. 納品物に必ず同梱するもの

1. 固定化した `school_codes.csv` / `school_geodata.csv` と対応 manifest  
2. 実行順コマンド（本RunBookの 1 / 3.1 / 3.5 / 4）  
3. 受け入れ `curl` 2本（recommendations / track）  
4. 「v4.0範囲内・範囲外」の明記
