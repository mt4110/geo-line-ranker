# 08. 用語集

## geo-line-ranker

地理条件・路線条件を主軸にしたルールベース推薦基盤です。

## 推薦

ユーザーに対して、学校・イベントなどの候補をおすすめ順に並べることです。

## ルールベース

AIや機械学習ではなく、人間が決めたルールで処理する方式です。

## 決定論的

同じ入力なら同じ結果になることです。  
検証や説明がしやすくなります。

## 地理条件

都道府県、市区町村、距離、緯度経度など、場所に関する条件です。

## 路線条件

駅、路線、沿線、同一路線、近接駅など、鉄道や移動に関する条件です。

## 候補抽出

推薦対象になりそうな学校・イベントを集める処理です。  
この段階で遠すぎる候補をなるべく落とします。

## スコアリング

候補に点数を付けることです。  
近さ、路線一致、人気、行動履歴などを使います。

## placement

推薦枠が表示される場所です。  
例: home, search, detail, mypage

## placement profile

placement ごとのランキング設定です。  
トップページではイベントを強める、検索結果では学校を強める、などを設定できます。

## fallback

データが足りないときに、別の方法で推薦を成立させる仕組みです。

## fallback stage

どの段階の fallback で推薦したかを示す値です。  
例: strict, relaxed, area_popular, national_safe

## diversity control

同じ学校や同じグループばかり出ないようにする制御です。

## score breakdown

スコアの内訳です。  
なぜ点数が入ったかを説明するために使います。

## explanation

推薦理由です。  
固定文言とscore breakdownから作ります。

## algorithm version

ランキングロジックのバージョンです。  
ルールを変えたときに追跡するために使います。

## profile version

placement profile のバージョンです。  
設定変更を追跡するために使います。

## PostgreSQL/PostGIS

正しいデータの保存場所です。  
PostGIS は地理情報を扱うための拡張です。

## Redis

高速な一時保存場所です。  
このプロジェクトでは cache only として扱います。

## OpenSearch

検索エンジンです。  
full mode で候補抽出を補助します。

## minimal mode

PostgreSQL中心で動く、ローカル検証しやすいモードです。

## full mode

OpenSearchも使う、より大きなデータ向けのモードです。

## crawler

許可されたWebページを取得し、イベントなどを取り込む仕組みです。

## allowlist crawl

事前に許可・設定したURLだけを取得する方式です。

## manifest

データソースやcrawler設定を書いたファイルです。

## parser

取得したHTMLやJSONから、イベントなどの情報を抜き出す処理です。

## source maturity

crawler のソースがどの程度運用可能かを示すラベルです。  
例: live_ready, parser_only, policy_blocked

## fixture

ローカルテスト用のサンプルデータです。

## seed

fixture をDBに入れることです。

## import

CSVや公式データをDBに取り込むことです。

## snapshot

行動ログなどを集計して、ランキングで使いやすくしたデータです。

## job queue

worker が処理する仕事の一覧です。  
このプロジェクトでは DB を正本にします。
