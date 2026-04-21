# geo-line-ranker 設計ドキュメント案内

このディレクトリは、`geo-line-ranker` を **非エンジニアでも理解できるように説明するための設計ドキュメント群**です。

既存の `docs/ARCHITECTURE.md`、`docs/QUICKSTART.md`、`docs/OPERATIONS.md` は開発者・運用者向けの情報が中心です。  
このディレクトリでは、まず「何のためのOSSなのか」「どんなデータを使うのか」「なぜAIではないのか」「どう安全に運用するのか」を、業務側・企画側・データ提供者・協力会社にも伝わる言葉で整理します。

## 読む順番

| 順番 | ファイル | 読む人 | 内容 |
|---:|---|---|---|
| 1 | `00_EXECUTIVE_SUMMARY_JA.md` | 全員 | 5分で分かる全体像 |
| 2 | `01_REQUIREMENTS_JA.md` | 企画・PM・開発 | 要件と非目標 |
| 3 | `02_SYSTEM_OVERVIEW_JA.md` | 企画・PM・開発 | システムの構成と役割 |
| 4 | `03_RECOMMENDATION_LOGIC_JA.md` | 企画・PM・開発 | 推薦ロジックの考え方 |
| 5 | `04_DATA_FLOW_PRIVACY_JA.md` | 企画・法務・開発 | データの流れと個人情報の考え方 |
| 6 | `05_CRAWLING_AND_IMPORT_POLICY_JA.md` | 運用・開発 | クローリングとCSV/API取り込みの方針 |
| 7 | `06_OPERATIONS_GUIDE_JA.md` | 運用・CS・PM | 日々の運用で見るところ |
| 8 | `07_NON_ENGINEER_QUICKSTART_JA.md` | 触ってみる人 | ローカルで試すときの道順 |
| 9 | `08_GLOSSARY_JA.md` | 全員 | 用語集 |
| 10 | `09_FAQ_JA.md` | 全員 | よくある質問 |
| 11 | `10_DOCS_PROMPT_TEMPLATE.md` | 開発者 | この docs 群を再生成・移植するときのプロンプト雛形 |

## この設計で守ること

- 推薦は **AI / 機械学習ではなく、ルールとスコアリング**で行う。
- 地理・路線条件を必ず主軸にし、遠すぎる候補を安易に出さない。
- 推薦理由を説明できるようにする。
- Redis はキャッシュとして扱い、正しいデータの正本にはしない。
- PostgreSQL/PostGIS を正本にし、OpenSearch は候補抽出の補助として扱う。
- クローリングは任意機能とし、許可されたソースだけを安全に扱う。
- ローカルで試せる quickstart を重視する。

## このディレクトリの位置づけ

この docs は、実装コードを直接説明する「内部仕様書」ではありません。  
目的は、OSS 利用者や関係者が **安全に導入判断できる状態**を作ることです。

より細かい実装情報は、以下の既存ドキュメントを参照します。

- `docs/ARCHITECTURE.md`
- `docs/QUICKSTART.md`
- `docs/OPERATIONS.md`
- `docs/DATA_SOURCES.md`
- `docs/DATA_LICENSES.md`
- `docs/TESTING.md`
