# Reason Catalog

Recommendation explanations are generated from cataloged score components. The
catalog keeps deterministic score math, public labels, and explanation text in
one vocabulary.

Profile-owned reason catalogs live under `configs/profiles/*/reasons.yaml`.
They layer current reason codes into `core` and `profile` ownership without
changing the public `reason_code` values listed here.

## Contract

- `score_breakdown[].feature` is the internal component name.
- `score_breakdown[].reason_code` is the stable public reason code.
- `score_breakdown[].reason` is human-readable explanatory text.
- Top-level and item explanations may mention only catalog labels or fallback /
  diversity policy text.

Unknown score features are treated as `uncataloged` during deserialization for
cache and trace compatibility, but new ranking code should emit only cataloged
features.

## Catalog

| Feature | Reason code | Label |
| --- | --- | --- |
| `direct_station_bonus` | `geo.direct_station` | 直結条件 |
| `line_match_bonus` | `geo.line_match` | 沿線一致 |
| `school_station_distance` | `geo.station_distance` | 駅からの近さ |
| `walking_minutes` | `geo.walking_minutes` | 徒歩分数 |
| `neighbor_station_proximity` | `geo.neighbor_station_proximity` | 近傍駅との距離 |
| `open_day_bonus` | `event.open_day` | 公開イベント |
| `featured_event_bonus` | `event.featured` | 注目イベント |
| `event_priority_boost` | `event.priority` | 運用優先度 |
| `popularity_snapshot_bonus` | `behavior.popularity` | 最近の人気 |
| `area_affinity_bonus` | `behavior.area_affinity` | エリア需要 |
| `user_affinity_bonus` | `behavior.user_affinity` | ユーザー反応 |
| `content_kind_boost` | `placement.content_kind_boost` | placement調整 |
| `neighbor_area_penalty` | `fallback.neighbor_area_penalty` | 近隣エリア調整 |
| `safe_global_distance_penalty` | `fallback.safe_global_distance_penalty` | 遠距離抑制 |

## Integrity Checks

Run the ranking tests when changing score components or explanation text:

```bash
cargo test -p ranking
```

Public API changes to `score_breakdown` must also refresh:

```bash
cargo run -p cli -- dump-openapi
```
