# API Spec

The generated OpenAPI artifact is `schemas/openapi.json`.

## `POST /v1/recommendations`

The request keeps `target_station_id` compatibility and also accepts optional `context`.

Supported context inputs:

- `context.station_id`
- `context.line_id`
- `context.line_name`
- `context.area.prefecture_code`
- `context.area.prefecture_name`
- `context.area.city_code`
- `context.area.city_name`

The response includes:

- `context.context_source`
- `context.confidence`
- `context.privacy_level`
- `context.warnings`
- `fallback_stage`
- `candidate_counts`

Fallback stages are:

- `strict_station`
- `same_line`
- `same_city`
- `same_prefecture`
- `neighbor_area`
- `safe_global_popular`

## `POST /v1/track`

`occurred_at` is optional. When provided, it must be RFC3339, for example:

```text
2026-04-22T12:00:00+09:00
2026-04-22T03:00:00Z
```

`search_execute` requires `target_station_id` until context-derived tracking is persisted end-to-end.

Raw addresses and raw external profile payloads are outside this API boundary.
