# API Spec

The generated OpenAPI artifact is `schemas/openapi.json`.

## `POST /v1/context/resolve`

Resolves recommendation context without running ranking, writing tracking events,
or creating context-resolution traces.

The request accepts:

- `request_id`
- `user_id`
- `target_station_id`
- `context.station_id`
- `context.line_id`
- `context.line_name`
- `context.area.country`
- `context.area.prefecture_code`
- `context.area.prefecture_name`
- `context.area.city_code`
- `context.area.city_name`

The response includes:

- `request_id`
- `context.context_source`
- `context.confidence`
- `context.privacy_level`
- `context.area`, when area context resolved
- `context.line`, when line context resolved
- `context.station`, when station context resolved
- `context.warnings`, when warnings are present
- `evidence_summary`

The response intentionally omits ranking-only policy fields such as
`fallback_policy` and `gate_policy`.

When no explicit station, line, or area context is supplied, this endpoint may
use read-only user context such as recent `search_execute` evidence or a stored
profile context. If no usable context is available, it returns
`default_safe_context`.

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
- `context.evidence_summary`
- `context.warnings`
- `fallback_stage`
- `candidate_counts`
- `items[].fallback_stage`
- `score_breakdown[].reason_code`
- `items[].score_breakdown[].reason_code`

Fallback stages are:

- `strict_station`
- `same_line`
- `same_city`
- `same_prefecture`
- `neighbor_area`
- `safe_global_popular`

`items[].fallback_stage` uses the same enum values as the top-level `fallback_stage`.
Score reason codes are cataloged in `docs/REASON_CATALOG.md`.

When a request has no explicit station, line, or area context, context
resolution may use a user's recent `search_execute` station as context evidence.
That path reports `context.context_source = recent_search_context` and
`context.evidence_summary.primary_kind = search_execute`.

Error responses use the common shape:

```json
{
  "error": "human readable message",
  "code": "bad_request"
}
```

## `POST /v1/track`

`occurred_at` is optional. When provided, it must be RFC3339, for example:

```text
2026-04-22T12:00:00+09:00
2026-04-22T03:00:00Z
```

`search_execute` requires `target_station_id` until context-derived tracking is persisted end-to-end.

Invalid tracking requests and reference-validation failures use the common
error response shape.

Raw addresses and raw external profile payloads are outside this API boundary.
