# Portfolio Demo

This is the M1/M2 demo pack for showing `geo-line-ranker` as an explainable
school/event and local discovery ranking engine. It keeps the existing
public-MVP boundary: SQL-only candidate retrieval, `event-csv` operational
content, PostgreSQL/PostGIS, Redis as cache only, no AI/ML/vector search, and
no required live crawler.

## Demo Goal

Show one deterministic story:

1. A visitor starts from a station, line, or area.
2. The API recommends schools and school events.
3. The response explains the ranking with stable reason codes.
4. Placement changes the mix or order without changing the profile version.

Use the default runnable profile and fixture set:

- profile manifest: `configs/profiles/local-discovery-generic/profile.yaml`
- reason catalog: `configs/profiles/local-discovery-generic/reasons.yaml`
- default small fixture: `storage/fixtures/minimal/`
- portfolio-oriented request samples: `examples/school-event-jp/requests/`

The `school-event-jp` profile pack remains the maintained JP adapter reference,
but the command path below intentionally uses the default profile from
`.env.example` so a clean checkout demonstrates the same profile and fixture
that `just setup` prepares.

## Start The Demo

Use the same narrow path as [First 15 Minutes](FIRST_15_MINUTES.md).

```bash
# terminal A: one-time setup
just setup
```

Then start the long-running worker/API loop:

```bash
# terminal B: keep running while you try the demo requests
just dev
```

`just dev` stays in the foreground until you press `Ctrl-C`. Run the request
commands below from a separate terminal while terminal B is running.

If you want the manual setup form, use the commands in [Quickstart](QUICKSTART.md).
Do not add OpenSearch, full mode, live crawler operation, or managed
infrastructure for this demo.

## Three Requests

### 1. Station: Tamachi

This is the fixed first demo anchor. It should remain easy to remember and easy
to inspect in fixtures because `st_tamachi` has direct school links.

```bash
# terminal C: send demo requests while `just dev` is running
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d @examples/school-event-jp/requests/station.request.json
```

Look for:

- non-empty `items`
- `primary_station_id` or context evidence tied to `st_tamachi`
- direct-station or same-line candidates before broader fallback
- item explanations with `geo.direct_station`, `geo.line_match`, or event
  reasons where applicable

Short response shape:

```json
{
  "items": [
    {
      "content_kind": "event",
      "school_id": "school_seaside",
      "event_id": "event_seaside_open",
      "primary_station_id": "st_tamachi",
      "line_name": "JR Yamanote Line",
      "score_breakdown": [
        { "reason_code": "geo.line_match" },
        { "reason_code": "event.featured" }
      ]
    }
  ],
  "fallback_stage": "strict_station",
  "candidate_counts": {
    "strict_station": 2,
    "same_line": 5,
    "same_city": 2
  }
}
```

Exact scores, event titles, and fallback counts can vary with fixtures, event
imports, and ranking config. The important contract is that the response is
non-empty, deterministic for the same input/config/data, and explainable by
stable reason codes.

### 2. Line: JR Yamanote Line

This shows that the same engine can recommend from a route-level intent without
hardcoding one school.

```bash
# terminal C
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d @examples/school-event-jp/requests/line.request.json
```

Expected difference from the station request:

- candidate context is line-first rather than direct station-first
- same-line schools such as Seaside, Garden, Hillside, Creative, or Aoyama can
  compete depending on placement and event tags
- `geo.line_match` should be a natural explanation reason
- `search` placement can favor a different item mix or order than `home`

### 3. Area: Minato, Tokyo

This shows area-first discovery when a visitor knows the neighborhood but not a
specific station.

```bash
# terminal C
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d @examples/school-event-jp/requests/area.request.json
```

Expected difference from the line request:

- context is coarse area instead of line intent
- Minato schools should be eligible before unrelated remote areas
- fallback and candidate counts should make broadening visible when the strict
  area does not fill the requested limit
- reason codes should still explain geography, placement, event priority, and
  behavior features without introducing ML-style opaque scores

## Reason Codes To Explain

Use [Reason Catalog](REASON_CATALOG.md) for the full contract. For the portfolio
demo, these are the useful talking points:

| Reason code | What it means in the demo |
|---|---|
| `geo.direct_station` | The school or event is tied to the requested station. |
| `geo.line_match` | The candidate is on the requested or resolved line. |
| `geo.station_distance` | The linked school station is close enough to matter. |
| `geo.walking_minutes` | Walking time from the station contributes to rank. |
| `event.open_day` | An open-campus style event is present. |
| `event.featured` | The imported event is marked as featured. |
| `event.priority` | Event priority weight changes the score. |
| `placement.content_kind_boost` | The placement intentionally changes school/event mix. |
| `behavior.popularity` | Recent aggregate behavior contributes through snapshots. |
| `behavior.user_affinity` | User-specific behavior contributes after tracking events. |
| `fallback.neighbor_area_penalty` | Broader fallback was used but distance is penalized. |

## What The E2E Gate Proves

`just mvp-acceptance` is the right-sized E2E proof for M1/M2 because it covers
the live API, worker, PostgreSQL/PostGIS, Redis, fixtures, event CSV import, and
replacement semantics without dragging optional crawler/full-mode/OpenSearch
paths into the public demo.

It proves:

- bootstrap readiness through Docker Compose, migration, seed, and snapshots
- `GET /readyz` with PostgreSQL and Redis reachable and OpenSearch disabled
- `POST /v1/recommendations` for `st_tamachi` under both `home` and `search`
- placement-sensitive ordering while keeping `profile_version` stable
- `POST /v1/track` through queued worker jobs into popularity and affinity
  snapshots
- rerunnable snapshot refresh
- audited `event-csv` import and replacement behavior

It deliberately does not prove:

- live crawler policy readiness
- OpenSearch/full-mode production readiness
- managed infrastructure readiness
- every ranking scenario in the DB-free golden replay pack

Those are covered by separate evidence:

- `cargo run -p cli -- eval golden` for DB-free ranking scenario coverage
- `DATA_QUALITY_FAIL_ON_WARNING=true just data-quality-doctor` for strict
  release evidence
- `just release-readiness` for the M3 release-candidate command plan

This is enough for M1/M2 because the demo is a deterministic public-MVP story,
not a claim that optional production paths are complete.
