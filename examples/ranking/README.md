# Ranking examples

Placement-aware recommendation requests:

```bash
curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d @examples/ranking/home.request.json

curl -X POST http://127.0.0.1:4000/v1/recommendations \
  -H "content-type: application/json" \
  -d @examples/ranking/search.request.json
```

Use the same target station to compare placement differences. `home` pushes mixed event content harder, while `search` keeps school candidates closer to the front.
