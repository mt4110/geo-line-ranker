#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

run() {
  printf '[openapi] %s\n' "$*"
  "$@"
}

cat <<'EOF'
[openapi] generated contract drift check
[openapi] This checks schemas/openapi.json only; it does not change the fixed public-MVP gate.
EOF

run cargo run -p cli -- dump-openapi
run git diff --exit-code -- schemas/openapi.json
