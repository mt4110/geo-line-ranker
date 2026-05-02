#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

run() {
  printf '[ts-sdk] %s\n' "$*"
  "$@"
}

cat <<'EOF'
[ts-sdk] TypeScript SDK build check
[ts-sdk] This is contributor and CI tooling, not a public-MVP gate case.
EOF

run npm --prefix packages/ts-sdk ci --ignore-scripts --no-audit --no-fund
run npm --prefix packages/ts-sdk run check
