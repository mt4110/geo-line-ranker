#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

run() {
  printf '[frontend] %s\n' "$*"
  "$@"
}

cat <<'EOF'
[frontend] example frontend production-build smoke
[frontend] This keeps the example visible to contributors and CI without expanding the fixed public-MVP gate.
EOF

run npm --prefix examples/frontend-next ci --no-audit --no-fund
run npm --prefix examples/frontend-next run smoke
