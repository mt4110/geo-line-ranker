#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

run() {
  printf '[smoke] %s\n' "$*"
  "$@"
}

cat <<'EOF'
[smoke] SQL-only contributor smoke
[smoke] This is a read-only check for configs, manifests, the default fixture, crawler manifests, and whitespace.
[smoke] It does not run the fixed public-MVP gate; use just mvp-acceptance for that.
EOF

run cargo run -p cli -- config lint
run cargo run -p cli -- source-manifest lint
run cargo run -p cli -- fixtures doctor --path storage/fixtures/minimal
run cargo run -p crawler -- manifest lint
run git diff --check
