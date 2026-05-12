#!/usr/bin/env bash
set -euo pipefail

if cargo nextest --version >/dev/null 2>&1; then
  exit 0
fi

cat >&2 <<'EOF'
cargo-nextest >= 0.9.127 is required for the fast Rust test lane.

Install one of:
  cargo install cargo-nextest --locked
  brew install cargo-nextest

Prebuilt binary docs:
  https://nexte.st/docs/installation/pre-built-binaries/
EOF

exit 127
