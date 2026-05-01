#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

run() {
  printf '[eval] %s\n' "$*"
  "$@"
}

run python3 scripts/local_review_eval.py --self-test

if [[ "${RUN_REPLAY_EVAL:-0}" == "1" ]]; then
  run cargo run -p cli -- replay evaluate --limit "${REPLAY_EVAL_LIMIT:-20}"
else
  cat <<'EOF'
[eval] replay evaluation skipped by default
[eval] after just setup and a few recommendation traces, run:
[eval]   RUN_REPLAY_EVAL=1 just eval
EOF
fi
