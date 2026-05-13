#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ $# -eq 0 ]]; then
  set -- .
fi

if command -v cspell >/dev/null 2>&1; then
  exec cspell --config cspell.json --no-progress "$@"
fi

if ! command -v npm >/dev/null 2>&1; then
  printf '[spellcheck] cspell or npm is required. Install Node.js/npm or cspell.\n' >&2
  exit 127
fi

exec npx --yes cspell@latest --config cspell.json --no-progress "$@"
