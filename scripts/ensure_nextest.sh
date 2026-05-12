#!/usr/bin/env bash
set -euo pipefail

required_version="0.9.127"

version_at_least() {
  local current="$1"
  local required="$2"
  local current_parts=()
  local required_parts=()

  IFS=. read -r -a current_parts <<<"${current%%[-+]*}"
  IFS=. read -r -a required_parts <<<"${required%%[-+]*}"

  for index in 0 1 2; do
    local current_part="${current_parts[$index]:-0}"
    local required_part="${required_parts[$index]:-0}"
    if ((10#$current_part > 10#$required_part)); then
      return 0
    fi
    if ((10#$current_part < 10#$required_part)); then
      return 1
    fi
  done

  return 0
}

installed_version="$(cargo nextest --version 2>/dev/null | awk 'NR == 1 { print $2 }' || true)"

if [[ -n "$installed_version" ]] && version_at_least "$installed_version" "$required_version"; then
  exit 0
fi

if [[ -n "$installed_version" ]]; then
  printf 'Found cargo-nextest %s, but %s or newer is required.\n\n' "$installed_version" "$required_version" >&2
  exit_code=1
else
  printf 'cargo-nextest was not found.\n\n' >&2
  exit_code=127
fi

cat >&2 <<'EOF'
cargo-nextest is required for the Rust test lanes.

Install one of:
  cargo install cargo-nextest --locked
  brew install cargo-nextest

Prebuilt binary docs:
  https://nexte.st/docs/installation/pre-built-binaries/
EOF

exit "$exit_code"
