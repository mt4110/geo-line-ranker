# shellcheck shell=bash
# cspell:ignore gsub

read_env_candidate_mode() {
  local env_file="${1:-.env}"

  awk -F= '
    /^[[:space:]]*#/ { next }
    {
      key = $1
      value = $2
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", key)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      gsub(/^["'\''"]|["'\''"]$/, "", value)
    }
    key == "CANDIDATE_RETRIEVAL_MODE" {
      print value
      exit
    }
  ' "$env_file"
}

require_sql_only_contributor_mode() {
  local label="$1"
  local env_hint="$2"
  local shell_hint="$3"
  local env_file_mode

  env_file_mode="$(read_env_candidate_mode ".env")"

  if [[ -n "$env_file_mode" && "$env_file_mode" != "sql_only" ]]; then
    printf '[%s] expected .env CANDIDATE_RETRIEVAL_MODE=sql_only for the contributor baseline, got %s\n' \
      "$label" "$env_file_mode" >&2
    printf '[%s] %s\n' "$label" "$env_hint" >&2
    exit 1
  fi

  export CANDIDATE_RETRIEVAL_MODE="${CANDIDATE_RETRIEVAL_MODE:-${env_file_mode:-sql_only}}"

  if [[ "$CANDIDATE_RETRIEVAL_MODE" != "sql_only" ]]; then
    printf '[%s] expected CANDIDATE_RETRIEVAL_MODE=sql_only for the contributor baseline, got %s\n' \
      "$label" "$CANDIDATE_RETRIEVAL_MODE" >&2
    printf '[%s] %s\n' "$label" "$shell_hint" >&2
    exit 1
  fi
}
