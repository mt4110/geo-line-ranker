#!/usr/bin/env bash
# cspell:ignore pathlib urllib startswith lstrip finditer
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

required_files=(
  README.md
  README_EN.md
  docs/README.md
  docs/FIRST_15_MINUTES.md
  docs/QUICKSTART.md
  docs/CONTRIBUTING_LOCAL.md
  docs/TESTING.md
  docs/MVP_ACCEPTANCE.md
  docs/OPERATIONS.md
  docs/OPTIONAL_EVIDENCE_HANDOFF.md
  docs/ARCHITECTURE.md
  docs/DATA_SOURCES.md
  docs/DATA_LICENSES.md
  docs/REASON_CATALOG.md
  docs/VERSIONING.md
  docs/DEPRECATION_POLICY.md
  docs/PROFILE_PACKS.md
  docs/MYSQL_COMPATIBILITY.md
  docs/design_document/README_JA.md
  .github/pull_request_template.md
)

missing=0
for path in "${required_files[@]}"; do
  if [[ ! -f "$path" ]]; then
    printf '[docs] missing required file: %s\n' "$path" >&2
    missing=1
  fi
done

if (( missing != 0 )); then
  exit 1
fi

python3 - <<'PY'
from pathlib import Path
from urllib.parse import unquote
import re
import sys

root = Path.cwd()
patterns = [
    "README.md",
    "README_EN.md",
    "API_SPEC.md",
    "docs/**/*.md",
    "examples/**/README.md",
]

docs = []
for pattern in patterns:
    docs.extend(
        path
        for path in root.glob(pattern)
        if path.is_file() and "node_modules" not in path.parts
    )
docs = sorted(set(docs))

inline_link = re.compile(r"(!?)\[[^\]]+\]\(([^)]+)\)")
reference_link = re.compile(r"^\s*\[[^\]]+\]:\s+(\S+)", re.MULTILINE)
scheme = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
errors = []


def first_markdown_target(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("<"):
        end = raw.find(">")
        if end != -1:
            return raw[1:end].strip()
    return raw.split()[0] if raw.split() else ""


def local_target(path: Path, target: str):
    if not target or target.startswith("#"):
        return path
    if scheme.match(target) or target.startswith("//"):
        return None

    target = target.split("#", 1)[0].split("?", 1)[0]
    if not target:
        return path

    if target.startswith("/"):
        resolved = (root / target.lstrip("/")).resolve()
    else:
        resolved = (path.parent / unquote(target)).resolve()

    try:
        resolved.relative_to(root)
    except ValueError:
        errors.append(f"{path.relative_to(root)} links outside repo: {target}")
        return None

    return resolved


for path in docs:
    text = path.read_text(encoding="utf-8")
    raw_targets = [match.group(2) for match in inline_link.finditer(text)]
    raw_targets.extend(match.group(1) for match in reference_link.finditer(text))

    for raw in raw_targets:
        target = first_markdown_target(raw)
        resolved = local_target(path, target)
        if resolved is None:
            continue
        if not resolved.exists():
            errors.append(f"{path.relative_to(root)} has missing local link: {target}")

if errors:
    for error in errors:
        print(f"[docs] {error}", file=sys.stderr)
    sys.exit(1)

print(f"[docs] checked {len(docs)} markdown files and local links")
PY
