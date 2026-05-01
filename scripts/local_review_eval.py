#!/usr/bin/env python3
# cspell:ignore DiffTooLarge jsonl sha256 review_probe unwrap usize
"""Artifact harness for local review trials and workflow captures."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


SCHEMA_VERSION = "local-review-eval-v1"
DEFAULT_ARTIFACT_ROOT = Path(".storage/local_review_eval")
EXPECTED_ARTIFACT_PATHS = {
    "diff": "pr.diff",
    "error": "error.json",
    "findings": "findings.jsonl",
    "review": "review.md",
}
FINDING_SECTIONS = {
    "Blockers": "blocker",
    "Serious risks": "serious_risk",
    "Missing tests": "missing_test",
}
CHECKSUM_RE = re.compile(r"^[0-9a-f]{64}$")
METADATA_KEY_RE = re.compile(r"^[A-Za-z0-9_.-]+$")

SAMPLE_DIFF = """diff --git a/apps/api/src/review_probe.rs b/apps/api/src/review_probe.rs
new file mode 100644
index 0000000..1111111
--- /dev/null
+++ b/apps/api/src/review_probe.rs
@@ -0,0 +1,3 @@
+pub fn parse_limit(raw: &str) -> usize {
+    raw.parse::<usize>().unwrap()
+}
"""

SAMPLE_REVIEW = """## Blockers

## Serious risks

- apps/api/src/review_probe.rs:2: `unwrap()` can panic on malformed input and turn a user request into a 500. Return a typed error and let the handler map it to a 400 response.

## Missing tests

- apps/api/src/review_probe.rs:1: Add a malformed-limit test so the failure path stays covered.

## Summary

Two high-confidence findings were recorded from the synthetic diff.
"""

NO_FINDINGS_REVIEW = "No serious findings.\n"


@dataclass(frozen=True)
class WrittenArtifact:
    path: Path
    bytes: int
    sha256: str


class EvaluationError(Exception):
    """Raised when the simulated review flow should fail after saving artifacts."""


class InspectionError(Exception):
    """Raised when a saved artifact directory does not verify."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_bytes(path: Path, data: bytes) -> WrittenArtifact:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return WrittenArtifact(path=path, bytes=len(data), sha256=sha256_bytes(data))


def read_input_bytes(
    path: Optional[Path],
    fallback: str,
    *,
    allow_fallback: bool,
) -> Optional[bytes]:
    if path is None:
        if allow_fallback:
            return fallback.encode("utf-8")
        return None
    return path.read_bytes()


def normalize_review_for_scenario(
    scenario: str,
    review_path: Optional[Path],
    *,
    allow_fallback: bool,
) -> Optional[bytes]:
    if review_path is not None:
        return review_path.read_bytes()
    if scenario == "skipped":
        return None
    if not allow_fallback:
        return None
    if scenario == "no-findings":
        return NO_FINDINGS_REVIEW.encode("utf-8")
    return SAMPLE_REVIEW.encode("utf-8")


def parse_findings(review_text: str) -> list[dict[str, Any]]:
    if review_text.strip() == "No serious findings.":
        return []

    findings: list[dict[str, Any]] = []
    current_section: Optional[str] = None
    current_body: list[str] = []

    def flush_current() -> None:
        nonlocal current_body
        if current_section is None or not current_body:
            current_body = []
            return

        body = " ".join(line.strip() for line in current_body).strip()
        if not body:
            current_body = []
            return

        finding: dict[str, Any] = {
            "id": f"{current_section}-{len(findings) + 1}",
            "category": current_section,
            "body": body,
        }
        match = re.search(r"(?P<path>[\w./-]+):(?P<line>\d+)", body)
        if match:
            finding["file"] = match.group("path")
            finding["line"] = int(match.group("line"))
        findings.append(finding)
        current_body = []

    for raw_line in review_text.splitlines():
        line = raw_line.rstrip()
        if line.startswith("## "):
            flush_current()
            current_section = FINDING_SECTIONS.get(line[3:].strip())
            continue
        if current_section is None:
            continue
        if line.startswith("- "):
            flush_current()
            current_body = [line[2:]]
        elif current_body and line.strip():
            current_body.append(line)

    flush_current()
    return findings


def findings_jsonl(findings: list[dict[str, Any]]) -> bytes:
    lines = [
        json.dumps(finding, ensure_ascii=True, sort_keys=True)
        for finding in findings
    ]
    return ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")


def artifact_record(artifact: WrittenArtifact, out_dir: Path) -> dict[str, Any]:
    return {
        "path": artifact.path.relative_to(out_dir).as_posix(),
        "bytes": artifact.bytes,
        "sha256": artifact.sha256,
    }


def derive_run_id(
    scenario: str,
    diff_bytes: Optional[bytes],
    review_bytes: Optional[bytes],
    failure_message: Optional[str],
) -> str:
    def update_optional_bytes(label: str, value: Optional[bytes]) -> None:
        hasher.update(label.encode("ascii"))
        hasher.update(b"\0")
        if value is None:
            hasher.update(b"missing\0")
            return
        hasher.update(b"present\0")
        hasher.update(str(len(value)).encode("ascii"))
        hasher.update(b"\0")
        hasher.update(value)
        hasher.update(b"\0")

    hasher = hashlib.sha256()
    hasher.update(SCHEMA_VERSION.encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(scenario.encode("utf-8"))
    hasher.update(b"\0")
    update_optional_bytes("diff", diff_bytes)
    update_optional_bytes("review", review_bytes)
    if failure_message is None:
        update_optional_bytes("failure_message", None)
    else:
        update_optional_bytes("failure_message", failure_message.encode("utf-8"))
    return hasher.hexdigest()[:16]


def write_checksums(out_dir: Path, artifacts: list[WrittenArtifact]) -> WrittenArtifact:
    rows = [
        f"{artifact.sha256}  {artifact.path.relative_to(out_dir).as_posix()}"
        for artifact in sorted(artifacts, key=lambda item: item.path.as_posix())
    ]
    return write_bytes(out_dir / "checksums.txt", ("\n".join(rows) + "\n").encode("utf-8"))


def resolve_for_safety(path: Path) -> Path:
    return path.expanduser().resolve(strict=False)


def is_relative_to_path(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def ensure_force_target_is_safe(out_dir: Path, artifact_root: Path) -> None:
    resolved_out_dir = resolve_for_safety(out_dir)
    resolved_artifact_root = resolve_for_safety(artifact_root)
    if resolved_out_dir == resolved_artifact_root or not is_relative_to_path(
        resolved_out_dir,
        resolved_artifact_root,
    ):
        raise ValueError(
            "--force can only replace output directories below "
            f"{artifact_root}: {out_dir}"
        )


def prepare_out_dir(
    out_dir: Path,
    force: bool,
    artifact_root: Path = DEFAULT_ARTIFACT_ROOT,
) -> None:
    if force:
        ensure_force_target_is_safe(out_dir, artifact_root)

    if out_dir.exists():
        if not out_dir.is_dir():
            raise ValueError(f"output path is not a directory: {out_dir}")
        if any(out_dir.iterdir()):
            if not force:
                raise ValueError(f"output directory is not empty: {out_dir}")
            shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)


def safe_relative_artifact_path(raw_path: Any, field_name: str) -> str:
    if not isinstance(raw_path, str) or not raw_path:
        raise InspectionError(f"{field_name} must be a non-empty relative path")
    if "\0" in raw_path or "\\" in raw_path:
        raise InspectionError(f"{field_name} must be a portable relative path")

    candidate = Path(raw_path)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise InspectionError(f"{field_name} must stay inside the artifact directory")
    normalized = candidate.as_posix()
    if normalized in {"", "."}:
        raise InspectionError(f"{field_name} must name a file")
    return normalized


def safe_artifact_file(out_dir: Path, raw_path: Any, field_name: str) -> tuple[str, Path]:
    relative_path = safe_relative_artifact_path(raw_path, field_name)
    artifact_path = out_dir / relative_path
    resolved_out_dir = resolve_for_safety(out_dir)
    resolved_path = resolve_for_safety(artifact_path)
    if not is_relative_to_path(resolved_path, resolved_out_dir):
        raise InspectionError(f"{field_name} resolves outside the artifact directory")
    return relative_path, artifact_path


def read_json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise InspectionError(f"missing {label}") from error
    except UnicodeDecodeError as error:
        raise InspectionError(f"{label} is not valid UTF-8") from error
    except json.JSONDecodeError as error:
        raise InspectionError(f"{label} is not valid JSON: {error.msg}") from error
    if not isinstance(payload, dict):
        raise InspectionError(f"{label} must be a JSON object")
    return payload


def read_json_artifact_object(out_dir: Path, raw_path: str, label: str) -> dict[str, Any]:
    relative_path, path = safe_artifact_file(out_dir, raw_path, label)
    if not path.exists():
        raise InspectionError(f"missing artifact file: {relative_path}")
    if path.is_symlink():
        raise InspectionError(f"artifact path is a symlink: {relative_path}")
    if not path.is_file():
        raise InspectionError(f"artifact path is not a file: {relative_path}")
    return read_json_object(path, label)


def expect_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise InspectionError(f"{label} must be an object")
    return value


def expect_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise InspectionError(f"{label} must be a non-empty string")
    return value


def expect_sha256(value: Any, label: str) -> str:
    digest = expect_string(value, label)
    if CHECKSUM_RE.fullmatch(digest) is None:
        raise InspectionError(f"{label} must be a lowercase sha256 hex digest")
    return digest


def expect_nonnegative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InspectionError(f"{label} must be a non-negative integer")
    return value


def actual_artifact_record(out_dir: Path, raw_path: Any, field_name: str) -> dict[str, Any]:
    relative_path, path = safe_artifact_file(out_dir, raw_path, field_name)
    if not path.exists():
        raise InspectionError(f"missing artifact file: {relative_path}")
    if path.is_symlink():
        raise InspectionError(f"artifact path is a symlink: {relative_path}")
    if not path.is_file():
        raise InspectionError(f"artifact path is not a file: {relative_path}")

    data = path.read_bytes()
    return {
        "path": relative_path,
        "bytes": len(data),
        "sha256": sha256_bytes(data),
    }


def parse_checksums(out_dir: Path) -> dict[str, str]:
    _, checksum_path = safe_artifact_file(out_dir, "checksums.txt", "checksums.txt")
    if not checksum_path.exists():
        raise InspectionError("missing checksums.txt")
    if checksum_path.is_symlink():
        raise InspectionError("artifact path is a symlink: checksums.txt")
    if not checksum_path.is_file():
        raise InspectionError("artifact path is not a file: checksums.txt")

    checksums: dict[str, str] = {}
    with checksum_path.open("r", encoding="utf-8") as checksum_file:
        for line_number, raw_line in enumerate(checksum_file, start=1):
            line = raw_line.rstrip("\r\n")
            if not line:
                raise InspectionError(f"checksums.txt:{line_number} is blank")
            if "  " not in line:
                raise InspectionError(
                    f"checksums.txt:{line_number} must use '<sha256>  <path>'"
                )
            digest, raw_path = line.split("  ", 1)
            if CHECKSUM_RE.fullmatch(digest) is None:
                raise InspectionError(
                    f"checksums.txt:{line_number} has an invalid sha256 digest"
                )
            relative_path = safe_relative_artifact_path(
                raw_path,
                f"checksums.txt:{line_number}",
            )
            if relative_path in checksums:
                raise InspectionError(f"checksums.txt duplicates {relative_path}")
            checksums[relative_path] = digest

    if not checksums:
        raise InspectionError("checksums.txt must contain at least manifest.json")
    return checksums


def read_findings_count(out_dir: Path, raw_path: str) -> int:
    relative_path, findings_path = safe_artifact_file(
        out_dir,
        raw_path,
        "findings.path",
    )
    if not findings_path.exists():
        raise InspectionError(f"missing artifact file: {relative_path}")
    if findings_path.is_symlink():
        raise InspectionError(f"artifact path is a symlink: {relative_path}")
    if not findings_path.is_file():
        raise InspectionError(f"artifact path is not a file: {relative_path}")

    count = 0
    with findings_path.open("r", encoding="utf-8") as findings_file:
        for line_number, raw_line in enumerate(findings_file, start=1):
            line = raw_line.rstrip("\r\n")
            if not line:
                raise InspectionError(f"findings.jsonl:{line_number} is blank")
            try:
                finding = json.loads(line)
            except json.JSONDecodeError as error:
                raise InspectionError(
                    f"findings.jsonl:{line_number} is not valid JSON: {error.msg}"
                ) from error
            if not isinstance(finding, dict):
                raise InspectionError(f"findings.jsonl:{line_number} must be an object")
            count += 1
    return count


def validate_manifest_shape(
    manifest: dict[str, Any],
) -> tuple[str, str, str, str, int, Optional[int]]:
    schema_version = expect_string(manifest.get("schema_version"), "schema_version")
    if schema_version != SCHEMA_VERSION:
        raise InspectionError(
            f"unsupported schema_version {schema_version}; expected {SCHEMA_VERSION}"
        )

    run_id = expect_string(manifest.get("run_id"), "run_id")
    scenario = expect_string(manifest.get("scenario"), "scenario")
    status = expect_string(manifest.get("status"), "status")
    if scenario not in {"failure", "no-findings", "skipped", "success"}:
        raise InspectionError(f"unsupported scenario: {scenario}")
    if status not in {"completed", "failed", "skipped"}:
        raise InspectionError(f"unsupported status: {status}")
    if scenario in {"success", "no-findings"} and status != "completed":
        raise InspectionError(f"{scenario} artifacts must have completed status")
    if scenario == "failure" and status != "failed":
        raise InspectionError("failure artifacts must have failed status")
    if scenario == "skipped" and status != "skipped":
        raise InspectionError("skipped artifacts must have skipped status")

    summary = expect_object(manifest.get("summary"), "summary")
    findings_count = expect_nonnegative_int(
        summary.get("findings_count"),
        "summary.findings_count",
    )
    diff_bytes = summary.get("diff_bytes")
    if diff_bytes is not None:
        expect_nonnegative_int(diff_bytes, "summary.diff_bytes")
    if summary.get("deterministic_manifest") is not True:
        raise InspectionError("summary.deterministic_manifest must be true")
    if scenario == "no-findings" and findings_count != 0:
        raise InspectionError("no-findings artifacts must have zero findings")

    return schema_version, run_id, scenario, status, findings_count, diff_bytes


def validate_manifest_artifacts(
    out_dir: Path,
    manifest: dict[str, Any],
    scenario: str,
    status: str,
) -> dict[str, dict[str, Any]]:
    raw_artifacts = expect_object(manifest.get("artifacts"), "artifacts")
    artifacts: dict[str, dict[str, Any]] = {}

    if "findings" not in raw_artifacts:
        raise InspectionError("manifest.artifacts must include findings")
    if status == "completed":
        for required_key in ["diff", "review"]:
            if required_key not in raw_artifacts:
                raise InspectionError(
                    f"completed artifacts must include {required_key}"
                )
        if "error" in raw_artifacts:
            raise InspectionError("completed artifacts must not include error")
    if status in {"failed", "skipped"} and "error" not in raw_artifacts:
        raise InspectionError(f"{status} artifacts must include error")

    for key, raw_record in raw_artifacts.items():
        if key not in EXPECTED_ARTIFACT_PATHS:
            raise InspectionError(f"unknown manifest artifact key: {key}")
        record = expect_object(raw_record, f"artifacts.{key}")
        expected_path = EXPECTED_ARTIFACT_PATHS[key]
        actual_path = safe_relative_artifact_path(
            record.get("path"),
            f"artifacts.{key}.path",
        )
        if actual_path != expected_path:
            raise InspectionError(
                f"artifacts.{key}.path must be {expected_path}, got {actual_path}"
            )
        expected_bytes = expect_nonnegative_int(
            record.get("bytes"),
            f"artifacts.{key}.bytes",
        )
        expected_sha256 = expect_sha256(
            record.get("sha256"),
            f"artifacts.{key}.sha256",
        )
        actual_record = actual_artifact_record(
            out_dir,
            actual_path,
            f"artifacts.{key}.path",
        )
        if actual_record["bytes"] != expected_bytes:
            raise InspectionError(f"byte count mismatch for {actual_path}")
        if actual_record["sha256"] != expected_sha256:
            raise InspectionError(f"sha256 mismatch for {actual_path}")
        artifacts[key] = actual_record

    if scenario == "skipped" and "diff" in artifacts:
        raise InspectionError("skipped oversized-diff artifacts must omit pr.diff")
    return artifacts


def inspect_artifact_dir(out_dir: Path) -> dict[str, Any]:
    if not out_dir.exists():
        raise InspectionError(f"artifact directory does not exist: {out_dir}")
    if not out_dir.is_dir():
        raise InspectionError(f"artifact path is not a directory: {out_dir}")

    manifest = read_json_artifact_object(out_dir, "manifest.json", "manifest.json")
    (
        schema_version,
        run_id,
        scenario,
        status,
        expected_findings_count,
        diff_bytes,
    ) = validate_manifest_shape(manifest)
    artifacts = validate_manifest_artifacts(out_dir, manifest, scenario, status)
    if "diff" in artifacts:
        if diff_bytes != artifacts["diff"]["bytes"]:
            raise InspectionError("summary.diff_bytes does not match pr.diff")
    elif diff_bytes is not None:
        raise InspectionError("summary.diff_bytes must be null when pr.diff is absent")

    checksums = parse_checksums(out_dir)
    expected_checksum_paths = {record["path"] for record in artifacts.values()}
    expected_checksum_paths.add("manifest.json")
    missing_checksum_paths = sorted(expected_checksum_paths - set(checksums))
    if missing_checksum_paths:
        raise InspectionError(
            "checksums.txt is missing "
            + ", ".join(missing_checksum_paths)
        )

    unexpected_checksum_paths = sorted(set(checksums) - expected_checksum_paths)
    if unexpected_checksum_paths:
        raise InspectionError(
            "checksums.txt contains unexpected paths: "
            + ", ".join(unexpected_checksum_paths)
        )

    checksum_records = []
    for relative_path, expected_sha256 in sorted(checksums.items()):
        actual_record = actual_artifact_record(
            out_dir,
            relative_path,
            f"checksums[{relative_path}]",
        )
        if actual_record["sha256"] != expected_sha256:
            raise InspectionError(f"checksum mismatch for {relative_path}")
        checksum_records.append(actual_record)

    manifest_record = actual_artifact_record(out_dir, "manifest.json", "manifest.json")
    checksums_record = actual_artifact_record(out_dir, "checksums.txt", "checksums.txt")
    if checksums["manifest.json"] != manifest_record["sha256"]:
        raise InspectionError("checksum mismatch for manifest.json")

    actual_findings_count = read_findings_count(out_dir, artifacts["findings"]["path"])
    if actual_findings_count != expected_findings_count:
        raise InspectionError(
            "findings.jsonl count does not match summary.findings_count"
        )

    error_payload = None
    if "error" in artifacts:
        error_payload = read_json_artifact_object(
            out_dir,
            artifacts["error"]["path"],
            "error.json",
        )
        expect_string(error_payload.get("error_type"), "error.error_type")
        expect_string(error_payload.get("message"), "error.message")

    metadata = manifest.get("metadata")
    if metadata is not None:
        metadata = expect_object(metadata, "metadata")
        for key, value in metadata.items():
            expect_string(key, "metadata key")
            if METADATA_KEY_RE.fullmatch(key) is None:
                raise InspectionError(f"metadata key is invalid: {key}")
            if not isinstance(value, str):
                raise InspectionError(f"metadata.{key} must be a string")

    tracked_files = set(expected_checksum_paths)
    tracked_files.add("checksums.txt")
    # The artifact format is intentionally flat. Inspect only direct children so
    # an unexpected directory or symlinked directory cannot expand the scan.
    existing_entries = {path.name for path in out_dir.iterdir()}
    untracked_entries = sorted(existing_entries - tracked_files)
    if untracked_entries:
        raise InspectionError(
            "artifact directory contains unexpected entries: "
            + ", ".join(untracked_entries)
        )

    return {
        "artifacts": [
            {"key": key, **record}
            for key, record in sorted(artifacts.items(), key=lambda item: item[0])
        ],
        "checksums": checksum_records,
        "checksums_file": checksums_record,
        "diff_bytes": diff_bytes,
        "error": error_payload,
        "findings_count": expected_findings_count,
        "manifest_file": manifest_record,
        "metadata": metadata or {},
        "out_dir": out_dir.as_posix(),
        "run_id": run_id,
        "scenario": scenario,
        "schema_version": schema_version,
        "status": status,
        "untracked_entries": untracked_entries,
    }


def truncate_for_report(value: str, limit: int = 180) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3] + "..."


def format_inspection_report(report: dict[str, Any]) -> str:
    lines = [
        "local review artifact inspection: ok",
        f"- directory: {report['out_dir']}",
        f"- run_id: {report['run_id']}",
        f"- schema: {report['schema_version']}",
        f"- scenario/status: {report['scenario']} / {report['status']}",
        f"- findings: {report['findings_count']}",
    ]
    if report["diff_bytes"] is not None:
        lines.append(f"- diff bytes: {report['diff_bytes']}")

    lines.append("- artifacts:")
    for artifact in report["artifacts"]:
        lines.append(
            "  "
            + f"{artifact['key']}: {artifact['path']} "
            + f"({artifact['bytes']} bytes, sha256 {artifact['sha256'][:12]}...)"
        )
    lines.append(
        "- manifest.json: "
        + f"{report['manifest_file']['bytes']} bytes, "
        + f"sha256 {report['manifest_file']['sha256'][:12]}..."
    )
    lines.append(
        "- checksums.txt: "
        + f"{report['checksums_file']['bytes']} bytes, "
        + f"sha256 {report['checksums_file']['sha256'][:12]}..."
    )

    if report["error"] is not None:
        lines.append(
            "- error: "
            + f"{report['error']['error_type']} - "
            + truncate_for_report(report["error"]["message"])
        )
    if report["metadata"]:
        metadata_keys = ", ".join(sorted(report["metadata"]))
        lines.append(f"- metadata keys: {metadata_keys}")
    if report["untracked_entries"]:
        lines.append(
            "- untracked entries: " + ", ".join(report["untracked_entries"])
        )
    return "\n".join(lines)


def parse_metadata_pairs(pairs: list[str]) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"--metadata must be KEY=VALUE: {pair}")
        key, value = pair.split("=", 1)
        if not key or METADATA_KEY_RE.fullmatch(key) is None:
            raise ValueError(f"--metadata key is invalid: {key}")
        metadata[key] = value
    return dict(sorted(metadata.items()))


def run_evaluation(
    *,
    out_dir: Path,
    scenario: str,
    diff_path: Optional[Path],
    review_path: Optional[Path],
    run_id: Optional[str],
    failure_message: str,
    error_type: Optional[str],
    artifact_root: Path,
    metadata: dict[str, str],
    synthetic_inputs: bool,
    force: bool,
) -> int:
    prepare_out_dir(out_dir, force, artifact_root)

    diff_bytes = read_input_bytes(
        diff_path,
        SAMPLE_DIFF,
        allow_fallback=synthetic_inputs and scenario != "skipped",
    )
    if diff_bytes is None and scenario in {"success", "no-findings"}:
        raise ValueError("--diff is required when synthetic inputs are disabled")

    if scenario == "failure" and review_path is not None:
        review_bytes = review_path.read_bytes()
    elif scenario == "failure":
        review_bytes = None
    else:
        review_bytes = normalize_review_for_scenario(
            scenario,
            review_path,
            allow_fallback=synthetic_inputs,
        )
    if review_bytes is None and scenario in {"success", "no-findings"}:
        raise ValueError("--review is required when synthetic inputs are disabled")

    selected_run_id = run_id or derive_run_id(
        scenario,
        diff_bytes,
        review_bytes,
        failure_message if scenario in {"failure", "skipped"} else None,
    )

    artifacts: list[WrittenArtifact] = []

    status = "skipped" if scenario == "skipped" else "completed"
    if scenario == "failure":
        status = "failed"

    findings: list[dict[str, Any]] = []
    manifest_artifacts: dict[str, Any] = {}

    if diff_bytes is not None:
        diff_artifact = write_bytes(out_dir / "pr.diff", diff_bytes)
        artifacts.append(diff_artifact)
        manifest_artifacts["diff"] = artifact_record(diff_artifact, out_dir)

    if scenario in {"failure", "skipped"}:
        error_payload = {
            "error_type": error_type
            or (
                "ReviewSkipped"
                if scenario == "skipped"
                else "SimulatedReviewFailure"
            ),
            "message": failure_message,
        }
        error_artifact = write_bytes(
            out_dir / "error.json",
            json.dumps(error_payload, ensure_ascii=True, indent=2, sort_keys=True).encode(
                "utf-8"
            )
            + b"\n",
        )
        artifacts.append(error_artifact)
        manifest_artifacts["error"] = artifact_record(error_artifact, out_dir)

    if review_bytes is not None:
        review_artifact = write_bytes(out_dir / "review.md", review_bytes)
        artifacts.append(review_artifact)
        manifest_artifacts["review"] = artifact_record(review_artifact, out_dir)
        findings = parse_findings(review_bytes.decode("utf-8", errors="replace"))

    findings_artifact = write_bytes(out_dir / "findings.jsonl", findings_jsonl(findings))
    artifacts.append(findings_artifact)
    manifest_artifacts["findings"] = artifact_record(findings_artifact, out_dir)

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "run_id": selected_run_id,
        "scenario": scenario,
        "status": status,
        "summary": {
            "findings_count": len(findings),
            "diff_bytes": None if diff_bytes is None else len(diff_bytes),
            "deterministic_manifest": True,
        },
        "artifacts": manifest_artifacts,
    }
    if metadata:
        manifest["metadata"] = metadata

    manifest_artifact = write_bytes(
        out_dir / "manifest.json",
        json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True).encode("utf-8")
        + b"\n",
    )
    artifacts.append(manifest_artifact)

    checksum_artifact = write_checksums(out_dir, artifacts)
    artifacts.append(checksum_artifact)

    if scenario == "failure":
        raise EvaluationError(failure_message)
    return 0


def assert_same_file(left: Path, right: Path) -> None:
    if left.read_bytes() != right.read_bytes():
        raise AssertionError(f"{left.name} differs between deterministic runs")


def assert_value_error(action: Any, expected: str) -> None:
    try:
        action()
    except ValueError as error:
        if expected not in str(error):
            raise AssertionError(f"expected {expected!r} in {error!s}") from error
    else:
        raise AssertionError(f"expected ValueError containing {expected!r}")


def assert_inspection_error(action: Any, expected: str) -> None:
    try:
        action()
    except InspectionError as error:
        if expected not in str(error):
            raise AssertionError(f"expected {expected!r} in {error!s}") from error
    else:
        raise AssertionError(f"expected InspectionError containing {expected!r}")


def run_self_test() -> int:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        first = root / "first-success"
        second = root / "second-success"
        run_evaluation(
            out_dir=first,
            scenario="success",
            diff_path=None,
            review_path=None,
            run_id="fixed-success",
            failure_message="local review failed",
            error_type=None,
            artifact_root=DEFAULT_ARTIFACT_ROOT,
            metadata={},
            synthetic_inputs=True,
            force=False,
        )
        run_evaluation(
            out_dir=second,
            scenario="success",
            diff_path=None,
            review_path=None,
            run_id="fixed-success",
            failure_message="local review failed",
            error_type=None,
            artifact_root=DEFAULT_ARTIFACT_ROOT,
            metadata={},
            synthetic_inputs=True,
            force=False,
        )
        for filename in [
            "pr.diff",
            "review.md",
            "findings.jsonl",
            "manifest.json",
            "checksums.txt",
        ]:
            assert_same_file(first / filename, second / filename)

        manifest = json.loads((first / "manifest.json").read_text(encoding="utf-8"))
        if manifest["summary"]["findings_count"] != 2:
            raise AssertionError("success scenario should record two findings")
        success_inspection = inspect_artifact_dir(first)
        if success_inspection["status"] != "completed":
            raise AssertionError("success inspection should record completed status")
        if success_inspection["findings_count"] != 2:
            raise AssertionError("success inspection should count findings")

        if derive_run_id("skipped", None, None, "x") == derive_run_id(
            "skipped",
            b"",
            None,
            "x",
        ):
            raise AssertionError("missing and empty diff inputs should not share a run id")
        if derive_run_id("failure", b"x", None, "abc") == derive_run_id(
            "failure",
            b"x",
            b"",
            "abc",
        ):
            raise AssertionError("missing and empty review inputs should not share a run id")
        if derive_run_id("failure", b"x", b"ab", "c") == derive_run_id(
            "failure",
            b"x",
            b"a",
            "bc",
        ):
            raise AssertionError("review and failure message boundaries should affect run id")

        no_findings = root / "no-findings"
        run_evaluation(
            out_dir=no_findings,
            scenario="no-findings",
            diff_path=None,
            review_path=None,
            run_id="fixed-no-findings",
            failure_message="local review failed",
            error_type=None,
            artifact_root=DEFAULT_ARTIFACT_ROOT,
            metadata={},
            synthetic_inputs=True,
            force=False,
        )
        no_findings_manifest = json.loads(
            (no_findings / "manifest.json").read_text(encoding="utf-8")
        )
        if no_findings_manifest["summary"]["findings_count"] != 0:
            raise AssertionError("no-findings scenario should record zero findings")

        failure = root / "failure"
        second_failure = root / "second-failure"
        try:
            run_evaluation(
                out_dir=failure,
                scenario="failure",
                diff_path=None,
                review_path=None,
                run_id="fixed-failure",
                failure_message="simulated transport failure",
                error_type=None,
                artifact_root=DEFAULT_ARTIFACT_ROOT,
                metadata={},
                synthetic_inputs=True,
                force=False,
            )
        except EvaluationError:
            pass
        else:
            raise AssertionError("failure scenario should raise after saving artifacts")

        failure_manifest = json.loads((failure / "manifest.json").read_text(encoding="utf-8"))
        if failure_manifest["status"] != "failed":
            raise AssertionError("failure scenario should write failed status")
        if not (failure / "error.json").exists():
            raise AssertionError("failure scenario should write error.json")
        failure_inspection = inspect_artifact_dir(failure)
        if failure_inspection["error"]["error_type"] != "SimulatedReviewFailure":
            raise AssertionError("failure inspection should expose the error type")

        failure_review_input = root / "failure-review.md"
        failure_review_input.write_text(SAMPLE_REVIEW, encoding="utf-8")
        failure_with_review = root / "failure-with-review"
        try:
            run_evaluation(
                out_dir=failure_with_review,
                scenario="failure",
                diff_path=None,
                review_path=failure_review_input,
                run_id="fixed-failure-with-review",
                failure_message="simulated comment failure",
                error_type="HTTPError",
                artifact_root=DEFAULT_ARTIFACT_ROOT,
                metadata={},
                synthetic_inputs=True,
                force=False,
            )
        except EvaluationError:
            pass
        else:
            raise AssertionError("failure scenario with review should still raise")
        failure_with_review_manifest = json.loads(
            (failure_with_review / "manifest.json").read_text(encoding="utf-8")
        )
        if failure_with_review_manifest["summary"]["findings_count"] != 2:
            raise AssertionError("failure scenario should preserve supplied review findings")
        if not (failure_with_review / "review.md").exists():
            raise AssertionError("failure scenario should retain supplied review output")

        try:
            run_evaluation(
                out_dir=second_failure,
                scenario="failure",
                diff_path=None,
                review_path=None,
                run_id="fixed-failure",
                failure_message="simulated transport failure",
                error_type=None,
                artifact_root=DEFAULT_ARTIFACT_ROOT,
                metadata={},
                synthetic_inputs=True,
                force=False,
            )
        except EvaluationError:
            pass
        else:
            raise AssertionError("second failure scenario should raise after saving artifacts")

        for filename in [
            "pr.diff",
            "error.json",
            "findings.jsonl",
            "manifest.json",
            "checksums.txt",
        ]:
            assert_same_file(failure / filename, second_failure / filename)

        not_a_dir = root / "not-a-dir"
        not_a_dir.write_text("not a directory", encoding="utf-8")
        assert_value_error(
            lambda: prepare_out_dir(not_a_dir, force=False),
            "not a directory",
        )

        safe_root = root / "safe-root"
        safe_out_dir = safe_root / "run"
        safe_out_dir.mkdir(parents=True)
        (safe_out_dir / "stale.txt").write_text("stale", encoding="utf-8")
        prepare_out_dir(safe_out_dir, force=True, artifact_root=safe_root)
        if not safe_out_dir.is_dir() or any(safe_out_dir.iterdir()):
            raise AssertionError("safe forced cleanup should recreate an empty directory")

        unsafe_out_dir = root / "unsafe"
        unsafe_out_dir.mkdir()
        (unsafe_out_dir / "keep.txt").write_text("keep", encoding="utf-8")
        assert_value_error(
            lambda: prepare_out_dir(unsafe_out_dir, force=True, artifact_root=safe_root),
            "--force can only replace output directories below",
        )
        if not (unsafe_out_dir / "keep.txt").exists():
            raise AssertionError("unsafe forced cleanup should not delete files")

        root_out_dir = safe_root
        root_out_dir.mkdir(exist_ok=True)
        (root_out_dir / "keep-root.txt").write_text("keep", encoding="utf-8")
        assert_value_error(
            lambda: prepare_out_dir(root_out_dir, force=True, artifact_root=safe_root),
            "--force can only replace output directories below",
        )
        if not (root_out_dir / "keep-root.txt").exists():
            raise AssertionError("forced cleanup should not delete the artifact root")

        missing_unsafe_out_dir = root / "missing-unsafe"
        assert_value_error(
            lambda: prepare_out_dir(
                missing_unsafe_out_dir,
                force=True,
                artifact_root=safe_root,
            ),
            "--force can only replace output directories below",
        )
        if missing_unsafe_out_dir.exists():
            raise AssertionError("unsafe forced creation should not create a directory")

        skipped = root / "skipped"
        run_evaluation(
            out_dir=skipped,
            scenario="skipped",
            diff_path=None,
            review_path=None,
            run_id="fixed-skipped",
            failure_message="diff exceeds configured limit",
            error_type="DiffTooLarge",
            artifact_root=DEFAULT_ARTIFACT_ROOT,
            metadata={"pr_number": "7", "repository": "example/project"},
            synthetic_inputs=False,
            force=False,
        )
        skipped_manifest = json.loads((skipped / "manifest.json").read_text(encoding="utf-8"))
        if skipped_manifest["status"] != "skipped":
            raise AssertionError("skipped scenario should write skipped status")
        if "diff" in skipped_manifest["artifacts"]:
            raise AssertionError("skipped scenario without a diff should not write pr.diff")
        if skipped_manifest["metadata"]["pr_number"] != "7":
            raise AssertionError("metadata should be recorded in the manifest")
        skipped_inspection = inspect_artifact_dir(skipped)
        if skipped_inspection["status"] != "skipped":
            raise AssertionError("skipped inspection should record skipped status")

        tampered = root / "tampered"
        shutil.copytree(first, tampered)
        tampered_review_path = tampered / "review.md"
        tampered_review_path.write_bytes(
            tampered_review_path.read_bytes().replace(b"unwrap", b"panic!", 1)
        )
        assert_inspection_error(
            lambda: inspect_artifact_dir(tampered),
            "sha256 mismatch for review.md",
        )

        traversal = root / "traversal"
        shutil.copytree(first, traversal)
        traversal_manifest_path = traversal / "manifest.json"
        traversal_manifest = json.loads(
            traversal_manifest_path.read_text(encoding="utf-8")
        )
        traversal_manifest["artifacts"]["review"]["path"] = "../review.md"
        traversal_manifest_path.write_text(
            json.dumps(
                traversal_manifest,
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        assert_inspection_error(
            lambda: inspect_artifact_dir(traversal),
            "must stay inside the artifact directory",
        )

        windows_path = root / "windows-path"
        shutil.copytree(first, windows_path)
        windows_manifest_path = windows_path / "manifest.json"
        windows_manifest = json.loads(windows_manifest_path.read_text(encoding="utf-8"))
        windows_manifest["artifacts"]["review"]["path"] = r"nested\review.md"
        windows_manifest_path.write_text(
            json.dumps(windows_manifest, ensure_ascii=True, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        assert_inspection_error(
            lambda: inspect_artifact_dir(windows_path),
            "portable relative path",
        )

        inconsistent_diff_bytes = root / "inconsistent-diff-bytes"
        shutil.copytree(first, inconsistent_diff_bytes)
        inconsistent_manifest_path = inconsistent_diff_bytes / "manifest.json"
        inconsistent_manifest = json.loads(
            inconsistent_manifest_path.read_text(encoding="utf-8")
        )
        inconsistent_manifest["summary"]["diff_bytes"] = 0
        inconsistent_manifest_path.write_text(
            json.dumps(
                inconsistent_manifest,
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        assert_inspection_error(
            lambda: inspect_artifact_dir(inconsistent_diff_bytes),
            "summary.diff_bytes does not match pr.diff",
        )

        unexpected_file = root / "unexpected-file"
        shutil.copytree(first, unexpected_file)
        (unexpected_file / "extra.txt").write_text("extra", encoding="utf-8")
        assert_inspection_error(
            lambda: inspect_artifact_dir(unexpected_file),
            "unexpected entries",
        )

        unexpected_directory = root / "unexpected-directory"
        shutil.copytree(first, unexpected_directory)
        (unexpected_directory / "extra").mkdir()
        assert_inspection_error(
            lambda: inspect_artifact_dir(unexpected_directory),
            "unexpected entries",
        )

        symlink_escape = root / "symlink-escape"
        shutil.copytree(first, symlink_escape)
        escaped_manifest = root / "escaped-manifest.json"
        escaped_manifest.write_text("{}", encoding="utf-8")
        (symlink_escape / "manifest.json").unlink()
        try:
            (symlink_escape / "manifest.json").symlink_to(escaped_manifest)
        except OSError:
            pass
        else:
            assert_inspection_error(
                lambda: inspect_artifact_dir(symlink_escape),
                "resolves outside the artifact directory",
            )

        symlink_inside = root / "symlink-inside"
        shutil.copytree(first, symlink_inside)
        inside_manifest = symlink_inside / "inside-manifest.json"
        inside_manifest.write_text("{}", encoding="utf-8")
        (symlink_inside / "manifest.json").unlink()
        try:
            (symlink_inside / "manifest.json").symlink_to(inside_manifest)
        except OSError:
            pass
        else:
            assert_inspection_error(
                lambda: inspect_artifact_dir(symlink_inside),
                "artifact path is a symlink: manifest.json",
            )

        invalid_utf8_manifest = root / "invalid-utf8-manifest"
        shutil.copytree(first, invalid_utf8_manifest)
        (invalid_utf8_manifest / "manifest.json").write_bytes(b"\xff")
        assert_inspection_error(
            lambda: inspect_artifact_dir(invalid_utf8_manifest),
            "manifest.json is not valid UTF-8",
        )

        checksum_directory = root / "checksum-directory"
        shutil.copytree(first, checksum_directory)
        (checksum_directory / "checksums.txt").unlink()
        (checksum_directory / "checksums.txt").mkdir()
        assert_inspection_error(
            lambda: inspect_artifact_dir(checksum_directory),
            "artifact path is not a file: checksums.txt",
        )

    print("local review evaluation self-test ok")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Save deterministic artifacts for local review trials and captures.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run deterministic success, no-findings, and failure checks.",
    )
    parser.add_argument(
        "--inspect",
        type=Path,
        metavar="ARTIFACT_DIR",
        help="Read and verify a saved local review artifact directory.",
    )
    parser.add_argument(
        "--scenario",
        choices=["success", "no-findings", "failure", "skipped"],
        default="success",
        help="Review capture scenario. Success and no-findings use synthetic inputs by default.",
    )
    parser.add_argument(
        "--diff",
        type=Path,
        help="Path to a PR diff to store. Defaults to a synthetic diff.",
    )
    parser.add_argument(
        "--review",
        type=Path,
        help="Path to captured review markdown. Ignored for the failure scenario.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(".storage/local_review_eval/latest"),
        help="Directory for manifest, checksums, diff, review, and findings artifacts.",
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=DEFAULT_ARTIFACT_ROOT,
        help="Root directory that --force may safely replace below.",
    )
    parser.add_argument(
        "--run-id",
        help="Stable run id to write into manifest.json. Defaults to an input checksum prefix.",
    )
    parser.add_argument(
        "--metadata",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Manifest metadata entry. May be supplied more than once.",
    )
    parser.add_argument(
        "--error-type",
        help="Error type to store for failure or skipped scenarios.",
    )
    parser.add_argument(
        "--failure-message",
        default="local review failed",
        help="Failure message to store for the failure scenario.",
    )
    parser.add_argument(
        "--expect-failure",
        action="store_true",
        help="Return zero after writing failure artifacts for a failure scenario.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace a non-empty output directory.",
    )
    parser.add_argument(
        "--no-synthetic-inputs",
        action="store_true",
        help="Require supplied inputs instead of writing sample evaluation content.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.self_test:
        return run_self_test()
    if args.inspect is not None:
        try:
            print(format_inspection_report(inspect_artifact_dir(args.inspect)))
            return 0
        except InspectionError as error:
            print(f"artifact inspection failed: {error}", file=sys.stderr)
            return 2

    try:
        metadata = parse_metadata_pairs(args.metadata)
        return run_evaluation(
            out_dir=args.out_dir,
            scenario=args.scenario,
            diff_path=args.diff,
            review_path=args.review,
            run_id=args.run_id,
            failure_message=args.failure_message,
            error_type=args.error_type,
            artifact_root=args.artifact_root,
            metadata=metadata,
            synthetic_inputs=not args.no_synthetic_inputs,
            force=args.force,
        )
    except EvaluationError as error:
        if args.expect_failure:
            print(f"expected failure saved: {error}")
            return 0
        print(f"failure artifacts saved: {error}", file=sys.stderr)
        return 1
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
