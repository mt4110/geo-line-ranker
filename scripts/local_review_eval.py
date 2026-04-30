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
FINDING_SECTIONS = {
    "Blockers": "blocker",
    "Serious risks": "serious_risk",
    "Missing tests": "missing_test",
}
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
