"""Strict disposable candidate-run contract.

This module validates controller/board evidence only; it does not own Mnemes
semantics or authorize live deployment.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


class ContractError(ValueError):
    """A candidate manifest or result violates the admission contract."""


_REQUIRED_MANIFEST = {
    "run_id",
    "host_identity",
    "stage_path",
    "snapshot_manifest_sha256",
    "source_sha256",
    "binary_sha256",
    "harness_sha256",
    "timeout_seconds",
    "allowed_write_roots",
}
_REQUIRED_RESULT = {"run_id", "outcome", "receipt_sha256", "evidence"}
_TERMINAL_OUTCOMES = {"passed", "failed", "outcome_unknown", "cleanup_pending"}


def _require_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ContractError(f"{name} must be a non-empty string")
    return value


def validate_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    missing = sorted(_REQUIRED_MANIFEST - manifest.keys())
    if missing:
        raise ContractError(f"manifest missing fields: {', '.join(missing)}")
    for name in ("run_id", "host_identity", "stage_path", "snapshot_manifest_sha256", "source_sha256", "binary_sha256", "harness_sha256"):
        _require_string(manifest[name], name)
    if not isinstance(manifest["timeout_seconds"], (int, float)) or manifest["timeout_seconds"] <= 0:
        raise ContractError("timeout_seconds must be positive")
    roots = manifest["allowed_write_roots"]
    if not isinstance(roots, list) or not roots or any(not isinstance(root, str) or not root.startswith("/") for root in roots):
        raise ContractError("allowed_write_roots must contain absolute paths")
    return dict(manifest)


def validate_result(result: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, Any]:
    validate_manifest(manifest)
    missing = sorted(_REQUIRED_RESULT - result.keys())
    if missing:
        raise ContractError(f"result missing fields: {', '.join(missing)}")
    if result["run_id"] != manifest["run_id"]:
        raise ContractError("result run_id does not match manifest")
    outcome = result["outcome"]
    if outcome not in _TERMINAL_OUTCOMES:
        raise ContractError(f"invalid terminal outcome: {outcome!r}")
    _require_string(result["receipt_sha256"], "receipt_sha256")
    evidence = result["evidence"]
    if not isinstance(evidence, dict) or not evidence.get("required_files"):
        raise ContractError("evidence.required_files must be non-empty")
    if outcome == "passed":
        if evidence.get("teardown_verified") is not True:
            raise ContractError("passed result requires verified teardown")
        if evidence.get("input_integrity") is not True:
            raise ContractError("passed result requires input integrity")
        if evidence.get("remote_exit_code") != 0:
            raise ContractError("passed result requires remote exit code 0")
    if outcome == "cleanup_pending" and not evidence.get("quarantine_path"):
        raise ContractError("cleanup_pending requires a quarantine path")
    return dict(result)


@dataclass(frozen=True)
class CandidateDecision:
    accepted: bool
    reason: str


def decide(manifest: Mapping[str, Any], result: Mapping[str, Any]) -> CandidateDecision:
    try:
        validate_result(result, manifest)
    except ContractError as error:
        return CandidateDecision(False, str(error))
    if result["outcome"] != "passed":
        return CandidateDecision(False, f"terminal outcome is {result['outcome']}")
    return CandidateDecision(True, "validated candidate result")
