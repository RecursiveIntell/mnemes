"""Bounded local process probe used by disposable candidate validation."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Sequence

try:
    from .contract import ContractError, validate_manifest, validate_result
except ImportError:  # Direct fixture loading uses a file-backed module name.
    from contract import ContractError, validate_manifest, validate_result


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class CandidateController:
    """Reconcile a fixed manifest/result artifact set without live transport."""

    _ARTIFACTS = ("result.json", "stdout.log", "stderr.log")

    def collect_run(
        self,
        *,
        run_id: str,
        manifest_sha256: str,
        remote_exit: int,
        transport,
        evidence_dir: Path,
    ) -> dict:
        evidence_dir = Path(evidence_dir)
        controller_path = evidence_dir / "controller.json"
        had_prior_controller = controller_path.exists()
        if controller_path.exists():
            existing = json.loads(controller_path.read_text(encoding="utf-8"))
            if existing.get("run_id") != run_id:
                raise ValueError("evidence directory belongs to another run")
            if existing.get("state") == "complete":
                return existing

        evidence_dir.mkdir(mode=0o700, parents=False, exist_ok=True)
        state = {
            "schema": "MnemesCandidateControllerV1",
            "run_id": run_id,
            "manifest_sha256": manifest_sha256,
            "accepted": False,
            "state": "evidence_pending",
        }
        artifacts_complete = False

        try:
            for name in self._ARTIFACTS:
                target = evidence_dir / name
                if not target.exists():
                    target.write_bytes(transport.fetch(run_id, name))
            artifacts_complete = True
            result = json.loads((evidence_dir / "result.json").read_text(encoding="utf-8"))
            if result.get("manifest_sha256") != manifest_sha256:
                state["state"] = "rejected"
                state["reason"] = "manifest mismatch"
            elif result.get("exit_code") != remote_exit:
                state["state"] = "rejected"
                state["reason"] = "remote exit mismatch"
            elif result.get("outcome") != "expected_refusal":
                state["state"] = "rejected"
                state["reason"] = "unexpected remote outcome"
            elif result.get("oracle_matched") is not True:
                state["state"] = "rejected"
                state["reason"] = "oracle did not match"
            elif result.get("teardown_verified") is not True or result.get("input_unchanged") is not True:
                state["state"] = "rejected"
                state["reason"] = "containment evidence incomplete"
            else:
                for name in ("stdout.log", "stderr.log"):
                    expected = result.get("artifacts", {}).get(name)
                    if expected != _sha256_bytes((evidence_dir / name).read_bytes()):
                        state["state"] = "rejected"
                        state["reason"] = f"artifact digest mismatch: {name}"
                        break
                else:
                    cleanup = transport.cleanup(run_id, manifest_sha256)
                    if cleanup.get("stage_absent") is not True or cleanup.get("teardown_verified") is not True:
                        state["state"] = "cleanup_pending"
                        state["reason"] = "cleanup not verified"
                    else:
                        state["accepted"] = True
                        state["state"] = "complete"
                        state["result_sha256"] = _sha256_bytes((evidence_dir / "result.json").read_bytes())
                        if had_prior_controller:
                            attempts = evidence_dir / "attempts"
                            attempts.mkdir(mode=0o700, exist_ok=True)
                            (attempts / "1.json").write_text(
                                json.dumps(state, sort_keys=True) + "\n", encoding="utf-8"
                            )
        except ConnectionError as error:
            state["state"] = "cleanup_pending" if artifacts_complete else "evidence_pending"
            state["reason"] = type(error).__name__
        except (OSError, json.JSONDecodeError, KeyError) as error:
            state["state"] = "evidence_pending"
            state["reason"] = type(error).__name__

        controller_path.write_text(json.dumps(state, sort_keys=True) + "\n", encoding="utf-8")
        return state


def collect_run(**kwargs) -> dict:
    """Compatibility entry point for the controller fixture and callers."""
    return CandidateController().collect_run(**kwargs)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_local(
    manifest: dict,
    command: Sequence[str],
    result_path: Path,
    *,
    timeout_seconds: float | None = None,
) -> dict:
    validate_manifest(manifest)
    timeout = timeout_seconds or float(manifest["timeout_seconds"])
    try:
        completed = subprocess.run(
            list(command),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={"PATH": os.environ.get("PATH", "")},
        )
    except subprocess.TimeoutExpired as error:
        result = {
            "run_id": manifest["run_id"],
            "outcome": "cleanup_pending",
            "receipt_sha256": "timeout-no-receipt",
            "evidence": {
                "required_files": [],
                "teardown_verified": False,
                "input_integrity": True,
                "remote_exit_code": None,
                "quarantine_path": str(result_path.parent / "quarantine"),
                "timeout": str(error.timeout),
            },
        }
        result_path.write_text(json.dumps(result, sort_keys=True) + "\n")
        return result

    if not result_path.exists():
        raise ContractError("process exited without a result receipt")
    result = json.loads(result_path.read_text())
    result.setdefault("evidence", {})["remote_exit_code"] = completed.returncode
    validate_result(result, manifest)
    return result
