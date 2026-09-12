"""Bounded local process probe used by disposable candidate validation."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Sequence

from .contract import ContractError, validate_manifest, validate_result


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
