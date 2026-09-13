"""Bounded local process probe used by disposable candidate validation."""
from __future__ import annotations

import ctypes
import hashlib
import json
import os
import selectors
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Sequence

try:
    from .contract import ContractError, manifest_sha256, validate_manifest, validate_result
except ImportError:
    from contract import ContractError, manifest_sha256, validate_manifest, validate_result


_PR_SET_CHILD_SUBREAPER = 36
_subreaper_enabled = False


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _enable_child_subreaper() -> None:
    """Keep orphaned candidate descendants under this controller on Linux."""
    global _subreaper_enabled
    if _subreaper_enabled or sys.platform != "linux":
        return
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(_PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    _subreaper_enabled = True


def _proc_parent_map() -> dict[int, int]:
    if sys.platform != "linux":
        return {}
    parents = {}
    for raw_pid in os.listdir("/proc"):
        if not raw_pid.isdigit():
            continue
        try:
            pid = int(raw_pid)
            line = Path("/proc", raw_pid, "stat").read_text(encoding="ascii")
            after_command = line.rsplit(")", 1)[1].split()
            parents[pid] = int(after_command[1])
        except (OSError, ValueError, IndexError):
            continue
    return parents


def _descendant_pids(root_pid: int) -> set[int]:
    parents = _proc_parent_map()
    descendants = set()
    frontier = [root_pid]
    while frontier:
        parent = frontier.pop()
        for pid, candidate_parent in parents.items():
            if candidate_parent == parent and pid not in descendants:
                descendants.add(pid)
                frontier.append(pid)
    return descendants


def _group_exists(pid: int) -> bool:
    if not hasattr(os, "killpg"):
        return False
    try:
        os.killpg(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _kill_pids(pids: set[int]) -> None:
    for pid in sorted(pids, reverse=True):
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def _reap_children() -> None:
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
        except (ChildProcessError, OSError):
            return
        if pid == 0:
            return


def _terminate_group(child: subprocess.Popen, baseline_pids: set[int]) -> bool:
    """Terminate the group and descendants adopted by this subreaper."""
    targets = _descendant_pids(child.pid)
    if _subreaper_enabled:
        targets.update(_descendant_pids(os.getpid()) - baseline_pids - {child.pid})
    if hasattr(os, "killpg") and _group_exists(child.pid):
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    _kill_pids(targets)
    try:
        child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        return False

    for _ in range(20):
        adopted = _descendant_pids(os.getpid()) - baseline_pids - {child.pid}
        _kill_pids(adopted)
        _reap_children()
        if not adopted:
            break
        time.sleep(0.01)
    return not _group_exists(child.pid) and not (
        _descendant_pids(os.getpid()) - baseline_pids - {child.pid}
    )


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(raw)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


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
            result_path = evidence_dir / "result.json"
            result = None
            if result_path.exists():
                try:
                    result = json.loads(result_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    result = None
            if result is None:
                _atomic_write(result_path, transport.fetch(run_id, "result.json"))
                result = json.loads(result_path.read_text(encoding="utf-8"))

            for name in self._ARTIFACTS[1:]:
                target = evidence_dir / name
                expected = result.get("artifacts", {}).get(name)
                verified = False
                if target.exists() and expected:
                    try:
                        verified = _sha256_bytes(target.read_bytes()) == expected
                    except OSError:
                        verified = False
                if not verified:
                    _atomic_write(target, transport.fetch(run_id, name))
            artifacts_complete = True
            result = json.loads(result_path.read_text(encoding="utf-8"))
            if result.get("manifest_sha256") != manifest_sha256:
                state["state"] = "rejected"
                state["reason"] = "manifest mismatch"
            elif result.get("exit_code") != remote_exit:
                state["state"] = "rejected"
                state["reason"] = "remote exit mismatch"
            elif remote_exit == 0 or result.get("exit_code") == 0:
                state["state"] = "rejected"
                state["reason"] = "expected refusal exited successfully"
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
                        state["result_sha256"] = _sha256_bytes(result_path.read_bytes())
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

        _atomic_write(controller_path, (json.dumps(state, sort_keys=True) + "\n").encode())
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
    max_output_bytes: int = 1024 * 1024,
) -> dict:
    validate_manifest(manifest)
    if result_path.exists():
        raise ContractError("result path already exists; refusing stale receipt reuse")
    if type(max_output_bytes) is not int or not 0 < max_output_bytes <= 16 * 1024 * 1024:
        raise ContractError("max_output_bytes must be a positive bounded integer")
    timeout = timeout_seconds or float(manifest["timeout_seconds"])
    child = None
    baseline_pids = _descendant_pids(os.getpid())
    selector = selectors.DefaultSelector()
    captured = 0
    teardown_verified = False
    try:
        _enable_child_subreaper()
        child = subprocess.Popen(
            list(command),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            env={"PATH": os.environ.get("PATH", "")},
            start_new_session=True,
            close_fds=True,
        )
        for pipe in (child.stdout, child.stderr):
            assert pipe is not None
            os.set_blocking(pipe.fileno(), False)
            selector.register(pipe, selectors.EVENT_READ)
        deadline = time.monotonic() + timeout
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                teardown_verified = _terminate_group(child, baseline_pids)
                result = {
                    "run_id": manifest["run_id"],
                    "manifest_sha256": manifest_sha256(manifest),
                    "outcome": "cleanup_pending",
                    "receipt_sha256": "timeout-no-receipt",
                    "evidence": {
                        "required_files": ["result.json"],
                        "teardown_verified": teardown_verified,
                        "input_integrity": True,
                        "remote_exit_code": None,
                        "quarantine_path": str(result_path.parent / "quarantine"),
                        "timeout": timeout,
                    },
                }
                _atomic_write(result_path, (json.dumps(result, sort_keys=True) + "\n").encode())
                validate_result(result, manifest)
                return result
            for key, _ in selector.select(min(remaining, 0.05)):
                chunk = os.read(key.fd, 65536)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                captured += len(chunk)
                if captured > max_output_bytes:
                    teardown_verified = _terminate_group(child, baseline_pids)
                    raise ContractError(
                        f"process exceeded bounded output limit; teardown_verified={teardown_verified}"
                    )
        child.wait(timeout=max(0.01, deadline - time.monotonic()))
        teardown_verified = _terminate_group(child, baseline_pids)
    except subprocess.TimeoutExpired:
        teardown_verified = _terminate_group(child, baseline_pids) if child is not None else False
        result = {
            "run_id": manifest["run_id"],
            "manifest_sha256": manifest_sha256(manifest),
            "outcome": "cleanup_pending",
            "receipt_sha256": "timeout-no-receipt",
            "evidence": {
                "required_files": ["result.json"],
                "teardown_verified": teardown_verified,
                "input_integrity": True,
                "remote_exit_code": None,
                "quarantine_path": str(result_path.parent / "quarantine"),
                "timeout": timeout,
            },
        }
        _atomic_write(result_path, (json.dumps(result, sort_keys=True) + "\n").encode())
        validate_result(result, manifest)
        return result
    finally:
        selector.close()
        if child is not None:
            for pipe in (child.stdout, child.stderr):
                if pipe is not None:
                    pipe.close()

    if not result_path.exists():
        raise ContractError("process exited without a result receipt")
    result = json.loads(result_path.read_text(encoding="utf-8"))
    result.setdefault("evidence", {})["remote_exit_code"] = child.returncode
    result["evidence"]["teardown_verified"] = teardown_verified
    validate_result(result, manifest)
    return result
