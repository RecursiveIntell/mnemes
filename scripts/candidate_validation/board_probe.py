"""Offline candidate probes inside Bubblewrap's private PID/mount/net namespaces.

Input is a read-only bind mount. Scratch is private tmpfs. Neither live user
homes nor controller evidence are mounted. Bubblewrap's PID-namespace reaper
owns descendants even when a child calls setsid; a process group alone does not.
All effects here are disposable probes, not deployment or memory authority.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import time


BWRAP = "/usr/bin/bwrap"
ENVIRONMENT = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "TZ": "UTC"}


def digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def input_inventory(root):
    root = Path(root).absolute()
    for parent in [root, *root.parents]:
        if parent.is_symlink():
            raise ValueError("symlinked_input_root")
    if not root.is_dir():
        raise ValueError("missing_input_root")
    rows = []
    identities = set()
    for path in [root, *sorted(root.rglob("*"))]:
        value = path.lstat()
        if not (stat.S_ISDIR(value.st_mode) or stat.S_ISREG(value.st_mode)):
            raise ValueError("nonregular_input")
        identity = value.st_dev, value.st_ino
        if identity in identities:
            raise ValueError("aliased_input")
        identities.add(identity)
        row = {
            "path": str(path.relative_to(root)),
            "mode": stat.S_IMODE(value.st_mode),
            "device": value.st_dev, "inode": value.st_ino,
            "mtime_ns": value.st_mtime_ns, "size": value.st_size,
        }
        if path.is_file():
            row["sha256"] = digest(path)
        rows.append(row)
    return rows


def group_exists(pid):
    try:
        os.killpg(pid, 0)
        return True
    except ProcessLookupError:
        return False


def terminate(child):
    """Kill only our supervisor group; PID-namespace init exit kills its children."""
    if group_exists(child.pid):
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    child.wait(timeout=5)
    return not group_exists(child.pid)


def save_result(path, result):
    with path.open("x", encoding="utf-8") as handle:
        os.chmod(path, 0o600)
        json.dump(result, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def run_probe(command, *, input_dir, evidence_dir, timeout_seconds, max_output_bytes):
    """Return a bounded observation, never interpret an arbitrary exit as success.

Evidence directories must be new controller-owned siblings/outside the input.
No cleanup of input or evidence is automatic: verified retrieval/disposition is
owned by the controller, especially on connection loss.
"""
    input_dir = Path(input_dir).absolute()
    evidence_dir = Path(evidence_dir).absolute()
    for parent in [evidence_dir, *evidence_dir.parents]:
        if parent.is_symlink():
            raise ValueError("symlinked_evidence_root")
    if evidence_dir == input_dir or input_dir in evidence_dir.parents:
        raise ValueError("evidence_inside_input")
    evidence_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
    result = {
        "schema": "MnemesSandboxProbeV1", "outcome": "prelaunch_failure",
        "pid": None, "exit_code": None, "teardown_verified": False,
        "input_unchanged": False, "output_truncated": False,
        "started_ns": time.time_ns(), "command": command,
        "environment": dict(ENVIRONMENT),
        "isolation": "bubblewrap-private-pid-mount-network",
    }
    child = None
    streams = {}
    selector = selectors.DefaultSelector()
    try:
        if not isinstance(command, list) or not command or any(not isinstance(x, str) or "\0" in x for x in command):
            raise ValueError("invalid_command")
        if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 120:
            raise ValueError("invalid_timeout")
        if type(max_output_bytes) is not int or not 0 < max_output_bytes <= 1048576:
            raise ValueError("invalid_output_limit")
        result["input_before"] = input_inventory(input_dir)
        args = [
            BWRAP, "--unshare-all", "--die-with-parent", "--new-session",
            "--cap-drop", "ALL", "--ro-bind", "/usr", "/usr",
            "--symlink", "usr/bin", "/bin", "--symlink", "usr/lib", "/lib",
            "--symlink", "usr/lib64", "/lib64", "--proc", "/proc",
            "--dev", "/dev", "--tmpfs", "/tmp", "--tmpfs", "/work",
            "--ro-bind", str(input_dir), "/input", "--chdir", "/work",
            "--", *command,
        ]
        result["supervisor_sha256"] = digest(Path(BWRAP))
        result["supervisor_argv"] = args
        for name in ("stdout", "stderr"):
            path = evidence_dir / f"{name}.log"
            streams[name] = path.open("xb")
            os.chmod(path, 0o600)
        child = subprocess.Popen(
            args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=ENVIRONMENT, cwd=evidence_dir,
            start_new_session=True, close_fds=True,
        )
        result["pid"] = child.pid
        result["pgid"] = child.pid
        result["sid"] = child.pid
        for pipe, name in ((child.stdout, "stdout"), (child.stderr, "stderr")):
            assert pipe is not None
            os.set_blocking(pipe.fileno(), False)
            selector.register(pipe, selectors.EVENT_READ, name)
        deadline = time.monotonic() + timeout_seconds
        captured = 0
        result["outcome"] = "exited"
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                result["outcome"] = "timeout"
                break
            overflow = False
            for key, _ in selector.select(min(remaining, 0.05)):
                chunk = os.read(key.fd, 65536)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                available = max_output_bytes - captured
                streams[key.data].write(chunk[:available])
                captured += min(available, len(chunk))
                if len(chunk) > available:
                    result["outcome"] = "output_limit"
                    result["output_truncated"] = True
                    overflow = True
                    break
            if overflow:
                break
        if result["outcome"] == "exited":
            try:
                child.wait(timeout=max(0.01, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                result["outcome"] = "timeout"
    except Exception as error:
        result["outcome"] = "probe_failure" if child is not None else "prelaunch_failure"
        result["error_type"] = type(error).__name__
        result["error"] = str(error)
    finally:
        if child is not None:
            try:
                result["teardown_verified"] = terminate(child)
                result["exit_code"] = child.returncode
            except (OSError, subprocess.TimeoutExpired) as error:
                result["outcome"] = "cleanup_pending"
                result["cleanup_error"] = type(error).__name__
        selector.close()
        if child is not None:
            for pipe in (child.stdout, child.stderr):
                if pipe is not None:
                    pipe.close()
        for stream in streams.values():
            try:
                stream.flush()
                os.fsync(stream.fileno())
            finally:
                stream.close()
        try:
            result["input_after"] = input_inventory(input_dir)
            result["input_unchanged"] = result.get("input_before") == result["input_after"]
        except (OSError, ValueError):
            result["input_unchanged"] = False
        result["artifacts"] = {f"{name}.log": digest(evidence_dir / f"{name}.log") for name in streams}
        result["ended_ns"] = time.time_ns()
        save_result(evidence_dir / "probe.json", result)
    return result
