"""Controller disconnect/retrieval tests; transport doubles do not contact hosts."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


def load(case):
    path = ROOT / "scripts" / "candidate_validation" / "local_probe.py"
    case.assertTrue(path.is_file(), "missing controller collection boundary")
    sys.path.insert(0, str(path.parent))
    case.addCleanup(sys.path.remove, str(path.parent))
    spec = importlib.util.spec_from_file_location("candidate_local_probe", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class Transport:
    def __init__(self):
        self.cleanup_calls = []
        self.fetch_calls = []
        self.manifest = "a" * 64
        self.run_id = "disposable-test"
        self.remote_exit = 1
        self.disconnect: str | None = None
        self.cleanup_fails = False
        self.logs = {"stdout.log": b"", "stderr.log": b"expected-error"}
        self.result = {
            "schema": "MnemesCandidateResultV1", "manifest_sha256": self.manifest,
            "outcome": "expected_refusal", "exit_code": 1,
            "oracle_matched": True, "teardown_verified": True,
            "input_unchanged": True, "output_truncated": False,
            "artifacts": {k: hashlib.sha256(v).hexdigest() for k, v in self.logs.items()},
        }

    def fetch(self, run_id, name):
        assert run_id == self.run_id
        self.fetch_calls.append(name)
        if self.disconnect == name:
            raise ConnectionError("fixture_disconnected")
        if name == "result.json":
            return json.dumps(self.result).encode()
        return self.logs[name]

    def cleanup(self, run_id, manifest_sha256):
        assert run_id == self.run_id and manifest_sha256 == self.manifest
        self.cleanup_calls.append((run_id, manifest_sha256))
        if self.cleanup_fails:
            raise ConnectionError("fixture_cleanup_unreachable")
        return {"stage_absent": True, "teardown_verified": True}


class ControllerTests(unittest.TestCase):
    def collect(self, driver, transport, directory):
        return driver.collect_run(
            run_id=transport.run_id, manifest_sha256=transport.manifest,
            remote_exit=transport.remote_exit, transport=transport,
            evidence_dir=directory,
        )

    def test_success_requires_all_evidence_then_scoped_cleanup(self):
        driver = load(self)
        with tempfile.TemporaryDirectory() as raw:
            target = Path(raw) / "evidence"; transport = Transport()
            result = self.collect(driver, transport, target)
            self.assertTrue(result["accepted"])
            self.assertEqual(result["state"], "complete")
            self.assertEqual(len(transport.cleanup_calls), 1)
            for name in ("result.json", "stdout.log", "stderr.log", "controller.json"):
                self.assertTrue((target / name).is_file())
            calls_before = list(transport.fetch_calls)
            repeated = self.collect(driver, transport, target)
            self.assertEqual(repeated, result)
            self.assertEqual(transport.fetch_calls, calls_before)
            self.assertEqual(len(transport.cleanup_calls), 1)

    def test_transfer_failure_preserves_evidence_and_quarantine(self):
        driver = load(self)
        with tempfile.TemporaryDirectory() as raw:
            target = Path(raw) / "evidence"; transport = Transport(); transport.disconnect = "stderr.log"
            result = self.collect(driver, transport, target)
            self.assertFalse(result["accepted"])
            self.assertEqual(result["state"], "evidence_pending")
            self.assertEqual(transport.cleanup_calls, [])
            self.assertTrue((target / "result.json").is_file())
            self.assertTrue((target / "controller.json").is_file())
            transport.disconnect = None
            reconciled = self.collect(driver, transport, target)
            self.assertTrue(reconciled["accepted"])
            self.assertTrue((target / "attempts" / "1.json").is_file())

    def test_remote_failure_cannot_become_controller_success(self):
        driver = load(self)
        for field, value in [("outcome", "prelaunch_failure"), ("teardown_verified", False), ("manifest_sha256", "b"*64)]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as raw:
                transport = Transport(); transport.result[field] = value
                result = self.collect(driver, transport, Path(raw) / "evidence")
                self.assertFalse(result["accepted"])
                self.assertEqual(result["state"], "rejected")
                self.assertEqual(transport.cleanup_calls, [])

    def test_cleanup_disconnect_is_pending_not_success(self):
        driver = load(self)
        with tempfile.TemporaryDirectory() as raw:
            transport = Transport(); transport.cleanup_fails = True
            target = Path(raw) / "evidence"
            result = self.collect(driver, transport, target)
            self.assertFalse(result["accepted"])
            self.assertEqual(result["state"], "cleanup_pending")
            self.assertTrue((target / "stderr.log").is_file())
            transport.cleanup_fails = False
            result = self.collect(driver, transport, target)
            self.assertTrue(result["accepted"])

    def test_result_substitution_or_corrupt_artifact_never_deletes_stage(self):
        driver = load(self)
        with tempfile.TemporaryDirectory() as raw:
            transport = Transport(); transport.logs["stderr.log"] = b"corrupt"
            result = self.collect(driver, transport, Path(raw) / "evidence")
            self.assertFalse(result["accepted"])
            self.assertEqual(transport.cleanup_calls, [])

    def test_new_run_cannot_reuse_other_run_evidence_directory(self):
        driver = load(self)
        with tempfile.TemporaryDirectory() as raw:
            transport = Transport(); target = Path(raw) / "evidence"
            self.assertTrue(self.collect(driver, transport, target)["accepted"])
            transport.run_id = "different"
            with self.assertRaises(ValueError):
                self.collect(driver, transport, target)
            self.assertEqual(len(transport.cleanup_calls), 1)


if __name__ == "__main__":
    unittest.main()
