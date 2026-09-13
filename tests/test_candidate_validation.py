import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from candidate_validation.contract import ContractError, decide, manifest_sha256, validate_result
from candidate_validation.local_probe import run_local


MANIFEST = {
    "run_id": "candidate-1",
    "host_identity": "fixture-host",
    "stage_path": "/tmp/candidate-stage",
    "snapshot_manifest_sha256": "snapshot",
    "source_sha256": "source",
    "binary_sha256": "binary",
    "harness_sha256": "harness",
    "timeout_seconds": 1,
    "allowed_write_roots": ["/tmp/candidate-scratch"],
}


def result(**evidence):
    base = {
        "run_id": "candidate-1",
        "manifest_sha256": manifest_sha256(MANIFEST),
        "outcome": "passed",
        "receipt_sha256": "receipt",
        "evidence": {
            "required_files": ["result.json"],
            "teardown_verified": True,
            "input_integrity": True,
            "remote_exit_code": 0,
        },
    }
    base["evidence"].update(evidence)
    return base


class CandidateValidationTests(unittest.TestCase):
    def test_false_zero_without_receipt_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ContractError):
                run_local(MANIFEST, [sys.executable, "-c", "pass"], Path(directory) / "missing.json")

    def test_stale_receipt_is_rejected_before_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "result.json"
            receipt.write_text(json.dumps(result()), encoding="utf-8")
            with self.assertRaises(ContractError):
                run_local(MANIFEST, [sys.executable, "-c", "pass"], receipt)

    def test_pass_requires_receipt_and_teardown(self):
        accepted = decide(MANIFEST, result())
        self.assertTrue(accepted.accepted, accepted.reason)
        rejected = decide(MANIFEST, result(teardown_verified=False))
        self.assertFalse(rejected.accepted)
        self.assertIn("teardown", rejected.reason)

    def test_manifest_binding_rejects_changed_candidate(self):
        payload = result()
        changed = dict(MANIFEST, source_sha256="changed")
        with self.assertRaises(ContractError):
            validate_result(payload, changed)

    def test_nonzero_exit_cannot_be_promoted_to_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "result.json"
            payload = result()
            receipt.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(ContractError):
                run_local(
                    MANIFEST,
                    [sys.executable, "-c", "raise SystemExit(7)"],
                    receipt,
                )

    def test_timeout_is_contract_valid_cleanup_pending(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "result.json"
            observed = run_local(
                MANIFEST,
                [sys.executable, "-c", "import time; time.sleep(2)"],
                receipt,
                timeout_seconds=0.05,
            )
            self.assertEqual(observed["outcome"], "cleanup_pending")
            self.assertEqual(observed["evidence"]["required_files"], ["result.json"])
            self.assertTrue(observed["evidence"]["quarantine_path"])
            self.assertFalse(decide(MANIFEST, observed).accepted)

    def test_output_limit_is_rejected_with_group_teardown(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ContractError, "output limit"):
                run_local(
                    MANIFEST,
                    [sys.executable, "-c", "print('x' * 10000)"],
                    Path(directory) / "missing.json",
                    max_output_bytes=32,
                )

    def test_run_id_mismatch_is_rejected(self):
        payload = result()
        payload["run_id"] = "other"
        with self.assertRaises(ContractError):
            validate_result(payload, MANIFEST)


if __name__ == "__main__":
    unittest.main()
