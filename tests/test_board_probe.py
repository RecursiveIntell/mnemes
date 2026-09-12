"""Executable containment tests for the disposable Bubblewrap probe."""
from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from candidate_validation.board_probe import run_probe


class BoardProbeTests(unittest.TestCase):
    def run_case(self, command, *, timeout: float = 5, output_limit: int = 4096):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            input_dir = root / "input"
            input_dir.mkdir()
            fixture = input_dir / "fixture.txt"
            fixture.write_text("immutable\n", encoding="utf-8")
            evidence = root / "evidence"
            result = run_probe(
                command,
                input_dir=input_dir,
                evidence_dir=evidence,
                timeout_seconds=timeout,
                max_output_bytes=output_limit,
            )
            self.assertEqual(fixture.read_text(encoding="utf-8"), "immutable\n")
            self.assertTrue(result["input_unchanged"])
            self.assertTrue(result["teardown_verified"])
            self.assertTrue((evidence / "probe.json").is_file())
            stdout = (evidence / "stdout.log").read_bytes()
            return result, stdout

    def test_read_only_input_rejects_mutation_and_preserves_fixture(self):
        result, _ = self.run_case(
            ["/bin/sh", "-c", "printf changed > /input/fixture.txt"],
        )
        self.assertEqual(result["outcome"], "exited")
        self.assertNotEqual(result["exit_code"], 0)

    def test_timeout_is_observed_and_teardown_is_verified(self):
        result, _ = self.run_case(
            ["/bin/sh", "-c", "sleep 30"],
            timeout=0.2,
        )
        self.assertEqual(result["outcome"], "timeout")

    def test_output_limit_is_not_reported_as_normal_exit(self):
        result, stdout = self.run_case(
            ["/bin/sh", "-c", "printf 1234567890"],
            output_limit=4,
        )
        self.assertEqual(result["outcome"], "output_limit")
        self.assertTrue(result["output_truncated"])
        self.assertLessEqual(len(stdout), 4)


if __name__ == "__main__":
    unittest.main()
