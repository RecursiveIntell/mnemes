import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "benchmark-shard-routing.py"

spec = importlib.util.spec_from_file_location("shard_benchmark", SCRIPT)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)


class ShardBenchmarkTests(unittest.TestCase):
    def test_recall_uses_exhaustive_top_k_as_reference(self) -> None:
        self.assertEqual(module.recall_at_k(["a", "c"], ["a", "b"], 2), 0.5)
        self.assertEqual(module.recall_at_k(["a", "b"], ["a", "b"], 2), 1.0)
        self.assertEqual(module.recall_at_k([], [], 5), 1.0)

    def test_nearest_rank_percentile_is_deterministic(self) -> None:
        values = [1.0, 2.0, 3.0, 100.0]
        self.assertEqual(module.nearest_rank_percentile(values, 0.50), 2.0)
        self.assertEqual(module.nearest_rank_percentile(values, 0.95), 100.0)

    def test_receipt_completeness_requires_routing_evidence(self) -> None:
        complete = {
            "receipt_id": "r",
            "query_sha256": "0" * 64,
            "eligible_shards": ["a"],
            "ranked_shards": [{"device_id": "a", "score": 1}],
            "selected_shards": ["a"],
            "skipped_shards": [],
            "outcomes": [{"device_id": "a", "error": None}],
            "final_result_ids": ["fact:1"],
            "merge_digest": "1" * 64,
        }
        self.assertTrue(module.receipt_complete(complete))
        missing = dict(complete)
        missing.pop("outcomes")
        self.assertFalse(module.receipt_complete(missing))


if __name__ == "__main__":
    unittest.main()
