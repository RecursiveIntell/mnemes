import hashlib
import importlib.util
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "device-shard-migrate.py"


def load_migration_module():
    spec = importlib.util.spec_from_file_location("device_shard_migrate", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_registry(path: Path, device_ids: list[str]) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE devices(device_id TEXT PRIMARY KEY)")
    conn.executemany("INSERT INTO devices(device_id) VALUES (?)", [(value,) for value in device_ids])
    conn.commit()
    conn.close()


def make_semantic(path: Path, schema: int, marker: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE _schema_version(version INTEGER PRIMARY KEY);
        CREATE TABLE facts(id TEXT PRIMARY KEY, content TEXT NOT NULL);
        CREATE TABLE search_receipts(receipt_id TEXT PRIMARY KEY, receipt_digest TEXT NOT NULL);
        """
    )
    conn.execute("INSERT INTO _schema_version(version) VALUES (?)", (schema,))
    conn.execute("INSERT INTO facts(id, content) VALUES ('fact-1', ?)", (marker,))
    conn.execute("INSERT INTO search_receipts VALUES ('receipt-1', 'sha256:test')")
    conn.commit()
    conn.close()


class DeviceShardMigrationTests(unittest.TestCase):
    def run_cli(self, *args: object) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), *map(str, args)],
            text=True,
            capture_output=True,
        )

    def test_stage_creates_one_verified_sqlite_store_per_registered_device(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            laptop = str(uuid.uuid4())
            msi = str(uuid.uuid4())
            registry = root / "pooled.db"
            make_registry(registry, [laptop, msi])
            laptop_db = root / "laptop.db"
            msi_db = root / "msi.db"
            make_semantic(laptop_db, 36, "PRIVATE-LAPTOP-CONTENT")
            make_semantic(msi_db, 36, "PRIVATE-MSI-CONTENT")
            out = root / "staged"

            result = self.run_cli(
                "stage",
                "--registry",
                registry,
                "--out",
                out,
                "--expected-schema",
                36,
                "--source",
                f"{laptop}={laptop_db}",
                "--source",
                f"{msi}={msi_db}",
            )
            self.assertEqual(result.returncode, 0, result.stderr)

            manifest = json.loads((out / "manifest.json").read_text())
            self.assertEqual(manifest["manifest_version"], "pooled-device-shards-stage-v2")
            self.assertEqual(set(manifest["devices"]), {laptop, msi})
            expected_registry_set_digest = hashlib.sha256(
                json.dumps(sorted([laptop, msi]), separators=(",", ":")).encode()
            ).hexdigest()
            self.assertEqual(
                manifest["registered_devices_sha256"], expected_registry_set_digest
            )
            serialized = json.dumps(manifest)
            self.assertNotIn("PRIVATE-LAPTOP-CONTENT", serialized)
            self.assertNotIn("PRIVATE-MSI-CONTENT", serialized)
            for device_id in (laptop, msi):
                target = out / "memory" / "shards" / device_id / "memory.db"
                self.assertTrue(target.is_file())
                conn = sqlite3.connect(target)
                self.assertEqual(conn.execute("PRAGMA quick_check").fetchone()[0], "ok")
                self.assertEqual(conn.execute("SELECT version FROM _schema_version").fetchone()[0], 36)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0], 1)
                conn.close()
                self.assertEqual(manifest["devices"][device_id]["table_counts"]["facts"], 1)

    def test_registry_membership_is_write_locked_through_staging(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            device = str(uuid.uuid4())
            registry = root / "pooled.db"
            make_registry(registry, [device])
            migration = load_migration_module()
            with migration.locked_registered_devices(registry) as registered:
                self.assertEqual(registered, {device})
                writer = sqlite3.connect(registry, timeout=0.05)
                with self.assertRaises(sqlite3.OperationalError):
                    writer.execute(
                        "INSERT INTO devices(device_id) VALUES (?)", (str(uuid.uuid4()),)
                    )
                writer.close()

            writer = sqlite3.connect(registry)
            writer.execute(
                "INSERT INTO devices(device_id) VALUES (?)", (str(uuid.uuid4()),)
            )
            writer.commit()
            writer.close()

    def test_unregistered_device_fails_closed_and_removes_partial_stage(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            registered = str(uuid.uuid4())
            unknown = str(uuid.uuid4())
            registry = root / "pooled.db"
            make_registry(registry, [registered])
            source = root / "source.db"
            make_semantic(source, 36, "secret")
            out = root / "staged"
            result = self.run_cli(
                "stage", "--registry", registry, "--out", out,
                "--expected-schema", 36, "--source", f"{unknown}={source}",
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("not registered", result.stderr)
            self.assertFalse(out.exists())

    def test_schema_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            device = str(uuid.uuid4())
            registry = root / "pooled.db"
            make_registry(registry, [device])
            source = root / "source.db"
            make_semantic(source, 35, "secret")
            out = root / "staged"
            result = self.run_cli(
                "stage", "--registry", registry, "--out", out,
                "--expected-schema", 36, "--source", f"{device}={source}",
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("schema generation", result.stderr)
            self.assertFalse(out.exists())

    def test_missing_registered_device_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            first = str(uuid.uuid4())
            missing = str(uuid.uuid4())
            registry = root / "pooled.db"
            make_registry(registry, [first, missing])
            source = root / "source.db"
            make_semantic(source, 36, "secret")
            out = root / "staged"
            result = self.run_cli(
                "stage", "--registry", registry, "--out", out,
                "--expected-schema", 36, "--source", f"{first}={source}",
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("missing registered", result.stderr)
            self.assertFalse(out.exists())

    def test_invalid_device_identifier_is_rejected_before_path_construction(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            registry = root / "pooled.db"
            make_registry(registry, [])
            source = root / "source.db"
            make_semantic(source, 36, "secret")
            result = self.run_cli(
                "stage", "--registry", registry, "--out", root / "staged",
                "--source", f"../escape={source}",
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("UUID", result.stderr)
            self.assertFalse((root / "escape").exists())

    def test_existing_output_is_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            device = str(uuid.uuid4())
            registry = root / "pooled.db"
            make_registry(registry, [device])
            source = root / "source.db"
            make_semantic(source, 36, "secret")
            out = root / "staged"
            out.mkdir()
            sentinel = out / "KEEP"
            sentinel.write_text("preserve")
            result = self.run_cli(
                "stage", "--registry", registry, "--out", out,
                "--source", f"{device}={source}",
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("already exists", result.stderr)
            self.assertEqual(sentinel.read_text(), "preserve")


if __name__ == "__main__":
    unittest.main()
