#!/usr/bin/env python3
"""Stage verified per-device semantic-memory SQLite shards.

This tool never mutates the pooled registry or an existing output. It uses
SQLite's online backup API, writes into a sibling temporary directory, verifies
each target, emits a content-free manifest, and atomically renames the complete
stage into place.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any


class MigrationError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def fsync_path(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def fsync_tree(root: Path) -> None:
    for path in sorted(root.rglob("*"), key=lambda value: len(value.parts), reverse=True):
        fsync_path(path)
    fsync_path(root)


def read_only(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)


def quick_check(conn: sqlite3.Connection, label: str) -> None:
    rows = [str(row[0]) for row in conn.execute("PRAGMA quick_check")]
    if rows != ["ok"]:
        raise MigrationError(f"{label} quick_check failed: {rows}")


def schema_generation(conn: sqlite3.Connection, label: str) -> int:
    try:
        row = conn.execute("SELECT MAX(version) FROM _schema_version").fetchone()
    except sqlite3.Error as error:
        raise MigrationError(f"{label} has no readable _schema_version: {error}") from error
    if row is None or row[0] is None:
        raise MigrationError(f"{label} has no schema generation")
    return int(row[0])


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    names = [
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    counts: dict[str, int] = {}
    for name in names:
        quoted = '"' + name.replace('"', '""') + '"'
        counts[name] = int(conn.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0])
    return counts


def parse_device_id(raw: str) -> str:
    try:
        parsed = uuid.UUID(raw)
    except ValueError as error:
        raise MigrationError(f"device identifier must be an RFC 4122 UUID v4: {raw}") from error
    canonical = str(parsed)
    if parsed.version != 4 or canonical != raw.lower():
        raise MigrationError(f"device identifier must be a canonical RFC 4122 UUID v4: {raw}")
    return canonical


def parse_sources(values: list[str]) -> dict[str, Path]:
    if not values:
        raise MigrationError("at least one --source DEVICE_UUID=DB_PATH is required")
    parsed: dict[str, Path] = {}
    for value in values:
        if "=" not in value:
            raise MigrationError("--source must be DEVICE_UUID=DB_PATH")
        raw_device, raw_path = value.split("=", 1)
        device_id = parse_device_id(raw_device)
        if device_id in parsed:
            raise MigrationError(f"duplicate source for device {device_id}")
        path = Path(raw_path).expanduser().resolve()
        if not path.is_file():
            raise MigrationError(f"source database does not exist: {path}")
        parsed[device_id] = path
    return parsed


@contextmanager
def locked_registered_devices(registry: Path):
    if not registry.is_file():
        raise MigrationError(f"pooled registry does not exist: {registry}")
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(registry, timeout=5.0, isolation_level=None)
        conn.execute("BEGIN IMMEDIATE")
        quick_check(conn, "pooled registry")
        yield {str(row[0]) for row in conn.execute("SELECT device_id FROM devices")}
    except sqlite3.Error as error:
        raise MigrationError(f"failed to read pooled registry: {error}") from error
    finally:
        if conn is not None:
            try:
                conn.rollback()
            finally:
                conn.close()


def registered_devices_digest(device_ids: set[str]) -> str:
    encoded = json.dumps(sorted(device_ids), separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def backup_database(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, mode=0o700)
    with read_only(source) as source_conn, sqlite3.connect(target) as target_conn:
        quick_check(source_conn, str(source))
        source_conn.backup(target_conn)
        target_conn.commit()
    os.chmod(target, 0o600)


def stage(args: argparse.Namespace) -> dict[str, Any]:
    out = Path(args.out).expanduser().resolve()
    if out.exists():
        raise MigrationError(f"output already exists; refusing overwrite: {out}")
    sources = parse_sources(args.source)
    registry = Path(args.registry).expanduser().resolve()
    with locked_registered_devices(registry) as registered:
        return stage_locked(args, out, sources, registry, registered)


def stage_locked(
    args: argparse.Namespace,
    out: Path,
    sources: dict[str, Path],
    registry: Path,
    registered: set[str],
) -> dict[str, Any]:
    unknown = sorted(set(sources) - registered)
    if unknown:
        raise MigrationError(f"device is not registered in pooled.db: {', '.join(unknown)}")
    missing = sorted(registered - set(sources))
    if missing:
        raise MigrationError(f"missing registered device source: {', '.join(missing)}")

    out.parent.mkdir(parents=True, exist_ok=True)
    temporary = out.parent / f".{out.name}.tmp-{uuid.uuid4()}"
    if temporary.exists():
        raise MigrationError(f"temporary stage unexpectedly exists: {temporary}")
    temporary.mkdir(mode=0o700)
    published = False
    try:
        devices: dict[str, Any] = {}
        for device_id, source in sorted(sources.items()):
            with read_only(source) as source_conn:
                quick_check(source_conn, str(source))
                source_schema = schema_generation(source_conn, str(source))
                if source_schema != args.expected_schema:
                    raise MigrationError(
                        f"{source} schema generation {source_schema} != expected {args.expected_schema}"
                    )
                source_counts = table_counts(source_conn)
            target = temporary / "memory" / "shards" / device_id / "memory.db"
            backup_database(source, target)
            with read_only(target) as target_conn:
                quick_check(target_conn, str(target))
                target_schema = schema_generation(target_conn, str(target))
                target_counts = table_counts(target_conn)
            if target_schema != source_schema or target_counts != source_counts:
                raise MigrationError(f"staged verification mismatch for device {device_id}")
            devices[device_id] = {
                "relative_path": f"memory/shards/{device_id}/memory.db",
                "source_path": str(source),
                "source_sha256": sha256_file(source),
                "target_sha256": sha256_file(target),
                "schema_generation": target_schema,
                "quick_check": "ok",
                "table_counts": target_counts,
            }

        manifest: dict[str, Any] = {
            "manifest_version": "pooled-device-shards-stage-v2",
            "registry_path": str(registry),
            "registry_sha256": sha256_file(registry),
            "registered_devices_sha256": registered_devices_digest(registered),
            "expected_schema": args.expected_schema,
            "devices": devices,
        }
        manifest_path = temporary / "manifest.json"
        with manifest_path.open("w") as manifest_file:
            manifest_file.write(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
            manifest_file.flush()
            os.fsync(manifest_file.fileno())
        os.chmod(manifest_path, 0o600)
        fsync_tree(temporary)
        temporary.rename(out)
        published = True
        fsync_path(out.parent)
        return manifest
    except BaseException:
        if published and out.exists():
            try:
                out.rename(temporary)
                fsync_path(out.parent)
            except OSError:
                pass
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    command = commands.add_parser("stage", help="stage verified device shard databases")
    command.add_argument("--registry", required=True)
    command.add_argument("--out", required=True)
    command.add_argument("--expected-schema", type=int, default=36)
    command.add_argument("--source", action="append", default=[])
    return root


def main() -> int:
    try:
        args = parser().parse_args()
        if args.command == "stage":
            manifest = stage(args)
            print(json.dumps(manifest, sort_keys=True))
            return 0
        raise MigrationError(f"unsupported command: {args.command}")
    except (MigrationError, sqlite3.Error, OSError) as error:
        print(f"device-shard migration failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
