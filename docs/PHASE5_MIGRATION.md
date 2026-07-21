# Phase 5 migration runbook

## Authority decision

The laptop canonical semantic-memory store is the primary generation. Content-free row manifests proved it is a strict superset of the `msi` legacy store for facts, documents, chunks, graph edges, operation history, authority versions/receipts/lineages, origin labels, and replay inputs. `msi` contributes only two unique durable search receipts.

Singleton projections are adjudicated in favor of the laptop because its retrieval epoch and routing-policy timestamp are newer. Both source rows and their digests remain in the reconciliation ledger.

## Tool boundary

`scripts/phase5-migrate.py` is an offline migration verifier, not a runtime store owner. It never writes a live database path and has no install command.

Commands:

```bash
phase5-migrate.py snapshot --db SOURCE/memory.db --out envelope.db --source DEVICE
phase5-migrate.py reconcile --primary laptop.db --secondary msi.db --out merged.db --ledger reconciliation.json
phase5-migrate.py verify --db merged.db --manifest merged.db.manifest.json
```

Each snapshot uses SQLite's backup API and emits a `0600` manifest containing the DB SHA-256, canonical row-root SHA-256, schema metadata, table counts, source identity, and quick-check result.

Reconciliation fails closed if:

- source schemas differ;
- either source fails `PRAGMA quick_check`;
- a same-ID row differs outside the admitted singleton projection tables;
- any secondary-only row exists outside `search_receipts`;
- the merged database fails quick-check;
- an output path already exists.

## Cutover gates

1. Seal exact source generation and rollback archives.
2. Build/test/clippy mnemes against canonical semantic-memory schema 36.
3. Stop the laptop semantic-memory owner and verify no writer holds its DB.
4. Stop mnemes and verify no writer holds the central data directory.
5. Create final source snapshots; never copy SQLite/WAL files directly.
6. Reconcile and verify the final merged manifest.
7. Archive the complete pre-cutover mnemes data directory.
8. Install the schema-36 release binaries atomically.
9. Replace only the central semantic-memory projection directory from the verified merged envelope.
10. Restart mnemes and require authenticated integrity, stats, witnessed retrieval, Hermes MCP discovery, Codex wrapper, and source/target row-manifest parity.
11. Restart laptop semantic-memory in shadow-read mode until parity is accepted; do not delete legacy stores.

## Rollback

Stop mnemes, atomically move the failed data directory to quarantine, restore the complete pre-cutover archive, restore the previous server/admin binaries, restart, and verify authenticated integrity. Source stores and signed migration envelopes remain untouched.

Source parity rollback restores the dated `msi-libraries-pre-sync.tar.gz` archive. The sealed schema-36 owner source archive is the reproducible build witness for the migrated generation.
