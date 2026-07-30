#!/usr/bin/env bash
# Verify source-anchor files still exist before dispatching a worker.
# Usage: bash anchor-existence-check.sh /abs/Libraries /abs/mnemes-replication
set -euo pipefail
if [[ $# -ne 2 ]]; then
  printf 'usage: %s /abs/Libraries /abs/mnemes-replication\n' "$0" >&2
  exit 64
fi
libraries=$(realpath "$1")
mnemes=$(realpath "$2")

libraries_paths=(
  semantic-memory/src/lib.rs semantic-memory/src/db.rs semantic-memory/src/journal.rs
  semantic-memory/src/knowledge.rs semantic-memory/src/documents.rs semantic-memory/src/conversation.rs
  semantic-memory/src/episodes.rs semantic-memory/src/graph.rs semantic-memory/src/graph_edges.rs
  semantic-memory/src/projection_import.rs semantic-memory/src/provenance.rs semantic-memory/src/forgetting.rs
  semantic-memory/src/search.rs claim-ledger/src/types.rs claim-ledger/src/ledger.rs claim-ledger/src/envelope.rs
  semantic-memory-mcp/src/server_stable.rs
)
mnemes_paths=(
  src/replication/mod.rs src/replication/types.rs src/replication/canonical.rs src/replication/error.rs
  src/replication/fact_create.rs src/replication/state_machine.rs src/server.rs src/store.rs src/shards.rs
  src/sync.rs src/sync_handler.rs src/replica.rs tests/replication_protocol.rs tests/replication_fact_create_wire.rs
  tests/device_shards.rs tests/server.rs tests/admin_cli.rs docs/DEVICE_SHARDED_MEMORY.md
  docs/adr/ADR-0001-historical-bootstrap-and-live-epoch-rotation-v1.md
)
failed=0
for rel in "${libraries_paths[@]}"; do
  if [[ -e "$libraries/$rel" ]]; then printf 'OK libraries/%s\n' "$rel"; else printf 'MISSING libraries/%s\n' "$rel" >&2; failed=1; fi
done
for rel in "${mnemes_paths[@]}"; do
  if [[ -e "$mnemes/$rel" ]]; then printf 'OK mnemes/%s\n' "$rel"; else printf 'MISSING mnemes/%s\n' "$rel" >&2; failed=1; fi
done
[[ $failed -eq 0 ]] || exit 65
printf 'ANCHOR CHECK PASS\n'
