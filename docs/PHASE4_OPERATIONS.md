# Phase 4 operations

## Security boundary

The server binds only to `127.0.0.1`. Credentials live in owner-only `0600` env files and are never passed in argv or committed. Clients fail closed; there is no automatic local fallback.

## Install and bootstrap on msi

```bash
cd ~/Coding/pooled-memory
scripts/install-pooled-memory-service.sh          # audit only
scripts/install-pooled-memory-service.sh --apply
~/.local/bin/pooled-memory-admin bootstrap \
  ~/.local/share/pooled-memory msi-central linux msi service \
  > ~/.config/pooled-memory/operator-bootstrap.json
chmod 600 ~/.config/pooled-memory/operator-bootstrap.json
systemctl --user enable --now pooled-memory.service
curl http://127.0.0.1:1738/livez
```

Move the emitted credential into `~/.config/pooled-memory/client.env`, then securely delete the bootstrap JSON. Required client variables are `POOLED_MEMORY_URL`, `POOLED_MEMORY_CREDENTIAL`, `POOLED_MEMORY_ACTOR_ID`, and `POOLED_MEMORY_DEVICE_ID`.

## Laptop tunnel

Ports 1738–1741 are occupied on the laptop. The admitted local endpoint is 1748:

```bash
ssh -N -L 127.0.0.1:1748:127.0.0.1:1738 msi
```

Register a separate laptop device and actor through the Operator profile; do not copy the msi credential. Store the laptop credential in `~/.config/pooled-memory/client.env` mode 0600 with URL `http://127.0.0.1:1748`.

## Hermes/Codex MCP

Configure a stdio MCP server command pointing to `scripts/pooled-memory-mcp-proxy.py`; the proxy reads the env file and forwards authenticated JSON-RPC. Use `scripts/pooled-codex-task.sh -- <codex exec args>` for provenance-witnessed tasks.

## Acceptance

```bash
scripts/pooled-memory-client.py health
scripts/pooled-memory-client.py tools-list
scripts/pooled-memory-client.py witnessed-search 'current task context'
```

## Rotation and revocation

Use Operator MCP tools `sm_rotate_device_key`, `sm_revoke_device`, and `sm_register_device`. Replace the env file atomically after rotation and verify the old credential receives 401/403.

## Rollback

```bash
systemctl --user disable --now pooled-memory.service
rm ~/.config/systemd/user/pooled-memory.service
systemctl --user daemon-reload
```

Retain `~/.local/share/pooled-memory` and credentials for forensic rollback; do not delete data during service rollback.
