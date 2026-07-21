# Phase 4 operations

## Security boundary

The server binds only to `127.0.0.1`. Credentials live in owner-only `0600` env files and are never passed in argv or committed. Clients fail closed; there is no automatic local fallback.

## Install and bootstrap on msi

```bash
cd ~/Coding/mnemes
scripts/install-mneme-service.sh          # audit only
scripts/install-mneme-service.sh --apply
~/.local/bin/mnemes-admin bootstrap \
  ~/.local/share/mnemes msi-central linux msi service \
  > ~/.config/mnemes/operator-bootstrap.json
chmod 600 ~/.config/mnemes/operator-bootstrap.json
systemctl --user enable --now mnemes.service
curl http://127.0.0.1:1738/livez
```

Move the emitted credential into `~/.config/mnemes/client.env`, then securely delete the bootstrap JSON. Required client variables are `MNEME_URL`, `MNEME_CREDENTIAL`, `MNEME_ACTOR_ID`, and `MNEME_DEVICE_ID`.

## Laptop tunnel

Ports 1738–1741 are occupied on the laptop. The admitted local endpoint is 1748:

```bash
ssh -N -L 127.0.0.1:1748:127.0.0.1:1738 msi
```

Register a separate laptop device and actor through the Operator profile; do not copy the msi credential. Store the laptop credential in `~/.config/mnemes/client.env` mode 0600 with URL `http://127.0.0.1:1748`.

## Hermes/Codex MCP

Configure a stdio MCP server command pointing to `scripts/mneme-mcp-proxy.py`; the proxy reads the env file and forwards authenticated JSON-RPC. Use `scripts/mneme-codex-task.sh -- <codex exec args>` for provenance-witnessed tasks.

## Acceptance

```bash
scripts/mneme-client.py health
scripts/mneme-client.py tools-list
scripts/mneme-client.py witnessed-search 'current task context'
```

## Rotation and revocation

Use Operator MCP tools `sm_rotate_device_key`, `sm_revoke_device`, and `sm_register_device`. Replace the env file atomically after rotation and verify the old credential receives 401/403.

## Rollback

```bash
systemctl --user disable --now mnemes.service
rm ~/.config/systemd/user/mnemes.service
systemctl --user daemon-reload
```

Retain `~/.local/share/mnemes` and credentials for forensic rollback; do not delete data during service rollback.
