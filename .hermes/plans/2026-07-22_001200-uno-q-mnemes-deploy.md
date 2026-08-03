# UNO Q Mnemes Server Deployment Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Deploy mnemes-server on the Arduino UNO Q (192.168.50.249, port 2222) using Ollama embeddings (nomic-embed-text, already pulled and benchmarked at ~270ms/embed warm).

**Architecture:** UNO Q runs mnemes-server with `MNEMES_EMBEDDER=ollama`. The 1009-fact shard from MSI is migrated via SQLite `.backup`. Durability via crontab `@reboot` (no systemd user bus on UNO Q via ADB).

**Tech Stack:** Rust cross-compile for aarch64-unknown-linux-gnu, mnemes v0.1.1, Ollama nomic-embed-text 768d, SQLite WAL backup

---

## Phase 0: Cross-compile mnemes-server for aarch64

### Task 0.1: Add aarch64 target and cross-compile

**Objective:** Build mnemes-server binary for ARM64 without Candle (Ollama only).

**Step 1:** Add the target
```bash
rustup target add aarch64-unknown-linux-gnu
```

**Step 2:** Build without candle-local (Ollama only)
```bash
cd /home/sikmindz/Coding/mnemes
cargo build --release --target aarch64-unknown-linux-gnu --no-default-features --features server --bin mnemes-server
```

**Step 3:** Verify binary architecture
```bash
file target/aarch64-unknown-linux-gnu/release/mnemes-server
# Expected: ELF 64-bit LSB executable, ARM aarch64
```

**Fallback:** If cross-compile fails (linker issues), install Rust on UNO Q via rustup and build natively.

### Task 0.2: Cross-compile mnemes-admin

Same as above but for the admin binary:
```bash
cargo build --release --target aarch64-unknown-linux-gnu --no-default-features --features server --bin mnemes-admin
```

---

## Phase 1: Deploy to UNO Q

### Task 1.1: Transfer binaries

```bash
scp -P 2222 target/aarch64-unknown-linux-gnu/release/mnemes-server arduino@192.168.50.249:~/mnemes-server
scp -P 2222 target/aarch64-unknown-linux-gnu/release/mnemes-admin arduino@192.168.50.249:~/mnemes-admin
ssh -p 2222 arduino@192.168.50.249 'chmod +x ~/mnemes-server ~/mnemes-admin'
```

### Task 1.2: Bootstrap the store

```bash
ssh -p 2222 arduino@192.168.50.249 '~/mnemes-admin bootstrap ~/.local/share/mnemes "uno-q" "linux" "192.168.50.249"'
```

Save the output (device_id, actor_id, credential).

### Task 1.3: Configure environment

```bash
ssh -p 2222 arduino@192.168.50.249 'mkdir -p ~/.config/mnemes && cat > ~/.config/mnemes/server.env << "EOF"
MNEMES_PORT=1738
MNEMES_DATA_DIR=/home/arduino/.local/share/mnemes
MNEMES_EMBEDDER=ollama
MNEMES_OLLAMA_URL=http://127.0.0.1:11434
MNEMES_EMBEDDING_MODEL=nomic-embed-text
MNEMES_EMBEDDING_DIMENSIONS=768
EOF'
```

### Task 1.4: Start and verify

```bash
ssh -p 2222 arduino@192.168.50.249 'set -a; source ~/.config/mnemes/server.env; set +a; ~/mnemes-server $MNEMES_PORT $MNEMES_DATA_DIR &
sleep 3
curl -s http://127.0.0.1:1738/v1/health'
```

### Task 1.5: Set up crontab durability

```bash
ssh -p 2222 arduino@192.168.50.249 '(crontab -l 2>/dev/null | grep -v mnemes-server; echo "@reboot source ~/.config/mnemes/server.env && ~/mnemes-server \$MNEMES_PORT \$MNEMES_DATA_DIR >> ~/.local/share/mnemes/server.log 2>&1") | crontab -'
```

---

## Phase 2: Migrate the 1009-fact shard from MSI

### Task 2.1: Backup the shard on MSI

```bash
ssh msi 'sqlite3 ~/.local/share/mnemes/memory/shards/bb18a9fd-f73b-4e6e-935c-ce147706c18b/memory.db ".backup /tmp/uno-q-migrate.db"'
scp msi:/tmp/uno-q-migrate.db /tmp/uno-q-migrate.db
```

### Task 2.2: Transfer and place on UNO Q

```bash
# The shard path on UNO Q: ~/.local/share/mnemes/memory/shards/<device_uuid>/memory.db
# The device UUID from bootstrap will be different, so we place it under the bootstrap device's shard
scp -P 2222 /tmp/uno-q-migrate.db arduino@192.168.50.249:/tmp/uno-q-migrate.db

# Get the bootstrap device ID and place the DB
ssh -p 2222 arduino@192.168.50.249 'DEVICE_ID=$(~/mnemes-admin bootstrap --list ~/.local/share/mnemes 2>/dev/null | head -1 | jq -r .device_id 2>/dev/null || true)
# Actually just use sqlite3 to read the device_id from pooled.db
DEVICE_ID=$(sqlite3 ~/.local/share/mnemes/pooled.db "SELECT device_id FROM devices LIMIT 1;" 2>/dev/null)
echo "Device ID: $DEVICE_ID"
mkdir -p ~/.local/share/mnemes/memory/shards/$DEVICE_ID
cp /tmp/uno-q-migrate.db ~/.local/share/mnemes/memory/shards/$DEVICE_ID/memory.db'
```

### Task 2.3: Refresh shard metadata

```bash
# Update device_shards.fact_count to reflect the migrated data
ssh -p 2222 arduino@192.168.50.249 'sqlite3 ~/.local/share/mnemes/pooled.db "UPDATE device_shards SET fact_count=(SELECT COUNT(*) FROM facts) WHERE device_id=(SELECT device_id FROM devices LIMIT 1);" 2>/dev/null || true'
```

### Task 2.4: Verify search returns results

```bash
# Register a device credential, then search
ssh -p 2222 arduino@192.168.50.249 'curl -s -X POST http://127.0.0.1:1738/v1/search/witnessed -H "Authorization: Bearer <credential>" -H "Content-Type: application/json" -d "{\"query\":\"semantic memory\",\"limit\":5}"'
```

---

## Phase 3: Register UNO Q as a device on MSI (optional cross-device)

### Task 3.1: Register UNO Q on MSI mnemes server

```bash
ssh msi 'curl -s -X POST http://127.0.0.1:1738/v1/devices/register -H "Authorization: Bearer <msi-operator-credential>" -H "Content-Type: application/json" -d "{\"label\":\"uno-q\",\"platform\":\"linux\",\"hostname\":\"192.168.50.249\"}"'
```

### Task 3.2: Verify cross-device search on MSI

Search on MSI should now include the UNO Q shard in routing.

---

## Verification Gauntlet

- [ ] mnemes-server binary is aarch64 ELF
- [ ] UNO Q mnemes-server starts and `/v1/health` returns OK
- [ ] Crontab @reboot entry exists
- [ ] 1009-fact shard migrated and `fact_count` updated
- [ ] Authenticated search on UNO Q returns non-empty results
- [ ] Embedding latency < 500ms per query
- [ ] MSI cross-device registration (if done) shows UNO Q in device list

## Hard No List

- Do NOT use `cp` for live SQLite — use `sqlite3 .backup`
- Do NOT install Rust on UNO Q unless cross-compile fails
- Do NOT use systemd on UNO Q (user bus unavailable via ADB)
- Do NOT forget to set MNEMES_EMBEDDER=ollama (Candle not available on UNO Q)