# UNO Q Deployment Plan — Mnemes Server on Edge

> **Status:** implementation-ready plan
> **Target:** UNO Q (Qualcomm Kryo-V2, 4×A53 @ 2.0GHz, 4GB RAM, custom OS)
> **Current:** MSI (transitional host, Nobara/Fedora 44, GTX 1070)
> **Date:** 2026-07-21

## 1. Architecture

The UNO Q becomes the always-on mnemes-server — the authoritative routing brain for the multi-device memory mesh. Devices (laptop, MSI, phones) connect to it as clients.

```
                    ┌─────────────────────────────┐
                    │   UNO Q (4GB, A53×4 @2GHz)   │
                    │   Custom OS                  │
                    │                              │
                    │   mnemes-server :1738        │
                    │   ├── pooled.db (metadata)   │
                    │   ├── memory/shards/<uuid>/  │
                    │   │   └── memory.db (repl)   │
                    │   └── replicas/<store>.db    │
                    │                              │
                    │   hermes-infer C engine      │
                    │   ├── Qwen3.5-0.8B (LLM)     │
                    │   └── nomic-embed-text-v1.5  │
                    │       (embedding, 256d trunc)│
                    └──────────┬──────────────────┘
                               │
                    ┌──────────┴──────────────────┐
                    │                             │
              ┌─────┴─────┐               ┌───────┴───────┐
              │  Laptop   │               │     MSI       │
              │  (client) │               │  (GPU client) │
              │  768d emb │               │  768d emb     │
              └───────────┘               └───────────────┘
```

## 2. Constraints

| Resource | UNO Q | MSI (current) |
|---|---|---|
| RAM | 4GB | 16GB |
| CPU | 4×A53 @2.0GHz (ARM64) | Ryzen 7 + GTX 1070 |
| Storage | TBD (eMMC/SD?) | 500GB NVMe |
| Network | WiFi/Ethernet | Gigabit Ethernet |
| OS | Custom (Linux-based?) | Nobara/Fedora 44 |
| GPU | None | GTX 1070 (6GB VRAM) |

### Key implications

1. **mnemes-server is pure Rust + SQLite** — no GPU needed for the server itself. The 4×A53 @2GHz is sufficient for metadata operations, routing, and sync.
2. **Embedding inference is CPU-only on UNO Q** — `hermes-infer` C engine with nomic-embed-text Q4_K (~70MB). 4GB RAM is tight but workable: ~70MB model + ~500MB mnemes-server + SQLite + OS.
3. **Matryoshka truncation to 256d** — UNO Q stores and searches at 256d locally. Full 768d vectors are synced to/from peers. Recall@10 at 256d is ~95%+ vs 768d.
4. **No Ollama on UNO Q** — use `hermes-infer` embedding endpoint directly. The semantic-memory crate needs to be configured to use the C engine's `/embed` endpoint instead of Ollama.
5. **ARM64 cross-compilation** — mnemes-server needs to be cross-compiled for aarch64. The custom OS determines the sysroot and libc.
6. **Custom OS unknowns** — need to determine: kernel version, libc (glibc/musl), systemd availability, SSH access, package manager, storage layout.

## 3. Deployment phases

### Phase 0: UNO Q OS assessment (blocking)

Before any deployment work:

- [ ] Determine custom OS details: kernel version, init system (systemd? busybox?), libc
- [ ] Verify SSH access and network configuration
- [ ] Determine storage layout and available space
- [ ] Check available toolchain or cross-compilation target
- [ ] Assess RAM budget: OS + mnemes-server + hermes-infer + SQLite
- [ ] If no systemd: plan alternative supervision (s6, supervisord, raw init script)

### Phase 1: Cross-compile mnemes-server for ARM64

```bash
# Add ARM64 target
rustup target add aarch64-unknown-linux-gnu

# Cross-compile (adjust linker for target sysroot)
cargo build --release --target aarch64-unknown-linux-gnu --features server

# Or use cross (Docker-based):
cargo install cross
cross build --release --target aarch64-unknown-linux-gnu --features server
```

**Risk:** `rusqlite` uses `libsqlite3-sys` which compiles SQLite from source via `cc` crate. This should work for ARM64 but needs a C cross-compiler (`aarch64-linux-gnu-gcc`).

**Fallback:** If cross-compilation fails, native compilation on UNO Q if the toolchain is available. 4×A53 @2GHz can compile Rust, but it will be slow (~10-20 min for mnemes + deps).

### Phase 2: Deploy hermes-infer with embedding support

The `hermes-infer` C engine needs the embedding forward pass extension (~150 lines of C):
- Self-attention without causal mask (BERT encoder)
- Mean pooling
- L2 normalization
- `/embed` HTTP endpoint

This is separate work but blocks the full deployment because semantic-memory needs an embedder on the UNO Q.

**Interim:** Run mnemes-server without embeddings (routing-only mode). Searches delegate to peer devices that have working embedders. The UNO Q's own shard can be searched with FTS5 only.

### Phase 3: Configure semantic-memory for UNO Q

```toml
# mnemes config on UNO Q
[embedding]
model = "nomic-embed-text-v1.5"
dimensions = 256  # Matryoshka truncated
provider = "http"
endpoint = "http://127.0.0.1:8080/embed"  # hermes-infer
```

The `semantic-memory` crate needs an HTTP embedder adapter that calls `hermes-infer`'s `/embed` endpoint instead of Ollama. This is a code change in semantic-memory, not in mnemes.

### Phase 4: Migrate data from MSI to UNO Q

1. Stop mnemes-server on MSI
2. SQLite backup of `pooled.db` and all shard DBs:
   ```bash
   sqlite3 pooled.db ".backup /tmp/pooled-backup.db"
   # For each shard:
   sqlite3 memory/shards/<uuid>/memory.db ".backup /tmp/shard-<uuid>.db"
   ```
3. Transfer to UNO Q (scp or rsync)
4. Place in `~/.local/share/mnemes/`
5. Start mnemes-server on UNO Q
6. Verify health, device registry, and shard routing
7. Update laptop tunnel to point to UNO Q instead of MSI

### Phase 5: Update all clients

- Laptop: `mnemes-tunnel.service` SSH forward target changes from `msi` to `uno-q`
- MSI: becomes a client, runs `mneme-client.py` pointing to UNO Q
- Any other devices: update `MNEME_URL` in `~/.config/mnemes/client.env`

### Phase 6: Cutover and verification

1. Verify UNO Q health: `curl http://127.0.0.1:1738/v1/health`
2. Verify device registry intact: `curl -H "Authorization: Bearer ..." http://127.0.0.1:1738/v1/devices`
3. Test witnessed search: `mneme-client.py witnessed-search "test query"`
4. Test sync: push a journal entry from laptop, verify it appears in UNO Q replica
5. Monitor RAM usage: `free -h` and `ps aux | grep mnemes`
6. Monitor CPU: `top -p $(pgrep mnemes-server)`
7. MSI becomes read-only fallback (keep service disabled but binary available)

## 4. RAM budget estimate

| Component | Estimated RAM |
|---|---|
| Custom OS + kernel | ~200-400MB |
| mnemes-server (Rust + SQLite) | ~50-100MB |
| hermes-infer (nomic-embed Q4_K) | ~150MB (70MB model + working set) |
| hermes-infer (Qwen3.5-0.8B) | ~500MB-1GB (optional, can be on-demand) |
| SQLite shard caches | ~50-100MB per open shard |
| **Total (without LLM)** | **~450-750MB** |
| **Total (with LLM)** | **~950MB-1.75GB** |

4GB is sufficient if the LLM is loaded on-demand only. The embedding model should stay resident.

## 5. Risks and mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| Custom OS lacks systemd | Medium | Use s6-overlay or init script; the service file is already self-contained |
| Cross-compilation fails (rusqlite cc) | High | Native compile on UNO Q (slow but works) or use `cross` with Docker |
| 4GB RAM insufficient with LLM | Medium | Load Qwen3.5-0.8B on-demand only; keep embedding model resident |
| Storage wear (eMMC/SD) | Medium | Use WAL mode with periodic checkpoints; consider tmpfs for WAL |
| Network reliability (WiFi) | Low | mnemes-server is loopback-only; SSH tunnel handles reconnection |
| hermes-infer embedding extension not built | High | Phase 2 is blocking; interim is routing-only mode without local embeddings |
| SQLite ARM64 performance | Low | A53 @2GHz is adequate for metadata + routing; embeddings are the bottleneck |

## 6. Resolved UNO Q details (from semantic memory)

| Question | Answer |
|---|---|
| **OS** | Debian 13 (Trixie) aarch64, kernel 6.12-sdm670-1. Vendor-provided, not custom. |
| **Init system** | systemd (but no user bus via ADB). Durability via `crontab @reboot`. |
| **Storage** | 32GB eMMC. 4GB LPDDR4 RAM (3.6GB usable, ~3.0GB free). |
| **Network** | WiFi 5, `PurpleMama_5G`, static IP **192.168.50.249**. |
| **SSH** | Port **2222** (unprivileged sshd). `~/.ssh/roko.pub` in authorized_keys. Port 22 inactive (sudo-gated). `@reboot` cron starts sshd. |
| **ADB** | Device `2207799355` (USB-C data port). |
| **Qwen3.5-0.8B** | Already running via Ollama at `localhost:11434`, 4.2 t/s decode. |
| **hermes-infer** | C engine compiles but crashes during forward pass (SIGSEGV). Pivoted to Ollama. Embedding extension (~150 lines BERT encoder) not yet built. |
| **Ollama** | Installed and running. Port 11434 is the only externally accessible HTTP port. |
| **GCC** | 14.2 on device. Native Rust compilation possible but slow; no Rust currently installed. |

### SSH alias for tunnel

Add to `~/.ssh/config`:
```
Host uno-q
    HostName 192.168.50.249
    Port 2222
    User arduino
    IdentityFile ~/.ssh/roko
```

### Key deployment changes from resolved details

1. **Not a custom OS** — it's Debian 13. No special sysroot needed; standard aarch64-unknown-linux-gnu target works.
2. **No systemd user sessions via ADB** — mnemes-server must use `crontab @reboot` like the existing agent, or SSH in and use systemd if available.
3. **SSH already works on port 2222** — tunnel can point to `uno-q:2222` instead of `msi:22`.
4. **Qwen3.5-0.8B stays on UNO Q** — already running via Ollama. No need to move it.
5. **hermes-infer embedding extension is the remaining blocker** — without it, mnemes-server on UNO Q would need to use Ollama for embeddings (which works but is slower than a dedicated C endpoint).
6. **Native compilation is viable** — GCC 14.2 is on device. Installing Rust and building natively would take ~10-20 min but avoids cross-compilation complexity. The previous QEMU podman build attempt was too slow; native build on device is the practical path.