#!/usr/bin/env bash
set -euo pipefail
apply=0
[[ ${1:-} == --apply ]] && apply=1
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
config_dir=${HOME}/.config/pooled-memory
data_dir=${HOME}/.local/share/pooled-memory
bin_dir=${HOME}/.local/bin
unit_dir=${HOME}/.config/systemd/user
env_file=${config_dir}/server.env
if (( ! apply )); then
  printf 'AUDIT: would build release binaries and install pooled-memory service\n'
  printf 'repo=%s data=%s env=%s\n' "$repo" "$data_dir" "$env_file"
  exit 0
fi
umask 077
install -d -m 0700 "$config_dir" "$data_dir" "$bin_dir" "$unit_dir"
if [[ -e $env_file ]]; then
  printf 'refusing to overwrite existing %s\n' "$env_file" >&2; exit 2
fi
cargo build --manifest-path "$repo/Cargo.toml" --release --all-features --bins
install -m 0755 "$repo/target/release/pooled-memory-server" "$bin_dir/.pooled-memory-server.new"
install -m 0755 "$repo/target/release/pooled-memory-admin" "$bin_dir/.pooled-memory-admin.new"
mv -f "$bin_dir/.pooled-memory-server.new" "$bin_dir/pooled-memory-server"
mv -f "$bin_dir/.pooled-memory-admin.new" "$bin_dir/pooled-memory-admin"
install -m 0644 "$repo/ops/systemd/pooled-memory.service" "$unit_dir/.pooled-memory.service.new"
mv -f "$unit_dir/.pooled-memory.service.new" "$unit_dir/pooled-memory.service"
printf 'POOLED_MEMORY_PORT=1738\nPOOLED_MEMORY_DATA_DIR=%s\n' "$data_dir" > "$env_file"
chmod 0600 "$env_file"
systemctl --user daemon-reload
printf 'installed; bootstrap credentials before enabling pooled-memory.service\n'
