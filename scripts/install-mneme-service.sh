#!/usr/bin/env bash
set -euo pipefail
apply=0
[[ ${1:-} == --apply ]] && apply=1
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
config_dir=${HOME}/.config/mnemes
data_dir=${HOME}/.local/share/mnemes
bin_dir=${HOME}/.local/bin
unit_dir=${HOME}/.config/systemd/user
env_file=${config_dir}/server.env
if (( ! apply )); then
  printf 'AUDIT: would build release binaries and install mnemes service\n'
  printf 'repo=%s data=%s env=%s\n' "$repo" "$data_dir" "$env_file"
  exit 0
fi
umask 077
install -d -m 0700 "$config_dir" "$data_dir" "$bin_dir" "$unit_dir"
if [[ -e $env_file ]]; then
  printf 'refusing to overwrite existing %s\n' "$env_file" >&2; exit 2
fi
cargo build --manifest-path "$repo/Cargo.toml" --release --all-features --bins
install -m 0755 "$repo/target/release/mnemes-server" "$bin_dir/.mnemes-server.new"
install -m 0755 "$repo/target/release/mnemes-admin" "$bin_dir/.mnemes-admin.new"
mv -f "$bin_dir/.mnemes-server.new" "$bin_dir/mnemes-server"
mv -f "$bin_dir/.mnemes-admin.new" "$bin_dir/mnemes-admin"
install -m 0644 "$repo/ops/systemd/mnemes.service" "$unit_dir/.mnemes.service.new"
mv -f "$unit_dir/.mnemes.service.new" "$unit_dir/mnemes.service"
printf 'MNEMES_PORT=1738\nMNEMES_DATA_DIR=%s\n' "$data_dir" > "$env_file"
chmod 0600 "$env_file"
systemctl --user daemon-reload
printf 'installed; bootstrap credentials before enabling mnemes.service\n'
