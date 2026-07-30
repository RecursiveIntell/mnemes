#!/usr/bin/env bash
# Read-only preflight for isolated implementation worktrees.
# Usage: bash worker-preflight.sh /abs/worktree <expected-head> -- path/one path/two
set -euo pipefail

if [[ $# -lt 3 || "$3" != "--" ]]; then
  printf 'usage: %s /abs/worktree <expected-head> -- <allowed-path>...\n' "$0" >&2
  exit 64
fi

repo=$(realpath "$1")
expected=$2
shift 3

actual_root=$(git -C "$repo" rev-parse --show-toplevel)
actual_head=$(git -C "$repo" rev-parse HEAD)
branch=$(git -C "$repo" branch --show-current || true)

printf 'repo=%s\nbranch=%s\nexpected_head=%s\nactual_head=%s\n' "$actual_root" "$branch" "$expected" "$actual_head"
if [[ "$actual_head" != "$expected" ]]; then
  printf 'FAIL: HEAD moved or wrong worktree\n' >&2
  exit 65
fi

printf '\nallowed_paths:\n'
for path in "$@"; do
  case "$path" in
    /*|*'..'*) printf 'FAIL: allowlist must be a repository-relative non-parent path: %s\n' "$path" >&2; exit 66 ;;
  esac
  printf '%s\n' "$path"
done

printf '\nscoped_status:\n'
git -C "$repo" status --short -- "$@" || true
printf '\ntracked_hashes:\n'
for path in "$@"; do
  if [[ -f "$repo/$path" ]]; then
    sha256sum "$repo/$path"
  fi
done
printf '\nPRECHECK PASS\n'
