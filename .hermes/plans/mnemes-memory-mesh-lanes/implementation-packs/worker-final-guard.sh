#!/usr/bin/env bash
# Final containment check for a clean, isolated worker worktree.
# Usage: bash worker-final-guard.sh /abs/worktree <expected-head> <receipt.md> -- path/one path/two
set -euo pipefail

if [[ $# -lt 4 || "$4" != "--" ]]; then
  printf 'usage: %s /abs/worktree <expected-head> <receipt.md> -- <allowed-path>...\n' "$0" >&2
  exit 64
fi
repo=$(realpath "$1")
expected=$2
receipt=$3
shift 4
actual_head=$(git -C "$repo" rev-parse HEAD)

if [[ "$actual_head" != "$expected" ]]; then
  printf 'FAIL: HEAD changed from %s to %s; no-commit rule violated\n' "$expected" "$actual_head" >&2
  exit 65
fi
if [[ ! -s "$receipt" ]]; then
  printf 'FAIL: missing non-empty worker receipt: %s\n' "$receipt" >&2
  exit 66
fi

is_allowed() {
  local candidate=$1 allowed
  for allowed in "$@"; do :; done
}

# Clean worktrees are mandatory. Thus every changed path must be owned by this task.
violations=0
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  path=${line:3}
  # Porcelain rename format may include an old and new name; forbid it unless both are allowlisted.
  ok=0
  for allowed in "$@"; do
    [[ "$path" == "$allowed" || "$path" == "$allowed"/* ]] && ok=1
  done
  if [[ $ok -eq 0 ]]; then
    printf 'OUT_OF_SCOPE %s\n' "$line" >&2
    violations=1
  fi
done < <(git -C "$repo" status --porcelain=v1 --untracked-files=all)

if [[ $violations -ne 0 ]]; then
  exit 67
fi
if ! git -C "$repo" diff --check || ! git -C "$repo" diff --cached --check; then
  printf 'FAIL: whitespace errors in tracked or staged diff\n' >&2
  exit 68
fi
printf 'FINAL GUARD PASS\nhead=%s\nreceipt=%s\n' "$actual_head" "$receipt"
