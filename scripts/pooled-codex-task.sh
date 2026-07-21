#!/usr/bin/env bash
set -uo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
client=${MNEMES_CLIENT:-$script_dir/mnemes-client.py}
receipt_dir=${MNEMES_RECEIPT_DIR:-$HOME/.local/state/mnemes/codex-receipts}
strict=${MNEMES_STRICT:-1}
dry=0
[[ ${1:-} == --dry-run ]] && { dry=1; shift; }
[[ ${1:-} == -- ]] && shift
if (( dry )); then printf '{"dry_run":true,"would_run":"codex exec","argument_count":%d}\n' "$#"; exit 0; fi
mkdir -p "$receipt_dir"; chmod 700 "$receipt_dir"
task_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
base="$receipt_dir/$task_id"; context="$base.context.json"; transcript="$base.transcript.txt"; receipt="$base.receipt.json"
query=${MNEMES_CONTEXT_QUERY:-${*:-coding task}}
if ! "$client" witnessed-search "$query" >"$context"; then
  [[ $strict == 1 ]] && exit 70
  printf '{"degraded":"pooled context unavailable"}\n' >"$context"
fi
codex exec "$@" 2>&1 | tee "$transcript"
rc=${PIPESTATUS[0]}
digest="sha256:$(sha256sum "$transcript" | cut -d' ' -f1)"
status=complete; (( rc != 0 )) && status=incomplete
if "$client" submit-operation --idempotency-key "codex-task:$task_id" --operation-kind observe --target-kind coding_task --target-id "$task_id" --content-digest "$digest" >"$receipt.tmp"; then
  python3 - "$task_id" "$status" "$rc" "$digest" "$context" "$transcript" "$receipt.tmp" >"$receipt" <<'PY'
import json,sys
id,status,rc,digest,context,transcript,server=sys.argv[1:]
print(json.dumps({'schema':'pooled-codex-task-receipt.v1','task_id':id,'status':status,'codex_exit_code':int(rc),'content_digest':digest,'context_receipt_path':context,'transcript_path':transcript,'server_receipt':json.load(open(server))},sort_keys=True,indent=2))
PY
  rm -f "$receipt.tmp"
else
  rm -f "$receipt.tmp"
  if (( rc == 0 )) && [[ $strict == 1 ]]; then exit 71; fi
fi
exit "$rc"
