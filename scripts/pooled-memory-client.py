#!/usr/bin/env python3
"""Strict stdlib client for pooled-memory HTTP/MCP."""
import argparse, json, os, sys, time, urllib.error, urllib.request
from pathlib import Path

class ClientError(RuntimeError):
    def __init__(self, message, status=None, body=None):
        super().__init__(message); self.status=status; self.body=body

def load_env_file(path):
    values={}
    p=Path(path).expanduser()
    if not p.exists(): return values
    if p.stat().st_mode & 0o077: raise ClientError(f"credential env must be mode 0600: {p}")
    for raw in p.read_text().splitlines():
        raw=raw.strip()
        if not raw or raw.startswith("#"): continue
        if "=" not in raw: raise ClientError(f"invalid env line in {p}")
        k,v=raw.split("=",1); values[k.strip()]=v.strip().strip('"').strip("'")
    return values

class PooledClient:
    def __init__(self, env=None):
        source=dict(os.environ if env is None else env)
        path=source.get("POOLED_MEMORY_ENV_FILE", str(Path.home()/".config/pooled-memory/client.env"))
        file_values=load_env_file(path)
        for k,v in file_values.items(): source.setdefault(k,v)
        self.url=source.get("POOLED_MEMORY_URL", "").rstrip("/")
        self.credential=source.get("POOLED_MEMORY_CREDENTIAL", "")
        self.actor_id=source.get("POOLED_MEMORY_ACTOR_ID", "")
        self.device_id=source.get("POOLED_MEMORY_DEVICE_ID", "")
        self.timeout=float(source.get("POOLED_MEMORY_TIMEOUT", "15"))
        if not all((self.url,self.credential,self.actor_id,self.device_id)):
            raise ClientError("missing pooled-memory URL/credential/actor/device configuration")

    def request(self, path, method="GET", body=None, retries=0):
        payload=None if body is None else json.dumps(body,separators=(",",":")).encode()
        headers={"Authorization":"Bearer "+self.credential,"Content-Type":"application/json"}
        for attempt in range(retries+1):
            req=urllib.request.Request(self.url+path,data=payload,headers=headers,method=method)
            try:
                with urllib.request.urlopen(req,timeout=self.timeout) as response:
                    raw=response.read()
                    try: value=json.loads(raw or b"{}")
                    except Exception as e: raise ClientError("malformed JSON response",response.status) from e
                    return response.status,value
            except urllib.error.HTTPError as e:
                raw=e.read()
                try: body_value=json.loads(raw or b"{}")
                except Exception: body_value={"error":"malformed error response"}
                raise ClientError(body_value.get("error",f"HTTP {e.code}"),e.code,body_value)
            except (urllib.error.URLError,TimeoutError) as e:
                if attempt>=retries: raise ClientError(f"transport failure: {e}") from e
                time.sleep(0.2*(2**attempt))

    def mcp(self, method, params=None):
        params=dict(params or {})
        if method in ("tools/list","tools/call"): params.setdefault("actor_id",self.actor_id)
        _,value=self.request("/v1/mcp","POST",{"jsonrpc":"2.0","id":"pooled-client","method":method,"params":params})
        if value.get("error"): raise ClientError(value["error"].get("message","MCP error"),body=value)
        if "result" not in value: raise ClientError("MCP response missing result")
        return value["result"]

def output(value): print(json.dumps(value,sort_keys=True,indent=2))
def main(argv=None):
    p=argparse.ArgumentParser(); sub=p.add_subparsers(dest="command",required=True)
    sub.add_parser("health"); sub.add_parser("tools-list")
    tc=sub.add_parser("tool-call"); tc.add_argument("name"); tc.add_argument("--arguments",default="{}")
    ws=sub.add_parser("witnessed-search"); ws.add_argument("query"); ws.add_argument("--top-k",type=int,default=5); ws.add_argument("--source-types",default="facts")
    so=sub.add_parser("submit-operation")
    for name in ("idempotency_key","operation_kind","target_kind","target_id","content_digest"): so.add_argument("--"+name.replace("_","-"),required=True)
    a=p.parse_args(argv); c=PooledClient()
    if a.command=="health": _,v=c.request("/v1/health",retries=2); output(v)
    elif a.command=="tools-list": output(c.mcp("tools/list"))
    elif a.command=="tool-call": output(c.mcp("tools/call",{"name":a.name,"arguments":json.loads(a.arguments)}))
    elif a.command=="witnessed-search": output(c.mcp("tools/call",{"name":"sm_search_witnessed","arguments":{"query":a.query,"top_k":a.top_k,"source_types":[x for x in a.source_types.split(",") if x]}}))
    else:
        args={"idempotency_key":a.idempotency_key,"requesting_device_id":c.device_id,"requesting_actor_id":c.actor_id,"operation_kind":a.operation_kind,"target_kind":a.target_kind,"target_id":a.target_id,"content_digest":a.content_digest,"recording_device_id":c.device_id,"recording_server_id":c.device_id}
        output(c.mcp("tools/call",{"name":"sm_submit_operation","arguments":args}))
    return 0
if __name__=="__main__":
    try: raise SystemExit(main())
    except ClientError as e:
        print(json.dumps({"ok":False,"error":str(e),"status":e.status},sort_keys=True),file=sys.stderr); raise SystemExit(1)
