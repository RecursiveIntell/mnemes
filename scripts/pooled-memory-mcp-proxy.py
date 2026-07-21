#!/usr/bin/env python3
"""Line-delimited stdio JSON-RPC bridge to mnemes HTTP MCP."""
import importlib.util, json, sys
from pathlib import Path
_module_path = Path(__file__).resolve().with_name("mnemes-client.py")
_spec = importlib.util.spec_from_file_location("mnemes_client", _module_path)
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
PooledClient, ClientError = _module.PooledClient, _module.ClientError

def respond(value):
    sys.stdout.write(json.dumps(value,separators=(",",":"))+"\n"); sys.stdout.flush()
def main():
    client=PooledClient()
    for line in sys.stdin:
        try:
            req=json.loads(line); ident=req.get("id"); method=req.get("method",""); params=dict(req.get("params") or {})
            if method == "notifications/initialized":
                continue
            if method == "initialize":
                respond({
                    "jsonrpc": "2.0", "id": ident,
                    "result": {
                        "protocolVersion": params.get("protocolVersion", "2024-11-05"),
                        "capabilities": {"tools": {"listChanged": False}},
                        "serverInfo": {"name": "mnemes", "version": "0.1.0"},
                    },
                })
                continue
            if method == "ping":
                respond({"jsonrpc": "2.0", "id": ident, "result": {}})
                continue
            if method in ("tools/list","tools/call"): params.setdefault("actor_id",client.actor_id)
            _,result=client.request("/v1/mcp","POST",{"jsonrpc":"2.0","id":ident,"method":method,"params":params})
            result["id"]=ident
            respond(result)
        except ClientError as e: respond({"jsonrpc":"2.0","id":locals().get("ident"),"error":{"code":-32000,"message":str(e)}})
        except Exception: respond({"jsonrpc":"2.0","id":locals().get("ident"),"error":{"code":-32700,"message":"invalid request"}})
if __name__=="__main__": main()
