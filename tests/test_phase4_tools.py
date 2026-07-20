import http.server, importlib.util, json, os, stat, subprocess, tempfile, threading, unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
CLIENT=ROOT/'scripts/pooled-memory-client.py'
PROXY=ROOT/'scripts/pooled-memory-mcp-proxy.py'
WRAPPER=ROOT/'scripts/pooled-codex-task.sh'
INSTALL=ROOT/'scripts/install-pooled-memory-service.sh'

class Handler(http.server.BaseHTTPRequestHandler):
    malformed=False
    seen=[]
    def log_message(self,*args): pass
    def do_GET(self):
        if self.headers.get('Authorization')!='Bearer device:secret': self.send_response(401); self.end_headers(); self.wfile.write(b'{"error":"invalid credentials"}'); return
        self.send_response(200); self.end_headers(); self.wfile.write(b'not-json' if self.malformed else b'{"service":"ok"}')
    def do_POST(self):
        n=int(self.headers.get('content-length','0')); body=json.loads(self.rfile.read(n)); Handler.seen.append(body)
        if self.headers.get('Authorization')!='Bearer device:secret': self.send_response(403); self.end_headers(); self.wfile.write(b'{"error":"access denied"}'); return
        self.send_response(200); self.end_headers()
        if body.get('method')=='tools/list': value={'jsonrpc':'2.0','id':body.get('id'),'result':{'tools':[{'name':'sm_health'}]}}
        else: value={'jsonrpc':'2.0','id':body.get('id'),'result':{'ok':True,'receipt_id':'receipt-1'}}
        self.wfile.write(json.dumps(value).encode())

class Phase4Tools(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
        cls.thread=threading.Thread(target=cls.server.serve_forever,daemon=True); cls.thread.start()
    @classmethod
    def tearDownClass(cls): cls.server.shutdown()
    def env(self,d):
        p=Path(d)/'client.env'; p.write_text(f'POOLED_MEMORY_URL=http://127.0.0.1:{self.server.server_port}\nPOOLED_MEMORY_CREDENTIAL=device:secret\nPOOLED_MEMORY_ACTOR_ID=actor-1\nPOOLED_MEMORY_DEVICE_ID=device-1\n'); p.chmod(0o600)
        e=os.environ.copy(); e['POOLED_MEMORY_ENV_FILE']=str(p); return e
    def test_health_and_no_secret_leak(self):
        with tempfile.TemporaryDirectory() as d:
            r=subprocess.run([str(CLIENT),'health'],env=self.env(d),text=True,capture_output=True)
            self.assertEqual(r.returncode,0,r.stderr); self.assertNotIn('device:secret',r.stdout+r.stderr)
    def test_malformed_json_fails(self):
        with tempfile.TemporaryDirectory() as d:
            Handler.malformed=True
            try: r=subprocess.run([str(CLIENT),'health'],env=self.env(d),text=True,capture_output=True)
            finally: Handler.malformed=False
            self.assertNotEqual(r.returncode,0); self.assertIn('malformed JSON',r.stderr); self.assertNotIn('device:secret',r.stderr)
    def test_proxy_initializes_as_standard_mcp(self):
        with tempfile.TemporaryDirectory() as d:
            request=json.dumps({'jsonrpc':'2.0','id':3,'method':'initialize','params':{'protocolVersion':'2024-11-05'}})+'\n'
            r=subprocess.run([str(PROXY)],input=request,env=self.env(d),text=True,capture_output=True)
            self.assertEqual(r.returncode,0,r.stderr)
            result=json.loads(r.stdout)['result']
            self.assertEqual(result['protocolVersion'],'2024-11-05')
            self.assertIn('tools',result['capabilities'])
            self.assertEqual(result['serverInfo']['name'],'pooled-memory')

    def test_proxy_injects_actor(self):
        with tempfile.TemporaryDirectory() as d:
            request=json.dumps({'jsonrpc':'2.0','id':7,'method':'tools/list','params':{}})+'\n'
            r=subprocess.run([str(PROXY)],input=request,env=self.env(d),text=True,capture_output=True)
            self.assertEqual(r.returncode,0,r.stderr); self.assertEqual(json.loads(r.stdout)['id'],7)
            self.assertEqual(Handler.seen[-1]['params']['actor_id'],'actor-1')
    def test_install_audit_has_no_side_effect(self):
        with tempfile.TemporaryDirectory() as d:
            e=os.environ.copy(); e['HOME']=d
            r=subprocess.run([str(INSTALL)],env=e,text=True,capture_output=True)
            self.assertEqual(r.returncode,0); self.assertFalse((Path(d)/'.config/pooled-memory').exists())
    def test_codex_exit_code_preserved(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); bindir=root/'bin'; bindir.mkdir(); receipts=root/'receipts'
            codex=bindir/'codex'; codex.write_text('#!/bin/sh\necho fake-codex\nexit 7\n'); codex.chmod(0o755)
            client=bindir/'client'; client.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$CLIENT_ARGS_LOG"\ncase "$1" in witnessed-search) echo "{\\"receipt\\":{}}";; submit-operation) echo "{\\"receipt_id\\":\\"r1\\"}";; esac\n'); client.chmod(0o755)
            args_log=root/'client-args.log'; e=os.environ.copy(); e.update({'PATH':str(bindir)+':'+e['PATH'],'POOLED_MEMORY_CLIENT':str(client),'POOLED_MEMORY_RECEIPT_DIR':str(receipts),'CLIENT_ARGS_LOG':str(args_log),'HOME':d})
            r=subprocess.run([str(WRAPPER),'--','prompt'],env=e,text=True,capture_output=True)
            self.assertEqual(r.returncode,7); self.assertTrue(list(receipts.glob('*.receipt.json'))); self.assertIn('--operation-kind observe',args_log.read_text())

if __name__=='__main__': unittest.main()
