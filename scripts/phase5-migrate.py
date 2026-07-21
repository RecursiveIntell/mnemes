#!/usr/bin/env python3
"""Fail-closed, offline semantic-memory snapshot and reconciliation tool."""
from __future__ import annotations
import argparse, hashlib, json, os, shutil, sqlite3, sys, tempfile
from datetime import datetime, timezone
from pathlib import Path

DERIVED_PREFIXES=("sqlite_","chunks_fts","facts_fts","messages_fts","episodes_fts")
DERIVED_TABLES={"hnsw_keymap","hnsw_metadata","pending_index_ops","facts_rowid_map","chunks_rowid_map","messages_rowid_map","episodes_rowid_map","embedding_metadata","derived_vector_artifacts","derived_vector_artifact_generations"}
SINGLETON_WINNERS={"authority_state","routing_policy"}
MERGE_TABLE="search_receipts"
SCHEMA="mnemes.migration-envelope.v1"

class MigrationError(RuntimeError): pass

def now(): return datetime.now(timezone.utc).isoformat()
def sha(path:Path):
 h=hashlib.sha256()
 with path.open("rb") as f:
  for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
 return h.hexdigest()
def norm(v):
 if isinstance(v,bytes): return {"blob_sha256":hashlib.sha256(v).hexdigest(),"bytes":len(v)}
 return v
def qident(s): return '"'+s.replace('"','""')+'"'
def open_ro(path): return sqlite3.connect(f"file:{Path(path).resolve()}?mode=ro",uri=True)
def quick(c): return c.execute("pragma quick_check").fetchone()[0]
def tables(c):
 return [r[0] for r in c.execute("select name from sqlite_master where type='table' order by name") if not r[0].startswith(DERIVED_PREFIXES) and r[0] not in DERIVED_TABLES]
def info(c,t): return list(c.execute(f"pragma table_info({qident(t)})"))
def pk_cols(c,t): return [r[1] for r in sorted((r for r in info(c,t) if r[5]),key=lambda x:x[5])]
def row_map(c,t):
 cols=[r[1] for r in info(c,t)]; pk=pk_cols(c,t); out={}
 for idx,row in enumerate(c.execute(f"select * from {qident(t)}")):
  d={k:norm(v) for k,v in zip(cols,row)}; raw=json.dumps(d,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode(); digest=hashlib.sha256(raw).hexdigest()
  key=json.dumps([norm(row[cols.index(k)]) for k in pk],sort_keys=True,separators=(",",":")) if pk else f"row:{digest}"
  if key in out: raise MigrationError(f"duplicate identity in {t}: {key}")
  out[key]={"digest":digest,"row":d}
 return out
def manifest(db,source):
 c=open_ro(db)
 try:
  qc=quick(c)
  if qc!="ok": raise MigrationError(f"quick_check failed: {qc}")
  entries={}; root=[]
  for t in tables(c):
   rows=row_map(c,t); entries[t]={"count":len(rows),"pk":pk_cols(c,t)}
   root.extend(f"{t}\0{k}\0{v['digest']}" for k,v in rows.items())
  rr=hashlib.sha256("\n".join(sorted(root)).encode()).hexdigest()
  uv=c.execute("pragma user_version").fetchone()[0]
  sv=None
  if "_schema_version" in tables(c):
   try: sv=c.execute("select max(version) from _schema_version").fetchone()[0]
   except sqlite3.Error: pass
  return {"schema":SCHEMA,"created_at":now(),"source":source,"db_sha256":sha(Path(db)),"row_root_sha256":rr,"quick_check":qc,"user_version":uv,"schema_version":sv,"tables":entries}
 finally:c.close()
def atomic_json(path,obj):
 p=Path(path); p.parent.mkdir(parents=True,exist_ok=True); tmp=p.with_suffix(p.suffix+".tmp"); tmp.write_text(json.dumps(obj,indent=2,sort_keys=True)+"\n"); os.chmod(tmp,0o600); os.replace(tmp,p)
def snapshot(args):
 src=Path(args.db); out=Path(args.out); out.parent.mkdir(parents=True,exist_ok=True)
 if out.exists(): raise MigrationError(f"refusing overwrite: {out}")
 s=open_ro(src); tmp=out.with_suffix(out.suffix+".tmp")
 try:
  d=sqlite3.connect(tmp); s.backup(d); d.close()
 finally:s.close()
 os.chmod(tmp,0o600); os.replace(tmp,out)
 m=manifest(out,args.source); atomic_json(str(out)+".manifest.json",m); print(json.dumps(m,sort_keys=True))
def same_schema(a,b):
 ta=tables(a); tb=tables(b)
 if ta!=tb: raise MigrationError(f"table sets differ: primary_only={sorted(set(ta)-set(tb))}, secondary_only={sorted(set(tb)-set(ta))}")
 for t in ta:
  if info(a,t)!=info(b,t): raise MigrationError(f"schema mismatch: {t}")
 return ta
def reconcile(args):
 p=Path(args.primary); s=Path(args.secondary); out=Path(args.out)
 if out.exists(): raise MigrationError(f"refusing overwrite: {out}")
 a=open_ro(p); b=open_ro(s); conflicts=[]; comparisons={}
 try:
  if quick(a)!="ok" or quick(b)!="ok": raise MigrationError("source quick_check failed")
  ts=same_schema(a,b)
  for t in ts:
   am=row_map(a,t); bm=row_map(b,t); ak=set(am); bk=set(bm); common=ak&bk
   differing=[k for k in common if am[k]["digest"]!=bm[k]["digest"]]
   secondary_only=sorted(bk-ak)
   if differing and t not in SINGLETON_WINNERS: raise MigrationError(f"same-id conflict in {t}: {differing[:3]}")
   if secondary_only and t!=MERGE_TABLE: raise MigrationError(f"secondary-only rows in {t}: {secondary_only[:3]}")
   if t in SINGLETON_WINNERS:
    for k in differing: conflicts.append({"table":t,"key":json.loads(k),"decision":"primary_wins","primary":am[k]["row"],"secondary":bm[k]["row"],"primary_digest":am[k]["digest"],"secondary_digest":bm[k]["digest"]})
   comparisons[t]={"primary":len(am),"secondary":len(bm),"secondary_only":len(secondary_only),"same_id_conflicts":len(differing)}
 finally:a.close(); b.close()
 shutil.copy2(p,out); os.chmod(out,0o600)
 c=sqlite3.connect(out)
 try:
  c.execute("attach database ? as secondary",(str(s.resolve()),))
  cols=[r[1] for r in info(c,MERGE_TABLE)]; names=",".join(qident(x) for x in cols); pk=pk_cols(c,MERGE_TABLE)
  if len(pk)!=1: raise MigrationError("search_receipts must have one primary key")
  sql=f"insert into main.{qident(MERGE_TABLE)} ({names}) select {names} from secondary.{qident(MERGE_TABLE)} s where not exists (select 1 from main.{qident(MERGE_TABLE)} p where p.{qident(pk[0])}=s.{qident(pk[0])})"
  before=c.execute(f"select count(*) from {qident(MERGE_TABLE)}").fetchone()[0]; c.execute(sql); after=c.execute(f"select count(*) from {qident(MERGE_TABLE)}").fetchone()[0]; c.commit(); c.execute("detach database secondary")
  if quick(c)!="ok": raise MigrationError("merged quick_check failed")
 finally:c.close()
 ledger={"schema":"mnemes.reconciliation-ledger.v1","created_at":now(),"primary":str(p),"secondary":str(s),"primary_sha256":sha(p),"secondary_sha256":sha(s),"winner_policy":"primary strict-superset; newer authority/routing projection wins","merged_search_receipts":after-before,"comparisons":comparisons,"projection_conflicts":conflicts}
 atomic_json(args.ledger,ledger); m=manifest(out,"reconciled"); m["reconciliation_ledger_sha256"]=sha(Path(args.ledger)); atomic_json(str(out)+".manifest.json",m); print(json.dumps({"merged":str(out),"merged_search_receipts":after-before,"db_sha256":m["db_sha256"],"row_root_sha256":m["row_root_sha256"]},sort_keys=True))
def verify(args):
 m=manifest(Path(args.db),args.source); expected=json.load(open(args.manifest)) if args.manifest else None
 if expected:
  for k in ("db_sha256","row_root_sha256","quick_check"):
   if m[k]!=expected[k]: raise MigrationError(f"manifest mismatch {k}: {m[k]} != {expected[k]}")
 print(json.dumps(m,sort_keys=True))
def main():
 ap=argparse.ArgumentParser(); sub=ap.add_subparsers(dest="cmd",required=True)
 x=sub.add_parser("snapshot"); x.add_argument("--db",required=True); x.add_argument("--out",required=True); x.add_argument("--source",required=True); x.set_defaults(fn=snapshot)
 x=sub.add_parser("reconcile"); x.add_argument("--primary",required=True); x.add_argument("--secondary",required=True); x.add_argument("--out",required=True); x.add_argument("--ledger",required=True); x.set_defaults(fn=reconcile)
 x=sub.add_parser("verify"); x.add_argument("--db",required=True); x.add_argument("--source",default="verify"); x.add_argument("--manifest"); x.set_defaults(fn=verify)
 a=ap.parse_args()
 try:a.fn(a)
 except (MigrationError,sqlite3.Error,OSError,ValueError) as e: print(json.dumps({"ok":False,"error":str(e)}),file=sys.stderr); return 2
 return 0
if __name__=="__main__": raise SystemExit(main())
