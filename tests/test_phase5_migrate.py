import importlib.util,json,sqlite3,subprocess,sys,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]; SCRIPT=ROOT/'scripts/phase5-migrate.py'
def make(path,facts,receipts,epoch,policy,version=36):
 c=sqlite3.connect(path)
 c.executescript('''create table _schema_version(version integer primary key); create table facts(id text primary key,content text not null); create table search_receipts(receipt_id text primary key,schema_version text not null,evaluation_time text not null,search_profile text not null,candidate_backend text not null,approximate integer not null,exact_rerank integer not null,fallback text,requested_candidates integer not null,returned_candidates integer not null,post_filter_candidates integer not null,result_ids_json text not null,receipt_json text not null,receipt_digest text not null,created_at text not null); create table authority_state(id integer primary key,retrieval_epoch integer not null,projection_epoch integer not null,cache_epoch integer not null,export_epoch integer not null,replay_epoch integer not null); create table routing_policy(id integer primary key,policy_json text not null,updated_at text not null);''')
 c.execute('insert into _schema_version values(?)',(version,)); c.executemany('insert into facts values(?,?)',facts)
 for rid in receipts:c.execute('insert into search_receipts values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(rid,'v1','2026-01-01T00:00:00Z','lean','exact',0,1,None,1,1,1,'[]','{}','sha:'+rid,'2026-01-01'))
 c.execute('insert into authority_state values(1,?,?,?,?,?)',(epoch,2,2,2,2)); c.execute('insert into routing_policy values(1,?,?)',(policy,f'2026-01-{epoch:02d}')); c.commit(); c.close()
class TestPhase5(unittest.TestCase):
 def cli(self,*a):return subprocess.run([sys.executable,str(SCRIPT),*map(str,a)],text=True,capture_output=True)
 def test_snapshot_and_reconcile_union(self):
  with tempfile.TemporaryDirectory() as d:
   d=Path(d); p=d/'p.db'; s=d/'s.db'; make(p,[('a','A'),('b','B')],['p','shared'],5,'new'); make(s,[('a','A')],['s1','s2','shared'],4,'old')
   ps=d/'ps.db'; ss=d/'ss.db'; self.assertEqual(self.cli('snapshot','--db',p,'--out',ps,'--source','laptop').returncode,0); self.assertEqual(self.cli('snapshot','--db',s,'--out',ss,'--source','msi').returncode,0)
   out=d/'merged.db'; ledger=d/'ledger.json'; r=self.cli('reconcile','--primary',ps,'--secondary',ss,'--out',out,'--ledger',ledger); self.assertEqual(r.returncode,0,r.stderr)
   c=sqlite3.connect(out); self.assertEqual(c.execute('select count(*) from search_receipts').fetchone()[0],4); self.assertEqual(c.execute('select retrieval_epoch from authority_state').fetchone()[0],5); c.close()
   l=json.loads(ledger.read_text()); self.assertEqual(l['merged_search_receipts'],2); self.assertEqual({x['table'] for x in l['projection_conflicts']},{'authority_state','routing_policy'})
   self.assertEqual(self.cli('verify','--db',out,'--manifest',str(out)+'.manifest.json').returncode,0)
 def test_secondary_truth_row_fails_closed(self):
  with tempfile.TemporaryDirectory() as d:
   d=Path(d); p=d/'p.db'; s=d/'s.db'; make(p,[('a','A')],[],5,'new'); make(s,[('a','A'),('z','Z')],[],4,'old'); r=self.cli('reconcile','--primary',p,'--secondary',s,'--out',d/'o.db','--ledger',d/'l.json'); self.assertEqual(r.returncode,2); self.assertIn('secondary-only rows in facts',r.stderr); self.assertFalse((d/'o.db').exists())
 def test_same_id_receipt_conflict_fails(self):
  with tempfile.TemporaryDirectory() as d:
   d=Path(d); p=d/'p.db'; s=d/'s.db'; make(p,[],['same'],5,'new'); make(s,[],['same'],4,'old'); c=sqlite3.connect(s); c.execute("update search_receipts set receipt_digest='different' where receipt_id='same'"); c.commit(); c.close(); r=self.cli('reconcile','--primary',p,'--secondary',s,'--out',d/'o.db','--ledger',d/'l.json'); self.assertEqual(r.returncode,2); self.assertIn('same-id conflict in search_receipts',r.stderr)
if __name__=='__main__':unittest.main()
