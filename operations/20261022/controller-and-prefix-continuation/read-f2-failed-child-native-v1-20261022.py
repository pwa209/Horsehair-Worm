from pathlib import Path
from datetime import datetime,timezone
import gzip,hashlib,io,json,os,subprocess,time
from horsehair.core import canonical_hash
os.environ['TZ']='UTC';time.tzset();assert os.geteuid()==3164768
assert os.environ.get('SLURM_JOB_ID','').isdigit() and os.environ.get('SLURM_CPUS_PER_TASK')=='1'
root=Path('/scratch/pwa209/horsehair-worm');case=root/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
b=(case/'state-binding.json').read_bytes();binding=json.loads(b);compressed=(case/'state.json.gz').read_bytes();assert len(compressed)<64000000 and hashlib.sha256(compressed).hexdigest()==binding['state_file_sha256']
with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as f:plain=f.read(512000001)
assert len(plain)<=512000000;state=json.loads(plain);assert state['state_sha256']==canonical_hash({k:v for k,v in state.items() if k!='state_sha256'})==binding['state_sha256'];assert (case/'state-binding.json').read_bytes()==b
out={'checked_utc':datetime.now(timezone.utc).isoformat(),'state_binding':binding,'status':state['status'],'needs_repair':state['needs_repair'],'training_complete':sum(j['status']=='complete' and j['intent']['kind']=='training' for j in state['jobs'].values()),'policies_complete':sum(c['complete'] for c in state['cells']),'incomplete':{},'logs':{}}
ids=[]
for name,j in state['jobs'].items():
 if j['status']=='complete':continue
 out['incomplete'][name]={k:v for k,v in j.items() if k not in ['receipt','failure_receipt']};ids+=[j['job_id']] if j['job_id'] is not None else []
 if j['status']=='needs_repair':
  log=root/('logs/af-f2-'+name+'-'+str(j['job_id'])+'_4294967294.log')
  if log.is_file():
   with log.open('rb') as f:f.seek(max(0,log.stat().st_size-12000));out['logs'][name]=f.read(12000).decode(errors='replace')
if ids:
 p=subprocess.run(['sacct','--array','-j',','.join(ids),'--format=JobID%64,JobIDRaw%64,State,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES,NodeList%128,Start,End,User,Account,MaxRSS','--units=K','--noheader','--parsable2'],capture_output=True,text=True,timeout=45);out['native']={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
with (Path(__file__).resolve().parent/'failed-child-summary.json').open('x') as stream:json.dump(out,stream,sort_keys=True)
print('AF_FAILED_CHILD_READ '+json.dumps(out,sort_keys=True),flush=True)