#!/bin/bash
set -euo pipefail
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src
python -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import gzip,hashlib,io,json,os,subprocess,time
from horsehair.core import canonical_hash
assert os.geteuid()==3164768
os.environ['TZ']='UTC';time.tzset()
root=Path('/scratch/pwa209/horsehair-worm');case=root/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
out={'checked_utc':datetime.now(timezone.utc).isoformat(),'read_only':True,'records':{},'journals':{},'unbound_native_plans':[]}
def raw(path,maximum=2000000):
 assert path.is_file() and not path.is_symlink() and path.stat().st_uid==3164768 and path.stat().st_size<=maximum
 with path.open('rb') as stream:value=stream.read(maximum+1)
 assert len(value)<=maximum
 return value
def record(name,path,maximum=2000000):
 if not path.is_file():return
 value=raw(path,maximum);out['records'][name]={'path':str(path),'sha256':hashlib.sha256(value).hexdigest(),'bytes':len(value),'value':json.loads(value)}
binding_raw=raw(case/'state-binding.json');binding=json.loads(binding_raw);compressed=raw(case/'state.json.gz',8000000)
assert hashlib.sha256(compressed).hexdigest()==binding['state_file_sha256']
with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:plain=stream.read(64000001)
assert len(plain)<=64000000
state=json.loads(plain);plan=json.loads(raw(case/'campaign-plan.json'))
assert raw(case/'state-binding.json')==binding_raw and state['state_sha256']==binding['state_sha256'] and state['revision']==binding['revision']
assert state['state_sha256']==canonical_hash({k:v for k,v in state.items() if k!='state_sha256'})
assert plan['plan_sha256']==canonical_hash({k:v for k,v in plan.items() if k!='plan_sha256'})
assert state['plan_sha256']==plan['plan_sha256'] and len(state['cells'])==240
out['canonical_state']={'binding':binding,'binding_sha256':hashlib.sha256(binding_raw).hexdigest(),'canonical_hash_and_binding_validation_passed':True,'full_public_state_validation_passed':False,
 'status':state['status'],'jobs':len(state['jobs']),'policies_complete':sum(c['complete'] for c in state['cells']),
 'training_complete':sum(j['intent']['kind']=='training' and j['status']=='complete' for j in state['jobs'].values())}
jobs={'6701493','6701494','6701495','6701498','6752621','6753241','6753455','6753750'}
intent_map={}
for name,value in state['jobs'].items():
 intent_map[value['intent']['intent_sha256']]=name
 if value['job_id'] is not None:jobs.add(value['job_id'])
 path=case/('submission-r1-training-0000.json' if name=='training-0000' else 'submission-'+name+'.json')
 entry={'state_job_id':value['job_id'],'state_status':value['status'],'intent_sha256':value['intent']['intent_sha256'],'path':str(path),'exists':path.exists()}
 if path.is_file():
  body=raw(path);journal=json.loads(body)
  entry.update(sha256=hashlib.sha256(body).hexdigest(),status=journal.get('status'),journal_job_id=journal.get('job_id'),intent_matches=journal.get('intent')==value['intent'])
  if journal.get('status')!='released' or journal.get('job_id')!=value['job_id'] or not entry['intent_matches']:entry['value']=journal
 out['journals'][name]=entry
for directory in sorted((root/'plans').glob('af-f2-*')):
 p=directory/'plan.json'
 if not p.is_file():continue
 value=json.loads(raw(p))
 if value.get('campaign_plan_sha256')!=plan['plan_sha256']:continue
 name=intent_map.get(value.get('intent_sha256'));ledger=root/'state'/(directory.name+'-submission.json')
 if name is None or state['jobs'][name]['job_id'] is None:
  entry={'path':str(p),'intent_id':name,'plan':value,'ledger_exists':ledger.exists()}
  if ledger.is_file():entry['ledger']=json.loads(raw(ledger))
  out['unbound_native_plans'].append(entry)
for name,rel in [
 ('pause','maintenance/study-af-controller-maintenance-pause-v1-20261022/receipt.json'),
 ('stock','maintenance/study-af-stock-metadata-support-v2-20261022/receipt.json'),
 ('pack_qualification','maintenance/study-af-policy-pack-qualification-support-v1-20261022/qualification.json'),
 ('figure01_qualification','maintenance/study-af-figure01-qualification-support-v1-20261022/qualification.json'),
 ('figure01_receipt','maintenance/study-af-figure01-qualification-support-v1-20261022/receipt.json'),
 ('figure01_bootstrap','maintenance/study-af-figure01-qualification-stage-v1-20261022/qualifier74-proof.json'),
 ('fresh_census','maintenance/study-af-fresh-storage-census-v2-20261022/receipt.json'),
 ('global_census','state/storage-budget.json')]:record(name,root/rel)
budget=json.loads(raw(root/'state/resource-budget.json',32000000));out['selected_cost_rows']={j:budget.get('native_jobs',{}).get(j) for j in ['6752621','6753241','6753455','6753750']}
for n,j in [('342','6752621'),('336','6753241'),('349','6753455'),('352','6753750')]:
 p=root/('logs/recovery-'+n+'-'+j+'_4294967294.log')
 if p.is_file():
  with p.open('rb') as stream:stream.seek(max(0,p.stat().st_size-5000));out['log_'+n]=stream.read(5000).decode(errors='replace')
for key,args in [('native13',['sacct','--array','-j',','.join(sorted(jobs)),'--format=JobID%64,JobIDRaw%64,State,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES,NodeList%128,Start,End,User,Account,MaxRSS','--units=K','--noheader','--parsable2']),('queue',['squeue','-h','-u','pwa209','-o','%i|%j|%T|%r|%E|%S|%M'])]:
 p=subprocess.run(args,capture_output=True,text=True,timeout=45)
 out[key]={'returncode':p.returncode,'stderr':p.stderr,'stdout':'\n'.join(x for x in p.stdout.splitlines() if key!='queue' or '|hhw-' in x)}
print('AF_POSTPAUSE_QUALIFICATION_READ '+json.dumps(out,sort_keys=True),flush=True)
PY
