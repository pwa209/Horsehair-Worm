#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import os,json,hashlib,base64,subprocess,time,gzip,xml.etree.ElementTree as ET
assert os.geteuid()==3164768
os.environ['TZ']='UTC';time.tzset()
r=Path('/scratch/pwa209/horsehair-worm');a=r/'maintenance/study-primary-external-census-handoff-v3-20261023'
o={'checked_utc':datetime.now(timezone.utc).isoformat(),'read_only':True,'files':{},'logs':{},'suites':{}};ids=['6867492','6864491','6866457','6865503','6865525','6865619','6865674','6865697','6865706','6632164','6632209','6632225','6632239']
def read(p,limit=8000000):
 assert not p.is_symlink()
 with p.open('rb') as f:raw=f.read(limit+1)
 assert len(raw)<=limit
 return raw
def capture(p):
 try:
  raw=read(p);o['files'][str(p)]={'sha256':hashlib.sha256(raw).hexdigest(),'base64':base64.b64encode(raw).decode()};return json.loads(raw) if p.suffix=='.json' else None
 except FileNotFoundError:o['files'][str(p)]={'missing':True}
plans=[r/'plans/primary-external-census-controllers-v1/plan.json',r/'plans/recovery-support-20261021-490/plan.json',r/'plans/primary-external-census-controllers-v2/plan.json',r/'plans/study-af-f2-registry-r2-controllers-v1-20261022/plan.json',r/'plans/study-af-f2-external-census-controllers-v1-20261023/plan.json']
handoff=None
for p in plans:
 plan=capture(p)
 if plan is None:continue
 phase=plan['phase'];lp=r/('state/'+phase+'-submission.json' if phase!='recovery-support' else 'state/recovery-support-20261021-490-submission.json')
 ledger=capture(lp)
 for task in plan['tasks']:capture(Path(task['script']))
 if ledger:
  jobs=[str(row['job_id']) for row in ledger['tasks'].values()];ids.extend(jobs)
  if phase=='recovery-support':handoff=jobs[0]
for p in [r/'maintenance/study-primary-external-census-policy-v1-20261023/native-6866457'/('baseline_budget-module-origins.json'),
 r/'maintenance/study-primary-external-census-policy-v1-20261023/native-6866457/baseline_pipeline-module-origins.json',
 r/'maintenance/study-primary-external-census-policy-v1-20261023/native-6866457/primary_policy-module-origins.json',
 r/'maintenance/study-af-rolling-storage-census-v4-20261023/native-6864491/receipt.json',
 r/'maintenance/study-af-registry-repair-support-v6-20261022/private-qualification.json']:
 capture(p)
if handoff:
 h=a/('native-'+handoff)
 if h.is_dir():
  members=list(h.glob('*'));assert len(members)<1000
  for member in members:
   if member.is_file() and member.suffix in {'.json','.txt'}:capture(member)
 for name in ['receipt.json','run-policy.json','handoff-failure.json','native-handoff-qualification.json','all-four-new-held-native-proof.json','old-controller-final-native.json','release-intent.json','native-own-bindings.json','partial-reconciliation.json','partial-all-held-preflight.json']:
  capture(h/name)
 for p in h.glob('*.xml'):
  raw=read(p,1000000);t=ET.fromstring(raw);cases=list(t.iter('testcase'))
  o['suites'][str(p)]={'sha256':hashlib.sha256(raw).hexdigest(),'base64':base64.b64encode(raw).decode(),'tests':len(cases),
   'failed_cases':[x.get('name') for x in cases if any(list(x.iter(k)) for k in ['failure','error','skipped'])]}
 paths=[r/('logs/recovery-490-'+handoff+'_4294967294.log'),h/'native-selftest-stderr.txt',h/'native-selftest-stdout.txt']
else:paths=[]
paths.append(r/'logs/rolling-storage-census-01-6864491_4294967294.log')
for name in ['receipt.json','handoff-failure.json','native-handoff-qualification.json']:
 capture(r/'maintenance/study-af-external-census-handoff-v1-20261023/native-6865706'/name)
paths.append(r/'logs/recovery-451-6865706_4294967294.log')

for p in paths:
 try:
  with p.open('rb') as f:f.seek(0,2);n=f.tell();f.seek(max(0,n-16000));raw=f.read(16000)
  o['logs'][str(p)]={'bytes':n,'tail':raw.decode(errors='replace')}
 except FileNotFoundError:o['logs'][str(p)]={'missing':True}
for phase in ['primary-external-census-controllers-v2','study-af-f2-external-census-controllers-v1-20261023']:
 p=r/'plans'/phase/'plan.json';lp=r/'state'/(phase+'-submission.json')
 if p.is_file() and lp.is_file():
  plan=json.loads(read(p));ledger=json.loads(read(lp));first=plan['tasks'][0];jid=ledger['tasks'].get(first['name'],{}).get('job_id')
  if jid:
   if phase=='primary-external-census-controllers-v2':
    for name in ['native-bindings.json','startup-pending.json','source-preservation.json','completion.json']:
     capture(r/'maintenance/study-primary-external-census-policy-v1-20261023'/('native-'+str(jid))/name)
   log=r/'logs'/(first['name']+'-'+str(jid)+'_4294967294.log')
   try:
    with log.open('rb') as f:f.seek(0,2);n=f.tell();f.seek(max(0,n-16000));body=f.read(16000)
    o['logs'][str(log)]={'bytes':n,'tail':body.decode(errors='replace')}
   except FileNotFoundError:o['logs'][str(log)]={'missing':True}
 p=r/'maintenance/study-af-external-census-handoff-v1-20261023/native-6865706/handoff-contracts.xml'
 if p.is_file() and str(p) not in o['suites']:
  body=read(p,1000000);tree=ET.fromstring(body);cases=list(tree.iter('testcase'));o['suites'][str(p)]={'sha256':hashlib.sha256(body).hexdigest(),'base64':base64.b64encode(body).decode(),'tests':len(cases),'failed_cases':[v.get('name') for v in cases if any(list(v.iter(k)) for k in ['failure','error','skipped'])]}
for key,cmd in [('queue',['squeue','-h','-j',','.join(dict.fromkeys(ids)),'-o','%i|%j|%T|%r|%E|%S|%M']),
 ('native',['sacct','-j',','.join(dict.fromkeys(ids)),'--format=JobID%64,JobIDRaw%64,State,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES,NodeList%128,Start,End,User,Account,MaxRSS','--units=K','--noheader','--parsable2'])]:
 p=subprocess.run(cmd,capture_output=True,text=True,timeout=50);o[key]={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
capture(r/'state/pipeline/20261013-829e3817bee1-0e2b1ebce8bf/status.json')
raw=read(r/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1/state.json.gz',20000000);state=json.loads(gzip.decompress(raw));o['campaign']={'revision':state.get('revision'),'sha256':hashlib.sha256(raw).hexdigest()}
print('PRIMARY_HANDOFF_LIVE '+json.dumps(o,sort_keys=True),flush=True)
PY
