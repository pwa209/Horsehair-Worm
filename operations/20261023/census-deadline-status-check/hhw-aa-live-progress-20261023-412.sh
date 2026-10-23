#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import os,json,hashlib,subprocess,time
assert os.geteuid()==3164768
os.environ['TZ']='UTC';time.tzset()
root=Path('/scratch/pwa209/horsehair-worm')
support=root/'maintenance/study-af-registry-repair-support-v6-20261022'
campaign=root/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
phase='study-af-f2-registry-r2-controllers-v1-20261022'
out={'read_only':True,'checked_utc':datetime.now(timezone.utc).isoformat(),'host':subprocess.check_output(['hostname','-f'],text=True).strip(),'files':{},'commands':{},'log_tails':{}}
paths=[root/'state/storage-budget.json',root/'maintenance/study-af-fresh-storage-census-v4-20261022/receipt.json',root/'maintenance/study-af-fresh-storage-census-v4-20261022/sweep-result.json',root/'maintenance/study-af-fresh-storage-census-v4-20261022/sweep-failure.json',support/'private-qualification.json',support/'native-activation-prestart-three-v1/receipt.json',support/'native-activation-prestart-three-v1/descriptor-prestart-three-v1.json',root/'state'/(phase+'-submission.json'),campaign/'state-binding.json',campaign/'status.json',root/'maintenance/study-af-archive-consumer-qualification-support-v3-20261023/qualification.json',support/'native-activation-prestart-three-v1/activation-failure.json',root/'maintenance/study-af-prestart-three-recovery-v1-20261022/installation.json',campaign/'controller-failure.json',campaign/'storage-reservation.json',*[campaign/('submission-r2-'+name+'.json') for name in ('evaluation-0073-01','training-0076','training-0077')]]
values={}
for p in paths:
 try:
  assert not p.is_symlink()
  with p.open('rb') as f:raw=f.read(1000001)
  if len(raw)>1000000:out['files'][str(p)]={'exists':True,'over_read_bound':True};continue
  value=json.loads(raw);values[str(p)]=value;out['files'][str(p)]={'exists':True,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'value':value}
 except Exception as e:out['files'][str(p)]={'error_type':type(e).__name__,'errno':getattr(e,'errno',None),'error':str(e)}
ids=['6811498','6819399','6822432','6822568','6822730','6824584']
ledger=values.get(str(root/'state'/(phase+'-submission.json')),{});controllers={}
for name,row in ledger.get('tasks',{}).items():
 jid=str(row.get('job_id',''))
 if jid.isdigit():ids.append(jid);controllers[jid]=name
out['replacement_controllers']=controllers
for key,args in [('queue',['squeue','-h','-u','pwa209','-o','%i|%j|%T|%r|%E|%S|%M']),('native',['sacct','--array','-j',','.join(ids),'--format=JobID%64,JobIDRaw%64,State,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES,NodeList%128,Start,End,User,Account,MaxRSS','--units=K','--noheader','--parsable2'])]:
 try:
  p=subprocess.run(args,capture_output=True,text=True,timeout=40);out['commands'][key]={'returncode':p.returncode,'stdout':'\n'.join(l for l in p.stdout.splitlines() if '|hhw-' in l) if key=='queue' else p.stdout,'stderr':p.stderr}
 except Exception as e:out['commands'][key]={'error_type':type(e).__name__,'error':str(e)}
logs=[root/'logs'/('recovery-'+label+'-'+jid+'_4294967294.log') for label,jid in [('375','6811498'),('377','6819399'),('394','6822432'),('395','6822568'),('399','6822730'),('406','6824584')]]
logs += [root/'logs'/(name+'-'+jid+'_4294967294.log') for jid,name in controllers.items()]
for p in logs:
 try:
  with p.open('rb') as f:f.seek(0,2);n=f.tell();f.seek(max(0,n-9000));out['log_tails'][str(p)]=f.read(9000).decode(errors='replace')
 except Exception as e:out['log_tails'][str(p)]={'error_type':type(e).__name__,'errno':getattr(e,'errno',None),'error':str(e)}
print('NARVAL_LIVE_PROGRESS '+json.dumps(out,sort_keys=True),flush=True)
PY
