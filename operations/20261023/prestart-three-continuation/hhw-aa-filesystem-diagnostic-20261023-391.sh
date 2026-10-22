#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import os,json,subprocess,time
assert os.geteuid()==3164768
os.environ['TZ']='UTC';time.tzset()
root=Path('/scratch/pwa209/horsehair-worm')
out={'read_only':True,'checked_utc':datetime.now(timezone.utc).isoformat(),'host':subprocess.check_output(['hostname','-f'],text=True).strip(),'paths':{},'commands':{}}
paths=['/scratch/pwa209',str(root),str(root/'state'),str(root/'state/storage-budget.json'),str(root/'plans/recovery-support-20261020-527/plan.json'),str(root/'state/af-campaign-registration.mutex'),str(root/'state/resource-budget.mutex'),str(root/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1/state-binding.json'),str(root/'state/recovery-support-20261021-376-submission.json'),str(root/'plans/recovery-support-20261021-376'),str(root/'maintenance/study-af-cohort-prefix-stage-v5-r1-20261022/submission-intent.json'),str(root/'maintenance/behaviour-full-cohort-prefix-v5-20261022'),str(root/'maintenance/behaviour-full-cohort-prefix-v5-support-20261022'),str(root/'state/recovery-support-20261021-377-submission.json')]
for name in paths:
 try:
  p=Path(name);s=p.stat();v={'exists':True,'owner':s.st_uid,'bytes':s.st_size,'device':s.st_dev,'inode':s.st_ino}
  if name.endswith('.json') and s.st_size<100000:
   with p.open() as f:v['value']=json.load(f)
  out['paths'][name]=v
 except Exception as e:out['paths'][name]={'error_type':type(e).__name__,'errno':getattr(e,'errno',None),'error':str(e)}
for key,args in [('queue',['squeue','-h','-u','pwa209','-o','%i|%j|%T|%r|%E|%S|%M']),('native',['sacct','--array','-j','6811498,6632164','--format=JobID%64,JobIDRaw%64,State,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES,NodeList%128,Start,End,User,Account,MaxRSS','--units=K','--noheader','--parsable2'])]:
 try:
  p=subprocess.run(args,capture_output=True,text=True,timeout=40);out['commands'][key]={'returncode':p.returncode,'stdout':'\n'.join(l for l in p.stdout.splitlines() if '|hhw-' in l) if key=='queue' else p.stdout,'stderr':p.stderr}
 except Exception as e:out['commands'][key]={'error_type':type(e).__name__,'error':str(e)}
print('FILESYSTEM_DIAGNOSTIC '+json.dumps(out,sort_keys=True),flush=True)
PY
