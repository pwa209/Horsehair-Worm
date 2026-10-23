#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import os,json,subprocess,hashlib,time
os.environ["TZ"]="UTC";time.tzset()
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');q=r/'maintenance/study-af-rolling-census-qualification-v4-20261023'
out={'checked_utc':datetime.now(timezone.utc).isoformat(),'host':subprocess.check_output(['hostname','-f'],text=True).strip(),'read_only':True,'files':{}}
for p in [q/'qualification.json',q/'native-canary.json',q/'failure.json']:
 try:
  with p.open('rb') as f:raw=f.read(4000001)
  assert len(raw)<=4000000 and not p.is_symlink()
  out['files'][str(p)]={'sha256':hashlib.sha256(raw).hexdigest(),'value':json.loads(raw)}
 except Exception as e:out['files'][str(p)]={'type':type(e).__name__,'errno':getattr(e,'errno',None),'message':str(e)}
for key,cmd in [('queue',['squeue','-h','-j','6861567','-o','%i|%j|%T|%r|%E|%S|%M']),('estimate',['squeue','--start','-h','-j','6861567','-o','%i|%j|%T|%r|%S|%N']),('native',['sacct','--array','-j','6861567','--format=JobID%64,JobIDRaw%64,State,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES,NodeList%128,Start,End,User,Account,MaxRSS','--units=K','--noheader','--parsable2'])]:
 p=subprocess.run(cmd,capture_output=True,text=True,timeout=40);out[key]={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
out['qualification_files']=[str(p.relative_to(q)) for p in q.glob('*') if p.is_file()] if q.exists() else []
log=r/'logs/recovery-452-6861567_4294967294.log'
try:
 with log.open('rb') as f:f.seek(0,2);n=f.tell();f.seek(max(0,n-12000));out['log_tail']=f.read(12000).decode(errors='replace')
except Exception as e:out['log_error']=str(e)
print('STREAMING_Q_PROGRESS '+json.dumps(out,sort_keys=True),flush=True)
PY
