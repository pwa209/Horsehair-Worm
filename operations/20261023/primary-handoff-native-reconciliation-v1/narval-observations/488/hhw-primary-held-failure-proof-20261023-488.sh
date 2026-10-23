#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import os,json,subprocess,hashlib,base64,time
assert os.geteuid()==3164768
os.environ['TZ']='UTC';time.tzset()
r=Path('/scratch/pwa209/horsehair-worm');h=r/'maintenance/study-primary-external-census-handoff-v2-20261023/native-6868176'
o={'checked_utc':datetime.now(timezone.utc).isoformat(),'read_only':True,'files':{},'native':{},'directories':{}}
for p in [r/'plans/primary-external-census-controllers-v1/plan.json',r/'state/primary-external-census-controllers-v1-submission.json',h/'handoff-failure.json']:
 assert not p.is_symlink();raw=p.read_bytes();assert len(raw)<2000000
 o['files'][str(p)]={'sha256':hashlib.sha256(raw).hexdigest(),'base64':base64.b64encode(raw).decode()}
 if p.name=='plan.json':
  for task in json.loads(raw)['tasks']:
   s=Path(task['script']);assert not s.is_symlink();body=s.read_bytes();assert len(body)<200000
   o['files'][str(s)]={'sha256':hashlib.sha256(body).hexdigest(),'base64':base64.b64encode(body).decode()}
for job in ['6868176','6868884','6868886','6868888','6868889']:
 p=subprocess.run(['scontrol','show','job','-o',job],capture_output=True,text=True,timeout=50)
 o['native'][job]={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
with os.scandir(h) as scan:entries=[(e.name,e.is_dir(follow_symlinks=False)) for e in scan]
assert len(entries)<1000;o['directories'][str(h)]=entries
for name,isdir in entries:
 if name.startswith('new-native-') and isdir:
  path=h/name
  with os.scandir(path) as scan:children=[(e.name,e.is_dir(follow_symlinks=False)) for e in scan]
  assert len(children)<100;o['directories'][str(path)]=children
  for child,childdir in children:
   p=path/child
   if not childdir and p.suffix in {'.json','.txt'}:
    raw=p.read_bytes();assert len(raw)<2000000;o['files'][str(p)]={'sha256':hashlib.sha256(raw).hexdigest(),'base64':base64.b64encode(raw).decode()}
print('PRIMARY_HELD_FAILURE_PROOF '+json.dumps(o,sort_keys=True),flush=True)
PY
