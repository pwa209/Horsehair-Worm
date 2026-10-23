#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import os,json,hashlib,base64,subprocess,time
assert os.geteuid()==3164768
os.environ['TZ']='UTC';time.tzset()
r=Path('/scratch/pwa209/horsehair-worm')
o={'checked_utc':datetime.now(timezone.utc).isoformat(),'read_only':True,'files':{},'native':{}}
ids=[]
for n in ['429','430','431','432','433','434','451']:
 for p in [r/('plans/recovery-support-20261021-'+n+'/plan.json'),r/('state/recovery-support-20261021-'+n+'-submission.json')]:
  assert not p.is_symlink()
  raw=p.read_bytes();assert len(raw)<1000000
  v=json.loads(raw);o['files'][str(p)]={'sha256':hashlib.sha256(raw).hexdigest(),'base64':base64.b64encode(raw).decode()}
  if p.name=='plan.json':
   for task in v['tasks']:
    s=Path(task['script']);assert s.is_relative_to(r/'plans') and not s.is_symlink()
    body=s.read_bytes();assert len(body)<200000
    o['files'][str(s)]={'sha256':hashlib.sha256(body).hexdigest(),'base64':base64.b64encode(body).decode()}
  elif n!='429':ids.extend(str(t['job_id']) for t in v['tasks'].values())
ids.extend(['6864491','6864084','6632164','6632209','6632225','6632239'])
for job in ids:
 p=subprocess.run(['scontrol','show','job',job,'-o'],capture_output=True,text=True,timeout=50)
 o['native'][job]={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
for relative in ['maintenance/study-af-rolling-census-registration-v4-20261023/receipt.json',
 'maintenance/study-af-rolling-storage-census-v4-20261023/rolling-01/native-bindings.json',
 'maintenance/study-af-rolling-storage-census-v4-20261023/rolling-01/sweep-start.json',
 'maintenance/study-af-rolling-storage-census-v4-20261023/rolling-01/receipt.json']:
 p=r/relative
 try:
  assert not p.is_symlink();raw=p.read_bytes();assert len(raw)<1000000
  o['files'][str(p)]={'sha256':hashlib.sha256(raw).hexdigest(),'base64':base64.b64encode(raw).decode()}
 except FileNotFoundError:o['files'][str(p)]={'missing':True}
print('RESTORED_NATIVE_PROOF '+json.dumps(o,sort_keys=True),flush=True)
PY
