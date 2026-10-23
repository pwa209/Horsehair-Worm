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
r=Path('/scratch/pwa209/horsehair-worm');case=r/'maintenance/study-af-rolling-storage-census-v4-20261023/native-6864491'
o={'checked_utc':datetime.now(timezone.utc).isoformat(),'read_only':True,'files':{}}
for p in [case/'native-bindings.json',case/'sweep-start.json',case/'receipt.json',case/'progress-failure.json',
 r/'logs/rolling-storage-census-01-6864491_4294967294.log']:
 try:
  assert not p.is_symlink()
  with p.open('rb') as f:f.seek(0,2);size=f.tell();f.seek(max(0,size-16000));raw=f.read(16000)
  o['files'][str(p)]={'bytes':size,'tail':raw.decode(errors='replace')}
 except FileNotFoundError:o['files'][str(p)]={'missing':True}
p=r/'state/resource-budget.json';assert not p.is_symlink()
raw=p.read_bytes();assert len(raw)<10000000;v=json.loads(raw)
active={k:x for k,x in v.get('leases',{}).items() if x.get('state')=='running'}
o['budget']={'sha256':hashlib.sha256(raw).hexdigest(),'spent_cpu_core_hours':v.get('spent_cpu_core_hours'),
 'spent_gpu_hours':v.get('spent_gpu_hours'),'active':active,
 'active_cpus':sum(x['cpus'] for x in active.values()),'active_gpus':sum(x['gpus'] for x in active.values()),
 'conservative_active_cpu_reservation':sum(x['cpus']*x['max_hours'] for x in active.values()),
 'conservative_active_gpu_reservation':sum(x['gpus']*x['max_hours'] for x in active.values()),
 'is_final_accounting_refresh':False}
p=subprocess.run(['sacct','-j','6864491','--format=JobID,State,ExitCode,ElapsedRaw,MaxRSS','--units=K','--noheader','--parsable2'],capture_output=True,text=True,timeout=50)
o['native']={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
print('CENSUS_AND_BUDGET_LIVE '+json.dumps(o,sort_keys=True),flush=True)
PY
