#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import json,subprocess,re,os,hashlib,time
assert os.geteuid()==3164768
os.environ['TZ']='UTC';time.tzset()
r=Path('/scratch/pwa209/horsehair-worm');a=r/'maintenance/study-primary-descriptor-prestart-stop-v1-20261023';a.mkdir(mode=0o700,exist_ok=False)
job='6870245';script=r/'plans/recovery-support-20261021-490/recovery-490.sbatch'
source=r/'maintenance/study-primary-external-census-handoff-v3-20261023/handoff-primary-external-census-v1-20261023.py'
assert not source.is_symlink() and hashlib.sha256(source.read_bytes()).hexdigest()=='0d37f96bd93dd96266eb3f46eb66bf7bd1ecb929b1e8474f3153b1624fda50f3'
p=subprocess.run(['scontrol','show','job','-o',job],capture_output=True,text=True,timeout=50);p.check_returncode()
key=r'[A-Za-z][A-Za-z0-9_/:]*';fields=dict(re.findall(r'(?<!\S)('+key+r')=(.*?)(?=\s+'+key+r'=|$)',p.stdout.strip()));fields={k:v.strip() for k,v in fields.items()}
assert fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)' and fields.get('Account')=='def-ptewarie_cpu' and fields.get('JobName')=='hhw-recovery-490' and fields.get('Command')==str(script) and fields.get('NumCPUs')=='4' and fields.get('MinMemoryNode') in {'16G','16384M'}
assert fields.get('JobState')=='RUNNING','Administrative job is no longer running; inspect native state without replay'
command=['scancel','--ctld','--state=RUNNING','--user=pwa209','--name=hhw-recovery-490','--full','--signal=STOP',job]
intent={'checked_utc':datetime.now(timezone.utc).isoformat(),'reason':'Actual emitted run descriptor is outside the qualified consumer namespace; prevent further administrative release','native_before':fields,'command':command,'scientific_worker':False,'automatic_replay_permitted':False}
with (a/'stop-intent.json').open('x') as f:json.dump(intent,f,sort_keys=True,indent=2);f.flush();os.fsync(f.fileno())
q=subprocess.run(command,capture_output=True,text=True,timeout=30)
result={'returncode':q.returncode,'stdout':q.stdout,'stderr':q.stderr,'native_job_id':job,'signal':'STOP','termination_requested':False}
with (a/'stop-result.json').open('x') as f:json.dump(result,f,sort_keys=True,indent=2);f.flush();os.fsync(f.fileno())
print('ADMINISTRATIVE_STOP '+json.dumps(result,sort_keys=True),flush=True);q.check_returncode()
PY
