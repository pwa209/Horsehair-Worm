#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import os,json,hashlib,subprocess,time,base64,xml.etree.ElementTree as ET
assert os.geteuid()==3164768
os.environ['TZ']='UTC';time.tzset()
r=Path('/scratch/pwa209/horsehair-worm');area=r/'maintenance/study-primary-external-census-policy-v1-20261023'
o={'checked_utc':datetime.now(timezone.utc).isoformat(),'read_only':True,'files':{},'logs':{},'suites':{}}
for p in [r/'plans/recovery-support-20261021-469/plan.json',r/'state/recovery-support-20261021-469-submission.json',
 r/'plans/recovery-support-20261021-469/recovery-469.sbatch',area/'qualification.json',area/'native-6866457/completion.json',area/'native-6866457/native-bindings.json']:
 try:
  assert not p.is_symlink();raw=p.read_bytes();assert len(raw)<8000000
  o['files'][str(p)]={'sha256':hashlib.sha256(raw).hexdigest(),'base64':base64.b64encode(raw).decode()}
 except FileNotFoundError:o['files'][str(p)]={'missing':True}
for p in (area/'native-6866457').glob('*.xml'):
 raw=p.read_bytes();assert len(raw)<1000000;t=ET.fromstring(raw);cases=list(t.iter('testcase'))
 o['suites'][p.name]={'sha256':hashlib.sha256(raw).hexdigest(),'base64':base64.b64encode(raw).decode(),'tests':len(cases),
 'failed_cases':[x.get('name') for x in cases if any(list(x.iter(k)) for k in ['failure','error','skipped'])]}
for p in [r/'logs/recovery-469-6866457_4294967294.log',r/'logs/rolling-storage-census-01-6864491_4294967294.log']:
 try:
  with p.open('rb') as f:f.seek(0,2);n=f.tell();f.seek(max(0,n-16000));raw=f.read(16000)
  o['logs'][str(p)]={'bytes':n,'tail':raw.decode(errors='replace')}
 except FileNotFoundError:o['logs'][str(p)]={'missing':True}
for key,cmd in [('queue',['squeue','-h','-j','6866457,6864491,6865503,6865525,6865619,6865674,6865697,6865706','-o','%i|%j|%T|%r|%E|%S|%M']),
 ('native',['sacct','-j','6866457,6864491','--format=JobID,State,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES,Start,End,User,Account,MaxRSS','--units=K','--noheader','--parsable2'])]:
 p=subprocess.run(cmd,capture_output=True,text=True,timeout=50);o[key]={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
print('PRIMARY_QUALIFICATION_LIVE '+json.dumps(o,sort_keys=True),flush=True)
PY
