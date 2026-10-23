#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import os,json,hashlib,subprocess,time,gzip,xml.etree.ElementTree as ET
assert os.geteuid()==3164768
os.environ['TZ']='UTC';time.tzset()
r=Path('/scratch/pwa209/horsehair-worm');q=r/'maintenance/study-af-rolling-census-qualification-v5-20261023';c=r/'maintenance/study-af-reviewed-registration-coordinator-v4-20261023/coordinator-execution'
o={'checked_utc':datetime.now(timezone.utc).isoformat(),'read_only':True,'files':{},'registrations':{},'suites':{},'logs':{}};ids=['6863966','6864084']
def packet(p,limit=8000000):
 assert not p.is_symlink()
 with p.open('rb') as f:raw=f.read(limit+1)
 assert len(raw)<=limit
 return raw
for p in [q/'qualification.json',q/'native-canary.json',c/'receipt.json',*[c/('child-'+n+'-'+suffix+'.json') for n in ['429','430','431','432','433','434','451'] for suffix in ['completion','failure']]]:
 try:
  raw=packet(p);o['files'][str(p)]={'sha256':hashlib.sha256(raw).hexdigest(),'value':json.loads(raw)}
 except Exception as e:o['files'][str(p)]={'error_type':type(e).__name__,'errno':getattr(e,'errno',None),'error':str(e)}
for n in ['425','429','430','431','432','433','434','451']:
 p=r/('state/recovery-support-20261021-'+n+'-submission.json');pl=r/('plans/recovery-support-20261021-'+n+'/plan.json')
 try:
  raw=packet(p);v=json.loads(raw);pr=packet(pl);plan=json.loads(pr);rows={k:{x:t.get(x) for x in ['job_id','state','issued_native_dependencies']} for k,t in v['tasks'].items()};o['registrations'][n]={'ledger_sha256':hashlib.sha256(raw).hexdigest(),'plan_sha256':hashlib.sha256(pr).hexdigest(),'tasks':rows};ids.extend(str(t['job_id']) for t in rows.values() if str(t.get('job_id','')).isdigit())
 except Exception as e:o['registrations'][n]={'error_type':type(e).__name__,'errno':getattr(e,'errno',None),'error':str(e)}
for p in q.glob('*.xml'):
 raw=packet(p);t=ET.fromstring(raw);cases=list(t.iter('testcase'));o['suites'][p.name]={'sha256':hashlib.sha256(raw).hexdigest(),'tests':len(cases),'failed_cases':[x.get('name') for x in cases if any(list(x.iter(k)) for k in ['failure','error','skipped'])]}
for key,cmd in [('queue',['squeue','-h','-j',','.join(dict.fromkeys(ids)),'-o','%i|%j|%T|%r|%E|%S|%M']),('native',['sacct','--array','-j',','.join(dict.fromkeys(ids)),'--format=JobID%64,JobIDRaw%64,State,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES,Start,End,User,Account,MaxRSS','--units=K','--noheader','--parsable2'])]:
 p=subprocess.run(cmd,capture_output=True,text=True,timeout=50);o[key]={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
logs=[r/'logs/recovery-456-6863966_4294967294.log',r/'logs/recovery-425-6864084_4294967294.log']
first=o['registrations'].get('429',{}).get('tasks',{}).get('rolling-storage-census-01',{}).get('job_id')
if first:logs.append(r/('logs/rolling-storage-census-01-'+first+'_4294967294.log'))
for p in logs:
 try:
  with p.open('rb') as f:f.seek(0,2);length=f.tell();f.seek(max(0,length-16000));raw=f.read(16000)
  o['logs'][str(p)]={'bytes':length,'tail':raw.decode(errors='replace')}
 except Exception as e:o['logs'][str(p)]={'error':str(e)}
p=r/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1/state.json.gz';raw=packet(p,20000000);state=json.loads(gzip.decompress(raw));o['campaign']={'revision':state.get('revision'),'sha256':hashlib.sha256(raw).hexdigest()}
print('RESTORATION_LIVE '+json.dumps(o,sort_keys=True),flush=True)
PY
