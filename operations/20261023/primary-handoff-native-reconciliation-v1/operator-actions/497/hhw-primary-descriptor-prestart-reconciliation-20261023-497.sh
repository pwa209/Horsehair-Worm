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
payload=json.loads('{"jobs": ["6870608", "6870610", "6870628", "6870632"], "phase": "primary-external-census-controllers-v2", "pins": {"/scratch/pwa209/horsehair-worm/maintenance/study-primary-external-census-handoff-v3-20261023/native-6870245/old-controller-final-native.json": "0ee8473497fad87588ce93a4a20cdf551413eeb6a4dac5ac14bccca001f3882d", "/scratch/pwa209/horsehair-worm/maintenance/study-primary-external-census-handoff-v3-20261023/native-6870245/receipt.json": "9e525425ba063c824eed9fe40e15272d33be5ae88bc208850167b0e2d07523a1", "/scratch/pwa209/horsehair-worm/plans/primary-external-census-controllers-v2/plan.json": "2b5b77e7230b5365de91caf64f72484c456856b0526549a53400320ee7c32cb3", "/scratch/pwa209/horsehair-worm/plans/primary-external-census-controllers-v2/primary-external-census-controller-0.sbatch": "69157d93af7578a3abbf0f9663a57d44975ab8675adc13867e5b5672d01fa1c5", "/scratch/pwa209/horsehair-worm/plans/primary-external-census-controllers-v2/primary-external-census-controller-1.sbatch": "b5cfa87d46d747f15dcd2be6e3e5d991b222b854c6b9b211cf026af234486ff5", "/scratch/pwa209/horsehair-worm/plans/primary-external-census-controllers-v2/primary-external-census-controller-2.sbatch": "ec21853c8c3ea8b680a7ed1b8ab9652044504255e5e89c44574634e5961d8d7a", "/scratch/pwa209/horsehair-worm/plans/primary-external-census-controllers-v2/primary-external-census-controller-3.sbatch": "a05ab5ae921596ce703a99246dfaea0932d1f166e4f5f495549b911e2da35d3c", "/scratch/pwa209/horsehair-worm/state/primary-external-census-controllers-v2-submission.json": "0bd363318e2c4195de7f1a4bc5dc424555e26e8ff8251d4333c5c902649ce55c"}}')
r=Path('/scratch/pwa209/horsehair-worm');a=r/'maintenance/study-primary-descriptor-prestart-reconciliation-v1-20261023';a.mkdir(mode=0o700,exist_ok=False)
def save(name,value):
 with (a/name).open('x') as f:json.dump(value,f,sort_keys=True,indent=2);f.flush();os.fsync(f.fileno())
def verify():
 for name,pin in payload['pins'].items():
  p=Path(name);assert not p.is_symlink() and hashlib.sha256(p.read_bytes()).hexdigest()==pin
verify();plan=json.loads((r/'plans'/payload['phase']/'plan.json').read_bytes());ledger=json.loads((r/'state'/(payload['phase']+'-submission.json')).read_bytes())
assert [ledger['tasks'][t['name']]['job_id'] for t in plan['tasks']]==payload['jobs']
log=r/'logs/primary-external-census-controller-0-6870608_4294967294.log';raw=log.read_bytes();assert len(raw)<100000 and b'ValueError: Exact primary policy namespace required' in raw
assert not (r/'maintenance/study-primary-external-census-policy-v1-20261023/native-6870608').exists()
save('first-prestart-refusal.json',{'log':str(log),'sha256':hashlib.sha256(raw).hexdigest(),'native_area_absent':True,'scientific_result':False})
key=r'[A-Za-z][A-Za-z0-9_/:]*'
def pending(task,job):
 p=subprocess.run(['scontrol','show','job','-o',job],capture_output=True,text=True,timeout=50);p.check_returncode()
 pairs=re.findall(r'(?<!\S)('+key+r')=(.*?)(?=\s+'+key+r'=|$)',p.stdout.strip());f={k:v.strip() for k,v in pairs};assert len(f)==len(pairs)
 assert f.get('JobId')==job and f.get('UserId')=='pwa209(3164768)' and f.get('Account')=='def-ptewarie_cpu' and f.get('JobName')=='hhw-'+task['name'] and f.get('Command')==task['script']
 assert f.get('WorkDir')=='/lustre06/project/6101854/pwa209/horsehair-worm/releases/20261021-57e0eb8c4c6c-d89b31180d4c'
 assert f.get('JobState')=='PENDING' and f.get('RunTime')=='00:00:00' and f.get('StartTime') in {'Unknown','None'} and f.get('NodeList') in {'','(null)','None'} and f.get('AllocTRES')=='(null)' and f.get('Requeue')=='0' and f.get('Restarts')=='0'
 assert f.get('NumCPUs')=='1' and f.get('MinMemoryNode') in {'64G','65536M'} and f.get('TimeLimit') in {'3-00:00:00','72:00:00'} and f.get('ExitCode')=='0:0' and 'ArrayJobId' not in f and 'ArrayTaskId' not in f
 return f
save('all-three-pending-preflight.json',{job:pending(task,job) for task,job in zip(plan['tasks'][1:],payload['jobs'][1:])})
for task,job in reversed(list(zip(plan['tasks'][1:],payload['jobs'][1:]))):
 verify();before=pending(task,job);command=['scancel','--ctld','--state=PENDING','--user=pwa209','--name=hhw-'+task['name'],job]
 save('cancel-'+job+'-intent.json',{'command':command,'native_before':before,'automatic_replay_permitted':False})
 p=subprocess.run(command,capture_output=True,text=True,timeout=30);save('cancel-'+job+'-result.json',{'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr});p.check_returncode()
 for attempt in range(12):
  command=['sacct','--array','-j',job,'--format=JobID%64,JobIDRaw%64,State,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES,NodeList%128,Start,End,User,Account,MaxRSS','--units=K','--noheader','--parsable2']
  p=subprocess.run(command,capture_output=True,text=True,timeout=50);p.check_returncode();lines=[line.split('|') for line in p.stdout.splitlines() if line.strip()];assert len(lines)==1 and len(lines[0])==13
  row=lines[0];assert row[0]==row[1]==job and row[10]=='pwa209' and row[11]=='def-ptewarie_cpu' and row[2].split()[0] in {'PENDING','CANCELLED'}
  if row[2].split()[0]=='CANCELLED':
   assert row[3]=='0:0' and row[4]==row[5]=='0' and row[6]=='' and row[7]=='None assigned' and row[8] in {'None','Unknown'} and row[9] not in {'None','Unknown',''} and row[12] in {'','0','0K'}
   save('cancel-'+job+'-positive-never-started.json',{'command':command,'raw':p.stdout,'cpu_core_hours':0,'scientific_result':False});break
  assert attempt<11;time.sleep(5)
verify();save('receipt.json',{'status':'three_exact_unstarted_bad_descriptor_successors_retired','jobs':list(reversed(payload['jobs'][1:])),'first_failed_native_preserved':payload['jobs'][0],'original_jobs_modified':False,'scientific_result':False,'checked_utc':datetime.now(timezone.utc).isoformat()})
print('PRESTART_DESCRIPTOR_RECONCILIATION '+json.dumps({'status':'three_exact_unstarted_bad_descriptor_successors_retired','jobs':list(reversed(payload['jobs'][1:]))}),flush=True)

PY
