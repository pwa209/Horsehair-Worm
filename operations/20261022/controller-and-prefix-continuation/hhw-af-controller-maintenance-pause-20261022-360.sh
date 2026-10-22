#!/bin/bash
set -euo pipefail
module load StdEnv/2023 python/3.11.5
source /home/pwa209/hhw-environment/bin/activate
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src
python -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import fcntl,gzip,hashlib,io,json,os,re,subprocess,time
from horsehair.core import canonical_hash,source_identity
root=Path('/scratch/pwa209/horsehair-worm')
framework=root/'releases/20261021-6ed201653dfa-4f3903d7c26e'
case=root/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
phase='study-af-f2-registry-r1-controllers-v3-20261021'
area=root/'maintenance/study-af-controller-maintenance-pause-v1-20261022'
ids=['6701493','6701494','6701495','6701498']
descriptor=root/'maintenance/study-af-registry-repair-support-v4-20261021/native-activation-v3/descriptor-v6-registry-controller.json'
controller=descriptor.parent.parent/'run-af-agent-controller-registry-v4-20261021.py'
assert os.geteuid()==3164768 and source_identity(framework)['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
assert hashlib.sha256(descriptor.read_bytes()).hexdigest()=='9ef405cf3f012f21bdec79da147c3fc57ca89e90dbe6124cacf8a5b4a0f7c9d8'
assert hashlib.sha256(controller.read_bytes()).hexdigest()=='475a170aab2d158cfd17adb250707e1d0767086d4c50b95516c360f2222560f8'
assert not area.exists() and not area.is_symlink(),'Pause must never be replayed'

def encoded(v):return (json.dumps(v,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
def save(name,value):
 raw=value if type(value) is bytes else encoded(value)
 with (area/name).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
def read(path,maximum=2000000):
 assert path.is_file() and not path.is_symlink() and path.stat().st_uid==3164768 and path.stat().st_size<=maximum
 with path.open('rb') as stream:raw=stream.read(maximum+1)
 assert len(raw)<=maximum
 return raw
def native(job):
 p=subprocess.run(['scontrol','show','job',job,'-o'],capture_output=True,text=True,check=True,timeout=45)
 lines=[x for x in p.stdout.splitlines() if x.strip()];assert len(lines)==1
 pairs=re.findall(r'(?<!\S)(\w+)=([^\s]*)',lines[0]);fields=dict(pairs);assert len(fields)==len(pairs)
 i=ids.index(job);script=root/'plans'/phase/('af-f2-registry-r1-v3-controller-'+str(i)+'.sbatch')
 assert fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)' and fields.get('Account')=='def-ptewarie_cpu'
 assert fields.get('JobName')=='hhw-af-f2-registry-r1-v3-controller-'+str(i) and fields.get('Command')==str(script)
 assert fields.get('NumCPUs')=='1' and fields.get('MinMemoryNode')=='16G' and fields.get('TimeLimit')=='4-00:00:00'
 assert all('gpu' not in fields.get(k,'').lower() for k in ['ReqTRES','AllocTRES','TresPerNode','TresPerJob','TresPerTask'])
 task=next(t for t in control_plan['tasks'] if t['script']==str(script))
 assert control_ledger['tasks'][task['name']]['job_id']==job and canonical_hash(script.read_text())==task['script_sha256']
 return fields

control_plan=json.loads(read(root/'plans'/phase/'plan.json'))
control_ledger=json.loads(read(root/'state'/(phase+'-submission.json')))
assert canonical_hash(control_plan)==control_ledger['plan_sha256']
assert control_plan['release_identity']['sha256']==control_ledger['source_sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
before={job:native(job) for job in ids}
assert before[ids[0]]['JobState']=='RUNNING' and all(before[j]['JobState']=='PENDING' for j in ids[1:])
binding_raw=read(case/'state-binding.json');binding=json.loads(binding_raw)
state_raw=read(case/'state.json.gz',8000000)
assert hashlib.sha256(state_raw).hexdigest()==binding['state_file_sha256']
with gzip.GzipFile(fileobj=io.BytesIO(state_raw)) as stream:plain=stream.read(64000001)
assert len(plain)<=64000000
state=json.loads(plain);plan_raw=read(case/'campaign-plan.json');plan=json.loads(plan_raw)
assert state['state_sha256']==binding['state_sha256'] and state['revision']==binding['revision']
assert read(case/'state-binding.json')==binding_raw
assert state['state_sha256']==canonical_hash({k:v for k,v in state.items() if k!='state_sha256'})
assert plan['plan_sha256']==canonical_hash({k:v for k,v in plan.items() if k!='plan_sha256'})
assert state['plan_sha256']==plan['plan_sha256']==binding['campaign_plan_sha256'] and len(state['cells'])==240
area.mkdir(mode=0o700)
save('preflight.json',{'checked_utc':datetime.now(timezone.utc).isoformat(),'native':before,'state_binding':binding,
 'controller_sha256':hashlib.sha256(controller.read_bytes()).hexdigest(),'descriptor_sha256':hashlib.sha256(descriptor.read_bytes()).hexdigest(),
 'scientific_workers_to_cancel':[],'original_files_to_remove':[],'automatic_replay_permitted':False})
for name,raw in [('original-state.json.gz',state_raw),('original-state-binding.json',binding_raw),('original-campaign-plan.json',plan_raw),('original-controller-plan.json',encoded(control_plan)),('original-controller-ledger.json',encoded(control_ledger))]:save(name,raw)
completed=[]
try:
 for job in ids[1:]:
  fields=native(job);assert fields['JobState']=='PENDING'
  command=['scontrol','hold',job];save('hold-'+job+'-intent.json',{'command':command,'native_before':fields})
  p=subprocess.run(command,capture_output=True,text=True,timeout=45)
  save('hold-'+job+'-response.json',{'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr});p.check_returncode()
  fields=native(job);assert fields['JobState']=='PENDING' and fields['Reason']=='JobHeldUser'
  completed.append(job);save('hold-'+job+'-verified.json',fields)
 fields=native(ids[0]);assert fields['JobState']=='RUNNING'
 command=['scancel',ids[0]];save('cancel-controller-intent.json',{'command':command,'native_before':fields,'only_controller_cancelled':True,'scientific_workers_cancelled':[]})
 p=subprocess.run(command,capture_output=True,text=True,timeout=45)
 save('cancel-controller-response.json',{'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr});p.check_returncode()
 deadline=time.monotonic()+180;last=None;guards_free=False
 while time.monotonic()<deadline:
  last=native(ids[0])
  if last['JobState'] in {'CANCELLED','COMPLETED','FAILED','TIMEOUT'}:
   handles=[]
   try:
    for name in ['af-campaign-registration.mutex','resource-budget.mutex']:
     path=root/'state'/name;assert path.is_file() and not path.is_symlink() and path.stat().st_uid==3164768
     h=path.open('r+b');handles.append(h);fcntl.flock(h.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    guards_free=True
   except BlockingIOError:pass
   finally:
    for h in reversed(handles):h.close()
   if guards_free:break
  time.sleep(5)
 save('receipt.json',{'status':'controller_paused_original_kernel_guards_free' if guards_free else 'controller_stop_requested_waiting_for_native_cleanup_or_kernel_guards',
  'checked_utc':datetime.now(timezone.utc).isoformat(),'stopped_controller':ids[0],'native_last':last,'held_successors':completed,
  'kernel_guards_observed_available':guards_free,'guard_files_removed_or_replaced':False,'scientific_workers_cancelled':[],
  'campaign_state_rewritten':False,'requires_poststop_state_and_journal_reconciliation':True,'automatic_replay_permitted':False})
 print('AF_CONTROLLER_MAINTENANCE_PAUSE '+(area/'receipt.json').read_text(),flush=True)
except BaseException as e:
 save('failure.json',{'error_type':type(e).__name__,'error':str(e),'held_successors':completed,'automatic_replay_permitted':False,'scientific_workers_cancelled':[]})
 raise
PY
