#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src
export PYTHONDONTWRITEBYTECODE=1 TZ=UTC
python -B -u - <<'PY'
from pathlib import Path
import hashlib,json,os,shlex,subprocess
from horsehair import budget,slurm
from horsehair.core import read_json,atomic_json,canonical_hash,exclusive_file_lock,source_identity
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');base=r/'maintenance/storage-reserve-200k-v3-20261106';reg=base/'prepare-registration'
phase='storage-reserve-200k-v3-prepare-20261106';name='storage-reserve-200k-prepare';planpath=r/'plans'/phase/'plan.json';ledgerpath=r/'state'/(phase+'-submission.json')
framework=r/'releases/20261021-6ed201653dfa-4f3903d7c26e';assert source_identity(framework)['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
plan=read_json(planpath);assert len(plan['tasks'])==1;task=plan['tasks'][0];assert task['name']==name and task['script_sha256']==canonical_hash(Path(task['script']).read_text())
assert task['external_dependencies']==['8630687','8632769','8632772']
response=read_json(reg/'mutation-1.response.json');intent=read_json(reg/'mutation-1.intent.json')
expected_note='sbatch: error: NOTE: Your memory request of 16384.0M was likely submitted as 16.0G. Please note that Slurm interprets memory requests denominated in G as multiples of 1024M, not 1000M.\n'
assert response=={'returncode':1,'stdout':'','stderr':expected_note+'sbatch: error: Batch job submission failed: Job dependency problem\n'}
assert intent['command']==['sbatch','--parsable','--dependency=afterok:8630687:8632769:8632772',task['script']]
assert not (reg/'accepted-held.json').exists() and not (reg/'complete.json').exists()
area=reg/'explicit-rejection-recovery';area.mkdir(mode=0o700,exist_ok=False)
def bounded(cmd,**kwargs):return subprocess.run(cmd,**{**kwargs,'timeout':60})
def native(job):
 a=bounded(['scontrol','show','job',job,'-o'],capture_output=True,text=True);a.check_returncode();return dict(t.split('=',1) for t in shlex.split(a.stdout) if '=' in t)
def absent_queued_script():
 a=bounded(['squeue','--local','-u','pwa209','-h','-o','%i|%j|%T|%o'],capture_output=True,text=True);a.check_returncode()
 assert not [row for row in a.stdout.splitlines() if row.split('|')[-1]==task['script']],'An existing owned submission must be reconciled, never duplicated'
 return a.stdout
with exclusive_file_lock(r/'state/af-campaign-registration.mutex',timeout=180):
 absent=absent_queued_script()
 # Persist true completed parents before Slurm resolves dependency flags.
 budget.native_accounting(r,task['external_dependencies'],runner=bounded)
 proof=slurm.completed_native_dependency(r,'8630687');assert proof is not None and proof['dependency_kind']=='afterok'
 atomic_json(area/'expired-parent-positive-completion.json',proof)
 with exclusive_file_lock(ledgerpath.with_suffix('.mutex'),timeout=180):
  raw=ledgerpath.read_bytes();ledger=json.loads(raw);assert ledger['plan_sha256']==canonical_hash(plan)
  assert set(ledger['tasks'])=={name};row=ledger['tasks'][name]
  assert row['state']=='ambiguous_or_rejected' and 'job_id' not in row and row['stdout']=='' and row['stderr']==response['stderr'] and row['command']==intent['command']
  assert 'rejected_submission_history' not in ledger
  with (area/'original-ledger.json').open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
  atomic_json(area/'certain-rejection-proof.json',{'response':response,'original_intent':intent,'old_ledger_sha256':hashlib.sha256(raw).hexdigest(),'no_matching_live_script':True,'scheduler_response_explicitly_rejected':True,'accepted_job_id':None,'plan_unchanged':True})
  del ledger['tasks'][name]
  ledger['rejected_submission_history']=[{'task':name,'original_ledger':{'path':str(area/'original-ledger.json'),'sha256':hashlib.sha256(raw).hexdigest()},'reason':'Explicit native nonzero rejection with empty stdout; expired successful parent now has actual persisted completion proof','original_response':{'path':str(reg/'mutation-1.response.json'),'sha256':hashlib.sha256((reg/'mutation-1.response.json').read_bytes()).hexdigest()}}]
  atomic_json(ledgerpath,ledger)
 absent_queued_script();counter=[0]
 def runner(cmd,**kwargs):
  counter[0]+=1;p=area/('mutation-'+str(counter[0]));atomic_json(p.with_suffix('.intent.json'),{'command':cmd,'automatic_replay_permitted':False})
  result=bounded(cmd,**kwargs);atomic_json(p.with_suffix('.response.json'),{'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr});return result
 result=slurm.submit(planpath,ledgerpath,runner=runner);job=result['tasks'][name]['job_id'];f=native(job)
 assert f['JobId']==job and f['UserId']=='pwa209(3164768)' and f['Account']=='def-ptewarie_cpu' and f['JobState']=='PENDING' and f['Reason']=='JobHeldUser'
 assert f['Command']==task['script'] and f['JobName']=='hhw-'+name and f['NumCPUs']=='2' and f['NumNodes'] in {'1','1-1'} and f['MinMemoryNode'] in {'16G','16384M'} and f['TimeLimit']=='01:00:00' and f['Requeue']=='0' and 'ArrayJobId' not in f
 assert all('gpu' not in f.get(k,'').lower() for k in ['ReqTRES','AllocTRES','Gres','TresPerNode','TresPerTask'])
 req=base/'preparation-request.json';source=base/'prepare_study.py'
 receipt={'status':'finite_preparation_registered_held','job_id':job,'plan':str(planpath),'ledger':str(ledgerpath),'request':{'path':str(req),'sha256':hashlib.sha256(req.read_bytes()).hexdigest()},'source':{'path':str(source),'sha256':hashlib.sha256(source.read_bytes()).hexdigest()},'actual_held_native':f,'request_id':'hhw-storage-reserve-prepare-repair-20261106-1008','recovery_request_id':'hhw-recover-rejected-preparation-20261106-1010','prior_native_submission_explicitly_rejected':True,'duplicate_submission_performed':False}
 atomic_json(reg/'accepted-held.json',receipt)
 a=runner(['scontrol','release',job],capture_output=True,text=True);a.check_returncode();after=native(job)
 assert after['JobId']==job and after['UserId']=='pwa209(3164768)' and after['Command']==task['script'] and (after['JobState'] in {'RUNNING','COMPLETING','COMPLETED'} or after['JobState']=='PENDING' and after['Reason'] not in {'JobHeldUser','JobHeldAdmin'} and int(after.get('Priority','0'))>0)
 receipt={**receipt,'status':'finite_preparation_registered_and_released','actual_unheld_native':after};atomic_json(reg/'complete.json',receipt)
print('STORAGE_RESERVE_PREPARATION '+json.dumps({k:v for k,v in receipt.items() if k not in {'actual_held_native','actual_unheld_native'}}),flush=True)
PY
