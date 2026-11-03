#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src:"${PYTHONPATH:-}"
export PYTHONDONTWRITEBYTECODE=1
export TZ=UTC
python -B -u - <<'PY'
from pathlib import Path
import json,hashlib,subprocess,shlex,os,time,runpy,base64
from horsehair import slurm,budget
from horsehair.core import canonical_hash,source_identity,read_json,exclusive_file_lock,atomic_json
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');s=r/'maintenance/agent-coordination-repair-v8-20261103'
manifest_sha='e8a77557e9ffdbeb35c834b8aab6ad1b01874e381d677df1d3d7a6e8527ac725'
assert hashlib.sha256((s/'source-manifest.json').read_bytes()).hexdigest()==manifest_sha
manifest=read_json(s/'source-manifest.json')
for name,pin in manifest['files'].items():assert hashlib.sha256((s/name).read_bytes()).hexdigest()==pin
q=read_json(s/'qualification.json');qsha=hashlib.sha256((s/'qualification.json').read_bytes()).hexdigest()
assert q['status']=='qualified_agent_coordination_repair' and q['native_job_id'].isdigit() and q['tests']==195 and q['source_manifest_sha256']==manifest_sha
assert hashlib.sha256(Path(q['junit']['path']).read_bytes()).hexdigest()==q['junit']['sha256']
framework=r/'releases/20261021-6ed201653dfa-4f3903d7c26e';config=read_json(framework/'configs/study.json')
assert source_identity(framework)['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'

case=r/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
actors=['7495474','7495475','7495476','7495477']
area=s/'controller-registration-20261103'
area.mkdir(exist_ok=False)
assert not (r/'plans/agent-coordination-v8-resume-20261103').exists()
assert not (r/'state/agent-coordination-v8-resume-20261103-submission.json').exists()
counter=[0]
def run(cmd,**kwargs):
 counter[0]+=1;p=area/('native-900-'+str(counter[0]).zfill(3));kwargs['timeout']=180
 with Path(str(p)+'-intent.json').open('x') as f:json.dump({'command':cmd},f);f.flush();os.fsync(f.fileno())
 response=subprocess.run(cmd,**kwargs)
 with Path(str(p)+'-response.json').open('x') as f:json.dump({'returncode':response.returncode,'stdout':response.stdout,'stderr':response.stderr},f);f.flush();os.fsync(f.fileno())
 return response
def readonly(cmd):
 p=subprocess.run(cmd,text=True,capture_output=True,timeout=90);p.check_returncode();assert not p.stderr;return p
def prior_queue():
 p=readonly(['squeue','--local','-u','pwa209','-h','-o','%i|%j|%T|%u'])
 return [line for line in p.stdout.splitlines() if line.split('|')[1].startswith('hhw-agent-coordination-')]
with exclusive_file_lock(case/'coordination-controller.mutex',timeout=180):
 with exclusive_file_lock(r/'state/af-campaign-registration.mutex',timeout=180):
  assert not prior_queue(),'A prior writer is still active; no replay is permitted'
  stable=(case/'state.json.gz').read_bytes();stable_sha=hashlib.sha256(stable).hexdigest()
  atomic_json(area/'pre-registration-state.json',{'state_sha256':stable_sha,'prior_actors':actors})
  native=readonly(['sacct','-X','-n','-P','-j',','.join(actors+[q['native_job_id']]),'--format=JobIDRaw,State,ExitCode,ElapsedRaw,AllocCPUS,User,Account'])
  rows={v[0]:v for line in native.stdout.splitlines() if len(v:=line.split('|'))>=7}
  assert set(rows)==set(actors+[q['native_job_id']])
  assert rows[q['native_job_id']][1:3]==['COMPLETED','0:0'] and rows[q['native_job_id']][4:]==['4','pwa209','def-ptewarie_cpu']
  assert all(rows[j][1:3]==['COMPLETED','0:0'] and rows[j][4:]==['1','pwa209','def-ptewarie_cpu'] for j in actors)
  atomic_json(area/'positive-native-final.json',rows)
  budget.native_accounting(r,actors+[q['native_job_id']],runner=lambda cmd,**kw:subprocess.run(cmd,**{**kw,'timeout':90}))
  headroom=budget.require_storage_headroom(r,required_files=2000,required_bytes=10_000_000_000,reserve_files=248000,reserve_bytes=2_000_000_000_000)
  assert headroom['ready'] is True
  phase='agent-coordination-v8-resume-20261103';directory=r/'plans'/phase;assert not directory.exists();directory.mkdir()
  planpath=directory/'plan.json';ledger=r/'state'/(phase+'-submission.json');tasks=[]
  for index in range(4):
   name='agent-coordination-v8-resume-20261103-controller-'+str(index);script=directory/(name+'.sbatch')
   argv=['python',str(s/'coordination.py'),'--mode','run','--source-manifest-sha256',manifest_sha,
     '--qualification',str(s/'qualification.json'),'--qualification-sha256',qsha,'--segment',str(index),
     '--native-plan',str(planpath),'--native-ledger',str(ledger),'--native-script',str(script),'--native-task',name]
   command=shlex.join(argv);script_text=slurm._script(config,framework,name,command,cpus=1,memory='64G',hours=96,gpu=False)
   script_text=script_text.replace('set -euo pipefail','#SBATCH --hold\n#SBATCH --time-min=08:00:00\nset -euo pipefail',1)
   script.write_text(script_text)
   tasks.append({'name':name,'script':str(script),'script_sha256':canonical_hash(script_text),
     'depends':[] if index==0 else [tasks[-1]['name']],'external_dependencies':[q['native_job_id']] if index==0 else [],
     'dependency_kind':'afterok','resources':{'cpus':1,'memory':'64G','hours':96,'gpu':False,'array':None},
     'command':command,'coordination_manifest_sha256':manifest_sha})
  plan={'schema_version':1,'root':str(r),'phase':phase,'config_sha256':canonical_hash(config),'release_identity':source_identity(framework),'tasks':tasks}
  planpath.write_text(json.dumps(plan,indent=2,sort_keys=True)+'\n')
  record=slurm.submit(planpath,ledger,runner=run);jobs=[record['tasks'][t['name']]['job_id'] for t in tasks]
  assert len(set(jobs))==4 and not set(jobs)&set(actors)
  held={}
  for task,job in zip(tasks,jobs):
   p=readonly(['scontrol','show','job',job,'-o']);f=dict(x.split('=',1) for x in shlex.split(p.stdout) if '=' in x)
   assert f['JobId']==job and f['UserId']=='pwa209(3164768)' and f['JobState']=='PENDING' and f['Reason']=='JobHeldUser'
   assert f['JobName']=='hhw-'+task['name'] and f['Command']==task['script'] and f['Account']=='def-ptewarie_cpu'
   assert f['NumCPUs']=='1' and f['MinMemoryNode'] in ['64G','65536M'] and f['TimeLimit']=='4-00:00:00' and f['TimeMin']=='08:00:00'
   held[job]=f
  atomic_json(area/'held.json',held)
  assert hashlib.sha256((case/'state.json.gz').read_bytes()).hexdigest()==stable_sha
print('CONTROLLER_RESUME_900 four replacements registered '+','.join(jobs),flush=True)
verified={}
for task,job in zip(tasks,jobs):
 response=run(['scontrol','release',job],text=True,capture_output=True)
 p=readonly(['scontrol','show','job',job,'-o']);f=dict(x.split('=',1) for x in shlex.split(p.stdout) if '=' in x)
 assert f['JobId']==job and f['UserId']=='pwa209(3164768)' and f['Command']==task['script'] and f['JobName']=='hhw-'+task['name']
 assert f['JobState'] in ['RUNNING','COMPLETING','COMPLETED'] or f['JobState']=='PENDING' and f['Reason'] not in ['JobHeldUser','JobHeldAdmin'] and int(f['Priority'])>0
 verified[job]={'actual_unheld_native':f,'client_response':{'returncode':response.returncode,'stdout':response.stdout,'stderr':response.stderr}}
atomic_json(area/'verified-release.json',verified)
receipt={'status':'registered_and_released_four_sequential_controllers','jobs':jobs,'qualification_job':q['native_job_id'],
 'manifest_sha256':manifest_sha,'plan':str(planpath),'ledger':str(ledger),'source_only_science_unchanged':True,
 'prior_terminal_controllers':actors,'no_worker_jobs_cancelled':True,'canonical_state_unchanged_during_registration':True,
 'release_acknowledgement_basis':'actual_owned_unheld_native_states','scientific_gates_changed':False}
atomic_json(area/'registration.json',receipt)
print('CONTROLLER_RESUME_900 '+json.dumps(receipt),flush=True)
PY
