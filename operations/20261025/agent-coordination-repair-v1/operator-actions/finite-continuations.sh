#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src
export PYTHONDONTWRITEBYTECODE=1
export TZ=UTC
python -B -u - <<'PY'
from pathlib import Path
import json,hashlib,subprocess,shlex,os,time,gzip
from horsehair import slurm
from horsehair.core import canonical_hash,source_identity,read_json,exclusive_file_lock
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');s=r/'maintenance/agent-coordination-repair-v1-20261025';c=r/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
pin='1612bdee43c7bfd51f172d1f51479a8110c2884edc3bf9aca99535718c6a0938'
assert hashlib.sha256((s/'source-manifest.json').read_bytes()).hexdigest()==pin
for name,sha in read_json(s/'source-manifest.json')['files'].items():assert hashlib.sha256((s/name).read_bytes()).hexdigest()==sha
# Require one completely acknowledged/released real production batch. Never
# infer success from a running parent or register continuations during a failure.
stop=time.monotonic()+360
while True:
 status=read_json(c/'status.json')
 with gzip.open(c/'state.json.gz','rt') as f:state=json.load(f)
 active={k:v for k,v in state['jobs'].items() if v['status']!='complete'}
 ready=(status.get('native_controller_job')=='7178192' and status.get('new_intentions')==11
        and state['status']=='active' and not state['needs_repair'] and all(v['job_id'] is not None for v in active.values()))
 if ready:
  for name in active:
   path=c/('submission-r3-training-0080.json' if name=='training-0080' else 'submission-'+name+'.json')
   if not path.exists() or read_json(path).get('status')!='released':ready=False;break
 if ready:break
 assert time.monotonic()<stop,'First production batch has not released; no continuation submissions made'
 time.sleep(10)
area=s/'finite-continuations-registration-751';area.mkdir();counter=[0]
def run(cmd,**kw):
 counter[0]+=1;p=area/('mutation-'+str(counter[0]).zfill(3));kw['timeout']=45
 with Path(str(p)+'-intent.json').open('x') as f:json.dump({'command':cmd},f)
 v=subprocess.run(cmd,**kw)
 with Path(str(p)+'-response.json').open('x') as f:json.dump({'returncode':v.returncode,'stdout':v.stdout,'stderr':v.stderr},f)
 return v
def observe(job):
 last=None
 for attempt in range(3):
  try:
   v=subprocess.run(['scontrol','show','job',job,'-o'],text=True,capture_output=True,timeout=30);v.check_returncode();assert not v.stderr
   return dict(x.split('=',1) for x in shlex.split(v.stdout) if '=' in x)
  except (subprocess.TimeoutExpired,subprocess.CalledProcessError) as e:last=e
 raise last
framework=r/'releases/20261021-6ed201653dfa-4f3903d7c26e';config=read_json(framework/'configs/study.json')
qsha=hashlib.sha256((s/'qualification.json').read_bytes()).hexdigest();parent='7178228';groups=[];registrations=[]
with exclusive_file_lock(r/'state/af-campaign-registration.mutex',timeout=180):
 for group in [2,3]:
  phase='agent-coordination-continuation-'+str(group)+'-20261025';directory=r/'plans'/phase;directory.mkdir()
  pp=directory/'plan.json';lp=r/'state'/(phase+'-submission.json');tasks=[]
  for index in range(4):
   name='agent-coordination-g'+str(group)+'-controller-'+str(index);script=directory/(name+'.sbatch')
   argv=['python',str(s/'coordination.py'),'--mode','run','--source-manifest-sha256',pin,
     '--qualification',str(s/'qualification.json'),'--qualification-sha256',qsha,'--segment',str(index),
     '--native-plan',str(pp),'--native-ledger',str(lp),'--native-script',str(script),'--native-task',name]
   command=shlex.join(argv);text=slurm._script(config,framework,name,command,cpus=1,memory='64G',hours=8,gpu=False)
   text=text.replace('set -euo pipefail','#SBATCH --hold\n#SBATCH --time-min=08:00:00\nset -euo pipefail',1)
   assert text.count('export HHW_MAX_HOURS=8')==1;text=text.replace('export HHW_MAX_HOURS=8','export HHW_MAX_HOURS=96')
   script.write_text(text)
   tasks.append({'name':name,'script':str(script),'script_sha256':canonical_hash(text),
     'depends':[] if index==0 else [tasks[-1]['name']],'external_dependencies':['7177258',parent] if index==0 else [],
     'dependency_kind':'afterany','resources':{'cpus':1,'memory':'64G','hours':8,'gpu':False,'array':None},
     'command':command,'coordination_manifest_sha256':pin})
  plan={'schema_version':1,'root':str(r),'phase':phase,'config_sha256':canonical_hash(config),
      'release_identity':source_identity(framework),'tasks':tasks}
  pp.write_text(json.dumps(plan,indent=2,sort_keys=True)+'\n')
  ledger=slurm.submit(pp,lp,runner=run);jobs=[ledger['tasks'][t['name']]['job_id'] for t in tasks]
  assert len(set(jobs))==4;parent=jobs[-1];groups.append({'phase':phase,'jobs':jobs,'plan':str(pp),'ledger':str(lp)})
  registrations.extend(zip(tasks,jobs))
# Reading held scheduler records and releasing controls does not require the
# global registry barrier; scientific workers can now perform admission.
held={}
for task,job in registrations:
 f=observe(job)
 assert f['JobId']==job and f['UserId']=='pwa209(3164768)' and f['JobName']=='hhw-'+task['name']
 assert f['JobState']=='PENDING' and f['Reason']=='JobHeldUser' and f['Command']==task['script']
 assert f['Account']=='def-ptewarie_cpu' and f['NumCPUs']=='1' and f['MinMemoryNode'] in ['64G','65536M'] and f['TimeLimit']==f['TimeMin']=='08:00:00'
 held[job]=f
with (area/'held.json').open('x') as f:json.dump(held,f,indent=2)
for _,job in registrations:
 v=run(['scontrol','release',job],text=True,capture_output=True);v.check_returncode();assert not v.stderr
receipt={'status':'released_two_finite_continuation_groups','groups':groups,'entry':str(s/'coordination.py'),
 'source_manifest_sha256':pin,'total_controller_slots_including_initial':12,'maximum_registered_hours':96,
 'maximum_work_window_hours':48,'parallel_controller_authorized':False,'new_worker_or_scientific_source':False}
with (s/'finite-continuations.json').open('x') as f:json.dump(receipt,f,indent=2)
print('FINITE_CONTINUATIONS_751 '+json.dumps(receipt),flush=True)
PY
