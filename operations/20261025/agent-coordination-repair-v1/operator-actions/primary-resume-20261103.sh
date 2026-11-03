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
from datetime import datetime,timezone
import hashlib,importlib.util,json,os,shlex,subprocess,time
from horsehair import slurm,budget
from horsehair.core import atomic_json,read_json,canonical_hash,exclusive_file_lock
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');s=r/'maintenance/study-census-resilience-consumers-v6-20261103'
manifest_sha='a1ee7f4be2d8b872e36ec9ba314d68c8b2fe8b2d149873638e6f094700a9bc2d'
assert hashlib.sha256((s/'source-manifest.json').read_bytes()).hexdigest()==manifest_sha
manifest=read_json(s/'source-manifest.json')
for name,sha in manifest['files'].items():assert hashlib.sha256((s/name).read_bytes()).hexdigest()==sha
spec=importlib.util.spec_from_file_location('qualified_consumer_v6',s/'run-census-resilience-consumer-v2-20261024.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
spec=importlib.util.spec_from_file_location('qualified_consumer_builder_v6',s/'build-consumer-plan.py');builder=importlib.util.module_from_spec(spec);spec.loader.exec_module(builder)
q=read_json(s/'qualification.json');qjob=q['native_job_id'];assert q['source_manifest']=={'path':str(s/'source-manifest.json'),'sha256':manifest_sha}
actors=['7046213','7046249','7046368','7046404'];phase=builder.PHASES['primary']
directory=r/'plans'/phase;planpath=directory/'plan.json';ledger=r/'state'/(phase+'-submission.json')
area=s/'primary-resume-registration';area.mkdir(mode=0o700,exist_ok=False)
counter=[0]
def readonly(cmd):
 p=subprocess.run(cmd,text=True,capture_output=True,timeout=90);p.check_returncode();assert not p.stderr;return p
def runner(cmd,**kwargs):
 counter[0]+=1;prefix=area/str(counter[0]);atomic_json(Path(str(prefix)+'-intent.json'),{'command':cmd,'automatic_replay_permitted':False})
 result=subprocess.run(cmd,**kwargs,timeout=180)
 atomic_json(Path(str(prefix)+'-response.json'),{'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr});return result
with exclusive_file_lock(r/'state/af-campaign-registration.mutex',timeout=180):
 assert not directory.exists() and not ledger.exists(),'An existing plan must never be replayed'
 queue=readonly(['squeue','--local','-u','pwa209','-h','-o','%i|%j|%T|%u']).stdout.splitlines()
 assert not [x for x in queue if x.split('|')[1].startswith('hhw-primary-')],'Existing primary allocation must be positively retired first'
 native=readonly(['sacct','-X','-n','-P','-j',','.join(actors+[qjob]),'--format=JobIDRaw,State,ExitCode,ElapsedRaw,AllocCPUS,User,Account'])
 rows={v[0]:v for line in native.stdout.splitlines() if len(v:=line.split('|'))>=7}
 assert set(rows)==set(actors+[qjob])
 assert rows[qjob][1:3]==['COMPLETED','0:0'] and rows[qjob][4:]==['4','pwa209','def-ptewarie_cpu']
 assert all(rows[j][1]=='FAILED' and rows[j][2]=='1:0' and int(rows[j][3])>0 and rows[j][4:]==['1','pwa209','def-ptewarie_cpu'] for j in actors)
 atomic_json(area/'positive-prior-final.json',rows)
 budget.native_accounting(r,actors+[qjob],runner=lambda cmd,**kw:subprocess.run(cmd,**{**kw,'timeout':90}))
 mb={'path':str(s/'source-manifest.json'),'sha256':manifest_sha}
 qb={'path':str(s/'qualification.json'),'sha256':m.digest(s/'qualification.json'),'job_id':qjob}
 m.validate_qualification(qb,mb,m.bounded_native_reader(time.time()+1200))
 snapshot=read_json(r/'state/storage-budget.json');now=time.time()
 assert snapshot['status']=='measured' and 0<=now-snapshot['measured_unix']<21600
 assert 0<=now-datetime.fromisoformat(snapshot['captured_utc']).timestamp()<21600,'A genuinely fresh published census is required before restart'
 budget.require_storage_headroom(r,required_files=2000,required_bytes=10_000_000_000,reserve_files=248000,reserve_bytes=2_000_000_000_000)
 prior_support=r/'maintenance/study-census-resilience-consumers-v2-20261024'
 oldbinding=read_json(prior_support/'primary-consumer-binding.json')
 binding={**oldbinding,'source_manifest':mb,'qualification':qb,'afterany_parent':'7046404'}
 bp=s/'primary-consumer-binding.json';assert not bp.exists();atomic_json(bp,binding)
 binding,checked_manifest,pins=m.check_binding(bp,m.digest(bp))
 oldplanpath=r/'plans/study-primary-census-resilience-consumers-v2-20261024/plan.json'
 oldledgerpath=r/'state/study-primary-census-resilience-consumers-v2-20261024-submission.json'
 oldplan=read_json(oldplanpath);oldledger=read_json(oldledgerpath)
 assert oldledger['plan_sha256']==canonical_hash(oldplan)
 assert [oldledger['tasks'][t['name']]['job_id'] for t in oldplan['tasks']]==actors
 rendered=builder.render(oldplan,'primary',m.locator(bp),binding,checked_manifest,script_reader=lambda p:Path(p).read_text())
 assert rendered['plan_path']==str(planpath) and rendered['ledger_path']==str(ledger)
 m.source_guard(pins);directory.mkdir(mode=0o700)
 for path,text in rendered['scripts'].items():
  with Path(path).open('x',encoding='utf8',newline='\n') as f:f.write(text)
 atomic_json(planpath,rendered['plan']);m.source_guard(pins)
 record=slurm.submit(planpath,ledger,runner=runner);tasks=rendered['plan']['tasks'];jobs=[record['tasks'][t['name']]['job_id'] for t in tasks]
 assert len(set(jobs))==4 and not set(jobs)&set(actors)
 held={}
 for task,job in zip(tasks,jobs):
  f=dict(x.split('=',1) for x in shlex.split(readonly(['scontrol','show','job',job,'-o']).stdout) if '=' in x)
  assert f['JobId']==job and f['UserId']=='pwa209(3164768)' and f['Command']==task['script'] and f['JobName']=='hhw-'+task['name']
  assert f['Account']=='def-ptewarie_cpu' and f['JobState']=='PENDING' and f['Reason']=='JobHeldUser'
  assert f['NumCPUs']=='1' and f['MinMemoryNode'] in {'64G','65536M'} and m.seconds(f['TimeLimit'])==259200
  held[job]=f
 atomic_json(area/'held-native.json',held)
print('PRIMARY_V6 registered held jobs '+','.join(jobs),flush=True)
verified={}
for task,job in zip(tasks,jobs):
 runner(['scontrol','release',job],text=True,capture_output=True)
 f=dict(x.split('=',1) for x in shlex.split(readonly(['scontrol','show','job',job,'-o']).stdout) if '=' in x)
 assert f['JobId']==job and f['UserId']=='pwa209(3164768)' and f['Command']==task['script'] and f['JobName']=='hhw-'+task['name']
 assert f['JobState'] in {'RUNNING','COMPLETING','COMPLETED'} or f['JobState']=='PENDING' and f['Reason'] not in {'JobHeldUser','JobHeldAdmin'} and int(f['Priority'])>0
 verified[job]=f;atomic_json(area/'verified-release.json',verified)
receipt={'status':'four_primary_controllers_registered_and_released','jobs':jobs,'prior_terminal_controllers':actors,'qualification_job_id':qjob,'source_manifest_sha256':manifest_sha,'plan':str(planpath),'ledger':str(ledger),'scientific_workers_unchanged':True,'scientific_gates_changed':False}
atomic_json(area/'registration.json',receipt);print('PRIMARY_V6 '+json.dumps(receipt),flush=True)
PY
