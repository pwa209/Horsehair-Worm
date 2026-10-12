"""Package committed public source and build a scoped immutable Narval request.

No biological data, private proposal, credentials, Git internals or local work
artifacts enter the release. The generated request is an ignored local artifact.
"""
import argparse
import base64
import gzip
import hashlib
import io
import json
from pathlib import Path,PurePosixPath
import subprocess
import tarfile
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from horsehair.core import source_identity,canonical_hash,read_json,validate_config

p=argparse.ArgumentParser()
p.add_argument('--git',required=True)
p.add_argument('--request-id',required=True)
p.add_argument('--output',required=True,type=Path)
queue=p.add_mutually_exclusive_group()
queue.add_argument('--queue',action='store_true')
queue.add_argument('--managed',action='store_true')
p.add_argument('--transfers',action='store_true')
a=p.parse_args()
repo=Path(__file__).resolve().parents[1]
if not a.request_id.replace('-','').isalnum():raise ValueError('Invalid request ID')
def git(*args):return subprocess.check_output([a.git,*args],cwd=repo).decode().strip()
if git('status','--porcelain','--untracked-files=normal'):raise ValueError('Commit reviewed public source before packaging')
commit=git('rev-parse','HEAD')
identity=source_identity(repo)
files=[x for x in git('ls-files','-z').split('\0') if x]
for name in files:
    path=PurePosixPath(name)
    if path.is_absolute() or '..' in path.parts or name.startswith(('.git/','work/','raw/','data/','results/','.venv/')) or path.suffix.lower() in ['.docx','.pdf']:
        raise ValueError('Private or generated file appears in tracked release')
    if (repo/name).is_symlink():raise ValueError('Release symlinks are forbidden')
c=read_json(repo/'configs/study.json');validate_config(c)
release_id='20261012-'+commit[:12]+'-'+identity['sha256'][:12]
record={'schema_version':1,'release_id':release_id,'git_commit':commit,'github':'https://github.com/pwa209/Horsehair-Worm','config_sha256':canonical_hash(c),'source_identity':identity}
buffer=io.BytesIO()
with tarfile.open(fileobj=buffer,mode='w') as archive:
    for name in files:
        data=(repo/name).read_bytes();entry=tarfile.TarInfo(name);entry.size=len(data);entry.mode=0o644;archive.addfile(entry,io.BytesIO(data))
    data=(json.dumps(record,indent=2,sort_keys=True)+'\n').encode();entry=tarfile.TarInfo('release.json');entry.size=len(data);entry.mode=0o644;archive.addfile(entry,io.BytesIO(data))
data=gzip.compress(buffer.getvalue(),mtime=0)
payload={'base64':base64.b64encode(data).decode(),'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}
body=r'''#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
python - <<'PY'
import base64,hashlib,io,json,pathlib,tarfile,subprocess,os,time,sys
payload=PAYLOAD
record=RECORD
c=CONFIG
root=pathlib.Path('/scratch/pwa209/horsehair-worm')
project=pathlib.Path('/project/def-ptewarie/pwa209/horsehair-worm')
release=project/'releases'/record['release_id']
data=base64.b64decode(payload['base64'])
assert len(data)==payload['bytes'] and hashlib.sha256(data).hexdigest()==payload['sha256']
if release.exists():raise FileExistsError('Immutable release already exists: inspect receipt without replay')
release.mkdir(parents=True)
with tarfile.open(fileobj=io.BytesIO(data),mode='r:gz') as archive:
 for member in archive.getmembers():
  path=pathlib.PurePosixPath(member.name)
  if not member.isfile() or path.is_absolute() or '..' in path.parts:raise ValueError('Unsafe release member')
  destination=release/member.name;destination.parent.mkdir(parents=True,exist_ok=True)
  with destination.open('xb') as output:output.write(archive.extractfile(member).read())
os.environ['PYTHONPATH']=str(release/'src')+os.pathsep+os.environ.get('PYTHONPATH','')
sys.path.insert(0,str(release/'src'))
prefix=['python','-m','horsehair.cli','--config','configs/study.json','--root',str(root)]
subprocess.run(prefix+['verify-source','--identity','release.json'],cwd=release,check=True)
if MANAGED:
 active=subprocess.run(['squeue','-u','pwa209','--noheader','--format=%i|%j|%T'],text=True,capture_output=True,check=True)
 study_active=[line for line in active.stdout.splitlines() if len(line.split('|'))==3 and line.split('|')[1].startswith('hhw-')]
 if study_active:raise RuntimeError('Existing study jobs must finish or be explicitly superseded before switching ledger lock protocols: '+repr(study_active))
budget=root/'state/resource-budget.json'
known=list(json.loads(budget.read_text()).get('leases',{})) if budget.exists() else []
for old_ledger in (root/'state').glob('*submission*.json'):
 old=json.loads(old_ledger.read_text())
 if old.get('job_id'):known.append(str(old['job_id']))
 known.extend(str(task['job_id']) for task in old.get('tasks',{}).values() if task.get('state')=='submitted')
subprocess.run(prefix+['accounting','--job-ids','5380869','5381567',*known],cwd=release,check=True)
if MANAGED and budget.with_suffix('.lock').exists():
 legacy=budget.with_suffix('.lock')
 observed=json.loads(budget.read_text())
 remaining=[ident for ident,lease in observed.get('leases',{}).items() if lease.get('state')=='running' and ident not in observed.get('native_jobs',{})]
 if remaining:raise RuntimeError('Legacy running lease lacks native terminal proof: '+repr(remaining))
 archive=root/'state'/('legacy-resource-lock-'+record['release_id']+'.json')
 archive.write_text(json.dumps({'legacy_lock_sha256':hashlib.sha256(legacy.read_bytes()).hexdigest(),'legacy_lock_mtime':legacy.stat().st_mtime,'native_terminal_lease_ids':list(observed.get('native_jobs',{})),'no_active_study_jobs':True,'new_guard':'resource-budget.mutex','recovery_class':'One-time transition after positive native terminal reconciliation'},indent=2)+'\n')
 legacy.rename(root/'state'/('legacy-resource-lock-'+record['release_id']+'.preserved'))
deployment={'status':'deployed','release':str(release),'release_id':record['release_id'],'git_commit':record['git_commit'],'source_sha256':record['source_identity']['sha256'],'config_sha256':record['config_sha256'],'created_unix':time.time()}
if QUEUE or MANAGED:
 assert json.loads((root/'state/environment.json').read_text())['status']=='complete','Environment bootstrap is not complete'
 phase='managed-initial' if MANAGED else 'initial'
 plan=root/'plans'/(record['release_id']+'-'+phase)
 subprocess.run(prefix+['slurm-plan','--release',str(release),'--output',str(plan),'--phase',phase],cwd=release,check=True)
 if MANAGED:
  from horsehair.pipeline import prospective_capacity,prospective_registered_budget
  initial={'initial':json.loads((plan/'plan.json').read_text())}
  capacity=prospective_capacity(initial,{},{})
  assert capacity['cpus']<=c['resources']['cpu_ceiling'] and capacity['gpus']<=c['resources']['gpu_concurrency'],'Initial managed graph exceeds concurrent study bounds'
  deployment['prospective_initial_capacity']=capacity
  allocations=prospective_registered_budget(initial,{},{},json.loads(budget.read_text()))
  assert allocations['cpu_core_hours']<=c['resources']['cpu_core_hours_cap'] and allocations['gpu_hours']<=c['resources']['gpu_hours_cap'],'Initial registered allocation bounds exceed the study budget'
  deployment['prospective_initial_budget']=allocations
  deployment['managed_phase_graph']=[name for name,_,_ in __import__('horsehair.pipeline',fromlist=['NODES']).NODES]
 ledger=root/'state'/(record['release_id']+'-'+phase+'-submission.json')
 subprocess.run(prefix+['submit','--plan',str(plan/'plan.json'),'--ledger',str(ledger)],cwd=release,check=True)
 deployment['submission_ledger']=str(ledger);deployment['tasks']=json.loads(ledger.read_text())['tasks']
if TRANSFERS:
 ledger=root/'state'/('transfer-start-'+record['release_id']+'.json')
 if ledger.exists():raise FileExistsError('Transfer launch already recorded; never replay ambiguity')
 ledger.write_text(json.dumps({'status':'starting','release':str(release),'created_unix':time.time()}))
 log=root/'logs'/('transfer-launcher-'+record['release_id']+'.log')
 with log.open('x') as output:
  proc=subprocess.Popen(['nohup','python','scripts/start_transfers.py','--root',str(root),'--config','configs/study.json','--identity','release.json'],cwd=release,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
 ledger.write_text(json.dumps({'status':'started','pid':proc.pid,'release':str(release),'created_unix':time.time(),'log':str(log)}))
 deployment['transfer_launch']=json.loads(ledger.read_text())
(root/'state'/('deployment-'+record['release_id']+'.json')).write_text(json.dumps(deployment,indent=2)+'\n')
(project/('deployment-'+record['release_id']+'.json')).write_text(json.dumps(deployment,indent=2)+'\n')
print('DEPLOYMENT_RECEIPT',json.dumps(deployment))
PY
'''
body=body.replace('PAYLOAD',repr(payload)).replace('RECORD',repr(record)).replace('CONFIG',repr(c)).replace('QUEUE',str(a.queue)).replace('MANAGED',str(a.managed)).replace('TRANSFERS',str(a.transfers))
a.output.parent.mkdir(parents=True,exist_ok=True)
target=a.output/(a.request_id+'.sh')
with target.open('x',encoding='utf8',newline='\n') as output:output.write(body)
(a.output/(a.request_id+'.release.json')).write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps({'request':str(target),'release_id':release_id,'source_sha256':identity['sha256'],'archive_bytes':len(data)}))
