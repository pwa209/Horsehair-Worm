#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src:${PYTHONPATH:-}"
cd /project/def-ptewarie/pwa209/horsehair-worm/releases/20261016-5c3ae6df532d-91c8824f2adb
python -u - <<'PY'

from pathlib import Path
from datetime import datetime,timezone
import ast,base64,hashlib,json,os,re,runpy,shlex,subprocess,time,zlib
payload=json.loads(zlib.decompress(base64.b64decode('eNqtUV1qhDAQvsric9NoEqP2Aj1EKcuYjBrQJEx0Fym9e43Qbl8LffuG4fub+SicLV4uhRJd8XQpgBDyyJMhWM3E4x1E2fEpUMIJHLF7oIUv4PyKHrxBntbN7gwGRmGenR+ZQZ+2xAhHl9ZDxQXPbpKJUuiqFDLbTDhHpGxEm2cDYZpYWgPBiN/0m/5hPMf9QbqmCUStM/dYllVXSWyUrmVjQdatNKJVyjS6HHTddr1qe2y0BKWGVvaolLa1FaWCykIWNeCDdwbm6yPTP5RPW4yB1t+9+R+qmrilI0l7wAWXQPtZV72eZwgb5aU+3zVm+Pb++QXJIo/m')))
from horsehair.core import read_json,digest,source_identity,canonical_hash,atomic_json
from horsehair.slurm import _script,submit,completed_native_dependency
root=Path('/scratch/pwa209/horsehair-worm')
primary=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
framework=root/'releases/20261021-6ed201653dfa-4f3903d7c26e'
os.environ['TZ']='UTC';time.tzset()
assert os.geteuid()==3164768
config=read_json(primary/'configs/study.json');science=read_json(primary/'release.json');identity=read_json(framework/'release.json')
assert source_identity(primary)==science['source_identity'] and science['source_identity']['sha256']=='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
assert source_identity(framework)==identity['source_identity'] and identity['source_identity']['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
assert canonical_hash(config)==identity['config_sha256']==science['config_sha256']=='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35' and read_json(framework/'configs/study.json')==config
source_manifest=Path('/scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-support-v3-20261023/source-manifest.json');assert digest(source_manifest)=='43660c97e50c5a24d42abb321514405107b9f1cade649a492f1c91355486ecb8'
manifest=read_json(source_manifest)
for name,sha in manifest['files'].items():assert digest(source_manifest.parent/name)==sha,'Qualified source closure changed'
helper=source_manifest.parent/'run-fresh-storage-census-v6-20261023.py';helper_sha=digest(helper)
area=Path('/scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-registration-v3-20261023');assert area==area.resolve() and not area.exists() and not area.is_symlink();area.mkdir(mode=0o700)
gp=root/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py';assert digest(gp)=='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
runpy.run_path(str(gp))['install'](area/'submission-native-accounting-reads')
admin=source_manifest.parent/'registered-snapshot-read-guard-v4-20261022.py';assert digest(admin)=='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
api=runpy.run_path(str(admin));api['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+1500)
from horsehair.campaign import combined_admission
from horsehair import repair
policy=(root/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json','f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5')
directory=root/'plans/recovery-support-20261021-429';ledger_path=root/'state/recovery-support-20261021-429-submission.json'
qplan_path=root/'plans/recovery-support-20261021-452/plan.json';qledger_path=root/'state/recovery-support-20261021-452-submission.json'
qpath=root/'maintenance/study-af-rolling-census-qualification-v4-20261023/qualification.json'
def native_held_fields(text,task,job,record,index,qjob):
    lines=[line for line in text.splitlines() if line.strip()];assert len(lines)==1,'One native held allocation required'
    key=r'[A-Za-z][A-Za-z0-9_/:]*'
    pairs=re.findall(r'(?<!\S)('+key+r')=(.*?)(?=\s+'+key+r'=|$)',lines[0]);fields={k:v.strip() for k,v in pairs}
    assert len(fields)==len(pairs),'Duplicate native fields refused'
    assert fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)' and fields.get('Account')=='def-ptewarie_cpu'
    assert fields.get('JobName')=='hhw-'+task['name'] and fields.get('Command')==task['script'] and fields.get('JobState')=='PENDING' and fields.get('Reason')=='JobHeldUser'
    assert fields.get('NumCPUs')=='8' and fields.get('NumNodes') in {'1','1-1'} and fields.get('TimeLimit') in {'06:00:00','6:00:00'}
    assert validator['memory'](fields.get('MinMemoryNode',''))==64*1024**3 and fields.get('AllocTRES')=='(null)'
    validator['cpu_only_tres'](fields.get('ReqTRES',''))
    assert all('gpu' not in fields.get(k,'').lower() for k in ('TresPerNode','TresPerJob','TresPerTask','Gres'))
    assert 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields
    issued=record.get('issued_native_dependencies');assert type(issued) is list
    if index==0:
        assert issued==[] and fields.get('Dependency')=='(null)'
        proofs=record.get('completed_native_dependencies',[])
        assert len(proofs)==1 and proofs[0].get('job_id')==qjob and proofs[0].get('dependency_kind')=='afterok'
        assert completed_native_dependency(root,qjob) is not None,'First native edge replaced only by real positive final Q2 proof'
    else:
        expected=ids[index-1];assert issued==[expected]
        assert fields.get('Dependency') in {'afterok:'+expected,'afterok:'+expected+'(unfulfilled)'}
    eligible=fields.get('EligibleTime','')
    if eligible in {'Unknown','N/A','None'}:
        assert index==0,'Future placement must retain actual native earliest eligibility'
    else:
        observed=datetime.fromisoformat(eligible).replace(tzinfo=timezone.utc).timestamp()
        assert observed>=task['begin_not_before_unix'] and (index==0 or observed<=task['begin_not_before_unix']+1)
    return fields
with api['registration_lock'](area/'submission-registration-lock',time.time()+1500,role='controller-start'):
    assert not directory.exists() and not ledger_path.exists(),'No registration replay permitted'
    # 128*24 conservatively admits the complete 3,072-core-hour finite chain;
    # its actual DAG maximum is eight CPUs and every native allocation is6h.
    admission=combined_admission(root,config,science['release_id'],area,'submit-rolling-chain-admission',resource_policy=policy,cpus=128,gpus=0,hours=24)
    qplan=read_json(qplan_path);qledger=read_json(qledger_path)
    assert canonical_hash(qplan)==qledger['plan_sha256'] and qledger['source_sha256']==identity['source_identity']['sha256']
    qjob=str(qledger['tasks']['recovery-452']['job_id']);qsha=digest(qpath)
    observed=repair._repair_native_states([qjob],{},owner='pwa209',account='def-ptewarie_cpu')
    validator=runpy.run_path(str(helper),run_name='reviewed_rolling_registration_validator')
    qproof=validator['kernel_qualification'](qpath,qsha,qjob,source_manifest,'43660c97e50c5a24d42abb321514405107b9f1cade649a492f1c91355486ecb8',observed)
    assert completed_native_dependency(root,qjob) is not None
    quota=subprocess.run(['lfs','quota','-u','pwa209','/scratch/pwa209'],capture_output=True,text=True,timeout=30,check=True)
    rows=[x.split() for x in quota.stdout.splitlines() if x.startswith('/scratch/pwa209 ')]
    assert len(rows)==1 and int(rows[0][7])-int(rows[0][5].rstrip('*'))>=250000
    directory.mkdir(mode=0o700)
    tasks=[];start=int(time.time());previous=None
    for slot in range(1,65):
        name='rolling-storage-census-'+str(slot).zfill(2);case='rolling-'+str(slot).zfill(2);script=directory/(name+'.sbatch')
        args=['--native-plan',str(directory/'plan.json'),'--native-ledger',str(ledger_path),'--native-task',name,'--native-script',str(script),
            '--case-id',case,'--source-manifest',str(source_manifest),'--source-manifest-sha256','43660c97e50c5a24d42abb321514405107b9f1cade649a492f1c91355486ecb8',
            '--kernel-qualification',str(qpath),'--kernel-qualification-sha256',qsha,'--kernel-qualification-job-id',qjob,
            '--registry-qualification',str(root/'maintenance/study-af-registry-repair-support-v4-20261021/private-qualification.json'),
            '--registry-qualification-sha256','1f391caccd4629eb47150862f7bc2575e991e4ce5cb5aae99e955cdeae4510ca','--registry-qualification-job-id','6699561']
        command=shlex.join(['python','-u',str(helper),*args]);text=_script(config,framework,name,command,cpus=8,memory='64G',hours=6)
        begin=start+(slot-1)*21600;begin_text=datetime.fromtimestamp(begin,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
        text=text.replace('set -euo pipefail','#SBATCH --hold\n#SBATCH --begin='+begin_text+'\nset -euo pipefail\nexport PYTHONDONTWRITEBYTECODE=1',1)
        with script.open('x') as f:f.write(text);f.flush();os.fsync(f.fileno())
        tasks.append({'name':name,'script':str(script),'script_sha256':canonical_hash(text),'command':command,
            'depends':[] if previous is None else [previous],'dependency_kind':'afterok','external_dependencies':[qjob] if previous is None else [],
            'resources':{'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None},
            'rolling_census_provider':{'helper':{'path':str(helper),'sha256':helper_sha},'source_manifest':{'path':str(source_manifest),'sha256':'43660c97e50c5a24d42abb321514405107b9f1cade649a492f1c91355486ecb8'},'case_id':case},
            'begin_not_before_unix':begin,'initially_submitted_held':True})
        previous=name
    plan={'schema_version':2,'phase':'study-af-rolling-storage-census-v3-20261023','root':str(root),'config_sha256':canonical_hash(config),
        'release_identity':identity['source_identity'],'tasks':tasks,'cpu_ceiling':256,'gpu_concurrency':4,'resource_policy':admission['resource_policy'],
        'maximum_simultaneous_resources':{'cpus':8,'gpus':0}}
    atomic_json(directory/'plan.json',plan)
    atomic_json(area/'submission-intent.json',{'id':'429','helper':helper.name,'helper_sha256':helper_sha,'source_manifest':{'path':str(source_manifest),'sha256':'43660c97e50c5a24d42abb321514405107b9f1cade649a492f1c91355486ecb8'},
        'qualification':{'path':str(qpath),'sha256':qsha,'job_id':qjob},'plan_path':str(directory/'plan.json'),'ledger_path':str(ledger_path),
        'finite_slots':64,'maximum_cpu_core_hours':3072,'maximum_simultaneous_cpus':8,'not_before_interval_seconds':21600,
        'quota_stdout':quota.stdout,'automatic_replay_permitted':False,'scientific_result':False})
    submitted=submit(directory/'plan.json',ledger_path)
    ids=[str(submitted['tasks'][task['name']]['job_id']) for task in tasks]
    assert len(ids)==len(set(ids))==64 and all(x.isdigit() for x in ids)
    scheduler_path=root/'maintenance/benchmark-preflight-stage-v4-20261021/native-scheduler-read.py'
    assert digest(scheduler_path)=='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
    scheduler=runpy.run_path(str(scheduler_path))['run_scontrol_job'];verified=[]
    for index,(task,job) in enumerate(zip(tasks,ids)):
        result=scheduler(job,area/('held-native-'+job));result.check_returncode()
        fields=native_held_fields(result.stdout,task,job,submitted['tasks'][task['name']],index,qjob)
        verified.append({'job_id':job,'fields':fields})
    atomic_json(area/'held-native-verification.json',{'tasks':verified,'all_registered_before_release':True,'automatic_replay_permitted':False})
    # Release every dependent first. A lost acknowledgement stops here; the
    # first allocation remains held until all downstream release acknowledgments.
    for index,job in enumerate(reversed(ids),1):
        atomic_json(area/('release-'+str(index)+'-intent.json'),{'job_id':job,'command':['scontrol','release',job],'automatic_replay_permitted':False})
        response=subprocess.run(['scontrol','release',job],capture_output=True,text=True,timeout=30,check=False)
        atomic_json(area/('release-'+str(index)+'-response.json'),{'returncode':response.returncode,'stdout':response.stdout,'stderr':response.stderr,'job_id':job})
        response.check_returncode()
    assert canonical_hash(read_json(directory/'plan.json'))==canonical_hash(plan)
    atomic_json(area/'receipt.json',{'status':'registered_and_released_finite_rolling_census_chain','job_ids':ids,'qualification':qproof,
        'slot_count':64,'maximum_cpu_core_hours':3072,'maximum_simultaneous_cpus':8,'freshness_gaps_remain_possible':True,'automatic_replay_permitted':False,'scientific_result':False})
print('RECOVERY_STAGE_SUBMITTED '+json.dumps({'request_id':'429','area':str(area),'helper_sha256':helper_sha,'tasks':{name:{key:value.get(key) for key in ['job_id','state']} for name,value in submitted['tasks'].items()}},sort_keys=True),flush=True)

PY
