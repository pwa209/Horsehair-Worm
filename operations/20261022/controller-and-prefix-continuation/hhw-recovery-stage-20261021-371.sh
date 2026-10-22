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
import ast,base64,hashlib,json,os,runpy,shlex,zlib
from horsehair.core import read_json,digest,source_identity,canonical_hash,atomic_json
from horsehair.slurm import _script,submit
payload=json.loads(zlib.decompress(base64.b64decode('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')))
root=Path('/scratch/pwa209/horsehair-worm');primary=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
old=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261016-5c3ae6df532d-91c8824f2adb')
framework=Path(payload['framework']);config=read_json(primary/'configs/study.json');identity=read_json(framework/'release.json');science=read_json(primary/'release.json')
assert os.geteuid()==3164768 and source_identity(primary)==science['source_identity'] and source_identity(old)==read_json(old/'release.json')['source_identity']
assert source_identity(framework)==identity['source_identity'] and read_json(framework/'configs/study.json')==config and canonical_hash(config)==science['config_sha256']==identity['config_sha256']
q=read_json(root/'maintenance/sixth-repair-support-20261016/qualification.json')
assert q['status']=='complete' and q['slurm_job_id']=='5865804' and q['source_identity']==read_json(old/'release.json')['source_identity'] and q['test_counts']=={'tests':809,'failures':0,'errors':0,'skipped':0}
driver=root/'maintenance/sixth-repair-support-20261016/controller-primary-repair-sixth-20261016.py'
assert digest(driver)=='49815eeb509909d5eac2ebde2e4f4a3918cf40cefdc5f643f2bcaffead629eb1'
area=Path(payload['area']);assert area.is_relative_to(root/'maintenance') and not area.exists() and not area.is_symlink(),'Immutable stage cannot be replayed'
area.mkdir(mode=0o700,parents=True,exist_ok=False)
raw=zlib.decompress(base64.b64decode(payload['helper_body']));assert hashlib.sha256(raw).hexdigest()==payload['helper_sha256'];ast.parse(raw)
helper=area/payload['helper']
with helper.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
assert not payload['after'] and not payload['after_specs'],'Expanded maintenance requires helper-owned source-bound native prerequisites'
gp=root/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py';assert digest(gp)=='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91';runpy.run_path(str(gp))['install'](area/'submission-native-accounting-reads')
import time
rgp=root/'maintenance/study-af-registry-repair-support-v4-20261021/registered-snapshot-read-guard-v3-20261021.py'
assert digest(rgp)=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'
api=runpy.run_path(str(rgp));api['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+1500)
from horsehair.campaign import combined_admission
policy_pair=(Path('/scratch/pwa209/horsehair-worm/maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'),'f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5')
with api['registration_lock'](area/'submission-registration-lock',time.time()+1500,role='controller-start'):
    admission=combined_admission(root,config,science['release_id'],area,'submit-expanded-combined-admission',resource_policy=policy_pair,cpus=payload['cpus'],gpus=0,hours=payload['hours'])
    quota=__import__('subprocess').run(['lfs','quota','-u','pwa209','/scratch/pwa209'],capture_output=True,text=True,check=True,timeout=30)
    rows=[x.split() for x in quota.stdout.splitlines() if x.startswith('/scratch/pwa209 ')]
    assert len(rows)==1 and int(rows[0][7])-int(rows[0][5].rstrip('*'))>=config['resources']['scratch_budget_files'],'Insufficient current personal inode headroom'
    args=[str(driver) if x=='__DRIVER__' else digest(driver) if x=='__DRIVER_SHA__' else x for x in payload['args']]
    command=shlex.join(['python','-u',str(helper),*args]);name='recovery-'+payload['id']
    text=_script(config,framework,name,command,cpus=payload['cpus'],memory=payload['memory'],hours=payload['hours'],network=payload.get('network',False))
    if payload['native_minutes'] is not None:
        # This native execution bound is below the preserved conservative hour
        # reservation. It changes only a finite maintenance task, never science.
        expected='#SBATCH --time='+str(payload['hours'])+':00:00'
        assert text.count(expected)==1
        minutes=payload['native_minutes']
        text=text.replace(expected,'#SBATCH --time='+str(minutes//60).zfill(2)+':'+str(minutes%60).zfill(2)+':00',1)
    text=text.replace('set -euo pipefail','set -euo pipefail\nexport PYTHONDONTWRITEBYTECODE=1',1)
    if payload['exclude']:text=text.replace('set -euo pipefail','#SBATCH --exclude='+payload['exclude']+'\nset -euo pipefail',1)
    if payload['nodelist']:text=text.replace('set -euo pipefail','#SBATCH --nodelist='+payload['nodelist']+'\nset -euo pipefail',1)
    if payload['submission_exclude']:
        # Scoped scheduler placement policy for this management allocation and its
        # nested study submissions. Frozen numerical scripts/resources stay intact;
        # native held-job evidence retains the actual effective ExcNodeList.
        text=text.replace('export PYTHONDONTWRITEBYTECODE=1','export PYTHONDONTWRITEBYTECODE=1\nexport SBATCH_EXCLUDE='+shlex.quote(payload['submission_exclude']),1)
    if payload['after']:
        # Native afterok completion does not by itself refresh the study's durable
        # accounting ledger. Reconcile in this allocation before helper prerequisite
        # checks; retain the exact qualified operational helper bytes unchanged.
        bootstrap=('from pathlib import Path;import runpy;from horsehair.core import digest,read_json;'
            +'d=Path('+repr(str(driver))+');assert digest(d)=='+repr(digest(driver))+';'
            +'m=runpy.run_path(str(d),run_name="native_stage_start_accounting");'
            +'r=Path('+repr(str(root))+');p=Path('+repr(str(primary))+');'
            +'m["combined_admission"](r,read_json(p/"configs/study.json"),read_json(p/"release.json")["release_id"],Path('+repr(str(area))+'),"native-start-admission")')
        text=text.replace(command+'\n',shlex.join(['python','-u','-c',bootstrap])+'\n'+command+'\n',1)
    directory=root/'plans'/('recovery-support-20261021-'+payload['id']);ledger=root/'state'/('recovery-support-20261021-'+payload['id']+'-submission.json')
    assert not directory.exists() and not ledger.exists();directory.mkdir(mode=0o700)
    script=directory/(name+'.sbatch');script.write_text(text)
    task={'name':name,'script':str(script),'script_sha256':canonical_hash(text),'command':command,'depends':[],'resources':{'cpus':payload['cpus'],'memory':payload['memory'],'hours':payload['hours'],'gpu':False,'array':None}}
    task['dependency_kind']=payload['dependency_kind']
    if payload['after']:task['external_dependencies']=payload['after']
    if payload['after_specs']:task['external_dependency_specs']=payload['after_specs']
    if payload['native_minutes'] is not None:task['native_time_limit_minutes']=payload['native_minutes']
    plan={'schema_version':2,'phase':'recovery-support','root':str(root),'config_sha256':canonical_hash(config),'release_identity':identity['source_identity'],'tasks':[task],'cpu_ceiling':256,'gpu_concurrency':4,'resource_policy':admission['resource_policy'],'maximum_simultaneous_resources':{'cpus':payload['cpus'],'gpus':0}}
    atomic_json(directory/'plan.json',plan)
    intent={k:v for k,v in payload.items() if k!='helper_body'};intent.update(automatic_replay_permitted=False,plan_path=str(directory/'plan.json'),ledger_path=str(ledger),quota_stdout=quota.stdout)
    atomic_json(area/'submission-intent.json',intent)
    submitted=submit(directory/'plan.json',ledger)
print('RECOVERY_STAGE_SUBMITTED '+json.dumps({'request_id':payload['id'],'area':str(area),'helper_sha256':payload['helper_sha256'],'tasks':{k:{x:v.get(x) for x in ['job_id','state']} for k,v in submitted['tasks'].items()}},sort_keys=True),flush=True)

PY
