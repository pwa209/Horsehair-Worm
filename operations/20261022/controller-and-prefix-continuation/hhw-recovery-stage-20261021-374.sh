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
rgp=area/'registered-snapshot-read-guard-v4-20261022.py'
raw=zlib.decompress(base64.b64decode('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'));assert hashlib.sha256(raw).hexdigest()=='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
with rgp.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
assert digest(rgp)=='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
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
