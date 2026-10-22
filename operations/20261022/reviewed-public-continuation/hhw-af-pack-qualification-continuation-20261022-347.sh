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
area=Path(payload['area'])
assert area==root/'maintenance/study-af-policy-pack-qualification-stage-v1-20261022'
assert area.is_dir() and area==area.resolve() and not area.is_symlink() and area.stat().st_uid==3164768
staged_pins={'run-f2-policy-archive-pack-v1-20261021.py': '0490b796efcbd6b7063ef3fc1e454f8b38cd6692d139d06626829e0db04a018d', 'test_f2_policy_archive_pack_v1_20261021.py': '25a4c507d4f2b23a70ef496519fcfafd643f986277d11224cb38413edbe93225', 'qualify-f2-policy-pack-v1-20261022.py': '63dbc330c9e8b21e643e1337ff44c07cb31ef05ca123fc07721eab1fd34989d9', 'test_qualify_f2_policy_pack_v1_20261022.py': '05799d2828d8dcd571de8051adf9dc62471ae86826ddf73715873dc7b28d7649'}
def verify_staged_sources():
 for source_name,source_sha in staged_pins.items():
  source=area/source_name
  assert source.is_file() and not source.is_symlink() and source.stat().st_uid==3164768 and digest(source)==source_sha,source_name
verify_staged_sources()
helper=area/payload['helper']
assert helper.name=='qualify-f2-policy-pack-v1-20261022.py' and digest(helper)==payload['helper_sha256']==staged_pins[helper.name]
def require_unpublished():
 verify_staged_sources()
 for path in [root/'plans/recovery-support-20261021-336',root/'state/recovery-support-20261021-336-submission.json',area/'submission-intent.json',root/'maintenance/study-af-policy-pack-qualification-support-v1-20261022']:
  assert not path.exists() and not path.is_symlink(),'Logical336 publication/result exists; reconcile without replay: '+str(path)
require_unpublished()
assert not payload['after'] and not payload['after_specs'],'Expanded maintenance requires helper-owned source-bound native prerequisites'
gp=root/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py';assert digest(gp)=='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91';runpy.run_path(str(gp))['install'](area/'resumption-347-native-accounting-reads')
import time
verify_staged_sources()
rgp=root/'maintenance/study-af-registry-repair-support-v4-20261021/registered-snapshot-read-guard-v3-20261021.py'
assert digest(rgp)=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'
api=runpy.run_path(str(rgp));api['install'](area/'resumption-347-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+900)
from horsehair.campaign import combined_admission
policy_pair=(Path('/scratch/pwa209/horsehair-worm/maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'),'f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5')
with api['registration_lock'](area/'resumption-347-registration-lock',time.time()+900,role='controller-start'):
    require_unpublished()
    admission=combined_admission(root,config,science['release_id'],area,'resumption-347-expanded-combined-admission',resource_policy=policy_pair,cpus=payload['cpus'],gpus=0,hours=payload['hours'])
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
    bootstrap="from pathlib import Path\nimport hashlib,os,subprocess,sys,xml.etree.ElementTree as ET\nfrom horsehair.core import canonical_hash\nfrom horsehair.cli import _qualification_junit\narea=Path('/scratch/pwa209/horsehair-worm/maintenance/study-af-policy-pack-qualification-stage-v1-20261022')\npins={'run-f2-policy-archive-pack-v1-20261021.py': '0490b796efcbd6b7063ef3fc1e454f8b38cd6692d139d06626829e0db04a018d', 'test_f2_policy_archive_pack_v1_20261021.py': '25a4c507d4f2b23a70ef496519fcfafd643f986277d11224cb38413edbe93225', 'qualify-f2-policy-pack-v1-20261022.py': '63dbc330c9e8b21e643e1337ff44c07cb31ef05ca123fc07721eab1fd34989d9', 'test_qualify_f2_policy_pack_v1_20261022.py': '05799d2828d8dcd571de8051adf9dc62471ae86826ddf73715873dc7b28d7649'}\nfor name,sha in pins.items():\n p=area/name;assert p.is_file() and not p.is_symlink() and hashlib.sha256(p.read_bytes()).hexdigest()==sha\ntemporary=Path(os.environ['SLURM_TMPDIR']);assert temporary.is_absolute() and temporary==temporary.resolve() and temporary.is_dir() and temporary.stat().st_uid==3164768\nfixtures=temporary/('hhw-policy-pack-qualifier-bootstrap-'+os.environ['SLURM_JOB_ID']);fixtures.mkdir(mode=0o700,exist_ok=False)\nenvironment={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1','PYTHONNOUSERSITE':'1','TZ':'UTC'}\nfor key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):environment.pop(key,None)\nxml=area/'qualifier19.xml'\nsubprocess.run([sys.executable,'-s','-m','pytest',str(area/'test_qualify_f2_policy_pack_v1_20261022.py'),'-q','-p','no:cacheprovider','--rootdir',str(area),'--basetemp',str(fixtures/'tests'),'--junitxml',str(xml)],env=environment,check=True,timeout=180)\ncases=list(ET.fromstring(xml.read_bytes()).iter('testcase'));rows=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in cases)\nassert len(cases)==len(set(rows))==19 and canonical_hash(rows)=='ca1dd220a3349edcd2c91656c4e75338fb2325cfec801ba7d7cedc3490478820' and all(not any(c.find(k) is not None for k in ('failure','error','skipped')) for c in cases)\nassert _qualification_junit(xml)=={'tests':19,'failures':0,'errors':0,'skipped':0}\nfor name,sha in pins.items():assert hashlib.sha256((area/name).read_bytes()).hexdigest()==sha\n"
    text=text.replace(command+'\n',shlex.join(['python','-u','-c',bootstrap])+'\n'+command+'\n',1)
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
    require_unpublished()
    assert not directory.exists() and not ledger.exists();directory.mkdir(mode=0o700)
    script=directory/(name+'.sbatch');script.write_text(text)
    task={'name':name,'script':str(script),'script_sha256':canonical_hash(text),'command':command,'depends':[],'resources':{'cpus':payload['cpus'],'memory':payload['memory'],'hours':payload['hours'],'gpu':False,'array':None}}
    task['dependency_kind']=payload['dependency_kind']
    task['external_dependencies']=['6699739']
    task['external_dependency_specs']=[{'job_id':'6699739','array':None,'purpose':'Positive exact-size census before disposable private pack qualification; runtime source/freshness checks retained'}]
    if payload['after']:task['external_dependencies']=payload['after']
    if payload['after_specs']:task['external_dependency_specs']=payload['after_specs']
    if payload['native_minutes'] is not None:task['native_time_limit_minutes']=payload['native_minutes']
    plan={'schema_version':2,'phase':'recovery-support','root':str(root),'config_sha256':canonical_hash(config),'release_identity':identity['source_identity'],'tasks':[task],'cpu_ceiling':256,'gpu_concurrency':4,'resource_policy':admission['resource_policy'],'maximum_simultaneous_resources':{'cpus':payload['cpus'],'gpus':0}}
    atomic_json(directory/'plan.json',plan)
    intent={k:v for k,v in payload.items() if k!='helper_body'};intent.update(automatic_replay_permitted=False,plan_path=str(directory/'plan.json'),ledger_path=str(ledger),quota_stdout=quota.stdout)
    atomic_json(area/'submission-intent.json',intent)
    submitted=submit(directory/'plan.json',ledger)
print('RECOVERY_STAGE_CONTINUED_TRANSPORT_347 '+json.dumps({'request_id':payload['id'],'area':str(area),'helper_sha256':payload['helper_sha256'],'tasks':{k:{x:v.get(x) for x in ['job_id','state']} for k,v in submitted['tasks'].items()}},sort_keys=True),flush=True)

PY
