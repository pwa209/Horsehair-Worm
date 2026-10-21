"""Local builder for exclusive, accounted maintenance stages; no auto replay."""
from pathlib import Path
import argparse,base64,hashlib,json,zlib,ast,shlex
p=argparse.ArgumentParser();p.add_argument('--id',required=True);p.add_argument('--helper',required=True,type=Path);p.add_argument('--area',required=True);p.add_argument('--framework',default='/project/def-ptewarie/pwa209/horsehair-worm/releases/20261016-5c3ae6df532d-91c8824f2adb');p.add_argument('--cpus',type=int,default=4);p.add_argument('--hours',type=int,default=1);p.add_argument('--memory',default='16G');p.add_argument('--args-json',default='[]');p.add_argument('--after-json',default='[]');p.add_argument('--exclude',default='');p.add_argument('--submission-exclude',default='');p.add_argument('--nodelist',default='');p.add_argument('--dependency-kind',choices=['afterok','afterany'],default='afterok');p.add_argument('--after-specs-json',default='[]');p.add_argument('--native-minutes',type=int,default=None);p.add_argument('--network',action='store_true');a=p.parse_args()
raw=a.helper.read_bytes();ast.parse(raw)
payload={'id':a.id,'helper':a.helper.name,'helper_body':base64.b64encode(zlib.compress(raw)).decode(),'helper_sha256':hashlib.sha256(raw).hexdigest(),'area':a.area,'framework':a.framework,'cpus':a.cpus,'hours':a.hours,'memory':a.memory,'args':json.loads(a.args_json),'after':json.loads(a.after_json),'dependency_kind':a.dependency_kind,'after_specs':json.loads(a.after_specs_json),'exclude':a.exclude,'submission_exclude':a.submission_exclude,'nodelist':a.nodelist,'native_minutes':a.native_minutes,'network':a.network}
assert a.id.isdigit() and 1<=a.cpus<=8 and a.hours<=2 and a.area.startswith('/scratch/pwa209/horsehair-worm/maintenance/')
assert isinstance(payload['after'],list) and all(type(x) is str and x.isdigit() for x in payload['after'])
assert not a.exclude or all(x.startswith('nc') and x[2:].isdigit() for x in a.exclude.split(','))
assert not a.submission_exclude or all(x.startswith('nc') and x[2:].isdigit() for x in a.submission_exclude.split(','))
assert not a.nodelist or all(x.startswith('nc') and x[2:].isdigit() for x in a.nodelist.split(','))
assert a.native_minutes is None or 1<=a.native_minutes<=60*a.hours
body=r'''
from pathlib import Path
import ast,base64,hashlib,json,os,runpy,shlex,zlib
from horsehair.core import read_json,digest,source_identity,canonical_hash,atomic_json
from horsehair.slurm import _script,submit
payload=json.loads(zlib.decompress(base64.b64decode('__PAYLOAD__')))
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
admitter=runpy.run_path(str(driver),run_name='postdownload_admission_only')
admission=admitter['combined_admission'](root,config,science['release_id'],area,'submit-combined-admission')
assert admission['totals']['cpu_core_hours']+payload['cpus']*payload['hours']<=config['resources']['cpu_core_hours_cap'] and admission['capacity']['cpus']+payload['cpus']<=config['resources']['cpu_ceiling']
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
plan={'schema_version':2,'phase':'recovery-support','root':str(root),'config_sha256':canonical_hash(config),'release_identity':identity['source_identity'],'tasks':[task],'cpu_ceiling':64,'gpu_concurrency':1,'maximum_simultaneous_resources':{'cpus':payload['cpus'],'gpus':0}}
atomic_json(directory/'plan.json',plan)
intent={k:v for k,v in payload.items() if k!='helper_body'};intent.update(automatic_replay_permitted=False,plan_path=str(directory/'plan.json'),ledger_path=str(ledger),quota_stdout=quota.stdout)
atomic_json(area/'submission-intent.json',intent)
submitted=submit(directory/'plan.json',ledger)
print('RECOVERY_STAGE_SUBMITTED '+json.dumps({'request_id':payload['id'],'area':str(area),'helper_sha256':payload['helper_sha256'],'tasks':{k:{x:v.get(x) for x in ['job_id','state']} for k,v in submitted['tasks'].items()}},sort_keys=True),flush=True)
'''.replace('__PAYLOAD__',base64.b64encode(zlib.compress(json.dumps(payload).encode())).decode())
old='/project/def-ptewarie/pwa209/horsehair-worm/releases/20261016-5c3ae6df532d-91c8824f2adb'
text='#!/bin/bash\nset -euo pipefail\ntest "$(id -un)" = pwa209\ncase "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac\nmodule load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1\nsource /home/pwa209/hhw-environment/bin/activate\nexport PYTHONDONTWRITEBYTECODE=1\nexport PYTHONPATH="'+old+'/src:${PYTHONPATH:-}"\ncd '+old+'\npython -u - <<\'PY\'\n'+body+'\nPY\n'
target=Path('work/hhw-recovery-stage-20261021-'+a.id+'.sh')
with target.open('x',newline='\n') as f:f.write(text)
print(json.dumps({'request':str(target),'helper_sha256':payload['helper_sha256']}))
