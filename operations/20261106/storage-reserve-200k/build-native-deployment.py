"""Build one exact source capsule and the three existing native qualification plans."""
from pathlib import Path
import argparse,base64,hashlib,json,re,zlib


def build(request_id,components=('census','consumers','coordination')):
    if not re.fullmatch(r'[a-zA-Z0-9_-]+',request_id):
        raise ValueError('A fresh plain request identifier is required')
    here=Path(__file__).resolve().parent
    repo=here.parents[2]
    if not components or len(set(components))!=len(components) or set(components)-{'census','consumers','coordination'}:
        raise ValueError('Select distinct existing components')
    expected={'storage_policy.py','policy.json','test_storage_policy.py'}
    for component in components:
        manifest_path=here/component/'source-manifest.json'
        manifest=json.loads(manifest_path.read_bytes())
        expected.add(component+'/source-manifest.json')
        expected.update(component+'/'+name for name in manifest['files'])
    payload={}
    for name in sorted(expected):
        path=here/name
        if path.is_file() and path.resolve().is_relative_to(here.resolve()):
            relative=path.relative_to(here)
            raw=path.read_bytes()
            if path.suffix=='.py' and b'\r\n' in raw:
                raise ValueError('Freeze LF source before sealing: '+str(path))
            name=path.relative_to(here).as_posix()
            payload[name]={'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),
                           'data':base64.b64encode(zlib.compress(raw,9)).decode()}
        else:raise ValueError('The complete manifest member is missing or redirected: '+name)
    for component in components:
        if component+'/source-manifest.json' not in payload:
            raise ValueError('Seal every source-matched component before deployment')
    remote=REMOTE.replace('PAYLOAD',repr(payload)).replace('COMPONENTS',repr(tuple(components))).replace('REGISTRATION_ID',repr(request_id))
    target=repo/'work'/(request_id+'.sh')
    if target.exists():raise ValueError('Never overwrite a sent request; use a fresh identifier')
    target.write_text(remote,encoding='utf-8',newline='\n')
    return {'request':str(target),'files':len(payload),'source_sha256':{
        k:v['sha256'] for k,v in payload.items() if k.endswith('source-manifest.json') or k in {'policy.json','storage_policy.py'}}}


REMOTE='''#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src
export PYTHONDONTWRITEBYTECODE=1 TZ=UTC
python -B -u - <<'PY'
from pathlib import Path,PurePosixPath
import base64,hashlib,importlib.util,json,os,shlex,subprocess,time,zlib
from horsehair import slurm
from horsehair.core import atomic_json,canonical_hash,exclusive_file_lock,read_json,source_identity
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');area=r/'maintenance/storage-reserve-200k-v2-20261106'
area.mkdir(mode=0o700,parents=True,exist_ok=True)
framework=r/'releases/20261021-6ed201653dfa-4f3903d7c26e'
identity=source_identity(framework);assert identity['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
config=read_json(framework/'configs/study.json');assert canonical_hash(config)=='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
assert area.resolve()==area and area.is_dir() and area.stat().st_uid==3164768
payload=PAYLOAD
for name,item in payload.items():
    rel=PurePosixPath(name);assert not rel.is_absolute() and '..' not in rel.parts
    raw=zlib.decompress(base64.b64decode(item['data']));assert len(raw)==item['bytes'] and hashlib.sha256(raw).hexdigest()==item['sha256']
    path=area/name;path.parent.mkdir(parents=True,exist_ok=True)
    assert path.parent.resolve()==path.parent and path.parent.stat().st_uid==3164768
    if path.exists():assert not path.is_symlink() and path.read_bytes()==raw
    else:
        with path.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
        path.chmod(0o444)
def module(path,name):
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec)
    import sys
    sys.modules[name]=m;spec.loader.exec_module(m);return m
shared=module(area/'storage_policy.py','approved_storage_policy')
approved=shared.verify_policy(area/'policy.json',payload['policy.json']['sha256'],root=r)
assert shared.reserve_for(2000,approved)==197995
deployment=area/'qualification-registration'/REGISTRATION_ID;deployment.mkdir(mode=0o700,parents=True,exist_ok=False)
counter=[0]
def runner(cmd,**kwargs):
    counter[0]+=1;prefix=deployment/('mutation-'+str(counter[0]))
    atomic_json(prefix.with_suffix('.intent.json'),{'command':cmd,'automatic_replay_permitted':False})
    response=subprocess.run(cmd,**{**kwargs,'timeout':120})
    atomic_json(prefix.with_suffix('.response.json'),{'returncode':response.returncode,'stdout':response.stdout,'stderr':response.stderr})
    return response
def native(job):
    p=subprocess.run(['scontrol','show','job',job,'-o'],text=True,capture_output=True,timeout=45)
    p.check_returncode();return dict(x.split('=',1) for x in shlex.split(p.stdout) if '=' in x)
contracts={}
with exclusive_file_lock(r/'state/af-campaign-registration.mutex',timeout=180):
    for component in COMPONENTS:
        support=area/component;mp=support/'source-manifest.json';manifest_sha=hashlib.sha256(mp.read_bytes()).hexdigest();manifest=read_json(mp)
        for name,sha in manifest['files'].items():assert hashlib.sha256((support/name).read_bytes()).hexdigest()==sha
        if component=='census':
            b=module(support/'build-native-plans.py','amended_census_plan_builder')
            b.PROVIDER_SHA=manifest['files']['run-fresh-storage-census-resilience-v1-20261024.py']
            b.QUALIFIER_SHA=manifest['files']['qualify-census-resilience-v1-20261024.py']
            plan,scripts=b.render(config,identity,manifest_sha,slurm._script,mode='qualification')
            phase=plan['phase'];directory=r/'plans'/phase;ledger=r/'state'/(phase+'-submission.json')
            assert not directory.exists() and not ledger.exists();directory.mkdir()
            assert len(plan['tasks'])==1
            task=plan['tasks'][0];name=task['name'];script=Path(task['script']);text=scripts[script.name]
        else:
            phase='storage-reserve-200k-v2-'+('consumer' if component=='consumers' else 'coordination')+'-qualification-20261106'
            directory=r/'plans'/phase;ledger=r/'state'/(phase+'-submission.json');assert not ledger.exists()
            if directory.exists():
                assert directory.resolve()==directory and not directory.is_symlink() and directory.stat().st_uid==3164768 and not list(directory.iterdir()),'Only an empty never-submitted plan directory can be recovered'
                atomic_json(deployment/(component+'-empty-plan-recovery.json'),{'directory':str(directory),'ledger_absent':True,'directory_empty':True,'submission_replayed':False})
            else:directory.mkdir()
            name='storage-reserve-200k-'+('consumer' if component=='consumers' else 'coordination')+'-qualification';script=directory/(name+'.sbatch')
            flags=['--native-plan',str(directory/'plan.json'),'--native-ledger',str(ledger),'--native-task',name,'--native-script',str(script)]
            if component=='consumers':
                m=module(support/'run-census-resilience-consumer-v2-20261024.py','amended_consumer_runtime')
                qualifiers=[name for name in manifest['files'] if name.startswith('qualify-census-resilience-consumers-') and name.endswith('.py')];assert len(qualifiers)==1
                helper=support/qualifiers[0]
                command=shlex.join(['python','-s','-B',str(helper),'--source-manifest',str(mp),'--source-manifest-sha256',manifest_sha,*flags])
                task_extra={'census_resilience_consumer_qualification':{'helper':{'path':str(helper),'sha256':manifest['files'][helper.name]},'source_manifest':{'path':str(mp),'sha256':manifest_sha},'suite':manifest['suites']['consumers']}}
            else:
                helper=support/'coordination.py'
                command=shlex.join(['python','-s','-B',str(helper),'--mode','qualify','--source-manifest-sha256',manifest_sha,*flags])
                task_extra={'coordination_manifest_sha256':manifest_sha}
            text=slurm._script(config,framework,name,command,cpus=4,memory='16G',hours=1,gpu=False)
            task={'name':name,'command':command,'script':str(script),'script_sha256':canonical_hash(text),'depends':[],'external_dependencies':[],
                  'dependency_kind':'afterok','resources':{'cpus':4,'memory':'16G','hours':1,'gpu':False,'array':None},**task_extra}
            plan={'schema_version':1,'root':str(r),'phase':phase,'release_identity':identity,'config_sha256':canonical_hash(config),'tasks':[task]}
        assert '#SBATCH --hold' not in text.splitlines()
        text=text.replace('set -euo pipefail','#SBATCH --hold\\nset -euo pipefail',1)
        task['script_sha256']=canonical_hash(text)
        with script.open('x') as f:f.write(text);f.flush();os.fsync(f.fileno())
        atomic_json(directory/'plan.json',plan)
        submitted=slurm.submit(directory/'plan.json',ledger,runner=runner);job=submitted['tasks'][name]['job_id'];f=native(job)
        assert f['JobId']==job and f['UserId']=='pwa209(3164768)' and f['JobState']=='PENDING' and f['Reason']=='JobHeldUser'
        assert f['Command']==str(script) and f['JobName']=='hhw-'+name and f['Account']=='def-ptewarie_cpu' and f['NumCPUs']=='4' and f['MinMemoryNode'] in {'16G','16384M'} and f['TimeLimit']=='01:00:00'
        contracts[component]={'job_id':job,'plan':str(directory/'plan.json'),'ledger':str(ledger),'source_manifest_sha256':manifest_sha,'held_native':f}
        atomic_json(deployment/(component+'-accepted-held.json'),contracts[component])
    atomic_json(deployment/'all-accepted-held.json',contracts)
    for component,contract in contracts.items():
        job=contract['job_id'];f=native(job);assert f['JobState']=='PENDING' and f['Reason']=='JobHeldUser' and f['Command']==contract['held_native']['Command']
        response=runner(['scontrol','release',job],text=True,capture_output=True);response.check_returncode()
        f=native(job);assert f['UserId']=='pwa209(3164768)' and f['Command']==contract['held_native']['Command'] and not (f['JobState']=='PENDING' and f['Reason']=='JobHeldUser')
        atomic_json(deployment/(component+'-released.json'),{'job_id':job,'actual_native':f})
atomic_json(deployment/'complete.json',{'status':'source_matched_qualifications_registered_and_released','components':contracts,'personal_free_minimum':200000,'scientific_gates_changed':False})
print('STORAGE_RESERVE_QUALIFICATIONS '+json.dumps({k:{x:y for x,y in v.items() if x!='held_native'} for k,v in contracts.items()}),flush=True)
PY
'''


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--request-id',required=True);p.add_argument('--components',nargs='+',choices=['census','consumers','coordination'],default=['census','consumers','coordination'])
    a=p.parse_args();print(json.dumps(build(a.request_id,tuple(a.components))))
