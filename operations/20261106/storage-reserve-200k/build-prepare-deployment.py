"""Freeze the finite native preparation request; generation never submits jobs."""
from pathlib import Path
import argparse,base64,hashlib,json,re,zlib

ROOT='/scratch/pwa209/horsehair-worm'
BASE=ROOT+'/maintenance/storage-reserve-200k-v2-20261106'
JOBS={'census':'8630687','consumers':'8630826','coordination':'8630827'}
MANIFESTS={'census':'288af211cad1ad186105e6b2dafa08d4195920b132ab9efef367af98c0fbdf6e',
           'consumers':'9326aae2e0bb3775b23efba2e61fef6a0365f070c18e03383a9f433bde31614b',
           'coordination':'45feaea32e02fce2ca94ca841858920fd6342ec46ab4c7d9a17552775b0f9d53'}
FILES=['storage_policy.py','policy.json','test_storage_policy.py','archive_continuation.py',
       'test_archive_continuation.py','archive-native-test-contract.json','release_census.py',
       'test_release_census.py','activate_controllers.py','test_activate_controllers.py',
       'prepare_study.py','test_prepare_study.py']


def build(request_id,observation):
    if not re.fullmatch('[a-zA-Z0-9_-]+',request_id):raise ValueError('Fresh plain request identifier required')
    here=Path(__file__).resolve().parent;repo=here.parents[2]
    payload={};pins={}
    for name in FILES:
        raw=(here/name).read_bytes()
        if b'\r\n' in raw:raise ValueError('Freeze LF source before deployment: '+name)
        digest=hashlib.sha256(raw).hexdigest();pins[BASE+'/'+name]=digest
        payload[name]={'sha256':digest,'bytes':len(raw),'data':base64.b64encode(zlib.compress(raw,9)).decode()}
    if payload['prepare_study.py']['sha256']!='41e5a9b10297c856a90b42a62cab30617e6e77f4aa0b7d42c8f1eaaf0574068f':raise ValueError('Preparation source differs from reviewed final source')
    if payload['test_prepare_study.py']['sha256']!='a8d8cd89f5e145752e533950bad06b7e9b87ace8339a562d14192c0f18cc9a35':raise ValueError('Preparation tests differ from final qualification contract')
    def old(relative):
        value=observation[relative]['locator']
        if value['path']!=ROOT+'/'+relative or not re.fullmatch('[0-9a-f]{64}',value['sha256']):raise ValueError('Original live locator does not match its exact authorized path')
        return value
    def pair(phase):return {'plan':old('plans/'+phase+'/plan.json'),'ledger':old('state/'+phase+'-submission.json')}
    request={'kind':'finite-storage-reserve-200k-study-preparation','root':ROOT,'source_pins':pins,
             'components':{role:{'manifest':{'path':BASE+'/'+role+'/source-manifest.json','sha256':MANIFESTS[role]},
                'qualification_job_id':JOBS[role],'qualification_path':BASE+'/'+role+('/qualification/qualification.json' if role=='census' else '/qualification.json')} for role in JOBS},
             'activation_inputs':{'legacy_bindings':{kind:old('maintenance/study-census-resilience-consumers-v2-20261024/'+kind+'-consumer-binding.json') for kind in ['af','primary']},
                'primary_template':pair('study-primary-census-resilience-consumers-v2-20261024'),
                'old_registrations':{'primary':pair('study-primary-census-resilience-consumers-v6-20261103'),'agent':pair('agent-coordination-v9-resume-20261103')},
                'native_tests':{'source':{'path':BASE+'/test_activate_controllers.py','sha256':payload['test_activate_controllers.py']['sha256']},'tests':62,'roster_sha256':'d0009730c331db78767d9a40da0afaaad0adc7ee0c749d414adcf083febe7aef'}}}
    code=REMOTE.replace('__PAYLOAD__',repr(payload)).replace('__REQUEST__',repr(request)).replace('__REQUEST_ID__',repr(request_id))
    target=repo/'work'/(request_id+'.sh')
    if target.exists():raise ValueError('Never replace a previously generated administrative request')
    target.write_text(code,encoding='utf-8',newline='\n')
    return {'request':str(target),'root_source_files':len(payload),'qualification_dependencies':list(JOBS.values()),'minimum_free_files':200000}


REMOTE='''#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src
export PYTHONDONTWRITEBYTECODE=1 TZ=UTC
python -B -u - <<'PY'
from pathlib import Path
import base64,hashlib,json,os,shlex,subprocess,zlib
from horsehair import slurm
from horsehair.core import source_identity,canonical_hash,read_json,atomic_json,exclusive_file_lock
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');area=r/'maintenance/storage-reserve-200k-v2-20261106'
assert area.resolve()==area and area.is_dir() and area.stat().st_uid==3164768
payload=__PAYLOAD__;request=__REQUEST__
for name,item in payload.items():
    assert Path(name).name==name
    raw=zlib.decompress(base64.b64decode(item['data']));assert len(raw)==item['bytes'] and hashlib.sha256(raw).hexdigest()==item['sha256']
    path=area/name
    if path.exists():assert not path.is_symlink() and path.resolve()==path and path.read_bytes()==raw
    else:
        with path.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
        path.chmod(0o444)
for component in request['components'].values():
    p=Path(component['manifest']['path']);assert p.resolve()==p and hashlib.sha256(p.read_bytes()).hexdigest()==component['manifest']['sha256']
for kind in ['af','primary']:
    loc=request['activation_inputs']['legacy_bindings'][kind];assert hashlib.sha256(Path(loc['path']).read_bytes()).hexdigest()==loc['sha256']
for pair in [request['activation_inputs']['primary_template'],*request['activation_inputs']['old_registrations'].values()]:
    for loc in pair.values():assert hashlib.sha256(Path(loc['path']).read_bytes()).hexdigest()==loc['sha256']
request_path=area/'preparation-request.json';assert not request_path.exists();atomic_json(request_path,request)
request_sha=hashlib.sha256(request_path.read_bytes()).hexdigest()
framework=r/'releases/20261021-6ed201653dfa-4f3903d7c26e';identity=source_identity(framework)
assert identity['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
config=read_json(framework/'configs/study.json');assert canonical_hash(config)=='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
phase='storage-reserve-200k-v2-prepare-20261106';name='storage-reserve-200k-prepare'
directory=r/'plans'/phase;ledger=r/'state'/(phase+'-submission.json');script=directory/(name+'.sbatch')
assert not directory.exists() and not ledger.exists()
flags=['--request',str(request_path),'--request-sha256',request_sha,'--source-sha256',payload['prepare_study.py']['sha256'],
       '--native-plan',str(directory/'plan.json'),'--native-ledger',str(ledger),'--native-script',str(script),'--native-task',name]
command=shlex.join(['python','-s','-B','-u',str(area/'prepare_study.py'),*flags])
assertion='from pathlib import Path;import hashlib;'+''.join('assert hashlib.sha256(Path('+repr(str(area/n))+').read_bytes()).hexdigest()=='+repr(payload[n]['sha256'])+';' for n in ['prepare_study.py','test_prepare_study.py'])
prefix=shlex.join(['python','-B','-c',assertion])+'\\npython -B -m pytest '+shlex.quote(str(area/'test_prepare_study.py'))+' -q -p no:cacheprovider --basetemp "$SLURM_TMPDIR/prepare-tests" --junitxml "$SLURM_TMPDIR/prepare-tests.xml"\\n'
text=slurm._script(config,framework,name,command,cpus=2,memory='16G',hours=1,gpu=False)
assert text.count(command)==1;text=text.replace(command,prefix+command,1)
text=text.replace('set -euo pipefail','#SBATCH --hold\\n#SBATCH --no-requeue\\nset -euo pipefail',1)
syntax=subprocess.run(['bash','-n'],input=text,text=True,capture_output=True,timeout=30);syntax.check_returncode()
task={'name':name,'command':command,'script':str(script),'script_sha256':canonical_hash(text),
      'resources':{'cpus':2,'memory':'16G','hours':1,'gpu':False,'array':None},'depends':[],
      'external_dependencies':[request['components'][role]['qualification_job_id'] for role in ['census','consumers','coordination']],
      'dependency_kind':'afterok','finite_storage_reserve_preparation':{'request':{'path':str(request_path),'sha256':request_sha},'source':{'path':str(area/'prepare_study.py'),'sha256':payload['prepare_study.py']['sha256']},'persistent':False}}
plan={'schema_version':2,'root':str(r),'phase':phase,'release_identity':identity,'config_sha256':canonical_hash(config),'tasks':[task]}
registration=area/'prepare-registration';registration.mkdir(mode=0o700,exist_ok=False);counter=[0]
def runner(cmd,**kwargs):
    counter[0]+=1;prefix=registration/('mutation-'+str(counter[0]))
    atomic_json(prefix.with_suffix('.intent.json'),{'command':cmd,'automatic_replay_permitted':False})
    response=subprocess.run(cmd,**{**kwargs,'timeout':120})
    atomic_json(prefix.with_suffix('.response.json'),{'returncode':response.returncode,'stdout':response.stdout,'stderr':response.stderr});return response
def native(job):
    a=subprocess.run(['scontrol','show','job',job,'-o'],capture_output=True,text=True,timeout=45);a.check_returncode()
    return dict(x.split('=',1) for x in shlex.split(a.stdout) if '=' in x)
with exclusive_file_lock(r/'state/af-campaign-registration.mutex',timeout=180):
    directory.mkdir(mode=0o700)
    with script.open('x') as f:f.write(text);f.flush();os.fsync(f.fileno())
    atomic_json(directory/'plan.json',plan);record=slurm.submit(directory/'plan.json',ledger,runner=runner);job=record['tasks'][name]['job_id'];f=native(job)
    assert f['JobId']==job and f['UserId']=='pwa209(3164768)' and f['Account']=='def-ptewarie_cpu' and f['JobState']=='PENDING' and f['Reason']=='JobHeldUser'
    assert f['Command']==str(script) and f['JobName']=='hhw-'+name and f['NumCPUs']=='2' and f['NumNodes'] in {'1','1-1'} and f['MinMemoryNode'] in {'16G','16384M'} and f['TimeLimit']=='01:00:00' and f['Requeue']=='0' and 'ArrayJobId' not in f
    assert all('gpu' not in f.get(key,'').lower() for key in ['ReqTRES','AllocTRES','Gres','TresPerNode','TresPerTask'])
    receipt={'status':'finite_preparation_registered_held','job_id':job,'plan':str(directory/'plan.json'),'ledger':str(ledger),'request':{'path':str(request_path),'sha256':request_sha},'source':{'path':str(area/'prepare_study.py'),'sha256':payload['prepare_study.py']['sha256']},'actual_held_native':f,'request_id':__REQUEST_ID__}
    atomic_json(registration/'accepted-held.json',receipt)
    response=runner(['scontrol','release',job],text=True,capture_output=True);response.check_returncode();after=native(job)
    assert after['JobId']==job and after['Command']==str(script) and after['UserId']=='pwa209(3164768)' and after['Account']=='def-ptewarie_cpu'
    assert after['JobState'] in {'RUNNING','COMPLETING','COMPLETED'} or after['JobState']=='PENDING' and after['Reason'] not in {'JobHeldUser','JobHeldAdmin'} and int(after.get('Priority','0'))>0
    receipt={**receipt,'status':'finite_preparation_registered_and_released','actual_unheld_native':after};atomic_json(registration/'complete.json',receipt)
print('STORAGE_RESERVE_PREPARATION '+json.dumps({k:v for k,v in receipt.items() if k not in {'actual_held_native','actual_unheld_native'}}),flush=True)
PY
'''


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--request-id',required=True);p.add_argument('--activation-inputs',type=Path,required=True)
    a=p.parse_args();print(json.dumps(build(a.request_id,json.loads(a.activation_inputs.read_bytes()))))
