"""One native, source-qualified three-target prestart recovery and finite controllers.

The activation allocation must finish successfully before its first replacement
controller may start. Partial CAS and ambiguous scheduler mutations are retained
for explicit reconciliation; this program never automatically replays them.
"""
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime,timezone
import argparse
import gzip
import hashlib
import io
import json
import math
import os
import re
import runpy
import shlex
import subprocess
import sys
import time
from horsehair.core import canonical_hash,digest,read_json,source_identity,native_job_id
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy
from horsehair import slurm

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
SUPPORT=ROOT/'maintenance/study-af-registry-repair-support-v6-20261022'
AREA=SUPPORT/'native-activation-prestart-three-v1'
AMENDMENT=ROOT/'maintenance/study-af-prestart-three-recovery-v1-20261022'
CAMPAIGN=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
QHELPER=SUPPORT/'qualify-f2-registry-repair-v6-20261022.py'
QTEST='test_qualify_f2_registry_repair_v6_20261022.py'
CONTROLLER=SUPPORT/'run-af-agent-controller-registry-v6-20261022.py'
GUARD=SUPPORT/'registered-snapshot-read-guard-v5-20261022.py'
RECOVERY=SUPPORT/'recover-f2-prestart-three-state-v1-20261022.py'
WORKER_DESCRIPTOR=CAMPAIGN.parent/'precision-and-f2-continuation-v2/descriptor-v2-worker.json'
WORKER_DESCRIPTOR_SHA='a56965b46c09cdffa4512302dea87ca895372f7668f88748ac2263c4b9a2c6b2'
OLD_DESCRIPTOR=ROOT/'maintenance/study-af-registry-repair-support-v4-20261021/native-activation-v3/descriptor-v6-registry-controller.json'
OLD_DESCRIPTOR_SHA='9ef405cf3f012f21bdec79da147c3fc57ca89e90dbe6124cacf8a5b4a0f7c9d8'
OLD_RECOVERY=ROOT/'maintenance/study-af-training0000-state-repair-v1-20261021/amendment.json'
OLD_RECOVERY_SHA='41bebed391a755ead5edee1bdcb56a203d713e533ca2d3d52c4cf2e6ae37b46b'
PLAN_SHA='36f7db5dc790108040e454674006421144184db929b3de88aec2b9a9008727e0'
ORIGINAL_SHA='80c8d048550f4e0e606ff323e2ce99af177b92cfe6c43144956c3c2b852f792b'
ORIGINAL_FILE_SHA='d44aa7c09bcd09c007009d9d5af58c2976f1d53eabbdd93c8f0daf92d9afa684'
PHASE='study-af-f2-registry-r2-controllers-v1-20261022'
MUTEX=ROOT/'state/af-campaign-registration.mutex'
TARGETS={'evaluation-0073-01':'6795585','training-0076':'6795113','training-0077':'6795225'}
CONTROLLERS={'6701493','6701494','6701495','6701498'}
OLD_IDS=CONTROLLERS|set(TARGETS.values())
MANIFEST_KIND='f2-prestart-three-source-freeze-v1'


def require(value,message):
    if not value:raise ValueError(message)


def sha(raw):return hashlib.sha256(raw).hexdigest()


def exclusive_bytes(path,raw):
    require(type(raw) is bytes and len(raw)<=100_000_000,'Bounded immutable bytes required')
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def exclusive(path,value):
    exclusive_bytes(path,(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode())


def packet(path,expected_sha=None):
    p=Path(path);require(p.is_absolute() and p==p.resolve() and p.is_relative_to(ROOT) and not p.is_symlink()
        and all(not a.is_symlink() for a in p.parents if a.is_relative_to(ROOT)),'Ordinary confined source file required')
    require(p.is_file() and p.stat().st_size<=100_000_000,'Bounded source file required')
    raw=p.read_bytes();h=sha(raw)
    require(expected_sha is None or h==expected_sha,'Original immutable source bytes changed: '+str(p))
    return {'path':str(p),'sha256':h,'raw':raw}


def frozen(path,expected_sha):return json.loads(packet(path,expected_sha)['raw'])


def preserved_packet(path,raw):
    exclusive_bytes(path,raw);return {'path':str(path),'sha256':sha(raw),'raw':raw}


def require_absent(paths):
    for p in paths:
        p=Path(p)
        require(p.is_absolute() and p==p.resolve() and p.is_relative_to(ROOT)
            and all(not q.is_symlink() for q in [p,*p.parents] if q.is_relative_to(ROOT)),
            'Redirected absence path refused')
        require(not p.exists() and not p.is_symlink(),'Target/retry path already exists; no automatic replay: '+str(p))


def freeze_controllers(c,descriptor):
    directory=ROOT/'plans'/PHASE;ledger=ROOT/'state'/(PHASE+'-submission.json')
    require(not directory.exists() and not ledger.exists(),'Fresh controller plan/ledger only; no replay')
    directory.mkdir(mode=0o700);tasks=[];previous=None
    for i in range(4):
        name='af-f2-registry-r2-v1-controller-'+str(i)
        command=shlex.join([sys.executable,'-s','-u',str(CONTROLLER),'--descriptor',str(descriptor),
            '--descriptor-sha256',digest(descriptor),'--segment',str(i)])
        text=slurm._script(c['config'],FRAMEWORK,name,command,cpus=1,memory='16G',hours=96,gpu=False)
        text=text.replace('set -euo pipefail','set -euo pipefail\nexport TZ=UTC\nexport PYTHONDONTWRITEBYTECODE=1',1)
        script=directory/(name+'.sbatch');exclusive_bytes(script,text.encode())
        tasks.append({'name':name,'command':command,'script':str(script),'script_sha256':canonical_hash(text),
            'depends':[previous] if previous else [],'dependency_kind':'afterany' if previous else 'afterok',
            'external_dependencies':[] if previous else [c['job']],
            'resources':{'cpus':1,'gpu':False,'hours':96,'memory':'16G','array':None}});previous=name
    plan={'schema_version':2,'phase':PHASE,'root':str(ROOT),'config_sha256':CONFIG,'release_identity':source_identity(FRAMEWORK),
        'tasks':tasks,'maximum_simultaneous_resources':{'cpus':1,'gpus':0},'cpu_ceiling':256,'gpu_concurrency':4,
        'resource_policy':c['policy'].receipt(),'descriptor_path':str(descriptor),'descriptor_sha256':digest(descriptor),
        'scientific_gates_changed':False,'operational_recovery_case':str(AMENDMENT)}
    path=directory/'plan.json';exclusive(path,plan);return {'path':path,'ledger':ledger,'value':plan}


def read_held(c,task,job):
    require(job not in OLD_IDS and job!=c['job'] and re.fullmatch('[0-9]+',job),'Only new owned controller jobs may be managed')
    c['reads']=c.get('reads',0)+1
    v=c['controller']['fields'](c['scheduler'](job,AREA/('held-read-'+str(c['reads']).zfill(5))))
    r=task['resources'];memory=lambda s:int(s[:-1])*(1024**(2 if s[-1]=='M' else 3)) if re.fullmatch('[1-9][0-9]*[MG]',s) else -1
    require(v.get('JobId')==job and v.get('UserId')=='pwa209(3164768)' and v.get('Account')=='def-ptewarie_cpu'
        and v.get('JobName')=='hhw-'+task['name'] and v.get('Command')==task['script']
        and v.get('NumCPUs')=='1' and v.get('NumNodes') in {'1','1-1'} and memory(v.get('MinMemoryNode',''))==memory('16G')
        and c['controller']['seconds'](v.get('TimeLimit',''))==96*3600
        and c['controller']['gpu_count'](v.get('ReqTRES',''))==0,'Exact new held native owner/script/resources required')
    for key in ('TresPerNode','TresPerJob','TresPerTask','Gres'):require('gpu' not in v.get(key,'').lower(),'Unexpected GPU field in new controller')
    alloc=v.get('AllocTRES','')
    require(not alloc or alloc=='(null)' or c['controller']['gpu_count'](alloc)==0,'Unexpected allocated GPU')
    return v


def cleanup_held(c,pack,*,runner=subprocess.run):
    """Never cancel an existing, released, running, foreign or ambiguous job."""
    if pack is None or not pack['ledger'].is_file():return []
    ledger=read_json(pack['ledger']);out=[]
    require(ledger.get('plan_sha256')==canonical_hash(pack['value']) and ledger.get('source_sha256')==SOURCE,'Cleanup refuses an unbound ledger')
    for task in pack['value']['tasks']:
        value=ledger.get('tasks',{}).get(task['name'],{});job=value.get('job_id')
        if not job or value.get('state')!='submitted' or value.get('script_sha256')!=task['script_sha256']:continue
        try:
            v=read_held(c,task,str(job))
            if v.get('JobState')!='PENDING' or v.get('Reason')!='JobHeldUser':
                out.append({'job_id':job,'status':'not_cancelled_not_still_held'});continue
            path=AREA/('cancel-new-held-'+job+'-intent.json')
            exclusive(path,{'job_id':job,'native_owner_proof':v,'automatic_replay_permitted':False})
            response=runner(['scancel',job],capture_output=True,text=True,timeout=30)
            exclusive(AREA/('cancel-new-held-'+job+'-result.json'),{'returncode':response.returncode,'stdout':response.stdout,'stderr':response.stderr})
            response.check_returncode();out.append({'job_id':job,'status':'cancel_requested_once_for_positive_new_held_job'})
        except BaseException as error:out.append({'job_id':job,'status':'cleanup_needs_explicit_reconciliation','error':str(error)})
    exclusive(AREA/'cleanup-new-held.json',{'jobs':out,'existing_jobs_cancelled':False,'automatic_replay_permitted':False})
    return out


def verify_manifest(path,expected_sha):
    require(Path(path)==SUPPORT/'source-manifest.json','Exact source manifest path required')
    manifest=frozen(path,expected_sha)
    require(set(manifest)=={'kind','files'} and manifest['kind']==MANIFEST_KIND
        and isinstance(manifest['files'],dict) and 5<=len(manifest['files'])<=50,'Exact bounded source freeze manifest required')
    for name,h in manifest['files'].items():
        require(isinstance(name,str) and Path(name).name==name and name.endswith('.py')
            and isinstance(h,str) and re.fullmatch('[0-9a-f]{64}',h),'Exact frozen source basename/SHA required')
        packet(SUPPORT/name,h)
    require(manifest['files'].get(Path(__file__).name)==digest(Path(__file__))
        and QHELPER.name in manifest['files'] and QTEST in manifest['files'],'Own source and native qualifier must be frozen')
    return manifest['files']


def assert_guard(c):
    require(c.get('registration_locked') is True and not MUTEX.is_symlink() and MUTEX.is_file(),
        'Permanent registration mutex must be held')
    value=MUTEX.stat()
    require((value.st_dev,value.st_ino)==c.get('mutex_identity') and value.st_ino>0,
        'Permanent registration mutex inode changed')
    return value


@contextmanager
def registration_lock(c,label):
    require(not c.get('registration_locked'),'Nested activation registration transaction refused')
    with c['registry_api']['registration_lock'](AREA/label,c['end'],role='controller-start'):
        require(MUTEX.is_file() and not MUTEX.is_symlink(),'Permanent ordinary mutex required')
        stat=MUTEX.stat();c['mutex_identity']=(stat.st_dev,stat.st_ino);c['registration_locked']=True
        try:
            assert_guard(c);yield;assert_guard(c)
        finally:c['registration_locked']=False


def normalized(c,job,record):
    value=c['controller']['normalized_native'](job,record)
    require(set(value)=={'job_id','state','exit_code','cpus','gpus','elapsed_seconds','end','user','account'},
        'Exact scalar native proof required')
    return value


def fresh_native(c,ids):
    observed=c['native_reader'](sorted(ids),{},owner='pwa209',account=None)
    require(set(observed)==set(ids),'Every exact required native allocation must be freshly observed')
    return {job:normalized(c,job,record) for job,record in observed.items()}


def evaluation_inventory(plan,state):
    binding=state['cells'][73]['evaluation_plan'];directory=Path(binding['case'])
    require(directory==Path(plan['cells'][73]['evaluation_case']) and directory.is_dir()
        and directory==directory.resolve() and not directory.is_symlink(),'Exact original evaluation directory required')
    expected={'plan.json':binding['plan_sha256'],'plan-binding.json':binding['plan_binding_sha256']};values=[]
    for child in directory.iterdir():
        require(child.name in expected and len(values)<2 and child.is_file() and not child.is_symlink(),
            'Evaluation directory contains partial outputs or an unexpected entry')
        raw=packet(child,expected[child.name])
        values.append({'path':str(child),'sha256':raw['sha256'],'is_file':True,'is_dir':False,'is_symlink':False})
    require({Path(v['path']).name for v in values}==set(expected),'Complete two-file evaluation plan inventory required')
    return sorted(values,key=lambda v:v['path'])


def absence_snapshot(c,plan,state,original_file_sha):
    stat=assert_guard(c);now=time.time();paths=[Path(p) for p in c['recovery']['absence_paths'](plan)]
    require_absent(paths);inventory=evaluation_inventory(plan,state);assert_guard(c);validated=time.time()
    require(0<=validated-now<=30,'Actual absence inventory exceeded its finite freshness budget')
    return {'registration_mutex':str(MUTEX),'registration_mutex_held':True,'mutex_device':stat.st_dev,'mutex_inode':stat.st_ino,
        'same_lock_held_through_validation':True,'captured_original_state_sha256':state['state_sha256'],
        'captured_original_state_file_sha256':original_file_sha,'captured_original_revision':state['revision'],
        'native_campaign_controller_ids':sorted(CONTROLLERS),'captured_unix':now,'validation_unix':validated,
        'entries':[{'path':str(p),'exists':False,'is_symlink':False} for p in sorted(paths,key=str)],
        'evaluation_top_level':inventory}


def freeze_packets(proof):
    """Keep every supplied byte packet and its original locator before pure derive."""
    directory=AMENDMENT/'packets';directory.mkdir();index=[]
    def visit(value):
        if isinstance(value,dict) and set(value)=={'path','sha256','raw'}:
            require(sha(value['raw'])==value['sha256'],'Proof byte packet changed before preservation')
            dest=directory/('packet-'+str(len(index)).zfill(4)+'.bin');exclusive_bytes(dest,value['raw'])
            index.append({'original_path':value['path'],'sha256':value['sha256'],'bytes':len(value['raw']),
                'preserved_path':str(dest),'preserved_sha256':digest(dest)})
        elif isinstance(value,dict):
            for item in value.values():visit(item)
        elif isinstance(value,list):
            for item in value:visit(item)
    visit(proof);exclusive(AMENDMENT/'source-packet-index.json',{'packets':index,'scientific_result':False,'admission_authority':False})
    return index


def verify_preserved_sources(index):
    for entry in index:
        require(digest(Path(entry['preserved_path']))==entry['preserved_sha256']==entry['sha256'],
            'Preserved source packet changed')
        packet(entry['original_path'],entry['sha256'])


def prepare_recovery(c,approval):
    assert_guard(c);c['verify_sources']()
    require(not AMENDMENT.exists() and not AMENDMENT.is_symlink(),'One-time amendment namespace exists; explicit reconciliation required')
    current=packet(CAMPAIGN/'state.json.gz',ORIGINAL_FILE_SHA);binding=packet(CAMPAIGN/'state-binding.json');original_plan=packet(CAMPAIGN/'campaign-plan.json')
    require(len(current['raw'])<=64_000_000,'Bounded original compressed state required')
    with gzip.GzipFile(fileobj=io.BytesIO(current['raw'])) as stream:expanded=stream.read(512_000_001)
    require(len(expanded)<=512_000_000,'Bounded original expanded state required')
    state=json.loads(expanded);plan=json.loads(original_plan['raw'])
    require(state.get('revision')==405 and state.get('state_sha256')==ORIGINAL_SHA and len(state.get('jobs',{}))==219,
        'Exact original revision405 state required')
    require(plan.get('plan_sha256')==PLAN_SHA and canonical_hash({k:v for k,v in plan.items() if k!='plan_sha256'})==PLAN_SHA,
        'Exact immutable original campaign plan required')
    before=evaluation_inventory(plan,state)
    paths=[Path(p) for p in c['recovery']['absence_paths'](plan)];require_absent(paths)
    fresh=fresh_native(c,OLD_IDS);producer=fresh_native(c,{c['job']})[c['job']]
    AMENDMENT.mkdir(mode=0o700);(AMENDMENT/'native').mkdir(mode=0o700)
    originals={'original_state_gzip':preserved_packet(AMENDMENT/'original-state.json.gz',current['raw']),
        'original_state_binding':preserved_packet(AMENDMENT/'original-state-binding.json',binding['raw']),
        'original_campaign_plan':preserved_packet(AMENDMENT/'original-campaign-plan.json',original_plan['raw'])}
    def save(path,value):
        exclusive(path,value);return packet(path)
    proof={'case':str(AMENDMENT),'dispatcher_sha256':c['recovery']['DISPATCH_SHA'],**originals,
        'approval':save(AMENDMENT/'current-user-authorization.json',approval),
        'producer':save(AMENDMENT/'native/producer.json',{'native':producer,'source_sha256':SOURCE,'config_sha256':CONFIG}),
        'worker_descriptor':packet(WORKER_DESCRIPTOR,WORKER_DESCRIPTOR_SHA),'failed':{},'old_controllers':{},
        'old_recovery':{'path':str(OLD_RECOVERY),'sha256':OLD_RECOVERY_SHA}}
    for job in CONTROLLERS:proof['old_controllers'][job]=save(AMENDMENT/'native'/('controller-'+job+'.json'),fresh[job])
    for ident,job in TARGETS.items():
        name='af-f2-'+ident;directory=ROOT/'plans'/name
        evidence={'native':save(AMENDMENT/'native'/('worker-'+job+'.json'),fresh[job]),
            'native_plan':packet(directory/'plan.json'),'submission_ledger':packet(ROOT/'state'/(name+'-submission.json')),
            'script':packet(directory/(name+'.sbatch')),'submission_journal':packet(CAMPAIGN/('submission-'+ident+'.json')),
            'traceback':packet(ROOT/'logs'/(name+'-'+job+'_4294967294.log'))}
        if ident.startswith('evaluation-'):
            directory=Path(state['cells'][73]['evaluation_plan']['case'])
            evidence.update(evaluation_plan=packet(directory/'plan.json'),evaluation_plan_binding=packet(directory/'plan-binding.json'))
        proof['failed'][ident]=evidence
    absent=absence_snapshot(c,plan,state,current['sha256'])
    require(absent['evaluation_top_level']==before,'Evaluation inventory changed while capturing proof')
    proof['absence']=save(AMENDMENT/'verified-absence-under-lock.json',absent)
    index=freeze_packets(proof);assert_guard(c)
    result=c['recovery']['recover'](plan,state,proof)
    require(result['state']['revision']==406 and result['amendment']['original_state_sha256']==ORIGINAL_SHA,
        'Pure helper returned a different state lineage')
    c['controller']['validate_retry_value']({**c['old_descriptor'],'prestart_retry_amendment':{'job_id':c['job']}},result['amendment'])
    exclusive(AMENDMENT/'amendment.json',result['amendment'])
    return result,plan,state,paths,originals,index


def install_state(c,result,plan,old_state,paths,originals,index,*,replace=os.replace):
    """Two exact replacements under the same permanent guard; no implicit rollback."""
    assert_guard(c);c['verify_sources']();verify_preserved_sources(index)
    current=CAMPAIGN/'state.json.gz';binding=CAMPAIGN/'state-binding.json'
    require(digest(current)==originals['original_state_gzip']['sha256']
        and digest(binding)==originals['original_state_binding']['sha256']
        and packet(CAMPAIGN/'campaign-plan.json')['raw']==originals['original_campaign_plan']['raw'],
        'Original active state/binding/plan changed before CAS')
    absence_snapshot(c,plan,old_state,originals['original_state_gzip']['sha256'])
    state=result['state'];buffer=io.BytesIO()
    with gzip.GzipFile(fileobj=buffer,mode='wb',mtime=0,filename='') as stream:
        stream.write(json.dumps(state,sort_keys=True,separators=(',',':'),allow_nan=False).encode())
    raw=buffer.getvalue();new_binding={'state_file_sha256':sha(raw),'state_sha256':state['state_sha256'],
        'campaign_plan_sha256':plan['plan_sha256'],'revision':state['revision'],'scientific_result':False,'admission_authority':False}
    binding_raw=(json.dumps(new_binding,indent=2,sort_keys=True)+'\n').encode()
    exclusive_bytes(AMENDMENT/'derived-state.json.gz',raw);exclusive_bytes(AMENDMENT/'derived-state-binding.json',binding_raw)
    intent={'status':'before_exact_state_pair_replacement','old_state_file_sha256':digest(current),'old_binding_file_sha256':digest(binding),
        'new_state_file_sha256':sha(raw),'new_binding_file_sha256':sha(binding_raw),'automatic_replay_permitted':False,'native_job_id':c['job'],
        'mutex_device':c['mutex_identity'][0],'mutex_inode':c['mutex_identity'][1]}
    exclusive(AMENDMENT/'cas-intent.json',intent)
    temp=CAMPAIGN/('.state-prestart-r2-'+c['job']+'.tmp');temp_binding=CAMPAIGN/('.binding-prestart-r2-'+c['job']+'.tmp')
    exclusive_bytes(temp,raw);exclusive_bytes(temp_binding,binding_raw)
    assert_guard(c);require_absent(paths)
    require(digest(current)==intent['old_state_file_sha256'] and digest(binding)==intent['old_binding_file_sha256'],
        'CAS source changed immediately before replacement')
    replace(temp,current);assert_guard(c);replace(temp_binding,binding)
    require(digest(current)==sha(raw) and digest(binding)==sha(binding_raw),'Installed exact state pair failed verification')
    exclusive(AMENDMENT/'installation.json',{'status':'exact_three_target_derivative_installed','native_job_id':c['job'],
        'state_sha256':state['state_sha256'],'state_file_sha256':sha(raw),'binding_sha256':sha(binding_raw),
        'amendment_sha256':result['amendment']['amendment_sha256'],'failed_history_preserved':True,
        'healthy_policies_retrained':False,'scientific_result':False,'admission_authority':False})


def orchestrate(c,approval,*,runner=subprocess.run):
    pack=None
    try:
        with registration_lock(c,'activation-three-target-registration'):
            c['admission']('activation-before-new-registration',cpus=1,gpus=0,hours=96)
            result,plan,state,paths,originals,index=prepare_recovery(c,approval)
            old=c['old_descriptor'];descriptor=dict(old,controller_sha256=c['pins'][CONTROLLER.name],
                registry_overlay={'guard':{'path':str(GUARD),'sha256':c['pins'][GUARD.name]},
                    'worker_shim':{'path':str(SUPPORT/'run-f00d-worker-registry-overlay-20261021.py'),
                        'sha256':c['pins']['run-f00d-worker-registry-overlay-20261021.py']},
                    'recovery':dict(old['registry_overlay']['recovery']),
                    'qualification':{'path':str(c['qpath']),'sha256':c['qsha'],'job_id':str(c['q']['slurm_job_id'])}},
                prestart_retry_amendment={'path':str(AMENDMENT/'amendment.json'),'sha256':digest(AMENDMENT/'amendment.json'),'job_id':c['job']})
            require(descriptor['registry_overlay']['recovery']=={'path':str(OLD_RECOVERY),'sha256':OLD_RECOVERY_SHA},
                'Original training0000 recovery locator must remain unchanged')
            descriptor_path=AREA/'descriptor-prestart-three-v1.json';exclusive(descriptor_path,descriptor)
            c['controller']['worker_binding'](descriptor);c['controller']['registry_binding'](descriptor)
            c['controller']['validate_retry_value'](descriptor,result['amendment'])
            pack=freeze_controllers(c,descriptor_path)
            exclusive(AREA/'new-held-submission-intent.json',{'plan_path':str(pack['path']),'plan_sha256':canonical_hash(pack['value']),
                'ledger_path':str(pack['ledger']),'automatic_replay_permitted':False,'original_state_not_yet_replaced':True})
            def held(command,**kw):
                require(command[0]=='sbatch' and '--hold' not in command and time.time()+900<c['end'],
                    'One new held sbatch within native margin required')
                return runner(['sbatch','--hold',*command[1:]],timeout=30,**kw)
            ledger=slurm.submit(pack['path'],pack['ledger'],runner=held)
            jobs=[ledger['tasks'][t['name']]['job_id'] for t in pack['value']['tasks']]
            require(len(jobs)==len(set(jobs))==4,'Exactly four distinct new controller jobs required')
            proofs={}
            for task,job in zip(pack['value']['tasks'],jobs):
                v=read_held(c,task,job)
                require(v.get('JobState')=='PENDING' and v.get('Reason')=='JobHeldUser','All new controllers must remain held before CAS')
                expected='afterok:'+c['job'] if task is pack['value']['tasks'][0] else 'afterany:'+jobs[len(proofs)-1]
                require(re.fullmatch(re.escape(expected)+r'(?:\(unfulfilled\))?',v.get('Dependency','')),
                    'Actual held dependency must bind activation success or the previous finite controller')
                proofs[job]=v
            exclusive(AREA/'new-held-native-proof.json',proofs)
            c['admission']('activation-all-new-controllers-registered',cpus=0,gpus=0,hours=1)
            install_state(c,result,plan,state,paths,originals,index)
            exclusive(AREA/'release-intent.json',{'job_ids':jobs,'held_proof_sha256':digest(AREA/'new-held-native-proof.json'),
                'state_installation_sha256':digest(AMENDMENT/'installation.json'),'automatic_replay_permitted':False})
        for task,job in zip(pack['value']['tasks'],jobs):
            require(not c.get('registration_locked') and time.time()+600<c['end'],'Outside-lock finite release margin required')
            c['verify_sources']();v=read_held(c,task,job)
            require(v.get('JobState')=='PENDING' and v.get('Reason')=='JobHeldUser','New job changed before one-time release')
            exclusive(AREA/('release-'+job+'-intent.json'),{'job_id':job,'automatic_replay_permitted':False})
            response=runner(['scontrol','release',job],capture_output=True,text=True,timeout=30)
            exclusive(AREA/('release-'+job+'-result.json'),{'returncode':response.returncode,'stdout':response.stdout,'stderr':response.stderr})
            response.check_returncode()
        receipt={'status':'three_target_derivative_installed_and_four_finite_controllers_released','native_job_id':c['job'],
            'jobs':jobs,'qualification':{'path':str(c['qpath']),'sha256':c['qsha'],'job_id':str(c['q']['slurm_job_id'])},
            'controller_descriptor':{'path':str(descriptor_path),'sha256':digest(descriptor_path)},
            'state_installation':{'path':str(AMENDMENT/'installation.json'),'sha256':digest(AMENDMENT/'installation.json')},
            'failed_history_preserved':True,'healthy_trainers_retrained':False,'existing_jobs_held_or_cancelled':False,
            'plan':{'path':str(pack['path']),'sha256':digest(pack['path']),'ledger_path':str(pack['ledger'])},
            'scientific_result':False,'admission_authority':False}
        exclusive(AREA/'receipt.json',receipt);return receipt
    except BaseException as error:
        cleanup=[]
        try:cleanup=cleanup_held(c,pack,runner=runner)
        except BaseException as cleanup_error:cleanup=[{'status':'cleanup_needs_explicit_reconciliation','error':str(cleanup_error)}]
        exclusive(AREA/'activation-failure.json',{'status':'needs_explicit_reconciliation','error_type':type(error).__name__,'error':str(error),
            'cleanup':cleanup,'automatic_replay_permitted':False,'scientific_result':False,'admission_authority':False})
        raise


def registered_activation(api,plan_path,ledger_path,task_name,script,job,config,argv):
    plan_path=Path(plan_path);ledger_path=Path(ledger_path);script=Path(script)
    packet(plan_path);packet(ledger_path);packet(script)
    require(plan_path.is_relative_to(ROOT/'plans') and ledger_path.is_relative_to(ROOT/'state')
        and script.is_relative_to(ROOT/'plans'),'Registered activation must use the study plan/ledger namespace')
    driver=runpy.run_path(str(api['DRIVER']),run_name='prestart_activation_binding_driver')
    plan,ledger=driver['checked_pair'](plan_path,ledger_path,ROOT,config)
    selected=[t for t in plan['tasks'] if t['name']==task_name]
    require(len(selected)==1,'Exactly one registered activation task required')
    task=selected[0];record=ledger.get('tasks',{}).get(task_name,{})
    command=shlex.split(task['command']);own=str(Path(__file__))
    require(command.count(own)==1 and command[command.index(own)+1:]==list(argv)
        and task['command'] in script.read_text() and task.get('script')==str(script)
        and canonical_hash(script.read_text())==task.get('script_sha256')==record.get('script_sha256')
        and plan.get('release_identity',{}).get('sha256')==SOURCE and plan.get('config_sha256')==CONFIG
        and record.get('job_id')==job and record.get('state')=='submitted'
        and task.get('resources')=={'cpus':4,'gpu':False,'hours':4,'memory':'16G'},
        'Exact own4CPU4h source/argv/plan/ledger/script binding required')
    return {'plan':{'path':str(plan_path),'sha256':digest(plan_path)},'ledger':{'path':str(ledger_path),'sha256':digest(ledger_path)},
        'script':{'path':str(script),'sha256':digest(script)},'task_name':task_name,'native_job_id':job}


def verify_qualification(api,controller,pins,job,config,expected_sha=None):
    qpath=SUPPORT/'private-qualification.json';actual=packet(qpath,expected_sha);q=json.loads(actual['raw'])
    require(q.get('status')=='complete_f2_registry_overlay_qualification' and type(q.get('returncode')) is int and q['returncode']==0
        and q.get('source_sha256')==SOURCE and q.get('config_sha256')==CONFIG and str(q.get('slurm_job_id'))==job
        and q.get('qualification_helper_sha256')==pins[QHELPER.name]
        and q.get('controller_sha256')==pins[CONTROLLER.name] and q.get('registry_guard_sha256')==pins[GUARD.name]
        and q.get('worker_shim_sha256')==pins['run-f00d-worker-registry-overlay-20261021.py']
        and q.get('scientific_result') is False and q.get('admission_authority') is False,
        'Positive exact source-bound separately owned native qualification required')
    controller['verify_operational_tests'](q)
    for key,helper,test,count,roster in (
        ('new_recovery_qualification',RECOVERY.name,'test_recover_f2_prestart_three_state_v1_20261022.py',api['NEW_RECOVERY_COUNT'],api['NEW_RECOVERY_ROSTER']),
        ('activation_qualification',Path(__file__).name,'test_activate_f2_prestart_three_v1_20261022.py',api['ACTIVATION_COUNT'],api['ACTIVATION_ROSTER'])):
        value=q.get(key,{})
        require(type(count) is int and count>0 and re.fullmatch('[0-9a-f]{64}',roster)
            and value.get('helper')=={'path':str(SUPPORT/helper),'sha256':pins[helper]}
            and value.get('test_source')=={'path':str(SUPPORT/test),'sha256':pins[test]}
            and value.get('test_counts')=={'tests':count,'failures':0,'errors':0,'skipped':0}
            and value.get('test_roster_sha256')==roster,'Complete separately frozen native helper/test qualification required')
        junit=value.get('junit',{});require(set(junit)=={'path','sha256'},'Exact actual native helper JUnit locator required')
        require(Path(junit['path']).is_relative_to(SUPPORT),'Native helper JUnit must remain in its qualified support case')
        packet(junit['path'],junit['sha256']);api['junit_roster'](junit['path'],count,roster)
    registration=q.get('native_registration',{})
    require(registration.get('native_job_id')==job,'Qualification must bind its actual registered native producer')
    observed=api['registered_task'](registration['plan']['path'],registration['ledger']['path'],registration['task_name'],
        registration['script']['path'],job,config)
    require(observed==registration,'Actual qualification plan/ledger/script changed')
    require(slurm.completed_native_dependency(ROOT,job) is not None,'Positive completed native qualification producer required')
    return q,actual['sha256']


def main():
    parser=argparse.ArgumentParser()
    for name in ('native-plan','native-ledger','native-script','source-manifest'):parser.add_argument('--'+name,type=Path,required=True)
    for name in ('native-task','source-manifest-sha256','qualification-job-id','approval-reference'):parser.add_argument('--'+name,required=True)
    parser.add_argument('--qualification-sha256')
    args=parser.parse_args();os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    pins=verify_manifest(args.source_manifest,args.source_manifest_sha256)
    api=runpy.run_path(str(QHELPER),run_name='prestart_three_qualified_support')
    require(set(pins)==set(api['PINS'])|{QHELPER.name,QTEST}
        and all(pins[name]==h for name,h in api['PINS'].items()),'Complete qualification source/test manifest differs')
    api['verify_pins']()
    config=read_json(PRIMARY/'configs/study.json');job=str(native_job_id() or '')
    import horsehair
    require(job.isdigit() and job!=args.qualification_job_id and re.fullmatch('[0-9]+',args.qualification_job_id)
        and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209' and os.environ.get('SLURM_CPUS_PER_TASK')=='4'
        and Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA
        and canonical_hash(config)==CONFIG,'Owned4CPU qualified/frozen native activation required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval context required')
    require(not AREA.exists() and not AMENDMENT.exists() and not (ROOT/'plans'/PHASE).exists()
        and not (ROOT/'state'/(PHASE+'-submission.json')).exists(),'One-time activation namespace already exists; no replay')
    AREA.mkdir(mode=0o700)
    controller=runpy.run_path(str(CONTROLLER),run_name='prestart_three_controller_contract')
    scheduler=runpy.run_path(str(api['SCHEDULER']))['run_scontrol_job']
    allocation=controller['fields'](scheduler(job,AREA/'own-scheduler-read'))
    require(allocation.get('JobId')==job and allocation.get('UserId')=='pwa209(3164768)' and allocation.get('Account')=='def-ptewarie_cpu'
        and allocation.get('JobState')=='RUNNING' and allocation.get('NumCPUs')=='4' and allocation.get('NumNodes') in {'1','1-1'}
        and allocation.get('Command')==str(args.native_script) and controller['seconds'](allocation.get('TimeLimit',''))==4*3600
        and allocation.get('MinMemoryNode') in {'16G','16384M'} and controller['gpu_count'](allocation.get('AllocTRES',''))==0,
        'Exact owned4CPU16GB4h activation allocation/script required')
    require(all('gpu' not in allocation.get(k,'').lower() for k in ('ReqTRES','TresPerNode','TresPerTask','TresPerJob','Gres')),
        'CPU-only activation required')
    end=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    native_end=datetime.fromisoformat(allocation['EndTime']).replace(tzinfo=timezone.utc).timestamp()
    require(math.isfinite(end) and abs(end-native_end)<=2 and time.time()+1800<end,'Verified native activation margin required')
    # Install the pinned parser before campaign imports capture its reference.
    runpy.run_path(str(api['NATIVE_GUARD']))['install'](AREA/'accounting-reads')
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'358 must precede campaign native binding')
    registry_api=runpy.run_path(str(GUARD),run_name='prestart_three_registry_guard')
    registry_api['install'](AREA/'registered-snapshot-reads',registration_mode='controller-already-locked',allocation_deadline_unix=end)
    registration=registered_activation(api,args.native_plan,args.native_ledger,args.native_task,args.native_script,job,config,sys.argv[1:])
    policy=load_resource_policy(api['POLICY'],api['POLICY_SHA'],root=ROOT,config=config)
    # The afterok qualifier has ended, but its final sacct proof must also be
    # published by the actual guarded admission reader before dependency checks.
    startup={'registry_api':registry_api,'end':end}
    with registration_lock(startup,'activation-qualification-accounting'):
        api['require_fresh_census'](read_json(ROOT/'state/storage-budget.json'))
        campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],AREA,
            'activation-qualification-accounting',resource_policy=policy,cpus=0,gpus=0,hours=1)
    q,qsha=verify_qualification(api,controller,pins,args.qualification_job_id,config,args.qualification_sha256)
    old=frozen(OLD_DESCRIPTOR,OLD_DESCRIPTOR_SHA)
    require(old.get('execution_source_sha256')==SOURCE and old.get('agent_campaign_case')==str(CAMPAIGN)
        and old.get('registry_overlay',{}).get('recovery')=={'path':str(OLD_RECOVERY),'sha256':OLD_RECOVERY_SHA},
        'Exact original controller and training0000 recovery lineage required')
    packet(OLD_RECOVERY,OLD_RECOVERY_SHA);packet(WORKER_DESCRIPTOR,WORKER_DESCRIPTOR_SHA)
    c={'job':job,'config':config,'policy':policy,'controller':controller,'scheduler':scheduler,'native_reader':repair._repair_native_states,
        'registry_api':registry_api,'end':end,'recovery':runpy.run_path(str(RECOVERY),run_name='prestart_three_pure_recovery'),
        'pins':pins,'q':q,'qpath':SUPPORT/'private-qualification.json','qsha':qsha,'old_descriptor':old}
    def unchanged_sources():
        require(verify_manifest(args.source_manifest,args.source_manifest_sha256)==pins,'Frozen activation manifest changed')
        api['verify_pins']()
        require(digest(c['qpath'])==qsha and source_identity(FRAMEWORK)['sha256']==SOURCE
            and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,'Qualified source/Q changed before activation')
        packet(OLD_DESCRIPTOR,OLD_DESCRIPTOR_SHA);packet(OLD_RECOVERY,OLD_RECOVERY_SHA);packet(WORKER_DESCRIPTOR,WORKER_DESCRIPTOR_SHA)
    c['verify_sources']=unchanged_sources
    def admit(label,**resources):
        assert_guard(c);api['require_fresh_census'](read_json(ROOT/'state/storage-budget.json'))
        return campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],AREA,label,
            resource_policy=policy,**resources)
    c['admission']=admit
    with resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=4,job_id=job,resource_policy=policy):
        quota=runpy.run_path(str(api['QUOTA']))['require_storage_headroom_with_wait'](ROOT,required_files=3000,required_bytes=10_000_000_000,
            reserve_files=247000,reserve_bytes=2_000_000_000_000,evidence_dir=AREA/'quota-reads',allocation_deadline_unix=end)
        from horsehair.admission import _storage
        census=ROOT/'state/storage-budget.json';census_sha=digest(census);api['require_fresh_census'](read_json(census))
        future_bytes,future_files,paths=_storage(ROOT,config,10_000_000_000,3000,personal_headroom=quota)
        require(digest(census)==census_sha,'Actual storage census changed during activation validation')
        exclusive(AREA/'intent.json',{'native_registration':registration,'qualification_sha256':qsha,'qualification_job_id':args.qualification_job_id,
            'source_manifest':{'path':str(args.source_manifest),'sha256':args.source_manifest_sha256},'native_allocation':allocation,
            'census_sha256':census_sha,'future_study_bytes':future_bytes,'future_study_files':future_files,
            'automatic_replay_permitted':False,'scientific_result':False,'admission_authority':False})
        approval={'source':'current_user','authorizes_targeted_operational_repair':True,'targets':dict(TARGETS),'request_reference':args.approval_reference}
        require(bool(args.approval_reference.strip()),'Nonempty current-user authorization reference required')
        receipt=orchestrate(c,approval)
        print('AF_F2_PRESTART_THREE_ACTIVATED '+json.dumps(receipt,sort_keys=True),flush=True)


if __name__=='__main__':main()
