"""One explicit0000 migration/CAS and four new finite controller segments.

Only newly created, positively owned still-held controller jobs may be cancelled
on failure. No existing profiler, trainer, CDE controller or scientific data is
held/cancelled. Ambiguous mutations never replay automatically.
"""
from pathlib import Path
from datetime import datetime,timezone
import argparse
import base64
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

from horsehair.core import canonical_hash,digest,read_json,source_identity,native_job_id,exclusive_file_lock
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy
from horsehair import slurm

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PROJECT_ALIAS=Path('/project/def-ptewarie')
PROJECT_TARGET=Path('/lustre06/project/6101854')
PRIMARY_RESOLVED=PROJECT_TARGET/'pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf'
PRIMARY_CONFIG_RAW_SHA='b872f121b6cef11a7726fc56b4c237c714fab41bbafa2a6090d7e2c1e2f91353'
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
SUPPORT=ROOT/'maintenance/study-af-registry-repair-support-v3-20261021'
AREA=SUPPORT/'native-activation-v2'
AMENDMENT=ROOT/'maintenance/study-af-training0000-state-repair-v1-20261021'
CAMPAIGN=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
QHELPER=SUPPORT/'qualify-f2-registry-repair-v3-20261021.py'
QHELPER_SHA='88814b738f37fb8470072e65027b4844a69794c720cd1ce98749904f5ce97d52'
CONTROLLER=SUPPORT/'run-af-agent-controller-registry-v3-20261021.py'
PHASE='study-af-f2-registry-r1-controllers-v2-20261021'
POST_ACTIVATION=ROOT/'maintenance/study-af-postactivation-evidence-v1-20261021/evidence.json'
FAILED_ACTIVATION=ROOT/'maintenance/study-af-registry-repair-support-v1-20261021/native-activation/activation-failure.json'
FAILED_ACTIVATION_SHA='34ff2e73aa3909d4a6bac87fd3fafb04fd7cc3fb4fa353e34835c3a15478c943'
OLD_IDS={'6689664','6689708','6689921','6689974','6689295','6689301','6689323','6689324'}
MUTEX=ROOT/'state/af-campaign-registration.mutex'


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


def native_proof(job,record,migration):return migration['normalize_native'](job,record)


def preserved_packet(path,raw):
    exclusive_bytes(path,raw);return {'path':str(path),'sha256':sha(raw),'raw':raw}


def absence_paths(plan):
    target=Path(plan['cells'][0]['training_case'])
    return sorted([target/'checkpoint.pt',target/'summary.json',ROOT/'maintenance/study-af-campaign-v1-20261021/native-6689664/receipt.json',
        ROOT/'plans/af-f2-r1-training-0000',ROOT/'state/af-f2-r1-training-0000-submission.json',CAMPAIGN/'submission-r1-training-0000.json'],key=str)


def require_absent(paths):
    for p in paths:require(not p.exists() and not p.is_symlink(),'Target/retry path already exists; no automatic replay: '+str(p))


def primary_config_packet(path,expected_sha):
    p=Path(path);require(p==PRIMARY/'configs/study.json' and p.is_absolute()
        and PROJECT_ALIAS.is_symlink() and PROJECT_ALIAS.resolve()==PROJECT_TARGET and PROJECT_TARGET.is_dir()
        and p.resolve()==PRIMARY_RESOLVED/'configs/study.json'
        and p.is_file() and not p.is_symlink()
        and all(not a.is_symlink() for a in p.parents if a!=PROJECT_ALIAS)
        and all(not a.is_symlink() for a in [p.resolve(),*p.resolve().parents]),
        'Only the exact original primary config is permitted outside personal scratch')
    identity=source_identity(PRIMARY)
    require(identity['sha256']==PRIMARY_SHA and identity.get('files',{}).get('configs/study.json')==expected_sha==PRIMARY_CONFIG_RAW_SHA,
        'Original primary source/config raw identity required')
    raw=p.read_bytes();require(len(raw)<=1_000_000 and sha(raw)==expected_sha and canonical_hash(json.loads(raw))==CONFIG,
        'Original full configuration raw and canonical identity required')
    require(source_identity(PRIMARY)==identity and digest(p)==expected_sha,'Primary source/config changed during read')
    return {'path':str(p),'sha256':sha(raw),'raw':raw}


def verify_old_files(source):
    for role,entry in source['files'].items():
        p=primary_config_packet(entry['path'],entry['sha256']) if role=='full_config' else packet(entry['path'],entry['sha256'])
        require(len(p['raw'])==entry['bytes'],'Original metadata byte count changed')
    for entry in source['checkpoint_identities'].values():
        p=packet(entry['path'],entry['sha256']);require(len(p['raw'])==entry['bytes'],'Checkpoint size changed')


def registration_lock(c,label):
    return c['registry_api']['registration_lock'](AREA/label,c['end'],role='controller-start')


def verify_pristine_postactivation(source,observed):
    """Bind failed v1 history; current files are revalidated under the CAS lock."""
    failure=observed.get('activation_failure',{})
    require(failure.get('path')==str(FAILED_ACTIVATION) and failure.get('sha256')==FAILED_ACTIVATION_SHA
        and packet(FAILED_ACTIVATION,FAILED_ACTIVATION_SHA)['raw'] and failure.get('record')==frozen(FAILED_ACTIVATION,FAILED_ACTIVATION_SHA),
        'Exact preserved failed v1 activation evidence required')
    record=failure['record']
    require(record.get('status')=='needs_explicit_reconciliation' and record.get('error')=='Ordinary confined source file required'
        and record.get('cleanup')==[] and record.get('automatic_replay_permitted') is False,
        'Only the verified pre-migration v1 activation failure is recoverable here')
    files=source['files'];plan=json.loads(base64.b64decode(files['campaign_plan']['base64']))
    old_phase='study-af-f2-registry-r1-controllers-v1-20261021'
    targets={'amendment':AMENDMENT,'failed_training_case':Path(plan['cells'][0]['training_case']),
        'new_controller_plan':ROOT/'plans'/old_phase,'new_controller_ledger':ROOT/'state'/(old_phase+'-submission.json')}
    for key,path in targets.items():
        value=observed.get(key,{})
        require(value=={'path':str(path),'exists':False,'is_symlink':False},'Postactivation v1 absence proof differs')
    require_absent(list(targets.values())+[ROOT/'plans'/PHASE,ROOT/'state'/(PHASE+'-submission.json')])
    current=observed.get('current_original_state',{})
    original=json.loads(gzip.decompress(base64.b64decode(files['original_state_gzip']['base64'])))
    require(current.get('path')==files['original_state_gzip']['path'] and current.get('file_sha256')==files['original_state_gzip']['sha256']
        and current.get('state_sha256')==original['state_sha256'] and current.get('revision')==original['revision']
        and current.get('needs_repair')==['training-0000']
        and current.get('jobs')=={k:{'job_id':v['job_id'],'status':v['status']} for k,v in original['jobs'].items()},
        'Postactivation proof must retain the exact unchanged rev9 state and healthy1/2/3')
    verify_old_files(source)


def prepare_migration(source,source_packet,fresh,migration,approval):
    """Caller owns the registration lock; copy original bytes before deriving."""
    require(not AMENDMENT.exists(),'One-time amendment already exists; explicit reconciliation required')
    require(set(fresh)==OLD_IDS,'All eight exact historical allocations must be observed freshly')
    normalized={j:native_proof(j,r,migration) for j,r in fresh.items()}
    original_normalized={j:native_proof(j,r,migration) for j,r in source['native_observed'].items()}
    require(normalized==original_normalized,'Fresh UTC terminal/native evidence changed from original253 packet')
    verify_old_files(source)
    files=source['files'];plan=json.loads(base64.b64decode(files['campaign_plan']['base64']))
    original_raw=packet(files['original_state_gzip']['path'],files['original_state_gzip']['sha256'])['raw']
    state=json.loads(gzip.decompress(original_raw));paths=absence_paths(plan);require_absent(paths)
    require(not Path(plan['cells'][0]['training_case']).exists(),'Original0000 training case must remain entirely absent')
    AMENDMENT.mkdir(mode=0o700);(AMENDMENT/'native').mkdir(mode=0o700)
    originals={
        'original_state_gzip':preserved_packet(AMENDMENT/'original-state.json.gz',original_raw),
        'original_state_binding':preserved_packet(AMENDMENT/'original-state-binding.json',packet(files['original_state_binding']['path'],files['original_state_binding']['sha256'])['raw']),
        'original_campaign_plan':preserved_packet(AMENDMENT/'original-campaign-plan.json',packet(files['campaign_plan']['path'],files['campaign_plan']['sha256'])['raw'])}
    proof={'case':str(AMENDMENT),'dispatcher_sha256':migration['DISPATCH_SHA'],'approval':approval,**originals,
        'worker_descriptor':packet(files['worker_descriptor_v2']['path'],files['worker_descriptor_v2']['sha256']),
        'native_evidence_packet':source_packet,'old_controllers':{},'healthy':{}}
    native_packets={}
    for job,value in normalized.items():
        name=('controller-' if job in migration['CONTROLLERS'] else 'worker-')+job+'.json'
        raw=(json.dumps(value,sort_keys=True,allow_nan=False)+'\n').encode();native_packets[job]=preserved_packet(AMENDMENT/'native'/name,raw)
    proof['old_controllers']={j:native_packets[j] for j in migration['CONTROLLERS']}
    proof['failed']={'native':native_packets['6689664'],
        **{key:packet(files[role]['path'],files[role]['sha256']) for key,role in [('native_plan','training-0000_plan'),
            ('submission_ledger','training-0000_ledger'),('script','training-0000_script'),('traceback','failed_training_traceback')]}}
    errata={}
    for intent,old in state['jobs'].items():
        job=old['job_id'];prior=old.get('native_proof');now=normalized[job]
        if prior!=now:
            errata[job]={'old_end':prior['end'],'new_end':now['end'],'all_other_native_fields_unchanged':True,'observed_shift_seconds':14400}
    erratum={'status':'source_time_basis_erratum','old_timezone_verified':False,'old_timezone':'unverified_naive_slurm_local',
        'new_timezone':'UTC','new_query_environment':{'TZ':'UTC'},'jobs':errata}
    proof['clock_erratum']=preserved_packet(AMENDMENT/'clock-erratum.json',(json.dumps(erratum,sort_keys=True)+'\n').encode())
    absent={'registration_mutex':str(MUTEX),'registration_mutex_held':True,'native_campaign_controller_ids':sorted(migration['CONTROLLERS']),
        'captured_original_state_sha256':state['state_sha256'],'captured_unix':time.time(),
        'entries':[{'path':str(p),'exists':False,'is_symlink':False} for p in paths]}
    proof['failed']['absence']=preserved_packet(AMENDMENT/'verified-absence-under-lock.json',(json.dumps(absent,sort_keys=True)+'\n').encode())
    for index,job in migration['HEALTHY'].items():
        ident='training-'+str(int(index)).zfill(4)
        cp=source['checkpoint_identities'][ident]
        proof['healthy'][index]={'native':native_packets[job],
            'worker_receipt':packet(files[ident+'_worker_receipt']['path'],files[ident+'_worker_receipt']['sha256']),
            'summary':packet(files[ident+'_summary']['path'],files[ident+'_summary']['sha256']),
            'checkpoint':packet(cp['path'],cp['sha256'])}
    result=migration['migrate_training0000'](plan,state,proof)
    exclusive(AMENDMENT/'amendment.json',result['amendment'])
    return result,plan,paths,originals


def install_state(result,plan,paths,originals,job,*,replace=os.replace):
    """Lock-held CAS; interruptions remain explicit and never replay silently."""
    current=CAMPAIGN/'state.json.gz';binding=CAMPAIGN/'state-binding.json'
    require(digest(current)==originals['original_state_gzip']['sha256'] and digest(binding)==originals['original_state_binding']['sha256'],
        'Original active state/binding changed before CAS')
    require_absent(paths)
    state=result['state'];buffer=io.BytesIO()
    with gzip.GzipFile(fileobj=buffer,mode='wb',mtime=0,filename='') as stream:
        stream.write(json.dumps(state,sort_keys=True,separators=(',',':'),allow_nan=False).encode())
    raw=buffer.getvalue();new_binding={'state_file_sha256':sha(raw),'state_sha256':state['state_sha256'],
        'campaign_plan_sha256':plan['plan_sha256'],'revision':state['revision'],'scientific_result':False,'admission_authority':False}
    binding_raw=(json.dumps(new_binding,indent=2,sort_keys=True)+'\n').encode()
    exclusive_bytes(AMENDMENT/'derived-state.json.gz',raw);exclusive_bytes(AMENDMENT/'derived-state-binding.json',binding_raw)
    intent={'status':'before_exact_state_pair_replacement','old_state_file_sha256':digest(current),'old_binding_file_sha256':digest(binding),
        'new_state_file_sha256':sha(raw),'new_binding_file_sha256':sha(binding_raw),'automatic_replay_permitted':False,'native_job_id':job}
    exclusive(AMENDMENT/'cas-intent.json',intent)
    temp=CAMPAIGN/('.state-registry-r1-'+job+'.tmp');temp_binding=CAMPAIGN/('.binding-registry-r1-'+job+'.tmp')
    exclusive_bytes(temp,raw);exclusive_bytes(temp_binding,binding_raw)
    require(digest(current)==intent['old_state_file_sha256'] and digest(binding)==intent['old_binding_file_sha256'],
        'CAS source changed immediately before replacement')
    replace(temp,current);replace(temp_binding,binding)
    require(digest(current)==sha(raw) and digest(binding)==sha(binding_raw),'Installed exact state pair failed verification')
    exclusive(AMENDMENT/'installation.json',{'status':'exact_state_derivative_installed','native_job_id':job,
        'state_sha256':state['state_sha256'],'state_file_sha256':sha(raw),'binding_sha256':sha(binding_raw),
        'amendment_sha256':result['amendment']['amendment_sha256'],'failed_history_preserved':True,
        'healthy_policies_retrained':False,'scientific_result':False,'admission_authority':False})


def freeze_controllers(c,descriptor):
    directory=ROOT/'plans'/PHASE;ledger=ROOT/'state'/(PHASE+'-submission.json')
    require(not directory.exists() and not ledger.exists(),'Fresh controller plan/ledger only; no replay')
    directory.mkdir(mode=0o700);tasks=[];previous=None
    for i in range(4):
        name='af-f2-registry-r1-v2-controller-'+str(i)
        command=shlex.join([sys.executable,'-s','-u',str(CONTROLLER),'--descriptor',str(descriptor),
            '--descriptor-sha256',digest(descriptor),'--segment',str(i)])
        text=slurm._script(c['config'],FRAMEWORK,name,command,cpus=1,memory='16G',hours=96,gpu=False)
        text=text.replace('set -euo pipefail','set -euo pipefail\nexport TZ=UTC\nexport PYTHONDONTWRITEBYTECODE=1',1)
        script=directory/(name+'.sbatch');exclusive_bytes(script,text.encode())
        tasks.append({'name':name,'command':command,'script':str(script),'script_sha256':canonical_hash(text),
            'depends':[previous] if previous else [],'dependency_kind':'afterany',
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
        if not job:continue
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


def orchestrate(c,source,source_packet,old_descriptor,approval,*,runner=subprocess.run):
    pack=None
    try:
        with registration_lock(c,'activation-migration-registration'):
            c['admission']('activation-before-new-registration',cpus=1,gpus=0,hours=96)
            fresh=c['native_reader'](sorted(OLD_IDS),{},owner='pwa209',account=None)
            result,plan,paths,originals=prepare_migration(source,source_packet,fresh,c['migration'],approval)
            descriptor=dict(old_descriptor,controller_sha256=c['pins']['run-af-agent-controller-registry-v3-20261021.py'],
                registry_overlay={'guard':{'path':str(SUPPORT/'registered-snapshot-read-guard-v3-20261021.py'),
                    'sha256':c['pins']['registered-snapshot-read-guard-v3-20261021.py']},
                    'worker_shim':{'path':str(SUPPORT/'run-f00d-worker-registry-overlay-20261021.py'),
                        'sha256':c['pins']['run-f00d-worker-registry-overlay-20261021.py']},
                    'recovery':{'path':str(AMENDMENT/'amendment.json'),'sha256':digest(AMENDMENT/'amendment.json')},
                    'qualification':{'path':str(c['qpath']),'sha256':c['qsha'],'job_id':str(c['q']['slurm_job_id'])}})
            descriptor_path=AREA/'descriptor-v5-registry-controller.json';exclusive(descriptor_path,descriptor)
            c['controller']['worker_binding'](descriptor);c['controller']['registry_binding'](descriptor)
            pack=freeze_controllers(c,descriptor_path)
            exclusive(AREA/'new-held-submission-intent.json',{'plan_path':str(pack['path']),'plan_sha256':canonical_hash(pack['value']),
                'ledger_path':str(pack['ledger']),'automatic_replay_permitted':False,'original_state_not_yet_replaced':True})
            def held(command,**kw):
                require(command[0]=='sbatch' and '--hold' not in command and time.time()+300<c['end'],'One new held sbatch within native margin required')
                return runner(['sbatch','--hold',*command[1:]],timeout=30,**kw)
            ledger=slurm.submit(pack['path'],pack['ledger'],runner=held)
            jobs=[ledger['tasks'][t['name']]['job_id'] for t in pack['value']['tasks']]
            require(len(jobs)==len(set(jobs))==4,'Exactly four distinct new controller jobs required')
            proofs={}
            for task,job in zip(pack['value']['tasks'],jobs):
                v=read_held(c,task,job);require(v.get('JobState')=='PENDING' and v.get('Reason')=='JobHeldUser','All new controllers must remain held before CAS')
                proofs[job]=v
            exclusive(AREA/'new-held-native-proof.json',proofs)
            c['admission']('activation-all-new-controllers-registered',cpus=0,gpus=0,hours=1)
            c['verify_sources']()
            verify_old_files(source);require_absent(paths)
            install_state(result,plan,paths,originals,c['job'])
            exclusive(AREA/'release-intent.json',{'job_ids':jobs,'held_proof_sha256':digest(AREA/'new-held-native-proof.json'),
                'state_installation_sha256':digest(AMENDMENT/'installation.json'),'automatic_replay_permitted':False})
        # New controller startup admission needs the same mutex. Drop it before
        # release so the first controller never waits behind this release loop.
        for task,job in zip(pack['value']['tasks'],jobs):
            require(time.time()+210<c['end'],'No finite margin for the next one-time release')
            v=read_held(c,task,job);require(v.get('JobState')=='PENDING' and v.get('Reason')=='JobHeldUser','New job changed before one-time release')
            exclusive(AREA/('release-'+job+'-intent.json'),{'job_id':job,'automatic_replay_permitted':False})
            response=runner(['scontrol','release',job],capture_output=True,text=True,timeout=30)
            exclusive(AREA/('release-'+job+'-result.json'),{'returncode':response.returncode,'stdout':response.stdout,'stderr':response.stderr})
            response.check_returncode()
        receipt={'status':'explicit0000_derivative_installed_and_four_new_controllers_released','native_job_id':c['job'],
            'jobs':jobs,'controller_descriptor':{'path':str(descriptor_path),'sha256':digest(descriptor_path)},
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


def main():
    p=argparse.ArgumentParser()
    for n in ('qualification','evidence-packet','postactivation-evidence','original-descriptor','native-plan','native-ledger','native-script'):p.add_argument('--'+n,type=Path,required=True)
    for n in ('qualification-sha256','qualification-job-id','evidence-packet-sha256','postactivation-evidence-sha256','original-descriptor-sha256','native-task','approval-reference'):p.add_argument('--'+n,required=True)
    p.add_argument('--qualification-helper',type=Path,required=True)
    a=p.parse_args();os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    require(digest(QHELPER)==QHELPER_SHA,'Reviewed qualification adapter changed')
    require(a.qualification_helper==QHELPER,'Exact reviewed qualification support helper required')
    api=runpy.run_path(str(QHELPER),run_name='private_registry_activation_support');api['verify_pins']()
    require(a.qualification==SUPPORT/'private-qualification.json' and re.fullmatch('[0-9]+',a.qualification_job_id),'Exact prospective native Q identity required')
    q=frozen(a.qualification,a.qualification_sha256)
    require(q.get('status')=='complete_f2_registry_overlay_qualification' and q.get('returncode')==0 and q.get('source_sha256')==SOURCE
        and q.get('config_sha256')==CONFIG and q.get('qualification_helper_sha256')==QHELPER_SHA
        and str(q.get('slurm_job_id'))==a.qualification_job_id,'Exact source-bound native private Q receipt required')
    controller=runpy.run_path(str(CONTROLLER),run_name='private_registry_activation_controller')
    controller['verify_operational_tests'](q)
    mq=q.get('migration_qualification',{})
    require(mq.get('helper')=={'path':str(SUPPORT/'recover-f2-training0000-state-20261021.py'),'sha256':api['PINS']['recover-f2-training0000-state-20261021.py']}
        and mq.get('test_source')=={'path':str(SUPPORT/'test_recover_f2_training0000_state_20261021.py'),'sha256':api['PINS']['test_recover_f2_training0000_state_20261021.py']}
        and mq.get('test_counts')=={'tests':42,'failures':0,'errors':0,'skipped':0},'Exact separate42 migration qualification required')
    api['junit_roster'](mq['junit']['path'],42,api['MIGRATION_ROSTER']);require(digest(Path(mq['junit']['path']))==mq['junit']['sha256'],'Migration JUnit bytes changed')
    migration=runpy.run_path(str(SUPPORT/'recover-f2-training0000-state-20261021.py'),run_name='private_registry_activation_migration')
    require(a.evidence_packet==Path(str(migration['NATIVE_EVIDENCE'])),'Exact immutable253 source packet required')
    source_packet=packet(a.evidence_packet,a.evidence_packet_sha256);source=json.loads(source_packet['raw'])
    require(a.postactivation_evidence==POST_ACTIVATION,'Exact original postactivation275 evidence path required')
    post_packet=packet(a.postactivation_evidence,a.postactivation_evidence_sha256);post_observed=json.loads(post_packet['raw'])
    verify_pristine_postactivation(source,post_observed)
    original=source['files']['controller_descriptor_v3']
    require(str(a.original_descriptor)==original['path'] and a.original_descriptor_sha256==original['sha256'],'Exact original controller descriptor binding required')
    old_descriptor=frozen(a.original_descriptor,a.original_descriptor_sha256)
    config=read_json(PRIMARY/'configs/study.json');job=str(native_job_id() or '')
    import horsehair
    require(job.isdigit() and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209' and os.environ.get('SLURM_CPUS_PER_TASK')=='4'
        and Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py' and source_identity(FRAMEWORK)['sha256']==SOURCE
        and source_identity(PRIMARY)['sha256']==PRIMARY_SHA and canonical_hash(config)==CONFIG,'Owned4CPU qualified/frozen native activation required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval required')
    require(not AREA.exists() and not AMENDMENT.exists() and not (ROOT/'plans'/PHASE).exists(),'One-time activation namespace already exists; no replay')
    AREA.mkdir(mode=0o700)
    scheduler=runpy.run_path(str(api['SCHEDULER']))['run_scontrol_job'];allocation=controller['fields'](scheduler(job,AREA/'own-scheduler-read'))
    require(allocation.get('JobId')==job and allocation.get('UserId')=='pwa209(3164768)' and allocation.get('Account')=='def-ptewarie_cpu'
        and allocation.get('JobState')=='RUNNING' and allocation.get('NumCPUs')=='4' and allocation.get('NumNodes') in {'1','1-1'}
        and allocation.get('Command')==str(a.native_script) and controller['seconds'](allocation.get('TimeLimit',''))==3600
        and allocation.get('MinMemoryNode') in {'16G','16384M'} and controller['gpu_count'](allocation.get('AllocTRES',''))==0,'Exact own activation allocation/script required')
    require(all('gpu' not in allocation.get(k,'').lower() for k in ('ReqTRES','TresPerNode','TresPerTask','TresPerJob','Gres')),'CPU-only activation required')
    end=float(os.environ.get('SLURM_JOB_END_TIME','nan'));native_end=datetime.fromisoformat(allocation['EndTime']).replace(tzinfo=timezone.utc).timestamp()
    require(math.isfinite(end) and abs(end-native_end)<=2 and time.time()+900<end,'Verified native activation margin required')
    runpy.run_path(str(api['NATIVE_GUARD']))['install'](AREA/'accounting-reads')
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'358 must precede campaign binding')
    registry_api=runpy.run_path(str(SUPPORT/'registered-snapshot-read-guard-v3-20261021.py'))
    registry_api['install'](AREA/'registered-snapshot-reads',
        registration_mode='controller-already-locked',allocation_deadline_unix=end)
    own_tasks=[t for t in read_json(a.native_plan)['tasks'] if t['name']==a.native_task]
    require(len(own_tasks)==1 and str(Path(__file__)) in shlex.split(own_tasks[0]['command']),'Native plan must invoke this activation helper')
    registration=api['registered_task'](a.native_plan,a.native_ledger,a.native_task,a.native_script,job,config)
    policy=load_resource_policy(api['POLICY'],api['POLICY_SHA'],root=ROOT,config=config)
    admit=lambda label,**kw:campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],AREA,label,resource_policy=policy,**kw)
    with registry_api['registration_lock'](AREA/'activation-start-registration',end,role='controller-start'):
        admit('activation-native-start',cpus=0,gpus=0,hours=1)
    require(slurm.completed_native_dependency(ROOT,a.qualification_job_id) is not None,'Positive completed private native141 Q producer required')
    c={'job':job,'config':config,'policy':policy,'controller':controller,'scheduler':scheduler,'native_reader':repair._repair_native_states,
        'admission':admit,'registry_api':registry_api,'end':end,'migration':migration,'pins':api['PINS'],'q':q,'qpath':a.qualification,'qsha':a.qualification_sha256}
    def unchanged_sources():
        api['verify_pins']()
        require(digest(a.postactivation_evidence)==a.postactivation_evidence_sha256,'Postactivation history bytes changed')
        require(digest(QHELPER)==QHELPER_SHA and digest(a.qualification)==a.qualification_sha256
            and source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,
            'Qualified source/Q changed before native activation')
    c['verify_sources']=unchanged_sources
    with resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        quota=runpy.run_path(str(api['QUOTA']))['require_storage_headroom_with_wait'](ROOT,required_files=3000,required_bytes=10_000_000_000,
            reserve_files=247000,reserve_bytes=2_000_000_000_000,evidence_dir=AREA/'quota-reads',allocation_deadline_unix=end)
        from horsehair.admission import _storage
        census=ROOT/'state/storage-budget.json';h=digest(census)
        api['require_fresh_census'](read_json(census))
        future_bytes,future_files,paths=_storage(ROOT,config,10_000_000_000,3000,personal_headroom=quota)
        require(digest(census)==h,'Census changed during activation storage validation')
        exclusive(AREA/'intent.json',{'native_registration':registration,'qualification_sha256':a.qualification_sha256,
            'evidence_packet_sha256':a.evidence_packet_sha256,'postactivation_evidence_sha256':a.postactivation_evidence_sha256,'native_allocation':allocation,'census_sha256':h,
            'future_study_bytes':future_bytes,'future_study_files':future_files,'automatic_replay_permitted':False,
            'existing_jobs_held_or_cancelled':False,'scientific_result':False,'admission_authority':False})
        approval={'source':'current_user','authorizes_targeted_operational_repair':True,'request_reference':a.approval_reference}
        receipt=orchestrate(c,source,source_packet,old_descriptor,approval)
        print('AF_F2_REGISTRY_ACTIVATED '+json.dumps(receipt,sort_keys=True),flush=True)


if __name__=='__main__':main()
