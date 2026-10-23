"""Finite native coordinator for seven separately reviewed registration scripts.

This registers queued work only. It never retries an ambiguous registration,
claims qualification coverage for itself, or runs the registered research jobs.
"""
from pathlib import Path
from datetime import datetime,timezone
import argparse
import ast
import base64
import hashlib
import json
import math
import os
import re
import runpy
import signal
import shlex
import subprocess
import sys
import time
import zlib

from horsehair.core import canonical_hash,digest,read_json,source_identity,native_job_id
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
AREA=ROOT/'maintenance/study-af-reviewed-registration-coordinator-v2-20261023'
CASE=AREA/'coordinator-execution'
HELPER='coordinate-reviewed-registrations-v2-20261023.py'
IDS=('429','430','431','432','433','434','451')
SHELLS={ident:'hhw-recovery-stage-20261021-'+ident+'.sh' for ident in IDS}
POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=GUARD.with_name('native-scheduler-read.py')
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
ADMIN=ROOT/'maintenance/study-af-registry-repair-support-v6-20261022/administration-registry-read-guard-v4-20261022.py'
ADMIN_SHA='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
QPATH=ROOT/'maintenance/study-af-rolling-census-qualification-v3-20261023/qualification.json'
QSUPPORT=ROOT/'maintenance/study-af-rolling-census-support-v2-20261023'
FLAGS={'scientific_result':False,'admission_authority':False,'coordinator_native_qualification_claimed':False,'automatic_retry_permitted':False}

def require(value,message):
    if not value:raise ValueError(message)

def ordinary(path,namespace):
    path=Path(path)
    require(path.is_absolute() and path==path.resolve() and path!=namespace and path.is_relative_to(namespace)
        and all(not p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(namespace)) and path.is_file(),
        'Ordinary confined immutable source required')
    return path

def strict(raw):
    def pairs(items):
        value={}
        for key,item in items:require(key not in value,'Duplicate JSON key refused');value[key]=item
        return value
    def constant(value):raise ValueError('Nonfinite JSON refused')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=constant)

def exclusive(path,value):
    raw=(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    require(len(raw)<=4_000_000,'Finite coordinator evidence required')
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())

def source_manifest(path,expected):
    require(Path(path)==AREA/'coordinator-source-manifest.json' and re.fullmatch('[0-9a-f]{64}',expected or ''),'Exact coordinator manifest locator/SHA required')
    raw=ordinary(path,ROOT/'maintenance').read_bytes();require(len(raw)<=100_000 and hashlib.sha256(raw).hexdigest()==expected,'Coordinator manifest changed')
    value=strict(raw)
    require(set(value)=={'kind','files','qualification'} and value['kind']=='finite-reviewed-registration-coordinator-source-manifest-v1'
        and isinstance(value['files'],dict) and set(value['files'])=={HELPER,*SHELLS.values()},'Exact own source plus seven reviewed shell sources required')
    pins={}
    for name,sha in value['files'].items():
        p=ordinary(AREA/name,ROOT/'maintenance');require(re.fullmatch('[0-9a-f]{64}',str(sha)) and digest(p)==sha,'Reviewed source changed: '+name);pins[str(p)]=sha
    require(Path(__file__)==AREA/HELPER and digest(Path(__file__))==value['files'][HELPER],'Actual coordinator source must be frozen')
    q=value['qualification'];require(set(q)=={'path','plan','ledger','task','validator','source_manifest'},'Exact positive qualification contract required')
    require(q['path']==str(QPATH) and q['plan']==str(ROOT/'plans/recovery-support-20261021-444/plan.json')
        and q['ledger']==str(ROOT/'state/recovery-support-20261021-444-submission.json') and q['task']=='recovery-444','Exact new Q444 lineage required')
    validator=q['validator'];manifest=q['source_manifest']
    require(set(validator)=={'path','sha256','function'} and validator['function']=='kernel_qualification'
        and Path(validator['path'])==QSUPPORT/'run-fresh-storage-census-v6-20261023.py'
        and set(manifest)=={'path','sha256'} and Path(manifest['path'])==QSUPPORT/'source-manifest.json','Exact separately qualified v6 verifier contract required')
    for item in (validator,manifest):
        p=ordinary(item['path'],ROOT/'maintenance');require(re.fullmatch('[0-9a-f]{64}',str(item['sha256'])) and digest(p)==item['sha256'],'Qualified verifier/source manifest changed');pins[str(p)]=item['sha256']
    require(digest(path)==expected,'Coordinator source manifest changed during verification')
    return value,pins

def verify_sources(pins):
    for path,sha in pins.items():require(digest(Path(path))==sha,'Bound source changed: '+path)
    require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,'Frozen framework or primary changed')

def bound_pair(plan_path,ledger_path):
    plan_path=ordinary(plan_path,ROOT/'plans');ledger_path=ordinary(ledger_path,ROOT/'state')
    plan=read_json(plan_path);ledger=read_json(ledger_path)
    require(plan.get('root')==str(ROOT) and plan.get('config_sha256')==ledger.get('config_sha256')==CONFIG
        and canonical_hash(plan)==ledger.get('plan_sha256') and plan.get('release_identity',{}).get('sha256')==ledger.get('source_sha256'),
        'Actual immutable source/config plan and ledger pair required')
    return plan,ledger

def scalar_job(value):
    require(type(value) is str and re.fullmatch('[1-9][0-9]*',value),'Actual scalar native job required');return value

def qualification_job(contract):
    plan,ledger=bound_pair(contract['plan'],contract['ledger']);selected=[t for t in plan['tasks'] if t['name']==contract['task']]
    require(len(selected)==1 and len(plan['tasks'])==1 and plan['release_identity']['sha256']==SOURCE,'Exact Q444 source/task required')
    task=selected[0];record=ledger['tasks'][task['name']];script=ordinary(task['script'],ROOT/'plans')
    require(task['resources']=={'cpus':8,'memory':'64G','hours':1,'gpu':False,'array':None}
        and record['state']=='submitted' and task['command'] in script.read_text()
        and canonical_hash(script.read_text())==task['script_sha256']==record['script_sha256'],'Exact Q444 script/resources required')
    return scalar_job(record['job_id'])

def own_registration(args,job,argv,manifest_sha,qjob):
    require(args.native_plan==ROOT/'plans/recovery-support-20261021-425/plan.json'
        and args.native_ledger==ROOT/'state/recovery-support-20261021-425-submission.json'
        and args.native_task=='recovery-425' and args.native_script==args.native_plan.parent/'recovery-425.sbatch','Exact coordinator425 registration required')
    plan,ledger=bound_pair(args.native_plan,args.native_ledger);tasks=plan.get('tasks',[]);require(len(tasks)==1,'One actual coordinator task required')
    task=tasks[0];record=ledger['tasks'].get(args.native_task,{})
    script=ordinary(args.native_script,ROOT/'plans');command=shlex.split(task.get('command',''));own=str(AREA/HELPER)
    require(task.get('name')==args.native_task and plan['release_identity']['sha256']==SOURCE and task.get('script')==str(script)
        and task.get('resources')=={'cpus':1,'memory':'16G','hours':4,'gpu':False,'array':None}
        and task.get('depends')==[] and task.get('external_dependencies')==[qjob] and task.get('dependency_kind')=='afterok'
        and task.get('reviewed_registration_coordinator')=={'helper':{'path':own,'sha256':digest(Path(__file__))},'source_manifest':{'path':str(args.source_manifest),'sha256':manifest_sha}}
        and command[:3]==['python','-u',own] and command[3:]==list(argv) and task['command'] in script.read_text()
        and canonical_hash(script.read_text())==task.get('script_sha256')==record.get('script_sha256')
        and record.get('job_id')==job and record.get('state')=='submitted','Actual own source/args/afterok/native registration binding required')
    return {'plan':{'path':str(args.native_plan),'sha256':digest(args.native_plan)},'ledger':{'path':str(args.native_ledger),'sha256':digest(args.native_ledger)},'script':{'path':str(script),'sha256':digest(script)},'native_job_id':job}

def memory(value):
    match=re.fullmatch('([1-9][0-9]*)([KMG])',value);require(match is not None,'Positive explicit native memory required')
    return int(match[1])*1024**('KMG'.index(match[2])+1)

def native_allocation(job,script,reader,*,wall=time.time):
    response=reader(job,CASE/'own-scheduler-reads');response.check_returncode();lines=[x for x in response.stdout.splitlines() if x.strip()]
    require(len(lines)==1,'One actual coordinator allocation required')
    pairs=re.findall(r'(?<!\S)([A-Za-z][A-Za-z0-9_/:]*)=(.*?)(?=\s+[A-Za-z][A-Za-z0-9_/:]*=|$)',lines[0]);fields={k:v.strip() for k,v in pairs}
    require(len(pairs)==len(fields) and fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
        and fields.get('Account')=='def-ptewarie_cpu' and fields.get('JobState')=='RUNNING' and fields.get('NumCPUs')=='1'
        and fields.get('NumNodes') in {'1','1-1'} and fields.get('TimeLimit') in {'04:00:00','4:00:00','0-04:00:00'}
        and memory(fields.get('MinMemoryNode',''))==16*1024**3 and fields.get('Command')==str(script)
        and fields.get('JobName')=='hhw-recovery-425' and 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields,'Exact owned scalar1CPU16G4h allocation required')
    for key in ('ReqTRES','AllocTRES'):
        items=[x.partition('=') for x in fields.get(key,'').split(',')];tres={k:v for k,s,v in items}
        require(len(tres)==len(items) and all(s and k and v for k,s,v in items) and tres.get('cpu')=='1' and tres.get('node')=='1'
            and memory(tres.get('mem',''))==16*1024**3 and not any('gpu' in k.lower() for k in tres),'Positive CPU-only native allocation TRES required')
    require(all('gpu' not in fields.get(k,'').lower() for k in ('TresPerNode','TresPerJob','TresPerTask','Gres')),'No GPU coordinator allocation permitted')
    end=datetime.fromisoformat(fields.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp();env=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(env) and abs(env-end)<=2 and wall()+600<end,'Actual finite native allocation end/margin required')
    return {'fields':fields,'allocation_end_unix':end},end

def decode_child(path,expected_sha,ident):
    raw=Path(path).read_bytes();require(len(raw)<=2_000_000 and hashlib.sha256(raw).hexdigest()==expected_sha and b'\r' not in raw,'Exact bounded reviewed LF shell bytes required')
    source=raw.decode();lines=[x for x in source.splitlines() if x.startswith('payload=')];require(len(lines)==1,'One exact static child payload required')
    strings=[n.value for n in ast.walk(ast.parse(lines[0])) if isinstance(n,ast.Constant) and isinstance(n.value,str) and len(n.value)>100]
    require(len(strings)==1,'One compressed child payload required');payload=strict(zlib.decompress(base64.b64decode(strings[0])))
    require(payload.get('id')==ident and re.fullmatch('[0-9a-f]{64}',str(payload.get('helper_sha256',''))),'Exact reviewed child identity/helper required')
    body=source.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0];compile(body,'<reviewed-registration-'+ident+'>','exec')
    return payload

def child_timeout(end,*,wall=time.time):
    remaining=end-wall()-180;require(math.isfinite(remaining) and remaining>=600,'Insufficient finite child registration/finalization margin')
    return min(1800,int(remaining))

def submission_line(path):
    with Path(path).open('rb') as stream:raw=stream.read(16_000_001)
    require(len(raw)<=16_000_000,'Bounded registration stdout required')
    lines=[x[len('RECOVERY_STAGE_SUBMITTED '):] for x in raw.decode().splitlines() if x.startswith('RECOVERY_STAGE_SUBMITTED ')]
    require(len(lines)==1,'Ambiguous child submission output; no automatic retry')
    return strict(lines[0])

def run_shell_once(command,*,cwd,env,stdout,stderr,timeout,check):
    """Bound the child session; its queued Slurm allocations are independent.

    Ten seconds of the finite allowance are reserved for TERM/KILL supervision.
    Uninterruptible native I/O can still require the allocation's external end.
    """
    require(os.name=='posix' and check is False and timeout>=600,'Owned finite POSIX shell supervision required')
    process=subprocess.Popen(command,cwd=cwd,env=env,stdout=stdout,stderr=stderr,start_new_session=True)
    try:
        returncode=process.wait(timeout=timeout-10)
    except subprocess.TimeoutExpired:
        try:os.killpg(process.pid,signal.SIGTERM)
        except ProcessLookupError:pass
        try:process.wait(timeout=5)
        except subprocess.TimeoutExpired:pass
        # A reaped bash leader does not establish that its descendants exited.
        # Always address the original owned process group before propagating.
        try:os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:pass
        try:process.wait(timeout=5)
        except subprocess.TimeoutExpired:pass
        raise
    return subprocess.CompletedProcess(command,returncode)

def post_registration(ident,payload,reported,qjob,qsha,validator,config):
    require(reported.get('request_id')==ident and reported.get('area')==payload['area'] and reported.get('helper_sha256')==payload['helper_sha256'],'Child output does not bind reviewed registration')
    directory=ROOT/('plans/recovery-support-20261021-'+ident);ledger_path=ROOT/('state/recovery-support-20261021-'+ident+'-submission.json')
    plan,ledger=bound_pair(directory/'plan.json',ledger_path)
    framework=Path(payload.get('framework',str(FRAMEWORK)))
    require(plan['release_identity']['sha256']==source_identity(framework)['sha256'],'Child registration must bind its reviewed framework source')
    names=['rolling-storage-census-'+str(i).zfill(2) for i in range(1,65)] if ident=='429' else ['recovery-'+ident]
    require([t['name'] for t in plan['tasks']]==names and set(ledger['tasks'])==set(names)==set(reported.get('tasks',{})),'Exact complete child task roster required')
    jobs=[]
    for task in plan['tasks']:
        name=task['name'];record=ledger['tasks'][name];job=scalar_job(record['job_id']);script=ordinary(task['script'],ROOT/'plans')
        require(script==directory/(name+'.sbatch') and record['state']=='submitted' and reported['tasks'][name]=={'job_id':job,'state':'submitted'}
            and canonical_hash(script.read_text())==task['script_sha256']==record['script_sha256'] and task['command'] in script.read_text(),'Actual submitted child script/ledger mismatch')
        command=shlex.split(task['command']);require(len(command)>=3 and command[:2]==['python','-u'],'Reviewed direct helper invocation required')
        helper=ordinary(command[2],ROOT/'maintenance');require(digest(helper)==payload['helper_sha256'],'Actual child helper differs from reviewed bytes')
        if ident=='429':require(str(helper)==validator['__file__'],'Rolling task must invoke the qualified v6 provider')
        else:
            expected_helper=str(Path(payload['area'])/payload['helper']) if payload.get('derived_helper_body') else payload.get('canonical_helper',str(Path(payload['area'])/payload['helper']))
            require(str(helper)==expected_helper and task['resources']=={'cpus':payload['cpus'],'memory':payload['memory'],'hours':payload['hours'],'gpu':False,**({'array':None} if ident in {'433','451'} else {})},'Reviewed child helper/resources changed')
            args=list(payload['args'])
            if ident=='432':
                _,qledger=bound_pair(ROOT/'plans/recovery-support-20261021-431/plan.json',ROOT/'state/recovery-support-20261021-431-submission.json')
                args=[qledger['tasks']['recovery-431']['job_id'] if x=='__Q_JOB__' else x for x in args]
            require(command[3:]==args and task.get('depends')==[] and task.get('dependency_kind')=='afterok','Actual child args/internal dependency changed')
            _,cledger=bound_pair(ROOT/'plans/recovery-support-20261021-429/plan.json',ROOT/'state/recovery-support-20261021-429-submission.json')
            dependencies=[cledger['tasks']['rolling-storage-census-01']['job_id']]
            if ident in {'432','434'}:
                _,qledger=bound_pair(ROOT/'plans/recovery-support-20261021-431/plan.json',ROOT/'state/recovery-support-20261021-431-submission.json');dependencies.append(qledger['tasks']['recovery-431']['job_id'])
            if ident=='451':
                _,aledger=bound_pair(ROOT/'plans/recovery-support-20261021-432/plan.json',ROOT/'state/recovery-support-20261021-432-submission.json');dependencies=[aledger['tasks']['recovery-432']['job_id']]
            require(task.get('external_dependencies')==dependencies,'Actual child native afterok dependencies changed')
        jobs.append(job)
    require(len(set(jobs))==len(jobs),'Every registered allocation must have a distinct actual job')
    provider=None
    if ident=='429':
        require(validator.get('MAX_SLOTS')==64,'Frozen v6 maximum must match the reviewed64-slot chain')
        first=plan['tasks'][0];tokens=shlex.split(first['command'])[3:];require(len(tokens)%2==0 and len(set(tokens[::2]))==len(tokens[::2]),'Unique provider flags required')
        flags=dict(zip(tokens[::2],tokens[1::2]));require(flags.get('--kernel-qualification-job-id')==qjob and flags.get('--kernel-qualification-sha256')==qsha,'Provider must bind actual completed Q2')
        args=argparse.Namespace(**{k[2:].replace('-','_'):v for k,v in flags.items()})
        for name in ('native_plan','native_ledger','native_script','source_manifest','kernel_qualification','registry_qualification'):setattr(args,name,Path(getattr(args,name)))
        provider=validator['provider_registration'](args,jobs[0],config)
        require(provider['slot_count']==64,'All64 provider registrations must be present')
    return {'plan':{'path':str(directory/'plan.json'),'sha256':digest(directory/'plan.json')},'ledger':{'path':str(ledger_path),'sha256':digest(ledger_path)},'native_jobs':jobs,'provider_registration':provider}

def run_child(ident,path,expected_sha,payload,end,verify,*,runner=run_shell_once):
    prefix=CASE/('child-'+ident);timeout=child_timeout(end);require(digest(path)==expected_sha,'Reviewed child source changed before execution')
    exclusive(str(prefix)+'-intent.json',{'request_id':ident,'command':['bash',str(path)],'shell_sha256':expected_sha,'timeout_seconds':timeout,'attempt':1,**FLAGS})
    try:
        with Path(str(prefix)+'-stdout.txt').open('xb') as out,Path(str(prefix)+'-stderr.txt').open('xb') as err:
            result=runner(['bash',str(path)],cwd=AREA,env=os.environ.copy(),stdout=out,stderr=err,timeout=timeout,check=False)
        require(result.returncode==0,'Child failed or registration is ambiguous; no automatic retry')
        require(digest(path)==expected_sha,'Reviewed child source changed during execution')
        reported=submission_line(str(prefix)+'-stdout.txt');proof=verify(ident,payload,reported)
        receipt={'status':'registered_once_and_actual_plan_verified','request_id':ident,'returncode':0,'stdout':{'path':str(prefix)+'-stdout.txt','sha256':digest(str(prefix)+'-stdout.txt')},'stderr':{'path':str(prefix)+'-stderr.txt','sha256':digest(str(prefix)+'-stderr.txt')},'registration':proof,**FLAGS}
        exclusive(str(prefix)+'-completion.json',receipt);return receipt
    except BaseException as error:
        exclusive(str(prefix)+'-failure.json',{'status':'stopped_no_automatic_retry','request_id':ident,'error_type':type(error).__name__,'error':str(error),'registration_may_have_occurred':True,**FLAGS});raise

def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv);p=argparse.ArgumentParser(description=__doc__)
    for name in ('native-plan','native-ledger','native-script','source-manifest'):p.add_argument('--'+name,type=Path,required=True)
    for name in ('native-task','source-manifest-sha256'):p.add_argument('--'+name,required=True)
    args=p.parse_args(argv);os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    manifest,pins=source_manifest(args.source_manifest,args.source_manifest_sha256);verify_sources(pins)
    job=scalar_job(str(native_job_id() or ''));config=read_json(PRIMARY/'configs/study.json')
    import horsehair
    require(os.geteuid()==3164768 and os.environ.get('USER')=='pwa209' and os.environ.get('SLURM_CPUS_PER_TASK')=='1'
        and os.environ.get('HHW_GPU_COUNT')=='0' and os.environ.get('HHW_MAX_HOURS')=='4'
        and Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and canonical_hash(config)==CONFIG and read_json(FRAMEWORK/'configs/study.json')==config,'Owned exact qualified1CPU4h native context required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Native Narval coordinator required')
    for path,sha in ((GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(ADMIN,ADMIN_SHA),(POLICY,POLICY_SHA)):
        require(digest(ordinary(path,ROOT/'maintenance'))==sha,'Pinned operational read/policy lineage changed')
    require(not CASE.exists() and not CASE.is_symlink(),'Coordinator is one-shot; previous evidence forbids replay');CASE.mkdir(mode=0o700)
    qjob=qualification_job(manifest['qualification']);registration=own_registration(args,job,argv,args.source_manifest_sha256,qjob)
    allocation,end=native_allocation(job,args.native_script,runpy.run_path(str(SCHEDULER))['run_scontrol_job'])
    runpy.run_path(str(GUARD))['install'](CASE/'accounting-reads')
    admin=runpy.run_path(str(ADMIN));admin['install'](CASE/'registered-snapshot-reads',registration_mode='controller-already-locked',allocation_deadline_unix=end)
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'358 native reader must precede campaign binding')
    with admin['registration_lock'](CASE/'admission-lock',end,role='controller-start'):
        absent=[ROOT/('plans/recovery-support-20261021-'+ident) for ident in IDS]+[ROOT/('state/recovery-support-20261021-'+ident+'-submission.json') for ident in IDS]
        require(all(not path.exists() and not path.is_symlink() for path in absent),'Every child plan/ledger must be absent at coordinator start; never replay')
        admission=campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],CASE,'coordinator-admission',resource_policy=policy,cpus=0,gpus=0,hours=4)
        exclusive(CASE/'initial-registration-absence.json',{'paths':list(map(str,absent)),'registration_mutex':str(ROOT/'state/af-campaign-registration.mutex'),**FLAGS})
    observed=repair._repair_native_states([qjob],{},owner='pwa209',account='def-ptewarie_cpu')
    contract=manifest['qualification'];qsha=digest(ordinary(contract['path'],ROOT/'maintenance'))
    validator=runpy.run_path(contract['validator']['path'],run_name='coordinator_source_bound_v6_verifier')
    proof=validator[contract['validator']['function']](Path(contract['path']),qsha,qjob,Path(contract['source_manifest']['path']),contract['source_manifest']['sha256'],observed)
    require(proof['job_id']==qjob and proof['sha256']==qsha,'Actual positive Q2 completion proof required')
    exclusive(CASE/'native-bindings.json',{'own_registration':registration,'allocation':allocation,'qualification':proof,'source_manifest':{'path':str(args.source_manifest),'sha256':args.source_manifest_sha256},'resource_policy':policy.receipt(),'admission':admission,**FLAGS})
    receipts=[]
    with resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=4,job_id=job,resource_policy=policy):
        for ident in IDS:
            verify_sources(pins);require(source_manifest(args.source_manifest,args.source_manifest_sha256)==(manifest,pins) and digest(QPATH)==qsha,'Source closure or completed Q2 receipt changed before child')
            path=AREA/SHELLS[ident];payload=decode_child(path,manifest['files'][SHELLS[ident]],ident)
            receipts.append(run_child(ident,path,manifest['files'][SHELLS[ident]],payload,end,lambda i,p,r:post_registration(i,p,r,qjob,qsha,validator,config)))
        verify_sources(pins);require(digest(QPATH)==qsha and time.time()+180<end,'Q2 receipt changed or finite coordinator completion margin exhausted')
        exclusive(CASE/'receipt.json',{'status':'complete_seven_reviewed_registrations','native_job_id':job,'children':receipts,'qualification':proof,'resource_policy':policy.receipt(),**FLAGS})
    print('REVIEWED_REGISTRATIONS_COMPLETE '+json.dumps({'native_job_id':job,'child_count':len(receipts),'receipt_sha256':digest(CASE/'receipt.json')},sort_keys=True),flush=True)

if __name__=='__main__':main()
