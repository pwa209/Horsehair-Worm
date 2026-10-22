"""Native exact-size census with truthful start/quota/end evidence; no science."""
from pathlib import Path
from datetime import datetime,timezone
import argparse
import hashlib
import json
import math
import os
import re
import runpy
import signal
import subprocess
import time

from horsehair.core import read_json,source_identity,canonical_hash,digest,native_job_id,exclusive_file_lock
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy
from horsehair.slurm import completed_native_dependency

ROOT=Path('/scratch/pwa209/horsehair-worm')

PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')

PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'

FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'

SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'

CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'

QUALIFICATION=ROOT/'maintenance/study-af-execution-support-v1-20261021/qualification.json'

POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'

POLICY_SHA='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'

GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'

GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'

SCHEDULER=GUARD.with_name('native-scheduler-read.py')

SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'

QUOTA=ROOT/'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'

QUOTA_SHA='5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045'

REGISTRY_SUPPORT=ROOT/'maintenance/study-af-registry-repair-support-v4-20261021'

REGISTRY_GUARD=REGISTRY_SUPPORT/'registered-snapshot-read-guard-v3-20261021.py'

REGISTRY_GUARD_SHA='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'

REGISTRY_Q=REGISTRY_SUPPORT/'private-qualification.json'
REGISTRY_Q_SHA='1f391caccd4629eb47150862f7bc2575e991e4ce5cb5aae99e955cdeae4510ca'
REGISTRY_Q_JOB='6699561'

REGISTRY_Q_HELPER=REGISTRY_SUPPORT/'qualify-f2-registry-repair-v4-20261021.py'

REGISTRY_Q_HELPER_SHA='7fd2348c3370811cb5ca2933357a007447b18c2ea57be1cef502fdd035fdda51'

REGISTRY_CONTROLLER=REGISTRY_SUPPORT/'run-af-agent-controller-registry-v4-20261021.py'

REGISTRY_CONTROLLER_SHA='475a170aab2d158cfd17adb250707e1d0767086d4c50b95516c360f2222560f8'

QUALIFICATION_SHA="29ed530f3783f3c35c3cf20343bdb663732ec2bf4bf5c3218f8b029510becc2f"

SUPPORT=ROOT/'maintenance/study-af-fresh-storage-census-v3-20261022'

FALSE_FLAGS=('scientific_result','admission_authority','scientific_gates_cleared','freshness_timestamp_rewritten','end_time_extends_admission_freshness')

def require(value,message):
    if not value:raise ValueError(message)

def exclusive(path,value):
    raw=(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())

def memory(value):
    match=re.fullmatch(r'([1-9][0-9]*)([MG])',value)
    require(match is not None,'Exact positive native memory units required')
    return int(match[1])*1024**(2 if match[2]=='M' else 3)

def qualification_proof(path,sha,job,observed):
    path=Path(path)
    require(path==QUALIFICATION and not path.is_symlink() and digest(path)==sha
        and re.fullmatch('[1-9][0-9]*',job) is not None,'Exact qualified4f qualification path/SHA/scalar job required')
    value=read_json(path);record=observed.get(job,{})
    require(value.get('status')=='complete' and type(value.get('returncode')) is int and value['returncode']==0
        and value.get('source_sha256')==SOURCE and value.get('config_sha256')==CONFIG
        and value.get('source_verified_unchanged') is True and value.get('slurm_job_id')==job
        and value.get('test_counts')=={'tests':1694,'failures':0,'errors':0,'skipped':0},
        'Positive actual full1694 qualified4f qualification required')
    require(record.get('state')=='COMPLETED' and record.get('exit_code')=='0:0'
        and record.get('user')=='pwa209' and record.get('account')=='def-ptewarie_cpu'
        and record.get('job_id_raw')==job and record.get('unallocated_metadata') is False
        and record.get('cpus')==4 and type(record.get('elapsed_seconds')) is int
        and 0<record['elapsed_seconds']<=3600 and 'gpu' not in record.get('alloc_tres','').lower(),
        'Positive owned scalar four-CPU qualification native completion required')
    proof=completed_native_dependency(ROOT,job)
    require(proof is not None,'Qualification final native accounting proof is pending')
    artifact=value.get('artifacts',{}).get('junit',{});junit=Path(artifact.get('path',''))
    require(junit.is_absolute() and junit.resolve().is_relative_to(ROOT/'state/qualification-evidence')
        and not junit.is_symlink() and digest(junit)==artifact.get('sha256')
        and junit.stat().st_size==artifact.get('bytes'),'Exact full qualification JUnit bytes required')
    from horsehair.cli import _qualification_junit
    require(_qualification_junit(junit)=={'tests':1694,'failures':0,'errors':0,'skipped':0},'Actual1694 native JUnit roster required')
    return {'path':str(path),'sha256':sha,'job_id':job,'native_observed':record,'completion':proof,
        'junit':artifact,'resource_policy':value.get('resource_policy')}

def registry_qualification(path,sha,job):
    """Bind actual full100 source/JUnit evidence before installing its read guard."""
    path=Path(path)
    require(path==REGISTRY_Q and sha==REGISTRY_Q_SHA and job==REGISTRY_Q_JOB
        and path==path.resolve() and not path.is_symlink()
        and re.fullmatch('[0-9a-f]{64}',sha) is not None and digest(path)==sha
        and re.fullmatch('[1-9][0-9]*',job) is not None,'Exact explicit private registry qualification path/SHA/job required')
    for source,expected in [(REGISTRY_Q_HELPER,REGISTRY_Q_HELPER_SHA),(REGISTRY_GUARD,REGISTRY_GUARD_SHA),
        (REGISTRY_CONTROLLER,REGISTRY_CONTROLLER_SHA)]:
        require(source.is_file() and source==source.resolve() and not source.is_symlink()
            and source.is_relative_to(ROOT/'maintenance') and digest(source)==expected,'Pinned qualified registry helper changed')
    q=read_json(path)
    require(q.get('status')=='complete_f2_registry_overlay_qualification' and type(q.get('returncode')) is int and q['returncode']==0
        and q.get('source_sha256')==SOURCE and q.get('config_sha256')==CONFIG and q.get('slurm_job_id')==job
        and q.get('qualification_helper_sha256')==REGISTRY_Q_HELPER_SHA
        and q.get('registry_guard_sha256')==REGISTRY_GUARD_SHA and q.get('controller_sha256')==REGISTRY_CONTROLLER_SHA
        and q.get('worker_shim_sha256')=='dd39905ce44f89f7f8bbdd325d1a0f1d5ec7950dd89d0e3bb267f6fdfe77a09a'
        and q.get('test_counts')=={'tests':100,'failures':0,'errors':0,'skipped':0},'Positive exact source-bound private100 registry qualification required')
    runpy.run_path(str(REGISTRY_Q_HELPER),run_name='vtl_registry_v4_q_source_validation')['verify_pins']()
    runpy.run_path(str(REGISTRY_CONTROLLER),run_name='vtl_registry_v4_q_roster_validation')['verify_operational_tests'](q)
    require(digest(path)==sha,'Private registry qualification changed during verification')
    return {'path':str(path),'sha256':sha,'job_id':job,'value':q,
        'guard':{'path':str(REGISTRY_GUARD),'sha256':REGISTRY_GUARD_SHA}}

def completed_registry_qualification(binding,observed):
    job=binding['job_id'];record=observed.get(job,{})
    require(record.get('state')=='COMPLETED' and record.get('exit_code')=='0:0' and record.get('user')=='pwa209'
        and record.get('account')=='def-ptewarie_cpu' and record.get('job_id_raw')==job
        and record.get('unallocated_metadata') is False and type(record.get('cpus')) is int and record['cpus']==4
        and type(record.get('elapsed_seconds')) is int and 0<record['elapsed_seconds']<=3600
        and 'gpu' not in record.get('alloc_tres','').lower(),'Positive owned scalar4CPU private registry native completion required')
    proof=completed_native_dependency(ROOT,job)
    require(proof is not None,'Private registry native qualification is not positively complete')
    registry_qualification(binding['path'],binding['sha256'],job)
    return {k:v for k,v in binding.items() if k!='value'}|{'native_observed':record,'completion':proof}


def ordinary(path, root, *, fresh=False):
    p=Path(path)
    require(p.is_absolute() and p==p.resolve() and p!=root and p.is_relative_to(root)
        and all(not a.is_symlink() for a in [p,*p.parents] if a.is_relative_to(root)),
        'Exact ordinary personal evidence path required')
    require(not p.exists() if fresh else p.is_file(),'Fresh evidence required' if fresh else 'Ordinary source required')
    return p


def native_allocation(job,script,reader):
    script=ordinary(script,ROOT/'plans')
    response=reader(job,SUPPORT/'own-scheduler-reads');response.check_returncode()
    lines=[s for s in response.stdout.splitlines() if s.strip()]
    require(len(lines)==1,'One exact actual native allocation required')
    pairs=[(key,value.strip()) for key,value in re.findall(r'(?<!\S)(\w+)=(.*?)(?=\s+\w+=|$)',lines[0])];f=dict(pairs)
    require(len(f)==len(pairs) and f.get('JobId')==job and f.get('UserId')=='pwa209(3164768)'
        and f.get('JobState')=='RUNNING' and f.get('Account')=='def-ptewarie_cpu'
        and f.get('NumCPUs')=='4' and f.get('NumNodes') in {'1','1-1'}
        and f.get('TimeLimit') in {'04:00:00','4:00:00','0-04:00:00'}
        and memory(f.get('MinMemoryNode',''))==16*1024**3 and f.get('Command')==str(script),
        'Owned scalar4CPU16G4h allocation/script required')
    require(all('gpu' not in f.get(k,'').lower() for k in
        ('TresPerNode','TresPerJob','TresPerTask','ReqTRES','AllocTRES','Gres')),'Census must allocate no GPU')
    end=datetime.fromisoformat(f.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp()
    env=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(env) and abs(env-end)<=2 and time.time()+900<end,'Verified native finite end/margin required')
    plan_path=ordinary(script.parent/'plan.json',ROOT/'plans')
    ledger_path=ordinary(ROOT/'state'/(script.parent.name+'-submission.json'),ROOT/'state')
    plan=read_json(plan_path);ledger=read_json(ledger_path)
    tasks=[t for t in plan['tasks'] if t['script']==str(script)]
    require(len(tasks)==1 and plan.get('config_sha256')==CONFIG and plan.get('release_identity',{}).get('sha256')==SOURCE
        and ledger.get('source_sha256')==SOURCE and ledger.get('plan_sha256')==canonical_hash(plan),
        'Exact registered source/config plan and ledger required')
    task=tasks[0];r=ledger['tasks'][task['name']]
    require(r.get('job_id')==job and r.get('state')=='submitted'
        and r.get('script_sha256')==task.get('script_sha256')==canonical_hash(script.read_text())
        and f.get('JobName')=='hhw-'+task['name']
        and task.get('resources')=={'cpus':4,'memory':'16G','hours':4,'gpu':False,'array':None},
        'Actual registered script/resources differ')
    return {'fields':f,'allocation_end_unix':end,'native_stdout':response.stdout,
        'plan':{'path':str(plan_path),'sha256':digest(plan_path)},
        'ledger':{'path':str(ledger_path),'sha256':digest(ledger_path)},
        'script':{'path':str(script),'sha256':digest(script)}}


def brief_admission(callback,end,*,registration_lock,evidence,wall=time.time):
    """Use only the already-qualified2583 bounded acquisition; callback once."""
    require(type(end) in (int,float) and math.isfinite(end) and wall()+300<end,
        'Finite native admission margin required')
    with registration_lock(evidence,end,role='controller-start'):
        require(wall()+300<end,'Native margin exhausted acquiring registration guard')
        return callback()


def snapshot_clocks(value,begin,end):
    measured=value.get('measured_unix')
    captured=datetime.fromisoformat(value.get('captured_utc','').replace('Z','+00:00'))
    require(type(measured) in (int,float) and math.isfinite(measured) and captured.tzinfo is not None
        and begin<=measured<=end and begin<=captured.timestamp()<=end
        and abs(measured-captured.timestamp())<=2,'Conservative quota-capture clocks must lie within observed invocation')
    return {'unchanged_snapshot_measured_unix':measured,'unchanged_snapshot_captured_utc':value['captured_utc'],
        'original_freshness_clock_semantics':'quota_capture_before_directory_sweep',
        'snapshot_is_atomic_point_measurement':False,'freshness_timestamp_rewritten':False,
        'end_time_extends_admission_freshness':False}


def preserve_snapshot(root,case,value,begin,end):
    budget_path=ordinary(root/'state/storage-budget.json',root)
    require(budget_path.stat().st_size<=1_000_000,'Bounded original budget receipt required')
    budget_raw=budget_path.read_bytes();record=json.loads(budget_raw)
    require(record==value and record.get('status')=='measured' and Path(record.get('root',''))==root,
        'Returned census must match exact current published budget bytes')
    clocks=snapshot_clocks(record,begin,end)
    entry=record.get('exact_entry_measurements',{})
    detail_path=ordinary(root/'state/storage-entry-sizes.json',root)
    require(entry.get('receipt_path')==str(detail_path) and entry.get('method')=='owned_regular_length',
        'Original exact-size census evidence required')
    with detail_path.open('rb') as stream:detail_raw=stream.read(512*1024**2+1)
    require(len(detail_raw)<=512*1024**2 and hashlib.sha256(detail_raw).hexdigest()==entry.get('receipt_sha256'),
        'Bounded exact census detail bytes/hash required')
    detail=json.loads(detail_raw)
    require(detail.get('status')=='measured' and detail.get('method')=='owned_regular_length'
        and detail.get('count')==entry.get('count')==record.get('study_files')==len(detail.get('items',[]))
        and detail.get('bytes')==entry.get('bytes')==record.get('study_bytes')
        and sum(i['bytes'] for i in detail['items'])==detail['bytes']
        and all(i.get('status')=='measured' and i.get('method')=='owned_regular_length'
            and i.get('eof_observed') is True and type(i.get('bytes')) is int and i['bytes']>=0 for i in detail['items']),
        'Complete measured regular-length inventory must match original totals')
    sources=record.get('native_source_hashes',{})
    require(set(record.get('native_source_paths',[]))==set(sources) and sources,'Original native quota source bytes required')
    for path,sha in sources.items():
        ordinary(path,root/'state');require(digest(path)==sha,'Original native quota source changed')
    for path,raw in ((case/'original-storage-budget.json',budget_raw),(case/'original-storage-entry-sizes.json',detail_raw)):
        with path.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
    require(digest(budget_path)==hashlib.sha256(budget_raw).hexdigest()
        and digest(detail_path)==hashlib.sha256(detail_raw).hexdigest(),'Concurrent census publication changed evidence; no mixed pair')
    return {**clocks,'budget':{'path':str(case/'original-storage-budget.json'),'sha256':hashlib.sha256(budget_raw).hexdigest()},
        'entry_sizes':{'path':str(case/'original-storage-entry-sizes.json'),'sha256':hashlib.sha256(detail_raw).hexdigest()},
        'study_files':record['study_files'],'study_bytes':record['study_bytes'],
        'original_native_quota_sources':sources}


def run_snapshot(snapshot,root,case,deadline,*,wall=time.time,mono=time.monotonic):
    begin=wall();started=mono()
    require(begin+300<deadline,'Finite native snapshot finalization margin required')
    exclusive(case/'sweep-start.json',{'started_unix':begin,'started_utc':datetime.fromtimestamp(begin,timezone.utc).isoformat(),
        'started_monotonic':started,'exact_sizes':True,'registration_mutex_held':False,
        'original_function':'horsehair.budget.storage_snapshot','original_function_modified':False})
    try:
        value=snapshot(root,exact_sizes=True)
        end=wall();elapsed=mono()-started
        require(begin<=end<deadline-180 and elapsed>=0,'Snapshot must finish inside native finalization margin')
        bound=preserve_snapshot(root,case,value,begin,end)
        result={'status':'complete_exact_size_sweep_with_original_conservative_freshness',
            'started_unix':begin,'finished_unix':end,'elapsed_monotonic_seconds':elapsed,
            'finished_utc':datetime.fromtimestamp(end,timezone.utc).isoformat(),**bound,
            **{k:False for k in FALSE_FLAGS}}
        exclusive(case/'sweep-result.json',result);return result
    except BaseException as error:
        exclusive(case/'sweep-failure.json',{'status':'failed_no_timestamp_rewrite_or_automatic_replay',
            'started_unix':begin,'failed_unix':wall(),'error_type':type(error).__name__,'error':str(error),
            **{k:False for k in FALSE_FLAGS}})
        raise


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--native-script',type=Path,required=True)
    p.add_argument('--registry-qualification',type=Path,required=True)
    p.add_argument('--registry-qualification-sha256',required=True)
    p.add_argument('--registry-qualification-job-id',required=True)
    args=p.parse_args();own_sha=digest(Path(__file__));os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    job=str(native_job_id() or '')
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='4' and os.environ.get('HHW_GPU_COUNT')=='0'
        and os.environ.get('HHW_MAX_HOURS')=='4','Owned finite4CPU4h native context required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval required')
    import horsehair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,
        'Exact qualified4f framework and unchanged primary required')
    config=read_json(PRIMARY/'configs/study.json')
    require(canonical_hash(config)==CONFIG and read_json(FRAMEWORK/'configs/study.json')==config,'Full frozen config required')
    for path,sha in ((GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(QUOTA,QUOTA_SHA),(QUALIFICATION,QUALIFICATION_SHA)):
        ordinary(path,ROOT/'maintenance');require(digest(path)==sha,'Pinned context source changed')
    ordinary(SUPPORT,ROOT/'maintenance',fresh=True);SUPPORT.mkdir(mode=0o700)
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    runpy.run_path(str(GUARD))['install'](SUPPORT/'accounting-reads')
    allocation=native_allocation(job,args.native_script,runpy.run_path(str(SCHEDULER))['run_scontrol_job'])
    end=allocation['allocation_end_unix']
    def expired(signum,frame):raise TimeoutError('Finite census deadline reached; original timestamps preserved')
    require(hasattr(signal,'SIGALRM'),'Native POSIX hard deadline required')
    signal.signal(signal.SIGALRM,expired);signal.alarm(max(1,int(end-time.time()-180)))
    registry=registry_qualification(args.registry_qualification,args.registry_qualification_sha256,args.registry_qualification_job_id)
    registry_api=runpy.run_path(str(REGISTRY_GUARD))
    installed=registry_api['install'](SUPPORT/'registered-snapshot-reads',
        registration_mode='controller-already-locked',allocation_deadline_unix=end)
    from horsehair import campaign,repair,budget
    require(campaign._repair_native_states is repair._repair_native_states,'358 must precede campaign reader binding')
    observed=repair._repair_native_states(sorted({str(read_json(QUALIFICATION)['slurm_job_id']),registry['job_id']}),{},owner='pwa209',account='def-ptewarie_cpu')
    public_q=qualification_proof(QUALIFICATION,QUALIFICATION_SHA,str(read_json(QUALIFICATION)['slurm_job_id']),observed)
    private_q=completed_registry_qualification(registry,observed)
    admission=brief_admission(lambda:campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],
        SUPPORT,'fresh-census-baseline',resource_policy=policy,cpus=0,gpus=0,hours=4),end,
        registration_lock=registry_api['registration_lock'],evidence=SUPPORT/'baseline-registration-lock')
    exclusive(SUPPORT/'native-bindings.json',{'allocation':allocation,'public_qualification':public_q,
        'registry_qualification':private_q,'registry_installation':installed,'admission':admission,
        'wrapper_sha256':own_sha,'source_sha256':SOURCE,'primary_source_sha256':PRIMARY_SHA,
        'full_config_sha256':CONFIG,'resource_policy':policy.receipt(),**{k:False for k in FALSE_FLAGS}})
    with resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=4,job_id=job,resource_policy=policy):
        quota_api=runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait']
        quota_before=quota_api(ROOT,required_files=2000,required_bytes=1_000_000_000,reserve_files=248000,
            reserve_bytes=2_000_000_000_000,evidence_dir=SUPPORT/'quota-before',allocation_deadline_unix=end)
        result=run_snapshot(budget.storage_snapshot,ROOT,SUPPORT,end)
        quota_after=quota_api(ROOT,required_files=2000,required_bytes=1_000_000_000,reserve_files=248000,
            reserve_bytes=2_000_000_000_000,evidence_dir=SUPPORT/'quota-after',allocation_deadline_unix=end)
        require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA
            and digest(POLICY)==POLICY_SHA and digest(Path(__file__))==own_sha,'Qualified sources/policy/wrapper changed during sweep')
        exclusive(SUPPORT/'receipt.json',{'status':result['status'],'native_job_id':job,'allocation':allocation,
            'sweep':{'path':str(SUPPORT/'sweep-result.json'),'sha256':digest(SUPPORT/'sweep-result.json')},
            'quota_before':quota_before,'quota_after':quota_after,'source_sha256':SOURCE,
            'resource_policy':policy.receipt(),**{k:False for k in FALSE_FLAGS}})
    signal.alarm(0)
    print(json.dumps({'status':result['status'],'study_files':result['study_files'],'study_bytes':result['study_bytes']}))


if __name__=='__main__':main()
