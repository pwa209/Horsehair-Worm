"""Repeatable finite private census; original byte proofs and quota clocks."""
from pathlib import Path
from datetime import datetime,timezone
from contextlib import contextmanager
import argparse
import ast
import hashlib
import json
import math
import inspect
import os
import re
import runpy
import signal
import shlex
import subprocess
import sys
import textwrap
import time
import xml.etree.ElementTree as ET

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

CASE_ROOT=ROOT/'maintenance/study-census-resilience-v1-20261024'
SUPPORT=CASE_ROOT
SOURCE_SUPPORT=ROOT/'maintenance/study-census-resilience-support-v1-20261024'
KERNEL_Q=ROOT/'maintenance/study-census-resilience-qualification-v1-20261024/qualification.json'
KERNEL_NAME='streaming-storage-census-v1-20261023.py'
QUALIFIER_NAME='qualify-census-resilience-v1-20261024.py'
WRAPPER_NAME='run-fresh-storage-census-resilience-v1-20261024.py'
BASE_NAME='run-fresh-storage-census-v5-20261023.py'
BASE_SHA='b448607b00512ef865a3369da56c18bf6f98ab589b6a524795e5bbed9c64e569'
OLD_QUALIFIER='qualify-streaming-census-v1-20261023.py'
OUTER='run-af-agent-controller-census-policy-v1-20261023.py'
BASE_CONTROLLER=ROOT/'maintenance/study-af-registry-repair-support-v6-20261022/run-af-agent-controller-registry-v6-20261022.py'
BASE_CONTROLLER_SHA='e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
PHASE='study-census-resilience-v1-20261024'
MAX_SLOTS=64
DRIVER=ROOT/'maintenance/sixth-repair-support-20261016/controller-primary-repair-sixth-20261016.py'
DRIVER_SHA='49815eeb509909d5eac2ebde2e4f4a3918cf40cefdc5f643f2bcaffead629eb1'
PRIVATE_FILES={WRAPPER_NAME,'test_run_fresh_storage_census_v6_20261023.py',BASE_NAME,
    'test_run_fresh_storage_census_v5_20261023.py',KERNEL_NAME,'test_streaming_storage_census_v1_20261023.py',
    QUALIFIER_NAME,'test_qualify_streaming_census_v2_20261023.py',OLD_QUALIFIER,
    'test_qualify_streaming_census_v1_20261023.py',OUTER,'test_af_agent_controller_census_policy_v1_20261023.py',
    'registered-snapshot-read-guard-v4-20261022.py','test_registered_snapshot_read_guard_v4_20261022.py'}
BASELINE_NAMES=('budget','storage_artifacts','storage_artifact_cache','storage_boundary_cache',
    'storage_snapshot_artifacts','storage_snapshot_boundary')
SUITE_SOURCES={**{'baseline_'+name:'tests/test_'+name+'.py' for name in BASELINE_NAMES},
    'kernel':'test_streaming_storage_census_v1_20261023.py','wrapper':'test_run_fresh_storage_census_v5_20261023.py',
    'registry':'test_registered_snapshot_read_guard_v4_20261022.py','qualifier':'test_qualify_streaming_census_v1_20261023.py',
    'repeatable_wrapper':'test_run_fresh_storage_census_v6_20261023.py',
    'rolling_qualifier':'test_qualify_streaming_census_v2_20261023.py',
    'external_census_policy':'test_af_agent_controller_census_policy_v1_20261023.py'}
PRESERVED_ARTIFACT_FUNCTIONS=('_require_linux','_relative_parts','_length_arguments','_open_directory',
    '_metadata','_directory_identity','_stable_regular_length','measure_regular_length','measure_regular_eof',
    'validate_regular_length_measurement','_regular_identity','_boundary_length','artifact_read_cap',
    '_statx_function','_open_leaf','_parallel_lengths')
FRESHNESS_SECONDS=21600
FINALIZATION_SECONDS=900

EXTENDED_REGISTRY_GUARD=Path(__file__).with_name('registered-snapshot-read-guard-resilience-v1-20261024.py')
EXTENDED_REGISTRY_SHA='983ec0ca4c6fc9ff8f995ec6dc15f81adbf2e9205d0235964d8094bbbdb05fcc'

FALSE_FLAGS=('scientific_result','admission_authority','scientific_gates_cleared','freshness_timestamp_rewritten','end_time_extends_admission_freshness')

LEGACY_SUPPORT=ROOT/'maintenance/study-af-rolling-census-support-v4-20261023'
LEGACY_BINDING={'helper': {'path': '/scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-support-v4-20261023/run-fresh-storage-census-v6-20261023.py', 'sha256': '487bf2dea2f5c892c369586e11c4885e44e4c5bbddbda1e9e3b82f185666f9f3'}, 'source_manifest': {'path': '/scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-support-v4-20261023/source-manifest.json', 'sha256': '1b21742f6ad5b7b48899a4890988da7ddba439798ba8b12a2bad57c7f1898e6e'}, 'qualification': {'path': '/scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-qualification-v5-20261023/qualification.json', 'sha256': '8b0dc612cee64461b991a9dfb9a975cf5afe1389a037b1357244d829f5aca76c', 'job_id': '6863966'}}


def require(value,message):
    if not value:raise ValueError(message)


def create_case(case_id,job):
    """One immutable attempt per actual scalar allocation, within sixty-four slots."""
    require(isinstance(case_id,str) and re.fullmatch(r'rolling-(?:0[1-9]|[1-5][0-9]|6[0-4])',case_id)
        and isinstance(job,str) and re.fullmatch('[1-9][0-9]*',job),'Explicit finite rolling case and actual scalar job required')
    require(CASE_ROOT.is_absolute() and CASE_ROOT==CASE_ROOT.resolve() and CASE_ROOT.is_relative_to(ROOT/'maintenance')
        and all(not p.is_symlink() for p in [CASE_ROOT,*CASE_ROOT.parents] if p.is_relative_to(ROOT/'maintenance')),
        'Ordinary confined rolling case root required')
    CASE_ROOT.mkdir(mode=0o700,exist_ok=True)
    require(CASE_ROOT.is_dir() and not CASE_ROOT.is_symlink(),'Ordinary permanent rolling case root required')
    path=CASE_ROOT/('native-'+job);ordinary(path,CASE_ROOT,fresh=True);path.mkdir(mode=0o700)
    return path


def provider_registration(args,job,config):
    """Bind all finite slots and the selected invocation; never submit or replay."""
    directory=ordinary(args.native_plan,ROOT/'plans');ledger_path=ordinary(args.native_ledger,ROOT/'state')
    require(digest(DRIVER)==DRIVER_SHA,'Original source-bound registration validator changed')
    driver=runpy.run_path(str(DRIVER),run_name='rolling_census_original_registration')
    plan,ledger=driver['checked_pair'](directory,ledger_path,ROOT,config)
    tasks=plan.get('tasks');require(type(tasks) is list and 1<=len(tasks)<=MAX_SLOTS and plan.get('phase')==PHASE,
        'One finite rolling plan with at most sixty-four allocations required')
    own={'path':str(Path(__file__)),'sha256':digest(Path(__file__))}
    manifest={'path':str(args.source_manifest),'sha256':args.source_manifest_sha256}
    predecessor=None;selected=None
    for index,task in enumerate(tasks,1):
        slot='rolling-'+str(index).zfill(2);name='rolling-storage-census-'+str(index).zfill(2)
        expected={'helper':own,'source_manifest':manifest,'case_id':slot}
        require(task.get('name')==name and task.get('rolling_census_provider')==expected
            and task.get('resources')=={'cpus':8,'gpu':False,'hours':6,'memory':'64G','array':None}
            and task.get('depends')==([] if index==1 else ['rolling-storage-census-'+str(index-1).zfill(2)])
            and task.get('dependency_kind')==('afterok' if index==1 else 'afterany')
            and task.get('external_dependencies')==([args.kernel_qualification_job_id] if index==1 else []),
            'Exact source-bound sequential six-hour rolling slot required')
        command=shlex.split(task.get('command',''))
        require(command.count(own['path'])==1,'Every rolling task must invoke the qualified v6 helper once')
        tail=command[command.index(own['path'])+1:]
        expected_flags={'--case-id':slot,'--source-manifest':manifest['path'],
            '--source-manifest-sha256':manifest['sha256'],'--kernel-qualification':str(args.kernel_qualification),
            '--kernel-qualification-sha256':args.kernel_qualification_sha256,
            '--kernel-qualification-job-id':args.kernel_qualification_job_id,
            '--native-plan':str(args.native_plan),'--native-ledger':str(args.native_ledger),'--native-task':name,
            '--native-script':str(task.get('script','')),
            '--registry-qualification':str(args.registry_qualification),
            '--registry-qualification-sha256':args.registry_qualification_sha256,
            '--registry-qualification-job-id':args.registry_qualification_job_id}
        require(len(tail)==2*len(expected_flags) and set(tail[::2])==set(expected_flags)
            and all(tail.count(flag)==1 and tail[tail.index(flag)+1]==value for flag,value in expected_flags.items()),
            'Every rolling slot must bind all exact native/manifest/qualification arguments')
        script=ordinary(task['script'],ROOT/'plans')
        record=ledger.get('tasks',{}).get(name,{})
        require(task['command'] in script.read_text()
            and canonical_hash(script.read_text())==task.get('script_sha256')==record.get('script_sha256')
            and record.get('state')=='submitted' and re.fullmatch('[1-9][0-9]*',str(record.get('job_id',''))),
            'Every rolling slot must retain an actual unchanged registered script/job')
        if name==args.native_task:
            require(slot==args.case_id and record['job_id']==job and str(args.native_script)==task['script'],
                'Actual allocation must bind its selected finite slot')
            selected={'case_id':slot,'native_job_id':job,'predecessor_job_id':predecessor,
                'slot_count':len(tasks),'maximum_cpu_core_hours':48*len(tasks)}
        predecessor=record['job_id']
    require(selected is not None,'Selected rolling task is absent')
    require(len({str(ledger['tasks'][t['name']]['job_id']) for t in tasks})==len(tasks),
        'Rolling slots must bind distinct actual allocations')
    return selected


def preserve_closed_progress(local_path,case,*,error=None):
    """Copy only after the writer/measurement context closes; never an inventory.

    Native hard termination or uninterruptible I/O can prevent this finally
    path from executing. A saved prefix never establishes complete coverage.
    """
    name='partial-progress.jsonl' if error is not None else 'sweep-progress.jsonl'
    target=case/name
    with Path(local_path).open('rb') as source,target.open('xb') as output:
        while block:=source.read(1_000_000):output.write(block)
        output.flush();os.fsync(output.fileno())
    proof={'path':str(target),'sha256':digest(target),'partial_census_authority':False,
        'progress_is_complete_inventory':False,'writer_closed_before_copy':True,
        'hard_native_termination_may_prevent_preservation':True}
    if error is not None:
        exclusive(case/'progress-failure.json',{'status':'failed_prefix_progress_preserved',
            'error_type':type(error).__name__,'error':str(error),'progress':proof,
            'automatic_replay_permitted':False,**{k:False for k in FALSE_FLAGS}})
    return proof


def measured_sweep(snapshot,kernel,root,case,end,job):
    """Preserve a closed progress journal on normal return or ordinary failure."""
    local=progress_path(job);failure=None
    try:
        with local.open('xb') as stream:
            progress,latest=progress_recorder(stream)
            with operational_adapter(kernel,deadline=time.monotonic()+max(0.,end-time.time()-FINALIZATION_SECONDS),progress=progress) as adapter:
                result=run_snapshot(snapshot,root,case,end,adapter=adapter)
    except BaseException as error:
        failure=error
        raise
    finally:
        if local.is_file():
            try:preserve_closed_progress(local,case,error=failure)
            except BaseException as preserve_error:
                if failure is None:raise
                # Keep the primary measurement refusal authoritative. This
                # explicit record may itself be impossible on quota/I/O loss.
                exclusive(case/'progress-preservation-failure.json',{'status':'progress_preservation_failed',
                    'measurement_error_type':type(failure).__name__,'measurement_error':str(failure),
                    'preservation_error_type':type(preserve_error).__name__,'preservation_error':str(preserve_error),
                    'partial_census_authority':False,**{k:False for k in FALSE_FLAGS}})
    return result,adapter,latest

def exclusive(path,value):
    raw=(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())

def memory(value):
    match=re.fullmatch(r'([1-9][0-9]*)([KMG])',value)
    require(match is not None,'Exact positive native memory units required')
    return int(match[1])*1024**('KMG'.index(match[2])+1)


def cpu_only_tres(text):
    pairs=[item.partition('=') for item in text.split(',')];value={key:item for key,sep,item in pairs}
    require(len(value)==len(pairs) and all(sep and key and item for key,sep,item in pairs)
        and value.get('cpu')=='8' and value.get('node')=='1' and memory(value.get('mem',''))==64*1024**3
        and not any('gpu' in key.lower() for key in value),'Exact positive native8CPU64G single-node TRES required')
    return value

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


def registered_task(plan_path,ledger_path,task_name,script,job,*,hours,helper,argv=None):
    plan_path=ordinary(plan_path,ROOT/'plans');ledger_path=ordinary(ledger_path,ROOT/'state')
    script=ordinary(script,ROOT/'plans')
    plan=read_json(plan_path);ledger=read_json(ledger_path)
    selected=[t for t in plan.get('tasks',[]) if t.get('name')==task_name]
    require(len(selected)==1 and plan.get('config_sha256')==CONFIG
        and plan.get('release_identity',{}).get('sha256')==SOURCE and ledger.get('source_sha256')==SOURCE
        and ledger.get('plan_sha256')==canonical_hash(plan),'Exact registered source/config plan and ledger required')
    task=selected[0];record=ledger.get('tasks',{}).get(task_name,{})
    command=shlex.split(task.get('command',''));helper=str(helper)
    require(command.count(helper)==1 and task.get('script')==str(script)
        and task['command'] in script.read_text() and record.get('job_id')==job and record.get('state')=='submitted'
        and record.get('script_sha256')==task.get('script_sha256')==canonical_hash(script.read_text())
        and task.get('resources')=={'cpus':8,'memory':'64G','hours':hours,'gpu':False,'array':None},
        'Exact registered scalar8CPU64G source/script/resources required')
    tail=command[command.index(helper)+1:]
    if argv is not None:require(tail==list(argv),'All actual invocation arguments must match the registered command')
    return {'plan':{'path':str(plan_path),'sha256':digest(plan_path)},
        'ledger':{'path':str(ledger_path),'sha256':digest(ledger_path)},
        'script':{'path':str(script),'sha256':digest(script)},'task_name':task_name,'native_job_id':job},tail


def native_allocation(job,script,reader,*,plan_path,ledger_path,task_name,argv):
    registration,_=registered_task(plan_path,ledger_path,task_name,script,job,hours=6,helper=Path(__file__),argv=argv)
    response=reader(job,SUPPORT/'own-scheduler-reads');response.check_returncode()
    lines=[s for s in response.stdout.splitlines() if s.strip()]
    require(len(lines)==1,'One exact actual native allocation required')
    pairs=[(key,value.strip()) for key,value in re.findall(r'(?<!\S)(\w+)=(.*?)(?=\s+\w+=|$)',lines[0])];f=dict(pairs)
    require(len(f)==len(pairs) and f.get('JobId')==job and f.get('UserId')=='pwa209(3164768)'
        and f.get('JobState')=='RUNNING' and f.get('Account')=='def-ptewarie_cpu'
        and f.get('NumCPUs')=='8' and f.get('NumNodes') in {'1','1-1'}
        and f.get('TimeLimit') in {'06:00:00','6:00:00','0-06:00:00'}
        and memory(f.get('MinMemoryNode',''))==64*1024**3 and f.get('Command')==str(script)
        and f.get('JobName')=='hhw-'+task_name and 'ArrayJobId' not in f and 'ArrayTaskId' not in f,
        'Owned scalar8CPU64G6h allocation/script required')
    require(all('gpu' not in f.get(k,'').lower() for k in
        ('TresPerNode','TresPerJob','TresPerTask','ReqTRES','AllocTRES','Gres')),'Census must allocate no GPU')
    cpu_only_tres(f.get('ReqTRES',''));cpu_only_tres(f.get('AllocTRES',''))
    end=datetime.fromisoformat(f.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp()
    env=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(env) and abs(env-end)<=2 and time.time()+900<end,'Verified native finite end/margin required')
    return {'fields':f,'allocation_end_unix':end,'native_stdout':response.stdout,
        'native_registration':registration}


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
    require(detail.get('root')==str(root) and type(record.get('personal_uid')) is int
        and record['personal_uid']>=0,'Original owned complete inventory root/UID required')
    names=[item.get('relative_path') for item in detail['items']]
    require(all(type(name) is str for name in names) and names==sorted(set(names)),
        'Complete inventory must have unique sorted relative paths')
    from horsehair.storage_artifacts import validate_regular_length_measurement
    for item in detail['items']:
        validate_regular_length_measurement(item,expected_uid=record['personal_uid'],max_bytes=6_000_000_000_000,
            root=root,path=root/item['relative_path'])
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


def fresh_original_clocks(value,now):
    measured=value['unchanged_snapshot_measured_unix']
    captured=datetime.fromisoformat(value['unchanged_snapshot_captured_utc'].replace('Z','+00:00')).timestamp()
    require(type(now) in (int,float) and math.isfinite(now)
        and all(type(t) in (int,float) and math.isfinite(t) and 0<=now-t<FRESHNESS_SECONDS for t in (measured,captured)),
        'Original quota clocks must remain strictly younger than six hours; later quota cannot retime them')
    return {'validated_unix':now,'original_measured_age_seconds':now-measured,
        'original_captured_age_seconds':now-captured,'maximum_age_seconds':FRESHNESS_SECONDS}


def function_ast(function):
    tree=ast.parse(textwrap.dedent(inspect.getsource(function)))
    return hashlib.sha256(ast.dump(tree,include_attributes=False).encode()).hexdigest()


def primitive_identity(budget_module=None,artifacts_module=None):
    if budget_module is None:
        from horsehair import budget as budget_module
    if artifacts_module is None:
        from horsehair import storage_artifacts as artifacts_module
    functions={'horsehair.budget.storage_snapshot':budget_module.storage_snapshot,
        **{'horsehair.storage_artifacts.'+name:getattr(artifacts_module,name) for name in PRESERVED_ARTIFACT_FUNCTIONS}}
    return {'functions':{name:{'object_id':id(function),'ast_sha256':function_ast(function)} for name,function in functions.items()},
        'source_files':{str(Path(module.__file__).resolve()):digest(Path(module.__file__)) for module in (budget_module,artifacts_module)}}


@contextmanager
def operational_adapter(kernel,*,deadline,progress=None,cancelled=None,budget_module=None,artifacts_module=None):
    """Replace only the iterator for one dedicated census; restore on all exits."""
    if budget_module is None:
        from horsehair import budget as budget_module
    if artifacts_module is None:
        from horsehair import storage_artifacts as artifacts_module
    require(kernel.get('ARTIFACTS') is artifacts_module and callable(kernel.get('iter_regular_lengths'))
        and kernel.get('MAX_WORKERS')==64 and kernel.get('MAX_PENDING')==128,
        'Private kernel must use the unchanged qualified measurement module')
    before=primitive_identity(budget_module,artifacts_module)
    objects={name:getattr(artifacts_module,name) for name in PRESERVED_ARTIFACT_FUNCTIONS}
    snapshot=budget_module.storage_snapshot;original=artifacts_module.iter_regular_lengths
    require(getattr(original,'__module__',None)==artifacts_module.__name__,
        'Canonical original iterator required before private adapter installation')
    proof={'before':before,'requested_workers':64,'requested_pending':128,
        'pending_capacity_adapts_to_native_file_descriptor_budget':True,
        'budget_function_body_unchanged':True,'measurement_functions_unchanged':True,
        'traversal_policy_changed':True,'other_measurements_may_remain_active_across_yield':True,
        'each_returned_measurement_has_closed_own_descriptors':True,
        'only_pending_measurements_and_results_are_bounded':True,'restored':False}
    def iterator(root,max_bytes,expected_uid=None,*,workers=1):
        return kernel['iter_regular_lengths'](root,max_bytes,expected_uid,workers=64,pending_limit=128,
            deadline=deadline,progress=progress,cancelled=cancelled)
    artifacts_module.iter_regular_lengths=iterator
    try:
        yield proof
    finally:
        installed=artifacts_module.iter_regular_lengths
        artifacts_module.iter_regular_lengths=original
        after=primitive_identity(budget_module,artifacts_module)
        proof.update({'after':after,'restored':True,'original_iterator_object_id':id(original),
            'restored_iterator_object_id':id(artifacts_module.iter_regular_lengths)})
        require(installed is iterator and budget_module.storage_snapshot is snapshot
            and all(getattr(artifacts_module,name) is function for name,function in objects.items()) and before==after,
            'Original function identity/source changed during private traversal; no successful census')


def run_snapshot(snapshot,root,case,deadline,*,wall=time.time,mono=time.monotonic,adapter=None):
    begin=wall();started=mono()
    require(begin+FINALIZATION_SECONDS<deadline,'Finite native snapshot finalization margin required')
    exclusive(case/'sweep-start.json',{'started_unix':begin,'started_utc':datetime.fromtimestamp(begin,timezone.utc).isoformat(),
        'started_monotonic':started,'exact_sizes':True,'registration_mutex_held':False,
        'original_function':'horsehair.budget.storage_snapshot','budget_function_body_unchanged':True,
        'traversal_policy_changed':adapter is not None,'private_adapter':adapter})
    try:
        value=snapshot(root,exact_sizes=True)
        end=wall();elapsed=mono()-started
        require(begin<=end<deadline-FINALIZATION_SECONDS and elapsed>=0,'Snapshot must finish inside native finalization margin')
        bound=preserve_snapshot(root,case,value,begin,end)
        freshness=fresh_original_clocks(bound,end)
        result={'status':'complete_exact_size_sweep_with_original_conservative_freshness',
            'started_unix':begin,'finished_unix':end,'elapsed_monotonic_seconds':elapsed,
            'finished_utc':datetime.fromtimestamp(end,timezone.utc).isoformat(),**bound,'freshness_at_sweep_end':freshness,
            **{k:False for k in FALSE_FLAGS}}
        exclusive(case/'sweep-result.json',result);return result
    except BaseException as error:
        exclusive(case/'sweep-failure.json',{'status':'failed_no_timestamp_rewrite_or_automatic_replay',
            'started_unix':begin,'failed_unix':wall(),'error_type':type(error).__name__,'error':str(error),
            **{k:False for k in FALSE_FLAGS}})
        raise


def registry_read_extension():
    original=LEGACY_SUPPORT/'registered-snapshot-read-guard-v4-20261022.py'
    require(digest(original)=='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
        and digest(EXTENDED_REGISTRY_GUARD)==EXTENDED_REGISTRY_SHA,'Exact original and resilient registration guards required')
    before=ast.parse(original.read_bytes());after=ast.parse(EXTENDED_REGISTRY_GUARD.read_bytes())
    def reduce(tree):
        values={}
        for n in list(tree.body):
            if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id in {'CONTROLLER_LOCK_SECONDS','LOCK_MARGIN'}:
                values[n.targets[0].id]=ast.literal_eval(n.value);tree.body.remove(n)
        function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='registration_lock')
        loop=next(n for n in ast.walk(function) if isinstance(n,ast.For));values['loop']=ast.unparse(loop.iter)
        loop.iter=ast.Constant('bounded registration acquisition attempts')
        return ast.dump(tree,include_attributes=False),values
    a,x=reduce(before);b,y=reduce(after)
    require(a==b and x=={'CONTROLLER_LOCK_SECONDS':900,'LOCK_MARGIN':300,'loop':'range(1, 61 if role == \'worker-admission\' else 16)'}
        and y=={'CONTROLLER_LOCK_SECONDS':7200,'LOCK_MARGIN':600,'loop':'range(1, 61 if role == \'worker-admission\' else 121)'},
        'Only finite provider registration wait/attempts/finalization reserve may change')
    return {'baseline_guard':{'path':str(original),'sha256':digest(original)},
        'extended_guard':{'path':str(EXTENDED_REGISTRY_GUARD),'sha256':EXTENDED_REGISTRY_SHA},
        'maximum_registration_wait_seconds':7200,'native_finalization_reserve_seconds':600,
        'registration_callback_retries':0,'guard_removed_or_replaced':False,
        'scientific_controller_qualification_authorized':False}



def source_manifest(path,sha):
    require(Path(path)==SOURCE_SUPPORT/'source-manifest.json' and re.fullmatch('[0-9a-f]{64}',str(sha)),
        'Exact new resilience source manifest required')
    ordinary(path,ROOT/'maintenance');require(Path(path).stat().st_size<=200_000 and digest(path)==sha,'Source manifest changed')
    value=read_json(path)
    require(set(value)=={'kind','files','suites','legacy'} and value['kind']=='census-resilience-source-manifest-v1'
        and value['legacy']==LEGACY_BINDING and set(value['suites'])=={'resilience'},'Exact new and original source closure required')
    require(isinstance(value['files'],dict) and Path(__file__).name in value['files'],'Explicit complete new source pins required')
    pins={}
    for name,pin in value['files'].items():
        require(Path(name).name==name and re.fullmatch('[0-9a-f]{64}',str(pin)),'Flat pinned source capsule required')
        target=ordinary(SOURCE_SUPPORT/name,ROOT/'maintenance');require(digest(target)==pin,'Resilience source changed: '+name)
        pins[str(target)]=pin
    suite=value['suites']['resilience']
    require(set(suite)=={'source','source_sha256','tests','roster_sha256'} and suite['source']=='test_census_resilience_v1_20261024.py'
        and type(suite['tests']) is int and suite['tests']>0 and re.fullmatch('[0-9a-f]{64}',suite['roster_sha256'])
        and value['files'].get(suite['source'])==suite['source_sha256'],'Exact generic test source/count/roster required')
    _,old_pins=legacy_module().source_manifest(Path(LEGACY_BINDING['source_manifest']['path']),LEGACY_BINDING['source_manifest']['sha256'])
    require(not set(pins)&set(old_pins),'New and legacy source namespaces must remain separate')
    pins.update(old_pins);require(digest(path)==sha,'Manifest changed during source verification')
    return value,pins



def junit_roster(path,tests,roster,expected_sha=None):
    path=ordinary(path,KERNEL_Q.parent)
    with path.open('rb') as stream:raw=stream.read(8_000_001)
    require(len(raw)<=8_000_000 and (expected_sha is None or hashlib.sha256(raw).hexdigest()==expected_sha),
        'Bounded exact actual native JUnit bytes required')
    tree=ET.fromstring(raw);cases=list(tree.iter('testcase'))
    require(len(cases)==tests and not any(list(case.iter(name)) for case in cases for name in ('failure','error','skipped')),
        'Every native qualification test must actually pass without skips')
    names=[str(case.get('classname','')).split('.')[-1]+'::'+str(case.get('name','')) for case in cases]
    require(all(case.get('classname') and case.get('name') for case in cases) and len(set(names))==tests
        and canonical_hash(sorted(names))==roster,'Exact actual complete native JUnit roster required')
    suites=[node for node in tree.iter('testsuite') if not list(node.findall('testsuite'))]
    require(suites and sum(int(node.get('tests','-1')) for node in suites)==tests
        and all(all(int(node.get(key,'0'))==0 for key in ('failures','errors','skipped'))
            for node in tree.iter() if node.tag in {'testsuite','testsuites'}),
        'Native JUnit summary disagrees with actual cases')


def primitive_contract(proof):
    return {'source_files':proof['source_files'],
        'functions':{name:value['ast_sha256'] for name,value in proof['functions'].items()}}


def kernel_qualification(path,sha,job,manifest_path,manifest_sha,observed):
    require(Path(path)==KERNEL_Q and re.fullmatch('[0-9a-f]{64}',str(sha)) and re.fullmatch('[1-9][0-9]*',str(job)),
        'Exact new resilience qualification locator required')
    ordinary(path,ROOT/'maintenance');require(Path(path).stat().st_size<=2_000_000 and digest(path)==sha,'Qualification bytes changed')
    manifest,pins=source_manifest(manifest_path,manifest_sha);q=read_json(path)
    require(q.get('status')=='complete_census_resilience_operational_qualification' and q.get('returncode')==0
        and type(q['returncode']) is int and q.get('slurm_job_id')==job and q.get('source_sha256')==SOURCE
        and q.get('config_sha256')==CONFIG and q.get('source_verified_unchanged') is True and q.get('source_pins')==pins
        and q.get('source_manifest')=={'path':str(manifest_path),'sha256':manifest_sha}
        and q.get('qualification_helper')=={'path':str(SOURCE_SUPPORT/QUALIFIER_NAME),'sha256':pins[str(SOURCE_SUPPORT/QUALIFIER_NAME)]}
        and q.get('legacy_binding')==LEGACY_BINDING and all(q.get(k) is False for k in FALSE_FLAGS),
        'Complete actual new qualification and unchanged original lineage required')
    expected=manifest['suites']['resilience'];suite=q.get('suite',{})
    require(suite.get('source')=={'path':str(SOURCE_SUPPORT/expected['source']),'sha256':expected['source_sha256']}
        and suite.get('test_counts')=={'tests':expected['tests'],'failures':0,'errors':0,'skipped':0}
        and suite.get('test_roster_sha256')==expected['roster_sha256'],'Exact full passing generic qualification required')
    junit=suite['junit'];require(Path(junit['path'])==KERNEL_Q.parent/('native-'+job)/'resilience.xml','Confined native JUnit required')
    junit_roster(junit['path'],expected['tests'],expected['roster_sha256'],junit['sha256'])
    legacy=legacy_module();binding=LEGACY_BINDING['qualification']
    old=legacy.kernel_qualification(Path(binding['path']),binding['sha256'],binding['job_id'],
        Path(LEGACY_BINDING['source_manifest']['path']),LEGACY_BINDING['source_manifest']['sha256'],observed)
    registration=q['native_registration']
    actual=qualification_registration(registration,job,manifest_path=manifest_path,manifest_sha=manifest_sha)
    require(actual==registration,'Actual source-bound new qualifier registration changed')
    record=observed.get(job,{})
    require(record.get('state')=='COMPLETED' and record.get('exit_code')=='0:0' and record.get('job_id_raw')==job
        and record.get('user')=='pwa209' and record.get('account')=='def-ptewarie_cpu'
        and record.get('unallocated_metadata') is False and record.get('cpus')==4
        and type(record.get('elapsed_seconds')) is int and 0<record['elapsed_seconds']<=3600
        and 'gpu' not in record.get('alloc_tres','').lower(),'Positive owned actual4CPU new qualification native completion required')
    proof=completed_native_dependency(ROOT,job);require(proof is not None,'New qualification final native proof pending')
    require(source_manifest(manifest_path,manifest_sha)==(manifest,pins) and digest(path)==sha,'Qualification inputs changed')
    return {'path':str(path),'sha256':sha,'job_id':job,'native_observed':record,'completion':proof,
        'source_manifest':{'path':str(manifest_path),'sha256':manifest_sha},'source_pins':pins,
        'legacy_qualification':old,'registration_wait':registry_read_extension()}



def progress_recorder(stream):
    latest={};written=[0]
    def progress(value):
        require(type(value) is dict and value.get('partial_census_authority') is False,'Progress never proves complete inventory')
        latest.clear();latest.update(value)
        if value['entries']==1 or value['entries']-written[0]>=1000:
            stream.write((json.dumps(value,sort_keys=True,allow_nan=False)+'\n').encode());stream.flush();os.fsync(stream.fileno());written[0]=value['entries']
    return progress,latest


def progress_path(job):
    temporary=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temporary.is_absolute() and temporary==temporary.resolve() and temporary.is_dir()
        and not temporary.is_relative_to(ROOT) and all(not p.is_symlink() for p in [temporary,*temporary.parents])
        and temporary.stat().st_uid==3164768,'Owned allocation-local ordinary progress directory outside census required')
    path=temporary/('streaming-census-'+job+'-progress.jsonl')
    require(not path.exists() and not path.is_symlink(),'Fresh allocation-local progress journal required')
    return path


def final_snapshot_check(result,root,case,*,wall=time.time):
    freshness=fresh_original_clocks(result,wall())
    for active,key in ((root/'state/storage-budget.json','budget'),(root/'state/storage-entry-sizes.json','entry_sizes')):
        saved=result[key];require(Path(saved['path']).parent==case and digest(active)==digest(saved['path'])==saved['sha256'],
            'Published complete census pair changed before final receipt')
    return freshness


def legacy_module():
    import importlib.util
    path=LEGACY_SUPPORT/'run-fresh-storage-census-v6-20261023.py'
    require(digest(path)=='487bf2dea2f5c892c369586e11c4885e44e4c5bbddbda1e9e3b82f185666f9f3','Exact original v6 provider source required')
    spec=importlib.util.spec_from_file_location('qualified_legacy_census_v6',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def predecessor_terminal(provider,observed):
    job=provider['predecessor_job_id']
    if job is None:return None
    record=observed.get(job,{})
    terminal={'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}
    require(record.get('job_id_raw')==job and record.get('state') in terminal and record.get('user')=='pwa209'
        and record.get('account')=='def-ptewarie_cpu' and re.fullmatch('[0-9]+:[0-9]+',str(record.get('exit_code','')))
        and type(record.get('cpus')) is int and record['cpus'] in {0,8}
        and type(record.get('elapsed_seconds')) is int and 0<=record['elapsed_seconds']<=21600
        and 'gpu' not in record.get('alloc_tres','').lower(),'Actual owned terminal predecessor proof required')
    require((record['cpus']==0 and record['elapsed_seconds']==0 and record.get('unallocated_metadata') is True)
        or (record['cpus']==8 and record['elapsed_seconds']>0 and record.get('unallocated_metadata') is False),
        'Actual allocated cost or explicit zero-cost unallocated terminal metadata required')
    proof=completed_native_dependency(ROOT,job,dependency_kind='afterany')
    require(proof is not None,'Persisted terminal predecessor accounting is pending')
    return {'job_id':job,'native_observed':record,'completion':proof,'dependency_kind':'afterany',
        'successful_census_claimed':False,'admission_authority':False,'scientific_result':False}


def qualification_registration(registration,job,*,manifest_path,manifest_sha):
    plan_path=ordinary(registration['plan']['path'],ROOT/'plans');ledger_path=ordinary(registration['ledger']['path'],ROOT/'state')
    script=ordinary(registration['script']['path'],ROOT/'plans');plan=read_json(plan_path);ledger=read_json(ledger_path)
    require(plan.get('config_sha256')==CONFIG and plan.get('release_identity',{}).get('sha256')==SOURCE
        and ledger.get('source_sha256')==SOURCE and ledger.get('plan_sha256')==canonical_hash(plan),
        'Source/config-bound qualification plan/ledger required')
    task=next((t for t in plan['tasks'] if t['name']==registration['task_name']),None)
    require(task is not None and task.get('resources')=={'cpus':4,'memory':'16G','hours':1,'gpu':False,'array':None}
        and task['script']==str(script) and task['command'] in script.read_text()
        and shlex.split(task['command']).count(str(SOURCE_SUPPORT/QUALIFIER_NAME))==1,
        'Exact finite qualification helper/script/resources required')
    command=shlex.split(task['command']);tail=command[command.index(str(SOURCE_SUPPORT/QUALIFIER_NAME))+1:]
    flags={'--source-manifest':str(manifest_path),'--source-manifest-sha256':manifest_sha,
        '--native-plan':str(plan_path),'--native-ledger':str(ledger_path),'--native-script':str(script),'--native-task':task['name']}
    require(len(tail)==2*len(flags) and len(set(tail[::2]))==len(flags) and dict(zip(tail[::2],tail[1::2]))==flags
        and task.get('census_resilience_qualification')=={'helper':{'path':str(SOURCE_SUPPORT/QUALIFIER_NAME),'sha256':digest(SOURCE_SUPPORT/QUALIFIER_NAME)},
            'source_manifest':{'path':str(manifest_path),'sha256':manifest_sha}},
        'Every actual qualification command argument and source-bound helper/manifest must match')
    entry=ledger['tasks'].get(task['name'],{})
    require(entry.get('state')=='submitted' and entry.get('job_id')==job
        and entry.get('script_sha256')==task['script_sha256']==canonical_hash(script.read_text())
        and all(digest(registration[k]['path'])==registration[k]['sha256'] for k in ('plan','ledger','script')),
        'Exact actual qualification registration bytes required')
    return registration


def validate_schedule_binding(schedule,qualification_binding,native_reader):
    """Read-only actual emitted schedule/Q consumer; never scan or dispatch."""
    from types import SimpleNamespace
    require(set(schedule)=={'plan','ledger','helper','source_manifest','qualification','case_root'}
        and schedule['qualification']==qualification_binding and schedule['case_root']==str(CASE_ROOT)
        and schedule['helper']=={'path':str(Path(__file__)),'sha256':digest(Path(__file__))},
        'Exact new provider schedule and helper required')
    for key,expected in [('plan',ROOT/'plans'/PHASE/'plan.json'),('ledger',ROOT/'state'/(PHASE+'-submission.json'))]:
        require(schedule[key]['path']==str(expected) and digest(ordinary(expected,ROOT/('plans' if key=='plan' else 'state')))==schedule[key]['sha256'],
            'Exact immutable new provider plan/ledger required')
    manifest,pins=source_manifest(Path(schedule['source_manifest']['path']),schedule['source_manifest']['sha256'])
    plan=read_json(schedule['plan']['path']);ledger=read_json(schedule['ledger']['path'])
    require(len(plan.get('tasks',[]))==64,'Exactly sixty-four future provider slots required')
    task=plan['tasks'][0];parts=shlex.split(task['command']);tail=parts[parts.index(str(Path(__file__)))+1:]
    require(len(tail)%2==0 and len(set(tail[::2]))==len(tail)//2,'Unique provider command arguments required')
    arguments={k.removeprefix('--').replace('-','_'):v for k,v in zip(tail[::2],tail[1::2])}
    args=SimpleNamespace(**arguments)
    for key in ('native_plan','native_ledger','native_script','source_manifest','kernel_qualification','registry_qualification'):
        setattr(args,key,Path(getattr(args,key)))
    require(str(args.kernel_qualification)==qualification_binding['path'] and args.kernel_qualification_sha256==qualification_binding['sha256']
        and args.kernel_qualification_job_id==qualification_binding['job_id'],'Schedule must bind exact completed new qualification')
    provider_registration(args,str(ledger['tasks'][task['name']]['job_id']),read_json(PRIMARY/'configs/study.json'))
    observed=native_reader([qualification_binding['job_id'],'6863966'],{},owner='pwa209',account='def-ptewarie_cpu')
    proof=kernel_qualification(Path(qualification_binding['path']),qualification_binding['sha256'],qualification_binding['job_id'],
        Path(schedule['source_manifest']['path']),schedule['source_manifest']['sha256'],observed)
    checked=dict(pins)
    for key in ('plan','ledger','helper','source_manifest','qualification'):checked[schedule[key]['path']]=schedule[key]['sha256']
    for task in plan['tasks']:checked[task['script']]=digest(task['script'])
    for qpath in (Path(qualification_binding['path']),Path(LEGACY_BINDING['qualification']['path'])):
        q=read_json(qpath);checked[str(qpath)]=digest(qpath)
        suites=q.get('suites',[]) if 'suites' in q else [q['suite']]
        for suite in suites:checked[suite['junit']['path']]=suite['junit']['sha256']
        if q.get('native_canary'):checked[q['native_canary']['path']]=q['native_canary']['sha256']
    require(all(digest(path)==pin for path,pin in checked.items()),'Source/qualification/schedule bytes changed during verification')
    return {'checked_paths':checked,'qualification':proof,'schedule':schedule,'numerical_work_executed':False,
        'canonical_state_changed':False,'scientific_result':False,'admission_authority':False}


def validate_predecessor_receipt(receipt,native_reader):
    provider=receipt['provider_registration'];job=provider['predecessor_job_id'];saved=receipt.get('predecessor_terminal')
    if job is None:
        require(saved is None,'First slot cannot invent a predecessor');return None
    current=predecessor_terminal(provider,native_reader([job],{},owner='pwa209',account='def-ptewarie_cpu'))
    require(isinstance(saved,dict) and saved.get('job_id')==job and saved.get('dependency_kind')=='afterany'
        and saved.get('successful_census_claimed') is False and saved.get('admission_authority') is False
        and saved.get('scientific_result') is False
        and saved.get('completion',{}).get('native_records')==current['completion']['native_records'],
        'Successful current census receipt must preserve actual terminal predecessor evidence without promoting it')
    return current



def main():
    global SUPPORT
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('native-script','native-plan','native-ledger','source-manifest','kernel-qualification'):
        p.add_argument('--'+name,type=Path,required=True)
    for name in ('native-task','source-manifest-sha256','kernel-qualification-sha256','kernel-qualification-job-id','case-id'):
        p.add_argument('--'+name,required=True)
    p.add_argument('--registry-qualification',type=Path,required=True)
    p.add_argument('--registry-qualification-sha256',required=True)
    p.add_argument('--registry-qualification-job-id',required=True)
    args=p.parse_args();own_sha=digest(Path(__file__));os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    job=str(native_job_id() or '')
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='8' and os.environ.get('HHW_GPU_COUNT')=='0'
        and os.environ.get('HHW_MAX_HOURS')=='6','Owned finite8CPU6h native context required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval required')
    import horsehair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,
        'Exact qualified4f framework and unchanged primary required')
    config=read_json(PRIMARY/'configs/study.json')
    require(canonical_hash(config)==CONFIG and read_json(FRAMEWORK/'configs/study.json')==config,'Full frozen config required')
    manifest,pins=source_manifest(args.source_manifest,args.source_manifest_sha256)
    require(Path(__file__)==SOURCE_SUPPORT/WRAPPER_NAME and own_sha==pins[str(Path(__file__))],
        'Wrapper must execute its own frozen qualified source bytes')
    for path,sha in ((GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(QUOTA,QUOTA_SHA),(QUALIFICATION,QUALIFICATION_SHA)):
        ordinary(path,ROOT/'maintenance');require(digest(path)==sha,'Pinned context source changed')
    SUPPORT=create_case(args.case_id,job)
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    runpy.run_path(str(GUARD))['install'](SUPPORT/'accounting-reads')
    allocation=native_allocation(job,args.native_script,runpy.run_path(str(SCHEDULER))['run_scontrol_job'],
        plan_path=args.native_plan,ledger_path=args.native_ledger,task_name=args.native_task,argv=sys.argv[1:])
    provider=provider_registration(args,job,config)
    end=allocation['allocation_end_unix']
    def expired(signum,frame):raise TimeoutError('Finite census deadline reached; original timestamps preserved')
    require(hasattr(signal,'SIGALRM'),'Native POSIX hard deadline required')
    signal.signal(signal.SIGALRM,expired);signal.alarm(max(1,int(end-time.time()-180)))
    registry=registry_qualification(args.registry_qualification,args.registry_qualification_sha256,args.registry_qualification_job_id)
    extension=registry_read_extension()
    from horsehair import repair
    observed=repair._repair_native_states(sorted({str(read_json(QUALIFICATION)['slurm_job_id']),registry['job_id'],
        args.kernel_qualification_job_id,'6863966',*([provider['predecessor_job_id']] if provider['predecessor_job_id'] else [])}),{},owner='pwa209',account='def-ptewarie_cpu')
    public_q=qualification_proof(QUALIFICATION,QUALIFICATION_SHA,str(read_json(QUALIFICATION)['slurm_job_id']),observed)
    private_q=completed_registry_qualification(registry,observed)
    registry_api=runpy.run_path(str(EXTENDED_REGISTRY_GUARD))
    installed=registry_api['install'](SUPPORT/'registered-snapshot-reads',
        registration_mode='controller-already-locked',allocation_deadline_unix=end)
    from horsehair import campaign,repair,budget
    require(campaign._repair_native_states is repair._repair_native_states,'358 must precede campaign reader binding')
    admission=brief_admission(lambda:campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],
        SUPPORT,'fresh-census-baseline',resource_policy=policy,cpus=0,gpus=0,hours=6),end,
        registration_lock=registry_api['registration_lock'],evidence=SUPPORT/'baseline-registration-lock')
    streaming_q=kernel_qualification(args.kernel_qualification,args.kernel_qualification_sha256,args.kernel_qualification_job_id,
        args.source_manifest,args.source_manifest_sha256,observed)
    predecessor=predecessor_terminal(provider,observed)
    exclusive(SUPPORT/'native-bindings.json',{'allocation':allocation,'public_qualification':public_q,
        'legacy_registry_qualification':private_q,'streaming_qualification':streaming_q,
        'registry_read_extension':extension,'registry_installation':installed,'admission':admission,
        'wrapper_sha256':own_sha,'source_sha256':SOURCE,'primary_source_sha256':PRIMARY_SHA,
        'full_config_sha256':CONFIG,'resource_policy':policy.receipt(),**{k:False for k in FALSE_FLAGS}})
    with resource_lease(ROOT,config,cpus=8,gpus=0,max_hours=6,job_id=job,resource_policy=policy):
        quota_api=runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait']
        quota_before=quota_api(ROOT,required_files=2000,required_bytes=1_000_000_000,reserve_files=248000,
            reserve_bytes=2_000_000_000_000,evidence_dir=SUPPORT/'quota-before',allocation_deadline_unix=end)
        kernel=runpy.run_path(str(LEGACY_SUPPORT/KERNEL_NAME),run_name='qualified_private_streaming_census')
        require(digest(SOURCE_SUPPORT/KERNEL_NAME)==pins[str(LEGACY_SUPPORT/KERNEL_NAME)],'Qualified private kernel changed')
        result,adapter,latest=measured_sweep(budget.storage_snapshot,kernel,ROOT,SUPPORT,end,job)
        exclusive(SUPPORT/'private-adapter.json',adapter|{'last_progress':latest,'inventory_complete_only_after_normal_exhaustion':True})
        freshness_before_quota=fresh_original_clocks(result,time.time())
        quota_after=quota_api(ROOT,required_files=2000,required_bytes=1_000_000_000,reserve_files=248000,
            reserve_bytes=2_000_000_000_000,evidence_dir=SUPPORT/'quota-after',allocation_deadline_unix=end-180)
        freshness_after_quota=final_snapshot_check(result,ROOT,SUPPORT)
        require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA
            and digest(POLICY)==POLICY_SHA and digest(Path(__file__))==own_sha
            and source_manifest(args.source_manifest,args.source_manifest_sha256)==(manifest,pins),
            'Qualified sources/policy/wrapper changed during sweep')
        freshness_at_receipt=final_snapshot_check(result,ROOT,SUPPORT)
        exclusive(SUPPORT/'receipt.json',{'status':result['status'],'native_job_id':job,'allocation':allocation,
            'case_id':args.case_id,'provider_registration':provider,'predecessor_terminal':predecessor,'legacy_binding':LEGACY_BINDING,
            'provider_helper':{'path':str(Path(__file__)),'sha256':own_sha},
            'wrapper_sha256':own_sha,
            'source_manifest':{'path':str(args.source_manifest),'sha256':args.source_manifest_sha256},
            'qualification':{'path':str(args.kernel_qualification),'sha256':args.kernel_qualification_sha256,
                'job_id':args.kernel_qualification_job_id},
            'sweep':{'path':str(SUPPORT/'sweep-result.json'),'sha256':digest(SUPPORT/'sweep-result.json')},
            'private_adapter':{'path':str(SUPPORT/'private-adapter.json'),'sha256':digest(SUPPORT/'private-adapter.json')},
            'progress':{'path':str(SUPPORT/'sweep-progress.jsonl'),'sha256':digest(SUPPORT/'sweep-progress.jsonl')},
            'streaming_qualification':streaming_q,'quota_before':quota_before,'quota_after':quota_after,
            'freshness_before_final_quota':freshness_before_quota,'freshness_after_final_quota':freshness_after_quota,
            'freshness_at_receipt':freshness_at_receipt,'source_sha256':SOURCE,'config_sha256':CONFIG,
            'resource_policy':policy.receipt(),**{k:False for k in FALSE_FLAGS}})
    signal.alarm(0)
    print(json.dumps({'status':result['status'],'study_files':result['study_files'],'study_bytes':result['study_bytes']}))


if __name__=='__main__':main()
