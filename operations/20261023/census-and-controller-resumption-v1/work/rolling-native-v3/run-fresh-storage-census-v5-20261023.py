"""Qualified private streaming census; original byte proofs and quota clocks."""
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

SUPPORT=ROOT/'maintenance/study-af-fresh-storage-census-v5-20261023'
SOURCE_SUPPORT=ROOT/'maintenance/study-af-streaming-census-support-v1-20261023'
KERNEL_Q=ROOT/'maintenance/study-af-streaming-census-qualification-v1-20261023/qualification.json'
KERNEL_NAME='streaming-storage-census-v1-20261023.py'
QUALIFIER_NAME='qualify-streaming-census-v1-20261023.py'
WRAPPER_NAME='run-fresh-storage-census-v5-20261023.py'
PRIVATE_FILES={WRAPPER_NAME,'test_run_fresh_storage_census_v5_20261023.py',KERNEL_NAME,
    'test_streaming_storage_census_v1_20261023.py',QUALIFIER_NAME,'test_qualify_streaming_census_v1_20261023.py',
    'registered-snapshot-read-guard-v4-20261022.py','test_registered_snapshot_read_guard_v4_20261022.py'}
BASELINE_NAMES=('budget','storage_artifacts','storage_artifact_cache','storage_boundary_cache',
    'storage_snapshot_artifacts','storage_snapshot_boundary')
SUITE_SOURCES={**{'baseline_'+name:'tests/test_'+name+'.py' for name in BASELINE_NAMES},
    'kernel':'test_streaming_storage_census_v1_20261023.py','wrapper':'test_run_fresh_storage_census_v5_20261023.py',
    'registry':'test_registered_snapshot_read_guard_v4_20261022.py','qualifier':'test_qualify_streaming_census_v1_20261023.py'}
PRESERVED_ARTIFACT_FUNCTIONS=('_require_linux','_relative_parts','_length_arguments','_open_directory',
    '_metadata','_directory_identity','_stable_regular_length','measure_regular_length','measure_regular_eof',
    'validate_regular_length_measurement','_regular_identity','_boundary_length','artifact_read_cap',
    '_statx_function','_open_leaf','_parallel_lengths')
FRESHNESS_SECONDS=21600
FINALIZATION_SECONDS=900

EXTENDED_REGISTRY_GUARD=Path(__file__).with_name('registered-snapshot-read-guard-v4-20261022.py')
EXTENDED_REGISTRY_SHA='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'

FALSE_FLAGS=('scientific_result','admission_authority','scientific_gates_cleared','freshness_timestamp_rewritten','end_time_extends_admission_freshness')

def require(value,message):
    if not value:raise ValueError(message)

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
    """Verify the exact one-constant operational extension of the old guard.

    Native guard30 contracts precede this helper. The old full100 receipt is
    retained as the baseline lineage, not presented as testing the new bytes.
    """
    require(digest(REGISTRY_GUARD)==REGISTRY_GUARD_SHA and digest(EXTENDED_REGISTRY_GUARD)==EXTENDED_REGISTRY_SHA,'Registry baseline/extension source bytes changed')
    def stripped(path):
        raw=path.read_bytes();require(len(raw)<=100000,'Bounded registry source required');tree=ast.parse(raw)
        nodes=[n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='MAX_SECONDS' for t in n.targets)]
        require(len(nodes)==1,'One explicit total registry read bound required');value=ast.literal_eval(nodes[0].value);tree.body.remove(nodes[0]);return ast.dump(tree,include_attributes=False),value
    old,old_bound=stripped(REGISTRY_GUARD);new,new_bound=stripped(EXTENDED_REGISTRY_GUARD)
    require(old==new and (old_bound,new_bound)==(180,900),'Only the reviewed finite read duration may differ from the qualified baseline')
    return {'baseline_guard':{'path':str(REGISTRY_GUARD),'sha256':REGISTRY_GUARD_SHA},
        'extended_guard':{'path':str(EXTENDED_REGISTRY_GUARD),'sha256':EXTENDED_REGISTRY_SHA},
        'only_module_ast_difference':'MAX_SECONDS180to900',
        'native_contracts':'registry_suite_in_separately_completed_streaming_qualification',
        'scientific_controller_qualification_authorized':False}


def source_manifest(path,sha):
    path=Path(path)
    require(path==SOURCE_SUPPORT/'source-manifest.json' and re.fullmatch('[0-9a-f]{64}',str(sha)),
        'Exact explicit private source manifest locator required')
    ordinary(path,ROOT/'maintenance');require(path.stat().st_size<=100_000 and digest(path)==sha,'Frozen manifest bytes changed')
    value=read_json(path)
    require(set(value)=={'kind','files','suites'} and value['kind']=='streaming-census-operational-source-manifest-v1'
        and set(value['files'])==PRIVATE_FILES and set(value['suites'])==set(SUITE_SOURCES),'Exact complete operational source closure required')
    pins={}
    for name,expected in value['files'].items():
        target=ordinary(SOURCE_SUPPORT/name,ROOT/'maintenance')
        require(re.fullmatch('[0-9a-f]{64}',str(expected)) and digest(target)==expected,'Frozen private source changed')
        pins[str(target)]=expected
    for scope,name in SUITE_SOURCES.items():
        suite=value['suites'][scope];target=FRAMEWORK/name if scope.startswith('baseline_') else SOURCE_SUPPORT/name
        ordinary(target,FRAMEWORK if scope.startswith('baseline_') else ROOT/'maintenance')
        require(set(suite)=={'source','source_sha256','tests','roster_sha256'} and suite['source']==name
            and type(suite['tests']) is int and suite['tests']>0
            and re.fullmatch('[0-9a-f]{64}',str(suite['roster_sha256'])) and digest(target)==suite['source_sha256']
            and (scope.startswith('baseline_') or value['files'][name]==suite['source_sha256']),
            'Exact positive source/count/roster manifest suite required')
        pins[str(target)]=suite['source_sha256']
    for name in ('tests/conftest.py','src/horsehair/budget.py','src/horsehair/storage_artifacts.py'):
        target=ordinary(FRAMEWORK/name,FRAMEWORK);pins[str(target)]=digest(target)
    require(digest(path)==sha,'Manifest changed during source closure verification')
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
    require(Path(path)==KERNEL_Q and re.fullmatch('[0-9a-f]{64}',str(sha))
        and re.fullmatch('[1-9][0-9]*',str(job)),'Exact completed streaming qualification locator required')
    ordinary(path,ROOT/'maintenance');require(Path(path).stat().st_size<=1_000_000 and digest(path)==sha,'Streaming qualification bytes changed')
    manifest,pins=source_manifest(manifest_path,manifest_sha);q=read_json(path)
    require(q.get('status')=='complete_streaming_census_operational_qualification'
        and type(q.get('returncode')) is int and q['returncode']==0 and q.get('slurm_job_id')==job
        and q.get('source_sha256')==SOURCE and q.get('config_sha256')==CONFIG
        and q.get('source_verified_unchanged') is True and q.get('source_pins')==pins
        and q.get('source_manifest')=={'path':str(manifest_path),'sha256':manifest_sha}
        and q.get('qualification_helper')=={'path':str(SOURCE_SUPPORT/QUALIFIER_NAME),'sha256':pins[str(SOURCE_SUPPORT/QUALIFIER_NAME)]}
        and all(q.get(flag) is False for flag in FALSE_FLAGS),'Positive exact operational qualification source/false flags required')
    scopes=q.get('suites',[])
    require(type(scopes) is list and len(scopes)==len(SUITE_SOURCES)
        and len({s.get('scope') for s in scopes})==len(scopes),'One complete suite receipt per frozen scope required')
    for suite in scopes:
        scope=suite.get('scope');require(scope in SUITE_SOURCES,'Unknown qualification suite scope')
        expected=manifest['suites'][scope];target=FRAMEWORK/expected['source'] if scope.startswith('baseline_') else SOURCE_SUPPORT/expected['source']
        require(suite.get('source')=={'path':str(target),'sha256':expected['source_sha256']}
            and suite.get('test_counts')=={'tests':expected['tests'],'failures':0,'errors':0,'skipped':0}
            and all(type(n) is int for n in suite['test_counts'].values())
            and suite.get('test_roster_sha256')==expected['roster_sha256'],'Exact frozen native suite contracts required')
        artifact=suite.get('junit',{});require(set(artifact)=={'path','sha256'},'Exact native JUnit locator required')
        ordinary(artifact['path'],KERNEL_Q.parent);require(digest(artifact['path'])==artifact['sha256'],'Actual native JUnit bytes changed')
        junit_roster(artifact['path'],expected['tests'],expected['roster_sha256'],artifact['sha256'])
    require(q.get('baseline_iterator_policy')=='original_canonical_iterator'
        and q.get('private_iterator_policy')=='scoped_private_kernel_only'
        and q.get('primitive_identity')==primitive_contract(primitive_identity())
        and q.get('native_completion_required_separately') is True,'Original primitive source/function contract required')
    canary=q.get('native_canary',{})
    require(set(canary)=={'path','sha256'} and Path(canary['path'])==KERNEL_Q.parent/'native-canary.json',
        'Exact native local syscall canary locator required')
    ordinary(canary['path'],KERNEL_Q.parent)
    with Path(canary['path']).open('rb') as stream:raw=stream.read(4_000_001)
    require(len(raw)<=4_000_000 and hashlib.sha256(raw).hexdigest()==canary['sha256'],'Native canary bytes changed')
    value=json.loads(raw);identities=value.get('primitive_identity',{})
    require(value.get('status')=='complete_native_local_syscall_canary' and value.get('owner_uid')==3164768
        and type(value['owner_uid']) is int and value.get('workers')==64 and value.get('pending_limit')==128
        and all(value.get(key) is True for key in ('serial_equals_streaming','symlink_not_followed','special_not_opened',
            'original_iterator_restored','original_measurement_functions_preserved','fixture_quota_simulated'))
        and value.get('fixture_quota_is_admission_authority') is False and all(value.get(flag) is False for flag in FALSE_FLAGS)
        and identities.get('restored') is True and identities.get('before')==identities.get('after')
        and primitive_contract(identities['before'])==value.get('primitive_contract')==q['primitive_identity'],
        'Positive native canary and restored original primitive identities required')
    registration=q.get('native_registration',{})
    actual,tail=registered_task(registration['plan']['path'],registration['ledger']['path'],registration['task_name'],
        registration['script']['path'],job,hours=1,helper=SOURCE_SUPPORT/QUALIFIER_NAME)
    require(actual==registration,'Qualification registered source/script bytes changed')
    task=next(t for t in read_json(registration['plan']['path'])['tasks'] if t['name']==registration['task_name'])
    require(task.get('streaming_census_qualification')=={'helper':q['qualification_helper'],'source_manifest':q['source_manifest']},
        'Qualification registered task must bind exact helper and source manifest intent')
    expected_args={'--source-manifest':str(manifest_path),'--source-manifest-sha256':manifest_sha,
        '--native-plan':registration['plan']['path'],'--native-ledger':registration['ledger']['path'],
        '--native-script':registration['script']['path'],'--native-task':registration['task_name']}
    require(len(tail)==2*len(expected_args) and len(set(tail[::2]))==len(expected_args)
        and dict(zip(tail[::2],tail[1::2]))==expected_args,
        'Qualification registration must bind every exact actual invocation argument')
    record=observed.get(job,{})
    require(record.get('state')=='COMPLETED' and record.get('exit_code')=='0:0' and record.get('user')=='pwa209'
        and record.get('account')=='def-ptewarie_cpu' and record.get('job_id_raw')==job
        and record.get('unallocated_metadata') is False and type(record.get('cpus')) is int and record['cpus']==8
        and type(record.get('elapsed_seconds')) is int and 0<record['elapsed_seconds']<=3600
        and 'gpu' not in record.get('alloc_tres','').lower(),'Positive owned scalar8CPU streaming qualification native completion required')
    cpu_only_tres(record.get('alloc_tres',''))
    proof=completed_native_dependency(ROOT,job);require(proof is not None,'Streaming qualifier final native accounting is pending')
    require(source_manifest(manifest_path,manifest_sha)==(manifest,pins) and digest(path)==sha,
        'Qualification or complete source closure changed during verification')
    return {'path':str(path),'sha256':sha,'job_id':job,'native_observed':record,'completion':proof,
        'source_manifest':{'path':str(manifest_path),'sha256':manifest_sha},'source_pins':pins}


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


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('native-script','native-plan','native-ledger','source-manifest','kernel-qualification'):
        p.add_argument('--'+name,type=Path,required=True)
    for name in ('native-task','source-manifest-sha256','kernel-qualification-sha256','kernel-qualification-job-id'):
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
    ordinary(SUPPORT,ROOT/'maintenance',fresh=True);SUPPORT.mkdir(mode=0o700)
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    runpy.run_path(str(GUARD))['install'](SUPPORT/'accounting-reads')
    allocation=native_allocation(job,args.native_script,runpy.run_path(str(SCHEDULER))['run_scontrol_job'],
        plan_path=args.native_plan,ledger_path=args.native_ledger,task_name=args.native_task,argv=sys.argv[1:])
    end=allocation['allocation_end_unix']
    def expired(signum,frame):raise TimeoutError('Finite census deadline reached; original timestamps preserved')
    require(hasattr(signal,'SIGALRM'),'Native POSIX hard deadline required')
    signal.signal(signal.SIGALRM,expired);signal.alarm(max(1,int(end-time.time()-180)))
    registry=registry_qualification(args.registry_qualification,args.registry_qualification_sha256,args.registry_qualification_job_id)
    extension=registry_read_extension()
    from horsehair import repair
    observed=repair._repair_native_states(sorted({str(read_json(QUALIFICATION)['slurm_job_id']),registry['job_id'],
        args.kernel_qualification_job_id}),{},owner='pwa209',account='def-ptewarie_cpu')
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
    exclusive(SUPPORT/'native-bindings.json',{'allocation':allocation,'public_qualification':public_q,
        'legacy_registry_qualification':private_q,'streaming_qualification':streaming_q,
        'registry_read_extension':extension,'registry_installation':installed,'admission':admission,
        'wrapper_sha256':own_sha,'source_sha256':SOURCE,'primary_source_sha256':PRIMARY_SHA,
        'full_config_sha256':CONFIG,'resource_policy':policy.receipt(),**{k:False for k in FALSE_FLAGS}})
    with resource_lease(ROOT,config,cpus=8,gpus=0,max_hours=6,job_id=job,resource_policy=policy):
        quota_api=runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait']
        quota_before=quota_api(ROOT,required_files=2000,required_bytes=1_000_000_000,reserve_files=248000,
            reserve_bytes=2_000_000_000_000,evidence_dir=SUPPORT/'quota-before',allocation_deadline_unix=end)
        kernel=runpy.run_path(str(SOURCE_SUPPORT/KERNEL_NAME),run_name='qualified_private_streaming_census')
        require(digest(SOURCE_SUPPORT/KERNEL_NAME)==pins[str(SOURCE_SUPPORT/KERNEL_NAME)],'Qualified private kernel changed')
        local_progress=progress_path(job)
        with local_progress.open('xb') as stream:
            progress,latest=progress_recorder(stream)
            with operational_adapter(kernel,deadline=time.monotonic()+max(0.,end-time.time()-FINALIZATION_SECONDS),progress=progress) as adapter:
                result=run_snapshot(budget.storage_snapshot,ROOT,SUPPORT,end,adapter=adapter)
        with local_progress.open('rb') as source,(SUPPORT/'sweep-progress.jsonl').open('xb') as target:
            while block:=source.read(1_000_000):target.write(block)
            target.flush();os.fsync(target.fileno())
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
