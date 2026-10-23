"""Source-bound native qualification of a private streaming census scheduler.

This repair qualifier deliberately does not require the census it repairs.
All test fixtures and its real-syscall canary live in SLURM_TMPDIR. Only finite
qualification evidence is saved in the study. A positive receipt still needs a
separate positive final native completion before a consumer can use it.
"""
from pathlib import Path
from datetime import datetime,timezone
import argparse
import hashlib
import json
import math
import os
import re
import runpy
import shlex
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

from horsehair.core import canonical_hash,digest,read_json,source_identity,native_job_id
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
SUPPORT=ROOT/'maintenance/study-af-rolling-census-support-v4-20261023'
CASE=ROOT/'maintenance/study-af-rolling-census-qualification-v5-20261023'
POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
NATIVE_GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
NATIVE_GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=NATIVE_GUARD.with_name('native-scheduler-read.py')
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
QUOTA=ROOT/'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'
QUOTA_SHA='5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045'
DRIVER=ROOT/'maintenance/sixth-repair-support-20261016/controller-primary-repair-sixth-20261016.py'
DRIVER_SHA='49815eeb509909d5eac2ebde2e4f4a3918cf40cefdc5f643f2bcaffead629eb1'
BASELINE=ROOT/'maintenance/study-af-fresh-storage-census-stage-v4-20261022/run-fresh-storage-census-v4-20261022.py'
BASELINE_SHA='05627118529a153e38eb45f2cf778fb4fbc09289bb890cb7962cdb8c2b137c18'
GUARD_NAME='registered-snapshot-read-guard-v4-20261022.py'
GUARD_SHA='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
KERNEL='streaming-storage-census-v1-20261023.py'
WRAPPER='run-fresh-storage-census-v6-20261023.py'
HELPER='qualify-streaming-census-v2-20261023.py'
BASE_WRAPPER='run-fresh-storage-census-v5-20261023.py'
BASE_WRAPPER_SHA='b448607b00512ef865a3369da56c18bf6f98ab589b6a524795e5bbed9c64e569'
BASE_CONTROLLER=ROOT/'maintenance/study-af-registry-repair-support-v6-20261022/run-af-agent-controller-registry-v6-20261022.py'
BASE_CONTROLLER_SHA='e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
OUTER='run-af-agent-controller-census-policy-v1-20261023.py'
PRIVATE_FILES={KERNEL,WRAPPER,HELPER,GUARD_NAME,BASE_WRAPPER,OUTER,
    'qualify-streaming-census-v1-20261023.py',
    'test_streaming_storage_census_v1_20261023.py','test_run_fresh_storage_census_v5_20261023.py',
    'test_qualify_streaming_census_v1_20261023.py','test_registered_snapshot_read_guard_v4_20261022.py',
    'test_run_fresh_storage_census_v6_20261023.py','test_qualify_streaming_census_v2_20261023.py',
    'test_af_agent_controller_census_policy_v1_20261023.py'}
PUBLIC_NAMES=('budget','storage_artifacts','storage_artifact_cache','storage_boundary_cache',
    'storage_snapshot_artifacts','storage_snapshot_boundary')
SUITE_SOURCES={**{'baseline_'+n:'tests/test_'+n+'.py' for n in PUBLIC_NAMES},
    'kernel':'test_streaming_storage_census_v1_20261023.py',
    'wrapper':'test_run_fresh_storage_census_v5_20261023.py',
    'registry':'test_registered_snapshot_read_guard_v4_20261022.py',
    'qualifier':'test_qualify_streaming_census_v1_20261023.py',
    'repeatable_wrapper':'test_run_fresh_storage_census_v6_20261023.py',
    'rolling_qualifier':'test_qualify_streaming_census_v2_20261023.py',
    'external_census_policy':'test_af_agent_controller_census_policy_v1_20261023.py'}
MANIFEST_KIND='streaming-census-operational-source-manifest-v2'
FALSE_FLAGS={'scientific_result':False,'admission_authority':False,
    'scientific_gates_cleared':False,'freshness_timestamp_rewritten':False,
    'end_time_extends_admission_freshness':False}


def require(value,message):
    if not value:raise ValueError(message)


def ordinary(path,namespace,*,exists=True):
    p=Path(path)
    require(p.is_absolute() and p==p.resolve() and p!=namespace and p.is_relative_to(namespace)
        and all(not q.is_symlink() for q in [p,*p.parents] if q.is_relative_to(namespace)),
        'Exact ordinary confined path required')
    if exists:require(p.is_file(),'Required immutable file missing')
    return p


def exclusive(path,value):
    raw=(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    require(len(raw)<=4_000_000,'Bounded qualification evidence required')
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def strict_json(raw):
    def pairs(items):
        result={}
        for key,value in items:
            require(key not in result,'Duplicate JSON key refused');result[key]=value
        return result
    def constant(value):raise ValueError('Nonfinite JSON value refused')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=constant)


def source_manifest(path,expected_sha):
    require(Path(path)==SUPPORT/'source-manifest.json' and re.fullmatch('[0-9a-f]{64}',expected_sha or ''),
        'Exact source manifest locator/SHA required')
    raw=ordinary(path,ROOT/'maintenance').read_bytes()
    require(len(raw)<=100_000 and hashlib.sha256(raw).hexdigest()==expected_sha,'Frozen source manifest bytes changed')
    value=strict_json(raw)
    require(set(value)=={'kind','files','suites'} and value['kind']==MANIFEST_KIND
        and isinstance(value['files'],dict) and set(value['files'])==PRIVATE_FILES
        and isinstance(value['suites'],dict) and set(value['suites'])==set(SUITE_SOURCES),
        'Complete exact streaming source/suite manifest required')
    pins={}
    for name,h in value['files'].items():
        require(isinstance(h,str) and re.fullmatch('[0-9a-f]{64}',h),'Frozen private byte SHA required')
        p=ordinary(SUPPORT/name,ROOT/'maintenance');require(digest(p)==h,'Frozen private source changed: '+name)
        pins[str(p)]=h
    require(value['files'][GUARD_NAME]==GUARD_SHA and Path(__file__)==SUPPORT/HELPER
        and digest(Path(__file__))==value['files'][HELPER],'Exact original900 guard and own helper bytes required')
    for scope,relative in SUITE_SOURCES.items():
        contract=value['suites'][scope]
        require(isinstance(contract,dict) and set(contract)=={'source','source_sha256','tests','roster_sha256'}
            and contract['source']==relative and type(contract['tests']) is int and contract['tests']>0
            and all(isinstance(contract[k],str) and re.fullmatch('[0-9a-f]{64}',contract[k])
                for k in ('source_sha256','roster_sha256')),'Exact positive source-bound suite contract required')
        p=FRAMEWORK/relative if scope.startswith('baseline_') else SUPPORT/relative
        ordinary(p,FRAMEWORK if scope.startswith('baseline_') else ROOT/'maintenance')
        require(digest(p)==contract['source_sha256'],'Suite source bytes changed: '+scope)
        require(scope.startswith('baseline_') or contract['source_sha256']==value['files'][relative],
            'Private suite must bind the frozen test source')
        pins[str(p)]=contract['source_sha256']
    for relative in ('tests/conftest.py','src/horsehair/budget.py','src/horsehair/storage_artifacts.py'):
        p=ordinary(FRAMEWORK/relative,FRAMEWORK);pins[str(p)]=digest(p)
    require(len(pins)==23,'Exact complete23 source/test pins required')
    require(digest(path)==expected_sha,'Source manifest changed during verification')
    return value,pins


def verify_sources(pins):
    for path,h in pins.items():require(digest(Path(path))==h,'Source changed during qualification: '+path)
    require(digest(BASE_CONTROLLER)==BASE_CONTROLLER_SHA and digest(SUPPORT/BASE_WRAPPER)==BASE_WRAPPER_SHA,
        'Original base controller/v5 wrapper lineage changed')
    require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,
        'Frozen framework/primary source changed')


def baseline_library():
    for path,h in ((BASELINE,BASELINE_SHA),(NATIVE_GUARD,NATIVE_GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),
                   (QUOTA,QUOTA_SHA),(DRIVER,DRIVER_SHA),(POLICY,POLICY_SHA)):
        require(digest(ordinary(path,ROOT/'maintenance'))==h,'Pinned original operational lineage changed')
    return runpy.run_path(str(BASELINE),run_name='streaming_q_original_lineage_only')


def registered_task(plan_path,ledger_path,task_name,script,job,config,argv,manifest_path,manifest_sha):
    plan_path=ordinary(plan_path,ROOT/'plans');ledger_path=ordinary(ledger_path,ROOT/'state');script=ordinary(script,ROOT/'plans')
    namespace=runpy.run_path(str(DRIVER),run_name='streaming_q_registered_binding')
    plan,ledger=namespace['checked_pair'](plan_path,ledger_path,ROOT,config)
    tasks=[t for t in plan['tasks'] if t['name']==task_name];require(len(tasks)==1,'Unique actual registered qualifier task required')
    task=tasks[0];record=ledger.get('tasks',{}).get(task_name,{})
    command=shlex.split(task['command']);own=str(Path(__file__))
    intent={'helper':{'path':own,'sha256':digest(Path(__file__))},
        'source_manifest':{'path':str(manifest_path),'sha256':manifest_sha}}
    require(command.count(own)==1 and command[command.index(own)+1:]==list(argv)
        and task.get('rolling_census_qualification')==intent
        and plan.get('release_identity',{}).get('sha256')==SOURCE and plan.get('config_sha256')==CONFIG
        and task.get('script')==str(script) and task['command'] in script.read_text()
        and canonical_hash(script.read_text())==task.get('script_sha256')==record.get('script_sha256')
        and record.get('job_id')==job and record.get('state')=='submitted'
        and task.get('resources')=={'cpus':8,'gpu':False,'hours':1,'memory':'64G','array':None},
        'Exact source/helper/argv/manifest/plan/ledger/script8CPU64G1h binding required')
    return {'plan':{'path':str(plan_path),'sha256':digest(plan_path)},'ledger':{'path':str(ledger_path),'sha256':digest(ledger_path)},
        'script':{'path':str(script),'sha256':digest(script)},'task_name':task_name,'native_job_id':job}


def cpu_only_tres(text):
    pairs=[item.partition('=') for item in text.split(',')];value={key:item for key,sep,item in pairs}
    require(len(value)==len(pairs) and all(sep and key and item for key,sep,item in pairs)
        and value.get('cpu')=='8' and value.get('node')=='1' and value.get('mem') in {'64G','65536M'}
        and not any('gpu' in key.lower() for key in value),'Exact actual8CPU64G single-node TRES required')


def native_allocation(job,script,task_name,reader,*,wall=time.time):
    response=reader(job,CASE/'own-scheduler-reads');response.check_returncode()
    lines=[x for x in response.stdout.splitlines() if x.strip()];require(len(lines)==1,'One actual native allocation required')
    pairs=re.findall(r'(?<!\S)(\w+)=(.*?)(?=\s+\w+=|$)',lines[0]);fields={k:v.strip() for k,v in pairs}
    require(len(pairs)==len(fields) and fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
        and fields.get('Account')=='def-ptewarie_cpu' and fields.get('JobState')=='RUNNING'
        and fields.get('NumCPUs')=='8' and fields.get('NumNodes') in {'1','1-1'}
        and fields.get('TimeLimit') in {'01:00:00','1:00:00','0-01:00:00'}
        and fields.get('MinMemoryNode') in {'64G','65536M'} and fields.get('Command')==str(script)
        and fields.get('JobName')=='hhw-'+task_name and 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields,
        'Exact owned scalar8CPU64G1h native allocation/script required')
    require(all('gpu' not in fields.get(k,'').lower() for k in
        ('TresPerNode','TresPerJob','TresPerTask','ReqTRES','AllocTRES','Gres')),'No GPU qualification allocation permitted')
    cpu_only_tres(fields.get('ReqTRES',''));cpu_only_tres(fields.get('AllocTRES',''))
    end=datetime.fromisoformat(fields.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp()
    env=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(env) and abs(env-end)<=2 and wall()+600<end,'Verified finite native allocation end/margin required')
    return {'fields':fields,'allocation_end_unix':end},end


def junit_roster(path,count,expected_sha):
    raw=Path(path).read_bytes()
    require(len(raw)<=4_000_000 and b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Bounded ordinary JUnit required')
    cases=list(ET.fromstring(raw).iter('testcase'))
    roster=sorted(c.get('classname','').split('.')[-1]+'::'+c.get('name','') for c in cases)
    require(len(roster)==len(set(roster))==count and all(c.get('classname') and c.get('name') for c in cases)
        and canonical_hash(roster)==expected_sha
        and all(not any(c.find(k) is not None for k in ('failure','error','skipped')) for c in cases),
        'Exact complete passing testcase roster required')
    from horsehair.cli import _qualification_junit
    require(_qualification_junit(path)=={'tests':count,'failures':0,'errors':0,'skipped':0},'Actual JUnit summary counts differ')
    return {'path':str(path),'sha256':digest(path)}


def pytest_environment():
    env={**os.environ,'TZ':'UTC','PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1',
        'PYTHONPATH':os.pathsep.join(dict.fromkeys([str(FRAMEWORK/'src'),
            *[entry for entry in os.environ.get('PYTHONPATH','').split(os.pathsep) if entry]]))}
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):env.pop(key,None)
    return env


def dependency_probe(*,runner=subprocess.run):
    """Verify child imports under the same preserved native module environment."""
    code=('import json,sys,horsehair,pytest,pygments; '
        'print(json.dumps({"executable":sys.executable,"sys_path":sys.path,'
        '"modules":{n:str(m.__file__) for n,m in '
        '[("horsehair",horsehair),("pytest",pytest),("pygments",pygments)]}},sort_keys=True))')
    result=runner([sys.executable,'-s','-c',code],cwd=FRAMEWORK,capture_output=True,text=True,
        timeout=30,env=pytest_environment(),check=False)
    require(result.returncode==0 and len(result.stdout)<=100_000 and not result.stderr,
        'Native child pytest/pygments dependency imports must succeed without source mixing')
    proof=strict_json(result.stdout)
    require(set(proof)=={'executable','sys_path','modules'} and set(proof['modules'])=={'horsehair','pytest','pygments'}
        and Path(proof['modules']['horsehair']).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and isinstance(proof['sys_path'],list) and all(isinstance(p,str) for p in proof['sys_path']),
        'Child imports must use the exact qualified horsehair package')
    proof['module_source_sha256']={name:digest(Path(path)) for name,path in proof['modules'].items()}
    proof['inherited_dependency_paths_preserved']=True
    return proof


def run_suite(scope,contract,workspace,end,*,runner=subprocess.run,wall=time.time):
    source=FRAMEWORK/contract['source'] if scope.startswith('baseline_') else SUPPORT/contract['source']
    require(digest(source)==contract['source_sha256'],'Suite bytes changed before actual execution')
    xml=CASE/(scope+'.xml');timeout=min(2400,end-wall()-180)
    require(timeout>60,'Insufficient finite native test finalization margin')
    command=[sys.executable,'-s','-m','pytest',str(source),'-q','-p','no:cacheprovider',
        '--basetemp',str(workspace/scope),'--junitxml',str(xml)]
    exclusive(CASE/(scope+'-intent.json'),{'command':command,'source_sha256':contract['source_sha256'],
        'baseline_iterator_unadapted':scope.startswith('baseline_'),'private_adapter_scoped_by_test':not scope.startswith('baseline_'),**FALSE_FLAGS})
    with (CASE/(scope+'-stdout.txt')).open('xb') as out,(CASE/(scope+'-stderr.txt')).open('xb') as err:
        result=runner(command,cwd=FRAMEWORK,stdout=out,stderr=err,timeout=timeout,env=pytest_environment(),check=False)
    require(result.returncode==0,'Actual native suite failed: '+scope)
    return {'scope':scope,'source':{'path':str(source),'sha256':contract['source_sha256']},
        'junit':junit_roster(xml,contract['tests'],contract['roster_sha256']),
        'test_counts':{'tests':contract['tests'],'failures':0,'errors':0,'skipped':0},'test_roster_sha256':contract['roster_sha256']}


def local_workspace(job):
    temp=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temp.is_absolute() and temp.is_dir() and temp==temp.resolve() and not temp.is_symlink()
        and temp.stat().st_uid==3164768 and shutil.disk_usage(temp).free>=3_000_000_000
        and os.statvfs(temp).f_favail>=50000,'Owned allocation-local fixture capacity required')
    workspace=temp/('hhw-streaming-census-q-'+job);workspace.mkdir(mode=0o700,exist_ok=False);return workspace


def validate_canary_rows(serial,streamed,tree,sizes,artifacts):
    require(serial==streamed,'Native serial/streaming exact measurement transcripts differ')
    require(set(streamed)=={str(tree/name) for name in sizes}|{str(tree/'unfollowed-link'),str(tree/'unopened-fifo')},
        'Native canary exact path coverage differs')
    for path,row in streamed.items():
        if row['status']=='measured':
            require(artifacts.validate_regular_length_measurement(row,expected_uid=os.geteuid(),
                max_bytes=6_000_000_000_000,root=tree,path=path)==sizes[str(Path(path).relative_to(tree))],
                'Native canary exact byte length differs')
    require({row['status'] for row in streamed.values()}=={'measured','symlink','special'},'Native canary lost entry classification coverage')


def native_canary(workspace,kernel,wrapper,*,deadline):
    """Read native local files with original syscalls, then the scoped consumer."""
    from horsehair import storage_artifacts as artifacts,budget
    tree=workspace/'native-canary';tree.mkdir();(tree/'state').mkdir();(tree/'nested').mkdir()
    sizes={'empty':0,'small':17,'prefix':65536,'nested/sparse':1048577}
    for name,size in sizes.items():
        with (tree/name).open('xb') as stream:
            if size:stream.seek(size-1);stream.write(b'x')
    outside=workspace/'outside-canary';outside.write_bytes(b'outside')
    (tree/'unfollowed-link').symlink_to(outside);os.mkfifo(tree/'unopened-fifo',0o600)
    original=artifacts.iter_regular_lengths;primitives={name:getattr(artifacts,name) for name in kernel['ORIGINAL_MEASUREMENT_NAMES']}
    uid=os.geteuid();maximum=6_000_000_000_000
    serial={str(p):row for p,row in original(tree,maximum,uid,workers=1)}
    streamed={str(p):row for p,row in kernel['iter_regular_lengths'](tree,maximum,uid,workers=64,pending_limit=128,deadline=deadline)}
    validate_canary_rows(serial,streamed,tree,sizes,artifacts)
    old_root=budget.PERSONAL_STUDY_ROOT
    quota_text=('Filesystem kbytes quota limit grace files quota limit grace\n'
        '/scratch/pwa209 1024 20000000000 20000000000 - 1000 1000000 1000000 -\n')
    def fixture_quota(command,**kwargs):
        require(command==['lfs','quota','-u','3164768','/scratch/pwa209'],'Unexpected fixture quota command')
        return subprocess.CompletedProcess(command,0,quota_text,'')
    try:
        budget.PERSONAL_STUDY_ROOT=tree
        with wrapper['operational_adapter'](kernel,deadline=deadline) as primitive_identity:
            snapshot=budget.storage_snapshot(tree,runner=fixture_quota,exact_sizes=True)
    finally:budget.PERSONAL_STUDY_ROOT=old_root
    require(artifacts.iter_regular_lengths is original and all(getattr(artifacts,n) is f for n,f in primitives.items()),
        'Scoped native canary did not restore original iterator/primitives')
    detail=read_json(tree/'state/storage-entry-sizes.json')
    require(snapshot['study_files']==len(sizes)==detail['count'] and snapshot['study_bytes']==sum(sizes.values())==detail['bytes']
        and [x['relative_path'] for x in detail['items']]==sorted(sizes),
        'Original budget consumer changed sorted native canary totals/coverage')
    return {'status':'complete_native_local_syscall_canary','fixtures_root':str(tree),'owner_uid':uid,
        'serial_equals_streaming':True,'workers':64,'pending_limit':128,'sorted_regular_lengths':sizes,
        'symlink_not_followed':True,'special_not_opened':True,'original_iterator_restored':True,
        'original_measurement_functions_preserved':True,'primitive_identity':primitive_identity,
        'primitive_contract':wrapper['primitive_contract'](primitive_identity['before']),
        'fixture_quota_simulated':True,'fixture_quota_is_admission_authority':False,**FALSE_FLAGS}


def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv)
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('native-plan','native-ledger','native-script','source-manifest'):parser.add_argument('--'+name,type=Path,required=True)
    for name in ('native-task','source-manifest-sha256'):parser.add_argument('--'+name,required=True)
    args=parser.parse_args(argv);os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    manifest,pins=source_manifest(args.source_manifest,args.source_manifest_sha256);baseline=baseline_library()
    job=str(native_job_id() or '');config=read_json(PRIMARY/'configs/study.json')
    import horsehair
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='8' and os.environ.get('HHW_GPU_COUNT')=='0'
        and os.environ.get('HHW_MAX_HOURS')=='1' and Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and canonical_hash(config)==CONFIG and read_json(FRAMEWORK/'configs/study.json')==config,'Owned exact source/config8CPU native qualifier required')
    verify_sources(pins)
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval required')
    ordinary(CASE,ROOT/'maintenance',exists=False);require(not CASE.exists(),'Fresh native qualification namespace required');CASE.mkdir(mode=0o700)
    registration=registered_task(args.native_plan,args.native_ledger,args.native_task,args.native_script,job,config,argv,args.source_manifest,args.source_manifest_sha256)
    allocation,end=native_allocation(job,args.native_script,args.native_task,runpy.run_path(str(SCHEDULER))['run_scontrol_job'])
    runpy.run_path(str(NATIVE_GUARD))['install'](CASE/'accounting-reads')
    from horsehair import campaign,repair,storage_artifacts
    require(campaign._repair_native_states is repair._repair_native_states,'358 native accounting must precede campaign reader binding')
    guard=runpy.run_path(str(SUPPORT/GUARD_NAME),run_name='streaming_q_registry900')
    guard['install'](CASE/'registered-snapshot-reads',registration_mode='controller-already-locked',allocation_deadline_unix=end)
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    with guard['registration_lock'](CASE/'admission-lock',end,role='controller-start'):
        admission=campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],CASE,
            'streaming-qualification-admission',resource_policy=policy,cpus=0,gpus=0,hours=1)
    registry=baseline['registry_qualification'](baseline['REGISTRY_Q'],baseline['REGISTRY_Q_SHA'],baseline['REGISTRY_Q_JOB'])
    public=read_json(baseline['QUALIFICATION']);ids={str(public['slurm_job_id']),registry['job_id']}
    observed=repair._repair_native_states(sorted(ids),{},owner='pwa209',account='def-ptewarie_cpu')
    public_proof=baseline['qualification_proof'](baseline['QUALIFICATION'],baseline['QUALIFICATION_SHA'],str(public['slurm_job_id']),observed)
    registry_proof=baseline['completed_registry_qualification'](registry,observed)
    workspace=local_workspace(job);original_iterator=storage_artifacts.iter_regular_lengths
    with resource_lease(ROOT,config,cpus=8,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        quota=runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,required_bytes=1_000_000_000,
            reserve_files=248000,reserve_bytes=2_000_000_000_000,evidence_dir=CASE/'quota-reads',allocation_deadline_unix=end)
        dependencies=dependency_probe()
        exclusive(CASE/'native-python-dependencies.json',dependencies)
        suites=[]
        for scope in SUITE_SOURCES:
            require(storage_artifacts.iter_regular_lengths is original_iterator,'Public baseline iterator was adapted in qualifier process')
            suites.append(run_suite(scope,manifest['suites'][scope],workspace,end))
        kernel=runpy.run_path(str(SUPPORT/KERNEL),run_name='native_streaming_canary_kernel')
        wrapper=runpy.run_path(str(SUPPORT/WRAPPER),run_name='native_streaming_canary_wrapper')
        canary=native_canary(workspace,kernel,wrapper,deadline=time.monotonic()+max(0,end-time.time()-180))
        exclusive(CASE/'native-canary.json',canary)
        verify_sources(pins);baseline_library()
        require(dependency_probe()==dependencies,'Native Python dependency paths/source bytes changed during qualification')
        require(source_manifest(args.source_manifest,args.source_manifest_sha256)==(manifest,pins),
            'Complete source manifest changed before qualification publication')
        require(storage_artifacts.iter_regular_lengths is original_iterator,'Original iterator was not restored before publication')
        result={'status':'complete_rolling_census_operational_qualification','returncode':0,'slurm_job_id':job,
            'source_sha256':SOURCE,'config_sha256':CONFIG,'source_verified_unchanged':True,
            'qualification_helper':{'path':str(Path(__file__)),'sha256':digest(Path(__file__))},
            'source_manifest':{'path':str(args.source_manifest),'sha256':args.source_manifest_sha256},
            'source_pins':pins,'suites':suites,'native_registration':registration,'allocation':allocation,
            'base_controller':{'path':str(BASE_CONTROLLER),'sha256':BASE_CONTROLLER_SHA},
            'native_python_dependencies':{'path':str(CASE/'native-python-dependencies.json'),
                'sha256':digest(CASE/'native-python-dependencies.json')},
            'resource_policy':policy.receipt(),'public_qualification':public_proof,'registry_qualification':registry_proof,
            'native_accounting_admission':{'path':str(CASE/'streaming-qualification-admission.json'),
                'sha256':digest(CASE/'streaming-qualification-admission.json')},'current_quota':quota,
            'workload':{'required_files':2000,'required_bytes':1_000_000_000,'fixtures_allocation_local':True,
                'fresh_census_prerequisite':False,'actual_census_published':False},
            'native_canary':{'path':str(CASE/'native-canary.json'),'sha256':digest(CASE/'native-canary.json')},
            'primitive_identity':canary['primitive_contract'],
            'baseline_iterator_policy':'original_canonical_iterator','private_iterator_policy':'scoped_private_kernel_only',
            'native_completion_required_separately':True,**FALSE_FLAGS}
        exclusive(CASE/'qualification.json',result)
        print('STREAMING_CENSUS_QUALIFIED '+json.dumps({'job_id':job,'suite_count':len(suites),
            'qualification_sha256':digest(CASE/'qualification.json')},sort_keys=True),flush=True)


if __name__=='__main__':main()
