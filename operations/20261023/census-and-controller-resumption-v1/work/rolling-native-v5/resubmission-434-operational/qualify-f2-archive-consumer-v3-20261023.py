"""Qualify only the frozen archive consumer's disposable native test closure.

This producer runs the complete 111 overlay and 25 verifier contracts in an
owned allocation. It does not activate an overlay, hand over a controller, pack
study files, delete data, credit savings, or claim native consumer integration.
Its passing test receipt still requires a later genuine native completion join.
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
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

from horsehair.core import canonical_hash,digest,read_json,source_identity,native_job_id
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy

ROOT=Path('/scratch/pwa209/horsehair-worm')
SUPPORT=ROOT/'maintenance/study-af-resubmission-434-20261023'
CASE=ROOT/'maintenance/study-af-archive-consumer-qualification-support-v3-20261023'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
BASE_PATH=ROOT/'maintenance/study-af-registry-repair-support-v6-20261022/run-af-agent-controller-registry-v6-20261022.py'
OVERLAY='run-af-agent-controller-archive-v3-20261023.py'
OVERLAY_TEST='test_af_agent_controller_archive_v3_20261023.py'
VERIFIER='verify-f2-condition-archives-v2-20261022.py'
VERIFIER_TEST='test_verify_f2_condition_archives_v2_20261022.py'
PINS={OVERLAY:'2004e959019a2fe564f58d1e5fce70ee988a3afb9e62ebc42b95488eaa9fdc02',
    OVERLAY_TEST:'de914a3b8a3ad99586a9082912dcbf36004fcd9391501cbe5f44cfee79f8e461',
    VERIFIER:'b02afa747f8e37a654dc0eda6f3a370808cdd381cf92a55a9b56ea9a8b6bfcae',
    VERIFIER_TEST:'88e79c8fcb4fe7b80882c176d78006f301c50a0c0f6cb8266d4c5bfbb867c565',
    BASE_PATH.name:'e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'}
SUITES={OVERLAY_TEST:{'sha256':PINS[OVERLAY_TEST],'tests':111,
    'roster_sha256':'ecd310d80640df1e95c408c1f2e2b3ceaba7fd168a8705066c69b19ca83835b7'},
    VERIFIER_TEST:{'sha256':PINS[VERIFIER_TEST],'tests':25,
    'roster_sha256':'3efab27035c49b116b8ef8583d15b531fb9f081c1e49d77e119629e42dcee7d2'}}
FRAMEWORK_Q={'path':str(ROOT/'maintenance/study-af-execution-support-v1-20261021/qualification.json'),
    'sha256':'29ed530f3783f3c35c3cf20343bdb663732ec2bf4bf5c3218f8b029510becc2f','job_id':'6684484'}
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=GUARD.with_name('native-scheduler-read.py')
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
QUOTA=ROOT/'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'
QUOTA_SHA='5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045'
REGISTRY_GUARD=BASE_PATH.with_name('registered-snapshot-read-guard-v5-20261022.py')
REGISTRY_GUARD_SHA='7bd2bf562d0a81887ef1afd308ddd9265e5cadb23ba4b2e5946980b659505431'
FLAGS={'scientific_result':False,'admission_authority':False,'activation_performed':False,
    'controller_handover_performed':False,'actual_policy_packing_performed':False,'original_removal_authorized':False,
    'native_consumer_integration_claimed':False,'inference_claimed':False,'future_storage_savings_credited':0}


def require(value,message):
    if not value:raise ValueError(message)


def ordinary(path,namespace,*,fresh=False):
    path=Path(path)
    require(path.is_absolute() and path==path.resolve() and path!=namespace and path.is_relative_to(namespace)
        and all(not p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(ROOT)),
        'Exact ordinary private qualification path required')
    require(not path.exists() if fresh else path.is_file(),'Fresh evidence required' if fresh else 'Required source missing')
    return path


def verify_sources():
    require(set(PINS)=={OVERLAY,OVERLAY_TEST,VERIFIER,VERIFIER_TEST,BASE_PATH.name}
        and all(type(h) is str and re.fullmatch('[0-9a-f]{64}',h) for h in PINS.values()),
        'Complete frozen archive source closure required')
    for name,expected in PINS.items():
        path=ordinary(SUPPORT/name,SUPPORT)
        require(path.stat().st_size<=512_000 and digest(path)==expected,'Frozen archive source changed: '+name)
    require(digest(ordinary(BASE_PATH,ROOT/'maintenance'))==PINS[BASE_PATH.name],
        'Canonical frozen registry controller changed')


def load_overlay(args):
    require(args.qualification_case==CASE and args.overlay==SUPPORT/OVERLAY
        and args.overlay_sha256==PINS[OVERLAY] and args.verifier==SUPPORT/VERIFIER
        and args.verifier_sha256==PINS[VERIFIER] and args.base_controller==BASE_PATH
        and args.base_controller_sha256==PINS[BASE_PATH.name]
        and args.overlay_tests==SUPPORT/OVERLAY_TEST and args.verifier_tests==SUPPORT/VERIFIER_TEST,
        'Only the complete exact staged archive qualification inputs are supported')
    verify_sources();raw=args.overlay.read_bytes()
    require(hashlib.sha256(raw).hexdigest()==PINS[OVERLAY],'Archive overlay bytes changed before compilation')
    module=type(sys)('private_verified_archive_consumer_qualification_target');module.__file__=str(args.overlay)
    exec(compile(raw,str(args.overlay),'exec'),module.__dict__)
    require(args.overlay.read_bytes()==raw and module.ROOT==ROOT and module.FRAMEWORK==FRAMEWORK
        and module.SOURCE==SOURCE and module.CONFIG==CONFIG and module.BASE_PATH==BASE_PATH
        and module.BASE_SHA==PINS[BASE_PATH.name] and module.VERIFIER_SHA==PINS[VERIFIER]
        and module.TEST_SUITES==SUITES,'Exact frozen archive consumer runtime contract differs')
    return module


def registered_source_intent(module,args,producer):
    """Bind the running helper bytes to the already published registration intent."""
    path=ordinary(SUPPORT/'submission-intent.json',SUPPORT);binding=module.locator(path)
    value=module.strict(module.read_bound(binding));ident=value.get('id');tokens=value.get('args')
    expected={'--overlay':str(args.overlay),'--overlay-sha256':args.overlay_sha256,
        '--verifier':str(args.verifier),'--verifier-sha256':args.verifier_sha256,
        '--base-controller':str(args.base_controller),'--base-controller-sha256':args.base_controller_sha256,
        '--overlay-tests':str(args.overlay_tests),'--verifier-tests':str(args.verifier_tests),
        '--qualification-case':str(args.qualification_case),'--native-plan':str(args.native_plan),
        '--native-ledger':str(args.native_ledger),'--native-script':str(args.native_script),'--native-task':args.native_task}
    require(type(ident) is str and re.fullmatch('[1-9][0-9]*',ident)
        and value.get('helper')==Path(__file__).name and value.get('helper_sha256')==producer['sha256']
        and producer['path']==str(SUPPORT/Path(__file__).name) and value.get('area')==str(SUPPORT)
        and value.get('automatic_replay_permitted') is False
        and value.get('plan_path')==str(args.native_plan) and value.get('ledger_path')==str(args.native_ledger)
        and args.native_plan==ROOT/('plans/recovery-support-20261021-'+ident)/'plan.json'
        and args.native_ledger==ROOT/('state/recovery-support-20261021-'+ident+'-submission.json')
        and args.native_task=='recovery-'+ident and args.native_script==args.native_plan.parent/(args.native_task+'.sbatch')
        and type(tokens) is list and len(tokens)==2*len(expected) and all(type(v) is str for v in tokens)
        and len(set(tokens[::2]))==len(expected) and dict(zip(tokens[::2],tokens[1::2]))==expected,
        'Published registration intent does not bind these helper bytes and exact native arguments')
    return binding


def testcase_roster(raw):
    require(type(raw) is bytes and len(raw)<=8_000_000 and b'<!DOCTYPE' not in raw.upper()
        and b'<!ENTITY' not in raw.upper(),'Bounded plain actual JUnit required')
    cases=list(ET.fromstring(raw).iter('testcase'))
    roster=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in cases)
    require(len(set(roster))==len(roster) and all(a and b for a,b in roster)
        and not any(list(c.iter(k)) for c in cases for k in ('failure','error','skipped')),
        'Every distinct actual testcase must pass without skips')
    return roster


def verify_suite(module,junit,test_source,expected):
    from horsehair.cli import _qualification_junit
    roster=testcase_roster(Path(junit).read_bytes());counts={'tests':expected['tests'],'failures':0,'errors':0,'skipped':0}
    require(len(roster)==expected['tests'] and canonical_hash(roster)==expected['roster_sha256']
        and _qualification_junit(Path(junit))==counts,'Complete actual frozen archive testcase roster differs')
    require(Path(test_source).name in SUITES and digest(test_source)==expected['sha256'],
        'Executed archive test source changed')
    return {'tests':counts,'test_source':module.locator(test_source),'junit':module.locator(junit)}


def child_environment(module,framework):
    from horsehair.rna_recovery import _primary_environment
    require(framework in (FRAMEWORK,module.CONSUMER_FRAMEWORK),'Whole qualified test package required')
    env=_primary_environment({'primary_release':str(framework),'maintenance_release':str(FRAMEWORK),'root':str(ROOT)})
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS','PYTHONHOME','PYTHONSTARTUP','PYTHONINSPECT'):env.pop(key,None)
    env.update(PYTEST_DISABLE_PLUGIN_AUTOLOAD='1',PYTHONDONTWRITEBYTECODE='1',PYTHONNOUSERSITE='1',TZ='UTC',
        HHW_OVERLAY_PUBLIC_FIXTURE_ROOT=str(module.CONSUMER_FRAMEWORK),
        HHW_ARCHIVE_SYNTHETIC_FIXTURE_ROOT=str(module.CONSUMER_FRAMEWORK))
    return env


def allocated_fixture_directory(job):
    temporary=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temporary.is_absolute() and temporary==temporary.resolve() and temporary.is_dir()
        and not temporary.is_symlink() and temporary.stat().st_uid==3164768,
        'Owned canonical allocated-node fixture directory required')
    require(shutil.disk_usage(temporary).free>=4_000_000_000 and os.statvfs(temporary).f_favail>=40_000,
        'Four GB and forty thousand local fixture inodes required')
    destination=temporary/('archive-consumer-v3-q-'+job)
    require(not destination.exists(),'Fresh disposable fixture directory required');destination.mkdir(mode=0o700)
    return destination


def native_owner(job):
    response=runpy.run_path(str(SCHEDULER))['run_scontrol_job'](job,CASE/'scheduler-reads');response.check_returncode()
    lines=[s for s in response.stdout.splitlines() if s.strip()];require(len(lines)==1,'One native allocation required')
    pairs=re.findall(r'(?<!\S)(\w+)=([^\s]*)',lines[0]);row=dict(pairs)
    match=re.fullmatch(r'(?:(\d+)-)?(\d+):(\d{2}):(\d{2})',row.get('TimeLimit',''))
    require(match is not None,'Finite native time limit required');d,h,m,s=(int(v or 0) for v in match.groups())
    for name in ('AllocTRES','ReqTRES'):
        values={}
        for item in row.get(name,'').split(','):
            key,separator,value=item.partition('=')
            require(separator and key and key not in values,'Unique explicit native TRES required');values[key]=value
        require(values.get('cpu')=='4' and not any('gpu' in key.lower() for key in values),'Positive four-CPU-only native TRES required')
    require(len(row)==len(pairs) and row.get('JobId')==job and row.get('UserId')=='pwa209(3164768)'
        and row.get('Account')=='def-ptewarie_cpu' and row.get('JobState')=='RUNNING' and row.get('NumCPUs')=='4'
        and row.get('NumNodes') in {'1','1-1'} and row.get('MinMemoryNode') in {'16G','16384M'}
        and d*86400+h*3600+m*60+s==3600 and all('gpu' not in row.get(k,'').lower()
        for k in ('TresPerNode','TresPerJob','TresPerTask','Gres')),'Exact owned four-CPU 16GB one-hour allocation required')
    end=datetime.fromisoformat(row['EndTime']).replace(tzinfo=timezone.utc).timestamp();env=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(env) and abs(env-end)<=2 and end>time.time()+900,'Verified native completion margin required')
    return row,end


def registration(module,args,job,allocation):
    record={'plan':module.locator(args.native_plan,namespace=ROOT/'plans'),
        'ledger':module.locator(args.native_ledger,namespace=ROOT/'state'),
        'script':module.locator(args.native_script,namespace=ROOT/'plans'),'task_name':args.native_task,'native_job_id':job}
    inputs={'overlay':module.locator(args.overlay),'verifier':module.locator(args.verifier),
        'base_controller':module.locator(args.base_controller),'overlay_tests':module.locator(args.overlay_tests),
        'verifier_tests':module.locator(args.verifier_tests),'fixture_root':str(module.CONSUMER_FRAMEWORK)}
    producer=module.locator(Path(__file__))
    proof={'native_registration':record,'qualification_inputs':inputs,'producer_allocation':allocation,
        'suites':{OVERLAY_TEST:{'test_source':inputs['overlay_tests']},VERIFIER_TEST:{'test_source':inputs['verifier_tests']}}}
    module.private_producer_registration(proof,{'path':str(CASE/'qualification.json'),'sha256':'0'*64,'job_id':job,'producer':producer})
    return record,inputs,producer


def qualification(module,binding,source,count,join,end):
    value=module.strict(module.read_bound({k:binding[k] for k in ('path','sha256')}))
    counts={'tests':count,'failures':0,'errors':0,'skipped':0}
    require(value.get('status')=='complete' and type(value.get('returncode')) is int and value['returncode']==0
        and value.get('source_sha256')==source and value.get('config_sha256')==CONFIG
        and value.get('source_verified_unchanged') is True and value.get('slurm_job_id')==binding['job_id']
        and value.get('test_counts')==counts and all(type(v) is int for v in value['test_counts'].values()),
        'Exact positive unchanged public qualification required')
    artifact=value.get('artifacts',{}).get('junit',{});raw=module.read_bound({k:artifact[k] for k in ('path','sha256')},8_000_000,
        namespace=ROOT/'state/qualification-evidence')
    from horsehair.cli import _qualification_junit
    require(type(artifact.get('bytes')) is int and len(raw)==artifact['bytes']
        and len(testcase_roster(raw))==count and _qualification_junit(Path(artifact['path']))==counts,
        'Actual complete public qualification JUnit differs')
    remaining=end-time.time()-300;require(math.isfinite(remaining) and remaining>0,'Native public completion-join margin exhausted')
    return join(binding['job_id'],deadline=time.monotonic()+min(60.,remaining))


def fresh_census(record,*,now=None):
    now=time.time() if now is None else now;measured=record.get('measured_unix')
    require(type(measured) in (int,float) and math.isfinite(measured),'Finite actual census time required')
    captured=datetime.fromisoformat(record.get('captured_utc','').replace('Z','+00:00'))
    require(captured.tzinfo is not None and 0<=now-measured<21600 and 0<=now-captured.timestamp()<21600,
        'Actual storage census must be fresh within six hours')


def storage_gate(module,config,end):
    path=ROOT/'state/storage-budget.json';before=digest(path);fresh_census(read_json(path))
    quota=runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,required_bytes=10_000_000_000,
        reserve_files=248000,reserve_bytes=2_000_000_000_000,evidence_dir=CASE/'quota-reads',allocation_deadline_unix=end)
    from horsehair.admission import _storage
    future_bytes,future_files,paths=_storage(ROOT,config,10_000_000_000,2000,personal_headroom=quota)
    current=path.read_bytes()
    require(hashlib.sha256(current).hexdigest()==before,'Storage census changed during native quota validation')
    fresh_census(module.strict(current))
    value={'census':{'path':str(path),'sha256':before},'personal_quota':quota,'future_study_bytes':future_bytes,
        'future_study_files':future_files,'source_paths':list(map(str,paths)),'fixtures_allocation_local':True,**FLAGS}
    module.exclusive(CASE/'storage-proof.json',value);return value


def run_suite(module,name,framework,workspace,end,*,runner=subprocess.run):
    junit=CASE/(name+'.xml');source=SUPPORT/name
    command=[sys.executable,'-s','-m','pytest',str(source),'-q','-p','no:cacheprovider',
        '--rootdir',str(SUPPORT),'--basetemp',str(workspace/name),'--junitxml',str(junit)]
    timeout=min(2400.,end-time.time()-300.);require(math.isfinite(timeout) and timeout>60,'Native full-suite finalization margin exhausted')
    module.exclusive(CASE/(name+'-intent.json'),{'command':command,'test_source':module.locator(source),
        'package_source':str(framework),'maximum_seconds':timeout,**FLAGS})
    with (CASE/(name+'-stdout.txt')).open('xb') as out,(CASE/(name+'-stderr.txt')).open('xb') as err:
        try:response=runner(command,cwd=framework,stdout=out,stderr=err,timeout=timeout,
            env=child_environment(module,framework),check=False,stdin=subprocess.DEVNULL)
        except subprocess.TimeoutExpired:
            module.exclusive(CASE/(name+'-failure.json'),{'status':'native_test_closure_timeout','command':command,**FLAGS});raise
    require(type(response.returncode) is int and response.returncode==0,'Actual complete native archive suite failed')
    for suffix in ('-stdout.txt','-stderr.txt'):
        require((CASE/(name+suffix)).stat().st_size<=8_000_000,'Native qualification output exceeds its finite bound')
    return verify_suite(module,junit,source,SUITES[name])


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('overlay','verifier','base-controller','overlay-tests','verifier-tests','qualification-case','native-plan','native-ledger','native-script'):
        parser.add_argument('--'+name,type=Path,required=True)
    for name in ('overlay-sha256','verifier-sha256','base-controller-sha256','native-task'):parser.add_argument('--'+name,required=True)
    args=parser.parse_args(argv);os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    module=load_overlay(args);producer=module.locator(Path(__file__))
    require(producer['path']==str(SUPPORT/Path(__file__).name),'Exact separately staged qualification producer required')
    stage_intent=registered_source_intent(module,args,producer)
    job=str(native_job_id() or '')
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='4','Owned scalar four-CPU qualification required')
    import horsehair
    config=read_json(PRIMARY/'configs/study.json')
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA
        and source_identity(module.CONSUMER_FRAMEWORK)['sha256']==module.CONSUMER_SOURCE
        and canonical_hash(config)==CONFIG,'Exact separate qualified packages and frozen configuration required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Native Narval allocation required')
    for path,expected in ((GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(QUOTA,QUOTA_SHA),(REGISTRY_GUARD,REGISTRY_GUARD_SHA)):
        require(digest(ordinary(path,ROOT/'maintenance'))==expected,'Qualified native read helper changed')
    ordinary(CASE,ROOT/'maintenance',fresh=True);CASE.mkdir(mode=0o700)
    allocation,end=native_owner(job)
    runpy.run_path(str(GUARD))['install'](CASE/'accounting-reads')
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'Native accounting guard must precede campaign binding')
    runpy.run_path(str(REGISTRY_GUARD))['install'](CASE/'registered-snapshot-reads',registration_mode='worker-lock',allocation_deadline_unix=end)
    native_registration,inputs,producer=registration(module,args,job,allocation)
    policy=load_resource_policy(module.POLICY,module.POLICY_SHA,root=ROOT,config=config)
    campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],CASE,
        'native-start-admission',resource_policy=policy,cpus=0,gpus=0,hours=1)
    join,join_source=module.public_native_join()
    public_proofs={'framework':qualification(module,FRAMEWORK_Q,SOURCE,1694,join,end),
        'consumer':qualification(module,module.CONSUMER_Q,module.CONSUMER_SOURCE,1889,join,end)}
    workspace=allocated_fixture_directory(job)
    intent={'status':'native_archive_test_closure_only','qualification_driver':producer,'qualification_inputs':inputs,
        'stage_submission_intent':stage_intent,
        'native_registration':native_registration,'producer_allocation':allocation,'resource_policy':policy.receipt(),
        'public_completion_proofs':public_proofs,'native_join_source':join_source,**FLAGS}
    module.exclusive(CASE/'intent.json',intent)
    with resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        storage_gate(module,config,end)
        suites={OVERLAY_TEST:run_suite(module,OVERLAY_TEST,FRAMEWORK,workspace,end),
            VERIFIER_TEST:run_suite(module,VERIFIER_TEST,module.CONSUMER_FRAMEWORK,workspace,end)}
        verify_sources();require(time.time()+300<end,'Native qualification completion margin exhausted')
        require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA
            and source_identity(module.CONSUMER_FRAMEWORK)['sha256']==module.CONSUMER_SOURCE,
            'Qualified source changed during actual native tests')
        require(module.locator(Path(__file__))==producer,'Qualification producer source changed')
        require(module.locator(SUPPORT/'submission-intent.json')==stage_intent,'Published registration intent changed')
        value={'schema_version':1,'status':'complete_f2_archive_overlay_qualification','returncode':0,
            'source_sha256':SOURCE,'config_sha256':CONFIG,'source_verified_unchanged':True,'slurm_job_id':job,
            'wrapper_sha256':PINS[OVERLAY],'base_controller_sha256':PINS[BASE_PATH.name],'verifier_sha256':PINS[VERIFIER],
            'consumer_qualification':module.CONSUMER_Q,'qualification_driver':producer,'qualification_inputs':inputs,
            'stage_submission_intent':stage_intent,
            'native_registration':native_registration,'producer_allocation':allocation,'resource_policy':policy.receipt(),
            'suites':suites,'public_completion_proofs':public_proofs,'producer_native_completion':'pending',**FLAGS}
        module.private_producer_registration(value,{'path':str(CASE/'qualification.json'),'sha256':'0'*64,
            'job_id':job,'producer':producer})
        module.exclusive(CASE/'qualification.json',value)
    return value


if __name__=='__main__':main()
