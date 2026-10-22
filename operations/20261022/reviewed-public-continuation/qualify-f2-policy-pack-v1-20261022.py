"""Native qualification of the private originals-retaining policy pack driver.

Only disposable allocated-node fixtures are packed. This entry point does not
read actual policy payloads, publish a controller descriptor, or remove data.
"""
from pathlib import Path
import argparse
import hashlib
import importlib.util
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

ROOT=Path('/scratch/pwa209/horsehair-worm')
# The gateway creates only the immutable source stage. The native entry point
# creates the separate result support case once; neither pathname is replayed.
SUPPORT=ROOT/'maintenance/study-af-policy-pack-qualification-stage-v1-20261022'
CASE=ROOT/'maintenance/study-af-policy-pack-qualification-support-v1-20261022'


def require(value,message):
    if not value:raise ValueError(message)


def load_pack(path,sha):
    path=Path(path)
    require(type(sha) is str and re.fullmatch('[0-9a-f]{64}',sha)
        and path.is_absolute() and path==path.resolve() and path.parent==SUPPORT
        and path.name=='run-f2-policy-archive-pack-v1-20261021.py'
        and all(not p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(ROOT))
        and path.is_file() and path.stat().st_size<=256_000,'Exact immutable supported pack driver required')
    raw=path.read_bytes();require(hashlib.sha256(raw).hexdigest()==sha,'Private pack driver source SHA changed')
    # Compile the already verified bytes; do not perform a second unchecked read.
    module=type(sys)('private_verified_policy_pack_qualification_target');module.__file__=str(path)
    exec(compile(raw,str(path),'exec'),module.__dict__)
    require(path.read_bytes()==raw and module.ROOT==ROOT and module.PRIVATE_TESTS
        and module.VALIDATION_Q['job_id']=='6703313'
        and module.VALIDATION_Q['sha256']=='8c3e1910deabea6c1061ddb353c3a6881fb70fb9fe86948007557a03ca809176',
        'Exact current pack source and separately completed1889 qualification required')
    return module


def testcase_roster(raw):
    require(type(raw) is bytes and len(raw)<=8_000_000 and b'<!DOCTYPE' not in raw.upper()
        and b'<!ENTITY' not in raw.upper(),'Bounded plain actual JUnit required')
    cases=list(ET.fromstring(raw).iter('testcase'))
    roster=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in cases)
    require(len(set(roster))==len(roster) and all(name for _,name in roster)
        and not any(list(c.iter(k)) for c in cases for k in ('failure','error','skipped')),
        'Every distinct actual private contract testcase must pass without skips')
    return roster


def verify_suite(module,path,test_source,expected):
    raw=Path(path).read_bytes();roster=testcase_roster(raw)
    counts={'tests':expected['tests'],'failures':0,'errors':0,'skipped':0}
    from horsehair.cli import _qualification_junit
    require(len(roster)==expected['tests'] and module.canonical_hash(roster)==expected['roster_sha256']
        and _qualification_junit(Path(path))==counts,'Actual complete frozen private pack JUnit roster differs')
    require(Path(test_source).name in module.PRIVATE_TESTS and module.digest(Path(test_source))==expected['sha256'],
        'Private contract test source changed during execution')
    return {'tests':counts,'test_source':module.locator(test_source),'junit':module.locator(path)}


def child_environment(module):
    from horsehair.rna_recovery import _primary_environment
    environment=_primary_environment({'primary_release':str(module.FRAMEWORK),
        'maintenance_release':str(module.FRAMEWORK),'root':str(ROOT)})
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS','PYTHONHOME','PYTHONSTARTUP','PYTHONINSPECT'):
        environment.pop(key,None)
    environment.update(PYTEST_DISABLE_PLUGIN_AUTOLOAD='1',PYTHONDONTWRITEBYTECODE='1',PYTHONNOUSERSITE='1',
        HHW_PACK_FIXTURE_ROOT=str(module.VALIDATION_FRAMEWORK),TZ='UTC')
    return environment


def allocated_fixture_directory(job):
    temporary=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temporary.is_absolute() and temporary==temporary.resolve() and temporary.is_dir()
        and not temporary.is_symlink() and temporary.stat().st_uid==3164768,
        'Owned canonical allocated-node fixture directory required')
    require(shutil.disk_usage(temporary).free>=4_000_000_000,'Four GB of disposable allocated-node fixture space required')
    filesystem=os.statvfs(temporary)
    require(filesystem.f_favail>=40_000,'Forty thousand available allocated-node fixture inodes required')
    destination=temporary/('policy-pack-q-'+job)
    require(not destination.exists(),'Fresh disposable fixture stage required')
    destination.mkdir(mode=0o700)
    return destination


def registration(module,args,job):
    result={'plan':module.locator(args.native_plan),'ledger':module.locator(args.native_ledger),
        'script':module.locator(args.native_script),'task_name':args.native_task,'native_job_id':job}
    q={'native_registration':result,'pack_driver':module.locator(args.pack_driver),
        'fixture_root':str(module.VALIDATION_FRAMEWORK),'framework_qualification':module.FRAMEWORK_Q,
        'suites':{args.test_source.name:{'test_source':module.locator(args.test_source)}}}
    binding={'path':str(CASE/'qualification.json'),'sha256':'0'*64,'job_id':job,'producer':module.locator(Path(__file__))}
    module.private_producer_registration(q,binding)
    return result


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pack-driver',type=Path,required=True);parser.add_argument('--pack-driver-sha256',required=True)
    parser.add_argument('--test-source',type=Path,required=True);parser.add_argument('--qualification-case',type=Path,required=True)
    for name in ('native-plan','native-ledger','native-script'):parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--native-task',required=True);args=parser.parse_args(argv)
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    require(args.qualification_case==CASE,'Only the fresh root-reserved private qualification case is supported')
    module=load_pack(args.pack_driver,args.pack_driver_sha256)
    source_name=args.test_source.name
    require(set(module.PRIVATE_TESTS)=={source_name} and args.test_source.parent==SUPPORT
        and module.bound({'path':str(args.test_source),'sha256':module.PRIVATE_TESTS[source_name]['sha256']}),
        'Exact same-stage full private contract suite required')
    require(module.locator(Path(__file__))['path']==str(SUPPORT/Path(__file__).name),
        'Immutable root-reserved qualification producer source required')
    job=str(module.native_job_id() or '')
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='4','Owned scalar four-CPU native qualification required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),
        'Native Narval qualification required')
    config=module.read_json(module.PRIMARY/'configs/study.json')
    require(module.source_identity(module.FRAMEWORK)['sha256']==module.SOURCE
        and module.source_identity(module.PRIMARY)['sha256']==module.PRIMARY_SHA
        and module.canonical_hash(config)==module.CONFIG,'Qualified4f and original frozen science/config required')
    for path,sha in ((module.GUARD,module.GUARD_SHA),(module.SCHEDULER,module.SCHEDULER_SHA),
        (module.QUOTA,module.QUOTA_SHA),(module.REGISTRY_GUARD,module.REGISTRY_GUARD_SHA)):
        require(module.digest(module.ordinary(path))==sha,'Exact reviewed read-only native guard required')
    module.ordinary(CASE,fresh=True,namespace=ROOT/'maintenance');CASE.mkdir(mode=0o700)
    allocation,end=module.native_owner(job,CASE)
    module.runpy.run_path(str(module.GUARD))['install'](CASE/'accounting-reads')
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'358 must precede campaign function binding')
    module.runpy.run_path(str(module.REGISTRY_GUARD))['install'](CASE/'registered-snapshot-reads',
        registration_mode='worker-lock',allocation_deadline_unix=end)
    native_registration=registration(module,args,job);module.native_registration_owner(allocation,native_registration)
    policy=module.load_resource_policy(module.POLICY,module.POLICY_SHA,root=ROOT,config=config)
    campaign.combined_admission(ROOT,config,module.read_json(module.PRIMARY/'release.json')['release_id'],
        CASE,'native-start-admission',resource_policy=policy,cpus=0,gpus=0,hours=1)
    join,join_binding=module.public_native_join()
    module.qualification(module.FRAMEWORK_Q,module.SOURCE,1694,join,end=end)
    module.qualification(module.VALIDATION_Q,module.VALIDATION_SOURCE,1889,join,end=end)
    archive_path=module.VALIDATION_FRAMEWORK/'tests/fixtures/evidence_archive/verified-evidence-archive-20261021.py'
    archive=module.load_archive({'path':str(archive_path),'sha256':module.ARCHIVE_SHA})
    archive._verify_canary(module.CANARY,ROOT)
    context={'case':CASE,'end':end,'config':config}
    temporary=allocated_fixture_directory(job)
    intent={'schema_version':1,'status':'private_pack_tests_only','qualification_driver':module.locator(Path(__file__)),
        'pack_driver':module.locator(args.pack_driver),'test_source':module.locator(args.test_source),
        'fixture_root':str(module.VALIDATION_FRAMEWORK),'native_registration':native_registration,
        'allocation':allocation,'resource_policy':policy.receipt(),'native_join_binding':join_binding,
        'scientific_result':False,'admission_authority':False,'original_removal_authorized':False}
    module.exclusive(CASE/'intent.json',intent)
    with module.resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        module.storage_gate(context,2000,10_000_000_000,1)
        junit=CASE/'junit.xml';environment=child_environment(module)
        command=[sys.executable,'-s','-m','pytest',str(args.test_source),'-q','-p','no:cacheprovider',
            '--rootdir',str(SUPPORT),'--basetemp',str(temporary/'pytest'),'--junitxml',str(junit)]
        timeout=min(2400.,end-time.time()-300.)
        require(math.isfinite(timeout) and timeout>600,'Finite margin for complete private qualification required')
        try:
            result=subprocess.run(command,cwd=module.FRAMEWORK,env=environment,capture_output=True,timeout=timeout)
        except subprocess.TimeoutExpired as error:
            for name,value in [('stdout',error.stdout),('stderr',error.stderr)]:
                if value:
                    with (CASE/('tests-'+name+'.partial')).open('xb') as stream:stream.write(value[:8_000_000])
            module.exclusive(CASE/'failure.json',{'status':'private_qualification_timeout','timeout_seconds':timeout,
                'native_job_id':job,'command':command,**module.FLAGS})
            raise
        require(type(result.returncode) is int and len(result.stdout)<=8_000_000 and len(result.stderr)<=8_000_000,
            'Finite private qualification output required')
        for name,value in [('stdout',result.stdout),('stderr',result.stderr)]:
            with (CASE/('tests-'+name+'.txt')).open('xb') as stream:stream.write(value)
        require(result.returncode==0,'Actual private pack contract suite failed; no qualification is issued')
        suite=verify_suite(module,junit,args.test_source,module.PRIVATE_TESTS[source_name])
        require(time.time()+300<end and module.digest(args.pack_driver)==args.pack_driver_sha256,
            'Private source or native completion margin changed')
        module.public_native_join();archive._verify_canary(module.CANARY,ROOT)
        require(module.locator(Path(__file__))==intent['qualification_driver'],'Qualification producer source changed')
        value={'schema_version':1,'status':'complete_f2_policy_pack_operational_qualification_v1','returncode':0,
            'source_sha256':module.SOURCE,'config_sha256':module.CONFIG,'slurm_job_id':job,
            'wrapper_sha256':args.pack_driver_sha256,'archive_helper_sha256':module.ARCHIVE_SHA,
            'validation_source_sha256':module.VALIDATION_SOURCE,'validation_qualification':module.VALIDATION_Q,
            'framework_qualification':module.FRAMEWORK_Q,'qualification_driver':intent['qualification_driver'],
            'pack_driver':intent['pack_driver'],'fixture_root':str(module.VALIDATION_FRAMEWORK),
            'native_registration':native_registration,'resource_policy':policy.receipt(),
            'suites':{source_name:suite},'scientific_result':False,'admission_authority':False,
            'original_removal_authorized':False,'producer_native_completion':'pending'}
        # This positive test receipt is deliberately separate from native
        # completion. A later pack/finalizer must join its completed owner proof.
        binding={'path':str(CASE/'qualification.json'),'sha256':'0'*64,'job_id':job,'producer':intent['qualification_driver']}
        module.private_producer_registration(value,binding)
        module.exclusive(CASE/'qualification.json',value)
    return value


if __name__=='__main__':main()
