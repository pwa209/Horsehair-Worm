"""Run real allocated scheduler regressions; never submit or cancel a job."""
from pathlib import Path
from datetime import datetime,timezone
import argparse
import importlib.util
import json
import math
import os
import runpy
import shutil
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

from horsehair.core import canonical_hash,digest,native_job_id,read_json,source_identity

HERE=Path(__file__).resolve().parent
ROOT=Path('/scratch/pwa209/horsehair-worm')
OUTER_NAME='run-af-agent-controller-backfill-policy-v2-20261024.py'
BASE_NAME='run-af-agent-controller-backfill-base-v2-20261024.py'
OLD_BASE_NAME='run-af-agent-controller-registry-v6-20261022.py'
OLD_OUTER_NAME='run-af-agent-controller-census-policy-v1-20261023.py'
OLD_POLICY=ROOT/'maintenance/study-af-external-census-handoff-v1-20261023/native-6865706/external-census-policy.json'
OLD_POLICY_SHA='2b0e6ebaa0f1b511bca4343d4722d43702cf051f956cce59494ddccae656c517'
SCHEDULER=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-scheduler-read.py'
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
NATIVE_GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
NATIVE_GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
OPERATIONAL_TESTS=('test_registered_snapshot_read_guard_v5_20261022.py','test_f00d_worker_registry_overlay_20261021.py',
    'test_af_agent_controller_registry_v6_20261022.py')
PROVIDER_TEST='test_af_agent_controller_census_policy_v1_20261023.py'


def require(value,message):
    if not value:raise ValueError(message)


def outer_module():
    spec=importlib.util.spec_from_file_location('native_backfill_qualification_outer',HERE/OUTER_NAME)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def exclusive(path,value):
    path=Path(path)
    with path.open('x',encoding='utf8',newline='\n') as stream:
        json.dump(value,stream,indent=2,sort_keys=True);stream.write('\n')


def source_pins(outer):
    require(digest(HERE/'qualification-asset-pins.json')==outer.BACKFILL_ASSET_MANIFEST_SHA,'Frozen qualification asset manifest changed')
    assets=read_json(HERE/'qualification-asset-pins.json')
    pins={str(HERE/name):digest(HERE/name) for name in (BASE_NAME,OUTER_NAME,Path(__file__).name,outer.BACKFILL_TEST_NAME,
        'qualification-asset-pins.json','build-backfill-native-plans-v2-20261024.py')}
    require(pins[str(HERE/BASE_NAME)]==outer.BACKFILL_BASE_SHA
        and pins[str(HERE/outer.BACKFILL_TEST_NAME)]==outer.BACKFILL_TEST_SHA,'New base or backfill suite bytes changed')
    for name,sha in assets.items():
        require(Path(name).name==name and digest(HERE/'qualification-assets'/name)==sha,'Preserved regression asset changed')
        pins[str(HERE/'qualification-assets'/name)]=sha
    return pins,assets


def junit_result(path,expected_tests,maximum_bytes):
    require(type(maximum_bytes) is int and maximum_bytes==8_000_000,'Exact scoped qualification artifact bound required')
    with Path(path).open('rb') as stream:raw=stream.read(maximum_bytes+1)
    require(len(raw)<=maximum_bytes and b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Bounded ordinary JUnit required')
    tree=ET.fromstring(raw);cases=list(tree.iter('testcase'))
    roster=sorted((case.get('classname','').split('.')[-1],case.get('name','')) for case in cases)
    require(len(cases)==expected_tests and len(set(roster))==len(roster) and all(a and b for a,b in roster)
        and not any(list(case.iter('failure')) or list(case.iter('error')) or list(case.iter('skipped')) for case in cases),
        'Every exact regression testcase must pass without skips')
    leaves=[node for node in tree.iter('testsuite') if not list(node.iter('testsuite'))[1:]]
    require(leaves and sum(int(node.get('tests','-1')) for node in leaves)==expected_tests,'JUnit reported counts differ')
    counts={'tests':expected_tests,'failures':0,'errors':0,'skipped':0}
    return {'test_counts':counts,'test_roster_sha256':canonical_hash(sorted(a+'::'+b for a,b in roster)),
        'junit':{'path':str(Path(path).resolve()),'sha256':digest(path)}},roster


def execute_suites(area,outer,*,deadline=None):
    """A local caller may exercise this read-only staging/test function."""
    area=Path(area);area.mkdir(mode=0o700,parents=True,exist_ok=False)
    pins,assets=source_pins(outer);results=[];failed=[]
    environment={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1'}
    environment.pop('PYTEST_ADDOPTS',None);environment.pop('PYTEST_PLUGINS',None)
    for scope,names,count,controller in (
        ('backfill',(outer.BACKFILL_TEST_NAME,),40,outer.BACKFILL_BASE_SHA),
        ('new_base_operational',OPERATIONAL_TESTS,172,outer.BACKFILL_BASE_SHA),
        ('original_provider',(PROVIDER_TEST,),71,outer.ORIGINAL_POLICY_SHA)):
        stage=area/scope;stage.mkdir(mode=0o700)
        if scope=='backfill':
            selected=[HERE/name for name in names]
        else:
            for name in assets:shutil.copyfile(HERE/'qualification-assets'/name,stage/name)
            if scope=='new_base_operational':
                shutil.copyfile(HERE/BASE_NAME,stage/OLD_BASE_NAME)
                require(digest(stage/OLD_BASE_NAME)==outer.BACKFILL_BASE_SHA,'Operational tests must exercise exact new base bytes')
            selected=[stage/name for name in names]
        timeout=900 if deadline is None else min(900,math.floor(deadline-time.time()-180))
        require(timeout>30,'Native qualification has insufficient finalization margin')
        xml=area/(scope+'.xml')
        command=[sys.executable,'-s','-m','pytest',*[str(path) for path in selected],'-q','-p','no:cacheprovider',
            '--basetemp',str(stage/'fixtures'),'--junitxml',str(xml)]
        response=subprocess.run(command,text=True,capture_output=True,timeout=timeout,env=environment)
        (area/(scope+'-stdout.txt')).write_text(response.stdout,encoding='utf8')
        (area/(scope+'-stderr.txt')).write_text(response.stderr,encoding='utf8')
        if response.returncode!=0:
            failed.append(scope);continue
        if scope!='backfill':
            for name,sha in assets.items():
                expected=outer.BACKFILL_BASE_SHA if scope=='new_base_operational' and name==OLD_BASE_NAME else sha
                require(digest(stage/name)==expected,'Staged exact regression source changed during tests')
        value,roster=junit_result(xml,count,outer.BACKFILL_ARTIFACT_MAX_BYTES)
        value.update(scope=scope,test_sources={name:assets[name] for name in names} if scope!='backfill' else {},
            tested_controller_sha256=controller,contract={'tests':count,'roster_sha256':value['test_roster_sha256']})
        if scope=='new_base_operational':value['original_operational_roster_sha256']=canonical_hash(roster)
        results.append(value)
    require(source_pins(outer)[0]==pins,'Sources changed while native regressions ran')
    require(not failed,'Regression suites failed: '+','.join(failed)+'; preserve actual stdout/stderr')
    require(results[1]['original_operational_roster_sha256']=='cae9f3349cf709af939c6953b626cf54e9d7ef1a4a0a711874960d89e939195b',
        'Full original operational roster must run against the new base')
    return results,pins


def admitted_regressions(outer,job,area,end,pins):
    """Actual accounting/admission precedes compute; fixtures remain node local."""
    require(digest(NATIVE_GUARD)==NATIVE_GUARD_SHA,'Pinned accounting guard changed')
    runpy.run_path(str(NATIVE_GUARD))['install'](area/'accounting-reads')
    from horsehair import campaign,repair
    from horsehair.budget import resource_lease
    from horsehair.execution_resources import load_resource_policy
    require(campaign._repair_native_states is repair._repair_native_states,'Accounting guard must precede campaign native-reader binding')
    guard=runpy.run_path(str(HERE/'qualification-assets/registered-snapshot-read-guard-v5-20261022.py'))
    guard['install'](area/'registered-snapshot-reads',registration_mode='controller-already-locked',allocation_deadline_unix=end)
    config=read_json(FRAMEWORK/'configs/study.json')
    require(canonical_hash(config)==outer.CONFIG,'Actual qualification configuration differs')
    old_plan=read_json(ROOT/'plans/study-af-f2-external-census-controllers-v1-20261023/plan.json')
    binding=old_plan['resource_policy']
    policy=load_resource_policy(binding['path'],binding['sha256'],root=ROOT,config=config)
    with guard['registration_lock'](area/'admission-lock',end,role='controller-start'):
        campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],area,
            'backfill-qualification-admission',resource_policy=policy,cpus=0,gpus=0,hours=1)
    temp=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temp.is_absolute() and temp.is_dir() and temp==temp.resolve() and not temp.is_symlink()
        and temp.stat().st_uid==3164768 and shutil.disk_usage(temp).free>=3_000_000_000
        and os.statvfs(temp).f_favail>=15000,'Owned allocation-local fixture capacity required')
    workspace=temp/('hhw-f2-backfill-q-'+job)
    with resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        provider_paths=outer.qualification(read_json(OLD_POLICY))
        results,test_pins=execute_suites(workspace,outer,deadline=end)
        # Keep actual output bytes and hashes on persistent storage. Temporary
        # fixture trees stay on the allocation-local filesystem.
        for result in results:
            scope=result['scope'];source=Path(result['junit']['path']);target=area/source.name
            with target.open('xb') as stream:stream.write(source.read_bytes())
            result['junit']={'path':str(target),'sha256':digest(target)}
            for suffix in ('-stdout.txt','-stderr.txt'):
                output=workspace/(scope+suffix)
                with (area/output.name).open('xb') as stream:stream.write(output.read_bytes())
        fixture_proof={'fixtures_allocation_local':True,'workspace':str(workspace),'SLURM_TMPDIR':str(temp),
            'source_pins_verified_before_and_after':test_pins,'fixture_quota_is_admission_authority':False}
        exclusive(area/'local-fixture-source-proof.json',fixture_proof)
        require(all(digest(Path(path))==sha for path,sha in {**pins,**provider_paths}.items()),'Actual source closure changed during admitted tests')
        require(time.time()+180<end,'Native qualification publication margin exhausted')
    return results,provider_paths,policy.receipt(),{'path':str(area/'backfill-qualification-admission.json'),
        'sha256':digest(area/'backfill-qualification-admission.json')},{'path':str(area/'local-fixture-source-proof.json'),
        'sha256':digest(area/'local-fixture-source-proof.json')}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('native-plan','native-ledger','native-script'):parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--native-task',required=True);args=parser.parse_args(argv)
    outer=outer_module();job=str(native_job_id() or '')
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    require(HERE==outer.BACKFILL_SUPPORT and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and socket.getfqdn().endswith('.narval.calcul.quebec') and job.isdigit(),'Actual owned Narval qualification allocation required')
    require(os.environ.get('SLURM_CPUS_PER_TASK')=='1' and os.environ.get('HHW_GPU_COUNT')=='0'
        and os.environ.get('HHW_MAX_HOURS')=='1','One-CPU scalar one-hour qualification environment required')
    pins,_=source_pins(outer)
    import horsehair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and source_identity(FRAMEWORK)['sha256']==outer.SOURCE
        and source_identity(PRIMARY)['sha256']=='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630',
        'Actual qualified framework or preserved primary source differs')
    registration={'plan':outer.locator(args.native_plan),'ledger':outer.locator(args.native_ledger),
        'script':outer.locator(args.native_script),'task_name':args.native_task,'native_job_id':job}
    task,tail=outer.registered(registration['plan'],registration['ledger'],args.native_task,registration['script'],job,
        helper=Path(__file__),hours=1,cpus=1,memory='64G')
    require(tail==list(sys.argv[1:] if argv is None else argv),'Actual qualifier invocation differs from frozen registered command')
    closure=task.get('backfill_qualification_sources',{});assets=read_json(HERE/'qualification-asset-pins.json')
    expected_names={BASE_NAME,OUTER_NAME,Path(__file__).name,outer.BACKFILL_TEST_NAME,'qualification-asset-pins.json',
        'build-backfill-native-plans-v2-20261024.py'}|{'qualification-assets/'+name for name in assets}
    require(closure.get('kind')=='f2-backfill-controller-transport-source-manifest-v2'
        and closure.get('native_support')==str(HERE) and closure.get('native_framework')==str(FRAMEWORK)
        and set(closure.get('files',{}))==expected_names
        and all(digest(HERE/name)==sha for name,sha in closure['files'].items()),
        'Actual native qualifier source closure differs from immutable registered task')
    require(digest(SCHEDULER)==SCHEDULER_SHA,'Pinned native scheduler reader differs')
    area=HERE/('native-qualification-'+job);area.mkdir(mode=0o700,exist_ok=False)
    response=runpy.run_path(str(SCHEDULER))['run_scontrol_job'](job,area/'scheduler-read')
    response.check_returncode()
    pairs=__import__('re').findall(r'(?<!\S)(\w+)=([^\s]*)',response.stdout.strip());fields=dict(pairs)
    require(len(response.stdout.strip().splitlines())==1 and len(pairs)==len(fields),'One nonduplicate actual native allocation required')
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
        and fields.get('Account')=='def-ptewarie_cpu' and fields.get('JobState')=='RUNNING'
        and fields.get('Command')==str(args.native_script) and fields.get('JobName')=='hhw-'+args.native_task
        and fields.get('NumCPUs')=='1' and fields.get('NumNodes') in {'1','1-1'}
        and fields.get('TimeLimit') in {'01:00:00','1:00:00','0-01:00:00'}
        and fields.get('MinMemoryNode') in {'64G','65536M'} and 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields
        and all('gpu' not in fields.get(key,'').lower() for key in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Exact real owned one-CPU64G one-hour scalar qualification allocation required')
    end=datetime.fromisoformat(fields.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp()
    env_end=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(end) and math.isfinite(env_end) and abs(end-env_end)<=2 and end>time.time()+600,
        'Actual native qualification end and environment must agree with finite margin')
    require(digest(OLD_POLICY)==OLD_POLICY_SHA,'Preserved original provider policy differs')
    # The original tests may choose these existing native helper paths. Pin the
    # actual selected files as well as the identical staged fallback bytes.
    assets=read_json(HERE/'qualification-asset-pins.json')
    for path,name in ((ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py','native-accounting-read-guard-20261021.py'),
        (ROOT/'maintenance/sixth-repair-support-20261016/controller-primary-repair-sixth-20261016.py','controller-primary-repair-sixth-20261016.py')):
        require(digest(path)==assets[name],'Actual native regression helper differs from frozen asset');pins[str(path)]=assets[name]
    results,provider_paths,policy_receipt,admission_proof,fixture_proof=admitted_regressions(outer,job,area,end,pins)
    require(all(digest(Path(path))==sha for path,sha in {**pins,**provider_paths}.items()),'Actual source closure changed during qualification')
    backfill=results[0];backfill['source']={'path':str(HERE/outer.BACKFILL_TEST_NAME),'sha256':outer.BACKFILL_TEST_SHA}
    suite_contract={'source':outer.BACKFILL_TEST_NAME,'source_sha256':outer.BACKFILL_TEST_SHA,
        'tests':40,'roster_sha256':backfill['test_roster_sha256']}
    report={'status':'complete_f2_backfill_controller_qualification','returncode':0,'slurm_job_id':job,
        'source_sha256':outer.SOURCE,'config_sha256':outer.CONFIG,'source_verified_unchanged':True,
        'runtime_contract':outer.BACKFILL_RUNTIME,'original_base':{'path':str(outer.BASE),'sha256':outer.BASE_SHA},
        'original_policy':{'path':str(outer.ORIGINAL_POLICY_SOURCE),'sha256':outer.ORIGINAL_POLICY_SHA},
        'provider_policy':{'path':str(OLD_POLICY),'sha256':OLD_POLICY_SHA},
        'sources':{'base':{'path':str(HERE/BASE_NAME),'sha256':outer.BACKFILL_BASE_SHA},
            'outer':{'path':str(HERE/OUTER_NAME),'sha256':digest(HERE/OUTER_NAME)}},
        'source_pins':{**pins,**provider_paths},'suite':backfill,'suite_contract':suite_contract,
        'resource_policy':policy_receipt,'native_accounting_admission':admission_proof,'local_fixture_source_proof':fixture_proof,
        'regression_suites':results[1:],'native_registration':registration,'actual_native_allocation':fields,
        'qualification_helper':{'path':str(Path(__file__).resolve()),'sha256':digest(Path(__file__))},
        'native_completion_required_separately':True,**{flag:False for flag in outer.FALSE_FLAGS}}
    # Publish only evidence whose persisted actual bytes already meet the exact
    # production consumer's finite artifact limits. This is independent of its
    # later positive finalized native-completion requirement.
    report['persisted_artifact_bounds']=outer.qualification_artifact_bounds(report)
    outer.backfill_evidence(report)
    exclusive(area/'qualification-artifact-bounds.json',report['persisted_artifact_bounds'])
    exclusive(area/'qualification.json',report)
    print(json.dumps({'status':report['status'],'report':{'path':str(area/'qualification.json'),'sha256':digest(area/'qualification.json')},
        'native_completion_required_separately':True},sort_keys=True),flush=True)


if __name__=='__main__':main()
