"""Private native operational/recovery qualification; no state migration or scheduler writes."""
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

from horsehair.core import read_json,digest,canonical_hash,source_identity,native_job_id,atomic_json
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy
from horsehair.slurm import completed_native_dependency

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
SUPPORT=ROOT/'maintenance/study-af-registry-repair-support-v6-20261022'
CASE=SUPPORT/'native-qualification'
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
FRAMEWORK_Q=ROOT/'maintenance/study-af-execution-support-v1-20261021/qualification.json'
FRAMEWORK_Q_SHA='29ed530f3783f3c35c3cf20343bdb663732ec2bf4bf5c3218f8b029510becc2f'
PINS={'registered-snapshot-read-guard-v5-20261022.py': '7bd2bf562d0a81887ef1afd308ddd9265e5cadb23ba4b2e5946980b659505431', 'run-f00d-worker-registry-overlay-20261021.py': 'dd39905ce44f89f7f8bbdd325d1a0f1d5ec7950dd89d0e3bb267f6fdfe77a09a', 'run-af-agent-controller-registry-v6-20261022.py': 'e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866', 'test_registered_snapshot_read_guard_v5_20261022.py': '934196d84276299ce2b567bef6925515e4656978401a0dd4f49c8bd2d1339d72', 'test_f00d_worker_registry_overlay_20261021.py': '2fdcf83a1abc796f82e86d3d3014fd785d195bf7ca4dc6457f2d6dd5db9b3968', 'test_af_agent_controller_registry_v6_20261022.py': 'da703c4cc8b172a2ea79067a8f5c5e9d0976853823a8ed230e3fb1a721633a3d', 'recover-f2-training0000-state-20261021.py': '504eb2764073a5b614d986827ccacad3189ea4be5988313ead47ab41805fe26a', 'test_recover_f2_training0000_state_20261021.py': '6cb7fdcc55f12164d81666f70e545b6efd610a1299baef66e483a271fbbe1673', 'recover-f2-prestart-three-state-v1-20261022.py': '7151017a3579694228b1ba78f14da9cdda3e4a4608d98f9f257d2d2c4b26807d', 'test_recover_f2_prestart_three_state_v1_20261022.py': 'dddccdc4d800110ecfbdf72f8a3fb25f7726a858d80272c016cce3c233b4253d', 'activate-f2-prestart-three-v1-20261022.py': '586bbf301ef69621cc1132b8b25a9c243f29a2e918b54d564e84be816af2d2eb', 'test_activate_f2_prestart_three_v1_20261022.py': '7969911a11d54c71c33c41f9888f4d3889db5326489ff6468399823640d54964'}
MIGRATION_ROSTER='60eb41a79c2a23b9c29460b8022eb81149db3455c3b200ba1700512f273844f0'
NEW_RECOVERY_COUNT=86
NEW_RECOVERY_ROSTER='a0bc49565ca6163d108de11ef76ec882a71745cb9075768d5f0347bca237cfaa'
ACTIVATION_COUNT=64
ACTIVATION_ROSTER='c2b3dcf5200995bc40a549ab172e60273e11f120ef3d3e36ed09dfc804ba03be'
CONTROLLER='run-af-agent-controller-registry-v6-20261022.py'
OVERLAY_TEST_NAMES=('test_registered_snapshot_read_guard_v5_20261022.py',
    'test_f00d_worker_registry_overlay_20261021.py','test_af_agent_controller_registry_v6_20261022.py')
NEW_RECOVERY_HELPER='recover-f2-prestart-three-state-v1-20261022.py'
NEW_RECOVERY_TEST='test_recover_f2_prestart_three_state_v1_20261022.py'
ACTIVATION_HELPER='activate-f2-prestart-three-v1-20261022.py'
ACTIVATION_TEST='test_activate_f2_prestart_three_v1_20261022.py'


def require(value,message):
    if not value:raise ValueError(message)


def ordinary(path,namespace,*,exists=True):
    p=Path(path);resolved=p.resolve()
    require(p.is_absolute() and p==resolved and p!=namespace and p.is_relative_to(namespace)
        and all(not q.is_symlink() for q in [p,*p.parents] if q.is_relative_to(namespace)),
        'Exact ordinary personal path required')
    if exists:require(p.is_file(),'Required immutable file missing')
    return p


def verify_pins():
    require(all(isinstance(sha,str) and re.fullmatch('[0-9a-f]{64}',sha) for sha in PINS.values()),'Private source/test byte pins are not frozen')
    for name,sha in PINS.items():require(digest(ordinary(SUPPORT/name,ROOT/'maintenance'))==sha,'Private source/test changed: '+name)
    for p,sha in [(NATIVE_GUARD,NATIVE_GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(QUOTA,QUOTA_SHA),(DRIVER,DRIVER_SHA),(FRAMEWORK_Q,FRAMEWORK_Q_SHA)]:
        require(digest(ordinary(p,ROOT/'maintenance'))==sha,'Pinned qualified native helper changed')


def qualification_contract():
    """Load operational counts only after every private source byte is checked."""
    verify_pins()
    for count,roster in ((NEW_RECOVERY_COUNT,NEW_RECOVERY_ROSTER),(ACTIVATION_COUNT,ACTIVATION_ROSTER)):
        require(type(count) is int and count>0 and isinstance(roster,str) and re.fullmatch('[0-9a-f]{64}',roster),
            'Separate recovery/activation testcase roster is not frozen')
    controller=runpy.run_path(str(SUPPORT/CONTROLLER),run_name='private_native_q_contract')
    count=controller.get('OPERATIONAL_TEST_COUNT');roster=controller.get('OPERATIONAL_TEST_ROSTER_SHA256')
    require(type(count) is int and count>0 and isinstance(roster,str) and re.fullmatch('[0-9a-f]{64}',roster)
        and controller.get('OPERATIONAL_TEST_SOURCES')=={n:PINS[n] for n in OVERLAY_TEST_NAMES},
        'Exact source-bound controller operational testcase roster is not frozen')
    return controller,count,roster


def junit_roster(path,count,expected_sha):
    raw=Path(path).read_bytes()
    require(len(raw)<=2_000_000 and b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Bounded ordinary JUnit required')
    cases=list(ET.fromstring(raw).iter('testcase'))
    roster=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in cases)
    require(len(roster)==len(set(roster))==count and all(a and b for a,b in roster)
        and canonical_hash(roster)==expected_sha and all(not any(c.find(k) is not None for k in ('failure','error','skipped')) for c in cases),
        'Exact complete passing private testcase roster required')
    from horsehair.cli import _qualification_junit
    require(_qualification_junit(path)=={'tests':count,'failures':0,'errors':0,'skipped':0},'Actual private JUnit counts differ')
    return {'path':str(path),'sha256':digest(path)}


def registered_task(plan_path,ledger_path,task_name,script,job,config):
    plan_path=ordinary(plan_path,ROOT/'plans');ledger_path=ordinary(ledger_path,ROOT/'state');script=ordinary(script,ROOT/'plans')
    namespace=runpy.run_path(str(DRIVER),run_name='private_registry_qualification_binding_only')
    plan,ledger=namespace['checked_pair'](plan_path,ledger_path,ROOT,config)
    tasks=[t for t in plan['tasks'] if t['name']==task_name];require(len(tasks)==1,'Unique registered native task required')
    task=tasks[0];record=ledger.get('tasks',{}).get(task_name,{})
    require(plan.get('release_identity',{}).get('sha256')==SOURCE and task.get('script')==str(script)
        and canonical_hash(script.read_text())==task.get('script_sha256')==record.get('script_sha256')
        and record.get('job_id')==job and record.get('state')=='submitted'
        and task.get('resources')=={'cpus':4,'gpu':False,'hours':1,'memory':'16G'}
        and str(Path(__file__)) in shlex.split(task['command']) and task['command'] in script.read_text(),
        'Actual plan/ledger/script/native identity does not bind this qualification')
    return {'plan':{'path':str(plan_path),'sha256':digest(plan_path)},'ledger':{'path':str(ledger_path),'sha256':digest(ledger_path)},
        'script':{'path':str(script),'sha256':digest(script)},'task_name':task_name,'native_job_id':job}


def native_owner(job):
    response=runpy.run_path(str(SCHEDULER))['run_scontrol_job'](job,CASE/'scheduler-reads');response.check_returncode()
    lines=[s for s in response.stdout.splitlines() if s.strip()];require(len(lines)==1,'One native allocation required')
    pairs=re.findall(r'(?<!\S)(\w+)=([^\s]*)',lines[0]);v=dict(pairs)
    match=re.fullmatch(r'(?:(\d+)-)?(\d+):(\d{2}):(\d{2})',v.get('TimeLimit',''))
    require(match is not None,'Finite native time limit required')
    d,h,m,s=map(lambda x:int(x or 0),match.groups())
    require(len(v)==len(pairs) and v.get('JobId')==job and v.get('UserId')=='pwa209(3164768)'
        and v.get('Account')=='def-ptewarie_cpu' and v.get('JobState')=='RUNNING'
        and v.get('NumCPUs')=='4' and v.get('NumNodes') in {'1','1-1'} and d*86400+h*3600+m*60+s==3600,
        'Exact owned4CPU1h native allocation required')
    require(all('gpu' not in v.get(k,'').lower() for k in ('TresPerNode','TresPerJob','TresPerTask','AllocTRES','ReqTRES','Gres')),'No GPU permitted')
    memory=v.get('MinMemoryNode','');require(memory in {'16G','16384M'},'Exact16GB native node memory required')
    end=datetime.fromisoformat(v['EndTime']).replace(tzinfo=timezone.utc).timestamp();env=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(env) and abs(env-end)<=2 and end>time.time()+600,'Verified native finalization margin required')
    return v,end


def pytest_environment():
    env={**os.environ,'TZ':'UTC','PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1'}
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):env.pop(key,None)
    return env


def require_fresh_census(record,*,now=None):
    now=time.time() if now is None else now
    measured=record.get('measured_unix')
    require(type(measured) in (int,float) and math.isfinite(measured),'Finite actual storage census time required')
    captured=datetime.fromisoformat(record.get('captured_utc','').replace('Z','+00:00'))
    require(captured.tzinfo is not None and 0<=now-measured<21600 and 0<=now-captured.timestamp()<21600,
        'Exact storage census must remain fresh within six hours')


def run_tests(names,label,workspace,end):
    xml=CASE/(label+'.xml')
    command=[sys.executable,'-s','-m','pytest',*[str(SUPPORT/n) for n in names],'-q','-p','no:cacheprovider',
        '--basetemp',str(workspace/label),'--junitxml',str(xml)]
    atomic_json(CASE/(label+'-intent.json'),{'command':command,'test_sources':{n:PINS[n] for n in names},'scientific_result':False})
    timeout=min(2400,end-time.time()-180);require(timeout>60,'Insufficient native test finalization margin')
    with (CASE/(label+'-stdout.txt')).open('xb') as out,(CASE/(label+'-stderr.txt')).open('xb') as err:
        result=subprocess.run(command,cwd=FRAMEWORK,stdout=out,stderr=err,timeout=timeout,env=pytest_environment(),check=False)
    require(result.returncode==0,'Actual private native suite failed: '+label)
    return xml


def main():
    p=argparse.ArgumentParser();p.add_argument('--native-plan',required=True,type=Path);p.add_argument('--native-ledger',required=True,type=Path)
    p.add_argument('--native-task',required=True);p.add_argument('--native-script',required=True,type=Path);a=p.parse_args()
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    config=read_json(PRIMARY/'configs/study.json');job=str(native_job_id() or '')
    require(job.isdigit() and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='4','Owned native qualification required')
    import horsehair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py' and source_identity(FRAMEWORK)['sha256']==SOURCE
        and source_identity(PRIMARY)['sha256']==PRIMARY_SHA and canonical_hash(config)==CONFIG,'Exact qualified source/frozen science required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval required')
    controller,overlay_count,overlay_roster=qualification_contract()
    require(not CASE.exists() and not (SUPPORT/'private-qualification.json').exists(),'Fresh private qualification only')
    CASE.mkdir(mode=0o700)
    allocation,end=native_owner(job)
    runpy.run_path(str(NATIVE_GUARD))['install'](CASE/'accounting-reads')
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'358 must precede campaign binding')
    runpy.run_path(str(SUPPORT/'registered-snapshot-read-guard-v5-20261022.py'))['install'](CASE/'registered-snapshot-reads',
        registration_mode='worker-lock',allocation_deadline_unix=end)
    registration=registered_task(a.native_plan,a.native_ledger,a.native_task,a.native_script,job,config)
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],CASE,'native-start-admission',
        resource_policy=policy,cpus=0,gpus=0,hours=1)
    q=read_json(FRAMEWORK_Q)
    require(q.get('status')=='complete' and q.get('source_sha256')==SOURCE and q.get('returncode')==0
        and q.get('config_sha256')==CONFIG and q.get('test_counts')=={'tests':1694,'failures':0,'errors':0,'skipped':0}
        and completed_native_dependency(ROOT,str(q['slurm_job_id'])) is not None,'Positive original public1694 qualification required')
    temp=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temp.is_absolute() and temp.is_dir() and temp==temp.resolve() and not temp.is_symlink() and temp.stat().st_uid==3164768
        and shutil.disk_usage(temp).free>=3_000_000_000 and os.statvfs(temp).f_favail>=15000,'Actual native local fixture capacity required')
    workspace=temp/('hhw-f2-registry-q-'+job);workspace.mkdir(mode=0o700,exist_ok=False)
    with resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        quota=runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,required_bytes=10_000_000_000,
            reserve_files=248000,reserve_bytes=2_000_000_000_000,evidence_dir=CASE/'quota-reads',allocation_deadline_unix=end)
        from horsehair.admission import _storage
        census_path=ROOT/'state/storage-budget.json';census_sha=digest(census_path)
        require_fresh_census(read_json(census_path))
        future_bytes,future_files,paths=_storage(ROOT,config,10_000_000_000,2000,personal_headroom=quota)
        require(digest(census_path)==census_sha,'Storage census changed during quota validation')
        atomic_json(CASE/'storage-proof.json',{'census_sha256':census_sha,'future_study_bytes':future_bytes,'future_study_files':future_files,
            'source_paths':list(map(str,paths)),'fixtures_allocation_local':True})
        overlay_names=list(OVERLAY_TEST_NAMES)
        xml=run_tests(overlay_names,'overlay'+str(overlay_count),workspace,end)
        result={'status':'complete_f2_registry_overlay_qualification','returncode':0,'slurm_job_id':job,'source_sha256':SOURCE,'config_sha256':CONFIG,
            'controller_sha256':PINS[CONTROLLER],
            'registry_guard_sha256':PINS['registered-snapshot-read-guard-v5-20261022.py'],
            'worker_shim_sha256':PINS['run-f00d-worker-registry-overlay-20261021.py'],
            'test_counts':{'tests':overlay_count,'failures':0,'errors':0,'skipped':0},'test_roster_sha256':overlay_roster,
            'test_sources':{n:{'path':str(SUPPORT/n),'sha256':PINS[n]} for n in overlay_names},'junit':{'path':str(xml),'sha256':digest(xml)},
            'native_registration':registration,'allocation':allocation,'resource_policy':policy.receipt(),
            'qualification_helper_sha256':digest(Path(__file__)),'scientific_result':False,'admission_authority':False}
        controller['verify_operational_tests'](result)
        migration_xml=run_tests(['test_recover_f2_training0000_state_20261021.py'],'migration42',workspace,end)
        result['migration_qualification']={'helper':{'path':str(SUPPORT/'recover-f2-training0000-state-20261021.py'),
            'sha256':PINS['recover-f2-training0000-state-20261021.py']},
            'test_source':{'path':str(SUPPORT/'test_recover_f2_training0000_state_20261021.py'),'sha256':PINS['test_recover_f2_training0000_state_20261021.py']},
            'junit':junit_roster(migration_xml,42,MIGRATION_ROSTER),'test_counts':{'tests':42,'failures':0,'errors':0,'skipped':0}}
        new_recovery_xml=run_tests([NEW_RECOVERY_TEST],'prestart-recovery'+str(NEW_RECOVERY_COUNT),workspace,end)
        result['new_recovery_qualification']={'helper':{'path':str(SUPPORT/NEW_RECOVERY_HELPER),'sha256':PINS[NEW_RECOVERY_HELPER]},
            'test_source':{'path':str(SUPPORT/NEW_RECOVERY_TEST),'sha256':PINS[NEW_RECOVERY_TEST]},
            'junit':junit_roster(new_recovery_xml,NEW_RECOVERY_COUNT,NEW_RECOVERY_ROSTER),
            'test_counts':{'tests':NEW_RECOVERY_COUNT,'failures':0,'errors':0,'skipped':0},'test_roster_sha256':NEW_RECOVERY_ROSTER}
        activation_xml=run_tests([ACTIVATION_TEST],'activation'+str(ACTIVATION_COUNT),workspace,end)
        result['activation_qualification']={'helper':{'path':str(SUPPORT/ACTIVATION_HELPER),'sha256':PINS[ACTIVATION_HELPER]},
            'test_source':{'path':str(SUPPORT/ACTIVATION_TEST),'sha256':PINS[ACTIVATION_TEST]},
            'junit':junit_roster(activation_xml,ACTIVATION_COUNT,ACTIVATION_ROSTER),
            'test_counts':{'tests':ACTIVATION_COUNT,'failures':0,'errors':0,'skipped':0},'test_roster_sha256':ACTIVATION_ROSTER}
        verify_pins();require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,'Source changed during private tests')
        atomic_json(SUPPORT/'private-qualification.json',result)
        print('AF_F2_REGISTRY_QUALIFIED '+json.dumps({'job_id':job,'overlay_tests':overlay_count,'migration_tests':42,
            'new_recovery_tests':NEW_RECOVERY_COUNT,'activation_tests':ACTIVATION_COUNT,
            'qualification_sha256':digest(SUPPORT/'private-qualification.json')},sort_keys=True),flush=True)


if __name__=='__main__':main()
