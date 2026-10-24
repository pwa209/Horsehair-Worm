"""Native tiny generic qualification; no complete census or fresh-census prerequisite."""
from pathlib import Path
from datetime import datetime,timezone
import argparse
import hashlib
import importlib.util
import json
import os
import re
import runpy
import signal
import subprocess
import sys
import time

ROOT=Path('/scratch/pwa209/horsehair-worm')
AREA=ROOT/'maintenance/study-census-resilience-support-v1-20261024'
OUTPUT=ROOT/'maintenance/study-census-resilience-qualification-v1-20261024'


def require(value,message):
    if not value:raise ValueError(message)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('source-manifest','native-plan','native-ledger','native-script'):p.add_argument('--'+name,type=Path,required=True)
    for name in ('source-manifest-sha256','native-task'):p.add_argument('--'+name,required=True)
    a=p.parse_args();require(Path(__file__).resolve()==AREA/Path(__file__).name,'Exact new qualification namespace required')
    raw=a.source_manifest.read_bytes()
    require(a.source_manifest==AREA/'source-manifest.json' and len(raw)<=200_000
        and hashlib.sha256(raw).hexdigest()==a.source_manifest_sha256,'Pinned bounded source manifest required before import')
    early=json.loads(raw);helper=AREA/'run-fresh-storage-census-resilience-v1-20261024.py'
    require(hashlib.sha256(helper.read_bytes()).hexdigest()==early['files'][helper.name],
        'Pinned provider bytes required before module import')
    spec=importlib.util.spec_from_file_location('resilient_provider',AREA/'run-fresh-storage-census-resilience-v1-20261024.py')
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    manifest,pins=m.source_manifest(a.source_manifest,a.source_manifest_sha256)
    job=str(m.native_job_id() or '')
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='4' and os.environ.get('HHW_GPU_COUNT')=='0'
        and os.environ.get('HHW_MAX_HOURS')=='1','Owned finite4CPU1h qualification context required')
    require(subprocess.check_output(['hostname','-f'],text=True,timeout=10).strip().endswith('.narval.calcul.quebec'),'Narval required')
    import horsehair
    require(Path(horsehair.__file__).resolve()==m.FRAMEWORK/'src/horsehair/__init__.py'
        and m.source_identity(m.FRAMEWORK)['sha256']==m.SOURCE,'Exact qualified4f interpreter required')
    config=m.read_json(m.PRIMARY/'configs/study.json');require(m.canonical_hash(config)==m.CONFIG,'Unchanged full configuration required')
    OUTPUT.mkdir(mode=0o700,exist_ok=True);case=OUTPUT/('native-'+job);case.mkdir(mode=0o700,exist_ok=False)
    for path,pin in ((m.GUARD,m.GUARD_SHA),(m.SCHEDULER,m.SCHEDULER_SHA)):
        require(m.digest(path)==pin,'Pinned native reader changed')
    runpy.run_path(str(m.GUARD))['install'](case/'accounting-reads')
    reader=runpy.run_path(str(m.SCHEDULER))['run_scontrol_job'];response=reader(job,case/'scheduler-reads');response.check_returncode()
    f=dict(re.findall(r'(?<!\S)(\w+)=([^\s]*)',response.stdout))
    require(f.get('JobId')==job and f.get('UserId')=='pwa209(3164768)' and f.get('Account')=='def-ptewarie_cpu'
        and f.get('JobState')=='RUNNING' and f.get('NumCPUs')=='4' and f.get('NumNodes') in {'1','1-1'}
        and f.get('MinMemoryNode') in {'16G','16384M'} and f.get('TimeLimit') in {'01:00:00','1:00:00','0-01:00:00'}
        and f.get('Command')==str(a.native_script) and f.get('JobName')=='hhw-'+a.native_task
        and all('gpu' not in f.get(k,'').lower() for k in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Exact owned registered4CPU16G1h allocation required')
    end=datetime.fromisoformat(f['EndTime']).replace(tzinfo=timezone.utc).timestamp()
    require(abs(float(os.environ['SLURM_JOB_END_TIME'])-end)<=2 and end>time.time()+600,'Verified native end/reserve required')
    def expired(signum,frame):raise TimeoutError('Finite generic qualification deadline; no census authority')
    signal.signal(signal.SIGALRM,expired);signal.alarm(max(1,int(end-time.time()-600)))
    registration={'plan':{'path':str(a.native_plan),'sha256':m.digest(a.native_plan)},
        'ledger':{'path':str(a.native_ledger),'sha256':m.digest(a.native_ledger)},
        'script':{'path':str(a.native_script),'sha256':m.digest(a.native_script)},'task_name':a.native_task,'native_job_id':job}
    m.qualification_registration(registration,job,manifest_path=a.source_manifest,manifest_sha=a.source_manifest_sha256)
    from horsehair import repair
    observed=repair._repair_native_states(['6863966'],{},owner='pwa209',account='def-ptewarie_cpu')
    old=m.LEGACY_BINDING;legacy=m.legacy_module().kernel_qualification(Path(old['qualification']['path']),old['qualification']['sha256'],'6863966',
        Path(old['source_manifest']['path']),old['source_manifest']['sha256'],observed)
    policy=m.load_resource_policy(m.POLICY,m.POLICY_SHA,root=ROOT,config=config)
    temporary=Path(os.environ.get('SLURM_TMPDIR',''));require(temporary.is_absolute() and temporary.is_dir()
        and not temporary.is_symlink() and temporary.stat().st_uid==3164768,'Owned native local fixtures required')
    workspace=temporary/('census-resilience-tests-'+job);workspace.mkdir(mode=0o700,exist_ok=False)
    suite=manifest['suites']['resilience'];xml=case/'resilience.xml'
    env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1','HHW_TEST_WORKSPACE':str(workspace),'HHW_TEST_RESULT_DIR':str(case)}
    # Generic qualification must start while the real census pair is stale.
    # The resource lease enforces approved global CPU/GPU bounds; no storage
    # admission is fabricated from these tiny allocation-local test fixtures.
    with m.resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        with (case/'stdout.log').open('xb') as out,(case/'stderr.log').open('xb') as err:
            result=subprocess.run([sys.executable,'-B',str(AREA/suite['source'])],cwd=workspace,env=env,
                stdout=out,stderr=err,timeout=min(600,end-time.time()-600),check=False)
        require(result.returncode==0,'Actual native generic suite failed')
        count=m.read_json(case/'test-result.json')
        require(count['tests']==suite['tests'] and count['failures']==count['errors']==count['skipped']==0
            and count['successful'] is True and count['tests_sha256']==suite['source_sha256']
            and count['provider_sha256']==m.digest(AREA/m.WRAPPER_NAME),'Exact complete actual generic test result required')
        m.junit_roster(xml,suite['tests'],suite['roster_sha256'],m.digest(xml))
        require(m.source_manifest(a.source_manifest,a.source_manifest_sha256)==(manifest,pins),'Sources changed during qualification')
        q={'status':'complete_census_resilience_operational_qualification','returncode':0,'slurm_job_id':job,
            'source_sha256':m.SOURCE,'config_sha256':m.CONFIG,'source_verified_unchanged':True,'source_pins':pins,
            'source_manifest':{'path':str(a.source_manifest),'sha256':a.source_manifest_sha256},'legacy_binding':m.LEGACY_BINDING,
            'qualification_helper':{'path':str(Path(__file__)),'sha256':m.digest(Path(__file__))},
            'suite':{'source':{'path':str(AREA/suite['source']),'sha256':suite['source_sha256']},
                'test_counts':{'tests':suite['tests'],'failures':0,'errors':0,'skipped':0},'test_roster_sha256':suite['roster_sha256'],
                'junit':{'path':str(xml),'sha256':m.digest(xml)}},'native_registration':registration,
            'native_allocation':{'fields':f,'native_stdout':response.stdout},'resource_policy':policy.receipt(),
            'legacy_qualification':legacy,'native_completion_required_separately':True,
            'fresh_census_required_to_run_generic_qualification':False,**{k:False for k in m.FALSE_FLAGS}}
        m.exclusive(case/'qualification.json',q);m.exclusive(OUTPUT/'qualification.json',q)
    print(json.dumps({'status':q['status'],'qualification':str(OUTPUT/'qualification.json'),'sha256':m.digest(OUTPUT/'qualification.json')},sort_keys=True))
    signal.alarm(0)


if __name__=='__main__':main()
