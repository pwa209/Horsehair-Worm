"""Run real generic consumer fixtures in an owned finite native allocation."""
from pathlib import Path
from datetime import datetime,timezone
import argparse
import hashlib
import importlib.util
import json
import os
import re
import runpy
import shlex
import subprocess
import sys
import time

ROOT=Path('/scratch/pwa209/horsehair-worm')
SUPPORT=ROOT/'maintenance/study-census-resilience-consumers-v1-20261024'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
RUNTIME='run-census-resilience-consumer-v1-20261024.py'
SELF='qualify-census-resilience-consumers-v1-20261024.py'
POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=GUARD.with_name('native-scheduler-read.py')
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'


def require(value,message):
    if not value:raise ValueError(message)


def save(path,value):
    raw=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def main():
    os.environ['TZ']='UTC';time.tzset()
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('native-plan','native-ledger','native-script','source-manifest'):p.add_argument('--'+name,type=Path,required=True)
    for name in ('native-task','source-manifest-sha256'):p.add_argument('--'+name,required=True)
    a=p.parse_args()
    require(Path(__file__).resolve()==SUPPORT/SELF,'Exact reviewed qualifier source required')
    require(os.geteuid()==3164768 and os.environ.get('USER')=='pwa209','Owned native user required')
    require(subprocess.check_output(['hostname','-f'],text=True,timeout=10).strip().endswith('.narval.calcul.quebec'),'Narval allocation required')
    spec=importlib.util.spec_from_file_location('actual_census_resilience_consumer',SUPPORT/RUNTIME)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    binding={'path':str(a.source_manifest),'sha256':a.source_manifest_sha256}
    manifest,pins=m.source_manifest(binding)
    import horsehair
    from horsehair.core import native_job_id,source_identity,read_json
    from horsehair.budget import resource_lease
    from horsehair.execution_resources import load_resource_policy
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py' and source_identity(FRAMEWORK)['sha256']==SOURCE,'Exact unchanged4f interpreter/source required')
    config=read_json(FRAMEWORK/'configs/study.json');require(m.canonical(config)==CONFIG,'Unchanged full configuration required')
    job=str(native_job_id() or '');require(re.fullmatch('[1-9][0-9]*',job),'Actual scalar job identity required')
    require(os.environ.get('SLURM_CPUS_PER_TASK')=='4' and os.environ.get('HHW_GPU_COUNT')=='0' and os.environ.get('HHW_MAX_HOURS')=='1','Exact bounded4CPU1h qualification requested')
    case=SUPPORT/('native-qualification-'+job);case.mkdir(mode=0o700,exist_ok=False)
    for path,sha in [(GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA)]:require(m.digest(path)==sha,'Pinned native reader source changed')
    runpy.run_path(str(GUARD))['install'](case/'accounting-reads')
    run_scheduler=runpy.run_path(str(SCHEDULER))['run_scontrol_job']
    fields=m.native_fields(run_scheduler(job,case/'scheduler-reads'))
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)' and fields.get('Account')=='def-ptewarie_cpu'
        and fields.get('JobState')=='RUNNING' and fields.get('NumCPUs')=='4' and fields.get('NumNodes') in {'1','1-1'}
        and fields.get('Command')==str(a.native_script) and fields.get('JobName')=='hhw-'+a.native_task
        and fields.get('MinMemoryNode') in {'16G','16384M'} and m.seconds(fields.get('TimeLimit',''))==3600
        and 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields
        and all('gpu' not in fields.get(k,'').lower() for k in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Exact owned scalar4CPU16G1h native qualifier allocation required')
    end=datetime.fromisoformat(fields['EndTime']).replace(tzinfo=timezone.utc).timestamp()
    require(abs(float(os.environ['SLURM_JOB_END_TIME'])-end)<=2 and time.time()+600<end,'Verified native finalization margin required')
    reg={'plan':m.locator(a.native_plan),'ledger':m.locator(a.native_ledger),'script':m.locator(a.native_script),'task_name':a.native_task,'native_job_id':job}
    argv=sys.argv[1:]
    task=m.registration(reg['plan'],reg['ledger'],reg['script'],a.native_task,job,entry=SUPPORT/SELF,argv=argv,source=SOURCE,cpus=4,hours=1,memory='16G')
    contract=manifest['suites']['consumers']
    require(task.get('census_resilience_consumer_qualification')=={'helper':{'path':str(SUPPORT/SELF),'sha256':manifest['files'][SELF]},'source_manifest':binding,'suite':contract},'Exact registered source/manifest/suite contract required')
    temporary=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temporary.is_absolute() and temporary.is_dir() and not temporary.is_symlink() and temporary.stat().st_uid==3164768,'Owned allocation-local fixtures required')
    workspace=temporary/('census-resilience-consumers-'+job);workspace.mkdir(mode=0o700,exist_ok=False)
    xml=case/'consumers.xml';suite=SUPPORT/contract['source']
    command=[sys.executable,'-s','-B','-m','pytest',str(suite),'-q','-p','no:cacheprovider','--basetemp',str(workspace),'--junitxml',str(xml)]
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    # These allocation-local generic fixtures may run while census is stale.
    # Production storage, freshness and scientific admission remain mandatory.
    with resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        save(case/'test-intent.json',{'command':command,'source_manifest':binding,'source_pins':pins,'native_registration':reg,'allocation':fields,**m.FALSE_FLAGS})
        with (case/'stdout.log').open('xb') as out,(case/'stderr.log').open('xb') as err:
            result=subprocess.run(command,cwd=workspace,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'},stdout=out,stderr=err,timeout=min(1200,end-time.time()-600),check=False)
        require(result.returncode==0,'Actual native generic consumer suite failed; preserve logs')
        junit=m.locator(xml);m.passing_junit(junit,contract)
        require(m.source_manifest(binding)==(manifest,pins),'Actual source closure changed during native qualification')
        report={'status':'complete_census_resilience_consumer_qualification','returncode':0,'native_job_id':job,'source_manifest':binding,'source_pins':pins,
            'suite':{'source':{'path':str(suite),'sha256':contract['source_sha256']},'test_counts':{'tests':contract['tests'],'failures':0,'errors':0,'skipped':0},'test_roster_sha256':contract['roster_sha256'],'junit':junit},
            'native_registration':reg,'allocation':fields,'checked_utc':datetime.now(timezone.utc).isoformat(),'final_native_completion_required_before_use':True,**m.FALSE_FLAGS}
        save(SUPPORT/'qualification.json',report)
        print('CENSUS_RESILIENCE_CONSUMERS_QUALIFIED '+json.dumps({'native_job_id':job,'tests':contract['tests'],'report':m.locator(SUPPORT/'qualification.json'),'scientific_result':False,'admission_authority':False},sort_keys=True),flush=True)


if __name__=='__main__':main()
