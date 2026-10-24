"""One allocated metadata activation of the already sealed behaviour V2 plan."""
from pathlib import Path
from datetime import datetime,timezone
from types import SimpleNamespace
import argparse
import hashlib
import importlib.util
import json
import math
import os
import re
import runpy
import shlex
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

ROOT=Path('/scratch/pwa209/horsehair-worm')
SUPPORT=ROOT/'maintenance/study-behaviour-continuation-activation-v1-20261024'
PHASE='behaviour-continuation-activation-v1-20261024'
TASK='behaviour-continuation-activation-v1'
SELF='run-behaviour-continuation-activation-v1-20261024.py'
TEST='test_behaviour_continuation_activation_v1_20261024.py'
FILES={SELF,TEST,'build-native-plan.py','README.md'}
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=GUARD.with_name('native-scheduler-read.py')
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
FIRST='6982722'
PROVIDER=ROOT/'maintenance/study-census-resilience-support-v1-20261024/run-fresh-storage-census-resilience-v1-20261024.py'
PROVIDER_SHA='94ac29d11da50412ba3c1f9269ed5762224f93635cd1fae505bfabc5f9b63a93'
SCHEDULE={'path':str(ROOT/'maintenance/study-census-resilience-deployment-v1-20261024/provider-registration-577/provider-schedule.json'),
    'sha256':'205c15ce28bfbf46bc791bee0d462c364fddbae8ab7b23715e5a2a93aee72d47'}
PROVIDER_MANIFEST={'path':str(PROVIDER.with_name('source-manifest.json')),
    'sha256':'dbe6c589a7dda4e8d54cc041ddee1dfa77f7804d67e290efcd396c7108b6f152'}
PROVIDER_Q={'path':str(ROOT/'maintenance/study-census-resilience-qualification-v1-20261024/qualification.json'),
    'sha256':'7291b858df276bcc8dc6439bbe384b970c3f6a9fa47bfe20f4a9baa0b9e6b04b','job_id':'6980189'}
BEHAVIOUR=ROOT/'maintenance/study-behaviour-full-cohort-continuation-v2-20261024'
BEHAVIOUR_MANIFEST_SHA='d428473704dcd73b398985df287331132cea96e0ac05ef37f149c7aee473f71c'
BEHAVIOUR_SOURCE='8495a3d985515bf71f476295da87e663a1e764864ddce47a06794e7e11d88f94'
BEHAVIOUR_FRAMEWORK=ROOT/'releases/20261021-caebc9c1dc77-8495a3d98551'
BEHAVIOUR_TASK='behaviour-full-cohort-continuation-v2'
BEHAVIOUR_PLAN=ROOT/'plans/behaviour-full-cohort-continuation-v2-20261024/plan.json'
BEHAVIOUR_LEDGER=ROOT/'state/behaviour-full-cohort-continuation-v2-20261024-submission.json'
VERIFIER='6942117'
VERIFIER_RECEIPT_SHA='bb28123bea6c467d9b6f6f8b17f0e2552de78cfd22108d1d89f4b67e95efe19b'
VERIFIER_MANIFEST_SHA='7c473ee5f6a6ade2510aa270399c4b3c4bdfdfc18a501bde837a37de03b89227'
DEPENDENCIES={FIRST,VERIFIER,'6689965','6943757'}
FALSE_FLAGS={'scientific_result':False,'scientific_fit':False,'complete_declared_workload':False,
    'admission_authority':False,'scientific_gates_cleared':False,'freshness_timestamp_rewritten':False,
    'numerical_work_executed_by_activation':False,'automatic_mutation_replay_permitted':False}


def require(value,message):
    if not value:raise ValueError(message)


def canonical(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def digest(path):
    result=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(8*1024**2),b''):result.update(block)
    return result.hexdigest()


def ordinary(path,namespace=None):
    path=Path(path);namespace=ROOT if namespace is None else Path(namespace)
    require(path.is_absolute() and path==path.resolve() and path!=namespace and path.is_relative_to(namespace)
        and path.is_file() and all(not p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(namespace)),
        'Ordinary confined existing source or metadata required')
    return path


def packet(path,pin=None,maximum=2_000_000):
    path=ordinary(path)
    with path.open('rb') as stream:raw=stream.read(maximum+1)
    require(len(raw)<=maximum and (pin is None or hashlib.sha256(raw).hexdigest()==pin),'Bounded pinned metadata changed')
    return raw


def strict(raw):
    def unique(items):
        result={}
        for key,value in items:
            require(key not in result,'Duplicate JSON key refused');result[key]=value
        return result
    def nonfinite(value):raise ValueError('Nonfinite JSON refused')
    return json.loads(raw,object_pairs_hook=unique,parse_constant=nonfinite)


def bound(binding,maximum=2_000_000):
    require(set(binding)=={'path','sha256'} and re.fullmatch('[0-9a-f]{64}',str(binding['sha256'])),'Exact immutable locator required')
    return strict(packet(binding['path'],binding['sha256'],maximum))


def locator(path):return {'path':str(path),'sha256':digest(path)}


def save(path,value):
    raw=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    require(len(raw)<=8_000_000,'Bounded operational evidence required')
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def source_guard(pins):
    for path,pin in pins.items():require(digest(ordinary(path))==pin,'Source/evidence bytes changed: '+str(path))


def load(path,pin,name):
    packet(path,pin,200_000);spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    require(digest(path)==pin,'Imported source changed');return module


def source_manifest(binding):
    require(binding['path']==str(SUPPORT/'source-manifest.json'),'Exact activation source namespace required')
    manifest=bound(binding,200_000)
    require(set(manifest)=={'kind','files','suite'} and manifest['kind']=='behaviour-continuation-activation-source-manifest-v1'
        and set(manifest['files'])==FILES,'Complete activation source closure required')
    pins={binding['path']:binding['sha256']}
    for name,pin in manifest['files'].items():packet(SUPPORT/name,pin,200_000);pins[str(SUPPORT/name)]=pin
    suite=manifest['suite']
    require(set(suite)=={'source','source_sha256','tests','roster_sha256'} and suite['source']==TEST
        and suite['source_sha256']==manifest['files'][TEST] and type(suite['tests']) is int and suite['tests']>0
        and re.fullmatch('[0-9a-f]{64}',str(suite['roster_sha256'])),'Exact nonempty generic source-bound test roster required')
    require(Path(__file__)==SUPPORT/SELF and manifest['files'][SELF]==digest(__file__),'Exact activation entry source required')
    return manifest,pins


def native_fields(response):
    response.check_returncode();require(not response.stderr,'Native scheduler stderr refused')
    text=response.stdout.decode() if isinstance(response.stdout,bytes) else response.stdout
    require(len([s for s in text.splitlines() if s.strip()])==1,'Exactly one native allocation required')
    pairs=[s.split('=',1) for s in shlex.split(text) if '=' in s];fields=dict(pairs)
    require(len(fields)==len(pairs),'Duplicate native scheduler fields refused');return fields


def seconds(value):
    match=re.fullmatch(r'(?:(\d+)-)?(\d+):(\d\d):(\d\d)',value)
    require(match is not None,'Exact finite native duration required')
    days,hours,minutes,secs=(int(x or 0) for x in match.groups())
    require(minutes<60 and secs<60,'Malformed native duration');return days*86400+hours*3600+minutes*60+secs


def allocation(fields,job,task,*,wall=time.time):
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
        and fields.get('Account')=='def-ptewarie_cpu' and fields.get('JobState')=='RUNNING'
        and fields.get('NumCPUs')=='1' and fields.get('NumNodes') in {'1','1-1'}
        and fields.get('MinMemoryNode') in {'16G','16384M'} and seconds(fields.get('TimeLimit',''))==14400
        and fields.get('JobName')=='hhw-'+TASK and fields.get('Command')==task['script']
        and 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields
        and all('gpu' not in fields.get(k,'').lower() for k in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Actual owned scalar1CPU16G4h/noGPU activation required')
    end=datetime.fromisoformat(fields.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp()
    environment=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(environment) and abs(end-environment)<=2 and wall()+600<end,'Actual native end/cleanup reserve required')
    return end


def completed_set(proofs,expected,completion,verify):
    require(type(proofs) is list and {p.get('job_id') for p in proofs}==set(expected),'Exact positive dependency logical set required')
    selected={}
    for proof in proofs:
        job=proof['job_id'];require(proof.get('dependency_kind')=='afterok','Only positive afterok proof may authorize activation')
        verify(proof);current=completion(job)
        require(current is not None and current.get('native_records')==proof.get('native_records'),'Actual selected final native proof changed')
        if job in selected:require(selected[job]==proof['native_records'],'Duplicate dependency has different native proof')
        selected[job]=proof['native_records']
    return selected


def registration(args,job,argv,manifest_binding,manifest):
    plan=bound(locator(args.native_plan),8_000_000);ledger=bound(locator(args.native_ledger),8_000_000)
    require(args.native_plan==ROOT/'plans'/PHASE/'plan.json' and args.native_ledger==ROOT/'state'/(PHASE+'-submission.json')
        and plan.get('root')==str(ROOT) and plan.get('phase')==PHASE
        and plan.get('config_sha256')==ledger.get('config_sha256')==CONFIG
        and plan.get('release_identity',{}).get('sha256')==ledger.get('source_sha256')==SOURCE
        and ledger.get('plan_sha256')==canonical(plan) and len(plan.get('tasks',[]))==1,'Actual registered activation plan/source/config required')
    task=plan['tasks'][0];row=ledger.get('tasks',{}).get(TASK,{})
    command=shlex.split(task.get('command',''));script=packet(args.native_script,maximum=200_000).decode()
    require(args.native_task==task.get('name')==TASK and task.get('script')==str(args.native_script)
        and args.native_script.parent==args.native_plan.parent and command==['python','-s','-u',str(SUPPORT/SELF),*argv]
        and script.count(task['command'])==1 and task.get('script_sha256')==row.get('script_sha256')==canonical(script)
        and task.get('resources')=={'cpus':1,'memory':'16G','hours':4,'gpu':False,'array':None}
        and task.get('depends')==[] and task.get('external_dependencies')==[FIRST] and task.get('dependency_kind')=='afterok'
        and task.get('behaviour_continuation_activation')=={'helper':{'path':str(SUPPORT/SELF),'sha256':manifest['files'][SELF]},
            'source_manifest':manifest_binding,'first_provider_job_id':FIRST,'provider_schedule':SCHEDULE,'suite':manifest['suite']}
        and row.get('state')=='submitted' and row.get('job_id')==job,'Exact activation entry/argv/resource/native intent required')
    issued=row.get('issued_native_dependencies');done=row.get('completed_native_dependencies',[])
    require(type(issued) is list and len(issued)==len(set(issued)) and set(issued)|{p.get('job_id') for p in done}=={FIRST}
        and row.get('dependency_kind')=='afterok' and row.get('dependency_resolution_sha256')==canonical({'kind':'afterok','issued':issued,'completed':done}),
        'Exact actual first-provider native dependency resolution required')
    return task,done


def bounded_transport(end,*,runner=subprocess.run,wall=time.time):
    def run(command,**options):
        require(command and command[0] in {'sacct','scontrol'},'Only explicit read-only native metadata transport permitted')
        if command[0]=='scontrol':require(command[1:3]==['show','job'],'Only read-only scheduler query permitted')
        remaining=end-wall()-600;require(remaining>0,'Native metadata cleanup margin exhausted')
        options['timeout']=min(45.,remaining);return runner(command,**options)
    return run


def passing_junit(path,contract):
    raw=packet(path,maximum=2_000_000);require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Plain generic XML required')
    tree=ET.fromstring(raw);cases=list(tree.iter('testcase'));names=sorted(c.get('classname','').split('.')[-1]+'::'+c.get('name','') for c in cases)
    require(len(cases)==len(set(names))==contract['tests'] and canonical(names)==contract['roster_sha256']
        and not any(list(c.iter(k)) for c in cases for k in ('failure','error','skipped'))
        and all(int(n.get(k,'0'))==0 for n in tree.iter() if n.tag in {'testsuite','testsuites'} for k in ('failures','errors','skipped')),
        'Actual complete passing generic native fixture roster required')
    return locator(path)


def fixture_workspace(temporary,job):
    require(re.fullmatch('[1-9][0-9]*',str(job)),'Actual fixture allocation identity required')
    workspace=Path(temporary)/('behaviour-activation-'+str(job));workspace.mkdir(mode=0o700,exist_ok=False)
    basetemp=workspace/'pytest-temp';basetemp.mkdir(mode=0o700)
    return workspace,basetemp


def qualify(case,end,contract):
    temporary=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temporary.is_absolute() and temporary.is_dir() and not temporary.is_symlink()
        and temporary.stat().st_uid==3164768,'Owned allocation-local generic fixtures required')
    workspace,basetemp=fixture_workspace(temporary,os.environ['SLURM_JOB_ID'])
    command=[sys.executable,'-s','-B','-m','pytest',str(SUPPORT/TEST),'-q','-p','no:cacheprovider',
        '--basetemp',str(basetemp),'--junitxml',str(case/'generic.xml')]
    save(case/'generic-intent.json',{'command':command,'cwd':str(workspace),'additional_retries':0,**FALSE_FLAGS})
    with (case/'generic-stdout.txt').open('xb') as out,(case/'generic-stderr.txt').open('xb') as err:
        result=subprocess.run(command,cwd=workspace,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'},stdout=out,stderr=err,
            timeout=min(300.,end-time.time()-600),check=False)
    require(result.returncode==0,'Actual native generic activation qualification failed')
    return {'junit':passing_junit(case/'generic.xml',contract),'tests':contract['tests'],'source':locator(SUPPORT/TEST),**FALSE_FLAGS}


def behaviour_sources():
    binding={'path':str(BEHAVIOUR/'source-manifest.json'),'sha256':BEHAVIOUR_MANIFEST_SHA};manifest=bound(binding,200_000)
    require(manifest.get('complete_native_qualification_tests')==179 and len(manifest.get('files',{}))==20,'Unchanged sealed179-case behaviour capsule required')
    pins={binding['path']:binding['sha256']}
    for name,pin in manifest['files'].items():
        require(Path(name).name==name,'Flat original behaviour source closure required')
        packet(BEHAVIOUR/name,pin,200_000);pins[str(BEHAVIOUR/name)]=pin
    return manifest,pins


def provider_evidence(producer,native_reader,completion,config):
    schedule=bound(SCHEDULE,200_000)
    require(schedule.get('helper')=={'path':str(PROVIDER),'sha256':PROVIDER_SHA}
        and schedule.get('source_manifest')==PROVIDER_MANIFEST and schedule.get('qualification')==PROVIDER_Q,
        'Exact new provider577/Q6980189 bindings required')
    checked=producer.validate_schedule_binding(schedule,PROVIDER_Q,native_reader)
    plan=bound(schedule['plan'],8_000_000);ledger=bound(schedule['ledger'],8_000_000);task=plan['tasks'][0]
    require(ledger['tasks'][task['name']]['job_id']==FIRST,'Actual completed first new provider required')
    proof=completion(FIRST);row=(proof or {}).get('native_records',{}).get(FIRST,{})
    observed=native_reader([FIRST],{},owner='pwa209',account='def-ptewarie_cpu').get(FIRST,{})
    require(row.get('state')=='COMPLETED' and row.get('exit_code')=='0:0' and row.get('cpus')==8 and row.get('gpus')==0
        and type(row.get('elapsed_seconds')) is int and 0<row['elapsed_seconds']<=21600
        and observed.get('job_id_raw')==FIRST and observed.get('user')=='pwa209' and observed.get('account')=='def-ptewarie_cpu'
        and observed.get('state')=='COMPLETED' and observed.get('exit_code')=='0:0' and observed.get('cpus')==8
        and observed.get('unallocated_metadata') is False and observed.get('elapsed_seconds')==row['elapsed_seconds']
        and 'gpu' not in observed.get('alloc_tres','').lower(),'Positive owned actual first-provider completion required')
    receipt_path=Path(schedule['case_root'])/('native-'+FIRST)/'receipt.json';receipt_binding=locator(ordinary(receipt_path));receipt=bound(receipt_binding)
    registration,tail=producer.registered_task(schedule['plan']['path'],schedule['ledger']['path'],task['name'],task['script'],FIRST,
        hours=6,helper=PROVIDER)
    arguments=SimpleNamespace(**{flag.removeprefix('--').replace('-','_'):value for flag,value in zip(tail[::2],tail[1::2])})
    selected=producer.provider_registration(arguments,FIRST,config)
    require(receipt.get('status')=='complete_exact_size_sweep_with_original_conservative_freshness'
        and receipt.get('native_job_id')==FIRST and receipt.get('provider_helper')==schedule['helper']
        and receipt.get('wrapper_sha256')==PROVIDER_SHA and receipt.get('source_sha256')==SOURCE and receipt.get('config_sha256')==CONFIG
        and receipt.get('source_manifest')==PROVIDER_MANIFEST and receipt.get('qualification')==PROVIDER_Q
        and receipt.get('allocation',{}).get('native_registration')==registration and receipt.get('provider_registration')==selected
        and all(receipt.get(flag) is False for flag in producer.FALSE_FLAGS),'Exact successful first-provider receipt required')
    producer.validate_predecessor_receipt(receipt,native_reader)
    sweep=bound(receipt['sweep']);require(receipt['sweep']['path']==str(receipt_path.parent/'sweep-result.json'),'Exact first-provider sweep metadata required')
    pins={**checked['checked_paths'],SCHEDULE['path']:SCHEDULE['sha256'],receipt_binding['path']:receipt_binding['sha256'],receipt['sweep']['path']:receipt['sweep']['sha256']}
    source_guard(pins)
    return {'schedule':schedule,'receipt':receipt_binding,'completion':proof,'native_observed':observed,'sweep':receipt['sweep'],
        'qualification':checked,'checked_paths':pins,**FALSE_FLAGS}


def current_pair(producer,*,wall=time.time):
    """Check original clock metadata and exact pair bytes; this grants no admission."""
    budget_path=ROOT/'state/storage-budget.json';binding=locator(ordinary(budget_path));budget=bound(binding,1_000_000)
    measured=budget.get('measured_unix');captured=datetime.fromisoformat(budget.get('captured_utc','').replace('Z','+00:00'))
    require(budget.get('status')=='measured' and budget.get('root')==str(ROOT) and type(budget.get('personal_uid')) is int
        and budget['personal_uid']==3164768 and type(measured) in (int,float) and math.isfinite(measured)
        and captured.tzinfo is not None,'Original complete current budget metadata required')
    clocks=producer.fresh_original_clocks({'unchanged_snapshot_measured_unix':measured,
        'unchanged_snapshot_captured_utc':budget['captured_utc']},wall())
    entry=budget.get('exact_entry_measurements',{});detail_path=ROOT/'state/storage-entry-sizes.json'
    require(entry.get('receipt_path')==str(detail_path) and entry.get('method')=='owned_regular_length'
        and type(entry.get('count')) is int and entry['count']==budget.get('study_files')
        and type(entry.get('bytes')) is int and entry['bytes']==budget.get('study_bytes')
        and ordinary(detail_path).stat().st_size<=512*1024**2 and digest(detail_path)==entry.get('receipt_sha256'),
        'Current original exact-size pair bytes required')
    pins={binding['path']:binding['sha256'],str(detail_path):entry['receipt_sha256']};sources=budget.get('native_source_hashes',{})
    require(sources and set(budget.get('native_source_paths',[]))==set(sources),'Original native quota source pins required')
    for path,pin in sources.items():
        ordinary(path,ROOT/'state');packet(path,pin,1_000_000);pins[path]=pin
    source_guard(pins)
    producer.fresh_original_clocks({'unchanged_snapshot_measured_unix':measured,'unchanged_snapshot_captured_utc':budget['captured_utc']},wall())
    return {'budget':binding,'entry_sizes':{'path':str(detail_path),'sha256':entry['receipt_sha256']},'checked_paths':pins,
        'clocks':clocks,'freshness_metadata_only':True,'full_storage_admission_remains_required':True,**FALSE_FLAGS}


def stop_child(process):
    result={'termination_confirmed':False,'returncode':None,'signals':[]}
    for sig,wait in ((signal.SIGTERM,10),(signal.SIGKILL,20)):
        if process.poll() is not None:break
        try:os.killpg(process.pid,sig);result['signals'].append(sig.name)
        except ProcessLookupError:pass
        try:process.wait(timeout=wait)
        except subprocess.TimeoutExpired:continue
    code=process.poll();result.update(termination_confirmed=type(code) is int,returncode=code);return result


def run_builder(case,end,*,factory=subprocess.Popen,wall=time.time):
    require(not BEHAVIOUR_PLAN.parent.exists() and not BEHAVIOUR_LEDGER.exists(),'Existing behaviour plan or ledger requires reconciliation; never replay')
    command=[sys.executable,'-s','-u',str(BEHAVIOUR/'build-native-plan.py'),'--source-manifest-sha256',BEHAVIOUR_MANIFEST_SHA,
        '--verifier-job-id',VERIFIER,'--verifier-receipt-sha256',VERIFIER_RECEIPT_SHA,
        '--verifier-source-manifest-sha256',VERIFIER_MANIFEST_SHA,'--census-provider-job-id',FIRST,
        '--census-provider-schedule',SCHEDULE['path'],'--census-provider-schedule-sha256',SCHEDULE['sha256']]
    timeout=min(300.,end-wall()-600);require(timeout>0,'Builder cleanup margin exhausted')
    environment={**os.environ,'PYTHONPATH':str(BEHAVIOUR_FRAMEWORK/'src'),'PYTHONNOUSERSITE':'1','PYTHONDONTWRITEBYTECODE':'1'}
    save(case/'builder-intent.json',{'command':command,'cwd':str(BEHAVIOUR_FRAMEWORK),'maximum_seconds':timeout,'additional_retries':0,**FALSE_FLAGS})
    with (case/'builder-stdout.txt').open('xb') as out,(case/'builder-stderr.txt').open('xb') as err:
        process=factory(command,cwd=BEHAVIOUR_FRAMEWORK,env=environment,stdout=out,stderr=err,start_new_session=True,close_fds=True)
        try:code=process.wait(timeout=timeout)
        except BaseException as error:
            save(case/'builder-process.json',{'status':'builder_failed_no_retry','error_type':type(error).__name__,'termination':stop_child(process),**FALSE_FLAGS});raise
    save(case/'builder-process.json',{'returncode':code,'positive_reaped_exit':type(code) is int,**FALSE_FLAGS})
    require(code==0 and type(code) is int,'Original isolated8495 builder failed; preserve evidence')
    require((case/'builder-stderr.txt').stat().st_size==0,'Original builder stderr refused')
    response=strict(packet(case/'builder-stdout.txt',maximum=200_000))
    require(response.get('status')=='immutable_plan_written_not_submitted' and response.get('plan')==str(BEHAVIOUR_PLAN)
        and response.get('scientific_result') is False and response.get('complete_declared_workload') is False,
        'Exact original builder completion required')
    return response


def expected_worker_flags(receipt_sha):
    return {'--qualification':str(ROOT/'maintenance/study-af-execution-support-v2-20261021/qualification.json'),
        '--qualification-sha256':'f4f7684e3b04fde1a61c2ce20baec430a1dfe56b9e055b75cfaabc78cbdc7c13','--qualification-job-id':'6689965',
        '--resource-policy':str(ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v2/policy.json'),
        '--resource-policy-sha256':'5dac85400e709525458cc34a2edc2bb40b029eb743078672cd721ab444c17015',
        '--native-script':str(BEHAVIOUR_PLAN.parent/(BEHAVIOUR_TASK+'.sbatch')),
        '--context-helper':str(BEHAVIOUR/'run-full-cohort-continuation-context-v2-20261024.py'),
        '--context-helper-sha256':'b90861f983c8318d7968b8e56ab0a368932f611369880a9f05e41f7556dfeb24',
        '--tests':str(BEHAVIOUR/'test_full_record_adjoint_v3_20261021.py'),
        '--tests-sha256':'6e15c7c00f4ab421cc76e269ad5f1524bc301b9770d0b8efd073b51906e30978',
        '--prefix-receipt':str(ROOT/'maintenance/study-behaviour-supervisor-reconciliation-v1-20261024'/('native-'+VERIFIER)/'receipt.json'),
        '--prefix-receipt-sha256':VERIFIER_RECEIPT_SHA,'--prefix-job-id':VERIFIER,'--source-manifest-sha256':BEHAVIOUR_MANIFEST_SHA,
        '--verifier-source-manifest-sha256':VERIFIER_MANIFEST_SHA,'--census-provider-job-id':FIRST,
        '--census-provider-schedule':SCHEDULE['path'],'--census-provider-schedule-sha256':SCHEDULE['sha256'],
        '--census-provider-receipt-sha256':receipt_sha}


def worker_plan(receipt_sha,completion,verify):
    plan=bound(locator(BEHAVIOUR_PLAN),8_000_000);require(len(plan.get('tasks',[]))==1,'One sealed original behaviour task required')
    task=plan['tasks'][0];command=shlex.split(task.get('command',''));helper=BEHAVIOUR/'run-full-cohort-continuation-v2-20261024.py'
    require(command[:4]==['python','-s','-u',str(helper)] and len(command[4:])%2==0
        and len(set(command[4::2]))==len(command[4:])//2,'Exact original worker entry and unique arguments required')
    flags=dict(zip(command[4::2],command[5::2]))
    require(plan.get('root')==str(ROOT) and plan.get('phase')=='behaviour-full-cohort-continuation-v2-20261024'
        and plan.get('release')==str(BEHAVIOUR_FRAMEWORK) and plan.get('release_identity',{}).get('sha256')==BEHAVIOUR_SOURCE
        and plan.get('config_sha256')==CONFIG and plan.get('source_manifest_sha256')==BEHAVIOUR_MANIFEST_SHA
        and task.get('name')==BEHAVIOUR_TASK and task.get('resources')=={'cpus':1,'memory':'64G','hours':24,'gpu':False,'array':None}
        and task.get('depends')==[] and task.get('external_dependencies')==[] and task.get('dependency_kind')=='afterok'
        and task.get('script')==str(BEHAVIOUR_PLAN.parent/(BEHAVIOUR_TASK+'.sbatch'))
        and flags==expected_worker_flags(receipt_sha)
        and all(plan.get(k) is False and task.get(k) is False for k in ('scientific_result','complete_declared_workload','admission_authority')),
        'Unmodified bounded source-qualified worker plan required')
    completed_set(task.get('completed_external_dependencies'),DEPENDENCIES,completion,verify)
    script=packet(task['script'],maximum=200_000).decode()
    require(script.count(task['command'])==1 and canonical(script)==task['script_sha256'],'Exact original frozen worker script required')
    return plan,task


def held_native(fields,task,job):
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)' and fields.get('Account')=='def-ptewarie_cpu'
        and fields.get('JobState')=='PENDING' and fields.get('Reason')=='JobHeldUser' and fields.get('Command')==task['script']
        and fields.get('JobName')=='hhw-'+BEHAVIOUR_TASK and fields.get('NumCPUs')=='1' and fields.get('NumNodes') in {'1','1-1'}
        and fields.get('MinMemoryNode') in {'64G','65536M'} and seconds(fields.get('TimeLimit',''))==86400
        and fields.get('RunTime')=='00:00:00' and fields.get('Restarts')=='0' and fields.get('Dependency') in {'(null)','None',''}
        and 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields
        and all('gpu' not in fields.get(k,'').lower() for k in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Actual zero-allocation held scalar1CPU64G24h original worker required')


def dispatch(case,plan,task,end,submit,completion,verify,freshness,*,runner=subprocess.run):
    require(not BEHAVIOUR_LEDGER.exists(),'Existing ledger requires explicit reconciliation; never replay')
    require(bound(locator(BEHAVIOUR_PLAN),8_000_000)==plan
        and canonical(packet(task['script'],maximum=200_000).decode())==task['script_sha256'],
        'Previously verified worker plan/script changed before submission')
    plan_pin=digest(BEHAVIOUR_PLAN);script_pin=digest(task['script']);freshness()
    def held(command,**options):
        require(command==['sbatch','--parsable',task['script']],'Only exact original single held submission permitted')
        freshness()
        require(digest(BEHAVIOUR_PLAN)==plan_pin and digest(task['script'])==script_pin,
            'Worker plan/script changed at actual submission boundary')
        actual=['sbatch','--hold',*command[1:]]
        save(case/'submit-intent.json',{'command':actual,'plan_sha256':canonical(plan),**FALSE_FLAGS})
        result=runner(actual,**options,timeout=min(45.,end-time.time()-600))
        save(case/'submit-response.json',{'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr,**FALSE_FLAGS});return result
    ledger=submit(BEHAVIOUR_PLAN,BEHAVIOUR_LEDGER,runner=held);row=ledger['tasks'][BEHAVIOUR_TASK];job=str(row.get('job_id',''))
    require(re.fullmatch('[1-9][0-9]*',job) and row.get('state')=='submitted' and row.get('issued_native_dependencies')==[]
        and row.get('dependency_kind')=='afterok','One actual registered worker with no outstanding native dependencies required')
    completed_set(row.get('completed_native_dependencies'),DEPENDENCIES,completion,verify)
    require(row.get('dependency_resolution_sha256')==canonical({'kind':'afterok','issued':[], 'completed':row['completed_native_dependencies']})
        and ledger.get('plan_sha256')==canonical(plan) and ledger.get('source_sha256')==BEHAVIOUR_SOURCE
        and ledger.get('config_sha256')==CONFIG and row.get('script_sha256')==task['script_sha256'],'Exact registered original worker ledger required')
    ledger_binding=locator(BEHAVIOUR_LEDGER)
    require(bound(ledger_binding,8_000_000)==ledger,'Persisted actual submission ledger differs')
    response=runner(['scontrol','show','job',job,'-o'],capture_output=True,text=True,timeout=min(45.,end-time.time()-600))
    fields=native_fields(response);held_native(fields,task,job);save(case/'held-native.json',{'fields':fields,'native_stdout':response.stdout,**FALSE_FLAGS})
    require(digest(BEHAVIOUR_PLAN)==plan_pin and digest(task['script'])==script_pin,'Worker plan/script changed before release')
    freshness();completed_set(row['completed_native_dependencies'],DEPENDENCIES,completion,verify)
    require(digest(BEHAVIOUR_PLAN)==plan_pin and digest(task['script'])==script_pin
        and digest(BEHAVIOUR_LEDGER)==ledger_binding['sha256'],'Worker registration changed at release boundary')
    save(case/'release-intent.json',{'command':['scontrol','release',job],'native_job_id':job,**FALSE_FLAGS})
    result=runner(['scontrol','release',job],capture_output=True,text=True,timeout=min(45.,end-time.time()-600))
    save(case/'release-response.json',{'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr,**FALSE_FLAGS})
    require(result.returncode==0 and not result.stderr,'Unconfirmed release requires explicit reconciliation; never replay')
    return {'worker_native_job_id':job,'plan':locator(BEHAVIOUR_PLAN),'ledger':locator(BEHAVIOUR_LEDGER),'held_native':fields,
        'native_qualification_tests_required_by_worker':179,'worker_completion_not_claimed':True,**FALSE_FLAGS}


def main(argv=None):
    os.environ['TZ']='UTC';time.tzset();argv=list(sys.argv[1:] if argv is None else argv)
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('native-plan','native-ledger','native-script','source-manifest'):p.add_argument('--'+name,type=Path,required=True)
    for name in ('native-task','source-manifest-sha256'):p.add_argument('--'+name,required=True)
    args=p.parse_args(argv);binding={'path':str(args.source_manifest),'sha256':args.source_manifest_sha256}
    manifest,pins=source_manifest(binding);job=os.environ.get('SLURM_JOB_ID','')
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='1' and os.environ.get('HHW_GPU_COUNT')=='0' and os.environ.get('HHW_MAX_HOURS')=='4',
        'Owned finite native1CPU4h activation environment required')
    require(subprocess.check_output(['hostname','-f'],text=True,timeout=10).strip().endswith('.narval.calcul.quebec'),'Native Narval allocation required')
    import horsehair
    from horsehair.core import source_identity,read_json,exclusive_file_lock
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py' and source_identity(FRAMEWORK)['sha256']==SOURCE,
        'Exact original4f interpreter/source required')
    config=read_json(FRAMEWORK/'configs/study.json');require(canonical(config)==CONFIG,'Full original config required')
    task,done=registration(args,job,argv,binding,manifest)
    for path in (args.native_plan,args.native_ledger,args.native_script):pins[str(path)]=digest(path)
    case=SUPPORT/('native-'+job);require(not case.exists(),'Activation allocation cannot replay existing evidence');case.mkdir(mode=0o700)
    for path,pin in ((GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(PROVIDER,PROVIDER_SHA),(POLICY,POLICY_SHA)):
        packet(path,pin,200_000);pins[str(path)]=pin
    runpy.run_path(str(GUARD))['install'](case/'accounting-reads')
    fields=native_fields(runpy.run_path(str(SCHEDULER))['run_scontrol_job'](job,case/'scheduler-reads'));end=allocation(fields,job,task)
    def hard_deadline(signum,frame):raise TimeoutError('Finite activation allocation cleanup reserve reached')
    signal.signal(signal.SIGALRM,hard_deadline);signal.alarm(max(1,int(end-time.time()-600)))
    from horsehair import budget,repair,slurm
    from horsehair.execution_resources import load_resource_policy
    transport=bounded_transport(end);original=repair._repair_native_states
    native_reader=lambda ids,rosters,**kw:original(ids,rosters,runner=transport,**kw)
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    completion=lambda ident:slurm.completed_native_dependency(ROOT,ident)
    verify=lambda proof:slurm._verify_completed_dependency(ROOT,proof)
    with budget.resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=4,job_id=job,resource_policy=policy):
        budget.native_accounting(ROOT,[FIRST],runner=transport)
        if done:completed_set(done,{FIRST},completion,verify)
        require(completion(FIRST) is not None,'First provider is not finalized0:0; no activation')
        native_tests=qualify(case,end,manifest['suite'])
        producer=load(PROVIDER,PROVIDER_SHA,'activation_exact_provider')
        provider=provider_evidence(producer,native_reader,completion,config);pins.update(provider['checked_paths'])
        _,behaviour_pins=behaviour_sources();pins.update(behaviour_pins)
        try:pair=current_pair(producer)
        except (OSError,ValueError,KeyError,TypeError) as error:
            save(case/'receipt.json',{'status':'pending_current_census_no_submission','reason':str(error),'native_job_id':job,**FALSE_FLAGS});return 0
        save(case/'input-bindings.json',{'native_job_id':job,'allocation':fields,'source_manifest':binding,'provider':provider,
            'current_pair':pair,'generic_native_qualification':native_tests,'source_pins':pins,'resource_policy':policy.receipt(),**FALSE_FLAGS})
        require(not (SUPPORT/'activation-intent.json').exists(),'Existing activation intent requires reconciliation; never replay')
        save(SUPPORT/'activation-intent.json',{'native_job_id':job,'source_manifest':binding,'source_pins':pins,**FALSE_FLAGS})
        source_guard(pins);run_builder(case,end)
        plan,worker=worker_plan(provider['receipt']['sha256'],completion,verify)
        timeout=min(7200.,end-time.time()-600);require(timeout>0,'Registration-lock cleanup reserve exhausted')
        with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex',timeout=timeout):
            source_guard(pins)
            def freshness():
                source_guard(pins);current=current_pair(producer)
                require(current['budget']==pair['budget'] and current['entry_sizes']==pair['entry_sizes'],
                    'Current census pair changed during activation; reconcile without submission replay')
                return current
            result=dispatch(case,plan,worker,end,slurm.submit,completion,verify,freshness)
            save(case/'receipt.json',{'status':'sealed_behaviour_v2_technical_continuation_queued','activation_native_job_id':job,
                'source_manifest':binding,'provider':provider,'generic_native_qualification':native_tests,**result})
    signal.alarm(0)
    print('BEHAVIOUR_CONTINUATION_ACTIVATED '+json.dumps({'receipt':str(case/'receipt.json'),**result},sort_keys=True),flush=True)
    return 0


if __name__=='__main__':raise SystemExit(main())
