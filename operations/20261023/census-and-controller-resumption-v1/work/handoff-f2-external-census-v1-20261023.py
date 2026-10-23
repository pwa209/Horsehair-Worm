"""One finite native handoff from ACT6 controllers to qualified census consumers.

Native selftests precede scheduler changes. They are separate handoff evidence,
not coverage claimed by Q2. New controllers depend afterok on this allocation.
No campaign state, journal, worker job, condition packet or science is changed.
"""
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime,timezone
import argparse
import gzip
import hashlib
import importlib.util
import io
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

from horsehair import slurm
from horsehair.core import canonical_hash,digest,read_json,source_identity,native_job_id
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
SUPPORT=ROOT/'maintenance/study-af-external-census-handoff-v1-20261023'
Q2_SUPPORT=ROOT/'maintenance/study-af-rolling-census-support-v3-20261023'
Q2_PATH=ROOT/'maintenance/study-af-rolling-census-qualification-v4-20261023/qualification.json'
ACT_SOURCE=ROOT/'maintenance/study-af-registry-repair-support-v6-20261022/activate-f2-prestart-three-v1-20261022.py'
ACT_SHA='586bbf301ef69621cc1132b8b25a9c243f29a2e918b54d564e84be816af2d2eb'
ACT_AREA=ACT_SOURCE.parent/'native-activation-prestart-three-v1'
OLD_PHASE='study-af-f2-registry-r2-controllers-v1-20261022'
PHASE='study-af-f2-external-census-controllers-v1-20261023'
SELF_NAME='handoff-f2-external-census-v1-20261023.py'
TEST_NAME='test_handoff_f2_external_census_v1_20261023.py'
OUTER_NAME='run-af-agent-controller-census-policy-v1-20261023.py'
PROVIDER_NAME='run-fresh-storage-census-v6-20261023.py'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=GUARD.with_name('native-scheduler-read.py')
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
REGISTRY=Q2_SUPPORT/'registered-snapshot-read-guard-v4-20261022.py'
REGISTRY_SHA='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
FALSE_FLAGS={'scientific_result':False,'admission_authority':False,'scientific_gates_cleared':False,
    'freshness_timestamp_rewritten':False,'end_time_extends_admission_freshness':False}
TERMINAL={'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}


def require(value,message):
    if not value:raise ValueError(message)


def ordinary(path,namespace):
    p=Path(path)
    require(p.is_absolute() and p==p.resolve() and p!=namespace and p.is_relative_to(namespace) and p.is_file()
        and all(not q.is_symlink() for q in [p,*p.parents] if q.is_relative_to(namespace)),'Ordinary confined file required')
    return p


def packet(path,sha=None,limit=8_000_000):
    p=ordinary(path,ROOT)
    with p.open('rb') as stream:raw=stream.read(limit+1)
    h=hashlib.sha256(raw).hexdigest();require(len(raw)<=limit and (sha is None or sha==h),'Bounded exact source/evidence changed')
    return raw,h


def locator(path):return {'path':str(path),'sha256':packet(path)[1]}


def exclusive_bytes(path,raw):
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def exclusive(path,value):exclusive_bytes(path,(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode())


def loaded(path,sha,name):
    packet(path,sha,200_000);spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module);require(digest(path)==sha,'Library changed during import');return module


def native_fields(response):
    response.check_returncode();lines=[line for line in response.stdout.splitlines() if line.strip()]
    require(len(lines)==1,'One complete native scheduler row required')
    key=r'[A-Za-z][A-Za-z0-9_/:]*'
    pairs=re.findall(r'(?<!\S)('+key+r')=(.*?)(?=\s+'+key+r'=|$)',lines[0])
    value={name:item.strip() for name,item in pairs}
    require(len(value)==len(pairs),'Duplicate native scheduler fields refused');return value


def manifest(path,sha):
    require(Path(path)==SUPPORT/'source-manifest.json','Exact handoff source manifest required')
    value=json.loads(packet(path,sha,100_000)[0])
    require(set(value)=={'kind','files','tests','q2_source_manifest','q2_registration','activation_source','activation_registration','provider_registration'}
        and value['kind']=='f2-external-census-handoff-source-manifest-v1'
        and set(value['files'])=={SELF_NAME,TEST_NAME}
        and set(value['tests'])=={'count','roster_sha256'} and type(value['tests']['count']) is int and value['tests']['count']>0
        and re.fullmatch('[0-9a-f]{64}',str(value['tests']['roster_sha256'])),'Exact separately frozen handoff test/source contract required')
    for name,h in value['files'].items():
        require(re.fullmatch('[0-9a-f]{64}',str(h)),'Exact handoff source SHA required');packet(SUPPORT/name,h,200_000)
    require(Path(__file__)==SUPPORT/SELF_NAME and digest(Path(__file__))==value['files'][SELF_NAME]
        and value['activation_source']=={'path':str(ACT_SOURCE),'sha256':ACT_SHA},'Own helper and unchanged ACT source required')
    require(set(value['q2_source_manifest'])=={'path','sha256'}
        and value['q2_source_manifest']['path']==str(Q2_SUPPORT/'source-manifest.json')
        and re.fullmatch('[0-9a-f]{64}',str(value['q2_source_manifest']['sha256'])),'Exact frozen Q2 source manifest required')
    qmanifest=json.loads(packet(value['q2_source_manifest']['path'],value['q2_source_manifest']['sha256'],100_000)[0])
    require(qmanifest.get('kind')=='streaming-census-operational-source-manifest-v2'
        and len(qmanifest.get('files',{}))==14 and len(qmanifest.get('suites',{}))==13,'Frozen qualified Q2 closure required')
    for name,h in qmanifest['files'].items():
        require(Path(name).name==name and re.fullmatch('[0-9a-f]{64}',str(h)),'Private source basename/exact SHA required');packet(Q2_SUPPORT/name,h,200_000)
    for key in ('q2_registration','activation_registration'):
        require(set(value[key])=={'plan_path','ledger_path','task_name','script_path'},'Exact producer registered-pair locator required')
    require(set(value['provider_registration'])=={'plan_path','ledger_path'},'Exact finite provider registered-pair locator required')
    return value,qmanifest


def registration_record(binding):
    plan=json.loads(packet(binding['plan_path'])[0]);ledger=json.loads(packet(binding['ledger_path'])[0])
    require(plan.get('release_identity',{}).get('sha256')==SOURCE and plan.get('config_sha256')==CONFIG
        and ledger.get('source_sha256')==SOURCE and ledger.get('config_sha256')==CONFIG
        and ledger.get('plan_sha256')==canonical_hash(plan),'Registered source/config pair changed')
    tasks=[task for task in plan.get('tasks',[]) if task.get('name')==binding['task_name']]
    require(len(tasks)==1 and tasks[0]['script']==binding['script_path'],'One exact source-bound task required')
    task=tasks[0];row=ledger.get('tasks',{}).get(task['name'],{});script=packet(task['script'])[0].decode()
    require(re.fullmatch('[1-9][0-9]*',str(row.get('job_id',''))) and row.get('state')=='submitted'
        and task['command'] in script and canonical_hash(script)==task['script_sha256']==row.get('script_sha256'),
        'Actual registered job/script bytes required')
    return task,str(row['job_id']),{'plan':locator(binding['plan_path']),'ledger':locator(binding['ledger_path']),
        'script':locator(binding['script_path']),'task_name':binding['task_name'],'native_job_id':str(row['job_id'])}


def native_allocation(job,task_name,script,reader,area,*,wall=time.time):
    f=native_fields(reader(job,area/'own-native'))
    require(f.get('JobId')==job and f.get('UserId')=='pwa209(3164768)'
        and f.get('Account')=='def-ptewarie_cpu' and f.get('JobState')=='RUNNING' and f.get('NumCPUs')=='4'
        and f.get('NumNodes') in {'1','1-1'} and f.get('MinMemoryNode') in {'16G','16384M'}
        and f.get('TimeLimit') in {'02:00:00','2:00:00','0-02:00:00'} and f.get('Command')==str(script)
        and f.get('JobName')=='hhw-'+task_name and 'ArrayJobId' not in f and 'ArrayTaskId' not in f
        and all('gpu' not in f.get(key,'').lower() for key in ('ReqTRES','AllocTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Owned scalar4CPU16G2h handoff allocation/script required')
    for key in ('ReqTRES','AllocTRES'):
        pairs=[item.partition('=') for item in f.get(key,'').split(',')];values={name:item for name,sep,item in pairs}
        require(len(values)==len(pairs) and all(name and sep and item for name,sep,item in pairs)
            and values.get('cpu')=='4' and values.get('node')=='1' and values.get('mem') in {'16G','16384M'},'Exact actual handoff TRES required')
    end=datetime.fromisoformat(f.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp();env=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(env) and abs(env-end)<=2 and wall()+1200<end,'Finite native handoff end/margin required')
    return f,end


def self_qualification(c,*,runner=subprocess.run):
    contract=c['manifest']['tests'];temp=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temp.is_absolute() and temp==temp.resolve() and temp.is_dir() and not temp.is_symlink()
        and temp.stat().st_uid==3164768 and not temp.is_relative_to(ROOT),'Owned allocation-local selftest namespace required')
    workspace=temp/('hhw-external-census-handoff-'+c['job']);workspace.mkdir(mode=0o700,exist_ok=False)
    xml=c['area']/'handoff-contracts.xml';test=SUPPORT/TEST_NAME
    paths=[str(FRAMEWORK/'src'),*[path for path in os.environ.get('PYTHONPATH','').split(os.pathsep) if path and path!=str(FRAMEWORK/'src')]]
    env={**os.environ,'PYTHONPATH':os.pathsep.join(paths),'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1','TZ':'UTC'}
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):env.pop(key,None)
    command=[sys.executable,'-s','-m','pytest',str(test),'-q','-p','no:cacheprovider','--basetemp',str(workspace),'--junitxml',str(xml)]
    exclusive(c['area']/'native-selftest-intent.json',{'command':command,'source':locator(SUPPORT/SELF_NAME),'test_source':locator(test),
        'q2_coverage_claimed_for_handoff':False,**FALSE_FLAGS})
    with (c['area']/'native-selftest-stdout.txt').open('xb') as out,(c['area']/'native-selftest-stderr.txt').open('xb') as err:
        result=runner(command,cwd=FRAMEWORK,env=env,stdout=out,stderr=err,timeout=min(1800,c['end']-time.time()-900),check=False)
    require(result.returncode==0,'Actual native handoff contract suite failed before scheduler changes')
    raw=packet(xml,limit=8_000_000)[0];require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Unsafe native JUnit refused')
    tree=ET.fromstring(raw);cases=list(tree.iter('testcase'));names=sorted(case.get('classname','').split('.')[-1]+'::'+case.get('name','') for case in cases)
    leaves=[node for node in tree.iter('testsuite') if not node.findall('testsuite')]
    require(len(cases)==len(set(names))==contract['count'] and all(case.get('classname') and case.get('name') for case in cases)
        and canonical_hash(names)==contract['roster_sha256'] and not any(list(case.iter(key)) for case in cases for key in ('failure','error','skipped'))
        and leaves and sum(int(node.get('tests','-1')) for node in leaves)==contract['count']
        and all(all(int(n.get(key,'0'))==0 for key in ('failures','errors','skipped')) for n in tree.iter() if n.tag in {'testsuite','testsuites'}),
        'Actual complete native selftest counts/roster required')
    value={'status':'complete_native_handoff_contracts_completion_pending','native_job_id':c['job'],'source':locator(SUPPORT/SELF_NAME),
        'test_source':locator(test),'junit':locator(xml),'test_counts':{'tests':contract['count'],'failures':0,'errors':0,'skipped':0},
        'test_roster_sha256':contract['roster_sha256'],'q2_coverage_claimed_for_handoff':False,
        'native_completion_required_before_controllers_start':True,**FALSE_FLAGS}
    exclusive(c['area']/'native-handoff-qualification.json',value);c['selftest_proof']=value;return value


def verify_sources(c):
    require(time.time()+600<c['end'],'Native handoff finalization margin exhausted')
    for path,h in c['pins'].items():packet(path,h,64_000_000)
    require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,
        'Qualified framework/science source changed')


def native_job(c,task,job,*,memory):
    c['reads']=c.get('reads',0)+1
    fields=native_fields(c['scheduler'](job,c['area']/('controller-native-'+str(c['reads']).zfill(6))))
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)' and fields.get('Account')=='def-ptewarie_cpu'
        and fields.get('JobName')=='hhw-'+task['name'] and fields.get('Command')==task['script'] and fields.get('NumCPUs')=='1'
        and fields.get('NumNodes') in {'1','1-1'} and c['provider'].memory(fields.get('MinMemoryNode',''))==memory*1024**3
        and c['base'].seconds(fields.get('TimeLimit',''))==96*3600 and 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields
        and all('gpu' not in fields.get(key,'').lower() for key in ('ReqTRES','AllocTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Exact owned finite controller script/name/resources required')
    tres=[item.partition('=') for item in fields.get('ReqTRES','').split(',')];requested={name:value for name,sep,value in tres}
    require(len(requested)==len(tres) and all(name and sep and value for name,sep,value in tres)
        and requested.get('cpu')=='1' and requested.get('node')=='1'
        and c['provider'].memory(requested.get('mem',''))==memory*1024**3,'Exact controller requested TRES required')
    return fields


def mutation(c,name,command,*,runner):
    verify_sources(c);intent=c['area']/(name+'-intent.json');require(not intent.exists(),'A scheduler mutation cannot replay')
    exclusive(intent,{'command':command,'automatic_replay_permitted':False,**FALSE_FLAGS})
    response=runner(command,text=True,capture_output=True,timeout=30)
    exclusive(c['area']/(name+'-result.json'),{'returncode':response.returncode,'stdout':response.stdout,'stderr':response.stderr})
    response.check_returncode();return response


def old_controllers(c):
    receipt=c['activation'];plan=json.loads(packet(receipt['plan']['path'],receipt['plan']['sha256'])[0])
    ledger_path=receipt['plan']['ledger_path'];ledger=json.loads(packet(ledger_path)[0]);jobs=receipt['jobs']
    require(plan.get('phase')==OLD_PHASE and Path(receipt['plan']['path'])==ROOT/'plans'/OLD_PHASE/'plan.json'
        and Path(ledger_path)==ROOT/'state'/(OLD_PHASE+'-submission.json') and len(plan.get('tasks',[]))==4
        and len(jobs)==len(set(jobs))==4 and ledger.get('plan_sha256')==canonical_hash(plan)
        and plan.get('release_identity',{}).get('sha256')==SOURCE and plan.get('config_sha256')==CONFIG
        and ledger.get('source_sha256')==SOURCE and ledger.get('config_sha256')==CONFIG
        and plan.get('descriptor_path')==c['base_descriptor']['path'] and plan.get('descriptor_sha256')==c['base_descriptor']['sha256'],
        'Only the exact ACT-created four-controller phase may be handed off')
    for index,(task,job) in enumerate(zip(plan['tasks'],jobs)):
        name='af-f2-registry-r2-v1-controller-'+str(index);row=ledger.get('tasks',{}).get(name,{})
        command=shlex.split(task.get('command',''));helper=str(c['outer'].BASE)
        require(task.get('name')==name and command.count(helper)==1 and command[command.index(helper)+1:]==[
            '--descriptor',c['base_descriptor']['path'],'--descriptor-sha256',c['base_descriptor']['sha256'],'--segment',str(index)]
            and task.get('resources')=={'cpus':1,'gpu':False,'hours':96,'memory':'16G','array':None}
            and task.get('depends')==([] if index==0 else ['af-f2-registry-r2-v1-controller-'+str(index-1)])
            and task.get('dependency_kind')==('afterok' if index==0 else 'afterany')
            and task.get('external_dependencies')==([c['activation_job']] if index==0 else [])
            and row.get('job_id')==job and row.get('state')=='submitted'
            and row.get('script_sha256')==task.get('script_sha256')==canonical_hash(packet(task['script'])[0].decode()),
            'ACT-created controller source/descriptor/script/DAG identity changed')
        c['pins'][task['script']]=digest(task['script'])
    c['pins'][receipt['plan']['path']]=receipt['plan']['sha256'];c['pins'][ledger_path]=digest(ledger_path)
    return plan,jobs


def quiesce_old(c,plan,jobs,*,runner=subprocess.run,sleeper=time.sleep,clock=time.time):
    require(c.get('registration_locked') is True,'Old controller handoff requires the permanent AF registration lock')
    records=c['native_reader'](jobs,{},owner='pwa209',account='def-ptewarie_cpu')
    require(set(records)==set(jobs) and all(records[job].get('job_id_raw')==job and records[job].get('user')=='pwa209'
        and records[job].get('account')=='def-ptewarie_cpu' and records[job].get('state') in TERMINAL|{'RUNNING','PENDING'} for job in jobs),
        'Exact owned native state for every ACT-created controller required')
    terminal={job for job in jobs if records[job]['state'] in TERMINAL};observations={}
    for task,job in zip(plan['tasks'],jobs):
        if job in terminal:observations[job]={'final_native':records[job]};continue
        observations[job]=native_job(c,task,job,memory=16)
        require(observations[job].get('JobState') in TERMINAL|{'RUNNING','PENDING'},'Unexpected old native controller transition')
    require(sum(value.get('JobState')=='RUNNING' for value in observations.values())<=1,'Old finite chain has conflicting active controllers')
    exclusive(c['area']/'old-controller-native-before.json',observations)
    for task,job in zip(plan['tasks'],jobs):
        if job in terminal or observations[job].get('JobState') in TERMINAL:continue
        current=native_job(c,task,job,memory=16)
        if current.get('JobState')!='PENDING':continue
        if current.get('Reason')!='JobHeldUser':mutation(c,'hold-old-'+job,['scontrol','hold',job],runner=runner)
        held=native_job(c,task,job,memory=16);require(held.get('JobState')=='PENDING' and held.get('Reason')=='JobHeldUser','Old pending controller is not positively held')
    for task,job in zip(plan['tasks'],jobs):
        if job in terminal or observations[job].get('JobState') in TERMINAL:continue
        current=native_job(c,task,job,memory=16)
        if current.get('JobState') in TERMINAL:continue
        require(current.get('JobState')=='RUNNING' or (current.get('JobState')=='PENDING' and current.get('Reason')=='JobHeldUser'),
            'Only exact owned active or positively held ACT controllers may be cancelled')
        mutation(c,'cancel-old-'+job,['scancel',job],runner=runner)
    for attempt in range(30):
        records=c['native_reader'](jobs,{},owner='pwa209',account='def-ptewarie_cpu')
        if set(records)==set(jobs) and all(records[job].get('state') in TERMINAL and records[job].get('job_id_raw')==job
            and records[job].get('user')=='pwa209' and records[job].get('account')=='def-ptewarie_cpu' for job in jobs):
            exclusive(c['area']/'old-controller-final-native.json',records);return records
        require(clock()+600<c['end'],'Old controller terminal proof exceeded native margin');sleeper(5)
    raise ValueError('ACT controllers not positively terminal; no new controller release permitted')


def freeze_new(c,descriptor):
    directory=ROOT/'plans'/PHASE;ledger=ROOT/'state'/(PHASE+'-submission.json')
    require(not directory.exists() and not ledger.exists(),'Fresh finite handoff phase required; no replay');directory.mkdir(mode=0o700)
    tasks=[];previous=None
    for index in range(4):
        name='af-f2-external-census-v1-controller-'+str(index);script=directory/(name+'.sbatch')
        args=['--policy-descriptor',str(descriptor),'--policy-descriptor-sha256',digest(descriptor),'--segment',str(index),
            '--native-plan',str(directory/'plan.json'),'--native-ledger',str(ledger),'--native-task',name,'--native-script',str(script)]
        command=shlex.join([sys.executable,'-s','-u',str(Q2_SUPPORT/OUTER_NAME),*args])
        text=slurm._script(c['config'],FRAMEWORK,name,command,cpus=1,memory='64G',hours=96,gpu=False)
        text=text.replace('set -euo pipefail','set -euo pipefail\nexport TZ=UTC\nexport PYTHONDONTWRITEBYTECODE=1',1)
        exclusive_bytes(script,text.encode());tasks.append({'name':name,'command':command,'script':str(script),'script_sha256':canonical_hash(text),
            'depends':[previous] if previous else [],'dependency_kind':'afterany' if previous else 'afterok',
            'external_dependencies':[] if previous else [c['job']],'resources':{'cpus':1,'gpu':False,'hours':96,'memory':'64G','array':None}})
        previous=name
    plan={'schema_version':2,'phase':PHASE,'root':str(ROOT),'config_sha256':CONFIG,'release_identity':source_identity(FRAMEWORK),
        'tasks':tasks,'maximum_simultaneous_resources':{'cpus':1,'gpus':0},'cpu_ceiling':256,'gpu_concurrency':4,
        'resource_policy':c['policy'].receipt(),'descriptor_path':str(descriptor),'descriptor_sha256':digest(descriptor),
        'scientific_gates_changed':False,'handoff_native_job_id':c['job']}
    path=directory/'plan.json';exclusive(path,plan);return {'path':path,'ledger':ledger,'value':plan}


def new_held(c,task,job):
    require(job not in c['old_jobs'] and job!=c['job'] and re.fullmatch('[1-9][0-9]*',str(job)),
        'Only new source-bound controller allocations may be released or cleaned up')
    value=native_job(c,task,job,memory=64)
    require(value.get('JobState')=='PENDING' and value.get('Reason')=='JobHeldUser','New controller must remain positively user-held')
    expected=c.get('new_dependencies',{}).get(job)
    if expected is not None:
        require(re.fullmatch(re.escape(expected)+r'(?:\(unfulfilled\))?',value.get('Dependency','')),
            'Fresh native dependency changed before release or cleanup')
    return value


def cleanup_new(c,pack,*,runner=subprocess.run):
    if pack is None or not pack['ledger'].is_file():return []
    ledger=json.loads(packet(pack['ledger'])[0]);require(ledger.get('plan_sha256')==canonical_hash(pack['value'])
        and ledger.get('source_sha256')==SOURCE,'Cleanup refuses any unbound or ambiguous ledger')
    result=[]
    for task in pack['value']['tasks']:
        row=ledger.get('tasks',{}).get(task['name'],{});job=row.get('job_id')
        if not job or row.get('state')!='submitted' or row.get('script_sha256')!=task['script_sha256']:continue
        try:
            value=new_held(c,task,job)
            mutation(c,'cleanup-new-'+job,['scancel',job],runner=runner)
            result.append({'job_id':job,'status':'cancel_requested_once_for_positive_new_held_controller','native_owner_proof':value})
        except BaseException as error:result.append({'job_id':job,'status':'not_cancelled_needs_explicit_reconciliation','error':str(error)})
    return result


def journal_snapshot(c):
    """Under AF lock, join completed outside-lock releases without replaying any."""
    require(c.get('registration_locked') is True,'Journal quiescence requires the permanent AF lock')
    case=Path(c['descriptor']['agent_campaign_case']);plan=json.loads(packet(case/'campaign-plan.json',limit=16_000_000)[0])
    compressed,state_file_sha=packet(case/'state.json.gz',limit=64_000_000)
    with gzip.GzipFile(fileobj=io.BytesIO(compressed),mode='rb') as stream:raw_state=stream.read(512_000_001)
    require(len(raw_state)<=512_000_000,'Bounded expanded campaign state required before handoff')
    state=json.loads(raw_state);binding=json.loads(packet(case/'state-binding.json',limit=1_000_000)[0])
    require(binding.get('state_file_sha256')==state_file_sha and binding.get('state_sha256')==state.get('state_sha256')
        and binding.get('campaign_plan_sha256')==state.get('plan_sha256')==plan.get('plan_sha256')
        and binding.get('revision')==state.get('revision'),'Exact unchanged scientific state/binding/plan required')
    c['base'].dispatch._state(plan,state)
    paths=sorted(case.glob('submission*.json'));records={};inventory={};pending=[]
    for path in paths:
        raw,h=packet(path,limit=2_000_000);value=json.loads(raw);status=value.get('status')
        require(status in {'released','registered_held_verified','releasing'},'Partial or ambiguous submission journal forbids controller cancellation')
        if status!='released':pending.append(str(path))
        records[str(path)]=value;inventory[str(path)]=h
    if pending:return None
    expected={}
    for name,job in state.get('jobs',{}).items():
        require(re.fullmatch('[1-9][0-9]*',str(job.get('job_id',''))),'Unbound scientific intent forbids handoff')
        path=c['base'].submission_journal(c['journal_context'],name);expected[str(path)]=(name,job)
        require(str(path) in records,'Active state lacks its exact source-bound released journal')
    for path,value in records.items():
        intent=value.get('intent',{});job=value.get('job_id')
        require(re.fullmatch('[1-9][0-9]*',str(job)) and value.get('scientific_result') is False
            and value.get('admission_authority') is False,'Exact released worker journal identity required')
        if path in expected:
            name,state_job=expected[path]
            require(intent==state_job.get('intent') and intent.get('intent_id')==name and job==state_job['job_id'],
                'Released journal must bind the exact active scientific intent/native job')
            namespace=c['base'].native_namespace(c['journal_context'],name)
            require(Path(value.get('plan_path',''))==ROOT/'plans'/namespace/'plan.json'
                and Path(value.get('ledger_path',''))==ROOT/'state'/(namespace+'-submission.json'),
                'Active worker journal must retain original0000/r2 namespace mappings')
        native_plan=json.loads(packet(value['plan_path'],value['plan_sha256'])[0]);ledger=json.loads(packet(value['ledger_path'])[0])
        tasks=native_plan.get('tasks',[])
        require(len(tasks)==1 and native_plan.get('release_identity',{}).get('sha256')==SOURCE
            and native_plan.get('config_sha256')==CONFIG and native_plan.get('campaign_plan_sha256')==plan['plan_sha256']
            and native_plan.get('intent_sha256')==intent.get('intent_sha256')
            and native_plan.get('worker_descriptor_sha256')==c['descriptor']['worker_descriptor']['sha256']
            and ledger.get('source_sha256')==SOURCE and ledger.get('plan_sha256')==canonical_hash(native_plan),
            'Released journal must retain its original registered scientific worker plan/ledger')
        task=tasks[0];row=ledger.get('tasks',{}).get(task['name'],{});script=packet(task['script'])[0].decode()
        require(row.get('job_id')==job and row.get('state')=='submitted'
            and task.get('script_sha256')==row.get('script_sha256')==canonical_hash(script) and task['command'] in script,
            'Released journal script/native binding changed')
        args=shlex.split(task['command'])
        for flag,actual in (('--descriptor',c['descriptor']['worker_descriptor']['path']),
                            ('--descriptor-sha256',c['descriptor']['worker_descriptor']['sha256'])):
            require(args.count(flag)==1 and args.index(flag)+1<len(args) and args[args.index(flag)+1]==actual,
                'Released worker must retain the immutable qualified worker descriptor')
    return inventory


def await_released_journals(c,*,clock=time.time,sleeper=time.sleep):
    cutoff=min(c['end']-900,clock()+300)
    for attempt in range(61):
        inventory=journal_snapshot(c)
        if inventory is not None:
            require(clock()<cutoff,'Journal quiescence proof exhausted its finite wait budget');return inventory
        require(clock()<cutoff,'Outside-lock worker release remains pending; old controller cannot be cancelled')
        sleeper(min(5,max(0,cutoff-clock())))
    raise ValueError('Outside-lock worker release did not reach a complete released journal; no cancellation permitted')


def verify_protected(c,before,journals):
    require(all(digest(path)==h for path,h in before.items()),'Protected campaign state or journal bytes changed before handoff completion')
    case=Path(c['descriptor']['agent_campaign_case'])
    require({str(path) for path in case.glob('submission*.json')}==set(journals),'Flat submission journal inventory changed during handoff')


def protected_state(c,journals):
    case=Path(c['descriptor']['agent_campaign_case']);out={}
    for name,limit in (('campaign-plan.json',16_000_000),('state.json.gz',64_000_000),('state-binding.json',1_000_000)):
        path=case/name;raw,h=packet(path,limit=limit);exclusive_bytes(c['area']/('preserved-'+name),raw);out[str(path)]=h
    directory=c['area']/'preserved-journals';directory.mkdir(mode=0o700)
    for path,h in journals.items():
        raw,_=packet(path,h,2_000_000);exclusive_bytes(directory/Path(path).name,raw);out[path]=h
    exclusive(c['area']/'protected-state-index.json',out);return out


def orchestrate(c,*,runner=subprocess.run):
    pack=None
    try:
        require(c.get('selftest_proof',{}).get('status')=='complete_native_handoff_contracts_completion_pending',
            'Actual handoff selftests must pass before scheduler mutations')
        with c['registry']['registration_lock'](c['area']/'handoff-registration-lock',c['end'],role='controller-start'):
            c['registration_locked']=True
            try:
                verify_sources(c);old,ids=old_controllers(c);c['old_jobs']=ids
                journals=await_released_journals(c);before=protected_state(c,journals);verify_protected(c,before,journals)
                quiesce_old(c,old,ids,runner=runner)
                verify_protected(c,before,journals)
                c['admission']('handoff-before-new-registration',cpus=1,gpus=0,hours=96)
                descriptor_path=c['area']/'external-census-policy.json';exclusive(descriptor_path,c['outer_policy'])
                c['pins'][str(descriptor_path)]=digest(descriptor_path)
                c['outer'].qualification(c['outer_policy'])
                pack=freeze_new(c,descriptor_path)
                c['pins'][str(pack['path'])]=digest(pack['path'])
                for task in pack['value']['tasks']:c['pins'][task['script']]=digest(task['script'])
                exclusive(c['area']/'new-held-submission-intent.json',{'plan':str(pack['path']),'plan_sha256':canonical_hash(pack['value']),
                    'ledger':str(pack['ledger']),'afterok_handoff_job':c['job'],'automatic_replay_permitted':False})
                def held(command,**kw):
                    verify_sources(c);require(command[0]=='sbatch' and '--hold' not in command,'One exact new held submission required')
                    return runner(['sbatch','--hold',*command[1:]],timeout=30,**kw)
                ledger=slurm.submit(pack['path'],pack['ledger'],runner=held)
                jobs=[ledger['tasks'][task['name']]['job_id'] for task in pack['value']['tasks']]
                require(len(jobs)==len(set(jobs))==4 and not set(jobs)&set(ids),'Exactly four distinct new controller identities required')
                c['pins'][str(pack['ledger'])]=digest(pack['ledger'])
                c['new_dependencies']={job:('afterok:'+c['job'] if index==0 else 'afterany:'+jobs[index-1]) for index,job in enumerate(jobs)}
                proofs={}
                for index,(task,job) in enumerate(zip(pack['value']['tasks'],jobs)):
                    value=new_held(c,task,job);expected=('afterok:'+c['job']) if index==0 else ('afterany:'+jobs[index-1])
                    require(re.fullmatch(re.escape(expected)+r'(?:\(unfulfilled\))?',value.get('Dependency','')),
                        'Native held dependency must bind successful handoff or previous finite controller')
                    proofs[job]=value
                exclusive(c['area']/'new-held-native-proof.json',proofs)
                c['admission']('handoff-all-new-controllers-registered',cpus=0,gpus=0,hours=1)
                verify_sources(c);verify_protected(c,before,journals)
                exclusive(c['area']/'release-intent.json',{'jobs':jobs,'held_proof_sha256':digest(c['area']/'new-held-native-proof.json'),
                    'afterok_handoff_job':c['job'],'automatic_replay_permitted':False})
            finally:c['registration_locked']=False
        for task,job in zip(pack['value']['tasks'],jobs):
            require(not c.get('registration_locked'),'New controllers must be released outside the AF registration lock')
            verify_sources(c);new_held(c,task,job);mutation(c,'release-new-'+job,['scontrol','release',job],runner=runner)
        verify_sources(c);verify_protected(c,before,journals)
        result={'status':'four_owned_ACT_controllers_retired_and_four_finite_external_census_controllers_released',
            'native_job_id':c['job'],'old_jobs':ids,'new_jobs':jobs,'source_manifest':c['manifest_binding'],
            'native_selftests':locator(c['area']/'native-handoff-qualification.json'),'q2':c['qualification'],
            'activation':c['activation_binding'],'controller_policy':locator(descriptor_path),
            'plan':locator(pack['path']),'ledger':locator(pack['ledger']),'protected_state_sha256':before,'flat_submission_journal_inventory':journals,
            'worker_jobs_changed':False,'campaign_state_changed':False,'journals_changed':False,
            'native_handoff_completion_required_before_new_controllers_start':True,**FALSE_FLAGS}
        exclusive(c['area']/'receipt.json',result);return result
    except BaseException as error:
        cleanup=[]
        try:cleanup=cleanup_new(c,pack,runner=runner)
        except BaseException as cleanup_error:cleanup=[{'status':'cleanup_needs_explicit_reconciliation','error':str(cleanup_error)}]
        exclusive(c['area']/'handoff-failure.json',{'status':'needs_explicit_reconciliation','error_type':type(error).__name__,'error':str(error),
            'cleanup':cleanup,'automatic_replay_permitted':False,'old_controllers_never_automatically_released':True,**FALSE_FLAGS});raise


def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv);parser=argparse.ArgumentParser(description=__doc__)
    for name in ('native-plan','native-ledger','native-script','source-manifest'):parser.add_argument('--'+name,type=Path,required=True)
    for name in ('native-task','source-manifest-sha256'):parser.add_argument('--'+name,required=True)
    args=parser.parse_args(argv);os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    value,qmanifest=manifest(args.source_manifest,args.source_manifest_sha256);job=str(native_job_id() or '')
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='4' and os.environ.get('HHW_GPU_COUNT')=='0'
        and os.environ.get('HHW_MAX_HOURS')=='2','Owned native4CPU2h handoff context required')
    import horsehair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,
        'Exact qualified framework and unchanged primary required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval required')
    config=read_json(PRIMARY/'configs/study.json');require(canonical_hash(config)==CONFIG
        and read_json(FRAMEWORK/'configs/study.json')==config,'Full frozen scientific config required')
    area=SUPPORT/('native-'+job);require(not area.exists() and not area.is_symlink(),'Fresh one-shot native handoff area required');area.mkdir(mode=0o700)
    for path,h in ((GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(REGISTRY,REGISTRY_SHA),(POLICY,POLICY_SHA),(ACT_SOURCE,ACT_SHA)):packet(path,h,200_000)
    outer=loaded(Q2_SUPPORT/OUTER_NAME,qmanifest['files'][OUTER_NAME],'handoff_qualified_outer')
    provider=loaded(Q2_SUPPORT/PROVIDER_NAME,qmanifest['files'][PROVIDER_NAME],'handoff_qualified_provider')
    own_task,own_job,own_registration=registration_record({'plan_path':str(args.native_plan),'ledger_path':str(args.native_ledger),
        'task_name':args.native_task,'script_path':str(args.native_script)})
    require(own_job==job,'Actual handoff job differs from registered own identity')
    own_binding={'path':str(args.source_manifest),'sha256':args.source_manifest_sha256}
    outer.registered(own_registration['plan'],own_registration['ledger'],args.native_task,own_registration['script'],job,
        helper=Path(__file__),hours=2,cpus=4,memory='16G',argv=argv)
    require(own_task.get('external_census_handoff')=={'helper':{'path':str(Path(__file__)),'sha256':value['files'][SELF_NAME]},
        'source_manifest':own_binding},'Registered handoff source/manifest intent differs')
    scheduler=runpy.run_path(str(SCHEDULER))['run_scontrol_job'];allocation,end=native_allocation(job,args.native_task,args.native_script,scheduler,area)
    def expired(signum,frame):raise TimeoutError('Finite native handoff margin exhausted; no replay')
    signal.signal(signal.SIGALRM,expired);signal.alarm(max(1,int(end-time.time()-180)))
    runpy.run_path(str(GUARD))['install'](area/'accounting-reads')
    registry=runpy.run_path(str(REGISTRY));registry['install'](area/'registered-snapshot-reads',registration_mode='controller-already-locked',allocation_deadline_unix=end)
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'358 must precede campaign native binding')
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    qtask,qjob,qreg=registration_record(value['q2_registration']);atask,ajob,areg=registration_record(value['activation_registration'])
    require(own_task.get('external_dependencies')==[ajob] and own_task.get('dependency_kind','afterok')=='afterok',
        'Native handoff must depend afterok on the exact ACT allocation')
    admission=lambda label,**resources:campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],area,
        label,resource_policy=policy,**resources)
    pins={str(SUPPORT/name):h for name,h in value['files'].items()}
    pins.update({str(Q2_SUPPORT/name):h for name,h in qmanifest['files'].items()})
    for item in (own_registration,qreg,areg):
        for key in ('plan','ledger','script'):pins[item[key]['path']]=item[key]['sha256']
    pins.update({str(args.source_manifest):args.source_manifest_sha256,value['q2_source_manifest']['path']:value['q2_source_manifest']['sha256'],
        str(GUARD):GUARD_SHA,str(SCHEDULER):SCHEDULER_SHA,str(REGISTRY):REGISTRY_SHA,str(POLICY):POLICY_SHA,str(ACT_SOURCE):ACT_SHA})
    c={'manifest':value,'manifest_binding':own_binding,'job':job,'area':area,'end':end,'config':config,'policy':policy,
        'outer':outer,'provider':provider,'base':outer.load_base(),'registry':registry,'scheduler':scheduler,
        'native_reader':repair._repair_native_states,'admission':admission,'pins':pins,'old_jobs':[],'allocation':allocation}
    with resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=2,job_id=job,resource_policy=policy):
        self_qualification(c);verify_sources(c)
        with registry['registration_lock'](area/'producer-accounting-lock',end,role='controller-start'):
            admission('handoff-finalized-producer-accounting',cpus=0,gpus=0,hours=1)
        observed=repair._repair_native_states([qjob,ajob],{},owner='pwa209',account='def-ptewarie_cpu')
        for ident,cpus,hours in ((qjob,8,1),(ajob,4,4)):
            row=observed.get(ident,{})
            require(row.get('state')=='COMPLETED' and row.get('exit_code')=='0:0' and row.get('job_id_raw')==ident
                and row.get('user')=='pwa209' and row.get('account')=='def-ptewarie_cpu' and row.get('unallocated_metadata') is False
                and type(row.get('cpus')) is int and row['cpus']==cpus and type(row.get('elapsed_seconds')) is int
                and 0<row['elapsed_seconds']<=hours*3600 and 'gpu' not in row.get('alloc_tres','').lower()
                and slurm.completed_native_dependency(ROOT,ident) is not None,'Exact positive finalized owned Q2/ACT native producers required')
        activation_path=ACT_AREA/'receipt.json';activation_raw,activation_sha=packet(activation_path);activation=json.loads(activation_raw)
        require(activation.get('status')=='three_target_derivative_installed_and_four_finite_controllers_released'
            and activation.get('native_job_id')==ajob and activation.get('scientific_result') is False and activation.get('admission_authority') is False,
            'Positive exact ACT-created four-controller receipt required')
        require(str(ACT_SOURCE) in shlex.split(atask.get('command','')),'Actual activation registration must invoke unchanged ACT helper')
        descriptor_binding=activation['controller_descriptor'];require(descriptor_binding['path']==str(ACT_AREA/'descriptor-prestart-three-v1.json'),
            'Exact ACT-created immutable controller descriptor required')
        descriptor=json.loads(packet(descriptor_binding['path'],descriptor_binding['sha256'])[0])
        c['base'].worker_binding(descriptor);registry_binding=c['base'].registry_binding(descriptor)
        retry=c['base'].retry_binding(descriptor)
        require(descriptor.get('controller_sha256')==outer.BASE_SHA,'ACT descriptor must retain frozen controller6')
        qraw,qsha=packet(Q2_PATH);q=json.loads(qraw);require(q.get('slurm_job_id')==qjob,'Actual Q2 receipt must bind selected registered producer')
        qbinding={'path':str(Q2_PATH),'sha256':qsha,'job_id':qjob}
        policy_descriptor={'kind':'af-controller-external-census-policy-v1','base_controller':{'path':str(outer.BASE),'sha256':outer.BASE_SHA},
            'base_descriptor':descriptor_binding,'policy_source':{'path':str(Q2_SUPPORT/OUTER_NAME),'sha256':qmanifest['files'][OUTER_NAME]},
            'qualification':qbinding,'provider_schedule':{'plan':locator(value['provider_registration']['plan_path']),
                'ledger':locator(value['provider_registration']['ledger_path']),'helper':{'path':str(Q2_SUPPORT/PROVIDER_NAME),'sha256':qmanifest['files'][PROVIDER_NAME]},
                'source_manifest':value['q2_source_manifest'],'qualification':qbinding,'case_root':str(outer.PROVIDER_CASE)}}
        checked=outer.qualification(policy_descriptor);pins.update(checked);pins.update({str(activation_path):activation_sha,
            descriptor_binding['path']:descriptor_binding['sha256']})
        for item in descriptor['registry_overlay'].values():pins[item['path']]=item['sha256']
        for key in ('worker_descriptor','qualification','gpu_qualification'):
            item=descriptor[key];pins[item['path']]=item['sha256']
        if 'prestart_retry_amendment' in descriptor:
            item=descriptor['prestart_retry_amendment'];pins[item['path']]=item['sha256']
        pins[descriptor['worker_path']]=descriptor['worker_sha256']
        q6=json.loads(packet(descriptor['registry_overlay']['qualification']['path'],descriptor['registry_overlay']['qualification']['sha256'])[0])
        for item in q6.get('test_sources',{}).values():pins[item['path']]=item['sha256']
        if 'junit' in q6:pins[q6['junit']['path']]=q6['junit']['sha256']
        journal_context={'descriptor':descriptor,'case':Path(descriptor['agent_campaign_case']),
            'registry_overlay':{**registry_binding,'prestart_retry_value':retry,
                'prestart_retry_binding':dict(descriptor['prestart_retry_amendment']) if 'prestart_retry_amendment' in descriptor else None}}
        c.update(activation=activation,activation_binding={'path':str(activation_path),'sha256':activation_sha,'job_id':ajob},
            activation_job=ajob,base_descriptor=descriptor_binding,descriptor=descriptor,outer_policy=policy_descriptor,qualification=qbinding,
            journal_context=journal_context)
        result=orchestrate(c)
    signal.alarm(0);print(json.dumps({'status':result['status'],'old_jobs':result['old_jobs'],'new_jobs':result['new_jobs']},sort_keys=True));return result


if __name__=='__main__':main()
