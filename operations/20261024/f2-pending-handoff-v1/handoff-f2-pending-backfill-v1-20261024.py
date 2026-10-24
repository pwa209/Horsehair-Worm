"""One allocated, pending-only F2 handoff. No ambiguous mutation is replayed."""
from pathlib import Path
from datetime import datetime,timezone
import argparse
import importlib.util
import json
import math
import os
import re
import runpy
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

from horsehair import slurm
from horsehair.core import canonical_hash,digest,native_job_id,read_json,source_identity

ROOT=Path('/scratch/pwa209/horsehair-worm')
SUPPORT=ROOT/'maintenance/study-af-pending-handoff-support-v1-20261024'
BACKFILL_SUPPORT=ROOT/'maintenance/study-af-backfill-controller-support-v1-20261024'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
OLD_IDS=('6868746','6868747','6868750','6868753')
OLD_PHASE='study-af-f2-external-census-controllers-v1-20261023'
OLD_PLAN=ROOT/'plans'/OLD_PHASE/'plan.json'
OLD_LEDGER=ROOT/'state'/(OLD_PHASE+'-submission.json')
OLD_PLAN_SHA='33e8a9b97997caf3f50ef1b0c4e3deac6186fd7738cac8c326f49aeb46ee7f01'
OLD_LEDGER_SHA='02c8d1f2cd74e8405c2a4fcee3b0c8d5c758dbd5f8f2a20910058680232dc728'
OLD_POLICY=ROOT/'maintenance/study-af-external-census-handoff-v1-20261023/native-6865706/external-census-policy.json'
OLD_POLICY_SHA='2b0e6ebaa0f1b511bca4343d4722d43702cf051f956cce59494ddccae656c517'
OLD_DESCRIPTOR=ROOT/'maintenance/study-af-registry-repair-support-v6-20261022/native-activation-prestart-three-v1/descriptor-prestart-three-v1.json'
OLD_DESCRIPTOR_SHA='9a0706642ac1660fb7f39fe68279ca34bdc10f89812a0c2397608d2a382cf464'
OLD_HELPER=ROOT/'maintenance/study-af-rolling-census-support-v4-20261023/run-af-agent-controller-census-policy-v1-20261023.py'
OLD_HELPER_SHA='120f02d098b6c9afa0ea0df54f590ae381b4227d4892ba0f9ee4acc93e6fec0f'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-scheduler-read.py'
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
REGISTRY_SHA='7bd2bf562d0a81887ef1afd308ddd9265e5cadb23ba4b2e5946980b659505431'
SELF_NAME=Path(__file__).name
TEST_NAME='test_f2_pending_handoff_v1_20261024.py'
CANARY_NAME='run_emitted_descriptor_canary.py'
NEW_PHASE='study-af-f2-backfill-controllers-v1-20261024'
FALSE_FLAGS={name:False for name in ('scientific_result','admission_authority','scientific_gates_cleared','worker_jobs_changed','campaign_state_changed')}


class OldStarted(ValueError):pass


def require(value,message):
    if not value:raise ValueError(message)


def packet(path,sha=None,limit=8_000_000):
    path=Path(path)
    require(path.is_absolute() and path==path.resolve() and path.is_relative_to(ROOT) and path!=ROOT
        and path.is_file() and all(not p.is_symlink() for p in (path,*path.parents) if p.is_relative_to(ROOT)),
        'Exact confined ordinary evidence file required')
    with path.open('rb') as stream:raw=stream.read(limit+1)
    actual=__import__('hashlib').sha256(raw).hexdigest()
    require(len(raw)<=limit and (sha is None or sha==actual),'Frozen source/evidence bytes changed')
    return raw,actual


def locator(path):return {'path':str(path),'sha256':digest(path)}


def exclusive(path,value):
    with Path(path).open('x',encoding='utf8',newline='\n') as stream:
        json.dump(value,stream,sort_keys=True,indent=2);stream.write('\n')


def loaded(path,sha,name):
    packet(path,sha,300_000)
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    require(digest(path)==sha,'Source changed during module import');return module


def seconds(value):
    match=re.fullmatch(r'(?:(\d+)-)?(\d+):(\d{2}):(\d{2})',value)
    require(match is not None,'Finite native time required')
    d,h,m,s=(int(v or 0) for v in match.groups());return d*86400+h*3600+m*60+s


def fields(response):
    response.check_returncode();lines=[line for line in response.stdout.splitlines() if line.strip()]
    require(len(lines)==1,'Exactly one native scheduler record required')
    pairs=re.findall(r'(?<!\S)(\w+)=([^\s]*)',lines[0]);value=dict(pairs)
    require(len(value)==len(pairs),'Duplicate native scheduler field refused');return value


def bounded_native_reader(original,evidence,allocation_end,*,transport=subprocess.run,wall=time.time,mono=time.monotonic):
    """Bound real sacct transport while retaining unchanged 358 parsing/retries."""
    marker=getattr(original,'__hhw_native_accounting_read_guard__',None)
    require(type(marker) is dict and marker.get('path')==str(GUARD) and marker.get('sha256')==GUARD_SHA,
        'Only exact already installed 358 native reader may be bounded')
    require(type(allocation_end) in (int,float) and math.isfinite(allocation_end) and allocation_end>wall()+600,
        'Finite native read finalization reserve required')
    evidence=Path(evidence)
    require(evidence.is_absolute() and evidence==evidence.resolve() and evidence.is_relative_to(SUPPORT)
        and not evidence.exists() and not evidence.is_symlink(),'Fresh bounded native transport evidence required')
    evidence.mkdir(mode=0o700);sequence=[0]
    def read(ids,rosters,*,runner=None,owner=None,account=None):
        actual=transport if runner is None else runner
        deadline=min(mono()+150,mono()+allocation_end-wall()-600)
        def bounded(command,**kwargs):
            require(isinstance(command,(list,tuple)) and len(command)==8 and command[:3]==['sacct','--array','-j']
                and re.fullmatch('[0-9]+(?:_[0-9]+)?(?:,[0-9]+(?:_[0-9]+)?)*',command[3])
                and command[4].startswith('--format=') and command[5:]==['--units=K','--noheader','--parsable2'],
                'Only the unchanged strict read-only sacct command is supported')
            maximum=min(45.,deadline-mono(),allocation_end-wall()-600)
            if 'timeout' in kwargs:
                requested=kwargs['timeout'];require(type(requested) in (int,float) and math.isfinite(requested) and requested>0,
                    'Finite positive upstream timeout required');maximum=min(maximum,requested)
            require(math.isfinite(maximum) and maximum>0,'Native query deadline exhausted before transport')
            sequence[0]+=1;prefix=evidence/('query-'+str(sequence[0]).zfill(6))
            exclusive(str(prefix)+'-intent.json',{'command':list(command),'maximum_seconds':maximum,
                'maximum_original_read_seconds':150,'native_allocation_end_unix':allocation_end,'new_layer_retries':0})
            kwargs['timeout']=maximum
            try:response=actual(command,**kwargs)
            except BaseException as error:
                exclusive(str(prefix)+'-result.json',{'status':'native_read_failed_without_added_retry','error_type':type(error).__name__,'error':str(error)});raise
            exclusive(str(prefix)+'-result.json',{'status':'returned_to_unchanged_strict_parser','returncode':response.returncode,
                'stdout_sha256':__import__('hashlib').sha256((response.stdout or '').encode()).hexdigest()})
            return response
        return original(ids,rosters,runner=bounded,owner=owner,account=account)
    read.__hhw_native_accounting_read_guard__=dict(marker)
    read.__hhw_pending_handoff_deadline_layer__={'source_sha256':digest(Path(__file__)),'maximum_query_seconds':45,
        'maximum_original_read_seconds':150,'additional_retries':0}
    return read


def request_tres(value,cpus,memory):
    items={}
    for item in value.split(','):
        require(item.count('=')==1,'Malformed TRES request');key,val=item.split('=',1)
        require(key not in items and key,'Duplicate TRES request');items[key]=val
    require(items.get('cpu')==str(cpus) and items.get('node')=='1' and items.get('mem')==memory
        and not any('gpu' in key.lower() for key in items)
        and set(items)<={'cpu','node','mem','billing'},'Exact scalar CPU/memory/no-GPU TRES required')
    if 'billing' in items:require(re.fullmatch('[1-9][0-9]*',items['billing']),'Finite positive billing TRES required')


def pending_fields(value,task,job,*,held=False,backfill=False):
    require(value.get('JobId')==job and value.get('UserId')=='pwa209(3164768)'
        and value.get('Account')=='def-ptewarie_cpu' and value.get('JobName')=='hhw-'+task['name']
        and value.get('Command')==task['script'] and value.get('WorkDir')==str(FRAMEWORK.resolve()),
        'Exact owned task/name/script/working-directory identity required')
    require(value.get('NumCPUs')=='1' and value.get('NumNodes') in {'1','1-1'}
        and value.get('MinMemoryNode') in {'64G','65536M'} and seconds(value.get('TimeLimit',''))==96*3600
        and 'ArrayJobId' not in value and 'ArrayTaskId' not in value
        and all('gpu' not in value.get(key,'').lower() for key in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Exact original scalar native resource request required')
    request_tres(value.get('ReqTRES',''),1,'64G')
    if value.get('JobState')!='PENDING':raise OldStarted('Exact old/native job is no longer PENDING: '+job+' '+str(value.get('JobState')))
    require(value.get('RunTime')=='00:00:00' and value.get('StartTime') in {'Unknown','None'}
        and value.get('NodeList') in {'','(null)','None'} and value.get('AllocTRES')=='(null)'
        and value.get('Restarts')=='0' and value.get('Requeue')=='0' and value.get('ExitCode')=='0:0'
        and value.get('NumCPUs')=='1' and value.get('NumNodes') in {'1','1-1'}
        and value.get('MinMemoryNode') in {'64G','65536M'} and seconds(value.get('TimeLimit',''))==96*3600
        and 'ArrayJobId' not in value and 'ArrayTaskId' not in value
        and all('gpu' not in value.get(key,'').lower() for key in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Exact scalar unstarted one-CPU64G96h PENDING allocation required')
    request_tres(value.get('ReqTRES',''),1,'64G')
    if held:require(value.get('Reason')=='JobHeldUser' and value.get('Priority')=='0','Positive owned user-held native record required')
    if backfill:require(seconds(value.get('TimeMin',''))==8*3600,'Exact native eight-hour TimeMin required')
    else:require(value.get('TimeMin')=='N/A','Original finite allocation TimeMin changed')
    return value


def unused_native(row,job,*,state):
    require(row.get('job_id_raw')==job and row.get('user')=='pwa209' and row.get('account')=='def-ptewarie_cpu'
        and row.get('state')==state and state in {'PENDING','CANCELLED'} and row.get('exit_code')=='0:0'
        and type(row.get('cpus')) is int and row['cpus']==0 and type(row.get('elapsed_seconds')) is int and row['elapsed_seconds']==0
        and row.get('alloc_tres')=='' and row.get('node_list')=='None assigned'
        and row.get('start') in {'None','Unknown'} and row.get('max_rss') in {'','0','0K'}
        and row.get('unallocated_metadata') is False
        and row.get('identity_basis')=='Unique scalar native job; zero-allocation queue record',
        'Positive exact scalar native proof must establish zero allocation and no start')
    require(row.get('end') in {'None','Unknown'} if state=='PENDING' else
        bool(re.fullmatch('[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:]+',str(row.get('end','')))),
        'Native unused record end time is inconsistent');return row


def confirm_started_old(c,task,job,value):
    """Only positive physical native evidence can authorize restoration mode."""
    try:pending_fields(value,task,job)
    except OldStarted:pass
    else:raise ValueError('A PENDING old job cannot be treated as a started original')
    rows=c['native_reader']([job],{},owner='pwa209',account='def-ptewarie_cpu')
    require(set(rows)=={job},'Exact original start-race accounting identity required');row=rows[job]
    require(row.get('job_id_raw')==job and row.get('user')=='pwa209' and row.get('account')=='def-ptewarie_cpu'
        and row.get('state') not in {None,'PENDING','CANCELLED'} and type(row.get('cpus')) is int and row['cpus']==1
        and type(row.get('elapsed_seconds')) is int and row['elapsed_seconds']>=0
        and row.get('node_list') not in {None,'','None assigned'} and row.get('start') not in {None,'','None','Unknown'}
        and row.get('unallocated_metadata') is False and row.get('identity_basis')=='Actual native allocation identity'
        and bool(row.get('alloc_tres')) and 'gpu' not in row['alloc_tres'].lower(),
        'Start-race restoration requires positive owned physical old allocation proof')
    c.setdefault('positively_started_old',set()).add(job);c.setdefault('started_old_native',{})[job]=row
    return row


def no_old_leases(c,*,target=None):
    record=read_json(ROOT/'state/resource-budget.json')
    require(type(record.get('leases')) is dict,'Actual durable lease map required')
    old_leases=set(OLD_IDS)&set(record['leases'])
    if old_leases:
        tasks=c.get('old_tasks',[]);require(len(tasks)==4,'Original task bindings required to classify an old lease')
        for job in sorted(old_leases-set(c.get('positively_started_old',set()))):
            task=tasks[OLD_IDS.index(job)];value=native(c,job,'old-lease-start');c.setdefault('old_observations',{})[job]=value
            confirm_started_old(c,task,job,value)
        if c.get('restoring_after_positive_start_race'):
            require(target in OLD_IDS and target not in old_leases and old_leases<=set(c.get('positively_started_old',set())),
                'Only the exact still-held pending target may be restored without a lease')
            return {'target':target,'target_lease_absent':True,'positively_started_original_leases':sorted(old_leases)}
        raise OldStarted('Positive physical original allocation now has a durable lease: '+','.join(sorted(old_leases)))
    require(target is None or target in OLD_IDS,'Exact original restoration target required')
    return {'old_jobs':list(OLD_IDS),'leases_absent':True}


def native(c,job,label):
    c['reads']=c.get('reads',0)+1
    return fields(c['scheduler'](job,c['area']/(label+'-'+str(c['reads']).zfill(6))))


def old_pending(c,task,job,*,held=False):
    require(job in OLD_IDS,'Only exact original controller identities may be managed')
    value=native(c,job,'old-native');c.setdefault('old_observations',{})[job]=value
    try:value=pending_fields(value,task,job,held=held)
    except OldStarted:
        confirm_started_old(c,task,job,value);raise
    index=OLD_IDS.index(job)
    expected='(null)' if index==0 else 'afterany:'+OLD_IDS[index-1]+'(unfulfilled)'
    require(value.get('Dependency')==expected,'Original native afterany DAG changed')
    if not held:require(not value.get('Reason','').startswith('JobHeld') and bool(re.fullmatch('[1-9][0-9]*',value.get('Priority',''))),
        'An existing external hold must never be adopted or released by this handoff')
    records=c['native_reader']([job],{},owner='pwa209',account='def-ptewarie_cpu')
    require(set(records)=={job},'Exactly one old accounting identity required')
    unused_native(records[job],job,state='PENDING')
    c['no_old_leases'](target=job if c.get('restoring_after_positive_start_race') else None);return value


def old_bindings(c):
    plan=json.loads(packet(OLD_PLAN,OLD_PLAN_SHA)[0]);ledger=json.loads(packet(OLD_LEDGER,OLD_LEDGER_SHA)[0])
    require(plan.get('root')==str(ROOT) and plan.get('phase')==OLD_PHASE and len(plan.get('tasks',[]))==4
        and plan.get('release_identity',{}).get('sha256')==ledger.get('source_sha256')==SOURCE
        and plan.get('config_sha256')==ledger.get('config_sha256')==CONFIG and canonical_hash(plan)==ledger.get('plan_sha256')
        and set(ledger.get('tasks',{}))=={'af-f2-external-census-v1-controller-'+str(i) for i in range(4)},'Exact original plan/ledger binding required')
    for index,task in enumerate(plan['tasks']):
        name='af-f2-external-census-v1-controller-'+str(index);row=ledger['tasks'][name];path=OLD_PLAN.parent/(name+'.sbatch')
        raw,sha=packet(path);text=raw.decode();tokens=shlex.split(task['command']);kind='afterok' if index==0 else 'afterany'
        dependency='6865706' if index==0 else OLD_IDS[index-1]
        require(task.get('name')==name and task.get('script')==str(path) and row.get('job_id')==OLD_IDS[index]
            and row.get('state')=='submitted' and canonical_hash(text)==task.get('script_sha256')==row.get('script_sha256')
            and task['command'] in text and task.get('resources')=={'cpus':1,'memory':'64G','hours':96,'gpu':False,'array':None}
            and task.get('depends')==([] if index==0 else ['af-f2-external-census-v1-controller-'+str(index-1)])
            and task.get('external_dependencies')==(['6865706'] if index==0 else []) and task.get('dependency_kind')==row.get('dependency_kind')==kind
            and row.get('issued_native_dependencies')==[dependency] and row.get('completed_native_dependencies')==[]
            and row.get('command')==['sbatch','--parsable','--dependency='+kind+':'+dependency,str(path)]
            and tokens.count(str(OLD_HELPER))==1,'Exact original scripts, resources, confirmed IDs and native DAG required')
        expected=['--policy-descriptor',str(OLD_POLICY),'--policy-descriptor-sha256',OLD_POLICY_SHA,'--segment',str(index),
            '--native-plan',str(OLD_PLAN),'--native-ledger',str(OLD_LEDGER),'--native-task',name,'--native-script',str(path)]
        require(tokens[tokens.index(str(OLD_HELPER))+1:]==expected,'Every original controller argument must match')
        c['pins'][str(path)]=sha
    c['pins'].update({str(OLD_PLAN):OLD_PLAN_SHA,str(OLD_LEDGER):OLD_LEDGER_SHA,str(OLD_HELPER):OLD_HELPER_SHA,
        str(OLD_POLICY):OLD_POLICY_SHA,str(OLD_DESCRIPTOR):OLD_DESCRIPTOR_SHA})
    return plan


def protected_inventory(c):
    descriptor=json.loads(packet(OLD_DESCRIPTOR,OLD_DESCRIPTOR_SHA)[0]);case=Path(descriptor['agent_campaign_case'])
    require(case==ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1','Exact original campaign namespace required')
    paths=[case/name for name in ('campaign-plan.json','state-binding.json','state.json.gz')]
    paths+=sorted(case.glob('submission*.json'))
    paths += [Path(descriptor['worker_path']),Path(descriptor['worker_descriptor']['path'])]
    paths += [Path(descriptor[key]['path']) for key in ('qualification','gpu_qualification')]
    paths += [Path(value['path']) for value in descriptor['registry_overlay'].values()]
    return {str(path):packet(path,limit=64_000_000)[1] for path in paths}


def verify_sources(c):
    require(time.time()+600<c['end'],'Finite native handoff finalization margin exhausted')
    for path,sha in c['pins'].items():packet(path,sha,64_000_000)
    require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,'Original execution or primary source changed')
    if 'protected' in c:
        observed=protected_inventory(c)
        if c.get('restoring_after_positive_start_race'):
            require(bool(c.get('positively_started_old')) and not c.get('cancellation_started'),
                'Controlled restoration requires positive original start proof before any cancellation')
            # A positively identified original controller may already publish
            # state/journals. Restore only our still-held old jobs, while every
            # worker identity and immutable campaign plan stay pinned.
            fixed={path:sha for path,sha in c['protected'].items() if Path(path).name not in {'state.json.gz','state-binding.json'} and not Path(path).name.startswith('submission')}
            require(all(observed.get(path)==sha for path,sha in fixed.items()),'Immutable worker/campaign identity changed during start-race restoration')
            bindings=[Path(path) for path in c['protected'] if Path(path).name=='state-binding.json']
            require(len(bindings)==1,'Exactly one original state binding required for controlled restoration')
            binding_path=bindings[0];binding=json.loads(packet(binding_path)[0]);case=binding_path.parent
            plan=json.loads(packet(case/'campaign-plan.json',c['protected'][str(case/'campaign-plan.json')])[0])
            require(set(binding)=={'state_file_sha256','state_sha256','campaign_plan_sha256','revision','scientific_result','admission_authority'}
                and binding['campaign_plan_sha256']==plan['plan_sha256']
                and binding['scientific_result'] is False and binding['admission_authority'] is False
                and type(binding['revision']) is int and binding['revision']>=c.get('protected_binding_revision',0)
                and bool(re.fullmatch('[0-9a-f]{64}',str(binding['state_sha256'])))
                and binding['state_file_sha256']==packet(case/'state.json.gz')[1],
                'Only source-bound original state/hash/revision advancement may accompany a confirmed start race')
            c['start_race_protected_observation']=observed
        elif observed!=c['protected']:
            c['no_old_leases']()  # A positive original start is classified before strict state refusal.
            raise ValueError('Campaign state, worker identities or flat submission journals changed')


def mutation(c,name,command,*,runner=subprocess.run,precheck=None):
    exclusive(c['area']/(name+'-intent.json'),{'command':command,'automatic_replay_permitted':False,**FALSE_FLAGS})
    result={'command':command,'status':'stopped_ambiguous','mutation_may_have_occurred':False}
    try:
        if precheck is not None:result['precheck']=precheck()
        c['verify']()
        result['mutation_may_have_occurred']=True
        response=runner(command,text=True,capture_output=True,timeout=30)
        result.update(returncode=response.returncode,stdout=response.stdout,stderr=response.stderr)
        response.check_returncode();result['status']='native_command_returned_zero';return response
    except BaseException as error:
        result.update(exception=type(error).__name__,message=str(error)[:4000]);raise
    finally:exclusive(c['area']/(name+'-result.json'),result)


def restore_confirmed_old_holds(c,tasks,held,*,operation=mutation):
    restored=[]
    require(not c.get('cancellation_started'),'Start-race restoration is forbidden after any cancellation begins')
    c['restoring_after_positive_start_race']=True
    try:
        for task,job in reversed(list(zip(tasks,OLD_IDS))):
            if job not in held:continue
            # This path is used only after a positively identified start race
            # before cancellation. Never release foreign or ambiguous jobs.
            try:old_pending(c,task,job,held=True)
            except OldStarted:continue
            operation(c,'restore-old-'+job,['scontrol','release',job],precheck=lambda task=task,job=job:old_pending(c,task,job,held=True))
            restored.append(job)
    finally:c['restoring_after_positive_start_race']=False
    return restored


def hold_all_old(c,tasks,*,operation=mutation):
    require(c.get('registration_locked') is True,'Permanent AF registration lock required')
    c['old_tasks']=tasks
    held=[]
    try:
        for task,job in zip(tasks,OLD_IDS):old_pending(c,task,job)
        for task,job in zip(tasks,OLD_IDS):
            operation(c,'hold-old-'+job,['scontrol','hold',job],precheck=lambda task=task,job=job:old_pending(c,task,job))
            old_pending(c,task,job,held=True);held.append(job);c['old_held']=list(held)
        for task,job in zip(tasks,OLD_IDS):old_pending(c,task,job,held=True)
        return {'status':'all_four_original_controllers_positively_held','held':held}
    except OldStarted as error:
        restored=restore_confirmed_old_holds(c,tasks,held,operation=operation)
        return {'status':'declined_original_job_no_longer_pending','reason':str(error),'restored_positive_old_holds':restored,
            'scheduler_cancellation_performed':False,'old_native_observations':c.get('old_observations',{}),
            'original_start_protected_observation':c.get('start_race_protected_observation')}


def terminal_old(c):
    rows=c['native_reader'](list(OLD_IDS),{},owner='pwa209',account='def-ptewarie_cpu')
    require(set(rows)==set(OLD_IDS),'All four final old native records required')
    for job in OLD_IDS:unused_native(rows[job],job,state='CANCELLED')
    c['no_old_leases']();return rows


def retire_old(c,tasks,*,operation=mutation,sleeper=time.sleep):
    require(c.get('registration_locked') is True,'Retirement requires permanent AF registration lock')
    for task,job in zip(tasks,OLD_IDS):old_pending(c,task,job,held=True)
    for task,job in reversed(list(zip(tasks,OLD_IDS))):
        command=['scancel','--ctld','--state=PENDING','--user=pwa209','--name=hhw-'+task['name'],job]
        operation(c,'retire-old-'+job,command,precheck=lambda task=task,job=job:old_pending(c,task,job,held=True))
        c['cancellation_started']=True
        for attempt in range(12):
            rows=c['native_reader']([job],{},owner='pwa209',account='def-ptewarie_cpu')
            require(set(rows)=={job},'Exact pending-only cancellation accounting target required')
            row=rows[job]
            if row.get('state')=='CANCELLED':unused_native(row,job,state='CANCELLED');break
            unused_native(row,job,state='PENDING')
            require(attempt<11 and time.time()+600<c['end'],'No finite positive cancellation proof; no replay');sleeper(5)
        exclusive(c['area']/('retire-old-'+job+'-terminal.json'),row)
    result=terminal_old(c);exclusive(c['area']/'old-controller-final-native.json',result);return result


def new_held(c,task,job,index,jobs,ledger):
    require(job not in {*OLD_IDS,c['job']} and len(jobs)==len(set(jobs))==4,'Four distinct newly submitted native identities required')
    row=ledger['tasks'][task['name']];kind='afterok' if index==0 else 'afterany';dependency=c['job'] if index==0 else jobs[index-1]
    require(ledger.get('plan_sha256')==canonical_hash(c['new_plan']) and ledger.get('source_sha256')==SOURCE
        and row.get('state')=='submitted' and row.get('job_id')==job and row.get('script_sha256')==task['script_sha256']
        and row.get('dependency_kind')==kind and row.get('issued_native_dependencies')==[dependency]
        and row.get('completed_native_dependencies')==[]
        and row.get('command')==['sbatch','--parsable','--dependency='+kind+':'+dependency,task['script']],
        'Exact new source-bound confirmed ledger and issued native dependency required')
    raw,_=packet(task['script']);text=raw.decode()
    require(canonical_hash(text)==task['script_sha256'] and task['command'] in text
        and text.splitlines().count('#SBATCH --time-min=08:00:00')==1
        and task.get('resources')=={'cpus':1,'memory':'64G','hours':96,'gpu':False,'array':None}
        and task.get('depends')==([] if index==0 else [c['new_plan']['tasks'][index-1]['name']])
        and task.get('external_dependencies')==([c['job']] if index==0 else []) and task.get('dependency_kind')==kind,
        'Exact new held script, conservative resources, TimeMin and DAG required')
    try:value=pending_fields(native(c,job,'new-native'),task,job,held=True,backfill=True)
    except OldStarted as error:raise ValueError('New held controller is no longer pending; preserve without automatic cleanup') from error
    require(value.get('Dependency')==kind+':'+dependency+'(unfulfilled)','Exact held new native dependency required')
    rows=c['native_reader']([job],{},owner='pwa209',account='def-ptewarie_cpu')
    require(set(rows)=={job},'Exact new held native accounting identity required');unused_native(rows[job],job,state='PENDING')
    return value


def orchestrate(c,*,runner=subprocess.run):
    require(c.get('selftest_proof',{}).get('status')=='complete_native_pending_handoff_contracts_completion_pending',
        'Actual complete native contract qualification must precede scheduler mutation')
    with c['registry']['registration_lock'](c['area']/'handoff-lock',c['end'],role='controller-start'):
        c['registration_locked']=True
        try:
            c['verify']();plan=old_bindings(c);c['old_tasks']=plan['tasks'];c['protected']=protected_inventory(c)
            binding_paths=[Path(path) for path in c['protected'] if Path(path).name=='state-binding.json']
            require(len(binding_paths)==1,'Exact original state binding required before handoff')
            c['protected_binding_revision']=json.loads(packet(binding_paths[0],c['protected'][str(binding_paths[0])])[0])['revision']
            exclusive(c['area']/'protected-before.json',c['protected'])
            directory=ROOT/'plans'/NEW_PHASE;ledger_path=ROOT/'state'/(NEW_PHASE+'-submission.json')
            new_area=BACKFILL_SUPPORT/('native-handoff-'+c['job'])
            require(not directory.exists() and not ledger_path.exists() and not new_area.exists(),
                'Fresh target plan, submission ledger and descriptor evidence only; explicit reconciliation required on replay')
            for task,job in zip(plan['tasks'],OLD_IDS):old_pending(c,task,job)
            c['admission']('pending-handoff-before-new-registration',cpus=1,gpus=0,hours=96)
            new_area.mkdir(mode=0o700)
            pack=c['builder'].controller_plan(c['qualification_report'],handoff_job_id=c['job'],area=new_area)
            c['new_plan']=read_json(pack['plan']);c['pins'][pack['plan']]=digest(pack['plan'])
            for task in c['new_plan']['tasks']:c['pins'][task['script']]=digest(task['script'])
            policy=read_json(c['new_plan']['descriptor_path']);c['pins'][c['new_plan']['descriptor_path']]=c['new_plan']['descriptor_sha256']
            c['pins'][policy['base_descriptor']['path']]=policy['base_descriptor']['sha256']
            proof=c['descriptor_canary'](pack,Path(c['new_plan']['descriptor_path']))
            exclusive(c['area']/'emitted-descriptor-canary.json',proof);c['verify']()
            held=hold_all_old(c,plan['tasks'])
            exclusive(c['area']/'all-old-hold-outcome.json',held)
            if held['status']!='all_four_original_controllers_positively_held':return held
            exclusive(c['area']/'new-chain-held-submission-intent.json',{'plan':pack['plan'],'ledger':pack['ledger'],'afterok_handoff':c['job'],'automatic_replay_permitted':False})
            def held_submit(command,**kwargs):
                index=len(c.setdefault('submission_attempts',[]));c['submission_attempts'].append(command)
                require(command[0]=='sbatch' and '--hold' not in command and not any(arg.startswith('--chdir') for arg in command),
                    'One exact source-bound held submission required')
                actual=['sbatch','--hold','--chdir='+str(FRAMEWORK.resolve()),*command[1:]]
                return mutation(c,'submit-new-'+str(index),actual,runner=runner)
            ledger=slurm.submit(pack['plan'],pack['ledger'],runner=held_submit)
            jobs=[ledger['tasks'][task['name']]['job_id'] for task in c['new_plan']['tasks']];c['new_jobs']=jobs
            c['pins'][pack['ledger']]=digest(pack['ledger'])
            c['admission']('pending-handoff-all-new-registered',cpus=0,gpus=0,hours=1)
            proofs={job:new_held(c,task,job,index,jobs,ledger) for index,(task,job) in enumerate(zip(c['new_plan']['tasks'],jobs))}
            exclusive(c['area']/'all-new-held-native.json',proofs)
            c['verify']();retire_old(c,plan['tasks']);c['verify']()
            c['admission']('pending-handoff-old-terminal-accounting',cpus=0,gpus=0,hours=1)
            final=terminal_old(c);exclusive(c['area']/'release-intent.json',{'jobs':jobs,'release_order':list(reversed(jobs)),
                'old_terminal':final,'automatic_replay_permitted':False})
        except OldStarted as error:
            if c.get('cancellation_started'):raise
            restored=restore_confirmed_old_holds(c,plan['tasks'],c.get('old_held',[]))
            return {'status':'declined_original_job_no_longer_pending','reason':str(error),
                'restored_positive_old_holds':restored,'scheduler_cancellation_performed':False,
                'new_jobs_preserved_held':c.get('new_jobs',[]),'old_native_observations':c.get('old_observations',{}),
                'original_start_protected_observation':c.get('start_race_protected_observation')}
        finally:c['registration_locked']=False
    for index in reversed(range(4)):
        task=c['new_plan']['tasks'][index];job=jobs[index]
        def check(task=task,job=job,index=index):
            terminal_old(c);c['qualification_guard']();return new_held(c,task,job,index,jobs,ledger)
        mutation(c,'release-new-'+job,['scontrol','release',job],runner=runner,precheck=check)
    c['verify']();terminal_old(c)
    result={'status':'four_unstarted_original_controllers_retired_and_four_source_bound_backfill_controllers_released',
        'native_job_id':c['job'],'old_jobs':list(OLD_IDS),'new_jobs':jobs,'plan':locator(pack['plan']),'ledger':locator(pack['ledger']),
        'qualification_report':locator(c['qualification_report']),'native_selftests':locator(c['area']/'native-handoff-qualification.json'),
        'native_handoff_completion_required_before_new_controllers_start':True,'old_plan_ledger_scripts_preserved':True,**FALSE_FLAGS}
    exclusive(c['area']/'receipt.json',result);return result


def selftests(c):
    contract=c['manifest']['tests'];temp=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temp.is_absolute() and temp==temp.resolve() and temp.is_dir() and not temp.is_symlink()
        and temp.stat().st_uid==3164768 and not temp.is_relative_to(ROOT),'Owned allocation-local contract fixture namespace required')
    workspace=temp/('hhw-f2-pending-handoff-'+c['job']);require(not workspace.exists(),'Fresh selftest workspace only')
    xml=c['area']/'handoff-contracts.xml';env={**os.environ,'TZ':'UTC','PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1'}
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):env.pop(key,None)
    command=[sys.executable,'-s','-m','pytest',str(SUPPORT/TEST_NAME),'-q','-p','no:cacheprovider','--basetemp',str(workspace),'--junitxml',str(xml)]
    exclusive(c['area']/'native-selftest-intent.json',{'command':command,'source':locator(Path(__file__)),'test_source':locator(SUPPORT/TEST_NAME),**FALSE_FLAGS})
    with (c['area']/'native-selftest-stdout.txt').open('xb') as out,(c['area']/'native-selftest-stderr.txt').open('xb') as err:
        response=subprocess.run(command,cwd=FRAMEWORK,env=env,stdout=out,stderr=err,timeout=min(900,c['end']-time.time()-900),check=False)
    require(response.returncode==0,'Actual native pending-only handoff contract tests failed before any scheduler mutation')
    raw=packet(xml)[0];require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Unsafe contract JUnit refused')
    tree=ET.fromstring(raw);cases=list(tree.iter('testcase'));names=sorted(node.get('classname','').split('.')[-1]+'::'+node.get('name','') for node in cases)
    leaves=[node for node in tree.iter('testsuite') if not node.findall('testsuite')]
    require(contract['count']==25 and len(cases)==len(set(names))==25 and canonical_hash(names)==contract['roster_sha256']
        and all(node.get('classname') and node.get('name') for node in cases)
        and not any(list(node.iter(key)) for node in cases for key in ('failure','error','skipped'))
        and leaves and sum(int(node.get('tests','-1')) for node in leaves)==25
        and all(all(int(node.get(key,'0'))==0 for key in ('failures','errors','skipped')) for node in tree.iter() if node.tag in {'testsuite','testsuites'}),
        'Exact complete 25-test native contract roster required')
    value={'status':'complete_native_pending_handoff_contracts_completion_pending','native_job_id':c['job'],
        'source':locator(Path(__file__)),'test_source':locator(SUPPORT/TEST_NAME),'junit':locator(xml),
        'test_counts':{'tests':25,'failures':0,'errors':0,'skipped':0},'test_roster_sha256':contract['roster_sha256'],
        'native_completion_required_before_controllers_start':True,**FALSE_FLAGS}
    exclusive(c['area']/'native-handoff-qualification.json',value);c['selftest_proof']=value


def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv);parser=argparse.ArgumentParser(description=__doc__)
    for name in ('native-plan','native-ledger','native-script','source-manifest','qualification-report'):parser.add_argument('--'+name,type=Path,required=True)
    for name in ('native-task','source-manifest-sha256','qualification-report-sha256'):parser.add_argument('--'+name,required=True)
    args=parser.parse_args(argv);os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    value=json.loads(packet(args.source_manifest,args.source_manifest_sha256)[0]);job=str(native_job_id() or '')
    require(set(value)=={'kind','files','tests','backfill_package'} and value['kind']=='f2-pending-handoff-source-manifest-v1'
        and set(value['files'])=={SELF_NAME,TEST_NAME,CANARY_NAME,'build-pending-f2-handoff-plan-v1-20261024.py'},'Exact handoff source manifest required')
    pins={str(SUPPORT/name):sha for name,sha in value['files'].items()}
    pins[str(args.source_manifest)]=args.source_manifest_sha256
    for path,sha in pins.items():packet(path,sha,300_000)
    require(Path(__file__)==SUPPORT/SELF_NAME and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and socket.getfqdn().endswith('.narval.calcul.quebec') and re.fullmatch('[1-9][0-9]*',job)
        and os.environ.get('SLURM_CPUS_PER_TASK')=='1' and os.environ.get('HHW_GPU_COUNT')=='0' and os.environ.get('HHW_MAX_HOURS')=='1',
        'Actual owned native one-CPU16G one-hour handoff allocation required')
    import horsehair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py','Actual isolated qualified administration source required')
    area=SUPPORT/('native-'+job);require(not area.exists(),'Existing native handoff evidence cannot replay');area.mkdir(mode=0o700)
    package=value['backfill_package'];require(package.get('kind')=='f2-backfill-controller-transport-source-manifest-v1'
        and package.get('native_support')==str(BACKFILL_SUPPORT) and len(package.get('files',{}))==16,'Exact frozen backfill package manifest required')
    for name,sha in package['files'].items():packet(BACKFILL_SUPPORT/name,sha,300_000);pins[str(BACKFILL_SUPPORT/name)]=sha
    outer=loaded(BACKFILL_SUPPORT/'run-af-agent-controller-backfill-policy-v1-20261024.py',package['files']['run-af-agent-controller-backfill-policy-v1-20261024.py'],'pending_handoff_frozen_outer')
    builder=loaded(BACKFILL_SUPPORT/'build-backfill-native-plans-v1-20261024.py',package['files']['build-backfill-native-plans-v1-20261024.py'],'pending_handoff_frozen_builder')
    descriptor_canary=loaded(SUPPORT/CANARY_NAME,value['files'][CANARY_NAME],'pending_handoff_emitted_descriptor_canary')
    packet(args.qualification_report,args.qualification_report_sha256);q=read_json(args.qualification_report);qid=q['slurm_job_id']
    own_plan=read_json(args.native_plan);own_ledger=read_json(args.native_ledger)
    task,tail=outer.registered(locator(args.native_plan),locator(args.native_ledger),args.native_task,locator(args.native_script),job,
        helper=Path(__file__),hours=1,cpus=1,memory='16G',argv=argv)
    require(len(own_plan.get('tasks',[]))==1 and set(own_ledger.get('tasks',{}))=={args.native_task}
        and own_plan.get('root')==str(ROOT) and own_plan.get('phase')=='study-af-f2-pending-handoff-v1-20261024'
        and args.native_task=='af-f2-pending-handoff-v1'
        and task.get('external_dependencies')==[qid] and task.get('depends')==[] and task.get('dependency_kind')=='afterok'
        and task.get('pending_f2_handoff')=={'helper':locator(Path(__file__)),'source_manifest':locator(args.source_manifest),
            'qualification_report':locator(args.qualification_report)},'Exact own confirmed afterok-qualification source-bound handoff task required')
    own_row=own_ledger['tasks'][args.native_task];issued=own_row.get('issued_native_dependencies');completed=own_row.get('completed_native_dependencies')
    require(issued in ([],[qid]) and own_row.get('dependency_kind')=='afterok'
        and own_row.get('command')==['sbatch','--parsable',*(['--dependency=afterok:'+qid] if issued else []),str(args.native_script)]
        and (completed==[] if issued else type(completed) is list and len(completed)==1 and completed[0].get('job_id')==qid and completed[0].get('dependency_kind')=='afterok')
        and own_row.get('dependency_resolution_sha256')==canonical_hash({'kind':'afterok','issued':issued,'completed':completed}),
        'Own immutable submission ledger must retain the exact qualification dependency resolution')
    for path in (args.native_plan,args.native_ledger,args.native_script,args.qualification_report):pins[str(path)]=digest(path)
    for path,sha in ((GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA)):packet(path,sha,300_000);pins[str(path)]=sha
    scheduler=runpy.run_path(str(SCHEDULER))['run_scontrol_job'];allocation=fields(scheduler(job,area/'own-native-read'))
    require(allocation.get('JobId')==job and allocation.get('UserId')=='pwa209(3164768)' and allocation.get('Account')=='def-ptewarie_cpu'
        and allocation.get('JobState')=='RUNNING' and allocation.get('JobName')=='hhw-'+args.native_task and allocation.get('Command')==str(args.native_script)
        and allocation.get('WorkDir')==str(FRAMEWORK.resolve()) and allocation.get('NumCPUs')=='1' and allocation.get('NumNodes') in {'1','1-1'}
        and allocation.get('MinMemoryNode') in {'16G','16384M'} and seconds(allocation.get('TimeLimit',''))==3600
        and 'ArrayJobId' not in allocation and 'ArrayTaskId' not in allocation
        and all('gpu' not in allocation.get(key,'').lower() for key in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Exact actual owned scalar one-CPU16G one-hour handoff native contract required')
    end=datetime.fromisoformat(allocation.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp();environment_end=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(end) and math.isfinite(environment_end) and abs(end-environment_end)<=2 and end>time.time()+1200,'Real finite native end/environment and margin required')
    def expired(signum,frame):raise TimeoutError('Finite handoff allocation finalization deadline exhausted')
    signal.signal(signal.SIGALRM,expired);signal.alarm(max(1,int(end-time.time()-180)))
    runpy.run_path(str(GUARD))['install'](area/'accounting-reads')
    from horsehair import repair
    repair._repair_native_states=bounded_native_reader(repair._repair_native_states,area/'bounded-native-reads',end)
    descriptor=json.loads(packet(OLD_DESCRIPTOR,OLD_DESCRIPTOR_SHA)[0]);registry_binding=descriptor['registry_overlay']['guard']
    require(registry_binding['sha256']==REGISTRY_SHA,'Exact original permanent AF mutex implementation required')
    packet(registry_binding['path'],REGISTRY_SHA,300_000);pins[registry_binding['path']]=REGISTRY_SHA
    registry=runpy.run_path(registry_binding['path']);registry['install'](area/'registered-reads',registration_mode='controller-already-locked',allocation_deadline_unix=end)
    from horsehair import campaign,repair
    from horsehair.budget import resource_lease
    from horsehair.execution_resources import load_resource_policy
    require(campaign._repair_native_states is repair._repair_native_states,'Actual accounting guard must precede campaign import binding')
    config=read_json(FRAMEWORK/'configs/study.json');require(canonical_hash(config)==CONFIG,'Exact original configuration required')
    policy_binding=read_json(OLD_PLAN)['resource_policy'];policy=load_resource_policy(policy_binding['path'],policy_binding['sha256'],root=ROOT,config=config)
    c={'job':job,'area':area,'end':end,'pins':pins,'manifest':value,'outer':outer,'builder':builder,'scheduler':scheduler,
        'native_reader':repair._repair_native_states,'registry':registry,'qualification_report':args.qualification_report,'registration_locked':False}
    c['descriptor_canary']=descriptor_canary.canary
    c['verify']=lambda:verify_sources(c);c['no_old_leases']=lambda **kwargs:no_old_leases(c,**kwargs)
    c['admission']=lambda label,**resources:campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],area,label,resource_policy=policy,**resources)
    def qualification_guard():
        provider_paths=outer.qualification(read_json(OLD_POLICY))
        checked,_=outer.backfill_qualification({'backfill_qualification':{'path':str(args.qualification_report),
            'sha256':args.qualification_report_sha256,'job_id':qid}},provider_paths=provider_paths)
        c['pins'].update(checked);return checked
    c['qualification_guard']=qualification_guard
    try:
        with resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
            with registry['registration_lock'](area/'qualification-admission-lock',end,role='controller-start'):
                c['admission']('pending-handoff-finalized-qualification-accounting',cpus=0,gpus=0,hours=1)
            qualification_guard();c['verify']();selftests(c);c['verify']();result=orchestrate(c)
            if result.get('status')=='declined_original_job_no_longer_pending':exclusive(area/'declined-receipt.json',result)
        signal.alarm(0);print(json.dumps(result,sort_keys=True),flush=True);return result
    except BaseException as error:
        exclusive(area/'handoff-failure.json',{'status':'needs_explicit_reconciliation','exception':type(error).__name__,'message':str(error)[:4000],
            'old_positive_holds':c.get('old_held',[]),'new_jobs':c.get('new_jobs',[]),'cancellation_started':c.get('cancellation_started',False),
            'automatic_replay_release_cancel_or_cleanup_permitted':False,**FALSE_FLAGS});raise


if __name__=='__main__':main()
