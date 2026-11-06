"""Finite operational continuation of the unchanged synthetic agent campaign.

The expensive, read-only condition audit runs outside the global registration
mutex. A separate permanent controller mutex and a state compare-and-swap guard
the single writer. Native deadlines are conservative; neither clock is extended.
"""
from pathlib import Path
from datetime import datetime, timezone
from contextlib import contextmanager
import argparse
import copy
import gzip
import hashlib
import importlib.util
import json
import math
import os
import re
import shlex
import subprocess
import sys
import time

ROOT=Path('/scratch/pwa209/horsehair-worm')
SUPPORT=ROOT/'maintenance/storage-reserve-200k-v3-20261106/coordination'
PRIOR_SUPPORT=ROOT/'maintenance/agent-coordination-repair-v1-20261025'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
CAMPAIGN=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
CONSUMER=ROOT/'maintenance/storage-reserve-200k-v3-20261106/consumers/run-census-resilience-consumer-v2-20261024.py'
CONSUMER_SHA='2f8af54f0c2b158d45d884281ba9ccd0fb51d3d0cdc74bfd70276747723d5ccf'
CONSUMER_BINDING=CONSUMER.parent/'af-consumer-binding.json'
CONSUMER_BINDING_SHA=None # Explicit source-bound CLI locator, verified at actual registration.
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SOURCE='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
PRIMARY_PHASES=('rna-full','agents-development','agents-final','behaviour-benchmark','behaviour',
    'interventions','screen','molecular-benchmark','molecular-transfer','molecular-corroboration','synthesis')
OLD_CONTROLLERS=('7178192','7178209','7178218','7178228','7187374','7187376',
                 '7187384','7187393','7187402','7187410','7187424','7187433')
FAILED='7110041'
TARGET='training-0080'
PEERS={'training-0081':'7110400','training-0082':'7110771','training-0083':'7111066'}
ORIGINAL_STATE='68935200ddcb81342cffa0f40072d6adc3146a88ae142ddf04018890937bf115'
ORIGINAL_FILE='ac52c45fdc2da3302525532988e7cc3b864013539e09688594854bfbaf95a40e'
FLAGS={'scientific_result':False,'admission_authority':False,'scientific_gates_changed':False,
       'freshness_timestamp_rewritten':False,'numeric_functions_replaced':False}


STORAGE_POLICY_MODULE=ROOT/'maintenance/storage-reserve-200k-v2-20261106/storage_policy.py'
STORAGE_POLICY_MODULE_SHA='0e8691671a32e2b55cfcd626d461c2708d11d05b00c0491721c6c3be13107f7d'
STORAGE_POLICY=ROOT/'maintenance/storage-reserve-200k-v2-20261106/policy.json'
STORAGE_POLICY_SHA='2e8e2bd2c1f70aa988a9414d2b44d9ffd0f768f8bc2e329747b7839b2855f2d9'

def checked_storage_policy():
    helper=module(STORAGE_POLICY_MODULE,STORAGE_POLICY_MODULE_SHA,'verified_200k_storage_policy')
    return helper,helper.verify_policy(STORAGE_POLICY,STORAGE_POLICY_SHA,root=ROOT)


class PendingAcknowledgement(TimeoutError):
    """No resubmission is permitted while the original attempt is unresolved."""


class SubmissionWindowExhausted(PendingAcknowledgement):
    """A normal finite cutoff; the existing successor resumes saved work."""


class PendingCapacity(PendingAcknowledgement):
    """Prospective commitments defer new work without replacing accepted jobs."""


COORDINATED_LIMITS={'scientific_gpus':4,'hhw_cpus':53,
    'provider_cpu_reserve':8,'whole_user_narval_cpus':120}


def capacity_totals(stdout,resources,*,serial_chains=(),missing_commitments=()):
    """Conservative committed resources, with proven serial chains counted once.

    Every unknown HHW commitment is scientific until an exact source-bound
    controller/provider chain proves its smaller concurrency. Other projects'
    allocated work is observed; their parent-coordinated caps are separate.
    """
    require(type(resources) is dict and set(resources)=={'cpus','gpus','hours'}
        and all(type(v) is int for v in resources.values()) and resources['cpus']>=0
        and resources['gpus']>=0 and resources['hours']>0,'Finite prospective resources required')
    roles={};groups=[]
    for chain in serial_chains:
        require(set(chain)=={'role','jobs'} and chain['role'] in {'controller','provider'}
            and isinstance(chain['jobs'],dict) and chain['jobs'],'Explicit proven serial chain required')
        group={'role':chain['role'],'commitments':[],'allocated':0};groups.append(group)
        for job,contract in chain['jobs'].items():
            require(re.fullmatch('[1-9][0-9]*',job) and job not in roles and set(contract)=={'name','cpus','gpus'}
                and contract['name'].startswith('hhw-') and contract['cpus']==(1 if chain['role']=='controller' else 8)
                and contract['gpus']==0,'Distinct exact serial CPU contracts required')
            group['commitments'].append(contract['cpus'])
            roles[job]=(group,contract)
    science_cpus=resources['cpus'];science_gpus=resources['gpus'];other=0;seen=set();selected=[]
    def add(job,name,status,cpus,gpus):
        nonlocal science_cpus,science_gpus,other
        allocated=status!='PENDING'
        if job in roles:
            group,contract=roles[job]
            require(name==contract['name'] and cpus==contract['cpus'] and gpus==contract['gpus'],
                'Observed serial allocation differs from its registered contract')
            group['commitments'].append(cpus);group['allocated']+=int(allocated)
        elif name.startswith('hhw-'):
            science_cpus+=cpus;science_gpus+=gpus
            selected.append({'job_id':job,'name':name,'state':status,'cpus':cpus,'gpus':gpus})
        elif allocated:other+=cpus
    for line in stdout.splitlines():
        if not line.strip():continue
        cells=[v.strip() for v in line.split('|')]
        require(len(cells)==7,'Complete native capacity fields required')
        job,user,name,status,cpus,gres,workdir=cells
        require(re.fullmatch(r'[1-9][0-9]*(?:_[0-9]+)?',job) and job not in seen and user=='pwa209'
            and cpus.isdigit() and int(cpus)>0 and status,'Unique owned native commitments required')
        seen.add(job);gpu=0
        if gres not in {'N/A','(null)','None',''}:
            match=re.fullmatch(r'gres/gpu(?::[A-Za-z0-9_.-]+)?:([0-9]+)(?:\([^)]*\))?',gres)
            require(match is not None,'Exact requested GPU count required');gpu=int(match[1])
        add(job,name,status,int(cpus),gpu)
    for item in missing_commitments:
        require(set(item)=={'job_id','name','cpus','gpus'} and item['job_id'] not in seen
            and re.fullmatch(r'[1-9][0-9]*(?:_[0-9]+)?',item['job_id']) and item['name'].startswith('hhw-')
            and type(item['cpus']) is int and item['cpus']>0 and type(item['gpus']) is int and item['gpus']>=0,
            'Exact unresolved registered commitment required')
        seen.add(item['job_id']);add(item['job_id'],item['name'],'PENDING',item['cpus'],item['gpus'])
    controllers=providers=0
    for group in groups:
        require(group['allocated']<=1,'Proven serial chain has concurrent native allocations')
        value=max(group['commitments'],default=0)
        if group['role']=='controller':controllers+=value
        else:providers+=value
    hhw=science_cpus+controllers+max(providers,8);whole=other+hhw
    return {'allowed':science_gpus<=4 and hhw<=53 and whole<=120,
        'limits':COORDINATED_LIMITS.copy(),'prospective_scientific_cpus':science_cpus,
        'prospective_scientific_gpus':science_gpus,'prospective_hhw_cpus':hhw,
        'prospective_whole_user_narval_cpus':whole,'other_allocated_cpus':other,
        'controller_cpu_commitments':controllers,'provider_cpu_commitments':providers,
        'registered_hhw_workers':selected,'native_job_count':len(seen),
        'pending_held_hhw_work_reserved':True,'other_pending_work_is_not_a_reservation':True,
        'atomic_cross_project_reservation_claimed':False}


def serial_capacity_chain(plan,ledger,role):
    """Read only the exact ordered controller/provider registration contract."""
    expected=1 if role=='controller' else 8
    require(role in {'controller','provider'} and plan['root']==str(ROOT) and plan['config_sha256']==CONFIG
        and ledger['config_sha256']==CONFIG and ledger['source_sha256']==plan['release_identity']['sha256']
        and ledger['plan_sha256']==canonical(plan),'Source-bound serial capacity plan/ledger required')
    phase=plan['phase'];source=plan['release_identity']['sha256']
    provider=role=='provider' and phase in {'study-census-resilience-v1-20261024','storage-reserve-200k-v2-census-20261106'} and source==SOURCE
    primary=role=='controller' and phase in {'study-primary-census-resilience-consumers-v6-20261103','storage-reserve-200k-v3-primary-20261106'} and source=='d89b31180d4c4ffe9c4023dc5534b96f2d157a3a8e3aecaff30849283d00a705'
    agent=role=='controller' and phase.startswith('agent-coordination-') and source==SOURCE
    require(provider or primary or agent,'Only the exact existing workflow chain roles may reserve concurrency')
    tasks=plan['tasks'];require(len(tasks)==(4 if role=='controller' else 64),'Exact finite serial chain length required')
    jobs={}
    for index,task in enumerate(tasks):
        prefix='rolling-storage-census-' if provider else 'primary-resilient-census-controller-' if primary else 'agent-coordination-'
        require(task['name'].startswith(prefix) and task['depends']==([] if index==0 else [tasks[index-1]['name']])
            and task['dependency_kind'] in {'afterok','afterany'} and task['resources']['cpus']==expected
            and task['resources']['gpu'] is False and task['resources'].get('array') is None,
            'Exact serial CPU-only capacity edges required')
        row=ledger['tasks'][task['name']];job=row['job_id']
        require(row['state']=='submitted' and re.fullmatch('[1-9][0-9]*',job) and job not in jobs
            and row['script_sha256']==task['script_sha256'],'Exact unique registered serial jobs required')
        jobs[job]={'name':'hhw-'+task['name'],'cpus':expected,'gpus':0}
    return {'role':role,'jobs':jobs}


def capacity_chains(c):
    bindings=[(c['capacity_registration'],'controller'),
        ({'plan':str(ROOT/'plans/study-primary-census-resilience-consumers-v6-20261103/plan.json'),
          'ledger':str(ROOT/'state/study-primary-census-resilience-consumers-v6-20261103-submission.json')},'controller'),
        ({'plan':str(ROOT/'plans/study-census-resilience-v1-20261024/plan.json'),
          'ledger':str(ROOT/'state/study-census-resilience-v1-20261024-submission.json')},'provider'),
        ({'plan':str(ROOT/'plans/storage-reserve-200k-v3-primary-20261106/plan.json'),
          'ledger':str(ROOT/'state/storage-reserve-200k-v3-primary-20261106-submission.json')},'controller'),
        ({'plan':str(ROOT/'plans/storage-reserve-200k-v2-census-20261106/plan.json'),
          'ledger':str(ROOT/'state/storage-reserve-200k-v2-census-20261106-submission.json')},'provider')]
    result=[];seen=set()
    bp=ROOT/'state/resource-budget.json';costs=json.loads(read(bp)).get('native_jobs',{}) if bp.exists() else {}
    terminal={'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}
    for binding,role in bindings:
        pp,lp=Path(binding['plan']),Path(binding['ledger'])
        if (str(pp),str(lp)) in seen:continue
        seen.add((str(pp),str(lp)))
        if not pp.exists() and not lp.exists():continue
        require(pp.exists() and lp.exists(),'Partial serial registration cannot admit capacity')
        chain=serial_capacity_chain(json.loads(read(pp)),json.loads(read(lp)),role)
        remaining={}
        for job,contract in chain['jobs'].items():
            cost=costs.get(job,{})
            final=cost.get('basis')=='sacct final allocated wall time' and cost.get('state') in terminal
            final=final and re.fullmatch('[0-9]+:[0-9]+',str(cost.get('exit_code',''))) is not None
            final=final and type(cost.get('elapsed_seconds')) is int and cost['elapsed_seconds']>=0
            final=final and type(cost.get('cpus')) is int and cost['cpus']>=0
            if not final:remaining[job]=contract
        if remaining:result.append({'role':role,'jobs':remaining})
    return result


def primary_visibility_guard(seen,native_reader,*,root=None,primary=None):
    """An invisible or ambiguous primary registration cannot free capacity.

    The original phase plans and journals are the authority. Unconfirmed
    attempts pause new admission without inventing a job ID or replaying it.
    Acknowledged scalar/array allocations absent from squeue need genuine final
    accounting before this guard lets either existing workflow admit more work.
    """
    root=ROOT if root is None else Path(root);primary=PRIMARY if primary is None else Path(primary)
    raw=(primary/'release.json').read_bytes();require(len(raw)<=1_000_000,'Finite primary identity required')
    identity=json.loads(raw);release=identity['release_id']
    require(identity['source_identity']['sha256']==PRIMARY_SOURCE and identity['config_sha256']==CONFIG
        and re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,127}',release),'Original primary release/source/config required')
    initial=next((phase for phase in ('managed-initial','initial') if (root/'state'/(release+'-'+phase+'-submission.json')).exists()),None)
    require(initial is not None,'Original primary initial registration is unavailable')
    budget_path=root/'state/resource-budget.json'
    costs=json.loads(read(budget_path)).get('native_jobs',{}) if budget_path.exists() else {}
    absent=set();rosters={};pins={};terminal={'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}
    for phase in (initial,*PRIMARY_PHASES):
        pp=root/'plans'/(release+'-'+phase)/'plan.json';lp=root/'state'/(release+'-'+phase+'-submission.json')
        jp=root/'state/pipeline'/release/(phase+'-plan-journal.json')
        journal=json.loads(read(jp)) if jp.exists() else None
        if not lp.exists():
            require(journal is None or journal.get('state') in {'planning','plan_frozen'},
                'Primary phase has an unconfirmed submission; capacity remains pending')
            continue
        require(pp.exists(),'Primary registered ledger has no original phase plan')
        plan=json.loads(read(pp));ledger=json.loads(read(lp));pins[str(pp)]=digest(pp);pins[str(lp)]=digest(lp)
        require(plan['root']==str(root) and plan['phase']==phase and plan['config_sha256']==CONFIG
            and plan['release_identity']['sha256']==PRIMARY_SOURCE and ledger['plan_sha256']==canonical(plan)
            and ledger.get('source_sha256',PRIMARY_SOURCE)==PRIMARY_SOURCE and ledger.get('config_sha256',CONFIG)==CONFIG,
            'Original primary phase source/config/plan/ledger binding differs')
        if journal is not None:
            pins[str(jp)]=digest(jp)
            require(journal.get('source_sha256')==PRIMARY_SOURCE and journal.get('phase')==phase
                and journal.get('plan_sha256')==canonical(plan),'Primary phase mutation journal differs from its bound plan')
        tasks=plan['tasks'];require(tasks and len({t['name'] for t in tasks})==len(tasks),'Distinct original primary tasks required')
        require(set(ledger['tasks'])=={t['name'] for t in tasks},'Partial primary phase ledger remains unresolved')
        for task in tasks:
            row=ledger['tasks'][task['name']];job=row.get('job_id')
            require(row.get('state')=='submitted' and isinstance(job,str) and re.fullmatch('[1-9][0-9]*',job)
                and row.get('script_sha256')==task['script_sha256'],'Unconfirmed primary task preserves its capacity')
            array=task['resources'].get('array');ids=[job]
            if array:
                match=re.fullmatch(r'([0-9]+)(?:-([0-9]+))?(?:%[1-9][0-9]*)?',str(array))
                require(match is not None,'Exact finite original primary array roster required')
                first,last=int(match[1]),int(match[2] or match[1]);require(first<=last and last-first<=10000,'Bounded original primary array required')
                rosters[job]=list(range(first,last+1));ids=[job+'_'+str(index) for index in rosters[job]]
            for ident in ids:
                cost=costs.get(ident,{})
                final=cost.get('basis')=='sacct final allocated wall time' and cost.get('state') in terminal
                final=final and re.fullmatch('[0-9]+:[0-9]+',str(cost.get('exit_code',''))) is not None
                final=final and type(cost.get('elapsed_seconds')) is int and cost['elapsed_seconds']>=0
                final=final and type(cost.get('cpus')) is int and cost['cpus']>=0
                if ident not in seen and not final:absent.add(ident)
    records=native_reader(sorted(absent),rosters,owner='pwa209',account=None) if absent else {}
    for ident in absent:
        record=records.get(ident,{})
        require(record.get('state') in terminal and record.get('user')=='pwa209'
            and record.get('account') in {'def-ptewarie_cpu','def-ptewarie_gpu'}
            and re.fullmatch('[1-9][0-9]*',str(record.get('job_id_raw',''))),
            'Registered primary allocation is invisible without positive final accounting')
    for path,pin in pins.items():require(digest(path)==pin,'Primary registration changed during capacity observation')
    return {'status':'all_primary_registrations_visible_or_final','phase_source_pins':pins,
        'fresh_final_records':records,'absent_final_job_ids':sorted(absent)}


def coordinated_snapshot(resources,*,state,serial_chains,native_reader,evidence_dir,intent_id,
                         runner=None,clock=time.time):
    from horsehair.core import atomic_json
    command=['squeue','--local','-r','--user=pwa209','--noheader','--format=%i|%u|%.200j|%T|%C|%b|%Z']
    before=clock();runner=subprocess.run if runner is None else runner
    try:
        response=runner(command,text=True,capture_output=True,timeout=30)
        require(response.returncode==0 and not response.stderr and len(response.stdout.encode())<=2_000_000,
            'Fresh complete actual native capacity query required')
        seen={line.split('|',1)[0].strip() for line in response.stdout.splitlines() if line.strip()}
        primary_proof=primary_visibility_guard(seen,native_reader)
        absent={j['job_id']:j for j in state['jobs'].values() if j['job_id'] is not None and j['status']!='complete' and j['job_id'] not in seen}
        observed=native_reader(sorted(absent),{},owner='pwa209',account=None) if absent else {}
        missing=[];terminal={'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}
        for job,item in absent.items():
            native=observed.get(job)
            if native and native.get('job_id_raw')==job and native.get('state') in terminal:continue
            resource=item['intent']['resources'];missing.append({'job_id':job,'name':item.get('native_name','hhw-af-f2-'+item['intent']['intent_id']),
                'cpus':resource['cpus'],'gpus':resource['gpus']})
        record=capacity_totals(response.stdout,resources,serial_chains=serial_chains,missing_commitments=missing)
        now=clock();require(0<=now-before<=35,'Native capacity observation expired')
        record.update({'observed_unix':now,'elapsed_seconds':now-before,'command':command,
            'native_stdout_sha256':hashlib.sha256(response.stdout.encode()).hexdigest(),'intent_id':intent_id,
            'absent_registered_native_records':observed,**FLAGS})
        record['primary_registration_visibility']=primary_proof
        atomic_json(Path(evidence_dir)/'coordinated-capacity-last.json',record)
        if not record['allowed']:raise PendingCapacity('Coordinated HHW capacity pending: '+json.dumps(record['limits'],sort_keys=True))
        return record
    except PendingCapacity:raise
    except (subprocess.SubprocessError,OSError,ValueError,KeyError,TypeError) as error:
        raise PendingCapacity('Actual prospective capacity observation pending: '+str(error)) from error


def coordinated_capacity(c,intent,state,*,runner=None,clock=time.time):
    try:chains=capacity_chains(c)
    except (OSError,ValueError,KeyError,TypeError) as error:
        raise PendingCapacity('Source-bound capacity registration is pending: '+str(error)) from error
    return coordinated_snapshot(intent['resources'],state=state,serial_chains=chains,
        native_reader=c['native_reader'],evidence_dir=c['area'],intent_id=intent['intent_id'],runner=runner,clock=clock)


def before_new_attempt(c,intent,state):
    submission_window(c);coordinated_capacity(c,intent,state);submission_window(c)


def submission_window(c,*,now=None):
    now=time.time() if now is None else now
    if now+300>=c['deadline']:
        raise SubmissionWindowExhausted('Submission finalization margin exhausted; defer to next finite segment')


def held_identity(fields,task,intent,job):
    """A job name alone never authorizes adoption of an accepted submission."""
    resources=intent['resources']
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
        and fields.get('JobName')=='hhw-'+task['name'] and fields.get('Command')==task['script']
        and fields.get('Account')==('def-ptewarie_gpu' if resources['gpus'] else 'def-ptewarie_cpu')
        and fields.get('JobState')=='PENDING' and fields.get('Reason')=='JobHeldUser'
        and fields.get('RunTime')=='00:00:00' and fields.get('NumCPUs')==str(resources['cpus'])
        and fields.get('MinMemoryNode') in {task['resources']['memory'],str(int(task['resources']['memory'][:-1])*1024)+'M'}
        and duration(fields.get('TimeLimit',''))==resources['hours']*3600
        and 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields,
        'Exact owned scalar held job/script/account/resources required')
    tres={}
    for pair in fields.get('ReqTRES','').split(','):
        if '=' in pair:
            k,v=pair.split('=',1);tres[k]=v
    gpu=int(tres.get('gres/gpu',sum(int(v) for k,v in tres.items() if k.startswith('gres/gpu:'))))
    require(gpu==resources['gpus'],'Held job GPU request differs')
    return fields


def submit_once(row,command,invoke,discover,persist,*,before_attempt=lambda:None):
    """Durable before-call intent plus real native adoption; never a fake reply."""
    if row is None:
        # A deadline cutoff must not leave a durable record claiming that a
        # client call might have happened. Existing rows still only reconcile.
        before_attempt()
        row={'state':'submitting','command':command,'attempt_started_unix':time.time()}
        persist(row)
        try:
            result=invoke(command)
            row['client_response']={'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr}
            candidate=result.stdout.strip().split(';')[0]
            if result.returncode==0 and re.fullmatch('[1-9][0-9]*',candidate):
                row.update(state='submitted',job_id=candidate,acknowledgement_basis='actual_sbatch_stdout')
                persist(row)
        except subprocess.TimeoutExpired as error:
            def string(value):return value.decode(errors='replace') if isinstance(value,bytes) else value
            row['client_timeout']={'seconds':error.timeout,'stdout':string(error.stdout),'stderr':string(error.stderr)}
        persist(row)
    else:
        require(row.get('command')==command and row.get('state') in {'submitting','submitted'},
                'Existing submission intent changed; no automatic replay')
    # Even a normal acknowledgement must be joined to the actual held job.
    # Discovery returns observed metadata, not fabricated process stdout.
    job,fields=discover(row.get('job_id'))
    require(row.get('job_id') in {None,job},'Native reconciliation cannot replace an acknowledged job ID')
    row.update(state='submitted',job_id=job)
    if 'acknowledgement_basis' not in row:row['acknowledgement_basis']='exact_owned_held_native_reconciliation'
    row['native_held_observation']=fields;persist(row)
    return row


def observe_job(base,c,job):
    try:return base.fields(c['scheduler'](job,base.read_evidence(c,'submission-native')))
    except (subprocess.SubprocessError,OSError) as error:
        raise PendingAcknowledgement('Native job observation pending: '+str(error)) from error


def discover_held(base,c,task,intent,hint=None):
    if hint is None and intent['intent_id']=='assembly-0081':hint='7191569'
    if hint is not None:
        return hint,held_identity(observe_job(base,c,hint),task,intent,hint)
    command=['squeue','--local','--user=pwa209','--name=hhw-'+task['name'],'--noheader','--format=%i']
    try:
        result=subprocess.run(command,text=True,capture_output=True,timeout=60)
        result.check_returncode()
    except (subprocess.SubprocessError,OSError) as error:
        raise PendingAcknowledgement('Accepted submission lookup pending: '+str(error)) from error
    if result.stderr:raise PendingAcknowledgement('Submission lookup returned stderr; acceptance remains unresolved')
    ids=result.stdout.split()
    if len(ids)!=1 or not re.fullmatch('[1-9][0-9]*',ids[0]):
        raise PendingAcknowledgement('Expected one uniquely identified held submission; found '+str(len(ids)))
    job=ids[0]
    return job,held_identity(observe_job(base,c,job),task,intent,job)


def worker_registration(base,c,plan,state,intent):
    """Create only absent work; existing artifacts must join their original intent."""
    ident=intent['intent_id'];name=base.native_namespace(c,ident)
    pp=ROOT/'plans'/name/'plan.json';lp=ROOT/'state'/(name+'-submission.json');jp=base.submission_journal(c,ident)
    if not pp.exists() and not lp.exists() and not jp.exists():
        c['state']=state;pp,lp,task=base.native_plan(c,intent)
        record={'intent':intent,'plan_path':str(pp),'plan_sha256':digest(pp),'ledger_path':str(lp),
                'status':'before_sbatch',**FLAGS}
        base.atomic_json(jp,record)
    else:
        require(pp.exists() and jp.exists(),'Partial plan without a durable journal requires explicit reconciliation')
        record=base.read_json(jp);task=base.read_json(pp)['tasks'][0]
    native_plan=base.read_json(pp)
    require(len(native_plan['tasks'])==1 and record['intent']==intent
        and record['plan_path']==str(pp) and record['plan_sha256']==digest(pp) and record['ledger_path']==str(lp)
        and native_plan['root']==str(ROOT) and native_plan['config_sha256']==CONFIG
        and native_plan['release_identity']['sha256']==SOURCE and native_plan['intent_sha256']==intent['intent_sha256']
        and native_plan['campaign_plan_sha256']==plan['plan_sha256'] and task['name']==name
        and task['script_sha256']==canonical(read(task['script']).decode())
        and task['resources']['cpus']==intent['resources']['cpus'] and task['resources']['hours']==intent['resources']['hours']
        and task['resources']['gpu']==bool(intent['resources']['gpus']) and '#SBATCH --hold' in read(task['script']).decode(),
        'Original complete worker plan/script/intent/source binding required')
    return pp,lp,jp,record,task


def submit_intentions(base,c,plan,state,intents,*,runner=subprocess.run):
    """Resume saved batch members and recover accepted jobs without another sbatch."""
    for intent in intents:
        ident=intent['intent_id'];job=state['jobs'][ident]
        if job['job_id'] is None:base.admission(c,'ack-before-'+ident,**intent['resources'])
        pp,lp,jp,record,task=worker_registration(base,c,plan,state,intent)
        if job['job_id'] is not None:
            ledger=base.read_json(lp);row=ledger['tasks'][task['name']]
            require(record.get('job_id') in {None,job['job_id']} and row.get('job_id')==job['job_id']
                    and row.get('state')=='submitted' and ledger['plan_sha256']==canonical(base.read_json(pp)),
                    'Existing acknowledgement changed')
            if record['status']=='released':continue
            pp=Path(record['plan_path']);task=base.read_json(pp)['tasks'][0]
            if reconcile_completed_release(base,c,jp,record,task,intent,job['job_id']):continue
            fields=observe_job(base,c,job['job_id'])
            if record['status']=='releasing' and released_identity(fields,task,job['job_id']):
                record.update(status='released',job_id=job['job_id'],release_basis='actual_unheld_native_state')
                base.atomic_json(jp,record);continue
            held_identity(fields,task,intent,job['job_id'])
            record.update(status='registered_held_verified',job_id=job['job_id'],held_allocation=fields);base.atomic_json(jp,record)
        else:
            with base.exclusive_file_lock(lp.with_suffix('.mutex'),timeout=60):
                ledger=base.read_json(lp) if lp.exists() else {'schema_version':2,'plan_sha256':canonical(base.read_json(pp)),
                    'environment_job':None,'excluded_nodes':[],'source_sha256':SOURCE,'config_sha256':CONFIG,'tasks':{}}
                require(ledger['plan_sha256']==canonical(base.read_json(pp)) and ledger['source_sha256']==SOURCE
                        and ledger['config_sha256']==CONFIG,'Existing worker ledger changed')
                row=ledger['tasks'].get(task['name']);command=['sbatch','--parsable',task['script']]
                def persist(value):
                    value.update(script_sha256=task['script_sha256'],dependency_kind='afterok',issued_native_dependencies=[],
                        completed_native_dependencies=[],dependency_resolution_sha256=canonical({'kind':'afterok','issued':[],'completed':[]}))
                    ledger['tasks'][task['name']]=value
                    base.atomic_json(lp,ledger)
                def invoke(command):
                    # Mutation is invoked once. A timeout is followed only by
                    # discovery of the already accepted, script-bound held job.
                    return runner(command,text=True,capture_output=True,timeout=180)
                row=submit_once(row,command,invoke,lambda hint:discover_held(base,c,task,intent,hint),persist,
                                before_attempt=lambda:before_new_attempt(c,intent,state))
            jid=row['job_id']
            state=base.dispatch.record_submission(plan,state,ident,jid,expected_revision=state['revision']);base.save_state(c,state)
            record.update(status='registered_held_verified',job_id=jid,held_allocation=row['native_held_observation'],
                          acknowledgement_basis=row['acknowledgement_basis'])
            base.atomic_json(jp,record)
            base.admission(c,'ack-registered-'+ident,cpus=0,gpus=0,hours=1)
        c.setdefault('release_queue',[]).append({'job_id':state['jobs'][ident]['job_id'],'task':task,'intent':intent,'journal':str(jp)})
    return state


def released_identity(fields,task,job):
    owned=(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
        and fields.get('Command')==task['script'] and fields.get('JobName')=='hhw-'+task['name'])
    state=fields.get('JobState')
    return owned and (state in {'RUNNING','COMPLETING','COMPLETED'} or
        state=='PENDING' and fields.get('Reason') not in {'JobHeldUser','JobHeldAdmin'}
        and fields.get('Priority','').isdigit() and int(fields['Priority'])>0)


def completed_release_proof(record,task,intent,job,native):
    """Final accounting confirms a release; it never substitutes a result receipt."""
    require(record.get('status')=='releasing' and record.get('job_id')==job,
            'Only the exact previously issued release may reconcile')
    held_identity(record['held_allocation'],task,intent,job)
    resources=intent['resources']
    require(native.get('job_id')==job and native.get('state')=='COMPLETED' and native.get('exit_code')=='0:0'
            and native.get('user')=='pwa209'
            and native.get('account')==('def-ptewarie_gpu' if resources['gpus'] else 'def-ptewarie_cpu')
            and type(native.get('elapsed_seconds')) is int and native['elapsed_seconds']>0
            and native.get('cpus')==resources['cpus'] and native.get('gpus')==resources['gpus']
            and native.get('end') not in {None,'Unknown','None',''},
            'Positive exact owned successful final allocation required for release acknowledgement')
    return copy.deepcopy(native)


def reconcile_completed_release(base,c,jp,record,task,intent,job):
    if record.get('status')!='releasing':return False
    try:rows=c['native_reader']([job],{},owner='pwa209',account=None)
    except (subprocess.SubprocessError,OSError) as error:
        raise PendingAcknowledgement('Final release accounting pending: '+str(error)) from error
    require(set(rows).issubset({job}),'Release accounting returned an unrelated allocation')
    if job not in rows or rows[job].get('state')!='COMPLETED':return False
    raw=rows[job]
    require(str(raw.get('job_id_raw',job))==job,'Scalar native release allocation identity changed')
    proof=completed_release_proof(record,task,intent,job,base.normalized_native(job,raw))
    record.update(status='released',release_basis='actual_final_accounting_completed_0_0',
                  release_final_native=proof,release_acknowledged_unix=time.time())
    base.atomic_json(jp,record)
    return True


def release_ready(base,c,*,runner=subprocess.run):
    base.source_guard(c)
    for item in list(c.get('release_queue',[])):
        jp=Path(item['journal']);record=base.read_json(jp);job=item['job_id']
        require(record.get('job_id')==job,'Release acknowledgement belongs to another job')
        if record['status']=='released':continue
        if reconcile_completed_release(base,c,jp,record,item['task'],item['intent'],job):continue
        fields=observe_job(base,c,job)
        if fields.get('JobState')!='PENDING' or fields.get('Reason')!='JobHeldUser':
            require(record['status']=='releasing' and released_identity(fields,item['task'],job),
                'Lost release acknowledgement requires positive matching native execution')
        else:
            held_identity(fields,item['task'],item['intent'],job)
            record['status']='releasing';base.atomic_json(jp,record)
            try:
                response=runner(['scontrol','release',job],text=True,capture_output=True,timeout=60)
                response.check_returncode();require(not response.stderr,'Native release stderr refused')
            except (subprocess.SubprocessError,OSError) as error:
                raise PendingAcknowledgement('Held-job release acknowledgement pending: '+str(error)) from error
        record.update(status='released',released_unix=time.time());base.atomic_json(jp,record)
    c['release_queue']=[]


def reconcile_only(plan,state,native,packets,dispatch):
    """Use original receipt validators without generating any additional work."""
    result=dispatch._state(plan,state)
    for ident,packet in packets.items():
        job=result['jobs'][ident]
        require(job['status']=='submitted' and native[job['job_id']]['state']=='COMPLETED'
                and native[job['job_id']]['exit_code']=='0:0','Only positively completed existing work may reconcile')
        dispatch._native(job,native[job['job_id']]);cell=result['cells'][job['intent']['cell_index']]
        if job['intent']['kind']=='training':dispatch._training(plan,job,packet)
        elif job['intent']['kind']=='evaluation':dispatch._evaluation(plan,result,cell,job,packet)
        else:dispatch._assembly(plan,cell,job,packet);cell['complete']=True
        job.update(status='complete',receipt=copy.deepcopy(packet))
    result=dispatch._finish(result);dispatch._state(plan,result)
    return result


def require(value,message):
    if not value:raise ValueError(message)


def canonical(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path,sha=None):
    path=Path(path)
    require(path.is_absolute() and path==path.resolve() and path.is_relative_to(ROOT)
            and path.is_file() and not any(p.is_symlink() for p in (path,*path.parents)),
            'Owned ordinary personal-scratch file required')
    raw=path.read_bytes();require(len(raw)<=32_000_000 and (sha is None or hashlib.sha256(raw).hexdigest()==sha),'Frozen bytes changed')
    return raw


def packet(binding):return json.loads(read(binding['path'],binding['sha256']))


def locator(path):return {'path':str(path),'sha256':digest(path)}


def exclusive(path,value):
    with Path(path).open('xb') as f:
        f.write((json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode());f.flush();os.fsync(f.fileno())


def module(path,sha,name):
    read(path,sha);spec=importlib.util.spec_from_file_location(name,path)
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result)
    require(digest(path)==sha,'Imported source changed');return result


def duration(value):
    days,clock=value.split('-',1) if '-' in value else ('0',value)
    parts=[int(v) for v in clock.split(':')];require(len(parts)==3,'Native duration required')
    return int(days)*86400+parts[0]*3600+parts[1]*60+parts[2]


def conservative_window(fields,environment_end,*,now=None):
    """Use the verified current allocation, retaining its shutdown reserve.

    Slurm may extend a backfill allocation after exporting the batch environment.
    That inherited environment is diagnostic; StartTime + actual TimeLimit must
    independently agree with the current native EndTime before it is used.
    The caller has already bound these fields to the owned current allocation.
    """
    now=time.time() if now is None else now
    require(type(environment_end) in (int,float) and math.isfinite(environment_end)
            and type(now) in (int,float) and math.isfinite(now),'Finite native clocks required')
    limit=duration(fields['TimeLimit'])
    start=datetime.fromisoformat(fields['StartTime']).replace(tzinfo=timezone.utc).timestamp()
    end=datetime.fromisoformat(fields['EndTime']).replace(tzinfo=timezone.utc).timestamp()
    require(28800<=limit<=345600 and duration(fields['TimeMin'])==28800
            and abs(end-start-limit)<=2 and start<=now+2 and environment_end>=start,
            'Owned native start/end/effective backfill window must agree')
    effective=end
    require(now+14400+300<effective,'Original four-hour shutdown reserve required')
    return {'allocation_end_unix':effective,'native_end_unix':end,'environment_end_unix':environment_end,
            'native_environment_delta_seconds':end-environment_end,'effective_seconds':limit,
            'work_deadline_unix':effective-14400,'shutdown_reserve_seconds':14400}


def synchronize_native_deadline(fields,environment_end,*,now=None):
    """Propagate the verified current end to existing downstream lease readers."""
    window=conservative_window(fields,environment_end,now=now)
    os.environ['SLURM_JOB_END_TIME']=str(window['allocation_end_unix'])
    return window


def recover_one(plan,state,native,packets,dispatch):
    """Reconcile three real receipts and remove only the proven pre-start attempt."""
    dispatch._state(plan,state)
    require(state['revision']==443 and state['state_sha256']==ORIGINAL_STATE
            and state['status']=='needs_repair' and state['needs_repair']==[TARGET],
            'Exact preserved revision443 required')
    failed=state['jobs'][TARGET]
    require(failed['job_id']==FAILED and failed['status']=='needs_repair' and failed['receipt'] is None
            and failed['intent']['kind']=='training' and failed['intent']['cell_index']==80
            and failed['native_proof']==native[FAILED]
            and native[FAILED]['state']=='FAILED' and native[FAILED]['exit_code']=='1:0',
            'Exact failed pre-start allocation required')
    require(set(packets)==set(PEERS) and all(state['jobs'][k]['job_id']==v for k,v in PEERS.items()),
            'Only three acknowledged completed peers may reconcile')
    result,intents=dispatch.dispatch_tick(plan,state,native,packets)
    require(not intents and result['needs_repair']==[TARGET]
            and all(result['jobs'][k]['status']=='complete' for k in PEERS),
            'Original dispatcher must accept all three receipts without new work')
    require(all(result['jobs'][k]==v for k,v in state['jobs'].items() if k not in PEERS)
            and result['cells']==state['cells'],'Unrelated campaign records must be preserved')
    cell=result['cells'][80]
    require(cell['training_job']==TARGET and cell['evaluation_plan'] is None and not cell['evaluation_jobs']
            and cell['assembly_job'] is None and not cell['conditions'] and cell['complete'] is False,
            'Failed training must have no downstream computation')
    del result['jobs'][TARGET];cell['training_job']=None
    result.update(status='active',needs_repair=[])
    result=dispatch._finish(result);dispatch._state(plan,result)
    return result


def audit_then_commit(snapshot,audit,guard,load_current,commit):
    """No registration lock while auditing; reject a changed state on re-entry."""
    packets=audit()
    with guard():
        current=load_current()
        require(current==snapshot,'Campaign changed during unlocked immutable condition audit')
        return commit(packets)


def pending_intents(base,c,state):
    result=[]
    for ident,job in state['jobs'].items():
        journal=base.submission_journal(c,ident)
        if job['job_id'] is None or not journal.exists() or base.read_json(journal).get('status')!='released':
            result.append(job['intent'])
    return result


def preserve_pending(base,c,state):
    area=SUPPORT/('pending-state-'+str(state['revision']))
    original=CAMPAIGN/'state.json.gz'
    if area.exists():
        require(digest(area/'state.json.gz')==digest(original),'Preserved pending state changed');return area
    area.mkdir();sources={'state.json.gz':original,'state-binding.json':CAMPAIGN/'state-binding.json',
                        'campaign-plan.json':CAMPAIGN/'campaign-plan.json'}
    absence=[]
    for intent in pending_intents(base,c,state):
        ident=intent['intent_id'];phase=base.native_namespace(c,ident)
        for kind,p in [('journal',base.submission_journal(c,ident)),('plan',ROOT/'plans'/phase/'plan.json'),
                       ('ledger',ROOT/'state'/(phase+'-submission.json'))]:
            if p.exists():sources[ident+'-'+kind+'.json']=p
            else:absence.append(str(p))
    evidence={}
    for name,path in sources.items():
        raw=read(path)
        with (area/name).open('xb') as out:out.write(raw);out.flush();os.fsync(out.fileno())
        evidence[name]={'original_path':str(path),**locator(area/name)}
    exclusive(area/'before.json',{'native_actor':c['job_id'],'revision':state['revision'],'state_sha256':state['state_sha256'],
        'preserved':evidence,'absent_before_attempt':absence,**FLAGS})
    return area


def resume_pending_batch(base,c,plan,state):
    """One original batch, including its accepted held member, resumes in place."""
    with base.registration_guard(c,'controller-tick'):
        base.source_guard(c);require(base.initialize(c)[1]==state,'State changed before pending-batch recovery')
        archive=preserve_pending(base,c,state)
        base.admission(c,'resume-snapshot-'+str(state['revision']))
    ids=[j['job_id'] for j in state['jobs'].values() if j['job_id'] is not None and j['status']=='submitted']
    records=c['native_reader'](ids,{},owner='pwa209',account=None) if ids else {}
    native={job:base.normalized_native(job,row) for job,row in records.items()}
    targets={k:j for k,j in state['jobs'].items() if j['status']=='submitted' and base.positive_native(native,j['job_id'])}
    # Keep all indices for the original collector's indexing, but audit only
    # the completed workers being reconciled. No new intentions are generated.
    view=copy.deepcopy(state);view['jobs']=targets
    needed={j['intent']['cell_index'] for j in targets.values() if j['intent']['kind']!='training'}
    for cell in view['cells']:
        if cell['index'] not in needed:cell['evaluation_plan']=None
    packets=base.collect_packets(c,plan,view,native) if targets else {}
    base.prepare_census(c)
    with base.registration_guard(c,'controller-tick'):
        base.source_guard(c);require(base.initialize(c)[1]==state,'Pending state changed during receipt validation')
        submission_window(c)
        base.admission(c,'resume-commit-'+str(state['revision']))
        submission_window(c)
        if packets:
            state=reconcile_only(plan,state,native,packets,base.dispatch);base.save_state(c,state)
        intents=pending_intents(base,c,state);c['release_queue']=[]
        state=base.submit_intentions(c,plan,state,intents)
    base.release_ready(c)
    summary={'status':state['status'],'revision':state['revision'],'native_controller_job':c['job_id'],
        'segment':c['segment'],'training_complete':sum(j['status']=='complete' and j['intent']['kind']=='training' for j in state['jobs'].values()),
        'policies_complete':sum(cell['complete'] for cell in state['cells']),'resumed_intentions':len(intents),
        'reconciled_existing_jobs':list(packets),'jobs':len(state['jobs']),'state_sha256':state['state_sha256'],
        'storage_pending':None,'audit_outside_registration_mutex':True,'coordination_source':locator(SUPPORT/'coordination.py'),
        'H4_inference_status':state['H4_inference_status'],**FLAGS}
    base.atomic_json(c['case']/'status.json',summary)
    if not (SUPPORT/'acknowledgement-recovery.json').exists():
        exclusive(SUPPORT/'acknowledgement-recovery.json',{'status':'original_pending_batch_recovered','actor':c['job_id'],
            'archive':str(archive),'held_job_preserved':'7191569','summary':summary,**FLAGS})
    return summary


def coordinated_tick(base,c):
    """Original validation/dispatch/submission with the slow audit outside the lock."""
    base.prepare_census(c)
    with base.registration_guard(c,'controller-tick'):
        initial_plan,initial_state=base.initialize(c)
        pending=pending_intents(base,c,initial_state)
    if pending:return resume_pending_batch(base,c,initial_plan,initial_state)
    with base.registration_guard(c,'controller-tick'):
        base.source_guard(c);plan,state=base.initialize(c)
        for ident,job in state['jobs'].items():
            journal=base.submission_journal(c,ident)
            require(job['job_id'] is not None and journal.exists() and base.read_json(journal)['status']=='released',
                    'Existing submission acknowledgement must be preserved')
        base.admission(c,'snapshot-'+str(state['revision']))
    ids={j['job_id'] for j in state['jobs'].values() if j['job_id'] is not None}
    ids.update(str(c['descriptor']['precision_gate'][k]) for k in ('diagnostics_job_id','assembly_job_id'))
    records=c['native_reader'](sorted(ids),{},owner='pwa209',account=None)
    native={ident:base.normalized_native(ident,row) for ident,row in records.items()}
    def audit():
        c['audit_started_unix']=time.time()
        base.atomic_json(c['area']/'audit-progress.json',{'status':'validating_condition_bytes_outside_registration_mutex',
             'revision':state['revision'],'started_unix':c['audit_started_unix'],**FLAGS})
        packets=base.collect_packets(c,plan,state,native)
        # A long audit must obtain fresh provider/admission evidence again.
        base.prepare_census(c)
        return packets
    def commit(packets):
        submission_window(c)
        base.source_guard(c);base.admission(c,'commit-'+str(state['revision']))
        submission_window(c)
        candidate,intents=base.dispatch.dispatch_tick(plan,state,native,packets)
        if candidate['status']=='needs_repair':
            base.save_state(c,candidate)
            raise ValueError('Qualified dispatcher reports needs_repair; native evidence retained')
        base.storage_guard(c,candidate);current=candidate;base.save_state(c,current)
        qualified=base.precision_gate(c,native)
        current=base.bind_ready(c,plan,current,qualified)
        candidate,extra=base.dispatch.dispatch_tick(plan,current,native,{})
        try:base.storage_guard(c,candidate)
        except base.PendingStorage as error:c['storage_pending']=str(error);extra=[]
        else:current=candidate;base.save_state(c,current)
        current=base.submit_intentions(c,plan,current,intents+extra)
        summary={'status':current['status'],'revision':current['revision'],'precision_gate':qualified,
            'native_controller_job':c['job_id'],'segment':c['segment'],'jobs':len(current['jobs']),
            'training_complete':sum(j['intent']['kind']=='training' and j['status']=='complete' for j in current['jobs'].values()),
            'policies_complete':sum(v['complete'] for v in current['cells']),'new_intentions':len(intents+extra),
            'state_sha256':current['state_sha256'],'storage_pending':c.get('storage_pending'),
            'coordination_source':locator(SUPPORT/'coordination.py'),'audit_outside_registration_mutex':True,
            'H4_inference_status':current['H4_inference_status'],**FLAGS}
        base.atomic_json(c['case']/'status.json',summary);return summary
    summary=audit_then_commit(state,audit,lambda:base.registration_guard(c,'controller-tick'),
                              lambda:base.initialize(c)[1],commit)
    base.release_ready(c)
    return summary


def verify_sources(manifest_sha):
    manifest=packet({'path':str(SUPPORT/'source-manifest.json'),'sha256':manifest_sha})
    require(manifest['kind']=='agent-coordination-repair-v1'
            and set(manifest['files'])=={'coordination.py','registry-wait.py','test_coordination.py','test_registry.py','test_acknowledgements.py'},'Exact repair closure required')
    for name,sha in manifest['files'].items():read(SUPPORT/name,sha)
    require(Path(__file__)==SUPPORT/'coordination.py','Only the frozen repair entry may execute')
    return manifest


def registration(a,job,mode,argv):
    plan=json.loads(read(a.native_plan));ledger=json.loads(read(a.native_ledger));text=read(a.native_script).decode()
    require(plan['root']==str(ROOT) and plan['config_sha256']==ledger['config_sha256']==CONFIG
            and plan['release_identity']['sha256']==ledger['source_sha256']==SOURCE
            and ledger['plan_sha256']==canonical(plan),'Registered source/config/plan required')
    selected=[t for t in plan['tasks'] if t['name']==a.native_task];require(len(selected)==1,'One registered task required')
    task=selected[0];row=ledger['tasks'][a.native_task]
    command=shlex.split(task['command']);entry=str(SUPPORT/'coordination.py')
    require(command.count(entry)==1 and command[command.index(entry)+1:]==argv and task['command'] in text
            and task['script']==str(a.native_script) and task['script_sha256']==row['script_sha256']==canonical(text)
            and row['state']=='submitted' and row['job_id']==job,'Actual invocation/native ID/script required')
    contract={'cpus':4 if mode=='qualify' else 1,'memory':'16G' if mode=='qualify' else '64G',
              'hours':1 if mode=='qualify' else 96,'gpu':False,'array':None}
    require(task['resources']==contract and task.get('coordination_manifest_sha256')==a.source_manifest_sha256,
            'Exact finite source-bound repair reservation required')
    return plan,ledger,task


def qualification(a,manifest,area,fields):
    import xml.etree.ElementTree as ET
    checked_storage_policy()
    from horsehair.core import read_json
    from horsehair.budget import resource_lease
    from horsehair.execution_resources import load_resource_policy
    policy=load_resource_policy(ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json',
        'f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5',root=ROOT,config=read_json(FRAMEWORK/'configs/study.json'))
    scratch=Path(os.environ['SLURM_TMPDIR'])/('coordination-'+os.environ['SLURM_JOB_ID']);scratch.mkdir()
    junit=area/'junit.xml'
    with resource_lease(ROOT,read_json(FRAMEWORK/'configs/study.json'),cpus=4,gpus=0,max_hours=1,
                        job_id=os.environ['SLURM_JOB_ID'],resource_policy=policy):
        result=subprocess.run([sys.executable,'-m','pytest',str(SUPPORT/'test_coordination.py'),str(SUPPORT/'test_registry.py'),str(SUPPORT/'test_acknowledgements.py'),'-q','-p','no:cacheprovider',
            '--basetemp',str(scratch/'pytest-temp'),'--junitxml',str(junit)],cwd=scratch,timeout=2400)
    require(result.returncode==0,'Native repair tests failed')
    tree=ET.parse(junit);cases=list(tree.getroot().iter('testcase'))
    names=sorted(c.get('classname','').split('.')[-1]+'::'+c.get('name','') for c in cases)
    require(len(names)==manifest['tests'] and len(set(names))==len(names) and canonical(names)==manifest['roster_sha256']
            and not any(list(c.iter(k)) for c in cases for k in ('failure','error','skipped')),'Complete passing native roster required')
    report={'status':'qualified_agent_coordination_repair','native_job_id':os.environ['SLURM_JOB_ID'],
        'source_manifest_sha256':a.source_manifest_sha256,'tests':len(names),'roster_sha256':canonical(names),
        'junit':locator(junit),'allocation':fields,'native_plan':locator(a.native_plan),'native_ledger':locator(a.native_ledger),
        'native_script':locator(a.native_script),'native_task':a.native_task,'returncode':0,**FLAGS}
    exclusive(SUPPORT/'qualification.json',report);print('COORDINATION_QUALIFICATION '+json.dumps(report),flush=True)


def positive_old_controllers(base,c):
    rows=c['native_reader'](list(OLD_CONTROLLERS),{},owner='pwa209',account=None)
    require(set(rows)==set(OLD_CONTROLLERS),'All four old controllers require actual final records')
    for job,row in rows.items():
        n=base.normalized_native(job,row)
        require(n['state'] in base.dispatch.TERMINAL_FAILURES|{'COMPLETED'} and n['cpus']==1 and n['gpus']==0
                and n['elapsed_seconds']>0 and n['end']!='Unknown','Old AF lineage must be positively terminal')


def recover_native(base,c,manifest_sha):
    receipt=SUPPORT/'recovery.json'
    if receipt.exists():
        value=json.loads(read(receipt));require(value['status']=='reconciled_three_and_recovered_one_prestart'
            and value['source_manifest_sha256']==manifest_sha,'Exact prior recovery receipt required')
        for item in value['preserved'].values():read(item['path'],item['sha256'])
        return
    # Preserve the already completed first recovery; this version continues the
    # same campaign and never reruns the old revision443 transition.
    prior={'path':str(PRIOR_SUPPORT/'recovery.json'),
           'sha256':'dda2ecc22fd584d8d910dd0085421f0d84f509274f12bcc80d3ea41b9dcda606'}
    value=packet(prior)
    require(value['status']=='reconciled_three_and_recovered_one_prestart','Original completed recovery required')
    for item in value['preserved'].values():read(item['path'],item['sha256'])
    exclusive(receipt,{**value,'source_manifest_sha256':manifest_sha,'preserved_prior_recovery':prior})
    return


def install_coordination(base,manifest,*,storage_source_sha=None):
    if storage_source_sha is not None:
        helper,policy=checked_storage_policy()
        base.storage_guard,proof=helper.derive_reserved_function(base.storage_guard,policy,
            expected_source_sha256=storage_source_sha,
            expected_reserve_expression='max(0,250000-forecast_files)',
            required_files_expression='forecast_files',expected_count=1)
        require(proof['reverse_transform_ast_equal'] and proof['original_module_unchanged']
            and proof['study_occupancy_and_byte_checks_preserved'],'Only explicit private inode reserve may differ')
    old_namespace=base.native_namespace;old_journal=base.submission_journal;old_plan=base.native_plan
    base.native_backfill_window=conservative_window
    base.native_namespace=lambda c,ident:'af-f2-r3-training-0080' if ident==TARGET else old_namespace(c,ident)
    base.submission_journal=lambda c,ident:CAMPAIGN/'submission-r3-training-0080.json' if ident==TARGET else old_journal(c,ident)
    def native_plan(c,intent):
        alternate={**c,'registry_overlay':{**c['registry_overlay'],'guard':{
            'path':str(SUPPORT/'registry-wait.py'),'sha256':manifest['files']['registry-wait.py']}}}
        return old_plan(alternate,intent)
    base.native_plan=native_plan
    base.submit_intentions=lambda c,p,s,i,**kw:submit_intentions(base,c,p,s,i,**kw)
    base.release_ready=lambda c,**kw:release_ready(base,c,**kw)
    def tick(c,**kw):
        try:return coordinated_tick(base,c)
        except PendingCapacity as error:
            try:release_ready(base,c)
            except PendingAcknowledgement as release_error:
                raise base.PendingRegistration('partial_batch_release_pending: '+str(release_error)) from release_error
            raise base.PendingRegistration('coordinated_capacity_pending: '+str(error)) from error
        except PendingAcknowledgement as error:
            raise base.PendingRegistration('submission_acknowledgement_pending: '+str(error)) from error
    base.tick=tick


def run_controller(a,manifest,area,fields):
    from horsehair import slurm
    from horsehair.core import exclusive_file_lock
    consumer=module(CONSUMER,CONSUMER_SHA,'preserved_census_consumer')
    require(a.consumer_binding==CONSUMER_BINDING and re.fullmatch('[0-9a-f]{64}',str(a.consumer_binding_sha256)),
        'Explicit immutable new consumer binding required')
    binding,old_manifest,pins=consumer.check_binding(a.consumer_binding,a.consumer_binding_sha256)
    end=synchronize_native_deadline(fields,float(os.environ['SLURM_JOB_END_TIME']))['allocation_end_unix']
    native_reader=consumer.bounded_native_reader(end)
    checked=consumer.validate_qualification(binding['qualification'],binding['source_manifest'],native_reader)
    pins.update(checked['checked_paths'])
    q=packet({'path':str(a.qualification),'sha256':a.qualification_sha256})
    require(q['status']=='qualified_agent_coordination_repair' and q['source_manifest_sha256']==a.source_manifest_sha256
            and q['tests']==manifest['tests'] and q['roster_sha256']==manifest['roster_sha256'],'Actual repair qualification required')
    # Final accounting may lag the scheduler's afterok transition. Wait read-only
    # in this allocation, preserving the genuine records and finite deadline.
    stop=min(time.monotonic()+3600,time.monotonic()+end-time.time()-15000)
    sequence=0
    while True:
        sequence+=1
        rows=native_reader([q['native_job_id']],{},owner='pwa209',account=None)
        row=rows.get(q['native_job_id'],{})
        if row.get('state')=='COMPLETED' and row.get('exit_code')=='0:0':
            require(row.get('cpus')==4 and 0<row.get('elapsed_seconds',0)<=3600
                and 'gpu' not in row.get('alloc_tres','').lower(),'Positive native four-CPU qualification required')
            exclusive(area/'qualification-native-final.json',{'job_id':q['native_job_id'],'record':row})
            break
        require(not row or row.get('state') in {'PENDING','RUNNING','COMPLETING'},'Repair qualification failed')
        require(time.monotonic()+30<stop,'Finite wait for actual qualification accounting exhausted')
        if sequence==1:print('COORDINATION_WAIT actual qualification final accounting pending',flush=True)
        time.sleep(30)
    read(q['junit']['path'],q['junit']['sha256'])
    outer=consumer.module(binding['legacy_entry']['path'],binding['legacy_entry']['sha256'],'preserved_backfill_policy')
    policy=packet(binding['legacy_policy']);require(policy['kind']=='af-controller-backfill-external-census-policy-v1'
        and policy['base_controller']=={'path':str(outer.BACKFILL_BASE),'sha256':outer.BACKFILL_BASE_SHA}
        and policy['runtime_contract']==outer.BACKFILL_RUNTIME,'Preserved exact backfill policy required')
    provider_policy=packet(policy['provider_policy'])
    checked_old=outer.qualification(provider_policy)
    checked_backfill,backfill_proof=outer.backfill_qualification(policy,provider_paths=checked_old)
    base=outer.load_base();install_coordination(base,manifest,storage_source_sha=outer.BACKFILL_BASE_SHA)
    # The controller mutex is separate from the short registration barrier and
    # is held for the entire allocation. Never delete or replace either mutex.
    with exclusive_file_lock(CAMPAIGN/'coordination-controller.mutex',timeout=60):
        c=base.load_context(Path(policy['base_descriptor']['path']),policy['base_descriptor']['sha256'],a.segment)
        c['capacity_registration']={'plan':str(a.native_plan),'ledger':str(a.native_ledger)}
        positive_old_controllers(base,c)
        context={'policy_binding':policy['provider_policy'],'policy':provider_policy,'base_context':c,
            'checked_paths':{**checked_old,**checked_backfill},'backfill_policy_binding':binding['legacy_policy'],
            'backfill_policy':policy,'backfill_qualification_completion':backfill_proof}
        context['qualification_completion']=slurm.completed_native_dependency(ROOT,provider_policy['qualification']['job_id'])
        context['checked_paths'].update({binding['legacy_policy']['path']:binding['legacy_policy']['sha256'],
            policy['provider_policy']['path']:policy['provider_policy']['sha256'],
            policy['base_descriptor']['path']:policy['base_descriptor']['sha256']})
        outer.provider_proof=consumer.routed_provider(outer,binding,pins,native_reader)
        outer.install(base,context);outer.policy_guard(context)
        exclusive(area/'startup.json',{'status':'verified_repaired_controller_startup','window':c['backfill_window'],
            'source_manifest_sha256':a.source_manifest_sha256,'old_controllers_final':list(OLD_CONTROLLERS),**FLAGS})
        recover_native(base,c,a.source_manifest_sha256)
        return base.run(c)


def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv)
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--mode',choices=['qualify','run'],required=True)
    p.add_argument('--source-manifest-sha256',required=True)
    for k in ('native-plan','native-ledger','native-script','qualification','consumer-binding'):p.add_argument('--'+k,type=Path)
    p.add_argument('--qualification-sha256');p.add_argument('--consumer-binding-sha256');p.add_argument('--native-task',required=True);p.add_argument('--segment',type=int,default=0)
    a=p.parse_args(argv)
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    require(os.geteuid()==3164768 and os.environ.get('USER')=='pwa209','Owned Narval allocation required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval host required')
    job=os.environ['SLURM_JOB_ID'];require(re.fullmatch('[1-9][0-9]*',job),'Scalar native job required')
    manifest=verify_sources(a.source_manifest_sha256);plan,ledger,task=registration(a,job,a.mode,argv)
    area=SUPPORT/('native-'+a.mode+'-'+job);area.mkdir()
    scheduler=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-scheduler-read.py'
    lib=module(scheduler,'100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3','verified_native_scheduler')
    response=lib.run_scontrol_job(job,area/'scheduler');response.check_returncode();require(not response.stderr,'Scheduler error refused')
    text=response.stdout.decode() if isinstance(response.stdout,bytes) else response.stdout
    fields=dict(x.split('=',1) for x in shlex.split(text) if '=' in x)
    contract=task['resources']
    require(fields['JobId']==job and fields['UserId']=='pwa209(3164768)' and fields['JobState']=='RUNNING'
        and fields['Account']=='def-ptewarie_cpu' and fields['Command']==str(a.native_script)
        and fields['JobName']=='hhw-'+a.native_task and fields['NumCPUs']==str(contract['cpus'])
        and fields['NumNodes'] in {'1','1-1'} and fields['MinMemoryNode'] in {contract['memory'],str(int(contract['memory'][:-1])*1024)+'M'}
        and 'ArrayJobId' not in fields and all('gpu' not in fields.get(k,'').lower() for k in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask')),
        'Actual finite owned scalar CPU allocation required')
    require((a.mode=='qualify' and duration(fields['TimeLimit'])==3600)
        or (a.mode=='run' and 28800<=duration(fields['TimeLimit'])<=contract['hours']*3600
            and duration(fields['TimeMin'])==28800),'Actual scheduler limit must remain within the registered finite window')
    exclusive(area/'allocation.json',{'fields':fields,'environment_end_unix':float(os.environ['SLURM_JOB_END_TIME']),
        'source_manifest_sha256':a.source_manifest_sha256,**FLAGS})
    if a.mode=='qualify':qualification(a,manifest,area,fields)
    else:
        require(0<=a.segment<4 and len(plan['tasks'])==4,'Four finite ordered controller segments required')
        for index,t in enumerate(plan['tasks']):
            require(t['depends']==([] if index==0 else [plan['tasks'][index-1]['name']]) and t['dependency_kind']=='afterok',
                'No parallel controller lineage is authorized')
        return run_controller(a,manifest,area,fields)


if __name__=='__main__':main()
