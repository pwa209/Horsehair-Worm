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
SUPPORT=ROOT/'maintenance/agent-coordination-repair-v3-20261026'
PRIOR_SUPPORT=ROOT/'maintenance/agent-coordination-repair-v1-20261025'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
CAMPAIGN=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
CONSUMER=ROOT/'maintenance/study-census-resilience-consumers-v2-20261024/run-census-resilience-consumer-v2-20261024.py'
CONSUMER_SHA='d809f8b541ec7f764c00dfeda902b961bbdc28efe076ca20c3e6ff817b9c6efa'
CONSUMER_BINDING=CONSUMER.parent/'af-consumer-binding.json'
CONSUMER_BINDING_SHA='7c59a1e4a78ff3cbe5d44666a4830a16027585ea9525bf87c5f1040f83755288'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
OLD_CONTROLLERS=('7178192','7178209','7178218','7178228','7187374','7187376',
                 '7187384','7187393','7187402','7187410','7187424','7187433')
FAILED='7110041'
TARGET='training-0080'
PEERS={'training-0081':'7110400','training-0082':'7110771','training-0083':'7111066'}
ORIGINAL_STATE='68935200ddcb81342cffa0f40072d6adc3146a88ae142ddf04018890937bf115'
ORIGINAL_FILE='ac52c45fdc2da3302525532988e7cc3b864013539e09688594854bfbaf95a40e'
FLAGS={'scientific_result':False,'admission_authority':False,'scientific_gates_changed':False,
       'freshness_timestamp_rewritten':False,'numeric_functions_replaced':False}


class PendingAcknowledgement(TimeoutError):
    """No resubmission is permitted while the original attempt is unresolved."""


class SubmissionWindowExhausted(PendingAcknowledgement):
    """A normal finite cutoff; the existing successor resumes saved work."""


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
                                before_attempt=lambda:submission_window(c))
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


def release_ready(base,c,*,runner=subprocess.run):
    base.source_guard(c)
    for item in list(c.get('release_queue',[])):
        jp=Path(item['journal']);record=base.read_json(jp);job=item['job_id']
        require(record.get('job_id')==job,'Release acknowledgement belongs to another job')
        if record['status']=='released':continue
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
    """Accept genuine backfill shortening without extending either end clock."""
    now=time.time() if now is None else now
    require(type(environment_end) in (int,float) and math.isfinite(environment_end)
            and type(now) in (int,float) and math.isfinite(now),'Finite native clocks required')
    limit=duration(fields['TimeLimit'])
    start=datetime.fromisoformat(fields['StartTime']).replace(tzinfo=timezone.utc).timestamp()
    end=datetime.fromisoformat(fields['EndTime']).replace(tzinfo=timezone.utc).timestamp()
    require(28800<=limit<=345600 and duration(fields['TimeMin'])==28800
            and abs(end-start-limit)<=2 and start<=now+2 and environment_end>=start,
            'Owned native start/end/effective backfill window must agree')
    effective=min(end,environment_end)
    require(now+14400+300<effective,'Original four-hour shutdown reserve required')
    return {'allocation_end_unix':effective,'native_end_unix':end,'environment_end_unix':environment_end,
            'native_environment_delta_seconds':end-environment_end,'effective_seconds':limit,
            'work_deadline_unix':effective-14400,'shutdown_reserve_seconds':14400}


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


def install_coordination(base,manifest):
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
        except PendingAcknowledgement as error:
            raise base.PendingRegistration('submission_acknowledgement_pending: '+str(error)) from error
    base.tick=tick


def run_controller(a,manifest,area,fields):
    from horsehair import slurm
    from horsehair.core import exclusive_file_lock
    consumer=module(CONSUMER,CONSUMER_SHA,'preserved_census_consumer')
    binding,old_manifest,pins=consumer.check_binding(CONSUMER_BINDING,CONSUMER_BINDING_SHA)
    end=conservative_window(fields,float(os.environ['SLURM_JOB_END_TIME']))['allocation_end_unix']
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
    base=outer.load_base();install_coordination(base,manifest)
    # The controller mutex is separate from the short registration barrier and
    # is held for the entire allocation. Never delete or replace either mutex.
    with exclusive_file_lock(CAMPAIGN/'coordination-controller.mutex',timeout=60):
        c=base.load_context(Path(policy['base_descriptor']['path']),policy['base_descriptor']['sha256'],a.segment)
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
    for k in ('native-plan','native-ledger','native-script','qualification'):p.add_argument('--'+k,type=Path)
    p.add_argument('--qualification-sha256');p.add_argument('--native-task',required=True);p.add_argument('--segment',type=int,default=0)
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
