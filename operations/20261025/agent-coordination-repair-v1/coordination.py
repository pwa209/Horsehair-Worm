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
SUPPORT=ROOT/'maintenance/agent-coordination-repair-v1-20261025'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
CAMPAIGN=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
CONSUMER=ROOT/'maintenance/study-census-resilience-consumers-v2-20261024/run-census-resilience-consumer-v2-20261024.py'
CONSUMER_SHA='d809f8b541ec7f764c00dfeda902b961bbdc28efe076ca20c3e6ff817b9c6efa'
CONSUMER_BINDING=CONSUMER.parent/'af-consumer-binding.json'
CONSUMER_BINDING_SHA='7c59a1e4a78ff3cbe5d44666a4830a16027585ea9525bf87c5f1040f83755288'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
OLD_CONTROLLERS=('7028292','7044270','7044530','7044573')
FAILED='7110041'
TARGET='training-0080'
PEERS={'training-0081':'7110400','training-0082':'7110771','training-0083':'7111066'}
ORIGINAL_STATE='68935200ddcb81342cffa0f40072d6adc3146a88ae142ddf04018890937bf115'
ORIGINAL_FILE='ac52c45fdc2da3302525532988e7cc3b864013539e09688594854bfbaf95a40e'
FLAGS={'scientific_result':False,'admission_authority':False,'scientific_gates_changed':False,
       'freshness_timestamp_rewritten':False,'numeric_functions_replaced':False}


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


def coordinated_tick(base,c):
    """Original validation/dispatch/submission with the slow audit outside the lock."""
    require(not c.get('release_queue'),'Ambiguous prior release queue requires reconciliation')
    base.prepare_census(c)
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
        base.source_guard(c);base.admission(c,'commit-'+str(state['revision']))
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
            and set(manifest['files'])=={'coordination.py','registry-wait.py','test_coordination.py','test_registry.py'},'Exact repair closure required')
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
              'hours':1 if mode=='qualify' else 8,'gpu':False,'array':None}
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
        result=subprocess.run([sys.executable,'-m','pytest',str(SUPPORT/'test_coordination.py'),str(SUPPORT/'test_registry.py'),'-q','-p','no:cacheprovider',
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
    require(c['segment']==0,'Only first new controller may activate the one-time recovery')
    with base.registration_guard(c,'controller-start'):
        plan,state=base.initialize(c);require(digest(CAMPAIGN/'state.json.gz')==ORIGINAL_FILE,'Original compressed state changed')
        area=SUPPORT/('recovery-native-'+c['job_id']);area.mkdir()
        preserved={}
        for name,source in [('original-state.json.gz',CAMPAIGN/'state.json.gz'),('original-binding.json',CAMPAIGN/'state-binding.json'),
                            ('campaign-plan.json',CAMPAIGN/'campaign-plan.json'),('failed-submission.json',CAMPAIGN/('submission-'+TARGET+'.json')),
                            ('failed-native-plan.json',ROOT/'plans/af-f2-training-0080/plan.json'),
                            ('failed-native-ledger.json',ROOT/'state/af-f2-training-0080-submission.json'),
                            ('failed-script.sbatch',ROOT/'plans/af-f2-training-0080/af-f2-training-0080.sbatch'),
                            ('failed-traceback.txt',ROOT/'logs/af-f2-training-0080-7110041_4294967294.log')]:
            raw=read(source);target=area/name
            with target.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
            preserved[name]=locator(target)
        text=read(area/'failed-traceback.txt').decode()
        require('RegistrationWaitBudgetExceeded' in text and 'native-start-admission' in text and 'AF_NATIVE_WORK ' not in text,
                'Exact before-computation lock-wait failure required')
        training=Path(plan['cells'][80]['training_case'])
        require(not training.is_symlink() and (not training.exists() or training.is_dir() and not list(training.iterdir())),
                'No partial training output may be discarded')
        require(not (CAMPAIGN.parent/('native-'+FAILED)/'receipt.json').exists()
                and not (CAMPAIGN.parent/('native-'+FAILED)/'native-start-admission.json').exists(),
                'Failed native attempt must not have completed admission or work')
        for p in [ROOT/'plans/af-f2-r3-training-0080',ROOT/'state/af-f2-r3-training-0080-submission.json',
                  CAMPAIGN/'submission-r3-training-0080.json']:
            require(not p.exists() and not p.is_symlink(),'Fresh retry namespace required')
        rows=c['native_reader']([FAILED,*PEERS.values()],{},owner='pwa209',account=None)
        native={job:base.normalized_native(job,row) for job,row in rows.items()}
        require(set(native)=={FAILED,*PEERS.values()},'Full targeted native roster required')
        for job in PEERS.values():require(native[job]['state']=='COMPLETED' and native[job]['exit_code']=='0:0'
            and native[job]['cpus']==6 and native[job]['gpus']==1 and native[job]['elapsed_seconds']>0,'Positive completed GPU training required')
        # This collector invocation verifies only training receipts/checkpoints;
        # it need not re-audit unrelated evaluation conditions during recovery.
        subset={**state,'jobs':{k:state['jobs'][k] for k in PEERS},'cells':[]}
        packets=base.collect_packets(c,plan,subset,native)
        for key,p in packets.items():
            original,_=base.worker_receipt(c,PEERS[key],'agent-train',int(key[-4:]))
            preserved[key+'-receipt']=dict(path=str(original),sha256=p['sha256'])
        result=recover_one(plan,state,native,packets,base.dispatch)
        exclusive(area/'intent.json',{'before_state':state['state_sha256'],'after_state':result['state_sha256'],
            'failed_original':state['jobs'][TARGET],'native':native,'preserved':preserved,**FLAGS})
        require(base.initialize(c)[1]==state,'State changed before recovery compare-and-swap')
        base.save_state(c,result)
        value={'status':'reconciled_three_and_recovered_one_prestart','native_job_id':c['job_id'],
            'source_manifest_sha256':manifest_sha,'before_revision':state['revision'],'after_revision':result['revision'],
            'after_state_sha256':result['state_sha256'],'completed_training':83,'completed_policies':76,
            'reconciled_jobs':PEERS,'failed_job_preserved':FAILED,'retry_namespace':'af-f2-r3-training-0080',
            'preserved':preserved,**FLAGS}
        exclusive(receipt,value);print('COORDINATION_RECOVERY '+json.dumps(value),flush=True)


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
    base.tick=lambda c,**kw:coordinated_tick(base,c)


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
    require(duration(fields['TimeLimit'])==contract['hours']*3600,'Actual scheduler limit must equal this finite registration')
    exclusive(area/'allocation.json',{'fields':fields,'environment_end_unix':float(os.environ['SLURM_JOB_END_TIME']),
        'source_manifest_sha256':a.source_manifest_sha256,**FLAGS})
    if a.mode=='qualify':qualification(a,manifest,area,fields)
    else:
        require(0<=a.segment<4 and len(plan['tasks'])==4,'Four finite ordered controller segments required')
        for index,t in enumerate(plan['tasks']):
            require(t['depends']==([] if index==0 else [plan['tasks'][index-1]['name']]) and t['dependency_kind']=='afterany',
                'No parallel controller lineage is authorized')
        return run_controller(a,manifest,area,fields)


if __name__=='__main__':main()
