"""Generic metadata/native guards and mutation-order contracts, no Slurm calls."""
from pathlib import Path
from datetime import datetime,timezone
from types import SimpleNamespace
import copy
import importlib.util
import json
import shlex
import pytest

HERE=Path(__file__).resolve().parent


@pytest.fixture
def m():
    p=HERE/'run-census-resilience-handoff-v2-20261024.py';s=importlib.util.spec_from_file_location('controlled_handoff_test',p)
    value=importlib.util.module_from_spec(s);s.loader.exec_module(value);return value


def write(m,path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes((json.dumps(value,sort_keys=True)+'\n').encode());return m.locator(path)


def zero(job='123',state='PENDING'):
    return {'job_id_raw':job,'user':'pwa209','account':'def-ptewarie_cpu','state':state,'exit_code':'0:0','cpus':0,
        'elapsed_seconds':0,'alloc_tres':'','unallocated_metadata':False,'identity_basis':'Unique scalar native job; zero-allocation queue record',
        'node_list':'None assigned','start':'Unknown','end':'Unknown' if state=='PENDING' else '2026-10-24T12:00:00','max_rss':'0K'}


def test_scalar_pending_is_not_array_template(m):m.zero_pending(zero(),'123')
def test_scalar_cancelled_zero(m):m.cancelled_zero(zero(state='CANCELLED'),'123')


def accepted_fixture(m):
    script='/scratch/pwa209/horsehair-worm/plans/held/controller.sbatch'
    task={'name':'controller','script':script,'script_sha256':'a'*64}
    plan={'tasks':[task]}
    ledger={'plan_sha256':m.canonical(plan),'tasks':{'controller':{'state':'submitting',
        'command':['sbatch','--parsable',script],'script_sha256':'a'*64,'dependency_kind':'afterany',
        'issued_native_dependencies':[],'completed_native_dependencies':[{'job_id':m.OLD['af'][0]}]}}}
    intent={'command':['sbatch','--hold','--parsable',script],'native_job_id':'7027712','automatic_replay_permitted':False}
    response={'returncode':0,'stdout':'7028292\n','stderr':m.MEMORY_NOTE}
    partial={'kind':'exact-accepted-held-prepare-reconciliation-v1','failed_job_id':'7027712','accepted_job_id':'7028292'}
    return plan,ledger,intent,response,partial


def test_exact_memory_notice_preserves_successful_held_acknowledgement(m):
    _,_,intent,response,_=accepted_fixture(m)
    assert m.accepted_memory_notice(intent['command'],SimpleNamespace(**response))


@pytest.mark.parametrize('field,value',[('returncode',1),('stdout','unknown\n'),('stdout','7028292\n7028293\n'),
    ('stderr','sbatch: error: unavailable\n'),('stderr','another informational note\n')])
def test_other_submission_output_still_requires_reconciliation(m,field,value):
    _,_,intent,response,_=accepted_fixture(m);response[field]=value
    assert not m.accepted_memory_notice(intent['command'],SimpleNamespace(**response))


def test_memory_notice_cannot_authorize_release_or_unheld_submit(m):
    _,_,intent,response,_=accepted_fixture(m)
    for command in [['scontrol','release','7028292'],['sbatch','--parsable',intent['command'][-1]]]:
        assert not m.accepted_memory_notice(command,SimpleNamespace(**response))


def test_accepted_reconciliation_only_adds_confirmed_identity_and_state(m):
    plan,ledger,intent,response,partial=accepted_fixture(m);original=copy.deepcopy(ledger)
    result=m.accepted_registration_candidate(plan,ledger,intent,response,partial)
    assert ledger==original
    expected=copy.deepcopy(original);expected['tasks']['controller'].update(state='submitted',job_id='7028292')
    assert result==expected


@pytest.mark.parametrize('target,field,value',[('response','stdout','7028293\n'),('response','stderr','unexpected warning\n'),
    ('response','returncode',1),('intent','native_job_id','7027713'),('intent','automatic_replay_permitted',True),
    ('partial','accepted_job_id','7028293'),('partial','failed_job_id','7027713')])
def test_accepted_reconciliation_refuses_changed_acknowledgement(m,target,field,value):
    plan,ledger,intent,response,partial=accepted_fixture(m)
    {'intent':intent,'response':response,'partial':partial}[target][field]=value
    with pytest.raises(ValueError):m.accepted_registration_candidate(plan,ledger,intent,response,partial)


@pytest.mark.parametrize('field,value',[('state','submitted'),('job_id','7028292'),('issued_native_dependencies',['123']),
    ('script_sha256','f'*64),('completed_native_dependencies',[])])
def test_accepted_reconciliation_refuses_changed_partial_row(m,field,value):
    plan,ledger,intent,response,partial=accepted_fixture(m);ledger['tasks']['controller'][field]=value
    with pytest.raises(ValueError):m.accepted_registration_candidate(plan,ledger,intent,response,partial)


@pytest.mark.parametrize('key,value',[('unallocated_metadata',True),('job_id_raw','124'),('user','other'),('cpus',1),('elapsed_seconds',1),
    ('alloc_tres','cpu=1'),('node_list','nl1'),('start','2026-10-24T01:00:00'),('max_rss','1K'),('identity_basis','unverified')])
def test_positive_zero_proof_rejects_changes(m,key,value):
    row=zero();row[key]=value
    with pytest.raises(ValueError):m.zero_pending(row,'123')


def test_active_cancel_preserves_actual_nonzero_signal_exit(m):
    row=zero(state='CANCELLED');row.update(cpus=1,elapsed_seconds=1000,unallocated_metadata=False,exit_code='0:15',alloc_tres='cpu=1')
    m.parent_terminal(row,'123',cancelled=True)
    with pytest.raises(ValueError):m.parent_terminal(row,'123')


def test_graceful_parent_completed(m):
    row=zero(state='COMPLETED');row.update(cpus=1,elapsed_seconds=1000,unallocated_metadata=False,alloc_tres='cpu=1')
    m.parent_terminal(row,'123')


def test_pending_estimated_start_is_not_allocation(m):
    task={'name':'one','script':'/plans/one.sbatch'}
    f={'JobId':'123','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobName':'hhw-one','Command':task['script'],
        'NumCPUs':'1','NumNodes':'1','MinMemoryNode':'64G','TimeLimit':'96:00:00','TimeMin':'08:00:00','JobState':'PENDING',
        'Reason':'JobHeldUser','Priority':'0','RunTime':'00:00:00','Restarts':'0','AllocTRES':'(null)','NodeList':'(null)',
        'StartTime':'2030-01-01T12:00:00'}
    assert m.scalar_fields(f,task,'123',hours=96,held=True)==f
    f['AllocTRES']='cpu=1'
    with pytest.raises(ValueError):m.scalar_fields(f,task,'123',hours=96,held=True)


def test_both_original_clocks_expired(m):
    now=1792844000;value={'measured_unix':now-21601,'captured_utc':datetime.fromtimestamp(now-21601,timezone.utc).isoformat()}
    m.expired_original_budget(value,now=lambda:now)


@pytest.mark.parametrize('change',['measured','captured','future','nan','naive'])
def test_negative_authority_rejects_fresh_or_invalid_clock(m,change):
    now=1792844000;value={'measured_unix':now-21601,'captured_utc':datetime.fromtimestamp(now-21601,timezone.utc).isoformat()}
    if change=='measured':value['measured_unix']=now-21600+1
    elif change=='captured':value['captured_utc']=datetime.fromtimestamp(now-1,timezone.utc).isoformat()
    elif change=='future':value['measured_unix']=now+1
    elif change=='nan':value['measured_unix']=float('nan')
    else:value['captured_utc']='2026-10-23T00:00:00'
    with pytest.raises(ValueError):m.expired_original_budget(value,now=lambda:now)


def closed_fixture(m,tmp_path):
    m.ROOT=tmp_path;case=tmp_path/'maintenance/oldcase';tasks=[];rows={};records={}
    for i in range(64):
        job=str(10000+i);name='slot-'+str(i);tasks.append({'name':name});rows[name]={'job_id':job};records[job]=zero(job,'CANCELLED')
    schedule={'plan':write(m,tmp_path/'plans/old/plan.json',{'tasks':tasks}),
        'ledger':write(m,tmp_path/'state/old.json',{'tasks':rows}),'case_root':str(case)}
    for key in ('helper','source_manifest','qualification'):schedule[key]=write(m,tmp_path/'maintenance'/(key+'.json'),{'kind':key})
    schedule['qualification']['job_id']='99'
    # Runtime schedule qualification has3keys, while source_guard consumes
    # just path/SHA; no report is interpreted for these all-cancelled records.
    return schedule,records


def test_all64_cancelled_old_providers_are_closed(m,tmp_path):
    schedule,records=closed_fixture(m,tmp_path)
    result=m.closed_old_providers(schedule,records)
    assert len(result['jobs'])==64 and result['old_reader_cannot_regain_fresh_pair_authority'] is True


def test_missing_old_provider_native_record_refused(m,tmp_path):
    schedule,records=closed_fixture(m,tmp_path);records.pop('10063')
    with pytest.raises(ValueError):m.closed_old_providers(schedule,records)


def test_live_old_provider_refused(m,tmp_path):
    schedule,records=closed_fixture(m,tmp_path);records['10063']['state']='RUNNING'
    with pytest.raises(ValueError):m.closed_old_providers(schedule,records)


def test_completed_old_requires_actual_receipt(m,tmp_path):
    schedule,records=closed_fixture(m,tmp_path);records['10063'].update(state='COMPLETED',cpus=8,elapsed_seconds=100,unallocated_metadata=False)
    with pytest.raises((ValueError,FileNotFoundError)):m.closed_old_providers(schedule,records)


def test_completed_old_exact_receipt_clocks(m,tmp_path):
    schedule,records=closed_fixture(m,tmp_path);job='10063';records[job].update(state='COMPLETED',cpus=8,elapsed_seconds=100,unallocated_metadata=False)
    now=1792844000;case=Path(schedule['case_root'])/('native-'+job)
    budget=write(m,case/'original-storage-budget.json',{'measured_unix':now-21601,'captured_utc':datetime.fromtimestamp(now-21601,timezone.utc).isoformat()})
    sweep=write(m,case/'sweep-result.json',{'budget':budget})
    write(m,case/'receipt.json',{'status':'complete_exact_size_sweep_with_original_conservative_freshness','native_job_id':job,
        'wrapper_sha256':schedule['helper']['sha256'],'source_manifest':schedule['source_manifest'],'qualification':schedule['qualification'],'sweep':sweep})
    result=m.closed_old_providers(schedule,records,now=lambda:now)
    assert result['completed_receipts_with_expired_original_clocks']==[job]
    Path(budget['path']).write_text('{}')
    with pytest.raises(ValueError):m.closed_old_providers(schedule,records,now=lambda:now)


def test_fresh_primary_gate_cannot_be_stale_status(m):
    af={'status':'resource_pending_storage','native_controller_job':m.OLD['af'][0],'scientific_result':False,'admission_authority':False}
    primary={'status':'pending_external_storage_census','controller_job_id':m.OLD['primary'][0],'config_sha256':m.CONFIG,
        'source_sha256':'0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630',
        'controller_framework_source_sha256':'d89b31180d4c4ffe9c4023dc5534b96f2d157a3a8e3aecaff30849283d00a705',
        'scheduling_paused':True,'scientific_results_complete':False,'publication_ready':False,'scientific_result':False,'admission_authority':False,'checked_unix':1000}
    m.pending_status(af,primary,now=lambda:1100)
    with pytest.raises(ValueError):m.pending_status(af,primary,now=lambda:1200)
    primary['controller_job_id']='older'
    with pytest.raises(ValueError):m.pending_status(af,primary,now=lambda:1100)


def test_two_sleep_samples_pid_and_cpu_stability(m):
    first={'pid':1,'job_id':'123','start_ticks':99,'cpu_ticks':2,'command':['python'],'children':[],'state':'S','wchan':'hrtimer_nanosleep','sampled_unix':1000}
    second={**first,'sampled_unix':1001};m.two_samples(first,second)
    for key,value in [('pid',2),('start_ticks',100),('cpu_ticks',3),('children',['2']),('state','R'),('wchan','wait'),('sampled_unix',1011)]:
        with pytest.raises(ValueError):m.two_samples(first,{**second,key:value})


def test_real_proc_metadata_fixture(m,tmp_path,monkeypatch):
    monkeypatch.setattr(m.os,'geteuid',lambda:3164768,raising=False);pid='123';p=tmp_path/pid;p.mkdir()
    (p/'status').write_text('Uid:\t3164768\t3164768\t3164768\t3164768\nThreads:\t1\n')
    command=['/home/env/python','-s','-u','/scratch/controller.py','--segment','0'];(p/'cmdline').write_bytes(('\0'.join(command)+'\0').encode())
    (p/'cgroup').write_text('0::/slurm/uid_3164768/job_456/step_batch\n');rest=['0']*30;rest[0]='S';rest[11]='10';rest[12]='2';rest[19]='99'
    (p/'stat').write_text('123 (python) '+' '.join(rest));(p/'wchan').write_text('hrtimer_nanosleep\n')
    task=p/'task'/pid;task.mkdir(parents=True);(task/'children').write_text('')
    result=m.proc_sample(pid,'456',shlex.join(command),proc=tmp_path);assert result['start_ticks']==99 and result['cpu_ticks']==12
    (task/'children').write_text('999')
    with pytest.raises(ValueError):m.proc_sample(pid,'456',shlex.join(command),proc=tmp_path)


def test_detached_same_allocation_process_is_refused(m,tmp_path):
    import os
    uid=os.getuid() if hasattr(os,'getuid') else 0
    def process(pid,parent,command,state='S'):
        p=tmp_path/str(pid);p.mkdir();rest=['0']*25;rest[0]=state;rest[1]=str(parent)
        (p/'stat').write_text(str(pid)+' (fixture) '+' '.join(rest));(p/'cgroup').write_text('0::/slurm/job_456/step_batch\n')
        (p/'cmdline').write_bytes(('\0'.join(command)+'\0').encode())
    process(1,0,['/bin/bash','batch']);process(2,1,['python','old.py']);process(3,0,['python','read-only-probe.py'],'R')
    assert set(m.same_job_processes(2,'456',proc=tmp_path,probe_pid=3,uid=uid))=={1,2,3}
    process(4,1,['python','detached-work.py'],'R')
    with pytest.raises(ValueError,match='Detached'):m.same_job_processes(2,'456',proc=tmp_path,probe_pid=3,uid=uid)


def test_checkpoint_detects_ambiguous_release(m,tmp_path):
    m.ROOT=tmp_path
    for name,value in [('campaign-plan.json',{}),('state-binding.json',{}),('submission-one.json',{'status':'releasing','job_id':'123'})]:write(m,tmp_path/name,value)
    (tmp_path/'state.json.gz').write_bytes(b'fixture')
    with pytest.raises(ValueError,match='Ambiguous'):m.checkpoint(tmp_path)


def af_fixture(m,tmp_path,count=1):
    m.ROOT=tmp_path;m.AF_CASE=tmp_path/'maintenance/af-case'
    campaign={'scientific_result':False,'admission_authority':False};campaign_sha=m.canonical(campaign);campaign['plan_sha256']=campaign_sha
    write(m,m.AF_CASE/'campaign-plan.json',campaign);native={};metadata=[]
    helper=HERE/'primary-roster.py';spec=importlib.util.spec_from_file_location('af_terminal_fixture',helper)
    roster=importlib.util.module_from_spec(spec);spec.loader.exec_module(roster)
    for i in range(count):
        phase='af-f2-training-'+str(i).zfill(4);job=str(10000+i);directory=tmp_path/'plans'/phase
        script=directory/(phase+'.sbatch');script.parent.mkdir(parents=True);text='#!/bin/bash\npython exact-worker.py\n';script.write_bytes(text.encode())
        resources={'cpus':4,'gpu':True,'hours':2,'memory':'64G'}
        task={'name':phase,'script':str(script),'script_sha256':m.canonical(text),'command':'python exact-worker.py',
            'depends':[],'external_dependencies':[],'dependency_kind':'afterok','resources':resources}
        intent={'campaign_plan_sha256':campaign_sha,'kind':'training','resources':{'cpus':4,'gpus':1,'hours':2},
            'scientific_result':False,'admission_authority':False};intent['intent_sha256']=m.canonical(intent)
        plan={'root':str(tmp_path),'phase':phase,'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},
            'tasks':[task],'campaign_plan_sha256':campaign_sha,'intent_sha256':intent['intent_sha256']}
        pp=directory/'plan.json';write(m,pp,plan);lp=tmp_path/'state'/(phase+'-submission.json')
        ledger={'plan_sha256':m.canonical(plan),'config_sha256':m.CONFIG,'source_sha256':m.SOURCE,
            'tasks':{phase:{'state':'submitted','job_id':job,'script_sha256':m.canonical(text)}}};write(m,lp,ledger)
        journal=m.AF_CASE/('submission-training-'+str(i).zfill(4)+'.json')
        write(m,journal,{'status':'released','job_id':job,'intent':intent,'plan_path':str(pp),'ledger_path':str(lp),'plan_sha256':m.digest(pp)})
        # Native costs are retained even when Slurm allocates more CPUs than
        # requested for a GPU/memory placement.
        native[job]={**zero(job,'COMPLETED'),'account':'def-ptewarie_gpu','cpus':6,'elapsed_seconds':15,
            'alloc_tres':'cpu=6,gres/gpu=1','node_list':'ng1','start':'2026-10-24T11:00:00','end':'2026-10-24T11:00:15'}
        metadata.append((pp,lp,journal,plan,ledger))
    state={'plan_sha256':campaign_sha,'jobs':{str(i):{'job_id':job} for i,job in enumerate(native)},'scientific_result':False,'admission_authority':False}
    state['state_sha256']=m.canonical(state)
    state_path=m.AF_CASE/'state.json.gz';state_path.write_bytes(m.gzip.compress(json.dumps(state).encode()))
    write(m,m.AF_CASE/'state-binding.json',{'campaign_plan_sha256':campaign_sha,'state_sha256':state['state_sha256'],
        'state_file_sha256':m.digest(state_path),'scientific_result':False,'admission_authority':False})
    calls=[]
    def reader(ids,arrays,**kwargs):calls.append((ids,arrays,kwargs));return native
    return {'native_reader':reader,'roster_module':roster},native,metadata,calls


def test_complete_historical_af_roster_exceeds_old128_cap(m,tmp_path):
    c,native,metadata,calls=af_fixture(m,tmp_path,221);proof=m.af_workers(c)
    assert proof['historical_journals']==221 and len(proof['native_records'])==221 and calls[0][2]['account'] is None
    assert proof['terminal_proof']['allocated_terminal_workers']==221 and proof['native_records']['10000']['cpus']==6


def test_historical_af_never_started_cancelled_is_not_allocated_cost(m,tmp_path):
    c,native,metadata,calls=af_fixture(m,tmp_path);native['10000']={**zero('10000','CANCELLED'),'account':'def-ptewarie_gpu'}
    proof=m.af_workers(c);assert proof['terminal_proof']['explicit_unallocated_cancelled_workers']==1


@pytest.mark.parametrize('change',['pending','foreign_account','array','source','ambiguous_release','missing_current_journal','state_selfhash','plan_selfhash','intent_selfhash'])
def test_historical_af_real_schema_rejects_unproved_workers(m,tmp_path,change):
    c,native,metadata,calls=af_fixture(m,tmp_path);pp,lp,journal,plan,ledger=metadata[0]
    if change=='pending':native['10000']['state']='PENDING'
    elif change=='foreign_account':native['10000']['account']='def-ptewarie_cpu'
    elif change=='ambiguous_release':
        q=json.loads(journal.read_bytes());q['status']='releasing';write(m,journal,q)
    elif change in {'missing_current_journal','state_selfhash'}:
        state_path=m.AF_CASE/'state.json.gz';state=json.loads(m.gzip.decompress(state_path.read_bytes()))
        state['jobs']['extra']={'job_id':'10001'}
        if change=='missing_current_journal':state['state_sha256']=m.canonical({k:v for k,v in state.items() if k!='state_sha256'})
        state_path.write_bytes(m.gzip.compress(json.dumps(state).encode()))
        binding=json.loads((m.AF_CASE/'state-binding.json').read_bytes());binding['state_file_sha256']=m.digest(state_path)
        binding['state_sha256']=state['state_sha256']
        write(m,m.AF_CASE/'state-binding.json',binding)
    elif change=='plan_selfhash':
        path=m.AF_CASE/'campaign-plan.json';campaign=json.loads(path.read_bytes());campaign['extra']='changed';write(m,path,campaign)
    elif change=='intent_selfhash':
        q=json.loads(journal.read_bytes());q['intent']['extra']='changed';write(m,journal,q)
    else:
        if change=='array':plan['tasks'][0]['resources']['array']='0-1'
        else:plan['release_identity']['sha256']='c'*64
        write(m,pp,plan);ledger['plan_sha256']=m.canonical(plan);write(m,lp,ledger)
        q=json.loads(journal.read_bytes());q['plan_sha256']=m.digest(pp);write(m,journal,q)
    with pytest.raises(ValueError):m.af_workers(c)


def retirement_context(m,tmp_path,monkeypatch):
    events=[];c={'end':m.time.time()+3600,'area':tmp_path,'sequence':0,'job':'999','checkpoint_proof':{'source_bound':True},
        'cancelled_parents':set(),'old_tasks':{job:{'name':'task-'+job} for ids in m.OLD.values() for job in ids}}
    c.update(verify_quiescent=lambda:events.append('quiescent'),verify_backup=lambda j:events.append('backup-proof-'+j),
        wait_terminal=lambda j,p:events.append('terminal-'+j),already_completed=lambda j:j==m.OLD['af'][0],final_proof=lambda:events.append('all-final'))
    def run(command,**kwargs):
        events.append(command);return SimpleNamespace(returncode=0,stdout='',stderr='',check_returncode=lambda:None)
    monkeypatch.setattr(m.subprocess,'run',run);return c,events


def test_retirement_backup_first_no_cancel_graceful_af(m,tmp_path,monkeypatch):
    c,events=retirement_context(m,tmp_path,monkeypatch);m.retirement(c)
    commands=[e for e in events if isinstance(e,list)]
    assert len(commands)==7 and all('--state=PENDING' in x for x in commands[:6])
    assert commands[-1][-1]==m.OLD['primary'][0] and '--state=RUNNING' in commands[-1]
    assert all(cmd[-1]!=m.OLD['af'][0] for cmd in commands)
    assert max(events.index('terminal-'+j) for ids in m.OLD.values() for j in ids[1:])<events.index(commands[-1])
    assert (tmp_path/'pre-retirement-checkpoint.json').exists() and (tmp_path/'retirement-proof.json').exists()


def test_quiescence_failure_before_any_cancellation(m,tmp_path,monkeypatch):
    c,events=retirement_context(m,tmp_path,monkeypatch)
    def reject():raise ValueError('not quiescent')
    c['verify_quiescent']=reject
    with pytest.raises(ValueError):m.retirement(c)
    assert not any(isinstance(e,list) for e in events)


def test_ambiguous_mutation_intent_is_preserved_without_replay(m,tmp_path,monkeypatch):
    c,events=retirement_context(m,tmp_path,monkeypatch)
    def timeout(*a,**kw):raise m.subprocess.TimeoutExpired(a[0],45)
    monkeypatch.setattr(m.subprocess,'run',timeout)
    with pytest.raises(m.subprocess.TimeoutExpired):m.mutate(c,'cancel',['scancel','123'],lambda:None)
    assert len(list(tmp_path.glob('*intent.json')))==1 and len(list(tmp_path.glob('*response.json')))==1
    response=json.loads(next(tmp_path.glob('*response.json')).read_text());assert response['status']=='ambiguous_action_no_replay'


def test_precheck_consumes_reserve_before_intent_or_mutation(m,tmp_path,monkeypatch):
    c,events=retirement_context(m,tmp_path,monkeypatch);clock=[1000.0];c['end']=2000.0
    monkeypatch.setattr(m.time,'time',lambda:clock[0])
    def precheck():clock[0]=1400.0
    with pytest.raises(ValueError,match='after precheck'):m.mutate(c,'cancel',['scancel','123'],precheck)
    assert c['sequence']==0 and not list(tmp_path.glob('*intent.json')) and not any(isinstance(e,list) for e in events)


def test_slow_intent_publication_cannot_spawn_past_reserve(m,tmp_path,monkeypatch):
    c,events=retirement_context(m,tmp_path,monkeypatch);clock=[1000.0];c['end']=2000.0;save=m.save
    monkeypatch.setattr(m.time,'time',lambda:clock[0])
    def delayed_save(path,value):
        save(path,value)
        if str(path).endswith('-intent.json'):clock[0]=1400.0
    monkeypatch.setattr(m,'save',delayed_save)
    with pytest.raises(ValueError,match='before command'):m.mutate(c,'cancel',['scancel','123'],lambda:None)
    assert len(list(tmp_path.glob('*intent.json')))==1 and len(list(tmp_path.glob('*response.json')))==1
    assert not any(isinstance(e,list) for e in events)


def test_release_refuses_af_guard_held(m,tmp_path):
    with pytest.raises(ValueError):m.release_replacements({'registration_locked':True})


def test_source_guard_rechecks_absence_and_bytes(m,tmp_path):
    p=tmp_path/'state';p.write_text('old');missing=tmp_path/'absent'
    m.source_guard({str(p):m.digest(p)},[str(missing)]);missing.write_text('appeared')
    with pytest.raises(ValueError):m.source_guard({str(p):m.digest(p)},[str(missing)])


def test_strict_duplicate_and_nonfinite_json(m):
    for raw in ('{"x":1,"x":2}','{"x":NaN}'):
        with pytest.raises(ValueError):m.strict(raw)


def test_pytest_fixtures_are_child_of_child_cwd(m):
    import ast
    source=(HERE/m.SELF).read_text();compile(source,m.SELF,'exec')
    assert "str(cwd/'fixtures')" in source and 'cwd=cwd' in source


def dependency_fixture(m):
    task={'name':'new','depends':[],'external_dependencies':['123'],'dependency_kind':'afterany'}
    row={'dependency_kind':'afterany','issued_native_dependencies':['123'],'completed_native_dependencies':[]}
    row['dependency_resolution_sha256']=m.canonical({'kind':'afterany','issued':['123'],'completed':[]})
    return task,{'tasks':{'new':row}}


def test_native_afterany_matches_exact_registered_edge(m):
    task,ledger=dependency_fixture(m)
    m.dependency_fields({'Dependency':'afterany:123(unfulfilled)'},task,ledger,lambda j:None)


def test_disappeared_native_edge_requires_positive_final_proof(m):
    task,ledger=dependency_fixture(m)
    with pytest.raises(ValueError):m.dependency_fields({'Dependency':'(null)'},task,ledger,lambda j:None)
    m.dependency_fields({'Dependency':'(null)'},task,ledger,lambda j:{'final':True})


def test_duplicate_wrong_or_afterok_native_edges_refused(m):
    task,ledger=dependency_fixture(m)
    for value in ('afterany:124(unfulfilled)','afterok:123(unfulfilled)','afterany:123(unfulfilled),afterany:123(unfulfilled)'):
        with pytest.raises(ValueError):m.dependency_fields({'Dependency':value},task,ledger,lambda j:None)


def test_completed_dependency_retains_exact_native_record(m):
    task,ledger=dependency_fixture(m);proof={'job_id':'123','dependency_kind':'afterany','native_records':{'123':{'state':'COMPLETED'}}}
    row=ledger['tasks']['new'];row.update(issued_native_dependencies=[],completed_native_dependencies=[proof])
    row['dependency_resolution_sha256']=m.canonical({'kind':'afterany','issued':[],'completed':[proof]})
    m.dependency_fields({'Dependency':'(null)'},task,ledger,lambda j:proof)
    with pytest.raises(ValueError):m.dependency_fields({'Dependency':'(null)'},task,ledger,lambda j:{'native_records':{}})


@pytest.mark.parametrize('extra_change',[False,True])
def test_single_owned_held_first_af_eight_hour_amendment(m,tmp_path,monkeypatch,extra_change):
    c={'area':tmp_path};calls=[];before={'TimeLimit':'96:00:00','TimeMin':'08:00:00','Command':'/new/first.sbatch','Dependency':'(null)'}
    after={**before,'TimeLimit':'08:00:00'}
    if extra_change:after['Command']='/other.sbatch'
    values=iter([before,after]);monkeypatch.setattr(m,'native',lambda *a:next(values));monkeypatch.setattr(m,'new_held',lambda *a:calls.append('held'))
    def mutation(c,label,command,precheck):precheck();calls.append(command)
    monkeypatch.setattr(m,'mutate',mutation)
    if extra_change:
        with pytest.raises(ValueError):m.amend_first_af(c,{},'123',{})
    else:
        m.amend_first_af(c,{},'123',{})
        assert [x for x in calls if isinstance(x,list)]==[['scontrol','update','JobId=123','TimeLimit=08:00:00']]
        result=json.loads((tmp_path/'first-af-eight-hour-after.json').read_text());assert result['conservative_lease_hours']==96


def test_terminal_reconciliation_preserves_conservative_lease(m):
    native={'state':'COMPLETED','cpus':1,'elapsed_seconds':100,'exit_code':'0:0'}
    budget={'native_jobs':{'123':native},'leases':{'123':{'cpus':1,'gpus':0,'max_hours':96,'state':'native_reconciled','native':native}}}
    m.lease_guard(budget,{'123':96},['124'],final=True)
    budget['leases']['124']={'state':'running'}
    with pytest.raises(ValueError):m.lease_guard(budget,{'123':96},['124'],final=True)


def test_own_native_qualification_and_activation_contracts(m,monkeypatch):
    end=1893499200;monkeypatch.setenv('SLURM_JOB_END_TIME',str(end));monkeypatch.setattr(m.time,'time',lambda:1000)
    task={'name':'q','script':'/q.sbatch'};fields={'JobId':'123','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING',
        'NumCPUs':'4','NumNodes':'1','MinMemoryNode':'16G','TimeLimit':'01:00:00','Command':'/q.sbatch','JobName':'hhw-q','EndTime':'2030-01-01T12:00:00'}
    assert m.own_allocation(fields,'123',task,'qualify')==end
    fields.update(NumCPUs='1',MinMemoryNode='64G',TimeLimit='04:00:00')
    assert m.own_allocation(fields,'123',task,'activate')==end
    fields['AllocTRES']='gpu=1'
    with pytest.raises(ValueError):m.own_allocation(fields,'123',task,'activate')
