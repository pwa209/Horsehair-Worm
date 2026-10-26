from pathlib import Path
import copy
import importlib.util
import subprocess
import pytest
from test_coordination import recovery

spec=importlib.util.spec_from_file_location('acknowledgement_tests_runtime',Path(__file__).with_name('coordination.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def identity():
    task={'name':'af-f2-assembly-0081','script':'/scratch/pwa209/horsehair-worm/plans/af-f2-assembly-0081/af-f2-assembly-0081.sbatch',
          'resources':{'cpus':1,'gpu':False,'hours':1,'memory':'32G'}}
    intent={'intent_id':'assembly-0081','resources':{'cpus':1,'gpus':0,'hours':1}}
    native={'JobId':'7191569','UserId':'pwa209(3164768)','JobName':'hhw-'+task['name'],'Command':task['script'],
        'Account':'def-ptewarie_cpu','JobState':'PENDING','Reason':'JobHeldUser','RunTime':'00:00:00',
        'NumCPUs':'1','MinMemoryNode':'32G','TimeLimit':'01:00:00','ReqTRES':'cpu=1,mem=32G,node=1','Priority':'0'}
    return task,intent,native


def test_exact_original_held_job_matches_all_identity_fields():
    t,i,n=identity();assert m.held_identity(n,t,i,'7191569') is n


@pytest.mark.parametrize('field,value',[('JobId','7191570'),('UserId','somebody(2)'),('Command','/other/script'),
    ('JobName','hhw-other'),('Account','other'),('JobState','RUNNING'),('Reason','Priority'),('RunTime','00:00:01'),
    ('NumCPUs','2'),('MinMemoryNode','64G'),('TimeLimit','02:00:00'),('ReqTRES','cpu=1,gres/gpu=1'),('ArrayJobId','7191569')])
def test_name_alone_never_authorizes_adoption(field,value):
    t,i,n=identity();n[field]=value
    with pytest.raises(ValueError):m.held_identity(n,t,i,'7191569')


def test_timed_out_accepted_submission_is_adopted_without_a_second_sbatch():
    calls=[];saved=[];_,_,n=identity();command=['sbatch','--parsable','original.sbatch']
    def invoke(cmd):
        calls.append(cmd);raise subprocess.TimeoutExpired(cmd,180)
    def discover(hint):
        assert hint is None and saved[0]['state']=='submitting';return '7191569',n
    result=m.submit_once(None,command,invoke,discover,lambda value:saved.append(copy.deepcopy(value)))
    assert calls==[command] and result['job_id']=='7191569'
    assert result['acknowledgement_basis']=='exact_owned_held_native_reconciliation'
    assert result['client_timeout']['seconds']==180 and result['native_held_observation']==n
    # A restarted caller reuses the acknowledged native job, never resubmits.
    again=m.submit_once(result,command,lambda cmd:pytest.fail('duplicate sbatch'),
        lambda hint:('7191569',n) if hint=='7191569' else pytest.fail('lost native ID'),lambda value:None)
    assert again['job_id']=='7191569' and len(calls)==1


def test_unresolved_timeout_waits_and_restart_only_repeats_native_discovery():
    saved=[];calls=[];command=['sbatch','--parsable','original.sbatch']
    def invoke(cmd):calls.append(cmd);raise subprocess.TimeoutExpired(cmd,180,output=b'partial reply')
    def absent(hint):raise m.PendingAcknowledgement('not visible yet')
    with pytest.raises(m.PendingAcknowledgement):m.submit_once(None,command,invoke,absent,lambda row:saved.append(copy.deepcopy(row)))
    row=saved[-1];assert row['state']=='submitting' and row['client_timeout']['stdout']=='partial reply'
    with pytest.raises(m.PendingAcknowledgement):
        m.submit_once(row,command,lambda cmd:pytest.fail('must not repeat mutation'),absent,lambda row:None)
    assert len(calls)==1


def test_existing_legacy_submitting_ledger_recovers_without_a_client_call():
    _,_,n=identity();command=['sbatch','--parsable','legacy.sbatch']
    legacy={'state':'submitting','command':command,'script_sha256':'a'*64}
    result=m.submit_once(legacy,command,lambda cmd:pytest.fail('legacy accepted job duplicated'),
        lambda hint:('7191569',n),lambda row:None)
    assert result['job_id']=='7191569' and result['script_sha256']=='a'*64


def test_normal_acknowledgement_is_persisted_before_native_read_timeout():
    saved=[];command=['sbatch','--parsable','new.sbatch']
    def waiting(hint):
        assert hint=='123' and saved[-1]['state']=='submitted';raise m.PendingAcknowledgement('scheduler read timeout')
    with pytest.raises(m.PendingAcknowledgement):
        m.submit_once(None,command,lambda cmd:subprocess.CompletedProcess(cmd,0,'123\n',''),waiting,
            lambda row:saved.append(copy.deepcopy(row)))
    assert saved[-1]['job_id']=='123' and saved[-1]['acknowledgement_basis']=='actual_sbatch_stdout'


def test_negative_or_malformed_reply_never_causes_an_automatic_second_call():
    calls=[];saved=[];command=['sbatch','--parsable','new.sbatch']
    def invoke(cmd):calls.append(cmd);return subprocess.CompletedProcess(cmd,1,'','connection failed')
    def unresolved(hint):raise m.PendingAcknowledgement('no certified native job')
    with pytest.raises(m.PendingAcknowledgement):m.submit_once(None,command,invoke,unresolved,lambda row:saved.append(copy.deepcopy(row)))
    with pytest.raises(m.PendingAcknowledgement):m.submit_once(saved[-1],command,invoke,unresolved,lambda row:None)
    assert len(calls)==1


def test_changed_attempt_command_cannot_be_replayed_or_adopted():
    with pytest.raises(ValueError,match='changed'):
        m.submit_once({'state':'submitting','command':['sbatch','old']},['sbatch','new'],
            lambda c:pytest.fail('unexpected mutation'),lambda h:pytest.fail('wrong lookup'),lambda r:None)


def test_native_discovery_cannot_replace_an_acknowledged_id():
    _,_,native=identity();cmd=['sbatch','--parsable','original.sbatch']
    with pytest.raises(ValueError,match='replace an acknowledged'):
        m.submit_once({'state':'submitted','job_id':'123','command':cmd},cmd,lambda c:pytest.fail('repeat'),
                      lambda h:('7191569',native),lambda r:None)


def test_completed_receipts_reconcile_without_generating_work(recovery):
    plan,state,native,packets,dispatch=recovery;before=copy.deepcopy(state)
    result=m.reconcile_only(plan,state,native,packets,dispatch)
    assert state==before and set(result['jobs'])==set(state['jobs'])
    assert all(result['jobs'][ident]['status']=='complete' for ident in packets)
    assert result['jobs'][m.TARGET]==state['jobs'][m.TARGET] and result['needs_repair']==state['needs_repair']
    assert all(job['job_id'] is not None for job in result['jobs'].values())


def test_reconciliation_retains_original_training_validation(recovery):
    plan,state,native,packets,dispatch=recovery
    packets['training-0081']['value']['complete_training_budget']=False
    with pytest.raises(ValueError):m.reconcile_only(plan,state,native,packets,dispatch)


@pytest.mark.parametrize('state,reason,priority,expected',[
    ('PENDING','Priority','1',True),('PENDING','Resources','10',True),('RUNNING','None','10',True),
    ('COMPLETED','None','10',True),('PENDING','JobHeldUser','0',False),('PENDING','JobHeldAdmin','1',False),
    ('PENDING','Priority','0',False),('PENDING','Priority','unknown',False)])
def test_lost_release_ack_can_use_actual_unheld_native_state(state,reason,priority,expected):
    t,i,n=identity();n.update(JobState=state,Reason=reason,Priority=priority)
    assert m.released_identity(n,t,'7191569') is expected


def test_release_cannot_adopt_an_unrelated_unheld_job():
    t,i,n=identity();n.update(JobState='RUNNING',Command='/another/script')
    assert not m.released_identity(n,t,'7191569')


def test_native_backfill_day_minute_precision_and_environment_difference():
    f={'StartTime':'2026-10-25T00:53:29','EndTime':'2026-10-27T08:59:29','TimeLimit':'2-08:06:00','TimeMin':'08:00:00'}
    start=m.datetime.fromisoformat(f['StartTime']).replace(tzinfo=m.timezone.utc).timestamp()
    end=m.datetime.fromisoformat(f['EndTime']).replace(tzinfo=m.timezone.utc).timestamp()
    value=m.conservative_window(f,end+10*3600,now=start+10)
    assert value['allocation_end_unix']==end and value['effective_seconds']==56*3600+360


def test_pending_acknowledgement_is_a_wait_condition_not_a_fatal_controller_error():
    from types import SimpleNamespace
    base=SimpleNamespace(native_namespace=lambda c,i:i,submission_journal=lambda c,i:Path(i),
                         native_plan=lambda c,i:None,PendingRegistration=type('PendingRegistration',(Exception,),{}))
    original=m.coordinated_tick
    try:
        def wait(*args):raise m.PendingAcknowledgement('native lookup still pending')
        m.coordinated_tick=wait;m.install_coordination(base,{'files':{'registry-wait.py':'x'}})
        with pytest.raises(base.PendingRegistration,match='submission_acknowledgement_pending'):base.tick({})
    finally:m.coordinated_tick=original


def test_deadline_preflight_never_records_or_invokes_an_unstarted_submission():
    saved=[];calls=[];c={'deadline':1000.};before=copy.deepcopy(c)
    def gate():m.submission_window(c,now=700.)
    with pytest.raises(m.SubmissionWindowExhausted):
        m.submit_once(None,['sbatch','--parsable','new.sbatch'],lambda cmd:calls.append(cmd),
            lambda hint:pytest.fail('No submission exists to discover'),lambda row:saved.append(copy.deepcopy(row)),
            before_attempt=gate)
    assert not saved and not calls and c==before


@pytest.mark.parametrize('now,allowed',[(699.999,True),(700.,False),(701.,False)])
def test_submission_window_has_a_strict_boundary_without_extending_any_clock(now,allowed):
    c={'deadline':1000.,'backfill_window':{'allocation_end_unix':15400.,'shutdown_reserve_seconds':14400}}
    before=copy.deepcopy(c)
    if allowed:m.submission_window(c,now=now)
    else:
        with pytest.raises(m.SubmissionWindowExhausted):m.submission_window(c,now=now)
    assert c==before


@pytest.mark.parametrize('state,job',[('submitting',None),('submitted','7191569')])
def test_deadline_does_not_replay_or_gate_discovery_of_an_existing_submission(state,job):
    _,_,native=identity();command=['sbatch','--parsable','original.sbatch']
    row={'state':state,'command':command}
    if job is not None:row['job_id']=job
    hints=[]
    def discover(hint):hints.append(hint);return '7191569',native
    result=m.submit_once(row,command,lambda cmd:pytest.fail('Existing mutation cannot repeat'),discover,
        lambda value:None,before_attempt=lambda:pytest.fail('Existing attempts only reconcile'))
    assert hints==[job] and result['job_id']=='7191569' and result['state']=='submitted'


def test_partial_batch_retains_accepted_id_and_successor_submits_only_unstarted_member():
    _,_,native=identity();clock=[0.];saved={};calls=[];c={'deadline':1000.}
    first=['sbatch','--parsable','first.sbatch'];second=['sbatch','--parsable','second.sbatch']
    def invoke(command):calls.append(command);return subprocess.CompletedProcess(command,0,'7191569\n','')
    def persist(key):return lambda row:saved.__setitem__(key,copy.deepcopy(row))
    m.submit_once(None,first,invoke,lambda hint:('7191569',native),persist('first'),
        before_attempt=lambda:m.submission_window(c,now=clock[0]))
    clock[0]=701.
    with pytest.raises(m.SubmissionWindowExhausted):
        m.submit_once(None,second,invoke,lambda hint:pytest.fail('Second has not run'),persist('second'),
            before_attempt=lambda:m.submission_window(c,now=clock[0]))
    assert saved['first']['job_id']=='7191569' and 'second' not in saved and calls==[first]
    # The next finite segment preserves the first acknowledgement and only
    # invokes the member that never received a durable attempt record.
    c={'deadline':2000.}
    m.submit_once(saved['first'],first,lambda cmd:pytest.fail('Accepted member repeated'),
        lambda hint:('7191569',native),persist('first'),before_attempt=lambda:m.submission_window(c,now=clock[0]))
    def second_invoke(command):calls.append(command);return subprocess.CompletedProcess(command,0,'7191570\n','')
    result=m.submit_once(None,second,second_invoke,lambda hint:('7191570',{**native,'JobId':'7191570'}),persist('second'),
        before_attempt=lambda:m.submission_window(c,now=clock[0]))
    assert calls==[first,second] and result['job_id']=='7191570' and saved['first']['job_id']=='7191569'


def test_long_audit_reaching_cutoff_never_dispatches_or_commits(monkeypatch):
    from contextlib import nullcontext
    from types import SimpleNamespace
    clock=[0.];events=[];state={'revision':1,'jobs':{},'cells':[]};plan={}
    c={'deadline':1000.,'descriptor':{'precision_gate':{'diagnostics_job_id':'1','assembly_job_id':'2'}},'area':Path('audit')}
    def audit(*args):clock[0]=701.;events.append('audited');return {}
    base=SimpleNamespace(prepare_census=lambda c:None,registration_guard=lambda *args:nullcontext(),
        initialize=lambda c:(plan,state),source_guard=lambda c:None,admission=lambda c,label:events.append(label),
        collect_packets=audit,atomic_json=lambda *args:None,
        dispatch=SimpleNamespace(dispatch_tick=lambda *args:pytest.fail('Late audit must not generate work')),
        save_state=lambda *args:pytest.fail('Late audit must not commit state'))
    c['native_reader']=lambda *args,**kwargs:{}
    monkeypatch.setattr(m,'pending_intents',lambda *args:[])
    monkeypatch.setattr(m.time,'time',lambda:clock[0])
    with pytest.raises(m.SubmissionWindowExhausted):m.coordinated_tick(base,c)
    assert events==['snapshot-1','audited'] and state=={'revision':1,'jobs':{},'cells':[]}


def test_real_base_loop_turns_deadline_cutoff_into_successful_finite_handoff(tmp_path,monkeypatch):
    import ast
    import hashlib
    import math
    from contextlib import nullcontext
    from types import SimpleNamespace
    path=Path(__file__).resolve().parents[2]/'20261024/f2-backfill-controller-v2/run-af-agent-controller-backfill-base-v2-20261024.py'
    if not path.is_file():
        path=Path('/scratch/pwa209/horsehair-worm/maintenance/study-af-backfill-controller-support-v2-20261024/run-af-agent-controller-backfill-base-v2-20261024.py')
    source=path.read_bytes()
    assert hashlib.sha256(source).hexdigest()=='cbf01cffbbaa9a983ec1e8e6394b38ac04623483e768398c18f48b5e30e31445'
    # Exercise the exact frozen run function, while keeping its dependencies
    # synthetic and identical in local tests and the native qualification job.
    node=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='run')
    clock=[710.];writes={};calls=[]
    base=SimpleNamespace(time=m.time,math=math,json=m.json,ROOT=tmp_path,CONTROLLER_TICK_SECONDS=180,
        BACKFILL_MAX_SECONDS=96*3600,PendingStorage=type('PendingStorage',(Exception,),{}),
        PendingRegistration=type('PendingRegistration',(Exception,),{}),require=m.require,
        native_namespace=lambda c,i:i,submission_journal=lambda c,i:Path(i),native_plan=lambda c,i:None,
        resource_lease=lambda *args,**kwargs:nullcontext(),
        atomic_json=lambda path,value:writes.__setitem__(Path(path).name,copy.deepcopy(value)))
    exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),base.__dict__)
    c={'deadline':1000.,'config':{},'policy':{},'job_id':'123','segment':0,
       'case':tmp_path/'case','area':tmp_path/'area','backfill_window':{'allocation_end_unix':15400.}}
    before=copy.deepcopy(c)
    monkeypatch.setattr(m.time,'time',lambda:clock[0])
    def late_tick(runtime,context):calls.append('tick');m.submission_window(context)
    monkeypatch.setattr(m,'coordinated_tick',late_tick)
    m.install_coordination(base,{'files':{'registry-wait.py':'x'}})
    result=base.run(c,clock=lambda:clock[0],sleeper=lambda seconds:clock.__setitem__(0,clock[0]+seconds))
    assert calls==['tick'] and result['status']=='finite_segment_complete'
    assert writes['status.json']['status']=='resource_pending_registration'
    assert writes['segment-complete.json']==result and 'controller-failure.json' not in writes
    assert c==before and clock[0]<c['deadline']
