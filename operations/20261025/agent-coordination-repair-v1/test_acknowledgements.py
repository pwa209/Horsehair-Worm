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
