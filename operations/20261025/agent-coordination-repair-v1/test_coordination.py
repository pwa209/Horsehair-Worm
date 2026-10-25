from pathlib import Path
from datetime import datetime,timezone
from contextlib import contextmanager
from types import SimpleNamespace
import copy
import importlib.util
import json
import pytest

def load(name):
    spec=importlib.util.spec_from_file_location('coordination_test_'+name,Path(__file__).with_name(name+'.py'))
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value

m=load('coordination')

def fields(hours=8):
    start=1893456000
    return {'StartTime':datetime.fromtimestamp(start,timezone.utc).replace(tzinfo=None).isoformat(),
            'EndTime':datetime.fromtimestamp(start+hours*3600,timezone.utc).replace(tzinfo=None).isoformat(),
            'TimeLimit':f'{hours}:00:00','TimeMin':'08:00:00'}

@pytest.mark.parametrize('hours',[8,24,56,96])
@pytest.mark.parametrize('delta',[-3600,0,3600])
def test_backfill_never_extends_either_real_deadline(hours,delta):
    f=fields(hours);native=1893456000+hours*3600;environment=native+delta
    value=m.conservative_window(f,environment,now=1893456005)
    assert value['allocation_end_unix']==min(native,environment)
    assert value['work_deadline_unix']==min(native,environment)-14400
    assert value['shutdown_reserve_seconds']==14400
    assert value['environment_end_unix']==environment

@pytest.mark.parametrize('environment',[float('nan'),float('inf'),None,'1893484800',1893455900])
def test_missing_nonfinite_or_stale_environment_deadline_refused(environment):
    with pytest.raises(ValueError):m.conservative_window(fields(),environment,now=1893456005)

@pytest.mark.parametrize('key,value',[('TimeLimit','07:00:00'),('TimeLimit','97:00:00'),('TimeMin','04:00:00'),
    ('StartTime','2030-01-01T00:01:00'),('EndTime','2030-01-01T08:01:00')])
def test_native_limit_start_end_inconsistency_refused(key,value):
    f=fields();f[key]=value
    with pytest.raises(ValueError):m.conservative_window(f,1893484800,now=1893456005)

def test_shutdown_reserve_remains_strict():
    with pytest.raises(ValueError,match='reserve'):
        m.conservative_window(fields(),1893484800,now=1893484800-14700)

def test_audit_releases_global_mutex_and_commit_reacquires_it():
    held=[False];events=[];snapshot={'revision':443,'bytes':'unchanged'}
    @contextmanager
    def guard():
        assert not held[0];held[0]=True;events.append('lock')
        try:yield
        finally:held[0]=False;events.append('unlock')
    def audit():
        assert not held[0];events.append('expensive-byte-audit');return {'verified':'packets'}
    def commit(packets):
        assert held[0] and packets=={'verified':'packets'};events.append('commit');return 'ok'
    assert m.audit_then_commit(snapshot,audit,guard,lambda:copy.deepcopy(snapshot),commit)=='ok'
    assert events==['expensive-byte-audit','lock','commit','unlock'] and not held[0]

def test_audit_state_race_stops_before_any_commit():
    commits=[]
    @contextmanager
    def guard():yield
    with pytest.raises(ValueError,match='changed during'):
        m.audit_then_commit({'revision':443},lambda:{},guard,lambda:{'revision':444},lambda p:commits.append(p))
    assert not commits

def test_audit_exception_never_enters_commit_guard():
    events=[]
    def audit():raise OSError('Lustre unavailable')
    @contextmanager
    def guard():events.append('entered');yield
    with pytest.raises(OSError):m.audit_then_commit({},audit,guard,lambda:{},lambda p:events.append('commit'))
    assert not events

@pytest.fixture
def recovery(monkeypatch):
    from horsehair import agent_campaign as d,agents
    from horsehair.core import read_json,canonical_hash
    # The full original dispatcher is exercised against a synthetic frozen plan.
    config=read_json(Path(d.__file__).resolve().parents[2]/'configs/study.json')
    protocol={'config_sha256':agents._digest(config['agents']),'full_config_sha256':canonical_hash(config),
        'implementation_sha256':'a'*64,'H4_inference_specification':agents.h4_specification(config),
        'learning_rate':.001,'final_seeds':config['agents']['final_seeds'],'biological_directions':None}
    source={'primary_source_sha256':d.PRIMARY_SCIENTIFIC_SOURCE_SHA256,'primary_agents_sha256':'a'*64,
            'execution_source_sha256':'b'*64,'evaluation_module_sha256':'c'*64}
    policy={'path':str(d.ROOT/'maintenance/execution-resource-policies/fixture/policy.json'),'sha256':'d'*64,
        'execution_source_sha256':'b'*64,'base_config_sha256':canonical_hash(config),
        'primary_scientific_source_sha256':d.PRIMARY_SCIENTIFIC_SOURCE_SHA256,'limits':dict(d.LIMITS),
        'scientific_gates_changed':False,'approval':{'source':'current_user','authorizes_cpu_gpu_resource_amendment':True}}
    plan=d.plan_campaign(config,protocol,protocol_binding={'path':str(d.ROOT/'results/frozen_protocol.json'),'sha256':'e'*64},
        source_binding=source,policy_binding=policy,case=str(d.ROOT/'maintenance/synthetic-coordination-fixture'))
    state=d.initial_state(plan);native={};packets={}
    for index,job in [(0,'10000'),(80,m.FAILED),(81,'7110400'),(82,'7110771'),(83,'7111066')]:
        cell=state['cells'][index];intent=d._intent(plan,cell,'training');ident=intent['intent_id'];cell['training_job']=ident
        proof={'job_id':job,'state':'FAILED' if index==80 else 'COMPLETED','exit_code':'1:0' if index==80 else '0:0',
            'cpus':6,'gpus':1,'elapsed_seconds':60,'end':'2026-10-25T00:00:00','user':'pwa209','account':'def-ptewarie_gpu'}
        value={k:intent[k] for k in ('task','architecture','seed')};value.update(training_steps=1000000,
            complete_training_budget=True,implementation_sha256='a'*64,config_sha256=agents._digest(config['agents']),
            learning_rate=.001,checkpoint_sha256='1'*64,weight_sha256='2'*64)
        packet={'campaign_plan_sha256':plan['plan_sha256'],'intent_id':ident,'native_job_id':job,
            'case':intent['case'],'sha256':canonical_hash(value),'value':value}
        state['jobs'][ident]={'intent':intent,'job_id':job,'status':'needs_repair' if index==80 else 'complete' if index==0 else 'submitted',
            'native_proof':proof,'receipt':packet if index==0 else None}
        if index>=80:native[job]=proof
        if index>80:packets[ident]=packet
    state.update(revision=443,status='needs_repair',needs_repair=[m.TARGET]);state.pop('state_sha256')
    state=d._sealed(state,'state_sha256');monkeypatch.setattr(m,'ORIGINAL_STATE',state['state_sha256'])
    d._state(plan,state)
    return plan,state,native,packets,d

def test_three_receipts_reconcile_and_only_failed_prestart_record_is_removed(recovery):
    plan,state,native,packets,d=recovery;before=copy.deepcopy(state)
    result=m.recover_one(plan,state,native,packets,d)
    assert state==before and result['revision']==445 and result['status']=='active' and result['needs_repair']==[]
    assert m.TARGET not in result['jobs'] and result['cells'][80]['training_job'] is None
    assert all(result['jobs'][key]['status']=='complete' for key in m.PEERS)
    assert result['jobs']['training-0000']==state['jobs']['training-0000']
    assert all(result['cells'][i]==state['cells'][i] for i in range(240) if i!=80)
    assert result['H4_inference_status']==state['H4_inference_status'] and result['scientific_result'] is False
    d._state(plan,result)

@pytest.mark.parametrize('change',['revision','failed_identity','missing_peer','bad_checkpoint_summary','partial_downstream'])
def test_recovery_rejects_changed_or_incomplete_evidence(recovery,change):
    plan,state,native,packets,d=recovery
    if change=='revision':state['revision']=444
    elif change=='failed_identity':native[m.FAILED]={**native[m.FAILED],'exit_code':'2:0'}
    elif change=='missing_peer':packets.pop('training-0081')
    elif change=='bad_checkpoint_summary':packets['training-0081']['value']['complete_training_budget']=False
    else:state['cells'][80]['assembly_job']='unacknowledged-work'
    with pytest.raises((ValueError,KeyError)):m.recover_one(plan,state,native,packets,d)

def test_worker_guard_change_is_scoped_to_new_plans():
    calls=[]
    base=SimpleNamespace(native_namespace=lambda c,k:'original-'+k,submission_journal=lambda c,k:Path('original-'+k),
        native_plan=lambda c,i:calls.append((c,i)) or 'plan')
    guard={'path':'old','sha256':'a'*64};c={'registry_overlay':{'guard':guard,'untouched':'kept'}}
    manifest={'files':{'registry-wait.py':'b'*64}}
    m.install_coordination(base,manifest)
    assert base.native_namespace(c,m.TARGET)=='af-f2-r3-training-0080'
    assert base.native_namespace(c,'training-0081')=='original-training-0081'
    assert base.submission_journal(c,m.TARGET).name=='submission-r3-training-0080.json'
    assert base.native_plan(c,{'original_intent':True})=='plan'
    assert c['registry_overlay']['guard'] is guard and calls[0][0]['registry_overlay']['guard']['sha256']=='b'*64
    assert calls[0][1]=={'original_intent':True}

def test_worker_can_wait_past_previous_one_hour_budget(tmp_path,monkeypatch):
    g=load('registry-wait');monkeypatch.setattr(g,'ROOT',tmp_path)
    clock=[0.];attempts=[];held=[False]
    monkeypatch.setattr(g,'exclusive',lambda *args:None)
    @contextmanager
    def lock(path,timeout):
        attempts.append(timeout)
        if clock[0]<3660:
            clock[0]+=timeout;raise g.ReceiptLockTimeout(path,timeout,None)
        held[0]=True
        try:yield
        finally:held[0]=False
    with g.registration_lock(tmp_path/'maintenance/wait',20000.,role='worker-admission',lock_factory=lock,
            clock=lambda:clock[0],wall=lambda:10000.+clock[0]):
        assert held[0] and clock[0]==3660
    assert len(attempts)==62 and not held[0]
