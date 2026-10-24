"""Mocked native orchestration; never contacts a cluster or submits real work."""
import copy
from contextlib import contextmanager
from datetime import datetime,timezone
import gzip
import importlib.util
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys

import numpy as np
import pandas as pd
import pytest

from horsehair import agent_campaign as dispatch, agents
from horsehair.core import atomic_json,canonical_hash,digest,read_json
from horsehair.core import ReceiptLockTimeout
from horsehair.execution_resources import LIMITS

spec=importlib.util.spec_from_file_location('private_af_controller_v6',Path(__file__).with_name('run-af-agent-controller-registry-v6-20261022.py'))
c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)
REAL_PRECISION_GATE=c.precision_gate
REAL_STORAGE_GUARD=c.storage_guard
REAL_SOURCE_GUARD=c.source_guard
REAL_ADMISSION=c.admission


class Policy:
    def __init__(self,record):self.record=record
    def receipt(self):return copy.deepcopy(self.record)


@pytest.fixture
def fixture(tmp_path,monkeypatch):
    config=read_json(Path(agents.__file__).resolve().parents[2]/'configs/study.json')
    protocol={'config_sha256':agents._digest(config['agents']),'full_config_sha256':canonical_hash(config),
        'implementation_sha256':c.AGENTS_SHA,'H4_inference_specification':agents.h4_specification(config),
        'learning_rate':.001,'final_seeds':config['agents']['final_seeds'],'biological_directions':None}
    policy={'path':'/scratch/pwa209/horsehair-worm/maintenance/execution-resource-policies/fixture/policy.json',
        'sha256':'d'*64,'execution_source_sha256':c.FRAMEWORK_SHA,'base_config_sha256':canonical_hash(config),
        'primary_scientific_source_sha256':c.PRIMARY_SCIENTIFIC_SOURCE_SHA256,'limits':dict(LIMITS),
        'scientific_gates_changed':False,'approval':{'source':'current_user','authorizes_cpu_gpu_resource_amendment':True}}
    plan=dispatch.plan_campaign(config,protocol,protocol_binding={'path':'/scratch/pwa209/horsehair-worm/results/frozen_protocol.json','sha256':'e'*64},
        source_binding={'primary_source_sha256':c.PRIMARY_SCIENTIFIC_SOURCE_SHA256,'primary_agents_sha256':c.AGENTS_SHA,
        'execution_source_sha256':c.FRAMEWORK_SHA,'evaluation_module_sha256':'c'*64},policy_binding=policy,
        case='/scratch/pwa209/horsehair-worm/maintenance/final-campaign-fixture')
    root=tmp_path/'scratch';case=root/'maintenance/campaign/agent-final-f64-v1';case.mkdir(parents=True)
    area=case/'controller-0-99';area.mkdir();(root/'state').mkdir();framework=root/'releases/qualified';framework.mkdir(parents=True)
    worker=root/'maintenance/worker.py';worker.write_text('# private fixture')
    descriptor={'case':str(case.parent),'worker_path':str(worker),'execution_source_sha256':c.FRAMEWORK_SHA,
        'precision_gate':{'diagnostics_job_id':'1','assembly_job_id':'2'}}
    ctx={'descriptor':descriptor,'descriptor_path':root/'maintenance/descriptor.json','descriptor_sha256':'f'*64,
        'worker_descriptor_path':root/'maintenance/stable-worker-descriptor.json','worker_descriptor_sha256':'9'*64,
        'framework':framework,'config':config,'job_id':'99','segment':0,'case':case,'area':area,
        'policy':Policy(policy),'protocol':protocol,'protocol_path':root/'results/frozen_protocol.json',
        'deadline':1000000.,'primary_release_id':'original'}
    guard=root/'maintenance/registry-guard.py';guard.write_text('# guard fixture')
    shim=root/'maintenance/worker-overlay.py';shim.write_text('# shim fixture')
    ctx['registry_overlay']={'guard':{'path':str(guard),'sha256':digest(guard)},
        'worker_shim':{'path':str(shim),'sha256':digest(shim)},
        'recovery_value':{'native_namespace_overrides':{'training-0000':'af-f2-r1-training-0000'},
            'submission_journal_overrides':{'training-0000':'submission-r1-training-0000.json'}}}
    monkeypatch.setattr(c,'ROOT',root)
    monkeypatch.setattr(c,'source_identity',lambda path:{'files':{},'sha256':c.FRAMEWORK_SHA})
    monkeypatch.setattr(c,'initialize',lambda context:(copy.deepcopy(plan),dispatch.initial_state(plan)))
    events=[]
    @contextmanager
    def registration(context,role):
        context['fixture_registration_locked']=True;events.append(('registration-enter',role))
        try:yield
        finally:context['fixture_registration_locked']=False;events.append(('registration-exit',role))
    monkeypatch.setattr(c,'registration_guard',registration)
    monkeypatch.setattr(c,'prepare_census',lambda context:events.append(('census-outside-lock',context.get('fixture_registration_locked',False))))
    monkeypatch.setattr(c,'admission',lambda context,label,**kw:events.append(('admission',label,kw)) or {'status':'admitted'})
    monkeypatch.setattr(c,'storage_guard',lambda *args,**kwargs:{'status':'ready'})
    monkeypatch.setattr(c,'precision_gate',lambda *args:False)
    monkeypatch.setattr(c,'source_guard',lambda *args:None)
    counter=[100];submitted_scripts={}
    def runner(command,**kwargs):
        events.append(('run',command.copy(),kwargs.copy()))
        if command[0]=='sbatch':
            assert kwargs['timeout']==45
            assert '#SBATCH --hold' in Path(command[-1]).read_text()
            counter[0]+=1;job=str(counter[0]);submitted_scripts[job]=Path(command[-1])
            return subprocess.CompletedProcess(command,0,job+'\n','')
        assert command[:2]==['scontrol','release'] and not ctx.get('fixture_registration_locked',False)
        return subprocess.CompletedProcess(command,0,'','')
    def scheduler(job,evidence):
        if job not in submitted_scripts:raise ValueError('Foreign mocked native job identity')
        evidence.mkdir(parents=True,exist_ok=False)
        script=submitted_scripts[job];value=read_json(script.parent/'plan.json')
        task=next(task for task in value['tasks'] if Path(task['script'])==script)
        r=task['resources'];events.append(('held',job))
        text=f"JobId={job} UserId=pwa209(3164768) JobName=hhw-{task['name']} JobState=PENDING Reason=JobHeldUser NumCPUs={r['cpus']} Account={config['paths']['gpu_account' if r['gpu'] else 'cpu_account']} Command={task['script']} TimeLimit={r['hours']}:00:00 ReqTRES=cpu={r['cpus']},gres/gpu={int(r['gpu'])}"
        response=subprocess.CompletedProcess([],0,text,'')
        # Actual Narval paths have no spaces. Preserve the native field boundary
        # in this Windows workspace fixture, whose parent directory has a space.
        response.fixture_fields={'JobId':job,'UserId':'pwa209(3164768)','JobName':'hhw-'+task['name'],
            'JobState':'PENDING','Reason':'JobHeldUser','NumCPUs':str(r['cpus']),
            'NumNodes':'1','MinMemoryNode':r['memory'],
            'Account':config['paths']['gpu_account' if r['gpu'] else 'cpu_account'],
            'Command':task['script'],'TimeLimit':str(r['hours'])+':00:00','ReqTRES':f"cpu={r['cpus']},gres/gpu={int(r['gpu'])}"}
        return response
    ctx['scheduler']=scheduler
    original_fields=c.fields
    monkeypatch.setattr(c,'fields',lambda response:response.fixture_fields if hasattr(response,'fixture_fields') else original_fields(response))
    def native_reader(ids,*args,**kwargs):
        return {i:{'state':'PENDING','exit_code':'0:0','cpus':0,'alloc_tres':'','elapsed_seconds':0,
            'user':'pwa209','account':'def-ptewarie_cpu'} for i in ids}
    return ctx,plan,events,runner,native_reader


def load_state(ctx):
    with gzip.open(ctx['case']/'state.json.gz','rt') as stream:return json.load(stream)


def test_initial_four_gpu_jobs_are_registered_held_verified_then_released_once(fixture):
    ctx,plan,events,runner,reader=fixture
    result=c.tick(ctx,runner=runner,native_reader=reader)
    assert result['new_intentions']==result['jobs']==4 and result['precision_gate'] is False
    state=load_state(ctx)
    assert {j['job_id'] for j in state['jobs'].values()}=={'101','102','103','104'}
    assert all(j['intent']['resources']=={'cpus':6,'gpus':1,'hours':12} for j in state['jobs'].values())
    plans=list((c.ROOT/'plans').glob('*/plan.json'));ledgers=list((c.ROOT/'state').glob('*-submission.json'))
    assert len(plans)==len(ledgers)==4
    for path in ledgers:
        ledger=read_json(path);assert len(ledger['tasks'])==1
        task=next(iter(ledger['tasks'].values()));assert task['state']=='submitted'
    releases=[e for e in events if e[0]=='run' and e[1][0]=='scontrol'];assert len(releases)==4
    for event in releases:
        index=events.index(event);job=event[1][-1]
        assert ('held',job) in events[:index]
        before=[e for e in events[:index] if e[0]=='admission'][-1]
        assert before[1].startswith('registered-') and before[2]=={'cpus':0,'gpus':0,'hours':1}
    assert all(read_json(p)['status']=='released' for p in ctx['case'].glob('submission-*.json'))
    assert read_json(ctx['case']/'state-binding.json')['state_file_sha256']==digest(ctx['case']/'state.json.gz')


def test_sbatch_timeout_keeps_ambiguous_ledger_and_never_replays(fixture,monkeypatch):
    ctx,plan,events,runner,reader=fixture
    def fail(command,**kw):
        events.append(('attempt',command));raise subprocess.TimeoutExpired(command,45)
    with pytest.raises(subprocess.TimeoutExpired):c.tick(ctx,runner=fail,native_reader=reader)
    state=load_state(ctx);assert all(j['job_id'] is None for j in state['jobs'].values())
    ledger=read_json(next((c.ROOT/'state').glob('*-submission.json')))
    assert next(iter(ledger['tasks'].values()))['state']=='submitting'
    monkeypatch.setattr(c,'initialize',lambda context:(plan,state))
    with pytest.raises(ValueError,match='Ambiguous prior'):c.tick(ctx,runner=runner,native_reader=reader)
    assert len([e for e in events if e[0]=='attempt'])==1


@pytest.mark.parametrize('field,value',[('UserId','other(4)'),('NumCPUs','8'),('Account','def-other_gpu'),
    ('Reason','Resources'),('Command','/other/script'),('TimeLimit','24:00:00'),('ReqTRES','cpu=6,gres/gpu=2'),
    ('NumNodes','2'),('NumNodes','1-2'),('NumNodes','0-1'),('NumNodes','01'),('NumNodes','1,1'),('MinMemoryNode','128G')])
def test_foreign_or_changed_held_contract_is_never_released(fixture,field,value):
    ctx,plan,events,runner,reader=fixture;original=ctx['scheduler']
    def changed(job,evidence):
        response=original(job,evidence)
        response.fixture_fields[field]=value
        response.stdout=rewrite(response.stdout,field,value);return response
    ctx['scheduler']=changed
    with pytest.raises(ValueError):c.tick(ctx,runner=runner,native_reader=reader)
    assert not [e for e in events if e[0]=='run' and e[1][0]=='scontrol']
    assert read_json(next(ctx['case'].glob('submission-*.json')))['status']=='registered_held'


def rewrite(text,key,value):
    return ' '.join(f'{key}={value}' if p.startswith(key+'=') else p for p in text.split())


def test_semantically_exact_single_node_range_is_verified_and_released(fixture):
    ctx,plan,events,runner,reader=fixture;original=ctx['scheduler']
    def exact_range(job,evidence):
        response=original(job,evidence);response.fixture_fields['NumNodes']='1-1';return response
    ctx['scheduler']=exact_range
    result=c.tick(ctx,runner=runner,native_reader=reader)
    assert result['new_intentions']==4
    assert len([e for e in events if e[0]=='run' and e[1][0]=='scontrol'])==4


def test_mocked_scheduler_uses_exact_submitted_job_binding_under_reversed_directory_order(fixture,monkeypatch):
    ctx,plan,events,runner,reader=fixture;original=Path.glob
    def reversed_order(path,pattern):
        values=list(original(path,pattern))
        return iter(reversed(values)) if path==c.ROOT/'plans' and pattern=='*/plan.json' else iter(values)
    monkeypatch.setattr(Path,'glob',reversed_order)
    result=c.tick(ctx,runner=runner,native_reader=reader)
    assert result['new_intentions']==4
    assert len([e for e in events if e[0]=='run' and e[1][0]=='scontrol'])==4
    with pytest.raises(ValueError,match='Foreign mocked native job'):
        ctx['scheduler']('999999',ctx['area']/'foreign-native-read')


def test_postregistration_global_admission_failure_preserves_hold(fixture,monkeypatch):
    ctx,plan,events,runner,reader=fixture
    def admission(context,label,**kw):
        if label.startswith('registered-'):raise ValueError('combined capacity changed')
    monkeypatch.setattr(c,'admission',admission)
    with pytest.raises(ValueError,match='capacity changed'):c.tick(ctx,runner=runner,native_reader=reader)
    assert not [e for e in events if e[0]=='run' and e[1][0]=='scontrol']
    assert len(list((c.ROOT/'state').glob('*-submission.json')))==1


def test_release_timeout_stays_ambiguous_without_second_release(fixture,monkeypatch):
    ctx,plan,events,runner,reader=fixture
    original_glob=Path.glob
    def reversed_directory(path,pattern):
        values=original_glob(path,pattern)
        return iter(sorted(values,key=str,reverse=True)) if path==ctx['case'] else values
    monkeypatch.setattr(Path,'glob',reversed_directory)
    def timed(command,**kw):
        if command[0]=='scontrol':events.append(('release_attempt',command));raise subprocess.TimeoutExpired(command,45)
        return runner(command,**kw)
    with pytest.raises(subprocess.TimeoutExpired):c.tick(ctx,runner=timed,native_reader=reader)
    state=load_state(ctx)
    attempted=next(event[1][-1] for event in events if event[0]=='release_attempt')
    intent=next(name for name,job in state['jobs'].items() if job['job_id']==attempted)
    assert read_json(c.submission_journal(ctx,intent))['status']=='releasing'
    assert all(read_json(c.submission_journal(ctx,name))['status']=='registered_held_verified'
        for name in state['jobs'] if name!=intent)
    monkeypatch.setattr(c,'initialize',lambda context:(plan,state))
    with pytest.raises(ValueError,match='Ambiguous prior'):c.tick(ctx,runner=timed,native_reader=reader)
    assert len([e for e in events if e[0]=='release_attempt'])==1


def test_pending_storage_admits_no_intention_or_native_mutation(fixture,monkeypatch):
    ctx,plan,events,runner,reader=fixture
    monkeypatch.setattr(c,'storage_guard',lambda *args,**kwargs:(_ for _ in ()).throw(c.PendingStorage('study inode limit')))
    with pytest.raises(c.PendingStorage):c.tick(ctx,runner=runner,native_reader=reader)
    assert not [e for e in events if e[0]=='run'] and not (c.ROOT/'plans').exists()


def test_reservations_cover_all_selected_training_and_full_policy_parts_once(fixture):
    ctx,plan,*_=fixture;state,intents=dispatch.dispatch_tick(plan,dispatch.initial_state(plan),{},{})
    initial=c.storage_reservation(state);assert initial['files']==16000+4*(30+330)
    state['cells'][0]['evaluation_plan']={'frozen':'technical fixture'}
    once=c.storage_reservation(state);assert once['files']==initial['files']+630+330
    # Additional hypothetical wave charges330 maintenance, not a second600 parts.
    state['jobs']['fixture-wave']={'intent':{'kind':'evaluation','cell_index':0}}
    first=c.storage_reservation(state)
    state['jobs']['fixture-wave2']={'intent':{'kind':'evaluation','cell_index':0}}
    second=c.storage_reservation(state);assert second['files']-first['files']==330
    assert second['evaluation_policies']==1
    third=c.storage_reservation(state,extra_evaluations=[1])
    assert third['files']-second['files']==960


def test_stable_native_terminal_proof_excludes_late_rss_but_keeps_owner_and_cost():
    raw={'state':'COMPLETED','exit_code':'0:0','cpus':6,'alloc_tres':'cpu=6,gres/gpu:a100=1',
        'elapsed_seconds':120,'user':'pwa209','account':'def-ptewarie_gpu','max_rss':'2G'}
    a=c.normalized_native('123',raw);raw['max_rss']='3G'
    assert c.normalized_native('123',raw)==a and a['gpus']==1
    raw['user']='other'
    with pytest.raises(ValueError,match='Owned'):c.normalized_native('123',raw)


def test_finite_controller_sleeps_in_thirty_second_pieces_and_preserves_lease(fixture,monkeypatch,capsys):
    ctx,*_=fixture;clock=[1000.];ctx['deadline']=10000.;calls=[];leases=[]
    @contextmanager
    def lease(*args,**kw):leases.append(kw);yield {}
    monkeypatch.setattr(c,'resource_lease',lease)
    def tick(context):
        calls.append(clock[0]);return {'status':'computational_evaluations_complete_inference_pending' if len(calls)==3 else 'active'}
    monkeypatch.setattr(c,'tick',tick)
    sleeps=[]
    def sleep(n):sleeps.append(n);clock[0]+=n
    result=c.run(ctx,sleeper=sleep,clock=lambda:clock[0])
    assert len(calls)==3 and max(sleeps)<=30 and leases[0]['max_hours']==96
    assert result['status'].endswith('inference_pending')
    assert len(capsys.readouterr().out.splitlines())==3


def test_census_terminates_with_partial_evidence_before_deadline(fixture,monkeypatch):
    ctx,*_=fixture;clock=[1000.];ctx['deadline']=10000.;sleeps=[];children=[]
    class Child:
        returncode=None
        def __init__(self,*args,**kw):children.append(self);self.terminated=False
        def poll(self):return None
        def terminate(self):self.terminated=True
        def wait(self,timeout):self.returncode=-15;return -15
    def sleep(n):sleeps.append(n);clock[0]+=n
    with pytest.raises(c.PendingStorage,match='did not finish'):
        c.fresh_census(ctx,clock=lambda:clock[0],sleeper=sleep,popen=Child)
    assert children[0].terminated and max(sleeps)<=30 and clock[0]<ctx['deadline']


def test_census_with_insufficient_finalization_margin_never_starts(fixture):
    ctx,*_=fixture;ctx['deadline']=1200.
    def forbidden(*args,**kw):raise AssertionError('census must not start')
    with pytest.raises(c.PendingStorage,match='seventy-minute'):c.fresh_census(ctx,clock=lambda:1000.,popen=forbidden)


def test_fresh_census_is_reused_without_full_scan(fixture):
    ctx,*_=fixture
    record={'measured_unix':1000.,'captured_utc':datetime.fromtimestamp(1000,timezone.utc).isoformat()}
    atomic_json(c.ROOT/'state/storage-budget.json',record)
    def forbidden(*args,**kw):raise AssertionError('recent census should be reused')
    assert c.fresh_census(ctx,clock=lambda:1100.,popen=forbidden)==record


def test_committed_snapshot_keeps_exact_raw_receipt_sha_and_rejects_changed_file(fixture,monkeypatch):
    ctx,*_=fixture;case=ctx['case']/'condition-case';case.mkdir()
    value={'condition_index':0,'status':'complete'};path=case/'condition-0000.json';atomic_json(path,value)
    sha=digest(path)
    monkeypatch.setattr(c.evaluation,'_committed',lambda *args:{0:dict(value,_receipt_name=path.name,_receipt_sha256=sha)})
    assert c.condition_snapshot(case,{})==[{'sha256':sha,'value':value}]
    path.write_text('{}')
    with pytest.raises(ValueError,match='receipt changed'):c.condition_snapshot(case,{})


def test_invalid_or_multiple_scheduler_records_are_refused():
    for text in ['JobId=1 JobId=2','JobId=1\nJobId=2\n']:
        with pytest.raises(ValueError):c.fields(subprocess.CompletedProcess([],0,text,''))
    assert c.seconds('4-00:00:00')==96*3600
    with pytest.raises(ValueError):c.seconds('UNLIMITED')


@pytest.mark.parametrize('tres',['gres/gpu=1,gres/gpu:a100=2','gres/gpu=2,gres/gpu=1',
    'gres/gpu=1,gres/gpu:a100=-1','gres/gpu=1,gres/gpu:a100=bad','gres/gpu=1,gres/gpu:a100',
    'gres/gpu=1,gres/gpu:=1','gres/gpu=+1'])
def test_contradictory_duplicate_or_malformed_gpu_evidence_is_never_selected(tres):
    with pytest.raises(ValueError):c.gpu_count(tres)


def test_consistent_generic_and_typed_gpu_evidence():
    assert c.gpu_count('cpu=6,gres/gpu=1,gres/gpu:a100=1')==1
    assert c.gpu_count('cpu=6,gres/gpu:a100=1')==1
    assert c.gpu_count('cpu=1')==0


@pytest.mark.parametrize('preimport',[False,True])
def test_actual_pinned_guard_is_installed_before_campaign_function_binding(tmp_path,preimport):
    controller=Path(c.__file__).resolve()
    guard=(c.GUARD if c.GUARD.is_file() else Path(__file__).with_name('native-accounting-read-guard-20261021.py')).resolve()
    source='''import importlib.util,pathlib,sys
if sys.argv[4]=='True':import horsehair.campaign
spec=importlib.util.spec_from_file_location('private_controller_guard_test',sys.argv[1])
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
m.GUARD=pathlib.Path(sys.argv[2])
try:
 campaign,reader=m.guarded_modules(pathlib.Path(sys.argv[3]))
except ValueError as error:
 assert sys.argv[4]=='True' and 'before the verified guard' in str(error)
else:
 assert sys.argv[4]=='False' and campaign._repair_native_states is reader
 print('guard-binding-ok')
'''
    result=subprocess.run([sys.executable,'-c',source,str(controller),str(guard),str(tmp_path/'evidence'),str(preimport)],
        text=True,capture_output=True,timeout=30)
    assert result.returncode==0,result.stderr
    if not preimport:assert 'guard-binding-ok' in result.stdout


@pytest.fixture
def gate_fixture(fixture,monkeypatch):
    ctx,plan,*_=fixture;case=ctx['case']/'canary';case.mkdir()
    frame=pd.DataFrame({'numerical_status':['available']*1664,'calibration_numerical_status':['available']*1664,
        'success':[1.]*1664,'redirection':[0.]*1664,'motor_accuracy':[1.]*1664,'clean_policy_kl':[.1]*1664})
    artifact=case/'assembled.parquet';frame.to_parquet(artifact,index=False)
    atomic_json(case/'plan.json',{'fixture':'frozen-canary'});atomic_json(case/'plan-binding.json',{'fixture':'binding'})
    binding={'plan_sha256':digest(case/'plan.json'),'plan_binding_sha256':digest(case/'plan-binding.json')}
    d=ctx['descriptor'];d.update(framework_release=str(ctx['framework']),worker_sha256='a'*64,
        qualification={'path':'fixture','sha256':'b'*64},resource_policy={'path':'fixture','sha256':'d'*64},
        frozen_protocol_sha256='e'*64,canary_request={'path':'request','sha256':'f'*64},canary_plan=binding)
    gate=d['precision_gate'];gate.update(descriptor_path='prior-descriptor',descriptor_sha256='0'*64,
        diagnostics_receipt_path=str(case/'diagnostics.json'),assembly_receipt_path=str(case/'assembly.json'))
    reports=[{'dtype':dtype,'condition_index':i,'status':'available','rows':32,'unavailable_rows':0}
        for dtype in ('torch.float32','torch.float64') for i in range(52)]
    reports[0]={'dtype':'torch.float32','condition_index':0,'status':'unavailable','reason':'original numeric failure fixture'}
    diagnostics={'reports':reports,'plan_sha256':binding['plan_sha256']}
    summary={'status':'complete_precision_canary','canary':True,'conditions':52,'trial_rows':1664,
        'plan_sha256':binding['plan_sha256'],'artifact_name':artifact.name,'trial_file_sha256':digest(artifact)}
    atomic_json(case/'diagnostics.json',{'result':diagnostics});atomic_json(case/'assembly.json',{'result':summary})
    def frozen(path,sha,namespace=None):
        if path=='prior-descriptor':return copy.deepcopy(d)
        assert path=='request';return {'case':str(case),'required_conditions':52}
    monkeypatch.setattr(c,'frozen_file',frozen)
    def worker(context,job,mode,index=None,descriptor_sha=None):
        return (case/'diagnostics.json',{'result':diagnostics}) if mode=='precision-diagnostics' else (case/'assembly.json',{'result':summary})
    monkeypatch.setattr(c,'worker_receipt',worker)
    monkeypatch.setattr(c.evaluation,'_load_plan',lambda *args:{'canary':True,'conditions':list(range(52))})
    monkeypatch.setattr(c.evaluation,'assemble',lambda *args,**kwargs:summary)
    native={job:{'job_id':job,'state':'COMPLETED','exit_code':'0:0','elapsed_seconds':60,'cpus':4,'gpus':0} for job in ('1','2')}
    return ctx,native,diagnostics,summary,artifact


def test_precision_gate_requires_actual_f64_rows_and_f32_failure_reproduction(gate_fixture):
    ctx,native,*_=gate_fixture
    assert REAL_PRECISION_GATE(ctx,native) is True
    assert read_json(ctx['case']/'precision-gate.json')['status']=='qualified_precision_gate'
    assert REAL_PRECISION_GATE(ctx,native) is True


@pytest.mark.parametrize('failure',['f32_not_reproduced','f64_unavailable','wrong_rows','native_failed','nonfinite_actual_trial'])
def test_precision_gate_does_not_clear_on_partial_or_invalid_evidence(gate_fixture,failure):
    ctx,native,diagnostics,summary,artifact=gate_fixture
    if failure=='f32_not_reproduced':diagnostics['reports'][0].update(status='available',rows=32,unavailable_rows=0)
    if failure=='f64_unavailable':diagnostics['reports'][-1]['status']='unavailable'
    if failure=='wrong_rows':summary['trial_rows']=1663
    if failure=='native_failed':native['2']['state']='FAILED'
    if failure=='nonfinite_actual_trial':
        frame=pd.read_parquet(artifact);frame.loc[0,'clean_policy_kl']=np.nan;frame.to_parquet(artifact,index=False)
        summary['trial_file_sha256']=digest(artifact)
    with pytest.raises(ValueError):REAL_PRECISION_GATE(ctx,native)
    assert not (ctx['case']/'precision-gate.json').exists()


def test_pending_native_precision_gate_leaves_training_independent(gate_fixture):
    ctx,native,*_=gate_fixture;native['2']['state']='PENDING'
    assert REAL_PRECISION_GATE(ctx,native) is False


def test_storage_guard_counts_all_pending_outputs_against_actual_census(fixture,monkeypatch):
    ctx,plan,*_=fixture;state,_=dispatch.dispatch_tick(plan,dispatch.initial_state(plan),{},{})
    detail_path=c.ROOT/'state/storage-entry-sizes.json';quota_path=c.ROOT/'state/quota-1.txt';quota_path.write_text('quota fixture')
    training=agents._directory(c.ROOT,ctx['config']['agents'])/'final'/'foraging_rnn_1000'
    items=[{'relative_path':str(ctx['case'].relative_to(c.ROOT))+'/old.json','bytes':1000},
        {'relative_path':str(training.relative_to(c.ROOT))+'/checkpoint.pt','bytes':2000},
        {'relative_path':'unrelated.bin','bytes':3000}]
    detail={'status':'measured','count':3,'bytes':6000,'items':items};atomic_json(detail_path,detail)
    record={'status':'measured','root':str(c.ROOT),'study_files':3,'study_bytes':6000,
        'measured_unix':c.time.time(),'captured_utc':datetime.now(timezone.utc).isoformat(),
        'native_source_paths':[str(quota_path)],'native_source_hashes':{str(quota_path):digest(quota_path)},
        'exact_entry_measurements':{'receipt_path':str(detail_path),'receipt_sha256':digest(detail_path)}}
    atomic_json(c.ROOT/'state/storage-budget.json',record)
    headroom={'personal_available_bytes':10_000_000_000_000,'personal_available_files':1_000_000}
    calls=[]
    def quota(*args,**kw):
        kw['evidence_dir'].mkdir(parents=True,exist_ok=False)
        calls.append(kw);return headroom
    monkeypatch.setattr(c.runpy,'run_path',lambda *args,**kwargs:{'require_storage_headroom_with_wait':quota})
    original_read=c.read_json
    def no_detail_reopen(path):
        if Path(path)==detail_path:raise AssertionError('Parsed census detail must use its exact hashed byte buffer')
        return original_read(path)
    monkeypatch.setattr(c,'read_json',no_detail_reopen)
    result=REAL_STORAGE_GUARD(ctx,state)
    assert result['captured_campaign_files']==1
    assert calls[0]['required_files']==16000+4*(30+330)-1
    assert calls[0]['reserve_bytes']==2_000_000_000_000
    assert result['future_study_files']==3+16000+4*(30+330)-1
    assert REAL_STORAGE_GUARD(ctx,state)['future_study_files']==result['future_study_files']
    assert calls[0]['evidence_dir']!=calls[1]['evidence_dir']
    ctx['config']['resources']['scratch_budget_files']=100
    with pytest.raises(c.PendingStorage):REAL_STORAGE_GUARD(ctx,state)
    assert read_json(ctx['case']/'storage-reservation.json')['status']=='resource_pending_storage'
    def blocked(*args,**kwargs):raise ReceiptLockTimeout(c.ROOT/'state/storage-headroom.mutex',30,None)
    monkeypatch.setattr(c.runpy,'run_path',lambda *args,**kwargs:{'require_storage_headroom_with_wait':blocked})
    with pytest.raises(c.PendingStorage,match='Live process holds'):REAL_STORAGE_GUARD(ctx,state)
    def foreign(*args,**kwargs):raise ReceiptLockTimeout(c.ROOT/'state/another-live-guard.mutex',30,None)
    monkeypatch.setattr(c.runpy,'run_path',lambda *args,**kwargs:{'require_storage_headroom_with_wait':foreign})
    with pytest.raises(ReceiptLockTimeout):REAL_STORAGE_GUARD(ctx,state)


def test_completed_outputs_free_only_after_positive_native_end_precedes_census(fixture):
    ctx,plan,*_=fixture;state,_=dispatch.dispatch_tick(plan,dispatch.initial_state(plan),{},{})
    before=c.storage_reservation(state,census_start=2000.)
    first=next(iter(state['jobs'].values()))
    first['native_proof']={'state':'COMPLETED','end':datetime.fromtimestamp(1000.,timezone.utc).isoformat()}
    after=c.storage_reservation(state,census_start=2000.)
    assert before['files']-after['files']==360
    assert c.storage_reservation(state,census_start=500.)['files']==before['files']
    first['native_proof']['end']='Unknown'
    assert c.storage_reservation(state,census_start=2000.)['files']==before['files']


def test_controller_only_descriptor_replacement_keeps_historical_worker_receipts(fixture):
    ctx,*_=fixture;path=Path(ctx['descriptor']['case'])/'native-123'/'receipt.json'
    value={'status':'complete_native_work','mode':'agent-train','index':0,'native_job_id':'123',
        'descriptor_sha256':ctx['worker_descriptor_sha256'],'source_sha256':c.FRAMEWORK_SHA,
        'full_config_sha256':c.BASE_CONFIG_SHA256,'resource_policy':ctx['policy'].receipt(),
        'scientific_result':False,'admission_authority':False}
    atomic_json(path,value)
    assert c.worker_receipt(ctx,'123','agent-train',0)[1]==value
    ctx['descriptor_sha256']='4'*64
    ctx['descriptor_path']=c.ROOT/'maintenance/repaired-controller-v4.json'
    assert c.worker_receipt(ctx,'123','agent-train',0)[1]==value
    ctx['worker_descriptor_sha256']='8'*64
    with pytest.raises(ValueError,match='receipt binding differs'):c.worker_receipt(ctx,'123','agent-train',0)


def test_stable_worker_descriptor_rejects_mutated_execution_bindings(fixture):
    ctx,*_=fixture;descriptor=ctx['descriptor']
    descriptor.update(qualification={'sha256':'a'*64},resource_policy={'sha256':'b'*64},
        frozen_protocol_sha256='c'*64,gpu_qualification={'sha256':'d'*64},
        agent_campaign_case=str(ctx['case']),canary_request={'sha256':'e'*64},canary_plan={'sha256':'f'*64})
    path=ctx['worker_descriptor_path'];atomic_json(path,descriptor)
    descriptor['worker_descriptor']={'path':str(path),'sha256':digest(path)}
    assert c.worker_binding(descriptor)[1]==digest(path)
    descriptor['controller_sha256']='1'*64
    assert c.worker_binding(descriptor)[1]==digest(path)
    descriptor['gpu_qualification']['sha256']='2'*64
    with pytest.raises(ValueError,match='execution bindings'):c.worker_binding(descriptor)


def test_only_failed0000_native_namespace_and_journal_change(fixture):
    ctx,*_=fixture
    assert c.native_namespace(ctx,'training-0000')=='af-f2-r1-training-0000'
    assert c.submission_journal(ctx,'training-0000').name=='submission-r1-training-0000.json'
    assert c.native_namespace(ctx,'training-0001')=='af-f2-training-0001'
    assert c.submission_journal(ctx,'training-0001').name=='submission-training-0001.json'
    ctx['registry_overlay']['recovery_value']['native_namespace_overrides']['training-0001']='forbidden'
    with pytest.raises(ValueError):c.native_namespace(ctx,'training-0000')


def test_recovery_keeps_old_failed_native_namespace_and_uses_source_bound_worker_shim(fixture):
    ctx,plan,*_=fixture
    old=c.ROOT/'plans/af-f2-training-0000';old.mkdir(parents=True);(old/'unchanged.txt').write_text('failed native history')
    journal=ctx['case']/'submission-training-0000.json';journal.write_text('original failed attempt journal')
    state,intents=dispatch.dispatch_tick(plan,dispatch.initial_state(plan),{},{})
    path,ledger,task=c.native_plan(ctx,intents[0])
    assert path.parent.name=='af-f2-r1-training-0000'
    assert (old/'unchanged.txt').read_text()=='failed native history' and journal.read_text()=='original failed attempt journal'
    import shlex
    command=shlex.split(task['command'])
    assert command[1]==ctx['registry_overlay']['worker_shim']['path']
    assert command[command.index('--descriptor')+1]==str(ctx['worker_descriptor_path'])
    assert command[command.index('--descriptor-sha256')+1]==ctx['worker_descriptor_sha256']
    assert command[command.index('--registry-read-guard-sha256')+1]==ctx['registry_overlay']['guard']['sha256']


def test_new_production_controller_refuses_missing_registry_amendment():
    with pytest.raises(ValueError,match='registry overlay'):c.registry_binding({})


def test_all_real_releases_follow_complete_batch_registration_and_kernel_mutex_exit(fixture,monkeypatch):
    ctx,plan,events,runner,reader=fixture
    @contextmanager
    def actual_registration(context,role):
        with c.exclusive_file_lock(c.ROOT/'state/actual-registration.mutex',timeout=.2):
            context['fixture_registration_locked']=True
            try:yield
            finally:context['fixture_registration_locked']=False
    monkeypatch.setattr(c,'registration_guard',actual_registration)
    starts=[]
    def released_worker(command,**kwargs):
        if command[:2]==['scontrol','release']:
            # A real independent lock acquisition would fail if the controller
            # still held the registration transaction during child release.
            with c.exclusive_file_lock(c.ROOT/'state/actual-registration.mutex',timeout=.2):starts.append(command[-1])
        return runner(command,**kwargs)
    c.tick(ctx,runner=released_worker,native_reader=reader)
    assert len(starts)==4
    first_release=next(i for i,e in enumerate(events) if e[0]=='run' and e[1][0]=='scontrol')
    assert len([e for e in events[:first_release] if e[0]=='run' and e[1][0]=='sbatch'])==4
    assert len([e for e in events[:first_release] if e[0]=='admission' and e[1].startswith('registered-')])==4
    assert events[0]==('census-outside-lock',False)


def test_later_batch_registration_failure_leaves_all_earlier_jobs_held(fixture,monkeypatch):
    ctx,plan,events,runner,reader=fixture;calls=[]
    def later_refusal(context,label,**kw):
        calls.append(label)
        if label=='before-training-0002':raise ValueError('later capacity changed')
    monkeypatch.setattr(c,'admission',later_refusal)
    with pytest.raises(ValueError):c.tick(ctx,runner=runner,native_reader=reader)
    assert len([e for e in events if e[0]=='run' and e[1][0]=='sbatch'])==2
    assert not [e for e in events if e[0]=='run' and e[1][0]=='scontrol']
    assert all(read_json(p)['status']=='registered_held_verified' for p in ctx['case'].glob('submission-*.json'))


def test_changed_prepared_census_is_pending_and_never_scanned_inside_transaction(fixture):
    ctx,*_=fixture;path=c.ROOT/'state/storage-budget.json'
    record={'measured_unix':c.time.time(),'captured_utc':datetime.now(timezone.utc).isoformat()};atomic_json(path,record)
    ctx['prepared_census_sha256']=digest(path)
    assert c.existing_fresh_census(ctx)==record
    record['changed']=True;atomic_json(path,record)
    with pytest.raises(c.PendingStorage,match='outside-lock'):c.existing_fresh_census(ctx)


def test_registration_pending_status_waits_without_controller_failure_or_new_work(fixture,monkeypatch,capsys):
    ctx,*_=fixture;clock=[1000.];ctx['deadline']=10000.;calls=[]
    @contextmanager
    def lease(*args,**kw):yield {}
    monkeypatch.setattr(c,'resource_lease',lease)
    def tick(context):
        calls.append(1)
        if len(calls)==1:raise c.PendingRegistration('verified exact registration wait budget')
        return {'status':'computational_evaluations_complete_inference_pending'}
    monkeypatch.setattr(c,'tick',tick)
    result=c.run(ctx,sleeper=lambda seconds:clock.__setitem__(0,clock[0]+seconds),clock=lambda:clock[0])
    assert len(calls)==2 and result['status'].endswith('inference_pending')
    assert not (ctx['case']/'controller-failure.json').exists()
    assert 'resource_pending_registration' in capsys.readouterr().out


def test_startup_retries_only_lock_acquisition_before_one_admission(fixture,monkeypatch,capsys):
    ctx,*_=fixture;clock=[1000.];ctx['deadline']=10000.;attempts=[];admissions=[]
    @contextmanager
    def busy(context,role):
        attempts.append(role)
        if len(attempts)<3:raise c.PendingRegistration('busy exact guard')
        yield
    monkeypatch.setattr(c,'registration_guard',busy)
    monkeypatch.setattr(c,'admission',lambda *args,**kw:admissions.append('once'))
    c.startup_admission(ctx,clock=lambda:clock[0],sleeper=lambda value:clock.__setitem__(0,clock[0]+value))
    assert attempts==['controller-start']*3 and admissions==['once']
    assert capsys.readouterr().out.count('resource_pending_registration')==2


def test_startup_never_retries_an_admission_failure(fixture,monkeypatch):
    ctx,*_=fixture;ctx['deadline']=10000.;calls=[]
    def failure(*args,**kw):calls.append('once');raise ValueError('real original admission failure')
    monkeypatch.setattr(c,'admission',failure)
    with pytest.raises(ValueError):c.startup_admission(ctx,clock=lambda:1000.,sleeper=lambda value:None)
    assert calls==['once']


@pytest.fixture
def private_test_proof(tmp_path,monkeypatch):
    root=tmp_path/'study';support=root/'maintenance/private-q';support.mkdir(parents=True)
    source=support/'test_overlay.py';source.write_text('def test_guard(): pass\n')
    junit=support/'actual.xml'
    junit.write_text('<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0">'
        '<testcase classname="native.test_overlay" name="test_guard"/></testsuite></testsuites>')
    monkeypatch.setattr(c,'ROOT',root)
    monkeypatch.setattr(c,'OPERATIONAL_TEST_SOURCES',{'test_overlay.py':digest(source)})
    monkeypatch.setattr(c,'OPERATIONAL_TEST_COUNT',1)
    monkeypatch.setattr(c,'OPERATIONAL_TEST_ROSTER_SHA256',canonical_hash([('test_overlay','test_guard')]))
    return {'test_counts':{'tests':1,'failures':0,'errors':0,'skipped':0},
        'test_sources':{'test_overlay.py':{'path':str(source),'sha256':digest(source)}},
        'junit':{'path':str(junit),'sha256':digest(junit)}}


def test_private_qualification_checks_actual_full_roster_and_test_file_bytes(private_test_proof):
    c.verify_operational_tests(private_test_proof)
    path=Path(private_test_proof['test_sources']['test_overlay.py']['path'])
    path.write_text('def test_another(): pass\n')
    with pytest.raises(ValueError,match='test source changed'):c.verify_operational_tests(private_test_proof)


@pytest.mark.parametrize('failure',['partial_count','missing_source','foreign_source_sha','different_testcase','skipped','duplicate','dtd'])
def test_private_qualification_rejects_partial_or_attribute_only_canaries(private_test_proof,failure):
    q=private_test_proof;path=Path(q['junit']['path'])
    if failure=='partial_count':q['test_counts']['tests']=0
    if failure=='missing_source':q['test_sources']={}
    if failure=='foreign_source_sha':q['test_sources']['test_overlay.py']['sha256']='a'*64
    if failure=='different_testcase':path.write_text(path.read_text().replace('test_guard','test_other'))
    if failure=='skipped':path.write_text(path.read_text().replace('/></testsuite>','><skipped/></testcase></testsuite>'))
    if failure=='duplicate':path.write_text(path.read_text().replace('</testsuite>','<testcase classname="test_overlay" name="test_guard"/></testsuite>'))
    if failure=='dtd':path.write_text('<!DOCTYPE testsuites [<!ENTITY x "y">]>'+path.read_text())
    q['junit']['sha256']=digest(path)
    with pytest.raises(ValueError):c.verify_operational_tests(q)


def test_minimal_isolated_bundle_cannot_depend_on_unstaged_old_helpers(tmp_path):
    stage=tmp_path/'isolated-stage';stage.mkdir()
    controller=Path(c.__file__).resolve();test=Path(__file__).resolve()
    guard=(c.GUARD if c.GUARD.is_file() else test.with_name('native-accounting-read-guard-20261021.py')).resolve()
    assert digest(guard)==c.GUARD_SHA
    for path in (controller,test,guard):shutil.copyfile(path,stage/path.name)
    assert not (stage/'run-af-agent-controller-registry-v2-20261021.py').exists()
    assert not (stage/'run-af-agent-controller-registry-v3-20261021.py').exists()
    environment={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1'}
    environment.pop('PYTEST_ADDOPTS',None);environment.pop('PYTEST_PLUGINS',None)
    command=[sys.executable,'-m','pytest',str(stage/test.name),'-q','-p','no:cacheprovider',
        '-k','test_actual_pinned_guard_is_installed_before_campaign_function_binding or test_release_timeout_stays_ambiguous_without_second_release',
        '--basetemp',str(tmp_path/'isolated-fixtures')]
    result=subprocess.run(command,cwd=stage,text=True,capture_output=True,timeout=120,env=environment)
    assert result.returncode==0,result.stdout+result.stderr
    assert '3 passed' in result.stdout


@pytest.fixture
def prestart_retry(fixture):
    ctx,*_=fixture
    old_path=c.ROOT/'maintenance/original-training0000-recovery.json'
    atomic_json(old_path,{'kind':'explicit-prestart-training0000-operational-recovery-v1'})
    old={'path':str(old_path),'sha256':digest(old_path)}
    ctx['descriptor']['registry_overlay']={'recovery':old}
    ctx['registry_overlay']['recovery']=copy.deepcopy(old)
    case=c.ROOT/'maintenance/study-af-prestart-three-recovery-v1-20261022';case.mkdir()
    path=case/'prestart-retry-amendment.json'
    value={'schema_version':1,'kind':'explicit-prestart-three-operational-recovery-v1',
        'status':'validated_target_only_state_derivative_not_activated','case':str(case),
        'original_revision':405,'new_revision':406,'original_state_sha256':c.PRESTART_RETRY_ORIGINAL_STATE_SHA256,
        'new_state_sha256':'a'*64,'campaign_plan_sha256':c.PRESTART_RETRY_CAMPAIGN_PLAN_SHA256,
        'source_sha256':c.FRAMEWORK_SHA,'config_sha256':c.BASE_CONFIG_SHA256,
        'dispatcher_sha256':c.PRESTART_RETRY_DISPATCHER_SHA256,'targets':copy.deepcopy(c.PRESTART_RETRY_TARGETS),
        'native_namespace_overrides':{t:'af-f2-r2-'+t for t in c.PRESTART_RETRY_TARGETS},
        'submission_journal_overrides':{t:'submission-r2-'+t+'.json' for t in c.PRESTART_RETRY_TARGETS},
        'old_recovery':old,'activation_producer_job_id':'99001','scientific_result':False,'admission_authority':False}
    bind_retry(ctx,value,path)
    atomic_json(c.ROOT/'state/resource-budget.json',{'native_jobs':{'99001':{
        'state':'COMPLETED','exit_code':'0:0','basis':'sacct final allocated wall time',
        'elapsed_seconds':12.,'cpus':1}}})
    ctx['registry_overlay']['prestart_retry_value']=copy.deepcopy(value)
    ctx['registry_overlay']['prestart_retry_binding']=copy.deepcopy(ctx['descriptor']['prestart_retry_amendment'])
    return ctx,value,path


def bind_retry(ctx,value,path):
    value['amendment_sha256']=canonical_hash({k:v for k,v in value.items() if k!='amendment_sha256'})
    atomic_json(path,value)
    ctx['descriptor']['prestart_retry_amendment']={'path':str(path),'sha256':digest(path),'job_id':'99001'}


def test_prestart_retry_is_optional_and_does_not_change_unbound_v5_mappings(fixture):
    ctx,*_=fixture
    assert c.retry_binding(ctx['descriptor']) is None
    for target in c.PRESTART_RETRY_TARGETS:
        assert c.native_namespace(ctx,target)=='af-f2-'+target
        assert c.submission_journal(ctx,target)==ctx['case']/('submission-'+target+'.json')
    assert c.native_namespace(ctx,'training-0000')=='af-f2-r1-training-0000'
    assert c.submission_journal(ctx,'training-0000').name=='submission-r1-training-0000.json'


def test_exact_prestart_retry_uses_positive_native_helper_proof_and_preserves_old_maps(prestart_retry,monkeypatch):
    ctx,value,path=prestart_retry
    def no_native_mutation(*args,**kwargs):raise AssertionError('Controller binding must only read persisted proof')
    monkeypatch.setattr(c.subprocess,'run',no_native_mutation)
    monkeypatch.setattr(c.subprocess,'check_output',no_native_mutation)
    original=copy.deepcopy(ctx['registry_overlay']['recovery_value'])
    assert c.retry_binding(ctx['descriptor'])==value
    for target in c.PRESTART_RETRY_TARGETS:
        assert c.native_namespace(ctx,target)=='af-f2-r2-'+target
        assert c.submission_journal(ctx,target)==ctx['case']/('submission-r2-'+target+'.json')
    assert c.native_namespace(ctx,'training-0000')=='af-f2-r1-training-0000'
    assert c.submission_journal(ctx,'training-0000').name=='submission-r1-training-0000.json'
    assert c.native_namespace(ctx,'training-0078')=='af-f2-training-0078'
    assert c.submission_journal(ctx,'training-0078').name=='submission-training-0078.json'
    assert ctx['registry_overlay']['recovery_value']==original


@pytest.mark.parametrize('field,value',[
    ('kind','other'),('schema_version',True),('status','activated'),('case','/different/case'),
    ('original_revision',404),('original_revision',True),('new_revision',407),('new_revision',True),
    ('original_state_sha256','b'*64),('new_state_sha256','not-a-hash'),
    ('new_state_sha256',c.PRESTART_RETRY_ORIGINAL_STATE_SHA256),('campaign_plan_sha256','b'*64),
    ('source_sha256','b'*64),('config_sha256','b'*64),('dispatcher_sha256','b'*64),
    ('scientific_result',True),('admission_authority',True),('scientific_result',0),('admission_authority',0),
    ('activation_producer_job_id','99002'),('activation_producer_job_id',99001),
    ('old_recovery',{'path':'/different/old.json','sha256':'b'*64}),
])
def test_prestart_retry_rejects_resealed_foreign_or_scientifically_cleared_amendment(prestart_retry,field,value):
    ctx,amendment,path=prestart_retry
    amendment[field]=value;bind_retry(ctx,amendment,path)
    with pytest.raises(ValueError):c.retry_binding(ctx['descriptor'])


@pytest.mark.parametrize('field,change',[
    ('targets','missing'),('targets','extra'),('targets','wrong'),
    ('native_namespace_overrides','missing'),('native_namespace_overrides','extra'),('native_namespace_overrides','wrong'),
    ('submission_journal_overrides','missing'),('submission_journal_overrides','extra'),('submission_journal_overrides','wrong'),
])
def test_prestart_retry_rejects_any_expanded_or_changed_target_mapping(prestart_retry,field,change):
    ctx,value,path=prestart_retry
    if change=='missing':value[field].pop('training-0076')
    if change=='extra':value[field]['training-0000']='forbidden'
    if change=='wrong':value[field]['training-0076']='foreign'
    bind_retry(ctx,value,path)
    with pytest.raises(ValueError,match='exact three'):c.retry_binding(ctx['descriptor'])


@pytest.mark.parametrize('change',['raw_bytes','seal','binding_extra','binding_missing','binding_none','binding_job','binding_sha'])
def test_prestart_retry_rejects_unbound_or_changed_bytes_and_binding(prestart_retry,change):
    ctx,value,path=prestart_retry;binding=ctx['descriptor']['prestart_retry_amendment']
    if change=='raw_bytes':path.write_text(path.read_text()+'\n')
    if change=='seal':value['amendment_sha256']='b'*64;atomic_json(path,value);binding['sha256']=digest(path)
    if change=='binding_extra':binding['admission_authority']=False
    if change=='binding_missing':binding.pop('job_id')
    if change=='binding_none':ctx['descriptor']['prestart_retry_amendment']=None
    if change=='binding_job':binding['job_id']='099001'
    if change=='binding_sha':binding['sha256']='b'*64
    with pytest.raises(ValueError):c.retry_binding(ctx['descriptor'])


def test_prestart_retry_refuses_amendment_outside_its_exact_recovery_case(prestart_retry):
    ctx,value,path=prestart_retry
    elsewhere=c.ROOT/'maintenance/foreign-amendment.json';atomic_json(elsewhere,value)
    ctx['descriptor']['prestart_retry_amendment'].update(path=str(elsewhere),sha256=digest(elsewhere))
    with pytest.raises(ValueError,match='ordinary path'):c.retry_binding(ctx['descriptor'])


@pytest.mark.parametrize('native',[
    None,{}, {'state':'RUNNING','exit_code':'0:0','basis':'sacct final allocated wall time','elapsed_seconds':12.,'cpus':1},
    {'state':'COMPLETED','exit_code':'1:0','basis':'sacct final allocated wall time','elapsed_seconds':12.,'cpus':1},
    {'state':'COMPLETED','exit_code':'0:0','basis':'sacct final allocated wall time','elapsed_seconds':0.,'cpus':1},
    {'state':'COMPLETED','exit_code':'0:0','basis':'sacct final allocated wall time','elapsed_seconds':12.,'cpus':0},
    {'state':'COMPLETED','exit_code':'0:0','basis':'flag-only','elapsed_seconds':12.,'cpus':1},
])
def test_prestart_retry_refuses_absent_failed_or_nonpositive_native_producer(prestart_retry,native):
    ctx,value,path=prestart_retry
    atomic_json(c.ROOT/'state/resource-budget.json',{'native_jobs':{} if native is None else {'99001':native}})
    with pytest.raises(ValueError):c.retry_binding(ctx['descriptor'])


@pytest.mark.parametrize('direction',['no_descriptor','no_validated_value','no_cached_locator','changed_cached_locator','changed_validated_value'])
def test_prestart_retry_namespace_and_journal_refuse_unbound_context_reference(prestart_retry,direction):
    ctx,value,path=prestart_retry
    if direction=='no_descriptor':ctx['descriptor'].pop('prestart_retry_amendment')
    if direction=='no_validated_value':ctx['registry_overlay'].pop('prestart_retry_value')
    if direction=='no_cached_locator':ctx['registry_overlay'].pop('prestart_retry_binding')
    if direction=='changed_cached_locator':ctx['registry_overlay']['prestart_retry_binding']['sha256']='f'*64
    if direction=='changed_validated_value':ctx['registry_overlay']['prestart_retry_value']['targets']['training-0076']='other'
    with pytest.raises(ValueError):c.native_namespace(ctx,'training-0076')
    with pytest.raises(ValueError):c.submission_journal(ctx,'training-0076')


def test_prestart_retry_retains_existing_failed_native_plans_and_journals(prestart_retry):
    ctx,value,path=prestart_retry
    old_paths=[]
    for target in c.PRESTART_RETRY_TARGETS:
        old_plan=c.ROOT/'plans'/('af-f2-'+target);old_plan.mkdir(parents=True)
        old_file=old_plan/'original.txt';old_file.write_text('failed native history '+target);old_paths.append(old_file)
        old_journal=ctx['case']/('submission-'+target+'.json');old_journal.write_text('original journal '+target);old_paths.append(old_journal)
    before={str(p):digest(p) for p in old_paths}
    target='training-0076'
    intent={'intent_id':target,'kind':'training','cell_index':76,'resources':{'cpus':6,'gpus':1,'hours':12},
        'campaign_plan_sha256':c.PRESTART_RETRY_CAMPAIGN_PLAN_SHA256,'intent_sha256':'d'*64}
    plan_path,ledger,task=c.native_plan(ctx,intent)
    assert plan_path.parent.name=='af-f2-r2-training-0076'
    assert ledger.name=='af-f2-r2-training-0076-submission.json'
    assert c.submission_journal(ctx,target).name=='submission-r2-training-0076.json'
    assert {str(p):digest(p) for p in old_paths}==before
    assert task['resources']=={'cpus':6,'gpu':True,'hours':12,'memory':'64G'}


def test_repeated_retry_namespace_and_journal_lookups_use_no_filesystem_or_native_proof(prestart_retry,monkeypatch):
    ctx,value,path=prestart_retry
    def unexpected(*args,**kwargs):raise AssertionError('Cached retry lookup repeated filesystem/native validation')
    monkeypatch.setattr(c,'retry_binding',unexpected)
    monkeypatch.setattr(c,'frozen_file',unexpected)
    monkeypatch.setattr(c.slurm,'completed_native_dependency',unexpected)
    for _ in range(100):
        for target in c.PRESTART_RETRY_TARGETS:
            assert c.native_namespace(ctx,target)=='af-f2-r2-'+target
            assert c.submission_journal(ctx,target).name=='submission-r2-'+target+'.json'


def test_source_guard_revalidates_actual_retry_file_before_each_wave(prestart_retry,monkeypatch):
    ctx,value,path=prestart_retry;descriptor=ctx['descriptor']
    protocol=ctx['protocol_path'];atomic_json(protocol,ctx['protocol'])
    descriptor.update(controller_sha256=digest(Path(c.__file__)),worker_sha256=digest(Path(descriptor['worker_path'])),
        frozen_protocol_sha256=digest(protocol))
    for field in ('qualification','gpu_qualification'):
        proof=c.ROOT/'maintenance'/(field+'.json');atomic_json(proof,{})
        descriptor[field]={'path':str(proof),'sha256':digest(proof)}
    atomic_json(ctx['descriptor_path'],descriptor);ctx['descriptor_sha256']=digest(ctx['descriptor_path'])
    ctx['worker_descriptor']={}
    original={k:v for k,v in ctx['registry_overlay'].items() if k not in {'prestart_retry_value','prestart_retry_binding'}}
    monkeypatch.setattr(c,'registry_binding',lambda d:copy.deepcopy(original))
    monkeypatch.setattr(c,'source_identity',lambda path:{'sha256':c.PRIMARY_SCIENTIFIC_SOURCE_SHA256 if path==c.PRIMARY else c.FRAMEWORK_SHA})
    monkeypatch.setattr(c,'worker_binding',lambda d:(ctx['worker_descriptor_path'],ctx['worker_descriptor_sha256'],{}))
    REAL_SOURCE_GUARD(ctx)
    path.write_text(path.read_text()+'\n')
    with pytest.raises(ValueError,match='file SHA mismatch'):REAL_SOURCE_GUARD(ctx)


def test_unbound_startup_does_not_refresh_accounting_or_take_extra_lock(fixture,monkeypatch):
    ctx,*_=fixture
    def unexpected(*args,**kwargs):raise AssertionError('Unbound controller started extra admission')
    monkeypatch.setattr(c,'registration_guard',unexpected);monkeypatch.setattr(c,'admission',unexpected)
    assert c.refresh_retry_producer_accounting(ctx) is None


def test_optional_producer_refresh_uses_real_admission_under_exact_registration_guard(prestart_retry,monkeypatch):
    from types import SimpleNamespace
    ctx,*_=prestart_retry;events=[]
    @contextmanager
    def registration(context,role):
        assert context is ctx and role=='controller-start'
        ctx['refresh_guard_held']=True;events.append('lock-enter')
        try:yield
        finally:ctx['refresh_guard_held']=False;events.append('lock-exit')
    def combined(root,config,rid,area,label,**resources):
        assert ctx['refresh_guard_held'] and root==c.ROOT and config is ctx['config']
        assert rid==ctx['primary_release_id'] and area==ctx['area'] and label=='retry-producer-accounting'
        assert resources=={'resource_policy':ctx['policy'],'cpus':0,'gpus':0,'hours':1}
        events.append('actual-combined-admission');return {'status':'actual admission mock'}
    ctx['campaign_module']=SimpleNamespace(combined_admission=combined)
    monkeypatch.setattr(c,'registration_guard',registration);monkeypatch.setattr(c,'admission',REAL_ADMISSION)
    before=copy.deepcopy(ctx['registry_overlay'])
    c.refresh_retry_producer_accounting(ctx)
    assert events==['lock-enter','actual-combined-admission','lock-exit']
    assert ctx['registry_overlay']==before


def test_retry_producer_refresh_requires_lock_before_any_admission(prestart_retry,monkeypatch):
    ctx,*_=prestart_retry;calls=[]
    @contextmanager
    def busy(*args,**kwargs):
        raise c.PendingRegistration('Exact permanent registration guard remains busy')
        yield
    monkeypatch.setattr(c,'registration_guard',busy)
    monkeypatch.setattr(c,'admission',lambda *a,**kw:calls.append((a,kw)))
    with pytest.raises(c.PendingRegistration):c.refresh_retry_producer_accounting(ctx)
    assert calls==[]


def test_retry_producer_refresh_never_retries_an_admission_failure(prestart_retry,monkeypatch):
    ctx,*_=prestart_retry;calls=[];error=ValueError('actual strict accounting read failed')
    def rejected(*args,**kwargs):calls.append((args,kwargs));raise error
    monkeypatch.setattr(c,'admission',rejected)
    with pytest.raises(ValueError) as caught:c.refresh_retry_producer_accounting(ctx)
    assert caught.value is error and len(calls)==1
    assert ctx['fixture_registration_locked'] is False


def test_actual_refresh_can_publish_missing_producer_proof_before_retry_binding(prestart_retry,monkeypatch):
    from types import SimpleNamespace
    ctx,value,path=prestart_retry;budget=c.ROOT/'state/resource-budget.json'
    original=read_json(budget);atomic_json(budget,{'native_jobs':{}})
    with pytest.raises(ValueError,match='completed source-bound native'):c.retry_binding(ctx['descriptor'])
    calls=[]
    def combined(*args,**kwargs):
        assert ctx['fixture_registration_locked']
        calls.append('actual guarded admission')
        # Simulate the actual358 native reader's persisted positive result.
        # The controller itself must never write a fabricated completion.
        atomic_json(budget,original)
        return {'status':'admitted'}
    ctx['campaign_module']=SimpleNamespace(combined_admission=combined)
    monkeypatch.setattr(c,'admission',REAL_ADMISSION)
    c.refresh_retry_producer_accounting(ctx)
    assert calls==['actual guarded admission'] and c.retry_binding(ctx['descriptor'])==value
