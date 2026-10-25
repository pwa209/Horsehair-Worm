"""Synthetic metadata fixtures; these tests contain no study observations."""
from pathlib import Path
import copy
import importlib.util
import pytest


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


m=load('quota_recovery_action',Path(__file__).parent/'operator-actions/quota-preflight-recovery.py')
helpers=load('quota_test_original_campaign_helpers',Path(__file__).parents[3]/'tests/test_agent_campaign.py')


@pytest.fixture
def case():
    plan=helpers.plan.__wrapped__();state,intent=helpers.ready(plan)
    state=helpers.acknowledge(plan,state,intent,'991');ident=intent['intent_id']
    proof={**helpers.proof(intent,'991','FAILED','1:0'),'user':'pwa209','account':'def-ptewarie_cpu',
           'end':'2026-10-25T22:00:00'}
    receipt={'status':'running','native_job_id':'991','index':intent['cell_index'],
             'mode':'agent-evaluation','source_sha256':m.SOURCE,'descriptor_sha256':m.DESCRIPTOR}
    evidence={ident:{'native_proof':proof,'receipt':receipt,
        'quota_traceback':'native-worker.py", line 66\n quota=runpy.run_path(...)\n'
            'StorageHeadroomError: Personal scratch storage is pending: inode headroom is insufficient: fixture',
        'quota_failure_log':{'path':'fixture.log','sha256':'e'*64},
        'artifact_archive':{'path':'fixture-archive.json','sha256':'f'*64}}}
    return plan,state,{ident:'991'},evidence,helpers.c


def test_retry_preserves_every_scientific_cell_intent_and_unrelated_job(case):
    plan,state,targets,evidence,d=case;before=copy.deepcopy(state)
    result=m.recover(plan,state,targets,evidence,d);ident=next(iter(targets));job=result['jobs'][ident]
    assert state==before and result['cells']==before['cells']
    assert job['intent']==before['jobs'][ident]['intent'] and job['job_id'] is None
    assert job['status']=='ambiguous' and job['receipt'] is None and job['native_proof'] is None
    assert job['retired_preflight_attempts'][0]['native_proof']==evidence[ident]['native_proof']
    assert result['revision']==state['revision']+1 and not result['needs_repair']
    # The unchanged original dispatcher accepts the preserved intention and
    # retains its failed-attempt history when acknowledging a fresh allocation.
    acknowledged=d.record_submission(plan,result,ident,'992',expected_revision=result['revision'])
    assert acknowledged['jobs'][ident]['job_id']=='992'
    assert acknowledged['jobs'][ident]['retired_preflight_attempts']==job['retired_preflight_attempts']


@pytest.mark.parametrize('change',['completed','wrong_id','owner','account','cpus','gpus','no_end','no_elapsed',
    'result','training_summary','descriptor','source','mode','index','different_traceback','missing_archive'])
def test_unproven_or_scientifically_started_work_is_never_retried(case,change):
    plan,state,targets,evidence,d=case;ident=next(iter(targets));item=evidence[ident]
    if change=='completed':item['native_proof'].update(state='COMPLETED',exit_code='0:0')
    elif change=='wrong_id':item['native_proof']['job_id']='990'
    elif change=='owner':item['native_proof']['user']='someone'
    elif change=='account':item['native_proof']['account']='other_cpu'
    elif change=='cpus':item['native_proof']['cpus']=1
    elif change=='gpus':item['native_proof']['gpus']=1
    elif change=='no_end':item['native_proof']['end']='Unknown'
    elif change=='no_elapsed':item['native_proof']['elapsed_seconds']=0
    elif change=='result':item['receipt']['result']={'status':'complete'}
    elif change=='training_summary':item['receipt']['training_summary']={}
    elif change=='descriptor':item['receipt']['descriptor_sha256']='0'*64
    elif change=='source':item['receipt']['source_sha256']='0'*64
    elif change=='mode':item['receipt']['mode']='agent-train'
    elif change=='index':item['receipt']['index']+=1
    elif change=='different_traceback':item['quota_traceback']='arbitrary computational error'
    else:item['artifact_archive']['sha256']=''
    before=copy.deepcopy(state)
    with pytest.raises(ValueError):m.recover(plan,state,targets,evidence,d)
    assert state==before


def test_retirement_cannot_be_repeated(case):
    plan,state,targets,evidence,d=case
    result=m.recover(plan,state,targets,evidence,d)
    with pytest.raises(ValueError):m.recover(plan,result,targets,evidence,d)


def test_unrelated_failure_is_preserved_for_explicit_review(case):
    plan,state,targets,evidence,d=case
    other=next(k for k in state['jobs'] if k not in targets)
    state['needs_repair']=[other];state.pop('state_sha256');state=d._sealed(state,'state_sha256')
    with pytest.raises(ValueError,match='Unrelated'):m.recover(plan,state,targets,evidence,d)


@pytest.fixture
def gpu_case():
    plan=helpers.plan.__wrapped__();d=helpers.c
    state,intents=d.dispatch_tick(plan,d.initial_state(plan),{},{})
    intent=next(i for i in intents if i['kind']=='training')
    state=helpers.acknowledge(plan,state,intent,'995');ident=intent['intent_id']
    proof={**helpers.proof(intent,'995','FAILED','1:0'),'user':'pwa209','account':'def-ptewarie_gpu',
           'end':'2026-10-25T22:00:00'}
    receipt={'status':'running','native_job_id':'995','index':intent['cell_index'],'mode':'agent-train',
             'source_sha256':m.SOURCE,'descriptor_sha256':m.DESCRIPTOR}
    item={'native_proof':proof,'receipt':receipt,
          'quota_traceback':'native-worker.py", line 66\nquota=runpy.run_path(...)\n'
            'StorageHeadroomError: Personal scratch storage is pending: inode headroom is insufficient: fixture',
          'quota_failure_log':{'path':'gpu-fixture.log','sha256':'e'*64},
          'artifact_archive':{'path':'gpu-fixture-archive.json','sha256':'f'*64}}
    return plan,state,{ident:'995'},{ident:item},d


def test_gpu_failure_before_training_has_the_same_preserved_retry_history(gpu_case):
    plan,state,targets,evidence,d=gpu_case
    result=m.recover(plan,state,targets,evidence,d);ident=next(iter(targets))
    assert result['cells']==state['cells'] and result['jobs'][ident]['intent']==state['jobs'][ident]['intent']
    assert result['jobs'][ident]['retired_preflight_attempts'][0]['native_proof']['gpus']==1
    assert d.record_submission(plan,result,ident,'996',expected_revision=result['revision'])['jobs'][ident]['job_id']=='996'


@pytest.mark.parametrize('change',['missing_gpu','cpu_account','checkpoint_summary','completed'])
def test_allocated_gpu_identity_and_absence_of_computation_are_required(gpu_case,change):
    plan,state,targets,evidence,d=gpu_case;item=evidence[next(iter(targets))]
    if change=='missing_gpu':item['native_proof']['gpus']=0
    elif change=='cpu_account':item['native_proof']['account']='def-ptewarie_cpu'
    elif change=='checkpoint_summary':item['receipt']['training_summary']={'training_steps':1}
    else:item['native_proof'].update(state='COMPLETED',exit_code='0:0')
    with pytest.raises(ValueError):m.recover(plan,state,targets,evidence,d)
