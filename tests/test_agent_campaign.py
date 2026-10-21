import copy
from pathlib import Path

import pytest

from horsehair import agents, agent_campaign as c
from horsehair.core import canonical_hash, read_json
from horsehair.execution_resources import LIMITS


@pytest.fixture
def plan():
    config=read_json(Path(__file__).resolve().parents[1]/'configs/study.json')
    protocol={'config_sha256':agents._digest(config['agents']),
        'full_config_sha256':canonical_hash(config),'implementation_sha256':'a'*64,
        'H4_inference_specification':agents.h4_specification(config),'learning_rate':.001,
        'final_seeds':config['agents']['final_seeds'],'biological_directions':None}
    source={'primary_source_sha256':c.PRIMARY_SCIENTIFIC_SOURCE_SHA256,
        'primary_agents_sha256':'a'*64,'execution_source_sha256':'b'*64,'evaluation_module_sha256':'c'*64}
    policy={'path':str(c.ROOT/'maintenance/execution-resource-policies/fixture/policy.json'),
        'sha256':'d'*64,'execution_source_sha256':'b'*64,'base_config_sha256':canonical_hash(config),
        'primary_scientific_source_sha256':c.PRIMARY_SCIENTIFIC_SOURCE_SHA256,'limits':dict(LIMITS),
        'scientific_gates_changed':False,'approval':{'source':'current_user','authorizes_cpu_gpu_resource_amendment':True}}
    return c.plan_campaign(config,protocol,protocol_binding={'path':str(c.ROOT/'results/frozen_protocol.json'),'sha256':'e'*64},
        source_binding=source,policy_binding=policy,case=str(c.ROOT/'maintenance/final-campaign-fixture'))


def acknowledge(plan,state,intent,job):
    return c.record_submission(plan,state,intent['intent_id'],job,expected_revision=state['revision'])


def proof(intent,job,state='COMPLETED',exit_code='0:0'):
    return {'job_id':job,'state':state,'exit_code':exit_code,'cpus':intent['resources']['cpus'],
        'gpus':intent['resources']['gpus'],'elapsed_seconds':60}


def packet(plan,intent,job,value):
    return {'campaign_plan_sha256':plan['plan_sha256'],'intent_id':intent['intent_id'],
        'native_job_id':job,'case':intent['case'],'sha256':canonical_hash(value),'value':value}


def training(plan,intent,job):
    value={k:intent[k] for k in ('task','architecture','seed')}
    value.update(training_steps=1000000,complete_training_budget=True,implementation_sha256='a'*64,
        config_sha256=agents._digest(plan['config']['agents']),learning_rate=.001,
        checkpoint_sha256='1'*64,weight_sha256='2'*64)
    return packet(plan,intent,job,value)


def evaluation_binding(plan,index=0):
    cell=plan['cells'][index]
    value={'canary':False,'benchmark_directions':False,'amendment':c.evaluation.AMENDMENT,
        'evaluation_dtype':'float64','execution_module_sha256':'c'*64,'primary_agents_sha256':'a'*64,
        'full_config_sha256':c.BASE_CONFIG_SHA256,'config':plan['config'],'original_protocol':plan['protocol'],
        'original_protocol_sha256':plan['protocol_sha256'],'conditions':plan['conditions'],
        'checkpoint_sha256':'1'*64,'original_weight_sha256':'2'*64,
        'checkpoint':cell['training_case']+'/checkpoint.pt','original_training_steps':1000000,
        **{k:cell[k] for k in ('task','architecture','seed')}}
    return {'case':cell['evaluation_case'],'plan_sha256':canonical_hash(value),'value':value}


def ready(plan):
    state,intents=c.dispatch_tick(plan,c.initial_state(plan),{},{});intent=intents[0]
    state=acknowledge(plan,state,intent,'10')
    state,_=c.dispatch_tick(plan,state,{'10':proof(intent,'10')},{intent['intent_id']:training(plan,intent,'10')})
    state=c.bind_evaluation_plan(plan,state,0,evaluation_binding(plan),expected_revision=state['revision'])
    state,intents=c.dispatch_tick(plan,state,{},{})
    return state,next(i for i in intents if i['kind']=='evaluation')


def chunk(plan,state,intent,job,count):
    cell=state['cells'][0];items=copy.deepcopy(list(cell['conditions'].values()))
    start=len(items)
    for index in range(start,start+count):
        value={'status':'complete','plan_sha256':cell['evaluation_plan']['plan_sha256'],
            'condition_index':index,'condition':plan['conditions'][index],
            'original_weight_sha256':'2'*64,'evaluation_weight_sha256':'3'*64,
            'artifact_name':f'condition-{index}.parquet','artifact_sha256':'4'*64,'slurm_job_id':job}
        items.append({'sha256':canonical_hash(value),'value':value})
    value={'status':'condition_grid_complete' if len(items)==len(plan['conditions']) else 'chunk_complete',
        'plan_sha256':cell['evaluation_plan']['plan_sha256'],'required_conditions':len(plan['conditions']),
        'completed_conditions':len(items),'newly_completed_conditions':count,'canary':False,
        'scientific_result':False,'admission_authority':False}
    return dict(packet(plan,intent,job,value),condition_receipts=items)


def test_original_240_index_roster_twenty_seeds_and_plan_immutability(plan):
    expected=[(task,arch,seed) for task,arch,seed,_ in agents._cells(plan['config']['agents'])]
    assert [(x['task'],x['architecture'],x['seed']) for x in plan['cells']]==expected
    assert len(expected)==240 and {x['seed'] for x in plan['cells']}==set(range(1000,1020))
    assert len(plan['conditions'])==300
    assert plan['bounds']==c.BOUNDS and plan['window']==c.WINDOW
    directory=agents._directory(Path(str(c.ROOT)),plan['config']['agents'])
    for cell in plan['cells']:
        assert cell['training_case']==(directory/'final'/agents._cell_name(cell['task'],cell['architecture'],cell['seed'])).as_posix()
    assert plan['scientific_result'] is False and plan['admission_authority'] is False
    changed=copy.deepcopy(plan);changed['cells'][0]['seed']=900
    with pytest.raises(ValueError,match='Immutable campaign'):c.initial_state(changed)


@pytest.mark.parametrize('change',['config','source','protocol','policy','case'])
def test_plan_rejects_foreign_or_amended_science_and_missing_authority(plan,change):
    config=copy.deepcopy(plan['config']);protocol=copy.deepcopy(plan['protocol'])
    source=copy.deepcopy(plan['source_binding']);policy=copy.deepcopy(plan['policy_binding']);case=plan['case']
    if change=='config':config['agents']['final_seeds'].pop()
    if change=='source':source['primary_source_sha256']='f'*64
    if change=='protocol':protocol['biological_directions']={'guessed':True}
    if change=='policy':policy['approval']['source']='agent_guess'
    if change=='case':case='/scratch/shared/final'
    with pytest.raises(ValueError):c.plan_campaign(config,protocol,protocol_binding=plan['protocol_binding'],
        source_binding=source,policy_binding=policy,case=case)


def test_four_gpu_window_preserves_all_ambiguous_intentions_and_original_inputs(plan):
    state=c.initial_state(plan);before=copy.deepcopy(state)
    state,intents=c.dispatch_tick(plan,state,{},{})
    assert before==c.initial_state(plan)
    assert [i['cell_index'] for i in intents]==[0,1,2,3]
    assert all(i['kind']=='training' and i['resources']=={'cpus':6,'gpus':1,'hours':12} for i in intents)
    again,second=c.dispatch_tick(plan,state,{},{})
    assert not second and set(again['jobs'])==set(state['jobs'])
    assert all(j['job_id'] is None and j['status']=='ambiguous' for j in again['jobs'].values())


def test_acknowledgment_is_compare_revision_unique_and_never_replays(plan):
    state,intents=c.dispatch_tick(plan,c.initial_state(plan),{},{});intent=intents[0]
    with pytest.raises(ValueError,match='Stale'):c.record_submission(plan,state,intent['intent_id'],'10',expected_revision=0)
    state=acknowledge(plan,state,intent,'10')
    with pytest.raises(ValueError,match='Existing'):acknowledge(plan,state,intent,'11')
    with pytest.raises(ValueError,match='duplicate'):acknowledge(plan,state,intents[1],'10')
    again,new=c.dispatch_tick(plan,state,{},{});assert not new and again['jobs'][intent['intent_id']]['job_id']=='10'


def test_training_completion_requires_native_and_complete_original_receipt(plan):
    state,intents=c.dispatch_tick(plan,c.initial_state(plan),{},{});intent=intents[0]
    state=acknowledge(plan,state,intent,'10')
    state,new=c.dispatch_tick(plan,state,{'10':proof(intent,'10')},{})
    assert not new and state['jobs'][intent['intent_id']]['status']=='submitted'
    with pytest.raises(ValueError,match='positively completed'):c.bind_evaluation_plan(plan,state,0,evaluation_binding(plan),expected_revision=state['revision'])
    receipt=training(plan,intent,'10');receipt['value']['training_steps']=32768
    failed,new=c.dispatch_tick(plan,state,{'10':proof(intent,'10')},{intent['intent_id']:receipt})
    assert failed['status']=='needs_repair' and not new


@pytest.mark.parametrize('change',['case','checkpoint','canary','condition','source'])
def test_evaluation_plan_binding_refuses_reduced_or_foreign_work(plan,change):
    state,intents=c.dispatch_tick(plan,c.initial_state(plan),{},{});intent=intents[0]
    state=acknowledge(plan,state,intent,'10')
    state,_=c.dispatch_tick(plan,state,{'10':proof(intent,'10')},{intent['intent_id']:training(plan,intent,'10')})
    binding=evaluation_binding(plan)
    if change=='case':binding['case']+='/wrong'
    if change=='checkpoint':binding['value']['checkpoint_sha256']='f'*64
    if change=='canary':binding['value']['canary']=True
    if change=='condition':binding['value']['conditions']=binding['value']['conditions'][:-1]
    if change=='source':binding['value']['execution_module_sha256']='f'*64
    with pytest.raises(ValueError,match='Evaluation plan changed'):c.bind_evaluation_plan(plan,state,0,binding,expected_revision=state['revision'])


def test_complete_chunk_advances_once_only_with_matching_condition_evidence(plan):
    state,intent=ready(plan);state=acknowledge(plan,state,intent,'100')
    result=chunk(plan,state,intent,'100',2)
    state,new=c.dispatch_tick(plan,state,{'100':proof(intent,'100')},{intent['intent_id']:result})
    assert len(state['cells'][0]['conditions'])==2
    assert len(new)==1 and new[0]['kind']=='evaluation' and new[0]['wave']==2
    assert new[0]['previous_completed_conditions']==2 and new[0]['resources']['hours']==12
    again,repeated=c.dispatch_tick(plan,state,{}, {intent['intent_id']:result})
    assert not repeated and len(again['cells'][0]['evaluation_jobs'])==2


@pytest.mark.parametrize('failure',['no_progress','duplicate','foreign_job','missing_condition','changed_plan'])
def test_failed_or_dishonest_chunk_requires_repair_without_automatic_retry(plan,failure):
    state,intent=ready(plan);state=acknowledge(plan,state,intent,'100')
    result=chunk(plan,state,intent,'100',0 if failure=='no_progress' else 2)
    if failure=='duplicate':result['condition_receipts'].append(copy.deepcopy(result['condition_receipts'][0]))
    if failure=='foreign_job':result['condition_receipts'][0]['value']['slurm_job_id']='999'
    if failure=='missing_condition':result['condition_receipts'].pop()
    if failure=='changed_plan':result['value']['plan_sha256']='f'*64
    repaired,new=c.dispatch_tick(plan,state,{'100':proof(intent,'100')},{intent['intent_id']:result})
    assert repaired['status']=='needs_repair' and not new
    assert repaired['cells'][0]['evaluation_jobs']==[intent['intent_id']]
    assert repaired['cells'][0]['conditions']=={}
    assert repaired['jobs'][intent['intent_id']]['failure_receipt']==result
    again,new=c.dispatch_tick(plan,repaired,{}, {intent['intent_id']:chunk(plan,state,intent,'100',2)})
    assert not new and again['needs_repair']==repaired['needs_repair']
    assert again['jobs'][intent['intent_id']]['failure_receipt']==result


@pytest.mark.parametrize('native_state,exitcode',[('FAILED','1:0'),('TIMEOUT','0:15'),('COMPLETED','1:0')])
def test_native_failure_is_preserved_and_never_silently_resubmitted(plan,native_state,exitcode):
    state,intent=ready(plan);state=acknowledge(plan,state,intent,'100')
    result,new=c.dispatch_tick(plan,state,{'100':proof(intent,'100',native_state,exitcode)},{})
    assert result['status']=='needs_repair' and not new and result['jobs'][intent['intent_id']]['job_id']=='100'
    again,new=c.dispatch_tick(plan,result,{},{});assert again['status']=='needs_repair' and not new


def test_evaluation_cap_is_twelve_progressing_waves_not_infinite_retries(plan):
    state,intent=ready(plan)
    for wave in range(1,13):
        job=str(100+wave);assert intent['wave']==wave
        state=acknowledge(plan,state,intent,job)
        result=chunk(plan,state,intent,job,1)
        state,new=c.dispatch_tick(plan,state,{job:proof(intent,job)},{intent['intent_id']:result})
        if wave<12:intent=next(i for i in new if i['kind']=='evaluation')
    assert len(state['cells'][0]['conditions'])==12 and len(state['cells'][0]['evaluation_jobs'])==12
    assert state['status']=='needs_repair' and not new


def test_assembly_requires_all_three_hundred_conditions_then_keeps_inference_pending(plan):
    state,intent=ready(plan);state=acknowledge(plan,state,intent,'100')
    result=chunk(plan,state,intent,'100',len(plan['conditions']))
    state,new=c.dispatch_tick(plan,state,{'100':proof(intent,'100')},{intent['intent_id']:result})
    assert len(new)==1 and new[0]['kind']=='assembly'
    assemble=new[0];state=acknowledge(plan,state,assemble,'200')
    value={'status':'complete_amended_evaluation','canary':False,'scientific_result':False,'admission_authority':False,
        'plan_sha256':state['cells'][0]['evaluation_plan']['plan_sha256'],'conditions':300,
        'checkpoint_sha256':'1'*64,'original_weight_sha256':'2'*64,'trial_file_sha256':'5'*64,
        'condition_bindings':[{'condition_index':int(i),'receipt_sha256':r['sha256'],
            'artifact_sha256':r['value']['artifact_sha256']} for i,r in state['cells'][0]['conditions'].items()]}
    state,_=c.dispatch_tick(plan,state,{'200':proof(assemble,'200')},{assemble['intent_id']:packet(plan,assemble,'200',value)})
    assert state['cells'][0]['complete'] is True
    assert sum(cell['complete'] for cell in state['cells'])==1
    assert state['status']=='active' and state['scientific_result'] is False
    assert state['H4_inference_status']=='pending_separate_validated_analysis_and_bridge'


def test_state_completion_cannot_hide_unfinished_cells(plan):
    state=c.initial_state(plan);state['cells'][0]['complete']=True
    state.pop('state_sha256');c._sealed(state,'state_sha256')
    with pytest.raises(ValueError,match='Unproven policy completion'):c.dispatch_tick(plan,state,{},{})


def test_native_allocation_mismatch_and_changed_terminal_proof_refused(plan):
    state,intent=ready(plan);state=acknowledge(plan,state,intent,'100')
    bad=proof(intent,'100');bad['gpus']=1
    with pytest.raises(ValueError,match='allocation differs'):c.dispatch_tick(plan,state,{'100':bad},{})
    state,_=c.dispatch_tick(plan,state,{'100':proof(intent,'100')},{})
    bad=proof(intent,'100');bad['elapsed_seconds']=61
    with pytest.raises(ValueError,match='terminal native evidence changed'):c.dispatch_tick(plan,state,{'100':bad},{})


def test_thirty_two_cpu_evaluation_window_with_four_training_jobs_retained(plan):
    state,intents=c.dispatch_tick(plan,c.initial_state(plan),{},{})
    for batch in range(10):
        native,receipts={},{}
        for index,intent in enumerate(intents):
            job=str(1000+batch*4+index);state=acknowledge(plan,state,intent,job)
            native[job]=proof(intent,job);receipts[intent['intent_id']]=training(plan,intent,job)
        state,intents=c.dispatch_tick(plan,state,native,receipts)
    for index in range(33):
        state=c.bind_evaluation_plan(plan,state,index,evaluation_binding(plan,index),expected_revision=state['revision'])
    state,intents=c.dispatch_tick(plan,state,{},{})
    assert len(intents)==32 and all(i['kind']=='evaluation' and i['resources']['cpus']==4 for i in intents)
    assert sum(j['intent']['kind']=='training' and j['status']!='complete' for j in state['jobs'].values())==4
    again,new=c.dispatch_tick(plan,state,{},{});assert not new
    assert sum(j['status']!='complete' for j in again['jobs'].values())==36<=64


def test_global_queued_guard_applies_even_when_individual_window_has_room(plan,monkeypatch):
    # A deliberately smaller diagnostic queue ceiling exercises the same gate
    # without fabricating completed policies or a large condition-state fixture.
    monkeypatch.setitem(c.WINDOW,'queued',2)
    plan=c.plan_campaign(plan['config'],plan['protocol'],protocol_binding=plan['protocol_binding'],
        source_binding=plan['source_binding'],policy_binding=plan['policy_binding'],case=plan['case'])
    state,intents=c.dispatch_tick(plan,c.initial_state(plan),{},{})
    assert len(intents)==2
    assert c.dispatch_tick(plan,state,{}, {})[1]==[]
