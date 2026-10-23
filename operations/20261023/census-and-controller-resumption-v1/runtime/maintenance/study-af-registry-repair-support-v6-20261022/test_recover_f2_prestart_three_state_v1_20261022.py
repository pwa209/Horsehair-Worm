"""Synthetic state/proof tests only. No native work or real result is invented."""
from pathlib import Path
import copy
import gzip
import importlib.util
import json
import shlex

import pytest

from horsehair import agents,agent_campaign as dispatch
from horsehair.core import canonical_hash,read_json
from horsehair.execution_resources import LIMITS

spec=importlib.util.spec_from_file_location('exact_three_prestart_recovery',
    Path(__file__).with_name('recover-f2-prestart-three-state-v1-20261022.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def packet(body,path,*,raw=False):
    data=body if raw else json.dumps(body,sort_keys=True,allow_nan=False).encode()
    return {'path':str(path),'sha256':m.raw_sha(data),'raw':data}


def body(p):return json.loads(p['raw'])


def repacket(p,change):
    value=body(p);change(value);return packet(value,p['path'])


def normalized(job,cpus,gpus,state='COMPLETED',exit_code='0:0',elapsed=60):
    return {'job_id':job,'state':state,'exit_code':exit_code,'cpus':cpus,'gpus':gpus,'elapsed_seconds':elapsed,
        'end':'Unknown' if state=='RUNNING' else '2026-10-22T18:41:00',
        'user':'pwa209','account':'def-ptewarie_gpu' if gpus else 'def-ptewarie_cpu'}


def seal_state(state):
    state.pop('state_sha256',None);state['state_sha256']=canonical_hash(state)


@pytest.fixture
def bundle(monkeypatch):
    config=read_json(Path(agents.__file__).resolve().parents[2]/'configs/study.json')
    source={'primary_source_sha256':m.PRIMARY,'primary_agents_sha256':'a'*64,
        'execution_source_sha256':m.SOURCE,'evaluation_module_sha256':'c'*64}
    protocol={'config_sha256':agents._digest(config['agents']),'full_config_sha256':m.CONFIG,'implementation_sha256':'a'*64,
        'H4_inference_specification':agents.h4_specification(config),'learning_rate':.0003,
        'final_seeds':config['agents']['final_seeds'],'biological_directions':None}
    policy={'path':str(m.ROOT/'maintenance/execution-resource-policies/synthetic/policy.json'),'sha256':'d'*64,
        'execution_source_sha256':m.SOURCE,'base_config_sha256':m.CONFIG,'primary_scientific_source_sha256':m.PRIMARY,
        'limits':dict(LIMITS),'scientific_gates_changed':False,
        'approval':{'source':'current_user','authorizes_cpu_gpu_resource_amendment':True}}
    plan=dispatch.plan_campaign(config,protocol,protocol_binding={'path':str(m.ROOT/'results/synthetic-protocol.json'),'sha256':'e'*64},
        source_binding=source,policy_binding=policy,case=str(m.CAMPAIGN))
    descriptor={'worker_sha256':m.WORKER_SHA,'execution_source_sha256':m.SOURCE,'agent_campaign_case':str(m.CAMPAIGN),
        'resource_policy':{'sha256':policy['sha256']},'frozen_protocol_sha256':'e'*64,'synthetic_test_fixture':True}
    descriptor_packet=packet(descriptor,m.WORKER_DESCRIPTOR)
    # Only the unavailable live descriptor bytes and exact live state hash are
    # replaced for this explicitly synthetic fixture. Production pins stay exact.
    monkeypatch.setattr(m,'WORKER_DESCRIPTOR_SHA',descriptor_packet['sha256'])
    state=dispatch.initial_state(plan)
    state['audit_history']=[{'revision':390,'event':'synthetic preserved historical event'}]
    for index in range(78):
        cell=state['cells'][index];intent=dispatch._intent(plan,cell,'training');ident=intent['intent_id']
        job=m.TARGETS.get(ident,str(6000000+index*10))
        terminal=index<76
        status='complete' if terminal else 'needs_repair' if index==76 else 'submitted'
        native=normalized(job,6,1,'COMPLETED' if terminal else 'FAILED' if index==76 else 'RUNNING',
            '1:0' if index==76 else '0:0',100 if index==76 else 60)
        state['jobs'][ident]={'intent':intent,'job_id':job,'status':status,'native_proof':native,
            'receipt':{'synthetic_positive_fixture':True} if terminal else None,
            'preserved_opaque_history':{'attempts':['synthetic old immutable event']}}
        cell['training_job']=ident
        if index<74:
            ep={'synthetic_test_fixture':True,'checkpoint_sha256':'1'*64,'original_weight_sha256':'2'*64}
            epacket=packet(ep,m.CAMPAIGN/f'cell-{index:04d}'/'plan.json')
            ebpacket=packet({'plan_sha256':epacket['sha256']},m.CAMPAIGN/f'cell-{index:04d}'/'plan-binding.json')
            cell['evaluation_plan']={'case':str(m.CAMPAIGN/f'cell-{index:04d}'),'plan_sha256':epacket['sha256'],
                'plan_binding_sha256':ebpacket['sha256'],'value':ep}
            ei=dispatch._intent(plan,cell,'evaluation',1);eid=ei['intent_id'];ejob=m.TARGETS.get(eid,str(6000001+index*10))
            complete=index<67 or index in (68,69,70)
            estate='COMPLETED' if complete else 'FAILED' if index==73 else 'RUNNING'
            state['jobs'][eid]={'intent':ei,'job_id':ejob,'status':'complete' if complete else 'needs_repair' if index==73 else 'submitted',
                'native_proof':normalized(ejob,4,0,estate,'1:0' if index==73 else '0:0'),
                'receipt':{'synthetic_positive_fixture':True} if complete else None}
            cell['evaluation_jobs']=[eid]
            if complete:cell['conditions']={str(i):{'synthetic_condition_fixture':i} for i in range(300)}
        if index<67:
            ai=dispatch._intent(plan,cell,'assembly');aid=ai['intent_id'];ajob=str(6000002+index*10)
            state['jobs'][aid]={'intent':ai,'job_id':ajob,'status':'complete','native_proof':normalized(ajob,1,0),
                'receipt':{'synthetic_positive_fixture':True}}
            cell['assembly_job']=aid;cell['complete']=True
    state['revision']=405;state['status']='needs_repair';state['needs_repair']=['evaluation-0073-01','training-0076'];seal_state(state)
    assert len(state['jobs'])==219
    dispatch._state(plan,state)
    monkeypatch.setattr(m,'EXPECTED_STATE_SHA256',state['state_sha256'])
    original=packet(gzip.compress(json.dumps(state,sort_keys=True).encode(),mtime=0),m.CASE/'original-state.json.gz',raw=True)
    statebinding={'state_file_sha256':original['sha256'],'state_sha256':state['state_sha256'],
        'campaign_plan_sha256':plan['plan_sha256'],'revision':405,'scientific_result':False,'admission_authority':False}
    proof={'case':str(m.CASE),'dispatcher_sha256':m.DISPATCH_SHA,'original_state_gzip':original,
        'original_state_binding':packet(statebinding,m.CASE/'original-state-binding.json'),
        'original_campaign_plan':packet(plan,m.CASE/'original-campaign-plan.json'),'worker_descriptor':descriptor_packet,
        'approval':packet({'source':'current_user','authorizes_targeted_operational_repair':True,'targets':dict(m.TARGETS),
            'request_reference':'Synthetic test of explicit current human authorization'},m.CASE/'current-user-authorization.json'),
        'producer':packet({'native':normalized('9991000',4,0,'RUNNING'),
            'source_sha256':m.SOURCE,'config_sha256':m.CONFIG},m.CASE/'native/producer.json'),
        'old_recovery':{'path':str(m.OLD_RECOVERY),'sha256':m.OLD_RECOVERY_SHA},'old_controllers':{},'failed':{}}
    for job in m.CONTROLLERS:
        record=normalized(job,0 if job=='6701493' else 1,0,'CANCELLED' if job=='6701493' else 'FAILED',
            '0:0' if job=='6701493' else '1:0',0 if job=='6701493' else 60)
        proof['old_controllers'][job]=packet(record,m.CASE/'native'/('controller-'+job+'.json'))
    for ident,job in m.TARGETS.items():
        intent=state['jobs'][ident]['intent'];kind=intent['kind'];index=intent['cell_index'];name='af-f2-'+ident
        directory=m.ROOT/'plans'/name;scriptpath=directory/(name+'.sbatch')
        argv=['python',str(m.OLD_SHIM),'--registry-read-guard',str(m.OLD_GUARD),'--registry-read-guard-sha256',m.OLD_GUARD_SHA,
            '--descriptor',str(m.WORKER_DESCRIPTOR),'--descriptor-sha256',m.WORKER_DESCRIPTOR_SHA,
            '--mode','agent-train' if kind=='training' else 'agent-evaluation','--index',str(index),
            '--cpus',str(intent['resources']['cpus']),'--gpus',str(intent['resources']['gpus']),'--hours','12']
        if kind=='evaluation':
            binding=state['cells'][index]['evaluation_plan']
            argv+=['--evaluation-case',binding['case'],'--evaluation-plan-sha256',binding['plan_sha256'],
                '--evaluation-binding-sha256',binding['plan_binding_sha256']]
        command=shlex.join(argv);script='#!/bin/bash\n#SBATCH --hold\nset -euo pipefail\n'+command+'\n'
        scriptpacket=packet(script.encode(),scriptpath,raw=True)
        task={'name':name,'script':str(scriptpath),'script_sha256':canonical_hash(script),'command':command,
            'resources':{'cpus':intent['resources']['cpus'],'gpu':bool(intent['resources']['gpus']),'hours':12,
                'memory':'64G' if kind=='training' else '32G'},'depends':[],'external_dependencies':[],'dependency_kind':'afterok'}
        nativeplan={'root':str(m.ROOT),'phase':name,'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},
            'campaign_plan_sha256':plan['plan_sha256'],'intent_sha256':intent['intent_sha256'],
            'worker_descriptor_sha256':m.WORKER_DESCRIPTOR_SHA,'controller_descriptor_sha256':'f'*64,'tasks':[task]}
        pp=packet(nativeplan,directory/'plan.json')
        ledger={'source_sha256':m.SOURCE,'config_sha256':m.CONFIG,'plan_sha256':canonical_hash(nativeplan),
            'tasks':{name:{'job_id':job,'state':'submitted','script_sha256':task['script_sha256'],
                'command':['sbatch','--parsable',str(scriptpath)]}}}
        lp=packet(ledger,m.ROOT/'state'/(name+'-submission.json'))
        held={'JobId':job,'UserId':'pwa209(3164768)','Account':'def-ptewarie_gpu' if kind=='training' else 'def-ptewarie_cpu',
            'JobState':'PENDING','Reason':'JobHeldUser','JobName':'hhw-'+name,'NumCPUs':str(intent['resources']['cpus']),
            'Command':str(scriptpath),'MinMemoryNode':task['resources']['memory'],'TimeLimit':'12:00:00'}
        journal={'status':'released','job_id':job,'intent':intent,'plan_path':pp['path'],'plan_sha256':pp['sha256'],
            'ledger_path':lp['path'],'scientific_result':False,'admission_authority':False,'released_unix':1792692000.,'held_allocation':held}
        log="Traceback (most recent call last):\n admission(...area,'native-start-admission'...)\n all_registered_work\n"+m.FAILURE_MARKER+'\n'
        observed=copy.deepcopy(state['jobs'][ident]['native_proof'])
        if ident=='training-0077':observed.update(state='FAILED',exit_code='1:0',elapsed_seconds=2226,end='2026-10-22T18:41:00')
        proof['failed'][ident]={'native':packet(observed,m.CASE/'native'/('worker-'+job+'.json')),'native_plan':pp,
            'submission_ledger':lp,'script':scriptpacket,'submission_journal':packet(journal,m.CAMPAIGN/('submission-'+ident+'.json')),
            'traceback':packet(log.encode(),m.ROOT/'logs'/(name+'-'+job+'_4294967294.log'),raw=True)}
        if kind=='evaluation':
            proof['failed'][ident]['evaluation_plan']=packet(binding['value'],m.PurePosixPath(binding['case'])/'plan.json')
            proof['failed'][ident]['evaluation_plan_binding']=packet({'plan_sha256':binding['plan_sha256']},
                m.PurePosixPath(binding['case'])/'plan-binding.json')
    ep=proof['failed']['evaluation-0073-01']
    absent={'registration_mutex':str(m.MUTEX),'registration_mutex_held':True,'mutex_device':1,'mutex_inode':1234,
        'same_lock_held_through_validation':True,'captured_original_state_sha256':state['state_sha256'],
        'captured_original_state_file_sha256':original['sha256'],'captured_original_revision':405,
        'native_campaign_controller_ids':sorted(m.CONTROLLERS),'captured_unix':1792703000.,'validation_unix':1792703001.,
        'entries':[{'path':p,'exists':False,'is_symlink':False} for p in sorted(m.absence_paths(plan))],
        'evaluation_top_level':[{'path':ep[k]['path'],'sha256':ep[k]['sha256'],'is_file':True,'is_dir':False,'is_symlink':False}
            for k in ('evaluation_plan','evaluation_plan_binding')]}
    proof['absence']=packet(absent,m.CASE/'verified-absence-under-lock.json')
    return plan,state,proof


def test_exact_derivative_preserves_original219_and_every_unrelated_field(bundle):
    plan,state,proof=bundle;before=copy.deepcopy((plan,state,proof))
    result=m.recover(plan,state,proof);candidate=result['state'];amendment=result['amendment']
    assert (plan,state,proof)==before
    assert len(candidate['jobs'])==216 and candidate['revision']==406
    assert candidate['jobs']=={key:value for key,value in state['jobs'].items() if key not in m.TARGETS}
    for index,(old,new) in enumerate(zip(state['cells'],candidate['cells'])):
        expected=copy.deepcopy(old)
        if index in (76,77):expected['training_job']=None
        if index==73:expected['evaluation_jobs']=[]
        assert new==expected
    for key in state:
        if key not in {'jobs','cells','status','needs_repair','revision','state_sha256'}:assert candidate[key]==state[key]
    assert candidate['needs_repair']==[] and candidate['status']=='active'
    assert amendment['failed_history_preserved']=={key:state['jobs'][key] for key in m.TARGETS}
    assert amendment['failed_history_preserved']['training-0077']['native_proof']['state']=='RUNNING'
    assert amendment['observed_failed_native']['training-0077']['state']=='FAILED'
    assert amendment['original_jobs_preserved']==219 and amendment['preserved_unrelated_jobs']==216
    assert amendment['source_pins']['original_state_gzip']['sha256']==proof['original_state_gzip']['sha256']
    assert amendment['native_namespace_overrides']=={t:'af-f2-r2-'+t for t in m.TARGETS}
    assert amendment['submission_journal_overrides']=={t:'submission-r2-'+t+'.json' for t in m.TARGETS}
    assert amendment['activation_producer_job_id']=='9991000'
    assert amendment['amendment_sha256']==canonical_hash({k:v for k,v in amendment.items() if k!='amendment_sha256'})
    dispatch._state(plan,candidate)


def test_regeneration_preserves_frozen_wave1_and_can_also_issue_ordinary_work(bundle):
    plan,state,proof=bundle;result=m.recover(plan,state,proof)
    candidate,intents=dispatch.dispatch_tick(plan,result['state'],{},{})
    by_id={intent['intent_id']:intent for intent in intents}
    assert set(m.TARGETS)<=set(by_id)
    for ident in m.TARGETS:assert by_id[ident]==state['jobs'][ident]['intent']==result['amendment']['regenerated_intents'][ident]
    assert 'evaluation-0073-02' not in candidate['jobs']
    assert candidate['cells'][73]['evaluation_jobs']==['evaluation-0073-01']
    assert candidate['cells'][73]['evaluation_plan']==state['cells'][73]['evaluation_plan']
    assert {'training-0078','training-0079'}<=set(by_id)


@pytest.mark.parametrize('target,key',[(target,key) for target in m.TARGETS for key in
    ('native','native_plan','submission_ledger','script','submission_journal','traceback')])
def test_each_target_source_packet_is_byte_bound(bundle,target,key):
    plan,state,proof=bundle;proof['failed'][target][key]['raw']+=b' '
    with pytest.raises(ValueError,match='byte-bound'):m.recover(plan,state,proof)


@pytest.mark.parametrize('key',['approval','original_state_gzip','original_state_binding','original_campaign_plan','worker_descriptor','producer','absence'])
def test_every_original_or_authority_packet_is_byte_bound(bundle,key):
    plan,state,proof=bundle;proof[key]['raw']+=b' '
    with pytest.raises(ValueError,match='byte-bound'):m.recover(plan,state,proof)


@pytest.mark.parametrize('mutation',[
    lambda p:p.update(registration_mutex_held=False),
    lambda p:p.update(same_lock_held_through_validation=False),
    lambda p:p.update(registration_mutex=str(m.ROOT/'state/replacement.mutex')),
    lambda p:p.update(mutex_inode=0),
    lambda p:p.update(captured_original_state_sha256='0'*64),
    lambda p:p.update(captured_original_state_file_sha256='0'*64),
    lambda p:p.update(captured_original_revision=404),
    lambda p:p.update(validation_unix=p['captured_unix']+31),
    lambda p:p.update(validation_unix=p['captured_unix']-1),
    lambda p:p.update(native_campaign_controller_ids=sorted(m.CONTROLLERS)[:-1]),
    lambda p:p['entries'][0].update(exists=True),
    lambda p:p['entries'][0].update(is_symlink=True),
    lambda p:p['entries'].pop(),
    lambda p:p['evaluation_top_level'].append({'path':str(m.CAMPAIGN/'cell-0073/condition-00000-partial.parquet'),
        'sha256':'f'*64,'is_file':True,'is_dir':False,'is_symlink':False}),
    lambda p:p['evaluation_top_level'][0].update(sha256='0'*64),
    lambda p:p['evaluation_top_level'][0].update(is_symlink=True),
])
def test_stale_partial_redirected_or_wrong_state_inventory_refused(bundle,mutation):
    plan,state,proof=bundle;proof['absence']=repacket(proof['absence'],mutation)
    with pytest.raises(ValueError):m.recover(plan,state,proof)


@pytest.mark.parametrize('field,value', [('job_id','6795586'),('state','RUNNING'),('state','COMPLETED'),('exit_code','0:0'),
    ('cpus',6),('gpus',1),('user','another'),('account','def-ptewarie_gpu'),('end','Unknown'),('elapsed_seconds',0)])
def test_wrong_failed_native_allocation_refused(bundle,field,value):
    plan,state,proof=bundle;failed=proof['failed']['evaluation-0073-01']
    failed['native']=repacket(failed['native'],lambda p:p.update({field:value}))
    with pytest.raises(ValueError):m.recover(plan,state,proof)


def test_old_terminal_proof_cannot_be_rewritten_even_to_another_failure(bundle):
    plan,state,proof=bundle;failed=proof['failed']['training-0076']
    failed['native']=repacket(failed['native'],lambda p:p.update(elapsed_seconds=101))
    with pytest.raises(ValueError,match='Historical terminal'):m.recover(plan,state,proof)


@pytest.mark.parametrize('change',[
    lambda p:p.update(job_id='9999999'),lambda p:p.update(state='RUNNING',end='Unknown'),
    lambda p:p.update(user='other'),lambda p:p.update(cpus=2)])
def test_every_old_controller_must_be_exact_and_terminal(bundle,change):
    plan,state,proof=bundle;key='6701494';proof['old_controllers'][key]=repacket(proof['old_controllers'][key],change)
    with pytest.raises(ValueError):m.recover(plan,state,proof)


@pytest.mark.parametrize('change',[
    lambda p:p['native'].update(state='COMPLETED'),lambda p:p['native'].update(cpus=1),
    lambda p:p['native'].update(gpus=1),lambda p:p['native'].update(user='other'),
    lambda p:p.update(source_sha256='0'*64),lambda p:p.update(config_sha256='0'*64)])
def test_source_bound_current_producer_required(bundle,change):
    plan,state,proof=bundle;proof['producer']=repacket(proof['producer'],change)
    with pytest.raises(ValueError):m.recover(plan,state,proof)


@pytest.mark.parametrize('key', ['status','job_id','intent','plan_path','plan_sha256','ledger_path','held_allocation'])
def test_original_released_journal_chain_cannot_change(bundle,key):
    plan,state,proof=bundle;item=proof['failed']['training-0076']
    item['submission_journal']=repacket(item['submission_journal'],lambda p:p.update({key:None}))
    with pytest.raises((ValueError,AttributeError)):m.recover(plan,state,proof)


def test_unauthorized_or_arbitrary_fourth_target_refused(bundle):
    plan,state,proof=bundle
    proof['approval']=repacket(proof['approval'],lambda p:p['targets'].update({'training-0078':'9999999'}))
    with pytest.raises(ValueError,match='authorization'):m.recover(plan,state,proof)


def test_unrelated_job_mutation_rejected_by_actual_original_state_pin(bundle):
    plan,state,proof=bundle;state['jobs']['training-0001']['preserved_opaque_history']['attempts'].append('tampered')
    seal_state(state)
    with pytest.raises(ValueError,match='exact original revision405'):m.recover(plan,state,proof)


def test_self_consistent_changed_original_bytes_still_fail_exact_revision405_pin(bundle):
    plan,state,proof=bundle;state['revision']=406;seal_state(state)
    proof['original_state_gzip']=packet(gzip.compress(json.dumps(state).encode(),mtime=0),m.CASE/'original-state.json.gz',raw=True)
    with pytest.raises(ValueError,match='exact original revision405'):m.recover(plan,state,proof)


def test_mismatched_original_state_binding_refused(bundle):
    plan,state,proof=bundle
    proof['original_state_binding']=repacket(proof['original_state_binding'],lambda p:p.update(state_file_sha256='0'*64))
    with pytest.raises(ValueError,match='state/binding join'):m.recover(plan,state,proof)


def test_wrong_historical_recovery_chain_locator_refused(bundle):
    plan,state,proof=bundle;proof['old_recovery']['sha256']='0'*64
    with pytest.raises(ValueError,match='training0000 amendment'):m.recover(plan,state,proof)


def test_regenerated_intent_must_match_original_in_every_frozen_field(bundle,monkeypatch):
    plan,state,proof=bundle;original=dispatch._intent
    def changed(*args,**kwargs):
        result=original(*args,**kwargs);result['max_wall_seconds']=1;return result
    monkeypatch.setattr(dispatch,'_intent',changed)
    with pytest.raises(ValueError,match='exact frozen intention'):m.recover(plan,state,proof)


def test_independent_preservation_guard_refuses_dispatcher_mutation_of_other_job(bundle,monkeypatch):
    plan,state,proof=bundle;original=dispatch._finish
    def changed(candidate):
        candidate['jobs']['training-0001']['preserved_opaque_history']['attempts'].append('unrelated mutation')
        return original(candidate)
    monkeypatch.setattr(dispatch,'_finish',changed)
    with pytest.raises(ValueError,match='Unrelated state field'):m.recover(plan,state,proof)


@pytest.mark.parametrize('key',['evaluation_plan','evaluation_plan_binding'])
def test_both_evaluation_plan_source_packets_are_byte_bound(bundle,key):
    plan,state,proof=bundle;proof['failed']['evaluation-0073-01'][key]['raw']+=b' '
    with pytest.raises(ValueError,match='byte-bound'):m.recover(plan,state,proof)


def test_partial_conditions_refused_even_with_resealed_synthetic_state(bundle,monkeypatch):
    plan,state,proof=bundle;state['cells'][73]['conditions']['0']={'partial':'forbidden'};seal_state(state)
    monkeypatch.setattr(m,'EXPECTED_STATE_SHA256',state['state_sha256'])
    with pytest.raises(ValueError,match='empty first evaluation wave'):m.recover(plan,state,proof)


@pytest.mark.parametrize('raw',[b'{"source":1,"source":2}',b'{"source":NaN}',b'{"source":1e9999}'])
def test_duplicate_and_nonfinite_json_refused(raw):
    p=packet(raw,m.CASE/'current-user-authorization.json',raw=True)
    with pytest.raises(ValueError):m.packet(p,expected=m.CASE/'current-user-authorization.json')


def test_compressed_packet_has_separate_compressed_and_plaintext_bounds():
    raw=gzip.compress(b'"'+b'x'*1000+b'"',mtime=0);p=packet(raw,m.CASE/'original-state.json.gz',raw=True)
    value,_=m.packet(p,expected=m.CASE/'original-state.json.gz',encoding='gzip-json',maximum=100,expanded_maximum=2000)
    assert value=='x'*1000
    with pytest.raises(ValueError,match='Bounded original compressed'):
        m.packet(p,expected=m.CASE/'original-state.json.gz',encoding='gzip-json',maximum=100,expanded_maximum=100)


def test_pure_helper_never_opens_a_file_or_submits_a_process(bundle,monkeypatch):
    import builtins
    import subprocess
    plan,state,proof=bundle
    def forbidden(*args,**kwargs):raise AssertionError('Pure helper attempted external access')
    monkeypatch.setattr(builtins,'open',forbidden);monkeypatch.setattr(subprocess,'run',forbidden)
    assert m.recover(plan,state,proof)['state']['revision']==406
