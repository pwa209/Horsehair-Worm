"""Synthetic proof bundles only; no native operations or scientific checkpoints."""
from pathlib import Path
import copy
import gzip
import importlib.util
import json
import shlex

import pytest

from horsehair import agents,agent_campaign as d
from horsehair.core import canonical_hash,read_json
from horsehair.execution_resources import LIMITS

spec=importlib.util.spec_from_file_location('target_only_migration',Path(__file__).with_name('recover-f2-training0000-state-20261021.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def packet(value,path,*,binary=False):
    raw=value if binary else json.dumps(value,sort_keys=True,allow_nan=False).encode()
    return {'path':str(path),'sha256':m.raw_sha(raw),'raw':raw}


def value(packet):return json.loads(packet['raw'])


def change_packet(packet,change):
    body=value(packet);change(body);return globals()['packet'](body,packet['path'])


def native(job,cpus,gpus,state='COMPLETED',exit_code='0:0',elapsed=60):
    return {'job_id':job,'state':state,'exit_code':exit_code,'cpus':cpus,'gpus':gpus,'elapsed_seconds':elapsed,
        'user':'pwa209','account':'def-ptewarie_gpu' if gpus else 'def-ptewarie_cpu','end':'2026-10-21T15:00:00'}


@pytest.fixture
def bundle(monkeypatch):
    config=read_json(Path(agents.__file__).resolve().parents[2]/'configs/study.json')
    source={'primary_source_sha256':m.PRIMARY,'primary_agents_sha256':'a'*64,'execution_source_sha256':m.SOURCE,'evaluation_module_sha256':'c'*64}
    protocol={'config_sha256':agents._digest(config['agents']),'full_config_sha256':canonical_hash(config),'implementation_sha256':'a'*64,
        'H4_inference_specification':agents.h4_specification(config),'learning_rate':.0003,'final_seeds':config['agents']['final_seeds'],'biological_directions':None}
    policy={'path':str(m.ROOT/'maintenance/execution-resource-policies/synthetic/policy.json'),'sha256':'d'*64,
        'execution_source_sha256':m.SOURCE,'base_config_sha256':m.CONFIG,'primary_scientific_source_sha256':m.PRIMARY,
        'limits':dict(LIMITS),'scientific_gates_changed':False,'approval':{'source':'current_user','authorizes_cpu_gpu_resource_amendment':True}}
    plan=d.plan_campaign(config,protocol,protocol_binding={'path':str(m.ROOT/'results/synthetic-frozen-protocol.json'),'sha256':'e'*64},
        source_binding=source,policy_binding=policy,case=str(m.CAMPAIGN))
    descriptor={'worker_sha256':m.WORKER_SHA,'worker_path':str(m.ROOT/'maintenance/study-af-campaign-v1-20261021/native-worker.py'),
        'execution_source_sha256':m.SOURCE,'agent_campaign_case':str(m.CAMPAIGN),'frozen_protocol_sha256':'e'*64,
        'resource_policy':{'path':policy['path'],'sha256':policy['sha256']},'synthetic_test_fixture':True}
    descriptor_packet=packet(descriptor,m.WORKER_DESCRIPTOR);monkeypatch.setattr(m,'WORKER_DESCRIPTOR_SHA',descriptor_packet['sha256'])
    state,intents=d.dispatch_tick(plan,d.initial_state(plan),{},{});jobs=[m.FAILED,*m.HEALTHY.values()]
    for intent,job in zip(intents,jobs):state=d.record_submission(plan,state,intent['intent_id'],job,expected_revision=state['revision'])
    healthy={};receipts={};proofs={m.FAILED:native(m.FAILED,6,1,'FAILED','1:0',38)}
    for index,job in m.HEALTHY.items():
        cell=plan['cells'][int(index)];checkpoint=packet(('SYNTHETIC-CHECKPOINT-'+index).encode(),m.PurePosixPath(cell['training_case'])/'checkpoint.pt',binary=True)
        summary={k:cell[k] for k in ('task','architecture','seed')}
        summary.update(training_steps=config['agents']['total_steps'],complete_training_budget=True,implementation_sha256='a'*64,
            config_sha256=agents._digest(config['agents']),learning_rate=.0003,checkpoint_sha256=checkpoint['sha256'],weight_sha256='f'*64)
        summary_packet=packet(summary,m.PurePosixPath(cell['training_case'])/'summary.json')
        worker={'status':'complete_native_work','mode':'agent-train','index':int(index),'native_job_id':job,
            'descriptor_sha256':m.WORKER_DESCRIPTOR_SHA,'source_sha256':m.SOURCE,'full_config_sha256':m.CONFIG,'resource_policy':policy,
            'training_summary_path':summary_packet['path'],'training_summary_sha256':summary_packet['sha256'],'training_summary':summary,
            'allocation':{'JobId':job,'UserId':'pwa209(3164768)','Account':'def-ptewarie_gpu','NumCPUs':'6'},
            'scientific_result':False,'admission_authority':False}
        worker_packet=packet(worker,m.ROOT/'maintenance/study-af-campaign-v1-20261021'/('native-'+job)/'receipt.json')
        healthy[index]={'native':packet(native(job,6,1),m.CASE/'native'/('worker-'+job+'.json')),
            'worker_receipt':worker_packet,'summary':summary_packet,'checkpoint':checkpoint}
        ident=f'training-{int(index):04d}';receipts[ident]={'campaign_plan_sha256':plan['plan_sha256'],'intent_id':ident,'native_job_id':job,
            'case':cell['training_case'],'sha256':worker_packet['sha256'],'value':summary}
        proofs[job]=native(job,6,1)
    state,new=d.dispatch_tick(plan,state,proofs,receipts);assert not new
    failed_intent=state['jobs'][m.TARGET]['intent'];name='af-f2-training-0000'
    argv=['python',descriptor['worker_path'],'--descriptor',str(m.WORKER_DESCRIPTOR),'--descriptor-sha256',m.WORKER_DESCRIPTOR_SHA,
        '--mode','agent-train','--index','0','--cpus','6','--gpus','1','--hours','12']
    command=shlex.join(argv);script='#!/bin/bash\n#SBATCH --hold\n'+command+'\n'
    script_packet=packet(script.encode(),m.ROOT/'plans'/name/(name+'.sbatch'),binary=True)
    task={'name':name,'script':script_packet['path'],'script_sha256':canonical_hash(script),
        'resources':{'cpus':6,'gpu':True,'hours':12,'memory':'64G'},'command':command}
    native_plan={'root':str(m.ROOT),'phase':name,'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},
        'campaign_plan_sha256':plan['plan_sha256'],'intent_sha256':failed_intent['intent_sha256'],
        'worker_descriptor_sha256':m.WORKER_DESCRIPTOR_SHA,'tasks':[task]}
    ledger={'source_sha256':m.SOURCE,'plan_sha256':canonical_hash(native_plan),
        'tasks':{name:{'job_id':m.FAILED,'state':'submitted','script_sha256':task['script_sha256']}}}
    traceback="Traceback (most recent call last):\n admission(...area,'native-start-admission'...)\n all_registered_work\n"+m.FAILURE_MARKER+'\n'
    absence_paths=[m.PurePosixPath(plan['cells'][0]['training_case'])/'checkpoint.pt',m.PurePosixPath(plan['cells'][0]['training_case'])/'summary.json',
        m.ROOT/'maintenance/study-af-campaign-v1-20261021'/('native-'+m.FAILED)/'receipt.json',m.ROOT/'plans'/m.NAMESPACE,
        m.ROOT/'state'/(m.NAMESPACE+'-submission.json'),m.CAMPAIGN/'submission-r1-training-0000.json']
    absence={'registration_mutex':str(m.ROOT/'state/af-campaign-registration.mutex'),'registration_mutex_held':True,
        'native_campaign_controller_ids':sorted(m.CONTROLLERS),'captured_original_state_sha256':state['state_sha256'],'captured_unix':1792610000.,
        'entries':[{'path':str(p),'exists':False,'is_symlink':False} for p in absence_paths]}
    controllers={job:packet(native(job,0 if job=='6689324' else 1,0,'CANCELLED' if job=='6689324' else 'FAILED',
        '0:0' if job=='6689324' else '1:0',0 if job=='6689324' else 90),m.CASE/'native'/('controller-'+job+'.json')) for job in m.CONTROLLERS}
    failed={'native':packet(proofs[m.FAILED],m.CASE/'native'/('worker-'+m.FAILED+'.json')),
        'native_plan':packet(native_plan,m.ROOT/'plans'/name/'plan.json'),'submission_ledger':packet(ledger,m.ROOT/'state'/(name+'-submission.json')),
        'script':script_packet,'traceback':packet(traceback.encode(),m.ROOT/'logs/af-f2-training-0000-6689664_4294967294.log',binary=True),
        'absence':packet(absence,m.CASE/'verified-absence-under-lock.json')}
    proof={'case':str(m.CASE),'dispatcher_sha256':m.DISPATCH_SHA,
        'approval':{'source':'current_user','authorizes_targeted_operational_repair':True,'request_reference':'Synthetic test of already authorized repair'},
        'worker_descriptor':descriptor_packet,'failed':failed,'old_controllers':controllers,'healthy':healthy,
        'clock_erratum':packet({'status':'no_clock_correction_required','jobs':{}},m.CASE/'clock-erratum.json')}
    records={job:value(item) for job,item in controllers.items()}
    records.update({m.FAILED:proofs[m.FAILED],**{job:proofs[job] for job in m.HEALTHY.values()}})
    raw_records={}
    for job,record in records.items():
        raw_records[job]={key:item for key,item in record.items() if key not in {'job_id','gpus'}}
        raw_records[job].update(job_id_raw=job,unallocated_metadata=False,
            alloc_tres='cpu='+str(record['cpus'])+(',gres/gpu=1,gres/gpu:a100=1' if record['gpus'] else ''))
    proof['native_evidence_packet']=packet({'schema_version':1,'native_timezone':'UTC','no_source_evidence_modified':True,
        'native_observed':raw_records,'synthetic_test_fixture':True},m.NATIVE_EVIDENCE)
    freeze_state(plan,state,proof)
    return plan,state,proof


def freeze_state(plan,state,proof):
    state.pop('state_sha256',None);state['state_sha256']=canonical_hash(state)
    compressed=gzip.compress(json.dumps(state,sort_keys=True).encode(),mtime=0)
    proof['original_state_gzip']=packet(compressed,m.CASE/'original-state.json.gz',binary=True)
    proof['original_state_binding']=packet({'state_file_sha256':m.raw_sha(compressed),'state_sha256':state['state_sha256'],
        'campaign_plan_sha256':plan['plan_sha256'],'revision':state['revision'],'scientific_result':False,'admission_authority':False},m.CASE/'original-state-binding.json')
    proof['original_campaign_plan']=packet(plan,m.CASE/'original-campaign-plan.json')
    proof['failed']['absence']=change_packet(proof['failed']['absence'],lambda body:body.update(captured_original_state_sha256=state['state_sha256']))


def test_target_only_derivative_preserves_original_failure_and_all_three_healthy_jobs(bundle):
    plan,state,proof=bundle;before=copy.deepcopy((plan,state,proof))
    result=m.migrate_training0000(plan,state,proof);new=result['state'];amendment=result['amendment']
    assert (plan,state,proof)==before and new['revision']==state['revision']+1
    assert new['status']=='active' and new['needs_repair']==[] and new['cells'][0]['training_job'] is None
    assert new['cells'][1:]==state['cells'][1:] and new['jobs']=={k:v for k,v in state['jobs'].items() if k!=m.TARGET}
    assert amendment['failed_history_preserved']==state['jobs'][m.TARGET]
    assert amendment['native_namespace_overrides']=={m.TARGET:m.NAMESPACE}
    assert amendment['submission_journal_overrides']=={m.TARGET:'submission-r1-training-0000.json'}
    assert amendment['native_submission_performed'] is amendment['source_files_changed_or_deleted'] is False
    assert amendment['healthy_policies_retrained'] is amendment['original_native_failure_relabelled'] is False
    assert amendment['scientific_result'] is amendment['admission_authority'] is False
    assert amendment['amendment_sha256']==canonical_hash({k:v for k,v in amendment.items() if k!='amendment_sha256'})
    d._state(plan,new)


def test_public_dispatcher_can_continue_without_retraining_healthy_original_indices(bundle):
    plan,state,proof=bundle;result=m.migrate_training0000(plan,state,proof)
    new,intents=d.dispatch_tick(plan,result['state'],{},{});training={i['cell_index'] for i in intents if i['kind']=='training'}
    assert 0 in training and not training & {1,2,3}
    assert {new['jobs'][f'training-{i:04d}']['job_id'] for i in (1,2,3)}==set(m.HEALTHY.values())


def test_submitted_healthy_state_is_preserved_for_normal_positive_receipt_processing(bundle):
    plan,state,proof=bundle
    for index in (1,2,3):
        job=state['jobs'][f'training-{index:04d}'];job['status']='submitted';job['receipt']=None
        job['native_proof']=dict(job['native_proof'],state='RUNNING')
    freeze_state(plan,state,proof);new=m.migrate_training0000(plan,state,proof)['state']
    assert new['jobs']=={k:v for k,v in state['jobs'].items() if k!=m.TARGET}


@pytest.mark.parametrize('change',['other_repair','ambiguous','failed_relabel','foreign_job','eval_started','healthy_failed','changed_receipt'])
def test_unrelated_changes_or_ambiguity_are_not_automatically_repaired(bundle,change):
    plan,state,proof=bundle
    if change=='other_repair':state['needs_repair'].append('training-0001')
    elif change=='ambiguous':state['jobs']['training-0001'].update(job_id=None,status='ambiguous',native_proof=None,receipt=None)
    elif change=='failed_relabel':state['jobs'][m.TARGET]['status']='submitted'
    elif change=='foreign_job':state['jobs'][m.TARGET]['job_id']='999999'
    elif change=='eval_started':state['cells'][1]['conditions']={'foreign':{}}
    elif change=='healthy_failed':state['jobs']['training-0001']['status']='needs_repair'
    else:state['jobs']['training-0001']['receipt']['sha256']='0'*64
    freeze_state(plan,state,proof)
    with pytest.raises(ValueError):m.migrate_training0000(plan,state,proof)


@pytest.mark.parametrize('group',['failed','healthy','controller'])
def test_nonterminal_or_wrong_native_resources_never_allow_recovery(bundle,group):
    plan,state,proof=bundle
    if group=='failed':proof['failed']['native']=change_packet(proof['failed']['native'],lambda body:body.update(cpus=5))
    elif group=='healthy':proof['healthy']['1']['native']=change_packet(proof['healthy']['1']['native'],lambda body:body.update(state='RUNNING'))
    else:
        key=next(iter(proof['old_controllers']));proof['old_controllers'][key]=change_packet(proof['old_controllers'][key],lambda body:body.update(state='RUNNING'))
    with pytest.raises(ValueError):m.migrate_training0000(plan,state,proof)


@pytest.mark.parametrize('part',['original_state_gzip','original_state_binding','original_campaign_plan','worker_descriptor'])
def test_exact_original_byte_packets_cannot_be_detached_from_values(bundle,part):
    plan,state,proof=bundle;proof[part]['raw']+=b'corrupted'
    with pytest.raises(ValueError):m.migrate_training0000(plan,state,proof)


@pytest.mark.parametrize('part',['native_plan','submission_ledger','script','traceback','absence'])
def test_exact_failed_producer_inputs_are_all_required(bundle,part):
    plan,state,proof=bundle;proof['failed'][part]['raw']+=b'changed'
    with pytest.raises(ValueError):m.migrate_training0000(plan,state,proof)


@pytest.mark.parametrize('part',['worker_receipt','summary','checkpoint'])
def test_healthy_producer_summary_and_checkpoint_original_bytes_are_preserved(bundle,part):
    plan,state,proof=bundle;proof['healthy']['2'][part]['raw']+=b'changed'
    with pytest.raises(ValueError):m.migrate_training0000(plan,state,proof)


@pytest.mark.parametrize('change',['lock','already_exists','symlink','missing_entry','stale_state','controllers'])
def test_absence_is_exact_target_only_and_registration_lock_bound(bundle,change):
    plan,state,proof=bundle
    def mutate(body):
        if change=='lock':body['registration_mutex_held']=False
        elif change=='already_exists':body['entries'][0]['exists']=True
        elif change=='symlink':body['entries'][0]['is_symlink']=True
        elif change=='missing_entry':body['entries'].pop()
        elif change=='stale_state':body['captured_original_state_sha256']='0'*64
        else:body['native_campaign_controller_ids'].pop()
    proof['failed']['absence']=change_packet(proof['failed']['absence'],mutate)
    with pytest.raises(ValueError):m.migrate_training0000(plan,state,proof)


def apply_clock_erratum(plan,state,proof):
    corrections={}
    for job,record in [(m.FAILED,proof['failed']['native']),*[(job,proof['healthy'][index]['native']) for index,job in m.HEALTHY.items()]]:
        before=value(record);after=dict(before,end='2026-10-21T19:00:00')
        corrections[job]={'old_end':before['end'],'new_end':after['end'],'all_other_native_fields_unchanged':True,'observed_shift_seconds':14400}
        if job==m.FAILED:proof['failed']['native']=packet(after,record['path'])
        else:
            index=next(index for index,ident in m.HEALTHY.items() if ident==job);proof['healthy'][index]['native']=packet(after,record['path'])
        proof['native_evidence_packet']=change_packet(proof['native_evidence_packet'],
            lambda body:body['native_observed'][job].update(end=after['end']))
    proof['clock_erratum']=packet({'status':'source_time_basis_erratum','old_timezone_verified':False,
        'old_timezone':'unverified_naive_slurm_local','new_timezone':'UTC',
        'new_query_environment':{'TZ':'UTC'},'jobs':corrections},m.CASE/'clock-erratum.json')


def test_clock_erratum_changes_only_terminal_end_and_preserves_original_raw_proofs(bundle):
    plan,state,proof=bundle;before=copy.deepcopy(state);apply_clock_erratum(plan,state,proof)
    result=m.migrate_training0000(plan,state,proof);assert state==before
    for index,job in m.HEALTHY.items():
        old=state['jobs'][f'training-{int(index):04d}'];new=result['state']['jobs'][f'training-{int(index):04d}']
        expected=copy.deepcopy(old);expected['native_proof']['end']='2026-10-21T19:00:00';assert new==expected
        assert result['amendment']['native_clock_errata_preserving_original_proofs'][job]['old']==old['native_proof']
    assert result['amendment']['failed_history_preserved']==state['jobs'][m.TARGET]


@pytest.mark.parametrize('change',['wrong_offset','resources','missing_erratum','unused_erratum'])
def test_clock_erratum_is_not_a_general_native_proof_rewrite(bundle,change):
    plan,state,proof=bundle;apply_clock_erratum(plan,state,proof)
    if change=='wrong_offset':proof['healthy']['1']['native']=change_packet(proof['healthy']['1']['native'],lambda body:body.update(end='2026-10-21T20:00:00'))
    elif change=='resources':proof['healthy']['1']['native']=change_packet(proof['healthy']['1']['native'],lambda body:body.update(elapsed_seconds=61))
    elif change=='missing_erratum':proof['clock_erratum']=change_packet(proof['clock_erratum'],lambda body:body['jobs'].pop(m.HEALTHY['1']))
    else:proof['clock_erratum']=change_packet(proof['clock_erratum'],lambda body:body['jobs'].update(foreign={}))
    with pytest.raises(ValueError):m.migrate_training0000(plan,state,proof)


def test_recovery_is_single_use_and_requires_original_failed_state(bundle):
    plan,state,proof=bundle;new=m.migrate_training0000(plan,state,proof)['state']
    with pytest.raises(ValueError):m.migrate_training0000(plan,new,proof)


@pytest.mark.parametrize('change',['timezone','alias','contradictory_gpu','state_flag','approval'])
def test_native_source_provenance_and_inference_guards_remain_strict(bundle,change):
    plan,state,proof=bundle
    if change=='timezone':proof['native_evidence_packet']=change_packet(proof['native_evidence_packet'],lambda body:body.update(native_timezone='unverified'))
    elif change=='alias':proof['native_evidence_packet']=change_packet(proof['native_evidence_packet'],
        lambda body:body['native_observed'][m.FAILED].update(job_id_raw=m.FAILED+'_1'))
    elif change=='contradictory_gpu':proof['native_evidence_packet']=change_packet(proof['native_evidence_packet'],
        lambda body:body['native_observed'][m.FAILED].update(alloc_tres='cpu=6,gres/gpu=1,gres/gpu:a100=2'))
    elif change=='state_flag':state['scientific_result']=True;freeze_state(plan,state,proof)
    else:proof['approval']['authorizes_targeted_operational_repair']=False
    with pytest.raises(ValueError):m.migrate_training0000(plan,state,proof)
