"""Synthetic whole-clock math and finite supervision; no remote/source data."""
from pathlib import Path
import copy
import importlib.util
import json
import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
import time
from contextlib import contextmanager

import numpy as np
import pytest
import torch

from horsehair import duration_adjoint as adjoint,semimarkov as s
from horsehair.core import digest

spec=importlib.util.spec_from_file_location('native_record_probe',Path(__file__).with_name('run-full-record-adjoint-v3-20261021.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


@pytest.fixture(autouse=True)
def one_thread():
    previous=torch.get_num_threads();torch.set_num_threads(1)
    yield
    torch.set_num_threads(previous)


def clock(ident,experiment='training-group',length=17):
    rng=np.random.default_rng(810+int(ident));mask=np.ones(length,dtype=bool);mask[2:4]=False;mask[-2]=False
    y=rng.normal(size=(length,2))*.3;y[~mask]=np.nan
    return s.Sequence(str(ident),experiment,y,rng.normal(size=(length,2)),rng.normal(size=(length,1)),
        rng.normal(size=(length,2))*.1,rng.normal(size=(length,2))*.1,rng.normal(size=(length,2))*.1,
        mask,700.+np.arange(length)*10.,bin_seconds=10.)


def model(states=2):
    torch.manual_seed(13)
    value=s.MovementModel('MX',2,1,knots=[1.,2.,3.],animal_ids=['0','1','full-fold-unused'],
        experiment_ids=['training-group','full-fold-unused-group'],states=states,max_duration_bins=None)
    with torch.no_grad():
        for parameter in value.parameters():parameter.add_(torch.randn_like(parameter)*.05)
    return value


@pytest.mark.parametrize('states',[2,3])
@pytest.mark.parametrize('stage',m.STAGES)
def test_full_clock_scores_all_gradients_and_one_update_are_primary_equivalent(states,stage):
    value=model(states);baseline=copy.deepcopy(value.state_dict())
    sequence=clock(8,'unseen-group') if stage.startswith('validation_') else clock(0)
    result,artifacts=m.paired_stage(s,adjoint,value,sequence,100,{'warmup_seconds':20,'gradient_clip':.0001,'learning_rate':.001},
        stage,time.monotonic()+60)
    assert result['status']=='paired_complete' and result['one_record_component_only'] is True
    assert result['global_training_fold_normalizer']==100
    contexts=3**states if stage.startswith('validation_') else 1
    assert all(part['full_clock_bins']==len(sequence.times) and part['contexts']==contexts for part in result['measurements'])
    assert artifacts[0]['log_scores'].shape==(contexts,len(sequence.times))
    assert all(value['max_tolerance_ratio']<=1 for value in result['comparisons'].values())
    for name,tensor in value.state_dict().items():assert torch.equal(tensor,baseline[name])
    if 'gradients' in stage:
        assert set(result['all_named_gradients'])==dict(value.named_parameters()).keys()
        assert result['all_named_gradients']['animal_z']['shape']==[3,states]
        assert result['all_named_gradients']['experiment_z']['shape']==[2,states]
    if 'Adam_update' in stage:
        assert result['clip_calls_per_implementation']==result['Adam_update_calls_per_implementation']==1
        assert set(result['one_original_Adam_update'])==value.state_dict().keys()


def test_component_uses_full_prior_once_and_entire_training_fold_normalizer():
    value=model();sequence=clock(0);normalizer=100
    source=value(sequence);expected=(-value.log_prior()-source['log_likelihood'].sum())/normalizer
    result,artifacts=m.paired_stage(s,adjoint,value,sequence,normalizer,{'gradient_clip':5},
        m.STAGES[2],time.monotonic()+60)
    torch.testing.assert_close(artifacts[0]['objective'],expected.detach(),rtol=1e-12,atol=1e-12)
    assert result['prior_in_training_objective_exactly_once'] is True
    # All unused nuisance rows still have their original Gaussian-prior gradient.
    expected_unused=value.animal_z.detach()[-1]/normalizer
    torch.testing.assert_close(artifacts[0]['gradients']['animal_z'][-1],expected_unused)
    assert not torch.isclose(expected,expected*normalizer/int(sequence.observed.sum()))


def test_validation_matches_original_marginal_warmup_log_score_without_prior():
    value=model();sequence=clock(8,'unseen-group');settings={'warmup_seconds':20}
    effects,weights=value.predictive_random_effects(sequence,3);source=value(sequence,effects,return_filter=True)
    cumulative=source['log_scores'].cumsum(1);marginal=torch.logsumexp(cumulative+weights[:,None],0)
    increments=torch.cat([marginal[:1],marginal[1:]-marginal[:-1]])
    eligible=sequence.observed & (sequence.times-sequence.times[0]>=20)
    expected=-increments[torch.as_tensor(eligible)].mean()
    result,artifacts=m.paired_stage(s,adjoint,value,sequence,100,settings,m.STAGES[3],time.monotonic()+60)
    torch.testing.assert_close(artifacts[0]['objective'],expected.detach(),rtol=1e-12,atol=1e-12)
    assert result['validation_objective_has_no_prior'] is True and result['prior_in_training_objective_exactly_once'] is False


def test_clock_and_normalizer_drift_are_rejected_without_resampling():
    sequence=clock(0);sequence.times[4]+=1
    with pytest.raises(ValueError,match='regular clock'):
        m.paired_stage(s,adjoint,model(),sequence,100,{},m.STAGES[0],time.monotonic()+30)
    with pytest.raises(ValueError,match='normalizer'):
        m.paired_stage(s,adjoint,model(),clock(0),1,{},m.STAGES[0],time.monotonic()+30)


def test_selected_whole_clock_uses_longest_mask_eligible_source_id_and_stable_tie_break():
    roster=[{'animal_id':'a','observed_any':True,'bins':100}, {'animal_id':'b','observed_any':True,'bins':100},
            {'animal_id':'short','observed_any':True,'bins':80},{'animal_id':'all-missing-long','observed_any':False,'bins':1000}]
    allowed=['a','b','short'];left=m.select_clock(roster,allowed,salt='test:')
    right=m.select_clock(list(reversed(roster)),list(reversed(allowed)),salt='test:')
    assert left==right and left['animal_id'] in {'a','b'} and left['bins']==100
    assert 'outcomes not inspected' in left['selection']
    with pytest.raises(ValueError,match='Complete eligible'):
        m.select_clock(roster,['all-missing-long'],salt='test:')


def test_saved_storage_counts_views_once_and_rejects_oversized_real_autograd_graph():
    tensor=torch.ones(20,dtype=torch.float64,requires_grad=True)
    guard=m.MemoryGuard(time.monotonic()+30,limit=200,rss=lambda:0)
    guard.pack(tensor);guard.pack(tensor[:3]);assert guard.saved_bytes==160
    with pytest.raises(m.MemoryBound,match='saved storage'):guard.pack(torch.ones(30,dtype=torch.float64))
    with pytest.raises(m.MemoryBound):
        with m.MemoryGuard(time.monotonic()+30,limit=1,rss=lambda:0):(tensor.square().sum()).backward()
    assert tensor.grad is None


@pytest.mark.parametrize('cause',['rss','deadline'])
def test_guard_rejects_before_numeric_work(cause):
    guard=m.MemoryGuard(10.,limit=100,rss=lambda:100 if cause=='rss' else 0,clock=lambda:10. if cause=='deadline' else 0.)
    with pytest.raises(m.DeadlineBound if cause=='deadline' else m.MemoryBound):guard.check()


@pytest.mark.parametrize('change',['nonfinite','mismatch','shape','dtype'])
def test_false_numeric_equivalence_is_never_accepted(change):
    left=torch.ones(2,dtype=torch.float64);right=left.clone()
    if change=='nonfinite':right[0]=float('inf')
    elif change=='mismatch':right[0]+=1.
    elif change=='shape':right=right[:1]
    else:right=right.float()
    with pytest.raises(ValueError):m.compare_tensor(left,right)


def test_all_named_gradient_roster_presence_must_match():
    left=model();right=copy.deepcopy(left);left.log_prior().backward()
    with pytest.raises(ValueError,match='Gradient presence'):m.compare_gradients(left,right)


class FakeProcess:
    pid=54321
    def __init__(self,code=None):self.returncode=code;self.terminated=False
    def poll(self):return self.returncode
    def terminate(self):self.terminated=True;self.returncode=-15
    def kill(self):self.terminated=True;self.returncode=-9
    def wait(self,timeout=None):return self.returncode


def terminal_fixture(case,*,count=4,status='complete_one_record_first_order_probe'):
    case.mkdir();(case/'plan.json').write_text('{"synthetic":true}')
    completed=[]
    for stage in m.STAGES[:count]:
        artifact=case/(stage+'.pt');artifact.write_bytes(b'SYNTHETIC-unit-test-artifact-not-source-data')
        value={'stage':stage,'status':'paired_complete','artifacts_path':str(artifact),'artifacts_sha256':m.digest(artifact),
               'plan_sha256':m.digest(case/'plan.json'),**m.FALSE_FLAGS}
        (case/(stage+'.json')).write_text(json.dumps(value));completed.append(value)
    result={'status':status,'completed_stages':completed,'primary_source_sha256':m.PRIMARY_SHA,
        'framework_source_sha256':m.SOURCE,'full_config_sha256':m.CONFIG,**m.FALSE_FLAGS}
    (case/'worker-result.json').write_text(json.dumps(result));return result


def test_complete_receipt_requires_all_original_stages_and_exact_artifact_bytes(tmp_path):
    case=tmp_path/'case';result=terminal_fixture(case)
    m.validate_worker_result(result,case)
    (case/(m.STAGES[0]+'.pt')).write_bytes(b'tampered')
    with pytest.raises(ValueError,match='completed-stage bytes'):m.validate_worker_result(result,case)


@pytest.mark.parametrize('change',['missing','duplicate','out_of_order','admission','source'])
def test_partial_stages_cannot_be_disguised_as_complete_or_unbound(tmp_path,change):
    case=tmp_path/'case';result=terminal_fixture(case)
    if change=='missing':result['completed_stages'].pop()
    elif change=='duplicate':result['completed_stages'][-1]=result['completed_stages'][0]
    elif change=='out_of_order':result['completed_stages'].reverse()
    elif change=='admission':result['admission_authority']=True
    else:result['primary_source_sha256']='0'*64
    with pytest.raises(ValueError):m.validate_worker_result(result,case)


def test_memory_partial_preserves_completed_prefix_and_never_claims_whole_workload(tmp_path):
    case=tmp_path/'case';result=terminal_fixture(case,count=2,status='memory_bound_not_compared')
    m.validate_worker_result(result,case)
    assert result['complete_declared_workload'] is result['whole_cohort_objective_qualified'] is False


@pytest.mark.parametrize('cause',['deadline','rss','missing_result','worker_failed'])
def test_supervisor_stops_bounded_child_and_propagates_real_failure(tmp_path,monkeypatch,cause):
    case=tmp_path/'case';case.mkdir();process=FakeProcess(1 if cause in {'missing_result','worker_failed'} else None)
    if cause=='worker_failed':
        (case/'worker-result.json').write_text(json.dumps({'status':'worker_failed','completed_stages':[],
            'primary_source_sha256':m.PRIMARY_SHA,'framework_source_sha256':m.SOURCE,'full_config_sha256':m.CONFIG,**m.FALSE_FLAGS}))
    clock=[0.]
    def sleep(seconds):clock[0]+=seconds
    def stop(child):child.terminated=True;child.returncode=-15
    monkeypatch.setattr(m,'stop',stop)
    result=m.supervise(['not-executed'],{},2.,case,popen=lambda *a,**k:process,
        clock=lambda:clock[0],sleeper=sleep,rss=lambda pid:m.MEMORY_LIMIT if cause=='rss' else 100)
    expected={'deadline':'bounded_deadline_reached','rss':'memory_bound_not_compared',
              'missing_result':'worker_failed','worker_failed':'worker_failed'}[cause]
    assert result['status']==expected and all(result[key] is False for key in m.FALSE_FLAGS)
    assert process.terminated is (cause in {'deadline','rss'})


def test_successful_supervisor_rechecks_complete_worker_artifacts(tmp_path):
    case=tmp_path/'case';terminal_fixture(case)
    process=FakeProcess(0)
    result=m.supervise(['not-executed'],{},100.,case,popen=lambda *a,**k:process)
    assert result['status']=='complete_one_record_first_order_probe' and result['worker_returncode']==0


def test_exit_between_poll_and_rss_read_retains_positive_worker_receipt(tmp_path):
    case=tmp_path/'case';terminal_fixture(case);process=FakeProcess()
    def missing_rss(pid):process.returncode=0;raise ProcessLookupError('Synthetic child just exited')
    result=m.supervise(['not-executed'],{},time.monotonic()+100,case,popen=lambda *a,**k:process,rss=missing_rss)
    assert result['status']=='complete_one_record_first_order_probe' and result['worker_returncode']==0


def test_killed_worker_keeps_validated_completed_prefix_progress(tmp_path,monkeypatch):
    case=tmp_path/'case';partial=terminal_fixture(case,count=2,status='bounded_deadline_reached')
    (case/'worker-result.json').unlink();partial['status']='whole_record_pair_running'
    (case/'progress.json').write_text(json.dumps(partial));process=FakeProcess()
    monkeypatch.setattr(m,'stop',lambda child:setattr(child,'returncode',-15))
    result=m.supervise(['not-executed'],{},1.,case,popen=lambda *a,**k:process,clock=lambda:2.,rss=lambda pid:0)
    assert result['status']=='bounded_deadline_reached' and len(result['preserved_partial_progress']['value']['completed_stages'])==2






def test_current_process_rss_has_actual_positive_evidence():
    assert type(m.rss_bytes()) is int and m.rss_bytes()>0


def test_actual_shared_kernel_contention_is_bounded_then_releases(tmp_path,monkeypatch):
    from horsehair.core import exclusive_file_lock,ReceiptLockTimeout
    root=tmp_path/'scratch';(root/'state').mkdir(parents=True);(root/'maintenance').mkdir();monkeypatch.setattr(m,'ROOT',root)
    path=root/'state/af-campaign-registration.mutex';calls=[];elapsed=[0.]
    @contextmanager
    def bounded_lock(given,timeout):
        assert given==path.resolve() and 0<timeout<=30
        try:
            with exclusive_file_lock(given,timeout=.05):yield
        except ReceiptLockTimeout:
            elapsed[0]+=301;raise
    with exclusive_file_lock(path,timeout=1):
        with pytest.raises(m.DeadlineBound):m.shared_admission(lambda:calls.append('admission'),10000.,
            evidence_dir=root/'maintenance/failed',lock_factory=bounded_lock,wall=lambda:0.,clock=lambda:elapsed[0])
    assert not calls and path.exists()
    value,evidence=m.shared_admission(lambda:calls.append('admission') or 'positive',time.time()+1000,
        evidence_dir=root/'maintenance/success',lock_factory=bounded_lock)
    assert value=='positive' and calls==['admission'] and evidence['released_before_numerical_execution'] is True
    assert json.loads((root/'maintenance/failed/result.json').read_text())['status']=='pending_registration_no_probe_started'
    with exclusive_file_lock(path,timeout=.1):pass


def test_registration_lock_never_wraps_numerics_and_releases_on_read_error(tmp_path,monkeypatch):
    root=tmp_path/'scratch';(root/'maintenance').mkdir(parents=True);monkeypatch.setattr(m,'ROOT',root);events=[];held=[False]
    @contextmanager
    def lock(path,timeout):
        held[0]=True;events.append('lock')
        try:yield
        finally:held[0]=False;events.append('released')
    def callback():assert held[0];events.append('admission');return {'status':'positive'}
    m.shared_admission(callback,time.time()+1000,evidence_dir=root/'maintenance/success',lock_factory=lock)
    assert not held[0];events.append('numeric-stage-after-return')
    assert events==['lock','admission','released','numeric-stage-after-return']
    def invalid():raise ValueError('Original strict source/ledger failure')
    with pytest.raises(ValueError,match='strict'):m.shared_admission(invalid,time.time()+1000,
        evidence_dir=root/'maintenance/failure',lock_factory=lock)
    assert not held[0]


def test_insufficient_native_margin_does_not_acquire_registration_lock(tmp_path,monkeypatch):
    root=tmp_path/'scratch';(root/'maintenance').mkdir(parents=True);monkeypatch.setattr(m,'ROOT',root);events=[]
    with pytest.raises(m.DeadlineBound):m.shared_admission(lambda:events.append('admission'),100.,
        evidence_dir=root/'maintenance/short',lock_factory=lambda *a,**k:events.append('lock'),wall=lambda:99.)
    assert events==[]


def test_native_queries_use_utc_without_transforming_recording_arrays(monkeypatch):
    sequence=clock(0);before=sequence.times.copy();events=[]
    monkeypatch.setenv('TZ','unverified-interactive-setting');monkeypatch.setattr(m.time,'tzset',lambda:events.append('tzset'),raising=False)
    m.scheduler_utc();assert m.os.environ['TZ']=='UTC' and events==['tzset']
    np.testing.assert_array_equal(sequence.times,before)


@pytest.fixture
def prefix_input(tmp_path,monkeypatch,request):
    """Byte-bound synthetic producer; no native calls or scientific file reads."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from horsehair import behaviour as b,cohort_profile as profile,slurm
    from horsehair.core import atomic_json,digest
    root=tmp_path/'personal';case=root/'maintenance/prefix';case.mkdir(parents=True)
    support=root/'maintenance/prefix-support';support.mkdir();stage=root/'maintenance/source';stage.mkdir()
    helper=stage/'run-full-cohort-prefix-v3-20261021.py'
    shutil.copyfile(Path(__file__).with_name(helper.name),helper)
    assert digest(helper)==m.PREFIX_HELPER_SHA
    prefix=m.load_exact('synthetic_qualified_prefix',helper,m.PREFIX_HELPER_SHA)
    tests=stage/'test_full_cohort_prefix_v3_20261021.py';tests.write_text('# Synthetic producer test binding only\n')
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'PREFIX_CASE',case);monkeypatch.setattr(m,'PREFIX_SUPPORT',support)
    monkeypatch.setattr(m,'PREFIX_TEST_SHA',digest(tests))
    bins=root/'results/behaviour/fly_bins_10s.parquet';bins.parent.mkdir(parents=True)
    writer=None
    for group in 'abcde':
        for mode in ('late','early','none'):
            n=75;observed=np.ones(n,dtype=bool)
            if mode=='early' or (mode=='late' and getattr(request,'param','normal')=='all_null'):observed=np.arange(n)<2
            if mode=='none':observed[:]=False
            frame=pd.DataFrame({'animal_id':group+mode,'experiment_id':group,'analysis_role':'behavioural_development',
                'observed':observed,'record_time_s':np.arange(n)*10.,'dx':np.where(observed,.1,np.nan),'dy':np.where(observed,.2,np.nan),
                'x':np.arange(n)/10,'y':np.arange(n)/5,'state_bin_seconds':10.,'food_end':10.,'path_length':.2})
            table=pa.Table.from_pandas(frame)
            if writer is None:writer=pq.ParquetWriter(bins,table.schema)
            writer.write_table(table)
    writer.close();atomic_json(bins.with_name('streaming_bin_provenance.json'),{'bin_files':{str(bins):digest(bins)}})
    config={'behaviour':{'primary_bin_seconds':10,'learning_rates':[.0003,.001,.003],'inner_folds':3}}
    model,train,valid,scaler,n,settings,plan=prefix.prepare(s,b,profile,config,root,case,time.monotonic()+60,lambda e:None)
    # Preserve but never deserialize the original artifact in the new canary.
    artifact=case/(m.PREFIX_STAGE+'.pt');artifact.write_bytes(b'Synthetic opaque old tensor artifact: must not be unpickled')
    stage_value={'stage':m.PREFIX_STAGE,'status':'operator_prefix_stage_complete','artifacts_path':str(artifact),
        'artifacts_sha256':digest(artifact),'plan_sha256':digest(case/'plan.json'),'census_sha256':plan['census_sha256'],**m.FALSE_FLAGS}
    atomic_json(case/(m.PREFIX_STAGE+'.json'),stage_value)
    suite=ET.Element('testsuite',tests='32',failures='0',errors='0',skipped='0')
    for name in PREFIX_TEST_NAMES:ET.SubElement(suite,'testcase',classname='test_full_cohort_prefix_v3_20261021',name=name)
    xml=support/'private-tests.xml';xml.write_bytes(ET.tostring(suite))
    q={'path':str(xml),'sha256':digest(xml),'helper_sha256':m.PREFIX_HELPER_SHA,'test_counts':{'tests':32,'failures':0,'errors':0,'skipped':0},
        'test_roster_sha256':prefix.PRIVATE_TEST_ROSTER_SHA,'test_source':{'path':str(tests),'sha256':digest(tests)},
        'scientific_result':False,'admission_authority':False}
    receipt={'status':'bounded_deadline_reached','native_job_id':'555','case':str(case),'source_sha256':m.SOURCE,
        'primary_source_sha256':m.PRIMARY_SHA,'full_config_sha256':m.CONFIG,'private_qualification':q,
        'worker_result':{'completed_stages':[stage_value],'torch_version':str(torch.__version__)},**m.FALSE_FLAGS}
    rp=support/'receipt.json';atomic_json(rp,receipt)
    observed={'555':{'state':'COMPLETED','exit_code':'0:0','user':'pwa209','account':'def-ptewarie_cpu','job_id_raw':'555',
        'unallocated_metadata':False,'cpus':1,'elapsed_seconds':60,'alloc_tres':'cpu=1,mem=64G,node=1'}}
    monkeypatch.setattr(slurm,'completed_native_dependency',lambda root,job:{'positive_synthetic_completion':job})
    return {'root':root,'case':case,'support':support,'helper':helper,'model':model,'config':config,'plan':plan,
        'receipt':receipt,'rp':rp,'observed':observed,'profile':profile,'b':b,'stage':stage_value,'artifact':artifact}


def test_positive_bounded_prefix_requires_only_committed_full_stage1_and_native32_source_gate(prefix_input):
    from horsehair.core import digest
    f=prefix_input;proof=m.prefix_evidence(f['rp'],digest(f['rp']),'555',f['observed'])
    assert proof['full_prefix_completion_required'] is False and proof['only_complete_stage1_is_required'] is True
    assert proof['original_stage1_artifact']['loaded'] is False
    assert proof['helper']['sha256']==m.PREFIX_HELPER_SHA and proof['runtime_torch_version']==str(torch.__version__)


@pytest.mark.parametrize('change',['native','status','source','stage_missing','stage_hash','census','q_roster','q_source','old_artifact'])
def test_prefix_ingress_rejects_false_partial_or_changed_source_bindings(prefix_input,change):
    from horsehair.core import atomic_json,digest
    f=prefix_input;receipt=f['receipt']
    if change=='native':f['observed']['555']['exit_code']='1:0'
    elif change=='status':receipt['status']='pending_storage_no_probe_started'
    elif change=='source':receipt['source_sha256']='f'*64
    elif change=='stage_missing':receipt['worker_result']['completed_stages']=[]
    elif change=='stage_hash':receipt['worker_result']['completed_stages'][0]['plan_sha256']='f'*64
    elif change=='census':(f['case']/'clock-eligibility-census.json').write_text('{}')
    elif change=='q_roster':receipt['private_qualification']['test_counts']['tests']=1
    elif change=='q_source':receipt['private_qualification']['helper_sha256']='f'*64
    else:f['artifact'].write_bytes(b'changed original bytes')
    atomic_json(f['rp'],receipt)
    with pytest.raises(ValueError):m.prefix_evidence(f['rp'],digest(f['rp']),'555',f['observed'])


def test_recomputation_matches_entire_prefix_metadata_preserves_nulls_and_picks_only_defined_validation(prefix_input):
    from horsehair.core import digest
    f=prefix_input;proof=m.prefix_evidence(f['rp'],digest(f['rp']),'555',f['observed'])
    case=f['root']/'maintenance/new-canary';case.mkdir()
    before=f['artifact'].read_bytes()
    model,training,validation,n,settings,plan=m.prepare(s,f['b'],f['profile'],f['config'],f['root'],case,time.monotonic()+60,lambda e:None,proof)
    assert n==f['plan']['global_observed_bin_normalizer'] and model.animal_ids==f['model'].animal_ids
    for name,value in f['model'].state_dict().items():torch.testing.assert_close(value,model.state_dict()[name],rtol=0,atol=0)
    assert plan['selected_validation_clock']['animal_id'].endswith('late') and len(validation.times)==75
    assert bool((validation.observed & (validation.times-validation.times[0]>=600)).any())
    census=json.loads((case/'clock-eligibility-census.json').read_text())['rows']
    assert len(census)==15 and any(r['eligible_prediction_bins']==0 for r in census)
    assert plan['original_validation_criterion_defined'] is False and plan['preprocessing_recomputed_not_restored'] is True
    assert f['artifact'].read_bytes()==before and plan['original_stage1_tensor_artifact_loaded'] is False
    saved=torch.load(case/'initial-state.pt',weights_only=True)
    assert set(saved['initial_state'])==set(model.state_dict())
    assert plan['byte_parity_with_original_initial_tensor_artifact_claimed'] is False


@pytest.mark.parametrize('change',['scaler','normalizer','runtime'])
def test_recomputed_preprocessing_or_runtime_drift_refuses_before_paired_operators(prefix_input,change):
    from horsehair.core import atomic_json,digest
    f=prefix_input;proof=m.prefix_evidence(f['rp'],digest(f['rp']),'555',f['observed'])
    if change=='runtime':proof['runtime_torch_version']='unqualified runtime'
    else:
        plan=copy.deepcopy(f['plan'])
        if change=='scaler':plan['scaler']['mean'][0]+=.1
        else:plan['global_observed_bin_normalizer']+=1
        path=f['root']/'maintenance/different-plan.json';atomic_json(path,plan);proof['plan']={'path':str(path),'sha256':digest(path)}
    case=f['root']/'maintenance/drift';case.mkdir()
    with pytest.raises(ValueError):m.prepare(s,f['b'],f['profile'],f['config'],f['root'],case,time.monotonic()+60,lambda e:None,proof)


def test_source_bound_prefix_is_mandatory_without_implicit_old_selector_fallback(tmp_path):
    with pytest.raises(ValueError,match='prefix preprocessing evidence'):m.prepare(None,None,None,{},tmp_path,tmp_path,time.monotonic()+60,lambda e:None)


def test_unsupported_zero_window_status_remains_explicit_and_not_complete(tmp_path):
    value=terminal_fixture(tmp_path/'unsupported',count=0,status='unsupported_no_eligible_validation_clock')
    m.validate_worker_result(value,tmp_path)
    assert value['complete_declared_workload'] is False


@pytest.mark.parametrize('prefix_input',['all_null'],indirect=True)
def test_no_eligible_validation_clock_is_explicitly_unsupported_with_full_null_census(prefix_input):
    from horsehair.core import digest
    f=prefix_input;proof=m.prefix_evidence(f['rp'],digest(f['rp']),'555',f['observed'])
    case=f['root']/'maintenance/no-valid-clock';case.mkdir()
    with pytest.raises(m.NoEligibleValidation):m.prepare(s,f['b'],f['profile'],f['config'],f['root'],case,time.monotonic()+60,lambda e:None,proof)
    assert (case/'clock-eligibility-census.json').is_file() and not (case/'initial-state.pt').exists()


PREFIX_TEST_NAMES=('test_exact_full_prior_all_likelihood_gradient_and_one_update_match_primary_prefix', 'test_undefined_original_score_still_executes_original_whole_clock_forward', 'test_only_exact_original_zero_window_error_may_become_null[Duration tail probability failed]', 'test_only_exact_original_zero_window_error_may_become_null[different malformed input]', 'test_positive_window_error_cannot_be_reclassified_as_null', 'test_all_defined_summary_preserves_equal_experiment_then_animal_weight', 'test_all_null_experiment_is_retained_without_zero_or_alternative_criterion', 'test_census_exact_effective_mask_and_full_clocks[late-15]', 'test_census_exact_effective_mask_and_full_clocks[early-0]', 'test_census_exact_effective_mask_and_full_clocks[none-0]', 'test_nonfinite_displacement_uses_primary_effective_missingness_mask', 'test_census_rejects_clock_or_identity_change[clock]', 'test_census_rejects_clock_or_identity_change[identity]', 'test_census_rejects_clock_or_identity_change[length]', 'test_census_rejects_clock_or_identity_change[warmup]', 'test_actual_parquet_prepare_retains_whole_roster_null_clocks_and_full_nuisance', 'test_prospective_canary_v2_rejects_longest_ineligible_without_shortening_any_clock', 'test_prospective_canary_v2_ties_use_only_source_ids_and_all_null_remains_unsupported', 'test_deadline_before_first_gradient_does_not_clip_or_update', 'test_supervisor_refuses_falsely_complete_or_unbound_worker_receipt[scientific_flag]', 'test_supervisor_refuses_falsely_complete_or_unbound_worker_receipt[wrong_source]', 'test_supervisor_refuses_falsely_complete_or_unbound_worker_receipt[out_of_order]', 'test_supervisor_refuses_falsely_complete_or_unbound_worker_receipt[complete_without_all_stages]', 'test_empty_honest_native_partial_receipt_remains_incomplete', 'test_context_loader_refuses_unverified_code_before_any_execution', 'test_private_junit_requires_actual_unique_named_passing_roster', 'test_private_junit_suite_attributes_cannot_replace_real_testcases', 'test_private_qualification_refuses_changed_test_source_before_fixture_or_process', 'test_private_qualification_command_is_full_pinned_colocated_and_selector_free[none]', 'test_private_qualification_command_is_full_pinned_colocated_and_selector_free[nonzero]', 'test_private_qualification_command_is_full_pinned_colocated_and_selector_free[skipped]', 'test_isolated_child_accepts_real_primary_directory_alias_before_source_verification')


def private_xml(path):
    import xml.etree.ElementTree as ET
    suite=ET.Element('testsuite',tests='2',failures='0',errors='0',skipped='0')
    for name in ('test_first','test_second'):ET.SubElement(suite,'testcase',classname='test_full_record_adjoint_v3_20261021',name=name)
    path.write_bytes(ET.tostring(suite));return path


def test_private_junit_requires_actual_unique_named_passing_roster(tmp_path,monkeypatch):
    from horsehair.core import canonical_hash
    path=private_xml(tmp_path/'actual.xml')
    monkeypatch.setattr(m,'PRIVATE_TEST_COUNT',2)
    monkeypatch.setattr(m,'PRIVATE_TEST_ROSTER_SHA',canonical_hash([('test_full_record_adjoint_v3_20261021',n) for n in ('test_first','test_second')]))
    assert m.verify_private_junit(path)['test_counts']['tests']==2
    path.write_text(path.read_text().replace('test_second','different_test'))
    with pytest.raises(ValueError):m.verify_private_junit(path)


def test_private_junit_suite_attributes_cannot_replace_real_testcases(tmp_path,monkeypatch):
    path=tmp_path/'false.xml';path.write_text('<testsuite tests="28" failures="0" errors="0" skipped="0"/>')
    with pytest.raises(ValueError):m.verify_private_junit(path)


def test_private_qualification_refuses_changed_test_source_before_fixture_or_process(tmp_path,monkeypatch):
    root=tmp_path/'root';path=root/'maintenance/stage/test_full_record_adjoint_v3_20261021.py';path.parent.mkdir(parents=True);path.write_text('untrusted')
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'__file__',str(path.with_name('run-full-record-adjoint-v3-20261021.py')))
    with pytest.raises(ValueError,match='test source'):m.qualify_private(path,'f'*64,time.time()+3600,runner=lambda *a,**k:pytest.fail('unverified source executed'))


@pytest.mark.parametrize('bad',['none','nonzero','skipped'])
def test_private_qualification_command_is_full_pinned_colocated_and_selector_free(tmp_path,monkeypatch,bad):
    from horsehair.core import canonical_hash
    root=tmp_path/'root';stage=root/'maintenance/stage';stage.mkdir(parents=True);support=root/'maintenance/support';support.mkdir()
    helper=stage/'run-full-record-adjoint-v3-20261021.py';helper.write_text('synthetic helper source, never executed')
    tests=stage/'test_full_record_adjoint_v3_20261021.py';tests.write_text('synthetic test source, mocked runner')
    shutil.copyfile(Path(__file__).with_name('run-full-cohort-prefix-v3-20261021.py'),stage/'run-full-cohort-prefix-v3-20261021.py')
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'SUPPORT',support);monkeypatch.setattr(m,'__file__',str(helper))
    monkeypatch.setattr(m,'native_fixture_workspace',lambda:tmp_path/'native-fixtures')
    monkeypatch.setattr(m,'PRIVATE_TEST_COUNT',2)
    monkeypatch.setattr(m,'PRIVATE_TEST_ROSTER_SHA',canonical_hash([('test_full_record_adjoint_v3_20261021',n) for n in ('test_first','test_second')]))
    monkeypatch.setenv('PYTEST_ADDOPTS','-k selected');monkeypatch.setenv('PYTEST_PLUGINS','foreign_plugin')
    calls=[]
    def runner(command,**kwargs):
        calls.append(command)
        assert command[:4]==[m.sys.executable,'-s','-m','pytest'] and command[4]==str(tests)
        assert command[5:8]==['-q','-p','no:cacheprovider']
        assert 'PYTEST_ADDOPTS' not in kwargs['env'] and 'PYTEST_PLUGINS' not in kwargs['env']
        assert kwargs['env']['PYTEST_DISABLE_PLUGIN_AUTOLOAD']=='1' and 0<kwargs['timeout']<=600
        path=private_xml(support/'private-tests.xml')
        if bad=='skipped':path.write_text(path.read_text().replace(' />','><skipped/></testcase>',1))
        return subprocess.CompletedProcess(command,1 if bad=='nonzero' else 0)
    if bad=='none':
        result=m.qualify_private(tests,digest(tests),time.time()+3600,runner=runner)
        assert result['test_source']['sha256']==digest(tests) and result['helper_sha256']==digest(helper)
        assert (support/'private-qualification.json').is_file()
    else:
        with pytest.raises(ValueError):m.qualify_private(tests,digest(tests),time.time()+3600,runner=runner)
        assert not (support/'private-qualification.json').exists()
    assert len(calls)==1


def test_isolated_child_accepts_real_primary_directory_alias_before_source_verification(tmp_path):
    # Synthetic import-path fixture only. Scientific source verification is a
    # stop marker after the real package-path guard; no model computation runs.
    target=tmp_path/'actual-primary';package=target/'src/horsehair';package.mkdir(parents=True)
    for name in ('__init__.py','semimarkov.py','behaviour.py','core.py'):(package/name).write_text('# Synthetic primary path fixture\n')
    (package/'core.py').write_text('def atomic_json(*args): raise AssertionError("No fixture writes expected")\n')
    alias=tmp_path/'primary-alias'
    if os.name=='nt':
        # A real filesystem lexical alias needs no elevated Windows link
        # privilege; Linux native qualification exercises the directory symlink.
        alias=target/'..'/'actual-primary'
    else:alias.symlink_to(target,target_is_directory=True)
    program='''import importlib.util,pathlib,sys,time
alias,target,helper=map(pathlib.Path,sys.argv[1:])
sys.path.insert(0,str(alias/'src'))
spec=importlib.util.spec_from_file_location('synthetic_alias_prefix',helper)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
def verified_import_stop(primary,framework):
    assert primary==target.resolve() and framework==target.resolve()
    raise SystemExit(0)
m.source_bindings=verified_import_stop
m._worker(alias,target,target,target,time.monotonic()+60,target/'unused-proof','0'*64)
raise SystemExit(7)
'''
    env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'}
    result=subprocess.run([m.sys.executable,'-s','-c',program,str(alias),str(target),str(Path(m.__file__).resolve())],
        capture_output=True,text=True,timeout=45,env=env)
    assert result.returncode==0,result.stderr


@pytest.fixture
def registration_root(tmp_path,monkeypatch):
    root=tmp_path/'personal';(root/'state').mkdir(parents=True);(root/'maintenance').mkdir()
    monkeypatch.setattr(m,'ROOT',root);return root


def test_exact_acquisition_contention_retries_then_calls_once_with_durable_evidence(registration_root):
    from horsehair.core import ReceiptLockTimeout
    root=registration_root;now=[0.];entries=[];callbacks=[]
    @contextmanager
    def lock(path,timeout):
        entries.append((path,timeout))
        if len(entries)<3:
            now[0]+=timeout
            raise ReceiptLockTimeout(path,timeout,{'authority':'informational only'})
        yield
    value,proof=m.shared_admission(lambda:callbacks.append('once') or 7,10000.,
        evidence_dir=root/'maintenance/wait',lock_factory=lock,wall=lambda:now[0],clock=lambda:now[0])
    assert value==7 and callbacks==['once'] and len(entries)==3
    assert proof['maximum_acquisition_seconds']==900 and proof['acquired_after_seconds']==60
    assert [a['status'] for a in proof['attempts']]==['kernel_contention','kernel_contention','admission_complete']
    assert len(list((root/'maintenance/wait').glob('attempt-*-intent.json')))==3
    assert len(list((root/'maintenance/wait').glob('attempt-*-result.json')))==3
    assert json.loads((root/'maintenance/wait/result.json').read_text())==proof
    assert proof['callback_retried'] is proof['guard_removed_or_replaced'] is False


@pytest.mark.parametrize('kind',['other_path','subclass','timeout','permission'])
def test_only_exact_preacquisition_kernel_timeout_can_retry(registration_root,kind):
    from horsehair.core import ReceiptLockTimeout
    root=registration_root;calls=[];callbacks=[]
    class Derived(ReceiptLockTimeout):pass
    error={'other_path':ReceiptLockTimeout(root/'state/other.mutex',1,None),
        'subclass':Derived(root/'state/af-campaign-registration.mutex',1,None),
        'timeout':TimeoutError('Different timeout contract'),'permission':PermissionError('Different file permission')}[kind]
    @contextmanager
    def lock(path,timeout):
        calls.append(path);raise error;yield
    with pytest.raises(type(error)):
        m.shared_admission(lambda:callbacks.append(1),time.time()+2000,
            evidence_dir=root/'maintenance/error',lock_factory=lock)
    assert len(calls)==1 and callbacks==[]
    assert json.loads((root/'maintenance/error/result.json').read_text())['status']=='strict_admission_failure'


def test_callback_exact_mutex_timeout_is_never_replayed_and_lock_releases(registration_root):
    from horsehair.core import ReceiptLockTimeout
    root=registration_root;held=[False];calls=[];entries=[]
    @contextmanager
    def lock(path,timeout):
        entries.append(path);held[0]=True
        try:yield
        finally:held[0]=False
    def callback():
        calls.append(1);assert held[0]
        raise ReceiptLockTimeout(root/'state/af-campaign-registration.mutex',30,None)
    with pytest.raises(ReceiptLockTimeout):
        m.shared_admission(callback,time.time()+2000,evidence_dir=root/'maintenance/callback',lock_factory=lock)
    assert calls==[1] and len(entries)==1 and held==[False]
    proof=json.loads((root/'maintenance/callback/result.json').read_text())
    assert proof['attempts'][0]['callback_started'] is True and proof['callback_retried'] is False


def test_slow_intent_persistence_cannot_consume_native_acquisition_reserve(registration_root,monkeypatch):
    root=registration_root;now=[0.];calls=[];original=m.exclusive
    def slow(path,value):
        original(path,value)
        if path.name.endswith('-intent.json'):now[0]=701.
    monkeypatch.setattr(m,'exclusive',slow)
    with pytest.raises(m.DeadlineBound):
        m.shared_admission(lambda:calls.append('callback'),1000.,evidence_dir=root/'maintenance/slow',
            lock_factory=lambda *a,**k:calls.append('lock'),wall=lambda:now[0],clock=lambda:now[0])
    assert calls==[]
    proof=json.loads((root/'maintenance/slow/result.json').read_text())
    assert proof['maximum_acquisition_seconds']==700 and proof['attempts'][0]['status']=='not_attempted_margin_exhausted'


def test_mutex_acquired_after_deadline_is_released_without_callback(registration_root):
    root=registration_root;now=[0.];held=[False];callbacks=[]
    @contextmanager
    def lock(path,timeout):
        now[0]=901.;held[0]=True
        try:yield
        finally:held[0]=False
    with pytest.raises(m.DeadlineBound):
        m.shared_admission(lambda:callbacks.append(1),10000.,evidence_dir=root/'maintenance/late',
            lock_factory=lock,wall=lambda:now[0],clock=lambda:now[0])
    assert callbacks==[] and held==[False]
    proof=json.loads((root/'maintenance/late/result.json').read_text())
    assert proof['attempts'][0]['callback_started'] is False


def test_immediate_exact_lock_timeouts_have_finite_attempt_ceiling(registration_root):
    from horsehair.core import ReceiptLockTimeout
    root=registration_root;calls=[]
    @contextmanager
    def lock(path,timeout):
        calls.append(timeout);raise ReceiptLockTimeout(path,0,None);yield
    with pytest.raises(m.DeadlineBound):
        m.shared_admission(lambda:pytest.fail('No callback allowed'),10000.,evidence_dir=root/'maintenance/finite',
            lock_factory=lock,wall=lambda:0.,clock=lambda:0.)
    assert len(calls)==31 and set(calls)=={30.}
    assert json.loads((root/'maintenance/finite/result.json').read_text())['status']=='pending_registration_no_probe_started'

