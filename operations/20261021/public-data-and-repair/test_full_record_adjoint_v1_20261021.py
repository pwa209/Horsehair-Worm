"""Synthetic whole-clock math and finite supervision; no remote/source data."""
from pathlib import Path
import copy
import importlib.util
import json
import time
from contextlib import contextmanager

import numpy as np
import pytest
import torch

from horsehair import duration_adjoint as adjoint,semimarkov as s

spec=importlib.util.spec_from_file_location('native_record_probe',Path(__file__).with_name('run-full-record-adjoint-v1-20261021.py'))
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


def test_native_context_helper_is_pinned_and_fresh_namespaces_are_separate():
    path=Path(__file__).with_name('run-full-cohort-profile-v2-20261021.py')
    assert m.digest(path)==m.CONTEXT_SHA
    assert 'full-record-adjoint-v1' in m.CASE.name and 'full-cohort-profile-v2' not in m.CASE.name
    assert m.ADJOINT_SHA=='5ff03ba506d4ad8dab66dd51def1c728aade96a8af77661be03de20e40a205f9'


def test_actual_parquet_full_fold_preprocessing_uses_original_scaler_knots_nuisances_and_seed(tmp_path):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from dataclasses import asdict
    from horsehair import behaviour as b,cohort_profile as profile
    root=tmp_path/'synthetic-root';directory=root/'results/behaviour';directory.mkdir(parents=True)
    path=directory/'fly_bins_10s.parquet';frames=[]
    for group in range(5):
        for ident in range(3):
            n=8+ident;mask=np.ones(n,dtype=bool);mask[0]=False;mask[3]=False
            if ident==2:mask[:]=False
            rng=np.random.default_rng(group*10+ident);delta=rng.normal(size=(n,2))*.2
            position=np.cumsum(delta,axis=0);delta[~mask]=np.nan
            frame=pd.DataFrame({'animal_id':[f'g{group}:r{ident}']*n,'experiment_id':[f'g{group}']*n,
                'analysis_role':['behavioural_development']*n,'observed':mask,'record_time_s':700.+np.arange(n)*10,
                'dx':delta[:,0],'dy':delta[:,1],'x':position[:,0],'y':position[:,1],
                'state_bin_seconds':[10.]*n,'food_end':[10.]*n,'path_length':np.nan_to_num(np.linalg.norm(delta,axis=1))})
            frames.append(frame)
    schema=pa.Table.from_pandas(frames[0],preserve_index=False).schema
    with pq.ParquetWriter(path,schema) as writer:
        for frame in frames:writer.write_table(pa.Table.from_pandas(frame,schema=schema,preserve_index=False))
    before=m.digest(path)
    (directory/'streaming_bin_provenance.json').write_text(json.dumps({'bin_files':{str(path):before}}))
    config={'behaviour':{'primary_bin_seconds':10,'max_duration_bins':None,'learning_rates':[.0003,.001,.003],
        'inner_folds':3,'covariates':['circadian_sin','circadian_cos','previous_y','previous_missing'],'gradient_clip':5}}
    case=root/'maintenance/technical';case.mkdir(parents=True)
    value,training,validation,n,settings,plan=m.prepare(s,b,profile,config,root,case,time.monotonic()+60,lambda event:None)
    train_ids=plan['split']['training_ids'];selected_frames=[frame for frame in frames if frame.animal_id.iloc[0] in train_ids]
    original_train=[b.to_sequences(frame,settings)[0] for frame in selected_frames]
    expected_scaler=s.TrainingScaler.fit(original_train);expected_knots=s.training_duration_knots(original_train)
    assert plan['scaler']==asdict(expected_scaler)
    np.testing.assert_array_equal(plan['duration_knots'],expected_knots)
    assert n==sum(int(q.observed.sum()) for q in original_train)==plan['global_observed_bin_normalizer']
    assert value.animal_ids==sorted(train_ids) and set(value.experiment_ids)=={q.experiment_id for q in original_train}
    assert training.animal_id in train_ids and validation.animal_id in plan['split']['validation_ids']
    assert len(training.times)==plan['selected_training_clock']['bins'] and len(validation.times)==plan['selected_validation_clock']['bins']
    assert plan['predictive_contexts']==9 and plan['exposure_metadata_inferred'] is False
    assert len(plan['split']['excluded_all_missing_ids'])==5
    assert m.digest(path)==before and (case/'initial-state.pt').is_file()
    torch.manual_seed(0)
    reference=s.MovementModel('MX',len(expected_scaler.mean),len(expected_scaler.modulation_mean),dimension=2,
        knots=expected_knots,animal_ids=[q.animal_id for q in original_train],experiment_ids=[q.experiment_id for q in original_train],
        states=2,max_duration_bins=None,bin_seconds=10.)
    scale=np.maximum(np.std(np.concatenate([q.observations[q.observed] for q in original_train]),axis=0),1e-4)
    with torch.no_grad():
        reference.log_rest_scale.copy_(reference._tensor(np.log(scale/3)))
        for name,p in reference.named_parameters():
            if p.requires_grad and name not in ('log_rest_scale','log_animal_sd','log_experiment_sd'):p.add_(torch.randn_like(p)*.05)
    for name,tensor in value.state_dict().items():torch.testing.assert_close(tensor,reference.state_dict()[name],rtol=0,atol=0)


def test_current_process_rss_has_actual_positive_evidence():
    assert type(m.rss_bytes()) is int and m.rss_bytes()>0


def test_actual_shared_kernel_contention_is_bounded_then_releases(tmp_path,monkeypatch):
    from horsehair.core import exclusive_file_lock,ReceiptLockTimeout
    root=tmp_path/'scratch';(root/'state').mkdir(parents=True);monkeypatch.setattr(m,'ROOT',root)
    path=root/'state/af-campaign-registration.mutex';calls=[]
    def bounded_lock(given,timeout):
        assert given==path and 0<timeout<=60
        return exclusive_file_lock(given,timeout=.1)
    with exclusive_file_lock(path,timeout=1):
        with pytest.raises(ReceiptLockTimeout):m.shared_admission(lambda:calls.append('admission'),time.time()+1000,lock_factory=bounded_lock)
    assert not calls
    value,evidence=m.shared_admission(lambda:calls.append('admission') or 'positive',time.time()+1000,lock_factory=bounded_lock)
    assert value=='positive' and calls==['admission'] and evidence['released_before_numerical_execution'] is True
    with exclusive_file_lock(path,timeout=.1):pass


def test_registration_lock_never_wraps_numerics_and_releases_on_read_error(tmp_path,monkeypatch):
    root=tmp_path/'scratch';monkeypatch.setattr(m,'ROOT',root);events=[];held=[False]
    @contextmanager
    def lock(path,timeout):
        held[0]=True;events.append('lock')
        try:yield
        finally:held[0]=False;events.append('released')
    def callback():assert held[0];events.append('admission');return {'status':'positive'}
    m.shared_admission(callback,time.time()+1000,lock_factory=lock)
    assert not held[0];events.append('numeric-stage-after-return')
    assert events==['lock','admission','released','numeric-stage-after-return']
    def invalid():raise ValueError('Original strict source/ledger failure')
    with pytest.raises(ValueError,match='strict'):m.shared_admission(invalid,time.time()+1000,lock_factory=lock)
    assert not held[0]


def test_insufficient_native_margin_does_not_acquire_registration_lock():
    events=[]
    with pytest.raises(m.DeadlineBound):m.shared_admission(lambda:events.append('admission'),100.,
        lock_factory=lambda *a,**k:events.append('lock'),wall=lambda:99.)
    assert events==[]


def test_native_queries_use_utc_without_transforming_recording_arrays(monkeypatch):
    sequence=clock(0);before=sequence.times.copy();events=[]
    monkeypatch.setenv('TZ','unverified-interactive-setting');monkeypatch.setattr(m.time,'tzset',lambda:events.append('tzset'),raising=False)
    m.scheduler_utc();assert m.os.environ['TZ']=='UTC' and events==['tzset']
    np.testing.assert_array_equal(sequence.times,before)
