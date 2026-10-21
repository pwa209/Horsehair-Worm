import copy
import json
from pathlib import Path
import subprocess

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import torch

from horsehair import cohort_profile as c
from horsehair import semimarkov as s
from horsehair.core import atomic_json, digest


def clock(animal, experiment, length=8, offset=0):
    rng = np.random.default_rng(200+offset)
    observed = np.ones(length, dtype=bool)
    observed[2] = False
    observations = rng.normal(size=(length, 2))*.3
    observations[~observed] = np.nan
    previous = np.vstack([np.zeros((1, 2)), np.nan_to_num(observations[:-1])])
    return s.Sequence(animal, experiment, observations,
        np.column_stack([np.arange(length)+offset, rng.normal(size=length)]),
        np.linspace(-1, 1, length)[:, None], previous,
        np.tile([0., 1.], (length, 1)), np.tile([0., 1.], (length, 1)),
        observed, np.arange(length)*10.)


@pytest.fixture
def cohort():
    torch.set_num_threads(1)
    return [clock('a', 'train1', 7), clock('b', 'train2', 9, 1)]


def make_model(cohort, kind='MX'):
    return s.MovementModel(kind, 2, 1, knots=[1., 2., 3.],
        animal_ids=[q.animal_id for q in cohort], experiment_ids=[q.experiment_id for q in cohort])


def test_hash_frozen_fold_keeps_whole_groups_and_every_eligible_recording():
    roster = [dict(animal_id=f'{g}:{i}', experiment_id=g, observed_any=i != 2,
                   outcome='unused', bins=100+i) for g in ('b', 'a', 'c', 'd', 'e') for i in range(3)]
    split = c.grouped_profile_split(roster)
    again = c.grouped_profile_split([dict(r, outcome='changed') for r in reversed(roster)])
    for key in ('outer_experiment', 'inner_index', 'inner_folds'):
        assert split[key] == again[key]
    membership = [set(split[k]) for k in ('training_ids', 'validation_ids', 'outer_holdout_ids')]
    assert len(set.union(*membership)) == 10
    assert all(not membership[i] & membership[j] for i in range(3) for j in range(i))
    groups = [{key.split(':')[0] for key in values} for values in membership]
    assert all(not groups[i] & groups[j] for i in range(3) for j in range(i))
    assert all(len([key for key in set.union(*membership) if key.startswith(g+':')]) == 2 for g in ('a','b','c','d','e'))
    assert set(split['excluded_all_missing_ids']) == {f'{g}:2' for g in ('a','b','c','d','e')}
    assert split['biological_identity_claim'] is False


@pytest.mark.parametrize('change', ['duplicate', 'missing_mask', 'few_groups'])
def test_grouped_fold_rejects_ambiguous_roster(change):
    rows=[dict(animal_id=g,experiment_id=g,observed_any=True) for g in 'abcd']
    if change=='duplicate': rows.append(rows[0])
    if change=='missing_mask': rows[0]['observed_any']=None
    if change=='few_groups': rows=rows[:2]
    with pytest.raises(ValueError): c.grouped_profile_split(rows)


def test_real_parquet_statistics_preserve_all_missing_and_source_roles(tmp_path):
    path=tmp_path/'bins.parquet'
    schema=pa.schema([('animal_id',pa.string()),('experiment_id',pa.string()),
        ('analysis_role',pa.string()),('observed',pa.bool_())])
    with pq.ParquetWriter(path,schema) as writer:
        for ident,observed,role in [('a',[False,True,False],'behavioural_development'),
                                    ('b',[False]*3,'behavioural_development'),('c',[True]*3,'held_out')]:
            writer.write_table(pa.Table.from_pydict({'animal_id':[ident]*3,'experiment_id':[ident]*3,
                'analysis_role':[role]*3,'observed':observed},schema=schema))
    before=digest(path)
    result=c.recording_roster(pq.ParquetFile(path).metadata)
    assert [(r['animal_id'],r['observed_any'],r['bins'],r['row_group']) for r in result]==[('a',True,3,0),('b',False,3,1)]
    assert digest(path)==before


def test_parquet_missing_mask_statistics_refused(tmp_path):
    path=tmp_path/'empty-mask.parquet'
    pq.write_table(pa.table({'animal_id':['a','a'],'experiment_id':['e','e'],
        'analysis_role':['behavioural_development']*2,'observed':pa.array([None,None],type=pa.bool_())}),path)
    with pytest.raises(ValueError,match='statistics'): c.recording_roster(pq.ParquetFile(path).metadata)


@pytest.mark.parametrize('kind', ['M0','MG','MT','MD','MX'])
def test_full_objective_and_every_gradient_equal_joint_reference(cohort,kind):
    model=make_model(cohort,kind)
    with torch.no_grad():
        model.animal_z.copy_(torch.tensor([[.12,-.2],[-.11,.09]]))
        model.experiment_z.copy_(torch.tensor([[.3,-.1],[.25,-.15]]))
    reference=copy.deepcopy(model)
    n=sum(int(q.observed.sum()) for q in cohort)
    total=(-reference.log_prior()-sum(reference(q)['log_likelihood'].sum() for q in cohort))/n
    total.backward()
    timer=c.Timings()
    actual=c.objective_gradient(model,cohort,n,timer)
    torch.testing.assert_close(actual,total.detach(),rtol=1e-12,atol=1e-12)
    for (name,p),(other,q) in zip(model.named_parameters(),reference.named_parameters()):
        assert name==other and (p.grad is None)==(q.grad is None)
        if p.grad is not None: torch.testing.assert_close(p.grad,q.grad,rtol=1e-10,atol=1e-11)
    assert timer.totals['prior_backward']['calls']==1
    assert timer.totals['likelihood_backward']['calls']==len(cohort)


def test_adam_epoch_matches_one_global_clip_and_one_update(cohort):
    actual=make_model(cohort);reference=copy.deepcopy(actual)
    a=torch.optim.Adam(actual.parameters(),lr=.001)
    b=torch.optim.Adam(reference.parameters(),lr=.001)
    n=sum(int(q.observed.sum()) for q in cohort)
    b.zero_grad()
    value=(-reference.log_prior()-sum(reference(q)['log_likelihood'].sum() for q in cohort))/n
    value.backward()
    norm=torch.nn.utils.clip_grad_norm_(reference.parameters(),.01)
    b.step()
    timer=c.Timings()
    out,measurednorm=c.adam_epoch(actual,cohort,a,n,.01,timer)
    torch.testing.assert_close(out,value.detach(),rtol=1e-12,atol=1e-12)
    assert measurednorm==pytest.approx(float(norm),rel=1e-12)
    for p,q in zip(actual.parameters(),reference.parameters()):
        torch.testing.assert_close(p,q,rtol=1e-12,atol=1e-12)
    assert timer.totals['gradient_clip_once']['calls']==timer.totals['optimizer_update_once']['calls']==1


def test_wrong_cohort_normalizer_and_nonfinite_gradient_fail_closed(cohort,monkeypatch):
    model=make_model(cohort)
    n=sum(int(q.observed.sum()) for q in cohort)
    with pytest.raises(ValueError,match='global observed'): c.objective_gradient(model,cohort,n+1)
    model.exit_intercept.register_hook(lambda gradient: gradient*float('nan'))
    with pytest.raises(ValueError,match='Nonfinite full-cohort gradient'): c.objective_gradient(model,cohort,n)


@pytest.mark.parametrize('kind', ['MX','M0','HMM'])
@pytest.mark.parametrize('lbfgs', [0,2])
def test_instrumented_actual_convergence_path_matches_primary_fit(cohort,kind,lbfgs):
    validation=[clock('v','val',6,3),clock('w','val',7,4),clock('z','other',8,5)]
    config=dict(starts=2,max_epochs=3,patience=2,learning_rate=.003,
        lbfgs_iterations=lbfgs,warmup_seconds=0,seed=19,gradient_clip=.5)
    expected=s.fit_model(cohort,'MX' if kind=='HMM' else kind,
        dict(config,hmm_baseline=kind=='HMM'),validation)
    events=[];checkpoints=[]
    actual=c.profile_fit(cohort,validation,kind,config,emit=events.append,
        checkpoint=lambda value: checkpoints.append(copy.deepcopy(value)))
    for name,param in expected.model.state_dict().items():
        torch.testing.assert_close(actual['model'].state_dict()[name],param,rtol=1e-11,atol=1e-12)
    assert actual['scaler']==expected.scaler
    assert actual['normalizer']==14
    for left,right in zip(actual['records'],expected.diagnostics['starts']):
        assert left['epochs']==right['epochs'] and left['criterion']==pytest.approx(right['criterion'],abs=1e-12)
        assert left['objective']==pytest.approx(right['objective'],abs=1e-12)
        np.testing.assert_allclose([[q['objective'],q['criterion']] for q in left['history']],
            [[q['objective'],q['criterion']] for q in right['history']],rtol=1e-11,atol=1e-12)
    assert len([e for e in events if e['event']=='start_initialized'])==2
    assert any(e['event']=='training_preprocessing_complete' for e in events)
    assert all(q['exact_mid_lbfgs_resume_supported'] is False for q in checkpoints)
    assert sum(q['phase']=='before_original_lbfgs_step' for q in checkpoints)==(2 if lbfgs else 0)
    epoch=next(q for q in checkpoints if q['phase']=='completed_adam_epoch')
    assert {'model_state','optimizer_state','best_state','best_criterion','stale','torch_rng_state'}<=epoch.keys()
    if kind=='HMM': assert torch.count_nonzero(actual['model'].duration_beta)==0 and not actual['model'].duration_beta.requires_grad


@pytest.mark.parametrize('change', ['gpu','duration_cap','leakage','AR'])
def test_pending_alternatives_and_leakage_refused(cohort,change):
    kwargs=dict(device='cuda') if change=='gpu' else {}
    settings=dict(starts=1,max_epochs=1,lbfgs_iterations=0,warmup_seconds=0)
    if change=='duration_cap': settings['max_duration_bins']=5
    validation=cohort[:1] if change=='leakage' else []
    with pytest.raises(ValueError): c.profile_fit(cohort,validation,'AR' if change=='AR' else 'MX',settings,**kwargs)


def prepare_supervisor(tmp_path,monkeypatch):
    primary=tmp_path/'primary';primary.mkdir()
    root=tmp_path/'scratch';root.mkdir()
    config=tmp_path/'config.json';config.write_text(json.dumps({'paths':{'root':str(root)}}))
    monkeypatch.setattr(c,'PERSONAL_ROOT',root)
    monkeypatch.setenv('SLURM_JOB_ID','12345');monkeypatch.setenv('SLURM_CPUS_PER_TASK','1')
    monkeypatch.setenv('HHW_GPU_COUNT','0');monkeypatch.delenv('SLURM_ARRAY_JOB_ID',raising=False)
    monkeypatch.delenv('SLURM_ARRAY_TASK_ID',raising=False);monkeypatch.delenv('SLURM_JOB_END_TIME',raising=False)
    monkeypatch.setattr(c,'source_identity',lambda path:{'sha256':c.PRIMARY_SHA})
    from horsehair import rna_recovery
    monkeypatch.setattr(rna_recovery,'_primary_environment',lambda manifest:{'PYTHONNOUSERSITE':'1','PYTHONPATH':'vendor-dependencies'})
    return config,root,primary,root/'maintenance/case'


def test_supervisor_timeout_preserves_completed_partial_evidence(tmp_path,monkeypatch):
    config,root,primary,output=prepare_supervisor(tmp_path,monkeypatch)
    class Process:
        pid=54321
        def __init__(self,command,**kwargs):
            assert command[1]=='-s' and '-I' not in command
            assert kwargs['env']['PYTHONNOUSERSITE']=='1'
            atomic_json(output/'progress.json',{'status':'full_grouped_optimizer_running','last_completed_epoch':2,
                'last_event':{'phase':'likelihood_backward','source_id':'original-clock'},'scientific_result':False})
            self.calls=0
        def wait(self,timeout):
            self.calls+=1
            if self.calls==1: raise subprocess.TimeoutExpired('bounded',timeout)
            return -15
        def terminate(self): pass
    monkeypatch.setattr(c.subprocess,'Popen',Process)
    monkeypatch.setattr(c.os,'killpg',lambda *args:None,raising=False)
    receipt=c.run_profile(config,root,primary,output,max_seconds=30)
    assert receipt['status']=='bounded_deadline_reached'
    assert receipt['progress']['last_completed_epoch']==2
    assert receipt['worker_returncode']==-15
    assert all(receipt[key] is False for key in ('scientific_result','scientific_fit','complete_declared_workload','admission_authority'))
    assert json.loads((output/'receipt.json').read_text())==receipt


def test_zero_exit_without_complete_worker_artifact_is_not_a_completed_profile(tmp_path,monkeypatch):
    config,root,primary,output=prepare_supervisor(tmp_path,monkeypatch)
    class Process:
        def __init__(self,*args,**kwargs): pass
        def wait(self,timeout): return 0
    monkeypatch.setattr(c.subprocess,'Popen',Process)
    assert c.run_profile(config,root,primary,output,max_seconds=30)['status']=='worker_failed'


def test_cpu_contract_rejects_wrong_resources_before_launch(tmp_path,monkeypatch):
    config,root,primary,output=prepare_supervisor(tmp_path,monkeypatch)
    monkeypatch.setenv('SLURM_CPUS_PER_TASK','8')
    with pytest.raises(ValueError,match='exactly one CPU'): c.run_profile(config,root,primary,output,max_seconds=30)
    assert not output.exists()


@pytest.mark.parametrize('change',['wrong_root','wrong_config','outside','namespace','redirected_ancestor'])
def test_both_parent_and_worker_location_contract_reject_foreign_or_redirected_cases(tmp_path,monkeypatch,change):
    config,root,primary,output=prepare_supervisor(tmp_path,monkeypatch)
    cfg=json.loads(config.read_text())
    if change=='wrong_root': root=tmp_path
    if change=='wrong_config': cfg['paths']['root']=str(tmp_path)
    if change=='outside': output=tmp_path/'outside'
    if change=='namespace': output=root/'maintenance'
    if change=='redirected_ancestor':
        target=output.parent
        original=Path.is_symlink
        monkeypatch.setattr(Path,'is_symlink',lambda path:path==target or original(path))
    for fresh in (True,False):
        with pytest.raises(ValueError): c._locations(cfg,root,output,fresh=fresh)
    config.write_text(json.dumps(cfg))
    with pytest.raises(ValueError): c.run_profile(config,root,primary,output,max_seconds=30)
    assert not output.exists()


@pytest.mark.parametrize('status,exitcode,partial', [('worker_failed',1,False),
    ('bounded_deadline_reached',0,True),('declared_fold_profile_complete',0,False)])
def test_cli_propagates_child_failure_and_labels_partial_receipt(tmp_path,monkeypatch,capsys,status,exitcode,partial):
    receipt=dict(status=status,scientific_result=False,scientific_fit=False,
        complete_declared_workload=False,admission_authority=False)
    def run(*args,**kwargs):
        atomic_json(tmp_path/'receipt.json',receipt)
        return receipt
    monkeypatch.setattr(c,'run_profile',run)
    code=c.main(['--config','config','--root','root','--primary-release','primary','--output-dir',str(tmp_path)])
    assert code==exitcode
    result=json.loads(capsys.readouterr().out)
    assert result['status']==status and result['technical_partial'] is partial
    assert all(result[k] is False for k in ('scientific_result','scientific_fit','complete_declared_workload','admission_authority'))


def test_cli_refuses_partial_receipt_without_durable_matching_record(tmp_path,monkeypatch):
    monkeypatch.setattr(c,'run_profile',lambda *args,**kwargs:dict(status='bounded_deadline_reached',
        scientific_result=False,scientific_fit=False,complete_declared_workload=False,admission_authority=False))
    with pytest.raises(ValueError,match='persisted bounded worker receipt'):
        c.main(['--config','config','--root','root','--primary-release','primary','--output-dir',str(tmp_path)])
