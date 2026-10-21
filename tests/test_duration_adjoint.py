import copy

import numpy as np
import pytest
import torch

from horsehair import duration_adjoint as a
from horsehair import semimarkov as s
from horsehair.cohort_profile import objective_gradient as primary_objective_gradient


@pytest.fixture(autouse=True)
def one_thread():
    previous=torch.get_num_threads();torch.set_num_threads(1)
    yield
    torch.set_num_threads(previous)


def clock(ident,experiment='e0',length=13,width=10.,dimension=2,all_missing=False):
    rng=np.random.default_rng(800+int(ident))
    observed=np.ones(length,dtype=bool);observed[1:3]=False;observed[-2]=False
    if all_missing:observed[:]=False
    observations=rng.normal(size=(length,dimension))*.3;observations[~observed]=np.nan
    return s.Sequence(str(ident),experiment,observations,rng.normal(size=(length,2)),
        rng.normal(size=(length,1)),rng.normal(size=(length,dimension))*.2,
        rng.normal(size=(length,dimension))*.2,rng.normal(size=(length,dimension))*.2,
        observed,700.+np.arange(length)*width,bin_seconds=width)


def model(kind='MX',states=2,dimension=2):
    result=s.MovementModel(kind,2,1,dimension=dimension,states=states,knots=[1.,2.,3.],
        animal_ids=['0','1','2','unused'],experiment_ids=['e0','e1','unused'])
    torch.manual_seed(41)
    with torch.no_grad():
        for parameter in result.parameters():parameter.add_(torch.randn_like(parameter)*.12)
    return result


def compare_gradients(actual,reference,atol=2e-10,rtol=2e-9):
    for (name,p),(other,q) in zip(actual.named_parameters(),reference.named_parameters()):
        assert name==other and (p.grad is None)==(q.grad is None),name
        if p.grad is not None:torch.testing.assert_close(p.grad,q.grad,rtol=rtol,atol=atol,msg=lambda text:name+': '+text)


@pytest.mark.parametrize('kind',['M0','MG','MT','MD','MX'])
@pytest.mark.parametrize('states',[2,3])
@pytest.mark.parametrize('block',[1,4,100])
def test_every_log_score_prior_tail_and_parameter_gradient_preserves_original(kind,states,block):
    units=[clock(0,length=11),clock(1,'e1',length=11),clock(2,length=11,all_missing=True)]
    actual=model(kind,states);reference=copy.deepcopy(actual)
    source=[reference(q) for q in units]
    result=a.forward_independent(actual,units,block_size=block)
    for field in ('log_scores','log_likelihood','max_tail_probability'):
        expected=torch.cat([value[field] for value in source],0)
        torch.testing.assert_close(result[field],expected,rtol=2e-12,atol=2e-12)
    torch.testing.assert_close(result['log_prior'],reference.log_prior(),rtol=0,atol=0)
    weights=torch.linspace(-.3,1.1,11,dtype=torch.float64)
    reference_loss=-sum((value['log_scores']*weights).sum() for value in source)-reference.log_prior()
    actual_loss=-(result['log_scores']*weights).sum()-result['log_prior']
    reference_loss.backward();actual_loss.backward();compare_gradients(actual,reference)


@pytest.mark.parametrize('states',[2,3])
def test_unseen_effects_full_quadrature_and_intervention_override_gradients(states):
    unit=clock(8,'unseen-experiment',length=8)
    actual=model('MX',states);reference=copy.deepcopy(actual)
    overrides={name:torch.randn_like(getattr(actual,name)).requires_grad_()
        for name in ('transition_mod','gain_mod','direction_mod')}
    overrides.update(transition_shift=torch.tensor([.03]*states,dtype=torch.float64,requires_grad=True),
        gain_shift=torch.tensor([.1]*(states-1),dtype=torch.float64,requires_grad=True),
        direction_shift=torch.tensor([-.02]*(states-1),dtype=torch.float64,requires_grad=True))
    reference_overrides={name:value.detach().clone().requires_grad_() for name,value in overrides.items()}
    effects,weights=actual.predictive_random_effects(unit,quadrature=3)
    source_effects,source_weights=reference.predictive_random_effects(unit,quadrature=3)
    assert len(effects)==3**states
    result=a.forward_sequence(actual,unit,effects,overrides,block_size=3)
    expected=reference(unit,source_effects,reference_overrides)
    torch.testing.assert_close(result['log_scores'],expected['log_scores'],rtol=2e-12,atol=2e-12)
    def density(value,logweights,prior):
        marginal=torch.logsumexp(value['log_scores'].cumsum(1)+logweights[:,None],0)
        return (marginal*torch.linspace(.2,1.,len(marginal),dtype=torch.float64)).sum()+prior
    density(result,weights,actual.log_prior()).backward()
    density(expected,source_weights,reference.log_prior()).backward()
    compare_gradients(actual,reference)
    for name,value in overrides.items():
        torch.testing.assert_close(value.grad,reference_overrides[name].grad,rtol=2e-9,atol=2e-10)


def test_one_dimensional_original_model_and_missing_clock_bins():
    unit=clock(0,length=19,width=5.,dimension=1)
    actual=model('MG',dimension=1);reference=copy.deepcopy(actual)
    result=a.forward_sequence(actual,unit,block_size=4);expected=reference(unit)
    torch.testing.assert_close(result['log_scores'],expected['log_scores'],rtol=2e-12,atol=2e-12)
    (-result['log_likelihood'].sum()-result['log_prior']).backward()
    (-expected['log_likelihood'].sum()-expected['log_prior']).backward()
    compare_gradients(actual,reference)


def test_tensor_adjoint_matches_finite_difference_for_all_four_inputs():
    torch.manual_seed(31)
    base=torch.randn(2,5,3,dtype=torch.float64,requires_grad=True)*.3
    duration=torch.randn(5,3,dtype=torch.float64,requires_grad=True)*.1
    emission=torch.randn(2,5,3,dtype=torch.float64,requires_grad=True)*.2
    destination=torch.softmax(torch.randn(3,3,dtype=torch.float64),-1).requires_grad_()
    assert torch.autograd.gradcheck(lambda *args:a.duration_scores(*args,block_size=2),
        (base,duration,emission,destination),eps=1e-6,atol=2e-6,rtol=2e-5)


def test_primary_clamp_min_underflow_and_shift_derivatives_are_retained():
    unit=clock(0,length=4);actual=model('M0');reference=copy.deepcopy(actual)
    with torch.no_grad():
        for value in (actual,reference):
            value.exit_intercept.fill_(1000.);value.context_beta.zero_();value.duration_beta.zero_()
    density=torch.tensor([[0.,-1000.]]*4,dtype=torch.float64,requires_grad=True)
    source_density=density.detach().clone().requires_grad_()
    # Keep the primary recursion itself authoritative while injecting a finite
    # emission fixture that forces its documented normalizer clamp branch.
    actual.emissions=lambda sequence,overrides=None:(density,None,None)
    reference.emissions=lambda sequence,overrides=None:(source_density,None,None)
    result=a.forward_sequence(actual,unit,block_size=2);expected=reference(unit)
    torch.testing.assert_close(result['log_scores'],expected['log_scores'],rtol=0,atol=0)
    assert expected['log_scores'][0,1]<-700
    (-result['log_likelihood'].sum()-result['log_prior']).backward()
    (-expected['log_likelihood'].sum()-expected['log_prior']).backward()
    torch.testing.assert_close(density.grad,source_density.grad,rtol=0,atol=0)
    compare_gradients(actual,reference)


def test_large_finite_hazard_and_emission_parameters_keep_original_gradients():
    units=[clock(0,length=15),clock(1,'e1',length=15)]
    actual=model('MX',states=3)
    with torch.no_grad():
        actual.exit_intercept.copy_(torch.tensor([-40.,40.,-15.]))
        actual.duration_beta.copy_(torch.tensor([[4.,-3.],[-2.,3.],[2.,-1.]]))
        actual.ar_raw.fill_(4.);actual.gain_mod.fill_(3.);actual.direction_mod.fill_(-4.)
    reference=copy.deepcopy(actual);n=sum(int(q.observed.sum()) for q in units)
    expected=primary_objective_gradient(reference,units,n)
    result=a.objective_gradient(actual,units,n,batch_size=2,block_size=4)
    torch.testing.assert_close(result,expected,rtol=2e-11,atol=2e-11)
    compare_gradients(actual,reference,rtol=2e-8,atol=2e-9)


@pytest.mark.parametrize('batch',[1,2,8])
def test_full_mixed_clock_cohort_has_one_prior_global_normalizer_and_all_nuisance_gradients(batch):
    units=[clock(0,length=9),clock(1,'e1',length=9),clock(2,length=7,width=5.)]
    actual=model();reference=copy.deepcopy(actual)
    n=sum(int(q.observed.sum()) for q in units)
    expected=primary_objective_gradient(reference,units,n)
    result=a.objective_gradient(actual,units,n,batch_size=batch,block_size=3)
    torch.testing.assert_close(result,expected,rtol=2e-12,atol=2e-12)
    compare_gradients(actual,reference)
    # Unused nuisance rows still receive their original prior gradients.
    assert actual.animal_z.grad[actual.animal_ids.index('unused')].abs().sum()>0


def test_original_clip_once_and_adam_updates_match_across_multiple_epochs():
    units=[clock(0,length=8),clock(1,'e1',length=8),clock(2,length=6,width=20.)]
    actual=model();reference=copy.deepcopy(actual);n=sum(int(q.observed.sum()) for q in units)
    optimizers=[torch.optim.Adam(value.parameters(),lr=.001) for value in (actual,reference)]
    for _ in range(4):
        left=a.objective_gradient(actual,units,n,batch_size=2,block_size=3)
        right=primary_objective_gradient(reference,units,n)
        torch.testing.assert_close(left,right,rtol=2e-11,atol=2e-11)
        norms=[torch.nn.utils.clip_grad_norm_(value.parameters(),.13) for value in (actual,reference)]
        torch.testing.assert_close(norms[0],norms[1],rtol=2e-10,atol=2e-11)
        for optimizer in optimizers:optimizer.step()
        for p,q in zip(actual.parameters(),reference.parameters()):torch.testing.assert_close(p,q,rtol=2e-9,atol=2e-10)


def test_unchanged_200_iteration_strong_wolfe_step_equivalence():
    units=[clock(0,length=6),clock(1,'e1',length=6)]
    actual=model('M0');reference=copy.deepcopy(actual);n=sum(int(q.observed.sum()) for q in units)
    histories=[]
    for value,gradient in [(actual,lambda:a.objective_gradient(actual,units,n,batch_size=2,block_size=2)),
                           (reference,lambda:primary_objective_gradient(reference,units,n))]:
        optimizer=torch.optim.LBFGS(value.parameters(),max_iter=200,line_search_fn='strong_wolfe')
        history=[]
        def closure():
            result=gradient();history.append(float(result));return result
        optimizer.step(closure);histories.append(history)
    left=a.objective_gradient(actual,units,n,batch_size=2,block_size=2)
    right=primary_objective_gradient(reference,units,n)
    torch.testing.assert_close(left,right,rtol=2e-7,atol=2e-8)
    compare_gradients(actual,reference,atol=3e-6,rtol=2e-4)
    for p,q in zip(actual.parameters(),reference.parameters()):torch.testing.assert_close(p,q,rtol=2e-5,atol=2e-6)
    assert abs(len(histories[0])-len(histories[1]))<=2


def test_frozen_zero_duration_hmm_is_preserved_without_selecting_a_collapse():
    units=[clock(0,length=12),clock(1,'e1',length=12)]
    actual=model();actual.duration_beta.data.zero_();actual.duration_beta.requires_grad_(False)
    reference=copy.deepcopy(actual);n=sum(int(q.observed.sum()) for q in units)
    expected=primary_objective_gradient(reference,units,n)
    result=a.objective_gradient(actual,units,n,batch_size=2,block_size=4)
    torch.testing.assert_close(result,expected,rtol=2e-12,atol=2e-12)
    compare_gradients(actual,reference)
    assert actual.duration_beta.grad is None


def test_second_derivatives_fail_instead_of_supplying_an_unqualified_hessian():
    base=torch.zeros(1,4,2,dtype=torch.float64,requires_grad=True)
    duration=torch.zeros(4,2,dtype=torch.float64,requires_grad=True)
    emission=torch.tensor([[[.2,-.1],[.5,.2],[-.3,.4],[.2,.1]]],dtype=torch.float64,requires_grad=True)
    destination=torch.tensor([[0.,1.],[1.,0.]],dtype=torch.float64,requires_grad=True)
    with pytest.raises(RuntimeError,match='first-order only'):
        torch.autograd.grad(a.duration_scores(base,duration,emission,destination).sum(),base,create_graph=True)
    # functional.hessian defaults to strict=False and can otherwise turn a
    # detached first gradient into a plausible but incorrect all-zero Hessian.
    with pytest.raises(RuntimeError,match='first-order only'):
        torch.autograd.functional.hessian(lambda x:a.duration_scores(x,duration,emission,destination).sum(),base)
    scope=a.qualification_scope()
    assert all(scope[key] is False for key in ('second_derivatives_supported','native_gpu_qualified','native_performance_qualified',
        'optimizer_path_native_qualified','MALA_acceptance_qualified','scientific_result','complete_declared_workload','admission_authority'))


def test_full_clock_memory_counts_do_not_claim_linear_arithmetic_or_hide_arrays():
    result=a.filter_memory_shape(8,2,101,10)
    assert result['full_clock_bins']==101 and result['block_size']==10
    assert result['saved_checkpoint_filter_elements']==16*(1+sum(range(10,101,10)))
    assert result['reverse_block_filter_elements_upper_bound']==16*11*101
    assert result['forward_hazard_elements']==16*101*100//2
    assert result['float64_filter_bytes_upper_bound']==8*(result['saved_checkpoint_filter_elements']+result['reverse_block_filter_elements_upper_bound'])


@pytest.mark.parametrize('change',['cap','irregular_clock','duplicate','different_lengths','different_widths','bad_normalizer','float32','nonfinite'])
def test_unqualified_or_changed_semantics_are_refused(change):
    units=[clock(0),clock(1)];value=model()
    if change=='cap':value.max_duration_bins=7
    elif change=='irregular_clock':units[0].times[3]+=1.
    elif change=='duplicate':units[1].animal_id=units[0].animal_id
    elif change=='different_lengths':units[1]=clock(1,length=12)
    elif change=='different_widths':units[1]=clock(1,width=5.)
    elif change=='float32':value=value.float()
    elif change=='nonfinite':
        with torch.no_grad():value.exit_intercept[0]=torch.inf
    with pytest.raises(ValueError):
        if change=='bad_normalizer':a.objective_gradient(value,units,1)
        else:a.forward_independent(value,units)
