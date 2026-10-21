"""First-order technical probe for the unchanged full-age movement likelihood.

No fitting or admission path selects this operator. General time- and age-dependent
hazards retain O(B*K*T**2) arithmetic. A hand-derived reverse recursion and block
recomputation replace the per-clock automatic differentiation graph; retained
filter memory is O(B*K*(T**2/L + T*L)), with L approximately sqrt(T). This is not
an HMM collapse, a duration cap, or a change to clocks, priors or biological units.

Only first derivatives are implemented. Dense Hessians, higher derivatives,
posterior acceptance paths, native GPU accuracy and native full-clock performance
remain separate qualifications. Floating-point reductions need not be bitwise
identical to the serial primary, so optimizer-path equivalence must be measured.
Progress persistence and filesystem fsync costs are outside this numeric probe;
throttling telemetry is an independent operational improvement, not an adjoint
speedup or evidence of a faster scientific objective.
"""
from __future__ import annotations

import math
from typing import Any

import numpy as np
import torch

from .semimarkov import duration_basis


def _require(condition: bool, message: str) -> None:
    if not condition:raise ValueError(message)


def filter_memory_shape(batch: int, states: int, bins: int, block_size: int | None = None) -> dict[str, int]:
    """Count saved filters and a conservative one-block reverse filter bound.

    This excludes allocator overhead, emissions, optimizer state and temporary
    arithmetic tensors; native RSS/CUDA memory must still be measured.
    """
    _require(all(type(v) is int and v > 0 for v in (batch, states, bins)), 'Positive integral full tensor dimensions required')
    block = max(1, math.isqrt(bins)) if block_size is None else block_size
    _require(type(block) is int and block > 0, 'Positive integral checkpoint block required')
    block = min(block, bins)
    saved = batch*states*sum(max(1, start) for start in range(0, bins, block))
    reverse = batch*states*(min(block, bins)+1)*bins
    return {'batch':batch,'states':states,'full_clock_bins':bins,'block_size':block,
            'saved_checkpoint_filter_elements':saved,'reverse_block_filter_elements_upper_bound':reverse,
            'float64_filter_bytes_upper_bound':8*(saved+reverse),
            'forward_hazard_elements':batch*states*bins*(bins-1)//2}


def _step(base, duration, emission, destination, previous, t):
    if t:
        ages = previous.shape[-1]
        hazard = torch.sigmoid(base[:,t,:,None] + duration[:ages].T[None])
        exits = (previous*hazard).sum(-1)
        entrants = exits@destination
        predicted = torch.cat((entrants[:,:,None], previous*(1-hazard)), -1)
    else:
        predicted = previous
    shift = emission[:,t].amax(-1)
    scale = torch.exp(emission[:,t]-shift[:,None])
    weighted = predicted*scale[:,:,None]
    raw_normalizer = weighted.sum((1,2))
    normalizer = raw_normalizer.clamp_min(torch.finfo(emission.dtype).tiny)
    current = weighted/normalizer[:,None,None]
    return current, normalizer.log()+shift, normalizer, raw_normalizer, scale


class _DurationScores(torch.autograd.Function):
    @staticmethod
    def forward(ctx, base, duration, emission, destination, block):
        batch, bins, states = base.shape
        alpha = torch.ones(batch, states, 1, dtype=base.dtype, device=base.device)/states
        checkpoints, scores = [], []
        for t in range(bins):
            if t % block == 0:checkpoints.append(alpha)
            alpha, score, _, _, _ = _step(base,duration,emission,destination,alpha,t)
            scores.append(score)
        ctx.block = block
        ctx.save_for_backward(base,duration,emission,destination,*checkpoints)
        return torch.stack(scores,1)

    @staticmethod
    def backward(ctx, score_gradient):
        if torch.is_grad_enabled():
            raise RuntimeError('Duration adjoint is first-order only; higher derivatives require separately qualified primary/checkpointed Hessians')
        base,duration,emission,destination,*checkpoints = ctx.saved_tensors
        batch,bins,states = base.shape
        grad_base = torch.zeros_like(base)
        grad_duration = torch.zeros_like(duration)
        grad_emission = torch.zeros_like(emission)
        grad_destination = torch.zeros_like(destination)
        adjoint = torch.zeros(batch,states,bins,dtype=base.dtype,device=base.device)
        tiny = torch.finfo(base.dtype).tiny
        for block_index in reversed(range(len(checkpoints))):
            start = block_index*ctx.block
            end = min(start+ctx.block,bins)
            values = [checkpoints[block_index]]
            scales, normalizers, active = [], [], []
            for t in range(start,end):
                alpha,_,normalizer,raw,scale = _step(base,duration,emission,destination,values[-1],t)
                values.append(alpha);scales.append(scale);normalizers.append(normalizer);active.append(raw>=tiny)
            for offset in reversed(range(end-start)):
                t = start+offset
                previous,current = values[offset],values[offset+1]
                z = normalizers[offset]
                cotangent = score_gradient[:,t]
                centered = adjoint + active[offset][:,None,None]*(cotangent-(adjoint*current).sum((1,2)))[:,None,None]
                weighted_gradient = centered/z[:,None,None]
                predicted_gradient = weighted_gradient*scales[offset][:,:,None]
                # The residual handles the primary's clamp-min normalizer and
                # max-shift derivative even when all weighted states underflow.
                log_emission_gradient = (centered*current).sum(-1)
                residual = cotangent-log_emission_gradient.sum(-1)
                maximum = emission[:,t].amax(-1,keepdim=True)
                maximizers = (emission[:,t]==maximum).to(emission.dtype)
                log_emission_gradient += residual[:,None]*maximizers/maximizers.sum(-1,keepdim=True)
                grad_emission[:,t] = log_emission_gradient
                if t:
                    ages = previous.shape[-1]
                    hazard = torch.sigmoid(base[:,t,:,None]+duration[:ages].T[None])
                    entrance_gradient = predicted_gradient[:,:,0]
                    exit_gradient = entrance_gradient@destination.T
                    stay_gradient = predicted_gradient[:,:,1:]
                    logit_gradient = previous*(exit_gradient[:,:,None]-stay_gradient)*hazard*(1-hazard)
                    grad_base[:,t] = logit_gradient.sum(-1)
                    grad_duration[:ages] += logit_gradient.sum(0).T
                    exits = (previous*hazard).sum(-1)
                    grad_destination += exits.T@entrance_gradient
                    adjoint = hazard*exit_gradient[:,:,None]+(1-hazard)*stay_gradient
        return grad_base,grad_duration,grad_emission,grad_destination,None


def duration_scores(base: torch.Tensor, duration: torch.Tensor, emission: torch.Tensor,
                    destination: torch.Tensor, *, block_size: int | None = None) -> torch.Tensor:
    """Return every primary log-score with a checkpointed analytic first adjoint.

    ``base`` and ``emission`` have shape (independent-context, full-time, state),
    ``duration`` (full-time, state), and ``destination`` (state, state). The model
    wrappers construct the unchanged masked-softmax transition matrix; the tensor
    primitive also accepts a finite linear transition matrix for derivative tests.
    """
    values = (base,duration,emission,destination)
    _require(all(isinstance(v,torch.Tensor) and v.dtype==torch.float64 for v in values), 'Float64 tensors required for this technical qualification')
    _require(base.ndim==3 and base.shape==emission.shape, 'Aligned full-clock base/emission tensors required')
    batch,bins,states = base.shape
    _require(batch>0 and bins>=2 and states in (2,3) and duration.shape==(bins,states)
             and destination.shape==(states,states), 'Full duration/state tensor shapes disagree')
    _require(all(v.device==base.device and bool(torch.isfinite(v).all()) for v in values), 'Finite same-device technical tensors required')
    _require(bool((destination>=0).all()) and bool((destination.sum(-1)>0).all()), 'Nonnegative nonempty transition rows required')
    block = filter_memory_shape(batch,states,bins,block_size)['block_size']
    return _DurationScores.apply(base,duration,emission,destination,block)


def _shared(model, bins, width):
    _require(model.max_duration_bins is None, 'Only the unchanged uncapped full-age recursion is qualified by this probe')
    ages = torch.arange(1,bins+1,dtype=model.knots.dtype,device=model.knots.device)*width
    duration = duration_basis(ages,model.knots)@model.duration_beta.T
    diagonal = torch.eye(model.states,dtype=torch.bool,device=model.knots.device)
    destination = torch.softmax(model.destination_logits.masked_fill(diagonal,-torch.inf),-1)
    return duration,destination


def forward_sequence(model: Any, sequence: Any, random_effects: torch.Tensor | None = None,
                     overrides: dict[str,torch.Tensor] | None = None, *, block_size: int | None = None) -> dict[str,torch.Tensor]:
    """Full clock for one original animal, optionally all predictive quadrature rows.

    Filter occupancies/PIT diagnostics are deliberately not supplied. Validation
    can keep the primary ``Fit.score`` path until that separate interface is proven.
    """
    sequence.validate();override=overrides or {}
    effects = model._random_effect(sequence)[None] if random_effects is None else random_effects
    _require(effects.ndim==2 and effects.shape[0]>0 and effects.shape[1]==model.states, 'Original random-effect context matrix required')
    x,modulation = model._tensor(sequence.covariates),model._tensor(sequence.modulation)
    base = (x@model.context_beta.T + modulation@override.get('transition_mod',model.transition_mod).T
            + model.exit_intercept + override.get('transition_shift',model._tensor(np.zeros(model.states))))
    base = base[None]+effects[:,None,:]
    emission = model.emissions(sequence,overrides)[0][None].expand(len(effects),-1,-1)
    duration,destination = _shared(model,len(sequence.times),sequence.bin_seconds)
    scores = duration_scores(base,duration,emission,destination,block_size=block_size)
    return {'log_scores':scores,'log_likelihood':scores.sum(1),'log_prior':model.log_prior(),
            'max_tail_probability':torch.zeros(len(effects),dtype=base.dtype,device=base.device)}


def _independent_inputs(model, sequences):
    _require(bool(sequences), 'Nonempty independent full clocks required')
    for sequence in sequences:sequence.validate()
    _require(len({q.animal_id for q in sequences})==len(sequences), 'Duplicate independent animal representation refused')
    _require(len({(len(q.times),float(q.bin_seconds)) for q in sequences})==1, 'Equal full lengths/bin widths required within an exact batch')
    _require(all(q.observations.shape[1]==model.dimension for q in sequences), 'Original observation dimensions disagree')
    x = model._tensor(np.stack([q.covariates for q in sequences]))
    modulation = model._tensor(np.stack([q.modulation for q in sequences]))
    effects = torch.stack([model._random_effect(q) for q in sequences])
    base = x@model.context_beta.T + modulation@model.transition_mod.T + model.exit_intercept + effects[:,None,:]
    emission = torch.stack([model.emissions(q)[0] for q in sequences])
    duration,destination = _shared(model,len(sequences[0].times),sequences[0].bin_seconds)
    return base,duration,emission,destination


def forward_independent(model: Any, sequences: list[Any], *, block_size: int | None = None) -> dict[str,torch.Tensor]:
    inputs = _independent_inputs(model,sequences)
    scores = duration_scores(*inputs,block_size=block_size)
    return {'log_scores':scores,'log_likelihood':scores.sum(1),'log_prior':model.log_prior(),
            'max_tail_probability':torch.zeros(len(sequences),dtype=scores.dtype,device=scores.device)}


def objective_gradient(model: Any, sequences: list[Any], normalizer: int, *, batch_size: int = 8,
                       block_size: int | None = None) -> torch.Tensor:
    """One prior/global normalizer and all original nuisance rows, no optimizer.

    Contiguous equal-clock batches retain the source roster order. Different
    clock lengths or bin widths start a new batch; no sequence is padded, cropped,
    resampled, dropped, or assigned a different random effect.
    """
    _require(type(batch_size) is int and batch_size>0, 'Positive integral independent batch size required')
    _require(bool(sequences) and normalizer==sum(int(q.observed.sum()) for q in sequences) and normalizer>0,
             'Complete-cohort observed-bin normalizer required')
    _require(len({q.animal_id for q in sequences})==len(sequences), 'Complete unique source roster required')
    model.zero_grad()
    prior = -model.log_prior()/normalizer;prior.backward();value=prior.detach().clone()
    start=0
    while start<len(sequences):
        key=(len(sequences[start].times),float(sequences[start].bin_seconds));end=start+1
        while end<min(start+batch_size,len(sequences)) and (len(sequences[end].times),float(sequences[end].bin_seconds))==key:end+=1
        inputs=_independent_inputs(model,sequences[start:end])
        likelihood=duration_scores(*inputs,block_size=block_size).sum(1)
        loss=-likelihood.sum()/normalizer
        if not bool(torch.isfinite(loss)):raise FloatingPointError('Nonfinite exact full-clock technical likelihood')
        loss.backward()
        for row in likelihood:value=value-row.detach()/normalizer
        start=end
    if not all(p.grad is None or bool(torch.isfinite(p.grad).all()) for p in model.parameters()):
        raise FloatingPointError('Nonfinite full-cohort technical adjoint gradient')
    return value


def qualification_scope() -> dict[str,Any]:
    return {'operator':'full-age checkpointed first adjoint','arithmetic_complexity':'O(batch*states*full_clock_bins**2)',
            'model_scope':'uncapped MovementModel; original comparator kernels are outside this probe',
            'second_derivatives_supported':False,'filter_occupancies_supported':False,
            'end_to_end_profile_IO_qualified':False,
            'native_performance_qualified':False,'native_gpu_qualified':False,'optimizer_path_native_qualified':False,
            'MALA_acceptance_qualified':False,'scientific_result':False,'complete_declared_workload':False,'admission_authority':False}
