"""Exact independent-animal duration recursion for resource qualification.

This kernel keeps every age and each animal's random effects. It is deliberately
not selected by the scientific fitting path until native performance and complete
workload admission have been qualified. No clock or biological unit is pooled.
"""
from __future__ import annotations

from typing import Any
import numpy as np
import torch

from .semimarkov import duration_basis


def forward_independent_batch(model: Any, sequences: list[Any]) -> dict[str, torch.Tensor]:
    """Vectorize the original recursion over independent equal-clock animals.

    Gradients include exactly the original model's animal and experiment rows.
    The returned log likelihood has one row per input animal. The prior is
    returned once, matching accumulated likelihood fitting rather than counting
    it once per animal. Unequal clocks must use separate batches.
    """
    if not sequences:
        raise ValueError("An independent-animal batch cannot be empty")
    for sequence in sequences:
        sequence.validate()
    lengths = {len(sequence.times) for sequence in sequences}
    widths = {float(sequence.bin_seconds) for sequence in sequences}
    if len(lengths) != 1 or len(widths) != 1:
        raise ValueError("Exact batches require equal clock lengths and bin widths")
    if len({sequence.animal_id for sequence in sequences}) != len(sequences):
        raise ValueError("Duplicate biological animal representation in a batch")
    n, width = next(iter(lengths)), next(iter(widths))
    if any(sequence.observations.shape[1] != model.dimension for sequence in sequences):
        raise ValueError("Batch observation dimensions disagree with the fitted model")
    cap = n if model.max_duration_bins is None else int(model.max_duration_bins)
    if cap < 2:
        raise ValueError("Duration cap must allow at least two bins")
    x = model._tensor(np.stack([sequence.covariates for sequence in sequences]))
    modulation = model._tensor(np.stack([sequence.modulation for sequence in sequences]))
    effects = torch.stack([model._random_effect(sequence) for sequence in sequences])
    ages = torch.arange(1, cap + 1, dtype=x.dtype, device=x.device) * width
    duration_term = duration_basis(ages, model.knots) @ model.duration_beta.T
    hazards = x @ model.context_beta.T + modulation @ model.transition_mod.T + model.exit_intercept
    emissions = torch.stack([model.emissions(sequence)[0] for sequence in sequences])
    diagonal = torch.eye(model.states, dtype=torch.bool, device=x.device)
    destinations = torch.softmax(model.destination_logits.masked_fill(diagonal, -torch.inf), -1)
    alpha = torch.ones(len(sequences), model.states, 1, dtype=x.dtype, device=x.device) / model.states
    scores, tails = [], []
    for t in range(n):
        if t:
            age_count = alpha.shape[-1]
            hazard = torch.sigmoid(hazards[:, t, :, None] + effects[:, :, None]
                                   + duration_term[:age_count].T[None])
            entrants = (alpha * hazard).sum(-1) @ destinations
            stays = alpha * (1 - hazard)
            if age_count < cap:
                alpha = torch.cat([entrants[:, :, None], stays], -1)
            else:
                tail = stays[:, :, -1:] + stays[:, :, -2:-1]
                alpha = torch.cat([entrants[:, :, None], stays[:, :, :-2], tail], -1)
        emission = emissions[:, t]
        shift = emission.max(-1).values
        weighted = alpha * torch.exp(emission[:, :, None] - shift[:, None, None])
        normalizer = weighted.sum((1, 2)).clamp_min(torch.finfo(x.dtype).tiny)
        alpha = weighted / normalizer[:, None, None]
        scores.append(torch.log(normalizer) + shift)
        tails.append(alpha[:, :, -1].sum(-1) if t + 1 >= cap and cap < n
                     else torch.zeros(len(sequences), dtype=x.dtype, device=x.device))
    log_scores = torch.stack(scores, 1)
    return {"log_scores": log_scores, "log_likelihood": log_scores.sum(1),
            "max_tail_probability": torch.stack(tails, 1).max(1).values,
            "log_prior": model.log_prior()}
