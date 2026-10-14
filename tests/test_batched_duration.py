import copy

import numpy as np
import pytest
import torch

from horsehair.batched_duration import forward_independent_batch
from horsehair.semimarkov import MovementModel, Sequence


def sequence(animal, experiment, dimension=2, length=18):
    rng = np.random.default_rng(int(animal))
    observed = np.ones(length, bool)
    observed[3:5] = False
    y = rng.normal(size=(length, dimension))
    y[~observed] = np.nan
    return Sequence(animal, experiment, y, rng.normal(size=(length, 2)),
                    rng.normal(size=(length, 1)), rng.normal(size=(length, dimension)),
                    rng.normal(size=(length, dimension)), rng.normal(size=(length, dimension)),
                    observed, np.arange(length) * 10.)


@pytest.mark.parametrize("kind", ["M0", "MG", "MT", "MD", "MX"])
@pytest.mark.parametrize("states,cap", [(2, None), (2, 7), (3, None), (3, 7)])
def test_batch_preserves_scores_tail_prior_and_all_group_gradients(kind, states, cap):
    torch.set_num_threads(1)
    units = [sequence(str(i), "e" + str(i % 2)) for i in range(3)]
    reference = MovementModel(kind, 2, 1, animal_ids=["0", "1", "2", "unused"],
                              experiment_ids=["e0", "e1", "unused"], states=states,
                              max_duration_bins=cap)
    torch.manual_seed(33)
    with torch.no_grad():
        for parameter in reference.parameters():
            parameter.add_(torch.randn_like(parameter) * .1)
    batched = copy.deepcopy(reference)
    serial = [reference(unit) for unit in units]
    actual = forward_independent_batch(batched, units)
    for field in ("log_scores", "log_likelihood", "max_tail_probability"):
        expected = torch.cat([value[field] for value in serial], 0)
        torch.testing.assert_close(actual[field], expected, rtol=1e-11, atol=1e-11)
    torch.testing.assert_close(actual["log_prior"], reference.log_prior(), rtol=0, atol=0)
    loss = -sum(value["log_likelihood"].sum() for value in serial) - reference.log_prior()
    loss.backward()
    (-actual["log_likelihood"].sum() - actual["log_prior"]).backward()
    for (name, parameter), (batch_name, batch_parameter) in zip(reference.named_parameters(), batched.named_parameters()):
        assert name == batch_name
        if parameter.grad is None:
            assert batch_parameter.grad is None
        else:
            torch.testing.assert_close(batch_parameter.grad, parameter.grad, rtol=1e-10, atol=1e-10)


def test_unseen_ids_and_one_dimensional_batches_keep_original_marginal_context():
    torch.set_num_threads(1)
    units = [sequence("1", "unseen", dimension=1), sequence("2", "e", dimension=1)]
    model = MovementModel("MG", 2, 1, dimension=1, animal_ids=["2"], experiment_ids=["e"])
    expected = torch.cat([model(unit)["log_scores"] for unit in units], 0)
    torch.testing.assert_close(forward_independent_batch(model, units)["log_scores"], expected)


def test_batches_refuse_mismatched_clocks_and_duplicate_animals():
    model = MovementModel("MX", 2, 1)
    with pytest.raises(ValueError, match="empty"):
        forward_independent_batch(model, [])
    with pytest.raises(ValueError, match="equal clock"):
        forward_independent_batch(model, [sequence("1", "e"), sequence("2", "e", length=19)])
    second = sequence("2", "e")
    second.bin_seconds = 5
    second.times = np.arange(len(second.times)) * 5.
    with pytest.raises(ValueError, match="equal clock"):
        forward_independent_batch(model, [sequence("1", "e"), second])
    with pytest.raises(ValueError, match="Duplicate"):
        forward_independent_batch(model, [sequence("1", "e"), sequence("1", "e")])
