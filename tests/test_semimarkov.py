import copy
import numpy as np
import pytest
import torch

from horsehair.semimarkov import (MovementModel, Sequence, TrainingScaler,
                                 assert_disjoint, duration_basis, fit_model,
                                 simulate, student_t_logpdf)


def template(animal="a", experiment="e", length=48):
    return Sequence(animal, experiment, np.zeros((length, 2)), np.zeros((length, 2)),
                    np.linspace(-1, 1, length)[:, None], np.zeros((length, 2)),
                    np.zeros((length, 2)), np.tile([0., 1.], (length, 1)),
                    np.ones(length, dtype=bool), np.arange(length) * 10.)


def test_normalized_duration_filter_and_finite_gradient():
    torch.set_num_threads(1)
    sequence = template()
    model = MovementModel("MX", 2, 1, knots=[1., 2., 3.])
    output = model(sequence, return_filter=True)
    np.testing.assert_allclose(output["occupancy"].detach().numpy().sum(-1), 1, atol=1e-12)
    loss = -output["log_likelihood"].sum() - model.log_prior()
    loss.backward()
    assert torch.isfinite(loss)
    assert all(parameter.grad is None or parameter.grad.isfinite().all() for parameter in model.parameters())


def test_simulation_preserves_food_plane_zero_axes():
    sequence = template(length=20)
    sequence.food_vector[:] = [0., 1.]
    model = MovementModel("MG", 2, 1)
    generated, _ = simulate(model, sequence, seed=99)
    assert np.max(np.abs(generated.observations[:, 0])) > 0
    np.testing.assert_array_equal(generated.food_vector[:, 0], 0.)
    np.testing.assert_allclose(generated.food_vector[1:, 1], 1 - np.cumsum(generated.observations[:-1, 1]))


def test_transition_gradient_matches_finite_difference():
    torch.set_num_threads(1)
    model = MovementModel("MT", 2, 1)
    sequence, _ = simulate(model, template(length=20), seed=45)
    model.zero_grad()
    value = model(sequence)["log_likelihood"].sum()
    value.backward()
    analytical = model.transition_mod.grad[0, 0].item()
    epsilon = 1e-5
    with torch.no_grad():
        original = model.transition_mod[0, 0].item()
        model.transition_mod[0, 0] = original + epsilon
        plus = model(sequence)["log_likelihood"].sum().item()
        model.transition_mod[0, 0] = original - epsilon
        minus = model(sequence)["log_likelihood"].sum().item()
        model.transition_mod[0, 0] = original
    assert analytical == pytest.approx((plus - minus) / (2 * epsilon), rel=1e-4, abs=1e-5)


def test_exact_per_animal_gradient_accumulation_matches_full_objective():
    torch.set_num_threads(1)
    model = MovementModel("MX", 2, 1)
    sequences = [simulate(model, template(str(i), str(i), length=12), seed=i + 20)[0] for i in range(3)]
    normalizer = sum(int(sequence.observed.sum()) for sequence in sequences)
    model.zero_grad()
    full = (-model.log_prior() - sum(model(sequence)["log_likelihood"].sum() for sequence in sequences)) / normalizer
    full.backward()
    reference = {name: parameter.grad.clone() for name, parameter in model.named_parameters() if parameter.grad is not None}
    model.zero_grad()
    (-model.log_prior() / normalizer).backward()
    for sequence in sequences:
        (-model(sequence)["log_likelihood"].sum() / normalizer).backward()
    for name, parameter in model.named_parameters():
        if name in reference:
            torch.testing.assert_close(parameter.grad, reference[name], rtol=1e-10, atol=1e-11)


def test_gain_scales_mean_and_covariance_without_transition_change():
    model = MovementModel("MG", 2, 1)
    sequence = template()
    with torch.no_grad():
        model.axis_beta.fill_(1)
    _, mean, chol = model.emissions(sequence)
    override = {"gain_shift": torch.tensor([np.log(2)], dtype=torch.float64)}
    _, doubled_mean, doubled_chol = model.emissions(sequence, override)
    np.testing.assert_allclose(doubled_mean[:, 1].detach(), 2 * mean[:, 1].detach())
    np.testing.assert_allclose(doubled_chol[:, 1].detach(), 2 * chol[:, 1].detach())
    assert not torch.any(model.transition_mod)


def test_past_scores_invariant_to_future_observations():
    torch.set_num_threads(1)
    model = MovementModel("MG", 2, 1)
    sequence, _ = simulate(model, template(), seed=15)
    altered = copy.deepcopy(sequence)
    altered.observations[25:] += 1000
    a = model(sequence)["log_scores"][:, :25].detach()
    b = model(altered)["log_scores"][:, :25].detach()
    torch.testing.assert_close(a, b)


def test_training_scaler_and_animal_leakage():
    train = template()
    test = template("b", "f")
    train.covariates[:, 0] = np.arange(len(train.times))
    test.covariates[:, 0] = 1e9
    scaler = TrainingScaler.fit([train])
    assert scaler.mean[0] == pytest.approx(23.5)
    assert scaler.training_animals == ["a"]
    assert_disjoint([train], [test], experiment=True)
    with pytest.raises(ValueError, match="Animal leakage"):
        assert_disjoint([train], [train])


def test_direction_preserves_drift_magnitude():
    model = MovementModel("MD", 2, 1)
    sequence = template()
    with torch.no_grad():
        model.axis_beta.fill_(1.4)
        model.direction_mod.fill_(.8)
    _, mean, _ = model.emissions(sequence)
    norms = mean[:, 1].norm(dim=-1).detach().numpy()
    np.testing.assert_allclose(norms, 1.4, atol=1e-12)
    with pytest.raises(ValueError, match="two-dimensional"):
        MovementModel("MD", 2, 1, dimension=1)


def test_known_gain_only_fixture_favours_gain_and_recovers_sign():
    torch.set_num_threads(1)
    truth = MovementModel("MG", 2, 1, knots=[2, 3, 4])
    with torch.no_grad():
        truth.exit_intercept.copy_(torch.tensor([-1.2, -1.2]))
        truth.log_rest_scale.fill_(-2)
        truth.scale_separation.fill_(2)
        truth.gain_mod.fill_(.7)
        truth.axis_beta.fill_(1)
    sequences = [simulate(truth, template(f"a{i}", f"e{i // 2}", length=70), 100 + i)[0]
                 for i in range(6)]
    config = {"starts": 2, "max_epochs": 45, "learning_rate": .03,
              "lbfgs_iterations": 30, "patience": 20, "seed": 20}
    gain = fit_model(sequences[:4], "MG", config)
    transition = fit_model(sequences[:4], "MT", config)
    gain_score = np.mean([gain.score(s, warmup_seconds=0)["log_score"] for s in sequences[4:]])
    transition_score = np.mean([transition.score(s, warmup_seconds=0)["log_score"] for s in sequences[4:]])
    assert gain.model.gain_mod[0, 0].item() > 0
    assert gain_score > transition_score


def test_masked_observations_are_integrated_not_filled_as_rest():
    model = MovementModel("MG", 2, 1)
    sequence = template(length=20)
    sequence.observed[5:10] = False
    sequence.observations[5:10] = np.nan
    output = model(sequence)
    assert torch.isfinite(output["log_likelihood"]).all()
    torch.testing.assert_close(output["log_scores"][0, 5:10], torch.zeros(5, dtype=torch.float64), atol=1e-12, rtol=0)
