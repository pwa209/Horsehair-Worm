import numpy as np
import pandas as pd
import pytest
from scipy import io

from horsehair.behaviour import (aft_objective, bin_trajectories,
                                fit_censored_latency, fit_mantis_choice,
                                interpolate_short_gaps, read_fly_source,
                                to_sequences, validate_observations,
                                _history_dataset, fit_comparator, discover_fly_sources)
from horsehair.interventions import (calibration_half, shared_control_effects,
                                    restricted_mean_duration)


def source_fixture(tmp_path, length=361):
    times = np.arange(length) / 3.
    positions = np.zeros((length, 2, 2))
    positions[:, 0, :] = 1
    positions[:, 1, :] = times[:, None] / 120
    io.savemat(tmp_path / "tracking.mat", {"expmt": {"Centroid": {"data": positions},
                  "recording_times": times, "parameters": {"target_rate": 3}}})
    pd.DataFrame({"ROI": [1, 2], "Geno": ["Cs", "Cs"], "Sex": ["M", "F"],
                  "Rx": ["Exposed", "Uninfected"], "Outcome": [1, 0], "Last_mvmt": [200, 0]}).to_excel(tmp_path / "survival.xlsx", index=False)
    source = {"mat": "tracking.mat", "metadata": "survival.xlsx", "experiment_id": "source1",
              "time_path": "expmt.recording_times", "geometry": 30, "food_end": 0,
              "exposure_elapsed_at_start_hours": 24,
              "exposure_values": {"Exposed": 1, "Uninfected": 0}}
    return read_fly_source(source, tmp_path), source


def test_source_adapter_and_missing_clock_fail_closed(tmp_path):
    frame, source = source_fixture(tmp_path)
    assert frame.animal_id.nunique() == 2
    assert frame[frame.roi_id == 1].exposure_status.eq(1).all()
    assert frame[frame.roi_id == 2].exposure_status.eq(0).all()
    source.pop("time_path")
    with pytest.raises(ValueError, match="recording clock"):
        read_fly_source(source, tmp_path)


def test_native_classifier_discovery_uses_intervals_and_roi_bounds(tmp_path):
    name = "03-22-2019-18-21-01__Circadian_CsWF-BoardC9_MF_Emuscae_1-128_Day3"
    directory = tmp_path / "Fig 4 + Supp" / "experiment"
    directory.mkdir(parents=True)
    n = 31
    positions = np.zeros((n, 2, 2))
    positions[:, 0, :] = 1
    positions[:, 1, :] = .5
    io.savemat(directory / f"{name}_margoConvert.mat", {"expmt": {
        "Centroid": {"data": positions}, "Time": {"data": np.full(n, 1 / 3)},
        "ROI": {"bounds": np.array([[0., 0., 2., 2.], [0., 0., 2., 2.]])},
        "parameters": {"target_rate": 3, "mm_per_pix": .31}}})
    pd.DataFrame({"Geno": ["Cs", "Cs"], "Sex": ["M", "F"], "Rx": ["Exposed", "Exposed"]}).to_excel(
        directory / "03-22-2019-survival_data.xlsx", index=False)
    discovered = discover_fly_sources(tmp_path, tmp_path)
    assert discovered["all_candidates_resolved"]
    source = discovered["fly_sources"][0]
    assert source["time_kind"] == "intervals"
    assert source["analysis_role"] == "behavioural_development"
    data = read_fly_source(source, tmp_path)
    first = data[data.roi_id == 1].iloc[0]
    assert first.record_time_s == pytest.approx(18 * 3600 + 21 * 60 + 1 + 1 / 3)
    assert first.y == pytest.approx((2 - .5) * .31)
    assert first.coordinate_units == "mm"


def test_native_processed_three_hz_requires_deposited_validator_and_trims_only_blank_roi(tmp_path):
    from horsehair.behaviour import audit_exposure_source
    name = "03-22-2019-18-21-01__Circadian_CsWF-BoardC9_MF_Emuscae_1-128_Day3"
    directory = tmp_path / "Fig 4 + Supp" / "experiment"
    directory.mkdir(parents=True)
    positions = np.full((31, 2, 2), .5)
    io.savemat(directory / f"{name}_margoConvert.mat", {"expmt": {
        "Centroid": {"data": positions}, "ROI": {"bounds": np.array([[0., 0., 2., 4.], [0., 0., 2., 4.]])},
        "parameters": {"target_rate": 3, "mm_per_pix": 1}}})
    sheet = directory / "03-22-2019-survival_data.xlsx"
    pd.DataFrame({"ROI": [1, 2, None], "Geno": ["Cs", "Cs", None], "Sex": ["M", "F", None],
        "Rx": [None, None, None], "Outcome": ["Cadaver", "Survivor", None], "Notes": [None, None, "Explanatory trailing note"]}).to_excel(sheet, index=False)
    missing = discover_fly_sources(tmp_path, tmp_path)
    assert not missing["fly_sources"]
    assert "timestamp" in missing["unresolved"][0]["reason"]
    (tmp_path / "triggerOnDeath2019Fun.m").write_text("data.params.fr = 3;\nif expmt.parameters.target_rate ~= 3\nerror('rate');\nend\n")
    found = discover_fly_sources(tmp_path, tmp_path)
    assert found["all_candidates_resolved"]
    source = found["fly_sources"][0]
    assert source["excluded_blank_roi_excel_rows"] == [4]
    frame = read_fly_source(source, tmp_path)
    assert frame.exposure_status.eq(1).all()  # survival never becomes unexposed
    np.testing.assert_allclose(np.diff(frame[frame.roi_id.eq(1)].record_time_s), 1 / 3)
    assert frame.coordinate_units.eq("native_ROI_height").all()
    assert frame.y.iloc[0] == pytest.approx(.875)
    audit = audit_exposure_source(source, tmp_path)
    assert not audit["ready"] and any("offset" in reason for reason in audit["pending"])
    assert audit["native_exposure_levels"] == []
    assert audit["native_exposure_missing_count"] == 2
    import json
    from horsehair.core import atomic_json
    # Exercise the same strict writer used by native source-audit; successful
    # discovery alone did not catch pandas3's NaN metadata serialization.
    atomic_json(tmp_path / "fly_adapters.json", found)
    assert json.loads((tmp_path / "fly_adapters.json").read_text())["fly_sources"][0]["formal_exposure_readiness"]["ready"] is False
    (tmp_path / "triggerOnDeath2019Fun.m").write_text("data.params.fr = 30;\n")
    assert not discover_fly_sources(tmp_path, tmp_path)["fly_sources"]


def test_unknown_missing_rx_is_json_safe_and_remains_unknown(tmp_path):
    import json
    from horsehair.behaviour import audit_exposure_source
    pd.DataFrame({"Rx": [None, "Unrecorded treatment", "", "Unexposed"]}).to_excel(tmp_path / "metadata.xlsx", index=False)
    source = {"metadata": "metadata.xlsx"}
    audit = audit_exposure_source(source, tmp_path)
    assert not audit["ready"]
    assert audit["native_exposure_missing_count"] == 2
    assert audit["native_exposure_levels"] == ["Unexposed", "Unrecorded treatment"]
    assert any("<missing>" in reason and "Unrecorded treatment" in reason for reason in audit["pending"])
    assert json.loads(json.dumps(audit, allow_nan=False))["ready"] is False


@pytest.mark.parametrize("ids", [[1, 1], [1, 2.5], [1, 3], [1, "note"]])
def test_roi_labels_are_not_rounded_or_guessed(ids):
    from horsehair.behaviour import _validated_roi_metadata
    with pytest.raises(ValueError):
        _validated_roi_metadata(pd.DataFrame({"ROI": ids}), 2, {})


def test_author_summit_one_based_window_and_baseline_integral():
    from horsehair.behaviour import source_summit_metric
    speed = np.array([0, 0, 2, 4, 4, 1, 0, 2, 0, 0, 0, 0], float)[:, None]
    height = np.arange(12, dtype=float)[:, None]
    result = source_summit_metric(speed, height,
        {"smoothWin": 1, "summitStart": 3, "endPad": 0, "minSumDur": 0, "moveThresh": .1},
        {"lastFrames": 9, "fr": 3})
    assert result["sumStart"] == 3 and result["sumEnd"] == 8
    assert result["sumInt"][0] == pytest.approx(.2)
    assert result["sumHt"][0] == 5
    assert result["sumDur"] == pytest.approx(5 / 10800)


def test_full_group_curvature_matches_dense_posterior_hessian():
    import torch
    from torch.func import functional_call
    from horsehair.behaviour import full_parameter_laplace
    from horsehair.semimarkov import MovementModel, Sequence, TrainingScaler, Fit
    seqs = [Sequence(str(i), str(i), np.full((6, 2), .02), np.zeros((6, 1)),
        np.linspace(-1, 1, 6)[:, None], np.zeros((6, 2)), np.zeros((6, 2)), np.tile([0., 1.], (6, 1)),
        np.ones(6, bool), np.arange(6) * 10.) for i in range(2)]
    scaler = TrainingScaler.fit(seqs)
    model = MovementModel("MT", 1, 1, animal_ids=["0", "1"], experiment_ids=["0", "1"])
    fit = Fit(model, scaler, {})
    named = list(model.named_parameters())
    initial = torch.cat([parameter.detach().flatten() for _, parameter in named]).requires_grad_()
    transformed = [scaler.transform(sequence) for sequence in seqs]
    def density(theta):
        params, offset = {}, 0
        for name, parameter in named:
            params[name] = theta[offset:offset + parameter.numel()].reshape(parameter.shape)
            offset += parameter.numel()
        outputs = [functional_call(model, params, (sequence,)) for sequence in transformed]
        return -sum(out["log_likelihood"].sum() for out in outputs) - outputs[0]["log_prior"]
    expected = torch.autograd.functional.hessian(density, initial).detach().numpy()
    expected_gradient = torch.autograd.grad(density(initial), initial)[0].detach().numpy()
    actual = full_parameter_laplace(fit, seqs, include_curvature=True)
    np.testing.assert_allclose(actual["hessian"], expected, atol=1e-9, rtol=1e-9)
    np.testing.assert_allclose(actual["gradient"], expected_gradient, atol=1e-9, rtol=1e-9)
    assert not actual["parameter_uncertainty_validated"]
    assert "interval_low" not in actual


def test_mantis_heldout_design_uses_only_training_scaling():
    from horsehair.behaviour import _mantis_design_contract
    train = pd.DataFrame({"infection": [0, 1, 0, 1], "dop": [2., 4., 6., 8.],
        "log_illuminance": [1., 2., 3., 4.], "stimulus_side": "R", "year": "2024", "box": "A"})
    test = train.iloc[:1].copy()
    test.dop = 100
    design, names, contract = _mantis_design_contract(train, test)
    assert contract["continuous"]["dop"]["mean"] == 5
    assert design[0, names.index("dop")] == pytest.approx(95 / np.std([2, 4, 6, 8]))
    test.box = "unknown"
    with pytest.raises(ValueError, match="outside training"):
        _mantis_design_contract(train, test)


def test_constant_training_illuminance_cannot_be_extrapolated_silently():
    from horsehair.behaviour import _mantis_design_contract
    train = pd.DataFrame({"infection": [0, 1, 0, 1], "dop": [.2, .5, .7, .9],
        "log_illuminance": np.log(10.), "stimulus_side": "R", "year": "2021", "box": "A"})
    test = train.iloc[:1].copy()
    _, _, contract = _mantis_design_contract(train, test)
    assert contract["continuous"]["log_illuminance"]["constant_training_value"] == pytest.approx(np.log(10.))
    test.log_illuminance = np.log(100.)
    with pytest.raises(ValueError, match="omitted training covariate was constant"):
        _mantis_design_contract(train, test)


def _mantis_gradient_fixture():
    from horsehair.behaviour import _resolve_behavioural_design
    rng = np.random.default_rng(9017)
    year = np.tile([2021, 2022, 2023], 24)
    infection, dop = rng.integers(0, 2, len(year)), rng.normal(size=len(year))
    original = np.column_stack([np.ones(len(year)), infection, dop, infection * dop,
                                year == 2022, year == 2023, year == 2021])
    names = ["Intercept", "infection", "dop", "infection:dop", "year_2022", "year_2023", "box_D"]
    x, names, audit = _resolve_behavioural_design(original, names, ["infection", "dop", "infection:dop"])
    precision = np.asarray(audit["prior_precision"])
    assert np.max(np.abs(precision - np.diag(np.diag(precision)))) > .01
    # Every individual has repeated records; group membership is independent
    # of infection and assay context in this synthetic derivative fixture.
    groups = np.tile(np.arange(8), 9)
    return rng, x, names, precision, groups


def _central_gradient(function, theta, step=1e-6):
    result = np.empty(len(theta))
    for index in range(len(theta)):
        offset = np.zeros_like(theta)
        offset[index] = step
        result[index] = (function(theta + offset) - function(theta - offset)) / (2 * step)
    return result


@pytest.mark.parametrize("repeated_groups", [False, True])
def test_binary_map_analytic_gradient_matches_central_differences_with_induced_prior(repeated_groups):
    from horsehair.behaviour import _binary_map_value_gradient
    rng, x, names, precision, groups = _mantis_gradient_fixture()
    group_index = groups if repeated_groups else None
    theta = rng.normal(0, .3, x.shape[1] + (len(np.unique(groups)) + 1 if repeated_groups else 0))
    if repeated_groups:
        theta[-1] = -.7
    y = rng.integers(0, 2, len(x)).astype(float)
    value, actual = _binary_map_value_gradient(theta, x, y, precision, group_index)
    expected = _central_gradient(lambda point: _binary_map_value_gradient(point, x, y, precision, group_index)[0], theta)
    assert np.isfinite(value)
    np.testing.assert_allclose(actual, expected, rtol=2e-7, atol=2e-8)


def test_multinomial_map_analytic_gradient_matches_central_differences_with_induced_prior():
    from horsehair.behaviour import _multinomial_map_value_gradient
    rng, x, names, precision, groups = _mantis_gradient_fixture()
    theta, y = rng.normal(0, .3, x.shape[1] * 2), rng.integers(0, 3, len(x))
    value, actual = _multinomial_map_value_gradient(theta, x, y, precision)
    expected = _central_gradient(lambda point: _multinomial_map_value_gradient(point, x, y, precision)[0], theta)
    assert np.isfinite(value)
    np.testing.assert_allclose(actual, expected, rtol=2e-7, atol=2e-8)


def test_analytic_binary_and_multinomial_fits_match_legacy_objectives_and_predictions():
    from scipy import optimize, special
    from horsehair.behaviour import _binary_map_value_gradient, _multinomial_map_value_gradient, _map_binary
    rng, x, names, precision, groups = _mantis_gradient_fixture()
    p, g = x.shape[1], len(np.unique(groups))
    y = rng.integers(0, 2, len(x)).astype(float)
    def legacy_binary(theta):
        sd = np.exp(theta[-1])
        eta = x @ theta[:p] + theta[p:p + g][groups] * sd
        return float(np.sum(np.logaddexp(0, eta) - y * eta) + .5 * theta[:p] @ precision @ theta[:p]
                     + .5 * np.sum(theta[p:p + g] ** 2) + .5 * (sd / .5) ** 2 - theta[-1])
    initial = np.zeros(p + g + 1)
    initial[-1] = -1.5
    options = {"maxiter": 2000, "gtol": 1e-6}
    legacy = optimize.minimize(legacy_binary, initial, method="BFGS", options=options)
    analytic = optimize.minimize(_binary_map_value_gradient, initial, args=(x, y, precision, groups),
        jac=True, method="BFGS", options=options)
    assert np.linalg.norm(legacy.jac) < .02 and np.linalg.norm(analytic.jac) < .02
    assert analytic.fun == pytest.approx(legacy.fun, abs=1e-8)
    predict = lambda theta: special.expit(x @ theta[:p] + theta[p:p + g][groups] * np.exp(theta[-1]))
    np.testing.assert_allclose(predict(analytic.x), predict(legacy.x), atol=2e-6, rtol=2e-6)
    production = _map_binary(x, y, names, groups.astype(str), precision)
    assert production["objective"] == pytest.approx(legacy.fun, abs=1e-8)
    assert production["gradient_method"] == "exact_analytic"
    y = rng.integers(0, 3, len(x))
    def legacy_multinomial(theta):
        logits = np.column_stack([x @ theta[:p], x @ theta[p:], np.zeros(len(x))])
        return float(np.sum(special.logsumexp(logits, axis=1) - logits[np.arange(len(x)), y])
                     + .5 * theta[:p] @ precision @ theta[:p] + .5 * theta[p:] @ precision @ theta[p:])
    legacy = optimize.minimize(legacy_multinomial, np.zeros(p * 2), method="BFGS")
    analytic = optimize.minimize(_multinomial_map_value_gradient, np.zeros(p * 2),
        args=(x, y, precision), jac=True, method="BFGS")
    assert np.linalg.norm(legacy.jac) < .02 and np.linalg.norm(analytic.jac) < .02
    assert analytic.fun == pytest.approx(legacy.fun, abs=1e-8)
    predict = lambda theta: special.softmax(np.column_stack([x @ theta[:p], x @ theta[p:], np.zeros(len(x))]), axis=1)
    np.testing.assert_allclose(predict(analytic.x), predict(legacy.x), atol=2e-6, rtol=2e-6)


def test_mantis_validation_checks_cheap_heldout_support_before_fitting(monkeypatch):
    import horsehair.behaviour as behaviour
    frame = pd.DataFrame({"individual_id": ["a", "b", "c"], "date": ["d1", "d2", "d3"],
        "infection": [0, 1, 0], "dop": [.2, .4, .8], "illuminance": 10.,
        "stimulus_side": "R", "year": "2021", "box": ["A", "B", "C"],
        "response": ["middle", "polarized", "unpolarized"]})
    def unavailable_fit(*args, **kwargs):
        pytest.fail("An unsupported held-out categorical level should be rejected before fitting")
    monkeypatch.setattr(behaviour, "fit_mantis_choice", unavailable_fit)
    result = behaviour.validate_mantis_choice(frame, {"dop_units": "source"})
    assert result["status"] == "pending" and not result["scores"]
    assert len(result["pending"]) == 6
    assert all("outside training context support" in reason for reason in result["pending"])
    # A constant omitted training covariate also rejects its changed held-out
    # value before optimization; other supported folds may reach the fitter.
    frame.box = "A"
    frame.illuminance = [10., 10., 100.]
    attempted = []
    def observe_training(train, *args, **kwargs):
        attempted.append(set(train.individual_id))
        raise RuntimeError("Synthetic fitter sentinel for supported context")
    monkeypatch.setattr(behaviour, "fit_mantis_choice", observe_training)
    result = behaviour.validate_mantis_choice(frame, {"dop_units": "source"})
    assert {"a", "b"} not in attempted
    assert sum("omitted training covariate was constant" in reason for reason in result["pending"]) == 2


@pytest.mark.parametrize("invalid_field", ["x", "jac"])
def test_nonfinite_binary_map_parameters_or_gradient_cannot_pass_convergence(monkeypatch, invalid_field):
    from types import SimpleNamespace
    import horsehair.behaviour as behaviour
    fit = SimpleNamespace(fun=1., x=np.zeros(2), jac=np.zeros(2), hess_inv=np.eye(2), message="Synthetic numerical failure")
    getattr(fit, invalid_field)[0] = np.nan
    monkeypatch.setattr(behaviour.optimize, "minimize", lambda *args, **kwargs: fit)
    with pytest.raises(RuntimeError, match="did not converge"):
        behaviour._map_binary(np.column_stack([np.ones(4), [0, 1, 0, 1]]), np.array([0, 1, 1, 0]), ["Intercept", "infection"])


@pytest.mark.parametrize("invalid_field", ["x", "jac"])
def test_nonfinite_multinomial_map_parameters_or_gradient_cannot_pass_convergence(monkeypatch, invalid_field):
    from types import SimpleNamespace
    import horsehair.behaviour as behaviour
    rng = np.random.default_rng(23)
    n = 90
    frame = pd.DataFrame({"infection": np.tile([0, 1], n // 2), "dop": rng.uniform(.1, .9, n),
        "illuminance": 10., "stimulus_side": "R", "year": "2021", "box": "A",
        "response": np.tile(["middle", "polarized", "unpolarized"], n // 3)})
    monkeypatch.setattr(behaviour, "_map_binary", lambda *args, **kwargs: {})
    def numerically_unavailable(function, initial, *args, **kwargs):
        fit = SimpleNamespace(fun=1., x=np.zeros_like(initial), jac=np.zeros_like(initial), hess_inv=np.eye(len(initial)))
        getattr(fit, invalid_field)[0] = np.nan
        return fit
    monkeypatch.setattr(behaviour.optimize, "minimize", numerically_unavailable)
    with pytest.raises(RuntimeError, match="did not converge"):
        behaviour.fit_mantis_choice(frame, {"dop_units": "source"})


def test_nuisance_alias_reduction_preserves_predictors_and_original_gaussian_prior():
    from horsehair.behaviour import _resolve_behavioural_design
    rng = np.random.default_rng(307)
    infection = np.tile([0., 1.], 30)
    dop = rng.normal(size=len(infection))
    context = np.repeat([0., 1.], 30)
    x = np.column_stack([np.ones(len(dop)), infection, dop, infection * dop, context, context])
    names = ["Intercept", "infection", "dop", "infection:dop", "year_2022", "box_B"]
    reduced, kept, audit = _resolve_behavioural_design(x, names, ["infection", "dop", "infection:dop"])
    assert audit["protected_primary_estimable"] and audit["full_column_space_preserved"]
    assert audit["dropped_nuisance_terms"] == ["box_B"]
    assert audit["nuisance_aliases"]["box_B"] == pytest.approx({"year_2022": 1.})
    transform = np.asarray(audit["original_column_transform"])
    np.testing.assert_allclose(reduced @ transform, x, atol=1e-12)
    gamma = rng.normal(size=len(kept))
    original_minimum_norm_beta = transform.T @ np.linalg.solve(transform @ transform.T, gamma)
    np.testing.assert_allclose(x @ original_minimum_norm_beta, reduced @ gamma, atol=1e-12)
    actual_penalty = .5 * gamma @ np.asarray(audit["prior_precision"]) @ gamma
    assert actual_penalty == pytest.approx(.5 * np.sum(original_minimum_norm_beta ** 2) / 2.25)
    year = kept.index("year_2022")
    assert audit["prior_covariance_unit"][year][year] == pytest.approx(2.)


def test_protected_primary_alias_cannot_be_identified_by_dropping_context():
    from horsehair.behaviour import _resolve_behavioural_design, NonestimableDesignError
    infection = np.repeat([0., 1.], 20)
    dop = np.tile(np.linspace(.1, .9, 20), 2)
    x = np.column_stack([np.ones(40), infection, dop, infection * dop, infection])
    with pytest.raises(NonestimableDesignError) as raised:
        _resolve_behavioural_design(x, ["Intercept", "infection", "dop", "infection:dop", "year_2022"],
                                   ["infection", "dop", "infection:dop"])
    assert raised.value.diagnostics["protected_primary_estimable"] is False
    assert raised.value.diagnostics["protected_nullspace_norms"]["infection"] > .5


def test_three_year_context_alias_pushes_original_box_prior_into_context_covariance():
    from horsehair.behaviour import _resolve_behavioural_design
    rng = np.random.default_rng(418)
    year = np.tile([2021, 2022, 2023], 40)
    infection, dop = rng.integers(0, 2, len(year)), rng.normal(size=len(year))
    x = np.column_stack([np.ones(len(year)), infection, dop, infection * dop,
                         year == 2022, year == 2023, year == 2021])
    names = ["Intercept", "infection", "dop", "infection:dop", "year_2022", "year_2023", "box_D"]
    reduced, kept, audit = _resolve_behavioural_design(x, names, ["infection", "dop", "infection:dop"])
    assert audit["dropped_nuisance_terms"] == ["box_D"]
    assert audit["nuisance_aliases"]["box_D"] == pytest.approx({"Intercept": 1., "year_2022": -1., "year_2023": -1.})
    covariance = np.asarray(audit["prior_covariance_unit"])
    assert covariance[kept.index("Intercept"), kept.index("year_2022")] == pytest.approx(-1.)
    assert covariance[kept.index("year_2022"), kept.index("year_2023")] == pytest.approx(1.)
    assert reduced.shape[1] == 6


def test_nested_year_box_nuisance_fit_and_heldout_design_keep_primary_terms():
    from horsehair.behaviour import _mantis_design_contract
    rng = np.random.default_rng(177)
    n = 180
    frame = pd.DataFrame({"infection": rng.integers(0, 2, n), "dop": rng.uniform(.1, .9, n),
        "illuminance": 10., "stimulus_side": rng.choice(["R", "L"], n), "box": rng.choice(["A", "B"], n),
        "response": rng.choice(["middle", "polarized", "unpolarized"], n), "individual_id": np.arange(n).astype(str)})
    frame["year"] = np.where(frame.box.eq("B"), "2022", "2021")
    fitted = fit_mantis_choice(frame, {"dop_units": "source"})
    audit = fitted["design_audit"]["engagement"]
    assert audit["dropped_nuisance_terms"] == ["box_B"]
    assert all(name in fitted["engagement"]["terms"] for name in ["infection", "dop", "infection:dop"])
    training = frame.copy()
    training["log_illuminance"] = np.log(training.illuminance)
    matrix, terms, contract = _mantis_design_contract(training, training.iloc[:3], audit)
    assert terms == fitted["engagement"]["terms"] and matrix.shape[1] == len(terms)
    assert contract["training_nuisance_basis"]["full_column_space_preserved"]
    changed_context = training.iloc[:1].copy()
    changed_context["year"] = np.where(changed_context.box.eq("A"), "2022", "2021")
    with pytest.raises(ValueError, match="outside identifiable training contexts"):
        _mantis_design_contract(training, changed_context, audit)


def test_preference_nuisance_aliases_cannot_be_assumed_at_new_heldout_contexts():
    from horsehair.behaviour import _resolve_behavioural_design, _apply_training_design
    rng = np.random.default_rng(1229)
    infection = np.tile([0., 1.], 20)
    dop = rng.normal(size=40)
    chooser_matrix = np.column_stack([np.ones(40), infection, dop, infection * dop, np.zeros(40)])
    names = ["Intercept", "infection", "dop", "infection:dop", "year_2022"]
    _, _, audit = _resolve_behavioural_design(chooser_matrix, names, ["infection", "dop", "infection:dop"])
    assert audit["dropped_nuisance_terms"] == ["year_2022"]
    _apply_training_design(chooser_matrix[:2], names, audit)
    test = chooser_matrix[:1].copy()
    test[0, -1] = 1
    with pytest.raises(ValueError, match="outside identifiable training contexts"):
        _apply_training_design(test, names, audit)


def test_primary_or_chooser_only_nonidentifiability_completes_explicit_pending_descriptives():
    from horsehair.behaviour import analyze_mantis_choice
    rng = np.random.default_rng(889)
    n = 90
    frame = pd.DataFrame({"infection": np.tile([0, 1], n // 2), "dop": rng.uniform(.1, .9, n),
        "illuminance": 10., "stimulus_side": "R", "year": "2021", "box": "A", "date": "2021-01-01",
        "response": rng.choice(["middle", "polarized", "unpolarized"], n), "individual_id": np.arange(n).astype(str)})
    frame.loc[frame.infection.eq(0), "response"] = "middle"
    result = analyze_mantis_choice(frame, {"dop_units": "source"})
    assert result["choice"]["status"] == "pending_nonestimable_design"
    assert result["choice"]["numeric_claim_available"] is False
    assert result["choice"]["confirmatory_ready"] is False
    assert sum(row["trials"] for row in result["choice"]["source_descriptive_counts"]) == n
    assert "coefficient" not in result["choice"] and "engagement" not in result["choice"]
    assert result["choice"]["design_audit"]["protected_nullspace_norms"]["infection"] > .5
    frame["infection"] = 0
    assert analyze_mantis_choice(frame, {"dop_units": "source"})["choice"]["status"] == "pending_nonestimable_design"


def test_absent_workload_inputs_stay_pending(tmp_path):
    from horsehair.behaviour import run
    result = run({"behaviour": {}}, tmp_path, "benchmark-workload")
    assert result["complete_declared_workload"] is False
    assert result["pending"]
    assert (tmp_path / "state/behaviour-workload-benchmark.json").exists()


def test_duplicate_representations_do_not_increase_animals(tmp_path):
    frame, _ = source_fixture(tmp_path)
    duplicate = pd.concat([frame, frame], ignore_index=True)
    with pytest.raises(ValueError, match="Duplicate"):
        validate_observations(duplicate)
    clean, report = validate_observations(duplicate, True)
    assert len(clean) == len(frame)
    assert len(report) == 2 * len(frame)
    duplicate.loc[len(frame), "y"] = 99
    with pytest.raises(ValueError, match="Conflicting"):
        validate_observations(duplicate, True)


def test_short_gap_availability_and_long_gap_preserved():
    times = np.arange(8, dtype=float)
    positions = np.arange(8, dtype=float)[:, None]
    valid = np.ones(8, dtype=bool)
    valid[2] = False
    valid[4:7] = False
    positions[~valid] = np.nan
    output, available = interpolate_short_gaps(times, positions, valid)
    assert output[2, 0] == 2
    assert available[2] == 3
    assert np.isnan(output[4:7]).all()


def test_chronological_bins_and_future_predictor_rejected(tmp_path):
    frame, _ = source_fixture(tmp_path)
    bins = bin_trajectories(frame, 10)
    sequences = to_sequences(bins)
    assert len(sequences) == 2
    assert all(np.diff(s.times).min() == 10 for s in sequences)
    assert all(not s.observed[0] for s in sequences)
    for sequence in sequences:
        np.testing.assert_allclose(sequence.observations[sequence.observed, 1], 10 / 120, atol=1e-8)
    with pytest.raises(ValueError, match="Future-information"):
        to_sequences(bins, {"covariates": ["annotation_last_mvmt"]})


def test_censoring_uses_survival_density():
    x = np.ones((1, 1))
    theta = np.zeros(2)
    event = aft_objective(theta, x, np.array([1.]), np.array([False]))
    censor = aft_objective(theta, x, np.array([1.]), np.array([True]))
    assert event == pytest.approx(.5 * np.log(2 * np.pi))
    assert censor == pytest.approx(np.log(2))


def test_censored_mating_times_do_not_become_zero():
    rng = np.random.default_rng(4)
    n = 80
    times = rng.lognormal(1, .3, n)
    censored = rng.random(n) < .3
    frame = pd.DataFrame({"male_escape_time": np.where(censored, 0, times),
        "male_escape_censored": censored.astype(int), "observation_duration": np.maximum(times + .1, 5),
        "male_infection": rng.integers(0, 2, n), "female_infection": rng.integers(0, 2, n),
        "male_size": rng.normal(4, .5, n), "female_size": rng.normal(5, .5, n),
        "year": "2024", "pair_id": np.arange(n)})
    result = fit_censored_latency(frame, "male_escape")
    assert result["n_censored"] == censored.sum()
    assert result["censor_convention"] == "1=right-censored"
    frame.loc[~censored, "male_escape_time"] = 100
    with pytest.raises(ValueError, match="observation window"):
        fit_censored_latency(frame, "male_escape")


def test_three_category_choice_keeps_nonchoice():
    rng = np.random.default_rng(41)
    n = 180
    frame = pd.DataFrame({"response": rng.choice(["middle", "polarized", "unpolarized"], n),
        "infection": rng.integers(0, 2, n), "dop": rng.uniform(.1, .9, n),
        "illuminance": rng.uniform(10, 20, n), "stimulus_side": rng.choice(["L", "R"], n),
        "year": "2024", "box": "A", "individual_id": np.arange(n)})
    result = fit_mantis_choice(frame, {"dop_units": "proportion"})
    assert result["denominators"]["nonchoices"] == frame.response.eq("middle").sum()
    assert result["engagement"]["n"] == n
    assert result["conditional_preference"]["n"] < n


def test_shared_control_reuse_produces_covariance():
    rng = np.random.default_rng(43)
    animals = pd.DataFrame({"animal_id": np.arange(90).astype(str), "experiment_id": "e1",
        "condition_id": np.repeat(["control", "t1", "t2"], 30),
        "metric": rng.normal(0, 1, 90) + np.repeat([0., .7, .8], 30)})
    contrasts = pd.DataFrame({"contrast_id": ["c1", "c2"], "treatment_condition": ["t1", "t2"],
                              "control_condition": ["control", "control"]})
    effects, covariance = shared_control_effects(animals, contrasts, "metric", repetitions=400, seed=9)
    assert covariance[0, 1] > 0
    assert np.isfinite(effects.effect_se).all()
    assert calibration_half("same-control") == calibration_half("same-control")
    animals.loc[1, "animal_id"] = animals.loc[0, "animal_id"]
    with pytest.raises(ValueError, match="once"):
        shared_control_effects(animals, contrasts, "metric", repetitions=100)


def test_duration_right_censoring_integrates_survival():
    assert restricted_mean_duration([10, 20], [False, True], 30) == pytest.approx(20)


def test_neural_history_excludes_target_and_future():
    import copy
    from horsehair.semimarkov import Sequence
    n = 80
    sequence = Sequence("a", "e", np.column_stack([np.arange(n), np.arange(n)]),
        np.zeros((n, 1)), np.zeros((n, 1)), np.zeros((n, 2)), np.zeros((n, 2)),
        np.zeros((n, 2)), np.ones(n, dtype=bool), np.arange(n) * 10.)
    dataset = _history_dataset([sequence], np.ones(2))
    history, context, target = dataset[60]
    assert history.shape == (60, 3)
    assert history[-1, 0] == 59
    assert target[0] == 60
    modified = copy.deepcopy(sequence)
    modified.observations[60:] = 10000
    changed = _history_dataset([modified], np.ones(2))[60]
    np.testing.assert_equal(history.numpy(), changed[0].numpy())
    np.testing.assert_equal(context.numpy(), changed[1].numpy())


def test_small_gated_comparator_trains_with_grouped_validation(tmp_path):
    from horsehair.semimarkov import MovementModel, Sequence, simulate
    n = 40
    model = MovementModel("MG", 1, 1)
    def make(animal, experiment):
        sequence = Sequence(animal, experiment, np.zeros((n, 2)), np.zeros((n, 1)),
            np.linspace(-1, 1, n)[:, None], np.zeros((n, 2)), np.zeros((n, 2)),
            np.tile([0., 1.], (n, 1)), np.ones(n, dtype=bool), np.arange(n) * 10.)
        return simulate(model, sequence, len(animal) + len(experiment))[0]
    train, test = make("train", "one"), make("heldout", "two")
    fit = fit_comparator([train], "GRU", {"starts": 1, "max_epochs": 2,
        "hidden_size": 16, "warmup_seconds": 0, "lbfgs_iterations": 0}, [test])
    assert fit.diagnostics["parameter_count"] <= 100000
    assert np.isfinite(fit.score(test, 0)["log_score"])
    fit.save(tmp_path / "comparator.pt")
    from horsehair.behaviour import ComparatorFit
    restored = ComparatorFit.load(tmp_path / "comparator.pt")
    assert restored.score(test, 0)["log_score"] == pytest.approx(fit.score(test, 0)["log_score"])
    with pytest.raises(ValueError, match="Animal leakage"):
        fit_comparator([train], "GRU", {"max_epochs": 1}, [train])


def test_primary_candidate_uses_inner_scores_even_when_outer_winner_changes():
    from horsehair.behaviour import select_primary_from_inner
    inner = pd.DataFrame({"outer_experiment": ["e1", "e1", "e2", "e2"],
        "model": ["MT", "MX", "MT", "MX"], "inner_log_score": [2., 1., 1., 2.]})
    scores = pd.DataFrame({"experiment_id": ["e1", "e1", "e2", "e2"],
        "animal_id": ["a1", "a1", "a2", "a2"], "model": ["MT", "MX", "MT", "MX"],
        "log_score": [-100., 100., 100., -100.]})
    _, choices = select_primary_from_inner(scores, inner)
    assert choices == {"e1": "MT", "e2": "MX"}
    scores.log_score *= -1
    _, altered = select_primary_from_inner(scores, inner)
    assert altered == choices


def test_native_polarization_adapter_keeps_nonchoices_and_repeated_animals():
    from horsehair.behaviour import adapt_polarization_frame
    table = pd.DataFrame({"no": [1, 2, 1], "date": ["2021-01-01"] * 3, "year": [2021] * 3,
        "id": ["a", "a", "b"], "inf": [0, 0, 1], "degree": [.5, .5, .9],
        "result": ["hori-pola", "M", "unpola"], "result_hori": [1, np.nan, 0],
        "location": ["R", "M", "R"], "pola_window": ["R", "R", "L"],
        "light intensity": [10, 10, 12], "box": ["A"] * 3})
    adapted, audit = adapt_polarization_frame(table)
    assert adapted.response.tolist() == ["polarized", "middle", "unpolarized"]
    assert audit["individuals"] == 2 and audit["repeated_trials"] == 1
    bad = table.copy()
    bad.loc[0, "location"] = "L"
    ambiguous, audit = adapt_polarization_frame(bad)
    assert len(ambiguous) == len(table)
    assert ambiguous.source_location.iloc[0] == "L"
    assert ambiguous.response.iloc[0] == "polarized"
    assert not ambiguous.analysis_included.iloc[0]
    assert audit["ambiguous_trials"] == 1 and audit["source_coding_reconciled"] is False


def polarization_coding_fixture():
    """Generated rows matching the published aggregate coding combinations.

    IDs and covariates are synthetic unit fixtures, not deposited observations.
    No regression result from this fixture is study evidence.
    """
    combinations = [("M", "L", "M", 0, 49), ("M", "R", "M", 0, 47),
        ("hori-pola", "L", "L", 1, 30), ("hori-pola", "R", "R", 1, 29),
        ("unpola", "R", "L", 0, 23), ("unpola", "L", "R", 0, 22),
        ("hori-pola", "L", "R", 1, 2), ("hori-pola", "R", "L", 1, 1),
        ("unpola", "R", "R", 0, 1), ("M", "R", "M", 1, 1),
        ("unpola", "L", "L", 0, 1)]
    rows = [{"result": result, "pola_window": side, "location": location, "result_hori": binary}
            for result, side, location, binary, count in combinations for _ in range(count)]
    table = pd.DataFrame(rows)
    rng = np.random.default_rng(413)
    table["id"] = [f"generated-individual-{i}" for i in range(len(table))]
    table["no"] = np.arange(len(table)) + 1
    table["year"], table["date"] = 2021, "2021-01-01"
    table["box"], table["light intensity"] = "A", 10.
    table["inf"] = rng.integers(0, 2, len(table))
    table["degree"] = rng.uniform(.2, .9, len(table))
    return table


def test_deposited_coding_conflicts_are_preserved_and_counted_without_raw_correction():
    import json
    from horsehair.behaviour import adapt_polarization_frame
    raw = polarization_coding_fixture()
    original = raw.copy(deep=True)
    adapted, audit = adapt_polarization_frame(raw)
    pd.testing.assert_frame_equal(raw, original)
    assert len(adapted) == 206
    assert audit["mutually_consistent_trials"] == 200 and audit["ambiguous_trials"] == 6
    assert adapted.coding_ambiguity_reason.str.contains("result/location/window").sum() == 5
    assert adapted.coding_ambiguity_reason.str.contains("result/result_hori").sum() == 1
    np.testing.assert_array_equal(adapted.source_response, raw.result)
    np.testing.assert_array_equal(adapted.source_location, raw.location)
    np.testing.assert_array_equal(adapted.source_result_hori, raw.result_hori)
    nonchoice = adapted[adapted.source_response.eq("M") & adapted.source_result_hori.eq(1)].iloc[0]
    assert nonchoice.response == "middle" and nonchoice.response_location_window == "middle"
    assert not nonchoice.analysis_included  # binary conflict is not a hidden choice
    assert json.loads(json.dumps(audit, allow_nan=False))["source_coding_reconciled"] is False
    raw.loc[0, "result"] = "unknown endpoint"
    with pytest.raises(ValueError, match="Unrecognized"):
        adapt_polarization_frame(raw)


def test_mantis_conditional_and_alternative_fits_do_not_reconcile_source_conflicts():
    from horsehair.behaviour import adapt_polarization_frame, analyze_mantis_choice
    table, _ = adapt_polarization_frame(polarization_coding_fixture())
    original = table.copy(deep=True)
    result = analyze_mantis_choice(table, {"dop_units": "source"})
    pd.testing.assert_frame_equal(table, original)
    assert result["choice"]["denominators"]["trials"] == 200
    assert result["choice"]["confirmatory_ready"] is False
    assert result["choice_validation"]["status"] == "pending_source_reconciliation"
    sensitivity = result["choice_sensitivity"]
    assert sensitivity["status"] == "complete_diagnostic_source_ambiguity_pending"
    assert sensitivity["confirmatory_ready"] is False
    assert set(sensitivity["variants"]) == {"recorded_result_all_rows", "location_window_all_rows"}
    for variant in sensitivity["variants"].values():
        assert variant["trials"] == 206 and variant["fit"]["denominators"]["trials"] == 206
    for variants in sensitivity["source_unit_interaction_summaries"].values():
        for endpoint in variants.values():
            assert np.isfinite(endpoint["infection_by_dop_source_unit_slope"])
            assert np.isfinite(endpoint["ci95_laplace"]).all()


def test_native_mating_adapter_keeps_unknown_independence_explicit():
    from horsehair.behaviour import adapt_mating_frame
    frame = pd.DataFrame({"year": [2021, 2021], "m_size": [4., 4.2], "f_size": [5., 5.1],
        "m_inf": [0, 1], "f_inf": [1, 0], "obs_time": [10., 10.], "m_esc": [1, 0],
        "lat_m_esc": [2., 0.], "lat_m_esc_cens": [0, 1], "mount": [0, 1],
        "lat_mount": [0., 3.], "lat_mount_cens": [1, 0]})
    adapted, audit = adapt_mating_frame(frame, "source-hash")
    assert adapted.pair_id.nunique() == 2
    assert audit["independence_verified"] is False
    assert adapted.male_escape_censored.tolist() == [0, 1]
    frame.loc[0, "lat_m_esc_cens"] = 1
    with pytest.raises(ValueError, match="contradicts"):
        adapt_mating_frame(frame, "source-hash")


def test_gene_aggregation_retains_cross_gene_shared_control_covariance():
    from horsehair.interventions import aggregate_gene_effects
    effects = pd.DataFrame({"gene_id": ["g1", "g1", "g2"], "effect": [1., 1.1, .5],
        "contrast_id": ["r1", "r2", "r3"], "conditional_on_source_inclusion": [True] * 3})
    covariance = np.array([[.2, .1, .08], [.1, .2, .08], [.08, .08, .3]])
    genes = aggregate_gene_effects(effects, covariance)
    projected = genes.attrs["shared_control_covariance"]
    assert projected.shape == (2, 2) and projected[0, 1] > 0
    np.testing.assert_allclose(np.diag(projected), genes.effect_se.to_numpy() ** 2)


def test_target_export_preserves_distinct_transfer_hogs_and_original_families():
    from horsehair.interventions import attach_frozen_gene_families
    genes = pd.DataFrame({"gene_id": ["g2", "g1"], "effect": [.5, 1.], "effect_se": [.1, .2]})
    source = pd.DataFrame({"gene_id": ["g1", "g2"], "orthogroup": ["HOG001a", "HOG001b"],
                           "family_id": ["OG001", "OG001"]})
    attached = attach_frozen_gene_families(genes, source)
    assert attached.gene_id.tolist() == ["g2", "g1"]  # covariance order remains fixed
    assert attached.orthogroup.tolist() == ["HOG001b", "HOG001a"]
    assert attached.family_id.tolist() == ["OG001", "OG001"]
    with pytest.raises(ValueError, match="older HOG-only"):
        attach_frozen_gene_families(genes, source.drop(columns="family_id"))
    wrong = genes.assign(family_id="HOG001a")
    with pytest.raises(ValueError, match="disagrees"):
        attach_frozen_gene_families(wrong, source)


def test_streaming_native_ingestion_and_cached_bins(tmp_path):
    from horsehair.behaviour import run
    _, source = source_fixture(tmp_path)
    source["analysis_role"] = "behavioural_development"
    result = run({"behaviour": {"fly_sources": [source], "sensitivity_bins_seconds": [5, 20]}}, tmp_path, "ingest")
    assert result["animals_by_experiment"] == {"source1": 2}
    native = pd.read_parquet(tmp_path / "data" / "fly_observations.parquet")
    assert native.animal_id.nunique() == 2
    for value in (5, 10, 20):
        bins = pd.read_parquet(tmp_path / "results" / "behaviour" / f"fly_bins_{value}s.parquet")
        assert bins.animal_id.nunique() == 2
    assert (tmp_path / "results" / "behaviour" / "streaming_bin_provenance.json").is_file()


def test_h2_bootstrap_resamples_shared_controls_and_checks_saved_split_identities():
    import json
    from horsehair.interventions import h2_shared_control_bootstrap
    rng = np.random.default_rng(123)
    rows = []
    for condition, effect in (("control", 0.), ("t1", .6), ("t2", .7)):
        for half in (0, 1):
            for animal in range(10):
                value = float(effect + rng.normal(0, .1))
                rows.append({"animal_id": f"{condition}-{half}-{animal}", "experiment_id": "e1",
                    "condition_id": condition, "half": half, "integrated_activity": value,
                    "occupancy": value})
    animals = pd.DataFrame(rows)
    predictions = []
    for i, condition in enumerate(("t1", "t2")):
        for half in (0, 1):
            select = lambda c, h: animals[(animals.condition_id == c) & (animals.half == h)]
            activity = select(condition, half).integrated_activity.mean() - select("control", half).integrated_activity.mean()
            observed = select(condition, 1 - half).occupancy.mean() - select("control", 1 - half).occupancy.mean()
            for model in ("MT", "MG"):
                predictions.append({"contrast_id": f"contrast{i}", "model": model, "component": "occupancy",
                    "calibration_half": half, "treatment_condition": condition, "control_condition": "control",
                    "family": f"family{i}", "observed": observed,
                    "predicted": observed * .8 if model == "MT" else 0.,
                    "calibration_activity_derivative": 1., "prediction_component_derivative": 0.,
                    "integrated_activity_difference": activity, "amplitude_at_bound": False,
                    "derivative_step": 100., "derivative_relative_stability_error": 0.,
                    "calibration_treatment": json.dumps(select(condition, half).animal_id.tolist()),
                    "calibration_controls": json.dumps(select("control", half).animal_id.tolist()),
                    "evaluation_treatment": json.dumps(select(condition, 1 - half).animal_id.tolist()),
                    "evaluation_controls": json.dumps(select("control", 1 - half).animal_id.tolist())})
    predictions = pd.DataFrame(predictions)
    result = h2_shared_control_bootstrap(predictions, animals, "MT", "MG", {"occupancy": 1.}, repetitions=100, seed=4)
    assert result["estimate"] > .8
    assert np.array(result["shared_control_error_covariance"])[0, 1] > 0
    assert result["calibration_response_validated"] is True
    mismatch = predictions.copy()
    mismatch.loc[0, "evaluation_controls"] = mismatch.loc[0, "calibration_controls"]
    with pytest.raises(ValueError, match="different biological contrasts"):
        h2_shared_control_bootstrap(mismatch, animals, "MT", "MG", {"occupancy": 1.}, repetitions=20)
    selected = predictions.contrast_id.eq("contrast0") & predictions.calibration_half.eq(0)
    predictions.loc[selected, "evaluation_controls"] = predictions.loc[selected, "calibration_controls"]
    with pytest.raises(ValueError, match="overlap"):
        h2_shared_control_bootstrap(predictions, animals, "MT", "MG", {"occupancy": 1.}, repetitions=20)
