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
    (tmp_path / "triggerOnDeath2019Fun.m").write_text("data.params.fr = 30;\n")
    assert not discover_fly_sources(tmp_path, tmp_path)["fly_sources"]


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
    with pytest.raises(ValueError, match="contradict"):
        adapt_polarization_frame(bad)


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
