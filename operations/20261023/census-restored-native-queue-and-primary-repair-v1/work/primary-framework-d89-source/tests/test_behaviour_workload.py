import copy
from pathlib import Path

import numpy as np
import pytest

from horsehair.behaviour_workload import frozen_work_counts, measured_scope_diagnostic, native_batch_probe
from horsehair.core import canonical_hash


def fixture():
    config = {"behaviour": {"starts": 2, "final_starts": 3, "max_epochs": 5,
        "lbfgs_iterations": 2, "learning_rates": [.001, .003], "inner_folds": 2,
        "models": ["MG", "MT", "MD", "MX"], "comparators": ["AR", "HMM", "GRU"],
        "neural_hidden_sizes": [16, 32], "neural_dropout": [0, .1],
        "parameter_recovery_replicates": 2, "recovery_effects": [0, .5]},
        "resources": {"cpu_core_hours_cap": 20_000}}
    rosters = {f"{width}s": [{"animal_id": f"a{i}", "experiment_id": f"e{i//2}",
        "analysis_role": "behavioural_development", "bins": 30 * 10 // width} for i in range(6)]
        for width in [5, 10, 20]}
    return config, rosters


def test_full_structural_grid_and_recovery_budgets_count_without_new_subjects():
    config, rosters = fixture()
    result = frozen_work_counts(config, rosters)
    assert len(result["counts"]) == 3 * (4 + 2 + 4)
    assert {row["hidden_size"] for row in result["counts"] if row["model"] == "GRU"} == {16, 32}
    row = next(row for row in result["counts"] if row["model"] == "MG" and row["bin_seconds"] == 10)
    # Three outer folds; inner folds each fit2 traininganimals =>12. Two rates,
    # two starts; outer training12animals times5 starts; freeze6times5.
    # Recovery4models*2effects*2replicates*6animals*(2folds-1)*2starts.
    assert row["training_animal_passes_per_epoch"] == 12 * 2 * 2 + 12 * 5 + 6 * 5 + 4 * 2 * 2 * 6 * 2
    assert row["adam_animal_passes_upper"] == row["training_animal_passes_per_epoch"] * 5
    assert row["lbfgs_animal_closures_upper"] == row["training_animal_passes_per_epoch"] * 53
    assert row["recovery_full_parameter_curvature_animal_units"] == 2 * 2 * 6
    assert row["inner_post_fit_scoring_animal_passes"] == 12 * 2
    assert row["recovery_held_out_scoring_animal_passes"] == 4 * 2 * 2 * 6
    assert row["held_out_scoring_animal_passes_upper"] == 12 * 2 * 2 * 5 + 12 * 2 + 6 + 4 * 2 * 2 * 6
    assert row["inner_refinement_scoring_animal_passes"] == 0
    for neural in [row for row in result["counts"] if row["model"] in {"AR", "GRU"}]:
        assert neural["inner_refinement_scoring_animal_passes"] == 12 * 2 * 2
        assert neural["held_out_scoring_animal_passes_upper"] == 12 * 2 * 2 * 5 + 12 * 2 + 12 * 2 * 2 + 6
    without_refinement = frozen_work_counts(dict(config, behaviour=dict(config["behaviour"], lbfgs_iterations=0)), rosters)
    assert all(row["inner_refinement_scoring_animal_passes"] == 0 for row in without_refinement["counts"])
    assert row["native_animals"] == 6


@pytest.mark.parametrize("kind", ["M0", "MG", "MT", "MD", "MX"])
def test_probe_explicit_declared_model_preserves_native_identity_and_group_dimensions(monkeypatch, kind):
    from horsehair import behaviour_workload as workload
    from horsehair.semimarkov import Sequence
    rng = np.random.default_rng(88)
    units = [Sequence(f"a{i}", f"e{i}", rng.normal(size=(16, 2)), rng.normal(size=(16, 2)),
                rng.normal(size=(16, 1)), rng.normal(size=(16, 2)), rng.normal(size=(16, 2)),
                rng.normal(size=(16, 2)), np.ones(16, bool), np.arange(16) * 10.) for i in range(2)]
    roster = [{"animal_id": f"a{i}", "experiment_id": f"e{i}", "bins": 16,
               "analysis_role": "behavioural_development"} for i in range(3)]
    monkeypatch.setattr(workload, "native_job_id", lambda: "123_4")
    monkeypatch.setattr(workload, "_native_sequences", lambda *args: (units, roster, {"source_result_paths": ["native-fixture"], "source_result_hashes": {"native-fixture": "fixture-sha"}}))
    monkeypatch.setattr(workload, "source_identity", lambda *args: {"sha256": "qualified-source"})
    monkeypatch.setenv("SLURM_CPUS_PER_TASK", "1")
    result = native_batch_probe({"behaviour": {"models": [kind]}}, Path("unread-native-fixture"), 10., 2, kind)
    assert result["model"] == kind
    assert result["native_animal_ids"] == ["a0", "a1"]
    assert result["full_roster_animals"] == 3
    assert result["full_roster_experiments"] == 3
    assert result["slurm_job_id"] == "123_4"
    assert result["source_sha256"] == "qualified-source"
    assert result["maximum_absolute_parameter_gradient_error"] < 1e-8
    assert result["production_fit_kernel_changed"] is False
    assert result["complete_declared_workload"] is False


def test_probe_rejects_unknown_or_undeclared_model_before_input_reads(monkeypatch):
    from horsehair import behaviour_workload as workload
    monkeypatch.setattr(workload, "native_job_id", lambda: "123")
    monkeypatch.setattr(workload, "_native_sequences", lambda *args: pytest.fail("Undeclared model must not read outcomes"))
    for kind in ["HMM", "M0"]:
        with pytest.raises(ValueError, match="declared interpretable"):
            native_batch_probe({"behaviour": {"models": ["MX"]}}, Path("unread-native-fixture"), 10., 2, kind)


def test_scope_forecast_keeps_historical_identity_and_incomplete_families():
    config, rosters = fixture()
    report = {"full_config_sha256": canonical_hash(config), "source_sha256": "historical-source",
        "slurm_job_id": "123", "prospective_workload": {"clock_rosters": rosters},
        "tasks": [{"operation": "likelihood_gradient", "model": "MG", "bin_seconds": 10,
                   "elapsed_seconds": 3600., "allocated_cpus": 8, "slurm_job_id": "123", "source_sha256": "historical-source"}]}
    result = measured_scope_diagnostic(config, report)
    assert result["complete_declared_workload"] is False
    assert result["admission_authority"] is False
    assert result["kernel_source_sha256"] == "historical-source"
    assert result["partial_adam_only_forecast_cpu_core_hours"] > 0
    assert result["pending"]
    report["full_config_sha256"] = "altered"
    with pytest.raises(ValueError, match="frozen full configuration"):
        measured_scope_diagnostic(config, report)


@pytest.mark.parametrize("mutation", ["duplicate", "missing-width", "membership", "role", "folds"])
def test_scope_rejects_unfrozen_or_duplicate_designs(mutation):
    config, rosters = fixture()
    rosters = copy.deepcopy(rosters)
    if mutation == "duplicate":
        rosters["10s"].append(rosters["10s"][0])
    elif mutation == "missing-width":
        del rosters["5s"]
    elif mutation == "membership":
        rosters["5s"][0]["animal_id"] = "changed"
    elif mutation == "role":
        rosters["5s"][0]["analysis_role"] = "targeted_intervention"
    else:
        config["behaviour"]["inner_folds"] = 1
    with pytest.raises(ValueError):
        frozen_work_counts(config, rosters)
