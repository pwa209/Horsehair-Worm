"""Numerical refusal and runtime evidence for full-clock technical probes."""
import copy
import json
from types import SimpleNamespace

import numpy as np
import pytest

from horsehair import behaviour_feasibility as feasibility

torch = pytest.importorskip("torch")


def model_with_gradients():
    model = torch.nn.Linear(2, 1)
    model(torch.tensor([[.2, -.1]])).square().sum().backward()
    return model


def sequence_fixture():
    return SimpleNamespace(observations=np.array([[1., 2.], [np.nan, np.nan], [3., 4.], [5., 6.]]),
        observed=np.array([True, False, True, True]), times=np.array([0., 10., 20., 30.]))


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf")])
@pytest.mark.parametrize("field", ["parameter", "gradient"])
def test_nonfinite_parameters_and_gradients_are_refused_without_repairing_them(bad, field):
    model = model_with_gradients()
    tensor = model.weight if field == "parameter" else model.weight.grad
    with torch.no_grad():
        tensor[0, 0] = bad
    snapshot = tensor.detach().clone()
    with pytest.raises(FloatingPointError, match=field):
        feasibility._validate_model_finite(model, gradients=True)
    torch.testing.assert_close(tensor, snapshot, equal_nan=True, rtol=0, atol=0)


def test_validation_keeps_finite_parameters_gradients_and_unused_parameters_unchanged():
    model = model_with_gradients()
    model.register_parameter("unused", torch.nn.Parameter(torch.tensor(3.)))
    snapshots = [(p.detach().clone(), p.grad.detach().clone() if p.grad is not None else None)
                 for p in model.parameters()]
    evidence = feasibility._validate_model_finite(model, gradients=True)
    assert evidence["parameters_finite"] is True
    assert evidence["finite_gradient_tensors"] == 2
    assert evidence["trainable_parameters_without_gradient"] == ["unused"]
    for parameter, (value, gradient) in zip(model.parameters(), snapshots):
        torch.testing.assert_close(parameter, value, rtol=0, atol=0)
        if gradient is None:
            assert parameter.grad is None
        else:
            torch.testing.assert_close(parameter.grad, gradient, rtol=0, atol=0)


def test_missing_all_gradients_cannot_receive_a_finite_gradient_attestation():
    with pytest.raises(FloatingPointError, match="No behavioural objective gradients"):
        feasibility._validate_model_finite(torch.nn.Linear(2, 1), gradients=True)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_objective_is_refused(bad):
    with pytest.raises(FloatingPointError, match="objective"):
        feasibility._validate_objective_finite(torch.tensor(bad))


@pytest.mark.parametrize("bad", [torch.tensor([1., 2.]), torch.tensor(True), None, "1.0"])
def test_non_scalar_or_non_numeric_objective_is_refused(bad):
    with pytest.raises(ValueError, match="scalar"):
        feasibility._validate_objective_finite(bad)


def test_finite_objective_retains_tensor_value_dtype_and_autograd_graph():
    objective = torch.tensor(1.25, dtype=torch.float64, requires_grad=True).square()
    previous_gradient_function = objective.grad_fn
    evidence = feasibility._validate_objective_finite(objective)
    assert evidence == {"objective_finite": True, "objective_value": 1.5625}
    assert objective.dtype == torch.float64
    assert objective.grad_fn is previous_gradient_function


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_held_out_score_is_refused(bad):
    with pytest.raises(FloatingPointError, match="objective"):
        feasibility._validate_score_finite({"n_scored": 2, "log_score": bad}, sequence_fixture(), 20)


def test_score_checks_observed_warmup_window_and_optional_diagnostics():
    sequence = sequence_fixture()
    evidence = feasibility._validate_score_finite({"n_scored": 2, "log_score": -.5,
        "marginal_90pct_coverage": [.8, .9]}, sequence, 20)
    assert evidence["observed_window_preserved"] is True
    assert evidence["expected_n_scored"] == 2
    with pytest.raises(ValueError, match="prediction window"):
        feasibility._validate_score_finite({"n_scored": 3, "log_score": -.5}, sequence, 20)
    with pytest.raises(FloatingPointError, match="coverage"):
        feasibility._validate_score_finite({"n_scored": 2, "log_score": -.5,
            "marginal_90pct_coverage": [.8, np.nan]}, sequence, 20)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_observed_simulation_values_are_refused(bad):
    source = sequence_fixture()
    simulated = copy.deepcopy(source)
    simulated.observations[2, 1] = bad
    with pytest.raises(FloatingPointError, match="simulated displacement"):
        feasibility._validate_simulation_finite(simulated, source)


def test_missing_simulation_values_are_masked_and_original_sequence_is_not_changed():
    source = sequence_fixture()
    simulated = copy.deepcopy(source)
    simulated.observations[source.observed] *= 2
    before = simulated.observations.copy()
    for value in (simulated, (simulated, np.array([0, 1, 0, 1]))):
        evidence = feasibility._validate_simulation_finite(value, source)
        assert evidence["observed_rows"] == 3
        assert evidence["masked_rows"] == 1
        assert evidence["clock_and_mask_preserved"] is True
    np.testing.assert_array_equal(simulated.observations, before)
    assert np.isnan(simulated.observations[1]).all()


@pytest.mark.parametrize("change", ["mask", "clock", "dimensions"])
def test_simulation_cannot_change_the_original_mask_clock_or_dimensions(change):
    source = sequence_fixture()
    simulated = copy.deepcopy(source)
    if change == "mask":
        simulated.observed[1] = True
    elif change == "clock":
        simulated.times[2] += 1
    else:
        simulated.observations = simulated.observations[:, :1]
    with pytest.raises(ValueError, match="full clock"):
        feasibility._validate_simulation_finite(simulated, source)


def test_runtime_metadata_reports_actual_torch_threads_and_only_named_environment(monkeypatch):
    monkeypatch.setenv("UNRELATED_PRIVATE_RUNTIME_TOKEN", "must-not-be-copied")
    evidence = feasibility._runtime_metadata()
    assert evidence["torch_num_threads"] == torch.get_num_threads()
    assert evidence["torch_num_interop_threads"] == torch.get_num_interop_threads()
    assert evidence["torch_version"] == str(torch.__version__)
    assert "UNRELATED_PRIVATE_RUNTIME_TOKEN" not in evidence["thread_environment"]


def test_historical_timing_receipt_is_never_retroactively_declared_finite(tmp_path, monkeypatch):
    from horsehair import slurm
    from horsehair.core import atomic_json, digest
    from test_behaviour_feasibility import plan_fixture
    path, plan = plan_fixture(tmp_path)
    monkeypatch.setattr(slurm, "completed_native_dependency", lambda *args: {"native": "positive-completion-fixture"})
    progress = {key: plan[key] for key in ("full_config_sha256", "framework_source_sha256", "framework_module_sha256")}
    progress.update(cell=plan["cells"][0], plan_sha256=digest(path),
        primary_scientific_kernel_source_sha256=feasibility.PRIMARY_SOURCE_SHA256,
        native_clock_bins=16, slurm_job_id="123", allocated_cpus=8,
        operations=[{"name": "adam_epoch", "learning_rate": .001, "elapsed_seconds": 1.,
            "native_clock_bins": 16, "uses_full_primary_numerics": True}])
    folder = tmp_path / "cell-000-job-123"
    folder.mkdir()
    atomic_json(folder / "receipt.json", {"cell": plan["cells"][0], "plan_sha256": digest(path),
        "slurm_job_id": "123", "allocated_cpus": 8, "status": "operation_pack_complete", "progress": progress})
    summary = feasibility.aggregate(path, tmp_path / "summary.json")
    evidence = summary["numerical_validation"]
    assert evidence["timed_operation_records"] == 1
    assert evidence["explicitly_validated_records"] == 0
    assert evidence["legacy_or_unvalidated_records"] == 1
    assert evidence["all_recorded_outputs_explicitly_validated"] is False
    assert summary["complete_declared_workload"] is False
    assert summary["admission_authority"] is False


@pytest.mark.parametrize("bad_stage", ["parameter", "gradient", "score", "simulation", "refinement_objective"])
def test_real_worker_refuses_invalid_outputs_and_preserves_failed_progress(tmp_path, monkeypatch, bad_stage):
    from horsehair import behaviour
    from horsehair.semimarkov import Sequence
    from test_behaviour_feasibility import native, plan_fixture
    import pandas as pd
    native(monkeypatch)
    path, _ = plan_fixture(tmp_path, kind="AR")
    rng = np.random.default_rng(731)
    sequence = Sequence("a0", "e0", rng.normal(scale=.1, size=(16, 2)), rng.normal(scale=.1, size=(16, 2)),
        rng.normal(scale=.1, size=(16, 1)), rng.normal(scale=.1, size=(16, 2)), rng.normal(scale=.1, size=(16, 2)),
        rng.normal(scale=.1, size=(16, 2)), np.ones(16, bool), np.arange(16) * 10.)
    monkeypatch.setattr(pd, "read_parquet", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(behaviour, "to_sequences", lambda *args: [copy.deepcopy(sequence)])
    monkeypatch.setattr(feasibility, "source_identity", lambda *args: {"sha256": feasibility.PRIMARY_SOURCE_SHA256})
    if bad_stage in {"parameter", "gradient"}:
        original_step = torch.optim.Adam.step
        def invalid_after_step(self, *args, **kwargs):
            result = original_step(self, *args, **kwargs)
            parameter = self.param_groups[0]["params"][0]
            with torch.no_grad():
                (parameter if bad_stage == "parameter" else parameter.grad).fill_(float("inf"))
            return result
        monkeypatch.setattr(torch.optim.Adam, "step", invalid_after_step)
    elif bad_stage == "score":
        monkeypatch.setattr(behaviour.ComparatorFit, "score", lambda *args:
            {"n_scored": 16, "log_score": float("nan")})
    elif bad_stage == "simulation":
        def invalid_simulation(*args):
            result = copy.deepcopy(sequence)
            result.observations[4, 0] = np.inf
            return result
        monkeypatch.setattr(behaviour, "simulate_comparator", invalid_simulation)
    else:
        monkeypatch.setattr(torch.optim.LBFGS, "step", lambda *args: torch.tensor(float("nan")))
    attempt = tmp_path / "refused-worker"
    attempt.mkdir()
    with pytest.raises(FloatingPointError):
        feasibility._worker(path, 0, attempt)
    progress = json.loads((attempt / "progress.json").read_text())
    assert progress["status"] == "operation_error"
    assert progress["error"]["type"] == "FloatingPointError"
    assert progress["complete_declared_workload"] is False
    assert progress["admission_authority"] is False
    assert progress["scientific_fit"] is False
    assert progress["runtime"]["torch_num_threads"] == 1
    assert progress["numerical_validation_version"] == 1
    failed_name = {"parameter": "adam_epoch", "gradient": "adam_epoch", "score": "unseen_group_score",
        "simulation": "causal_full_clock_simulation", "refinement_objective": "lbfgs_iteration"}[bad_stage]
    assert progress["active_operation"]["name"] == failed_name
    assert not (attempt / "worker-result.json").exists()
    for operation in progress["operations"]:
        assert operation["numerical_validation"]["status"] == "passed"
        assert operation["elapsed_seconds"] > 0
        assert operation["process_cpu_seconds"] >= 0
        assert operation["post_operation_validation_seconds"] >= 0
        assert operation["torch_num_threads"] == 1
        assert operation["average_process_cpu_cores"] == pytest.approx(
            operation["process_cpu_seconds"] / operation["elapsed_seconds"])
