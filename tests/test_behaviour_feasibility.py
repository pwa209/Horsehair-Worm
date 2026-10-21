import copy
import json
from pathlib import Path
import subprocess

import numpy as np
import pytest

from horsehair import behaviour_feasibility as feasibility
from horsehair.behaviour_workload import frozen_work_counts
from horsehair.core import atomic_json, canonical_hash, digest
from horsehair.semimarkov import Sequence


def native(monkeypatch):
    monkeypatch.setenv("SLURM_JOB_ID", "12345")
    monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)
    monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
    monkeypatch.delenv("SLURM_JOB_END_TIME", raising=False)
    monkeypatch.setenv("SLURM_CPUS_PER_TASK", "1")


def plan_fixture(tmp_path, kind="MG", rates=None):
    settings = {"models": [kind] if kind in {"M0", "MG", "MT", "MD", "MX"} else [],
        "comparators": [kind] if kind in {"AR", "HMM", "GRU"} else [],
        "primary_bin_seconds": 10, "sensitivity_bins_seconds": [],
        "learning_rates": rates or [.001], "starts": 5, "final_starts": 20, "max_epochs": 500,
        "lbfgs_iterations": 2, "inner_folds": 3, "warmup_seconds": 0,
        "neural_hidden_sizes": [4], "neural_dropout": [0],
        "parameter_recovery_replicates": 100}
    config = {"behaviour": settings, "resources": {"cpu_core_hours_cap": 20000}}
    rosters = {"10s": [{"animal_id": f"a{i}", "experiment_id": f"e{i // 2}", "bins": 16,
                         "observed_any": True, "technical_probe_eligible": i == 0,
                         "analysis_role": "behavioural_development"} for i in range(6)]}
    frozen = frozen_work_counts(config, rosters)
    cells = feasibility.structural_cells(config)
    cells[0].update(native_animal_id="a0", native_experiment_id="e0", native_clock_bins=16)
    data = tmp_path / "results/behaviour"
    data.mkdir(parents=True)
    bin_path = data / "fly_bins_10s.parquet"
    proof_path = data / "streaming_bin_provenance.json"
    bin_path.write_bytes(b"native-fixture-bytes")
    proof_path.write_text("{}")
    plan = {"schema_version": 1, "root": str(tmp_path),
        "primary_release": str(Path(feasibility.__file__).resolve().parents[2]),
        "primary_scientific_kernel_source_sha256": feasibility.PRIMARY_SOURCE_SHA256,
        "framework_source_sha256": "new-wrapper-source", "framework_module_sha256": digest(Path(feasibility.__file__)),
        "full_config_sha256": canonical_hash(config), "config": config,
        "cells": cells, "clock_rosters": rosters, "frozen_workload": frozen,
        "frozen_workload_sha256": canonical_hash(frozen), "learning_rates": settings["learning_rates"],
        "operations": list(feasibility.OPERATIONS), "complete_declared_workload": False,
        "source_result_hashes": {str(bin_path): digest(bin_path), str(proof_path): digest(proof_path)}}
    path = tmp_path / "plan.json"
    atomic_json(path, plan)
    return path, plan


def test_frozen_full_configuration_enumerates_every_structural_class_and_width():
    config = json.loads((Path(__file__).resolve().parents[1] / "configs/study.json").read_text())
    cells = feasibility.structural_cells(config)
    assert len(cells) == 39
    for width in (5, 10, 20):
        selected = [cell for cell in cells if cell["bin_seconds"] == width]
        assert len(selected) == 13
        assert {cell["model"] for cell in selected} == {"M0", "MG", "MT", "MD", "MX", "AR", "HMM", "GRU"}
        assert {(cell["hidden_size"], cell["dropout"]) for cell in selected if cell["model"] == "GRU"} == {
            (hidden, dropout) for hidden in (16, 32, 64) for dropout in (0, .1)}


@pytest.mark.parametrize("value", [float("nan"), float("inf"), 0, 29, 7201])
def test_runtime_limits_refuse_invalid_or_unbounded_work(value):
    with pytest.raises(ValueError): feasibility.runtime_limit(value)


def test_runtime_bound_leaves_two_minutes_to_write_receipts():
    assert feasibility.runtime_limit(7200, now=1000, allocation_end="1400") == 280
    with pytest.raises(ValueError): feasibility.runtime_limit(60, now=1000, allocation_end="1149")


@pytest.mark.parametrize("mutation", ["config", "roster", "selection", "cells", "coverage", "rates"])
def test_plan_rejects_changed_numerics_identity_and_fabricated_coverage(tmp_path, mutation):
    path, plan = plan_fixture(tmp_path)
    if mutation == "config": plan["config"]["behaviour"]["starts"] = 1
    elif mutation == "roster": plan["clock_rosters"]["10s"][0]["animal_id"] = "changed"
    elif mutation == "selection": plan["cells"][0]["native_animal_id"] = "a1"
    elif mutation == "cells": plan["cells"][0]["model"] = "MX"
    elif mutation == "rates": plan["learning_rates"] = [.003]
    else: plan["complete_declared_workload"] = True
    atomic_json(path, plan)
    with pytest.raises(ValueError): feasibility._load_plan(path)


def test_clock_selection_preserves_longest_missing_roster_but_measures_only_eligible_full_clock(monkeypatch):
    import pandas as pd
    roster = [{"animal_id": "all-missing", "bins": 100, "observed_any": False},
              {"animal_id": "only-before-warmup", "bins": 90, "observed_any": True},
              {"animal_id": "eligible", "bins": 80, "observed_any": True}]
    def read(path, *, filters, columns):
        identity = filters[0][2]
        assert identity != "all-missing"
        n = 90 if identity == "only-before-warmup" else 80
        return pd.DataFrame({"record_time_s": np.arange(n) * 10., "observed":
            (np.arange(n) < 2 if identity == "only-before-warmup" else np.ones(n, bool)),
            "dx": np.zeros(n), "dy": np.zeros(n)})
    monkeypatch.setattr(pd, "read_parquet", read)
    selected = feasibility._select_native_clock(Path("native.parquet"), roster, 600)
    assert selected["animal_id"] == "eligible"
    assert selected["bins"] == 80
    assert selected["observed_after_warmup"] == 20
    assert len(roster) == 3


def test_roster_requires_original_observed_statistics_and_preserves_missing_animals(tmp_path):
    import pandas as pd
    path = tmp_path / "bins.parquet"
    frame = pd.DataFrame({"animal_id": ["a", "a"], "experiment_id": ["e", "e"],
                          "analysis_role": ["behavioural_development"] * 2, "observed": [False, False]})
    frame.to_parquet(path)
    roster = feasibility._roster(path)
    assert len(roster) == 1 and roster[0]["observed_any"] is False and roster[0]["bins"] == 2
    frame.drop(columns="observed").to_parquet(path)
    with pytest.raises(ValueError, match="observed-mask column statistics"):
        feasibility._roster(path)


def test_native_zero_exit_is_required_before_any_cost_receipt_is_counted(tmp_path, monkeypatch):
    from horsehair import slurm
    path, plan = plan_fixture(tmp_path)
    folder = tmp_path / "cell-000-job-123"
    folder.mkdir()
    atomic_json(folder / "receipt.json", {"plan_sha256": digest(path), "slurm_job_id": "123"})
    monkeypatch.setattr(slurm, "completed_native_dependency", lambda *args: None)
    with pytest.raises(ValueError, match="positive native COMPLETED"):
        feasibility.aggregate(path, tmp_path / "summary.json")


def test_run_timeout_preserves_completed_operations_and_only_terminates_its_child(tmp_path, monkeypatch):
    native(monkeypatch)
    path, plan = plan_fixture(tmp_path)
    monkeypatch.setattr(feasibility, "source_identity", lambda *args: {"sha256": "new-wrapper-source"})
    killed = []
    class Child:
        pid = 999999
        calls = 0
        def wait(self, timeout):
            self.calls += 1
            if self.calls == 1: raise subprocess.TimeoutExpired("isolated-worker", timeout)
            return -15
        def terminate(self): killed.append(self.pid)
    def start(command, **kwargs):
        attempt = Path(command[-1])
        atomic_json(attempt / "progress.json", {"status": "running", "operations": [{"name": "adam_epoch",
            "learning_rate": .001, "elapsed_seconds": 4.2, "uses_full_primary_numerics": True}]})
        assert command[1:3] == ["-s", "-c"] and "import horsehair" in command[3]
        assert command[4] == plan["primary_release"]
        assert kwargs["env"]["PYTHONNOUSERSITE"] == "1"
        return Child()
    monkeypatch.setattr(feasibility.subprocess, "Popen", start)
    if feasibility.os.name != "nt": monkeypatch.setattr(feasibility.os, "killpg", lambda pid, sig: killed.append(pid))
    result = feasibility.run_cell(path, 0, 60)
    assert result["status"] == "bounded_deadline_reached"
    assert result["progress"]["operations"][0]["elapsed_seconds"] == 4.2
    assert result["complete_declared_workload"] is False
    assert killed == [999999]
    with pytest.raises(FileExistsError): feasibility.run_cell(path, 0, 60)


def test_real_child_imports_vendor_dependency_without_foreign_horsehair_or_user_site(tmp_path, monkeypatch):
    native(monkeypatch)
    path, plan = plan_fixture(tmp_path)
    primary = tmp_path / "primary"
    package = primary / "src/horsehair"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("MARKER = 'frozen-primary'\n")
    framework = tmp_path / "qualified-framework"
    wrapper = framework / "src/horsehair/behaviour_feasibility.py"
    wrapper.parent.mkdir(parents=True)
    wrapper.write_text("# Qualified wrapper identity fixture\n")
    vendor = tmp_path / "alliance-module-site-packages"
    vendor.mkdir()
    (vendor / "alliance_dependency_fixture.py").write_text("MARKER = 'approved-vendor'\n")
    foreign = tmp_path / "unapproved-release/src"
    (foreign / "horsehair").mkdir(parents=True)
    (foreign / "horsehair/__init__.py").write_text("raise RuntimeError('Wrong scientific package')\n")
    plan.update(primary_release=str(primary), root=str(tmp_path / "personal-study"),
                framework_module_sha256=digest(wrapper))
    atomic_json(path, plan)
    monkeypatch.setattr(feasibility, "__file__", str(wrapper))
    monkeypatch.setattr(feasibility, "source_identity", lambda *args: {"sha256": "new-wrapper-source"})
    monkeypatch.setenv("PYTHONPATH", feasibility.os.pathsep.join([str(foreign), str(vendor), str(framework / "src")]))
    monkeypatch.setattr(feasibility, "_CHILD", """import json, pathlib, site, sys
import alliance_dependency_fixture, horsehair
assert horsehair.MARKER == 'frozen-primary'
assert alliance_dependency_fixture.MARKER == 'approved-vendor'
assert site.ENABLE_USER_SITE is False
pathlib.Path(sys.argv[-1], 'progress.json').write_text(json.dumps({
    'status': 'vendor_import_verified', 'package_origin': horsehair.__file__,
    'dependency_origin': alliance_dependency_fixture.__file__, 'user_site': site.ENABLE_USER_SITE}))
""")
    result = feasibility.run_cell(path, 0, 60)
    assert result["worker_returncode"] == 0
    assert result["progress"]["status"] == "vendor_import_verified"
    assert Path(result["progress"]["package_origin"]).is_relative_to(primary)
    assert Path(result["progress"]["dependency_origin"]).is_relative_to(vendor)
    assert result["progress"]["user_site"] is False


def test_aggregation_retains_missing_families_even_when_each_operation_has_receipt(tmp_path, monkeypatch):
    from horsehair import slurm
    monkeypatch.setattr(slurm, "completed_native_dependency", lambda *args: {"native": "positive-completion-fixture"})
    path, plan = plan_fixture(tmp_path)
    folder = tmp_path / "cell-000-job-123"
    folder.mkdir()
    operations = [{"name": name, "learning_rate": .001, "elapsed_seconds": 100,
                   "native_clock_bins": 16, "uses_full_primary_numerics": True} for name in feasibility.OPERATIONS]
    operations.append({"name": "lbfgs_closure", "learning_rate": .001, "elapsed_seconds": 5,
                       "native_clock_bins": 16, "uses_full_primary_numerics": True})
    progress = {"cell": plan["cells"][0], "plan_sha256": digest(path), "full_config_sha256": plan["full_config_sha256"],
        "framework_source_sha256": plan["framework_source_sha256"], "framework_module_sha256": plan["framework_module_sha256"],
        "primary_scientific_kernel_source_sha256": feasibility.PRIMARY_SOURCE_SHA256, "native_clock_bins": 16,
        "slurm_job_id": "123", "allocated_cpus": 8, "operations": operations}
    atomic_json(folder / "receipt.json", {"cell": plan["cells"][0], "plan_sha256": digest(path), "slurm_job_id": "123",
        "allocated_cpus": 8, "status": "operation_pack_complete", "progress": progress})
    result = feasibility.aggregate(path, tmp_path / "summary.json")
    assert result["coverage"][0]["missing"] == []
    assert result["complete_declared_workload"] is False
    assert result["admission_authority"] is False
    assert "MALA" in " ".join(result["missing_categories"])
    assert "H2" in " ".join(result["missing_categories"])
    assert result["status"] == "measured_partial_projection_exceeds_cap"
    assert result["partial_adam_only_forecast_cpu_core_hours"] > 20000
    expected = plan["frozen_workload"]["counts"][0]["lbfgs_animal_closures_upper"] * 5 * 8 / 3600
    assert result["partial_lbfgs_only_forecast_cpu_core_hours"] == expected
    assert result["lbfgs_only_projections"][0]["measured_lbfgs_iterations_per_rate"] == 1
    assert result["lbfgs_only_projections"][0]["frozen_lbfgs_iterations"] == 2
    with pytest.raises(FileExistsError): feasibility.aggregate(path, tmp_path / "summary.json")


def test_forecast_never_uses_another_plan_receipt(tmp_path):
    path, plan = plan_fixture(tmp_path)
    folder = tmp_path / "cell-000-job-123"
    folder.mkdir()
    atomic_json(folder / "receipt.json", {"plan_sha256": "another-plan"})
    with pytest.raises(ValueError, match="another prospective plan"):
        feasibility.aggregate(path, tmp_path / "summary.json")


@pytest.mark.parametrize("kind", ["MG", "HMM", "AR", "GRU"])
def test_worker_executes_full_selected_clock_primary_operations_without_scientific_fit(tmp_path, monkeypatch, kind):
    from horsehair import behaviour
    import pandas as pd
    native(monkeypatch)
    import torch
    original_lbfgs, refinements = torch.optim.LBFGS, []
    def bounded_lbfgs(*args, **kwargs):
        refinements.append(kwargs)
        return original_lbfgs(*args, **kwargs)
    monkeypatch.setattr(torch.optim, "LBFGS", bounded_lbfgs)
    path, plan = plan_fixture(tmp_path, kind=kind)
    rng = np.random.default_rng(731)
    sequence = Sequence("a0", "e0", rng.normal(scale=.1, size=(16, 2)), rng.normal(scale=.1, size=(16, 2)),
        rng.normal(scale=.1, size=(16, 1)), rng.normal(scale=.1, size=(16, 2)), rng.normal(scale=.1, size=(16, 2)),
        rng.normal(scale=.1, size=(16, 2)), np.ones(16, bool), np.arange(16) * 10.)
    monkeypatch.setattr(pd, "read_parquet", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(behaviour, "to_sequences", lambda *args: [copy.deepcopy(sequence)])
    monkeypatch.setattr(feasibility, "source_identity", lambda *args: {"sha256": feasibility.PRIMARY_SOURCE_SHA256})
    attempt = tmp_path / "worker"
    attempt.mkdir()
    feasibility._worker(path, 0, attempt)
    result = json.loads((attempt / "worker-result.json").read_text())
    assert result["status"] == "operation_pack_complete"
    assert result["full_roster_animals"] == 6
    assert result["native_clock_bins"] == 16
    assert result["primary_scientific_kernel_source_sha256"] == feasibility.PRIMARY_SOURCE_SHA256
    assert result["framework_source_sha256"] == "new-wrapper-source"
    assert result["complete_declared_workload"] is False
    assert result["scientific_fit"] is False
    assert refinements == [{"max_iter": 1, "line_search_fn": "strong_wolfe"}]
    assert result["optimizer_measurement_scope"]["frozen_requested_starts"] == 5
    assert result["optimizer_measurement_scope"]["frozen_requested_adam_epochs"] == 500
    assert result["optimizer_measurement_scope"]["full_optimizer_budget_executed"] is False
    assert set(feasibility.OPERATIONS).issubset({row["name"] for row in result["operations"]})
    assert all(row["native_clock_bins"] == 16 for row in result["operations"])
    assert any(row["name"] == "lbfgs_closure" for row in result["operations"])
    assert not list(tmp_path.glob("*.pt"))
