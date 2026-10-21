"""Hand-enumerated grouped work and immutable native cost evidence contracts."""
import ast
import copy
import hashlib
import importlib
import json
from pathlib import Path

import pytest

from horsehair import cost_forecast_review as r
from horsehair.behaviour_feasibility import structural_cells, OPERATIONS
from horsehair.behaviour_workload import frozen_work_counts
from horsehair.core import atomic_json, canonical_hash, digest


def configuration():
    return {"behaviour": {"models": ["M0"], "comparators": ["AR"],
        "primary_bin_seconds": 5, "sensitivity_bins_seconds": [], "inner_folds": 2,
        "starts": 2, "final_starts": 3, "max_epochs": 4, "lbfgs_iterations": 2,
        "learning_rates": [.001, .003], "recovery_models": ["MG"],
        "recovery_effects": [0], "parameter_recovery_replicates": 1},
        "resources": {"cpu_core_hours_cap": 20000}}


def roster():
    return {"5s": [{"animal_id": str(i), "experiment_id": g,
        "analysis_role": "behavioural_development", "bins": 10,
        "observed_any": i != 2} for i, g in enumerate(["a", "b", "b", "c"])]}


def packet():
    module = importlib.import_module("torch.optim.lbfgs")
    text = Path(module.__file__).read_text(encoding="utf8")
    tree = ast.parse(text)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "LBFGS")
    nodes = {"LBFGS." + n.name: n for n in cls.body if isinstance(n, ast.FunctionDef)}
    nodes.update({n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)})
    methods = {}
    for name in r.REVIEWED_METHODS:
        source = ast.get_source_segment(text, nodes[name])
        methods[name] = {"source_text": source, "source_sha256": hashlib.sha256(source.encode()).hexdigest()}
    return {"version": "2.6.0", "source_text": text,
            "source_sha256": hashlib.sha256(text.encode()).hexdigest(), "methods": methods}


def proof(root, job, cpus):
    native = {job: {"state": "COMPLETED", "exit_code": "0:0", "cpus": cpus, "gpus": 0,
                    "elapsed_seconds": 100, "basis": "sacct final allocated wall time"}}
    out = {"job_id": job, "array": None, "dependency_kind": "afterok",
           "source_path": str(root / "state/resource-budget.json"),
           "native_records": native, "native_records_sha256": canonical_hash(native)}
    out["completion_proof_sha256"] = canonical_hash(out)
    return out


def evidence(tmp_path):
    cfg, rosters = configuration(), roster()
    plan = {"schema_version": 1, "config": cfg, "clock_rosters": rosters,
        "root": str(tmp_path), "full_config_sha256": canonical_hash(cfg),
        "framework_source_sha256": "f" * 64, "framework_module_sha256": "e" * 64,
        "primary_scientific_kernel_source_sha256": r.PRIMARY_SOURCE_SHA256,
        "source_result_hashes": {str(tmp_path / "results/behaviour/fly_bins_5s.parquet"): "c" * 64},
        "learning_rates": cfg["behaviour"]["learning_rates"],
        "complete_declared_workload": False, "scientific_fit": False, "admission_authority": False,
        "cells": [dict(c, native_clock_bins=10, native_animal_id="0", native_experiment_id="a")
                  for c in structural_cells(cfg)]}
    plan["frozen_workload"] = frozen_work_counts(cfg, rosters)
    plan["frozen_workload_sha256"] = canonical_hash(plan["frozen_workload"])
    planpath = tmp_path / "plan.json"
    atomic_json(planpath, plan)
    sha = digest(planpath)
    records = []
    for cell in plan["cells"]:
        job, cpus = str(101 + cell["index"]), 2
        record = make_record(plan, sha, cell, job, cpus, 2, 3)
        path = tmp_path / ("receipt-" + job + ".json")
        atomic_json(path, record)
        records.append({"path": str(path), "sha256": digest(path), "receipt": record,
                        "native_completion_proof": proof(tmp_path, job, cpus)})
    # Hand enumeration: groups a1/b2/c1, six inner fits. Inner training
    # passes=8; rates2*starts2 ->32. Outer passes=8*5=40 for M0,
    # 8*2=16 for AR. M0 recovery=1truth*4ROIs*1*2starts=8.
    # Therefore M0=80, AR=48 passes/epoch; epochs4, closures1+26*2=53.
    aggregate = {k: plan[k] for k in ("full_config_sha256", "framework_source_sha256",
        "primary_scientific_kernel_source_sha256", "complete_declared_workload", "scientific_fit", "admission_authority")}
    aggregate.update(plan_sha256=sha,
        partial_adam_only_forecast_cpu_core_hours=128 * 4 * 4 / 3600,
        partial_lbfgs_only_forecast_cpu_core_hours=128 * 53 * 6 / 3600,
        partial_optimizer_forecast_cpu_core_hours=(128 * 4 * 4 + 128 * 53 * 6) / 3600,
        coverage=[{"cell": c, "required_operations": 8, "completed_operations": 8, "missing": []} for c in plan["cells"]],
        receipt_bindings=[{k: v[k] for k in ("path", "sha256")} for v in records],
        native_completion_proofs=[v["native_completion_proof"] for v in records])
    apath = tmp_path / "aggregate.json"
    atomic_json(apath, aggregate)
    return plan, sha, aggregate, records, planpath, apath


def make_record(plan, sha, cell, job, cpus, adam, closure):
    p = {k: plan[k] for k in ("full_config_sha256", "framework_source_sha256",
            "framework_module_sha256", "primary_scientific_kernel_source_sha256")}
    p.update(cell=cell, plan_sha256=sha, native_clock_bins=10,
             slurm_job_id=job, allocated_cpus=cpus, native_input_sha256="c" * 64,
             scientific_fit=False, complete_declared_workload=False, admission_authority=False)
    p["operations"] = [{"name": name, "learning_rate": rate,
        "elapsed_seconds": adam if name == "adam_epoch" else closure if name == "lbfgs_closure" else 1,
        "uses_full_primary_numerics": True, "native_clock_bins": 10}
        for rate in plan["learning_rates"] for name in [*OPERATIONS, "lbfgs_closure"]]
    return {"plan_sha256": sha, "cell": cell, "slurm_job_id": job,
            "allocated_cpus": cpus, "progress": p,
            "scientific_fit": False, "complete_declared_workload": False, "admission_authority": False}


def test_hand_enumerated_grouped_multiplicities():
    rows, report = r.roster_work_rows(configuration(), roster())
    assert report["by_bin"]["5s"]["inner_training_roi_passes_per_start_rate"] == 8
    assert report["by_bin"]["5s"]["inner_fits_per_structure_rate"] == 6
    assert [v["training_roi_passes_per_epoch"] for v in rows] == [80, 48]
    filtered, report = r.roster_work_rows(configuration(), roster(), exclude_all_missing=True)
    assert report["by_bin"]["5s"]["excluded_all_missing_ids"] == ["2"]
    assert report["by_bin"]["5s"]["recording_groups"] == {"a": 1, "b": 1, "c": 1}
    assert [v["training_roi_passes_per_epoch"] for v in filtered] == [60, 36]


def test_scale_roster_difference_is_reported_not_intersected():
    c = configuration(); c["behaviour"]["sensitivity_bins_seconds"] = [10]
    rs = roster(); rs["10s"] = copy.deepcopy(rs["5s"]); rs["10s"][2]["observed_any"] = True
    _, report = r.roster_work_rows(c, rs, exclude_all_missing=True)
    assert report["same_retained_recording_roster_across_bins"] is False
    assert [v["retained_recording_rois"] for v in report["by_bin"].values()] == [3, 4]


def test_selected_shape_bound_preserves_all_inner_candidates():
    c = configuration(); c["behaviour"].update(models=[], comparators=["GRU"], neural_hidden_sizes=[2], neural_dropout=[0, .1])
    rows, _ = r.roster_work_rows(c, roster())
    costs = {0: {"adam_epoch": 1}, 1: {"adam_epoch": 3}}
    old = r._scenario(rows, costs, 0)
    new = r._scenario(rows, costs, 0, select_one_outer_shape=True)
    # Each shape has inner32+outer16+freeze8=56. Inner BOTH retained;
    # selected outer/freeze charged at slowest cost3, not an outcome winner.
    assert old["partial_adam_cpu_core_hours"] == pytest.approx(56 * 4 * 4 / 3600)
    assert new["partial_adam_cpu_core_hours"] == pytest.approx((32 * (1 + 3) + (16 + 8) * 3) * 4 / 3600)


def test_review_replays_hand_arithmetic_and_leaves_authority_false(tmp_path):
    p, sha, a, records, *_ = evidence(tmp_path)
    result = r.review_documents(p, a, records, plan_sha256=sha)
    assert result["historical_arithmetic_replay"]["partial_optimizer_cpu_core_hours"] == pytest.approx(a["partial_optimizer_forecast_cpu_core_hours"])
    assert result["completed_required_operations"] == result["required_operations"] == 16
    assert result["pending"]
    assert all(result[k] is False for k in ["admission_authority", "scientific_fit", "scientific_result", "complete_declared_workload"])


def test_mixed_cpu_operations_keep_their_own_allocation(tmp_path):
    p, sha, a, records, *_ = evidence(tmp_path)
    old_paths = {v["path"] for v in records}
    extra = make_record(p, sha, p["cells"][0], "103", 1, 10, 1)
    records.append({"path": "additional", "sha256": "a" * 64, "receipt": extra,
                    "native_completion_proof": proof(tmp_path, "103", 1)})
    result = r.review_documents(p, a, records, plan_sha256=sha, historical_paths=old_paths)
    # Old M0 Adam=2s*2CPU=4; new=10s*1CPU=10, not10s*2CPU=20.
    paired = result["scenarios"]["paired_cpu_original_nominal_roster"]
    assert paired["partial_adam_cpu_core_hours"] == pytest.approx((80 * 10 + 48 * 4) * 4 / 3600)
    assert result["historical_arithmetic_replay"]["partial_adam_cpu_core_hours"] == pytest.approx(128 * 4 * 4 / 3600)


def test_incomplete_operation_remains_pending(tmp_path):
    p, sha, a, records, *_ = evidence(tmp_path)
    records[0]["receipt"]["progress"]["operations"] = [o for o in records[0]["receipt"]["progress"]["operations"] if not (o["name"] == "lbfgs_iteration" and o["learning_rate"] == .003)]
    a["coverage"][0].update(completed_operations=7, missing=[["lbfgs_iteration", .003]])
    # JSON preserves missing tuples as lists; normalize the synthetic fixture.
    a["coverage"][0]["missing"] = [("lbfgs_iteration", .003)]
    result = r.review_documents(p, a, records, plan_sha256=sha)
    assert result["completed_required_operations"] == 15
    assert result["coverage"][0]["missing"] == [("lbfgs_iteration", .003)]
    assert "remain unmeasured" in result["pending"][0]


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -1, 0, True])
def test_nonfinite_or_invalid_operation_refused(tmp_path, bad):
    p, sha, a, records, *_ = evidence(tmp_path)
    records[0]["receipt"]["progress"]["operations"][0]["elapsed_seconds"] = bad
    with pytest.raises(ValueError, match="finite positive"):
        r.review_documents(p, a, records, plan_sha256=sha)


def test_missing_mask_statistics_cannot_clear_exclusions():
    rs = roster(); del rs["5s"][0]["observed_any"]
    with pytest.raises(ValueError, match="observed_any"):
        r.roster_work_rows(configuration(), rs, exclude_all_missing=True)


def test_source_and_native_proof_mutations_refused(tmp_path):
    p, sha, a, records, *_ = evidence(tmp_path)
    records[0]["receipt"]["progress"]["framework_source_sha256"] = "changed"
    with pytest.raises(ValueError, match="binding changed"):
        r.review_documents(p, a, records, plan_sha256=sha)
    records[0]["receipt"]["progress"]["framework_source_sha256"] = p["framework_source_sha256"]
    records[0]["native_completion_proof"]["native_records"]["101"]["state"] = "FAILED"
    with pytest.raises(ValueError, match="proof hash"):
        r.review_documents(p, a, records, plan_sha256=sha)


def test_historical_arithmetic_mutation_refused(tmp_path):
    p, sha, a, records, *_ = evidence(tmp_path)
    a["partial_optimizer_forecast_cpu_core_hours"] *= 2
    with pytest.raises(ValueError, match="arithmetic"):
        r.review_documents(p, a, records, plan_sha256=sha)


def test_consistent_but_unreviewed_primary_source_is_refused(tmp_path):
    p, sha, a, records, *_ = evidence(tmp_path)
    p["primary_scientific_kernel_source_sha256"] = "d" * 64
    a["primary_scientific_kernel_source_sha256"] = "d" * 64
    with pytest.raises(ValueError, match="exact frozen primary"):
        r.review_documents(p, a, records, plan_sha256=sha)


def test_original_bin_hash_mutation_refused(tmp_path):
    p, sha, a, records, *_ = evidence(tmp_path)
    records[0]["receipt"]["progress"]["native_input_sha256"] = "changed"
    with pytest.raises(ValueError, match="native bin hash"):
        r.review_documents(p, a, records, plan_sha256=sha)


def test_receipt_cannot_inherit_scientific_authority(tmp_path):
    p, sha, a, records, *_ = evidence(tmp_path)
    records[0]["receipt"]["scientific_fit"] = True
    with pytest.raises(ValueError, match="scientific or admission authority"):
        r.review_documents(p, a, records, plan_sha256=sha)


def test_gpu_native_proof_is_refused_even_with_valid_hashes(tmp_path):
    p, sha, a, records, *_ = evidence(tmp_path)
    pf = records[0]["native_completion_proof"]
    pf["native_records"]["101"]["gpus"] = 1
    pf["native_records_sha256"] = canonical_hash(pf["native_records"])
    pf["completion_proof_sha256"] = canonical_hash({k: v for k, v in pf.items() if k != "completion_proof_sha256"})
    with pytest.raises(ValueError, match="zero-GPU"):
        r.review_documents(p, a, records, plan_sha256=sha)


def test_real_installed_torch_source_or_explicit_refusal():
    p = packet(); result = r.reviewed_lbfgs_bound(200, p)
    if p["source_sha256"] == r.REVIEWED_TORCH_SOURCE:
        assert result["supported"] is True
        assert result["closure_bound"] == 275
        assert result["historical_closure_bound"] == 5201
        # max_eval=1 is only checked after the first complete line search.
        assert r.reviewed_lbfgs_bound(1, p)["closure_bound"] == 27
    else:
        assert result["supported"] is False and result["closure_bound"] == 5201


@pytest.mark.parametrize("mutation", ["version", "module", "method", "method_hash"])
def test_torch_packet_changes_never_enable_tightening(mutation):
    p = packet()
    if mutation == "version": p["version"] = "2.7.0"
    if mutation == "module": p["source_text"] += "\n# changed\n"
    if mutation == "method": p["methods"]["LBFGS.step"]["source_text"] += "\n"
    if mutation == "method_hash": p["methods"]["LBFGS.step"]["source_sha256"] = "f" * 64
    result = r.reviewed_lbfgs_bound(200, p)
    assert result["supported"] is False and result["closure_bound"] == 5201


def test_no_source_packet_retains_original_bound():
    assert r.reviewed_lbfgs_bound(200, None)["closure_bound"] == 5201


def test_paths_preserve_all_inputs_and_refuse_replay(tmp_path):
    p, sha, a, records, pp, ap = evidence(tmp_path)
    inputs = [pp, ap, *(Path(v["path"]) for v in records)]
    original = {str(path): digest(path) for path in inputs}
    output = tmp_path / "review.json"
    result = r.review_paths(pp, sha, ap, digest(ap), output)
    assert result["input_bindings"] == original
    assert original == {str(path): digest(path) for path in inputs}
    assert json.loads(output.read_bytes())["admission_authority"] is False
    with pytest.raises(ValueError, match="replace"):
        r.review_paths(pp, sha, ap, digest(ap), output)


def test_paths_refuse_changed_receipt(tmp_path):
    _, sha, _, records, pp, ap = evidence(tmp_path)
    Path(records[0]["path"]).write_text("{}")
    with pytest.raises(ValueError, match="Input hash"):
        r.review_paths(pp, sha, ap, digest(ap), tmp_path / "review.json")
