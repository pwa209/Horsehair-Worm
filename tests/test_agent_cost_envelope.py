import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from horsehair import agent_cost_envelope as envelope
from horsehair import agents
from horsehair.core import canonical_hash, source_identity, digest


def primary_config():
    return json.loads((Path(__file__).resolve().parents[1] / "configs/study.json").read_text())


def measurement(rate=.0001):
    return {"steps": 200, "blocks": 4, "seconds_per_step": rate,
            "block_seconds_per_step": [rate / 2, rate, rate / 2, rate / 2]}


def frozen_protocol(config):
    s = config["agents"]
    return {"biological_directions": None, "implementation_sha256": agents._implementation_sha256(),
            "full_config_sha256": canonical_hash(config), "config_sha256": agents._digest(s),
            "H4_inference_specification": agents.h4_specification(config), "final_seeds": s["final_seeds"],
            "development_seeds": s["development_seeds"], "ranks": s["ranks"], "strengths": s["strengths"],
            "learning_rate": s["lr_grid"][0], "directions": {f"{m}:{r}": [1.] * r
                for m in agents.MECHANISMS for r in s["ranks"] if m != "motor_gain" or r == 1}}


def recorded_fixture(config=None, index=0):
    config = config or primary_config()
    task, architecture, seeds = agents.benchmark_cells(config)[index]
    variants = envelope.stress_variants(config)
    bounds = envelope.maximum_work(config)
    rows = [{**variant, "original_weights_unchanged": True, "setup_seconds": .01,
             "restoration_seconds": .02, "batch_measurements": [
                 {"batch": batch, "evaluation": measurement(), "clean_kl": measurement(.0002)}
                 for batch in sorted({bounds["evaluation_batch"], bounds["calibration_batch"]})]}
            for variant in variants]
    reports = [{"seed": seed, "measurements": copy.deepcopy(rows), "policy_load_and_evidence_seconds": .1,
                "environment": [{"probe": probe, "batch": bounds["evaluation_batch"], "setup_seconds": .02,
                                 "step": measurement()} for probe in ("main", *agents.PROBES)],
                "production_skeleton": [{"probe": probe, "batch": batch, "steps": 200, "capture": True,
                    "cue_removal": True, "evaluation_seconds_per_step": .0001, "kl_seconds_per_step": .0001}
                    for batch in sorted({bounds["evaluation_batch"], bounds["calibration_batch"]})
                    for probe in ("main", *agents.PROBES)],
                "original_numerical_grid": {"unavailable_episode_rows": 5, "unavailable_calibration_rows": 0,
                    "complete_mechanism_grid": True, "full_calibration_algorithm": True, "restoration_qualified": True},
                "io": {"synthetic": True, "rows": bounds["trial_rows"], "seconds": .3, "bytes": 1000, "files": 2}}
               for seed in seeds]
    bindings = [{"path": path, "sha256": "b" * 64, "bytes": 1000}
                for path in sorted(envelope._expected_input_paths(config, task, architecture, seeds))]
    frozen_binding = next(value for value in bindings if Path(value["path"]).name == "frozen_protocol.json")
    for report in reports:
        report["original_receipt"] = next(value for value in bindings
            if Path(value["path"]).name == "cpu_evaluation_benchmark.json"
            and Path(value["path"]).parent.name.endswith("_" + str(report["seed"])))
    receipt = {"kind": "agent-cpu-cost-envelope-cell", "status": "measured", "maximum_work": bounds,
               "primary_scientific_source_sha256": envelope.PRIMARY_SOURCE_SHA256,
               "full_config_sha256": canonical_hash(config), "measurement_steps": 200,
               "source_sha256": "f" * 64, "kernel_function_sha256": envelope._kernel_hashes(agents),
               "allocated_cpus": 4, "job_id": str(10000 + index), "task_index": index, "task": task,
               "architecture": architecture, "seeds": list(seeds), "variant_roster": variants, "reports": reports,
               "measured_seconds": 70, "inputs": bindings, "frozen_protocol_binding": frozen_binding,
               "frozen_protocol_record": frozen_protocol(config)}
    native = {"job_id": receipt["job_id"], "state": "COMPLETED", "exit_code": "0:0", "cpus": 4,
              "gpus": 0, "elapsed_seconds": 100, "peak_rss_bytes": 2 * 1024 ** 3}
    return config, receipt, native


def forecast(receipt, config, native, **kwargs):
    return envelope.forecast_cell(receipt, config, native, framework_source_sha256="f" * 64, **kwargs)


def test_exact_primary_loop_counts_include_zero_strength_calibration_and_every_seed():
    config = primary_config()
    counts = envelope.maximum_work(config)
    assert counts["mechanism_rank_combinations"] == 25
    assert counts["grid_points"] == 300
    assert counts["evaluations"] == 2404
    assert counts["kl_calls"] == 1550
    assert counts["kl_steps"] == 310000
    assert counts["restoration_checks"] == 300
    assert counts["trial_rows"] == 153600
    assert counts["evaluation_batch"] == 64 and counts["calibration_batch"] == 32
    assert counts["unavailable_episode_discount"] is False
    assert len(agents.benchmark_cells(config)) == 12
    assert all(seeds == (900, 901) for _, _, seeds in agents.benchmark_cells(config))


def test_stress_roster_covers_all_operator_shapes_and_search_upper_endpoint():
    config = primary_config()
    variants = envelope.stress_variants(config)
    assert len(variants) == 650
    assert len({tuple(row.values()) for row in variants}) == len(variants)
    assert {row["mechanism"] for row in variants} == set(agents.MECHANISMS)
    assert all(row["rank"] == 1 for row in variants if row["mechanism"] == "motor_gain")
    for row in variants:
        assert row["coefficient_norm"] <= 8
        if row["coefficient_norm"] == 8:
            assert row["matching_arm"] == "policy_kl"
    assert {row["random_restoration"] for row in variants} == {False, True}


@pytest.mark.parametrize("architecture", ["rnn", "gru", "modular", "feedforward"])
def test_real_full_200_step_operator_smoke_keeps_base_weights(architecture):
    torch = pytest.importorskip("torch")
    torch.set_num_threads(1)
    policy = agents.make_policy(architecture, 25000)
    policy.requires_grad_(False)
    before = agents.state_hash(policy)
    variant = {"mechanism": "recurrent_gate", "rank": 2, "matching_arm": "policy_kl",
               "coefficient_norm": 8., "random_restoration": True}
    measured = envelope._measure_mode(agents, policy, "delayed_choice", variant, [2, 4])
    assert agents.state_hash(policy) == before
    assert measured["original_weights_unchanged"] is True
    for batch in measured["batch_measurements"]:
        assert batch["evaluation"]["steps"] == batch["clean_kl"]["steps"] == 200
        assert batch["evaluation"]["seconds_per_step"] > 0


def test_full_200_step_fixture_cannot_be_shortened():
    with pytest.raises(ValueError, match="exactly 200"):
        envelope._maximum_block_seconds(lambda: None, 199)


def test_forecast_charges_full200_despite_unavailable_original_rows():
    config, receipt, native = recorded_fixture()
    result = forecast(receipt, config, native)
    assert result["cpu_core_hours_per_final_policy"] > 0
    assert result["safety_factor"] == 1.5
    assert result["native_fixed_overhead_seconds"] == 30
    assert result["all_episode_steps_charged"] == 200
    assert result["unavailable_episode_discount"] is False
    assert result["scientific_result"] is False and result["admission_status"] == "not_admitted"
    receipt["reports"][0]["original_numerical_grid"]["unavailable_episode_rows"] = 999999
    assert forecast(receipt, config, native) == result


@pytest.mark.parametrize("field,value", [("state", "RUNNING"), ("exit_code", "1:0"), ("cpus", 2),
                                        ("gpus", 1), ("peak_rss_bytes", 0), ("elapsed_seconds", float("inf"))])
def test_incomplete_failed_or_wrong_shape_native_accounting_cannot_forecast(field, value):
    config, receipt, native = recorded_fixture()
    native[field] = value
    with pytest.raises(ValueError, match="native accounting"):
        forecast(receipt, config, native)


@pytest.mark.parametrize("mutation", ["source", "kernel", "counts", "batch", "seed", "missing_variant", "weights", "negative_cost"])
def test_incomplete_or_changed_measurement_cannot_forecast(mutation):
    config, receipt, native = recorded_fixture()
    if mutation == "source":
        receipt["source_sha256"] = "a" * 64
    elif mutation == "kernel":
        receipt["kernel_function_sha256"]["evaluate"] = "a" * 64
    elif mutation == "counts":
        receipt["maximum_work"]["evaluations"] -= 1
    elif mutation == "batch":
        receipt["reports"][0]["measurements"][0]["batch_measurements"].pop()
    elif mutation == "seed":
        receipt["seeds"][0] = 1000
    elif mutation == "missing_variant":
        receipt["reports"][0]["measurements"].pop()
    elif mutation == "weights":
        receipt["reports"][0]["measurements"][0]["original_weights_unchanged"] = False
    else:
        receipt["reports"][0]["io"]["seconds"] = -1
    with pytest.raises(ValueError):
        forecast(receipt, config, native)


def test_both_seeds_and_declared_safety_factor_are_required():
    config, receipt, native = recorded_fixture()
    with pytest.raises(ValueError, match="at least 1.5"):
        forecast(receipt, config, native, safety=1.49)
    receipt["reports"].pop()
    with pytest.raises(ValueError, match="seed/operator coverage"):
        forecast(receipt, config, native)


def test_twelve_cell_aggregate_preserves_unavailable_counts_and_refuses_missing_cell(tmp_path, monkeypatch):
    config = primary_config()
    native = {}
    monkeypatch.setattr(agents, "_directory", lambda root, settings: tmp_path / "agents")
    frozen_path = tmp_path / "agents/frozen_protocol.json"
    frozen_path.parent.mkdir()
    frozen_path.write_text(json.dumps(frozen_protocol(config)))
    for index in range(12):
        _, receipt, job = recorded_fixture(config, index)
        for value in receipt["inputs"]:
            path = Path(value["path"])
            if path != frozen_path:
                path.parent.mkdir(exist_ok=True, parents=True)
                path.write_text("synthetic evidence fixture")
            value.update(envelope._binding(path))
        path = tmp_path / f"cell-{index:02d}" / "receipt.json"
        path.parent.mkdir()
        path.write_text(json.dumps(receipt))
        native[job["job_id"]] = job
    result = envelope.aggregate(tmp_path, config, native, framework_source_sha256="f" * 64)
    assert result["final_policies"] == 240 and result["measured_policies"] == 24
    assert result["original_unavailable_episode_rows_preserved"] == 120
    assert result["numerical_failures_repaired"] is False
    assert result["admission_status"] == "not_admitted"
    assert result["additional_evaluation_bytes"] == 240 * 1500
    assert result["additional_evaluation_files"] > 240 * 2
    (tmp_path / "cell-11/receipt.json").unlink()
    with pytest.raises(ValueError, match="regular file"):
        envelope.aggregate(tmp_path, config, native, framework_source_sha256="f" * 64)


def test_original_inputs_cannot_change_after_measurement(tmp_path):
    path = tmp_path / "original-evidence.json"
    path.write_text("unavailable outcomes retained")
    binding = envelope._binding(path)
    envelope._unchanged(binding)
    path.write_text("modified outcomes")
    with pytest.raises(ValueError, match="changed"):
        envelope._unchanged(binding)


@pytest.mark.parametrize("change", ["biological", "seeds", "ranks", "strengths", "H4", "missing_hash"])
def test_frozen_protocol_scope_and_exact_roster_are_required(change):
    config, receipt, native = recorded_fixture()
    protocol = receipt["frozen_protocol_record"]
    if change == "biological":
        protocol["biological_directions"] = {"classes": {"input_gain": 1.}}
    elif change == "seeds":
        protocol["final_seeds"] = protocol["final_seeds"][:-1]
    elif change == "ranks":
        protocol["ranks"] = [1, 2, 4]
    elif change == "strengths":
        protocol["strengths"] = [0, .1]
    elif change == "H4":
        protocol["H4_inference_specification"]["unavailable_points"] = "replace failures"
    else:
        receipt["inputs"] = []
    with pytest.raises(ValueError):
        forecast(receipt, config, native)


def test_real_environment_and_io_fixtures_keep_original_null_evidence(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    rows = envelope._environment_measurements(agents, "foraging", 2)
    assert {row["probe"] for row in rows} == {"main", *agents.PROBES}
    assert all(row["step"]["steps"] == 200 for row in rows)
    original = tmp_path / "original.parquet"
    pd.DataFrame({"numerical_status": ["available", "unavailable"], "success": [1., None]}).to_parquet(original)
    before = envelope._binding(original)
    measured = envelope._io_measurement(agents, original, tmp_path, 20)
    assert measured["rows"] == 20 and measured["synthetic"] is True and measured["scientific_result"] is False
    assert measured["seconds"] > 0 and measured["bytes"] > 0
    envelope._unchanged(before)
    diagnostic = json.loads((tmp_path / "synthetic-diagnostics-fixture.json").read_text())
    assert diagnostic["scientific_result"] is False and len(diagnostic["diagnostics"]) == 20


def test_real_production_skeleton_covers_masks_capture_and_full_200_steps():
    torch = pytest.importorskip("torch")
    torch.set_num_threads(1)
    policy = agents.make_policy("gru", 25000)
    policy.requires_grad_(False)
    before = agents.state_hash(policy)
    original_environment = agents.Environment
    rows = envelope._production_skeleton_measurements(agents, policy, "delayed_choice", [2, 4])
    assert len(rows) == 8 and {row["batch"] for row in rows} == {2, 4}
    assert all(row["steps"] == 200 and row["capture"] is True for row in rows)
    assert agents.state_hash(policy) == before and agents.Environment is original_environment


def test_changed_full_configuration_cannot_shrink_final_seeds_even_with_changed_receipt():
    config, receipt, native = recorded_fixture()
    config["agents"]["final_seeds"] = [1000]
    receipt["full_config_sha256"] = canonical_hash(config)
    receipt["frozen_protocol_record"] = frozen_protocol(config)
    with pytest.raises(ValueError, match="immutable primary configuration"):
        forecast(receipt, config, native)


@pytest.mark.parametrize("change", ["empty", "unbound_original", "missing_checkpoint", "extra"])
def test_exact_original_input_roster_cannot_be_omitted_or_redirected(change):
    config, receipt, native = recorded_fixture()
    if change == "empty":
        receipt["inputs"] = []
    elif change == "unbound_original":
        receipt["reports"][0]["original_receipt"] = {"path": "another.json", "sha256": "c" * 64, "bytes": 100}
    elif change == "missing_checkpoint":
        receipt["inputs"] = [value for value in receipt["inputs"] if Path(value["path"]).name != "checkpoint.pt"]
    else:
        receipt["inputs"].append({"path": "another.json", "sha256": "c" * 64, "bytes": 100})
    with pytest.raises(ValueError):
        forecast(receipt, config, native)


@pytest.fixture
def primary_launch_fixture(tmp_path, monkeypatch):
    from horsehair import core
    primary, framework, root, vendor, foreign = [tmp_path / name for name in
        ("primary", "maintenance", "scratch", "alliance-vendor", "foreign")]
    package = primary / "src/horsehair"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    shutil.copyfile(core.__file__, package / "core.py")
    (primary / "configs").mkdir()
    config = {"technical_launch_fixture": True}
    (primary / "configs/study.json").write_text(json.dumps(config))
    module = framework / "src/horsehair/agent_cost_envelope.py"
    module.parent.mkdir(parents=True)
    module.write_text('''def _measure_cell(primary, framework, root, case, index):
    import fixture_alliance_vendor, json, sys
    from pathlib import Path
    Path(case).write_text(json.dumps({"vendor": fixture_alliance_vendor.VALUE, "no_user_site": sys.flags.no_user_site, "index": index}))
''')
    vendor.mkdir()
    (vendor / "fixture_alliance_vendor.py").write_text('VALUE="qualified-vendor-dependency"')
    (foreign / "horsehair").mkdir(parents=True)
    (foreign / "horsehair/__init__.py").write_text('raise AssertionError("Foreign horsehair imported")')
    for release in (primary, framework):
        (release / "release.json").write_text(json.dumps({"source_identity": source_identity(release)}))
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(map(str, [framework / "src", vendor, foreign, root, primary / "src"])))
    monkeypatch.setenv("PYTHONHOME", "invalid-home-must-not-be-inherited")
    monkeypatch.setenv("PYTHONSTARTUP", "invalid-startup")
    monkeypatch.setenv("PYTHONINSPECT", "1")
    environment = envelope._primary_worker_environment(primary, framework, root)
    arguments = [str(primary), str(framework), str(root), str(tmp_path / "worker-output.json"), "7",
                 source_identity(primary)["sha256"], canonical_hash(config), source_identity(framework)["sha256"], digest(module)]
    return primary, framework, root, vendor, environment, arguments


def test_real_vendor_pythonpath_survives_source_isolation_and_disables_user_site(primary_launch_fixture):
    primary, framework, root, vendor, environment, arguments = primary_launch_fixture
    assert environment["PYTHONPATH"].split(os.pathsep) == [str((primary / "src").resolve()), str(vendor.resolve())]
    assert environment["PYTHONNOUSERSITE"] == "1"
    assert all(key not in environment for key in ("PYTHONHOME", "PYTHONSTARTUP", "PYTHONINSPECT"))
    result = subprocess.run([sys.executable, "-s", "-c", envelope.PRIMARY_COST_BOOTSTRAP, *arguments],
                            cwd=primary, env=environment, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    observed = json.loads(Path(arguments[3]).read_text())
    assert observed == {"vendor": "qualified-vendor-dependency", "no_user_site": 1, "index": 7}


@pytest.mark.parametrize("argument_index", [5, 6, 7, 8])
def test_real_worker_rejects_wrong_primary_config_framework_or_module_source_hash(primary_launch_fixture, argument_index):
    primary, framework, root, vendor, environment, arguments = primary_launch_fixture
    arguments[argument_index] = "0" * 64
    result = subprocess.run([sys.executable, "-s", "-c", envelope.PRIMARY_COST_BOOTSTRAP, *arguments],
                            cwd=primary, env=environment, capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    assert not Path(arguments[3]).exists()
