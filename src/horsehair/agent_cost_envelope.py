"""Separate CPU cost measurements around an unchanged, frozen agent experiment.

These synthetic operator fixtures produce no agent outcomes. Original complete
benchmark grids, including unavailable outcomes, remain immutable. The envelope
is an empirical cost estimate with a full 200-step charge for every episode; it
is neither numerical qualification nor permission to run the final experiment.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import inspect
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from horsehair.core import atomic_json, canonical_hash, digest, native_job_id, read_json, source_identity

PRIMARY_SOURCE_SHA256 = "0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630"
FULL_CONFIG_SHA256 = "446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35"
PRIMARY_RELEASE = Path("/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf")
MECHANISMS = ("input_gain", "recurrent_gate", "hidden_displacement", "output_gain",
              "random_subspace", "unstructured", "motor_gain")
KERNELS = ("evaluate", "perturbation_experiment", "_clean_kl", "_restoration_check",
           "_sample_action", "_perturbed_policy", "make_policy")
MEASUREMENT_STEPS = 200
MEASUREMENT_RESTORATIONS = 3


def require(condition, message):
    if not condition:
        raise ValueError(message)


def maximum_work(config):
    """Count the complete frozen loop, including zero-strength KL replays."""
    s = config["agents"]
    ranks, strengths = s["ranks"], s["strengths"]
    require(len(ranks) == len(set(ranks)) and set(ranks).issubset({1, 2, 4, 8}), "Invalid rank roster")
    require(len(strengths) == len(set(strengths)) and all(math.isfinite(float(x)) and x >= 0 for x in strengths), "Invalid strength roster")
    episodes, calibration = int(s["evaluation_episodes"]), int(s["calibration_episodes"])
    require(episodes > 0 and calibration >= 4 and int(s.get("max_episode_steps", 200)) == 200, "Invalid frozen episode bounds")
    combinations = sum(mechanism != "motor_gain" or rank == 1 for mechanism in MECHANISMS for rank in ranks)
    points = combinations * len(strengths) * 2
    kl_calls = points + combinations * sum(x > 0 for x in strengths) * int(s.get("kl_bisections", 10))
    evaluations = points * 8 + 4
    return {"grid_points": points, "mechanism_rank_combinations": combinations,
            "evaluations": evaluations, "calibration_evaluations": 1,
            "evaluation_steps": evaluations * 200, "calibration_steps": 200,
            "kl_calls": kl_calls, "kl_steps": kl_calls * 200, "restoration_checks": points,
            "trial_rows": points * 8 * episodes,
            "evaluation_batch": episodes, "calibration_batch": calibration, "restoration_batch": 4,
            "all_episodes_charged_at_200_steps": True, "unavailable_episode_discount": False}


def stress_variants(config):
    """Cover each operator shape; KL search coefficients range from zero to 8."""
    s = config["agents"]
    result = []
    for mechanism in MECHANISMS:
        for rank in s["ranks"]:
            if mechanism == "motor_gain" and rank != 1:
                continue
            for arm in ("parameter_norm", "policy_kl"):
                amplitudes = sorted(set(float(x) for x in s["strengths"]) | ({8.} if arm == "policy_kl" else set()))
                for amplitude in amplitudes:
                    for random_restoration in (False, True):
                        result.append({"mechanism": mechanism, "rank": rank, "matching_arm": arm,
                                       "coefficient_norm": amplitude, "random_restoration": random_restoration})
    return result


def _binding(path):
    path = Path(path).resolve()
    require(path.is_file() and not path.is_symlink(), "Envelope input must be a regular file")
    before = path.stat()
    sha = digest(path)
    after = path.stat()
    require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns), "Envelope input changed during hashing")
    return {"path": str(path), "sha256": sha, "bytes": after.st_size}


def _unchanged(binding):
    require(_binding(binding["path"]) == binding, "Envelope input changed")


def _kernel_hashes(agents):
    return {name: hashlib.sha256(inspect.getsource(getattr(agents, name)).encode()).hexdigest() for name in KERNELS}


def _expected_input_paths(config, task, architecture, seeds):
    """Exact successful original 24-policy roster, before any timing retry."""
    from horsehair import agents
    directory = agents._directory(Path(config["paths"]["root"]), config["agents"]).resolve()
    paths = [directory / "frozen_protocol.json"]
    for seed in seeds:
        name = agents._cell_name(task, architecture, seed)
        training = directory / "benchmark-training" / name
        original = directory / "benchmark-cpu-evaluation" / name
        paths.extend(training / name for name in ("training_benchmark.json", "checkpoint.pt", "summary.json",
                                                  "training_metrics.json", "calibrated_directions.json"))
        paths.extend(original / name for name in ("cpu_evaluation_benchmark.json", "agent_trials.parquet",
                                                  "restoration_checks.json", "evaluation_summary.json"))
    return {str(path.resolve()) for path in paths}


def _validate_input_roster(receipt, config):
    expected = _expected_input_paths(config, receipt["task"], receipt["architecture"], receipt["seeds"])
    bindings = receipt.get("inputs", [])
    require(bindings and len(bindings) == len(expected) and {value.get("path") for value in bindings} == expected,
            "Exact freeze/training/checkpoint/original/results input roster is required")
    require(all(isinstance(value.get("sha256"), str) and len(value["sha256"]) == 64
                and set(value["sha256"]).issubset(set("0123456789abcdef"))
                and isinstance(value.get("bytes"), int) and value["bytes"] > 0 for value in bindings), "Invalid input binding")
    for report in receipt["reports"]:
        original = report.get("original_receipt")
        require(original in bindings and Path(original["path"]).name == "cpu_evaluation_benchmark.json"
                and Path(original["path"]).parent.name.endswith("_" + str(report["seed"])), "Original receipt is not hash-bound as an input")


def _validate_frozen_protocol(agents, config, protocol):
    """The cost envelope only covers the existing synthetic primary freeze."""
    import numpy as np
    s = config["agents"]
    require("biological_directions" in protocol and protocol["biological_directions"] is None,
            "Synthetic-only cost envelope cannot cover biological rule types")
    require(protocol.get("implementation_sha256") == agents._implementation_sha256()
            and protocol.get("full_config_sha256") == canonical_hash(config)
            and protocol.get("config_sha256") == agents._digest(s)
            and protocol.get("H4_inference_specification") == agents.h4_specification(config)
            and protocol.get("final_seeds") == s["final_seeds"]
            and protocol.get("development_seeds") == s["development_seeds"]
            and protocol.get("ranks") == s["ranks"] and protocol.get("strengths") == s["strengths"]
            and protocol.get("learning_rate") in s["lr_grid"], "Frozen protocol source/configuration/seed/grid roster changed")
    expected = {f"{mechanism}:{rank}" for mechanism in MECHANISMS for rank in s["ranks"]
                if mechanism != "motor_gain" or rank == 1}
    require(set(protocol.get("directions", {})) == expected, "Frozen directions omit an operator shape")
    for key, value in protocol["directions"].items():
        theta = np.asarray(value, dtype=float)
        require(theta.shape == (int(key.split(":")[1]),) and np.isfinite(theta).all(), "Invalid frozen direction")


def _maximum_block_seconds(callback, steps=MEASUREMENT_STEPS):
    """Slowest of four blocks, including the first/cold-cache block."""
    require(steps == 200, "Cost fixtures must measure exactly 200 operator steps")
    blocks = []
    for _ in range(4):
        started = time.perf_counter()
        for _ in range(50):
            callback()
        blocks.append((time.perf_counter() - started) / 50)
    require(all(math.isfinite(x) and x > 0 for x in blocks), "Invalid measured operator duration")
    return {"steps": steps, "blocks": 4, "seconds_per_step": max(blocks), "block_seconds_per_step": blocks}


def _measure_mode(agents, policy, task, variant, batches, *, measurement_steps=200):
    """Fixed finite states isolate operator cost, without inventing trajectories."""
    import numpy as np
    torch = agents._torch()
    theta = agents._normalize(np.random.default_rng(419 + variant["rank"]).normal(size=variant["rank"]))
    perturbation = agents.Perturbation(variant["mechanism"], variant["rank"], theta * variant["coefficient_norm"],
                                      random_restoration=variant["random_restoration"])
    started = time.perf_counter()
    active, active_perturbation = agents._perturbed_policy(policy, perturbation)
    before = agents.state_hash(policy)
    # evaluate checks the base weight hash both before and after its loop.
    require(before == agents.state_hash(policy), "Policy changed during setup")
    setup_seconds = time.perf_counter() - started
    records = []
    for batch in sorted(set(batches)):
        obs = torch.as_tensor(np.stack([agents.Environment(task, 91_000_000 + i, "heldout").observe() for i in range(batch)]))
        state = policy.initial_state(batch)
        state[:] = torch.sin(torch.arange(state.numel()).reshape_as(state)) * .1
        streams = [np.random.default_rng(92_000_000 + i) for i in range(batch)]
        def evaluation_step():
            # States are intentionally fixed technical inputs at every call.
            # No artificial endpoint or replacement for a failed episode exists.
            logits, _, changed = active(obs, state, active_perturbation)
            require(bool(torch.isfinite(logits).all() and torch.isfinite(changed).all()), "Nonfinite fixed operator fixture")
            agents._sample_action(logits, streams)
            scratch = state.clone()
            scratch[:] = changed
        def kl_step():
            clean_logits = policy(obs, state)[0]
            changed_logits, _, changed = active(obs, state, active_perturbation)
            require(bool(torch.isfinite(clean_logits).all() and torch.isfinite(changed_logits).all() and torch.isfinite(changed).all()), "Nonfinite fixed KL fixture")
            log_p, log_q = torch.log_softmax(clean_logits.double(), -1), torch.log_softmax(changed_logits.double(), -1)
            divergence = (log_p.exp() * (log_p - log_q)).sum(-1)
            require(bool(torch.isfinite(divergence).all()), "Nonfinite fixed KL arithmetic")
            scratch = state.clone()
            scratch[:] = changed
            divergence.cpu().tolist()
        with torch.no_grad():
            records.append({"batch": batch, "evaluation": _maximum_block_seconds(evaluation_step, measurement_steps),
                            "clean_kl": _maximum_block_seconds(kl_step, measurement_steps)})
    restoration_seconds = []
    for _ in range(MEASUREMENT_RESTORATIONS):
        started = time.perf_counter()
        check = agents._restoration_check(policy, task, list(range(93_000_000, 93_000_004)), perturbation)
        require(check["base_weights_unchanged"] and check["restoration_max_probability_error"] <= 1e-7, "Exact restoration failed")
        restoration_seconds.append(time.perf_counter() - started)
    require(before == agents.state_hash(policy), "Cost fixture changed original policy weights")
    return {**variant, "setup_seconds": setup_seconds, "batch_measurements": records,
            "restoration_seconds": max(restoration_seconds), "original_weights_unchanged": True,
            "fixture_scope": "fixed finite synthetic operator inputs; no simulated episode outcomes"}


def _environment_measurements(agents, task, batch):
    """All task/probe operations at full batch; resets are conservatively charged."""
    result = []
    for probe in (None, *agents.PROBES):
        started = time.perf_counter()
        environments = [agents.Environment(task, 94_000_000 + i, "heldout", probe) for i in range(batch)]
        setup = time.perf_counter() - started
        def step():
            for index, environment in enumerate(environments):
                if environment.done:
                    environments[index] = environment = agents.Environment(task, 94_000_000 + index, "heldout", probe)
                environment.step(4)
        result.append({"probe": probe or "main", "batch": batch, "setup_seconds": setup,
                       "step": _maximum_block_seconds(step), "terminal_resets_charged": True})
    return result


def _production_skeleton_measurements(agents, policy, task, batches):
    """Time unchanged production indexing/capture on forced-length fixtures.

    The environment wrapper resets terminal source environments and only ends
    the technical call at step 200. Its endpoint is never saved or interpreted.
    This changes the fixture, not a primary episode, policy or source file.
    """
    original_environment = agents.Environment
    before = agents.state_hash(policy)
    class FullLengthCostEnvironment:
        def __init__(self, task, seed, split, probe):
            self.task, self.seed, self.split, self.probe = task, seed, split, probe
            self.actual = original_environment(task, seed, split, probe)
            self.t, self.serial, self.done = 0, 0, False
        def observe(self):
            return self.actual.observe()
        def step(self, action):
            if self.actual.done:
                self.serial += 1
                self.actual = original_environment(self.task, self.seed + self.serial * 1000000, self.split, self.probe)
            observation, reward, _, info = self.actual.step(action)
            self.t += 1
            self.done = self.t == 200
            return observation, reward, self.done, {**info, "steps": self.t, "truncated": self.done}
    records = []
    try:
        agents.Environment = FullLengthCostEnvironment
        for batch in sorted(set(batches)):
            for probe in (None, *agents.PROBES):
                started = time.perf_counter()
                rows, meta = agents.evaluate(policy, task, list(range(96_000_000, 96_000_000 + batch)),
                                             probe=probe, cue_removal=True, capture=True)
                evaluation_seconds = time.perf_counter() - started
                require(len(rows) == batch and all(row["steps"] == 200 for row in rows)
                        and len(meta["sequence"]) == 200 and meta["numerically_available"], "Incomplete production cost skeleton")
                started = time.perf_counter()
                agents._clean_kl(policy, agents.Perturbation("input_gain", 1, [0.]), meta["sequence"])
                kl_seconds = time.perf_counter() - started
                records.append({"probe": probe or "main", "batch": batch, "steps": 200,
                                "evaluation_seconds_per_step": evaluation_seconds / 200,
                                "kl_seconds_per_step": kl_seconds / 200,
                                "capture": True, "cue_removal": True,
                                "fixture_scope": "forced-length synthetic environment; no endpoint saved"})
    finally:
        agents.Environment = original_environment
    require(agents.state_hash(policy) == before, "Production cost skeleton changed policy weights")
    return records


def _io_measurement(agents, original, destination, rows):
    """Production row count plus incompressible technical payload, no outcomes."""
    import numpy as np
    import pandas as pd
    started = time.perf_counter()
    original = pd.read_parquet(original)
    source_rows = original.to_dict("records")
    # Python dictionary/list assembly is charged alongside DataFrame and file
    # construction, as in the production experiment's persisted row path.
    frame = pd.DataFrame([dict(source_rows[index % len(source_rows)]) for index in range(rows)])
    # Added deterministic bytes prevent repeated source rows from creating an
    # artificially compressible cost fixture. It is explicitly synthetic data.
    generator = np.random.default_rng(95_000_000)
    frame["technical_cost_fixture_payload"] = [
        value.tobytes().hex() for value in generator.integers(0, 256, size=(rows, 128), dtype=np.uint8)]
    path = destination / "synthetic-io-fixture.parquet"
    frame.to_parquet(path, index=False)
    # Also charge a maximal numerical diagnostic record for every planned row.
    diagnostics = [{"technical_cost_fixture": True, "episode_index": index, "stage": "evaluation",
                    "mechanism": "synthetic_cost_fixture", "rank": 8, "strength": .8,
                    "matching_arm": "policy_kl", "restoration_control": "random_subspace",
                    "reason": "synthetic storage envelope; not an observed numerical failure",
                    "technical_padding": "X" * 512} for index in range(rows)]
    agents._json(destination / "synthetic-diagnostics-fixture.json", {"scientific_result": False, "diagnostics": diagnostics})
    elapsed = time.perf_counter() - started
    return {"rows": rows, "seconds": elapsed, "bytes": path.stat().st_size +
            (destination / "synthetic-diagnostics-fixture.json").stat().st_size,
            "files": 2, "synthetic": True, "scientific_result": False}


def _measure_cell(primary, framework, root, case, task_index):
    from horsehair import agents
    primary, framework, root, case = map(lambda x: Path(x).resolve(), (primary, framework, root, case))
    require(Path(agents.__file__).resolve() == primary / "src/horsehair/agents.py", "Wrong numerical package import")
    primary_identity = read_json(primary / "release.json")
    config = read_json(primary / "configs/study.json")
    require(source_identity(primary) == primary_identity["source_identity"] and
            primary_identity["source_identity"]["sha256"] == PRIMARY_SOURCE_SHA256 and
            canonical_hash(config) == FULL_CONFIG_SHA256, "Frozen numerical source/configuration changed")
    cells = agents.benchmark_cells(config)
    require(task_index in range(len(cells)), "Invalid full-grid cell index")
    task, architecture, seeds = cells[task_index]
    directory = agents._directory(root, config["agents"])
    frozen = _binding(directory / "frozen_protocol.json")
    frozen_record = read_json(frozen["path"])
    _validate_frozen_protocol(agents, config, frozen_record)
    destination = case / f"cell-{task_index:02d}"
    destination.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    torch = agents._torch()
    cpus = int(os.environ["SLURM_CPUS_PER_TASK"])
    require(cpus == 4 and os.environ.get("HHW_GPU_COUNT", "0") == "0", "Envelope requires exactly four native CPUs and no GPU")
    torch.set_num_threads(cpus)
    bounds, variants = maximum_work(config), stress_variants(config)
    reports, inputs = [], [frozen]
    for seed in seeds:
        seed_started = time.perf_counter()
        training_path = directory / "benchmark-training" / agents._cell_name(task, architecture, seed) / "training_benchmark.json"
        original_path = directory / "benchmark-cpu-evaluation" / agents._cell_name(task, architecture, seed) / "cpu_evaluation_benchmark.json"
        training = agents._benchmark_checked(config, training_path, "training")
        original = agents._benchmark_checked(config, original_path, "cpu-evaluation")
        grid = agents._validate_benchmark_grid(config, original_path.parent, original)
        require((original["task"], original["architecture"], original["seed"]) == (task, architecture, seed), "Benchmark cell changed")
        bindings = [_binding(path) for path in (training_path, original_path, Path(training["checkpoint_path"]))]
        bindings += [_binding(path) for path in (*training["source_result_paths"], *original["source_result_paths"])]
        policy, checkpoint = agents._load_policy(Path(training["checkpoint_path"]), "cpu", config["agents"],
            seed=seed, architecture=architecture, task=task, learning_rate=float(config["agents"]["lr_grid"][0]))
        require(checkpoint["steps"] == agents.benchmark_specification(config)["training_steps"] and
                agents.state_hash(policy) == training["weight_sha256"], "Benchmark policy checkpoint changed")
        policy.requires_grad_(False)
        policy.eval()
        preparation_seconds = time.perf_counter() - seed_started
        seed_destination = destination / str(seed)
        seed_destination.mkdir()
        measurements = []
        for index, variant in enumerate(variants):
            measurements.append(_measure_mode(agents, policy, task, variant,
                                [bounds["evaluation_batch"], bounds["calibration_batch"]]))
            if index % 50 == 0:
                print(json.dumps({"technical_cost_progress": True, "cell": task_index, "seed": seed,
                                  "measured_variants": index + 1, "total_variants": len(variants)}), flush=True)
        environment = _environment_measurements(agents, task, bounds["evaluation_batch"])
        skeleton = _production_skeleton_measurements(agents, policy, task,
                    [bounds["evaluation_batch"], bounds["calibration_batch"]])
        io = _io_measurement(agents, original_path.parent / "agent_trials.parquet", seed_destination, bounds["trial_rows"])
        for value in bindings:
            _unchanged(value)
        inputs.extend(bindings)
        reports.append({"seed": seed, "original_numerical_grid": grid, "original_receipt": bindings[1],
                        "measurements": measurements, "environment": environment, "io": io,
                        "production_skeleton": skeleton,
                        "policy_load_and_evidence_seconds": preparation_seconds})
    for value in inputs:
        _unchanged(value)
    inputs = list({value["path"]: value for value in inputs}.values())
    require(source_identity(primary) == primary_identity["source_identity"], "Frozen source changed during measurement")
    result = {"schema_version": 1, "kind": "agent-cpu-cost-envelope-cell", "status": "measured",
              "scientific_result": False, "numerical_failures_repaired": False,
              "source_sha256": source_identity(framework)["sha256"], "primary_scientific_source_sha256": PRIMARY_SOURCE_SHA256,
              "full_config_sha256": FULL_CONFIG_SHA256, "kernel_function_sha256": _kernel_hashes(agents),
              "implementation_sha256": agents._implementation_sha256(), "job_id": native_job_id(),
              "allocated_cpus": cpus, "gpus": 0, "evaluation_device": "cpu", "task_index": task_index,
              "task": task, "architecture": architecture, "seeds": list(seeds), "maximum_work": bounds,
              "frozen_protocol_binding": frozen, "frozen_protocol_record": frozen_record,
              "variant_roster": variants, "measurement_steps": 200, "reports": reports,
              "measured_seconds": time.perf_counter() - started, "inputs": inputs,
              "claim_scope": "separate synthetic operation cost evidence; original unavailable outcomes remain unavailable"}
    _validate_input_roster(result, config)
    atomic_json(destination / "receipt.json", result)
    return result


def forecast_cell(receipt, config, native, *, safety=1.5, framework_source_sha256=None, kernel_hashes=None):
    """Conservative empirical forecast; no scientific/numerical admission flag."""
    require(math.isfinite(safety) and safety >= 1.5, "Safety factor must be at least 1.5")
    require(canonical_hash(config) == FULL_CONFIG_SHA256, "Full immutable primary configuration is required")
    if framework_source_sha256 is None:
        framework_source_sha256 = source_identity(Path(__file__).resolve().parents[2])["sha256"]
    from horsehair import agents
    if kernel_hashes is None:
        kernel_hashes = _kernel_hashes(agents)
    bounds = maximum_work(config)
    require(receipt.get("kind") == "agent-cpu-cost-envelope-cell" and receipt.get("status") == "measured"
            and receipt.get("maximum_work") == bounds and receipt.get("measurement_steps") == 200
            and receipt.get("primary_scientific_source_sha256") == PRIMARY_SOURCE_SHA256
            and receipt.get("full_config_sha256") == canonical_hash(config)
            and receipt.get("source_sha256") == framework_source_sha256
            and receipt.get("kernel_function_sha256") == kernel_hashes, "Envelope source/configuration/counts changed")
    require(native.get("job_id") == receipt.get("job_id") and native.get("state") == "COMPLETED"
            and native.get("exit_code") == "0:0" and native.get("cpus") == receipt.get("allocated_cpus") == 4
            and native.get("gpus") == 0 and native.get("elapsed_seconds", 0) > 0
            and math.isfinite(native["elapsed_seconds"]) and native.get("peak_rss_bytes", 0) > 0
            and math.isfinite(native["peak_rss_bytes"]), "Completed four-CPU native accounting is required")
    cells = agents.benchmark_cells(config)
    require(receipt.get("task_index") in range(len(cells)), "Unknown cell index")
    task, architecture, seeds = cells[receipt["task_index"]]
    require((receipt.get("task"), receipt.get("architecture"), receipt.get("seeds")) ==
            (task, architecture, list(seeds)), "Changed task/architecture/seed roster")
    _validate_frozen_protocol(agents, config, receipt.get("frozen_protocol_record", {}))
    require(receipt.get("frozen_protocol_binding") in receipt.get("inputs", []), "Frozen protocol hash is not bound as an input")
    _validate_input_roster(receipt, config)
    variants = stress_variants(config)
    require(receipt.get("variant_roster") == variants and len(receipt.get("reports", [])) == len(receipt.get("seeds", [])) == 2,
            "Incomplete cell/seed/operator coverage")
    require([row["seed"] for row in receipt["reports"]] == receipt["seeds"], "Changed seed roster")
    def positive(value):
        require(isinstance(value, (int, float)) and not isinstance(value, bool)
                and math.isfinite(value) and value > 0, "Invalid positive measured cost")
        return value
    forecasts = []
    for report in receipt["reports"]:
        grid = report["original_numerical_grid"]
        require(all(grid.get(name) is True for name in
                    ("complete_mechanism_grid", "full_calibration_algorithm", "restoration_qualified")),
                "Original complete numerical benchmark grid is required")
        require(all(isinstance(grid.get(name), int) and grid[name] >= 0 for name in
                    ("unavailable_episode_rows", "unavailable_calibration_rows")), "Invalid original availability counts")
        measurements = report["measurements"]
        require([{key: row[key] for key in variants[0]} for row in measurements] == variants, "Incomplete operator grid")
        evaluation_rates, kl_rates = [], []
        for row in measurements:
            require(row.get("original_weights_unchanged") is True, "Changed base policy weights")
            batches = {value["batch"]: value for value in row["batch_measurements"]}
            require(set(batches) == {bounds["evaluation_batch"], bounds["calibration_batch"]}, "Missing production batch shape")
            for value in batches.values():
                for operation in ("evaluation", "clean_kl"):
                    measurement = value[operation]
                    require(measurement.get("steps") == 200 and measurement.get("blocks") == 4 and
                            len(measurement.get("block_seconds_per_step", [])) == 4 and
                            all(positive(x) for x in measurement["block_seconds_per_step"]) and
                            measurement["seconds_per_step"] == max(measurement["block_seconds_per_step"]), "Invalid fixed-step measurement")
            evaluation_rates.append(batches[bounds["evaluation_batch"]]["evaluation"]["seconds_per_step"])
            kl_rates.append(batches[bounds["calibration_batch"]]["clean_kl"]["seconds_per_step"])
        environment = report["environment"]
        require({value["probe"] for value in environment} == {"main", "delayed_cue", "goal_detection", "motor_execution"}
                and len(environment) == 4 and all(value["batch"] == bounds["evaluation_batch"] and
                value["step"]["steps"] == 200 for value in environment), "Incomplete task/probe environment coverage")
        environment_rate = max(positive(value["step"]["seconds_per_step"]) for value in environment)
        setup = max(positive(row["setup_seconds"]) for row in measurements) + max(positive(value["setup_seconds"]) for value in environment)
        io = report["io"]
        require(io["synthetic"] is True and io["rows"] == bounds["trial_rows"], "Incomplete production I/O fixture")
        skeleton = report["production_skeleton"]
        require({(value["batch"], value["probe"]) for value in skeleton} == {
                    (batch, probe) for batch in {bounds["evaluation_batch"], bounds["calibration_batch"]}
                    for probe in ("main", "delayed_cue", "goal_detection", "motor_execution")}
                and len(skeleton) == 4 * len({bounds["evaluation_batch"], bounds["calibration_batch"]})
                and all(value["steps"] == 200 and value["capture"] is True and value["cue_removal"] is True
                        for value in skeleton), "Incomplete full-length production skeleton coverage")
        evaluation_overhead = max(positive(value["evaluation_seconds_per_step"]) for value in skeleton)
        kl_overhead = max(positive(value["kl_seconds_per_step"]) for value in skeleton)
        # The full production skeleton and the operator maxima intentionally
        # overlap. Charging both avoids subtracting a poorly identified overhead.
        raw = ((bounds["evaluation_steps"] + 2 * bounds["calibration_steps"]) * (max(evaluation_rates) + environment_rate + evaluation_overhead)
               + bounds["kl_steps"] * (max(kl_rates) + kl_overhead)
               + bounds["restoration_checks"] * max(positive(row["restoration_seconds"]) for row in measurements)
               + (bounds["evaluations"] + 1 + bounds["kl_calls"]) * setup + positive(io["seconds"])
               + positive(report["policy_load_and_evidence_seconds"]))
        require(math.isfinite(raw) and raw > 0, "Invalid cost forecast")
        forecasts.append(raw)
    overhead = max(0., native["elapsed_seconds"] - positive(receipt["measured_seconds"]))
    seconds = safety * (max(forecasts) + overhead)
    require(math.isfinite(seconds) and seconds > 0, "Nonfinite aggregate forecast")
    return {"seconds_per_final_policy": seconds, "cpu_core_hours_per_final_policy": seconds * 4 / 3600,
            "peak_rss_bytes": math.ceil(native["peak_rss_bytes"] * 1.3), "safety_factor": safety,
            "native_fixed_overhead_seconds": overhead, "all_episode_steps_charged": 200,
            "additional_evaluation_bytes_per_policy": math.ceil(safety * max(positive(report["io"]["bytes"]) for report in receipt["reports"])),
            "additional_evaluation_files_per_policy": math.ceil(safety * max(positive(report["io"]["files"]) for report in receipt["reports"])) + 8,
            "storage_scope": "CPU evaluation artifacts; final training checkpoints and GPU work require original training evidence and separate admission",
            "unavailable_episode_discount": False, "scientific_result": False, "admission_status": "not_admitted",
            "estimate_scope": "empirical conservative CPU operation envelope; not a mathematical duration guarantee"}


def aggregate(case, config, native, *, safety=1.5, framework_source_sha256=None, kernel_hashes=None):
    """Require all twelve cells and both fixed seeds; never write admission."""
    from horsehair import agents
    case = Path(case).resolve()
    require(canonical_hash(config) == FULL_CONFIG_SHA256, "Full immutable primary configuration is required")
    cells = agents.benchmark_cells(config)
    require(len(cells) == 12, "Complete primary twelve-cell roster is required")
    forecasts, receipts, inputs, unavailable, jobs, frozen = [], [], [], 0, set(), None
    for index, (task, architecture, seeds) in enumerate(cells):
        path = case / f"cell-{index:02d}" / "receipt.json"
        binding = _binding(path)
        receipt = read_json(path)
        require(receipt.get("task_index") == index and receipt.get("job_id") not in jobs, "Duplicate or reordered native cell")
        jobs.add(receipt["job_id"])
        forecast = forecast_cell(receipt, config, native.get(receipt["job_id"], {}), safety=safety,
                                 framework_source_sha256=framework_source_sha256, kernel_hashes=kernel_hashes)
        frozen_binding = receipt["frozen_protocol_binding"]
        require(frozen is None or frozen == frozen_binding, "Cells refer to different frozen protocols")
        frozen = frozen_binding
        _unchanged(frozen)
        require(read_json(frozen["path"]) == receipt["frozen_protocol_record"], "Frozen protocol record differs from hash-bound input")
        for value in receipt["inputs"]:
            _unchanged(value)
        inputs.extend(receipt["inputs"])
        unavailable += sum(report["original_numerical_grid"]["unavailable_episode_rows"] for report in receipt["reports"])
        forecasts.append({"task": task, "architecture": architecture, **forecast})
        receipts.append(binding)
    final_seeds = len(config["agents"]["final_seeds"])
    seconds = sum(value["seconds_per_final_policy"] * final_seeds for value in forecasts)
    return {"kind": "agent-cpu-cost-envelope-aggregate", "status": "measured", "scientific_result": False,
            "primary_scientific_source_sha256": PRIMARY_SOURCE_SHA256, "full_config_sha256": canonical_hash(config),
            "source_sha256": framework_source_sha256 or source_identity(Path(__file__).resolve().parents[2])["sha256"],
            "task_architecture_cells": 12, "measured_policies": 24, "final_policies": 12 * final_seeds,
            "original_unavailable_episode_rows_preserved": unavailable, "numerical_failures_repaired": False,
            "all_episode_steps_charged": 200, "unavailable_episode_discount": False,
            "forecast_cpu_core_hours": seconds * 4 / 3600, "maximum_final_policy_seconds": max(
                value["seconds_per_final_policy"] for value in forecasts), "safety_factor": safety,
            "additional_evaluation_bytes": sum(value["additional_evaluation_bytes_per_policy"] * final_seeds for value in forecasts),
            "additional_evaluation_files": sum(value["additional_evaluation_files_per_policy"] * final_seeds for value in forecasts),
            "uncovered_cost_families": ["final GPU training runtime/allocation and new checkpoints (original training evidence required)",
                "primary final H4 analysis runtime/storage"],
            "per_cell": forecasts, "receipts": receipts, "inputs": inputs, "admission_status": "not_admitted",
            "frozen_protocol_binding": frozen,
            "required_next_gate": "separate prospective resource-admission amendment plus full native accounting and quota/budget checks"}


def _primary_worker_environment(primary, framework, root):
    """Keep qualified Alliance vendor packages while isolating horsehair."""
    from horsehair.rna_recovery import _primary_environment
    environment = _primary_environment({"primary_release": str(primary),
        "maintenance_release": str(framework), "root": str(root)})
    for key in ("PYTHONHOME", "PYTHONSTARTUP", "PYTHONINSPECT"):
        environment.pop(key, None)
    return environment


PRIMARY_COST_BOOTSTRAP = """
import importlib.util,sys
from pathlib import Path
primary,framework,root,case,index,primary_sha,config_sha,framework_sha,module_sha=sys.argv[1:]
primary,framework=Path(primary).resolve(),Path(framework).resolve()
assert sys.flags.no_user_site==1
sys.path.insert(0,str(primary/'src'))
import horsehair
from horsehair import core
assert Path(horsehair.__file__).resolve()==primary/'src/horsehair/__init__.py'
assert Path(core.__file__).resolve()==primary/'src/horsehair/core.py'
science=core.read_json(primary/'release.json');identity=core.read_json(framework/'release.json')
assert core.source_identity(primary)==science['source_identity'] and science['source_identity']['sha256']==primary_sha
assert core.canonical_hash(core.read_json(primary/'configs/study.json'))==config_sha
assert core.source_identity(framework)==identity['source_identity'] and identity['source_identity']['sha256']==framework_sha
module_path=framework/'src/horsehair/agent_cost_envelope.py'
assert core.digest(module_path)==module_sha==identity['source_identity']['files']['src/horsehair/agent_cost_envelope.py']
spec=importlib.util.spec_from_file_location('qualified_agent_cost_envelope',module_path)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
module._measure_cell(primary,framework,root,case,int(index))
assert core.source_identity(primary)==science['source_identity']
assert core.source_identity(framework)==identity['source_identity']
"""


def run(primary, root, case, task_index):
    """Launch verified primary code with Alliance dependencies and no user site."""
    from horsehair.budget import resource_lease
    framework = Path(__file__).resolve().parents[2]
    primary, root, case = map(lambda x: Path(x).resolve(), (primary, root, case))
    identity = read_json(framework / "release.json")
    science = read_json(primary / "release.json")
    config = read_json(primary / "configs/study.json")
    require(os.environ.get("SLURM_CPUS_PER_TASK") == "4" and os.environ.get("HHW_GPU_COUNT", "0") == "0",
            "Envelope requires exactly four native CPUs and no GPU")
    require(source_identity(framework) == identity["source_identity"], "Qualified framework source changed")
    require(source_identity(primary) == science["source_identity"]
            and science["source_identity"]["sha256"] == PRIMARY_SOURCE_SHA256,
            "Frozen primary source changed")
    require(canonical_hash(config) == FULL_CONFIG_SHA256 and case.is_relative_to(root / "maintenance")
            and Path(config["paths"]["root"]).resolve() == root, "Wrong study/configuration/output namespace")
    require(task_index in range(12), "Exactly twelve task/architecture cells are required")
    environment = _primary_worker_environment(primary, framework, root)
    with resource_lease(root, config, cpus=4, gpus=0, max_hours=8, job_id=native_job_id()):
        subprocess.run([sys.executable, "-s", "-c", PRIMARY_COST_BOOTSTRAP, str(primary), str(framework), str(root),
                       str(case), str(task_index), PRIMARY_SOURCE_SHA256, FULL_CONFIG_SHA256,
                       identity["source_identity"]["sha256"], identity["source_identity"]["files"]["src/horsehair/agent_cost_envelope.py"]],
                       cwd=primary, env=environment, check=True, timeout=7.5 * 3600)
    require(source_identity(framework) == identity["source_identity"], "Framework changed during measurement")
    return read_json(case / f"cell-{task_index:02d}" / "receipt.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--primary-release", type=Path, default=PRIMARY_RELEASE)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--case", type=Path, required=True)
    parser.add_argument("--task-index", type=int, required=True)
    arguments = parser.parse_args()
    result = run(arguments.primary_release, arguments.root, arguments.case, arguments.task_index)
    print(json.dumps({"cost_envelope_receipt": str(arguments.case / f"cell-{arguments.task_index:02d}" / "receipt.json"),
                      "status": result["status"], "scientific_result": False}), flush=True)
