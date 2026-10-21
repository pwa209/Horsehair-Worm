"""Prospective float64 evaluation amendment; original training stays frozen.

This is a new numerical experiment, not a replacement for historical null rows.
The parent launcher owns Slurm qualification, admission, leases and hard process
deadlines. Chunks commit only whole conditions and retain interrupted attempts.

For a separate frozen package, launch a fresh Python process with primary/src
first on sys.path, import and verify primary horsehair/core, then load this
qualified file with spec_from_file_location('horsehair._agent_execution_v2',
framework_module_path). Relative scientific imports then remain in primary;
this file's own hash is recorded independently in every plan. Never replace
sys.modules entries in an already imported scientific interpreter.
"""
from __future__ import annotations

import argparse
import copy
import json
import math
import os
from pathlib import Path
import time
import uuid
from typing import Any

import numpy as np

from . import agents
from .core import atomic_json, canonical_hash, digest, exclusive_file_lock, native_job_id, read_json


AMENDMENT = "evaluation-only-float64-v1"
DIFFERENCE = ("Original float32 checkpoint weights and frozen float32 perturbation bases/coefficients "
              "are exactly promoted to float64. Recurrent arithmetic, observations, logits and "
              "action probabilities are then evaluated in float64. Rounding, calibration results "
              "and sampled trajectories may differ. Training, endpoints, seeds and algorithms are unchanged.")


def _require(value, message):
    if not value:
        raise ValueError(message)


def _settings(config):
    result = dict(config["agents"])
    if "output_dir" in config and "output_dir" not in result:
        result["output_dir"] = config["output_dir"]
    return result


def _source_check(expected):
    _require(digest(Path(agents.__file__)) == expected, "Original agents module differs from its pinned source")


def final_cells(config):
    """Pure dispatch roster; this never submits or admits a job."""
    return [{"index": i, "task": task, "architecture": architecture, "seed": seed}
            for i, (task, architecture, seed, _) in enumerate(agents._cells(_settings(config)))]


def _conditions(settings, protocol):
    from .agent_analysis import _conditions as original
    return [dict(zip(("rule_type", "mechanism", "rank", "matching_arm", "strength"), row))
            for row in original(settings, protocol)]


def _amended_protocol(original, evaluation, canary, benchmark_directions=False):
    result = dict(original, evaluation_amendment=AMENDMENT, evaluation_numerical_difference=DIFFERENCE,
        evaluation_dtype="float64", evaluation_source_module_sha256=digest(Path(__file__)),
        evaluation_settings_sha256=canonical_hash(evaluation), technical_precision_canary=canary,
        direction_source="original_predeclared_technical_benchmark" if benchmark_directions else "original_frozen_development_protocol")
    if benchmark_directions:
        result["directions"] = {f"{m}:{r}": agents._normalize(np.random.default_rng(419+r).normal(size=r)).tolist()
            for m in agents.MECHANISMS for r in evaluation["ranks"] if m != "motor_gain" or r == 1}
    return result


def prepare_plan(config, protocol, checkpoint, output, *, checkpoint_sha256,
                 primary_agents_sha256, task, architecture, seed, canary=None, benchmark_directions=False):
    """Pin one existing checkpoint; canary overrides never qualify as final H4."""
    _source_check(primary_agents_sha256)
    settings = _settings(config)
    _require(type(benchmark_directions) is bool and (not benchmark_directions or canary is not None),
             "Historical benchmark directions are restricted to a technical canary")
    _require(protocol.get("config_sha256") == agents._digest(settings)
             and protocol.get("full_config_sha256") == canonical_hash(config)
             and protocol.get("implementation_sha256") == primary_agents_sha256
             and protocol.get("H4_inference_specification") == agents.h4_specification(config),
             "Original frozen protocol/configuration binding changed")
    _require((task, architecture) in {(r[0], r[1]) for r in agents._cells(settings)}, "Unknown final policy cell")
    _require(type(seed) is int and seed in (settings["pilot_seeds"] if canary is not None else settings["final_seeds"]),
             "Canary/final training seed roster changed")
    checkpoint = Path(checkpoint).resolve()
    _require(checkpoint.is_file() and digest(checkpoint) == checkpoint_sha256, "Original checkpoint file hash changed")
    base, saved = agents._load_policy(checkpoint, "cpu", settings, seed=seed, architecture=architecture,
        task=task, learning_rate=float(saved_rate(settings, protocol, canary)), complete=canary is None)
    _require(all(p.dtype == agents._torch().float32 and agents._torch().isfinite(p).all() for p in base.parameters()),
             "The amendment requires finite original float32 weights")
    evaluation = dict(settings)
    if canary is not None:
        allowed = {"ranks", "strengths", "evaluation_episodes", "calibration_episodes", "kl_bisections"}
        _require(set(canary) <= allowed and bool(canary), "Unsupported technical canary override")
        evaluation.update(canary)
        _require(set(evaluation["ranks"]) <= set(settings["ranks"])
                 and set(evaluation["strengths"]) <= set(settings["strengths"])
                 and len(set(evaluation["ranks"])) == len(evaluation["ranks"])
                 and len(set(evaluation["strengths"])) == len(evaluation["strengths"]), "Canary changed or duplicated frozen ranks/strengths")
        for key in ("evaluation_episodes", "calibration_episodes", "kl_bisections"):
            value = evaluation.get(key, 10)
            _require(type(value) is int and 1 <= value <= settings.get(key, 10), "Invalid canary budget")
    _require(evaluation["ranks"] and evaluation["strengths"], "Empty evaluation roster")
    output = Path(output).resolve()
    _require(not output.exists(), "A prospective evaluation plan cannot overwrite an earlier case")
    output.mkdir(parents=True)
    plan = {"schema_version": 1, "amendment": AMENDMENT, "numerical_difference": DIFFERENCE,
        "config": config, "full_config_sha256": canonical_hash(config), "original_protocol": protocol,
        "original_protocol_sha256": agents._digest(protocol), "primary_agents_sha256": primary_agents_sha256,
        "execution_module_sha256": digest(Path(__file__)), "evaluation_dtype": "float64",
        "checkpoint": str(checkpoint), "checkpoint_sha256": checkpoint_sha256,
        "original_weight_sha256": saved["weight_sha256"], "original_training_steps": saved["steps"],
        "task": task, "architecture": architecture, "seed": seed, "evaluation_settings": evaluation,
        "canary": canary is not None, "canary_overrides": canary,
        "benchmark_directions": benchmark_directions,
        "conditions": _conditions(evaluation, protocol), "scientific_result": False,
        "admission_authority": False, "created_unix": time.time()}
    plan["evaluation_protocol"] = _amended_protocol(protocol, evaluation, plan["canary"], benchmark_directions)
    plan["evaluation_protocol_sha256"] = agents._digest(plan["evaluation_protocol"])
    atomic_json(output / "plan.json", plan)
    atomic_json(output / "plan-binding.json", {"plan_sha256": digest(output / "plan.json"),
        "checkpoint_sha256": checkpoint_sha256, "primary_agents_sha256": primary_agents_sha256,
        "execution_module_sha256": plan["execution_module_sha256"], "original_protocol_sha256": plan["original_protocol_sha256"],
        "slurm_job_id": native_job_id(), "created_unix": time.time(), "automatic_plan_rewrite_permitted": False})
    return plan


def saved_rate(settings, protocol, canary):
    return settings.get("lr_grid", [3e-4])[0] if canary is not None else protocol["learning_rate"]


def _load_plan(case, expected_plan_sha256=None):
    binding = read_json(Path(case) / "plan-binding.json")
    actual = digest(Path(case) / "plan.json")
    _require(actual == binding["plan_sha256"] and (expected_plan_sha256 is None or actual == expected_plan_sha256),
             "Immutable evaluation plan binding changed")
    plan = read_json(Path(case) / "plan.json")
    _require(all(binding[key] == plan[key] for key in ("checkpoint_sha256", "primary_agents_sha256",
        "execution_module_sha256", "original_protocol_sha256")), "Immutable plan input binding changed")
    _source_check(plan["primary_agents_sha256"])
    _require(plan["amendment"] == AMENDMENT and plan["execution_module_sha256"] == digest(Path(__file__))
             and plan["numerical_difference"] == DIFFERENCE and plan["evaluation_dtype"] == "float64", "Evaluation amendment/source changed")
    _require(canonical_hash(plan["config"]) == plan["full_config_sha256"]
             and agents._digest(plan["original_protocol"]) == plan["original_protocol_sha256"]
             and digest(Path(plan["checkpoint"])) == plan["checkpoint_sha256"], "Original input changed")
    settings = _settings(plan["config"])
    expected = dict(settings, **(plan["canary_overrides"] or {}))
    _require(type(plan["benchmark_directions"]) is bool and (not plan["benchmark_directions"] or plan["canary"]),
             "Final evaluation cannot use technical benchmark directions")
    _require(expected == plan["evaluation_settings"] and plan["canary"] == (plan["canary_overrides"] is not None)
             and plan["conditions"] == _conditions(expected, plan["original_protocol"]), "Evaluation condition roster changed")
    _require(plan["original_protocol"].get("config_sha256") == agents._digest(settings)
             and plan["original_protocol"].get("full_config_sha256") == canonical_hash(plan["config"])
             and plan["original_protocol"].get("implementation_sha256") == plan["primary_agents_sha256"]
             and plan["original_protocol"].get("H4_inference_specification") == agents.h4_specification(plan["config"]),
             "Frozen protocol input binding changed")
    _require(plan["evaluation_protocol"] == _amended_protocol(plan["original_protocol"], expected, plan["canary"], plan["benchmark_directions"])
             and plan["evaluation_protocol_sha256"] == agents._digest(plan["evaluation_protocol"]), "Amendment identity changed")
    return plan


def _precision_policy(base, dtype):
    """Wrap original forward code; promote inputs and initial state explicitly."""
    torch = agents._torch()
    class Policy(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.original = copy.deepcopy(base).to(dtype=dtype)
            self.architecture, self.state_dim = base.architecture, base.state_dim
        def initial_state(self, batch_size, device=None):
            return torch.zeros(batch_size, self.state_dim, dtype=dtype,
                               device=device or next(self.parameters()).device)
        def forward(self, obs, state, perturbation=None):
            return self.original(obs.to(dtype=dtype), state.to(dtype=dtype), perturbation)
    policy = Policy().eval().requires_grad_(False)
    for before, after in zip(base.parameters(), policy.parameters()):
        _require(torch.equal(before.detach().to(dtype=dtype), after.detach()), "Weight promotion changed an original value")
    return policy


def _perturbation(mechanism, rank, theta, dtype, *, seed=419, random_restoration=False):
    """Original QR directions and coefficients are quantized to f32, then promoted."""
    torch = agents._torch()
    class PrecisionPerturbation(agents.Perturbation):
        def _control(self, offset):
            key = ("precision_control", offset)
            if key not in self.cache:
                self.cache[key] = _perturbation(self.mechanism, self.rank, self.theta, dtype, seed=self.seed+offset)
            return self.cache[key]
        def _basis(self, dim, device):
            return super()._basis(dim, device).to(dtype=dtype)
        def _coefficient(self, device):
            return torch.as_tensor(self.theta, dtype=torch.float32, device=device).to(dtype=dtype)
        def vector(self, dim, device):
            value = self._basis(dim, device) @ self._coefficient(device)
            if self.random_restoration:
                correction = self._control(104729).vector(dim, device)
                value = value-correction*(value.norm()/correction.norm().clamp_min(1e-12))
            return value
        def recurrent(self, state):
            left, right = self._basis(state.shape[-1], state.device), self._control(271)._basis(state.shape[-1], state.device)
            value = ((state @ right)*self._coefficient(state.device)) @ left.T
            return value-self._control(104729).recurrent(state) if self.random_restoration else value
        def output_gain(self, latent, action_dim):
            right = self._basis(latent.shape[-1], latent.device)
            key = ("precision_output_left", action_dim, str(latent.device))
            if key not in self.cache:
                rng = np.random.default_rng(self.seed+action_dim*997)
                left = rng.normal(size=(action_dim, self.rank)); left /= np.linalg.norm(left, axis=0)
                self.cache[key] = torch.as_tensor(left, dtype=torch.float32, device=latent.device).to(dtype=dtype)
            value = ((latent @ right)*self._coefficient(latent.device)) @ self.cache[key].T
            return value-self._control(104729).output_gain(latent, action_dim) if self.random_restoration else value
    return PrecisionPerturbation(mechanism, rank, theta, seed=seed, random_restoration=random_restoration)


def _policy(plan, dtype=None):
    torch = agents._torch()
    base, saved = agents._load_policy(Path(plan["checkpoint"]), "cpu", _settings(plan["config"]),
        seed=plan["seed"], task=plan["task"], architecture=plan["architecture"],
        learning_rate=float(saved_rate(_settings(plan["config"]), plan["original_protocol"], plan["canary_overrides"])),
        complete=not plan["canary"])
    _require(saved["weight_sha256"] == plan["original_weight_sha256"], "Original checkpoint weights changed")
    return _precision_policy(base, dtype or torch.float64)


def _baselines(policy, plan):
    settings, seed, task = plan["evaluation_settings"], plan["seed"], plan["task"]
    evaluation_seeds = list(range(40_000_000+seed*1000, 40_000_000+seed*1000+settings["evaluation_episodes"]))
    calibration_seeds = list(range(50_000_000+seed*1000, 50_000_000+seed*1000+settings["calibration_episodes"]))
    _, clean_meta = agents.evaluate(policy, task, calibration_seeds, split="calibration", capture=True)
    baseline = {}
    for probe in (None, *agents.PROBES):
        rows, _ = agents.evaluate(policy, task, evaluation_seeds, probe=probe)
        baseline[probe] = sorted(rows, key=lambda row: row["episode_seed"])
    return evaluation_seeds, calibration_seeds, clean_meta, baseline


def _condition(policy, plan, condition, context):
    seeds, calibration_seeds, clean_meta, baseline = context
    settings, protocol, task = plan["evaluation_settings"], plan["evaluation_protocol"], plan["task"]
    mechanism, rank, strength, arm = (condition[k] for k in ("mechanism", "rank", "strength", "matching_arm"))
    theta = np.asarray(protocol["directions"][f"{mechanism}:{rank}"], dtype=float)
    _require(condition["rule_type"] == "synthetic", "This amendment has no validated biological bridge")
    dtype = next(policy.parameters()).dtype
    make = lambda multiplier, random=False: _perturbation(mechanism, rank, theta*multiplier, dtype, random_restoration=random)
    multiplier, target, failures = float(strength), float(strength)**2/2, []
    if arm == "policy_kl" and strength > 0:
        lo, hi = 0., 8.
        for _ in range(int(settings.get("kl_bisections", 10))):
            mid = (lo+hi)/2
            try:
                achieved = agents._clean_kl(policy, make(mid), clean_meta["sequence"])
            except agents.NumericalPolicyError as error:
                failures.append({"stage": "kl_bisection", "multiplier": mid, "reason": str(error)})
                hi = mid
                continue
            if achieved < target: lo = mid
            else: hi = mid
        multiplier = (lo+hi)/2
    perturbation = make(multiplier)
    achieved = agents._clean_kl(policy, perturbation, clean_meta["sequence"])
    restoration = {"mechanism": mechanism, "rank": rank, "strength": strength, "matching_arm": arm,
                   **agents._restoration_check(policy, task, calibration_seeds[:4], perturbation)}
    metadata = {"training_seed": plan["seed"], "architecture": plan["architecture"], "task": task,
        "perturbation": mechanism, "rank": rank, "effective_coefficients": rank, "rule_type": condition["rule_type"],
        "strength": strength, "matching_arm": arm, "achieved_parameter_norm": float(np.linalg.norm(theta*multiplier)),
        "clean_policy_kl": achieved, "target_policy_kl": target,
        "functional_match_within_10pct": abs(achieved-target) <= max(1e-6, target*.1),
        "norm_match_within_10pct": abs(float(np.linalg.norm(theta*multiplier))-float(strength)) <= max(1e-6, float(strength)*.1),
        "calibration_numerical_status": "available", "heldout_condition": True,
        "protocol_sha256": plan["evaluation_protocol_sha256"]}
    rows = []
    for probe in (None, *agents.PROBES):
        result, _ = agents.evaluate(policy, task, seeds, perturbation=perturbation, probe=probe, on_numerical_failure="record")
        for row, clean in zip(sorted(result, key=lambda row: row["episode_seed"]), baseline[probe]):
            rows.append({**metadata, "probe": probe or "main", "restoration_control": "none",
                "baseline_success": clean["success"], "baseline_redirection": clean["redirection"],
                "baseline_motor_accuracy": clean["motor_accuracy"], **row})
    for control in ("exact", "random_subspace", "cue_removal", "lower_action_noise"):
        active = None if control == "exact" else make(multiplier, control == "random_subspace")
        result, _ = agents.evaluate(policy, task, seeds, perturbation=active, cue_removal=control == "cue_removal",
            deterministic=control == "lower_action_noise", on_numerical_failure="record")
        rows.extend({**metadata, "probe": "main", "restoration_control": control, **row} for row in result)
    return rows, restoration, failures


def _validate_condition(rows, plan, condition):
    import pandas as pd
    frame = pd.DataFrame(rows)
    required = {"probe", "restoration_control", "episode_seed", "training_seed", "architecture", "task", "rule_type",
        "perturbation", "rank", "matching_arm", "strength", "protocol_sha256", "numerical_status",
        "calibration_numerical_status", "success", "redirection", "motor_accuracy", "achieved_parameter_norm",
        "clean_policy_kl", "target_policy_kl"}
    _require(required <= set(frame), "Missing evaluation condition columns")
    n, seed = plan["evaluation_settings"]["evaluation_episodes"], plan["seed"]
    keys = ["probe", "restoration_control", "episode_seed"]
    expected = {(p, c, e) for p, c in [(p, "none") for p in ("main", *agents.PROBES)] +
        [("main", c) for c in ("exact", "random_subspace", "cue_removal", "lower_action_noise")]
        for e in range(40_000_000+seed*1000, 40_000_000+seed*1000+n)}
    _require(len(frame) == len(expected) and not frame.duplicated(keys).any()
             and set(frame[keys].itertuples(index=False, name=None)) == expected, "Incomplete or duplicate condition episode roster")
    for key, value in {"training_seed": seed, "architecture": plan["architecture"], "task": plan["task"],
        "rule_type": condition["rule_type"], "perturbation": condition["mechanism"], "rank": condition["rank"],
        "matching_arm": condition["matching_arm"], "strength": condition["strength"],
        "protocol_sha256": plan["evaluation_protocol_sha256"]}.items():
        _require(frame[key].eq(value).all(), "Condition policy/protocol identity changed: " + key)
    _require(frame.numerical_status.eq("available").all() and frame.calibration_numerical_status.eq("available").all(),
             "Explicit unavailable numerical evidence; no rows may be filled or dropped")
    for key in ("success", "redirection", "motor_accuracy", "achieved_parameter_norm", "clean_policy_kl", "target_policy_kl"):
        _require(np.isfinite(frame[key].to_numpy(dtype=float)).all(), "Nonfinite evaluation field: " + key)
    _require(frame.success.isin([0, 1]).all() and frame.redirection.isin([0, 1]).all()
             and frame.motor_accuracy.between(0, 1).all(), "Invalid endpoint range")
    _require((frame.achieved_parameter_norm >= 0).all() and (frame.clean_policy_kl >= 0).all()
             and np.allclose(frame.target_policy_kl, float(condition["strength"])**2/2, rtol=1e-10, atol=1e-12),
             "Calibration metadata changed its frozen rule")
    primary = frame[frame.restoration_control.eq("none")]
    for key in ("baseline_success", "baseline_redirection", "baseline_motor_accuracy"):
        _require(key in primary and np.isfinite(primary[key]).all() and primary[key].between(0, 1).all(),
                 "Missing or nonfinite paired clean baseline")
    return frame


def _validate_restoration(restoration, condition):
    _require(all(restoration.get(key) == condition[key] for key in ("mechanism", "rank", "strength", "matching_arm"))
             and restoration.get("base_weights_unchanged") is True
             and restoration.get("intervened_probability_status") == "available"
             and math.isfinite(restoration["restoration_max_probability_error"])
             and 0 <= restoration["restoration_max_probability_error"] <= 1e-7
             and math.isfinite(restoration["altered_max_probability_change"]), "Invalid numerical restoration evidence")


def _committed(case, plan):
    completed, plan_sha = {}, digest(Path(case) / "plan.json")
    for path in sorted(Path(case).glob("condition-*.json")):
        receipt = read_json(path)
        index = receipt["condition_index"]
        _require(type(index) is int and 0 <= index < len(plan["conditions"]) and index not in completed,
                 "Duplicate or unknown committed condition")
        _require(receipt["plan_sha256"] == plan_sha and receipt["condition"] == plan["conditions"][index]
                 and receipt["status"] == "complete" and receipt["original_weight_sha256"] == plan["original_weight_sha256"],
                 "Committed condition changed its plan or original weights")
        _validate_restoration(receipt["restoration"], plan["conditions"][index])
        artifact = Path(case) / receipt["artifact_name"]
        _require(artifact.parent.resolve() == Path(case).resolve() and artifact.name == receipt["artifact_name"]
                 and not artifact.is_symlink() and digest(artifact) == receipt["artifact_sha256"], "Committed condition file changed")
        completed[index] = dict(receipt, _receipt_name=path.name, _receipt_sha256=digest(path))
    return completed


def run_chunk(case, *, max_conditions=1, max_wall_seconds=3600, expected_plan_sha256=None):
    """Commit finite whole conditions; the native parent enforces hard deadlines."""
    import pandas as pd
    case = Path(case).resolve(); plan = _load_plan(case, expected_plan_sha256)
    started = time.monotonic()
    _require(type(max_conditions) is int and 1 <= max_conditions <= len(plan["conditions"]), "Invalid finite chunk size")
    _require(math.isfinite(max_wall_seconds) and 0 < max_wall_seconds <= 7*86400, "Invalid finite chunk wall budget")
    with exclusive_file_lock(case / "execution.lock", timeout=0):
        completed = _committed(case, plan)
        policy = _policy(plan); before = agents.state_hash(policy)
        _require(all(r["evaluation_weight_sha256"] == before for r in completed.values()), "Committed evaluation weights changed")
        context = _baselines(policy, plan)
        count = 0
        for index, condition in enumerate(plan["conditions"]):
            if index in completed: continue
            if count >= max_conditions or time.monotonic()-started >= max_wall_seconds: break
            attempt = f"condition-{index:05d}-{uuid.uuid4().hex}"
            rows = None
            try:
                operation_started, cpu_started = time.perf_counter(), time.process_time()
                rows, restoration, failures = _condition(policy, plan, condition, context)
                wall_seconds, process_cpu_seconds = time.perf_counter()-operation_started, time.process_time()-cpu_started
                frame = _validate_condition(rows, plan, condition)
                _validate_restoration(restoration, condition)
                _require(agents.state_hash(policy) == before, "Evaluation changed frozen weights")
                artifact = case / (attempt + ".parquet")
                frame.to_parquet(artifact, index=False)
                receipt = {"status": "complete", "plan_sha256": digest(case / "plan.json"),
                    "condition_index": index, "condition": condition, "artifact_name": artifact.name,
                    "artifact_sha256": digest(artifact), "rows": len(frame), "restoration": restoration,
                    "calibration_numerical_diagnostics": failures, "original_weight_sha256": plan["original_weight_sha256"],
                    "evaluation_weight_sha256": before, "slurm_job_id": native_job_id(), "completed_unix": time.time()}
                receipt.update(condition_wall_seconds=wall_seconds, condition_process_cpu_seconds=process_cpu_seconds,
                    torch_num_threads=agents._torch().get_num_threads(), evaluation_dtype="float64")
                atomic_json(case / (attempt + ".json"), receipt)
                completed[index] = receipt; count += 1
            except BaseException as error:
                # Failed or interrupted attempts never become a committed row set.
                # Record raw unavailable rows when the primary kernel returned them.
                failure = {"condition_index": index, "condition": condition,
                    "plan_sha256": digest(case / "plan.json"), "error_type": type(error).__name__, "error": str(error)}
                if rows is not None:
                    failed = case / (attempt + "-uncommitted.parquet")
                    pd.DataFrame(rows).to_parquet(failed, index=False)
                    failure.update(raw_rows_path=failed.name, raw_rows_sha256=digest(failed))
                atomic_json(case / ("failure-" + attempt + ".json"), failure)
                raise
        result = {"status": "condition_grid_complete" if len(completed) == len(plan["conditions"]) else "chunk_complete",
            "plan_sha256": digest(case / "plan.json"), "completed_conditions": len(completed),
            "required_conditions": len(plan["conditions"]), "newly_completed_conditions": count,
            "canary": plan["canary"], "scientific_result": False, "admission_authority": False}
        atomic_json(case / "progress.json", result)
        return result


def assemble(case, *, expected_plan_sha256=None):
    """Exactly one complete receipt per condition; original endpoint checks apply."""
    import pandas as pd
    from .agent_analysis import validate_trial_roster
    case = Path(case).resolve(); plan = _load_plan(case, expected_plan_sha256)
    with exclusive_file_lock(case / "execution.lock", timeout=0):
        if (case / "evaluation_summary.json").exists():
            summary = read_json(case / "evaluation_summary.json")
            artifact = case / summary["artifact_name"]
            _require(artifact.parent.resolve() == case and artifact.name == summary["artifact_name"]
                     and not artifact.is_symlink() and summary["plan_sha256"] == digest(case / "plan.json")
                     and digest(artifact) == summary["trial_file_sha256"], "Completed assembly changed")
            for binding in summary["condition_bindings"]:
                receipt_path = case / binding["receipt_name"]
                _require(receipt_path.name == binding["receipt_name"]
                         and digest(receipt_path) == binding["receipt_sha256"], "Completed condition provenance changed")
            return summary
        completed = _committed(case, plan)
        _require(len(completed) == len(plan["conditions"]), "Evaluation grid is incomplete")
        expected_weight = agents.state_hash(_policy(plan))
        _require(all(r["evaluation_weight_sha256"] == expected_weight for r in completed.values()), "Evaluation weights changed")
        frames = [_validate_condition(pd.read_parquet(case / completed[i]["artifact_name"]).to_dict("records"), plan, condition)
                  for i, condition in enumerate(plan["conditions"])]
        frame = pd.concat(frames, ignore_index=True)
        validate_trial_roster(frame, plan["evaluation_settings"], plan["evaluation_protocol"],
            architecture=plan["architecture"], task=plan["task"], seed=plan["seed"])
        output = case / ("assembled-" + uuid.uuid4().hex + ".parquet"); frame.to_parquet(output, index=False)
        summary = {"status": "complete_precision_canary" if plan["canary"] else "complete_amended_evaluation",
            "amendment": AMENDMENT, "numerical_difference": DIFFERENCE, "plan_sha256": digest(case / "plan.json"),
            "original_protocol_sha256": plan["original_protocol_sha256"], "evaluation_protocol_sha256": plan["evaluation_protocol_sha256"],
            "trial_rows": len(frame), "artifact_name": output.name, "trial_file_sha256": digest(output), "conditions": len(completed),
            "checkpoint_sha256": plan["checkpoint_sha256"], "original_weight_sha256": plan["original_weight_sha256"],
            "evaluation_weight_sha256": next(iter(completed.values()))["evaluation_weight_sha256"],
            "condition_bindings": [{"condition_index": i, "receipt_name": completed[i]["_receipt_name"],
                "receipt_sha256": completed[i]["_receipt_sha256"], "artifact_name": completed[i]["artifact_name"],
                "artifact_sha256": completed[i]["artifact_sha256"]} for i in range(len(plan["conditions"]))],
            "canary": plan["canary"], "scientific_result": False, "admission_authority": False}
        atomic_json(case / "evaluation_summary.json", summary)
        return summary


def diagnostics(case, *, expected_plan_sha256=None):
    """Run identical complete canary conditions in both precisions; change no files."""
    plan = _load_plan(Path(case), expected_plan_sha256)
    _require(plan["canary"], "Full-grid paired precision diagnostics require a technical canary plan")
    torch = agents._torch(); reports = []
    for dtype in (torch.float32, torch.float64):
        policy = _policy(plan, dtype); context = _baselines(policy, plan)
        before = agents.state_hash(policy)
        for index, condition in enumerate(plan["conditions"]):
            try:
                rows, _, _ = _condition(policy, plan, condition, context)
                unavailable = sum(row["numerical_status"] != "available" for row in rows)
                reports.append({"dtype": str(dtype), "condition_index": index, "rows": len(rows),
                    "unavailable_rows": unavailable, "status": "available" if not unavailable else "unavailable",
                    "endpoint_record_sha256": canonical_hash(rows)})
            except agents.NumericalPolicyError as error:
                reports.append({"dtype": str(dtype), "condition_index": index, "status": "unavailable", "reason": str(error)})
        _require(agents.state_hash(policy) == before, "Precision diagnostics changed weights")
    return {"amendment": AMENDMENT, "numerical_difference": DIFFERENCE, "plan_sha256": digest(Path(case)/"plan.json"),
            "reports": reports, "scientific_result": False, "admission_authority": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("chunk", "assemble", "diagnostics"))
    parser.add_argument("--case", required=True, type=Path)
    parser.add_argument("--max-conditions", default=1, type=int)
    parser.add_argument("--max-wall-seconds", default=3600, type=float)
    parser.add_argument("--plan-sha256")
    args = parser.parse_args(argv)
    result = run_chunk(args.case, max_conditions=args.max_conditions, max_wall_seconds=args.max_wall_seconds, expected_plan_sha256=args.plan_sha256) if args.mode == "chunk" else assemble(args.case, expected_plan_sha256=args.plan_sha256) if args.mode == "assemble" else diagnostics(args.case, expected_plan_sha256=args.plan_sha256)
    print(json.dumps(result, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
