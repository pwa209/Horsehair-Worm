"""Finite native work-unit probes and honest frozen-scope cost diagnostics.

These probes never write scientific fits, confirmatory results or admission
receipts. Missing inputs and unmeasured families keep full coverage false.
"""
from __future__ import annotations

import argparse
import copy
import json
import math
import os
from pathlib import Path
import time
from typing import Any

import numpy as np
import pandas as pd

from .core import atomic_json, canonical_hash, digest, native_job_id, source_identity


def frozen_work_counts(config: dict[str, Any], rosters: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Count actual grouped-loop multiplicities without using observed outcomes.

    Counts are conservative maximum requested budgets. Early stopping and
    converged starts may cost less; neither is assumed before measurement.
    """
    settings = config.get("behaviour", {})
    starts, final = int(settings.get("starts", 5)), int(settings.get("final_starts", 20))
    epochs, lbfgs = int(settings.get("max_epochs", 500)), int(settings.get("lbfgs_iterations", 200))
    rates = list(settings.get("learning_rates", [.0003, .001, .003]))
    if min(starts, final, epochs) < 1 or lbfgs < 0 or not rates:
        raise ValueError("Frozen optimizer budgets must be positive and finite")
    models = list(settings.get("models", ["M0", "MG", "MT", "MD", "MX"]))
    comparators = list(settings.get("comparators", ["AR", "HMM", "GRU"]))
    if set(models) - {"M0", "MG", "MT", "MD", "MX"} or set(comparators) - {"AR", "HMM", "GRU"}:
        raise ValueError("Uncounted model family in frozen behaviour workload")
    widths = sorted(set([float(settings.get("primary_bin_seconds", 10)), *map(float, settings.get("sensitivity_bins_seconds", [5, 20]))]))
    if {f"{width:g}s" for width in widths} != set(rosters):
        raise ValueError("Every declared bin width needs its complete native roster")
    primary, rows = float(settings.get("primary_bin_seconds", 10)), []
    memberships = []
    for width in widths:
        roster = rosters[f"{width:g}s"]
        identities = [(str(row["animal_id"]), str(row["experiment_id"])) for row in roster]
        if not roster or len({animal for animal, _ in identities}) != len(identities):
            raise ValueError("Native workload needs a nonempty unique animal roster")
        if any(int(row["bins"]) < 2 or row.get("analysis_role") != "behavioural_development" for row in roster):
            raise ValueError("Unvalidated development identity or native clock")
        memberships.append(set(identities))
        experiments = sorted({experiment for _, experiment in identities})
        if len(experiments) < 3:
            raise ValueError("Nested work-unit plan needs at least three independent experiments")
        counts = {experiment: sum(value == experiment for _, value in identities) for experiment in experiments}
        n, folds = len(roster), int(settings.get("inner_folds", 3))
        if folds < 2:
            raise ValueError("Grouped inner folds must be at least two")
        inner_animals, inner_validation, inner_fits = 0, 0, 0
        for held_out in experiments:
            train = [experiment for experiment in experiments if experiment != held_out]
            k = min(folds, len(train))
            for fold in range(k):
                test = set(train[fold::k])
                inner_animals += sum(counts[experiment] for experiment in train if experiment not in test)
                inner_validation += sum(counts[experiment] for experiment in test)
                inner_fits += 1
        outer_animals = n * (len(experiments) - 1)
        for model in models + comparators:
            if model == "GRU":
                structural = [{"states": 2, "hidden_size": int(hidden), "dropout": float(dropout)}
                    for hidden in settings.get("neural_hidden_sizes", [16, 32, 64])
                    for dropout in settings.get("neural_dropout", [0, .1])]
            else:
                structural = [{"states": int(states)} for states in
                              ([2] if model in {"AR", "HMM"} else settings.get("state_counts", [2]))]
            for shape in structural:
                # Each structural candidate has all learning rates. The selected
                # outer shape may be any candidate; assigning its bound to each
                # shape deliberately overcounts rather than selecting on outcomes.
                retry = starts + final if model not in {"AR", "GRU"} else starts
                training_units = inner_animals * len(rates) * starts + outer_animals * retry
                # Validation at every start/epoch plus nested_validation's
                # explicit held-out score after selecting the best start.
                inner_post_fit_scoring = inner_validation * len(rates)
                refinement_scoring = inner_validation * len(rates) * starts if lbfgs and model in {"AR", "GRU"} else 0
                validation_units = inner_validation * len(rates) * starts * epochs + inner_post_fit_scoring + refinement_scoring + n
                fit_starts = inner_fits * len(rates) * starts + len(experiments) * retry
                freeze_units = 0
                if width == primary and model in {"MG", "MT", "MX", "GRU"}:
                    freeze_units = n * retry
                    training_units += freeze_units
                    fit_starts += retry
                recovery_units, curvature_units, recovery_scoring = 0, 0, 0
                if width == primary and model in models:
                    cells = (len(settings.get("recovery_models", ["MG", "MT", "MD", "MX"])) *
                             len(settings.get("recovery_effects", [0, .25, .5, 1])) *
                             int(settings.get("parameter_recovery_replicates", 100)))
                    k = min(folds, len(experiments))
                    recovery_units = cells * n * (k - 1) * starts
                    # Every synthetic generator/effect/replicate is scored once
                    # on each held-out animal across its grouped folds. Starts
                    # are alternative fits, not additional scoring subjects.
                    recovery_scoring = cells * n
                    validation_units += recovery_scoring
                    training_units += recovery_units
                    fit_starts += cells * k * starts
                    if model in settings.get("recovery_models", ["MG", "MT", "MD", "MX"]):
                        curvature_units = (len(settings.get("recovery_effects", [0, .25, .5, 1])) *
                                           int(settings.get("parameter_recovery_replicates", 100)) * n * (k - 1))
                rows.append({"model": model, "bin_seconds": width, **shape,
                    "native_animals": n, "native_experiments": len(experiments),
                    "native_state_bins": sum(int(row["bins"]) for row in roster),
                    "maximum_native_clock_bins": max(int(row["bins"]) for row in roster),
                    "starts_upper": fit_starts, "training_animal_passes_per_epoch": training_units,
                    "adam_animal_passes_upper": training_units * epochs,
                    "lbfgs_animal_closures_upper": training_units * (1 + 26 * lbfgs) if lbfgs else 0,
                    "held_out_scoring_animal_passes_upper": validation_units,
                    "inner_post_fit_scoring_animal_passes": inner_post_fit_scoring,
                    "inner_refinement_scoring_animal_passes": refinement_scoring,
                    "recovery_held_out_scoring_animal_passes": recovery_scoring,
                    "recovery_training_animal_starts": recovery_units,
                    "recovery_full_parameter_curvature_animal_units": curvature_units,
                    "freeze_animal_starts_upper": freeze_units,
                    "forecast_rule": "All requested epochs/starts and full clocks; selected structural shapes conservatively overcounted; strong-Wolfe at most25 search iterations plus initial evaluations per iteration"})
    if any(membership != memberships[0] for membership in memberships[1:]):
        raise ValueError("Sensitivity clocks changed the independent native roster")
    sampler = settings.get("bayesian_sampler", {})
    return {"counts": rows, "clock_rosters": rosters,
        "learning_rates": rates, "starts": starts, "final_starts": final,
        "max_epochs": epochs, "lbfgs_iterations": lbfgs,
        "bayesian_declared": {"chains": int(sampler.get("chains", 4)), "warmup": int(sampler.get("warmup", 1000)),
                               "draws": int(sampler.get("draws", 2000)), "maximum_parameters": int(sampler.get("maximum_parameters", 256)),
                               "roster_rule": "Original shortest/median/longest experiment clock strata and two identity-hashed animals per stratum, or explicit declared IDs"},
        "pending_dimensions": ["Optimizer and actual closure paths for all structural/rate cells",
            "Full group Hessian/factorisation and representative original-clock MALA",
            "Neural training/score/simulation work for every hidden/dropout cell",
            "Original mantis whole-date/individual validation and pair bootstrap",
            "Curated H2 contrast/control/animal/family calibration and shared-control bootstrap"],
        "not_independent_units": "Repeated fits, starts, kernels and simulated trajectories add no biological replication"}


def measured_scope_diagnostic(config: dict[str, Any], benchmark: dict[str, Any]) -> dict[str, Any]:
    """Archive a partial historical timing projection, never authorize admission."""
    if benchmark.get("full_config_sha256") != canonical_hash(config):
        raise ValueError("Kernel evidence differs from the frozen full configuration")
    plan = frozen_work_counts(config, benchmark["prospective_workload"]["clock_rosters"])
    estimates, pending = [], list(plan["pending_dimensions"])
    for cell in plan["counts"]:
        matches = [task for task in benchmark.get("tasks", []) if task.get("operation") == "likelihood_gradient"
                   and task.get("model") == cell["model"] and float(task.get("bin_seconds", 0)) == cell["bin_seconds"]]
        if len(matches) != 1:
            continue
        task = matches[0]
        seconds = float(task["elapsed_seconds"])
        cpus = int(task["allocated_cpus"])
        if not math.isfinite(seconds) or seconds <= 0 or cpus < 1:
            raise ValueError("Native timing has no positive measured duration/allocation")
        estimate = seconds * cell["adam_animal_passes_upper"] * cpus / 3600
        estimates.append({"model": cell["model"], "bin_seconds": cell["bin_seconds"],
            "native_kernel_job_id": task["slurm_job_id"], "kernel_source_sha256": task["source_sha256"],
            "maximum_clock_gradient_seconds": seconds, "allocated_cpus": cpus,
            "adam_only_forecast_cpu_core_hours": estimate,
            "work_units_upper": cell["adam_animal_passes_upper"],
            "interpretation": "Conservative projection at the largest measured native clock; excludes LBFGS, scoring, neural, curvature, MALA, mantis and H2. This is an estimate, not a mathematical lower bound or admission proof."})
    partial = sum(row["adam_only_forecast_cpu_core_hours"] for row in estimates)
    cap = float(config["resources"]["cpu_core_hours_cap"])
    return {"status": "frozen_scope_projection_exceeds_cap" if partial > cap else "pending_full_scope_measurements",
        "admission_authority": False, "benchmark_only": True, "scientific_fit": False,
        "complete_declared_workload": False, "full_config_sha256": canonical_hash(config),
        "kernel_source_sha256": benchmark["source_sha256"], "kernel_slurm_job_id": benchmark["slurm_job_id"],
        "prospective_workload": plan, "prospective_workload_sha256": canonical_hash(plan),
        "measured_model_projection": estimates, "partial_adam_only_forecast_cpu_core_hours": partial,
        "cpu_core_hours_cap": cap, "exceeds_cpu_cap_by_factor": partial / cap,
        "pending": pending, "forecast_interpretation": "Only measured serial kernels are projected. Faster equivalent numerical implementations require fresh complete source-bound timing and admission; historical timing is never relabeled."}


def _native_sequences(root: Path, config: dict[str, Any], width: float, count: int) -> tuple[list[Any], list[dict[str, Any]], dict[str, Any]]:
    import pyarrow.parquet as pq
    from .behaviour import to_sequences
    if count < 1 or count > 8:
        raise ValueError("Finite probe batch size must lie between1 and8")
    proof_path = root / "results/behaviour/streaming_bin_provenance.json"
    proof = json.loads(proof_path.read_text(encoding="utf8"))
    path = root / "results/behaviour" / f"fly_bins_{width:g}s.parquet"
    if proof.get("bin_files", {}).get(str(path.resolve())) != digest(path):
        raise ValueError("Native chronological bin content no longer matches ingestion proof")
    roster = []
    metadata = pq.ParquetFile(path).metadata
    for index in range(metadata.num_row_groups):
        group = metadata.row_group(index)
        fields = {}
        for name in ("animal_id", "experiment_id", "analysis_role"):
            columns = [group.column(j) for j in range(group.num_columns) if group.column(j).path_in_schema == name]
            if len(columns) != 1 or columns[0].statistics is None or columns[0].statistics.min != columns[0].statistics.max:
                raise ValueError("Native probe requires constant original identity/role row groups")
            fields[name] = str(columns[0].statistics.min)
        if fields["analysis_role"] == "behavioural_development":
            roster.append(dict(fields, bins=group.num_rows))
    longest = max(row["bins"] for row in roster)
    selected = sorted([row for row in roster if row["bins"] == longest], key=lambda row: row["animal_id"])[:count]
    if len(selected) != count:
        raise ValueError("Insufficient equally long distinct native animals for this finite probe")
    frame = pd.read_parquet(path, filters=[("animal_id", "in", [row["animal_id"] for row in selected])])
    sequences = to_sequences(frame, dict(config["behaviour"], modulation=["elapsed_hours"]))
    if {sequence.animal_id for sequence in sequences} != {row["animal_id"] for row in selected} or any(len(sequence.times) != longest for sequence in sequences):
        raise ValueError("Prospective maximum native clocks were shortened or changed")
    inputs = {str(path.resolve()): digest(path), str(proof_path.resolve()): digest(proof_path)}
    return sequences, roster, {"source_result_paths": list(inputs), "source_result_hashes": inputs,
                            "selection": "Maximum complete native clock, then animal identity; no outcomes inspected"}


def native_batch_probe(config: dict[str, Any], root: Path, width: float, count: int,
                       kind: str = "MX") -> dict[str, Any]:
    """Compare exact serial and independent-animal batches on real full clocks."""
    import torch
    from .batched_duration import forward_independent_batch
    from .behaviour import _process_peak_ram_bytes
    from .semimarkov import MovementModel, TrainingScaler, training_duration_knots
    if native_job_id() is None:
        raise ValueError("Full native probes require a scheduled Slurm allocation")
    settings = config["behaviour"]
    if kind not in {"M0", "MG", "MT", "MD", "MX"} or kind not in settings.get("models", ["M0", "MG", "MT", "MD", "MX"]):
        raise ValueError("Native probe model must be a declared interpretable model")
    threads = int(os.environ.get("SLURM_CPUS_PER_TASK", "1"))
    torch.set_num_threads(threads)
    units, roster, inputs = _native_sequences(Path(root), config, width, count)
    scaler = TrainingScaler.fit(units)
    units = [scaler.transform(sequence) for sequence in units]
    model = MovementModel(kind, len(scaler.mean), len(scaler.modulation_mean), units[0].observations.shape[1],
                          training_duration_knots(units), [row["animal_id"] for row in roster],
                          sorted({row["experiment_id"] for row in roster}), bin_seconds=width,
                          max_duration_bins=settings.get("max_duration_bins"))
    # Nonzero age, context and group coefficients qualify the generic recursion;
    # a zero age coefficient alone could accidentally qualify only an HMM.
    torch.manual_seed(int(config.get("seed", 0)))
    with torch.no_grad():
        for parameter in model.parameters():
            parameter.add_(torch.randn_like(parameter) * .02)
    serial_model = copy.deepcopy(model)
    started = time.perf_counter()
    serial_scores, serial_tails = [], []
    (-serial_model.log_prior()).backward()
    for unit in units:
        value = serial_model(unit)
        serial_scores.append(value["log_scores"].detach().clone())
        serial_tails.append(value["max_tail_probability"].detach().clone())
        (-value["log_likelihood"].sum()).backward()
        del value
    serial_seconds = time.perf_counter() - started
    serial_gradients = {name: parameter.grad.detach().clone() if parameter.grad is not None else None
                        for name, parameter in serial_model.named_parameters()}
    del serial_model
    started = time.perf_counter()
    batched = forward_independent_batch(model, units)
    (-batched["log_likelihood"].sum() - batched["log_prior"]).backward()
    batch_seconds = time.perf_counter() - started
    torch.testing.assert_close(batched["log_scores"], torch.cat(serial_scores), rtol=1e-9, atol=1e-9)
    torch.testing.assert_close(batched["max_tail_probability"], torch.cat(serial_tails), rtol=1e-9, atol=1e-9)
    maximum_error = 0.
    for name, parameter in model.named_parameters():
        expected = serial_gradients[name]
        if expected is None:
            if parameter.grad is not None:
                raise ValueError("Batch activated a previously inactive parameter")
        else:
            torch.testing.assert_close(parameter.grad, expected, rtol=1e-8, atol=1e-8)
            maximum_error = max(maximum_error, float((parameter.grad - expected).abs().max()))
    return {"status": "native_exact_batch_equivalence_measured", "benchmark_only": True,
        "scientific_fit": False, "admission_authority": False, "complete_declared_workload": False,
        "full_config_sha256": canonical_hash(config), "source_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"],
        "slurm_job_id": native_job_id(), "allocated_cpus": threads, "model": kind, "bin_seconds": width,
        "native_animal_ids": [unit.animal_id for unit in units], "native_clock_bins": len(units[0].times),
        "full_roster_animals": len(roster), "full_roster_experiments": len({row["experiment_id"] for row in roster}),
        "full_roster_parameter_count": sum(parameter.numel() for parameter in model.parameters()),
        "serial_gradient_seconds": serial_seconds, "batch_gradient_seconds": batch_seconds,
        "measured_work_units": count, "measured_speedup": serial_seconds / batch_seconds,
        "maximum_absolute_parameter_gradient_error": maximum_error,
        "equivalence_tolerances": {"log_score_absolute_relative": 1e-9, "gradient_absolute_relative": 1e-8},
        "process_peak_ram_bytes": _process_peak_ram_bytes(), "production_fit_kernel_changed": False,
        "biological_time_interpretation": "Technical recording-relative modulation only; no inferred exposure time or H1 result",
        **inputs}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=["scope", "batch"], required=True)
    parser.add_argument("--bin-seconds", type=float, default=10.)
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--model", choices=["M0", "MG", "MT", "MD", "MX"], default="MX")
    args = parser.parse_args(argv)
    config = json.loads(Path(args.config).read_text(encoding="utf8"))
    root = Path(args.root)
    if args.mode == "scope":
        report = json.loads((root / "state/behaviour-workload-benchmark.json").read_text(encoding="utf8"))
        result = measured_scope_diagnostic(config, report)
    else:
        result = native_batch_probe(config, root, args.bin_seconds, args.batch_size, args.model)
    output = Path(args.output)
    if output.exists():
        raise FileExistsError("A native benchmark receipt cannot overwrite an earlier measurement")
    atomic_json(output, result)
    print(json.dumps({key: value for key, value in result.items() if key not in {"prospective_workload", "measured_model_projection"}}, sort_keys=True))


if __name__ == "__main__":
    main()
