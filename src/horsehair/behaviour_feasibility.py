"""Bounded native cost measurements of unchanged behavioural numerical kernels.

These are technical recording-clock probes, never exposure analyses or fitted
biological results. A completed cell measures operations on one complete native
clock; it does not mean that the full cohort, optimizer or study was completed.
"""
from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any

from .core import atomic_json, canonical_hash, digest, native_job_id, source_identity


PRIMARY_SOURCE_SHA256 = "0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630"
MISSING_CATEGORIES = (
    "Full-cohort optimizer stopping and line-search paths for every grouped split",
    "Full-group posterior Hessian assembly and factorization",
    "Representative full-clock MALA setup and all declared chains/draws",
    "Known-truth recovery simulation, fitted intervals and grouped scoring",
    "Mantis whole-individual/date validation and pair bootstrap",
    "Source-validated H2 contrasts, calibration and shared-control uncertainty",
)
OPERATIONS = ("adam_epoch", "lbfgs_iteration", "unseen_group_score", "causal_full_clock_simulation")


def structural_cells(config: dict[str, Any]) -> list[dict[str, Any]]:
    settings = config.get("behaviour", {})
    widths = sorted(set([float(settings.get("primary_bin_seconds", 10)),
                        *map(float, settings.get("sensitivity_bins_seconds", [5, 20]))]))
    models = [*settings.get("models", ["M0", "MG", "MT", "MD", "MX"]),
              *settings.get("comparators", ["AR", "HMM", "GRU"])]
    if not models or len(set(models)) != len(models) or set(models) - {"M0", "MG", "MT", "MD", "MX", "AR", "HMM", "GRU"}:
        raise ValueError("The declared structural model roster is invalid")
    if any(not math.isfinite(width) or width <= 0 for width in widths):
        raise ValueError("Bin widths must be finite and positive")
    cells = []
    for width in widths:
        for name in models:
            structures = ([{"hidden_size": int(hidden), "dropout": float(dropout), "states": 2}
                           for hidden in settings.get("neural_hidden_sizes", [16, 32, 64])
                           for dropout in settings.get("neural_dropout", [0, .1])]
                          if name == "GRU" else
                          [{"states": int(states)} for states in
                           ([2] if name in {"AR", "HMM"} else settings.get("state_counts", [2]))])
            for shape in structures:
                if shape["states"] not in (2, 3) or (name == "GRU" and
                        (shape["hidden_size"] < 1 or not 0 <= shape["dropout"] < 1)):
                    raise ValueError("Invalid declared model structure")
                cells.append({"index": len(cells), "model": name, "bin_seconds": width, **shape})
    return cells


def _native_identity() -> dict[str, Any]:
    job = native_job_id()
    cpus = int(os.environ.get("SLURM_CPUS_PER_TASK", "0"))
    if job is None or not 1 <= cpus <= 8:
        raise ValueError("Cost probes require a native allocation of one to eight CPUs")
    return {"slurm_job_id": job, "allocated_cpus": cpus}


def runtime_limit(requested: float, *, now: float | None = None, allocation_end: str | None = None) -> float:
    value = float(requested)
    if not math.isfinite(value) or not 30 <= value <= 7200:
        raise ValueError("A finite probe must request between30 and7200 seconds")
    if allocation_end is not None:
        end = float(allocation_end)
        if not math.isfinite(end):
            raise ValueError("Native allocation end must be finite")
        value = min(value, end - (time.time() if now is None else now) - 120)
    if value < 30:
        raise ValueError("Less than30 seconds remain after the native finalization margin")
    return value


def _roster(path: Path) -> list[dict[str, Any]]:
    import pyarrow.parquet as pq
    metadata, rows = pq.ParquetFile(path).metadata, []
    for index in range(metadata.num_row_groups):
        group, fields = metadata.row_group(index), {}
        for name in ("animal_id", "experiment_id", "analysis_role"):
            columns = [group.column(j) for j in range(group.num_columns)
                       if group.column(j).path_in_schema == name]
            if len(columns) != 1 or columns[0].statistics is None or columns[0].statistics.min != columns[0].statistics.max:
                raise ValueError("A native row group must have one constant identity and role")
            fields[name] = str(columns[0].statistics.min)
        if fields["analysis_role"] == "behavioural_development":
            observed = [group.column(j) for j in range(group.num_columns)
                        if group.column(j).path_in_schema == "observed"]
            if len(observed) != 1 or observed[0].statistics is None or observed[0].statistics.max is None:
                raise ValueError("Native observed-mask column statistics are required before selecting a cost clock")
            rows.append(dict(fields, bins=group.num_rows,
                             observed_any=bool(observed[0].statistics.max)))
    if not rows or len({row["animal_id"] for row in rows}) != len(rows) or any(row["bins"] < 2 for row in rows):
        raise ValueError("Native development roster is empty, duplicated or has incomplete clocks")
    return rows


def _select_native_clock(path: Path, roster: list[dict[str, Any]], warmup_seconds: float) -> dict[str, Any]:
    import numpy as np
    import pandas as pd
    for row in sorted(roster, key=lambda value: (-value["bins"], value["animal_id"])):
        row["technical_probe_eligible"] = False
        if row.get("observed_any") is not True:
            continue
        frame = pd.read_parquet(path, filters=[("animal_id", "=", row["animal_id"])],
                                columns=["record_time_s", "observed", "dx", "dy"]).sort_values("record_time_s")
        if len(frame) != row["bins"]:
            raise ValueError("Prospective native clock length changed during eligibility inspection")
        times = frame.record_time_s.to_numpy(float)
        columns = ["dx", "dy"] if frame.dx.notna().any() else ["dy"]
        valid = frame.observed.to_numpy(bool) & np.isfinite(frame[columns].to_numpy(float)).all(1)
        row["observed_after_warmup"] = int((valid & (times - times[0] >= warmup_seconds)).sum())
        if row["observed_after_warmup"]:
            row["technical_probe_eligible"] = True
            return row
    raise ValueError("No original complete native development clock has valid observed targets after the declared warm-up")


def prepare_plan(config_path: Path, root: Path, primary_release: Path, output_dir: Path) -> dict[str, Any]:
    """Bind all native clocks and predeclare the complete structural grid once."""
    from .behaviour_workload import frozen_work_counts
    identity = _native_identity()
    root, primary_release, output_dir = map(lambda p: Path(p).resolve(), (root, primary_release, output_dir))
    config = json.loads(Path(config_path).read_text(encoding="utf8"))
    primary_identity = source_identity(primary_release)
    if primary_identity["sha256"] != PRIMARY_SOURCE_SHA256:
        raise ValueError("The numerical primary release differs from the frozen source")
    if canonical_hash(json.loads((primary_release / "configs/study.json").read_text(encoding="utf8"))) != canonical_hash(config):
        raise ValueError("The full configuration differs from the frozen primary configuration")
    if output_dir.exists():
        raise FileExistsError("A feasibility case cannot replace an earlier case")
    proof_path = root / "results/behaviour/streaming_bin_provenance.json"
    proof = json.loads(proof_path.read_text(encoding="utf8"))
    cells, rosters, inputs = structural_cells(config), {}, {str(proof_path): digest(proof_path)}
    for width in sorted({cell["bin_seconds"] for cell in cells}):
        path = root / f"results/behaviour/fly_bins_{width:g}s.parquet"
        checksum = digest(path)
        if proof.get("bin_files", {}).get(str(path)) != checksum:
            raise ValueError("Chronological bin bytes differ from the native ingestion proof")
        rosters[f"{width:g}s"], inputs[str(path)] = _roster(path), checksum
    selected_clocks = {width: _select_native_clock(root / f"results/behaviour/fly_bins_{width:g}s.parquet",
                        rosters[f"{width:g}s"], float(config["behaviour"].get("warmup_seconds", 600)))
                       for width in sorted({cell["bin_seconds"] for cell in cells})}
    frozen = frozen_work_counts(config, rosters)
    for cell in cells:
        selected = selected_clocks[cell["bin_seconds"]]
        cell.update(native_animal_id=selected["animal_id"], native_experiment_id=selected["experiment_id"],
                    native_clock_bins=selected["bins"])
    plan = {"schema_version": 1, "status": "prospective_bounded_operation_pack", **identity,
        "root": str(root), "primary_release": str(primary_release),
        "primary_scientific_kernel_source_sha256": PRIMARY_SOURCE_SHA256,
        "framework_source_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"],
        "framework_module_sha256": digest(Path(__file__)), "full_config_sha256": canonical_hash(config),
        "config": config, "cells": cells, "clock_rosters": rosters,
        "frozen_workload": frozen, "frozen_workload_sha256": canonical_hash(frozen),
        "source_result_hashes": inputs, "learning_rates": config["behaviour"].get("learning_rates", [.0003, .001, .003]),
        "operations": list(OPERATIONS), "missing_categories": list(MISSING_CATEGORIES),
        "optimizer_measurement_scope": {"adam_epochs_per_rate": 1, "lbfgs_iterations_per_rate": 1,
            "frozen_adam_epochs": config["behaviour"].get("max_epochs", 500),
            "frozen_lbfgs_iterations": config["behaviour"].get("lbfgs_iterations", 200),
            "full_optimizer_budget_executed": False,
            "reason": "Measure unchanged per-iteration objectives and original strong-Wolfe search; forecast every requested frozen iteration and start separately."},
        "complete_declared_workload": False, "scientific_fit": False, "admission_authority": False,
        "biological_time_interpretation": "Technical elapsed recording time; no inferred exposure clock or H1/H2 result",
        "selection": "Longest complete native development clock with finite observed targets after the declared warm-up, then animal identity; original observed-mask statistics and exact targets inspected, no eventual outcome selection. All source roster rows remain in cost multiplicities and group dimensions.",
        "scope": "One complete native animal clock per structural/rate cell; group parameter dimensions retain the full roster. Scaling/knots come only from the selected technical probe animal; this is not a full-cohort fit."}
    output_dir.mkdir(parents=True, exist_ok=False)
    atomic_json(output_dir / "plan.json", plan)
    return plan


def _load_plan(path: Path) -> dict[str, Any]:
    plan = json.loads(Path(path).read_text(encoding="utf8"))
    if plan.get("schema_version") != 1 or plan.get("primary_scientific_kernel_source_sha256") != PRIMARY_SOURCE_SHA256:
        raise ValueError("Unrecognized feasibility plan or scientific kernel")
    if canonical_hash(plan["config"]) != plan["full_config_sha256"] or canonical_hash(plan["frozen_workload"]) != plan["frozen_workload_sha256"]:
        raise ValueError("The immutable configuration or prospective workload changed")
    if plan["clock_rosters"] != plan["frozen_workload"]["clock_rosters"]:
        raise ValueError("The native roster differs from the frozen multiplicity roster")
    if plan.get("learning_rates") != plan["config"]["behaviour"].get("learning_rates", [.0003, .001, .003]):
        raise ValueError("Top-level probe learning rates differ from the full frozen configuration")
    expected = structural_cells(plan["config"])
    if len(expected) != len(plan["cells"]) or any(any(actual.get(key) != value for key, value in cell.items())
            for cell, actual in zip(expected, plan["cells"])):
        raise ValueError("The predeclared structural cell roster changed")
    if plan.get("operations") != list(OPERATIONS) or plan.get("complete_declared_workload") is not False:
        raise ValueError("Technical operation pack cannot claim complete scientific coverage")
    for cell in plan["cells"]:
        eligible = [row for row in plan["clock_rosters"][f"{cell['bin_seconds']:g}s"]
                    if row.get("technical_probe_eligible") is True]
        if not eligible:
            raise ValueError("No prospectively validated complete native clock is eligible for a technical probe")
        selected = sorted(eligible,
                          key=lambda row: (-row["bins"], row["animal_id"]))[0]
        if any(cell[key] != selected[column] for key, column in
               (("native_animal_id", "animal_id"), ("native_experiment_id", "experiment_id"), ("native_clock_bins", "bins"))):
            raise ValueError("The outcome-independent complete-clock selection changed")
    return plan


def _worker(plan_path: Path, index: int, attempt: Path) -> None:
    """Called only in an isolated child whose horsehair package is the primary."""
    import horsehair
    import numpy as np
    import pandas as pd
    import torch
    from . import behaviour as b
    from . import semimarkov as s
    from .behaviour import _process_peak_ram_bytes
    plan, identity = _load_plan(plan_path), _native_identity()
    primary = Path(plan["primary_release"]).resolve()
    for module in (horsehair, b, s):
        if not Path(module.__file__).resolve().is_relative_to(primary / "src"):
            raise ValueError("The worker did not import the frozen primary package")
    if source_identity(primary)["sha256"] != PRIMARY_SOURCE_SHA256 or digest(Path(__file__)) != plan["framework_module_sha256"]:
        raise ValueError("The qualified primary or wrapper source changed")
    cell = plan["cells"][index]
    path = Path(plan["root"]) / f"results/behaviour/fly_bins_{cell['bin_seconds']:g}s.parquet"
    if digest(path) != plan["source_result_hashes"][str(path)]:
        raise ValueError("Native bin bytes changed after prospective planning")
    proof_path = Path(plan["root"]) / "results/behaviour/streaming_bin_provenance.json"
    if digest(proof_path) != plan["source_result_hashes"][str(proof_path)]:
        raise ValueError("Native chronological provenance changed after prospective planning")
    frame = pd.read_parquet(path, filters=[("animal_id", "=", cell["native_animal_id"])])
    settings = dict(plan["config"]["behaviour"], modulation=["elapsed_hours"], **{key: value for key, value in cell.items() if key in {"states", "hidden_size", "dropout"}})
    units = b.to_sequences(frame, settings)
    if len(units) != 1 or len(units[0].times) != cell["native_clock_bins"] or units[0].experiment_id != cell["native_experiment_id"]:
        raise ValueError("The selected original clock or identity was shortened or changed")
    sequence, roster = units[0], plan["clock_rosters"][f"{cell['bin_seconds']:g}s"]
    torch.set_num_threads(identity["allocated_cpus"])
    scaler = s.TrainingScaler.fit(units)
    scaled = scaler.transform(sequence)
    state = {"status": "running", **identity, "cell": cell, "operations": [], "rate_results": [],
        "plan_sha256": digest(plan_path), "full_config_sha256": plan["full_config_sha256"],
        "primary_scientific_kernel_source_sha256": PRIMARY_SOURCE_SHA256,
        "framework_source_sha256": plan["framework_source_sha256"], "framework_module_sha256": plan["framework_module_sha256"],
        "full_roster_animals": len(roster), "full_roster_experiments": len({row["experiment_id"] for row in roster}),
        "native_clock_bins": len(sequence.times), "native_input_sha256": plan["source_result_hashes"][str(path)],
        "optimizer_measurement_scope": {"executed_starts_per_rate": 1, "adam_epochs_per_rate": 1,
            "lbfgs_iterations_per_rate": min(1, int(settings.get("lbfgs_iterations", 200))),
            "frozen_requested_starts": settings.get("starts", 5),
            "frozen_requested_adam_epochs": settings.get("max_epochs", 500),
            "frozen_requested_lbfgs_iterations": settings.get("lbfgs_iterations", 200),
            "full_optimizer_budget_executed": False},
        "scientific_fit": False, "complete_declared_workload": False, "admission_authority": False,
        "started_unix": time.time()}
    def persist():
        state["updated_unix"] = time.time()
        state["process_peak_ram_bytes"] = _process_peak_ram_bytes()
        atomic_json(attempt / "progress.json", state)
    def operation(name, rate, callback):
        state["active_operation"] = {"name": name, "learning_rate": rate, "started_unix": time.time()}
        persist()
        started = time.perf_counter()
        value = callback()
        elapsed = time.perf_counter() - started
        state["operations"].append({"name": name, "learning_rate": rate, "elapsed_seconds": elapsed,
            "measured_work_units": 1, "native_clock_bins": len(sequence.times),
            "uses_full_primary_numerics": True})
        state.pop("active_operation", None)
        persist()
        return value
    persist()
    try:
        for rate in plan["learning_rates"]:
            torch.manual_seed(int(settings.get("seed", 0)))
            observed = sequence.observations[sequence.observed]
            output_scale = np.maximum(np.std(observed, axis=0), 1e-4)
            comparator = cell["model"] in {"AR", "GRU"}
            if comparator:
                constructor = {"kind": cell["model"], "dimension": observed.shape[1],
                    "context_features": len(scaler.mean) + len(scaler.modulation_mean) + 2 * observed.shape[1],
                    "hidden_size": cell.get("hidden_size", 32), "dropout": cell.get("dropout", 0)}
                model = b._comparator_network(**constructor)
                dataset = b._history_dataset([scaled], output_scale)
                if not len(dataset):
                    raise ValueError("The full native comparator clock has no observed targets")
                from torch.utils.data import DataLoader
                scale_tensor = torch.as_tensor(output_scale, dtype=torch.float32)
                def closure(shuffle=False):
                    model.zero_grad()
                    prior = .5 * sum((p / 1.5).square().sum() for p in model.parameters()) / len(dataset)
                    prior.backward()
                    total = prior.detach().clone()
                    for history, context, target in DataLoader(dataset, batch_size=int(settings.get("comparator_batch_size", 512)), shuffle=shuffle):
                        mean, chol = model(history, context, scale_tensor)
                        loss = -s.student_t_logpdf(target, mean, chol).sum() / len(dataset)
                        loss.backward()
                        total = total + loss.detach()
                    return total
                # The primary Adam loop updates after every observed mini-batch.
                def adam_epoch():
                    model.train()
                    optimizer = torch.optim.Adam(model.parameters(), lr=float(rate))
                    for history, context, target in DataLoader(dataset, batch_size=int(settings.get("comparator_batch_size", 512)), shuffle=True):
                        optimizer.zero_grad()
                        mean, chol = model(history, context, scale_tensor)
                        prior = .5 * sum((p / 1.5).square().sum() for p in model.parameters()) / len(dataset)
                        loss = -s.student_t_logpdf(target, mean, chol).mean() + prior
                        if not torch.isfinite(loss):
                            raise FloatingPointError("Nonfinite unchanged primary comparator loss")
                        loss.backward()
                        torch.nn.utils.clip_grad_norm_(model.parameters(), 5)
                        optimizer.step()
            else:
                model = s.MovementModel("MX" if cell["model"] == "HMM" else cell["model"],
                    len(scaler.mean), len(scaler.modulation_mean), sequence.observations.shape[1],
                    s.training_duration_knots(units), [row["animal_id"] for row in roster],
                    sorted({row["experiment_id"] for row in roster}), states=cell["states"],
                    max_duration_bins=settings.get("max_duration_bins"), bin_seconds=sequence.bin_seconds)
                if cell["model"] == "HMM":
                    with torch.no_grad(): model.duration_beta.zero_()
                    model.duration_beta.requires_grad_(False)
                with torch.no_grad():
                    model.log_rest_scale.copy_(model._tensor(np.log(output_scale / 3)))
                    for name, parameter in model.named_parameters():
                        if parameter.requires_grad and name not in {"log_rest_scale", "log_animal_sd", "log_experiment_sd"}:
                            parameter.add_(torch.randn_like(parameter) * .05)
                n_observed = int(sequence.observed.sum())
                def closure():
                    model.zero_grad()
                    prior = -model.log_prior() / n_observed
                    prior.backward()
                    loss = -model(scaled)["log_likelihood"].sum() / n_observed
                    if not torch.isfinite(loss):
                        raise FloatingPointError("Nonfinite unchanged primary movement likelihood")
                    loss.backward()
                    return prior.detach() + loss.detach()
                def adam_epoch():
                    optimizer = torch.optim.Adam(model.parameters(), lr=float(rate))
                    closure()
                    torch.nn.utils.clip_grad_norm_(model.parameters(), float(settings.get("gradient_clip", 5)))
                    optimizer.step()
            state["model_parameter_count"] = sum(p.numel() for p in model.parameters())
            operation("adam_epoch", rate, adam_epoch)
            # Measure score/simulation before refinement so an expensive native
            # line search cannot erase coverage of the other finite operations.
            fit = (b.ComparatorFit(model, scaler, output_scale, constructor, {}) if comparator else
                   s.Fit(model, scaler, {"tail_tolerance": settings.get("tail_tolerance", 1e-8)}))
            unseen = copy.deepcopy(sequence)
            unseen.animal_id, unseen.experiment_id = "technical-unseen-animal", "technical-unseen-experiment"
            operation("unseen_group_score", rate, lambda: fit.score(unseen, settings.get("warmup_seconds", 600)))
            operation("causal_full_clock_simulation", rate,
                lambda: b.simulate_comparator(fit, sequence, int(settings.get("seed", 0))) if comparator else
                        s.simulate(model, scaled, int(settings.get("seed", 0))))
            iterations = int(settings.get("lbfgs_iterations", 200))
            if iterations:
                model.eval()
                # One complete original strong-Wolfe iteration is the measured
                # unit. All200 requested iterations remain in frozen counts;
                # a short unit cannot establish full-fit stopping/convergence.
                optimizer = torch.optim.LBFGS(model.parameters(), max_iter=1, line_search_fn="strong_wolfe")
                def measured_closure():
                    return operation("lbfgs_closure", rate, closure)
                operation("lbfgs_iteration", rate, lambda: optimizer.step(measured_closure))
            else:
                state["operations"].append({"name": "lbfgs_iteration", "learning_rate": rate,
                    "elapsed_seconds": 0., "not_requested": True})
            state["rate_results"].append({"learning_rate": rate, "status": "operation_pack_complete"})
            persist()
        state.update(status="operation_pack_complete", completed_unix=time.time())
    except BaseException as error:
        state.update(status="operation_error", error={"type": type(error).__name__, "message": str(error)})
        persist()
        raise
    persist()
    atomic_json(attempt / "worker-result.json", state)


_CHILD = """import importlib.util, pathlib, sys
primary, wrapper, plan, index, attempt = sys.argv[1:]
sys.path.insert(0, str(pathlib.Path(primary) / 'src'))
import horsehair
if not pathlib.Path(horsehair.__file__).resolve().is_relative_to(pathlib.Path(primary).resolve() / 'src'):
    raise RuntimeError('Frozen scientific package isolation failed')
spec = importlib.util.spec_from_file_location('horsehair._bounded_feasibility_wrapper', wrapper)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module._worker(pathlib.Path(plan), int(index), pathlib.Path(attempt))
"""


def run_cell(plan_path: Path, index: int, max_seconds: float = 3000) -> dict[str, Any]:
    from .rna_recovery import _primary_environment
    plan_path = Path(plan_path).resolve()
    plan, identity = _load_plan(plan_path), _native_identity()
    if not 0 <= index < len(plan["cells"]):
        raise ValueError("Cell index is outside the prospective structural roster")
    if digest(Path(__file__)) != plan["framework_module_sha256"]:
        raise ValueError("The operation wrapper changed after planning")
    if source_identity(Path(__file__).resolve().parents[2])["sha256"] != plan["framework_source_sha256"]:
        raise ValueError("The complete qualified wrapper release changed after planning")
    seconds = runtime_limit(max_seconds, allocation_end=os.environ.get("SLURM_JOB_END_TIME"))
    attempt = plan_path.parent / f"cell-{index:03d}-job-{identity['slurm_job_id']}"
    attempt.mkdir(exist_ok=False)
    started = time.time()
    record = {"status": "started", **identity, "cell": plan["cells"][index], "started_unix": started,
        "maximum_worker_seconds": seconds, "plan_sha256": digest(plan_path), "scientific_fit": False,
        "complete_declared_workload": False, "admission_authority": False}
    atomic_json(attempt / "started.json", record)
    environment = _primary_environment({"primary_release": plan["primary_release"],
        "maintenance_release": str(Path(__file__).resolve().parents[2]), "root": plan["root"]})
    command = [sys.executable, "-s", "-c", _CHILD, plan["primary_release"], str(Path(__file__).resolve()),
               str(plan_path), str(index), str(attempt)]
    timed_out = False
    with (attempt / "stdout.log").open("w", encoding="utf8") as stdout, (attempt / "stderr.log").open("w", encoding="utf8") as stderr:
        process = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=environment,
                                   cwd=plan["primary_release"], start_new_session=os.name != "nt")
        try:
            code = process.wait(timeout=seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            if os.name == "nt": process.terminate()
            else: os.killpg(process.pid, signal.SIGTERM)
            try: code = process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                if os.name == "nt": process.kill()
                else: os.killpg(process.pid, signal.SIGKILL)
                code = process.wait(timeout=10)
    progress = json.loads((attempt / "progress.json").read_text(encoding="utf8")) if (attempt / "progress.json").exists() else None
    record.update(status="bounded_deadline_reached" if timed_out else "operation_pack_complete" if code == 0 else "worker_failed",
        worker_returncode=code, elapsed_seconds=time.time() - started, progress=progress, completed_unix=time.time())
    atomic_json(attempt / "receipt.json", record)
    return record


def aggregate(plan_path: Path, output: Path) -> dict[str, Any]:
    """Keep measurements, projections and unmeasured scientific families separate."""
    from .slurm import completed_native_dependency
    plan_path, output = Path(plan_path).resolve(), Path(output)
    plan = _load_plan(plan_path)
    if output.exists(): raise FileExistsError("A feasibility summary cannot replace an earlier summary")
    plan_hash, records = digest(plan_path), []
    for path in sorted(plan_path.parent.glob("cell-*-job-*/receipt.json")):
        value = json.loads(path.read_text(encoding="utf8"))
        if value.get("plan_sha256") != plan_hash:
            raise ValueError("A receipt belongs to another prospective plan")
        proof = completed_native_dependency(Path(plan["root"]), str(value.get("slurm_job_id", "")))
        if proof is None:
            raise ValueError("Every counted numerical receipt requires positive native COMPLETED/0:0 proof")
        cell = value.get("cell")
        if cell not in plan["cells"] or not isinstance(value.get("allocated_cpus"), int) or not 1 <= value["allocated_cpus"] <= 8:
            raise ValueError("A receipt has an undeclared cell or invalid allocation")
        progress = value.get("progress")
        if progress:
            if any(progress.get(key) != expected for key, expected in {
                    "cell": cell, "plan_sha256": plan_hash,
                    "full_config_sha256": plan["full_config_sha256"],
                    "framework_source_sha256": plan["framework_source_sha256"],
                    "framework_module_sha256": plan["framework_module_sha256"],
                    "primary_scientific_kernel_source_sha256": PRIMARY_SOURCE_SHA256,
                    "native_clock_bins": cell["native_clock_bins"],
                    "slurm_job_id": value.get("slurm_job_id"),
                    "allocated_cpus": value["allocated_cpus"]}.items()):
                raise ValueError("A measured operation changed its source, clock or native allocation binding")
            for operation in progress.get("operations", []):
                seconds = operation.get("elapsed_seconds")
                if (operation.get("learning_rate") not in plan["learning_rates"] or
                        operation.get("name") not in {*OPERATIONS, "lbfgs_closure"} or
                        (not operation.get("not_requested") and (operation.get("uses_full_primary_numerics") is not True or
                        operation.get("native_clock_bins") != cell["native_clock_bins"] or
                        not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds <= 0))):
                    raise ValueError("A receipt contains a shortened, unbound or nonfinite numerical operation")
        records.append({"path": str(path), "sha256": digest(path), "receipt": value, "native_completion_proof": proof})
    projections, refinement_projections, missing, coverage = [], [], list(MISSING_CATEGORIES), []
    for cell in plan["cells"]:
        measured = [entry for entry in records if entry["receipt"].get("cell") == cell]
        operations = [operation for entry in measured for operation in (entry["receipt"].get("progress") or {}).get("operations", [])]
        required = {(name, float(rate)) for name in OPERATIONS for rate in plan["learning_rates"]}
        completed = {(operation["name"], float(operation["learning_rate"])) for operation in operations
                     if operation["name"] in OPERATIONS}
        coverage.append({"cell": cell, "completed_operations": len(required & completed), "required_operations": len(required),
                         "missing": sorted(required - completed)})
        counts = [row for row in plan["frozen_workload"]["counts"] if row["model"] == cell["model"] and
            row["bin_seconds"] == cell["bin_seconds"] and all(row.get(key) == cell.get(key) for key in ("states", "hidden_size", "dropout"))]
        if len(counts) != 1: raise ValueError("A structural cell lacks one exact frozen multiplicity row")
        row = counts[0]
        gradient = [operation["elapsed_seconds"] for operation in operations if operation["name"] == "adam_epoch"]
        cpus = [entry["receipt"]["allocated_cpus"] for entry in measured]
        if gradient:
            projection = max(gradient) * row["adam_animal_passes_upper"] * max(cpus) / 3600
            projections.append({"cell": cell, "maximum_complete_clock_adam_epoch_seconds": max(gradient),
                "adam_animal_passes_upper": row["adam_animal_passes_upper"], "cpu_core_hours": projection,
                "rule": "Conservative technical one-animal full-clock Adam epoch times every frozen animal pass; no favourable early stopping assumed. This is a forecast, not observed expenditure or a mathematical lower bound."})
        closures = [operation["elapsed_seconds"] for operation in operations if operation["name"] == "lbfgs_closure"]
        if closures:
            refinement_projections.append({"cell": cell, "maximum_complete_clock_closure_seconds": max(closures),
                "frozen_lbfgs_iterations": plan["config"]["behaviour"].get("lbfgs_iterations", 200),
                "measured_lbfgs_iterations_per_rate": 1,
                "lbfgs_animal_closures_upper": row["lbfgs_animal_closures_upper"],
                "cpu_core_hours": max(closures) * row["lbfgs_animal_closures_upper"] * max(cpus) / 3600,
                "rule": "Maximum measured full-clock original objective/gradient closure times the complete frozen strong-Wolfe closure bound (including every requested iteration and start). A single search iteration does not measure convergence."})
    if any(row["missing"] for row in coverage): missing.insert(0, "Some declared structural/rate operation packs remain unmeasured")
    projected = sum(row["cpu_core_hours"] for row in projections)
    refinement_projected = sum(row["cpu_core_hours"] for row in refinement_projections)
    optimizer_projected = projected + refinement_projected
    cap = float(plan["config"]["resources"]["cpu_core_hours_cap"])
    result = {"status": "measured_partial_projection_exceeds_cap" if optimizer_projected > cap else "pending_full_workload_measurements",
        "plan_sha256": plan_hash, "primary_scientific_kernel_source_sha256": PRIMARY_SOURCE_SHA256,
        "framework_source_sha256": plan["framework_source_sha256"], "full_config_sha256": plan["full_config_sha256"],
        "complete_declared_workload": False, "scientific_fit": False, "admission_authority": False,
        "coverage": coverage, "missing_categories": missing, "adam_only_projections": projections,
        "lbfgs_only_projections": refinement_projections,
        "partial_adam_only_forecast_cpu_core_hours": projected, "cpu_core_hours_cap": cap,
        "partial_lbfgs_only_forecast_cpu_core_hours": refinement_projected,
        "partial_optimizer_forecast_cpu_core_hours": optimizer_projected,
        "receipt_bindings": [{key: row[key] for key in ("path", "sha256")} for row in records],
        "native_completion_proofs": [row["native_completion_proof"] for row in records],
        "no_stopping_claim": "One optimizer epoch and a native line-search probe do not measure convergence of a full-cohort fit.",
        "additional_working_storage": "Not forecast: full-fit checkpoint/output sizes remain unmeasured."}
    atomic_json(output, result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    prepare = sub.add_parser("plan")
    for flag in ("config", "root", "primary-release", "output-dir"): prepare.add_argument("--" + flag, required=True)
    run = sub.add_parser("run")
    run.add_argument("--manifest", required=True)
    run.add_argument("--cell", required=True, type=int)
    run.add_argument("--max-seconds", default=3000, type=float)
    collect = sub.add_parser("aggregate")
    collect.add_argument("--manifest", required=True)
    collect.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    if args.mode == "plan":
        result = prepare_plan(Path(args.config), Path(args.root), Path(args.primary_release), Path(args.output_dir))
    elif args.mode == "run": result = run_cell(Path(args.manifest), args.cell, args.max_seconds)
    else: result = aggregate(Path(args.manifest), Path(args.output))
    print(json.dumps({key: result[key] for key in ("status", "complete_declared_workload", "admission_authority")}, sort_keys=True))


if __name__ == "__main__": main()
