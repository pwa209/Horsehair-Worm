"""Reproducible arithmetic review of technical costs; never resource admission.

Historical receipts are inputs, not editable caches. Corrected multiplicities
and paired CPU costs are scenarios, not observed full-fit stopping or bounds on
future runtime. No biological labels are read or inferred.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any

from .core import atomic_json, canonical_hash, digest, source_identity
from .behaviour_feasibility import MISSING_CATEGORIES, OPERATIONS, PRIMARY_SOURCE_SHA256, structural_cells
from .behaviour_workload import frozen_work_counts


REVIEWED_TORCH_SOURCE = "f181e9224a72596f0f9027cfc42c0bf3819fd136828aef0fdc002a4e94495367"
REVIEWED_METHODS = {
    "LBFGS.__init__": "83c148b90f4ee23930ecfe4a1c5c028c6293b3e85652734399dded11734989c3",
    "LBFGS.step": "cad8270df0a8cc974ad8717a028f162137a7e43a02c7abdc84a2ae005e6c350d",
    "_strong_wolfe": "955d1458c791b38cd961e2ebbb743ffba620bdbf1a5ae1e8f6c135cf68405405",
}


def _require(value: bool, message: str) -> None:
    if not value:
        raise ValueError(message)


def _positive(value: Any, name: str) -> float:
    _require(type(value) in (int, float) and math.isfinite(value) and value > 0,
             "A finite positive " + name + " is required")
    return float(value)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def reviewed_lbfgs_bound(iterations: int, packet: dict[str, Any] | None) -> dict[str, Any]:
    """Tighten only with the complete, exact installed-source packet reviewed here.

    PyTorch2.6 checks its global evaluation budget after a line search. Thus one
    last search can overshoot max_eval by at most25. This changes accounting,
    never an optimizer argument or executed numerical operation.
    """
    _require(type(iterations) is int and iterations >= 0, "Invalid iteration count")
    old = 1 + 26 * iterations if iterations else 0
    refused = {"supported": False, "closure_bound": old,
               "historical_closure_bound": old, "reason": "No reviewed installed-source packet"}
    if packet is None:
        return refused
    try:
        text = packet["source_text"]
        _require(packet["version"] == "2.6.0" and isinstance(text, str)
                 and _sha(text) == packet["source_sha256"] == REVIEWED_TORCH_SOURCE,
                 "Installed Torch module is not the reviewed exact source")
        tree = ast.parse(text)
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "LBFGS")
        nodes = {"LBFGS." + n.name: n for n in cls.body if isinstance(n, ast.FunctionDef)}
        nodes.update({n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)})
        for name, expected in REVIEWED_METHODS.items():
            method = packet["methods"][name]
            _require(_sha(method["source_text"]) == method["source_sha256"] == expected
                     and ast.get_source_segment(text, nodes[name]) == method["source_text"],
                     "Installed optimizer method differs from its module or reviewed bytes")
    except (KeyError, StopIteration, SyntaxError, TypeError, ValueError) as error:
        return dict(refused, reason="Tightening refused: " + str(error))
    max_eval = iterations * 5 // 4
    # The first iteration is entered even when max_eval is already one.
    bound = min(old, max(1, max_eval - 1) + 26) if iterations else 0
    return {"supported": True, "closure_bound": bound,
            "historical_closure_bound": old, "max_eval": max_eval, "max_ls": 25,
            "source_sha256": REVIEWED_TORCH_SOURCE, "method_sha256": REVIEWED_METHODS,
            "reason": "Reviewed global evaluation check plus one last complete line search; no optimizer modification"}


def roster_work_rows(config: dict[str, Any], rosters: dict[str, list[dict[str, Any]]],
                     *, exclude_all_missing: bool = False) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Independently enumerate frozen loops, retaining original recording-local IDs.

    Per-width missingness may differ. We report that fact, rather than silently
    intersecting scale rosters or asserting animal independence.
    """
    s = config["behaviour"]
    starts, final, epochs = (int(s.get(k, v)) for k, v in
                             (("starts", 5), ("final_starts", 20), ("max_epochs", 500)))
    rates, folds = s.get("learning_rates", [.0003, .001, .003]), int(s.get("inner_folds", 3))
    _require(min(starts, final, epochs) > 0 and folds >= 2 and bool(rates), "Invalid frozen loop dimensions")
    rows, summaries, memberships = [], {}, []
    for width in sorted({c["bin_seconds"] for c in structural_cells(config)}):
        original = rosters[f"{width:g}s"]
        _require(bool(original) and len({r["animal_id"] for r in original}) == len(original), "Duplicated or empty ROI roster")
        if exclude_all_missing:
            _require(all(type(r.get("observed_any")) is bool for r in original), "All original observed_any statistics are required")
        selected = [r for r in original if not exclude_all_missing or r["observed_any"]]
        _require(bool(selected) and all(type(r["bins"]) is int and r["bins"] >= 2 for r in selected), "No valid complete technical clocks")
        groups = sorted({r["experiment_id"] for r in selected})
        _require(len(groups) >= 3, "Missingness leaves fewer than three recording groups")
        counts = {g: sum(r["experiment_id"] == g for r in selected) for g in groups}
        memberships.append({(r["animal_id"], r["experiment_id"]) for r in selected})
        inner_train = inner_test = inner_fits = 0
        for outer in groups:
            train = [g for g in groups if g != outer]
            k = min(folds, len(train))
            for fold in range(k):
                held = set(train[fold::k])
                inner_train += sum(counts[g] for g in train if g not in held)
                inner_test += sum(counts[g] for g in held)
                inner_fits += 1
        n, outer = len(selected), len(selected) * (len(groups) - 1)
        summaries[f"{width:g}s"] = {
            "nominal_recording_rois": len(original), "retained_recording_rois": n,
            "excluded_all_missing_ids": [r["animal_id"] for r in original if r not in selected],
            "recording_groups": counts, "inner_training_roi_passes_per_start_rate": inner_train,
            "inner_validation_roi_passes_per_start_rate": inner_test, "inner_fits_per_structure_rate": inner_fits,
            "outer_training_roi_passes_per_start": outer,
            "identity_interpretation": "Original recording-local source keys; no cross-session animal independence claim"}
        for cell in [c for c in structural_cells(config) if c["bin_seconds"] == width]:
            model = cell["model"]
            retry = starts if model in {"AR", "GRU"} else starts + final
            inner = inner_train * len(rates) * starts
            outer_units = outer * retry
            freeze = n * retry if width == float(s.get("primary_bin_seconds", 10)) and model in {"MG", "MT", "MX", "GRU"} else 0
            recovery = 0
            if width == float(s.get("primary_bin_seconds", 10)) and model in s.get("models", ["M0", "MG", "MT", "MD", "MX"]):
                _require(s.get("state_counts", [2]) == [2], "Recovery structural counting beyond frozen two-state design is unreviewed")
                truth = (len(s.get("recovery_models", ["MG", "MT", "MD", "MX"])) *
                         len(s.get("recovery_effects", [0, .25, .5, 1])) * int(s.get("parameter_recovery_replicates", 100)))
                recovery = truth * n * (min(folds, len(groups)) - 1) * starts
            rows.append({"cell": cell, "inner_roi_starts": inner, "outer_roi_starts": outer_units,
                         "freeze_roi_starts": freeze, "recovery_roi_starts": recovery,
                         "training_roi_passes_per_epoch": inner + outer_units + freeze + recovery,
                         "adam_epochs_upper": epochs})
    return rows, {"by_bin": summaries, "same_retained_recording_roster_across_bins":
                  all(m == memberships[0] for m in memberships),
                  "remaining_eligibility": "Observed-mask presence alone does not validate finite targets, post-warmup scoring, exposure, or biological identities"}


def _proof(proof: dict[str, Any], receipt: dict[str, Any], root: str) -> None:
    job = str(receipt["slurm_job_id"])
    _require(re.fullmatch(r"\d+(?:_\d+)?", job) is not None and proof["job_id"] == job
             and proof.get("array") is None and proof["dependency_kind"] == "afterok", "Receipt native identity changed")
    _require(Path(proof["source_path"]).resolve() == (Path(root) / "state/resource-budget.json").resolve(), "Native proof belongs to another root")
    records = proof["native_records"]
    _require(set(records) == {job} and canonical_hash(records) == proof["native_records_sha256"]
             and canonical_hash({k: v for k, v in proof.items() if k != "completion_proof_sha256"}) == proof["completion_proof_sha256"], "Native proof hash changed")
    rec = records[job]
    _require(rec["state"] == "COMPLETED" and rec["exit_code"] == "0:0"
             and rec["basis"] == "sacct final allocated wall time"
             and type(rec["cpus"]) in (int, float) and rec["cpus"] == receipt["allocated_cpus"], "Positive completed native allocation is required")
    _require(type(rec.get("gpus")) in (int, float) and math.isfinite(rec["gpus"])
             and rec["gpus"] == 0, "A CPU operator review requires a native zero-GPU allocation")
    _positive(rec["elapsed_seconds"], "native elapsed time")


def _measurements(plan: dict[str, Any], records: list[dict[str, Any]], plan_sha: str) -> tuple[dict[int, list[dict[str, Any]]], list[dict[str, Any]]]:
    by_cell = {cell["index"]: [] for cell in plan["cells"]}
    seen_jobs, coverage = set(), []
    for bound in records:
        r, proof = bound["receipt"], bound["native_completion_proof"]
        _require(r["plan_sha256"] == plan_sha and r["cell"] in plan["cells"], "Receipt changed prospective plan/cell")
        _require(all(r.get(k) is False for k in ("scientific_fit", "complete_declared_workload", "admission_authority")),
                 "A cost receipt cannot claim scientific or admission authority")
        _require(r["slurm_job_id"] not in seen_jobs and type(r["allocated_cpus"]) is int and 1 <= r["allocated_cpus"] <= 8, "Duplicated job or invalid CPU allocation")
        seen_jobs.add(r["slurm_job_id"])
        _proof(proof, r, plan["root"])
        progress = r.get("progress")
        if progress is None:
            continue
        expected = {"cell": r["cell"], "plan_sha256": plan_sha,
                    "full_config_sha256": plan["full_config_sha256"],
                    "framework_source_sha256": plan["framework_source_sha256"],
                    "framework_module_sha256": plan["framework_module_sha256"],
                    "primary_scientific_kernel_source_sha256": plan["primary_scientific_kernel_source_sha256"],
                    "native_clock_bins": r["cell"]["native_clock_bins"], "slurm_job_id": r["slurm_job_id"],
                    "allocated_cpus": r["allocated_cpus"]}
        _require(all(progress.get(k) == v for k, v in expected.items()), "Measured source/clock/allocation binding changed")
        _require(all(progress.get(k) is False for k in ("scientific_fit", "complete_declared_workload", "admission_authority")),
                 "Measured progress cannot claim scientific or admission authority")
        bin_path = str(Path(plan["root"]) / f"results/behaviour/fly_bins_{r['cell']['bin_seconds']:g}s.parquet")
        _require(progress.get("native_input_sha256") == plan["source_result_hashes"].get(bin_path)
                 and isinstance(progress.get("native_input_sha256"), str), "Original native bin hash binding changed")
        for op in progress.get("operations", []):
            _require(op["name"] in {*OPERATIONS, "lbfgs_closure"} and op["learning_rate"] in plan["learning_rates"], "Undeclared operation/rate")
            if op.get("not_requested"):
                _require(op["name"] == "lbfgs_iteration" and plan["config"]["behaviour"].get("lbfgs_iterations", 200) == 0, "Requested operation cannot be skipped")
                continue
            seconds = _positive(op["elapsed_seconds"], "operation duration")
            _require(op.get("uses_full_primary_numerics") is True and op["native_clock_bins"] == r["cell"]["native_clock_bins"], "Shortened or different numerical operation")
            by_cell[r["cell"]["index"]].append({"operation": op["name"], "learning_rate": op["learning_rate"],
                "elapsed_seconds": seconds, "allocated_cpus": r["allocated_cpus"],
                "allocated_cpu_seconds": seconds * r["allocated_cpus"],
                "receipt_path": bound["path"], "receipt_sha256": bound["sha256"], "native_job_id": r["slurm_job_id"]})
    required = {(name, rate) for name in OPERATIONS for rate in plan["learning_rates"]}
    for cell in plan["cells"]:
        complete = {(r["operation"], r["learning_rate"]) for r in by_cell[cell["index"]]}
        coverage.append({"cell": cell, "required_operations": len(required),
                         "completed_operations": len(required & complete), "missing": sorted(required - complete)})
    return by_cell, coverage


def _costs(measured: dict[int, list[dict[str, Any]]], *, historical: bool = False) -> dict[int, dict[str, float]]:
    result = {}
    for idx, rows in measured.items():
        cpus = max((r["allocated_cpus"] for r in rows), default=0)
        result[idx] = {}
        for name in ("adam_epoch", "lbfgs_closure"):
            values = [r for r in rows if r["operation"] == name]
            if values:
                result[idx][name] = (max(r["elapsed_seconds"] for r in values) * cpus if historical else
                                     max(r["allocated_cpu_seconds"] for r in values))
    return result


def _scenario(rows: list[dict[str, Any]], costs: dict[int, dict[str, float]], closure_bound: int,
              *, select_one_outer_shape: bool = False) -> dict[str, Any]:
    adam = lbfgs = 0.
    groups: dict[tuple[str, float], list[dict[str, Any]]] = {}
    missing = []
    for row in rows:
        groups.setdefault((row["cell"]["model"], row["cell"]["bin_seconds"]), []).append(row)
        available = costs[row["cell"]["index"]]
        if "adam_epoch" not in available or closure_bound and "lbfgs_closure" not in available:
            missing.append(row["cell"]["index"])
    for group in groups.values():
        for op, factor in (("adam_epoch", group[0]["adam_epochs_upper"]), ("lbfgs_closure", closure_bound)):
            if not factor:
                continue
            values = [(row, costs[row["cell"]["index"]].get(op)) for row in group]
            amount = sum(cost * (row["inner_roi_starts"] + row["recovery_roi_starts"] +
                         (0 if select_one_outer_shape else row["outer_roi_starts"] + row["freeze_roi_starts"]))
                         for row, cost in values if cost is not None)
            if select_one_outer_shape:
                # One unknown winner per outer fold/freeze. Use the slowest
                # measured structural candidate, never a selected outcome.
                amount += max((cost * (row["outer_roi_starts"] + row["freeze_roi_starts"])
                               for row, cost in values if cost is not None), default=0.)
            if op == "adam_epoch": adam += factor * amount / 3600
            else: lbfgs += factor * amount / 3600
    return {"partial_adam_cpu_core_hours": adam, "partial_lbfgs_cpu_core_hours": lbfgs,
            "partial_optimizer_cpu_core_hours": adam + lbfgs, "missing_measured_cells": missing,
            "selected_outer_and_freeze_shapes_counted_once": select_one_outer_shape,
            "lbfgs_closures_per_start_upper": closure_bound,
            "interpretation": "Requested-budget multiplicities times measured maximum operator costs; empirical scenario, not a proven runtime maximum, mathematical lower bound, or observed expenditure"}


def review_documents(plan: dict[str, Any], aggregate: dict[str, Any], records: list[dict[str, Any]],
                     *, plan_sha256: str, torch_packet: dict[str, Any] | None = None,
                     historical_paths: set[str] | None = None) -> dict[str, Any]:
    """Pure arithmetic API. File bindings and unchanged inputs are enforced below."""
    _require(plan.get("primary_scientific_kernel_source_sha256") == PRIMARY_SOURCE_SHA256,
             "The reviewed work loops require the exact frozen primary scientific source")
    _require(canonical_hash(plan["config"]) == plan["full_config_sha256"]
             and canonical_hash(plan["frozen_workload"]) == plan["frozen_workload_sha256"]
             and plan["clock_rosters"] == plan["frozen_workload"]["clock_rosters"], "Plan configuration/workload binding changed")
    expected = frozen_work_counts(plan["config"], plan["clock_rosters"])
    _require(expected == plan["frozen_workload"], "Frozen multiplicity replay disagrees with the original counter")
    _require(plan["learning_rates"] == plan["config"]["behaviour"].get("learning_rates", [.0003, .001, .003]), "Probe learning rates changed")
    declared = structural_cells(plan["config"])
    _require(len(declared) == len(plan["cells"]) and all(all(a.get(k) == v for k, v in e.items())
             for e, a in zip(declared, plan["cells"])), "Structural cell roster changed")
    _require(aggregate["plan_sha256"] == plan_sha256 and all(aggregate[k] == plan[k] for k in
             ("full_config_sha256", "framework_source_sha256", "primary_scientific_kernel_source_sha256")), "Historical aggregate source/plan changed")
    _require(all(aggregate.get(k) is False and plan.get(k) is False for k in
             ("complete_declared_workload", "admission_authority", "scientific_fit")), "A technical review cannot inherit full-study authority")
    measured, coverage = _measurements(plan, records, plan_sha256)
    historical_paths = historical_paths if historical_paths is not None else {r["path"] for r in records}
    old_measurements, old_coverage = _measurements(plan, [r for r in records if r["path"] in historical_paths], plan_sha256)
    _require(canonical_hash(old_coverage) == canonical_hash(aggregate["coverage"]), "Historical operation coverage does not replay")
    rows, roster = roster_work_rows(plan["config"], plan["clock_rosters"])
    filtered, filtered_roster = roster_work_rows(plan["config"], plan["clock_rosters"], exclude_all_missing=True)
    iterations = int(plan["config"]["behaviour"].get("lbfgs_iterations", 200))
    policy = reviewed_lbfgs_bound(iterations, torch_packet)
    replay = _scenario(rows, _costs(old_measurements, historical=True), policy["historical_closure_bound"])
    for our, their in (("partial_adam_cpu_core_hours", "partial_adam_only_forecast_cpu_core_hours"),
                      ("partial_lbfgs_cpu_core_hours", "partial_lbfgs_only_forecast_cpu_core_hours"),
                      ("partial_optimizer_cpu_core_hours", "partial_optimizer_forecast_cpu_core_hours")):
        _require(math.isclose(replay[our], _positive(aggregate[their], their), rel_tol=1e-11, abs_tol=1e-6), "Historical forecast arithmetic failed to replay")
    excess = []
    for key in sorted({(r["cell"]["model"], r["cell"]["bin_seconds"]) for r in rows}):
        shapes = [r for r in rows if (r["cell"]["model"], r["cell"]["bin_seconds"]) == key]
        excess.append({"model": key[0], "bin_seconds": key[1], "structural_candidates": len(shapes),
                       "excess_outer_roi_starts_in_historical_upper": sum(r["outer_roi_starts"] for r in shapes) - max(r["outer_roi_starts"] for r in shapes),
                       "excess_freeze_roi_starts_in_historical_upper": sum(r["freeze_roi_starts"] for r in shapes) - max(r["freeze_roi_starts"] for r in shapes)})
    paired = _costs(measured)
    scenarios = {
        "paired_cpu_original_nominal_roster": _scenario(rows, paired, policy["historical_closure_bound"]),
        "paired_cpu_source_all_missing_exclusions": _scenario(filtered, paired, policy["historical_closure_bound"]),
        "paired_cpu_exclusions_one_selected_outer_shape_reviewed_lbfgs":
            _scenario(filtered, paired, policy["closure_bound"], select_one_outer_shape=True)}
    pending = list(MISSING_CATEGORIES) + ["Full-fit checkpoint/output storage and allocation overhead",
        "Full-cohort prior, optimizer and likelihood costs separately measured at actual fold dimensions",
        "Per-start convergence and runtime distributions; one probe start is not all five starts"]
    if any(c["missing"] for c in coverage): pending.insert(0, "Some declared structural/rate operations remain unmeasured")
    return {"schema_version": 1, "status": "arithmetic_review_only", "complete_declared_workload": False,
        "scientific_result": False, "scientific_fit": False, "admission_authority": False,
        "plan_sha256": plan_sha256, "full_config_sha256": plan["full_config_sha256"],
        "primary_scientific_kernel_source_sha256": plan["primary_scientific_kernel_source_sha256"],
        "historical_framework_source_sha256": plan["framework_source_sha256"],
        "original_native_source_result_hashes": plan["source_result_hashes"],
        "historical_arithmetic_replay": replay, "coverage": coverage,
        "completed_required_operations": sum(c["completed_operations"] for c in coverage),
        "required_operations": sum(c["required_operations"] for c in coverage),
        "native_proof_and_receipt_bindings": [{k: r[k] for k in ("path", "sha256", "native_completion_proof")} for r in records],
        "operation_cost_bindings": [r for rows_ in measured.values() for r in rows_],
        "nominal_roster_count_review": roster, "source_missingness_count_review": filtered_roster,
        "selected_shape_overcount": excess, "reviewed_lbfgs_policy": policy, "scenarios": scenarios,
        "cpu_core_hours_cap": plan["config"]["resources"]["cpu_core_hours_cap"], "pending": pending,
        "nuisance_cost_limitation": "Single-clock timings include full nominal-roster nuisance prior/optimizer work. Multiplying this combined cost by every ROI repeats cohort overhead; no unmeasured overhead discount is applied.",
        "primary_runtime_limitation": "Historical eight-CPU timings remain allocated-wall-time costs. They cannot be divided by eight or replaced by TotalCPU. Mixed receipts use each operation's own allocated CPU count.",
        "biological_scope": "Recording clocks and source missingness only; no Rx, exposure, independence, or biological gate inference"}


def review_paths(plan_path: Path, plan_sha256: str, aggregate_path: Path, aggregate_sha256: str,
                 output: Path, *, torch_packet_path: Path | None = None, torch_packet_sha256: str | None = None,
                 additional_aggregates: list[tuple[Path, str]] | None = None) -> dict[str, Any]:
    """Review one immutable plan; additional aggregates must use that same plan.

    A separately prepared plan (including a different framework release) needs
    its own review. Receipts are never transplanted between plan identities.
    """
    output = Path(output)
    _require(not output.exists(), "An arithmetic review cannot replace an earlier artifact")
    bindings: dict[str, str] = {}
    def load(path, expected):
        original = Path(path)
        path = original.resolve()
        _require(not original.is_symlink() and path.is_file() and digest(path) == expected, "Input hash or regular file binding changed: " + str(path))
        _require(str(path) not in bindings or bindings[str(path)] == expected, "Conflicting input hashes")
        bindings[str(path)] = expected
        return json.loads(path.read_text(encoding="utf8"))
    plan, aggregate = load(plan_path, plan_sha256), load(aggregate_path, aggregate_sha256)
    records, seen = [], {}
    historical = {b["path"] for b in aggregate["receipt_bindings"]}
    for a in [aggregate, *(load(p, sha) for p, sha in (additional_aggregates or []))]:
        _require(a["plan_sha256"] == plan_sha256 and all(a[k] == plan[k] for k in
                 ("full_config_sha256", "framework_source_sha256", "primary_scientific_kernel_source_sha256"))
                 and all(a.get(k) is False for k in ("complete_declared_workload", "scientific_fit", "admission_authority")),
                 "Supplemental aggregate belongs to another plan/source or claims full authority")
        proofs = {p["job_id"]: p for p in a["native_completion_proofs"]}
        _require(len(proofs) == len(a["native_completion_proofs"]), "Duplicated native proof")
        for b in a["receipt_bindings"]:
            r = load(b["path"], b["sha256"])
            if b["path"] in seen:
                _require(seen[b["path"]] == b["sha256"], "Receipt changed between aggregates")
                continue
            seen[b["path"]] = b["sha256"]
            records.append({**b, "receipt": r, "native_completion_proof": proofs[r["slurm_job_id"]]})
    _require((torch_packet_path is None) == (torch_packet_sha256 is None), "Torch packet path and expected hash must be supplied together")
    packet = load(torch_packet_path, torch_packet_sha256) if torch_packet_path is not None else None
    result = review_documents(plan, aggregate, records, plan_sha256=plan_sha256,
                              torch_packet=packet, historical_paths=historical)
    result["input_bindings"] = bindings
    result["review_module_sha256"] = digest(Path(__file__))
    result["review_framework_source_sha256"] = source_identity(Path(__file__).resolve().parents[2])["sha256"]
    for path, sha in bindings.items(): _require(digest(Path(path)) == sha, "Input changed during review")
    atomic_json(output, result)
    return result


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("plan", "plan-sha256", "aggregate", "aggregate-sha256", "output"): p.add_argument("--" + name, required=True)
    p.add_argument("--torch-source-packet"); p.add_argument("--torch-source-packet-sha256")
    p.add_argument("--additional-aggregate", action="append", default=[])
    p.add_argument("--additional-aggregate-sha256", action="append", default=[])
    a = p.parse_args(argv)
    _require(len(a.additional_aggregate) == len(a.additional_aggregate_sha256), "Each additional aggregate requires its expected hash")
    result = review_paths(Path(a.plan), a.plan_sha256, Path(a.aggregate), a.aggregate_sha256, Path(a.output),
        torch_packet_path=Path(a.torch_source_packet) if a.torch_source_packet else None,
        torch_packet_sha256=a.torch_source_packet_sha256,
        additional_aggregates=list(zip(map(Path, a.additional_aggregate), a.additional_aggregate_sha256)))
    print(json.dumps({"status": result["status"], "complete_declared_workload": False, "admission_authority": False}, sort_keys=True))


if __name__ == "__main__": main()
