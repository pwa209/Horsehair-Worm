"""Prospective phase admission from native benchmarks, accounting and quotas.

Missing measurements stay pending. This module never substitutes a synthetic
benchmark, a requested walltime, or a GPU timing for a CPU measurement.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .core import atomic_json, canonical_hash, digest, read_json, source_identity

PHASES = ("rna-full", "agents-development", "agents-final", "behaviour", "molecular")


class AdmissionError(ValueError):
    pass


def positive(value, name):
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise AdmissionError(f"Missing positive measured {name}")
    return value


def memory_bytes(value):
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([KMGT]?)", str(value).strip(), re.I)
    if not match:
        raise AdmissionError(f"Native peak RSS is unavailable: {value!r}")
    return positive(match[1], "peak RSS") * 1024 ** (" KMGT".index(match[2].upper()) if match[2] else 0)


def parse_sacct(text):
    """Parse `sacct -P --units=K`, joining allocation and step peak RSS."""
    rows = list(csv.DictReader(io.StringIO(text), delimiter="|"))
    required = {"JobIDRaw", "State", "ElapsedRaw", "AllocCPUS", "MaxRSS", "AllocTRES"}
    if not rows or not required.issubset(rows[0]):
        raise AdmissionError("Native sacct must contain JobIDRaw/State/ElapsedRaw/AllocCPUS/MaxRSS/AllocTRES")
    jobs = {}; aliases = {}
    for row in rows:
        raw_id = row["JobIDRaw"].split(".", 1)[0]
        # Older non-array evidence remains readable. New native evidence binds
        # both identities; JobIDRaw alone cannot identify an array task.
        job_id = row.get("JobID", row["JobIDRaw"]).split(".", 1)[0]
        if not re.fullmatch(r"\d+(?:_\d+)?", job_id):
            raise AdmissionError("Invalid native job identity")
        if not re.fullmatch(r"\d+", raw_id):
            raise AdmissionError("Physical JobIDRaw must be numeric; logical array JobID is required separately")
        if raw_id in aliases and aliases[raw_id] != job_id:
            raise AdmissionError("Physical allocation has conflicting logical identities")
        aliases[raw_id] = job_id
        record = jobs.setdefault(job_id, {"peak_rss_bytes": None,"job_id_raw":raw_id})
        if record["job_id_raw"] != raw_id:
            raise AdmissionError("Logical allocation has conflicting physical identities")
        if row["MaxRSS"]:
            peak = memory_bytes(row["MaxRSS"])
            record["peak_rss_bytes"] = max(peak, record["peak_rss_bytes"] or 0)
        if "." not in row.get("JobID", row["JobIDRaw"]):
            gpu = re.search(r"(?:^|,)gres/gpu(?:[:=][^,=]+)?=(\d+)(?:,|$)", row["AllocTRES"])
            elapsed, cpus = float(row["ElapsedRaw"]), int(row["AllocCPUS"])
            if not math.isfinite(elapsed) or elapsed < 0 or cpus < 0:
                raise AdmissionError("Invalid native elapsed time or allocated CPU count")
            allocation = dict(job_id=job_id, state=row["State"].split()[0].split('+')[0], elapsed_seconds=elapsed, cpus=cpus, gpus=int(gpu[1]) if gpu else 0, alloc_tres=row["AllocTRES"],exit_code=row.get("ExitCode", ""))
            if "elapsed_seconds" in record and any(record[key]!=value for key,value in allocation.items()):
                raise AdmissionError("Conflicting duplicate native allocation records")
            record.update(allocation)
    return jobs


def _native(root):
    path = root / "state" / "native-accounting.tsv"
    if not path.is_file():
        raise AdmissionError("Completed native sacct evidence is pending: state/native-accounting.tsv")
    return parse_sacct(path.read_text()), path


def _proof(record, config, identity, native, *, require_cuda=False):
    if record.get("full_config_sha256", record.get("config_sha256")) != canonical_hash(config):
        raise AdmissionError("Benchmark belongs to another full scientific configuration")
    if record.get("source_sha256") != identity:
        raise AdmissionError("Benchmark belongs to another immutable source release")
    job_id = str(record.get("slurm_job_id", ""))
    job = native.get(job_id)
    if not job or job.get("state") != "COMPLETED" or job.get("exit_code") not in (None,"","0:0") or not job.get("peak_rss_bytes"):
        raise AdmissionError(f"Completed native timing/peak RSS is pending for benchmark job {job_id}")
    positive(job["elapsed_seconds"], "selected native job elapsed time")
    positive(job["cpus"], "selected native allocated CPUs")
    if require_cuda and (job["gpus"] != 1 or not str(record.get("device", "")).startswith("cuda")):
        raise AdmissionError("GPU training benchmark must be measured on a single allocated CUDA device")
    return job


def _ledger(root):
    path = root / "state" / "resource-budget.json"
    if not path.is_file():
        raise AdmissionError("Measured spent/reserved resource ledger is pending")
    ledger = read_json(path)
    cpu, gpu = float(ledger["spent_cpu_core_hours"]), float(ledger["spent_gpu_hours"])
    if not all(math.isfinite(x) and x >= 0 for x in [cpu, gpu]):
        raise AdmissionError("Native resource ledger contains invalid spent hours")
    for lease in ledger["leases"].values():
        if lease["state"] == "running":
            cpu += positive(lease["max_hours"], "active reservation hours") * int(lease["cpus"])
            gpu += float(lease["max_hours"]) * int(lease["gpus"])
    return cpu, gpu, path


def _storage(root, config, forecast_bytes, forecast_files):
    path = root / "state" / "storage-budget.json"
    if not path.is_file():
        raise AdmissionError("Measured current study occupancy/personal quota is pending: state/storage-budget.json")
    record = read_json(path)
    if record.get("status") != "measured" or Path(record.get("root", "")).resolve() != root.resolve() or not record.get("native_source_paths"):
        raise AdmissionError("Personal scratch/storage evidence is not a measured record for this study root")
    captured = datetime.fromisoformat(record.get("captured_utc", "").replace("Z", "+00:00"))
    if captured.tzinfo is None or not 0 <= (datetime.now(timezone.utc)-captured).total_seconds() <= 86400:
        raise AdmissionError("Personal quota/storage measurement is older than24hours or lacks UTC provenance")
    source_paths = list(map(Path, record["native_source_paths"]))
    if not all(p.is_file() and record.get("native_source_hashes", {}).get(str(p.resolve())) == digest(p) for p in source_paths):
        raise AdmissionError("Personal quota source evidence is missing or changed")
    occupied = positive(record["study_bytes"], "study occupied bytes")
    available = positive(record["personal_available_bytes"], "personal available scratch bytes")
    files = int(positive(record["study_files"], "study occupied files"))
    available_files = int(positive(record["personal_available_files"], "personal available files"))
    resources = config["resources"]
    future = occupied + forecast_bytes
    if future > resources["scratch_budget_bytes"] or forecast_bytes + resources.get("scratch_reserve_bytes", 0) > available:
        raise AdmissionError("Measured working-storage forecast exceeds the study/personal scratch byte ceiling")
    if files + forecast_files > resources["scratch_budget_files"] or forecast_files > available_files:
        raise AdmissionError("Measured working-file forecast exceeds the study/personal file ceiling")
    return future, files + forecast_files, [path, *source_paths]


def _parameters(seconds, rss, cpus, *, gpu=False, concurrency=1):
    return {"cpus": int(cpus), "memory": f"{max(1, math.ceil(rss * 1.3 / 1024**3))}G", "hours": max(1, math.ceil(seconds / 3600)), "gpu": gpu, "concurrency": int(concurrency)}


def _rna(config, root, native, identity, safety):
    from .acquire import validate_manifest
    manifest_path = root / "manifests" / "source_manifest.json"
    manifest = read_json(manifest_path)
    validate_manifest(manifest)
    table = pd.DataFrame(manifest["sources"]["ant_rna"]["crosswalk"])
    table = table[table.included.astype(str).str.lower().isin(["true", "1"])]
    depth = table.groupby("biological_id").read_count.apply(lambda values: pd.to_numeric(values, errors="raise").sum())
    if len(depth) != 60 or (depth <= 0).any():
        raise AdmissionError("Full RNA forecast requires the 60 validated positive-depth brains")
    records, paths = [], [manifest_path]
    for index in [0, 1]:
        path = root / "state" / f"rna-pilot-{index}.json"
        if not path.is_file():
            raise AdmissionError("Both completed native ant RNA pilot brains are required")
        record = read_json(path)
        job = _proof(record, config, identity, native)
        if record.get("status") != "pilot_processed" or record.get("qc", {}).get("status") != "passed" or record.get("source_manifest_hash") != manifest["manifest_hash"]:
            raise AdmissionError("RNA pilot requires completed, source-matched, QC-passing native processing")
        if record["biological_id"] not in depth or int(record["read_count"]) != int(depth[record["biological_id"]]):
            raise AdmissionError("RNA pilot read depth differs from frozen biological metadata")
        records.append((record, job))
        paths.append(path)
    if records[0][0]["biological_id"] == records[1][0]["biological_id"]:
        raise AdmissionError("RNA pilot biological units are duplicated")
    seconds_per_read = max(max(positive(rec["elapsed_seconds"], "RNA runtime"), job["elapsed_seconds"]) / rec["read_count"] for rec, job in records)
    bytes_per_read = max(positive(rec["working_bytes"], "RNA expanded bytes") / rec["read_count"] for rec, _ in records)
    if any("working_files" not in rec for rec, _ in records):
        raise AdmissionError("RNA pilot expanded file-count measurement is pending")
    files_per_brain = max(int(positive(rec["working_files"], "RNA working files")) for rec, _ in records)
    cpus = max(job["cpus"] for _, job in records)
    planned_cpus = int(config["resources"]["rna_cores"])
    if cpus != planned_cpus:
        raise AdmissionError("RNA forecast CPU allocation differs from measured pilot allocation")
    # The full finite RNA plan currently includes all sixty jobs. Counting both
    # pilot brains again is conservative and avoids assuming an unverified cache.
    remaining = depth
    seconds = safety * seconds_per_read * remaining.sum()
    maximum = safety * seconds_per_read * remaining.max()
    rss = max(job["peak_rss_bytes"] for _, job in records)
    concurrency = int(config["resources"]["rna_concurrency"])
    return {"cpu_hours": seconds * cpus / 3600, "gpu_hours": 0., "additional_bytes": math.ceil(safety * bytes_per_read * remaining.sum()), "additional_files": len(remaining) * files_per_brain, "parameters": {"rna-full": _parameters(maximum, rss, cpus, concurrency=concurrency)}, "workload": {"biological_brains": 60, "remaining_brains": len(remaining), "read_count_scaled": True, "forecast_rule": "maximum observed seconds/bytes per deposited read across fixed low/high-depth native pilot brains"}, "inputs": paths}


def _agents(config, root, native, identity, safety, phase):
    from .agents import _cells, _directory, benchmark_cells, benchmark_specification, _benchmark_checked, _validate_benchmark_grid, _implementation_sha256, h4_specification, _digest, MECHANISMS, _cell_name
    settings = config["agents"]
    directory = _directory(root, settings)
    spec = benchmark_specification(config)
    cells = benchmark_cells(config)
    records, jobs, paths = [], [], []
    for task, architecture, seeds in cells:
        for seed in seeds:
            path = directory / "benchmark-training" / f"{task}_{architecture}_{seed}" / "training_benchmark.json"
            if not path.is_file():
                raise AdmissionError(f"Native GPU timing coverage is pending for {task}/{architecture}/seed{seed}")
            try:
                record = _benchmark_checked(config,path,"training")
            except RuntimeError as error:
                raise AdmissionError(str(error)) from error
            if (record.get("task"),record.get("architecture"),record.get("seed")) != (task,architecture,seed):
                raise AdmissionError("Native training benchmark cell is missing or duplicated")
            job = _proof(record,config,identity,native,require_cuda=True)
            if record.get("allocated_cpus") != job["cpus"] or record.get("training_steps") != spec["training_steps"] or record.get("full_primary_algorithm") is not True or record.get("software_qualified") is not True:
                raise AdmissionError("Training timing is not a completed source-matched primary PPO algorithm measurement")
            for key in ["steps_per_second","seconds","training_seconds","direction_selection_seconds","working_bytes","working_files"]:
                positive(record[key],f"agent {key}")
            records.append(record);jobs.append(job);paths.append(path)
    # Account for all overhead in each actual allocation without treating both
    # seeds in that allocation as if each occupied its entire walltime.
    inflation = {}
    for job_id in {job["job_id"] for job in jobs}:
        measured = sum(rec["seconds"]+rec["direction_selection_seconds"] for rec in records if str(rec["slurm_job_id"])==job_id)
        inflation[job_id] = max(1.,native[job_id]["elapsed_seconds"]/measured)
    cell_training, cell_selection = {}, {}
    for record in records:
        key = record["task"],record["architecture"]
        scale = inflation[str(record["slurm_job_id"])]
        elapsed = max(record["seconds"],record["training_seconds"])*scale
        cell_training[key] = max(cell_training.get(key,0.),safety*settings["total_steps"]*elapsed/record["training_steps"])
        cell_selection[key] = max(cell_selection.get(key,0.),safety*record["direction_selection_seconds"]*scale)
    cpus = max(job["cpus"] for job in jobs)
    rss = max(job["peak_rss_bytes"] for job in jobs)
    development_fits = len(_cells(settings, development=True))
    final_fits = len(_cells(settings))
    eligible_arch = settings.get("transfer_training_architectures",settings.get("architectures",["rnn","gru"])[:2])
    eligible_tasks = settings.get("transfer_training_tasks",settings.get("tasks",settings.get("envs",["foraging","corridor"]))[:2])
    def training_cost(development):
        return sum(cell_training[(task,arch)]+(cell_selection[(task,arch)] if development and arch in eligible_arch and task in eligible_tasks else 0.)
                   for task,arch,_,_ in _cells(settings,development=development))
    development_seconds, final_seconds = training_cost(True),training_cost(False)
    seconds = development_seconds if phase=="agents-development" else final_seconds
    remaining_seconds = development_seconds+final_seconds if phase=="agents-development" else final_seconds
    count = development_fits if phase=="agents-development" else final_fits
    maximum = max(cell_training[key]+(cell_selection[key] if key[1] in eligible_arch and key[0] in eligible_tasks else 0.) for key in cell_training) * len(settings["lr_grid"]) if phase=="agents-development" else max(cell_training.values())
    parameters = {"agent-development" if phase=="agents-development" else "agent-final":_parameters(maximum,rss,cpus,gpu=True)}
    cpu_hours, gpu_hours = seconds*cpus/3600,seconds/3600
    evaluation_bytes,evaluation_files = 0,0
    if phase == "agents-final":
        frozen = directory / "frozen_protocol.json"
        if not frozen.is_file() or read_json(frozen).get("full_config_sha256") != canonical_hash(config) or read_json(frozen).get("implementation_sha256") != _implementation_sha256():
            raise AdmissionError("Complete development protocol freeze is pending")
        protocol = read_json(frozen)
        directions = {f"{mechanism}:{rank}" for mechanism in MECHANISMS for rank in spec["ranks"] if mechanism!="motor_gain" or rank==1}
        if (protocol.get("config_sha256")!=_digest(settings) or protocol.get("H4_inference_specification")!=h4_specification(config)
                or protocol.get("learning_rate") not in settings["lr_grid"] or set(protocol.get("directions",{}))!=directions
                or protocol.get("development_seeds")!=list(settings.get("development_seeds",range(100,105)))
                or protocol.get("final_seeds")!=list(settings.get("final_seeds",range(1000,1020)))):
            raise AdmissionError("Frozen computational protocol is incomplete or mismatched")
        selected_weights = []
        for task,architecture,seed,rate in _cells(settings,development=True):
            path = directory/"development"/_cell_name(task,architecture,seed,rate)/"summary.json"
            if not path.is_file():
                raise AdmissionError("Frozen protocol lacks a complete development seed/rate roster")
            summary = read_json(path)
            checkpoint = path.parent/"checkpoint.pt"
            if (summary.get("config_sha256")!=_digest(settings) or summary.get("implementation_sha256")!=_implementation_sha256()
                    or summary.get("complete_training_budget") is not True or summary.get("training_steps")!=settings["total_steps"]
                    or (summary.get("task"),summary.get("architecture"),summary.get("seed"),summary.get("learning_rate"))!=(task,architecture,seed,rate)
                    or not checkpoint.is_file() or summary.get("checkpoint_sha256")!=digest(checkpoint)):
                raise AdmissionError("Frozen development evidence is incomplete or changed")
            if architecture in eligible_arch and task in eligible_tasks and rate==protocol["learning_rate"]:
                selected_weights.append(summary["weight_sha256"])
            paths.extend([path,checkpoint])
        if protocol.get("source_development_weights")!=selected_weights:
            raise AdmissionError("Frozen perturbations/LR refer to other development policy weights")
        paths.append(frozen)
        evaluation_path = root / "state" / "agent-cpu-evaluation-benchmark.json"
        if not evaluation_path.is_file():
            raise AdmissionError("Native CPU perturbation/probe evaluation benchmark is pending; GPU evaluation timings cannot replace it")
        evaluation = read_json(evaluation_path)
        if (evaluation.get("full_config_sha256")!=canonical_hash(config) or evaluation.get("source_sha256")!=identity
                or evaluation.get("benchmark_specification")!=spec or evaluation.get("evaluation_device")!="cpu"
                or evaluation.get("complete_mechanism_grid") is not True or evaluation.get("complete_architecture_task_coverage") is not True):
            raise AdmissionError("CPU aggregate belongs to another release or lacks complete configured coverage")
        expected = {(task,arch,seed) for task,arch,seeds in cells for seed in seeds}
        roster = evaluation.get("records",[])
        if len(roster)!=len(expected) or {(r["task"],r["architecture"],r["seed"]) for r in roster}!=expected:
            raise AdmissionError("CPU aggregate cell/seed roster is incomplete or duplicated")
        evaluation_records,evaluation_jobs = [],[]
        for entry in roster:
            path = Path(entry["path"])
            if not path.is_file() or entry.get("sha256")!=digest(path) or evaluation.get("source_result_hashes",{}).get(str(path.resolve()))!=digest(path):
                raise AdmissionError("CPU aggregate points to changed native evaluation evidence")
            try:
                record = _benchmark_checked(config,path,"cpu-evaluation")
                grid = _validate_benchmark_grid(config,path.parent,record)
            except RuntimeError as error:
                raise AdmissionError(str(error)) from error
            if (record.get("task"),record.get("architecture"),record.get("seed"))!=(entry["task"],entry["architecture"],entry["seed"]) or not grid["complete_mechanism_grid"]:
                raise AdmissionError("CPU evaluation native cell differs from its roster")
            if grid.get("numerically_valid_grid") is not True:
                raise AdmissionError("CPU evaluation grid contains explicit unavailable numerical evidence; complete finite cost coverage is pending")
            job = _proof(record,config,identity,native)
            if job["gpus"]!=0 or record.get("evaluation_device")!="cpu" or record.get("allocated_cpus")!=job["cpus"] or record.get("calibration_episodes")!=spec["calibration_episodes"] or record.get("kl_bisections")!=spec["kl_bisections"]:
                raise AdmissionError("Evaluation cost must use native CPU allocation and complete configured calibration")
            positive(record["evaluation_seconds"],"CPU evaluation runtime")
            positive(record["working_bytes"],"CPU evaluation bytes");positive(record["working_files"],"CPU evaluation files")
            evaluation_records.append(record);evaluation_jobs.append(job);paths.append(path)
        eval_inflation = {}
        for job_id in {job["job_id"] for job in evaluation_jobs}:
            measured = sum(rec["evaluation_seconds"] for rec in evaluation_records if str(rec["slurm_job_id"])==job_id)
            eval_inflation[job_id] = max(1.,native[job_id]["elapsed_seconds"]/measured)
        ratio = int(settings["evaluation_episodes"])/spec["evaluation_episodes"]
        costs = {}
        for record in evaluation_records:
            key = record["task"],record["architecture"]
            costs[key] = max(costs.get(key,0.),safety*record["evaluation_seconds"]*eval_inflation[str(record["slurm_job_id"]) ]*ratio)
        evaluation_cpus = max(job["cpus"] for job in evaluation_jobs)
        cpu_hours += sum(costs[(task,arch)]*evaluation_cpus/3600 for task,arch,_,_ in _cells(settings))
        # Calibration already measures batches of32 by default. The final batch
        # increase is bounded conservatively against that observed peak RSS.
        memory_ratio = max(int(settings["evaluation_episodes"]),spec["calibration_episodes"])/max(spec["evaluation_episodes"],spec["calibration_episodes"])
        evaluation_rss = max(job["peak_rss_bytes"] for job in evaluation_jobs)*memory_ratio
        parameters["agent-evaluation"] = _parameters(max(costs.values()),evaluation_rss,evaluation_cpus,concurrency=max(1,config["resources"]["cpu_ceiling"]//evaluation_cpus))
        evaluation_bytes = math.ceil(safety*final_fits*max(rec["working_bytes"] for rec in evaluation_records)*ratio)
        evaluation_files = final_fits*max(int(rec["working_files"]) for rec in evaluation_records)
        paths.append(evaluation_path)
    working_bytes = max(rec["working_bytes"] for rec in records)
    return {"cpu_hours":cpu_hours,"gpu_hours":gpu_hours,"all_remaining_training_gpu_hours":remaining_seconds/3600,
            "additional_bytes":math.ceil(safety*count*working_bytes)+evaluation_bytes,
            "additional_files":count*max(int(rec["working_files"]) for rec in records)+evaluation_files,"parameters":parameters,
            "workload":{"development_training_fits":development_fits,"final_training_fits":final_fits,
                        "cpu_evaluation_policies":final_fits,"configured_architecture_task_cells":len(cells),
                        "timing_policies":len(records),"training_seconds_by_cell":{f"{t}/{a}":v for (t,a),v in cell_training.items()},
                        "direction_selection_measured":True,"competence_is_separate_qualification":True,
                        "benchmark_competence_qualified_policies":sum(rec["policy_competence_qualified"] is True for rec in records),
                        "forecast_rule":"worst of both fixed benchmark seeds within each cell; all native allocation overhead; safety factor; full evaluation/calibration grid scaled conservatively"},"inputs":paths}


def _work_units(config, root, native, identity, safety, phase):
    path = root / "state" / f"{phase}-workload-benchmark.json"
    if not path.is_file():
        raise AdmissionError(f"Native full-design {phase} workload benchmarks are pending: {path.name}")
    report = read_json(path)
    _proof(report, config, identity, native)
    tasks = report.get("tasks", [])
    if report.get("complete_declared_workload") is not True or not tasks:
        raise AdmissionError("Full model/recovery/orthology work-unit coverage is not measured")
    cpu_hours = 0.
    parameters = {}
    for task in tasks:
        job = _proof(task, config, identity, native)
        if job["gpus"] or task.get("benchmark_uses_full_primary_numerics") is not True:
            raise AdmissionError("CPU numerical forecast requires the actual full-tail/full-model algorithm")
        seconds_per_unit = max(positive(task["elapsed_seconds"], "work-unit runtime"), job["elapsed_seconds"]) / positive(task["measured_work_units"], "measured work units")
        units = positive(task["planned_work_units"], "prospective planned work units")
        maximum = positive(task["maximum_job_work_units"], "maximum per-job work units")
        cpu_hours += seconds_per_unit * units * safety * job["cpus"] / 3600
        parameters[task["name"]] = _parameters(seconds_per_unit * maximum * safety, job["peak_rss_bytes"], job["cpus"], concurrency=int(task["concurrency"]))
    return {"cpu_hours": cpu_hours, "gpu_hours": 0., "additional_bytes": math.ceil(safety * positive(report["additional_working_bytes"], "full-design output storage")), "additional_files": int(positive(report["additional_working_files"], "full-design output files")), "parameters": parameters, "workload": {"tasks": tasks, "prospective_workload_sha256": report["prospective_workload_sha256"]}, "inputs": [path]}


def assess_admission(config, root, phase=None, *, safety_factor=1.5):
    """Write independent admitted/pending/rejected receipts; never submit jobs."""
    root = Path(root)
    phases = [phase] if phase else list(PHASES)
    if any(value not in PHASES for value in phases) or not math.isfinite(safety_factor) or safety_factor < 1:
        raise AdmissionError("Unknown phase or invalid prospective safety factor")
    identity = source_identity(Path(__file__).resolve().parents[2])["sha256"]
    results = {}
    for value in phases:
        record = {"phase": value, "config_sha256": canonical_hash(config), "source_sha256": identity, "evidence_class": "measured prospective resource admission", "safety_factor": safety_factor, "status": "pending", "scientific_result": False}
        try:
            native, native_path = _native(root)
            spent_cpu, spent_gpu, ledger_path = _ledger(root)
            if value == "rna-full":
                forecast = _rna(config, root, native, identity, safety_factor)
            elif value.startswith("agents-"):
                forecast = _agents(config, root, native, identity, safety_factor, value)
            else:
                forecast = _work_units(config, root, native, identity, safety_factor, value)
            storage_bytes, storage_files, storage_paths = _storage(root, config, forecast["additional_bytes"], forecast["additional_files"])
            cpu = spent_cpu + forecast["cpu_hours"]
            gpu = spent_gpu + forecast["gpu_hours"]
            forecast.update(projected_cpu_core_hours=cpu, projected_gpu_hours=gpu, projected_scratch_bytes=storage_bytes, projected_scratch_files=storage_files)
            record.update({key: val for key, val in forecast.items() if key != "inputs"})
            resource = config["resources"]
            if cpu > resource["cpu_core_hours_cap"] or gpu > resource["gpu_hours_cap"] or spent_gpu + forecast.get("all_remaining_training_gpu_hours", 0) > resource["gpu_hours_cap"]:
                record.update(status="rejected", reasons=["Measured prospective workload exceeds the remaining CPU/GPU study cap"])
            elif any(task["cpus"] * task["concurrency"] > resource["cpu_ceiling"] or task["hours"] > 168 for task in forecast["parameters"].values()):
                record.update(status="rejected", reasons=["Measured per-job/concurrency request exceeds bounded CPU or seven-day job duration; split finite work before admission"])
            else:
                record.update(status="admitted", reasons=[], allocation_parameters=forecast["parameters"])
            sources = [*forecast["inputs"], native_path, ledger_path, *storage_paths]
            record["source_result_paths"] = [str(path.resolve()) for path in sources]
            record["source_result_hashes"] = {str(path.resolve()): digest(path) for path in sources}
        except (AdmissionError, KeyError, FileNotFoundError, ValueError) as error:
            record.update(status="pending", reasons=[str(error)])
        atomic_json(root / "state" / f"admission-{value}.json", record)
        results[value] = record
    atomic_json(root / "state" / "measured-admission.json", results)
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--phase", choices=PHASES)
    parser.add_argument("--safety-factor", type=float, default=1.5)
    args = parser.parse_args(argv)
    result = assess_admission(read_json(args.config), args.root, args.phase, safety_factor=args.safety_factor)
    print(json.dumps({key: value["status"] for key, value in result.items()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
