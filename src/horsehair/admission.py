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
    jobs = {}
    for row in rows:
        job_id = row["JobIDRaw"].split(".", 1)[0]
        if not re.fullmatch(r"\d+(?:_\d+)?", job_id):
            raise AdmissionError("Invalid native job identity")
        record = jobs.setdefault(job_id, {"peak_rss_bytes": None})
        if row["MaxRSS"]:
            peak = memory_bytes(row["MaxRSS"])
            record["peak_rss_bytes"] = max(peak, record["peak_rss_bytes"] or 0)
        if "." not in row["JobIDRaw"]:
            gpu = re.search(r"(?:^|,)gres/gpu(?:[:=][^,=]+)?=(\d+)(?:,|$)", row["AllocTRES"])
            elapsed, cpus = float(row["ElapsedRaw"]), int(row["AllocCPUS"])
            if not math.isfinite(elapsed) or elapsed < 0 or cpus < 0:
                raise AdmissionError("Invalid native elapsed time or allocated CPU count")
            record.update(job_id=job_id, state=row["State"].split()[0], elapsed_seconds=elapsed, cpus=cpus, gpus=int(gpu[1]) if gpu else 0, alloc_tres=row["AllocTRES"])
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
    if not job or job.get("state") != "COMPLETED" or not job.get("peak_rss_bytes"):
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
    from .agents import _cells, _directory
    settings = config["agents"]
    directory = _directory(root, settings)
    paths = sorted((directory / "pilot").rglob("pilot_benchmark.json"))
    if not paths:
        raise AdmissionError("Native agent pilot benchmarks are pending")
    records, jobs = [], []
    for path in paths:
        record = read_json(path)
        jobs.append(_proof(record, config, identity, native, require_cuda=True))
        positive(record["steps_per_second"], "GPU environment throughput")
        positive(record["working_bytes"], "checkpoint/output bytes")
        records.append(record)
    expected = {(task, architecture) for task, architecture, _, _ in _cells(settings)}
    covered = {(rec["task"], rec["architecture"]) for rec in records}
    if not expected.issubset(covered):
        raise AdmissionError(f"Native GPU timing coverage is pending for architecture/task cells {sorted(expected-covered)}")
    throughput = min(rec["steps_per_second"] for rec in records)
    train_seconds = safety * settings["total_steps"] / throughput
    cpus = max(job["cpus"] for job in jobs)
    rss = max(job["peak_rss_bytes"] for job in jobs)
    development_fits = len(_cells(settings, development=True))
    final_fits = len(_cells(settings))
    all_training_hours = train_seconds * (development_fits + final_fits) / 3600
    count = development_fits if phase == "agents-development" else final_fits
    parameters = {"agent-development" if phase == "agents-development" else "agent-final": _parameters(train_seconds * (len(settings["lr_grid"]) if phase == "agents-development" else 1), rss, cpus, gpu=True)}
    cpu_hours, gpu_hours = count * train_seconds * cpus / 3600, count * train_seconds / 3600
    if phase == "agents-final":
        frozen = directory / "frozen_protocol.json"
        if not frozen.is_file() or read_json(frozen).get("full_config_sha256") != canonical_hash(config):
            raise AdmissionError("Complete development protocol freeze is pending")
        paths.append(frozen)
        evaluation_path = root / "state" / "agent-cpu-evaluation-benchmark.json"
        if not evaluation_path.is_file():
            raise AdmissionError("Native CPU perturbation/probe evaluation benchmark is pending; GPU evaluation timings cannot replace it")
        evaluation = read_json(evaluation_path)
        evaluation_job = _proof(evaluation, config, identity, native)
        if evaluation.get("evaluation_device") != "cpu" or evaluation_job["gpus"] != 0 or evaluation.get("complete_mechanism_grid") is not True:
            raise AdmissionError("Evaluation benchmark must execute the complete perturbation/probe grid on CPU")
        eval_seconds = max(positive(evaluation["evaluation_seconds"], "CPU evaluation time"), evaluation_job["elapsed_seconds"]) * settings["evaluation_episodes"] / positive(evaluation["evaluation_episodes"], "benchmark evaluation episodes") * safety
        cpu_hours += final_fits * eval_seconds * evaluation_job["cpus"] / 3600
        parameters["agent-evaluation"] = _parameters(eval_seconds, evaluation_job["peak_rss_bytes"], evaluation_job["cpus"], concurrency=max(1, config["resources"]["cpu_ceiling"] // evaluation_job["cpus"]))
        paths.append(evaluation_path)
    working_bytes = max(rec["working_bytes"] for rec in records)
    if any("working_files" not in rec for rec in records):
        raise AdmissionError("Native agent working-file count is pending")
    return {"cpu_hours": cpu_hours, "gpu_hours": gpu_hours, "all_remaining_training_gpu_hours": all_training_hours, "additional_bytes": math.ceil(safety * count * working_bytes), "additional_files": count * max(rec["working_files"] for rec in records), "parameters": parameters, "workload": {"development_training_fits": development_fits, "final_training_fits": final_fits, "cpu_evaluation_policies": final_fits, "configured_architecture_task_cells": len(expected)}, "inputs": paths}


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
