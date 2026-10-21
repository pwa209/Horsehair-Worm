"""Preserve completed RNA counts while replaying the unchanged primary task.

This is an operational guard. It does not implement trimming, counting, QC, or
scientific admission. Native preparation must preserve failed output directories
before any replay; the primary CLI retains its own resource lease and receipts.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import sys
import time
from pathlib import Path

# Direct absolute-file execution permits an otherwise unchanged primary sbatch
# cwd/PYTHONPATH. Scientific execution below uses a separate primary-only
# horsehair context; Alliance nonstudy dependency-module paths are retained.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from horsehair.core import canonical_hash, digest, native_job_id, read_json, source_identity
from horsehair.budget import require_storage_headroom
from horsehair import budget as _maintenance_budget, core as _maintenance_core

if any(Path(module.__file__).resolve().parent != Path(__file__).resolve().parent
       for module in (_maintenance_core, _maintenance_budget)):
    raise RuntimeError("RNA recovery operational helpers imported from another release")

PRIMARY_RELEASE_ID = "20261013-829e3817bee1-0e2b1ebce8bf"
PRIMARY_SOURCE_SHA256 = "0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630"
PRIMARY_CONFIG_SHA256 = "446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35"
PLAN_RELATIVE_PATH = "data/rna-plan-repair-v7-20261013/full/rna_plan.json"
PREVIOUS_ARRAY_JOB_ID = "5866358"
REUSED_INDICES = tuple([*range(18), 19, 20])
RERUN_INDICES = tuple([18, *range(21, 60)])
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED", "BOOT_FAIL", "DEADLINE", "REVOKED"}
REQUIRED_OUTPUTS = (
    ".processing_complete", "qc.json", "counts.txt", "counts.txt.summary",
    "aligned.Log.final.out", "aligned.ReadsPerGene.out.tab", "strand.json",
    "flagstat.txt", "reference_manifest_hash.txt", "STAR.version.txt",
    "samtools.version.txt", "featureCounts.version.txt", "fastqc.version.txt",
)
BINDING_ATTEMPTS = 4
BINDING_RETRY_SECONDS = 0.25


class RNARecoveryError(RuntimeError):
    pass


def _require(condition, message):
    if not condition:
        raise RNARecoveryError(message)


def _plain_file(path: Path) -> Path:
    path = Path(path)
    _require(path.is_file() and not path.is_symlink(), "Required regular file is missing or symlinked: " + str(path))
    # Reject a symlinked ancestor as well, including outside-study indirection.
    _require(all(not p.is_symlink() for p in path.parents), "Symlinked file ancestor: " + str(path))
    return path


def _binding(path: Path) -> dict:
    """Bind a regular file without mistaking one unsettled stat for corruption.

    Keep every previous metadata check and always require two consecutive reads
    with an identical stable identity and hash. Metadata can retain the same
    value during a same-size overwrite, so an apparently stable first read is
    never sufficient. A changing digest or replacement inode is never retried
    into acceptance. Four attempts bound metadata traffic and total rereads.
    """
    path = Path(path)
    observed_hash = None
    inode = None
    previous_stable = None
    for attempt in range(BINDING_ATTEMPTS):
        path = _plain_file(path)
        before = path.stat()
        current_inode = (before.st_dev, before.st_ino)
        _require(inode is None or current_inode == inode, "File replaced while hashing: " + str(path))
        inode = current_inode
        sha256 = digest(path)
        path = _plain_file(path)
        after = path.stat()
        _require((after.st_dev, after.st_ino) == inode, "File replaced while hashing: " + str(path))
        _require(observed_hash is None or observed_hash == sha256, "File content changed while hashing: " + str(path))
        observed_hash = sha256
        fields = lambda stat: (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
        identity = fields(after)
        stable = fields(before) == identity
        if stable and previous_stable == identity:
            return {"path": str(path.resolve()), "bytes": after.st_size, "sha256": sha256}
        previous_stable = identity if stable else None
        # A stable first pass needs a second full read, not a waiting period.
        # Back off only when metadata actually moved; delaying every immutable
        # source/output binding would multiply qualification and cohort latency.
        if not stable and attempt + 1 < BINDING_ATTEMPTS:
            time.sleep(BINDING_RETRY_SECONDS)
    raise RNARecoveryError("File did not stabilize while hashing: " + str(path))


def _unchanged(binding: dict):
    _require(_binding(Path(binding["path"])) == binding, "Bound file has changed: " + binding["path"])


def _exclusive_json(path: Path, value: dict):
    # An existing intent/receipt is evidence requiring inspection, never license
    # to repeat the biological task after an ambiguous earlier action.
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf8", newline="\n") as handle:
        json.dump(value, handle, sort_keys=True, indent=2, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _native_record(root: Path, ident: str, *, completed=False) -> dict:
    _require(re.fullmatch(r"\d+(?:_\d+)?", ident) is not None, "Invalid native identity")
    record = read_json(_plain_file(root / "state/resource-budget.json")).get("native_jobs", {}).get(ident)
    _require(isinstance(record, dict), "Missing positive native accounting proof: " + ident)
    _require(record.get("basis") == "sacct final allocated wall time" and record.get("state") in TERMINAL and
             record.get("job_id") == ident and re.fullmatch(r"\d+:\d+", str(record.get("exit_code", ""))) is not None,
             "Invalid final native accounting proof: " + ident)
    _require(type(record.get("cpus")) in (int, float) and math.isfinite(record["cpus"]) and record["cpus"] > 0 and
             type(record.get("elapsed_seconds")) in (int, float) and math.isfinite(record["elapsed_seconds"]) and
             record["elapsed_seconds"] >= 0 and (not completed or record["elapsed_seconds"] > 0),
             "Missing allocated native execution: " + ident)
    if completed:
        _require(record["state"] == "COMPLETED" and record["exit_code"] == "0:0", "Native task was not successful: " + ident)
    return record


def _execution_identity(index: int, job: dict) -> str:
    ident = native_job_id()
    _require(ident is not None and os.environ.get("SLURM_ARRAY_TASK_ID") == str(index), "RNA recovery task requires its exact native array allocation")
    _require(ident != PREVIOUS_ARRAY_JOB_ID + "_" + str(index), "A previous native task cannot become its own recovery")
    _require(os.environ.get("SLURM_CPUS_PER_TASK") == str(job["threads"]), "RNA recovery CPU allocation differs from the frozen task")
    return ident


def _validate_inputs(root: Path, primary: Path, plan_path: Path, expected_plan_sha256: str) -> tuple[dict, dict, dict]:
    root, primary, plan_path = root.resolve(), primary.resolve(), plan_path.resolve()
    _require(primary.name == PRIMARY_RELEASE_ID, "Unexpected primary release directory")
    release = read_json(_plain_file(primary / "release.json"))
    identity = source_identity(primary)
    _require(release.get("release_id") == PRIMARY_RELEASE_ID and release.get("source_identity") == identity and
             identity["sha256"] == PRIMARY_SOURCE_SHA256, "Frozen primary source identity changed")
    config = read_json(_plain_file(primary / "configs/study.json"))
    _require(canonical_hash(config) == PRIMARY_CONFIG_SHA256, "Frozen full configuration changed")
    _require(Path(config["paths"]["root"]).resolve() == root, "Configuration refers to another study root")
    _require(plan_path == root / PLAN_RELATIVE_PATH, "Unexpected frozen RNA plan path")
    plan = read_json(_plain_file(plan_path))
    _require(re.fullmatch(r"[0-9a-f]{64}", expected_plan_sha256) is not None and canonical_hash(plan) == expected_plan_sha256,
             "Frozen biological task plan changed")
    jobs = plan.get("jobs", [])
    _require(plan.get("status") == "executable_plan" and plan.get("preprocessing") == "published" and
             plan.get("strand_mode") == "infer" and len(jobs) == 60 and
             len({j.get("biological_id") for j in jobs}) == 60, "Frozen RNA cohort or processing design changed")
    work_root = Path(plan["work_directory"]).resolve()
    _require(work_root.is_relative_to(root), "RNA work root lies outside personal study scratch")
    for job in jobs:
        brain = job.get("biological_id", "")
        _require(re.fullmatch(r"[A-Za-z0-9_-]+", brain) is not None and job.get("threads") == 8 and
                 job.get("preprocessing") == "published" and job.get("cohort") in {"discovery", "challenge"} and
                 isinstance(job.get("runs"), list) and len(job["runs"]) > 0 and len(set(job["runs"])) == len(job["runs"]),
                 "Invalid frozen per-brain mapping")
        _require(Path(job["output"]).resolve() == work_root / brain and Path(job["script"]).resolve() == plan_path.parent / ("rna_" + brain + ".sh"),
                 "RNA brain/output/script mapping changed")
    return release, config, plan


def _scientific_outputs(plan: dict, job: dict, receipt: dict) -> dict:
    directory = Path(job["output"])
    _require(directory.is_dir() and not directory.is_symlink(), "Completed brain output directory is missing")
    bindings = {name: _binding(directory / name) for name in REQUIRED_OUTPUTS}
    _require((directory / ".processing_complete").read_text().strip() == "complete", "RNA completion marker is invalid")
    _require(all(value["bytes"] > 0 for value in bindings.values()), "Empty scientific output")
    qc = read_json(directory / "qc.json")
    _require(qc == receipt.get("qc") and qc.get("biological_id") == job["biological_id"] and qc.get("status") == "passed",
             "QC receipt or exact biological mapping changed")
    _require(qc.get("criteria") == {"min_unique_mapping": 0.5, "min_assignment": 0.25}, "Primary QC criteria changed")
    _require((directory / "reference_manifest_hash.txt").read_text().strip() == plan.get("reference_manifest_hash"), "Reference manifest mapping changed")
    strand = read_json(directory / "strand.json")
    _require(strand.get("status") == "validated" and strand.get("inferred") == "reverse" and strand.get("featurecounts_code") == 2,
             "Primary strandedness validation is missing or changed")
    # Retain and bind the original raw/trimmed FastQC reports; no report is
    # regenerated and no scientific threshold is introduced by this guard.
    for area in ("qc_raw", "qc_trimmed"):
        folder = directory / area
        _require(folder.is_dir() and not folder.is_symlink(), "Missing original FastQC output directory")
        names = sorted(p.name for p in folder.iterdir())
        _require(len([n for n in names if n.endswith("_fastqc.zip")]) >= 2 and
                 len([n for n in names if n.endswith("_fastqc.html")]) >= 2, "Incomplete original paired-end FastQC reports")
        for name in names:
            if name.endswith(("_fastqc.zip", "_fastqc.html")):
                bindings[area + "/" + name] = _binding(folder / name)
    return bindings


def validate_scientific_outputs(plan: dict, job: dict, receipt: dict) -> dict:
    """Read and bind original completed artifacts with the frozen QC criteria."""
    return _scientific_outputs(plan, job, receipt)


def _completed_task(root: Path, plan: dict, index: int) -> dict:
    job = plan["jobs"][index]
    ident = PREVIOUS_ARRAY_JOB_ID + "_" + str(index)
    native = _native_record(root, ident, completed=True)
    state_path = root / "state" / ("rna-full-" + str(index) + ".json")
    binding = _binding(state_path)
    receipt = read_json(state_path)
    expected = {"status": "processed", "biological_id": job["biological_id"], "cohort": job["cohort"],
                "plan_hash": canonical_hash(plan), "slurm_job_id": ident, "source_sha256": PRIMARY_SOURCE_SHA256,
                "config_sha256": PRIMARY_CONFIG_SHA256, "full_config_sha256": PRIMARY_CONFIG_SHA256,
                "native_execution": True, "allocated_cpus": job["threads"]}
    _require(all(receipt.get(k) == value for k, value in expected.items()), "Prior RNA task receipt does not prove this exact successful brain/native/source/config/plan mapping")
    _require(native["cpus"] == job["threads"], "Prior native allocation differs from its scientific receipt")
    return {"scientific_receipt": binding, "native_record": native, "outputs": _scientific_outputs(plan, job, receipt)}


def _storage_demand(config: dict, admission: dict | None) -> dict:
    resources = config["resources"]
    if admission is not None:
        _require(admission.get("status") == "admitted" and admission.get("phase") == "rna-full" and
                 admission.get("source_sha256") == PRIMARY_SOURCE_SHA256 and
                 admission.get("config_sha256") == PRIMARY_CONFIG_SHA256, "RNA storage forecast is not bound to the frozen primary admission")
        files, size = admission.get("additional_files"), admission.get("additional_bytes")
        basis = "Unchanged measured full RNA admission forecast, conservatively retained for each remaining task"
    else:
        files, size = resources.get("scratch_budget_files"), resources.get("scratch_budget_bytes")
        basis = "Strict full configured study scratch budget; no measured RNA forecast available"
    reserve_bytes, configured_files = resources.get("scratch_reserve_bytes"), resources.get("scratch_budget_files")
    _require(all(type(value) is int and value >= 0 for value in (files, size, reserve_bytes, configured_files)) and files > 0 and size > 0,
             "Missing finite RNA storage demand/configured reserve")
    return {"required_files": files, "required_bytes": size, "reserve_bytes": reserve_bytes,
            "reserve_files": max(0, configured_files - files), "basis": basis}


def prepare_recovery(root: str | Path, primary_release: str | Path, plan_path: str | Path,
                     expected_plan_sha256: str, case_directory: str | Path) -> dict:
    """Capture 20 positively completed tasks without changing any old output.

    Run under a native maintenance allocation after refreshing all 60 exact
    final sacct records into state/resource-budget.json. The separate native
    preservation job consumes this immutable manifest and writes preparation.json.
    """
    root, primary, plan_path, case = map(lambda p: Path(p).resolve(), (root, primary_release, plan_path, case_directory))
    _require(native_job_id() is not None, "Recovery capture requires native execution")
    _require(case.is_relative_to(root / "maintenance") and not case.exists(), "Recovery case must be a new personal-study maintenance directory")
    release, config, plan = _validate_inputs(root, primary, plan_path, expected_plan_sha256)
    admission_path = root / "state/admission-rna-full.json"
    admission = read_json(_plain_file(admission_path)) if admission_path.exists() else None
    demand = _storage_demand(config, admission)
    tasks = {}
    for index, job in enumerate(plan["jobs"]):
        ident = PREVIOUS_ARRAY_JOB_ID + "_" + str(index)
        record = _native_record(root, ident)
        task = {"index": index, "biological_id": job["biological_id"], "job": job,
                "script": _binding(Path(job["script"])), "previous_native_record": record,
                "action": "reuse" if index in REUSED_INDICES else "rerun"}
        if index in REUSED_INDICES:
            task["reuse"] = _completed_task(root, plan, index)
        else:
            _require(record["state"] != "COMPLETED" or record["exit_code"] != "0:0", "Unexpected successful task outside the positively reviewed reuse roster")
            directory = Path(job["output"])
            _require(not directory.is_symlink(), "Failed output directory is symlinked")
            task["prior_output_identity"] = {"device": directory.stat().st_dev, "inode": directory.stat().st_ino} if directory.exists() else None
            state = root / "state" / ("rna-full-" + str(index) + ".json")
            task["prior_scientific_receipt"] = _binding(state) if state.exists() else None
        tasks[str(index)] = task
    module_release = Path(__file__).resolve().parents[2]
    manifest = {"schema_version": 1, "status": "captured", "kind": "RNA count recovery; no scientific method amendment",
                "root": str(root), "case_directory": str(case), "primary_release": str(primary),
                "primary_release_id": release["release_id"], "primary_source_identity": release["source_identity"],
                "full_config_sha256": canonical_hash(config), "plan": _binding(plan_path), "plan_sha256": canonical_hash(plan),
                "maintenance_release": str(module_release), "maintenance_source_identity": source_identity(module_release),
                "capture_slurm_job_id": native_job_id(), "created_unix": time.time(),
                "previous_array_job_id": PREVIOUS_ARRAY_JOB_ID, "reuse_indices": list(REUSED_INDICES),
                "rerun_indices": list(RERUN_INDICES), "tasks": tasks, "storage_demand": demand,
                "storage_admission_source": _binding(admission_path) if admission is not None else None}
    case.mkdir(parents=True, exist_ok=False)
    if admission is not None:
        _exclusive_json(case / "storage-admission.json", admission)
        manifest["storage_admission_snapshot"] = _binding(case / "storage-admission.json")
    else:
        manifest["storage_admission_snapshot"] = None
    _exclusive_json(case / "manifest.json", manifest)
    return manifest


def _load_manifest(path: Path, expected_sha256: str) -> dict:
    _require(_binding(path)["sha256"] == expected_sha256, "Recovery manifest bytes changed")
    manifest = read_json(path)
    _require(manifest.get("schema_version") == 1 and manifest.get("status") == "captured" and
             manifest.get("previous_array_job_id") == PREVIOUS_ARRAY_JOB_ID and
             manifest.get("reuse_indices") == list(REUSED_INDICES) and manifest.get("rerun_indices") == list(RERUN_INDICES),
             "Recovery manifest schema or exact outcome roster changed")
    _require(Path(manifest["case_directory"]) / "manifest.json" == path.resolve(), "Recovery manifest relocated")
    _require(Path(__file__).resolve().parents[2] == Path(manifest["maintenance_release"]).resolve() and
             source_identity(Path(manifest["maintenance_release"])) == manifest["maintenance_source_identity"], "Recovery maintenance source changed")
    _unchanged(manifest["plan"])
    release, config, plan = _validate_inputs(Path(manifest["root"]), Path(manifest["primary_release"]), Path(manifest["plan"]["path"]), manifest["plan_sha256"])
    _require(release["source_identity"] == manifest["primary_source_identity"] and canonical_hash(config) == manifest["full_config_sha256"] and
             manifest.get("primary_release_id") == PRIMARY_RELEASE_ID, "Recovery frozen execution identity changed")
    snapshot = manifest.get("storage_admission_snapshot")
    if snapshot is not None:
        _unchanged(snapshot)
    _require(_storage_demand(config, read_json(Path(snapshot["path"])) if snapshot else None) == manifest.get("storage_demand"),
             "Recovery storage demand changed")
    _require(set(manifest["tasks"]) == {str(i) for i in range(60)}, "Recovery task inventory changed")
    for index, job in enumerate(plan["jobs"]):
        task = manifest["tasks"][str(index)]
        _require(task["index"] == index and task["job"] == job and task["biological_id"] == job["biological_id"] and
                 task["action"] == ("reuse" if index in REUSED_INDICES else "rerun"), "Recovery per-brain mapping changed")
        _unchanged(task["script"])
    return manifest


def _prepared_rerun(manifest: dict, index: int, preparation_path: Path, manifest_sha256: str):
    _require(preparation_path.resolve() == Path(manifest["case_directory"]) / "preparation.json", "Unexpected native preservation receipt path")
    preparation = read_json(_plain_file(preparation_path))
    _require(preparation.get("schema_version") == 1 and preparation.get("status") == "prepared" and
             preparation.get("manifest_sha256") == manifest_sha256 and preparation.get("primary_source_sha256") == PRIMARY_SOURCE_SHA256 and
             preparation.get("full_config_sha256") == PRIMARY_CONFIG_SHA256 and preparation.get("plan_sha256") == manifest["plan_sha256"],
             "Native preservation receipt has another source/config/plan/manifest identity")
    root = Path(manifest["root"])
    prep_native = _native_record(root, str(preparation.get("slurm_job_id", "")), completed=True)
    _require(set(preparation.get("tasks", {})) == {str(i) for i in RERUN_INDICES},
             "Preservation lacks the exact failed-task roster")
    task, saved = manifest["tasks"][str(index)], preparation["tasks"][str(index)]
    output, state = Path(task["job"]["output"]), root / "state" / ("rna-full-" + str(index) + ".json")
    _require(saved.get("biological_id") == task["biological_id"] and saved.get("output_directory") == str(output) and
             saved.get("state_receipt") == str(state), "Native preservation task mapping changed")
    _require(not output.exists() and not output.is_symlink() and not state.exists() and not state.is_symlink(), "Failed outputs/receipt were not isolated before replay")
    original = task["prior_output_identity"]
    if original is not None:
        preserved = Path(saved.get("preserved_output_directory", ""))
        _require(preserved.is_relative_to(Path(manifest["case_directory"]) / "preserved") and preserved.is_dir() and not preserved.is_symlink(), "Missing preserved failed output directory")
        stat = preserved.stat()
        _require({"device": stat.st_dev, "inode": stat.st_ino} == original, "Preserved failed directory is not the original inode")
    else:
        _require(saved.get("preserved_output_directory") is None, "Unexpected preserved output without a captured original")
    prior = task["prior_scientific_receipt"]
    if prior is not None:
        preserved = _binding(Path(saved.get("preserved_scientific_receipt", "")))
        _require(preserved["bytes"] == prior["bytes"] and preserved["sha256"] == prior["sha256"] and
                 Path(preserved["path"]).is_relative_to(Path(manifest["case_directory"]) / "preserved"), "Original scientific receipt was not preserved unchanged")
    else:
        _require(saved.get("preserved_scientific_receipt") is None, "Unexpected preserved scientific receipt")
    return _binding(preparation_path)


def validate_registration_manifest(manifest_path: str | Path, manifest_sha256: str,
                                    preparation_path: str | Path, preparation_sha256: str) -> dict:
    """Read-only validation before canonical registration/release.

    Capture/preparation may run in the same native maintenance job. Neither
    helper fabricates its future final accounting record: registration requires
    that positive final proof to have arrived in the study budget ledger.
    """
    manifest_path, preparation_path = Path(manifest_path).resolve(), Path(preparation_path).resolve()
    manifest = _load_manifest(manifest_path, manifest_sha256)
    _require(_binding(preparation_path)["sha256"] == preparation_sha256, "Native preparation bytes changed")
    root = Path(manifest["root"])
    capture = _native_record(root, str(manifest["capture_slurm_job_id"]), completed=True)
    plan = read_json(Path(manifest["plan"]["path"]))
    for index in REUSED_INDICES:
        _require(_completed_task(root, plan, index) == manifest["tasks"][str(index)]["reuse"], "Completed evidence changed before registration")
    for index in RERUN_INDICES:
        task = manifest["tasks"][str(index)]
        _require(_native_record(root, PREVIOUS_ARRAY_JOB_ID + "_" + str(index)) == task["previous_native_record"], "Failed native outcome changed before registration")
        _prepared_rerun(manifest, index, preparation_path, manifest_sha256)
    preparation = read_json(preparation_path)
    return {"manifest": manifest, "preparation": preparation, "capture_native_record": capture,
            "preparation_native_record": _native_record(root, str(preparation["slurm_job_id"]), completed=True)}


# The bootstrap has no maintenance scientific imports or fallback path. Primary
# horsehair.cli verifies/adopts its original resource lease and writes its usual
# scientific receipt under its own native identity.
PRIMARY_BOOTSTRAP = """import json,sys\nfrom pathlib import Path\np,root,source_sha,config_sha,index=sys.argv[1:]\np=Path(p).resolve()\nsys.path.insert(0,str(p/'src'))\nimport horsehair\nfrom horsehair.core import source_identity,canonical_hash,read_json\nassert Path(horsehair.__file__).resolve().parent==(p/'src/horsehair').resolve()\nrelease=read_json(p/'release.json')\nassert source_identity(p)==release['source_identity'] and release['source_identity']['sha256']==source_sha\nassert canonical_hash(read_json(p/'configs/study.json'))==config_sha\nfrom horsehair.cli import main\nraise SystemExit(main(['--config',str(p/'configs/study.json'),'--root',root,'molecular','--stage','rna-run','--task-index',index]))\n"""


def _primary_environment(manifest: dict) -> dict:
    """Retain Alliance module dependencies without a maintenance horsehair path.

    Python -I discards PYTHONPATH completely, which drops Arrow and other
    Alliance module packages. The existing primary-context policy uses -s and
    PYTHONNOUSERSITE; prepend the primary and remove every other horsehair
    package path before the bootstrap verifies the exact imported source.
    """
    environment = dict(os.environ)
    primary_src = (Path(manifest["primary_release"]) / "src").resolve()
    maintenance = Path(manifest["maintenance_release"]).resolve()
    root = Path(manifest["root"]).resolve()
    retained = []
    for value in environment.get("PYTHONPATH", "").split(os.pathsep):
        if not value:
            continue
        path = Path(value).resolve()
        if path == primary_src:
            continue
        if path.is_relative_to(maintenance) or path.is_relative_to(root) or (path / "horsehair").exists():
            continue
        if str(path) not in retained:
            retained.append(str(path))
    environment.update(PYTHONPATH=os.pathsep.join([str(primary_src), *retained]),
                       PYTHONNOUSERSITE="1", PYTHONDONTWRITEBYTECODE="1")
    return environment


def execute_recovery_task(manifest_path: str | Path, expected_manifest_sha256: str, index: int,
                          preparation_path: str | Path | None = None, preparation_sha256: str | None = None) -> dict:
    """Reuse one unchanged prior outcome, or run its untouched primary CLI once."""
    _require(isinstance(index, int) and not isinstance(index, bool) and 0 <= index < 60, "RNA recovery index is outside the fixed cohort")
    path = Path(manifest_path).resolve()
    manifest = _load_manifest(path, expected_manifest_sha256)
    task, root, case = manifest["tasks"][str(index)], Path(manifest["root"]), Path(manifest["case_directory"])
    current = _execution_identity(index, task["job"])
    _native_record(root, str(manifest["capture_slurm_job_id"]), completed=True)
    old_native = _native_record(root, PREVIOUS_ARRAY_JOB_ID + "_" + str(index), completed=task["action"] == "reuse")
    _require(old_native == task["previous_native_record"], "Prior native outcome changed since recovery capture")
    receipt = {"schema_version": 1, "index": index, "biological_id": task["biological_id"], "manifest_sha256": expected_manifest_sha256,
               "primary_source_sha256": PRIMARY_SOURCE_SHA256, "full_config_sha256": PRIMARY_CONFIG_SHA256,
               "plan_sha256": manifest["plan_sha256"], "recovery_slurm_job_id": current,
               "previous_slurm_job_id": PREVIOUS_ARRAY_JOB_ID + "_" + str(index), "started_unix": time.time()}
    if task["action"] == "reuse":
        plan = read_json(Path(manifest["plan"]["path"]))
        _require(_completed_task(root, plan, index) == task["reuse"], "Completed RNA scientific evidence changed since capture")
        receipt.update(status="reused", scientific_execution="Original positively completed native task; original scientific receipt retained", reuse=task["reuse"])
        # This is a recovery receipt, never a relabelled original count receipt.
        _exclusive_json(case / "receipts" / ("task-" + str(index) + ".json"), receipt)
        return receipt
    _require(preparation_path is not None and preparation_sha256 is not None, "Rerun requires explicit native preservation proof and byte identity")
    _require(_binding(Path(preparation_path))["sha256"] == preparation_sha256, "Native preparation bytes changed")
    receipt["preparation"] = _prepared_rerun(manifest, index, Path(preparation_path), expected_manifest_sha256)
    command = [sys.executable, "-s", "-c", PRIMARY_BOOTSTRAP, manifest["primary_release"], str(root), PRIMARY_SOURCE_SHA256, PRIMARY_CONFIG_SHA256, str(index)]
    receipt.update(status="execution_intent", command=command, primary_command=["python", "-m", "horsehair.cli", "--config", "configs/study.json", "--root", str(root), "molecular", "--stage", "rna-run", "--task-index", str(index)])
    demand = manifest["storage_demand"]
    receipt["storage_headroom"] = require_storage_headroom(root, **{key: demand[key] for key in ("required_files", "required_bytes", "reserve_files", "reserve_bytes")})
    _exclusive_json(case / "receipts" / ("task-" + str(index) + ".started.json"), receipt)
    result = subprocess.run(command, cwd=manifest["primary_release"], env=_primary_environment(manifest), check=False)
    receipt.update(finished_unix=time.time(), returncode=result.returncode)
    if result.returncode:
        receipt["status"] = "primary_failed"
        _exclusive_json(case / "receipts" / ("task-" + str(index) + ".json"), receipt)
        raise RNARecoveryError("Primary RNA task failed; preserve its outputs and inspect the native log")
    _load_manifest(path, expected_manifest_sha256)
    scientific = root / "state" / ("rna-full-" + str(index) + ".json")
    value = read_json(_plain_file(scientific))
    expected = {"status": "processed", "slurm_job_id": current, "native_execution": True,
                "source_sha256": PRIMARY_SOURCE_SHA256, "config_sha256": PRIMARY_CONFIG_SHA256,
                "full_config_sha256": PRIMARY_CONFIG_SHA256, "plan_hash": manifest["plan_sha256"],
                "biological_id": task["biological_id"], "cohort": task["job"]["cohort"], "allocated_cpus": task["job"]["threads"]}
    _require(all(value.get(k) == v for k, v in expected.items()), "New primary receipt does not prove the current frozen/native/brain identity")
    receipt.update(status="processed", scientific_receipt=_binding(scientific), outputs=_scientific_outputs(read_json(Path(manifest["plan"]["path"])), task["job"], value))
    _exclusive_json(case / "receipts" / ("task-" + str(index) + ".json"), receipt)
    return receipt


FROZEN_MANIFEST_BOOTSTRAP = """import json,sys
from pathlib import Path
release,path,sha,source_sha=sys.argv[1:]
release=Path(release).resolve()
sys.path.insert(0,str(release/'src'))
import horsehair.rna_recovery as recovery
recovery._require(Path(recovery.__file__).resolve()==release/'src/horsehair/rna_recovery.py','Frozen recovery import differs from release')
manifest=recovery._load_manifest(Path(path),sha)
recovery._require(manifest['maintenance_source_identity']['sha256']==source_sha,'Frozen maintenance source differs from qualified identity')
print(json.dumps({'manifest_sha256':recovery.canonical_hash(manifest),'maintenance_source_identity':manifest['maintenance_source_identity']},sort_keys=True))
"""


def load_frozen_recovery_manifest(manifest_path: str | Path, expected_manifest_sha256: str,
                                  expected_maintenance_source_sha256: str) -> dict:
    """Validate an old manifest in its exact source context, without execution.

    The child imports only its frozen operational helpers. It never enters the
    scientific CLI or changes prior state; the current helper checks the child
    result against the byte-bound parent manifest before using any of it.
    """
    path = _plain_file(Path(manifest_path)).resolve()
    binding = _binding(path)
    _require(binding["sha256"] == expected_manifest_sha256, "Recovery manifest bytes changed")
    _require(re.fullmatch(r"[0-9a-f]{64}", expected_maintenance_source_sha256) is not None,
             "Invalid expected frozen maintenance identity")
    manifest = read_json(path)
    release = Path(manifest["maintenance_release"]).resolve()
    _require(manifest.get("maintenance_source_identity", {}).get("sha256") == expected_maintenance_source_sha256,
             "Frozen maintenance identity differs from the qualified release")
    environment = _primary_environment({**manifest, "primary_release": str(release)})
    command = [sys.executable, "-s", "-c", FROZEN_MANIFEST_BOOTSTRAP, str(release), str(path),
               expected_manifest_sha256, expected_maintenance_source_sha256]
    result = subprocess.run(command, cwd=str(release), env=environment, capture_output=True,
                            text=True, timeout=180, check=False)
    _require(result.returncode == 0, "Frozen recovery context validation failed: " + result.stderr[-2000:])
    try:
        proof = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        raise RNARecoveryError("Frozen recovery context returned invalid proof") from exc
    _require(proof == {"manifest_sha256": canonical_hash(manifest),
                       "maintenance_source_identity": manifest["maintenance_source_identity"]},
             "Frozen recovery context proof differs from the bound manifest")
    _unchanged(binding)
    return manifest


def verify_finished_recovery_task(manifest_path: str | Path, expected_manifest_sha256: str, index: int,
                                  preparation_path: str | Path, preparation_sha256: str,
                                  failed_native_job_id: str, expected_maintenance_source_sha256: str,
                                  verification_case_directory: str | Path,
                                  failure_log_path: str | Path, failure_log_sha256: str) -> dict:
    """Verify an already processed brain after only its wrapper hash failed.

    This creates a separate operational verification receipt. The failed native
    allocation and its scientific receipt are retained unchanged; no numerical
    tool is run and no failed allocation is relabelled as successful. Admission
    of the new proof additionally requires final native success of this verifier.
    """
    _require(type(index) is int and index in RERUN_INDICES, "Verification index is outside the fixed rerun roster")
    current = native_job_id()
    _require(current is not None and re.fullmatch(r"\d+", current) is not None,
             "Verification requires its own scalar native allocation")
    _require(re.fullmatch(r"\d+_" + str(index), failed_native_job_id) is not None and current != failed_native_job_id.split("_")[0],
             "Verification requires the exact prior failed native array member")
    manifest_path, preparation_path, failure_log_path = map(
        lambda p: _plain_file(Path(p)).resolve(), (manifest_path, preparation_path, failure_log_path))
    case = Path(verification_case_directory).resolve()
    manifest = load_frozen_recovery_manifest(manifest_path, expected_manifest_sha256, expected_maintenance_source_sha256)
    root = Path(manifest["root"])
    task = manifest["tasks"][str(index)]
    _require(task["action"] == "rerun" and case.is_relative_to(root / "maintenance") and not case.exists(),
             "Verification requires a new personal-study maintenance case")
    failed_native = _native_record(root, failed_native_job_id)
    _require(failed_native["state"] == "FAILED" and failed_native["exit_code"] == "1:0" and
             failed_native["cpus"] == task["job"]["threads"] and failed_native["elapsed_seconds"] > 0,
             "Verification lacks the exact allocated wrapper failure")
    _native_record(root, str(manifest["capture_slurm_job_id"]), completed=True)
    preparation = _binding(preparation_path)
    _require(preparation_path == Path(manifest["case_directory"]) / "preparation.json" and
             preparation["sha256"] == preparation_sha256, "Verification preparation bytes or location changed")
    prepared = read_json(preparation_path)
    _require(prepared.get("schema_version") == 1 and prepared.get("status") == "prepared" and
             prepared.get("manifest_sha256") == expected_manifest_sha256 and
             prepared.get("primary_source_sha256") == PRIMARY_SOURCE_SHA256 and
             prepared.get("full_config_sha256") == PRIMARY_CONFIG_SHA256 and
             prepared.get("plan_sha256") == manifest["plan_sha256"] and
             set(prepared.get("tasks", {})) == {str(i) for i in RERUN_INDICES},
             "Verification preparation identity or roster changed")
    _native_record(root, str(prepared.get("slurm_job_id", "")), completed=True)
    old_case = Path(manifest["case_directory"])
    final = old_case / "receipts" / ("task-" + str(index) + ".json")
    _require(not final.exists() and not final.is_symlink(), "Original recovery already has a final outcome; inspect it instead")
    started_path = old_case / "receipts" / ("task-" + str(index) + ".started.json")
    started_binding = _binding(started_path)
    started = read_json(started_path)
    expected_started = {"schema_version": 1, "status": "execution_intent", "index": index,
                        "biological_id": task["biological_id"], "manifest_sha256": expected_manifest_sha256,
                        "primary_source_sha256": PRIMARY_SOURCE_SHA256, "full_config_sha256": PRIMARY_CONFIG_SHA256,
                        "plan_sha256": manifest["plan_sha256"], "recovery_slurm_job_id": failed_native_job_id,
                        "previous_slurm_job_id": PREVIOUS_ARRAY_JOB_ID + "_" + str(index), "preparation": preparation}
    _require(all(started.get(k) == v for k, v in expected_started.items()), "Original execution intent identity changed")
    command = started.get("command", [])
    _require(isinstance(command, list) and len(command) == 9 and command[1:] ==
             ["-s", "-c", PRIMARY_BOOTSTRAP, manifest["primary_release"], str(root), PRIMARY_SOURCE_SHA256, PRIMARY_CONFIG_SHA256, str(index)],
             "Original intent does not bind the unchanged primary command")
    _require(failure_log_path.is_relative_to(root / "logs") and failed_native_job_id in failure_log_path.name,
             "Wrapper failure log does not belong to this study/native member")
    failure_log = _binding(failure_log_path)
    _require(failure_log["sha256"] == failure_log_sha256, "Wrapper failure log bytes changed")
    log = failure_log_path.read_text(errors="replace")
    failure = "RNARecoveryError: File changed while hashing: " + str(Path(task["job"]["output"]) / "counts.txt")
    _require(failure in log and "_scientific_outputs" in log and "rna_recovery.py" in log,
             "Native log does not prove the reviewed post-processing hash failure")
    scientific_path = root / "state" / ("rna-full-" + str(index) + ".json")
    scientific_binding = _binding(scientific_path)
    scientific = read_json(scientific_path)
    expected = {"status": "processed", "slurm_job_id": failed_native_job_id, "native_execution": True,
                "source_sha256": PRIMARY_SOURCE_SHA256, "config_sha256": PRIMARY_CONFIG_SHA256,
                "full_config_sha256": PRIMARY_CONFIG_SHA256, "plan_hash": manifest["plan_sha256"],
                "biological_id": task["biological_id"], "cohort": task["job"]["cohort"], "allocated_cpus": task["job"]["threads"]}
    _require(all(scientific.get(k) == v for k, v in expected.items()),
             "Original primary receipt does not prove the exact failed-native/brain/source/config/plan mapping")
    _unchanged(manifest["plan"])
    _, _, plan = _validate_inputs(root, Path(manifest["primary_release"]), Path(manifest["plan"]["path"]), manifest["plan_sha256"])
    first = _scientific_outputs(plan, task["job"], scientific)
    time.sleep(BINDING_RETRY_SECONDS)
    _require(_scientific_outputs(plan, task["job"], scientific) == first,
             "Scientific outputs changed between independent verification passes")
    for binding in (started_binding, preparation, scientific_binding, failure_log, manifest["plan"]):
        _unchanged(binding)
    _require(_native_record(root, failed_native_job_id) == failed_native and not final.exists() and not final.is_symlink(),
             "Original native outcome or final recovery evidence changed during verification")
    receipt = {"schema_version": 1, "status": "verified_after_wrapper_failure", "index": index,
               "biological_id": task["biological_id"], "kind": "Verification only; no scientific recomputation or native outcome relabelling",
               "verification_slurm_job_id": current, "failed_native_record": failed_native,
               "recovery_manifest": _binding(manifest_path), "original_execution_intent": started_binding,
               "preparation": preparation, "failure_log": failure_log, "scientific_receipt": scientific_binding,
               "outputs": first, "primary_source_sha256": PRIMARY_SOURCE_SHA256,
               "full_config_sha256": PRIMARY_CONFIG_SHA256, "plan_sha256": manifest["plan_sha256"],
               "original_maintenance_source_sha256": expected_maintenance_source_sha256,
               "verifier_source_identity": source_identity(Path(__file__).resolve().parents[2]),
               "verified_unix": time.time()}
    case.mkdir(parents=True, exist_ok=False)
    _exclusive_json(case / "receipt.json", receipt)
    return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    prepare = sub.add_parser("prepare")
    for name in ("root", "primary-release", "plan", "plan-sha256", "case"):
        prepare.add_argument("--" + name, required=True)
    run = sub.add_parser("run")
    run.add_argument("--manifest", required=True)
    run.add_argument("--manifest-sha256", required=True)
    run.add_argument("--index", type=int, required=True)
    run.add_argument("--preparation")
    run.add_argument("--preparation-sha256")
    args = parser.parse_args(argv)
    if args.action == "prepare":
        result = prepare_recovery(args.root, args.primary_release, args.plan, args.plan_sha256, args.case)
    else:
        result = execute_recovery_task(args.manifest, args.manifest_sha256, args.index, args.preparation, args.preparation_sha256)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
