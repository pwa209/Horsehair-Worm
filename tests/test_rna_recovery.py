"""Recovery qualification fixtures, never biological evidence."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from horsehair import rna_recovery as recovery
from horsehair.core import canonical_hash, digest, source_identity


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def native(ident, state="COMPLETED", exit_code="0:0", cpus=8):
    return {"basis": "sacct final allocated wall time", "job_id": ident, "job_id_raw": ident,
            "state": state, "exit_code": exit_code, "cpus": cpus, "elapsed_seconds": 20}


@pytest.fixture
def cohort(tmp_path, monkeypatch):
    root = tmp_path / "scratch/horsehair-worm"
    primary = tmp_path / "releases" / recovery.PRIMARY_RELEASE_ID
    (primary / "src/horsehair").mkdir(parents=True)
    (primary / "src/horsehair/__init__.py").write_text('"""Fixture primary only."""\n')
    config = {"paths": {"root": str(root)}, "resources": {"rna_cores": 8, "scratch_budget_files": 250000,
              "scratch_budget_bytes": 6000000000000, "scratch_reserve_bytes": 2000000000000},
              "molecular": {"rna_preprocessing": "published"}}
    write_json(primary / "configs/study.json", config)
    identity = source_identity(primary)
    monkeypatch.setattr(recovery, "PRIMARY_SOURCE_SHA256", identity["sha256"])
    monkeypatch.setattr(recovery, "PRIMARY_CONFIG_SHA256", canonical_hash(config))
    write_json(primary / "release.json", {"release_id": recovery.PRIMARY_RELEASE_ID, "source_identity": identity})
    plan_path = root / recovery.PLAN_RELATIVE_PATH
    jobs = []
    for index in range(60):
        brain = "SAMN" + str(32600000 + index)
        output = root / "data/rna-repair-v7-20261013" / brain
        output.mkdir(parents=True)
        script = plan_path.parent / ("rna_" + brain + ".sh")
        script.parent.mkdir(parents=True, exist_ok=True)
        script.write_text("#!/bin/bash\n# frozen fixture " + brain + "\n")
        jobs.append({"biological_id": brain, "cohort": "discovery" if index < 48 else "challenge",
                     "output": str(output), "script": str(script), "runs": ["SRR" + str(index)],
                     "threads": 8, "preprocessing": "published"})
    plan = {"status": "executable_plan", "preprocessing": "published", "strand_mode": "infer",
            "reference_manifest_hash": "a" * 64, "crosswalk_hash": "b" * 64,
            "work_directory": str(root / "data/rna-repair-v7-20261013"), "jobs": jobs}
    write_json(plan_path, plan)
    records = {"9000": native("9000", cpus=4), "9002": native("9002", cpus=4)}
    for index, job in enumerate(jobs):
        ident = recovery.PREVIOUS_ARRAY_JOB_ID + "_" + str(index)
        records[ident] = native(ident) if index in recovery.REUSED_INDICES else native(ident, "FAILED", "1:0")
        directory = Path(job["output"])
        if index not in recovery.REUSED_INDICES:
            (directory / "failed.partial").write_text("Preserve this failed attempt\n")
            continue
        qc = {"biological_id": job["biological_id"], "status": "passed",
              "criteria": {"min_unique_mapping": 0.5, "min_assignment": 0.25},
              "unique_mapping_fraction": .81, "assigned_fragment_fraction": .65}
        for name in recovery.REQUIRED_OUTPUTS:
            (directory / name).write_text("original scientific output\n")
        (directory / ".processing_complete").write_text("complete\n")
        (directory / "reference_manifest_hash.txt").write_text(plan["reference_manifest_hash"] + "\n")
        write_json(directory / "qc.json", qc)
        write_json(directory / "strand.json", {"status": "validated", "inferred": "reverse", "featurecounts_code": 2})
        for area in ("qc_raw", "qc_trimmed"):
            (directory / area).mkdir()
            for mate in ("1", "2"):
                for suffix in ("zip", "html"):
                    (directory / area / ("mate" + mate + "_fastqc." + suffix)).write_text("original FastQC bytes")
        receipt = {"status": "processed", "biological_id": job["biological_id"], "cohort": job["cohort"],
                   "plan_hash": canonical_hash(plan), "slurm_job_id": ident, "source_sha256": identity["sha256"],
                   "config_sha256": canonical_hash(config), "full_config_sha256": canonical_hash(config),
                   "native_execution": True, "allocated_cpus": 8, "qc": qc}
        write_json(root / "state" / ("rna-full-" + str(index) + ".json"), receipt)
    write_json(root / "state/resource-budget.json", {"native_jobs": records})
    write_json(root / "state/admission-rna-full.json", {"status": "admitted", "phase": "rna-full",
               "source_sha256": identity["sha256"], "config_sha256": canonical_hash(config),
               "additional_files": 1860, "additional_bytes": 615855581617})
    monkeypatch.setattr(recovery, "require_storage_headroom", lambda root, **demand: {"ready": True, **demand})
    monkeypatch.setenv("SLURM_JOB_ID", "9000")
    monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)
    monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
    case = root / "maintenance/count-recovery-fixture"
    return SimpleNamespace(root=root, primary=primary, plan=plan, plan_path=plan_path, jobs=jobs,
                           case=case, records=records, monkeypatch=monkeypatch)


def capture(c):
    manifest = recovery.prepare_recovery(c.root, c.primary, c.plan_path, canonical_hash(c.plan), c.case)
    path = c.case / "manifest.json"
    return manifest, path, digest(path)


def allocation(c, index):
    c.monkeypatch.setenv("SLURM_ARRAY_JOB_ID", "9001")
    c.monkeypatch.setenv("SLURM_ARRAY_TASK_ID", str(index))
    c.monkeypatch.setenv("SLURM_CPUS_PER_TASK", "8")


def preserve_failed(c, manifest, manifest_sha):
    tasks = {}
    for index in recovery.RERUN_INDICES:
        job = c.jobs[index]
        output = Path(job["output"])
        preserved = c.case / "preserved" / job["biological_id"]
        preserved.parent.mkdir(parents=True, exist_ok=True)
        output.rename(preserved)
        state = c.root / "state" / ("rna-full-" + str(index) + ".json")
        prior = manifest["tasks"][str(index)]["prior_scientific_receipt"]
        saved_state = c.case / "preserved" / state.name
        if prior:
            state.rename(saved_state)
        tasks[str(index)] = {"biological_id": job["biological_id"], "output_directory": str(output),
                            "preserved_output_directory": str(preserved), "state_receipt": str(state),
                            "preserved_scientific_receipt": str(saved_state) if prior else None}
    preparation = {"schema_version": 1, "status": "prepared", "manifest_sha256": manifest_sha,
                   "primary_source_sha256": recovery.PRIMARY_SOURCE_SHA256,
                   "full_config_sha256": recovery.PRIMARY_CONFIG_SHA256, "plan_sha256": canonical_hash(c.plan),
                   "slurm_job_id": "9002", "tasks": tasks}
    path = c.case / "preparation.json"
    write_json(path, preparation)
    return path, digest(path)


def test_exact_cohort_capture_preserves_original_scientific_receipts(cohort):
    c = cohort
    before = digest(c.root / "state/rna-full-0.json")
    manifest, path, sha = capture(c)
    assert manifest["reuse_indices"] == [*range(18), 19, 20]
    assert manifest["rerun_indices"] == [18, *range(21, 60)]
    assert manifest["tasks"]["0"]["reuse"]["scientific_receipt"]["sha256"] == before
    assert len(manifest["tasks"]["0"]["reuse"]["outputs"]) == 21
    assert digest(c.root / "state/rna-full-0.json") == before
    assert not (c.case / "receipts").exists()


@pytest.mark.parametrize("mutation", ["failed_native", "receipt_source", "receipt_plan", "brain", "native_id", "config", "marker", "qc", "strand", "reference", "fastqc_missing"])
def test_capture_refuses_unproven_or_misidentified_success(cohort, mutation):
    c = cohort
    state = c.root / "state/rna-full-0.json"
    receipt = json.loads(state.read_text())
    output = Path(c.jobs[0]["output"])
    if mutation == "failed_native":
        c.records[recovery.PREVIOUS_ARRAY_JOB_ID + "_0"]["state"] = "FAILED"
        c.records[recovery.PREVIOUS_ARRAY_JOB_ID + "_0"]["exit_code"] = "1:0"
        write_json(c.root / "state/resource-budget.json", {"native_jobs": c.records})
    elif mutation in {"receipt_source", "receipt_plan", "brain", "native_id", "config"}:
        key = {"receipt_source": "source_sha256", "receipt_plan": "plan_hash", "brain": "biological_id", "native_id": "slurm_job_id", "config": "full_config_sha256"}[mutation]
        receipt[key] = "foreign"
        write_json(state, receipt)
    elif mutation == "marker":
        (output / ".processing_complete").write_text("incomplete")
    elif mutation == "qc":
        qc = json.loads((output / "qc.json").read_text())
        qc["status"] = "review_required"
        write_json(output / "qc.json", qc)
    elif mutation == "strand":
        write_json(output / "strand.json", {"status": "validated", "inferred": "forward", "featurecounts_code": 1})
    elif mutation == "reference":
        (output / "reference_manifest_hash.txt").write_text("other")
    else:
        (output / "qc_trimmed/mate1_fastqc.zip").unlink()
    with pytest.raises(recovery.RNARecoveryError):
        capture(c)


def test_capture_refuses_changed_primary_source(cohort):
    c = cohort
    (c.primary / "src/horsehair/__init__.py").write_text("# changed primary source\n")
    with pytest.raises(recovery.RNARecoveryError, match="primary source"):
        capture(c)


def test_capture_refuses_changed_plan_and_replay(cohort):
    c = cohort
    with pytest.raises(recovery.RNARecoveryError, match="task plan"):
        recovery.prepare_recovery(c.root, c.primary, c.plan_path, "f" * 64, c.case)
    capture(c)
    with pytest.raises(recovery.RNARecoveryError, match="new personal-study"):
        capture(c)


def test_success_reuse_writes_separate_receipt_without_scientific_execution(cohort, monkeypatch):
    c = cohort
    before = digest(c.root / "state/rna-full-0.json")
    manifest, path, sha = capture(c)
    allocation(c, 0)
    monkeypatch.setattr(recovery.subprocess, "run", lambda *a, **kw: pytest.fail("reuse must not execute tools"))
    monkeypatch.setattr(recovery, "require_storage_headroom", lambda *a, **kw: pytest.fail("reuse must not start write-heavy work"))
    receipt = recovery.execute_recovery_task(path, sha, 0)
    assert receipt["status"] == "reused"
    assert receipt["recovery_slurm_job_id"] == "9001_0"
    assert receipt["previous_slurm_job_id"] == "5866358_0"
    assert digest(c.root / "state/rna-full-0.json") == before
    assert not (c.case / "receipts/task-0.started.json").exists()
    with pytest.raises(FileExistsError):
        recovery.execute_recovery_task(path, sha, 0)


@pytest.mark.parametrize("which", ["counts.txt", "aligned.Log.final.out", "strand.json", "qc_raw/mate1_fastqc.html"])
def test_reuse_refuses_changed_scientific_artifact(cohort, which):
    c = cohort
    manifest, path, sha = capture(c)
    allocation(c, 0)
    artifact = Path(c.jobs[0]["output"]) / which
    artifact.write_text(artifact.read_text() + "changed")
    with pytest.raises((recovery.RNARecoveryError, json.JSONDecodeError)):
        recovery.execute_recovery_task(path, sha, 0)
    assert not (c.case / "receipts/task-0.json").exists()


def test_reuse_refuses_changed_script_or_manifest(cohort):
    c = cohort
    manifest, path, sha = capture(c)
    allocation(c, 0)
    script = Path(c.jobs[0]["script"])
    script.write_text(script.read_text() + "echo changed\n")
    with pytest.raises(recovery.RNARecoveryError, match="Bound file"):
        recovery.execute_recovery_task(path, sha, 0)
    write_json(path, {**manifest, "created_unix": 0})
    with pytest.raises(recovery.RNARecoveryError, match="manifest bytes"):
        recovery.execute_recovery_task(path, sha, 0)


@pytest.mark.parametrize("env", [{"SLURM_ARRAY_TASK_ID": "1"}, {"SLURM_CPUS_PER_TASK": "4"}, {"SLURM_ARRAY_JOB_ID": "5866358"}])
def test_execution_requires_exact_new_native_task_and_allocation(cohort, env):
    c = cohort
    _, path, sha = capture(c)
    allocation(c, 0)
    for key, value in env.items():
        c.monkeypatch.setenv(key, value)
    with pytest.raises(recovery.RNARecoveryError):
        recovery.execute_recovery_task(path, sha, 0)


def test_registration_requires_final_native_capture_and_preservation(cohort):
    c = cohort
    manifest, path, sha = capture(c)
    prep, prep_sha = preserve_failed(c, manifest, sha)
    context = recovery.validate_registration_manifest(path, sha, prep, prep_sha)
    assert context["capture_native_record"]["job_id"] == "9000"
    assert context["preparation_native_record"]["job_id"] == "9002"
    c.records["9002"]["state"] = "RUNNING"
    write_json(c.root / "state/resource-budget.json", {"native_jobs": c.records})
    with pytest.raises(recovery.RNARecoveryError, match="final native"):
        recovery.validate_registration_manifest(path, sha, prep, prep_sha)


def test_rerun_refuses_missing_preparation_or_stale_output(cohort):
    c = cohort
    manifest, path, sha = capture(c)
    allocation(c, 18)
    with pytest.raises(recovery.RNARecoveryError, match="preservation proof"):
        recovery.execute_recovery_task(path, sha, 18)
    prep, prep_sha = preserve_failed(c, manifest, sha)
    Path(c.jobs[18]["output"]).mkdir()
    with pytest.raises(recovery.RNARecoveryError, match="isolated"):
        recovery.execute_recovery_task(path, sha, 18, prep, prep_sha)
    assert not (c.case / "receipts/task-18.started.json").exists()


def test_rerun_refuses_replaced_preserved_directory(cohort):
    c = cohort
    manifest, path, sha = capture(c)
    prep, prep_sha = preserve_failed(c, manifest, sha)
    allocation(c, 18)
    output = c.case / "preserved" / c.jobs[18]["biological_id"]
    output.rename(output.with_name(output.name + "-original"))
    output.mkdir()
    with pytest.raises(recovery.RNARecoveryError, match="original inode"):
        recovery.execute_recovery_task(path, sha, 18, prep, prep_sha)


def test_failed_primary_run_is_single_attempt_and_preserves_originals(cohort, monkeypatch):
    c = cohort
    manifest, path, sha = capture(c)
    prep, prep_sha = preserve_failed(c, manifest, sha)
    allocation(c, 18)
    seen = []
    def failed(command, **kwargs):
        seen.append((command, kwargs))
        return SimpleNamespace(returncode=9)
    monkeypatch.setattr(recovery.subprocess, "run", failed)
    with pytest.raises(recovery.RNARecoveryError, match="Primary RNA task failed"):
        recovery.execute_recovery_task(path, sha, 18, prep, prep_sha)
    command, options = seen[0]
    assert command[1:3] == ["-s", "-c"]
    assert command[3] == recovery.PRIMARY_BOOTSTRAP
    assert command[4:] == [str(c.primary), str(c.root), recovery.PRIMARY_SOURCE_SHA256, recovery.PRIMARY_CONFIG_SHA256, "18"]
    assert options["cwd"] == str(c.primary)
    assert options["env"]["PYTHONPATH"].split(os.pathsep)[0] == str((c.primary / "src").resolve())
    assert options["env"]["PYTHONNOUSERSITE"] == "1"
    assert "from horsehair.cli import main" in command[3]
    assert (c.case / "preserved" / c.jobs[18]["biological_id"] / "failed.partial").read_text().startswith("Preserve")
    result = json.loads((c.case / "receipts/task-18.json").read_text())
    assert result["status"] == "primary_failed" and result["returncode"] == 9
    assert result["storage_headroom"]["required_files"] == 1860
    assert result["storage_headroom"]["reserve_files"] == 248140
    assert result["storage_headroom"]["required_bytes"] == 615855581617
    with pytest.raises(FileExistsError):
        recovery.execute_recovery_task(path, sha, 18, prep, prep_sha)
    assert len(seen) == 1


def test_quota_pressure_refuses_new_primary_execution(cohort, monkeypatch):
    c = cohort
    manifest, path, sha = capture(c)
    prep, prep_sha = preserve_failed(c, manifest, sha)
    allocation(c, 18)
    def pending(*args, **kwargs):
        raise ValueError("Personal scratch inode headroom is insufficient")
    monkeypatch.setattr(recovery, "require_storage_headroom", pending)
    monkeypatch.setattr(recovery.subprocess, "run", lambda *a, **kw: pytest.fail("quota failure must not execute primary tools"))
    with pytest.raises(ValueError, match="inode headroom"):
        recovery.execute_recovery_task(path, sha, 18, prep, prep_sha)
    assert not (c.case / "receipts/task-18.started.json").exists()
    assert not Path(c.jobs[18]["output"]).exists()


def test_zero_elapsed_failed_native_record_does_not_erase_its_failure(cohort):
    c = cohort
    c.records["5866358_25"]["elapsed_seconds"] = 0
    write_json(c.root / "state/resource-budget.json", {"native_jobs": c.records})
    manifest, path, sha = capture(c)
    assert manifest["tasks"]["25"]["previous_native_record"]["state"] == "FAILED"
    assert manifest["tasks"]["25"]["action"] == "rerun"


def test_missing_measured_admission_uses_strict_full_configured_budget(cohort):
    c = cohort
    (c.root / "state/admission-rna-full.json").unlink()
    manifest, path, sha = capture(c)
    assert manifest["storage_demand"]["required_files"] == 250000
    assert manifest["storage_demand"]["required_bytes"] == 6000000000000
    assert manifest["storage_admission_snapshot"] is None


def test_primary_environment_preserves_alliance_dependencies_and_excludes_other_horsehair(cohort):
    c = cohort
    manifest, path, sha = capture(c)
    alliance = c.root.parent.parent / "cvmfs-alliance-arrow"
    alliance.mkdir()
    foreign = c.root.parent.parent / "other-release/src"
    (foreign / "horsehair").mkdir(parents=True)
    maintenance = Path(manifest["maintenance_release"]) / "src"
    c.monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(maintenance), str(alliance), str(foreign), str(c.primary / "src")]))
    environment = recovery._primary_environment(manifest)
    assert environment["PYTHONPATH"].split(os.pathsep) == [str((c.primary / "src").resolve()), str(alliance.resolve())]
    assert environment["PYTHONNOUSERSITE"] == "1" and environment["PYTHONDONTWRITEBYTECODE"] == "1"


def test_primary_success_keeps_new_scientific_receipt_and_original_source(cohort, monkeypatch):
    c = cohort
    manifest, path, sha = capture(c)
    prep, prep_sha = preserve_failed(c, manifest, sha)
    allocation(c, 18)
    # Simulate only a successful native primary process; the guard itself must
    # not manufacture its scientific receipt, outputs, or mapping.
    source = Path(c.jobs[0]["output"])
    def success(command, **kwargs):
        import shutil
        directory = Path(c.jobs[18]["output"])
        shutil.copytree(source, directory)
        old = json.loads((c.root / "state/rna-full-0.json").read_text())
        qc = {**old["qc"], "biological_id": c.jobs[18]["biological_id"]}
        old.update(biological_id=c.jobs[18]["biological_id"], slurm_job_id="9001_18", qc=qc)
        write_json(directory / "qc.json", qc)
        write_json(c.root / "state/rna-full-18.json", old)
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(recovery.subprocess, "run", success)
    receipt = recovery.execute_recovery_task(path, sha, 18, prep, prep_sha)
    assert receipt["status"] == "processed"
    scientific = json.loads((c.root / "state/rna-full-18.json").read_text())
    assert scientific["slurm_job_id"] == "9001_18"
    assert scientific["source_sha256"] == recovery.PRIMARY_SOURCE_SHA256
    assert digest(c.root / "state/rna-full-0.json") == manifest["tasks"]["0"]["reuse"]["scientific_receipt"]["sha256"]


def test_direct_entrypoint_does_not_require_maintenance_pythonpath():
    path = Path(recovery.__file__).resolve()
    result = subprocess.run([os.sys.executable, "-I", str(path), "--help"], capture_output=True, text=True)
    assert result.returncode == 0
    assert "prepare" in result.stdout and "run" in result.stdout
