"""Synthetic fixtures for the honest replacement dependency barrier."""
import copy
import json
import shlex
from pathlib import Path
from types import SimpleNamespace

import pytest

from horsehair import repair, rna_followup as followup, rna_recovery as recovery
from horsehair.core import canonical_hash, digest, read_json, source_identity


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n")


def native(ident, state="COMPLETED", exit_code="0:0", cpus=8):
    return {"job_id": ident, "basis": "sacct final allocated wall time", "state": state,
            "exit_code": exit_code, "cpus": cpus, "elapsed_seconds": 20}


@pytest.fixture
def completed(tmp_path, monkeypatch):
    root = tmp_path / "study"
    old_case = root / "maintenance/original"
    plan_path = root / recovery.PLAN_RELATIVE_PATH
    jobs = [{"biological_id": "brain-" + str(index), "cohort": "discovery", "threads": 8,
             "output": str(root / "data" / ("brain-" + str(index)))} for index in range(60)]
    plan = {"jobs": jobs}
    write(plan_path, plan)
    old = {"root": str(root), "primary_release": str(tmp_path / "primary"), "case_directory": str(old_case),
           "plan": followup._binding(plan_path), "plan_sha256": canonical_hash(plan),
           "previous_array_job_id": recovery.PREVIOUS_ARRAY_JOB_ID, "capture_slurm_job_id": "9002",
           "reuse_indices": list(recovery.REUSED_INDICES), "rerun_indices": list(recovery.RERUN_INDICES)}
    manifest_path, prep_path = old_case / "manifest.json", old_case / "preparation.json"
    write(manifest_path, old)
    old_sha = digest(manifest_path)
    write(prep_path, {"schema_version": 1, "status": "prepared", "manifest_sha256": old_sha, "slurm_job_id": "9002",
                      "primary_source_sha256": recovery.PRIMARY_SOURCE_SHA256,
                      "full_config_sha256": recovery.PRIMARY_CONFIG_SHA256, "plan_sha256": canonical_hash(plan),
                      "tasks": {str(index): {} for index in recovery.RERUN_INDICES}})
    records = {ident: native(ident, cpus=1) for ident in ("9000", "9001", "9002")}
    output_bindings = {}
    science_bindings = {}
    for index, job in enumerate(jobs):
        ident = followup.PREVIOUS_ARRAY_JOB_ID + "_" + str(index)
        old_ident = recovery.PREVIOUS_ARRAY_JOB_ID + "_" + str(index)
        records[ident] = native(ident, "FAILED", "1:0") if index == 27 else native(ident)
        records[old_ident] = native(old_ident)
        output = Path(job["output"]) / "counts.txt"
        output.parent.mkdir(parents=True)
        output.write_text("Stable primary counts " + str(index))
        outputs = {"counts.txt": followup._binding(output)}
        output_bindings[str(index)] = outputs
        receipt = {"status": "processed", "slurm_job_id": old_ident if index in recovery.REUSED_INDICES else ident,
                   "native_execution": True, "source_sha256": recovery.PRIMARY_SOURCE_SHA256,
                   "config_sha256": recovery.PRIMARY_CONFIG_SHA256, "full_config_sha256": recovery.PRIMARY_CONFIG_SHA256,
                   "plan_hash": canonical_hash(plan), "biological_id": job["biological_id"], "cohort": job["cohort"], "allocated_cpus": 8}
        science = root / "state" / ("rna-full-" + str(index) + ".json")
        write(science, receipt)
        science_bindings[str(index)] = followup._binding(science)
        if index == 27:
            continue
        prior = {"status": "reused" if index in recovery.REUSED_INDICES else "processed", "index": index,
                 "biological_id": job["biological_id"], "manifest_sha256": old_sha,
                 "primary_source_sha256": recovery.PRIMARY_SOURCE_SHA256,
                 "full_config_sha256": recovery.PRIMARY_CONFIG_SHA256, "plan_sha256": canonical_hash(plan),
                 "recovery_slurm_job_id": ident, "previous_slurm_job_id": old_ident}
        evidence = {"scientific_receipt": science_bindings[str(index)], "outputs": outputs}
        if index in recovery.REUSED_INDICES:
            prior["reuse"] = {**evidence, "native_record": records[old_ident]}
        else:
            prior.update(evidence)
        write(old_case / "receipts" / ("task-" + str(index) + ".json"), prior)
    write(root / "state/resource-budget.json", {"native_jobs": records})
    framework = Path(followup.__file__).resolve().parents[2]
    identity = source_identity(framework)
    monkeypatch.setattr(followup, "source_identity", lambda path: identity)
    verification = root / "maintenance/verification/receipt.json"
    intent = old_case / "receipts/task-27.started.json"
    write(intent, {"status": "execution_intent"})
    failure_log = root / "logs/rna-counts-6570797_27.log"
    failure_log.parent.mkdir()
    failure_log.write_text("Preserved original wrapper failure traceback")
    proof = {"schema_version": 1, "status": "verified_after_wrapper_failure", "index": 27,
             "kind": "Verification only; no scientific recomputation or native outcome relabelling",
             "biological_id": jobs[27]["biological_id"],
             "verification_slurm_job_id": "9001", "failed_native_record": records[followup.PREVIOUS_ARRAY_JOB_ID + "_27"],
             "primary_source_sha256": recovery.PRIMARY_SOURCE_SHA256, "full_config_sha256": recovery.PRIMARY_CONFIG_SHA256,
             "plan_sha256": canonical_hash(plan), "verifier_source_identity": identity,
             "original_maintenance_source_sha256": followup.PREVIOUS_FRAMEWORK_SHA256,
             "recovery_manifest": followup._binding(manifest_path), "preparation": followup._binding(prep_path),
             "original_execution_intent": followup._binding(intent), "failure_log": followup._binding(failure_log),
             "scientific_receipt": science_bindings["27"], "outputs": output_bindings["27"]}
    write(verification, proof)
    monkeypatch.setattr(recovery, "load_frozen_recovery_manifest", lambda *args: copy.deepcopy(old))
    monkeypatch.setattr(followup, "_context", lambda original: (root, {"release_id": "primary", "source_identity": {"sha256": recovery.PRIMARY_SOURCE_SHA256}}, {}, plan, framework, identity))
    monkeypatch.setattr(recovery, "_scientific_outputs", lambda plan, job, receipt: {"counts.txt": followup._binding(Path(job["output"]) / "counts.txt")})
    monkeypatch.setenv("SLURM_JOB_ID", "9000")
    monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)
    monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
    return SimpleNamespace(root=root, old=old, manifest=manifest_path, manifest_sha=old_sha,
                           preparation=prep_path, verification=verification,
                           case=root / "maintenance/followup", records=records, monkeypatch=monkeypatch,
                           science_bindings=science_bindings)


def capture(c):
    return followup.capture_followup(c.manifest, c.manifest_sha, c.preparation, digest(c.preparation),
                                    c.verification, digest(c.verification), c.case)


def allocation(c, index):
    c.monkeypatch.setenv("SLURM_ARRAY_JOB_ID", "9100")
    c.monkeypatch.setenv("SLURM_ARRAY_TASK_ID", str(index))
    c.monkeypatch.setenv("SLURM_CPUS_PER_TASK", "8")


def test_capture_and_registration_preserve_every_original_scientific_receipt(completed):
    c = completed
    before = {path: Path(path).read_bytes() for path in (b["path"] for b in c.science_bindings.values())}
    manifest = capture(c)
    assert manifest["rerun_indices"] == [] and manifest["reuse_indices"] == list(range(60))
    result = followup.validate_registration_manifest(c.case / "manifest.json", digest(c.case / "manifest.json"),
                                                    c.case / "preparation.json", digest(c.case / "preparation.json"))
    assert result["manifest"] == manifest
    assert result["preparation"]["scientific_subprocesses"] == 0
    assert all(Path(path).read_bytes() == value for path, value in before.items())
    assert c.records[followup.PREVIOUS_ARRAY_JOB_ID + "_27"]["state"] == "FAILED"


@pytest.mark.parametrize("index", [0, 18, 20, 27, 59])
def test_successor_members_validate_existing_work_without_scientific_execution(completed, index):
    c = completed
    capture(c)
    allocation(c, index)
    result = followup.execute_followup_task(c.case / "manifest.json", digest(c.case / "manifest.json"), index,
                                            c.case / "preparation.json", digest(c.case / "preparation.json"))
    assert result["status"] == "validated_existing_primary_output" and result["scientific_execution"] is False
    assert result["followup_slurm_job_id"] == "9100_" + str(index)
    with pytest.raises(FileExistsError):
        followup.execute_followup_task(c.case / "manifest.json", digest(c.case / "manifest.json"), index,
                                       c.case / "preparation.json", digest(c.case / "preparation.json"))


@pytest.mark.parametrize("fault", ["running", "second-failure", "verifier-failed", "verifier-zero-cpu", "wrong-science", "changed-output", "wrong-verifier-source", "wrong-verifier-brain", "wrong-previous-native", "changed-intent", "changed-log", "wrong-proof-kind"])
def test_capture_refuses_incomplete_ambiguous_or_changed_evidence_without_new_case(completed, fault):
    c = completed
    if fault == "running":c.records[followup.PREVIOUS_ARRAY_JOB_ID + "_59"]["state"] = "RUNNING"
    elif fault == "second-failure":c.records[followup.PREVIOUS_ARRAY_JOB_ID + "_59"].update(state="FAILED", exit_code="1:0")
    elif fault == "verifier-failed":c.records["9001"].update(state="FAILED", exit_code="1:0")
    elif fault == "verifier-zero-cpu":c.records["9001"]["cpus"] = 0
    elif fault == "wrong-science":
        path = Path(c.science_bindings["59"]["path"]); value = read_json(path); value["biological_id"] = "other"; write(path, value)
    elif fault == "changed-output":
        path = c.root / "data/brain-59/counts.txt";path.write_text("Different counts")
    elif fault == "changed-intent":
        (Path(c.old["case_directory"]) / "receipts/task-27.started.json").write_text("changed intent")
    elif fault == "changed-log":
        (c.root / "logs/rna-counts-6570797_27.log").write_text("changed failure log")
    else:
        value = read_json(c.verification)
        if fault == "wrong-verifier-source":value["verifier_source_identity"] = {"sha256": "wrong"}
        elif fault == "wrong-verifier-brain":value["biological_id"] = "wrong"
        elif fault == "wrong-proof-kind":value["kind"] = "scientific rerun"
        else:value["failed_native_record"]["exit_code"] = "2:0"
        write(c.verification, value)
    write(c.root / "state/resource-budget.json", {"native_jobs": c.records})
    with pytest.raises(recovery.RNARecoveryError):capture(c)
    assert not c.case.exists()


def test_followup_refuses_changed_counts_after_capture(completed):
    c = completed
    capture(c)
    (c.root / "data/brain-27/counts.txt").write_text("changed after capture")
    allocation(c, 27)
    with pytest.raises(recovery.RNARecoveryError):
        followup.execute_followup_task(c.case / "manifest.json", digest(c.case / "manifest.json"), 27,
                                       c.case / "preparation.json", digest(c.case / "preparation.json"))
    assert not (c.case / "receipts/task-27.json").exists()


@pytest.mark.parametrize("fault", ["wrong-cpu", "wrong-index", "same-array", "no-allocation"])
def test_followup_requires_exact_new_native_array_identity(completed, fault):
    c = completed
    capture(c)
    allocation(c, 27)
    if fault == "wrong-cpu":c.monkeypatch.setenv("SLURM_CPUS_PER_TASK", "4")
    elif fault == "wrong-index":c.monkeypatch.setenv("SLURM_ARRAY_TASK_ID", "28")
    elif fault == "same-array":c.monkeypatch.setenv("SLURM_ARRAY_JOB_ID", followup.PREVIOUS_ARRAY_JOB_ID)
    else:
        c.monkeypatch.delenv("SLURM_JOB_ID");c.monkeypatch.delenv("SLURM_ARRAY_JOB_ID");c.monkeypatch.delenv("SLURM_ARRAY_TASK_ID")
    with pytest.raises(recovery.RNARecoveryError):
        followup.execute_followup_task(c.case / "manifest.json", digest(c.case / "manifest.json"), 27,
                                       c.case / "preparation.json", digest(c.case / "preparation.json"))


def wrapper(binding, module="rna_recovery"):
    return ("python " + shlex.quote(str(Path(binding["framework_release"]) / ("src/horsehair/" + module + ".py")))
            + " run --manifest " + shlex.quote(binding["manifest_path"]) + ' --index "$SLURM_ARRAY_TASK_ID" --manifest-sha256 '
            + binding["manifest_sha256"] + " --preparation " + shlex.quote(binding["preparation_path"])
            + " --preparation-sha256 " + binding["preparation_sha256"])


def test_second_wrapper_projection_binds_prior_guard_and_keeps_exact_primary_command(tmp_path):
    prior = {"framework_release": "/project/old", "manifest_path": "/scratch/study/maintenance/old/manifest.json",
             "manifest_sha256": "a" * 64, "preparation_path": "/scratch/study/maintenance/old/preparation.json",
             "preparation_sha256": "b" * 64}
    binding = {**prior, "framework_release": "/project/new", "root": "/scratch/study", "guard_module": "rna_followup", "previous_guard": prior}
    old_text = "#!/bin/bash\n" + wrapper(prior) + "\n"
    projected = repair._rna_followup_prior_projection(old_text, binding)
    new_text = "#!/bin/bash\n" + wrapper(binding, "rna_followup") + "\n"
    assert repair._rna_recovery_script_projection(new_text, projected, binding) == projected
    with pytest.raises(ValueError, match="prior RNA script"):
        repair._rna_followup_prior_projection(old_text.replace("a" * 64, "c" * 64), binding)
    with pytest.raises(ValueError, match="pinned invocation"):
        repair._rna_recovery_script_projection(new_text.replace("rna_followup.py", "other.py"), projected, binding)


def test_guard_kinds_cannot_be_combined():
    with pytest.raises(ValueError, match="two RNA guard kinds"):
        repair._rna_recovery_binding({"rna_recovery": {}, "rna_followup": {}}, set(), None, None, None, None, None, None)


@pytest.mark.parametrize("fault", [None, "missing-amendment", "changed-manifest", "changed-preparation"])
def test_registration_followup_binding_requires_exact_canonical_prior_amendment(completed, fault):
    c = completed
    manifest = capture(c)
    manifest_path, preparation_path = c.case / "manifest.json", c.case / "preparation.json"
    primary = Path(c.old["primary_release"])
    framework = Path(followup.__file__).resolve().parents[2]
    identity = {"sha256": recovery.PRIMARY_SOURCE_SHA256}
    qualification = c.root / "maintenance/qualification.json"
    write(qualification, {"fixture": True})
    prior = {"manifest_path": str(c.manifest), "manifest_sha256": c.manifest_sha,
             "preparation_path": str(c.preparation), "preparation_sha256": digest(c.preparation)}
    ledger = {"tasks": {"rna-counts": {"job_id": followup.PREVIOUS_ARRAY_JOB_ID}},
              "dag_amendments": [{"replaced_tasks": ["rna-counts", "ant-phase", "ant-score-freeze"], "rna_recovery": prior}]}
    if fault == "missing-amendment":ledger["dag_amendments"] = []
    elif fault == "changed-manifest":prior["manifest_sha256"] = "0" * 64
    elif fault == "changed-preparation":prior["preparation_sha256"] = "0" * 64
    qualified = {"framework_release": str(framework), "framework_source_identity": manifest["maintenance_source_identity"]}
    c.monkeypatch.setattr(repair, "_controller_maintenance", lambda *args: qualified)
    change = {"framework_release": str(framework), "qualification_path": str(qualification),
              "qualification_sha256": digest(qualification), "qualification_job_id": "9003",
              "manifest_path": str(manifest_path), "manifest_sha256": digest(manifest_path),
              "preparation_path": str(preparation_path), "preparation_sha256": digest(preparation_path)}
    descriptor = {"phase": "rna-full", "rna_followup": change,
                  "execution_releases": {name: str(primary) for name in ("rna-counts", "ant-phase", "ant-score-freeze")}}
    call = lambda: repair._rna_recovery_binding(descriptor, {"rna-counts", "ant-phase", "ant-score-freeze"},
                                              primary, identity, {}, c.root,
                                              {"rna-counts": {"resources": {"array": "0-59%2"}}}, ledger)
    if fault:
        with pytest.raises(ValueError, match="canonical first-recovery"):call()
    else:
        result = call()
        assert result["guard_module"] == "rna_followup" and result["previous_guard"] == prior
        assert result["rerun_indices"] == [] and result["reuse_indices"] == list(range(60))
