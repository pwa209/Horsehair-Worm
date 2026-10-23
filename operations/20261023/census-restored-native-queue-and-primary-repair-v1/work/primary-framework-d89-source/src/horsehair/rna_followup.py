"""A second RNA array validates completed work without rerunning the science.

The failed original scheduler allocation remains failed. A separate qualified
native verification proves its completed primary output; the successor array
provides a fresh, honest dependency barrier without relabelling that evidence.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import time

if __package__ in {None, ""}:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from horsehair.core import canonical_hash, native_job_id, read_json, source_identity
from horsehair import rna_recovery as recovery

PREVIOUS_ARRAY_JOB_ID = "6570797"
PREVIOUS_FRAMEWORK_SHA256 = "f88e3c277ea8a59b6b9d952b4a0dad4d9071fae5f7929cf3438be0293116aa9f"
VERIFIED_FAILURE_INDEX = 27
INDICES = tuple(range(60))


def _require(condition, message):
    recovery._require(condition, message)


def _binding(path):
    return recovery._binding(Path(path))


def _unchanged(binding):
    _require(_binding(binding["path"]) == binding, "Follow-up bound evidence changed: " + binding["path"])


def _context(old):
    root, primary = Path(old["root"]), Path(old["primary_release"])
    release, config, plan = recovery._validate_inputs(root, primary, Path(old["plan"]["path"]), old["plan_sha256"])
    framework = Path(__file__).resolve().parents[2]
    record = read_json(framework / "release.json")
    identity = source_identity(framework)
    _require(record.get("source_identity") == identity and
             read_json(framework / "configs/study.json") == config and
             record.get("config_sha256") == canonical_hash(config), "Follow-up framework is not immutable with the unchanged full configuration")
    return root, release, config, plan, framework, identity


def _original(manifest_path, manifest_sha256, preparation_path, preparation_sha256):
    old = recovery.load_frozen_recovery_manifest(Path(manifest_path), manifest_sha256, PREVIOUS_FRAMEWORK_SHA256)
    _require(old.get("previous_array_job_id") == recovery.PREVIOUS_ARRAY_JOB_ID and
             old.get("reuse_indices") == list(recovery.REUSED_INDICES) and
             old.get("rerun_indices") == list(recovery.RERUN_INDICES), "Unexpected first recovery cohort")
    _require(_binding(preparation_path)["sha256"] == preparation_sha256, "First preparation bytes changed")
    prep = read_json(preparation_path)
    _require(prep.get("schema_version") == 1 and prep.get("status") == "prepared" and
             prep.get("manifest_sha256") == manifest_sha256 and
             prep.get("primary_source_sha256") == recovery.PRIMARY_SOURCE_SHA256 and
             prep.get("full_config_sha256") == recovery.PRIMARY_CONFIG_SHA256 and
             prep.get("plan_sha256") == old["plan_sha256"] and
             set(prep.get("tasks", {})) == {str(index) for index in recovery.RERUN_INDICES} and
             Path(preparation_path).resolve() == Path(old["case_directory"]) / "preparation.json",
             "First preparation is not bound to its manifest/case")
    root = Path(old["root"])
    recovery._native_record(root, str(old["capture_slurm_job_id"]), completed=True)
    recovery._native_record(root, str(prep["slurm_job_id"]), completed=True)
    return old


def _scientific(root, plan, index, expected_ident):
    job = plan["jobs"][index]
    path = root / "state" / ("rna-full-" + str(index) + ".json")
    binding = _binding(path)
    value = read_json(path)
    expected = {"status": "processed", "slurm_job_id": expected_ident, "native_execution": True,
                "source_sha256": recovery.PRIMARY_SOURCE_SHA256,
                "config_sha256": recovery.PRIMARY_CONFIG_SHA256,
                "full_config_sha256": recovery.PRIMARY_CONFIG_SHA256,
                "plan_hash": canonical_hash(plan), "biological_id": job["biological_id"],
                "cohort": job["cohort"], "allocated_cpus": 8}
    _require(all(value.get(key) == item for key, item in expected.items()), "Original scientific receipt mapping changed")
    outputs = recovery._scientific_outputs(plan, job, value)
    _require(_binding(path) == binding, "Scientific receipt changed during output validation")
    return {"scientific_receipt": binding, "outputs": outputs}


def _verified_failure(root, old, old_sha, plan, verification_path, verification_sha):
    _require(Path(verification_path).resolve().is_relative_to(root / "maintenance"),
             "Separate wrapper verification is outside personal maintenance storage")
    _require(_binding(verification_path)["sha256"] == verification_sha, "Separate wrapper verification bytes changed")
    proof = read_json(verification_path)
    index, ident = VERIFIED_FAILURE_INDEX, PREVIOUS_ARRAY_JOB_ID + "_" + str(VERIFIED_FAILURE_INDEX)
    native = recovery._native_record(root, ident)
    verifier = str(proof.get("verification_slurm_job_id", ""))
    verifier_native = recovery._native_record(root, verifier, completed=True)
    framework = Path(__file__).resolve().parents[2]
    expected = {"schema_version": 1, "status": "verified_after_wrapper_failure", "index": index,
                "kind": "Verification only; no scientific recomputation or native outcome relabelling",
                "biological_id": plan["jobs"][index]["biological_id"],
                "failed_native_record": native, "primary_source_sha256": recovery.PRIMARY_SOURCE_SHA256,
                "full_config_sha256": recovery.PRIMARY_CONFIG_SHA256, "plan_sha256": canonical_hash(plan),
                "verifier_source_identity": source_identity(framework),
                "original_maintenance_source_sha256": PREVIOUS_FRAMEWORK_SHA256}
    _require(all(proof.get(key) == item for key, item in expected.items()) and
             native.get("state") == "FAILED" and native.get("exit_code") == "1:0" and native.get("cpus") == 8,
             "Separate verification does not prove this exact failed allocation and qualified verifier")
    _require(proof.get("recovery_manifest") == _binding(Path(old["case_directory"]) / "manifest.json") and
             proof["recovery_manifest"]["sha256"] == old_sha and
             proof.get("preparation") == _binding(Path(old["case_directory"]) / "preparation.json"),
             "Separate verification refers to another original recovery/preparation")
    intent = proof.get("original_execution_intent", {})
    log = proof.get("failure_log", {})
    expected_intent = Path(old["case_directory"]) / "receipts" / ("task-" + str(index) + ".started.json")
    _require(intent.get("path") == str(expected_intent) and isinstance(log.get("path"), str) and
             Path(log["path"]).is_relative_to(root / "logs") and ident in Path(log["path"]).name,
             "Separate verification intent/log locations changed")
    _unchanged(intent)
    _unchanged(log)
    scientific = _scientific(root, plan, index, ident)
    _require(all(proof.get(key) == value for key, value in scientific.items()), "Separately verified outputs changed")
    _require(_binding(verification_path)["sha256"] == verification_sha, "Separate verification changed during validation")
    return {**scientific, "previous_native_record": native, "verification": _binding(verification_path),
            "verification_native_record": verifier_native}


def _completed(root, old, old_sha, plan, index):
    ident = PREVIOUS_ARRAY_JOB_ID + "_" + str(index)
    native = recovery._native_record(root, ident, completed=True)
    receipt_path = Path(old["case_directory"]) / "receipts" / ("task-" + str(index) + ".json")
    receipt_binding = _binding(receipt_path)
    receipt = read_json(receipt_path)
    reused = index in recovery.REUSED_INDICES
    expected = {"status": "reused" if reused else "processed", "index": index,
                "biological_id": plan["jobs"][index]["biological_id"], "manifest_sha256": old_sha,
                "primary_source_sha256": recovery.PRIMARY_SOURCE_SHA256,
                "full_config_sha256": recovery.PRIMARY_CONFIG_SHA256, "plan_sha256": canonical_hash(plan),
                "recovery_slurm_job_id": ident,
                "previous_slurm_job_id": recovery.PREVIOUS_ARRAY_JOB_ID + "_" + str(index)}
    _require(all(receipt.get(key) == item for key, item in expected.items()) and native.get("cpus") == 8,
             "Prior completed recovery receipt/native identity changed")
    scientific_ident = expected["previous_slurm_job_id"] if reused else ident
    scientific = _scientific(root, plan, index, scientific_ident)
    prior = receipt.get("reuse", {}) if reused else receipt
    _require(all(prior.get(key) == value for key, value in scientific.items()), "Completed recovery scientific bindings changed")
    if reused:
        _require(recovery._native_record(root, scientific_ident, completed=True) == prior.get("native_record"),
                 "Original reused allocation changed")
    _require(_binding(receipt_path) == receipt_binding, "Prior recovery receipt changed during validation")
    return {**scientific, "previous_native_record": native, "previous_recovery_receipt": receipt_binding}


def capture_followup(manifest_path, manifest_sha256, preparation_path, preparation_sha256,
                     verification_path, verification_sha256, case_directory):
    """Capture all completed primary artifacts in a new native maintenance case."""
    _require(native_job_id() is not None and not os.environ.get("SLURM_ARRAY_TASK_ID"), "Follow-up capture requires a scalar native allocation")
    old = _original(manifest_path, manifest_sha256, preparation_path, preparation_sha256)
    root, release, config, plan, framework, identity = _context(old)
    case = Path(case_directory).resolve()
    _require(case.is_relative_to(root / "maintenance") and not case.exists() and not case.is_symlink(), "Follow-up case must be new personal maintenance storage")
    # Require every member terminal before doing any expensive validation or
    # creating the immutable case. No active science is interrupted or adopted.
    for index in INDICES:
        recovery._native_record(root, PREVIOUS_ARRAY_JOB_ID + "_" + str(index), completed=index != VERIFIED_FAILURE_INDEX)
    tasks = {}
    for index in INDICES:
        evidence = (_verified_failure(root, old, manifest_sha256, plan, verification_path, verification_sha256)
                    if index == VERIFIED_FAILURE_INDEX else _completed(root, old, manifest_sha256, plan, index))
        tasks[str(index)] = {"index": index, "biological_id": plan["jobs"][index]["biological_id"],
                             "action": "verified_after_wrapper_failure" if index == VERIFIED_FAILURE_INDEX else "reuse",
                             **evidence}
    manifest = {"schema_version": 1, "status": "captured", "kind": "RNA completed-output follow-up; no scientific execution",
                "root": str(root), "case_directory": str(case), "primary_release": str(Path(old["primary_release"])),
                "primary_release_id": release["release_id"], "primary_source_identity": release["source_identity"],
                "full_config_sha256": canonical_hash(config), "plan": _binding(old["plan"]["path"]),
                "plan_sha256": canonical_hash(plan), "maintenance_release": str(framework),
                "maintenance_source_identity": identity, "capture_slurm_job_id": str(native_job_id()),
                "previous_array_job_id": PREVIOUS_ARRAY_JOB_ID, "reuse_indices": list(INDICES), "rerun_indices": [],
                "original_manifest": _binding(manifest_path), "original_preparation": _binding(preparation_path),
                "verification": _binding(verification_path), "tasks": tasks, "created_unix": time.time()}
    case.mkdir(parents=True, exist_ok=False)
    recovery._exclusive_json(case / "manifest.json", manifest)
    preparation = {"schema_version": 1, "status": "ready_without_scientific_execution", "slurm_job_id": str(native_job_id()),
                   "manifest_sha256": _binding(case / "manifest.json")["sha256"], "tasks": list(INDICES),
                   "moved_outputs": [], "scientific_subprocesses": 0, "created_unix": time.time()}
    recovery._exclusive_json(case / "preparation.json", preparation)
    return manifest


def _load_manifest(path, expected_sha):
    path = Path(path).resolve()
    _require(_binding(path)["sha256"] == expected_sha, "Follow-up manifest bytes changed")
    manifest = read_json(path)
    _require(manifest.get("schema_version") == 1 and manifest.get("status") == "captured" and
             manifest.get("kind") == "RNA completed-output follow-up; no scientific execution" and
             manifest.get("previous_array_job_id") == PREVIOUS_ARRAY_JOB_ID and
             manifest.get("reuse_indices") == list(INDICES) and manifest.get("rerun_indices") == [] and
             set(manifest.get("tasks", {})) == {str(index) for index in INDICES} and
             Path(manifest["case_directory"]) / "manifest.json" == path,
             "Follow-up manifest cohort/schema/location changed")
    old = _original(manifest["original_manifest"]["path"], manifest["original_manifest"]["sha256"],
                    manifest["original_preparation"]["path"], manifest["original_preparation"]["sha256"])
    root, release, config, plan, framework, identity = _context(old)
    _require(manifest["maintenance_release"] == str(framework) and manifest["maintenance_source_identity"] == identity and
             manifest["primary_release"] == old["primary_release"] and manifest["primary_source_identity"] == release["source_identity"] and
             manifest["full_config_sha256"] == canonical_hash(config) and manifest["plan_sha256"] == canonical_hash(plan) and
             manifest["plan"] == old["plan"] and
             manifest["root"] == str(root) and Path(manifest["case_directory"]).is_relative_to(root / "maintenance"),
             "Follow-up source/configuration/namespace changed")
    for key in ("original_manifest", "original_preparation", "plan", "verification"):
        _unchanged(manifest[key])
    recovery._native_record(root, manifest["capture_slurm_job_id"], completed=True)
    return manifest, old, plan


def _task_evidence(manifest, old, plan, index):
    root = Path(manifest["root"])
    return (_verified_failure(root, old, manifest["original_manifest"]["sha256"], plan,
                              manifest["verification"]["path"], manifest["verification"]["sha256"])
            if index == VERIFIED_FAILURE_INDEX else
            _completed(root, old, manifest["original_manifest"]["sha256"], plan, index))


def validate_registration_manifest(manifest_path, manifest_sha256, preparation_path, preparation_sha256):
    manifest, old, plan = _load_manifest(manifest_path, manifest_sha256)
    prep_path = Path(preparation_path).resolve()
    _require(prep_path == Path(manifest["case_directory"]) / "preparation.json" and
             _binding(prep_path)["sha256"] == preparation_sha256, "Follow-up preparation changed or relocated")
    preparation = read_json(prep_path)
    _require(preparation.get("status") == "ready_without_scientific_execution" and
             preparation.get("manifest_sha256") == manifest_sha256 and preparation.get("tasks") == list(INDICES) and
             preparation.get("moved_outputs") == [] and preparation.get("scientific_subprocesses") == 0 and
             preparation.get("slurm_job_id") == manifest["capture_slurm_job_id"], "Follow-up preparation does not prove read-only capture")
    for index in INDICES:
        saved = manifest["tasks"][str(index)]
        evidence = _task_evidence(manifest, old, plan, index)
        _require(saved == {"index": index, "biological_id": plan["jobs"][index]["biological_id"],
                           "action": "verified_after_wrapper_failure" if index == VERIFIED_FAILURE_INDEX else "reuse", **evidence},
                 "Follow-up per-brain evidence changed before registration")
    native = recovery._native_record(Path(manifest["root"]), manifest["capture_slurm_job_id"], completed=True)
    return {"manifest": manifest, "preparation": preparation, "capture_native_record": native, "preparation_native_record": native}


def execute_followup_task(manifest_path, manifest_sha256, index, preparation_path, preparation_sha256):
    _require(type(index) is int and index in INDICES, "Follow-up task outside the frozen cohort")
    manifest, old, plan = _load_manifest(manifest_path, manifest_sha256)
    ident = native_job_id()
    _require(ident is not None and os.environ.get("SLURM_ARRAY_TASK_ID") == str(index) and
             os.environ.get("SLURM_CPUS_PER_TASK") == "8" and
             str(ident) != PREVIOUS_ARRAY_JOB_ID + "_" + str(index), "Follow-up requires a new exact eight-CPU array member")
    prep_path = Path(preparation_path).resolve()
    _require(prep_path == Path(manifest["case_directory"]) / "preparation.json" and
             _binding(prep_path)["sha256"] == preparation_sha256, "Follow-up preparation bytes changed")
    preparation = read_json(prep_path)
    _require(preparation.get("manifest_sha256") == manifest_sha256 and preparation.get("status") == "ready_without_scientific_execution" and
             preparation.get("slurm_job_id") == manifest["capture_slurm_job_id"] and preparation.get("tasks") == list(INDICES) and
             preparation.get("moved_outputs") == [] and preparation.get("scientific_subprocesses") == 0,
             "Follow-up preparation no longer proves zero scientific execution")
    evidence = _task_evidence(manifest, old, plan, index)
    saved = manifest["tasks"][str(index)]
    _require(all(saved.get(key) == value for key, value in evidence.items()) and saved.get("index") == index and
             saved.get("biological_id") == plan["jobs"][index]["biological_id"] and
             saved.get("action") == ("verified_after_wrapper_failure" if index == VERIFIED_FAILURE_INDEX else "reuse"),
             "Follow-up output evidence changed since capture")
    receipt = {"schema_version": 1, "status": "validated_existing_primary_output", "index": index,
               "biological_id": saved["biological_id"], "followup_slurm_job_id": str(ident),
               "manifest_sha256": manifest_sha256, "preparation_sha256": preparation_sha256,
               "primary_source_sha256": recovery.PRIMARY_SOURCE_SHA256, "full_config_sha256": recovery.PRIMARY_CONFIG_SHA256,
               "plan_sha256": canonical_hash(plan), "scientific_execution": False, "original_evidence": saved,
               "finished_unix": time.time()}
    recovery._exclusive_json(Path(manifest["case_directory"]) / "receipts" / ("task-" + str(index) + ".json"), receipt)
    return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    capture = sub.add_parser("capture")
    run = sub.add_parser("run")
    for command in (capture, run):
        for name in ("manifest", "manifest-sha256", "preparation", "preparation-sha256"):
            command.add_argument("--" + name, required=True)
    for name in ("verification", "verification-sha256", "case"):
        capture.add_argument("--" + name, required=True)
    run.add_argument("--index", type=int, required=True)
    args = parser.parse_args(argv)
    if args.action == "capture":
        result = capture_followup(args.manifest, args.manifest_sha256, args.preparation, args.preparation_sha256,
                                  args.verification, args.verification_sha256, args.case)
    else:
        result = execute_followup_task(args.manifest, args.manifest_sha256, args.index, args.preparation, args.preparation_sha256)
    print("RNA_FOLLOWUP " + str({"status": result["status"], "scientific_execution": False}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
