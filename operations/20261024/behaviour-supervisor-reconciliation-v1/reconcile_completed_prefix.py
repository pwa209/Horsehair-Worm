"""Read-only, hash-bound attestation of one preserved technical worker prefix.

This module neither loads artifact objects nor runs a worker, scheduler command,
or canonical-state write. The caller supplies fresh native evidence and the exact
original helper's validate_worker_result callable after binding that helper.
"""
from pathlib import Path, PurePosixPath
import copy
import hashlib
import json
import os
import re
import stat


ORIGINAL_JOB = "6865674"
ORIGINAL_HELPER_SHA256 = "dec3ace9591b71759bbd7418b550e657ecb87a85823652e9d1e56547fe19ec60"
CONTEXT_SHA256 = "e055352de535d029c658723b69707c371ab5d89e451cace506d305c344a3fd0f"
PRIMARY_SHA256 = "0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630"
FRAMEWORK_SHA256 = "8495a3d985515bf71f476295da87e663a1e764864ddce47a06794e7e11d88f94"
CONFIG_SHA256 = "446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35"
COMPLETE = "complete_full_cohort_first_update_technical_prefix"
FLAGS = {name: False for name in (
    "scientific_result", "scientific_fit", "complete_declared_workload",
    "admission_authority", "original_H1_H2_H3_gates_cleared", "native_gpu_qualified",
    "second_derivatives_supported", "whole_cohort_objective_qualified",
    "optimizer_convergence_qualified", "MALA_acceptance_qualified")}
STAGES = (
    "full_fold_preprocessing_and_eligibility_census",
    "full_training_objective_and_all_gradients",
    "single_original_clip_and_Adam_update",
    "all_original_validation_calls_with_explicit_nulls")
NATIVE = dict(job_id=ORIGINAL_JOB, state="FAILED", exit_code="1:0",
              elapsed_seconds=18500, cpus=1, owner="pwa209",
              account="def-ptewarie_cpu", start="2026-10-23T11:07:41",
              end="2026-10-23T16:16:01")
OLD_PLAN = "plans/recovery-support-20261021-433/plan.json"
OLD_LEDGER = "state/recovery-support-20261021-433-submission.json"
OLD_SCRIPT = "plans/recovery-support-20261021-433/recovery-433.sbatch"


def require(value, message):
    if not value:
        raise ValueError(message)


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode()


def canonical_hash(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "Duplicate JSON object key refused")
        result[key] = value
    return result


def _json(raw):
    def constant(_value):
        raise ValueError("Nonfinite JSON number refused")
    result = json.loads(raw, object_pairs_hook=_pairs, parse_constant=constant)
    require(type(result) is dict, "JSON object evidence required")
    canonical_bytes(result)
    return result


def _relative(value):
    require(type(value) is str and value and "\\" not in value and ":" not in value,
            "Canonical root-relative path required")
    path = PurePosixPath(value)
    require(not path.is_absolute() and path.as_posix() == value
            and all(part not in {"", ".", ".."} for part in path.parts),
            "Canonical confined relative path required")
    return path.parts


def _snapshot(value):
    return dict(device=value.st_dev, inode=value.st_ino, uid=value.st_uid,
                mode=value.st_mode, type=stat.S_IFMT(value.st_mode),
                size=value.st_size, mtime_ns=value.st_mtime_ns,
                ctime_ns=value.st_ctime_ns)


def _root_identity(value):
    return dict(device=value.st_dev, inode=value.st_ino, uid=value.st_uid,
                type=stat.S_IFMT(value.st_mode))


def _checked_path(root, relative):
    path = root
    owner = root.stat().st_uid
    parts = _relative(relative)
    for index, part in enumerate(parts):
        path = path / part
        value = path.lstat()
        require(not stat.S_ISLNK(value.st_mode), "Symlink evidence refused: " + relative)
        require(value.st_uid == owner, "Evidence ownership differs from root: " + relative)
        expected = stat.S_ISREG if index == len(parts) - 1 else stat.S_ISDIR
        require(expected(value.st_mode), "Ordinary confined evidence required: " + relative)
    return path, value


def _open_without_links(root, relative):
    """Use no-follow openat traversal on native POSIX; check every component elsewhere."""
    parts = _relative(relative)
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0)
    if os.open in os.supports_dir_fd and hasattr(os, "O_NOFOLLOW"):
        directory_flags = flags | os.O_DIRECTORY | os.O_NOFOLLOW
        directory = os.open(root, directory_flags)
        try:
            owner = os.fstat(directory).st_uid
            for part in parts[:-1]:
                replacement = os.open(part, directory_flags, dir_fd=directory)
                os.close(directory)
                directory = replacement
                require(os.fstat(directory).st_uid == owner,
                        "Opened evidence directory ownership differs from root")
            return os.open(parts[-1], flags | os.O_NOFOLLOW, dir_fd=directory)
        finally:
            os.close(directory)
    return os.open(root.joinpath(*parts), flags | getattr(os, "O_NOFOLLOW", 0))


def _stable_file(root, relative, expected, *, capture=False):
    require(type(expected) is str and re.fullmatch("[0-9a-f]{64}", expected),
            "Pinned lowercase SHA256 required")
    path, before = _checked_path(root, relative)
    initial = _snapshot(before)
    if capture:
        require(before.st_size <= 20_000_000, "Bounded metadata/source evidence required")
    descriptor = _open_without_links(root, relative)
    chunks, length, digest = [], 0, hashlib.sha256()
    try:
        opened = _snapshot(os.fstat(descriptor))
        # Windows path stat exposes creation time as ctime while descriptor
        # fstat may expose change time. Preserve both clocks independently.
        comparable = set(initial) - ({"ctime_ns"} if os.name == "nt" else set())
        require(all(opened[key] == initial[key] for key in comparable),
                "Evidence changed between stat and open: " + relative)
        while True:
            block = os.read(descriptor, 1024 * 1024)
            if not block:
                break
            length += len(block)
            digest.update(block)
            if capture:
                require(length <= 20_000_000, "Metadata grew beyond bounded size")
                chunks.append(block)
        require(_snapshot(os.fstat(descriptor)) == opened,
                "Evidence changed during streamed read: " + relative)
    finally:
        os.close(descriptor)
    _, after = _checked_path(root, relative)
    require(_snapshot(after) == initial and length == initial["size"],
            "Evidence changed after streamed read: " + relative)
    measured = digest.hexdigest()
    require(measured == expected, "Pinned bytes differ: " + relative)
    return dict(sha256=measured, snapshot=initial), b"".join(chunks) if capture else None


def _flags(value, label):
    require(type(value) is dict and all(value.get(key) is False for key in FLAGS),
            "False technical/result/admission flags required: " + label)


def _sources(value, label, *, source_key="framework_source_sha256"):
    require(value.get("primary_source_sha256") == PRIMARY_SHA256
            and value.get(source_key) == FRAMEWORK_SHA256
            and value.get("full_config_sha256") == CONFIG_SHA256,
            "Exact original source/config bindings required: " + label)


def verify(manifest, *, root, native, validator):
    """Return a certificate from stable reads; never write state or load .pt objects.

    Required manifest keys are schema_version, root, original_job_id, case,
    support, helper_path/helper_sha256, primary_source_sha256,
    framework_source_sha256, full_config_sha256, flags, original_native,
    pinned_files (relative paths), and artifact_pins (absolute artifact paths).
    The caller must load validator from the pinned original helper. Source
    execution/binding and new verification-allocation accounting belong to the
    caller. This function independently validates all persisted byte bindings.
    """
    require(type(manifest) is dict and type(native) is dict and callable(validator),
            "Manifest, native evidence and original validator required")
    manifest = _json(canonical_bytes(manifest))
    native = _json(canonical_bytes(native))
    require(type(manifest.get("schema_version")) is int and manifest["schema_version"] == 1,
            "Manifest schema version 1 required")
    logical_root = Path(root)
    require(logical_root.is_absolute() and manifest.get("root") == str(logical_root),
            "Exact declared root required")
    physical_root = logical_root.resolve(strict=True)
    require(physical_root.is_dir(), "Existing evidence root required")
    root_identity = _root_identity(physical_root.stat())
    require(manifest.get("original_job_id") == ORIGINAL_JOB
            and type(manifest.get("original_native")) is dict
            and set(manifest["original_native"]) == set(NATIVE)
            and all(type(manifest["original_native"].get(key)) is type(expected)
                    and manifest["original_native"][key] == expected for key, expected in NATIVE.items()),
            "Exact preserved failed native allocation required")
    for key, expected in NATIVE.items():
        require(type(native.get(key)) is type(expected) and native[key] == expected,
                "Fresh native failure identity differs: " + key)
    require("gpu" not in str(native.get("alloc_tres", "")).lower(),
            "Original CPU-only native allocation required")
    require(manifest.get("helper_sha256") == ORIGINAL_HELPER_SHA256
            and manifest.get("primary_source_sha256") == PRIMARY_SHA256
            and manifest.get("framework_source_sha256") == FRAMEWORK_SHA256
            and manifest.get("full_config_sha256") == CONFIG_SHA256,
            "Pinned original helper and scientific source/config bindings required")
    _flags(manifest.get("flags"), "manifest")
    require(set(manifest["flags"]) == set(FLAGS), "Exact manifest false flag roster required")
    case, support, helper = (manifest.get(key) for key in ("case", "support", "helper_path"))
    for relative in (case, support, helper):
        _relative(relative)
    require(case == "maintenance/behaviour-full-cohort-prefix-v5-20261022"
            and support == "maintenance/behaviour-full-cohort-prefix-v5-support-20261022"
            and helper == "maintenance/study-af-resubmission-433-20261023/run-full-cohort-prefix-v5-20261022.py",
            "Exact original evidence namespaces required")
    context = str(PurePosixPath(helper).with_name("run-full-cohort-prefix-context-v5-20261022.py"))
    pins, artifact_pins = manifest.get("pinned_files"), manifest.get("artifact_pins")
    require(type(pins) is dict and 16 <= len(pins) <= 256 and type(artifact_pins) is dict,
            "Bounded strict source/JSON and artifact pin mappings required")
    required = {helper, context, OLD_PLAN, OLD_LEDGER, OLD_SCRIPT,
                case + "/plan.json", case + "/worker-result.json", case + "/progress.json",
                case + "/started.json", case + "/receipt.json",
                support + "/receipt.json", support + "/input-bindings.json"}
    required.update(case + "/" + stage + ".json" for stage in STAGES)
    require(required <= pins.keys() and pins.get(helper) == ORIGINAL_HELPER_SHA256
            and pins.get(context) == CONTEXT_SHA256,
            "All required original source and metadata pins required")
    absolute = lambda relative: str(logical_root.joinpath(*_relative(relative)))
    artifact_map = {absolute(case + "/" + stage + ".pt"): case + "/" + stage + ".pt"
                    for stage in STAGES}
    require(set(artifact_pins) == set(artifact_map), "Exact four original artifact pins required")
    combined = dict(pins)
    for path, relative in artifact_map.items():
        require(relative not in combined, "Artifact/source pin alias refused")
        combined[relative] = artifact_pins[path]
    snapshots, raw = {}, {}
    for relative, expected in sorted(combined.items()):
        snapshots[relative], content = _stable_file(
            physical_root, relative, expected, capture=relative in pins)
        if relative in pins:
            raw[relative] = content
    documents = {relative: _json(content) for relative, content in raw.items()
                 if relative.endswith(".json")}
    worker, progress, receipt, support_receipt, plan, started, bindings = (
        documents[relative] for relative in (
            case + "/worker-result.json", case + "/progress.json", case + "/receipt.json",
            support + "/receipt.json", case + "/plan.json", case + "/started.json",
            support + "/input-bindings.json"))
    require(receipt == support_receipt, "Case/support receipts must retain identical failure evidence")
    for label, document in (("worker", worker), ("progress", progress), ("receipt", receipt),
                            ("plan", plan), ("started", started), ("bindings", bindings)):
        _flags(document, label)
    for label, document in (("worker", worker), ("progress", progress), ("plan", plan), ("started", started)):
        _sources(document, label)
    _sources(receipt, "receipt", source_key="source_sha256")
    require(receipt.get("native_job_id") == ORIGINAL_JOB and started.get("native_job_id") == ORIGINAL_JOB
            and receipt.get("case") == absolute(case), "Exact original worker allocation/case required")
    require(receipt.get("status") == "worker_failed"
            and receipt.get("supervisor_error") == {
                "type": "ProcessLookupError", "error": "Child no longer has resident process memory"},
            "Exact recorded supervisor exit-race diagnostic required")
    termination = receipt.get("termination", {})
    require(type(receipt.get("worker_returncode")) is int and receipt["worker_returncode"] == 0
            and termination.get("reaped") is True and termination.get("termination_confirmed") is True
            and type(termination.get("returncode")) is int and termination["returncode"] == 0
            and termination.get("errors") == []
            and termination.get("additional_teardown_replay_permitted") is False
            and receipt.get("requires_native_terminal_proof_before_consumption") is False,
            "Positive original child exit/reap evidence required")
    require(worker == receipt.get("worker_result") and worker.get("status") == COMPLETE
            and progress.get("status") == COMPLETE, "Standalone complete worker must equal original embedded result")
    require(progress.get("completed_stages") == worker.get("completed_stages")
            and progress.get("last_event") == {"stage": "worker_finished"},
            "Completed durable progress must agree with worker result")
    require(started.get("helper_sha256") == ORIGINAL_HELPER_SHA256
            and bindings.get("helper_sha256") == ORIGINAL_HELPER_SHA256
            and plan.get("private_helper_sha256") == ORIGINAL_HELPER_SHA256,
            "Original helper bindings required throughout")
    context_binding = bindings.get("context_helper", {})
    require(context_binding.get("path") == absolute(context)
            and context_binding.get("sha256") == CONTEXT_SHA256
            and context_binding.get("operational_rebinding") == {
                "CASE": absolute(case), "SUPPORT": absolute(support)},
            "Exact original context binding required")
    allocation = receipt.get("allocation", {})
    require(bindings.get("allocation") == allocation, "Input/receipt allocation binding differs")
    require(allocation.get("plan_path") == absolute(OLD_PLAN)
            and allocation.get("ledger_path") == absolute(OLD_LEDGER)
            and allocation.get("ledger_sha256") == pins[OLD_LEDGER]
            and allocation.get("script_sha256") == pins[OLD_SCRIPT],
            "Pinned original plan, ledger and script required")
    old_plan, old_ledger = documents[OLD_PLAN], documents[OLD_LEDGER]
    plan_canonical_sha = canonical_hash(old_plan)
    require(allocation.get("plan_sha256") == plan_canonical_sha
            and old_ledger.get("plan_sha256") == plan_canonical_sha
            and old_plan.get("config_sha256") == old_ledger.get("config_sha256") == CONFIG_SHA256
            and old_plan.get("release_identity", {}).get("sha256") == old_ledger.get("source_sha256") == FRAMEWORK_SHA256,
            "Original source/config/canonical plan registration differs")
    tasks = old_plan.get("tasks", [])
    require(type(tasks) is list and len(tasks) == 1, "Exact original one-task registration required")
    task = tasks[0]
    registered = old_ledger.get("tasks", {}).get("recovery-433", {})
    script_canonical_sha = canonical_hash(raw[OLD_SCRIPT].decode())
    require(task.get("name") == "recovery-433" and task.get("script") == absolute(OLD_SCRIPT)
            and task.get("resources") == dict(cpus=1, memory="64G", hours=8, gpu=False, array=None)
            and registered.get("job_id") == ORIGINAL_JOB and registered.get("state") == "submitted"
            and task.get("script_sha256") == registered.get("script_sha256") == script_canonical_sha,
            "Exact original task/script/resources registration required")
    completed = worker.get("completed_stages")
    require(type(completed) is list and len(completed) == 4
            and [item.get("stage") for item in completed] == list(STAGES),
            "Exact ordered four-stage worker completion required")
    require(worker.get("plan_sha256") == progress.get("plan_sha256") == pins[case + "/plan.json"],
            "Worker/progress plan hash differs")
    for stage, item in zip(STAGES, completed):
        _flags(item, stage)
        artifact = absolute(case + "/" + stage + ".pt")
        require(item == documents[case + "/" + stage + ".json"]
                and item.get("status") == "operator_prefix_stage_complete"
                and item.get("exact_mid_operation_resume_supported") is False
                and item.get("artifacts_path") == artifact
                and item.get("artifacts_sha256") == artifact_pins[artifact]
                and item.get("plan_sha256") == pins[case + "/plan.json"],
                "Recorded stage/artifact/plan byte bindings differ: " + stage)
    validator_input = copy.deepcopy(worker)
    validator(validator_input, logical_root.joinpath(*_relative(case)))
    require(validator_input == worker, "Original validator must not mutate recorded worker evidence")
    for relative, expected in sorted(combined.items()):
        repeated, _ = _stable_file(physical_root, relative, expected)
        require(repeated == snapshots[relative], "Evidence changed across independent validation: " + relative)
    require(_root_identity(physical_root.stat()) == root_identity, "Evidence root identity changed during verification")
    certificate = dict(schema_version=1, status="technical_prefix_independently_verified",
                       root=manifest["root"], case=absolute(case), original_job_id=ORIGINAL_JOB,
                       original_native=copy.deepcopy(NATIVE), original_receipt_status="worker_failed",
                       worker_result_status=COMPLETE, completed_stages=list(STAGES),
                       expected_manifest_sha256=canonical_hash(manifest),
                       original_helper_sha256=ORIGINAL_HELPER_SHA256,
                       primary_source_sha256=PRIMARY_SHA256, framework_source_sha256=FRAMEWORK_SHA256,
                       full_config_sha256=CONFIG_SHA256, evidence_files=snapshots,
                       original_native_history_rewritten=False, canonical_state_changed=False,
                       numerical_worker_replayed=False, artifacts_deserialized=False,
                       consumer_requires_verifier_native_completed_zero=True, **FLAGS)
    canonical_bytes(certificate)
    return certificate
