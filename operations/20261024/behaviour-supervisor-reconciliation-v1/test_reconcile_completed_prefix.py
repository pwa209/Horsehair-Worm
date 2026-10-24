"""Standalone stdlib reconciliation tests using inert files and no study execution."""
from pathlib import Path
from unittest import mock
import ast
import copy
import hashlib
import importlib.util
import io
import json
import os
import stat
import sys
import tempfile
import types
import unittest


AREA = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("prefix_reconciliation", AREA / "reconcile_completed_prefix.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
ORIGINAL = Path(os.environ.get("HHW_LIFECYCLE_ORIGINAL", str(
    AREA.parents[2] / "work/repair-20261024-509-evidence/maintenance/"
    "study-af-resubmission-433-20261023/run-full-cohort-prefix-v5-20261022.py")))
CASE = "maintenance/behaviour-full-cohort-prefix-v5-20261022"
SUPPORT = "maintenance/behaviour-full-cohort-prefix-v5-support-20261022"
HELPER = "maintenance/study-af-resubmission-433-20261023/run-full-cohort-prefix-v5-20261022.py"
CONTEXT = str(Path(HELPER).with_name("run-full-cohort-prefix-context-v5-20261022.py")).replace("\\", "/")


def original_validator():
    """Load only original generic hashing/validation definitions, never main/model code."""
    raw = ORIGINAL.read_bytes()
    if hashlib.sha256(raw).hexdigest() != m.ORIGINAL_HELPER_SHA256:
        raise ValueError("Exact captured original helper fixture required")
    constants = {"SOURCE", "PRIMARY_SHA", "CONFIG", "FALSE_FLAGS", "STAGES"}
    functions = {"require", "digest", "validate_worker_result"}
    body = []
    for node in ast.parse(raw).body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id in constants:
                body.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in functions:
            body.append(node)
    scope = dict(Path=Path, hashlib=hashlib, json=json)
    exec(compile(ast.Module(body=body, type_ignores=[]), str(ORIGINAL), "exec"), scope)
    return scope["validate_worker_result"]


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        workspace = Path(os.environ.get("HHW_TEST_WORKSPACE", str(
            AREA.parents[2] / "work/rec-1024-tests")))
        workspace.mkdir(exist_ok=True)
        workspace = workspace.resolve(strict=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="reconcile-case-", dir=workspace)
        self.root = Path(self.temporary.name).resolve()
        if not self.root.is_relative_to(workspace):
            raise AssertionError("Test cleanup must remain within the designated workspace")
        self.addCleanup(self.temporary.cleanup)
        self.validator = original_validator()
        self.native = copy.deepcopy(m.NATIVE)
        self.build_fixture()

    def path(self, relative):
        return self.root.joinpath(*relative.split("/"))

    def write(self, relative, value):
        path = self.path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value if isinstance(value, bytes) else
                         (json.dumps(value, sort_keys=True, allow_nan=False) + "\n").encode())

    def read(self, relative):
        return json.loads(self.path(relative).read_bytes())

    def pin(self, relative):
        self.manifest["pinned_files"][relative] = hashlib.sha256(self.path(relative).read_bytes()).hexdigest()

    def change(self, relative, callback):
        value = self.read(relative)
        callback(value)
        self.write(relative, value)
        self.pin(relative)

    def sources(self):
        return dict(primary_source_sha256=m.PRIMARY_SHA256,
                    framework_source_sha256=m.FRAMEWORK_SHA256,
                    full_config_sha256=m.CONFIG_SHA256)

    def build_fixture(self):
        self.write(HELPER, ORIGINAL.read_bytes())
        self.write(CONTEXT, ORIGINAL.with_name("run-full-cohort-prefix-context-v5-20261022.py").read_bytes())
        script = b"#!/bin/bash\n# Inert synthetic registration; never executed.\n"
        self.write(m.OLD_SCRIPT, script)
        old_plan = dict(config_sha256=m.CONFIG_SHA256,
                        release_identity={"sha256": m.FRAMEWORK_SHA256},
                        tasks=[dict(name="recovery-433", script=str(self.path(m.OLD_SCRIPT)),
                                    script_sha256=m.canonical_hash(script.decode()),
                                    resources=dict(cpus=1, memory="64G", hours=8, gpu=False, array=None))])
        self.write(m.OLD_PLAN, old_plan)
        ledger = dict(config_sha256=m.CONFIG_SHA256, source_sha256=m.FRAMEWORK_SHA256,
                      plan_sha256=m.canonical_hash(old_plan),
                      tasks={"recovery-433": dict(job_id=m.ORIGINAL_JOB, state="submitted",
                                                  script_sha256=m.canonical_hash(script.decode()))})
        self.write(m.OLD_LEDGER, ledger)
        allocation = dict(plan_path=str(self.path(m.OLD_PLAN)), ledger_path=str(self.path(m.OLD_LEDGER)),
                          plan_sha256=m.canonical_hash(old_plan),
                          ledger_sha256=hashlib.sha256(self.path(m.OLD_LEDGER).read_bytes()).hexdigest(),
                          script_sha256=hashlib.sha256(script).hexdigest())
        plan = dict(private_helper_sha256=m.ORIGINAL_HELPER_SHA256, **self.sources(), **m.FLAGS)
        self.write(CASE + "/plan.json", plan)
        plan_sha = hashlib.sha256(self.path(CASE + "/plan.json").read_bytes()).hexdigest()
        stages = []
        artifacts = {}
        for stage in m.STAGES:
            relative = CASE + "/" + stage + ".pt"
            self.write(relative, b"Not a tensor, pickle or executable: " + stage.encode())
            digest = hashlib.sha256(self.path(relative).read_bytes()).hexdigest()
            item = dict(stage=stage, status="operator_prefix_stage_complete",
                        artifacts_path=str(self.path(relative)), artifacts_sha256=digest,
                        plan_sha256=plan_sha, exact_mid_operation_resume_supported=False, **m.FLAGS)
            self.write(CASE + "/" + stage + ".json", item)
            stages.append(item)
            artifacts[str(self.path(relative))] = digest
        worker = dict(status=m.COMPLETE, completed_stages=stages, plan_sha256=plan_sha,
                      last_event={"stage": m.STAGES[-1], "state": "completed"}, **self.sources(), **m.FLAGS)
        self.write(CASE + "/worker-result.json", worker)
        self.write(CASE + "/progress.json", dict(worker, last_event={"stage": "worker_finished"}))
        self.write(CASE + "/started.json", dict(native_job_id=m.ORIGINAL_JOB,
                                                helper_sha256=m.ORIGINAL_HELPER_SHA256,
                                                **self.sources(), **m.FLAGS))
        receipt = dict(status="worker_failed", native_job_id=m.ORIGINAL_JOB, case=str(self.path(CASE)),
                       supervisor_error=dict(type="ProcessLookupError", error="Child no longer has resident process memory"),
                       worker_returncode=0, worker_result=worker, allocation=allocation,
                       termination=dict(reaped=True, termination_confirmed=True, returncode=0,
                                        errors=[], additional_teardown_replay_permitted=False),
                       requires_native_terminal_proof_before_consumption=False,
                       primary_source_sha256=m.PRIMARY_SHA256, source_sha256=m.FRAMEWORK_SHA256,
                       full_config_sha256=m.CONFIG_SHA256, **m.FLAGS)
        self.write(CASE + "/receipt.json", receipt)
        self.write(SUPPORT + "/receipt.json", receipt)
        self.write(SUPPORT + "/input-bindings.json", dict(helper_sha256=m.ORIGINAL_HELPER_SHA256,
                   context_helper=dict(path=str(self.path(CONTEXT)), sha256=m.CONTEXT_SHA256,
                                       operational_rebinding={"CASE": str(self.path(CASE)), "SUPPORT": str(self.path(SUPPORT))}),
                   allocation=allocation, **m.FLAGS))
        pins = {path.relative_to(self.root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in self.root.rglob("*") if path.is_file() and path.suffix != ".pt"}
        self.manifest = dict(schema_version=1, root=str(self.root), original_job_id=m.ORIGINAL_JOB,
                             original_native=copy.deepcopy(m.NATIVE), helper_path=HELPER,
                             helper_sha256=m.ORIGINAL_HELPER_SHA256, case=CASE, support=SUPPORT,
                             primary_source_sha256=m.PRIMARY_SHA256,
                             framework_source_sha256=m.FRAMEWORK_SHA256, full_config_sha256=m.CONFIG_SHA256,
                             flags=copy.deepcopy(m.FLAGS), pinned_files=pins, artifact_pins=artifacts)

    def verify(self, validator=None):
        return m.verify(self.manifest, root=self.root, native=self.native,
                        validator=validator or self.validator)

    def change_receipt(self, callback):
        for relative in (CASE + "/receipt.json", SUPPORT + "/receipt.json"):
            self.change(relative, callback)

    def test_success_is_read_only_preserves_failed_native_and_false_gates(self):
        before = {p.relative_to(self.root).as_posix(): p.read_bytes()
                  for p in self.root.rglob("*") if p.is_file()}
        certificate = self.verify()
        self.assertEqual(certificate["status"], "technical_prefix_independently_verified")
        self.assertEqual(certificate["original_native"], m.NATIVE)
        self.assertEqual(certificate["expected_manifest_sha256"], m.canonical_hash(self.manifest))
        self.assertEqual(certificate["completed_stages"], list(m.STAGES))
        for key in m.FLAGS:
            self.assertIs(certificate[key], False)
        for key in ("canonical_state_changed", "numerical_worker_replayed", "artifacts_deserialized",
                    "original_native_history_rewritten"):
            self.assertIs(certificate[key], False)
        self.assertEqual(before, {p.relative_to(self.root).as_posix(): p.read_bytes()
                                  for p in self.root.rglob("*") if p.is_file()})
        self.assertEqual(len(certificate["evidence_files"]), len(before))

    def test_exact_original_validator_is_called_once_with_unmodified_worker(self):
        calls = []
        def validator(value, case):
            calls.append((copy.deepcopy(value), case))
            return self.validator(value, case)
        self.verify(validator)
        self.assertEqual(calls, [(self.read(CASE + "/worker-result.json"), self.path(CASE))])

    def test_each_native_identity_or_completion_mismatch_is_refused(self):
        for key, replacement in dict(state="COMPLETED", exit_code="0:0", owner="foreign",
                                     account="other", cpus=2, elapsed_seconds=18501,
                                     start="2026-10-23T11:07:40", end="2026-10-23T16:16:00",
                                     job_id="99999").items():
            with self.subTest(key=key):
                native = dict(self.native, **{key: replacement})
                with self.assertRaises(ValueError):
                    m.verify(self.manifest, root=self.root, native=native, validator=self.validator)

    def test_native_boolean_cpu_is_not_an_integer_allocation(self):
        self.native["cpus"] = True
        with self.assertRaises(ValueError):
            self.verify()

    def test_manifest_numeric_zero_cannot_replace_false_flag(self):
        self.manifest["flags"]["scientific_result"] = 0
        with self.assertRaises(ValueError):
            self.verify()

    def test_missing_required_pin_is_refused(self):
        del self.manifest["pinned_files"][HELPER]
        with self.assertRaises(ValueError):
            self.verify()

    def test_changed_helper_pin_is_refused(self):
        self.manifest["helper_sha256"] = "f" * 64
        with self.assertRaises(ValueError):
            self.verify()

    def test_source_bytes_differing_from_pin_are_refused(self):
        self.path(HELPER).write_bytes(b"unreviewed helper")
        with self.assertRaises(ValueError):
            self.verify()

    def test_missing_artifact_pin_is_refused(self):
        self.manifest["artifact_pins"].pop(next(iter(self.manifest["artifact_pins"])))
        with self.assertRaises(ValueError):
            self.verify()

    def test_foreign_artifact_path_is_refused(self):
        value = self.manifest["artifact_pins"].pop(next(iter(self.manifest["artifact_pins"])))
        self.manifest["artifact_pins"][str(self.root.parent / "foreign.pt")] = value
        with self.assertRaises(ValueError):
            self.verify()

    def test_path_traversal_pin_is_refused(self):
        self.manifest["pinned_files"]["../outside"] = "f" * 64
        with self.assertRaises(ValueError):
            self.verify()

    def test_scientific_worker_flag_is_refused_even_when_repinned(self):
        self.change(CASE + "/worker-result.json", lambda d: d.update(scientific_result=True))
        with self.assertRaises(ValueError):
            self.verify()

    def test_different_standalone_and_embedded_worker_is_refused(self):
        self.change_receipt(lambda d: d["worker_result"].update(extra="changed"))
        with self.assertRaises(ValueError):
            self.verify()

    def test_other_supervisor_error_is_not_reconciled(self):
        self.change_receipt(lambda d: d["supervisor_error"].update(type="PermissionError"))
        with self.assertRaises(ValueError):
            self.verify()

    def test_nonzero_or_boolean_worker_exit_is_refused(self):
        self.change_receipt(lambda d: d.update(worker_returncode=True))
        with self.assertRaises(ValueError):
            self.verify()

    def test_unconfirmed_termination_is_refused(self):
        self.change_receipt(lambda d: d["termination"].update(termination_confirmed=False))
        with self.assertRaises(ValueError):
            self.verify()

    def test_partial_worker_is_not_promoted(self):
        self.change(CASE + "/worker-result.json", lambda d: d.update(status="bounded_deadline_reached"))
        with self.assertRaises(ValueError):
            self.verify()

    def test_running_progress_is_not_complete(self):
        self.change(CASE + "/progress.json", lambda d: d.update(status="whole_cohort_prefix_running"))
        with self.assertRaises(ValueError):
            self.verify()

    def test_reordered_stages_are_refused(self):
        self.change(CASE + "/worker-result.json", lambda d: d["completed_stages"].reverse())
        with self.assertRaises(ValueError):
            self.verify()

    def test_stage_json_must_equal_recorded_worker_stage(self):
        self.change(CASE + "/" + m.STAGES[0] + ".json", lambda d: d.update(extra="changed"))
        with self.assertRaises(ValueError):
            self.verify()

    def test_changed_artifact_bytes_are_refused_without_deserialization(self):
        self.path(CASE + "/" + m.STAGES[0] + ".pt").write_bytes(b"different inert bytes")
        with self.assertRaises(ValueError):
            self.verify()

    def test_worker_plan_hash_mismatch_is_refused(self):
        self.change(CASE + "/worker-result.json", lambda d: d.update(plan_sha256="f" * 64))
        with self.assertRaises(ValueError):
            self.verify()

    def test_private_helper_binding_mismatch_is_refused(self):
        self.change(CASE + "/plan.json", lambda d: d.update(private_helper_sha256="f" * 64))
        with self.assertRaises(ValueError):
            self.verify()

    def test_context_binding_mismatch_is_refused(self):
        self.change(SUPPORT + "/input-bindings.json", lambda d: d["context_helper"].update(sha256="f" * 64))
        with self.assertRaises(ValueError):
            self.verify()

    def test_plan_registration_mismatch_is_refused(self):
        self.change(m.OLD_LEDGER, lambda d: d.update(plan_sha256="f" * 64))
        with self.assertRaises(ValueError):
            self.verify()

    def test_case_and_support_receipts_must_match(self):
        self.change(SUPPORT + "/receipt.json", lambda d: d.update(extra="changed"))
        with self.assertRaises(ValueError):
            self.verify()

    def test_duplicate_json_keys_are_refused_even_with_matching_pin(self):
        relative = CASE + "/progress.json"
        self.write(relative, b'{"status":"first","status":"second"}')
        self.pin(relative)
        with self.assertRaisesRegex(ValueError, "Duplicate JSON"):
            self.verify()

    def test_nonfinite_json_is_refused_even_with_matching_pin(self):
        relative = CASE + "/progress.json"
        self.write(relative, b'{"not_finite":NaN}')
        self.pin(relative)
        with self.assertRaisesRegex(ValueError, "Nonfinite JSON"):
            self.verify()

    def test_symlink_leaf_is_refused_before_open(self):
        target = self.path(CASE + "/" + m.STAGES[0] + ".pt")
        original = Path.lstat
        def lstat(path, *args, **kwargs):
            value = original(path, *args, **kwargs)
            if path == target:
                return types.SimpleNamespace(st_mode=stat.S_IFLNK | 0o777, st_uid=value.st_uid)
            return value
        with mock.patch.object(Path, "lstat", lstat):
            with self.assertRaisesRegex(ValueError, "Symlink"):
                self.verify()

    def test_foreign_file_owner_is_refused(self):
        target = self.path(CASE + "/" + m.STAGES[0] + ".pt")
        original = Path.lstat
        def lstat(path, *args, **kwargs):
            value = original(path, *args, **kwargs)
            if path == target:
                return types.SimpleNamespace(st_mode=value.st_mode, st_uid=value.st_uid + 1)
            return value
        with mock.patch.object(Path, "lstat", lstat):
            with self.assertRaisesRegex(ValueError, "ownership"):
                self.verify()

    def test_foreign_directory_owner_is_refused(self):
        target = self.path(CASE)
        original = Path.lstat
        def lstat(path, *args, **kwargs):
            value = original(path, *args, **kwargs)
            if path == target:
                return types.SimpleNamespace(st_mode=value.st_mode, st_uid=value.st_uid + 1)
            return value
        with mock.patch.object(Path, "lstat", lstat):
            with self.assertRaisesRegex(ValueError, "ownership"):
                self.verify()

    def test_file_growth_during_streamed_read_is_refused(self):
        target = self.path(CASE + "/" + m.STAGES[0] + ".pt")
        inode = target.stat().st_ino
        read = os.read
        changed = []
        def growing_read(descriptor, count):
            value = read(descriptor, count)
            if value and not changed and os.fstat(descriptor).st_ino == inode:
                with target.open("ab") as stream:
                    stream.write(b"growth during hashing")
                changed.append(True)
            return value
        with mock.patch.object(m.os, "read", growing_read):
            with self.assertRaisesRegex(ValueError, "changed"):
                self.verify()
        self.assertEqual(changed, [True])

    def test_validator_cannot_mutate_received_worker(self):
        def validator(value, case):
            self.validator(value, case)
            value["extra"] = "mutation"
        with self.assertRaisesRegex(ValueError, "must not mutate"):
            self.verify(validator)

    def test_artifact_metadata_change_across_validator_is_refused(self):
        target = self.path(CASE + "/" + m.STAGES[0] + ".pt")
        def validator(value, case):
            self.validator(value, case)
            current = target.stat()
            os.utime(target, ns=(current.st_atime_ns, current.st_mtime_ns + 2_000_000_000))
        with self.assertRaisesRegex(ValueError, "changed across"):
            self.verify(validator)

    def test_root_directory_metadata_change_without_identity_change_is_allowed(self):
        def validator(value, case):
            self.validator(value, case)
            (self.root / "unrelated-new-directory").mkdir()
        self.assertEqual(self.verify(validator)["status"], "technical_prefix_independently_verified")

    def test_invalid_original_validator_result_is_not_swallowed(self):
        def validator(_value, _case):
            raise ValueError("original validation failed")
        with self.assertRaisesRegex(ValueError, "original validation failed"):
            self.verify(validator)


if __name__ == "__main__":
    result_directory = os.environ.get("HHW_TEST_RESULT_DIR")
    if result_directory:
        output = io.StringIO()
        result = unittest.TextTestRunner(stream=output, verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(ReconciliationTests))
        destination = Path(result_directory).resolve(strict=True)
        if not destination.is_dir():
            raise ValueError("Existing designated result directory required")
        summary = dict(tests=result.testsRun, failures=len(result.failures), errors=len(result.errors),
                       skipped=len(result.skipped), successful=result.wasSuccessful(),
                       tests_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                       reconciler_sha256=hashlib.sha256((AREA / "reconcile_completed_prefix.py").read_bytes()).hexdigest())
        with (destination / "test-output.txt").open("x") as stream:
            stream.write(output.getvalue())
        with (destination / "test-result.json").open("x") as stream:
            json.dump(summary, stream, sort_keys=True, indent=2, allow_nan=False)
            stream.write("\n")
        sys.stdout.write(output.getvalue())
        raise SystemExit(0 if result.wasSuccessful() else 1)
    unittest.main(verbosity=2)
