"""Standalone synthetic lifecycle tests; no study imports, data or model execution."""
from collections import deque
from pathlib import Path
import ast
import hashlib
import json
import math
import os
import signal
import subprocess
import sys
import tempfile
import time
import types
import unittest


AREA = Path(__file__).resolve().parent
REPAIRED = AREA / "run-full-cohort-continuation-v1-20261024.py"
ORIGINAL_SHA256 = "459e0aa542eabd824dfe6f9deacf5e0e702b0108e5816760024c155823e1bb2f"
ORIGINAL = AREA/'original-one-record-helper.py'
COMPLETE = "complete_full_training_fold_first_order_comparison"


def load_lifecycle(path):
    """Compile only generic lifecycle functions and their immutable constants."""
    constants = {"ROOT", "PRIMARY", "SOURCE", "PRIMARY_SHA", "CONFIG",
                 "MEMORY_LIMIT", "FALSE_FLAGS", "STAGES"}
    functions = {"require", "digest", "exclusive", "stop",
                 "validate_worker_result", "supervise"}
    parsed = ast.parse(path.read_bytes(), filename=str(path))
    selected = []
    for node in parsed.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id in constants:
                selected.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in functions:
            selected.append(node)
    def unavailable_rss(_pid=None):
        raise AssertionError("Every synthetic supervisor must inject its RSS reader")
    synthetic_signals = types.SimpleNamespace(SIGTERM=getattr(signal, "SIGTERM", 15),
                                             SIGKILL=getattr(signal, "SIGKILL", 9))
    scope = dict(Path=Path, os=os, math=math, signal=synthetic_signals,
                 subprocess=subprocess, time=time, json=json, hashlib=hashlib,
                 rss_bytes=unavailable_rss)
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), scope)
    return types.SimpleNamespace(**scope)


class FakeProcess:
    pid = 900000001

    def __init__(self, waits=(), polls=(), initial=None):
        self.returncode = initial
        self.answers = deque(waits)
        self.poll_answers = deque(polls)
        self.waits = []
        self.events = []

    def poll(self):
        answer = self.poll_answers.popleft() if self.poll_answers else self.returncode
        if type(answer) is int:
            self.returncode = answer
        self.events.append(("poll", answer))
        return answer

    def wait(self, timeout):
        self.waits.append(timeout)
        self.events.append(("wait", timeout))
        answer = self.answers.popleft() if self.answers else "timeout"
        if answer == "timeout":
            raise subprocess.TimeoutExpired(["synthetic-child"], timeout)
        if isinstance(answer, Exception):
            raise answer
        self.returncode = answer
        return answer


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        workspace = Path(os.environ.get("HHW_TEST_WORKSPACE", str(AREA / "test-work")))
        workspace.mkdir(exist_ok=True)
        workspace = workspace.resolve(strict=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="case-", dir=workspace)
        if not Path(self.temporary.name).resolve().is_relative_to(workspace):
            raise AssertionError("Test cleanup must remain in the designated test workspace")
        self.addCleanup(self.temporary.cleanup)
        self.case = Path(self.temporary.name)
        self.module = load_lifecycle(REPAIRED)
        self.signals = []
        self.stops = []

    def worker(self, *, status=COMPLETE, completed=True):
        m = self.module
        (self.case / "plan.json").write_text('{"synthetic":true}\n')
        stages = []
        if completed:
            for stage in m.STAGES:
                artifact = self.case / (stage + ".pt")
                artifact.write_bytes(("synthetic inert bytes: " + stage).encode())
                item = dict(stage=stage, status="full_fold_technical_stage_complete",
                            training_clocks=1107, training_bins=7828183, global_observed_bin_normalizer=2666463,
                            exact_mid_operation_resume_supported=False,
                            artifacts_path=str(artifact), artifacts_sha256=m.digest(artifact),
                            plan_sha256=m.digest(self.case / "plan.json"), **m.FALSE_FLAGS)
                (self.case / (stage + ".json")).write_text(json.dumps(item))
                stages.append(item)
        result = dict(status=status, completed_stages=stages,
                      primary_source_sha256=m.PRIMARY_SHA,
                      framework_source_sha256=m.SOURCE, full_config_sha256=m.CONFIG,
                      **m.FALSE_FLAGS)
        (self.case / "worker-result.json").write_text(json.dumps(result))
        return result

    def stop(self, process):
        self.stops.append(process)
        return self.module.stop(process, group_signal=lambda pid, value:
                                self.signals.append((pid, value)))

    def supervise(self, process, *, error=None, rss=None, clock=None, deadline=100):
        def missing(_pid):
            process.events.append(("rss", type(error).__name__))
            raise error
        return self.module.supervise(
            ["synthetic-child"], {}, deadline, self.case,
            popen=lambda *args, **kwargs: process,
            clock=clock or (lambda: 0),
            sleeper=lambda _seconds: self.fail("No child work or real sleep expected"),
            rss=rss or missing, stopper=self.stop)

    def assert_false_flags(self, result):
        for name in self.module.FALSE_FLAGS:
            self.assertIs(result[name], False, name)

    def test_original_source_hash_and_only_operational_functions_change(self):
        raw = ORIGINAL.read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), ORIGINAL_SHA256)
        before, after = ast.parse(raw), ast.parse(REPAIRED.read_bytes())
        permitted = {'prepare', '_worker', 'validate_worker_result', 'adjoint_runtime_seconds', 'main'}
        later = {n.name:n for n in after.body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
        lines_before, lines_after = raw.splitlines(keepends=True), REPAIRED.read_bytes().splitlines(keepends=True)
        changed = set()
        for node in before.body:
            if isinstance(node,(ast.FunctionDef,ast.ClassDef)):
                other = later[node.name]
                old = b''.join(lines_before[node.lineno-1:node.end_lineno])
                new = b''.join(lines_after[other.lineno-1:other.end_lineno])
                if old != new:
                    changed.add(node.name)
        self.assertEqual(changed,permitted)
        first = {n.targets[0].id:ast.dump(n.value,include_attributes=False) for n in before.body
                 if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name)}
        second = {n.targets[0].id:ast.dump(n.value,include_attributes=False) for n in after.body
                  if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name)}
        for name in ['ROOT','FRAMEWORK','PRIMARY','SOURCE','PRIMARY_SHA','CONFIG',
                     'ADJOINT_SHA','PROFILE_SHA','FALSE_FLAGS','MEMORY_LIMIT']:
            self.assertEqual(first[name],second[name],name)
        self.assertEqual(second['CONTEXT_SHA'],ast.dump(ast.Constant('4f051dc31d7cd5220f1e4990030e5eae18e02ba86d967876a6d0eb887252c60f'),include_attributes=False))

    def test_delayed_reap_after_process_lookup_completes_without_signal(self):
        self.worker()
        process = FakeProcess(waits=[0])
        result = self.supervise(process, error=ProcessLookupError("exiting"))
        self.assertEqual(result["status"], COMPLETE)
        self.assertEqual(result["worker_returncode"], 0)
        self.assertIsNone(result["supervisor_error"])
        self.assertEqual(process.waits, [1.])
        self.assertEqual(len(self.stops), 1)
        self.assertEqual(self.signals, [])
        self.assert_false_flags(result)

    def test_delayed_reap_after_file_disappears_completes_without_signal(self):
        self.worker()
        process = FakeProcess(waits=[0])
        result = self.supervise(process, error=FileNotFoundError("status disappeared"))
        self.assertEqual(result["status"], COMPLETE)
        self.assertEqual(self.signals, [])

    def test_immediate_second_poll_terminal_avoids_wait_and_signal(self):
        self.worker()
        process = FakeProcess(polls=[None, 0])
        result = self.supervise(process, error=ProcessLookupError("exiting"))
        self.assertEqual(result["status"], COMPLETE)
        self.assertEqual(process.waits, [])
        self.assertEqual(self.signals, [])

    def test_already_exited_child_does_not_read_rss(self):
        self.worker()
        process = FakeProcess(initial=0)
        result = self.supervise(process, rss=lambda _pid: self.fail("Exited child RSS read"))
        self.assertEqual(result["status"], COMPLETE)
        self.assertEqual(process.waits, [])

    def test_nonzero_natural_exit_remains_failed_with_complete_result(self):
        self.worker()
        result = self.supervise(FakeProcess(waits=[7]), error=ProcessLookupError("exiting"))
        self.assertEqual(result["status"], "worker_failed")
        self.assertEqual(result["worker_returncode"], 7)
        self.assertEqual(self.signals, [])
        self.assert_false_flags(result)

    def test_missing_result_remains_failed_after_zero_exit(self):
        result = self.supervise(FakeProcess(waits=[0]), error=ProcessLookupError("exiting"))
        self.assertEqual(result["status"], "worker_failed")
        self.assertIsNone(result["worker_result"])

    def test_timeout_preserves_missing_rss_error_even_if_teardown_reaps_zero(self):
        self.worker()
        process = FakeProcess(waits=["timeout", 0])
        result = self.supervise(process, error=ProcessLookupError("original RSS error"))
        self.assertEqual(result["status"], "worker_failed")
        self.assertEqual(result["supervisor_error"],
                         {"type": "ProcessLookupError", "error": "original RSS error"})
        self.assertEqual(process.waits, [1., 10.])
        self.assertEqual(len(self.stops), 1)
        self.assertEqual(len(self.signals), 1)

    def test_wait_error_is_not_promoted_by_valid_result(self):
        self.worker()
        result = self.supervise(FakeProcess(waits=[PermissionError("wait denied"), 0]),
                                error=ProcessLookupError("exiting"))
        self.assertEqual(result["status"], "worker_failed")
        self.assertEqual(result["supervisor_error"]["type"], "PermissionError")

    def test_wait_must_return_positive_integer_reap_evidence(self):
        self.worker()
        result = self.supervise(FakeProcess(waits=[None, 0]), error=ProcessLookupError("exiting"))
        self.assertEqual(result["status"], "worker_failed")
        self.assertEqual(result["supervisor_error"]["type"], "ValueError")

    def test_timeout_with_unreaped_child_preserves_unknown_termination(self):
        self.worker(status="bounded_deadline_reached", completed=False)
        (self.case / "worker-result.json").unlink()
        progress = dict(status="whole_cohort_prefix_running", completed_stages=[],
                        primary_source_sha256=self.module.PRIMARY_SHA,
                        framework_source_sha256=self.module.SOURCE,
                        full_config_sha256=self.module.CONFIG, **self.module.FALSE_FLAGS)
        (self.case / "progress.json").write_text(json.dumps(progress))
        process = FakeProcess(waits=["timeout", "timeout", "timeout"])
        result = self.supervise(process, error=ProcessLookupError("exiting"))
        self.assertEqual(result["status"], "worker_termination_unconfirmed")
        self.assertIsNone(result["worker_returncode"])
        self.assertIs(result["requires_native_terminal_proof_before_consumption"], True)
        self.assertEqual(len(self.stops), 1)
        self.assertEqual(len(self.signals), 2)
        self.assertEqual(process.waits, [1., 10., 20.])
        self.assertEqual(result["preserved_partial_progress"]["value"], progress)
        self.assert_false_flags(result)

    def test_unexpected_rss_error_stays_failed_even_with_complete_result(self):
        self.worker()
        result = self.supervise(FakeProcess(waits=[0]), error=OSError("inaccessible RSS"))
        self.assertEqual(result["status"], "worker_failed")
        self.assertEqual(result["supervisor_error"]["type"], "OSError")

    def test_existing_memory_bound_is_preserved(self):
        self.worker()
        result = self.supervise(FakeProcess(waits=[0]), rss=lambda _pid: self.module.MEMORY_LIMIT)
        self.assertEqual(result["status"], "memory_bound_not_compared")
        self.assertEqual(len(self.signals), 1)
        self.assert_false_flags(result)

    def test_existing_deadline_bound_is_preserved(self):
        self.worker()
        result = self.supervise(FakeProcess(waits=[0]), deadline=0,
                                rss=lambda _pid: self.fail("Deadline must precede RSS"))
        self.assertEqual(result["status"], "bounded_deadline_reached")
        self.assertEqual(len(self.signals), 1)

    def test_natural_wait_is_capped_by_remaining_deadline(self):
        self.worker()
        process = FakeProcess(waits=[0])
        result = self.supervise(process, error=ProcessLookupError("exiting"),
                                clock=lambda: 99.75)
        self.assertEqual(process.waits, [.25])
        self.assertEqual(result["status"], COMPLETE)

    def test_no_natural_wait_if_deadline_elapsed_after_rss_disappears(self):
        self.worker()
        clocks = iter([0, 100])
        process = FakeProcess(waits=[0])
        result = self.supervise(process, error=ProcessLookupError("exiting"), clock=lambda: next(clocks))
        self.assertEqual(process.waits, [10.])
        self.assertEqual(result["status"], "bounded_deadline_reached")

    def test_natural_wait_timeout_at_deadline_preserves_deadline_bound(self):
        self.worker()
        clocks = iter([99.75, 99.75, 100])
        process = FakeProcess(waits=["timeout", 0])
        result = self.supervise(process, error=ProcessLookupError("exiting"), clock=lambda: next(clocks))
        self.assertEqual(process.waits, [.25, 10.])
        self.assertEqual(result["status"], "bounded_deadline_reached")
        self.assertIsNone(result["supervisor_error"])

    def test_malformed_result_is_rejected_after_natural_exit(self):
        (self.case / "worker-result.json").write_text("{malformed")
        with self.assertRaises(json.JSONDecodeError):
            self.supervise(FakeProcess(waits=[0]), error=ProcessLookupError("exiting"))

    def test_incomplete_stage_roster_cannot_claim_completion(self):
        self.worker(completed=False)
        with self.assertRaises(ValueError):
            self.supervise(FakeProcess(waits=[0]), error=ProcessLookupError("exiting"))

    def test_tampered_stage_artifact_is_rejected(self):
        self.worker()
        (self.case / (self.module.STAGES[0] + ".pt")).write_bytes(b"tampered inert bytes")
        with self.assertRaises(ValueError):
            self.supervise(FakeProcess(waits=[0]), error=ProcessLookupError("exiting"))

    def test_reordered_stage_roster_is_rejected(self):
        value = self.worker()
        value["completed_stages"].reverse()
        (self.case / "worker-result.json").write_text(json.dumps(value))
        with self.assertRaises(ValueError):
            self.supervise(FakeProcess(waits=[0]), error=ProcessLookupError("exiting"))

    def test_changed_source_binding_is_rejected(self):
        value = self.worker()
        value["framework_source_sha256"] = "f" * 64
        (self.case / "worker-result.json").write_text(json.dumps(value))
        with self.assertRaises(ValueError):
            self.supervise(FakeProcess(waits=[0]), error=ProcessLookupError("exiting"))

    def test_scientific_or_admission_flag_cannot_be_promoted(self):
        value = self.worker()
        value["scientific_result"] = True
        (self.case / "worker-result.json").write_text(json.dumps(value))
        with self.assertRaises(ValueError):
            self.supervise(FakeProcess(waits=[0]), error=ProcessLookupError("exiting"))


if __name__ == "__main__":
    result_directory = os.environ.get("HHW_TEST_RESULT_DIR")
    if result_directory:
        import io
        output = io.StringIO()
        result = unittest.TextTestRunner(stream=output, verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(LifecycleTests))
        destination = Path(result_directory).resolve(strict=True)
        if not destination.is_dir():
            raise ValueError("Existing designated result directory required")
        summary = dict(tests=result.testsRun, failures=len(result.failures),
                       errors=len(result.errors), skipped=len(result.skipped),
                       successful=result.wasSuccessful(),
                       tests_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                       derivative_sha256=hashlib.sha256(REPAIRED.read_bytes()).hexdigest(),
                       original_sha256=ORIGINAL_SHA256)
        with (destination / "test-output.txt").open("x") as stream:
            stream.write(output.getvalue())
        with (destination / "test-result.json").open("x") as stream:
            json.dump(summary, stream, sort_keys=True, indent=2, allow_nan=False)
            stream.write("\n")
        sys.stdout.write(output.getvalue())
        raise SystemExit(0 if result.wasSuccessful() else 1)
    unittest.main(verbosity=2)
