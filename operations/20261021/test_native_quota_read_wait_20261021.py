import errno
import importlib.util
import json
from pathlib import Path
import subprocess

import pytest

from horsehair.budget import StorageHeadroomError
from horsehair.core import ReceiptLockTimeout


@pytest.fixture
def helper():
    spec = importlib.util.spec_from_file_location("private_quota_wait",
        Path(__file__).with_name("native_quota_read_wait_20261021.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Clock:
    def __init__(self):
        self.elapsed = 0
        self.wall_offset = 0
        self.sleeps = []

    def monotonic(self):
        return self.elapsed

    def time(self):
        return 1000 + self.elapsed + self.wall_offset

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.elapsed += seconds


def run(helper, tmp_path, function, clock=None, **kwargs):
    clock = clock or Clock()
    return helper.require_storage_headroom_with_wait(helper.PERSONAL_STUDY_ROOT,
        required_files=3, required_bytes=17, reserve_files=250000, reserve_bytes=2000000000000,
        evidence_dir=tmp_path / "evidence", allocation_deadline_unix=kwargs.pop("allocation_deadline_unix", 10000),
        quota_function=function, clock=clock, sleeper=clock.sleep, **kwargs)


def contention(helper, *, path=None):
    return ReceiptLockTimeout(path or helper.PERSONAL_STUDY_ROOT / "state/storage-headroom.mutex", 30,
        {"pid": 123, "host": "nc123.narval.calcul.quebec", "guard_path": "informational holder", "state": "held"})


def test_two_exact_lock_timeouts_then_returns_exact_new_quota_object(tmp_path, helper):
    clock = Clock()
    calls = []
    errors = [contention(helper), contention(helper)]
    fresh = {"ready": True, "measured_unix": 1066, "native_stdout": "fresh native quota", "self_sha256": "a" * 64}

    def function(root, **demand):
        calls.append((root, demand))
        assert (tmp_path / f"evidence/attempt-{len(calls)}-before.json").is_file()
        clock.elapsed += 30
        if len(calls) <= 2:
            raise errors[len(calls)-1]
        return fresh

    returned = run(helper, tmp_path, function, clock)
    assert returned is fresh and len(calls) == 3 and clock.sleeps == [2, 4]
    assert all(call == (helper.PERSONAL_STUDY_ROOT.resolve(), {
        "required_files": 3, "required_bytes": 17, "reserve_files": 250000, "reserve_bytes": 2000000000000}) for call in calls)
    after = json.loads((tmp_path / "evidence/attempt-1-after.json").read_text())
    assert after["exception"]["diagnostics"] == errors[0].diagnostics
    assert after["fresh_quota_call_retry"] is True
    final = json.loads((tmp_path / "evidence/result.json").read_text())
    assert final["status"] == "fresh_quota_complete" and final["attempts"] == 3
    assert final["quota_cache_used"] is False and final["admission_changed"] is False


def test_persistent_exact_contention_stops_after_five_calls_and_preserves_original(tmp_path, helper):
    clock = Clock()
    calls = []
    error = contention(helper)

    def function(*args, **kwargs):
        calls.append(1)
        clock.elapsed += 30
        raise error

    with pytest.raises(ReceiptLockTimeout) as failure:
        run(helper, tmp_path, function, clock)
    assert failure.value is error and len(calls) == 5 and clock.sleeps == [2, 4, 8, 16]
    assert len(list((tmp_path / "evidence").glob("attempt-*-before.json"))) == 5
    assert len(list((tmp_path / "evidence").glob("attempt-*-after.json"))) == 5
    assert json.loads((tmp_path / "evidence/result.json").read_text())["status"] == "quota_lock_attempts_exhausted"


@pytest.mark.parametrize("factory", [
    lambda: OSError(errno.EDQUOT, "quota exceeded"),
    lambda: OSError(errno.ENOSPC, "no space"),
    lambda: OSError(errno.EIO, "filesystem error"),
    lambda: StorageHeadroomError({"ready": False, "reasons": ["insufficient inode reserve"]}),
    lambda: ValueError("invalid quota row"),
    lambda: TimeoutError("ordinary timeout"),
    lambda: subprocess.TimeoutExpired(["lfs", "quota"], 30),
    lambda: RuntimeError("quota command failed"),
])
def test_non_contention_failures_are_never_retried(tmp_path, helper, factory):
    clock = Clock()
    calls = []
    error = factory()

    def function(*args, **kwargs):
        calls.append(1)
        raise error

    with pytest.raises(type(error)) as failure:
        run(helper, tmp_path, function, clock)
    assert failure.value is error and calls == [1] and clock.sleeps == []
    after = json.loads((tmp_path / "evidence/attempt-1-after.json").read_text())
    assert after["fresh_quota_call_retry"] is False and after["exact_guard_contention"] is False
    if isinstance(error, StorageHeadroomError):
        assert after["exception"]["storage_headroom_record"] == error.record


def test_other_receipt_guard_timeout_is_not_retried(tmp_path, helper):
    error = contention(helper, path=helper.PERSONAL_STUDY_ROOT / "state/budget.mutex")
    clock = Clock()
    with pytest.raises(ReceiptLockTimeout) as failure:
        run(helper, tmp_path, lambda *a, **k: (_ for _ in ()).throw(error), clock)
    assert failure.value is error and clock.sleeps == []
    assert json.loads((tmp_path / "evidence/attempt-1-after.json").read_text())["exact_guard_contention"] is False


def test_subclass_or_disagreeing_guard_diagnostics_is_not_retryable(tmp_path, helper):
    class OtherTimeout(ReceiptLockTimeout):
        pass
    guard = helper.PERSONAL_STUDY_ROOT / "state/storage-headroom.mutex"
    assert helper._retryable(OtherTimeout(guard, 30, None), guard.resolve()) is False
    error = contention(helper)
    error.diagnostics["guard_path"] = str(guard.with_name("other.mutex"))
    assert helper._retryable(error, guard.resolve()) is False


def test_exclusive_evidence_cannot_overwrite_prior_attempts(tmp_path, helper):
    fresh = {"ready": True}
    run(helper, tmp_path, lambda *a, **k: fresh)
    original = {p.name: p.read_bytes() for p in (tmp_path / "evidence").iterdir()}
    with pytest.raises(FileExistsError):
        run(helper, tmp_path, lambda *a, **k: pytest.fail("replay reached quota"))
    assert original == {p.name: p.read_bytes() for p in (tmp_path / "evidence").iterdir()}


def test_initial_insufficient_native_margin_does_not_call_quota(tmp_path, helper):
    with pytest.raises(helper.NativeAllocationMarginError):
        run(helper, tmp_path, lambda *a, **k: pytest.fail("no native time"), allocation_deadline_unix=1179)
    assert not list((tmp_path / "evidence").glob("attempt-*-before.json"))
    assert json.loads((tmp_path / "evidence/result.json").read_text())["attempts"] == 0


def test_native_margin_is_rechecked_before_retry_and_no_backoff_if_it_cannot_fit(tmp_path, helper):
    clock = Clock()
    calls = []
    error = contention(helper)

    def function(*args, **kwargs):
        calls.append(1)
        clock.elapsed += 30
        raise error

    with pytest.raises(ReceiptLockTimeout):
        run(helper, tmp_path, function, clock, allocation_deadline_unix=1200)
    assert calls == [1] and clock.sleeps == []
    after = json.loads((tmp_path / "evidence/attempt-1-after.json").read_text())
    assert after["retry_block_reason"] == "insufficient_native_allocation_margin"


def test_overall_300_second_window_blocks_new_call_without_replaying_operation(tmp_path, helper):
    clock = Clock()
    calls = []
    error = contention(helper)

    def function(*args, **kwargs):
        calls.append(1)
        clock.elapsed += 70
        raise error

    with pytest.raises(ReceiptLockTimeout):
        run(helper, tmp_path, function, clock)
    assert len(calls) == 4 and clock.sleeps == [2, 4, 8] and clock.elapsed == 294
    assert json.loads((tmp_path / "evidence/result.json").read_text())["status"] == "overall_quota_wait_budget_exhausted"


def test_sleep_delay_and_wall_clock_changes_cannot_bypass_next_call_guard(tmp_path, helper):
    clock = Clock()
    calls = []
    error = contention(helper)

    def sleep(seconds):
        clock.sleeps.append(seconds)
        clock.elapsed += 200
        clock.wall_offset -= 500

    clock.sleep = sleep

    def function(*args, **kwargs):
        calls.append(1)
        clock.elapsed += 30
        raise error

    with pytest.raises(helper.NativeAllocationMarginError):
        run(helper, tmp_path, function, clock, allocation_deadline_unix=1300)
    assert calls == [1]
    final = json.loads((tmp_path / "evidence/result.json").read_text())
    assert final["native_allocation_remaining_seconds"] == 70


def test_late_fresh_result_is_preserved_but_never_returned_as_success(tmp_path, helper):
    clock = Clock()
    fresh = {"ready": True, "measured_unix": 1301}

    def function(*args, **kwargs):
        clock.elapsed += 301
        return fresh

    with pytest.raises(helper.QuotaWaitBudgetExceeded):
        run(helper, tmp_path, function, clock)
    after = json.loads((tmp_path / "evidence/attempt-1-after.json").read_text())
    assert after["fresh_quota_result"] == fresh and after["result_accepted"] is False
    assert json.loads((tmp_path / "evidence/result.json").read_text())["returned_fresh_quota"] is False


def test_forward_wall_clock_jump_refuses_a_result_with_no_native_margin(tmp_path, helper):
    clock = Clock()

    def function(*args, **kwargs):
        clock.wall_offset += 9000
        return {"ready": True}

    with pytest.raises(helper.NativeAllocationMarginError):
        run(helper, tmp_path, function, clock)
    assert json.loads((tmp_path / "evidence/result.json").read_text())["status"] == "insufficient_native_allocation_margin"


@pytest.mark.parametrize("delay,deadline,error_type", [
    (100, 1250, "NativeAllocationMarginError"), (241, 10000, "QuotaWaitBudgetExceeded"),
])
def test_slow_pre_attempt_evidence_write_cannot_start_a_call_beyond_guards(
        tmp_path, helper, monkeypatch, delay, deadline, error_type):
    clock = Clock()
    writer = helper._json_exclusive

    def delayed_writer(path, value):
        result = writer(path, value)
        if Path(path).name == "attempt-1-before.json":
            clock.elapsed += delay
        return result

    monkeypatch.setattr(helper, "_json_exclusive", delayed_writer)
    with pytest.raises(getattr(helper, error_type)):
        run(helper, tmp_path, lambda *a, **k: pytest.fail("slow evidence left no call allowance"),
            clock, allocation_deadline_unix=deadline)
    after = json.loads((tmp_path / "evidence/attempt-1-after.json").read_text())
    assert after["quota_call_started"] is False
    assert json.loads((tmp_path / "evidence/result.json").read_text())["attempts"] == 0


def test_guard_and_owner_files_are_preserved_byte_for_byte(tmp_path, helper, monkeypatch):
    # Use a real local inode solely for this deterministic filesystem test.
    study = tmp_path / "study"
    monkeypatch.setattr(helper, "PERSONAL_STUDY_ROOT", study)
    state = study / "state"
    state.mkdir(parents=True)
    guard = state / "storage-headroom.mutex"
    owner = state / "storage-headroom.mutex.owner.json"
    guard.write_bytes(b"permanent locked inode placeholder")
    owner.write_bytes(b'{"state":"held","pid":123}')
    before = [(p.stat().st_ino, p.read_bytes()) for p in (guard, owner)]
    calls = []

    def function(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise ReceiptLockTimeout(guard, 30, {"pid": 123, "state": "held"})
        return {"ready": True}

    run(helper, tmp_path, function)
    assert before == [(p.stat().st_ino, p.read_bytes()) for p in (guard, owner)]


def test_production_default_calls_the_qualified_function_without_injection(tmp_path, helper, monkeypatch):
    called = []
    result = {"ready": True, "native_stdout": "fresh"}
    monkeypatch.setattr(helper, "require_storage_headroom", lambda *a, **k: (called.append((a, k)), result)[1])
    clock = Clock()
    returned = helper.require_storage_headroom_with_wait(helper.PERSONAL_STUDY_ROOT,
        required_files=0, evidence_dir=tmp_path / "evidence", allocation_deadline_unix=10000,
        clock=clock, sleeper=clock.sleep)
    assert returned is result and len(called) == 1
    assert called[0][1] == {"required_files": 0, "required_bytes": 0, "reserve_files": 0, "reserve_bytes": 0}


@pytest.mark.parametrize("kwargs", [
    {"allocation_deadline_unix": float("nan")}, {"allocation_deadline_unix": True},
    {"minimum_remaining_seconds": 119}, {"minimum_remaining_seconds": float("inf")},
])
def test_invalid_native_proof_or_reduced_margin_rejected_before_quota(tmp_path, helper, kwargs):
    with pytest.raises(ValueError):
        run(helper, tmp_path, lambda *a, **k: pytest.fail("invalid native proof"), **kwargs)
    assert not (tmp_path / "evidence").exists()


def test_wrong_personal_root_and_invalid_demand_are_rejected_before_any_quota(tmp_path, helper):
    with pytest.raises(ValueError, match="personal study root"):
        helper.require_storage_headroom_with_wait(tmp_path / "other", required_files=0,
            evidence_dir=tmp_path / "evidence", allocation_deadline_unix=10000)
    with pytest.raises(ValueError, match="nonnegative integer"):
        helper.require_storage_headroom_with_wait(helper.PERSONAL_STUDY_ROOT, required_files=True,
            evidence_dir=tmp_path / "evidence", allocation_deadline_unix=10000)
    assert not (tmp_path / "evidence").exists()
