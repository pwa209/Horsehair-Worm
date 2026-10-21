"""Bounded retries of one exact quota guard contention; never stage retries.

This private wrapper preserves the qualified quota function, its arguments and
its fresh return object. It never touches the guard or informational owner file,
changes scientific code, grants resource admission, or retries storage errors.
The caller supplies the end time from its unchanged native allocation proof.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import time

from horsehair.budget import require_storage_headroom
from horsehair.core import ReceiptLockTimeout


PERSONAL_STUDY_ROOT = Path("/scratch/pwa209/horsehair-worm")
MAX_ATTEMPTS = 5
BACKOFF_SECONDS = (2, 4, 8, 16)
MAX_WAIT_SECONDS = 300
CALL_ALLOWANCE_SECONDS = 60  # Qualified 30s guard wait plus 30s quota query.
MINIMUM_NATIVE_MARGIN_SECONDS = 120


class QuotaWaitBudgetExceeded(TimeoutError):
    """No further quota call fits the finite retry window."""


class NativeAllocationMarginError(TimeoutError):
    """No further quota call fits the caller's verified native allocation."""


def _number(value, name, *, minimum=None):
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) or minimum is not None and value < minimum):
        raise ValueError(name + " must be finite and within its declared bound")
    return float(value)


def _json_exclusive(path, value):
    body = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")
    with Path(path).open("xb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    return {"path": str(Path(path).resolve()), "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}


def _retryable(error, guard):
    # The exact qualified exception class and actual resolved guard are needed.
    # TimeoutError, subclasses, other receipt guards and changed diagnostics fail.
    if type(error) is not ReceiptLockTimeout:
        return False
    try:
        return (Path(error.path).resolve() == guard
                and Path(error.diagnostics["guard_path"]).resolve() == guard
                and math.isfinite(error.waited_seconds) and error.waited_seconds >= 0)
    except (AttributeError, KeyError, TypeError, ValueError, OSError):
        return False


def _exception_record(error):
    result = {"type": type(error).__name__, "message": str(error)}
    if isinstance(error, OSError):
        result["errno"] = error.errno
    if isinstance(error, ReceiptLockTimeout):
        result["diagnostics"] = error.diagnostics
    # Preserve fresh headroom refusal diagnostics without consulting any cache.
    if type(error).__module__ == "horsehair.budget" and type(error).__name__ == "StorageHeadroomError":
        result["storage_headroom_record"] = error.record
    return result


def require_storage_headroom_with_wait(root, *, required_files, required_bytes=0,
        reserve_bytes=0, reserve_files=0, evidence_dir, allocation_deadline_unix,
        minimum_remaining_seconds=MINIMUM_NATIVE_MARGIN_SECONDS,
        quota_function=None, clock=None, sleeper=None):
    """Return the unchanged fresh quota result after at most five exact calls.

    Only exact ReceiptLockTimeout at this personal study's storage-headroom
    mutex is retryable, using 2/4/8/16-second backoff. The 300-second monotonic
    window reserves 60 seconds for each new qualified call. The verified native
    end time must also leave that allowance plus at least 120 seconds of margin.
    Backward wall-clock changes cannot extend the original allocation allowance.

    An in-flight qualified call or filesystem write is not forcibly interrupted;
    a result arriving beyond either guard is evidenced and refused. No signal,
    global monkeypatch, cached result, lock removal or stage replay is used.
    evidence_dir must be new. Optional function/clock/sleeper injection exists
    for deterministic local tests; production callers omit it. An injected clock
    provides time() and monotonic() methods, like the standard time module.
    """
    root = Path(root).resolve()
    if root != PERSONAL_STUDY_ROOT.resolve():
        raise ValueError("Quota wait requires the exact authorized personal study root")
    guard = root / "state/storage-headroom.mutex"
    guard = guard.resolve()
    demand = {"required_files": required_files, "required_bytes": required_bytes,
              "reserve_bytes": reserve_bytes, "reserve_files": reserve_files}
    if any(type(value) is not int or value < 0 for value in demand.values()):
        raise ValueError("Quota demand and reserve must be nonnegative integer counts")
    allocation_end = _number(allocation_deadline_unix, "Native allocation deadline", minimum=0)
    margin = _number(minimum_remaining_seconds, "Native allocation margin",
                     minimum=MINIMUM_NATIVE_MARGIN_SECONDS)
    timer = time if clock is None else clock
    sleep = time.sleep if sleeper is None else sleeper
    reader = require_storage_headroom if quota_function is None else quota_function
    started = _number(timer.monotonic(), "Monotonic clock", minimum=0)
    started_unix = _number(timer.time(), "Wall clock", minimum=0)
    monotonic_allocation_end = started + allocation_end - started_unix
    last_monotonic = started
    evidence = Path(evidence_dir)
    if evidence.is_symlink():
        raise ValueError("Quota evidence directory cannot be a symlink")
    evidence = evidence.resolve()
    if evidence.is_relative_to(root / "state"):
        raise ValueError("Quota retry evidence must not write into the quota state directory")
    evidence.mkdir(mode=0o700, parents=True, exist_ok=False)
    _json_exclusive(evidence / "intent.json", {
        "operation": "fresh require_storage_headroom with exact guard-contention wait",
        "root": str(root), "retryable_guard_path": str(guard), "demand": demand,
        "max_attempts": MAX_ATTEMPTS, "backoff_seconds": list(BACKOFF_SECONDS),
        "overall_wait_seconds": MAX_WAIT_SECONDS, "new_call_allowance_seconds": CALL_ALLOWANCE_SECONDS,
        "allocation_deadline_unix": allocation_end, "minimum_native_margin_seconds": margin,
        "started_unix": started_unix, "lock_or_owner_modified": False,
        "scientific_implementation_changed": False, "admission_changed": False,
        "quota_cache_used": False, "whole_stage_retried": False})

    def snapshot():
        nonlocal last_monotonic
        mono = _number(timer.monotonic(), "Monotonic clock", minimum=0)
        wall = _number(timer.time(), "Wall clock", minimum=0)
        if mono < last_monotonic:
            raise ValueError("Monotonic clock moved backward")
        last_monotonic = mono
        return {"captured_unix": wall, "elapsed_seconds": mono - started,
                "retry_window_remaining_seconds": MAX_WAIT_SECONDS - (mono - started),
                "native_allocation_remaining_seconds": min(allocation_end - wall, monotonic_allocation_end - mono)}

    def block_reason(state, delay=0, *, new_call=True):
        allowance = CALL_ALLOWANCE_SECONDS if new_call else 0
        if state["retry_window_remaining_seconds"] < delay + allowance:
            return "overall_quota_wait_budget_exhausted"
        if state["native_allocation_remaining_seconds"] < delay + allowance + margin:
            return "insufficient_native_allocation_margin"
        return None

    def finish(status, attempts, state, **extra):
        _json_exclusive(evidence / "result.json", {
            "status": status, "attempts": attempts, **state,
            "lock_or_owner_modified": False, "scientific_implementation_changed": False,
            "admission_changed": False, "whole_stage_retried": False, **extra})

    for attempt in range(1, MAX_ATTEMPTS + 1):
        before = snapshot()
        reason = block_reason(before)
        if reason:
            finish(reason, attempt - 1, before, returned_fresh_quota=False)
            error_type = QuotaWaitBudgetExceeded if reason == "overall_quota_wait_budget_exhausted" else NativeAllocationMarginError
            raise error_type("Fresh quota call refused: " + reason)
        _json_exclusive(evidence / f"attempt-{attempt}-before.json", {
            "attempt": attempt, "demand": demand, "root": str(root), **before,
            "new_call_allowance_seconds": CALL_ALLOWANCE_SECONDS})
        # Lustre evidence persistence can itself wait. Refresh immediately before
        # the actual quota call rather than relying on the pre-write snapshot.
        immediate = snapshot()
        reason = block_reason(immediate)
        if reason:
            _json_exclusive(evidence / f"attempt-{attempt}-after.json", {
                "attempt": attempt, "status": "quota_call_not_started", "quota_call_started": False,
                "guard_block_reason": reason, **immediate})
            finish(reason, attempt - 1, immediate, returned_fresh_quota=False)
            error_type = QuotaWaitBudgetExceeded if reason == "overall_quota_wait_budget_exhausted" else NativeAllocationMarginError
            raise error_type("Fresh quota call refused after evidence write: " + reason)
        try:
            result = reader(root, **demand)
        except Exception as error:
            after = snapshot()
            eligible = _retryable(error, guard)
            delay = BACKOFF_SECONDS[attempt - 1] if attempt < MAX_ATTEMPTS else None
            reason = block_reason(after, delay) if eligible and delay is not None else None
            retry = eligible and delay is not None and reason is None
            _json_exclusive(evidence / f"attempt-{attempt}-after.json", {
                "attempt": attempt, "status": "quota_call_exception", "exception": _exception_record(error),
                "exact_guard_contention": eligible, "fresh_quota_call_retry": retry,
                "next_backoff_seconds": delay if retry else None, "retry_block_reason": reason, **after})
            if retry:
                sleep(delay)
                continue
            status = reason or ("quota_lock_attempts_exhausted" if eligible else "quota_call_failed_not_retryable")
            finish(status, attempt, after, returned_fresh_quota=False, exception_type=type(error).__name__)
            raise
        after = snapshot()
        reason = block_reason(after, new_call=False)
        quota_proof = _json_exclusive(evidence / f"attempt-{attempt}-after.json", {
            "attempt": attempt, "status": "fresh_quota_returned", "fresh_quota_result": result,
            "result_accepted": reason is None, "guard_block_reason": reason, **after})
        if reason:
            finish(reason, attempt, after, returned_fresh_quota=False, fresh_result_evidence=quota_proof)
            error_type = QuotaWaitBudgetExceeded if reason == "overall_quota_wait_budget_exhausted" else NativeAllocationMarginError
            raise error_type("Fresh quota result refused after call: " + reason)
        finish("fresh_quota_complete", attempt, after, returned_fresh_quota=True,
               fresh_result_evidence=quota_proof, quota_cache_used=False)
        return result
    raise AssertionError("Bounded quota wait exhausted without returning or raising")
