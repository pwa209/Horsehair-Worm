# Generic supervisor exit-race derivative

The derivative copies the exact job-6865674 helper captured in evidence 509,
SHA256 `dec3ace9591b71759bbd7418b550e657ecb87a85823652e9d1e56547fe19ec60`.
Only `supervise` changes. Every byte outside that function remains identical,
including all output flags, worker functions, source bindings and frozen private
test rosters. This derivative is for lifecycle regression and reconciliation;
its original scientific `main` is not authorized or invoked by these tests.

When Linux removes a terminating child's RSS record before `poll` reports its
exit, the new catch permits one natural-exit wait of at most one second, capped
by the remaining deadline. It requires an integer return code and retains the
existing worker artifact validator and bounded teardown. Timeout, nonzero exit,
unexpected RSS errors, invalid artifacts, and unconfirmed termination retain
their failure or bound classifications. No result or admission flag changes.

Run `python -B test_lifecycle_exit_race_20261024.py` from any working directory.
The test loader compiles only generic lifecycle functions and constants, with
synthetic process, RSS, time, signals and inert artifact bytes. It imports no
study module, opens no scientific dataset, makes no network or scheduler call,
and never executes numerical/model code. Set `HHW_TEST_WORKSPACE` to an existing
allocation-local workspace for fixtures. Without that setting, lifecycle test
fixtures use this directory's `test-work` subdirectory.

For a native run, set `HHW_LIFECYCLE_ORIGINAL` to the exact captured original
helper path. The original and repaired source hashes, byte-preservation check,
race reproduction, bounded success path, failure cases and integrity refusal
cases are all tested. `local-test-result.json` and `local-test-output.txt` record
the completed local run. Set `HHW_TEST_RESULT_DIR` to an existing output directory
to emit exclusive `test-result.json` and `test-output.txt` files there. Native
tests can therefore leave this source directory unchanged.

The derivative does not reconcile native job history. Job 6865674's original
failed allocation and receipts remain preserved; any attestation of its existing
worker artifacts must be a separate append-only, source-bound reconciliation.
