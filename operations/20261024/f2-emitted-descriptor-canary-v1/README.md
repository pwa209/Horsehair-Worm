# Actual emitted descriptor canary

`run_emitted_descriptor_canary.py` supplies `canary(pack, policy_path)` for the
handoff immediately after the frozen controller builder emits its new plan,
policy and descriptor, before holds, cancellations or submissions. It does not
modify any of the 16 qualified backfill source files.

The helper checks the actual four-task plan and source hashes, and requires the
new descriptor to equal the captured original descriptor with only the three
declared operational changes. It routes every actual command through the frozen
outer `main`, including its policy schema, argument/script/plan checks,
qualification/provider validators and guards. The adapter invokes the real
base `worker_binding`, `registry_binding`, `retry_binding` and `source_guard`
against existing immutable files and persisted completion proofs.

The controller has not been submitted, so its registration ledger, job identity
and owned allocation are explicitly in-memory fixtures. The real base
`load_context`, admission/guard installation, controller `run` and dispatch are
not executed. The adapter supplies only the metadata context needed by the
read-only gates. Every segment stops before the original controller run.
Filesystem writes and subprocesses are prohibited during routing; any attempted
mutation fails the canary before the handoff can proceed. The returned proof
states these limits explicitly and must not be described as live controller
startup or native allocation validation.

Eleven local generic tests pass, including the actual frozen `controller_plan`
function operating on captured descriptor `9a0706...` and actual qualification
6942315 metadata, with intercepted writes and plan freeze. The other cases
exercise exact preservation, duplicate-task refusal and read-only enforcement.
No numerical function, model, scheduler command or remote call executes in these
tests. The included captured JSON files are the exact independently observed
metadata inputs used by the tests.

Only the helper file needs deployment alongside the separate handoff source.
The caller records its returned proof after the helper returns successfully.
Any failure preserves the original pending controllers and requires review;
the canary performs no automatic repair or retry.
