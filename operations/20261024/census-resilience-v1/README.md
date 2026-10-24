This is a new provider-only operational capsule. It preserves the original v6
census measurement, primitive identity, quota and conservative freshness bodies.
It changes the exact registration-lock acquisition budget from 900 to at most
7200 seconds, with 600 seconds left before the verified allocation end. Only
acquisition of the same permanent AF mutex is retried. An admitted callback,
scheduler mutation, measurement, or failed sweep is never replayed.

The finite provider plan has 64 sequential 8 CPU, 64G, six-hour allocations.
Slot 1 is immediate and requires positive completed new qualification. Later
slots use afterany and four-hour absolute earliest-start spacing. They require
actual owned terminal predecessor records and persisted afterany proof; a failed
predecessor never becomes a successful census. Each newly usable sweep still
requires its own complete original receipt, full byte measurements, conservative
original timestamps, quota checks and COMPLETED/0:0 native outcome.

The last earliest start is 252 hours after the new anchor. This finite schedule
does not guarantee fresh six-hour clocks during queue delays, and does not cover
the theoretical 384-hour maximum of four 96-hour controller allocations.

Native support: /scratch/pwa209/horsehair-worm/maintenance/study-census-resilience-support-v1-20261024
Provider cases: /scratch/pwa209/horsehair-worm/maintenance/study-census-resilience-v1-20261024/native-JOB
Qualification: /scratch/pwa209/horsehair-worm/maintenance/study-census-resilience-qualification-v1-20261024/qualification.json
Provider plan: /scratch/pwa209/horsehair-worm/plans/study-census-resilience-v1-20261024/plan.json
Provider ledger: /scratch/pwa209/horsehair-worm/state/study-census-resilience-v1-20261024-submission.json

Run build-native-plans.py --mode qualification --source-manifest-sha256 SHA to
freeze a new 4 CPU, 16G, one-hour generic qualification plan. It uses the approved
resource lease and real source/registration/native bindings without requiring a
fresh global census. It runs only tiny metadata/lock fixtures and independently
revalidates original job 6863966, its exact old qualification report, all 23 old
source pins, all native JUnit suites, and the original native syscall canary.
It never performs a whole study scan or fabricates storage admission from tests.

After that job completes, build-native-plans.py --mode provider requires explicit
--qualification PATH --qualification-sha256 SHA --qualification-job-id JOB and
the source-manifest SHA. Both builder modes only write fresh immutable plans;
they never submit, hold, release or cancel jobs.

The public metadata-only validator is
validate_schedule_binding(schedule, qualification_binding, native_reader).
The schedule retains six keys: plan, ledger, helper, source_manifest,
qualification, case_root. It validates every source/command/script/slot/native
qualification and returns checked_paths plus the actual qualification proof.
Consumers under consumers/ are separate source capsules and must be independently
qualified; this provider sealer never includes or changes them. Existing running
consumers do not adopt this provider automatically. No biological definition,
scientific authority, complete-workload gate, or old history is changed.

After selecting a successful current receipt, consumers also call
validate_predecessor_receipt(receipt, native_reader). This separately preserves
actual terminal predecessor evidence without treating its failure as a census.
