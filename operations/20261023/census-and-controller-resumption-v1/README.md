# Census and controller resumption: qualified source checkpoint

This source checkpoint is fixed at **09:56:31 UTC on 23 October 2026**. Native
qualification 456, job **6863966**, completed with **exit `0:0`** at 09:54:30 UTC after
166 seconds using 8 CPUs. All 13 actual native JUnit suites passed **757 cases**
with no errors, failures or skips. Its allocation-local filesystem canary
completed and verified restoration of the original iterator and preservation of
the original measurement functions. Qualification used disposable fixtures; a
completed study-wide census remains a separate requirement.

Coordinator425, job **6864084**, was **RUNNING** from 09:56:20 UTC and binds the
successful qualification as its native dependency. At the checkpoint there were
no registration ledgers for 429–434 or451. The five cancelled downstream jobs
had **not** been resubmitted. A controller handoff, controller health and
scientific progress are not established by this checkpoint. The campaign stayed
at revision 405, with compressed state SHA256
`d44aa7c09bcd09c007009d9d5af58c2976f1d53eabbdd93c8f0daf92d9afa684`.

The earlier qualification 452, job 6861567, failed with exit `1:0`. Its other 12
suites passed 686 cases; its external-census policy suite passed 56 cases and
raised 15 fixture errors because staging omitted the exact frozen base-controller
sibling. The frozen v4 source set and failed-attempt evidence remain preserved.
The v5 successor adds that exact fixture to staging and changes only the reviewed
namespace constants. Its fourteen-file manifest is
`1b21742f6ad5b7b48899a4890988da7ddba439798ba8b12a2bad57c7f1898e6e`.

The candidate uses a bounded streaming storage iterator only in the dedicated
census process. Original file measurement, ownership, directory identity, race,
EOF and budget-validation functions remain unchanged. The finite provider plan
contains at most 64 sequential 8 CPU, 64 GB, six hour allocations, with an upper reservation
of 3,072 CPU core-hours and actual DAG concurrency of 8 CPUs. Original quota clocks
must remain strictly younger than six hours; neither completion time nor a later quota
probe refreshes the original admission clock. Scheduled spacing and queue delay
can leave gaps. The external-census policy waits through such gaps instead of
starting the original two-hour scan. Its new controller contract is 1 CPU, 64 GB and 96 hours;
actual memory fit still requires native observation.

The separately tested handoff requests 4 CPUs, 16 GB and two hours. Its 91 passing local tests do
not qualify it natively. Its own allocation must run those tests before scheduler
changes. It joins released worker journals under the permanent AF mutex before
retiring only the exact four ACT-created controllers. New controllers remain
held until their source, native ownership, resources and dependencies are
verified; their first dependency requires successful handoff completion.
Workers, campaign state, scientific records and existing journals are preserved.
Unknown or ambiguous scheduler actions are retained for explicit reconciliation.

`census/` preserves the historical v4 source set. `census-v5/` contains the
successfully qualified successor, its manifest and the additional exact base
fixture. `work/` mirrors the selected original relative paths needed by local
builders, test fixtures and reviewed registration envelopes. Additional local
evidence includes 138 registration/coordinator cases, 12 staging-fixture cases,
91 handoff cases and 37 handoff-builder cases. These local scopes overlap the
source closure and retain their own source hashes and JUnit rosters.

The own-job native observations, submit receipts and qualification summary are
preserved. `native-qualification/qualification.json` has the actual native SHA256
`8b0dc612cee64461b991a9dfb9a975cf5afe1389a037b1357244d829f5aca76c`.
It and the canary receipt were re-encoded from captured JSON values using their
producer's exact serialization and checked against the captured native hashes;
their bytes match those native files. The live459 evidence retains the original
capture, actual native rows and all13 actual JUnit counts and hashes.

Historical failures include the Windows/Linux errno test-ID difference and the
Python3.11 AST representation difference; neither repair changes an operational
function body. The original public framework packages, explicit source
identities, old qualifications and scientific gates remain required runtime
dependencies.

This source-only capsule does not establish activation, a completed census,
storage savings, deletion authority, controller health, or a scientific result.
No raw cross-project queue inventory is included. The separate new primary
controller policy is outside this capsule. Later coordinator and child outcomes
must be recorded as new observations.
