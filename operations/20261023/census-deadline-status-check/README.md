# Census deadline failure and unstarted repair chain

Read-only live checks on Narval 3 at 23 October 2026, 08:38-08:42 CEST.

Census **6811498 failed with exit 1:0 at 03:20:45 CEST** after 3h57m28s.
The wrapper's preserved error is `Finite census deadline reached; original
timestamps preserved`. Its last reported 75,000 files and 57,043,731,217
logical bytes are partial. No final sweep result or successful receipt exists.
This is an explicit deadline failure; native status does not report OUT_OF_MEMORY.

All five dependent jobs were cancelled at 03:25:56 CEST with no start time
and zero runtime: prior qualification 6819399, repair qualification 6822432,
activation 6822568, behavioural probe 6822730 and archive qualifier 6824584.
Native Reason is None, so these records do not identify who cancelled them.
The failed afterok prerequisite could not admit their execution.

No new replacement controller ledger, activation receipt, descriptor,
installation or three-target retry journals exist. The current compressed
campaign state exactly matches the previously validated revision 405 bytes:
76 completed trainings and 67 completed policies. The stale revision 404
status file is not used as current progress evidence.

The separate main controller 6632164 remains RUNNING. The stored inventory
record retains a 20:26 UTC 22 October quota capture clock and cannot substitute
for a fresh successful six-hour census. A running main controller does not
establish replacement F2 controller health.

No jobs, source controls, state files, locks, data or freshness timestamps
were changed by these checks. The next operational requirement is to correct
the bounded census execution, qualify that change and register fresh dependent
jobs after reconciling these terminal records. No failed request is replayed.
