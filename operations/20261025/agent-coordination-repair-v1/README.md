This repair addresses registration-lock starvation and controller deadline
checks in the existing synthetic agent campaign. It preserves the original
dispatcher, workers, protocol, checkpoints, condition checks, provider clocks,
resource policy and scientific gates.

The controller takes a short registration snapshot, releases the global mutex
for the complete condition-byte audit, then reacquires it and requires an
unchanged campaign before genuine fresh admission and state commit. A separate
permanent controller mutex prevents concurrent writers. Every new worker keeps
the original admission code with a finite two-hour lock wait, bounded by its
actual allocation cleanup margin. Neither mutex is removed or replaced.

The deadline calculation checks native start/end/limit consistency and uses the
earlier of the live native end and environment end. It retains the original
four-hour shutdown reserve and records both clocks. The four replacement
segments each request eight hours on one CPU with 64 GiB; scientific workers
retain their original allocations.

One exact recovery reconciles training jobs 7110400, 7110771 and 7111066 through
the original receipt, checkpoint and dispatcher validators. It preserves the
revision443 compressed state and original failed training 7110041, requires no
training/admission output from that attempt, and removes only its active
reference. The same frozen intention may then use the new r3 namespace. Other
campaign records and all existing submission journals remain intact. A partial
or ambiguous activation requires explicit reconciliation.

All 72 local cases pass, including the complete registry-reader regression
suite with the revised finite wait. Native qualification and actual controller
startup have their own source-bound receipts in personal scratch. The source
manifest binds the four executable/test files and exact test roster. Local
tests do not establish native execution, successful recovery, completed
scientific inference, or publication readiness.

Native qualification 7177258 completed 0:0 in 86 seconds with all 72 cases passing. Controllers 7178192, 7178209, 7178218 and 7178228 were registered and released as one finite sequential chain. The first has positive native startup evidence and published the exact recovery receipt: revision443 to445, 83 completed trainings, 76 policies, and no repair flags. The three successful peer jobs were not rerun. This observation establishes recovery and startup, while the next audited dispatch and full scientific outcomes remain separate milestones.

The preserved `operator-actions/short-reconciliation.py` is the one-off short allocation used to invoke the same qualified recovery routine if a short slot became available first. Its actual plan separately pins its source hash. Native job 7180418 completed 0:0 in 81 seconds and validated the already published recovery from 7178192, without repeating the state transition. It adds no recurring controller.

At 08:12 UTC on 25 October the first controller had released eleven acknowledged workers. Retry 7183860 and trainings 7184131/7184461/7184786 were RUNNING; evaluations 7184908/7185228/7185654 were RUNNING. All eleven workers had positive native-start admission using registry guard 63735406494fae9c1417fe3a32396c03835ab9dc03332da3aab1ca1852558f96. Assemblies 7182930/7183132/7183498/7183790 had positive COMPLETED/0:0 records and complete worker receipts. Canonical revision 461 remained active with 83 recognized trainings, 76 policies and no repair flags, pending the next receipt reconciliation.

The preserved `operator-actions/finite-continuations.sh` registered two more four-slot groups after verifying the first production batch was fully released. Groups 7187374/7187376/7187384/7187393 and 7187402/7187410/7187424/7187433 depend sequentially on the existing tail and each other. All twelve total slots execute this same qualified component with eight-hour reservations and existing shutdown margins. Their maximum combined registration is 96 one-CPU hours, supplying up to 48 hours of controller work windows. The actual plans, immutable scripts, ledgers, held-state observations and release acknowledgements are retained in personal scratch.
