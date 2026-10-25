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
