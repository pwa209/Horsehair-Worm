This is the existing coordination component for the unchanged synthetic agent
campaign. Its current revision repairs lost Slurm submission acknowledgements
as well as the earlier registration-lock and deadline problems. The executable
revision is frozen in `agent-coordination-repair-v2-20261025` on personal scratch;
no additional controller framework is introduced.

Every submission first persists its exact command and intention. A client timeout
triggers a read of the actual owned held job, checked against the original
script, job name, account, resources and scalar allocation. A known native ID
cannot be replaced. An unresolved attempt waits for reconciliation and never
calls `sbatch` a second time. Release acknowledgements receive the same treatment:
matching actual execution can establish that an uncertain release succeeded.
Normal process output is preserved; native recovery does not manufacture it.

Recovery preserves the compressed campaign state and all existing journals,
plans and ledgers, validates the four completed workers through the original
receipt and checkpoint validators, then resumes the original seven saved
intentions. Assembly job 7191569 is adopted from its original ledger and script;
its native job ID and original files remain unchanged. The other six intentions
may be submitted only when their original artifacts establish they were never
attempted. Each saved acknowledgement precedes release outside the registration
mutex. The normal dispatcher then resumes subsequent evaluation and assembly.

There are 106 passing local cases, including timeout/restart recovery without a
second submission, strict identity mismatches, missing native acknowledgements,
receipt reconciliation, lost release replies, conservative native deadlines,
and the full existing registry suite. The five executable/test files and exact
test roster are bound by `source-manifest.json`. Native qualification and actual
recovery require their own receipts; a local passing test is not native proof.

Replacement segments request at most 96 hours on one CPU with 64 GiB and permit
Slurm to shorten the allocation to fit a backfill window of at least eight
hours. The earlier native/environment deadline remains authoritative, with a
four-hour shutdown reserve. Sequential `afterok` dependencies prevent a fatal
controller error from consuming every successor. Existing workers, numerical
functions, frozen protocol, provider freshness rules and scientific gates are
preserved.

The earlier revision moved the complete condition-byte audit outside the global
registration mutex and used a compare-and-swap check before fresh admission and
commit. Its permanent controller mutex and finite worker admission waits remain
in place. Neither mutex is deleted or replaced.

Historical recovery: qualification 7177258 completed 0:0 with all 72 then-current
cases passing. Controller 7178192 reconciled trainings 7110400, 7110771 and
7111066, preserved the original failed pre-start training 7110041 and its state,
and allowed its unchanged intention to use the r3 namespace. The preserved
`operator-actions/short-reconciliation.py` validated that recovery in native job
7180418 without repeating the state transition.

The original twelve eight-hour controller slots subsequently failed after a
45-second `sbatch` timeout. Slurm had accepted held assembly 7191569, while the
campaign still lacked its acknowledgement. The successors then encountered the
same unresolved acknowledgement. Revision 473 preserved 87 recognized trainings
and 80 policies; completed trainings 7189552/7189779 and evaluations
7190034/7190227 awaited reconciliation. The current revision addresses that
specific failure without rerunning completed workers or replaying the handoff.
