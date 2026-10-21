# Metadata recovery and native compute feasibility

This extension preserves the frozen primary scientific source
`0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630`
and full configuration
`446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35`.
Its software release has a separate source identity. It produces source audits
and technical cost evidence; it does not change H1–H4 or waive their gates.

## Original metadata

`scripts/audit_dataverse_metadata.py` inventories the complete 875-workbook
roster in [Dataverse V1.0](https://doi.org/10.7910/DVN/LTMCFR). Direct downloads
target the user's personal Narval scratch. The user approved a metadata-only
local relay when the provider refused the Narval routes. Every workbook must match its
provider byte count and MD5, and its original dataset directory must be bound
to the same file identity. Original bytes, cell coordinates, literal values and
formulas are retained. Formulas are not executed. Two workers use finite
retries and a total deadline; verified per-file checkpoints permit an explicit
same-roster continuation without discarding failed attempts.

Inventory completion is distinct from a validated biological crosswalk.
Treatment, exposure clocks, donor/recipient links, animal reuse, gene/reagent
assignments and family labels require explicit original evidence. A blank Rx
is unknown. Survival and filenames do not establish exposure. Donor-to-well
tables alone do not establish recipient-to-tracking-ROI identity.

Official catalog records identify [BDSC25936 as an Lkr reagent](https://flybase.org/reports/FBsf0000086139.html)
and [BDSC25836 as a different reagent](https://flybase.org/reports/FBsf0000086060.html).
Catalog identity resolves a stock lookup; contradictory published study labels
remain unresolved until original experiment assignments establish the used stock.

The initial 21 October source review parsed 503 checksum-verified Dataverse workbooks,
including all 15 primary classifier workbooks and both transfusion identity
tables. After a cooldown, the ordinary official route served the remaining
372 originals. All **875 workbooks (21,143,827 provider bytes)** were verified
against provider size and MD5 and staged in personal Narval scratch. The
complete native checksum/cell audit completed in 93 seconds in job 6658785, with
zero unresolved originals. Bounded annotation discovery then completed all 875
files in job 6659341, scanning 4,184,749 cells. Its 300,000-record candidate export
limit was reached; complete original-cell files remain available. Detailed row
reviews cover 42 provider workbooks and the two separate archive originals.
Source acquisition and candidate scanning do
not establish a validated biological crosswalk.
An independent ZIP metadata audit also recovered two author-referenced recipient
workbooks from the already verified Zenodo archive; these are separate original
sources, outside the 875-file roster. All six recipient cohorts named by the
author's transfusion loaders now have original workbooks available.

Every primary workbook has 128 recording-local ROI rows. All 1,920 original
`Sheet1!G2:G129` treatment (`Rx`) cells are blank. Those sheets supply no labelled
exposure-start or elapsed-exposure-at-recording-start field. Repeated board/ROI
labels on adjacent recording days leave possible animal reuse unresolved.
These are observed source limitations, independent of computational capacity.

Explicit donor/well/recipient joins can be recovered for parts of the transfusion
experiments. Physical placement ambiguities, incompatible donor classifications,
outcome-derived formulas, lost recipients and blank treatment cells remain
attached to the affected original records. One original key explicitly says
blank treatment cells can contain dummy flies; blank cells cannot become
unexposed controls. Source-code replication does not settle these ambiguities.
No complete H1, H2 or H3 metadata gate was cleared by the review.

Additional source rows recover literal stock/genotype, sex and treatment-field
entries. The 20 newly reviewed workbooks contain 362 literal `Rx=Uninfected`
entries and 1,782 blank Rx entries. These counts refer to original ROI records,
including empty or annotated rows; independence and eligibility remain unvalidated. The
literal `Uninfected` token still requires original exposure-semantic validation.
Three reviewed “96 hour” directory labels supply no validated exposure event or
offset in their original sheets; two disagree with the row genotype labels.

Original file 6826794 explicitly calls its board information a placeholder.
Files 6820288 and 6820589 have identical parsed cell matrices despite different
dates, directory labels and workbook hashes. The complete 875-file parsed-hash
check found this one duplicate-annotation pair. It establishes neither animal
identity nor independent controls. Those source conflicts remain attached to
the affected records.

The private source review retains an explicit interpretation correction for
24 February's Outcome formulas: 30 reference ROIs 65–94 outside the author's
loaded ROIs 1–64. The original formulas and previous literal-cell records remain
unchanged. A separately verified 32-row rotation in donor-status values does
not establish the formula-reference pattern or authorize a source correction.

## Behavioural costs

`horsehair.behaviour_feasibility` prospectively binds all original development
rosters and the 39 structural model/bin-width combinations. Each cell uses the
longest complete native recording clock, chosen by length then identity,
and retains full-roster parameter dimensions. An isolated interpreter imports
the unchanged primary numerical package.

At each declared learning rate, the technical pack measures one Adam epoch,
one original strong-Wolfe LBFGS iteration (including its objective/gradient
closures), unseen-group scoring and causal full-clock simulation. It forecasts
the original multiplicities, including requested starts, 500 Adam epochs and
200 LBFGS iterations. It does not claim that one iteration measures convergence.
Recording-relative time is explicitly technical; it supplies no missing exposure
timestamp or biological fit. A supervisor preserves completed operations and
partial progress before the native allocation ends.

Even complete coverage of this pack leaves full-cohort optimizer stopping,
Hessian/MALA, recovery fits and intervals, Mantis validation and source-validated
H2 workloads unmeasured. `complete_declared_workload` and `admission_authority`
remain false. Forecasts are empirical projections, not observed expenditure or
mathematical lower bounds. An over-budget projection does not authorize a
smaller scientific experiment or new numerical approximations.

## Agent CPU costs

`horsehair.agent_cost_envelope` measures all 12 task/architecture cells and both
fixed benchmark seeds with the existing trained checkpoints. Separate, finite
synthetic operator fixtures measure production batch shapes (64 evaluation,
32 calibration) for 200 steps, all declared mechanisms/ranks/strengths and
matching/restoration operations. They produce no replacement episode outcomes.
Environment, restoration and production-row storage costs are also measured.

The complete frozen per-policy workload is charged: 300 grid points, 2,404
evaluation calls, one captured calibration evaluation, 1,550 KL calls and 300
restoration checks. No unavailable episode receives an early-failure discount.
Aggregation requires the complete native cell roster, source and configuration
bindings, original input hashes and successful native accounting. Empirical
costs include a safety factor of at least 1.5; they are not a duration guarantee.

Original numerical-null rows and diagnostics remain unchanged. The frozen H4
completion check explicitly rejects such rows. Any change to that scientific
completion rule requires a declared analysis amendment. A cost envelope never
writes resource admission or final scientific results. Final execution still
requires a separate valid admission, combined accounting, CPU/GPU ceilings,
fresh personal quota and the original 20,000 CPU-core-hour cap.

## Native execution and evidence

New code must pass the complete immutable test suite in a Slurm allocation.
Preparations, finite probes and aggregations run with registered plans and
ledgers, resource leases and fresh personal storage checks. Failed historical
jobs and evidence remain available. Technical completion, scientific readiness
and downstream admission are reported separately in the roadmap.

The qualified extension passed **1,444 tests** in native job 6649674. Actual-input
preflight job 6656526 then validated all 24 existing agent policies and all 39
behavioural cells. The released arrays are 6657914 (12 agent cells) and 6657821
(39 behavioural cells), with aggregators 6657916 and 6657889. Their current
receipts and native accounting determine completion; queue submission and
partial operation logs are not complete benchmark results.

Native wrappers retain strict owner, account, source and resource validation.
They preserve raw scheduler evidence and make only finite fresh retries of
timed-out read-only status requests or strictly validated live-record conflicts.
Scheduler mutation commands are never automatically retried. Frozen primary
interpreters retain qualified Alliance vendor libraries while verifying the
primary scientific package identity.

At 14:21 UTC on 21 October, all 12 agent CPU cells and aggregate job 6657916
completed successfully. The source-bound aggregate reports 24 measured policies
and a safety-scaled forecast of 10,725.24 CPU core-hours for evaluation of the
240 final policies, with approximately 64.15 GB and 2,640 additional files.
This is a projected evaluation cost, not observed final-study expenditure. Final
GPU training and H4 analysis remain outside this estimate. The original 254
unavailable episode rows remain preserved; scientific admission is still
`not_admitted`.

The largest per-policy evaluation forecast is 69.89 hours. Prospective final
job bounds and partitioning therefore still need review against the current
12-hour evaluation allocations before admission; a projected total below the
study CPU cap does not establish that each individual allocation is sufficient.

Behavioural cells 6657821_18 and 6657821_19 failed before numerical processing
when their 30-second storage-quota guard wait expired while controller 6632164
held that guard. The existing guard, owner records, failed accounting and all
successful measurements were preserved. A private launcher now retries only
the exact storage-headroom `ReceiptLockTimeout`, with at most five fresh calls,
a 300-second overall window, native allocation margins and durable per-attempt
evidence. Storage exhaustion and other failures remain immediate errors. The
guard passed 28 focused tests; the qualified numerical source remains unchanged.

Recovery deployment 6668893 completed successfully and released array 6669092
for only indices 18–19, sequentially. Fresh aggregate 6669095 depends on both
the original 39-cell array and the retry array. It writes a separate summary;
the original aggregate and submission ledger were not changed. Both retry
cells completed successfully with native exit code 0:0.

The 15:09 UTC completion check found no active or queued benchmark jobs. Both
behavioural aggregators, 6657889 and 6669095, completed successfully and retained
all 39 source-bound receipts. Coverage is **38 of 39 complete cells**, or
**463 of 468 required operations**. The five-second AR cell 6657821_5 reached
its 6,300-second worker deadline during the 0.001 learning-rate LBFGS probe.
Its receipt preserves 7 of 12 required operations; the 0.001 LBFGS iteration and
all four 0.003-rate operations remain unmeasured. The supervisor's native
COMPLETED/0:0 status does not replace the explicit `bounded_deadline_reached`
numerical receipt.

Both summaries report `measured_partial_projection_exceeds_cap`: approximately
91.21 billion CPU core-hours under the conservative frozen maximum-work
optimizer extrapolation, against the 20,000 CPU-core-hour study cap. This is
neither actual expenditure nor a mathematical lower bound on a converged fit.
Full optimizer paths, Hessian/MALA, recovery, Mantis, H2 and full-fit output
storage are still unmeasured. Workload and cost assumptions require review
before further expensive probes or scientific admission. Source data, frozen
analysis rules and all scientific/admission gates remain unchanged.

## Autoregressive repair and cost review, 21 October

The five-second AR outlier used eight Torch threads, whereas the frozen primary
fit defaults to one. Its 0.001-rate held-out score took 3,771.82 seconds, compared
with 0.76 seconds at 0.0003; Adam and simulation remained about 1–4 seconds.
The subsequent first LBFGS closure exhausted the bounded worker window. This
identifies a runtime outlier, not demonstrated statistical nonconvergence. The
precise cause remains unresolved. A separately registered one-CPU retry restores
the primary thread setting while retaining the full 15,120-bin clock, original
data, seed, rates, numerical precision and optimizer arguments. Historical files
remain unchanged. The repair graph reserves at most five CPU core-hours including
its supervisor and fresh aggregation, and retains fresh storage, combined
accounting and native allocation checks.

Source update `3d939128151e` adds finite objective, parameter, available-gradient,
held-out-score and observed-simulation validation, plus process CPU and thread
metadata for future probes. It changes no scientific kernel or optimizer update.
All 84 focused tests pass locally. Native job 6675953 passed the complete
1,504-test suite with zero failures, errors or skips and native COMPLETED/0:0
in 380 seconds. Its qualification receipt SHA is
`b3fbcdf65791833f4f59da3323674a1cb23ad06872ff98f7aa52b3168f55ec50`.
Old timings have not retrospectively acquired finite output validation. The
one-CPU retry uses the already qualified historical worker to complete that
original operator pack.

AR retry 6678131 completed with native COMPLETED/0:0 in 175 seconds. Its worker
completed all 12 required operation/rate pairs in about 64 seconds, including
all three learning rates on the full 15,120-bin clock. The previously stalled
0.001-rate score took 0.375 seconds and its LBFGS probe 2.51 seconds. The old
attempt remains unchanged. One thread plus a different node resolved this
observed runtime failure; their separate causal contributions are untested.
Aggregation 6678132 completed with native COMPLETED/0:0 in 84 seconds and confirms
39 of 39 cells and 468 of 468 required operator/rate pairs. Its summary SHA is
`9a6f4055fe574cc61c4cc80a387bd31bda601fa9d682a2ab09d27c01008728bd`.

The arithmetic review independently replays the old forecast and preserves all
original durations. It pairs each duration with its own allocated CPU count,
excludes only recording-local ROIs that the primary implementation already
rejects as entirely unobserved, and charges the selected worst-cost outer/freeze
structure once while retaining every inner candidate fit. ROIs with at least one
observed displacement number 1,575, 1,561 and 1,551 at 5, 10 and 20 seconds
respectively. Presence alone does not establish finite targets or a usable
post-warmup scoring window. These differing rosters do not establish animal
independence or biological treatment assignments.

The exact installed Torch 2.6 LBFGS source supports a whole-step bound of 275
closures for the frozen 200 iterations, replacing the historical loose bound of
5,201. This changes cost accounting only. The independently reproduced historical
timing scenario is approximately **7.732 billion CPU core-hours**, compared with
the original **91.210 billion** partial optimizer projection. Both use empirical
maximum operator durations and requested workload bounds; neither is observed
expenditure, a mathematical lower bound, or a guaranteed full-fit runtime. All
outliers remain available and no eightfold CPU discount is assumed.

Native audit 6677905 completed with COMPLETED/0:0 in 81 seconds and reproduced
the reviewed 7.732-billion-hour scenario directly from all 39 historical receipts,
their positive native completion proofs, exact bin hashes and installed Torch
source. Its reviewed document SHA is
`98e4bb4db5c1d0d298a37eb8b3ac369209397f37bee15099ca3fc4b242378bd4`.
This base review covers the historical 463 of 468 operations. Final audit 6678315
then completed with native COMPLETED/0:0 in 100 seconds. It verifies the new
same-plan aggregate and all 468 operations while retaining the historical outlier
and every original binding. The corrected scenario remains 7.732 billion CPU
core-hours. The final review SHA is
`191be9d79902b67c87e8e2ee3dce379c784b7e7a0b9ce39dde6e355ee8a906e3`.
Full declared-workload, scientific-fit and admission-authority flags remain false:
operator coverage is complete, but whole-study cost coverage is incomplete.

Within that reviewed scenario, Adam contributes 4.021 billion and LBFGS 3.711
billion CPU core-hours. Recovery refits contribute 77.00% of the total, while AR
contributes 0.40%. Completing the AR retry therefore closes an operation-coverage
gap but does not resolve the budget projection.

At 15:39:06 UTC, verified native spending was 992.82 CPU core-hours. Spending plus
full reservations for unfinished registered work was 1,570.82 CPU core-hours and
34.36 GPU-hours, leaving 18,429.18 CPU core-hours before the new qualification and
audit allocations. Subtracting the separate 10,725.24-hour agent evaluation
forecast would leave only 7,703.94 CPU core-hours for every other unfinished CPU
category. That subtraction is a planning illustration, not admission: the agent
estimate excludes final GPU training and H4 inference and preserves its numerical
null completion gate.

At the 16:23 UTC closing check, recorded spending was 993.82 CPU core-hours and
34.36 GPU-hours. The final audit's preceding conservative combined admission
check recorded 1,570.79 CPU core-hours including unfinished allocation bounds,
leaving 18,429.21 below the cap at that check. This includes a full one-hour
reservation for the final audit itself; its subsequent successful completion
does not increase that upper bound. Subtracting the separate agent evaluation
forecast leaves 7,703.97 hours for all other unfinished CPU categories, before
their missing cost estimates. This remains a planning illustration, not full
study admission.

The behavioural recovery multiplicity is part of the frozen design: 1,600 truth
datasets, grouped folds, five starts and up to 500 Adam epochs. It cannot be removed
as a duplicate count. Conversely, single-clock timings include some full-cohort
prior and optimizer overhead, which must be separated and measured at actual fold
dimensions before applying any discount. Full optimizer stopping paths,
Hessian/MALA work, interval/recovery inference, Mantis and H2 workloads, allocation
overhead and full-fit output storage remain incompletely measured.

Stage B remains open. Only bounded, registered diagnosis proceeds; the full
scientific analysis has not been admitted within 20,000 CPU core-hours. A new
performance implementation requires prospective source qualification, numerical
regression and representative full-work measurements. Changes to models, folds,
clocks, starts, recovery replications, final seeds/ranks or scientific completion
rules require an explicit prospective scientific amendment and a fresh combined
budget. Budget limits and missing-provenance gates remain in force.
