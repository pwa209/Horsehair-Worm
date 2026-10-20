# Narval execution runbook

Account: `pwa209`. CPU account: `def-ptewarie_cpu`. GPU account: `def-ptewarie_gpu`. Personal working storage: `/scratch/pwa209/horsehair-worm`. Durable source: `/project/def-ptewarie/pwa209/horsehair-worm`. Python environment: `/home/pwa209/hhw-environment`.

Credentials and MFA remain in the interactive user console. Reviewed deployment requests use an already authenticated SSH transport. Verify the username and Narval hostname before execution. Deployment receipts, native job identities, queue snapshots and detailed execution logs remain in ignored local artifacts and research storage; this public runbook describes the workflow.

## Freeze and deploy source

Commit the reviewed public checkout before packaging. The builder rejects uncommitted files, private/generated tracked artifacts and an existing immutable release destination.

```bash
python scripts/build_release.py --git git --request-id reviewed-managed-release \
  --managed --output work/requests
```

`--managed` builds the managed initial queue, including four sequential finite controller segments. `--queue` builds the earlier initial qualification/pilot plan with a single assessment; it does not provide the complete sustained controller graph. The flags are mutually exclusive. Omitting both packages/deploys source without queue submission. The builder generates an ignored request; execute it through the authenticated Narval transport and inspect its deployment/submission receipt before declaring queue registration successful.

For a fresh acquisition, `--transfers` starts the finite two-worker transfer launcher. Leave it off while an existing transfer continues or verified sources already exist. A code release update does not require a new raw-data download. Complete files are reverified against the unchanged provider manifest; compatible partial files resume with their asset signature and server range support. Changed source definitions require an explicit versioned manifest amendment, and mismatched partial provenance stops the transfer.

On the released checkout:

```bash
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
cd /project/def-ptewarie/pwa209/horsehair-worm/releases/RELEASE_ID
export PYTHONPATH="$PWD/src:${PYTHONPATH:-}"
python -m horsehair.cli --config configs/study.json validate-config
python -m horsehair.cli --config configs/study.json verify-source --identity release.json
```

Environment installation, numerical analysis and sustained processing run under Slurm. Qualification checks Python/CUDA/R and the molecular tools on allocated nodes. TransDecoder's isolated Perl/DB_File adapter retains its interpreter/wrapper hashes and must pass its native example before use. Compute-node network access is qualified independently of login-node access.

CPU regression fixtures and the pytest cache run in a unique directory under the allocation's `SLURM_TMPDIR`. This avoids repeated fixture `fsync` calls on Lustre. The full test suite remains mandatory. Test progress goes to the Slurm log; complete output, JUnit results and source/configuration/job-bound qualification evidence are preserved in personal study storage. The test subprocess has a finite deadline with time reserved for evidence preservation. A historical qualification receipt cannot establish that a revised release passed.

## Managed execution and advancement

The initial graph registers source waits/verification, Python/CUDA/R and tool qualification, source audit, fixed RNA pilots, primary orthology timing and complete architecture/task agent benchmarks. Agent training timing covers 12 task/architecture cells with two fixed pilot seeds each; CPU timing covers every configured perturbation, matching arm, probe and restoration condition. The finite benchmarks separate throughput from policy competence and confirmatory inference.

Four sequential controller segments each run for at most 140 hours, giving approximately 24 days of scheduled supervision. They reconcile native accounting, refresh personal scratch measurements and evaluate the entire phase graph. Later finite plans cover behavioural diagnostics/inference, independent interventions, ant molecular discovery/freeze, molecular transfer, separate mantis corroboration, agent development/freeze, final agent evaluation and synthesis. Scientific readiness and measured admission remain prerequisites for submission.

The controller verifies source/configuration and plan/ledger hashes, waits for every planned array member, checks combined prospective CPU/GPU capacity, and records missing inputs or rejected forecasts. Submission journals preserve a lost acknowledgement or partial submission for review. A native failed stage remains a repair condition. Controller-lock recovery requires native terminal proof and unchanged owner identity; its OS recovery mutex releases on process termination.

The study limits are 64 concurrent CPU cores, one GPU, 20,000 CPU core-hours, 400 GPU-hours and 6 TB of study scratch, with the configured personal reserve. Admission uses completed native job timing, peak memory, actual output bytes/file counts, complete workload coverage and a 1.5 safety factor. A shortened or duration-capped pilot alone cannot admit the full recovery, training or evaluation workload. An over-budget forecast requires an explicit amendment or finite work split.

The controller writes its release-specific status under `state/pipeline/RELEASE_ID/status.json`. Read that record together with native state, exit codes, logs and each stage's verified output. Queue registration, successful execution, scientific support and publication readiness are separate milestones. The four-segment window is finite; exhausting it preserves state for reviewed continuation.

A separately qualified maintenance controller retains the original `--primary-release` argument. Source-sensitive admission and plan creation execute in an isolated interpreter using that verified primary package, including its own benchmark helpers and planner checks. Returned results must match the primary source, full configuration and saved receipt. The controller records the context proof under its primary pipeline namespace. Repeated maintenance preserves the single existing argument; it does not append duplicate primary-release flags or relabel historical benchmarks as new-source evidence.

## Stage interfaces

Generated plans supply allocations, dependencies and array indices. These are the corresponding CLI interfaces, executed from the release with `--config configs/study.json --root /scratch/pwa209/horsehair-worm`:

| Work | CLI suffix |
|---|---|
| Source audit/verification | `source-audit`; `verify-sources --group fly` or `rna`/`mantis`/`tools` |
| RNA pilot | `rna-pilot --prepare-only`; `rna-pilot --index 0` or `1` |
| Full ant RNA/count phases | `molecular --stage prepare-rna-full`; `molecular --stage rna-run --task-index INDEX`; `molecular --stage ant` |
| Behavioural analysis | `behaviour --stage fit`, `recover`, `posterior-checks`, `bayesian-check`, `freeze-development`, `mantis`, `export-parameters` or `inference` |
| Independent interventions | `interventions --stage readiness`, `screen`, `challenge` or `inference` |
| Molecular work | `molecular-pipeline --stage benchmark-primary`, `freeze`, `benchmark-transfer`, `predict`, `mantis-orfs` or `mantis-orthology`; `molecular --stage mantis` |
| Agent timing arrays | `agents --mode benchmark-training --task-index INDEX`; `agents --mode benchmark-cpu-evaluation --task-index INDEX` |
| Agent timing aggregation | `agents --mode benchmark-aggregate` |
| Scientific agent stages | `agents --mode development --task-index INDEX`; `agents --mode freeze`; `agents --mode final --task-index INDEX`; `agents --mode evaluate --task-index INDEX`; `agent-analysis` |
| Report | `report` |
| Controller segment | `pipeline --segment INDEX --max-seconds 504000 --interval 60` |

Agent benchmark indices are 0–11, development seed-cell indices 0–59 (three learning rates per cell, 180 fits), and final/evaluation policy indices 0–239. RNA count indices are 0–59. Controller segment indices are 0–3. The full configuration and frozen protocol bind all scientific stages; source-changing repairs require fresh matching benchmark evidence and an appropriate release/output amendment.

## Repair and fresh evidence

Preserve the previous immutable release, its native receipts, logs and checkpoints before superseding a failed controller. Cancel only positively identified study controllers; terminal accounting must reconcile their leases before a replacement graph starts. Array evidence uses Slurm's logical `JobID` (parent and index) and retains numeric `JobIDRaw` as allocation provenance. Historical raw-key costs are migrated only from matching native allocation evidence, so a finished array member is charged once. Live receipt-lock contention defers a bounded controller tick; it never authorizes removing a kernel guard.

Code repairs require fresh source/configuration-bound benchmarks. Set `agents.output_dir` to a new relative result namespace to preserve earlier policy measurements. RNA processing and molecular-runtime namespaces likewise preserve earlier outputs; reuse of the unchanged reference index requires its pinned reference proof.

Lease completion bookkeeping writes a separate durable finalization intent before waiting for the shared budget guard. Lock acquisition retries are bounded by 120 seconds and the native allocation deadline. Exhausted contention preserves the study command's original outcome and retains the reservation until positive terminal Slurm accounting reconciles it. The intent records elapsed time and any original exception; it never releases a reservation or substitutes estimated costs for native allocation evidence.

Reservation entry retries live guard contention for at most 300 seconds, shortened by the allocation deadline with 120 seconds reserved for completion bookkeeping. Computation starts only after a durable reservation passes the concurrency and cost checks. Cap or identity refusals fail immediately. Informational lock-owner metadata is atomically replaced without forcing a disk sync; the permanent kernel guard and scientific ledger writes retain their locking and durability requirements. Identical native accounting snapshots do not rewrite the ledger or accounting table.

The explicit `scripts/repair_transfer.py` maintenance interface hashes and quarantines in Slurm. It accepts one checksum-failed eligible ant asset and a new recovery identity. A complete-length partial is independently hashed, compared with its unchanged provider checksum, and preserved with its sidecar and forensic receipt. Resume `scripts/transfer_sources.py --group rna` as a finite file transfer on the separately qualified Narval transfer endpoint; compute-proxy access to ENA is not assumed. Verified final sources are never quarantined or overwritten. A partial with different provenance, an active transfer guard, a valid partial, or an ambiguous recovery journal requires inspection rather than automatic replay.

Resumed downloads validate the complete byte-range response and the returned object validators before appending. Nonidentity content encoding is rejected because downloaded file offsets must refer to the original provider bytes. Range/body-length checks supplement the unchanged provider checksum; passing an HTTP check alone never verifies a source file.

The finite `scripts/transfer_rna_ftp.py` recovery launcher can use ENA's public FTP route for manifest-defined FASTQs while retaining their canonical source identities and provider checksums. It requires a source-bound native preservation receipt and actual-host proof that the previous worker ended. FTP size, modification time, transfer bytes and stream digests are checked, then the saved file is independently verified before promotion. Compatible FTP partials retain their own transport provenance; invalid complete partials require separate native preservation. There is no automatic quarantine loop. The launch journal, worker code and primary verification release are immutable, and only the owning worker may release its transfer guard. [ENA documents its public FTP routes and checksum checks](https://www.ebi.ac.uk/ena/browser/downloading-data).

For a quarantined checksum failure, an explicit preservation case may select `case_transport: canonical_https`. Legacy cases apply this route only to the preserved asset. After repeated FTP checksum failures, a new native preparation may bind `fastq_transport_policy` using `fastq_https_policy`: the policy covers every selected FASTQ and binds the complete roster, source, configuration, manifest, maintenance script and preparation allocation. Existing cases are not modified or silently upgraded. The new policy routes all remaining FASTQs and support sources through canonical HTTPS, and audits future destinations for incompatible partial provenance before launch. The case requires a direct identity-encoded range probe, unchanged provider pins, positive native preservation and the verified failed-worker release. The unchanged primary HTTP acquisition algorithm receives isolated verification/promotion hooks: stream digests must match a separate disk read and the provider checksum before exclusive promotion. Retries cover bounded transport failures only; checkpointed prefixes and object validators must still match. Redirects and a full-body response to a nonzero resume range are refused. A provider checksum mismatch remains fatal and preserves its evidence.

Agent development completion does not waive final evaluation admission. A benchmark grid with explicit unavailable numerical outcomes remains pending even when every Slurm member exited successfully. Preserve those outcomes and their calibration/perturbation identities; repeating deterministic failed cells or substituting finite observations does not establish complete numerical cost coverage. Changes to perturbation numerics or the admission method require a prospective, versioned amendment and fresh matching evidence.

For an interrupted valid FTP prefix, run `--prepare-resume` in a bounded compute allocation with the immutable primary release, a new personal maintenance case, the actual-host provider/worker proof, and the terminal canonical RNA wait job. The native proof independently hashes the existing prefix, checks its unchanged sidecar and provider representation, and binds the failed worker's complete immutable release. Launch only after that proof job is positively completed. This route preserves and resumes valid bytes rather than quarantining them. The transfer permits at most six transient network attempts per asset with bounded backoff under the unchanged 120-hour cutoff. Every reconnect rechecks the prefix; integrity, permanent provider and local storage failures stop and retain diagnostics.

An unreadable shared software link on a transfer host may be bypassed by materializing the same installed HTTP package versions from a healthy Narval node into a new personal project `maintenance-runtime` directory. Preserve package resources and distribution metadata, exclude bytecode and symlinks, and bind every file digest and version in `runtime-manifest.json`. Native preparation may record its exact directory and manifest SHA256 as `transport_runtime_overlay`; preparation, launch and worker validate that manifest and imported module origins. Invoke preparation and launch with the overlay on `PYTHONPATH` and `PYTHONDONTWRITEBYTECODE=1`. The launcher explicitly carries that binding into its child. This scoped runtime does not modify the installed study environment or supply evidence that another shared filesystem component is healthy.

If a transfer host's ordinary `ps -u` stalls, first diagnose the exact command. A system process utility may read unrelated users' command lines before applying its owner filter. An explicitly pinned launch adapter may instead enumerate effective UIDs from `/proc/PID/status`, bind every owned PID's start time, and obtain the real complete owner inventory with `/usr/bin/ps --quick-pid`. Preserve the actual command/output, before/after UID and start-time snapshots, and evidence that every still-live owned PID is covered. Unknown ownership, PID reuse, incomplete output or an unsettled roster must fail within the original twenty-second bound. Adapt only the exact process-inventory call; retain actual native accounting, qualification, worker code and launch. Known-PID progress checks should use direct `/proc` reads or quick-PID mode rather than a global scan. Generated adapters and their hashes belong with the private maintenance records.

A parent launch timeout can occur after its worker was spawned and before the parent finished its acknowledgement. Inspect the original restart journal, current source-bound transfer state, matching guard and actual-host PID/command/cwd before any next action. A `starting` parent receipt does not prove that no worker exists. Preserve an append-only reconciliation when a worker is positively running; never replay an ambiguous launch or rewrite the original journal to manufacture an acknowledgement.

Run recovery management in bounded Slurm allocations with positive native completion of their prerequisites. Collect actual-host worker proof, binding the qualified source, native-ready case, ownership guard and exact PID/command/cwd. Register the restored canonical RNA graph before its replacement controllers. New v3 recovery cases keep the RNA root explicitly held while qualified controllers operate. A finite Slurm completion coordinator independently verifies the entire frozen acquisition and rechecks combined resource admission before releasing that root. Transfer failure therefore leaves the downstream graph held for repair. Preserve each scheduler intent and acknowledgement, and inspect interrupted or ambiguous outcomes without replaying an existing intent. Retained older cases keep their own recorded handoff policy.

The explicit v3 FASTQ policy binds Narval3 and the system curl binary/version. It downloads fixed byte ranges into private staging, validates identity encoding, exact ranges, validators and lengths before appending, and preserves failed attempts. Known embedded connection-error XML is rejected before a segment is committed. Bounded network retries resume only from an independently checked committed prefix; a full-file checksum mismatch starts a separate fresh candidate. Independent disk MD5/SHA-256 must agree with the stream and frozen provider checksum before exclusive promotion. Older FTP/Requests partials require explicit native preservation rather than automatic adoption. Source pins and scientific methods remain unchanged.

A reconstructed source requires positive native evidence: immutable rejected originals, independently matching official replacement ranges, exact complete provider MD5, independent disk hashes, and successful full gzip validation. Its separate native restoration proof binds the final inode and qualified maintenance source. Small range agreement alone cannot establish a valid complete source.

The experimental independent-animal duration batch kernel is qualified separately from production fitting. Its probes preserve every native clock, age support and random-effect identity; numerical equivalence and native memory/timing measurements are prerequisites for any later implementation amendment. Frozen workload diagnostics retain all model, start, fold, optimizer, recovery and comparator multiplicities. A diagnostic projection or successful probe does not constitute phase admission.

Chronological ingestion computes the three configured bin widths together per ROI using an ingestion-only helper, with exact comparison against the unchanged numerical reference. A timed-out conversion's partial outputs remain ineligible for reuse. A source-audit runtime amendment must declare the old and new finite bounds, prove the original allocation timed out, match both Slurm and lease bounds, and preserve every other resource and command setting. The replacement audit stays held until the amended registered workload passes budget admission and both canonical plan/ledger pairs are committed. Numerical phases continue to execute their original immutable release.

Agent evaluation samples only active episodes, uses paired episode-specific action streams, and validates logits, recurrent states and clean-policy divergence. Genuine numerical failures retain planned episode/grid membership with null endpoints and explicit diagnostics. They cannot supply complete timing coverage, inferred effects or H4 support. Failed episodes and seeds are never imputed or discarded.

## Scientific source gates

After partial RNA array failure, the operational recovery guard in `horsehair.rna_recovery` can preserve positively completed count tasks while replaying the unchanged primary CLI for failed tasks. A native capture binds the exact original cohort, source, full configuration, per-brain recipes, native outcomes and successful QC/count artifacts. A separate native preparation moves failed output directories into a new immutable case without changing successful outputs. The full replacement array validates reused tasks and emits separate recovery receipts; it never relabels an original scientific receipt. Canonical registration requires a fully qualified maintenance framework, positive capture/preparation completion, unchanged primary execution/resources/dependencies and a held replacement count array. Register RNA before replacing the four terminal controllers, then reconcile the combined allocation budget before release.

Personal scratch inode capacity is checked independently of free bytes. The maintenance controller refreshes the personal UID quota on each loop and immediately before phase submission; RNA recovery refreshes it before every replay. Guards retain the existing study file ceiling and byte reserve, use the lower positive native soft/hard limit, and pause scheduling when headroom is insufficient. A fixed, serialized receipt avoids growing a new file per poll. These point-in-time checks do not reserve capacity against other projects. No cross-project cleanup or scientific gate waiver is implied.

The storage census measures archived curl stdout/stderr files and qualification text fixtures through exact EOF when full Lustre size attributes stall. This route opens directories and files without following symlinks, verifies native ownership and identity with metadata-only Linux statx, and refuses a changed identity, an exceeded read ceiling or an I/O error. It records actual byte counts, hashes and observable-race checks in one fixed artifact receipt, which the cached storage measurement validates. File contents are not logged. Other study files retain their ordinary logical-size measurement; all selected regular files still contribute to the study byte and file totals. Neither cached unrequested statx sizes nor physical quota is substituted for logical bytes.

The author-defined processed fly frame clock permits recording-relative chronology with its deposited validator and per-file metadata. Exact exposure progression additionally requires a source-verified exposure-to-recording offset for every exposed experiment. Missing unexposed controls, intervention identities, animal/reagent/gene/family crosswalks or shared-control covariance cannot be supplied from filenames or pooled summaries. These gates remain visible in controller status.

Independent mantis behavioural and RNA subjects remain separate. The computational agent primary protocol uses synthetic perturbations; behavioural model parameters do not identify signs of neural weight changes. Biological transfer requires a separately validated prospective bridge.

Deposited mantis endpoint disagreements remain in the canonical table and source audit. The declared parser amendment permits a conditional fit of mutually consistent annotations and separately labelled all-row recorded-result and location/window sensitivity fits. These diagnostics do not correct the source or establish an unqualified author replication; source reconciliation remains pending even if numerical fitting completes.

Primary H1–H4 completion and required auxiliary-assay readiness are recorded separately. Whole-study analysis and publication readiness require both, including source-matched mantis model, predictive-validation, coding and independence evidence. A completed mantis Slurm job alone establishes neither numerical model completion nor scientific readiness.

## Review and retention

```bash
squeue -u pwa209 -o '%i %j %T %R'
sacct -j JOB_ID --format=JobID,JobName,State,ExitCode,Elapsed,MaxRSS --parsable2
python -m horsehair.cli --config configs/study.json --root /scratch/pwa209/horsehair-worm \
  accounting --job-ids JOB_ID_1 JOB_ID_2
```

The managed controller performs accounting/admission itself. `assess-pilot --advance` remains the earlier assessment interface and is not a substitute for a registered managed controller. A pending job is not evidence that an analysis completed. A negative scientific result is valid when its required diagnostics are complete; missing diagnostics remain unresolved.

Scratch is temporary and not backed up. Retain final count tables, fits, source hashes, selected checkpoints and reports in persistent storage before expiry. Raw sources may be retained or reacquired through their immutable accession/checksum manifests. General scratch deletion is outside this workflow. Publication readiness also requires actual visual review of the six generated figures.
