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

## Managed execution and advancement

The initial graph registers source waits/verification, Python/CUDA/R and tool qualification, source audit, fixed RNA pilots, primary orthology timing and complete architecture/task agent benchmarks. Agent training timing covers 12 task/architecture cells with two fixed pilot seeds each; CPU timing covers every configured perturbation, matching arm, probe and restoration condition. The finite benchmarks separate throughput from policy competence and confirmatory inference.

Four sequential controller segments each run for at most 140 hours, giving approximately 24 days of scheduled supervision. They reconcile native accounting, refresh personal scratch measurements and evaluate the entire phase graph. Later finite plans cover behavioural diagnostics/inference, independent interventions, ant molecular discovery/freeze, molecular transfer, separate mantis corroboration, agent development/freeze, final agent evaluation and synthesis. Scientific readiness and measured admission remain prerequisites for submission.

The controller verifies source/configuration and plan/ledger hashes, waits for every planned array member, checks combined prospective CPU/GPU capacity, and records missing inputs or rejected forecasts. Submission journals preserve a lost acknowledgement or partial submission for review. A native failed stage remains a repair condition. Controller-lock recovery requires native terminal proof and unchanged owner identity; its OS recovery mutex releases on process termination.

The study limits are 64 concurrent CPU cores, one GPU, 20,000 CPU core-hours, 400 GPU-hours and 6 TB of study scratch, with the configured personal reserve. Admission uses completed native job timing, peak memory, actual output bytes/file counts, complete workload coverage and a 1.5 safety factor. A shortened or duration-capped pilot alone cannot admit the full recovery, training or evaluation workload. An over-budget forecast requires an explicit amendment or finite work split.

The controller writes its release-specific status under `state/pipeline/RELEASE_ID/status.json`. Read that record together with native state, exit codes, logs and each stage's verified output. Queue registration, successful execution, scientific support and publication readiness are separate milestones. The four-segment window is finite; exhausting it preserves state for reviewed continuation.

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

The explicit `scripts/repair_transfer.py` maintenance interface runs in a network-enabled Slurm allocation. It accepts one checksum-failed eligible ant asset and a new recovery identity. A complete-length partial is independently hashed, compared with its unchanged provider checksum, and preserved with its sidecar and forensic receipt before a fresh transfer begins directly on Narval. Verified final sources are never quarantined or overwritten. A partial with different provenance, an active transfer guard, a valid partial, or an ambiguous recovery journal requires inspection rather than automatic replay.

Agent evaluation samples only active episodes, uses paired episode-specific action streams, and validates logits, recurrent states and clean-policy divergence. Genuine numerical failures retain planned episode/grid membership with null endpoints and explicit diagnostics. They cannot supply complete timing coverage, inferred effects or H4 support. Failed episodes and seeds are never imputed or discarded.

## Scientific source gates

The author-defined processed fly frame clock permits recording-relative chronology with its deposited validator and per-file metadata. Exact exposure progression additionally requires a source-verified exposure-to-recording offset for every exposed experiment. Missing unexposed controls, intervention identities, animal/reagent/gene/family crosswalks or shared-control covariance cannot be supplied from filenames or pooled summaries. These gates remain visible in controller status.

Independent mantis behavioural and RNA subjects remain separate. The computational agent primary protocol uses synthetic perturbations; behavioural model parameters do not identify signs of neural weight changes. Biological transfer requires a separately validated prospective bridge.

Deposited mantis endpoint disagreements remain in the canonical table and source audit. The declared parser amendment permits a conditional fit of mutually consistent annotations and separately labelled all-row recorded-result and location/window sensitivity fits. These diagnostics do not correct the source or establish an unqualified author replication; source reconciliation remains pending even if numerical fitting completes.

## Review and retention

```bash
squeue -u pwa209 -o '%i %j %T %R'
sacct -j JOB_ID --format=JobID,JobName,State,ExitCode,Elapsed,MaxRSS --parsable2
python -m horsehair.cli --config configs/study.json --root /scratch/pwa209/horsehair-worm \
  accounting --job-ids JOB_ID_1 JOB_ID_2
```

The managed controller performs accounting/admission itself. `assess-pilot --advance` remains the earlier assessment interface and is not a substitute for a registered managed controller. A pending job is not evidence that an analysis completed. A negative scientific result is valid when its required diagnostics are complete; missing diagnostics remain unresolved.

Scratch is temporary and not backed up. Retain final count tables, fits, source hashes, selected checkpoints and reports in persistent storage before expiry. Raw sources may be retained or reacquired through their immutable accession/checksum manifests. General scratch deletion is outside this workflow. Publication readiness also requires actual visual review of the six generated figures.
