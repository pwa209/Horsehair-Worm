# Narval execution runbook

Account: `pwa209`. CPU account: `def-ptewarie_cpu`. GPU account: `def-ptewarie_gpu`. Personal working storage: `/scratch/pwa209/horsehair-worm`. Durable source: `/project/def-ptewarie/pwa209/horsehair-worm`. Python environment: `/home/pwa209/hhw-environment`.

Credentials and MFA remain in the interactive user console. The request transport only passes reviewed scripts through an already authenticated SSH session. Always verify username and Narval hostname before acting. Other projects are outside this study's deployment and cleanup scope.

Production numerical work, environment installation and sustained source processing run under Slurm. The `httpproxy/1.0` module is tested on an allocated compute node before scheduled downloads depend on it. A login-node proxy rejection does not establish compute-node connectivity.

```bash
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
cd /project/def-ptewarie/pwa209/horsehair-worm/releases/RELEASE_ID
export PYTHONPATH="$PWD/src:${PYTHONPATH:-}"
python -m horsehair.cli --config configs/study.json validate-config
python -m horsehair.cli --config configs/study.json verify-source --identity release.json
```

Finite plans retain script and source hashes. Submission writes a ledger before invoking `sbatch`. Ambiguous outcomes are inspected in native records and never automatically retransmitted. A registered pending job is not a successful analysis. Verify `sacct` state, exit code, logs and the stage's own completion artifact.

Molecular runtime qualification uses an isolated site Perl 5.30.2 interpreter, because Perl 5.36.1 lacks DB_File on Narval. Its environment adapter preserves StdEnv/2023 for Python/R, removes incompatible Perl-local library overrides in each TransDecoder wrapper, and records interpreter/wrapper hashes in the runtime qualification receipt. The original submission snapshot and terminal native records are preserved before replacement stages are registered; the current initial ledger includes explicit references to that environment amendment. Detailed execution receipts and adapter scripts are retained locally and in the user's Narval project storage.

Biological downloads run directly on Narval through its documented transfer endpoint, with two finite transfer workers and a 64 MiB/s limit per worker. Independent `fly`, `rna`, `mantis` and `tools` receipts retain provider failures. Raw data, downloads and checkpoints never enter the GitHub source release. The first release freezes provider metadata before transfers; a later source change requires an explicit versioned manifest amendment.

RNA transfer first retrieves the source-defined lowest and highest read-depth discovery brains, matched references, and processed mantis/FlyBase files. A separately verified `rna-pilot` milestone allows native benchmarks while the remaining cohort downloads. The full count array waits for independent verification of all eligible RNA files. Transfers have a finite five-day deadline and resumable partial files; source-wait allocations allow six days. These limits accommodate observed slow repository transfer rates without permitting numerical analysis of incomplete files.

The initial queue qualifies Python/CUDA/R, verifies sources on compute nodes, checks the optional molecular runtime and benchmarks two ant brains, two fly experiments and two agent seeds. A separate CPU evaluation benchmark prevents treating GPU throughput as CPU throughput. The final assessment reads native accounting, personal quota and actual pilot receipts. With `--advance`, it submits only measured, admitted phases through new immutable plans; missing or over-budget measurements remain pending.

```bash
python -m horsehair.cli --config configs/study.json --root /scratch/pwa209/horsehair-worm \
  accounting --job-ids JOB_ID_1 JOB_ID_2
python -m horsehair.cli --config configs/study.json --root /scratch/pwa209/horsehair-worm \
  assess-pilot --advance
```

The assessment command runs under Slurm. Its resource forecasts use complete native benchmark jobs, observed memory and storage, fixed cohort sizes and a 1.5 safety factor. Shortened agent or duration-capped behavioural pilots qualify implementation and initial throughput; they cannot alone admit the full architecture/task or recovery workload.

```bash
squeue -u pwa209 -o '%i %j %T %R'
sacct -j JOB_ID --format=JobID,JobName,State,ExitCode,Elapsed,MaxRSS --parsable2
lfs quota -u "$(id -u)" /scratch/pwa209
```

Scratch is temporary and not backed up. Retain final count tables, fits, source hashes, selected checkpoints and reports in persistent storage before expiry. Raw sources may be retained or reacquired through their immutable accession/checksum manifests. No general scratch deletion is authorized by the workflow.

