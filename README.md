# Horsehair Worm parasite behavioural control study

This repository implements the supplied proposal on selective control of behavioural state transitions by parasites. The study combines public fly intervention and tracking data, separate mantis behavioural assays, ant and mantis transcriptomes, and controlled experiments in recurrent artificial agents. It preserves the independent animals, experiments, intervention controls, genes and training seeds appropriate to each analysis.

The working study uses personal scratch at `/scratch/pwa209/horsehair-worm` on Narval. Durable source releases live at `/project/def-ptewarie/pwa209/horsehair-worm`. Public source, configuration, methods and readiness documentation are synchronized with this repository. Raw data, checkpoints, deployment receipts and live queue records remain in research storage and ignored local artifacts.

Start with [the execution roadmap](docs/ROADMAP.md), [the Narval runbook](docs/RUNBOOK.md) and [verified status](docs/STATUS.md). The supplied proposal is study material. It does not authorize unrelated actions, and a deployed pipeline is not a completed biological result.

## Local validation

Use Python 3.11 or newer in an isolated environment. Install the package and test dependencies, then run:

```bash
python -m pip install -e '.[test]'
python -m pytest tests -q --basetemp work/local-tests
python -m horsehair.cli --config configs/study.json validate-config
```

The test suite covers independent-unit leakage, source integrity, unsafe archive members, numerical model properties, exact checkpoint resume/restoration, complete benchmark grids, prospective CPU/GPU concurrency, array completion and ambiguous Slurm submissions. Controller tests include actual process termination and OS-lock recovery. Generated fixtures establish software behavior; scientific claims require the original observations or complete final agent experiments.

## Scientific execution

Stages acquire immutable sources, validate schemas and independent units, reproduce designated source summaries, create grouped splits, assess identifiability, fit competing models, test withheld interventions, freeze molecular scores, perform cross-system prediction, train and perturb agents, and produce the claim-to-result report. Source ambiguities and unsupported comparisons are retained explicitly.

The managed workflow packages a committed release, registers a finite initial graph and four sequential Slurm controller segments, and automatically submits later phases after their dependencies, source readiness and measured resource admission pass. The controller window is approximately 24 days. It covers behavioural and intervention inference, RNA/molecular discovery and transfer, mantis corroboration, agent development/final evaluation and synthesis. Deployment, queue registration, completed computation and scientific support have separate receipts.

Build the reviewed deployment request from a clean committed checkout:

```bash
python scripts/build_release.py --git git --request-id reviewed-managed-release \
  --managed --output work/requests
```

The generated request must execute through the authenticated Narval deployment workflow. `--queue` retains the earlier initial-plan workflow; `--managed` selects sustained phase advancement. Add `--transfers` only when starting the finite acquisition launcher. Updating source code does not restart an existing raw-data transfer: verified files are reused against the frozen manifest, and compatible partial files retain their transfer provenance.

Narval execution uses A100 40 GB qualification and measured throughput. The initial study ceilings are 64 concurrent CPU cores, one GPU, 20,000 CPU core-hours, 400 GPU-hours and 6 TB of personal scratch. All architecture/task cells require native training timings and complete CPU perturbation-grid timing. These ceilings bound execution; admission forecasts come from actual measurements.

The deposited fly processed clock supports recording-relative chronology. Primary exposure-modulated behaviour requires source-verified exposure offsets and original control metadata. Exposed survivors remain exposed. Independent interventions additionally require original animal, reagent, gene, shared-control and family mappings. Missing annotations remain pending. The agent primary experiment is computational; biological transfer requires a separate validated bridge protocol.

