# Horsehair Worm parasite behavioural control study

This repository implements the supplied proposal on selective control of behavioural state transitions by parasites. The study combines public fly intervention and tracking data, separate mantis behavioural assays, ant and mantis transcriptomes, and controlled experiments in recurrent artificial agents. It preserves the independent animals, experiments, intervention controls, genes and training seeds appropriate to each analysis.

The working study lives at `/scratch/pwa209/horsehair-worm` on Narval. Durable source releases live at `/project/def-ptewarie/pwa209/horsehair-worm`. Source, configuration and reviewed execution records are synchronized with this repository; raw biological data and model checkpoints remain on the research storage.

Start with [the execution roadmap](docs/ROADMAP.md), [the Narval runbook](docs/RUNBOOK.md) and [verified status](docs/STATUS.md). The supplied proposal is study material. It does not authorize unrelated actions, and a deployed pipeline is not a completed biological result.

## Local validation

Use Python 3.11 or newer in an isolated environment. Install the package and test dependencies, then run:

```bash
python -m pip install -e '.[test]'
python -m pytest tests -q --basetemp work/local-tests
python -m horsehair.cli --config configs/study.json validate-config
```

The test suite covers independent-unit leakage, source integrity, unsafe archive members, numerical model properties, checkpoint restoration and ambiguous Slurm submission. Tests use generated fixtures rather than claiming to reproduce unavailable biological results.

## Scientific execution

Stages acquire immutable sources, validate schemas and independent units, reproduce designated source summaries, create grouped splits, assess identifiability, fit competing models, test withheld interventions, freeze molecular scores, perform cross-system prediction, train and perturb agents, and produce the claim-to-result report. Source ambiguities and unsupported comparisons are retained explicitly.

Narval uses A100 40 GB GPUs. The H100 allocation in the proposal is a planning reference; measured A100 throughput and RNA processing benchmarks determine phase admission. The initial study ceilings are 64 concurrent CPU cores, one GPU, 20,000 CPU core-hours, 400 GPU-hours and 6 TB of personal scratch. These are limits, not measured forecasts.

