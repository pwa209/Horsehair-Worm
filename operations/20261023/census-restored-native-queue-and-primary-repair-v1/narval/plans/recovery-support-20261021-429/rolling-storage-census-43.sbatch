#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-rolling-storage-census-43
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=6:00:00
#SBATCH --output=/scratch/pwa209/horsehair-worm/logs/rolling-storage-census-43-%A_%a.log
#SBATCH --hold
#SBATCH --begin=2026-11-02T21:58:04
set -euo pipefail
export PYTHONDONTWRITEBYTECODE=1
test "$(id -un)" = pwa209
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
cd /scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src:"${PYTHONPATH:-}"
export MPLBACKEND=Agg
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export PYTHONUNBUFFERED=1
export CUBLAS_WORKSPACE_CONFIG=:4096:8
export HHW_GPU_COUNT=0
export HHW_MAX_HOURS=6
python -m horsehair.cli --config configs/study.json --root /scratch/pwa209/horsehair-worm verify-source --identity release.json
python -u /scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-support-v4-20261023/run-fresh-storage-census-v6-20261023.py --native-plan /scratch/pwa209/horsehair-worm/plans/recovery-support-20261021-429/plan.json --native-ledger /scratch/pwa209/horsehair-worm/state/recovery-support-20261021-429-submission.json --native-task rolling-storage-census-43 --native-script /scratch/pwa209/horsehair-worm/plans/recovery-support-20261021-429/rolling-storage-census-43.sbatch --case-id rolling-43 --source-manifest /scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-support-v4-20261023/source-manifest.json --source-manifest-sha256 1b21742f6ad5b7b48899a4890988da7ddba439798ba8b12a2bad57c7f1898e6e --kernel-qualification /scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-qualification-v5-20261023/qualification.json --kernel-qualification-sha256 8b0dc612cee64461b991a9dfb9a975cf5afe1389a037b1357244d829f5aca76c --kernel-qualification-job-id 6863966 --registry-qualification /scratch/pwa209/horsehair-worm/maintenance/study-af-registry-repair-support-v4-20261021/private-qualification.json --registry-qualification-sha256 1f391caccd4629eb47150862f7bc2575e991e4ce5cb5aae99e955cdeae4510ca --registry-qualification-job-id 6699561
