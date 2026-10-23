#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-recovery-470
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=1:00:00
#SBATCH --output=/scratch/pwa209/horsehair-worm/logs/recovery-470-%A_%a.log
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
export HHW_MAX_HOURS=1
python -m horsehair.cli --config configs/study.json --root /scratch/pwa209/horsehair-worm verify-source --identity release.json
python -u /scratch/pwa209/horsehair-worm/maintenance/study-primary-external-census-handoff-v1-20261023/handoff-primary-external-census-v1-20261023.py --native-plan /scratch/pwa209/horsehair-worm/plans/recovery-support-20261021-470/plan.json --native-ledger /scratch/pwa209/horsehair-worm/state/recovery-support-20261021-470-submission.json --native-script /scratch/pwa209/horsehair-worm/plans/recovery-support-20261021-470/recovery-470.sbatch --native-task recovery-470 --source-manifest /scratch/pwa209/horsehair-worm/maintenance/study-primary-external-census-handoff-v1-20261023/source-manifest.json --source-manifest-sha256 4159f3d3224b106773e4fea8704c1cece39460f27b1acf79130c6dfc94092444
