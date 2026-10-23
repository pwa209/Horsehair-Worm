#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-recovery-433
#SBATCH --cpus-per-task=1
#SBATCH --mem=64G
#SBATCH --time=8:00:00
#SBATCH --output=/scratch/pwa209/horsehair-worm/logs/recovery-433-%A_%a.log
set -euo pipefail
export PYTHONDONTWRITEBYTECODE=1
test "$(id -un)" = pwa209
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
cd /scratch/pwa209/horsehair-worm/releases/20261021-caebc9c1dc77-8495a3d98551
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-caebc9c1dc77-8495a3d98551/src:"${PYTHONPATH:-}"
export MPLBACKEND=Agg
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export PYTHONUNBUFFERED=1
export CUBLAS_WORKSPACE_CONFIG=:4096:8
export HHW_GPU_COUNT=0
export HHW_MAX_HOURS=8
python -m horsehair.cli --config configs/study.json --root /scratch/pwa209/horsehair-worm verify-source --identity release.json
python -u /scratch/pwa209/horsehair-worm/maintenance/study-af-resubmission-433-20261023/run-cohort-prefix-stage-v5-r1-20261022.py
