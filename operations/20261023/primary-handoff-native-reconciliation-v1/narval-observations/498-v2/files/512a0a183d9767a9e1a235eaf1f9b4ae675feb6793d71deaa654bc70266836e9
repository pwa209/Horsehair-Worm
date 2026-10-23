#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-primary-external-census-controller-0
#SBATCH --cpus-per-task=1
#SBATCH --mem=64G
#SBATCH --time=72:00:00
#SBATCH --output=/scratch/pwa209/horsehair-worm/logs/primary-external-census-controller-0-%A_%a.log
#SBATCH --hold
set -euo pipefail
export TZ=UTC
export PYTHONDONTWRITEBYTECODE=1
test "$(id -un)" = pwa209
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
cd /lustre06/project/6101854/pwa209/horsehair-worm/releases/20261021-57e0eb8c4c6c-d89b31180d4c
export PYTHONPATH=/lustre06/project/6101854/pwa209/horsehair-worm/releases/20261021-57e0eb8c4c6c-d89b31180d4c/src:"${PYTHONPATH:-}"
export MPLBACKEND=Agg
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export PYTHONUNBUFFERED=1
export CUBLAS_WORKSPACE_CONFIG=:4096:8
export HHW_GPU_COUNT=0
export HHW_MAX_HOURS=72
python -m horsehair.cli --config configs/study.json --root /scratch/pwa209/horsehair-worm verify-source --identity release.json
/home/pwa209/hhw-environment/bin/python -s -u /scratch/pwa209/horsehair-worm/maintenance/study-primary-external-census-policy-v1-20261023/run-primary-external-census-v1-20261023.py --mode run --policy-descriptor /scratch/pwa209/horsehair-worm/maintenance/study-primary-external-census-handoff-v2-20261023/native-6868176/run-policy.json --policy-descriptor-sha256 ad70a140cdd50384f860cb4adc2dc4fc38feb0a7eaa60be2403456373c9b2267 --native-plan /scratch/pwa209/horsehair-worm/plans/primary-external-census-controllers-v1/plan.json --native-ledger /scratch/pwa209/horsehair-worm/state/primary-external-census-controllers-v1-submission.json --native-task primary-external-census-controller-0 --native-script /scratch/pwa209/horsehair-worm/plans/primary-external-census-controllers-v1/primary-external-census-controller-0.sbatch --segment 0
