#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-recovery-469
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=1:00:00
#SBATCH --output=/scratch/pwa209/horsehair-worm/logs/recovery-469-%A_%a.log
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
python -u /scratch/pwa209/horsehair-worm/maintenance/study-primary-external-census-policy-v1-20261023/run-primary-external-census-v1-20261023.py --mode qualify --policy-descriptor /scratch/pwa209/horsehair-worm/maintenance/study-primary-external-census-policy-v1-20261023/qualification-policy.json --policy-descriptor-sha256 b597cd76d14a668e35a36a8a9745e0f034fc47981e29e3ddab779e6e4f011d18 --native-plan /scratch/pwa209/horsehair-worm/plans/recovery-support-20261021-469/plan.json --native-ledger /scratch/pwa209/horsehair-worm/state/recovery-support-20261021-469-submission.json --native-task recovery-469 --native-script /scratch/pwa209/horsehair-worm/plans/recovery-support-20261021-469/recovery-469.sbatch --segment 0
