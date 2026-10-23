#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-af-f2-external-census-v1-controller-2
#SBATCH --cpus-per-task=1
#SBATCH --mem=64G
#SBATCH --time=96:00:00
#SBATCH --output=/scratch/pwa209/horsehair-worm/logs/af-f2-external-census-v1-controller-2-%A_%a.log
set -euo pipefail
export TZ=UTC
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
export HHW_MAX_HOURS=96
python -m horsehair.cli --config configs/study.json --root /scratch/pwa209/horsehair-worm verify-source --identity release.json
/home/pwa209/hhw-environment/bin/python -s -u /scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-support-v4-20261023/run-af-agent-controller-census-policy-v1-20261023.py --policy-descriptor /scratch/pwa209/horsehair-worm/maintenance/study-af-external-census-handoff-v1-20261023/native-6865706/external-census-policy.json --policy-descriptor-sha256 2b0e6ebaa0f1b511bca4343d4722d43702cf051f956cce59494ddccae656c517 --segment 2 --native-plan /scratch/pwa209/horsehair-worm/plans/study-af-f2-external-census-controllers-v1-20261023/plan.json --native-ledger /scratch/pwa209/horsehair-worm/state/study-af-f2-external-census-controllers-v1-20261023-submission.json --native-task af-f2-external-census-v1-controller-2 --native-script /scratch/pwa209/horsehair-worm/plans/study-af-f2-external-census-controllers-v1-20261023/af-f2-external-census-v1-controller-2.sbatch
