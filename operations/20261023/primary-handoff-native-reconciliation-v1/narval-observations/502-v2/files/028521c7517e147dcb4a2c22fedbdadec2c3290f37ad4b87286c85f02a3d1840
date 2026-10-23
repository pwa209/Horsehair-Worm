#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-af-f2-registry-r2-v1-controller-1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=96:00:00
#SBATCH --output=/scratch/pwa209/horsehair-worm/logs/af-f2-registry-r2-v1-controller-1-%A_%a.log
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
/home/pwa209/hhw-environment/bin/python -s -u /scratch/pwa209/horsehair-worm/maintenance/study-af-registry-repair-support-v6-20261022/run-af-agent-controller-registry-v6-20261022.py --descriptor /scratch/pwa209/horsehair-worm/maintenance/study-af-registry-repair-support-v6-20261022/native-activation-prestart-three-v1/descriptor-prestart-three-v1.json --descriptor-sha256 9a0706642ac1660fb7f39fe68279ca34bdc10f89812a0c2397608d2a382cf464 --segment 1
