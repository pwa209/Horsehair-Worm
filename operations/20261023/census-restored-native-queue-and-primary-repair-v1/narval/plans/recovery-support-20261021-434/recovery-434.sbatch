#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-recovery-434
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=1:00:00
#SBATCH --output=/scratch/pwa209/horsehair-worm/logs/recovery-434-%A_%a.log
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
python -u /scratch/pwa209/horsehair-worm/maintenance/study-af-resubmission-434-20261023/qualify-f2-archive-consumer-v3-20261023.py --overlay /scratch/pwa209/horsehair-worm/maintenance/study-af-resubmission-434-20261023/run-af-agent-controller-archive-v3-20261023.py --overlay-sha256 2004e959019a2fe564f58d1e5fce70ee988a3afb9e62ebc42b95488eaa9fdc02 --verifier /scratch/pwa209/horsehair-worm/maintenance/study-af-resubmission-434-20261023/verify-f2-condition-archives-v2-20261022.py --verifier-sha256 b02afa747f8e37a654dc0eda6f3a370808cdd381cf92a55a9b56ea9a8b6bfcae --base-controller /scratch/pwa209/horsehair-worm/maintenance/study-af-registry-repair-support-v6-20261022/run-af-agent-controller-registry-v6-20261022.py --base-controller-sha256 e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866 --overlay-tests /scratch/pwa209/horsehair-worm/maintenance/study-af-resubmission-434-20261023/test_af_agent_controller_archive_v3_20261023.py --verifier-tests /scratch/pwa209/horsehair-worm/maintenance/study-af-resubmission-434-20261023/test_verify_f2_condition_archives_v2_20261022.py --qualification-case /scratch/pwa209/horsehair-worm/maintenance/study-af-archive-consumer-qualification-support-v3-20261023 --native-plan /scratch/pwa209/horsehair-worm/plans/recovery-support-20261021-434/plan.json --native-ledger /scratch/pwa209/horsehair-worm/state/recovery-support-20261021-434-submission.json --native-script /scratch/pwa209/horsehair-worm/plans/recovery-support-20261021-434/recovery-434.sbatch --native-task recovery-434
