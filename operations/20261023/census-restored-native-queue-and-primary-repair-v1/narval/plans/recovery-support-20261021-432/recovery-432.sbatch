#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-recovery-432
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=4:00:00
#SBATCH --output=/scratch/pwa209/horsehair-worm/logs/recovery-432-%A_%a.log
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
export HHW_MAX_HOURS=4
python -m horsehair.cli --config configs/study.json --root /scratch/pwa209/horsehair-worm verify-source --identity release.json
python -u -c 'from pathlib import Path
import math,os,runpy,time
from horsehair.core import digest,read_json,source_identity,canonical_hash
root=Path('"'"'/scratch/pwa209/horsehair-worm'"'"')
framework=root/'"'"'releases/20261021-6ed201653dfa-4f3903d7c26e'"'"'
primary=Path('"'"'/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf'"'"')
area=root/'"'"'maintenance/study-af-resubmission-432-20261023'"'"'
end=float(os.environ['"'"'SLURM_JOB_END_TIME'"'"']);assert math.isfinite(end) and time.time()+300<end
assert source_identity(framework)['"'"'sha256'"'"']=='"'"'4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'"'"'
assert source_identity(primary)['"'"'sha256'"'"']=='"'"'0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'"'"'
config=read_json(primary/'"'"'configs/study.json'"'"');assert canonical_hash(config)=='"'"'446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'"'"' and read_json(framework/'"'"'configs/study.json'"'"')==config
gp=root/'"'"'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'"'"';assert digest(gp)=='"'"'358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'"'"'
runpy.run_path(str(gp))['"'"'install'"'"'](area/'"'"'native-start-accounting-reads'"'"')
guard=root/'"'"'maintenance/study-af-registry-repair-support-v6-20261022/administration-registry-read-guard-v4-20261022.py'"'"';assert digest(guard)=='"'"'5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'"'"'
api=runpy.run_path(str(guard));api['"'"'install'"'"'](area/'"'"'native-start-registry-reads'"'"',registration_mode='"'"'controller-already-locked'"'"',allocation_deadline_unix=end)
from horsehair import campaign,repair
assert campaign._repair_native_states is repair._repair_native_states
policy=(root/'"'"'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'"'"','"'"'f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'"'"')
assert digest(policy[0])==policy[1]
with api['"'"'registration_lock'"'"'](area/'"'"'native-start-registration-lock'"'"',end,role='"'"'controller-start'"'"'):
    campaign.combined_admission(root,config,read_json(primary/'"'"'release.json'"'"')['"'"'release_id'"'"'],area,'"'"'native-start-before-frozen-activation'"'"',resource_policy=policy,cpus=0,gpus=0,hours=4)
'
python -u /scratch/pwa209/horsehair-worm/maintenance/study-af-registry-repair-support-v6-20261022/activate-f2-prestart-three-v1-20261022.py --native-plan /scratch/pwa209/horsehair-worm/plans/recovery-support-20261021-432/plan.json --native-ledger /scratch/pwa209/horsehair-worm/state/recovery-support-20261021-432-submission.json --native-task recovery-432 --native-script /scratch/pwa209/horsehair-worm/plans/recovery-support-20261021-432/recovery-432.sbatch --source-manifest /scratch/pwa209/horsehair-worm/maintenance/study-af-registry-repair-support-v6-20261022/source-manifest.json --source-manifest-sha256 4b09442dfa29fac9f3b442e9793a0476035b174577bf67f316f1afcd9b9091f4 --qualification-job-id 6865525 --approval-reference 'Existing user authorization to repair and continue the computational study, 22 October 2026.'
