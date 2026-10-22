#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
module load StdEnv/2023 python/3.11.5
source /home/pwa209/hhw-environment/bin/activate
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src
export PYTHONDONTWRITEBYTECODE=1
python -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import os,json,subprocess,hashlib
assert os.geteuid()==3164768
root=Path('/scratch/pwa209/horsehair-worm');out={'read_only':True,'host':subprocess.check_output(['hostname','-f'],text=True).strip(),'checked_utc':datetime.now(timezone.utc).isoformat(),'files':{},'identities':{},'publication':{}}
for relative in ['releases/20261021-6ed201653dfa-4f3903d7c26e/tests/test_slurm.py','plans/recovery-support-20261020-527/plan.json']:
 try:
  p=root/relative;raw=p.read_bytes();out['files'][relative]={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
 except Exception as e:out['files'][relative]={'error_type':type(e).__name__,'errno':getattr(e,'errno',None),'error':str(e)}
from horsehair.core import source_identity
for path,expected in [(root/'releases/20261021-6ed201653dfa-4f3903d7c26e','4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'),(Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf'),'0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630')]:
 try:
  value=source_identity(path);out['identities'][str(path)]={'sha256':value['sha256'],'files':len(value['files']),'matches':value['sha256']==expected}
 except Exception as e:out['identities'][str(path)]={'error_type':type(e).__name__,'errno':getattr(e,'errno',None),'error':str(e)}
for relative in ['maintenance/study-af-registry-repair-support-v6-20261022','maintenance/study-af-prestart-three-activation-stage-v1-20261022','plans/recovery-support-20261021-384','state/recovery-support-20261021-384-submission.json','plans/recovery-support-20261021-385','state/recovery-support-20261021-385-submission.json']:
 try:
  p=root/relative;out['publication'][relative]={'exists':p.exists(),'is_symlink':p.is_symlink()}
 except Exception as e:out['publication'][relative]={'error_type':type(e).__name__,'errno':getattr(e,'errno',None)}
print('SOURCE_HEALTH '+json.dumps(out,sort_keys=True),flush=True)
PY
