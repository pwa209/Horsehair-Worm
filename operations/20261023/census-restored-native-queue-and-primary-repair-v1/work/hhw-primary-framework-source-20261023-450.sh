#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import os,json,hashlib
assert os.geteuid()==3164768
f=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261021-57e0eb8c4c6c-d89b31180d4c')
o={'checked_utc':datetime.now(timezone.utc).isoformat(),'read_only':True,'framework':str(f),'files':{}}
for n in ['release.json','src/horsehair/pipeline.py','src/horsehair/budget.py','src/horsehair/cli.py']:
 p=f/n;assert not p.is_symlink();raw=p.read_bytes();assert len(raw)<300000;o['files'][n]={'sha256':hashlib.sha256(raw).hexdigest(),'text':raw.decode()}
print('PRIMARY_FRAMEWORK_SOURCE '+json.dumps(o,sort_keys=True),flush=True)
PY
