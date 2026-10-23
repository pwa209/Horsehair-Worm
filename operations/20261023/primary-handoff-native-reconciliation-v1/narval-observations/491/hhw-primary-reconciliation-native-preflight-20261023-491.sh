#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
import json,subprocess,os,time
from datetime import datetime,timezone
assert os.geteuid()==3164768
os.environ['TZ']='UTC';time.tzset()
ids=['6868176','6868884','6868886','6868888','6868889','6632164','6632209','6632225','6632239']
command=['sacct','--array','-j',','.join(ids),'--format=JobID%64,JobIDRaw%64,State,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES,NodeList%128,Start,End,User,Account,MaxRSS','--units=K','--noheader','--parsable2']
p=subprocess.run(command,text=True,capture_output=True,timeout=50)
print('NATIVE_SCALAR_RECONCILIATION '+json.dumps({'checked_utc':datetime.now(timezone.utc).isoformat(),'read_only':True,'command':command,'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr},sort_keys=True),flush=True)
p.check_returncode()
PY
