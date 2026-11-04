#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
export TZ=UTC
python3 -B -u - <<'PY'
from pathlib import Path
import hashlib,json,os,shlex,subprocess
assert os.geteuid()==3164768
s=Path('/scratch/pwa209/horsehair-worm/maintenance/orthology-storage-audit-20261104')
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
request=json.loads((s/'request.json').read_bytes());response=json.loads((s/'submission-response.json').read_bytes())
assert request['audit_source_sha256']==sha(s/'audit.py')=='5501f7682b65cf52a30b39a86eced038c30a72375dfb5a3d9c3490a66c57de33'
assert request['script_sha256']==sha(s/'audit.sbatch')=='f94340219e30ab5556c5e7b79e0ed8c56bc4ee3efdc5d8270431c3cb877d5ea7'
assert response['code']==0 and response['stdout'].strip().split(';')[0]=='8508393'
p=subprocess.run(['scontrol','show','job','8508393','-o'],capture_output=True,text=True,timeout=30);p.check_returncode()
f=dict(x.split('=',1) for x in shlex.split(p.stdout) if '=' in x)
assert f['JobId']=='8508393' and f['UserId']=='pwa209(3164768)' and f['Account']=='def-ptewarie_cpu' and f['Command']==str(s/'audit.sbatch') and f['JobName']=='hhw-orthology-storage-audit-20261104'
assert f['NumCPUs']=='1' and f['MinMemoryNode'] in {'4G','4096M'} and f['TimeLimit']=='01:00:00' and 'ArrayJobId' not in f
assert f['JobState'] in {'PENDING','RUNNING','COMPLETING','COMPLETED'} and all('gpu' not in f.get(k,'').lower() for k in ['ReqTRES','AllocTRES','Gres','TresPerNode','TresPerTask'])
v={'native_job_id':'8508393','source_sha256':request['audit_source_sha256'],'request_sha256':sha(s/'request.json'),'script_sha256':request['script_sha256'],'read_only_inputs':True,'files_removed':0,'actual_native_identity':f,'submission_stderr_preserved':True,'recovery_basis':'original successful submit response plus exact actual native identity; no resubmission'}
target=s/'registration.json'
if target.exists():
 old=json.loads(target.read_bytes());assert all(old.get(k)==v[k] for k in ['native_job_id','source_sha256','request_sha256','script_sha256'])
else:
 with target.open('x') as stream:json.dump(v,stream,indent=2);stream.flush();os.fsync(stream.fileno())
print('EXISTING_ORTHOLOGY_AUDIT_REGISTERED '+json.dumps(v),flush=True)
PY
