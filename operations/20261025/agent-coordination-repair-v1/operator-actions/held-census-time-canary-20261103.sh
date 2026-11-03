#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
export TZ=UTC
python3 -B -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone,timedelta
import ast,hashlib,json,os,re,shlex,subprocess,time
assert os.geteuid()==3164768
time.tzset();r=Path('/scratch/pwa209/horsehair-worm');a=r/'maintenance/census-held-time-canary-20261103-941';a.mkdir(mode=0o700,exist_ok=False)
def save(name,value):
 with (a/name).open('x') as f:json.dump(value,f,indent=2);f.flush();os.fsync(f.fileno())
def native(job):
 q=subprocess.run(['scontrol','show','job',job,'-o'],capture_output=True,text=True,timeout=30);q.check_returncode();assert not q.stderr
 f=dict(x.split('=',1) for x in shlex.split(q.stdout) if '=' in x);assert f['JobId']==job and f['UserId']=='pwa209(3164768)' and f['Account']=='def-ptewarie_cpu';return f
quota=native('8425418');cleanup=native('8425411');assert quota['JobState']=='PENDING' and quota['Command']==str(r/'maintenance/storage-recovery-handoff-v4-20261103/handoff.sbatch') and cleanup['JobState']=='RUNNING'
source=r/'maintenance/storage-recovery-handoff-v4-20261103/release-census.py';raw=source.read_bytes();assert hashlib.sha256(raw).hexdigest()=='0215539a33e603b36905fcd1bfb02afc58fb91059d131b1f52ac1d05790e58b4'
tree=ast.parse(raw);defs=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in {'epoch','text_time_proof','json_time_proof'}];assert len(defs)==3
proofs={'datetime':datetime,'timezone':timezone,'re':re};exec(compile(ast.Module(body=defs,type_ignores=[]),'<exact-frozen-native-time-proofs>','exec'),proofs)
phase='study-census-resilience-v1-20261024';plan=json.loads((r/'plans'/phase/'plan.json').read_bytes());ledger=json.loads((r/'state'/(phase+'-submission.json')).read_bytes())
canonical=lambda v:hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
assert plan['root']==str(r) and ledger['plan_sha256']==canonical(plan)
t=next(t for t in plan['tasks'] if t['name']=='rolling-storage-census-62');row=ledger['tasks'][t['name']]
assert row['job_id']=='6983056' and row['state']=='submitted' and t['resources']=={'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None}
assert t['script_sha256']==row['script_sha256']==canonical(Path(t['script']).read_text())
before=native('6983056');assert before['JobState']=='PENDING' and before['Reason']=='JobHeldUser' and before['Priority']=='0' and before['Command']==t['script'] and before['NumCPUs']=='8' and before['MinMemoryNode'] in {'64G','65536M'} and before['TimeLimit']=='06:00:00'
save('before.json',before);desired=datetime.now(timezone.utc).replace(microsecond=0)+timedelta(hours=4)
cmd=['scontrol','update','JobId=6983056','StartTime='+desired.strftime('%Y-%m-%dT%H:%M:%S')]
save('intent.json',{'command':cmd,'scope':'one already-held census job; existing A4 will reanchor at actual readiness','automatic_replay_permitted':False,'new_jobs':0})
q=subprocess.run(cmd,capture_output=True,text=True,timeout=30);save('response.json',{'code':q.returncode,'stdout':q.stdout,'stderr':q.stderr})
after=native('6983056');save('after.json',after)
assert after['JobState']=='PENDING' and after['Reason']=='JobHeldUser' and after['Priority']=='0'
for key in ('JobId','UserId','Account','Command','JobName','Dependency','NumCPUs','MinMemoryNode','TimeLimit','ReqTRES'):assert before.get(key)==after.get(key)
proof=proofs['text_time_proof'](after,desired.timestamp())
if proof is None:
 q=subprocess.run(['scontrol','--json','show','job','6983056'],capture_output=True,text=True,timeout=30);save('native-json.json',{'code':q.returncode,'stdout':q.stdout,'stderr':q.stderr})
 if q.returncode==0 and not q.stderr:proof=proofs['json_time_proof'](json.loads(q.stdout),'6983056',desired.timestamp())
result={'status':'actual_held_native_time_proof_verified' if proof else 'actual_native_time_proof_incomplete_requires_review','job_id':'6983056','desired_utc':desired.isoformat(),'proof':proof,'held_priority_zero':True,'new_jobs':0,'scientific_work_started':False,'A4_reanchors_later':True}
save('result.json',result);print('HELD_CENSUS_TIME_CANARY '+json.dumps(result),flush=True)
PY
