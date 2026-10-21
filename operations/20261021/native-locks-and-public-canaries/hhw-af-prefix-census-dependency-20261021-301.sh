#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
test "$(id -u)" = 3164768
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import json,hashlib,subprocess,re,os
os.environ['TZ']='UTC';root=Path('/scratch/pwa209/horsehair-worm');area=root/'maintenance/study-af-prefix-census-dependency-amendment-v1-20261021'
def require(v,s):
 if not v:raise ValueError(s)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def canon(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def call(args):
 p=subprocess.run(args,capture_output=True,text=True,timeout=30);require(p.returncode==0,'Native command failed: '+p.stderr);return {'command':args,'stdout':p.stdout,'stderr':p.stderr}
def fields(v):
 rows=v['stdout'].splitlines();require(len(rows)==1,'One native allocation required');pairs=re.findall(r'(?<!\S)(\w+)=([^\s]*)',rows[0]);d=dict(pairs);require(len(d)==len(pairs),'No duplicate native fields');return d
def save(name,v):
 with (area/name).open('xb') as f:f.write(json.dumps(v,indent=2,sort_keys=True).encode()+bytes([10]));f.flush();os.fsync(f.fileno())
def binding(request,job,source):
 directory=root/'plans'/('recovery-support-20261021-'+request);p=directory/'plan.json';l=root/'state'/(directory.name+'-submission.json')
 require(p==p.resolve() and l==l.resolve() and not p.is_symlink() and not l.is_symlink(),'Ordinary immutable native registration required')
 plan=json.loads(p.read_bytes());ledger=json.loads(l.read_bytes());require(len(plan['tasks'])==1,'One registered task required');task=plan['tasks'][0];entry=ledger['tasks'][task['name']];script=Path(task['script'])
 require(script==directory/('recovery-'+request+'.sbatch') and script==script.resolve() and not script.is_symlink(),'Exact script required')
 require(ledger['plan_sha256']==canon(plan) and ledger['source_sha256']==plan['release_identity']['sha256']==source,'Exact source/plan/ledger binding required')
 require(plan['config_sha256']==ledger['config_sha256']=='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35','Original full config required')
 require(entry['job_id']==job and entry['state']=='submitted' and entry['script_sha256']==task['script_sha256']==canon(script.read_text()),'Exact job/script/source ledger required')
 return {'plan':str(p),'plan_sha256':sha(p),'ledger':str(l),'ledger_sha256':sha(l),'script':str(script),'script_sha256':sha(script),'task':task,'submission':entry}
target=binding('284','6699432','8495a3d985515bf71f476295da87e663a1e764864ddce47a06794e7e11d88f94')
producer=binding('295','6699739','4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1')
before=call(['scontrol','show','job','--oneliner','6699432']);t=fields(before)
require(t.get('JobId')=='6699432' and t.get('JobState')=='PENDING' and t.get('UserId')=='pwa209(3164768)' and t.get('Account')=='def-ptewarie_cpu' and t.get('Command')==target['script'] and t.get('RunTime')=='00:00:00','Only the positively owned new unstarted prefix may change')
require(t.get('Dependency') in {'(null)','None'} and target['submission'].get('issued_native_dependencies')==[] and not target['task'].get('external_dependencies'),'Existing dependencies cannot be replaced')
native=call(['sacct','--allocations','-nP','-j','6699432','--format=JobIDRaw,State,ElapsedRaw,AllocCPUS,AllocNodes,User,Account']);rows=[x.split('|') for x in native['stdout'].splitlines() if x.strip()]
require(len(rows)==1 and rows[0][:5]==['6699432','PENDING','0','0','0'] and rows[0][5:7]==['pwa209','def-ptewarie_cpu'],'No allocated target may change')
producer_read=call(['scontrol','show','job','--oneliner','6699739']);p=fields(producer_read)
require(p.get('JobId')=='6699739' and p.get('UserId')=='pwa209(3164768)' and p.get('Account')=='def-ptewarie_cpu' and p.get('Command')==producer['script'] and p.get('JobState') in {'RUNNING','COMPLETED'},'Exact census producer must be active or positively completed')
policy=root/'maintenance/execution-resource-policies/af-expanded-20261021-v2/policy.json';q=root/'maintenance/study-af-execution-support-v2-20261021/qualification.json'
require(sha(policy)=='5dac85400e709525458cc34a2edc2bb40b029eb743078672cd721ab444c17015' and sha(q)=='f4f7684e3b04fde1a61c2ce20baec430a1dfe56b9e055b75cfaabc78cbdc7c13','Exact source-bound policy and full qualification required')
require(not area.exists(),'Immutable amendment cannot replay');area.mkdir(mode=0o700)
save('intent.json',{'target':target,'producer':producer,'before':before,'target_accounting':native,'producer_native':producer_read,'new_dependency':'afterok:6699739','original_issued_dependency_evidence_preserved':True,'original_plan_ledger_and_script_unchanged':True,'native_dependency_will_differ_prospectively_from_original_submission':True,'unchanged_six_hour_start_guard_remains_authoritative':True,'automatic_replay_permitted':False})
result=call(['scontrol','update','JobId=6699432','Dependency=afterok:6699739']);save('update-result.json',result)
after=call(['scontrol','show','job','--oneliner','6699432']);save('after.json',after);a=fields(after)
require(a.get('JobId')=='6699432' and a.get('JobState')=='PENDING' and a.get('UserId')=='pwa209(3164768)' and a.get('Command')==target['script'] and re.fullmatch(r'afterok:6699739(?:\([^)]*\))?',a.get('Dependency','')) is not None,'Dependency response ambiguous; preserve evidence and do not replay')
require(binding('284','6699432','8495a3d985515bf71f476295da87e663a1e764864ddce47a06794e7e11d88f94')==target and binding('295','6699739','4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1')==producer,'Original immutable bindings changed')
save('receipt.json',{'status':'confirmed_owned_pending_prefix_waits_for_exact_census','target_job':'6699432','producer_job':'6699739','actual_native_dependency':a['Dependency'],'checked_utc':datetime.now(timezone.utc).isoformat(),'original_plan_ledger_script_unchanged':True,'frozen_submission_dependency_evidence_preserved':True,'supplemental_scheduler_prerequisite_amendment':True,'no_hold_cancel_restart_or_numerical_source_change':True,'automatic_replay_permitted':False,'scientific_result':False})
print('AF_PREFIX_DEPENDENCY '+json.dumps({'status':'confirmed_owned_pending_prefix_waits_for_exact_census','receipt':str(area/'receipt.json'),'sha256':sha(area/'receipt.json')}),flush=True)
PY
