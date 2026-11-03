#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -B -u - <<'PY'
from pathlib import Path
import json,os,re,shlex,subprocess
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');s=r/'maintenance/terminal-controller-diagnostic-support-v3-20261103';a=s/'old-native-dependency-resolution';a.mkdir(exist_ok=False)
def save(name,v):
 with (a/name).open('x') as f:json.dump(v,f,indent=2);f.flush();os.fsync(f.fileno())
def run(cmd):
 p=subprocess.run(cmd,text=True,capture_output=True,timeout=30);p.check_returncode();assert not p.stderr;return p.stdout
native=run(['sacct','-X','-n','-P','-j','8420757','--format=JobIDRaw,State,ExitCode,ElapsedRaw,AllocCPUS,User,Account'])
rows=[line.split('|') for line in native.splitlines() if line.strip()];assert len(rows)==1
v=rows[0];assert len(v)==7 and v[0]=='8420757' and v[1].split()[0].split('+')[0] in {'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'} and re.fullmatch('[0-9]+:[0-9]+',v[2]) and int(v[3])>0 and v[4:]==['1','pwa209','def-ptewarie_cpu']
save('actual-old-final.json',{'native':native,'dependency_kind':'afterany','successful_execution_claimed':False})
parent=subprocess.run(['scontrol','show','job','8420757','-o'],text=True,capture_output=True,timeout=30)
save('actual-old-controller-state.json',{'code':parent.returncode,'stdout':parent.stdout,'stderr':parent.stderr})
if parent.returncode==0:
 pf=dict(x.split('=',1) for x in shlex.split(parent.stdout) if '=' in x)
 assert pf['JobId']=='8420757' and pf['UserId']=='pwa209(3164768)' and pf['Account']=='def-ptewarie_cpu'
 if pf['JobState'] not in {'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}:
  save('result.json',{'status':'actual_controller_parent_not_final_wait_without_mutation','job_id':'8425411','parent_job_state':pf['JobState'],'job_resubmitted':False})
  print('MAINTENANCE_DEPENDENCY_RESOLUTION: native parent still active/completing; no mutation',flush=True);raise SystemExit(0)
else:
 assert parent.returncode==1 and 'Invalid job id' in parent.stderr,'Unexpected native parent observation; never clear blindly'

def observe():
 f=dict(x.split('=',1) for x in shlex.split(run(['scontrol','show','job','8425411','-o'])) if '=' in x)
 assert f['JobId']=='8425411' and f['UserId']=='pwa209(3164768)' and f['Account']=='def-ptewarie_cpu' and f['Command']==str(s/'controller-compact.sbatch') and f['JobName']=='hhw-terminal-controller-read-compaction-20261103' and f['NumCPUs']=='1' and f['TimeLimit']=='12:00:00'
 return f
before=observe();save('before.json',before)
dep=before.get('Dependency','')
if before['JobState']=='PENDING' and re.fullmatch(r'afterany:8420757(?:\((?:unfulfilled|fulfilled)\))?',dep):
 cmd=['scontrol','update','JobId=8425411','Dependency='];save('update-intent.json',{'command':cmd,'basis':'positive actual owned final accounting satisfies original afterany','automatic_replay_permitted':False})
 p=subprocess.run(cmd,text=True,capture_output=True,timeout=30);save('update-response.json',{'code':p.returncode,'stdout':p.stdout,'stderr':p.stderr})
 after=observe();assert after['Dependency'] in {'(null)','None',''} and after['JobState'] in {'PENDING','RUNNING','COMPLETING','COMPLETED'}
 save('after.json',after);status='original_job_retained_actual_final_predecessor_dependency_resolved'
else:
 assert dep in {'(null)','None',''} and before['JobState'] in {'PENDING','RUNNING','COMPLETING','COMPLETED'},'Unexpected state needs explicit review; never resubmit'
 status='native_dependency_already_resolved_no_mutation'
save('result.json',{'status':status,'job_id':'8425411','actual_final_parent':'8420757','job_resubmitted':False,'source_unchanged':True})
print('MAINTENANCE_DEPENDENCY_RESOLUTION '+status,flush=True)
PY
