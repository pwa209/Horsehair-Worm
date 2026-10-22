#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import os,json,hashlib,subprocess,time
assert os.geteuid()==3164768
os.environ['TZ']='UTC';time.tzset()
root=Path('/scratch/pwa209/horsehair-worm')
def ch(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
result={'read_only':True,'checked_utc':datetime.now(timezone.utc).isoformat(),'host':subprocess.check_output(['hostname','-f'],text=True).strip(),'registrations':{},'commands':{}}
for label,jid in [('394','6822432'),('395','6822568'),('399','6822730'),('406','6824584')]:
 try:
  area=root/'plans'/('recovery-support-20261021-'+label);pp=area/'plan.json';lp=root/'state'/('recovery-support-20261021-'+label+'-submission.json')
  assert not pp.is_symlink() and not lp.is_symlink()
  plan=json.loads(pp.read_text());ledger=json.loads(lp.read_text());task=next(t for t in plan['tasks'] if t['name']=='recovery-'+label);entry=ledger['tasks']['recovery-'+label];script=Path(task['script'])
  assert script==area/('recovery-'+label+'.sbatch') and not script.is_symlink()
  assert ch(plan)==ledger['plan_sha256'] and ch(script.read_text())==entry['script_sha256']==task['script_sha256'] and entry['job_id']==jid and entry['state']=='submitted'
  assert ledger['source_sha256']==plan['release_identity']['sha256'] and ledger['config_sha256']==plan['config_sha256']
  result['registrations'][label]={'verified':True,'job_id':jid,'plan_sha256':ch(plan),'ledger_path':str(lp),'ledger_sha256':hashlib.sha256(lp.read_bytes()).hexdigest(),'script':str(script),'script_sha256':task['script_sha256'],'source_sha256':ledger['source_sha256'],'config_sha256':ledger['config_sha256'],'native_dependencies':entry['issued_native_dependencies'],'task':task}
 except Exception as e:result['registrations'][label]={'verified':False,'error_type':type(e).__name__,'errno':getattr(e,'errno',None),'error':str(e)}
for key,args in [('queue',['squeue','-h','-u','pwa209','-o','%i|%j|%T|%r|%E|%S|%M']),('native',['sacct','--array','-j','6811498,6819399,6822432,6822568,6822730,6824584','--format=JobID%64,JobIDRaw%64,State,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES,NodeList%128,Start,End,User,Account,MaxRSS','--units=K','--noheader','--parsable2'])]:
 try:
  p=subprocess.run(args,capture_output=True,text=True,timeout=40);result['commands'][key]={'returncode':p.returncode,'stdout':'\n'.join(l for l in p.stdout.splitlines() if '|hhw-' in l) if key=='queue' else p.stdout,'stderr':p.stderr}
 except Exception as e:result['commands'][key]={'error_type':type(e).__name__,'error':str(e)}
result['log_tails']={}
for jid,label in [('6811498','375'),('6819399','377'),('6822432','394'),('6822568','395'),('6822730','399'),('6824584','406')]:
 p=root/'logs'/('recovery-'+label+'-'+jid+'_4294967294.log')
 try:
  if p.is_file():
   with p.open('rb') as f:f.seek(max(0,p.stat().st_size-4000));result['log_tails'][jid]=f.read(4000).decode(errors='replace')
 except Exception as e:result['log_tails'][jid]={'error_type':type(e).__name__,'error':str(e)}
print('VERIFIED_REGISTRATIONS '+json.dumps(result,sort_keys=True),flush=True)
PY
