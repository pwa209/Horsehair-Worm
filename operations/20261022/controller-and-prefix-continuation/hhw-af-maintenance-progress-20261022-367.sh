#!/bin/bash
set -euo pipefail
module load StdEnv/2023 python/3.11.5
source /home/pwa209/hhw-environment/bin/activate
export PYTHONDONTWRITEBYTECODE=1
python -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import json,subprocess,os
assert os.geteuid()==3164768 and subprocess.check_output(['id','-un'],text=True).strip()=='pwa209'
assert subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec')
root=Path('/scratch/pwa209/horsehair-worm');out={'checked_utc':datetime.now(timezone.utc).isoformat(),'records':{},'logs':{}}
for name,rel in [
 ('census','maintenance/study-af-fresh-storage-census-v2-20261022/receipt.json'),
 ('census_support','maintenance/study-af-fresh-storage-census-support-v2-20261022/receipt.json'),
 ('global_census','state/storage-budget.json'),
 ('reconciliation','maintenance/study-af-unsubmitted-reconciliation-stage-v1-20261022/receipt.json'),
 ('reconciliation_commit','maintenance/study-af-unsubmitted-reconciliation-stage-v1-20261022/state-commit-verified.json'),
 ('prefix','maintenance/behaviour-full-cohort-prefix-v4-support-20261022/receipt.json'),
 ('prefix_progress','maintenance/behaviour-full-cohort-prefix-v4-20261022/progress.json'),
 ('f2_status','maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1/status.json'),
 ('f2_binding','maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1/state-binding.json')]:
 p=root/rel
 if p.is_file() and not p.is_symlink() and p.stat().st_size<=2000000:out['records'][name]=json.loads(p.read_bytes())
for n,j in [('352','6753750'),('365','6757358'),('366','6757565')]:
 p=root/('logs/recovery-'+n+'-'+j+'_4294967294.log')
 if p.is_file():
  with p.open('rb') as f:f.seek(max(0,p.stat().st_size-5500));out['logs'][n]=f.read(5500).decode(errors='replace')
for key,args in [('native',['sacct','--array','-j','6753750,6757358,6757565,6701494,6701495,6701498','--format=JobID%64,State,ExitCode,ElapsedRaw,AllocCPUS,NodeList%128,Start,End,User,Account','--noheader','--parsable2']),('queue',['squeue','-h','-u','pwa209','-o','%i|%j|%T|%r|%E|%S|%M'])]:
 p=subprocess.run(args,capture_output=True,text=True,timeout=45)
 out[key]={'returncode':p.returncode,'stderr':p.stderr,'stdout':'\n'.join(l for l in p.stdout.splitlines() if key!='queue' or '|hhw-' in l)}
print('AF_MAINTENANCE_PROGRESS '+json.dumps(out,sort_keys=True),flush=True)
PY
