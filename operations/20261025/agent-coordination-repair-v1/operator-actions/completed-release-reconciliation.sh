#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src
export PYTHONDONTWRITEBYTECODE=1
export TZ=UTC
python -B -u - <<'PY'
from pathlib import Path
import os,json,gzip,subprocess,hashlib,importlib.util,time,copy
from horsehair.core import exclusive_file_lock,atomic_json,canonical_hash,read_json
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');s=r/'maintenance/agent-coordination-repair-v3-20261026';c=r/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
source=s/'coordination.py';assert hashlib.sha256(source.read_bytes()).hexdigest()=='be9965f47e2cf85ab9e3ff885ef9b9f9c34f8c1d02e78bf5fb7d24d0a81c8ccd'
spec=importlib.util.spec_from_file_location('original_qualified_coordination',source);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
ident='assembly-0093';job='7352510';jp=c/('submission-'+ident+'.json');area=s/'completed-release-reconciliation-829'
with exclusive_file_lock(r/'state/af-campaign-registration.mutex',timeout=60):
 before=m.read(jp);record=json.loads(before)
 assert record['job_id']==job and record['status'] in ['releasing','released']
 plan=read_json(record['plan_path']);task=plan['tasks'][0];ledger=read_json(record['ledger_path']);row=ledger['tasks'][task['name']]
 assert len(plan['tasks'])==1 and hashlib.sha256(m.read(record['plan_path'])).hexdigest()==record['plan_sha256']
 assert ledger['plan_sha256']==canonical_hash(plan) and ledger['source_sha256']==plan['release_identity']['sha256']==m.SOURCE
 assert plan['config_sha256']==ledger['config_sha256']==m.CONFIG and row['job_id']==job and row['state']=='submitted'
 assert row['script_sha256']==task['script_sha256']==canonical_hash(m.read(task['script']).decode())
 m.held_identity(record['held_allocation'],task,record['intent'],job)
 state_raw=m.read(c/'state.json.gz');state=json.loads(gzip.decompress(state_raw));entry=state['jobs'][ident]
 assert entry['job_id']==job and entry['status']=='complete' and entry['intent']==record['intent']
 assert entry['receipt']['native_job_id']==job and entry['receipt']['intent_id']==ident
 command=['sacct','-X','-n','-P','-j',job,'--format=JobIDRaw,JobName%80,State,ExitCode,ElapsedRaw,AllocCPUS,End,User,Account,AllocTRES']
 p=subprocess.run(command,text=True,capture_output=True,timeout=90);p.check_returncode();assert not p.stderr
 rows=[line.split('|') for line in p.stdout.splitlines() if line.strip()]
 assert len(rows)==1 and len(rows[0])==10
 jid,name,status,exit_code,elapsed,cpus,end,user,account,tres=rows[0]
 assert jid==job and name=='hhw-'+task['name'] and status=='COMPLETED' and exit_code=='0:0'
 assert int(elapsed)>0 and int(cpus)==task['resources']['cpus']==1 and user=='pwa209' and account=='def-ptewarie_cpu' and 'gpu' not in tres.lower()
 proof={'job_id':jid,'state':status,'exit_code':exit_code,'elapsed_seconds':int(elapsed),'cpus':int(cpus),'gpus':0,'end':end,'user':user,'account':account}
 assert entry['native_proof']==proof,'Positive completed canonical/native proof changed'
 assert m.read(jp)==before and m.read(c/'state.json.gz')==state_raw,'Observed state changed before acknowledgement correction'
 if record['status']=='released':
  print('COMPLETED_RELEASE_RECONCILIATION_829 '+json.dumps({'status':'already_released','job_id':job}),flush=True)
 else:
  assert not area.exists();area.mkdir()
  original={'journal.json':before,'plan.json':m.read(record['plan_path']),'ledger.json':m.read(record['ledger_path']),
            'state.json.gz':state_raw,'state-binding.json':m.read(c/'state-binding.json'),'script.sbatch':m.read(task['script'])}
  preserved={}
  for name,raw in original.items():
   with (area/name).open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
   preserved[name]=hashlib.sha256(raw).hexdigest()
  evidence={'actual_final_native':proof,'command':command,'stdout':p.stdout,'stderr':p.stderr,'preserved_sha256':preserved}
  atomic_json(area/'before.json',evidence)
  updated=copy.deepcopy(record);updated.update(status='released',release_basis='actual_final_accounting_completed_0_0',release_final_native=proof,release_acknowledged_unix=time.time())
  assert m.read(jp)==before and m.read(c/'state.json.gz')==state_raw
  atomic_json(jp,updated)
  assert m.read(c/'state.json.gz')==state_raw and read_json(jp)==updated
  result={'status':'completed_release_acknowledgement_reconciled','job_id':job,'intent_id':ident,'canonical_state_unchanged':True,
          'jobs_submitted':0,'jobs_cancelled':0,'scientific_gates_changed':False,'archive':str(area),'native_proof':proof}
  atomic_json(area/'complete.json',result)
  print('COMPLETED_RELEASE_RECONCILIATION_829 '+json.dumps(result),flush=True)
PY
