"""Emit the A6 finite quota handoff after verified empty-directory recovery."""
from pathlib import Path
import ast,base64,hashlib,json

def digest(raw):return hashlib.sha256(raw).hexdigest()
operator=Path('work/hhw-shared-20261103-925.sh').read_text().split("python3 -B -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
operator=operator.replace("area=r/'maintenance/census-cadence-20261103-925';area.mkdir(mode=0o700,exist_ok=False)","area=s/'cadence-evidence';area.mkdir(mode=0o700,exist_ok=False)")
cut=operator.index('before={}')
definitions=operator[:cut]
operation=operator[cut:operator.index("result={'status':'three_existing_held_census_times_verified'")]
operation=operation.replace("anchor=datetime.now(timezone.utc).replace(microsecond=0)","anchor=readiness_anchor")
operation=operation.replace("'unchanged_quota_releaser':'8421133'","'quota_releaser_native_job':registration['job_id']")
expected={name:digest(Path('work',name).read_bytes()) for name in ['prune-archived-diagnostic-directories-20261104.py','test_prune_archived_diagnostic_directories_20261104.py','compact-terminal-controller-reads-20261104.py']}
expected_manifest=json.loads(Path('work/diagnostic-empty-prune-build-20261104.json').read_bytes())
assert expected_manifest['files']==expected and type(expected_manifest['native_tests']) is int and expected_manifest['native_tests']>0 and set(expected_manifest)=={'files','native_tests','native_roster_sha256'}
old_pins=json.loads(Path('work/quota-release-v5-20261104-pins.json').read_bytes())
old_sources={'release-census.py':old_pins['runtime_sha256'],'handoff.sbatch':old_pins['script_sha256']}
intro='''from pathlib import Path
from datetime import datetime,timezone
import os,json,hashlib,subprocess,shlex,time,re
import horsehair
from horsehair import budget,repair
from horsehair.core import source_identity
assert os.geteuid()==3164768 and re.fullmatch('[1-9][0-9]*',os.environ.get('SLURM_JOB_ID',''))
r=Path('/scratch/pwa209/horsehair-worm');s=r/'maintenance/storage-recovery-handoff-v6-20261104'
assert Path(__file__)==s/'release-census.py'
assert subprocess.check_output(['hostname','-f'],text=True,timeout=10).strip().endswith('.narval.calcul.quebec')
os.environ['TZ']='UTC'
if hasattr(time,'tzset'):time.tzset()
def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
manifest=json.loads((s/'source-manifest.json').read_bytes())
for name,pin in manifest.items():assert digest(s/name)==pin,'Frozen quota-handoff source changed'
framework=r/'releases/20261021-6ed201653dfa-4f3903d7c26e'
assert Path(horsehair.__file__).resolve()==framework/'src/horsehair/__init__.py' and source_identity(framework)['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
registration=json.loads((s/'registration.json').read_bytes());job=os.environ['SLURM_JOB_ID'];assert registration['job_id']==job
cleanup=registration['cleanup'];assert digest(cleanup['manifest']['path'])==cleanup['manifest']['sha256']
for name,pin in cleanup['source_pins'].items():assert digest(Path(cleanup['manifest']['path']).parent/name)==pin
old_support=r/'maintenance/storage-recovery-handoff-v5-20261104'
assert registration['old_handoff']['job_id']=='8463177'
for name,pin in registration['old_handoff']['source_pins'].items():assert digest(old_support/name)==pin,'Failed predecessor source changed'
assert not (old_support/'cadence-evidence').exists() and not (old_support/'result.json').exists(),'Failed predecessor may have issued census actions; explicit reconciliation required'
def read(cmd):
 p=subprocess.run(cmd,capture_output=True,text=True,timeout=30);p.check_returncode();assert not p.stderr;return p.stdout
f=dict(x.split('=',1) for x in shlex.split(read(['scontrol','show','job',job,'-o'])) if '=' in x)
assert f['JobId']==job and f['UserId']=='pwa209(3164768)' and f['JobState']=='RUNNING' and f['Account']=='def-ptewarie_cpu' and f['Command']==str(s/'handoff.sbatch') and f['JobName']=='hhw-storage-recovery-handoff-v6-20261104'
assert f['NumCPUs']=='1' and f['NumNodes'] in {'1','1-1'} and f['MinMemoryNode'] in {'2G','2048M'} and f['TimeLimit']=='00:15:00' and f['Restarts']=='0' and 'ArrayJobId' not in f
assert all('gpu' not in f.get(k,'').lower() for k in ('ReqTRES','AllocTRES','Gres','TresPerNode','TresPerTask'))
end=datetime.fromisoformat(f['EndTime']).replace(tzinfo=timezone.utc).timestamp();assert abs(end-float(os.environ['SLURM_JOB_END_TIME']))<=2 and time.time()+180<end
stop=min(time.monotonic()+720,time.monotonic()+end-time.time()-180)
accounting_attempt=0
while True:
 accounting_attempt+=1
 try:
  rows=repair._repair_native_states([cleanup['job_id']],{},owner='pwa209',account='def-ptewarie_cpu',runner=lambda cmd,**kw:subprocess.run(cmd,**{**kw,'timeout':30}))
 except (subprocess.TimeoutExpired,RuntimeError) as error:
  if isinstance(error,RuntimeError) and str(error)!='Repair native proof query failed':raise
  print(json.dumps({'status':'cleanup_final_accounting_read_pending','attempt':accounting_attempt,'error_type':type(error).__name__}),flush=True)
  assert time.monotonic()+5<stop,'Final cleanup accounting is still pending';time.sleep(5);continue
 row=rows.get(cleanup['job_id'],{})
 if row.get('state')=='COMPLETED' and row.get('exit_code')=='0:0':
  assert row['job_id_raw']==cleanup['job_id'] and row['cpus']==1 and row['elapsed_seconds']>0 and 'gpu' not in row['alloc_tres'].lower();break
 assert not row or row.get('state') in {'PENDING','RUNNING','COMPLETING'},'Cleanup prerequisite failed'
 assert time.monotonic()+5<stop,'Final cleanup accounting is still pending';time.sleep(5)
requestpath=Path(cleanup['manifest']['path']).parent/'request.json'
assert digest(requestpath)==cleanup['request_sha256'],'Pruner request changed'
request=json.loads(requestpath.read_bytes());assert request['case']==cleanup['case']
for binding in request['binding_documents']:assert digest(binding['path'])==binding['sha256'],'Canonical source changed after independently verified pruning'
resultpath=Path(cleanup['case'])/'result.json'
result=json.loads(resultpath.read_bytes())
assert result['status']=='archived_diagnostic_empty_directory_prune_complete' and result['verified_transactions']==30 and result['archived_member_files']==33782 and result['removed_files']==0 and result['archive_replay'] is False and result['scientific_result'] is False and result['admission_authority'] is False,'Independent complete thirty-transaction archive/prune proof required'
canonical_state=r/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1/state.json.gz'
assert result['canonical_state_sha256']==digest(canonical_state),'Canonical state changed after independently verified pruning'
assert result['request_sha256']==cleanup['request_sha256'] and result['wrapper_sha256']==cleanup['source_pins']['prune-archived-diagnostic-directories-20261104.py'] and result['helper_sha256']==request['helper']['sha256'],'Independent prune proof source/request differs'
proofs=result['producer_proofs']
assert result['producer_job_ids']==['8425411','8463111'] and set(proofs)=={'8425411','8463111'} and proofs['8425411']['proof_status'] is None and proofs['8425411']['ineligible_source_jobs']==[] and proofs['8463111']['proof_status']=='terminal_controller_diagnostic_recovery_incomplete' and proofs['8463111']['canonical_state_sha256']==result['canonical_state_sha256'] and proofs['8463111']['ineligible_source_jobs']==['6950429','7028292'],'Original partial cleanup provenance differs'
for producer in request['producers']:assert proofs[producer['job']]['proof_sha256']==producer['proof']['sha256']==digest(producer['proof']['path']),'Original producer frontier changed'
quota=budget.require_storage_headroom(r,required_files=2000,required_bytes=10_000_000_000,reserve_files=248000,reserve_bytes=2_000_000_000_000,runner=lambda cmd,**kw:subprocess.run(cmd,**{**kw,'timeout':30}))
assert quota['ready'] is True
readiness_anchor=datetime.now(timezone.utc).replace(microsecond=0);cleanup_native=row
'''
outro='''assert all(value['proof'] for value in results.values()),'Census times are not positively observable; preserve evidence and do not release jobs'
save('cadence-complete.json',{'status':'held_census_cadence_verified_before_release','jobs':results,'fresh_quota':quota,'cleanup_native':cleanup_native,'cleanup_result_sha256':digest(resultpath),'actual_empty_directories_removed':result['removed'],'archived_member_files':result['archived_member_files']})
released={}
for slot,job in [(61,'6983037'),(62,'6983056'),(63,'6983059'),(64,'6983060')]:
 assert time.time()+60<end,'Quota handoff finalization reserve exhausted'
 source_guard();pre=fields('before-release-'+job,job);held_guard(pre,job,'before-release-'+job)
 response=invoke('release-once-'+job,['scontrol','release',job]);post=fields('after-release-'+job,job)
 assert post['UserId']=='pwa209(3164768)' and post['JobName']=='hhw-'+tasks[job]['name'] and post['Command']==tasks[job]['script'] and post['Account']=='def-ptewarie_cpu'
 assert post['NumCPUs']=='8' and post['MinMemoryNode'] in {'64G','65536M'} and post['TimeLimit'] in {'06:00:00','6:00:00','0-06:00:00'}
 assert post['JobState'] in {'RUNNING','COMPLETING','COMPLETED'} or post['JobState']=='PENDING' and post['Reason'] not in {'JobHeldUser','JobHeldAdmin'} and int(post['Priority'])>0
 source_guard();released[job]={'actual_unheld':post,'client_response':response};save('released-'+job+'.json',released[job])
summary={'status':'fresh_quota_verified_existing_censuses_released_with_cadence','jobs':list(released),'free_files':quota['personal_available_files'],'cadence':results,'cleanup_job_id':cleanup['job_id'],'actual_empty_directories_removed':result['removed'],'archived_member_files':result['archived_member_files'],'new_census_jobs_submitted':0,'automatic_replay_permitted':False}
with (s/'result.json').open('x') as f:json.dump(summary,f,indent=2);f.flush();os.fsync(f.fileno())
print('EXISTING_CENSUSES_RELEASED '+json.dumps(summary),flush=True)
'''
# Verify slot61 before the readiness-anchored time changes. It remains unchanged
# until all three future times are positively confirmed and the release begins.
before_61="f61=fields('before-all-6983037','6983037');held_guard(f61,'6983037','before-all-6983037')\n"
code=intro+definitions+before_61+operation+outro
compile(code,'<quota-cadence-runtime-v6>','exec')
script='''#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-storage-recovery-handoff-v6-20261104
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=00:15:00
#SBATCH --hold
#SBATCH --no-requeue
#SBATCH --output=/scratch/pwa209/horsehair-worm/maintenance/storage-recovery-handoff-v6-20261104/native.log
set -euo pipefail
test "$(id -un)" = pwa209
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src:"${PYTHONPATH:-}"
export PYTHONDONTWRITEBYTECODE=1 TZ=UTC
python -s -B /scratch/pwa209/horsehair-worm/maintenance/storage-recovery-handoff-v6-20261104/release-census.py
'''
header='''#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
export TZ=UTC PYTHONDONTWRITEBYTECODE=1
python3 -B -u - <<'PY'
'''
body='''from pathlib import Path
import base64,json,subprocess,shlex,os,hashlib,re
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');s=r/'maintenance/storage-recovery-handoff-v6-20261104';cs=r/'maintenance/diagnostic-empty-directory-support-20261104'
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def query(cmd):
 p=subprocess.run(cmd,capture_output=True,text=True,timeout=30);p.check_returncode();assert not p.stderr;return p.stdout
old=query(['sacct','-X','-n','-P','-j','8463177','--format=JobIDRaw,State,ExitCode,ElapsedRaw,AllocCPUS,User,Account,AllocTRES'])
rows=[x.split('|') for x in old.splitlines() if x.strip()];assert len(rows)==1
v=rows[0];assert len(v)==8 and v[:7]==['8463177','FAILED','1:0','8','1','pwa209','def-ptewarie_cpu'] and 'gpu' not in v[7].lower(),'Old quota handoff does not have its exact positively observed terminal allocation'
old_support=r/'maintenance/storage-recovery-handoff-v5-20261104';old_sources=OLD_SOURCES
assert json.loads((old_support/'source-manifest.json').read_bytes())==old_sources
for name,pin in old_sources.items():assert sha(old_support/name)==pin,'Failed predecessor source changed'
assert json.loads((old_support/'registration.json').read_bytes())['job_id']=='8463177'
assert not (old_support/'cadence-evidence').exists() and not (old_support/'result.json').exists(),'Failed predecessor may have issued census actions; explicit reconciliation required'
manifest=json.loads((cs/'source-manifest.json').read_bytes());expected=EXPECTED
assert manifest==EXPECTED_MANIFEST,'Cleanup source/test manifest differs from the reviewed empty-directory proof closure'
for name,pin in expected.items():assert manifest['files'][name]==pin,'Cleanup source differs from the reviewed empty-directory proof closure'
for name,pin in manifest['files'].items():assert re.fullmatch('[A-Za-z0-9_.-]+',name) and re.fullmatch('[0-9a-f]{64}',pin) and sha(cs/name)==pin
record=json.loads((cs/'registration.json').read_bytes());cleanupjob=record['job_id'];assert re.fullmatch('[1-9][0-9]*',cleanupjob)
request=json.loads((cs/'request.json').read_bytes());assert record['request_sha256']==sha(cs/'request.json') and request['case']==str(r/'maintenance/diagnostic-empty-directory-recovery-20261104')
f=record['actual_unheld'];assert f['JobId']==cleanupjob and f['UserId']=='pwa209(3164768)' and f['Command']==str(cs/'prune.sbatch') and f['JobName']=='hhw-empty-diagnostic-directory-prune-20261104' and f['Account']=='def-ptewarie_cpu' and f['NumCPUs']=='1' and f['MinMemoryNode'] in {'8G','8192M'} and f['TimeLimit'] in {'02:00:00','0-02:00:00'}
assert f['JobState']=='RUNNING' or f['JobState']=='PENDING' and f['Reason'] not in {'JobHeldUser','JobHeldAdmin'} and int(f['Priority'])>0
cleanup={'job_id':cleanupjob,'manifest':{'path':str(cs/'source-manifest.json'),'sha256':sha(cs/'source-manifest.json')},'source_pins':manifest['files'],'request_sha256':record['request_sha256'],'registration_record_sha256':sha(cs/'registration.json'),'case':request['case']}
assert not s.exists();s.mkdir(mode=0o700)
code=base64.b64decode(CODE);text=base64.b64decode(SCRIPT)
for name,raw in [('release-census.py',code),('handoff.sbatch',text)]:
 with (s/name).open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
 (s/name).chmod(0o444)
(s/'source-manifest.json').write_text(json.dumps({'release-census.py':hashlib.sha256(code).hexdigest(),'handoff.sbatch':hashlib.sha256(text).hexdigest()},indent=2))
(s/'old-quota-final.json').write_text(json.dumps({'actual_native':rows,'source_guard':'8463177 positively failed before cadence or census release actions','old_source_pins':old_sources},indent=2))
p=s/'handoff.sbatch';command=['sbatch','--parsable','--dependency=afterok:'+cleanupjob,str(p)]
(s/'submit-intent.json').write_text(json.dumps({'command':command,'automatic_retry_permitted':False}))
response=subprocess.run(command,capture_output=True,text=True,timeout=30);(s/'submit-response.json').write_text(json.dumps({'code':response.returncode,'stdout':response.stdout,'stderr':response.stderr}));response.check_returncode();job=response.stdout.strip().split(';')[0];assert re.fullmatch('[1-9][0-9]*',job)
def observe():
 fields=dict(x.split('=',1) for x in shlex.split(query(['scontrol','show','job',job,'-o'])) if '=' in x)
 assert fields['JobId']==job and fields['UserId']=='pwa209(3164768)' and fields['Command']==str(p) and fields['JobName']=='hhw-storage-recovery-handoff-v6-20261104' and fields['Account']=='def-ptewarie_cpu'
 assert fields['NumCPUs']=='1' and fields['MinMemoryNode'] in {'2G','2048M'} and fields['TimeLimit']=='00:15:00' and fields['Requeue']=='0'
 return fields
f=observe();assert f['JobState']=='PENDING' and f['Reason']=='JobHeldUser' and f['Priority']=='0';(s/'held.json').write_text(json.dumps(f))
# Publish runtime registration before release, so an immediate start sees it.
(s/'registration.json').write_text(json.dumps({'job_id':job,'cleanup':cleanup,'old_handoff':{'job_id':'8463177','source_pins':old_sources},'source_manifest':{'path':str(s/'source-manifest.json'),'sha256':sha(s/'source-manifest.json')},'status':'owned_held_replacement_before_release'},indent=2))
(s/'release-intent.json').write_text(json.dumps({'command':['scontrol','release',job],'automatic_retry_permitted':False}))
response=subprocess.run(['scontrol','release',job],capture_output=True,text=True,timeout=30);(s/'release-response.json').write_text(json.dumps({'code':response.returncode,'stdout':response.stdout,'stderr':response.stderr}))
f=observe();assert f['JobState']=='RUNNING' or f['JobState']=='PENDING' and f['Reason'] not in {'JobHeldUser','JobHeldAdmin'} and int(f['Priority'])>0
(s/'actual-unheld.json').write_text(json.dumps(f,indent=2));print('STORAGE_HANDOFF_V6 '+json.dumps({'job_id':job,'cleanup_job_id':cleanupjob,'actual_unheld':f}),flush=True)
'''.replace('OLD_SOURCES',repr(old_sources)).replace('EXPECTED_MANIFEST',repr(expected_manifest)).replace('EXPECTED',repr(expected)).replace('CODE',repr(base64.b64encode(code.encode()).decode())).replace('SCRIPT',repr(base64.b64encode(script.encode()).decode()))
compile(body,'<quota-cadence-stage-v6>','exec')
Path('work/hhw-quota-release-20261104-960.sh').write_text(header+body+'\nPY\n',newline='\n')
Path('work/quota-release-v6-20261104-pins.json').write_text(json.dumps({'runtime_sha256':digest(code.encode()),'script_sha256':digest(script.encode()),'cleanup_core_pins':expected,'cleanup_manifest':expected_manifest},indent=2)+'\n')
print(json.dumps({'request':'work/hhw-quota-release-20261104-960.sh','runtime_sha256':digest(code.encode()),'script_sha256':digest(script.encode()),'cleanup_core_pins':expected,'cleanup_manifest':expected_manifest}))
