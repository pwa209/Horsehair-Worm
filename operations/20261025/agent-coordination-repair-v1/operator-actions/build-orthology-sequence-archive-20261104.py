"""Build a finite archive of the four audited orthology sequence-copy scopes."""
from pathlib import Path
import argparse,base64,hashlib,json,subprocess,sys

p=argparse.ArgumentParser();p.add_argument('--native-tests',required=True,type=int);args=p.parse_args()
names=['archive-orthology-sequence-copies-20261104.py','test_archive_orthology_sequence_copies_20261104.py','compact-terminal-controller-reads-20261104.py']
response=subprocess.run([sys.executable,'-m','pytest','work/'+names[1],'--collect-only','-q','-p','no:cacheprovider'],text=True,capture_output=True,check=True)
roster=sorted(line.split('::',1)[0].replace('\\','/').split('/')[-1][:-3]+'::'+line.split('::',1)[1]
    for line in response.stdout.splitlines() if '::' in line)
assert len(roster)==len(set(roster))==args.native_tests and args.native_tests>=20
canonical=lambda value:hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
payload={name:base64.b64encode(Path('work',name).read_bytes()).decode() for name in names}
pins={name:hashlib.sha256(Path('work',name).read_bytes()).hexdigest() for name in names}
assert pins[names[2]]=='e8a599304f4ac2561339114e7965f3cca2f06c87d565822bb32aabbdf322d979'
body='''#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src:"${PYTHONPATH:-}"
export PYTHONDONTWRITEBYTECODE=1 TZ=UTC
python -s -B -u - <<'PY'
from pathlib import Path
import os,json,hashlib,base64,subprocess,shlex,re,importlib.util
from horsehair import budget,slurm
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');s=r/'maintenance/orthology-sequence-archive-support-20261104'
assert not s.exists(),'Existing registration must be reconciled, never replayed'
s.mkdir()
payload=__PAYLOAD__
pins=__PINS__
for name,value in payload.items():
 path=s/name;path.write_bytes(base64.b64decode(value));assert hashlib.sha256(path.read_bytes()).hexdigest()==pins[name]
(s/'source-manifest.json').write_text(json.dumps({'files':pins,'native_tests':__TEST_COUNT__,'native_roster_sha256':'__ROSTER_SHA__'},sort_keys=True,indent=2))
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def locator(path):return {'path':str(path),'sha256':sha(path)}
def native(command):
 result=subprocess.run(command,capture_output=True,text=True,timeout=90);result.check_returncode();assert not result.stderr;return result
expected_native={'8508393':['COMPLETED','0:0','11','1'],'5455844':['COMPLETED','0:0','14551','8'],'6632058':['COMPLETED','0:0','936','8']}
prior_native=native(['sacct','-X','-n','-P','-j',','.join(expected_native),'--format=JobIDRaw,State,ExitCode,ElapsedRaw,AllocCPUS,User,Account,AllocTRES'])
rows=[line.split('|') for line in prior_native.stdout.splitlines() if line.strip()]
assert len(rows)==3 and {row[0] for row in rows}==set(expected_native)
for row in rows:
 assert row[1:5]==expected_native[row[0]] and row[5:7]==['pwa209','def-ptewarie_cpu'] and 'gpu' not in row[7].lower()
(s/'original-native-final.json').write_text(json.dumps({'actual_native_rows':rows,'native_stdout':prior_native.stdout,'cost_basis':'actual completed original scientific allocations and read-only audit'},indent=2))
budget.native_accounting(r,list(expected_native),runner=lambda command,**kw:subprocess.run(command,**{**kw,'timeout':90}))
cost_proofs={}
for job,expected in expected_native.items():
 proof=slurm.completed_native_dependency(r,job,dependency_kind='afterany');assert proof is not None
 cost=proof['native_records'][job]
 assert [cost['state'],cost['exit_code'],cost['elapsed_seconds'],cost['cpus'],cost['gpus']]==[expected[0],expected[1],int(expected[2]),int(expected[3]),0]
 cost_proofs[job]=proof
(s/'original-framework-accounting.json').write_text(json.dumps(cost_proofs,indent=2))
__REQUEST_BUILD__
raw=(json.dumps(request,sort_keys=True,indent=2)+'\\n').encode();(s/'request.json').write_bytes(raw);request_sha=hashlib.sha256(raw).hexdigest()
# Only known small metadata is read here. Sequence roster, content hashing,
# independent archive verification and removal remain compute-node work.
spec=importlib.util.spec_from_file_location('reviewed_orthology_archive',s/'archive-orthology-sequence-copies-20261104.py')
archive_module=importlib.util.module_from_spec(spec);spec.loader.exec_module(archive_module)
integrity=archive_module.load_helper(request['helper'],root=r)
archive_module.metadata_authority(request,integrity,root=r)
projected_tasks=archive_module.original_tasks(request,integrity,root=r)
(s/'small-metadata-preflight.json').write_text(json.dumps({'status':'original_immutable_metadata_and_two_task_projections_verified','original_jobs':[x['job'] for x in projected_tasks],'sequence_roster_read':False,'sequence_bytes_read':False},indent=2))
integrity.ordinary(r/'results',r,directory=True)
archive_parent=r/'results/storage-archives';archive_parent.mkdir(exist_ok=True)
integrity.ordinary(archive_parent,r,directory=True)
script=s/'archive.sbatch';text=__SCRIPT__
text=text.replace('WRAPPER_SHA',pins['archive-orthology-sequence-copies-20261104.py']).replace('REQUEST_SHA',request_sha)
assert '\\r' not in text;script.write_bytes(text.encode());assert script.read_bytes()==text.encode()
def mutate(command,kind):
 with (s/(kind+'-intent.json')).open('x') as stream:json.dump(command,stream);stream.flush();os.fsync(stream.fileno())
 try:result=subprocess.run(command,capture_output=True,text=True,timeout=90)
 except BaseException as error:
  (s/(kind+'-response.json')).write_text(json.dumps({'error_type':type(error).__name__,'error':str(error),'replay_permitted':False}));raise
 with (s/(kind+'-response.json')).open('x') as stream:json.dump({'code':result.returncode,'stdout':result.stdout,'stderr':result.stderr},stream);stream.flush();os.fsync(stream.fileno())
 return result
submitted=mutate(['sbatch','--parsable',str(script)],'submit');submitted.check_returncode()
assert re.fullmatch(r'[1-9][0-9]*(?:;[A-Za-z0-9_.-]+)?\\n?',submitted.stdout),'One actual parsable submission acknowledgement required'
job=submitted.stdout.strip().split(';')[0]
def observe():
 result=native(['scontrol','show','job',job,'-o']);fields=dict(part.split('=',1) for part in shlex.split(result.stdout) if '=' in part)
 assert fields['JobId']==job and fields['UserId']=='pwa209(3164768)' and fields['Account']=='def-ptewarie_cpu'
 assert fields['Command']==str(script) and fields['JobName']=='hhw-orthology-sequence-archive-20261104'
 assert fields['NumCPUs']=='1' and fields['NumNodes'] in {'1','1-1'} and fields['MinMemoryNode'] in {'8G','8192M'} and fields['TimeLimit']=='03:00:00'
 assert fields['Requeue']=='0' and 'ArrayJobId' not in fields and all('gpu' not in fields.get(key,'').lower() for key in ['AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask'])
 return fields
held=observe();assert held['JobState']=='PENDING' and held['Reason']=='JobHeldUser'
# The known site memory-unit NOTE is informational only after the actual
# held allocation has independently matched every identity/resource field.
warning=submitted.stderr
assert not warning or warning=='sbatch: NOTE: Your memory request of 8192.0M was likely submitted as 8.0G. Please note that Slurm interprets memory requests denominated in G as multiples of 1024M, not 1000M.\\n','Unknown submission stderr; inspect actual held job, never resubmit'
(s/'held.json').write_text(json.dumps(held,indent=2))
record={'job_id':job,'request_sha256':request_sha,'wrapper_sha256':pins['archive-orthology-sequence-copies-20261104.py'],'native_tests':__TEST_COUNT__,'candidate_files':43145,'actual_held':held,'submission_stderr_preserved':True}
(s/'held-registration.json').write_text(json.dumps(record,indent=2))
def fields(ident):
 result=native(['scontrol','show','job',ident,'-o']);return dict(part.split('=',1) for part in shlex.split(result.stdout) if '=' in part)
try:
 # Preserve the source-declared logical predecessor. Its already fulfilled
 # and possibly purged Slurm ID is not reintroduced as a native constraint.
 census_phase='study-census-resilience-v1-20261024'
 census_plan_path=r/'plans'/census_phase/'plan.json';census_ledger_path=r/'state'/(census_phase+'-submission.json')
 census_plan=json.loads(census_plan_path.read_bytes());census_ledger=json.loads(census_ledger_path.read_bytes())
 canonical=lambda value:hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
 census_source='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1';census_config='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
 assert census_plan['phase']==census_phase and census_plan['root']==str(r)
 assert census_plan['release_identity']['sha256']==census_ledger['source_sha256']==census_source and census_plan['config_sha256']==census_ledger['config_sha256']==census_config
 assert census_ledger['plan_sha256']==canonical(census_plan)
 census_tasks={task['name']:task for task in census_plan['tasks']};assert len(census_tasks)==len(census_plan['tasks'])
 for number,ident in [(60,'6983031'),(61,'6983037'),(62,'6983056'),(63,'6983059'),(64,'6983060')]:
  name='rolling-storage-census-'+str(number);task=census_tasks[name];row=census_ledger['tasks'][name]
  assert row['state']=='submitted' and row['job_id']==ident and task['resources']['cpus']==8 and task['resources']['memory']=='64G' and task['resources']['hours']==6 and not task['resources'].get('gpu',False) and task['resources'].get('array') is None
  assert task['script']==str(r/'plans'/census_phase/(name+'.sbatch')) and row['command'][-1]==task['script']
  assert task['script_sha256']==canonical(Path(task['script']).read_text()) and row.get('script_sha256',task['script_sha256'])==task['script_sha256']
 task61=census_tasks['rolling-storage-census-61'];row61=census_ledger['tasks']['rolling-storage-census-61']
 assert task61['dependency_kind']=='afterany' and task61['depends']==['rolling-storage-census-60'] and task61.get('external_dependencies',[])==[]
 assert row61.get('dependency_kind','afterany')=='afterany' and '6983031' in row61['issued_native_dependencies']
 source61=task61['rolling_census_provider'];manifest=source61['source_manifest'];helper61=source61['helper']
 assert manifest['path']==str(r/'maintenance/study-census-resilience-support-v1-20261024/source-manifest.json') and sha(manifest['path'])==manifest['sha256']
 assert helper61['path']==str(r/'maintenance/study-census-resilience-support-v1-20261024/run-fresh-storage-census-resilience-v1-20261024.py') and sha(helper61['path'])==helper61['sha256']
 assert json.loads(Path(manifest['path']).read_bytes())['files'][Path(helper61['path']).name]==helper61['sha256']
 prior60=native(['sacct','-X','-n','-P','-j','6983031','--format=JobIDRaw,User,Account,State%40,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES'])
 prior_rows=[line.split('|') for line in prior60.stdout.splitlines() if line.strip()]
 assert len(prior_rows)==1 and len(prior_rows[0])==8
 prior=prior_rows[0];assert prior[:3]==['6983031','pwa209','def-ptewarie_cpu'] and prior[3].split()[0] in {'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'} and re.fullmatch('[0-9]+:[0-9]+',prior[4]) and prior[5].isdigit() and prior[6].isdigit() and 'gpu' not in prior[7].lower()
 before=fields('6983037');(s/'census-61-before.json').write_text(json.dumps(before,indent=2))
 assert before['JobId']=='6983037' and before['UserId']=='pwa209(3164768)' and before['Account']=='def-ptewarie_cpu' and before['JobState']=='PENDING'
 assert before['Command']==task61['script'] and before['JobName']=='hhw-rolling-storage-census-61' and before['NumCPUs']=='8' and before['MinMemoryNode'] in {'64G','65536M'} and before['TimeLimit'] in {'06:00:00','0-06:00:00'} and before['NumNodes'] in {'1','1-1'}
 assert 'ArrayJobId' not in before and all('gpu' not in before.get(key,'').lower() for key in ['AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask'])
 assert before.get('Dependency') in {'(null)','None',''} and before.get('Reason') not in {'JobHeldUser','JobHeldAdmin'} and int(before['Priority'])>0,'First census no longer pending with zero unfulfilled constraints; preserve held archive'
 queue=native(['squeue','-h','-j','6983037,6983056,6983059,6983060','-o','%i|%u|%a|%T'])
 queue_rows=[line.split('|') for line in queue.stdout.splitlines() if line.strip()]
 assert len(queue_rows)==4 and {value[0] for value in queue_rows}=={'6983037','6983056','6983059','6983060'} and all(len(value)==4 and value[1:]==['pwa209','def-ptewarie_cpu','PENDING'] for value in queue_rows)
 gate={'job_id':'6983037','archive_job_id':job,'script':locator(task61['script']),'original_predecessor_job_id':'6983031','original_predecessor_native':prior,'original_predecessor_native_stdout':prior60.stdout,'source_plan':locator(census_plan_path),'source_ledger':locator(census_ledger_path),'source_sha256':census_source,'config_sha256':census_config,'provider':source61,'previous_native_dependency':before['Dependency'],'actual_before':before,'logical_original_afterany_satisfied':True}
 update=mutate(['scontrol','update','JobId=6983037','Dependency=afterany:'+job],'census-dependency-update')
 after=fields('6983037');(s/'census-61-after.json').write_text(json.dumps(after,indent=2))
 queue=native(['squeue','-h','-j','6983037,6983056,6983059,6983060','-o','%i|%u|%a|%T'])
 positive_gate=archive_module.gate_fields(after,queue.stdout,gate,job,root=r)
 gate['actual_after']=after;gate['positive_native_boundary']=positive_gate
 record['census_gate']=gate
 # This immutable registration is a per-file runtime pin. Release evidence
 # goes in a separate document and never rewrites this admitted proof.
 with (s/'registration.json').open('x') as stream:json.dump(record,stream,indent=2);stream.flush();os.fsync(stream.fileno())
except BaseException as error:
 report={'status':'needs_explicit_reconciliation_archive_preserved_held','job_id':job,'error_type':type(error).__name__,'error':str(error),'automatic_replay_permitted':False,'data_action_started':False}
 (s/'needs-reconciliation.json').write_text(json.dumps(report,indent=2));print('ORTHOLOGY_ARCHIVE_HELD '+json.dumps(report),flush=True);raise
released_response=mutate(['scontrol','release',job],'release')
released=observe();assert released['JobState']=='RUNNING' or released['JobState']=='PENDING' and released['Reason'] not in {'JobHeldUser','JobHeldAdmin'} and int(released['Priority'])>0
(s/'actual-unheld.json').write_text(json.dumps(released,indent=2));print('ORTHOLOGY_SEQUENCE_ARCHIVE_REGISTRATION '+json.dumps({'job_id':job,'request_sha256':request_sha,'wrapper_sha256':record['wrapper_sha256'],'native_tests':__TEST_COUNT__,'census_gate':positive_gate,'actual_unheld':released}),flush=True)
PY
'''
request_build='''audit_case=r/'maintenance/orthology-storage-audit-20261104'
audit_path=audit_case/'result.json';assert sha(audit_path)=='9532a2372dbafaad99976c7e079ca2556b8365b00b8ec90ef8907aa6e1ee0415'
audit=json.loads(audit_path.read_bytes());audit_registration=json.loads((audit_case/'registration.json').read_bytes());audit_request=json.loads((audit_case/'request.json').read_bytes())
assert audit['status']=='read_only_orthology_storage_audit_complete' and audit['native_job_id']=='8508393' and audit['files_removed']==0 and audit['input_files_changed'] is False
assert audit['source_sha256']==audit_registration['source_sha256']==audit_request['audit_source_sha256']==sha(audit_case/'audit.py')=='5501f7682b65cf52a30b39a86eced038c30a72375dfb5a3d9c3490a66c57de33'
assert audit_registration['native_job_id']=='8508393' and audit_registration['request_sha256']==audit['request_sha256']==sha(audit_case/'request.json')=='a9b0180fe27845062a17a00d67072705e4ea0967bf7e9b75d05b7bf45c67123a'
assert audit_registration['script_sha256']==audit_request['script_sha256']==sha(audit_case/'audit.sbatch')=='f94340219e30ab5556c5e7b79e0ed8c56bc4ee3efdc5d8270431c3cb877d5ea7'
assert not audit['inventory']['anomalies'] and not audit['metadata']['problems']
expected_scopes={
 'data/molecular/orthology-repair-v7-20261013/of_run/Results_Oct13/Orthogroup_Sequences':12597,
 'data/molecular/orthology-repair-v7-20261013/of_run/Results_Oct13/Single_Copy_Orthologue_Sequences':5429,
 'data/molecular/orthology_mantis/proteomes/OrthoFinder/Results_Oct21/Orthogroup_Sequences':21596,
 'data/molecular/orthology_mantis/proteomes/OrthoFinder/Results_Oct21/Single_Copy_Orthologue_Sequences':3523}
assert sum(expected_scopes.values())==43145
for scope,count in expected_scopes.items():assert audit['inventory']['named_scope_counts'][scope]=={'directories':1,'regular_files':count}
retained=[]
for table in audit['inventory']['required_original_tables']:
 assert table['preserve'] is True and sha(table['path'])==table['sha256']
 retained.append({'path':table['path'],'sha256':table['sha256']})
assert len(retained)==4 and len({x['path'] for x in retained})==4
# Bind the audited immutable molecular records. Original submission ledgers
# remain mutable registries: validate only their two actual task projections.
binding_paths=[audit_case/'audit.py',audit_case/'request.json',audit_case/'audit.sbatch',audit_case/'registration.json']
metadata=audit['metadata']['documents']
for name,observed in sorted(metadata.items()):
 path=Path(name)
 if observed['status']=='read' and not name.startswith(str(r/'plans')+'/') and not (path.parent==r/'state' and ('submission' in path.name or 'registry' in path.name)):
  assert sha(name)==observed['sha256'],'Audited immutable molecular record changed'
  binding_paths.append(Path(name))
bindings=[locator(path) for path in dict.fromkeys(binding_paths)]
release='20261013-829e3817bee1-0e2b1ebce8bf'
task_specs=[('5455844','molecular-pilot',r/('state/'+release+'-managed-initial-submission.json'),r/('plans/'+release+'-managed-initial/plan.json')),
 ('6632058','mantis-orthology',r/('state/'+release+'-molecular-corroboration-submission.json'),r/('plans/'+release+'-molecular-corroboration/plan.json'))]
tasks=[]
for job,name,ledger_path,plan_path in task_specs:
 if str(plan_path) in metadata:assert sha(plan_path)==metadata[str(plan_path)]['sha256']
 plan=json.loads(plan_path.read_bytes());ledger=json.loads(ledger_path.read_bytes());selected=[task for task in plan['tasks'] if task['name']==name]
 assert len(selected)==1;task=selected[0];script_path=Path(task['script'])
 row=ledger['tasks'][name];assert row['state']=='submitted' and row['job_id']==job and row.get('script_sha256',task['script_sha256'])==task['script_sha256']
 assert ledger['plan_sha256']==hashlib.sha256(json.dumps(plan,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
 assert hashlib.sha256(json.dumps(script_path.read_text(),sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()==task['script_sha256']
 assert isinstance(row.get('command'),list) and row['command'][-1]==str(script_path)
 tasks.append({'job':job,'task_name':name,'ledger_path':str(ledger_path),'plan':locator(plan_path),'script':locator(script_path)})
request={'case':str(r/'results/storage-archives/orthology-sequences-20261104'),'original_source_release':'/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf','helper':{'path':str(r/'maintenance/terminal-controller-diagnostic-support-v4-20261104/compact-terminal-controller-reads-20261104.py'),'sha256':pins['compact-terminal-controller-reads-20261104.py']},'audit':locator(audit_path),'binding_documents':bindings,'retained_tables':retained,'original_tasks':tasks}
assert not Path(request['case']).exists()
'''
script='''#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-orthology-sequence-archive-20261104
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=03:00:00
#SBATCH --hold
#SBATCH --no-requeue
#SBATCH --output=/scratch/pwa209/horsehair-worm/maintenance/orthology-sequence-archive-support-20261104/native.log
set -euo pipefail
test "$(id -un)" = pwa209
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src:"${PYTHONPATH:-}"
export PYTHONDONTWRITEBYTECODE=1 TZ=UTC OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
cd /scratch/pwa209/horsehair-worm/maintenance/orthology-sequence-archive-support-20261104
python -s -B - <<'SOURCE'
from pathlib import Path
import hashlib,json
manifest=json.loads(Path('source-manifest.json').read_bytes())
assert manifest=={'files':__PINS__,'native_tests':__TEST_COUNT__,'native_roster_sha256':'__ROSTER_SHA__'}
for name,digest in manifest['files'].items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==digest
SOURCE
python -s -B -m pytest test_archive_orthology_sequence_copies_20261104.py -q -p no:cacheprovider --basetemp "$SLURM_TMPDIR/orthology-sequence-archive-tests" --junitxml tests.xml
python -s -B - <<'CHECK'
import xml.etree.ElementTree as E,hashlib,json
cases=list(E.parse('tests.xml').getroot().iter('testcase'))
assert len(cases)==__TEST_COUNT__ and not any(list(case.iter(kind)) for case in cases for kind in ['failure','error','skipped'])
roster=sorted(case.get('classname','').split('.')[-1]+'::'+case.get('name','') for case in cases)
assert len(set(roster))==len(roster) and hashlib.sha256(json.dumps(roster,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()=='__ROSTER_SHA__'
CHECK
python -s -B archive-orthology-sequence-copies-20261104.py --request /scratch/pwa209/horsehair-worm/maintenance/orthology-sequence-archive-support-20261104/request.json --request-sha256 REQUEST_SHA --wrapper-sha256 WRAPPER_SHA
'''
script=script.replace('__PINS__',repr(pins)).replace('__TEST_COUNT__',str(args.native_tests)).replace('__ROSTER_SHA__',canonical(roster))
body=body.replace('__REQUEST_BUILD__',request_build).replace('__SCRIPT__',repr(script)).replace('__PAYLOAD__',repr(payload)).replace('__PINS__',repr(pins)).replace('__TEST_COUNT__',str(args.native_tests)).replace('__ROSTER_SHA__',canonical(roster))
code=body.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0];compile(code,'<orthology-sequence-archive-registration>','exec')
assert '\r' not in body and '--mem=8G' in script and '--time=03:00:00' in script and '--hold' in script
Path('work/hhw-orthology-sequence-archive-20261104-975.sh').write_bytes(body.encode())
manifest={'files':pins,'native_tests':args.native_tests,'native_roster_sha256':canonical(roster)}
Path('work/orthology-sequence-archive-build-20261104.json').write_text(json.dumps(manifest,indent=2))
print(json.dumps(manifest))
