"""Build the finite, independently verified archived-reader directory prune."""
from pathlib import Path
import argparse,base64,hashlib,json,subprocess,sys

p=argparse.ArgumentParser();p.add_argument('--native-tests',required=True,type=int);args=p.parse_args()
names=['prune-archived-diagnostic-directories-20261104.py','test_prune_archived_diagnostic_directories_20261104.py','compact-terminal-controller-reads-20261104.py']
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
import os,json,hashlib,base64,subprocess,shlex,re
from horsehair import budget,slurm
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');s=r/'maintenance/diagnostic-empty-directory-support-20261104'
assert not s.exists(),'Existing registration must be reconciled, never replayed'
s.mkdir()
payload=__PAYLOAD__
pins=__PINS__
for name,value in payload.items():
 p=s/name;p.write_bytes(base64.b64decode(value));assert hashlib.sha256(p.read_bytes()).hexdigest()==pins[name]
(s/'source-manifest.json').write_text(json.dumps({'files':pins,'native_tests':__TEST_COUNT__,'native_roster_sha256':'__ROSTER_SHA__'},sort_keys=True,indent=2))
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def locator(path):return {'path':str(path),'sha256':sha(path)}
def native(command):
 p=subprocess.run(command,capture_output=True,text=True,timeout=90);p.check_returncode();assert not p.stderr;return p
# Positive native final accounting precedes the new maintenance lease. No old
# pending reservation can be silently carried into the next finite allocation.
expected_native={'8425411':['FAILED','1:0','3470','1'],'8463111':['COMPLETED','0:0','4600','1']}
prior_native=native(['sacct','-X','-n','-P','-j',','.join(expected_native),'--format=JobIDRaw,State,ExitCode,ElapsedRaw,AllocCPUS,User,Account,AllocTRES'])
rows=[line.split('|') for line in prior_native.stdout.splitlines() if line.strip()]
assert len(rows)==2 and {row[0] for row in rows}==set(expected_native)
for row in rows:
 assert row[1:5]==expected_native[row[0]] and row[5:7]==['pwa209','def-ptewarie_cpu'] and 'gpu' not in row[7].lower()
(s/'producers-native-final.json').write_text(json.dumps({'actual_native_rows':rows,'native_stdout':prior_native.stdout,'cost_basis':'actual final allocations; incomplete v4 outcome preserved'},indent=2))
budget.native_accounting(r,list(expected_native),runner=lambda command,**kw:subprocess.run(command,**{**kw,'timeout':90}))
cost_proofs={}
for job,expected in expected_native.items():
 proof=slurm.completed_native_dependency(r,job,dependency_kind='afterany');assert proof is not None
 cost=proof['native_records'][job]
 assert [cost['state'],cost['exit_code'],cost['elapsed_seconds'],cost['cpus'],cost['gpus']]==[expected[0],expected[1],int(expected[2]),1,0]
 cost_proofs[job]=proof
(s/'producers-framework-accounting.json').write_text(json.dumps(cost_proofs,indent=2))
definitions={
 '8425411':{'support':'terminal-controller-diagnostic-support-v3-20261103','case':'terminal-controller-diagnostic-compaction-v3-20261103','request':'e5616c613b646818c9647087583ce22f3a844b1466992276ae3f9f5950ec88f6','helper':'compact-terminal-controller-reads-20261103.py','wrapper':'5019e6f543cd6a93195edf6e8db0319eaa7fc2d1abd8cc48e74db06465ff7550','proof':'progress.json','count':3},
 '8463111':{'support':'terminal-controller-diagnostic-support-v4-20261104','case':'terminal-controller-diagnostic-compaction-v4-20261104','request':'c5bcaa0c380e9676847fda4b464d8e8e61f0f812cea5e48453f6ee1b71b98105','helper':'compact-terminal-controller-reads-20261104.py','wrapper':'e8a599304f4ac2561339114e7965f3cca2f06c87d565822bb32aabbdf322d979','proof':'result.json','count':27}}
producers=[];transactions=[];member_files=0;v4request=None
for job,d in definitions.items():
 support=r/'maintenance'/d['support'];case=r/'maintenance'/d['case']
 assert sha(support/'request.json')==d['request'] and sha(support/d['helper'])==d['wrapper']
 registration=json.loads((support/'registration.json').read_bytes());original=json.loads((support/'request.json').read_bytes());proof=json.loads((case/d['proof']).read_bytes())
 assert registration['job_id']==job and registration['request_sha256']==d['request'] and original['case']==str(case)
 script_text=(support/'controller-compact.sbatch').read_text();assert d['wrapper'] in script_text and d['request'] in script_text and d['helper'] in script_text
 completed=proof['completed'];assert len(completed)==d['count'] and len({x['job'] for x in completed})==d['count']
 if job=='8425411':assert [x['job'] for x in completed]==['7046213','6689295','6701493'] and sum(x['files_removed'] for x in completed)==13130
 else:
  v4request=original
  assert proof['status']=='terminal_controller_diagnostic_recovery_incomplete' and proof['minimum_met'] is False and proof['files_removed']==33782 and proof['prior_verified_files_removed']==13130 and proof['current_pass_files_removed']==20652
  assert {x['job'] for x in proof['ineligible_sources']}=={'6950429','7028292'} and {x['job'] for x in proof['prior_verified']}=={'7046213','6689295','6701493'}
 producer={'job':job,'archive_base':str(case),'registration':locator(support/'registration.json'),'request':locator(support/'request.json'),'script':locator(support/'controller-compact.sbatch'),'proof':locator(case/d['proof'])}
 producers.append(producer)
 for item in completed:
  controller=item['job'];directory=case/('controller-'+controller);assert item['archive']==str(directory)
  transaction={'job':controller,'producer_job':job,'archive_directory':str(directory)}
  for kind in ['intent','receipt','removal']:transaction[kind]=locator(directory/(kind+'.json'))
  intent=json.loads((directory/'intent.json').read_bytes());receipt=json.loads((directory/'receipt.json').read_bytes());removal=json.loads((directory/'removal.json').read_bytes())
  assert intent['authority']['wrapper_sha256']==d['wrapper'] and intent['authority']['request_sha256']==d['request']
  assert receipt['status']=='independent_roundtrip_verified_originals_retained' and receipt['intent_sha256']==transaction['intent']['sha256'] and receipt['archive_sha256']==item['archive_sha256'] and receipt['files']==item['files_removed']
  assert removal['status']=='complete_terminal_diagnostic_compaction' and removal['intent_sha256']==transaction['intent']['sha256'] and removal['archive_sha256']==receipt['archive_sha256'] and removal['removed_count']==item['files_removed']
  assert len(intent['members'])==item['files_removed'] and removal['last_removed']==intent['members'][-1]['path']
  transactions.append(transaction);member_files+=item['files_removed']
assert len(transactions)==30 and len({x['job'] for x in transactions})==30 and member_files==33782
# These are the existing three canonical pins, never refreshed to excuse a
# changed state. Only small known transaction metadata is read on the login
# host; tar verification and all directory I/O happen on the compute node.
campaign=r/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
required={str(campaign/name) for name in ['state.json.gz','campaign-plan.json','state-binding.json']}
bindings=[dict(x) for x in v4request['binding_documents'] if x['path'] in required]
assert len(bindings)==3 and {x['path'] for x in bindings}==required
for pin in bindings:assert sha(pin['path'])==pin['sha256'],'Original canonical binding changed'
assert next(x['sha256'] for x in bindings if x['path']==str(campaign/'state.json.gz'))=='51207bcceb4e8b126b9a335975fa0399d30ed2a1c8285350ea65cce6dc0e0ced'
request={'case':str(r/'maintenance/diagnostic-empty-directory-recovery-20261104'),'helper':{'path':str(r/'maintenance/terminal-controller-diagnostic-support-v4-20261104/compact-terminal-controller-reads-20261104.py'),'sha256':pins['compact-terminal-controller-reads-20261104.py']},'binding_documents':bindings,'protected_paths':[],'producers':producers,'transactions':transactions}
assert not Path(request['case']).exists()
raw=(json.dumps(request,sort_keys=True,indent=2)+'\\n').encode();(s/'request.json').write_bytes(raw);request_sha=hashlib.sha256(raw).hexdigest()
(s/'verified-input-frontier.json').write_text(json.dumps({'transactions':30,'archived_member_files':33782,'native_archive_verification_pending':True,'archive_replay':False,'v4_minimum_met':False,'scientific_result':False,'admission_authority':False},indent=2))
script=s/'prune.sbatch';text=__SCRIPT__
text=text.replace('WRAPPER_SHA',pins['prune-archived-diagnostic-directories-20261104.py']).replace('REQUEST_SHA',request_sha);script.write_text(text)
def mutate(command,kind):
 with (s/(kind+'-intent.json')).open('x') as stream:json.dump(command,stream);stream.flush();os.fsync(stream.fileno())
 try:p=subprocess.run(command,capture_output=True,text=True,timeout=90)
 except BaseException as error:
  (s/(kind+'-response.json')).write_text(json.dumps({'error_type':type(error).__name__,'error':str(error),'replay_permitted':False}));raise
 with (s/(kind+'-response.json')).open('x') as stream:json.dump({'code':p.returncode,'stdout':p.stdout,'stderr':p.stderr},stream);stream.flush();os.fsync(stream.fileno())
 return p
p=mutate(['sbatch','--parsable',str(script)],'submit');p.check_returncode()
job=p.stdout.strip().split(';')[0];assert re.fullmatch('[1-9][0-9]*',job)
def observe():
 p=native(['scontrol','show','job',job,'-o']);f=dict(part.split('=',1) for part in shlex.split(p.stdout) if '=' in part)
 assert f['JobId']==job and f['UserId']=='pwa209(3164768)' and f['Account']=='def-ptewarie_cpu'
 assert f['Command']==str(script) and f['JobName']=='hhw-empty-diagnostic-directory-prune-20261104'
 assert f['NumCPUs']=='1' and f['NumNodes'] in {'1','1-1'} and f['MinMemoryNode'] in {'8G','8192M'} and f['TimeLimit']=='02:00:00'
 assert f['Requeue']=='0' and 'ArrayJobId' not in f and all('gpu' not in f.get(key,'').lower() for key in ['AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask'])
 return f
held=observe();assert held['JobState']=='PENDING' and held['Reason']=='JobHeldUser'
(s/'held.json').write_text(json.dumps(held,indent=2))
p=mutate(['scontrol','release',job],'release')
released=observe();assert released['JobState']=='RUNNING' or released['JobState']=='PENDING' and released['Reason'] not in {'JobHeldUser','JobHeldAdmin'} and int(released['Priority'])>0
record={'job_id':job,'request_sha256':request_sha,'native_tests':__TEST_COUNT__,'transactions':30,'archived_member_files':33782,'producer_job_ids':list(expected_native),'actual_unheld':released}
(s/'registration.json').write_text(json.dumps(record,indent=2));print('EMPTY_DIAGNOSTIC_DIRECTORY_PRUNE_REGISTRATION '+json.dumps(record),flush=True)
PY
'''
script='''#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-empty-diagnostic-directory-prune-20261104
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=02:00:00
#SBATCH --hold
#SBATCH --no-requeue
#SBATCH --output=/scratch/pwa209/horsehair-worm/maintenance/diagnostic-empty-directory-support-20261104/native.log
set -euo pipefail
test "$(id -un)" = pwa209
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src:"${PYTHONPATH:-}"
export PYTHONDONTWRITEBYTECODE=1 TZ=UTC OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
cd /scratch/pwa209/horsehair-worm/maintenance/diagnostic-empty-directory-support-20261104
python -s -B -m pytest test_prune_archived_diagnostic_directories_20261104.py -q -p no:cacheprovider --basetemp "$SLURM_TMPDIR/diagnostic-directory-tests" --junitxml tests.xml
python -s -B - <<'CHECK'
import xml.etree.ElementTree as E,hashlib,json
cases=list(E.parse('tests.xml').getroot().iter('testcase'))
assert len(cases)==__TEST_COUNT__ and not any(list(case.iter(kind)) for case in cases for kind in ['failure','error','skipped'])
roster=sorted(case.get('classname','').split('.')[-1]+'::'+case.get('name','') for case in cases)
assert len(set(roster))==len(roster) and hashlib.sha256(json.dumps(roster,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()=='__ROSTER_SHA__'
CHECK
python -s -B prune-archived-diagnostic-directories-20261104.py --request /scratch/pwa209/horsehair-worm/maintenance/diagnostic-empty-directory-support-20261104/request.json --request-sha256 REQUEST_SHA --wrapper-sha256 WRAPPER_SHA
'''
script=script.replace('__TEST_COUNT__',str(args.native_tests)).replace('__ROSTER_SHA__',canonical(roster))
body=body.replace('__SCRIPT__',repr(script)).replace('__PAYLOAD__',repr(payload)).replace('__PINS__',repr(pins)).replace('__TEST_COUNT__',str(args.native_tests)).replace('__ROSTER_SHA__',canonical(roster))
code=body.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0];compile(code,'<empty-directory-prune-registration>','exec')
assert '--mem=8G' in script and '--time=02:00:00' in script and '--hold' in script
Path('work/hhw-empty-directory-prune-20261104-959.sh').write_text(body,newline='\n')
manifest={'files':pins,'native_tests':args.native_tests,'native_roster_sha256':canonical(roster)}
Path('work/diagnostic-empty-prune-build-20261104.json').write_text(json.dumps(manifest,indent=2))
print(json.dumps(manifest))
