from pathlib import Path
import argparse,base64,hashlib,json,subprocess,sys

p=argparse.ArgumentParser();p.add_argument('--native-tests',required=True,type=int);args=p.parse_args()
names=['compact-terminal-controller-reads-20261104.py','test_compact_terminal_controller_reads_20261104.py']
response=subprocess.run([sys.executable,'-m','pytest','work/'+names[1],'--collect-only','-q','-p','no:cacheprovider'],text=True,capture_output=True,check=True)
roster=sorted(line.split('::',1)[0].replace('\\','/').split('/')[-1][:-3]+'::'+line.split('::',1)[1]
    for line in response.stdout.splitlines() if '::' in line)
assert len(roster)==len(set(roster))==args.native_tests and args.native_tests>=68
canonical=lambda value:hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
payload={name:base64.b64encode(Path('work',name).read_bytes()).decode() for name in names}
pins={name:hashlib.sha256(Path('work',name).read_bytes()).hexdigest() for name in names}
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
r=Path('/scratch/pwa209/horsehair-worm');s=r/'maintenance/terminal-controller-diagnostic-support-v4-20261104'
assert not s.exists(),'Existing registration must be reconciled, never replayed'
s.mkdir()
payload=__PAYLOAD__
pins=__PINS__
for name,value in payload.items():
 p=s/name;p.write_bytes(base64.b64decode(value));assert hashlib.sha256(p.read_bytes()).hexdigest()==pins[name]
(s/'source-manifest.json').write_text(json.dumps({'files':pins,'native_tests':__TEST_COUNT__,'native_roster_sha256':'__ROSTER_SHA__'},sort_keys=True,indent=2))
def native(command):
 p=subprocess.run(command,capture_output=True,text=True,timeout=90);p.check_returncode();assert not p.stderr;return p
prior_native=native(['sacct','-X','-n','-P','-j','8425411','--format=JobIDRaw,State,ExitCode,ElapsedRaw,AllocCPUS,User,Account,AllocTRES'])
rows=[line.split('|') for line in prior_native.stdout.splitlines() if line.strip()]
assert len(rows)==1 and rows[0][:7]==['8425411','FAILED','1:0','3470','1','pwa209','def-ptewarie_cpu']
assert 'gpu' not in rows[0][7].lower()
(s/'prior-attempt-native-final.json').write_text(json.dumps({'job_id':'8425411','actual_native_fields':rows[0],'native_stdout':prior_native.stdout,'cost_basis':'actual final allocation; preserved failed attempt'},indent=2))
budget.native_accounting(r,['8425411'],runner=lambda command,**kw:subprocess.run(command,**{**kw,'timeout':90}))
prior_cost=slurm.completed_native_dependency(r,'8425411',dependency_kind='afterany')
assert prior_cost is not None
cost=prior_cost['native_records']['8425411']
assert [cost['state'],cost['exit_code'],cost['elapsed_seconds'],cost['cpus'],cost['gpus']]==['FAILED','1:0',3470,1,0]
(s/'prior-attempt-framework-accounting.json').write_text(json.dumps(prior_cost,indent=2))
old_support=r/'maintenance/terminal-controller-diagnostic-support-v3-20261103'
old_case=r/'maintenance/terminal-controller-diagnostic-compaction-v3-20261103'
old_request_raw=(old_support/'request.json').read_bytes()
assert hashlib.sha256(old_request_raw).hexdigest()=='e5616c613b646818c9647087583ce22f3a844b1466992276ae3f9f5950ec88f6'
request=json.loads(old_request_raw);old_registration=json.loads((old_support/'registration.json').read_bytes())
assert old_registration['job_id']=='8425411' and old_registration['request_sha256']==hashlib.sha256(old_request_raw).hexdigest()
assert request['case']==str(old_case) and request['target_free_files']==300000 and request['minimum_files_removed']==60000
progress=json.loads((old_case/'progress.json').read_bytes())['completed']
expected=['7046213','6689295','6701493'];assert [item['job'] for item in progress]==expected
assert sum(item['files_removed'] for item in progress)==13130
prior=[]
for item in progress:
 job=item['job'];directory=old_case/('controller-'+job)
 assert item['archive']==str(directory)
 binding={'job':job,'archive_directory':str(directory)}
 for kind in ['intent','receipt','removal']:
  path=directory/(kind+'.json');raw=path.read_bytes();binding[kind]={'path':str(path),'sha256':hashlib.sha256(raw).hexdigest()}
 receipt=json.loads((directory/'receipt.json').read_bytes());removal=json.loads((directory/'removal.json').read_bytes())
 assert receipt['status']=='independent_roundtrip_verified_originals_retained' and receipt['archive_sha256']==item['archive_sha256']
 assert removal['status']=='complete_terminal_diagnostic_compaction' and removal['removed_count']==item['files_removed']
 prior.append(binding)
request['sources']=[item for item in request['sources'] if item['job'] not in set(expected)]
assert request['sources'] and len({item['job'] for item in request['sources']})==len(request['sources'])
request['case']=str(r/'maintenance/terminal-controller-diagnostic-compaction-v4-20261104')
assert not Path(request['case']).exists()
request['prior_completed']=prior
# Preserve the original explicit pins. A changed original binding cannot be
# made current by hashing it again and relabelling the new hash as provenance.
bindings=list(request['binding_documents'])
for item in request['sources']:bindings.extend(item.get('binding_documents',[]))
for binding in bindings:
 assert hashlib.sha256(Path(binding['path']).read_bytes()).hexdigest()==binding['sha256'],'Original explicit binding changed'
raw=(json.dumps(request,indent=2,sort_keys=True)+'\\n').encode();(s/'request.json').write_bytes(raw);request_sha=hashlib.sha256(raw).hexdigest()
(s/'continuation-source.json').write_text(json.dumps({'original_request_sha256':hashlib.sha256(old_request_raw).hexdigest(),'prior_completed_jobs':expected,'prior_completed_files':13130,'new_sources':len(request['sources']),'old_explicit_pins_preserved':True},indent=2))
script=s/'controller-compact.sbatch';text=__SCRIPT__
text=text.replace('WRAPPER_SHA',pins['compact-terminal-controller-reads-20261104.py']).replace('REQUEST_SHA',request_sha);script.write_text(text)
def mutate(command,kind):
 with (s/(kind+'-intent.json')).open('x') as stream:json.dump(command,stream);stream.flush();os.fsync(stream.fileno())
 try:p=subprocess.run(command,capture_output=True,text=True,timeout=90)
 except BaseException as error:
  (s/(kind+'-response.json')).write_text(json.dumps({'error_type':type(error).__name__,'error':str(error),'replay_permitted':False}))
  raise
 with (s/(kind+'-response.json')).open('x') as stream:json.dump({'code':p.returncode,'stdout':p.stdout,'stderr':p.stderr},stream);stream.flush();os.fsync(stream.fileno())
 return p
command=['sbatch','--parsable',str(script)]
p=mutate(command,'submit');p.check_returncode()
job=p.stdout.strip().split(';')[0];assert re.fullmatch('[1-9][0-9]*',job)
def observe():
 p=native(['scontrol','show','job',job,'-o']);f=dict(part.split('=',1) for part in shlex.split(p.stdout) if '=' in part)
 assert f['JobId']==job and f['UserId']=='pwa209(3164768)' and f['Account']=='def-ptewarie_cpu'
 assert f['Command']==str(script) and f['JobName']=='hhw-terminal-controller-read-compaction-20261104'
 assert f['NumCPUs']=='1' and f['NumNodes'] in {'1','1-1'} and f['MinMemoryNode'] in {'16G','16384M'} and f['TimeLimit']=='12:00:00'
 assert f['Requeue']=='0' and 'ArrayJobId' not in f and all('gpu' not in f.get(key,'').lower() for key in ['AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask'])
 return f
held=observe();assert held['JobState']=='PENDING' and held['Reason']=='JobHeldUser'
(s/'held.json').write_text(json.dumps(held,indent=2))
p=mutate(['scontrol','release',job],'release')
released=observe();assert released['JobState']=='RUNNING' or released['JobState']=='PENDING' and released['Reason'] not in {'JobHeldUser','JobHeldAdmin'} and int(released['Priority'])>0
result={'job_id':job,'request_sha256':request_sha,'sources':len(request['sources']),'prior_completed':prior,'prior_failed_native_job':'8425411','native_tests':__TEST_COUNT__,'actual_unheld':released}
(s/'registration.json').write_text(json.dumps(result,indent=2));print('CONTROLLER_DIAGNOSTIC_CONTINUATION_REGISTRATION '+json.dumps(result),flush=True)
PY
'''
script='''#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-terminal-controller-read-compaction-20261104
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=12:00:00
#SBATCH --hold
#SBATCH --no-requeue
#SBATCH --output=/scratch/pwa209/horsehair-worm/maintenance/terminal-controller-diagnostic-support-v4-20261104/native.log
set -euo pipefail
test "$(id -un)" = pwa209
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src:"${PYTHONPATH:-}"
export PYTHONDONTWRITEBYTECODE=1 TZ=UTC OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
cd /scratch/pwa209/horsehair-worm/maintenance/terminal-controller-diagnostic-support-v4-20261104
python -s -B -m pytest test_compact_terminal_controller_reads_20261104.py -q -p no:cacheprovider --basetemp "$SLURM_TMPDIR/controller-diagnostic-tests" --junitxml tests.xml
python -s -B - <<'CHECK'
import xml.etree.ElementTree as E,hashlib,json
cases=list(E.parse('tests.xml').getroot().iter('testcase'))
assert len(cases)==__TEST_COUNT__ and not any(list(case.iter(kind)) for case in cases for kind in ['failure','error','skipped'])
roster=sorted(case.get('classname','').split('.')[-1]+'::'+case.get('name','') for case in cases)
assert len(set(roster))==len(roster) and hashlib.sha256(json.dumps(roster,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()=='__ROSTER_SHA__'
CHECK
python -s -B compact-terminal-controller-reads-20261104.py --request /scratch/pwa209/horsehair-worm/maintenance/terminal-controller-diagnostic-support-v4-20261104/request.json --request-sha256 REQUEST_SHA --wrapper-sha256 WRAPPER_SHA
'''
script=script.replace('__TEST_COUNT__',str(args.native_tests)).replace('__ROSTER_SHA__',canonical(roster))
body=body.replace('__SCRIPT__',repr(script)).replace('__PAYLOAD__',repr(payload)).replace('__PINS__',repr(pins)).replace('__TEST_COUNT__',str(args.native_tests)).replace('__ROSTER_SHA__',canonical(roster))
code=body.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0];compile(code,'<controller-compactor-v4-registration>','exec')
assert 'verify-stopped-controller-maintenance' not in script
Path('work/hhw-controller-compactor-20261104-946.sh').write_text(body,newline='\n')
Path('work/controller-compactor-v4-build-20261104.json').write_text(json.dumps({'files':pins,'native_tests':args.native_tests,'native_roster_sha256':canonical(roster)},indent=2))
print(json.dumps({'files':pins,'native_tests':args.native_tests,'native_roster_sha256':canonical(roster)}))
