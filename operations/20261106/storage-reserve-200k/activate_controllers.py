"""One finite controller handoff after the qualified census and archive cleanup.

Only the six exact never-started held controller jobs can be cancelled. The
existing worker state is byte-preserved; scientific admission belongs to the
qualified replacement controllers, not this administrative handoff.
"""
from pathlib import Path
from datetime import datetime,timezone
from contextlib import nullcontext
from types import SimpleNamespace
import argparse,gzip,hashlib,importlib.util,json,os,re,shlex,stat,subprocess,sys,time
import xml.etree.ElementTree as ET

ROOT=Path('/scratch/pwa209/horsehair-worm')
BASE=ROOT/'maintenance/storage-reserve-200k-v2-20261106'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
POLICY_MODULE='0e8691671a32e2b55cfcd626d461c2708d11d05b00c0491721c6c3be13107f7d'
POLICY='2e8e2bd2c1f70aa988a9414d2b44d9ffd0f768f8bc2e329747b7839b2855f2d9'
MANIFESTS={'consumers':'9326aae2e0bb3775b23efba2e61fef6a0365f070c18e03383a9f433bde31614b',
 'coordination':'45feaea32e02fce2ca94ca841858920fd6342ec46ab4c7d9a17552775b0f9d53',
 'census':'288af211cad1ad186105e6b2dafa08d4195920b132ab9efef367af98c0fbdf6e'}
PRIMARY_PHASE='storage-reserve-200k-v2-primary-20261106'
AGENT_PHASE='agent-coordination-storage-reserve-200k-v2-20261106'
OLD_PRIMARY=('8559679','8559680','8559681','8559683')
OLD_AGENT=('8559704','8559707','8559708','8559725')
RETIRE=OLD_PRIMARY+OLD_AGENT[2:]
KEEP_HELD='6983060'
TERMINAL={'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}
TEST_SOURCE_SHA='ac87698ac8afcb7d208955e4f875d77982b89576b220fd1598caf8d94380d34b'
TEST_COUNT=62
TEST_ROSTER_SHA='d0009730c331db78767d9a40da0afaaad0adc7ee0c749d414adcf083febe7aef'

class PendingActivationCensus(ValueError):
 """A live publication/finality race admits no controller release."""

def require(ok,message):
 if not ok:raise ValueError(message)

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def canonical(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()

def ordinary(path,root=ROOT):
 p=Path(path);require(p.is_absolute() and p.is_relative_to(root) and p.resolve()==p,'Exact confined path required')
 for parent in [p,*p.parents]:
  if not parent.is_relative_to(root):break
  require(not parent.is_symlink(),'Redirected path refused')
 s=p.lstat();require(stat.S_ISREG(s.st_mode) and s.st_nlink==1,'Ordinary single-link file required')
 if hasattr(os,'geteuid'):require(s.st_uid==os.geteuid(),'Foreign-owned path refused')
 return p

def pin(binding):
 require(type(binding) is dict and set(binding)=={'path','sha256'} and re.fullmatch('[0-9a-f]{64}',str(binding['sha256'])),'Exact immutable locator required')
 p=ordinary(binding['path']);require(sha(p)==binding['sha256'],'Pinned source/evidence changed: '+str(p));return p

def read(binding):return json.loads(pin(binding).read_bytes())
def locator(path):return {'path':str(path),'sha256':sha(path)}

def module(path,expected,name):
 pin({'path':str(path),'sha256':expected});spec=importlib.util.spec_from_file_location(name,path)
 value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);require(sha(path)==expected,'Imported source changed');return value

def save(path,value):
 with Path(path).open('x',encoding='utf-8',newline='\n') as f:json.dump(value,f,sort_keys=True,indent=2,allow_nan=False);f.write('\n');f.flush();os.fsync(f.fileno())

def seconds(text):
 m=re.fullmatch(r'(?:(\d+)-)?(\d+):(\d\d):(\d\d)',text);require(m is not None,'Finite native duration required')
 d,h,n,s=(int(x or 0) for x in m.groups());return d*86400+h*3600+n*60+s

def native_fields(response):
 response.check_returncode();require(not response.stderr,'Native scheduler read error refused')
 return dict(x.split('=',1) for x in shlex.split(response.stdout) if '=' in x)

def own_registration(args,argv,job,fields,*,wall=time.time):
 from horsehair.core import canonical_hash
 plan=json.loads(ordinary(args.native_plan).read_bytes());ledger=json.loads(ordinary(args.native_ledger).read_bytes())
 tasks=[t for t in plan['tasks'] if t['name']==args.native_task];require(len(tasks)==1,'One actual activation task required');task=tasks[0]
 row=ledger['tasks'][task['name']];text=ordinary(args.native_script).read_text();tokens=shlex.split(task['command'])
 require(plan['root']==str(ROOT) and plan['config_sha256']==ledger['config_sha256']==CONFIG
  and plan['release_identity']['sha256']==ledger['source_sha256']==SOURCE and ledger['plan_sha256']==canonical_hash(plan),'Actual registered source/config/plan pair required')
 require(tokens.count(str(BASE/'activate_controllers.py'))==1,'One exact registered activation entry required')
 tail=tokens[tokens.index(str(BASE/'activate_controllers.py'))+1:]
 require(tail.count('--native-tests-junit')==1 and tail[tail.index('--native-tests-junit')+1]=='activation-tests.xml',
  'One exact literal allocation-local native JUnit basename required')
 require(tail==argv
  and task['command'] in text and task['script']==str(args.native_script)
  and task['script_sha256']==row['script_sha256']==canonical_hash(text) and row['state']=='submitted' and row['job_id']==job
  and task['resources']=={'cpus':2,'memory':'16G','hours':1,'gpu':False,'array':None},'Exact activation command/script/native/resource binding required')
 require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)' and fields.get('Account')=='def-ptewarie_cpu'
  and fields.get('JobState')=='RUNNING' and fields.get('Command')==str(args.native_script) and fields.get('JobName')=='hhw-'+args.native_task
  and fields.get('NumCPUs')=='2' and fields.get('NumNodes') in {'1','1-1'} and fields.get('MinMemoryNode') in {'16G','16384M'}
  and seconds(fields['TimeLimit'])==3600 and fields.get('Requeue')=='0' and 'ArrayJobId' not in fields
  and all('gpu' not in fields.get(k,'').lower() for k in ['AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask']),'Exact owned2CPU16G1h no-requeue activation allocation required')
 end=datetime.fromisoformat(fields['EndTime']).replace(tzinfo=timezone.utc).timestamp()
 require(abs(end-float(os.environ['SLURM_JOB_END_TIME']))<=2 and wall()+900<end,'Actual finite activation end required')
 return end,plan,ledger

def successful(rows,spec):
 for job,cpus in spec.items():
  row=rows.get(job)
  if row is None or row['state'] not in TERMINAL:return False
  require(row['state']=='COMPLETED' and row['exit_code']=='0:0' and row['cpus']==cpus and row['elapsed_seconds']>0,'Required native predecessor failed: '+job)
 return True

def qualification_identity(kind,report,job):
 contracts={'census':('slurm_job_id','complete_census_resilience_operational_qualification'),
  'consumers':('native_job_id','complete_census_resilience_consumer_qualification'),
  'coordination':('native_job_id','qualified_agent_coordination_repair')}
 require(kind in contracts,'Exact known qualification component required');field,status=contracts[kind]
 require(report.get(field)==job and report.get('status')==status and type(report.get('returncode')) is int and report['returncode']==0,
  'Exact actual component qualification identity/schema required')

def scalar_cpu_controller(fields):
 tres={key:value for key,sep,value in (x.partition('=') for x in fields.get('ReqTRES','').split(',')) if sep}
 require(fields.get('NumNodes') in {'1','1-1'} and fields.get('Requeue')=='0' and fields.get('Restarts')=='0'
  and 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields and tres.get('cpu')=='1' and tres.get('node')=='1'
  and tres.get('mem') in {'64G','65536M','67108864K'}
  and all('gpu' not in fields.get(k,'').lower() for k in ['AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask']),
  'Exact never-requeued scalar CPU-only controller allocation required')

def parse_accounting(text):
 result={}
 for line in text.splitlines():
  v=line.split('|')
  if len(v)<7 or not v[0]:continue
  require(v[0] not in result and re.fullmatch('[1-9][0-9]*',v[0]) and v[1:3]==['pwa209','def-ptewarie_cpu'],'Exact owned scalar native accounting required')
  result[v[0]]={'state':v[3].split()[0],'exit_code':v[4],'elapsed_seconds':int(v[5]),'cpus':int(v[6])}
 return result

def held_controller(fields,job,task):
 scalar_cpu_controller(fields)
 require(job in RETIRE and fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
  and fields.get('Account')=='def-ptewarie_cpu' and fields.get('JobState')=='PENDING' and fields.get('Reason')=='JobHeldUser'
  and fields.get('RunTime')=='00:00:00' and fields.get('JobName')=='hhw-'+task['name'] and fields.get('Command')==task['script']
  and fields.get('NumCPUs')=='1' and fields.get('MinMemoryNode') in {'64G','65536M'} and 'ArrayJobId' not in fields,
  'Only an exact never-started held old controller may be retired')

def held_replacement(fields,job,task,row):
 scalar_cpu_controller(fields)
 require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)' and fields.get('Account')=='def-ptewarie_cpu'
  and fields.get('JobState')=='PENDING' and fields.get('Reason')=='JobHeldUser' and fields.get('RunTime')=='00:00:00'
  and fields.get('Command')==task['script'] and fields.get('JobName')=='hhw-'+task['name'] and fields.get('NumCPUs')=='1'
  and fields.get('MinMemoryNode') in {'64G','65536M'} and seconds(fields['TimeLimit'])==task['resources']['hours']*3600,
  'Exact accepted held replacement required')
 dependency_contract(fields,row)

def dependency_contract(fields,row):
 expected={(row['dependency_kind'],str(j)) for j in row.get('issued_native_dependencies',[])}
 text=fields.get('Dependency','(null)');actual=set()
 require('?' not in text,'An OR dependency cannot establish serial controller authority')
 if text not in {'(null)','None',''}:
  for token in text.split(','):
   m=re.fullmatch(r'(afterok|afterany):([1-9][0-9]*)(?:\((?:unfulfilled|satisfied)\))?',token)
   require(m is not None,'Unexpected native dependency syntax');actual.add((m[1],m[2]))
 require(actual==expected,'Actual serial dependencies differ from the issued source-bound ledger')

def passing_tests(path,*,temporary=None):
 path=Path(path)
 if temporary is not None:require(path.is_absolute() and path.resolve()==path and path.is_relative_to(temporary),'Actual allocation-local native JUnit required')
 raw=path.read_bytes();require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Unsafe native JUnit refused')
 tree=ET.fromstring(raw);cases=list(tree.iter('testcase'));roster=sorted(c.get('classname','').split('.')[-1]+'::'+c.get('name','') for c in cases)
 leaves=[n for n in tree.iter('testsuite') if not n.findall('testsuite')]
 require(len(cases)==len(set(roster))==TEST_COUNT and canonical(roster)==TEST_ROSTER_SHA
  and all(c.get('classname') and c.get('name') for c in cases)
  and not any(list(c.iter(t)) for c in cases for t in ['failure','error','skipped'])
  and leaves and sum(int(n.get('tests','-1')) for n in leaves)==TEST_COUNT
  and all(int(n.get(k,'0'))==0 for n in tree.iter() if n.tag in {'testsuite','testsuites'} for k in ['failures','errors','skipped']),
  'Complete source-bound passing activation native roster required')
 return {'source':{'path':str(BASE/'test_activate_controllers.py'),'sha256':TEST_SOURCE_SHA},'tests':TEST_COUNT,'roster_sha256':TEST_ROSTER_SHA,'junit':{'path':str(path),'sha256':hashlib.sha256(raw).hexdigest()}}

def native_test_path(value,temporary):
 require(Path(value)==Path('activation-tests.xml'),'Exact literal native test basename required')
 return Path(temporary)/'activation-tests.xml'

def archive_authority(binding):
 result=json.loads(ordinary(binding['result_path']).read_bytes());registration=read(binding['registration'])
 source=pin(binding['source']);request=pin(binding['request']);submitted=json.loads(request.read_bytes())
 require(source==BASE/'archive_continuation.py' and submitted.get('new_registration_path')==binding['registration']['path']
  and registration.get('job_id')==binding['job_id'] and registration.get('source_sha256')==binding['source']['sha256']
  and registration.get('request_sha256')==binding['request']['sha256'],'Actual archive continuation registration/source/request join required')
 script=pin(registration['script']);held=registration['actual_held_native']
 require(registration.get('script_sha256')==registration['script']['sha256'] and held.get('JobId')==binding['job_id']
  and held.get('UserId')=='pwa209(3164768)' and held.get('Account')=='def-ptewarie_cpu' and held.get('Command')==str(script)
  and held.get('JobState')=='PENDING' and held.get('Reason')=='JobHeldUser' and held.get('NumCPUs')=='1'
  and held.get('MinMemoryNode') in {'8G','8192M'} and seconds(held['TimeLimit'])==43200 and held.get('Requeue')=='0',
  'Original exact held archive allocation required')
 require(result.get('native_job_id')==binding['job_id'] and result.get('source_sha256')==binding['source']['sha256']
  and result.get('request_sha256')==binding['request']['sha256']
  and result.get('status')=='complete_verified_orthology_sequence_copy_archive_continuation'
  and result.get('files_removed')==43145 and result.get('original_archive_sha256')=='1ef9c2985932b665860a5badf689a076857c3dbbfe311e74d1c2ddec1d6db558'
  and result.get('original_inputs_preserved') is True and result.get('no_repacking') is True,
  'Positive verified source-bound original archive continuation result required')
 return result

def metadata_forecast(record):
 require(type(record.get('study_files')) is int and record['study_files']>=0
  and type(record.get('study_bytes')) is int and record['study_bytes']>=0,'Actual measured study occupancy required')
 require(record['study_files']+512<=250000 and record['study_bytes']+1073741824<=6000000000000,
  'Bounded controller metadata must fit unchanged study occupancy limits')
 return {'required_files':512,'required_bytes':1073741824,'reserve_bytes':2000000000000}

def render_agent(old,manifest,qbinding,af_binding,script_reader):
 directory=ROOT/'plans'/AGENT_PHASE;planpath=directory/'plan.json';ledger=ROOT/'state'/(AGENT_PHASE+'-submission.json')
 tasks=[];scripts={};old_names=[t['name'] for t in old['tasks']];require(len(old_names)==len(set(old_names))==4,'Four original agent segments required')
 for i,original in enumerate(old['tasks']):
  require(original['resources']=={'cpus':1,'memory':'64G','hours':96,'gpu':False,'array':None}
   and original['depends']==([] if i==0 else [old_names[i-1]]),'Original sequential agent envelope required')
  text=script_reader(original['script']);require(canonical(text)==original['script_sha256'] and text.count(original['command'])==1,'Exact old script/invocation required')
  name=AGENT_PHASE+'-controller-'+str(i);path=directory/(name+'.sbatch')
  flags=['--mode','run','--source-manifest-sha256',manifest['sha256'],'--qualification',qbinding['path'],'--qualification-sha256',qbinding['sha256'],
   '--segment',str(i),'--native-plan',str(planpath),'--native-ledger',str(ledger),'--native-script',str(path),'--native-task',name,
   '--consumer-binding',af_binding['path'],'--consumer-binding-sha256',af_binding['sha256']]
  command=shlex.join(['python',str(BASE/'coordination/coordination.py'),*flags]);new=text.replace(original['command'],command)
  require(new.splitlines().count('#SBATCH --hold')==1 and new.splitlines().count('#SBATCH --time-min=08:00:00')==1,'Existing held/backfill agent script required')
  require(new.splitlines().count('#SBATCH --job-name=hhw-'+original['name'])==1,'One old native job name required')
  new=new.replace('#SBATCH --job-name=hhw-'+original['name'],'#SBATCH --job-name=hhw-'+name)
  lines=[]
  for line in new.splitlines(keepends=True):
   if line.startswith('#SBATCH --output='):line='#SBATCH --output='+str(ROOT/'logs'/(name+'-%A_%a.log'))+'\n'
   if line.startswith('#SBATCH --error='):line='#SBATCH --error='+str(ROOT/'logs'/(name+'-%A_%a.err'))+'\n'
   lines.append(line)
  new=''.join(lines);require(not any(x.startswith('#SBATCH --dependency') for x in new.splitlines()),'Plan owns every new dependency')
  require(not any(x.startswith('#SBATCH --requeue') for x in new.splitlines()),'Automatic controller requeue is unsupported')
  if '#SBATCH --no-requeue' not in new.splitlines():new=new.replace('#SBATCH --hold\n','#SBATCH --hold\n#SBATCH --no-requeue\n',1)
  task={**original,'name':name,'script':str(path),'command':command,'script_sha256':canonical(new),
   'depends':[] if i==0 else [tasks[-1]['name']],'external_dependencies':[qbinding['job_id']] if i==0 else [],
   'dependency_kind':'afterok','coordination_manifest_sha256':manifest['sha256']}
  tasks.append(task);scripts[str(path)]=new
 return {'plan':{**old,'phase':AGENT_PHASE,'tasks':tasks},'plan_path':str(planpath),'ledger_path':str(ledger),'scripts':scripts}

def disable_primary_requeue(rendered):
 for task in rendered['plan']['tasks']:
  text=rendered['scripts'][task['script']]
  require(not any(x.startswith('#SBATCH --requeue') for x in text.splitlines()),'Automatic primary requeue is unsupported')
  if '#SBATCH --no-requeue' not in text.splitlines():text=text.replace('#SBATCH --hold\n','#SBATCH --hold\n#SBATCH --no-requeue\n',1)
  require(text.splitlines().count('#SBATCH --no-requeue')==1,'Every replacement must explicitly disable requeue')
  rendered['scripts'][task['script']]=text;task['script_sha256']=canonical(text)
 return rendered

def main(argv=None):
 argv=list(sys.argv[1:] if argv is None else argv);p=argparse.ArgumentParser()
 for name in ['request','native-plan','native-ledger','native-script','native-tests-junit']:p.add_argument('--'+name,type=Path,required=True)
 for name in ['request-sha256','source-sha256','native-task']:p.add_argument('--'+name,required=True)
 args=p.parse_args(argv);require(sha(__file__)==args.source_sha256 and sha(ordinary(args.request))==args.request_sha256,'Exact activation source/request required')
 require(os.geteuid()==3164768 and re.fullmatch('[1-9][0-9]*',os.environ.get('SLURM_JOB_ID','')),'Owned scalar allocation required')
 require(subprocess.check_output(['hostname','-f'],text=True,timeout=10).strip().endswith('.narval.calcul.quebec'),'Narval allocation required')
 os.environ['TZ']='UTC'
 if hasattr(time,'tzset'):time.tzset()
 job=os.environ['SLURM_JOB_ID'];request=json.loads(args.request.read_bytes());require(request['kind']=='storage-reserve-200k-controller-activation-v1','Exact one-off activation request required')
 def readonly(cmd):
  result=subprocess.run(cmd,capture_output=True,text=True,timeout=45);result.check_returncode();return result
 fields=native_fields(readonly(['scontrol','show','job',job,'-o']));end,ownplan,ownledger=own_registration(args,argv,job,fields)
 require(sha(ordinary(BASE/'test_activate_controllers.py'))==TEST_SOURCE_SHA,'Exact native activation test source required')
 temporary=Path(os.environ['SLURM_TMPDIR']);require(temporary.is_absolute() and temporary.resolve()==temporary and not temporary.is_symlink() and temporary.stat().st_uid==os.geteuid(),'Actual owned node-local native test directory required')
 tests=passing_tests(native_test_path(args.native_tests_junit,temporary),temporary=temporary)
 def deadline():require(time.time()+300<end,'Five-minute activation finalization reserve exhausted')
 from horsehair import slurm,budget
 from horsehair.core import read_json,source_identity,canonical_hash,exclusive_file_lock,atomic_json
 require(source_identity(FRAMEWORK)['sha256']==SOURCE,'Original execution framework required');config=read_json(FRAMEWORK/'configs/study.json');require(canonical_hash(config)==CONFIG,'Original config required')
 area=BASE/'activation'/('native-'+job);area.mkdir(mode=0o700,parents=True,exist_ok=False);counter=[0]
 def run(cmd,**kwargs):
  deadline();counter[0]+=1;prefix=area/('native-'+str(counter[0]).zfill(4));save(Path(str(prefix)+'-intent.json'),{'command':cmd,'automatic_replay_permitted':False})
  try:result=subprocess.run(cmd,**{**kwargs,'timeout':90})
  except BaseException as error:save(Path(str(prefix)+'-exception.json'),{'type':type(error).__name__,'automatic_replay_permitted':False});raise
  save(Path(str(prefix)+'-response.json'),{'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr});return result
 def accounting(ids):
  return parse_accounting(readonly(['sacct','-X','-n','-P','-j',','.join(sorted(ids)),'--format=JobIDRaw,User,Account,State,ExitCode,ElapsedRaw,AllocCPUS']).stdout)
 def await_success(spec):
  stop=min(time.time()+720,end-600)
  while True:
   rows=accounting(spec)
   if successful(rows,spec):return rows
   require(time.time()+5<stop,'Finite wait for actual final accounting exhausted');time.sleep(5)
 components=request['components'];require(set(components)==set(MANIFESTS),'All three qualified component closures required')
 modules={};manifests={};qjobs={}
 for kind,expected in MANIFESTS.items():
  component=components[kind];require(component['manifest']=={'path':str(BASE/kind/'source-manifest.json'),'sha256':expected},'Exact qualified component manifest required')
  manifests[kind]=read(component['manifest']);q=read({k:component['qualification'][k] for k in ['path','sha256']});qjob=component['qualification']['job_id'];qualification_identity(kind,q,qjob);qjobs[qjob]=4
  for name,digest in manifests[kind]['files'].items():pin({'path':str(BASE/kind/name),'sha256':digest})
  entry={'consumers':'run-census-resilience-consumer-v2-20261024.py','coordination':'coordination.py','census':'run-fresh-storage-census-resilience-v1-20261024.py'}[kind]
  modules[kind]=module(BASE/kind/entry,manifests[kind]['files'][entry],'activation_'+kind)
 archive=request['archive'];required={**qjobs,archive['job_id']:1,request['first_census_job_id']:8,OLD_AGENT[0]:1,OLD_AGENT[1]:1}
 require(len(qjobs)==3 and len(required)==7,'Three qualifications, archive, first census and two old agents must be distinct actual jobs')
 positive=await_success(required);save(area/'positive-prerequisites.json',positive)
 continuation=archive_authority(archive)
 consumer=modules['consumers'];coordinator=modules['coordination'];provider=modules['census']
 storage=module(BASE/'storage_policy.py',POLICY_MODULE,'activation_storage_policy');policy=storage.verify_policy(BASE/'policy.json',POLICY,root=ROOT)
 budget.native_accounting(ROOT,sorted(required),runner=lambda cmd,**kw:subprocess.run(cmd,**{**kw,'timeout':60}))
 native_reader=consumer.bounded_native_reader(end)
 consumer.validate_qualification(components['consumers']['qualification'],components['consumers']['manifest'],native_reader)
 cq=read({k:components['coordination']['qualification'][k] for k in ['path','sha256']})
 require(cq.get('status')=='qualified_agent_coordination_repair' and cq.get('source_manifest_sha256')==MANIFESTS['coordination']
  and cq.get('tests')==manifests['coordination']['tests'] and cq.get('roster_sha256')==manifests['coordination']['roster_sha256'],'Source-bound coordinator qualification required')
 junit=pin(cq['junit']);cases=list(ET.parse(junit).getroot().iter('testcase'));roster=sorted(c.get('classname','').split('.')[-1]+'::'+c.get('name','') for c in cases)
 require(len(cases)==len(set(roster))==cq['tests'] and canonical(roster)==cq['roster_sha256'] and not any(list(c.iter(t)) for c in cases for t in ['failure','error','skipped']),'Complete actual coordinator native test roster required')
 provider.validate_schedule_binding(request['census_schedule'],components['census']['qualification'],native_reader)
 old={};retire_tasks={}
 for kind,wanted in [('primary',OLD_PRIMARY),('agent',OLD_AGENT)]:
  pair=request['old_registrations'][kind];plan=read(pair['plan']);ledger=read(pair['ledger']);require(ledger['plan_sha256']==canonical(plan),'Original controller plan/ledger identity required')
  require(tuple(ledger['tasks'][t['name']]['job_id'] for t in plan['tasks'])==wanted,'Exact old accepted controller roster required');old[kind]=plan
  retire_tasks.update({ledger['tasks'][t['name']]['job_id']:t for t in plan['tasks'] if ledger['tasks'][t['name']]['job_id'] in RETIRE})
 oldbindings={kind:read(request['legacy_bindings'][kind]) for kind in ['af','primary']}
 created={};pins={};coord_binding={'entry':locator(BASE/'coordination/coordination.py'),'source_manifest':components['coordination']['manifest']}
 for kind in ['af','primary']:
  bound={**oldbindings[kind],'source_manifest':components['consumers']['manifest'],'qualification':components['consumers']['qualification'],
   'provider_schedule':request['census_schedule'],'coordination':coord_binding}
  path=BASE/'consumers'/(kind+'-consumer-binding.json')
  if path.exists():require(read_json(ordinary(path))==bound,'Existing immutable binding differs; no overwrite permitted')
  else:save(path,bound)
  created[kind]=locator(path);_,checked,checked_pins=consumer.check_binding(path,created[kind]['sha256']);pins.update(checked_pins)
 outer=consumer.module(consumer.AF,consumer.AF_SHA,'activation_original_provider_proof')
 def refresh_provider(c,label,**kwargs):
  match=re.fullmatch('external-census-producer-([1-9][0-9]*)',label)
  ids=sorted(set(required)|({match[1]} if match else set()))
  return budget.native_accounting(ROOT,ids,runner=lambda cmd,**options:subprocess.run(cmd,**{**options,'timeout':60}))
 base=SimpleNamespace(PendingStorage=PendingActivationCensus,registration_guard=lambda *a,**kw:nullcontext(),admission=refresh_provider)
 context={'base_context':{'config':config},'policy':{'provider_schedule':request['census_schedule']}}
 provider_proof=consumer.routed_provider(outer,read(created['af']),pins,native_reader);stop=min(time.time()+1200,end-900)
 while True:
  try:
   record,pair=outer.fresh_pair(base);proof=provider_proof(base,context,pair);break
  except PendingActivationCensus:
   require(time.time()+15<stop,'Finite wait for genuinely fresh finalized census exhausted');time.sleep(15)
 save(area/'fresh-qualified-census.json',{'current_pair':pair,'producer':proof,'study_files':record['study_files'],'study_bytes':record['study_bytes']})
 def metadata_headroom():
  return budget.require_storage_headroom(ROOT,**metadata_forecast(record),reserve_files=storage.reserve_for(512,policy),
    runner=lambda cmd,**kw:subprocess.run(cmd,**{**kw,'timeout':45}))
 metadata_headroom();template=read(request['primary_template']['plan']);tl=read(request['primary_template']['ledger']);require(tl['plan_sha256']==canonical(template),'Original primary template identity required')
 builder=module(BASE/'consumers/build-consumer-plan.py',manifests['consumers']['files']['build-consumer-plan.py'],'activation_original_primary_builder')
 primary=disable_primary_requeue(builder.render(template,'primary',created['primary'],read(created['primary']),manifests['consumers'],script_reader=lambda p:ordinary(p).read_text()))
 agent=render_agent(old['agent'],components['coordination']['manifest'],components['coordination']['qualification'],created['af'],lambda p:ordinary(p).read_text())
 require(primary['plan']['phase']==PRIMARY_PHASE,'Exact new primary phase required')
 case=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1';stable=sha(case/'state.json.gz')
 with exclusive_file_lock(case/'coordination-controller.mutex',timeout=180):
  require(successful(accounting(OLD_AGENT[:2]),{j:1 for j in OLD_AGENT[:2]}),'Old active agent controller must have ended successfully')
  require(sha(case/'state.json.gz')==stable,'Canonical state changed before activation')
  held=native_fields(readonly(['scontrol','show','job',KEEP_HELD,'-o']));require(held.get('JobId')==KEEP_HELD and held.get('UserId')=='pwa209(3164768)' and held.get('JobState')=='PENDING' and held.get('Reason')=='JobHeldUser','Old census64 must stay held; activation never cancels it')
  old_held={j:native_fields(readonly(['scontrol','show','job',j,'-o'])) for j in RETIRE}
  for j in RETIRE:held_controller(old_held[j],j,retire_tasks[j])
  save(area/'old-held-controller-proof.json',old_held)
  registrations={}
  with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex',timeout=180):
   deadline();consumer.source_guard(pins);metadata_headroom()
   for kind,rendered in [('primary',primary),('agent',agent)]:
    directory=Path(rendered['plan_path']).parent;require(not directory.exists() and not Path(rendered['ledger_path']).exists(),'Existing registration must never be replayed');directory.mkdir(mode=0o700)
    for path,text in rendered['scripts'].items():
     with Path(path).open('x',encoding='utf-8',newline='\n') as f:f.write(text)
    save(rendered['plan_path'],rendered['plan']);ledger=slurm.submit(rendered['plan_path'],rendered['ledger_path'],runner=run)
    jobs=[ledger['tasks'][t['name']]['job_id'] for t in rendered['plan']['tasks']];require(len(set(jobs))==4 and not set(jobs)&set(RETIRE),'Four distinct replacement controllers required')
    for task,ident in zip(rendered['plan']['tasks'],jobs):
     observed=native_fields(readonly(['scontrol','show','job',ident,'-o']));held_replacement(observed,ident,task,ledger['tasks'][task['name']])
    registrations[kind]={'jobs':jobs,'plan':rendered['plan_path'],'ledger':rendered['ledger_path']};save(area/(kind+'-held-registration.json'),registrations[kind])
  require(sha(case/'state.json.gz')==stable,'Canonical state changed during registration')
  for ident in RETIRE:
   held_controller(native_fields(readonly(['scontrol','show','job',ident,'-o'])),ident,retire_tasks[ident]);response=run(['scancel',ident],capture_output=True,text=True);response.check_returncode()
  stop=min(time.time()+720,end-600)
  while True:
   retired=accounting(RETIRE)
   if all(retired.get(j,{}).get('state')=='CANCELLED' and retired[j]['elapsed_seconds']==0 for j in RETIRE):break
   require(all(j not in retired or retired[j]['state'] not in TERMINAL or retired[j]['state']=='CANCELLED' for j in RETIRE),'Unexpected final old-controller state')
   require(time.time()+5<stop,'Finite cancellation finality wait exhausted');time.sleep(5)
  save(area/'positive-controller-retirement.json',retired);require(sha(case/'state.json.gz')==stable,'Canonical state changed before release')
 # The permanent controller mutex is released before any replacement can run.
 releases={}
 for kind,rendered in [('primary',primary),('agent',agent)]:
  for task,ident in zip(rendered['plan']['tasks'],registrations[kind]['jobs']):
   deadline();consumer.source_guard(pins);run(['scontrol','release',ident],capture_output=True,text=True)
   observed=native_fields(readonly(['scontrol','show','job',ident,'-o']));require(observed.get('JobId')==ident and observed.get('UserId')=='pwa209(3164768)' and observed.get('Command')==task['script'] and observed.get('JobName')=='hhw-'+task['name'] and (observed.get('JobState') in {'RUNNING','COMPLETING','COMPLETED'} or observed.get('JobState')=='PENDING' and observed.get('Reason') not in {'JobHeldUser','JobHeldAdmin'} and int(observed.get('Priority','0'))>0),'Positive actual unheld replacement proof required')
   releases[ident]=observed;atomic_json(area/'verified-release.json',releases)
 result={'status':'eight_qualified_replacement_controllers_registered_and_released','native_job_id':job,'registrations':registrations,
  'retired_controller_jobs':list(RETIRE),'old_census64_kept_held':KEEP_HELD,'canonical_state_sha256_during_registration':stable,
  'worker_jobs_cancelled':False,'scientific_admission_claimed':False,'metadata_forecast_files':512,'minimum_personal_free_files':200000,
  'full_scientific_forecast_retained_in_controllers':True,'scientific_gates_changed':False,'source_sha256':args.source_sha256,'request_sha256':args.request_sha256}
 result['native_tests']=tests
 save(area/'result.json',result);print(json.dumps(result),flush=True)

if __name__=='__main__':main()
