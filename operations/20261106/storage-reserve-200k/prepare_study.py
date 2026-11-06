"""One finite preparation job: qualify sources, register held work, publish the Slurm DAG."""
from pathlib import Path
from datetime import datetime,timezone
import argparse,hashlib,importlib.util,json,os,re,shlex,stat,subprocess,sys,time
import xml.etree.ElementTree as ET

ROOT=Path('/scratch/pwa209/horsehair-worm')
BASE=ROOT/'maintenance/storage-reserve-200k-v3-20261106'
POLICY_BASE=ROOT/'maintenance/storage-reserve-200k-v2-20261106'
CENSUS_BASE=POLICY_BASE/'census'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
POLICY_MODULE='0e8691671a32e2b55cfcd626d461c2708d11d05b00c0491721c6c3be13107f7d'
POLICY='2e8e2bd2c1f70aa988a9414d2b44d9ffd0f768f8bc2e329747b7839b2855f2d9'
MANIFESTS={'census':'288af211cad1ad186105e6b2dafa08d4195920b132ab9efef367af98c0fbdf6e',
 'consumers':'a5dfc9c76ac2553de5bfa3c34068d4c38e0f04fb236d23768b9de6258ce4d78d',
 'coordination':'195be1555539e2894249660cf235bce0ee20d78d6554b632a825ef38882f17d5'}
QUALIFIERS={'census':'8630687','consumers':'8632769','coordination':'8632772'}

def require(ok,message):
 if not ok:raise ValueError(message)

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def canonical(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def locator(path):return {'path':str(path),'sha256':sha(path)}

def ordinary(path,root=ROOT):
 p=Path(path);require(p.is_absolute() and p.is_relative_to(root) and p.resolve()==p,'Exact ordinary personal path required')
 for parent in [p,*p.parents]:
  if not parent.is_relative_to(root):break
  require(not parent.is_symlink(),'Redirected source/evidence refused')
 info=p.lstat();require(stat.S_ISREG(info.st_mode) and info.st_nlink==1,'Owned ordinary single-link file required')
 if hasattr(os,'geteuid'):require(info.st_uid==os.geteuid(),'Foreign-owned source/evidence refused')
 return p

def pin(binding):
 require(type(binding) is dict and set(binding)=={'path','sha256'} and re.fullmatch('[0-9a-f]{64}',str(binding['sha256'])),'Exact source/evidence locator required')
 p=ordinary(binding['path']);require(sha(p)==binding['sha256'],'Pinned source/evidence changed: '+str(p));return p

def read(binding):return json.loads(pin(binding).read_bytes())
def save(path,value):
 with Path(path).open('x',encoding='utf-8',newline='\n') as f:json.dump(value,f,sort_keys=True,indent=2,allow_nan=False);f.write('\n');f.flush();os.fsync(f.fileno())

def module(path,expected,name):
 pin({'path':str(path),'sha256':expected});spec=importlib.util.spec_from_file_location(name,path);value=importlib.util.module_from_spec(spec);sys.modules[name]=value;spec.loader.exec_module(value);return value

def seconds(text):
 m=re.fullmatch(r'(?:(\d+)-)?(\d+):(\d\d):(\d\d)',text);require(m is not None,'Finite native duration required')
 d,h,n,s=(int(v or 0) for v in m.groups());return d*86400+h*3600+n*60+s

def held_text(text,hours):
 require(text.splitlines().count('set -euo pipefail')==1 and '#SBATCH --hold' not in text.splitlines(),'One fresh generated native script required')
 text=text.replace('set -euo pipefail','#SBATCH --hold\n#SBATCH --no-requeue\nset -euo pipefail',1)
 if hours==.5:
  require(text.splitlines().count('#SBATCH --time=1:00:00')==1,'Exact half-hour conversion requires original1h directive')
  text=text.replace('#SBATCH --time=1:00:00','#SBATCH --time=00:30:00',1).replace('export HHW_MAX_HOURS=1','export HHW_MAX_HOURS=0.5',1)
 return text

def held_native(f,job,task):
 resources=task['resources'];memory={'8G':{'8G','8192M'},'16G':{'16G','16384M'},'64G':{'64G','65536M'}}[resources['memory']]
 require(f.get('JobId')==job and f.get('UserId')=='pwa209(3164768)' and f.get('Account')=='def-ptewarie_cpu'
  and f.get('JobState')=='PENDING' and f.get('Reason')=='JobHeldUser' and f.get('Priority')=='0'
  and f.get('Command')==task['script'] and f.get('JobName')=='hhw-'+task['name'],'Exact accepted held native allocation required')
 require(f.get('NumCPUs')==str(resources['cpus']) and f.get('NumNodes') in {'1','1-1'} and f.get('MinMemoryNode') in memory
  and seconds(f.get('TimeLimit',''))==resources['hours']*3600 and f.get('Requeue')=='0' and 'ArrayJobId' not in f
  and all('gpu' not in f.get(k,'').lower() for k in ['ReqTRES','AllocTRES','Gres','TresPerNode','TresPerTask']),'Original scalar CPU-only resources required')

def successful_native(rows,ids):
 for job in ids:
  r=rows.get(job,{})
  require(r.get('state')=='COMPLETED' and r.get('exit_code')=='0:0' and r.get('user')=='pwa209' and r.get('account')=='def-ptewarie_cpu'
   and r.get('cpus')==4 and r.get('job_id_raw')==job and r.get('unallocated_metadata') is False
   and type(r.get('elapsed_seconds')) is int and 0<r['elapsed_seconds']<=3600 and 'gpu' not in r.get('alloc_tres','').lower(),
   'Actual successful owned4CPU1h qualification is required: '+job)

def wait_qualifications(reader,ids,end,*,guard=lambda:None,wall=time.time,monotonic=time.monotonic,sleeper=time.sleep):
 """Bound only final-accounting visibility; never repeat a submission."""
 stop=monotonic()+min(720,max(0,end-wall()-600));attempt=0
 while True:
  guard();remaining=min(stop-monotonic(),end-wall()-600);require(remaining>0,'Finite positive qualification accounting wait exhausted');attempt+=1
  try:rows=reader(min(30,remaining))
  except (subprocess.TimeoutExpired,RuntimeError) as error:
   if isinstance(error,RuntimeError) and str(error)!='Repair native proof query failed':raise
   rows={}
  for job in ids:
   row=rows.get(job)
   if not row:continue
   state=row.get('state');require(state in {'PENDING','RUNNING','COMPLETING','CONFIGURING','COMPLETED'},'Native qualification prerequisite failed: '+job)
   if state=='COMPLETED':successful_native({job:row},[job])
  if all(rows.get(job,{}).get('state')=='COMPLETED' for job in ids):
   successful_native(rows,ids);return rows
  remaining=min(stop-monotonic(),end-wall()-600);require(remaining>0,'Finite positive qualification accounting wait exhausted')
  print(json.dumps({'status':'qualification_final_accounting_pending','attempt':attempt,'job_ids':ids,'observed_states':{j:rows.get(j,{}).get('state') for j in ids}}),flush=True)
  sleeper(min(5,remaining))

def passing_report(role,record,manifest,job,manifest_binding):
 require(type(record.get('returncode')) is int and record['returncode']==0 and str(record.get('native_job_id',record.get('slurm_job_id')))==job,'Actual qualification report job/result differs')
 statuses={'census':'complete_census_resilience_operational_qualification','consumers':'complete_census_resilience_consumer_qualification','coordination':'qualified_agent_coordination_repair'}
 require(record.get('status')==statuses[role] and record.get('scientific_result') is False,'Complete operational qualification report required')
 expected=manifest_binding['sha256']
 require(record.get('source_manifest_sha256')==expected if role=='coordination' else record.get('source_manifest')==manifest_binding,'Qualification source closure differs')
 contract=manifest if role=='coordination' else manifest['suites']['resilience' if role=='census' else 'consumers']
 junit=record['junit'] if role=='coordination' else record['suite']['junit'];raw=pin(junit).read_bytes()
 require(len(raw)<=8_000_000 and b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Bounded safe native JUnit required')
 tree=ET.fromstring(raw);cases=list(tree.iter('testcase'));roster=sorted(c.get('classname','').split('.')[-1]+'::'+c.get('name','') for c in cases)
 require(len(cases)==len(set(roster))==contract['tests'] and canonical(roster)==contract['roster_sha256']
  and all(c.get('classname') and c.get('name') for c in cases)
  and not any(list(c.iter(k)) for c in cases for k in ['failure','error','skipped']),'Every source-matched native qualification case must pass')
 return junit

def one_plan(config,identity,phase,name,command,resources,dependencies,renderer,prefix=''):
 directory=ROOT/'plans'/phase;script=directory/(name+'.sbatch');ledger=ROOT/'state'/(phase+'-submission.json')
 text=renderer(config,FRAMEWORK,name,command,cpus=resources['cpus'],memory=resources['memory'],hours=1 if resources['hours']==.5 else resources['hours'],gpu=False)
 if prefix:
  require(text.count(command)==1,'One exact helper invocation required');text=text.replace(command,prefix+'\n'+command,1)
 text=held_text(text,resources['hours'])
 task={'name':name,'command':command,'script':str(script),'script_sha256':canonical(text),'resources':resources,
  'depends':[],'external_dependencies':list(dependencies),'dependency_kind':'afterok'}
 plan={'schema_version':2,'root':str(ROOT),'phase':phase,'release_identity':identity,'config_sha256':CONFIG,'tasks':[task]}
 return plan,{script.name:text},directory/'plan.json',ledger

def source_test_prefix(files,tests,junit,basetemp):
 assertion='from pathlib import Path;import hashlib;'+''.join('assert hashlib.sha256(Path('+repr(path)+').read_bytes()).hexdigest()=='+repr(digest)+';' for path,digest in files.items())
 return shlex.join(['python','-B','-c',assertion])+'\npython -B -m pytest '+shlex.join(tests)+' -q -p no:cacheprovider --basetemp "'+basetemp+'" --junitxml "'+junit+'"'

def original_archive():
 support=ROOT/'maintenance/orthology-sequence-archive-support-20261104';case=ROOT/'results/storage-archives/orthology-sequences-20261104'
 return {'wrapper':locator(support/'archive-orthology-sequence-copies-20261104.py'),'request':locator(support/'request.json'),
  'registration':locator(support/'registration.json'),'script':locator(support/'archive.sbatch'),'receipt':locator(case/'receipt.json')}

def census_gate(planpath,ledgerpath,plan,ledger):
 oldplan=ROOT/'plans/study-census-resilience-v1-20261024/plan.json';oldledger=ROOT/'state/study-census-resilience-v1-20261024-submission.json'
 old=json.loads(ordinary(oldplan).read_bytes());rows=json.loads(ordinary(oldledger).read_bytes());require(rows['plan_sha256']==canonical(old),'Original census plan/ledger differs')
 task=next(t for t in old['tasks'] if t['name']=='rolling-storage-census-64');require(rows['tasks'][task['name']]['job_id']=='6983060','Original held final census differs')
 held=[{'job_id':'6983060','script':locator(task['script']),'cpus':8,'memory':'64G','hours':6,'job_name':'hhw-'+task['name']}]
 for t in plan['tasks']:held.append({'job_id':ledger['tasks'][t['name']]['job_id'],'script':locator(t['script']),'cpus':8,'memory':'64G','hours':6,'job_name':'hhw-'+t['name']})
 require(len(held)==65 and len({x['job_id'] for x in held})==65,'All65 distinct existing/new census writers required')
 return {'terminal':{'6983037':{'state':'COMPLETED','exit_code':'0:0'},'6983056':{'state':'COMPLETED','exit_code':'0:0'},'6983059':{'state':'FAILED','exit_code':'1:0'}},'held':held,
  'old_provider':{'plan':locator(oldplan),'ledger_path':str(oldledger)},'new_provider':{'plan':locator(planpath),'ledger_path':str(ledgerpath)}}

def main(argv=None):
 argv=list(sys.argv[1:] if argv is None else argv);p=argparse.ArgumentParser()
 for name in ['request','native-plan','native-ledger','native-script']:p.add_argument('--'+name,type=Path,required=True)
 for name in ['request-sha256','source-sha256','native-task']:p.add_argument('--'+name,required=True)
 args=p.parse_args(argv);require(sha(__file__)==args.source_sha256 and sha(ordinary(args.request))==args.request_sha256,'Exact finite preparation source/request required')
 require(os.geteuid()==3164768 and re.fullmatch('[1-9][0-9]*',os.environ.get('SLURM_JOB_ID','')),'Owned actual scalar native allocation required')
 require(subprocess.check_output(['hostname','-f'],text=True,timeout=15).strip().endswith('.narval.calcul.quebec'),'Narval compute allocation required')
 os.environ['TZ']='UTC'
 if hasattr(time,'tzset'):time.tzset()
 request=json.loads(args.request.read_bytes());require(request['kind']=='finite-storage-reserve-200k-study-preparation' and request['root']==str(ROOT),'Exact finite preparation request required')
 def readonly(command):
  x=subprocess.run(command,capture_output=True,text=True,timeout=90);x.check_returncode();return x.stdout
 def native(job):return dict(x.split('=',1) for x in shlex.split(readonly(['scontrol','show','job',job,'-o'])) if '=' in x)
 job=os.environ['SLURM_JOB_ID'];own=native(job);require(own.get('JobId')==job and own.get('UserId')=='pwa209(3164768)' and own.get('Account')=='def-ptewarie_cpu'
  and own.get('JobState')=='RUNNING' and own.get('Command')==str(args.native_script) and own.get('JobName')=='hhw-'+args.native_task
  and own.get('NumCPUs')=='2' and own.get('NumNodes') in {'1','1-1'} and own.get('MinMemoryNode') in {'16G','16384M'} and seconds(own['TimeLimit'])==3600 and own.get('Requeue')=='0'
  and 'ArrayJobId' not in own and all('gpu' not in own.get(k,'').lower() for k in ['ReqTRES','AllocTRES','Gres','TresPerNode','TresPerTask']),'Exact owned scalar2CPU16G1h preparation allocation required')
 end=datetime.fromisoformat(own['EndTime']).replace(tzinfo=timezone.utc).timestamp();require(abs(end-float(os.environ['SLURM_JOB_END_TIME']))<=2,'Actual finite allocation end required')
 def deadline():require(time.time()+300<end,'Five-minute preparation finalization reserve exhausted')
 def bounded_read_runner(command,**kwargs):
  deadline();timeout=min(float(kwargs.get('timeout',60)),60,end-time.time()-300);require(timeout>0,'No native observation time remains')
  return subprocess.run(command,**{**kwargs,'timeout':timeout})
 from horsehair import budget,slurm,repair
 from horsehair.core import source_identity,read_json,canonical_hash,exclusive_file_lock,atomic_json
 from horsehair.execution_resources import load_resource_policy
 identity=source_identity(FRAMEWORK);config=read_json(FRAMEWORK/'configs/study.json');require(identity['sha256']==SOURCE and canonical_hash(config)==CONFIG,'Unchanged full execution source/config required')
 ownplan=read_json(args.native_plan);ownledger=read_json(args.native_ledger);selected=[t for t in ownplan['tasks'] if t['name']==args.native_task];require(len(selected)==1,'One actual preparation task required');task=selected[0];tokens=shlex.split(task['command'])
 require(ownplan['root']==str(ROOT) and ownplan['release_identity']['sha256']==ownledger['source_sha256']==SOURCE and ownplan['config_sha256']==ownledger['config_sha256']==CONFIG
  and ownledger['plan_sha256']==canonical(ownplan) and task['resources']=={'cpus':2,'memory':'16G','hours':1,'gpu':False,'array':None}
  and task['script']==str(args.native_script) and task['script_sha256']==ownledger['tasks'][args.native_task]['script_sha256']==canonical(Path(args.native_script).read_text())
  and ownledger['tasks'][args.native_task]['state']=='submitted' and ownledger['tasks'][args.native_task]['job_id']==job and tokens.count(str(Path(__file__)))==1 and tokens[tokens.index(str(Path(__file__)))+1:]==argv,'Source-bound actual preparation plan/ledger/invocation required')
 area=BASE/'preparation'/('native-'+job);area.mkdir(mode=0o700,parents=True,exist_ok=False);counter=[0]
 pins=dict(request['source_pins']);pins[str(args.request)]=args.request_sha256;pins[str(Path(__file__))]=args.source_sha256
 pins[str(POLICY_BASE/'storage_policy.py')]=POLICY_MODULE;pins[str(POLICY_BASE/'policy.json')]=POLICY
 required_files={'prepare_study.py','test_prepare_study.py','archive_continuation.py','archive-native-test-contract.json','release_census.py','test_release_census.py',
  'activate_controllers.py','test_activate_controllers.py','storage_policy.py','test_storage_policy.py','policy.json'}
 require({str(BASE/name) for name in required_files}<=set(pins),'Every finite helper/test/policy source must be pinned before registration')
 def guard():
  deadline()
  for path,digest in pins.items():pin({'path':path,'sha256':digest})
 def runner(command,**kwargs):
  guard();counter[0]+=1;prefix=area/('mutation-'+str(counter[0]).zfill(4));save(Path(str(prefix)+'-intent.json'),{'command':command,'automatic_replay_permitted':False})
  try:x=subprocess.run(command,**{**kwargs,'timeout':120})
  except BaseException as error:save(Path(str(prefix)+'-exception.json'),{'type':type(error).__name__,'automatic_replay_permitted':False});raise
  save(Path(str(prefix)+'-response.json'),{'returncode':x.returncode,'stdout':x.stdout,'stderr':x.stderr});return x
 policy=module(POLICY_BASE/'storage_policy.py',POLICY_MODULE,'finite_prepare_storage_policy');approved=policy.verify_policy(POLICY_BASE/'policy.json',POLICY,root=ROOT)
 resource=load_resource_policy(ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json','f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5',root=ROOT,config=config)
 components={};manifests={};require(set(request['components'])==set(MANIFESTS),'All three finite qualifications required')
 for role,expected in MANIFESTS.items():
  c=request['components'][role];support=CENSUS_BASE if role=='census' else BASE/role;binding=c['manifest'];require(binding=={'path':str(support/'source-manifest.json'),'sha256':expected} and c['qualification_job_id']==QUALIFIERS[role],'Exact declared qualification job/source required')
  manifest=read(binding);manifests[role]=manifest
  for name,digest in manifest['files'].items():require(Path(name).name==name,'Flat qualified source member required');pin({'path':str(support/name),'sha256':digest})
  qp=support/('qualification/qualification.json' if role=='census' else 'qualification.json');require(c['qualification_path']==str(qp),'Exact finite qualification output required');report=json.loads(ordinary(qp).read_bytes());qb={**locator(qp),'job_id':c['qualification_job_id']}
  junit=passing_report(role,report,manifest,qb['job_id'],binding);pins[qp.as_posix()]=qb['sha256'];pins[junit['path']]=junit['sha256'];components[role]={'manifest':binding,'qualification':qb}
 rows=wait_qualifications(lambda timeout:repair._repair_native_states(list(QUALIFIERS.values()),{},owner='pwa209',account='def-ptewarie_cpu',runner=lambda cmd,**kw:bounded_read_runner(cmd,**{**kw,'timeout':timeout})),list(QUALIFIERS.values()),end,guard=guard)
 budget.native_accounting(ROOT,list(QUALIFIERS.values()),runner=bounded_read_runner)
 provider=module(CENSUS_BASE/'run-fresh-storage-census-resilience-v1-20261024.py',manifests['census']['files']['run-fresh-storage-census-resilience-v1-20261024.py'],'finite_prepare_provider')
 consumer=module(BASE/'consumers/run-census-resilience-consumer-v2-20261024.py',manifests['consumers']['files']['run-census-resilience-consumer-v2-20261024.py'],'finite_prepare_consumer')
 native_reader=lambda ids,rosters,**kw:repair._repair_native_states(ids,rosters,**{**kw,'runner':bounded_read_runner})
 consumer.validate_qualification(components['consumers']['qualification'],components['consumers']['manifest'],native_reader)
 def held_old():
  f=native('6983060');require(f.get('JobId')=='6983060' and f.get('UserId')=='pwa209(3164768)' and f.get('JobState')=='PENDING' and f.get('Reason')=='JobHeldUser' and f.get('NumCPUs')=='8','Original census64 must remain held')
 def publish(plan,scripts,planpath,ledgerpath):
  held_old();guard();require(not planpath.parent.exists() and not ledgerpath.exists(),'Existing or uncertain registration must never be replayed');planpath.parent.mkdir(mode=0o700)
  for name,text in scripts.items():
   path=planpath.parent/name
   with path.open('x',encoding='utf-8',newline='\n') as f:f.write(text);f.flush();os.fsync(f.fileno())
  save(planpath,plan);ledger=slurm.submit(planpath,ledgerpath,runner=runner);ids=[];observed={}
  for t in plan['tasks']:
   ident=ledger['tasks'][t['name']]['job_id'];held=native(ident);held_native(held,ident,t);ids.append(ident);observed[ident]=held
  require(len(ids)==len(set(ids)),'Accepted native allocations must be distinct');save(area/(plan['phase']+'-accepted-held.json'),{'jobs':ids,'plan':locator(planpath),'ledger':locator(ledgerpath),'actual_held_native':observed})
  return ledger,ids,observed
 with budget.resource_lease(ROOT,config,cpus=2,gpus=0,max_hours=1,job_id=job,resource_policy=resource):
  guard();held_old();budget.require_storage_headroom(ROOT,required_files=2000,required_bytes=1_000_000_000,reserve_files=policy.reserve_for(2000,approved),reserve_bytes=2_000_000_000_000)
  builder=module(CENSUS_BASE/'build-native-plans.py',manifests['census']['files']['build-native-plans.py'],'finite_prepare_census_builder');builder.PROVIDER_SHA=manifests['census']['files']['run-fresh-storage-census-resilience-v1-20261024.py'];builder.QUALIFIER_SHA=manifests['census']['files']['qualify-census-resilience-v1-20261024.py']
  q=components['census']['qualification'];proof=slurm.completed_native_dependency(ROOT,q['job_id']);require(proof is not None,'Positive persisted census qualification required')
  plan,scripts=builder.render(config,identity,MANIFESTS['census'],slurm._script,mode='provider',qualification=q,proof=proof,anchor=datetime.now(timezone.utc))
  for t in plan['tasks']:scripts[Path(t['script']).name]=held_text(scripts[Path(t['script']).name],6);t['script_sha256']=canonical(scripts[Path(t['script']).name])
  planpath=ROOT/'plans'/plan['phase']/'plan.json';ledgerpath=ROOT/'state'/(plan['phase']+'-submission.json')
  with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex',timeout=180):ledger,ids,held=publish(plan,scripts,planpath,ledgerpath)
  schedule={'plan':locator(planpath),'ledger':locator(ledgerpath),'helper':locator(CENSUS_BASE/'run-fresh-storage-census-resilience-v1-20261024.py'),'source_manifest':components['census']['manifest'],'qualification':q,'case_root':str(CENSUS_BASE/'cases')}
  provider.validate_schedule_binding(schedule,q,native_reader);save(area/'census-schedule.json',schedule)
  archive_contract=json.loads(ordinary(BASE/'archive-native-test-contract.json').read_bytes());archive_source=locator(BASE/'archive_continuation.py');archive_request=BASE/'archive-continuation-request.json';archive_registration=BASE/'archive-continuation-registration.json'
  payload={'original':original_archive(),'new_registration_path':str(archive_registration),'continuation_directory':str(ROOT/'results/storage-archives/orthology-sequences-20261104/continuation-20261106'),'native_test_contract':archive_contract,'census_gate':census_gate(planpath,ledgerpath,plan,ledger)}
  save(archive_request,payload);ap=locator(archive_request);command=shlex.join(['python','-s','-B','-u',archive_source['path'],'--request',ap['path'],'--request-sha256',ap['sha256'],'--source-sha256',archive_source['sha256']])
  prefix='mkdir -m 700 "'+str(BASE)+'/native-${SLURM_JOB_ID}"\nexport TZ=UTC PYTHONDONTWRITEBYTECODE=1\n'+source_test_prefix(archive_contract['sources'],[str(BASE/'test_storage_policy.py'),str(BASE/'test_archive_continuation.py')],str(BASE)+'/native-${SLURM_JOB_ID}/tests.xml','$SLURM_TMPDIR/archive-tests')
  archive=one_plan(config,identity,'storage-reserve-200k-v3-archive-continuation-20261106','orthology-archive-continuation-20261106',command,{'cpus':1,'memory':'8G','hours':12,'gpu':False,'array':None},[],slurm._script,prefix)
  al,ai,ah=publish(*archive);cleanup=ai[0];script=Path(archive[0]['tasks'][0]['script']);save(archive_registration,{'job_id':cleanup,'request_sha256':ap['sha256'],'source_sha256':archive_source['sha256'],'script':locator(script),'script_sha256':sha(script),'actual_held_native':ah[cleanup]})
  cleanup_record={'job_id':cleanup,'source':archive_source,'request':ap,'registration':locator(archive_registration),'script':locator(script),'result_path':str(ROOT/'results/storage-archives/orthology-sequences-20261104/continuation-20261106/result.json')}
  release_request=BASE/'release-census-request.json';release_payload={'kind':'finite-held-census-release-after-verified-orthology-continuation','root':str(ROOT),'release_script':None,
   'policy_module':locator(POLICY_BASE/'storage_policy.py'),'policy':locator(POLICY_BASE/'policy.json'),'census':{'plan':schedule['plan'],'ledger':schedule['ledger'],'helper':schedule['helper'],'manifest':schedule['source_manifest'],'jobs':ids},
   'old_census':{'job_id':'6983060','script':payload['census_gate']['held'][0]['script']},'cleanup':cleanup_record}
  # The request binds the script, so the script obtains the request hash from
  # its separately published immutable registration instead of hashing itself.
  release_source=locator(BASE/'release_census.py');regpath=BASE/'release-census-registration.json'
  hash_reader=shlex.join(['python','-B','-c','import json;from pathlib import Path;print(json.loads(Path('+repr(str(regpath))+').read_bytes())["request_sha256"])'])
  rcommand=shlex.join(['python','-s','-B',release_source['path'],'--request',str(release_request),'--request-sha256','REGISTERED_REQUEST_SHA','--self-sha256',release_source['sha256']]).replace('REGISTERED_REQUEST_SHA','"$('+hash_reader+')"')
  release_tests={str(BASE/'test_release_census.py'):sha(BASE/'test_release_census.py')};rprefix='export TZ=UTC PYTHONDONTWRITEBYTECODE=1\n'+source_test_prefix(release_tests,list(release_tests),'$SLURM_TMPDIR/release-tests.xml','$SLURM_TMPDIR/release-tests')
  release=one_plan(config,identity,'storage-reserve-200k-v3-census-release-20261106','storage-reserve-200k-census-release',rcommand,{'cpus':1,'memory':'8G','hours':.5,'gpu':False,'array':None},[cleanup],slurm._script,rprefix)
  release_script=Path(release[0]['tasks'][0]['script']);release_payload['release_script']={'path':str(release_script),'sha256':hashlib.sha256(release[1][release_script.name].encode()).hexdigest()}
  save(release_request,release_payload);rp=locator(release_request)
  rl,ri,rh=publish(*release);release_job=ri[0];release_script=Path(release[0]['tasks'][0]['script']);save(BASE/'release-census-registration.json',{'job_id':release_job,'request_sha256':rp['sha256'],'source_sha256':release_source['sha256'],'script':locator(release_script),'script_sha256':sha(release_script),'actual_held_native':rh[release_job]})
  activation_payload={**request['activation_inputs'],'kind':'storage-reserve-200k-controller-activation-v1','components':components,'census_schedule':schedule,'archive':{k:cleanup_record[k] for k in ['job_id','source','request','registration','result_path']},'first_census_job_id':ids[0]}
  activation_request=BASE/'controller-activation-request.json';save(activation_request,activation_payload);cp=locator(activation_request);activation_source=locator(BASE/'activate_controllers.py')
  phase='storage-reserve-200k-v3-activation-20261106';name='storage-reserve-200k-v3-controller-activation';directory=ROOT/'plans'/phase;script=directory/(name+'.sbatch');activation_ledger=ROOT/'state'/(phase+'-submission.json')
  flags=['--request',cp['path'],'--request-sha256',cp['sha256'],'--source-sha256',activation_source['sha256'],'--native-plan',str(directory/'plan.json'),'--native-ledger',str(activation_ledger),'--native-script',str(script),'--native-task',name,'--native-tests-junit','activation-tests.xml']
  ccommand=shlex.join(['python','-s','-B',activation_source['path'],*flags])
  test=activation_payload['native_tests']['source'];cprefix='export TZ=UTC PYTHONDONTWRITEBYTECODE=1\n'+source_test_prefix({test['path']:test['sha256']},[test['path']],'$SLURM_TMPDIR/activation-tests.xml','$SLURM_TMPDIR/activation-tests')
  activation=one_plan(config,identity,phase,name,ccommand,{'cpus':2,'memory':'16G','hours':1,'gpu':False,'array':None},[cleanup,ids[0]],slurm._script,cprefix)
  cl,ci,ch=publish(*activation);activation_job=ci[0]
  save(area/'all-accepted-held.json',{'census_jobs':ids,'archive_job':cleanup,'release_job':release_job,'activation_job':activation_job,'all_requests_written_before_release':True})
  held_old()
  for ident,task in [(cleanup,archive[0]['tasks'][0]),(release_job,release[0]['tasks'][0]),(activation_job,activation[0]['tasks'][0])]:
   guard();held_native(native(ident),ident,task);runner(['scontrol','release',ident],capture_output=True,text=True);f=native(ident)
   require(f.get('JobId')==ident and f.get('UserId')=='pwa209(3164768)' and f.get('Command')==task['script'] and (f.get('JobState') in {'RUNNING','COMPLETING','COMPLETED'} or f.get('JobState')=='PENDING' and f.get('Reason') not in {'JobHeldUser','JobHeldAdmin'} and int(f.get('Priority','0'))>0),'Positive native release acknowledgement required')
   save(area/('released-'+ident+'.json'),f)
  held_old();result={'status':'finite_study_DAG_registered_and_released','native_job_id':job,'census_jobs_held_until_cleanup':ids,'archive_job':cleanup,'census_release_job':release_job,'activation_job':activation_job,'old_census64_retained_held':'6983060','minimum_personal_free_files':200000,'scientific_result':False,'automatic_replay_permitted':False}
  save(area/'complete.json',result);print(json.dumps(result),flush=True)

if __name__=='__main__':main()
