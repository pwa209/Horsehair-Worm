"""One finite, byte-verified archive of four completed OrthoFinder FASTA-copy scopes."""
from pathlib import Path,PurePosixPath
import argparse,errno,gzip,hashlib,importlib.util,json,os,re,shlex,stat,subprocess,tarfile,time

ROOT=Path('/scratch/pwa209/horsehair-worm')
HELPER_SHA='e8a599304f4ac2561339114e7965f3cca2f06c87d565822bb32aabbdf322d979'
AUDIT_SHA='9532a2372dbafaad99976c7e079ca2556b8365b00b8ec90ef8907aa6e1ee0415'
SOURCE='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
PRIMARY='data/molecular/orthology-repair-v7-20261013/of_run/Results_Oct13'
MANTIS='data/molecular/orthology_mantis/proteomes/OrthoFinder/Results_Oct21'
SPECS=((PRIMARY+'/Orthogroup_Sequences',12597),(PRIMARY+'/Single_Copy_Orthologue_Sequences',5429),(MANTIS+'/Orthogroup_Sequences',21596),(MANTIS+'/Single_Copy_Orthologue_Sequences',3523))
COMPLETIONS={'data/molecular/orthology-repair-v7-20261013/orthology_receipt.json':('5455844','native_primary_ant_fly_orthology_computed'),'state/molecular-benchmark-primary.json':('5455844','native_primary_molecular_benchmark_completed'),'state/molecular-mantis-orthology.json':('6632058','native_three_species_corroboration_computed')}
MAX_BYTES=20_000_000_000;MAX_FILES=43145

def require(ok,message):
 if not ok:raise ValueError(message)
def sha(path):
 h=hashlib.sha256()
 with Path(path).open('rb') as f:
  for raw in iter(lambda:f.read(1048576),b''):h.update(raw)
 return h.hexdigest()
def canonical(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def pin(locator,helper,*,root):
 require(set(locator)=={'path','sha256'} and re.fullmatch('[0-9a-f]{64}',str(locator['sha256'])),'Exact immutable locator required')
 path=helper.ordinary(locator['path'],root);require(sha(path)==locator['sha256'],'Immutable molecular/source proof changed: '+str(path));return path
def load_helper(locator,*,root):
 require(locator['sha256']==HELPER_SHA and sha(locator['path'])==HELPER_SHA,'Exact existing archive integrity helper required')
 spec=importlib.util.spec_from_file_location('original_integrity_helper',locator['path']);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);module.ordinary(locator['path'],root);return module
def expected_tables(root):
 return {str(root/base/name) for base in (PRIMARY,MANTIS) for name in ('Phylogenetic_Hierarchical_Orthogroups/N0.tsv','Orthogroups/Orthogroups.tsv')}

def reference_check(value,scopes,document,*,root):
 """No selected leaf, scope digest, or enclosing directory digest is disposable."""
 output=root/'data/molecular/orthology-repair-v7-20261013/of_run'
 def path_ref(text,location,checksum=None):
  if not isinstance(text,str):return
  path=Path(text)
  for scope in scopes:
   if path.is_absolute() and (path==scope or path.is_relative_to(scope)):
    raise ValueError('Selected sequence scope is referenced: '+str(path)+' in '+str(document)+' '+location)
   if checksum is not None and path.is_absolute() and scope.is_relative_to(path):
    raise ValueError('Aggregate content binding encloses selected scope: '+str(path))
   if str(scope) in text:raise ValueError('Selected sequence scope appears in retained metadata: '+str(document))
  if path==output:
   require(checksum is None and location in {'$/commands/0/command/10','$/orthology/commands/0/command/10'},'Only the two original output-command root references are informational')
 def visit(v,location='$'):
  if isinstance(v,str):path_ref(v,location)
  elif isinstance(v,dict):
   if isinstance(v.get('path'),str) and re.fullmatch('[0-9a-f]{64}',str(v.get('sha256',''))):path_ref(v['path'],location,v['sha256'])
   for key,item in v.items():
    path_ref(key,location+'/<key>',item if re.fullmatch('[0-9a-f]{64}',str(item)) else None)
    visit(item,location+'/'+str(key))
  elif isinstance(v,list):
   for index,item in enumerate(v):visit(item,location+'/'+str(index))
 visit(value)

def metadata_authority(request,helper,*,root):
 audit_path=pin(request['audit'],helper,root=root);require(request['audit']['sha256']==AUDIT_SHA and audit_path==root/'maintenance/orthology-storage-audit-20261104/result.json','Exact completed read-only audit required')
 audit=json.loads(audit_path.read_bytes());require(audit['status']=='read_only_orthology_storage_audit_complete' and audit['native_job_id']=='8508393' and not audit['input_files_changed'] and audit['files_removed']==0 and not audit['metadata']['problems'] and not audit['inventory']['anomalies'],'Original audit is incomplete or conflicting')
 tables=request['retained_tables'];require(len(tables)==4 and {x['path'] for x in tables}==expected_tables(root),'Exactly four original tables must remain ordinary files')
 observed={x['path']:x['sha256'] for x in audit['inventory']['required_original_tables']};require(all(observed[x['path']]==x['sha256'] for x in tables),'Retained original table hashes differ from actual audit')
 bindings={};documents={};scopes=[root/name for name,count in SPECS]
 for locator in request['binding_documents']+tables:
  path=pin(locator,helper,root=root);require(path.stat().st_size<=64*1024**2,'Finite immutable metadata required');bindings[str(path)]=locator['sha256']
  require(not (path.parent==root/'state' and ('submission' in path.name or 'registry' in path.name)),'Mutable whole ledger/registry cannot be a per-file immutable guard')
  if path.suffix=='.json' or path.name.endswith('.json.gz'):
   raw=path.read_bytes();raw=gzip.decompress(raw) if path.name.endswith('.json.gz') else raw;require(len(raw)<=64*1024**2,'Finite decoded metadata required');value=json.loads(raw);reference_check(value,scopes,path,root=root);documents[str(path)]=value
 for name,(job,status) in COMPLETIONS.items():
  path=str(root/name);require(path in documents,'Required original completion receipt missing');value=documents[path]
  require(value['status']==status and str(value['slurm_job_id'])==job and value['source_sha256']==SOURCE and value['config_sha256']==CONFIG,'Original successful molecular receipt identity differs')
  if value.get('completion_hash'):require(value['completion_hash']==canonical({k:v for k,v in value.items() if k!='completion_hash'}),'Original completion content hash differs')
 # Every readable immutable JSON/TSV/source in the bounded audit graph remains
 # pinned. Submission ledgers and plan records are handled as task projections.
 audited={path:item['sha256'] for path,item in audit['metadata']['documents'].items() if item.get('status')=='read' and not path.startswith(str(root/'plans')+'/') and not (Path(path).parent==root/'state' and ('submission' in Path(path).name or 'registry' in Path(path).name))}
 require(all(bindings.get(path)==digest for path,digest in audited.items()),'Complete bounded immutable molecular/source graph must be retained')
 bindings[str(audit_path)]=request['audit']['sha256'];return bindings,audit

def original_tasks(request,helper,*,root):
 values=request['original_tasks'];require(len(values)==2 and {(x['job'],x['task_name']) for x in values}=={('5455844','molecular-pilot'),('6632058','mantis-orthology')},'Exact two original successful task projections required')
 result=[]
 for item in values:
  ledger_path=helper.ordinary(item['ledger_path'],root);ledger=json.loads(ledger_path.read_bytes());planpath=pin(item['plan'],helper,root=root);plan=json.loads(planpath.read_bytes());script=pin(item['script'],helper,root=root);text=script.read_text().replace('\r\n','\n');task=next(t for t in plan['tasks'] if t['name']==item['task_name']);row=ledger['tasks'][item['task_name']]
  require(ledger['source_sha256']==SOURCE and ledger['config_sha256']==CONFIG and ledger['plan_sha256']==canonical(plan) and plan['config_sha256']==CONFIG and plan['release_identity']['sha256']==SOURCE,'Original task plan/source/config projection differs')
  require(row['state']=='submitted' and row['job_id']==item['job'] and task['script']==str(script) and isinstance(row.get('command'),list) and row['command'][-1]==str(script) and task.get('script_sha256')==canonical(text) and row.get('script_sha256',task.get('script_sha256'))==task.get('script_sha256') and task['resources']['cpus']==8 and task['resources'].get('gpus',0)==0,'Original successful task/script/resource projection differs')
  result.append({'job':item['job'],'task_name':item['task_name'],'ledger_path':str(ledger_path),'task':task,'submitted_row':row,'plan':item['plan'],'script':item['script']})
 return result

def native_proofs(stdout,jobs):
 result={}
 for job,cpus in jobs.items():
  rows=[line.split('|') for line in stdout.splitlines() if line.split('|',1)[0]==job]
  require(len(rows)==1 and len(rows[0])==8,'Unique native accounting row required');v=rows[0]
  require(v[1]=='pwa209' and v[2]=='def-ptewarie_cpu' and v[3]=='COMPLETED' and v[4]=='0:0' and v[5]==str(cpus) and v[6].isdigit() and int(v[6])>0 and 'gpu' not in v[7].lower(),'Actual owned successful original allocation differs')
  result[job]={'job':job,'user':v[1],'account':v[2],'state':v[3],'exit_code':v[4],'cpus':int(v[5]),'elapsed_seconds':int(v[6]),'alloc_tres':v[7]}
 return result

def gate_fields(fields,queue,gate,job,*,root):
 require(gate['job_id']=='6983037' and gate['archive_job_id']==job,'Exact existing first census and this archive job required')
 expected=str(root/'plans/study-census-resilience-v1-20261024/rolling-storage-census-61.sbatch')
 require(gate['script']['path']==expected,'Exact original census script required')
 f=fields
 require(f.get('JobId')=='6983037' and f.get('UserId')=='pwa209(3164768)' and f.get('Account')=='def-ptewarie_cpu' and f.get('JobState')=='PENDING' and f.get('JobName')=='hhw-rolling-storage-census-61' and f.get('Command')==expected and f.get('NumCPUs')=='8' and f.get('NumNodes') in {'1','1-1'} and f.get('MinMemoryNode') in {'64G','65536M'} and f.get('TimeLimit') in {'06:00:00','0-06:00:00'} and 'ArrayJobId' not in f and all('gpu' not in f.get(k,'').lower() for k in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask')),'Original owned pending8CPU64G6h census allocation required')
 dependencies=f.get('Dependency','');atom=r'[1-9][0-9]*(?:\((?:unfulfilled|fulfilled)\))?';group='afterany:'+atom+'(?::'+atom+')*'
 require(re.fullmatch(group+'(?:,'+group+')*',dependencies) is not None,'Only complete AND-afterany dependency syntax is accepted; OR/unknown modes refused')
 afterany=[number for group in dependencies.split(',') for number in re.findall(r'(?<=:)([1-9][0-9]*)(?=[:(]|$)',group)]
 require(job in afterany and set(afterany)<={job,'6983031'} and len(afterany)==len(set(afterany)),'Census must wait afterany on this archive writer and retain only its original predecessor')
 rows=[line.split('|') for line in queue.splitlines() if line.strip()];ids={'6983037','6983056','6983059','6983060'}
 require(len(rows)==4 and {x[0] for x in rows}==ids and all(len(x)==4 and x[1]=='pwa209' and x[2]=='def-ptewarie_cpu' and x[3]=='PENDING' for x in rows),'All four original censuses must remain pending; no RUNNING/COMPLETING census may overlap unlink')
 return {'first_census_job_id':'6983037','archive_job_id':job,'dependency':dependencies,'all_four_censuses_pending':True}

def census_gate(registration,helper,runner,*,root,job,request_sha,wrapper_sha):
 require(registration['job_id']==job and registration['request_sha256']==request_sha and registration['wrapper_sha256']==wrapper_sha,'Actual archive registration/source/request binding differs')
 gate=registration['census_gate'];pin(gate['script'],helper,root=root)
 native=runner(['scontrol','show','job','6983037','-o'],capture_output=True,text=True);native.check_returncode();require(not native.stderr,'Actual census dependency observation required');fields=dict(x.split('=',1) for x in shlex.split(native.stdout) if '=' in x)
 queue=runner(['squeue','-h','-j','6983037,6983056,6983059,6983060','-o','%i|%u|%a|%T'],capture_output=True,text=True);queue.check_returncode();require(not queue.stderr,'Actual four-census state observation required')
 return gate_fields(fields,queue.stdout,gate,job,root=root)

def checked_boundary(registration,helper,runner,*,root,job,request_sha,wrapper_sha,record_path,deadline,guard,clock=time.monotonic,wall=time.time):
 last=[None]
 def observe(force=False):
  if force or last[0] is None or clock()-last[0]>=60:
   observed=census_gate(registration,helper,runner,root=root,job=job,request_sha=request_sha,wrapper_sha=wrapper_sha);observed['checked_unix']=wall();helper.atomic(record_path,observed);last[0]=clock()
 def checkpoint():deadline();guard();observe()
 def boundary():deadline();guard();observe(force=True)
 boundary() # Actual gate proof is mandatory before the caller can hash/pack.
 return checkpoint,boundary

def stamp(info):return [info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns]
def file_observation(directory,name,*,root,descriptor=None):
 require(re.fullmatch(r'OG[0-9]+\.(?:fa|fasta)',name),'Only generated flat OG FASTA copies are eligible')
 flags=os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_CLOEXEC',0)
 named=os.stat(name,dir_fd=descriptor,follow_symlinks=False) if descriptor is not None else os.lstat(directory/name);require(stat.S_ISREG(named.st_mode) and named.st_nlink==1,'Ordinary non-symlink single-link leaf required')
 fd=os.open(name,flags,dir_fd=descriptor) if descriptor is not None else os.open(directory/name,flags)
 try:
  before=os.fstat(fd);require(stamp(before)==stamp(named),'Leaf replaced between anchored lookup and open');require(stat.S_ISREG(before.st_mode) and before.st_nlink==1 and (not hasattr(os,'geteuid') or before.st_uid==os.geteuid()),'Owned ordinary single-link sequence copy required');digest=hashlib.sha256();size=0
  while True:
   raw=os.read(fd,1048576)
   if not raw:break
   size+=len(raw);require(size<=MAX_BYTES,'Sequence byte bound exceeded');digest.update(raw)
  after=os.fstat(fd);require(stamp(before)==stamp(after) and size==before.st_size,'Sequence copy changed during independent EOF hash')
  return {'sha256':digest.hexdigest(),'bytes':size,'identity':stamp(before)}
 finally:os.close(fd)

def roster(specs,helper,*,root,checkpoint=lambda:None):
 rows=[]
 for relative,count in specs:
  directory=helper.ordinary(root/relative,root,directory=True)
  names=sorted(os.listdir(directory));require(len(names)==count,'Exact audited flat sequence count changed')
  fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW) if os.name=='posix' else None
  try:
   for name in names:checkpoint();rows.append({'path':relative+'/'+name,**file_observation(directory,name,root=root,descriptor=fd)})
  finally:
   if fd is not None:os.close(fd)
 require(len(rows)==sum(count for relative,count in specs) and sum(row['bytes'] for row in rows)<=MAX_BYTES,'Exact finite sequence roster/byte bound required');return sorted(rows,key=lambda row:row['path'])

def validate_rows(rows,specs):
 scopes={relative for relative,count in specs};seen=set();counts={relative:0 for relative in scopes}
 for row in rows:
  require(set(row)=={'path','sha256','bytes','identity'} and isinstance(row['path'],str),'Exact immutable sequence roster entry required');path=PurePosixPath(row['path']);parent=path.parent.as_posix()
  require(not path.is_absolute() and path.as_posix()==row['path'] and '..' not in path.parts and parent in scopes and re.fullmatch(r'OG[0-9]+\.(?:fa|fasta)',path.name) and row['path'] not in seen,'Scope escape, duplicate, or unknown sequence name refused')
  require(re.fullmatch('[0-9a-f]{64}',row['sha256']) and type(row['bytes']) is int and row['bytes']>=0 and isinstance(row['identity'],list) and len(row['identity'])==5,'Original byte/stat evidence required');seen.add(row['path']);counts[parent]+=1
 require(counts==dict(specs) and sum(r['bytes'] for r in rows)<=MAX_BYTES,'Audited scope count or byte bound differs')

def source_guard(specs,helper,bindings,*,root):
 guard=helper.binding_guard(bindings,root=root);identities={}
 for relative,count in specs:
  path=root/relative;info=helper.ordinary(path,root,directory=True).lstat();identities[path]=(info.st_dev,info.st_ino,info.st_uid,info.st_mode)
 def check():
  guard()
  for path,expected in identities.items():
   info=os.lstat(path);require(stat.S_ISDIR(info.st_mode) and (info.st_dev,info.st_ino,info.st_uid,info.st_mode)==expected,'Original flat sequence directory identity changed')
 return check

def pack_and_remove(case,rows,specs,authority,helper,bindings,*,root,checkpoint=lambda:None,boundary=lambda:None,after_unlink=lambda path:None):
 validate_rows(rows,specs);boundary();case=Path(case);require(case.is_relative_to(root/'results/storage-archives') and case.resolve()==case and not case.exists(),'Fresh archive transaction required');case.mkdir();helper.sync_directory(case.parent)
 intent={'kind':'completed-orthology-derived-sequence-copies','source_root':str(root),'members':rows,'authority':authority,'scientific_result':False,'admission_authority':False};helper.atomic(case/'intent.json',intent)
 descriptors={relative:(os.open(root/relative,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW) if os.name=='posix' else None) for relative,count in specs}
 def observed(row):
  path=PurePosixPath(row['path']);return file_observation(root/path.parent.as_posix(),path.name,root=root,descriptor=descriptors[path.parent.as_posix()])
 try:
  archive=case/'sequences.tar'
  with tarfile.open(archive,'x:') as stream:
   for row in rows:
    checkpoint();require(observed(row)=={k:row[k] for k in ('sha256','bytes','identity')},'Original changed before archival');path=PurePosixPath(row['path']);fd=descriptors[path.parent.as_posix()];flags=os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)
    opened=os.open(path.name,flags,dir_fd=fd) if fd is not None else os.open(root/row['path'],flags)
    with os.fdopen(opened,'rb') as source:
     require(stamp(os.fstat(source.fileno()))==row['identity'],'Source changed before tar read');info=tarfile.TarInfo(row['path']);info.size=row['bytes'];info.mode=0o600;stream.addfile(info,source);require(stamp(os.fstat(source.fileno()))==row['identity'],'Source changed during tar read')
  helper.sync_archive(archive);archive_sha=sha(archive);helper.verify_tar(archive,rows,archive_sha,checkpoint=checkpoint)
  index={'kind':'completed-orthology-derived-sequence-copy-index','source_root':str(root),'archive':{'name':'sequences.tar','sha256':archive_sha},'members':rows,'scientific_result':False,'admission_authority':False};helper.atomic(case/'index.json',index)
  receipt={'index_sha256':sha(case/'index.json'),'status':'independent_disk_roundtrip_verified_originals_retained','archive_sha256':archive_sha,'intent_sha256':sha(case/'intent.json'),'files':len(rows),'bytes':sum(row['bytes'] for row in rows)};helper.atomic(case/'receipt.json',receipt)
  bound={**bindings,str(archive):archive_sha,str(case/'intent.json'):sha(case/'intent.json'),str(case/'receipt.json'):sha(case/'receipt.json'),str(case/'index.json'):sha(case/'index.json')};guard=source_guard(specs,helper,bound,root=root)
  for row in rows:checkpoint();guard();require(observed(row)=={k:row[k] for k in ('sha256','bytes','identity')},'Original changed; no original deletion permitted')
  boundary()
  journal={'status':'removal_intent','removed_count':0,'last_removed':None,'archive_sha256':archive_sha,'intent_sha256':receipt['intent_sha256'],'scientific_result':False};helper.atomic(case/'removal.json',journal)
  try:
   for row in rows:
    checkpoint();guard();require(observed(row)=={k:row[k] for k in ('sha256','bytes','identity')},'Original changed immediately before unlink');path=PurePosixPath(row['path']);fd=descriptors[path.parent.as_posix()];guard()
    final=os.stat(path.name,dir_fd=fd,follow_symlinks=False) if fd is not None else os.lstat(root/row['path']);require(stamp(final)==row['identity'] and stat.S_ISREG(final.st_mode) and final.st_nlink==1,'Leaf changed at final anchored unlink guard')
    if fd is None:os.unlink(root/row['path'])
    else:os.unlink(path.name,dir_fd=fd)
    journal['removed_count']+=1;journal['last_removed']=row['path'];journal['status']='removing_verified_orthology_sequence_copies';after_unlink(row['path'])
    if journal['removed_count']%128==0:helper.atomic(case/'removal.json',journal)
   journal['status']='complete_verified_orthology_sequence_copy_compaction';helper.atomic(case/'removal.json',journal)
  except BaseException as error:journal.update(status='interrupted_explicit_reconciliation_required',error_type=type(error).__name__);helper.atomic(case/'removal.json',journal);raise
  return receipt,journal
 finally:
  for fd in descriptors.values():
   if fd is not None:os.close(fd)

def maintenance_allocation(f,job,script,end,*,wall=time.time):
 from datetime import datetime,timezone
 require(f.get('JobId')==job and f.get('UserId')=='pwa209(3164768)' and f.get('JobState')=='RUNNING' and f.get('Account')=='def-ptewarie_cpu' and f.get('NumCPUs')=='1' and f.get('NumNodes') in {'1','1-1'} and f.get('MinMemoryNode') in {'8G','8192M'} and f.get('TimeLimit') in {'03:00:00','0-03:00:00'} and f.get('JobName')=='hhw-orthology-sequence-archive-20261104' and f.get('Command')==str(script) and f.get('Requeue')=='0' and 'ArrayJobId' not in f and all('gpu' not in f.get(k,'').lower() for k in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask')),'Exact owned1CPU8G3h no-requeue archival allocation required')
 observed=datetime.fromisoformat(f['EndTime']).replace(tzinfo=timezone.utc).timestamp();require(abs(observed-end)<=2 and wall()+600<end,'Actual finite archive end required');return end

def main(argv=None):
 parser=argparse.ArgumentParser();parser.add_argument('--request',type=Path,required=True);parser.add_argument('--request-sha256',required=True);parser.add_argument('--wrapper-sha256',required=True);args=parser.parse_args(argv)
 require(sha(__file__)==args.wrapper_sha256 and sha(args.request)==args.request_sha256,'Frozen archive source/request changed');require(os.geteuid()==3164768 and re.fullmatch('[1-9][0-9]*',os.environ.get('SLURM_JOB_ID','')),'Owned native allocation required');require(subprocess.check_output(['hostname','-f'],text=True,timeout=10).strip().endswith('.narval.calcul.quebec'),'Narval allocation required')
 os.environ['TZ']='UTC'
 if hasattr(time,'tzset'):time.tzset()
 def run(cmd,**kwargs):return subprocess.run(cmd,**{**kwargs,'timeout':60})
 own=run(['scontrol','show','job',os.environ['SLURM_JOB_ID'],'-o'],capture_output=True,text=True);own.check_returncode();require(not own.stderr,'Actual allocation observation required');fields=dict(x.split('=',1) for x in shlex.split(own.stdout) if '=' in x);end=maintenance_allocation(fields,os.environ['SLURM_JOB_ID'],Path(__file__).with_name('archive.sbatch'),float(os.environ['SLURM_JOB_END_TIME']))
 def deadline():require(time.time()+300<end,'Five-minute finalization reserve exhausted')
 scratch=Path(os.environ.get('SLURM_TMPDIR',''));require(scratch.is_absolute() and scratch.is_dir() and not scratch.is_symlink() and scratch.resolve()==scratch and scratch.stat().st_uid==os.geteuid(),'Actual owned node-local roundtrip directory required')
 request=json.loads(args.request.read_bytes());helper=load_helper(request['helper'],root=ROOT);bindings,audit=metadata_authority(request,helper,root=ROOT);tasks=original_tasks(request,helper,root=ROOT)
 registration_path=helper.ordinary(Path(__file__).with_name('registration.json'),ROOT);registration=json.loads(registration_path.read_bytes());bindings[str(registration_path)]=sha(registration_path);bindings[registration['census_gate']['script']['path']]=registration['census_gate']['script']['sha256']
 native=run(['sacct','-X','-n','-P','-j','5455844,6632058,8508393','--format=JobIDRaw,User,Account,State,ExitCode,AllocCPUS,ElapsedRaw,AllocTRES%256'],capture_output=True,text=True);native.check_returncode();require(not native.stderr,'Actual final accounting observation required');proof=native_proofs(native.stdout,{'5455844':8,'6632058':8,'8508393':1})
 import horsehair
 from horsehair import budget,core
 framework=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e';require(Path(horsehair.__file__).resolve()==framework/'src/horsehair/__init__.py' and core.source_identity(framework)['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1','Original qualified execution framework required')
 original=Path(request['original_source_release']);expected=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf');require(original.resolve()==expected.resolve() and original.is_dir() and original.stat().st_uid==os.geteuid() and core.source_identity(original)['sha256']==SOURCE,'Actual complete immutable primary source release required')
 budget.native_accounting(ROOT,['5455844','6632058','8508393'],runner=run)
 from horsehair.execution_resources import load_resource_policy
 config=core.read_json(framework/'configs/study.json');policy=load_resource_policy(ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json','f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5',root=ROOT,config=config)
 with budget.resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=3,job_id=os.environ['SLURM_JOB_ID'],resource_policy=policy):
  guard=source_guard(SPECS,helper,bindings,root=ROOT)
  checkpoint,boundary=checked_boundary(registration,helper,run,root=ROOT,job=os.environ['SLURM_JOB_ID'],request_sha=args.request_sha256,wrapper_sha=args.wrapper_sha256,record_path=Path(__file__).with_name('census-gate-observation.json'),deadline=deadline,guard=guard)
  rows=roster(SPECS,helper,root=ROOT,checkpoint=checkpoint);require(len(rows)==MAX_FILES,'Exactly43145 actual eligible sequence copies required')
  authority={'domain':'completed_orthology_derived_sequence_copies','original_tasks':tasks,'original_source_release':str(original),'actual_native':proof,'source_sha256':SOURCE,'config_sha256':CONFIG,'audit':request['audit'],'immutable_bindings':bindings,'wrapper_sha256':args.wrapper_sha256,'request_sha256':args.request_sha256,'retained_tables':request['retained_tables']}
  case=Path(request['case']);require(case==ROOT/'results/storage-archives/orthology-sequences-20261104','Exact new finite archival case required');helper.ordinary(case.parent,ROOT,directory=True);receipt,journal=pack_and_remove(case,rows,SPECS,authority,helper,bindings,root=ROOT,checkpoint=checkpoint,boundary=boundary)
 # Keep the four now-empty sequence directories and all result-root structure.
 pruned=[]
 for locator in request['binding_documents']+request['retained_tables']:pin(locator,helper,root=ROOT)
 import horsehair
 from horsehair import budget,core
 framework=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e';require(Path(horsehair.__file__).resolve()==framework/'src/horsehair/__init__.py' and core.source_identity(framework)['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1','Original qualified quota reader required')
 quota=budget.require_storage_headroom(ROOT,required_files=0,runner=run);result={'status':'complete_verified_orthology_sequence_copy_archive','files_removed':journal['removed_count'],'empty_sequence_directories_removed':pruned,'archive_sha256':receipt['archive_sha256'],'archive_bytes':receipt['bytes'],'personal_available_files':quota['personal_available_files'],'wrapper_sha256':args.wrapper_sha256,'request_sha256':args.request_sha256,'scientific_result':False,'admission_authority':False,'original_inputs_preserved':True};helper.atomic(case/'result.json',result);print(json.dumps(result),flush=True)

if __name__=='__main__':main()
