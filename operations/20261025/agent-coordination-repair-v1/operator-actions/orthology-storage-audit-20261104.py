from pathlib import Path,PurePosixPath
from collections import Counter
import gzip,hashlib,json,os,re,shlex,stat,subprocess,time
ROOT=Path('/scratch/pwa209/horsehair-worm')
RELEASE='20261013-829e3817bee1-0e2b1ebce8bf'
PREFIXES=('data/molecular/orthology_mantis/proteomes','data/molecular/orthology-repair-v7-20261013/of_run')
SCOPES={'WorkingDirectory','Single_Copy_Orthologue_Sequences','Orthogroup_Sequences','Gene_Trees','Resolved_Gene_Trees','MultipleSequenceAlignments','Species_Tree','Comparative_Genomics_Statistics','Orthologues','Phylogenetic_Hierarchical_Orthogroups','Orthogroups'}
MAX_ENTRIES=100000;MAX_DOCS=128;MAX_SMALL=64*1024**2
def require(ok,message):
 if not ok:raise ValueError(message)
def digest(path):
 h=hashlib.sha256()
 with Path(path).open('rb') as f:
  for block in iter(lambda:f.read(1048576),b''):h.update(block)
 return h.hexdigest()
def ordinary(path,directory=False):
 path=Path(path);require(path.is_absolute() and path.is_relative_to(ROOT) and path.resolve()==path,'Confined ordinary path required')
 info=path.lstat();require(stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode),'Ordinary file/directory required')
 if hasattr(os,'geteuid'):require(info.st_uid==os.geteuid(),'Foreign-owned path refused')
 return info
def inventory(roots,checkpoint=lambda:None):
 totals=Counter();levels={str(i):{} for i in range(1,5)};scopes={};retained=[];anomalies=[];visited=0
 for prefix,base in roots:
  ordinary(base,True);pending=[base]
  while pending:
   checkpoint();directory=pending.pop();ordinary(directory,True)
   with os.scandir(directory) as stream:entries=sorted(stream,key=lambda e:e.name)
   for entry in entries:
    checkpoint();visited+=1;require(visited<=MAX_ENTRIES,'100000-entry inventory bound exceeded')
    path=directory/entry.name;parts=path.relative_to(base).parts
    kind='symlinks' if entry.is_symlink() else 'directories' if entry.is_dir(follow_symlinks=False) else 'regular_files' if entry.is_file(follow_symlinks=False) else 'special'
    totals[kind]+=1
    if kind in {'symlinks','special'}:anomalies.append({'path':str(path),'reason':kind});continue
    for depth in range(1,5):
     key=prefix+'/'+str(PurePosixPath(*parts[:depth]));v=levels[str(depth)].setdefault(key,Counter());v[kind]+=1
    for i,part in enumerate(parts):
     if part in SCOPES:
      key=prefix+'/'+str(PurePosixPath(*parts[:i+1]));v=scopes.setdefault(key,Counter());v[kind]+=1
    if kind=='directories':pending.append(path)
    elif path.name=='N0.tsv' and 'Phylogenetic_Hierarchical_Orthogroups' in parts or path.name=='Orthogroups.tsv' and 'Orthogroups' in parts:
     observed=ordinary(path);require(observed.st_size<=MAX_SMALL,'Required retained table exceeds bounded hash size');before=(observed.st_dev,observed.st_ino,observed.st_size,observed.st_mtime_ns,observed.st_ctime_ns);value=digest(path);after=path.lstat();require(before==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns),'Original table changed')
     retained.append({'path':str(path),'sha256':value,'bytes':observed.st_size,'preserve':True})
 return {'entries':visited,'logical_bytes_unmeasured':True,'per_leaf_ownership_unverified':True,'directory_ownership_verified':True,'leaf_classification':'DirEntry type; no explicit per-leaf stat or logical-size measurement','totals':dict(totals),'relative_depth_counts':{d:{k:dict(v) for k,v in sorted(values.items())} for d,values in levels.items()},'named_scope_counts':{k:dict(v) for k,v in sorted(scopes.items())},'required_original_tables':retained,'anomalies':anomalies}
def metadata(paths,roots,checkpoint=lambda:None):
 pending=[(Path(p),None,None) for p in paths];seen={};references={};problems=[];total=0;jobs=set();task_ids={};completions=[]
 while pending:
  checkpoint();path,expected,referrer=pending.pop()
  if str(path) in seen:
   if expected and seen[str(path)].get('sha256')!=expected:problems.append({'path':str(path),'reason':'conflicting_declared_sha','expected':expected})
   continue
  require(len(seen)<MAX_DOCS,'128-document closure bound exceeded')
  try:info=ordinary(path)
  except FileNotFoundError:seen[str(path)]={'status':'missing','referrer':referrer};continue
  if info.st_size>MAX_SMALL:seen[str(path)]={'status':'retained_large_not_hashed','bytes':info.st_size,'referrer':referrer};continue
  with path.open('rb') as stream:raw=stream.read(MAX_SMALL+1)
  require(len(raw)<=MAX_SMALL,'Observed metadata file exceeds bound');total+=len(raw);require(total<=256*1024**2,'Metadata total byte bound exceeded');checksum=hashlib.sha256(raw).hexdigest();seen[str(path)]={'status':'read','sha256':checksum,'bytes':len(raw),'referrer':referrer}
  if expected and checksum!=expected:problems.append({'path':str(path),'reason':'declared_sha_mismatch','expected':expected,'actual':checksum})
  if path.suffix!='.json' and not path.name.endswith('.json.gz'):continue
  decoded=gzip.decompress(raw) if path.suffix=='.gz' else raw;require(len(decoded)<=MAX_SMALL,'Decoded metadata bound exceeded');value=json.loads(decoded)
  if isinstance(value,dict):
   if value.get('status') in {'native_primary_ant_fly_orthology_computed','native_primary_molecular_benchmark_completed','native_three_species_corroboration_computed'}:
    ident=str(value.get('slurm_job_id',''));completions.append({k:value.get(k) for k in ('status','slurm_job_id','source_sha256','config_sha256','completion_hash') }|{'path':str(path)})
    if re.fullmatch('[1-9][0-9]*',ident):jobs.add(ident)
   for name,row in (value.get('tasks',{}).items() if isinstance(value.get('tasks',{}),dict) else []):
    if name in {'molecular-pilot','mantis-orthology'} and isinstance(row,dict) and re.fullmatch('[1-9][0-9]*',str(row.get('job_id',''))):task_ids[name]=str(row['job_id']);jobs.add(str(row['job_id']))
  def visit(v,location='$'):
   if isinstance(v,str):
    p=Path(v)
    if p.is_absolute():
     for prefix,base in roots:
      if p==base or p.is_relative_to(base):references.setdefault(str(p),[]).append({'document':str(path),'location':location,'declared_sha256':None})
   elif isinstance(v,dict):
    locators=[]
    if isinstance(v.get('path'),str) and re.fullmatch('[0-9a-f]{64}',str(v.get('sha256',''))):locators.append((v['path'],v['sha256']))
    for key,item in v.items():
     visit(key,location+'/<key>');visit(item,location+'/'+str(key))
     if isinstance(key,str) and re.fullmatch('[0-9a-f]{64}',str(item)):locators.append((key,item))
    for name,checksum in locators:
     p=Path(name)
     if p.is_absolute() and p.is_relative_to(ROOT):
      for prefix,base in roots:
       if p==base or p.is_relative_to(base):references.setdefault(str(p),[]).append({'document':str(path),'location':location,'declared_sha256':checksum})
      if p.suffix=='.json' or p.name.endswith('.json.gz'):pending.append((p,checksum,str(path)))
   elif isinstance(v,list):
    for index,item in enumerate(v):visit(item,location+'/'+str(index))
  visit(value)
 return {'documents':seen,'candidate_root_references':references,'problems':problems,'completion_records':completions,'original_task_job_ids':task_ids,'native_job_ids':sorted(jobs)}
def main():
 from datetime import datetime,timezone
 require(os.geteuid()==3164768 and re.fullmatch('[1-9][0-9]*',os.environ.get('SLURM_JOB_ID','')),'Owned actual allocated job required');require(subprocess.check_output(['hostname','-f'],text=True,timeout=10).strip().endswith('.narval.calcul.quebec'),'Narval allocation required')
 os.environ['TZ']='UTC'
 if hasattr(time,'tzset'):time.tzset()
 case=Path(__file__).parent;request_path=case/'request.json';request=json.loads(request_path.read_bytes());require(case==ROOT/'maintenance/orthology-storage-audit-20261104' and digest(__file__)==request['audit_source_sha256'] and digest(case/'audit.sbatch')==request['script_sha256'],'Exact pinned audit source/case required')
 p=subprocess.run(['scontrol','show','job',os.environ['SLURM_JOB_ID'],'-o'],capture_output=True,text=True,timeout=30);p.check_returncode();require(not p.stderr,'Actual allocation required');fields=dict(x.split('=',1) for x in shlex.split(p.stdout) if '=' in x)
 require(fields.get('JobId')==os.environ['SLURM_JOB_ID'] and fields.get('UserId')=='pwa209(3164768)' and fields.get('JobState')=='RUNNING' and fields.get('Account')=='def-ptewarie_cpu' and fields.get('NumCPUs')=='1' and fields.get('NumNodes') in {'1','1-1'} and fields.get('MinMemoryNode') in {'4G','4096M'} and fields.get('TimeLimit') in {'01:00:00','0-01:00:00'} and fields.get('JobName')=='hhw-orthology-storage-audit-20261104' and fields.get('Command')==str(case/'audit.sbatch') and 'ArrayJobId' not in fields and all('gpu' not in fields.get(k,'').lower() for k in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask')),'Exact scalar1CPU4G1h allocation required')
 end=datetime.fromisoformat(fields['EndTime']).replace(tzinfo=timezone.utc).timestamp();require(abs(end-float(os.environ['SLURM_JOB_END_TIME']))<=2,'Actual finite end differs')
 def checkpoint():require(time.time()+120<end,'Finite finalization reserve exhausted')
 roots=[(prefix,ROOT/prefix) for prefix in PREFIXES];primary=ROOT/'data/molecular/orthology-repair-v7-20261013';mantis=ROOT/'data/molecular/orthology_mantis'
 paths=[ROOT/('state/'+RELEASE+'-managed-initial-submission.json'),ROOT/('state/'+RELEASE+'-molecular-corroboration-submission.json'),ROOT/('plans/'+RELEASE+'-managed-initial/plan.json'),ROOT/'plans/mantis-reference-recovery-v14-20261021-auxiliary/plan.json',ROOT/'state/molecular-benchmark-primary.json',ROOT/'state/molecular-orthology.json',ROOT/'state/molecular-mantis-orthology.json']
 paths+=[primary/name for name in ['reference_preparation.json','orthology_receipt.json','proteomes.json','protein_gene_map.tsv','fly_target_families.source_provenance.json','orthology_coverage.json','gene_orthology.tsv','fly_ant_identity.tsv','fly_gene_catalog.tsv','fly_target_families.tsv']]
 paths+=[mantis/name for name in ['orthology_plan.json','orthology_coverage.json','gene_orthology.tsv','orthofinder.sh']]
 paths+=[ROOT/('releases/'+RELEASE+'/src/horsehair/'+name) for name in ['molecular_pipeline.py','molecular.py','mantis_recovery.py']]
 report={'status':'read_only_orthology_inventory_incomplete','scientific_result':False,'admission_authority':False,'files_removed':0,'input_files_changed':False,'source_sha256':request['audit_source_sha256'],'request_sha256':digest(request_path),'native_job_id':os.environ['SLURM_JOB_ID'],'allocation':fields,'started_unix':time.time(),'archive_authorized_by_this_report':False}
 try:
  report['metadata']=metadata(paths,roots,checkpoint);report['inventory']=inventory(roots,checkpoint);ids=report['metadata']['native_job_ids'];require(len(ids)<=16,'Finite original native proof roster required')
  if ids:
   native=subprocess.run(['sacct','-X','-n','-P','-j',','.join(ids),'--format=JobIDRaw,User,Account,State,ExitCode,AllocCPUS,ElapsedRaw,AllocTRES%256'],capture_output=True,text=True,timeout=60)
   report['native_accounting']={'returncode':native.returncode,'stdout':native.stdout,'stderr':native.stderr,'requested_job_ids':ids,'interpretation':'Read-only observation; pending/unknown/failed records do not authorize archiving'}
  report['status']='read_only_orthology_storage_audit_complete';report['finished_unix']=time.time()
 except BaseException as error:report.update(error_type=type(error).__name__,error=str(error),finished_unix=time.time());raise
 finally:
  raw=(json.dumps(report,indent=2,sort_keys=True)+'\n').encode();require(len(raw)<=64*1024**2,'Report size bound exceeded')
  with (case/'result.json').open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
  print(json.dumps({'status':report['status'],'native_job_id':report['native_job_id'],'files_removed':0,'input_files_changed':False,'result':str(case/'result.json'),'regular_files':report.get('inventory',{}).get('totals',{}).get('regular_files'),'original_task_job_ids':report.get('metadata',{}).get('original_task_job_ids',{})}),flush=True)
if __name__=='__main__':main()
