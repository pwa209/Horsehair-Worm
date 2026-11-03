"""One-off archive of unbound raw reader leaves from positively terminal controllers."""
from pathlib import Path,PurePosixPath
import argparse,gzip,hashlib,json,os,re,shlex,shutil,stat,subprocess,tarfile,tempfile,time

ROOT=Path('/scratch/pwa209/horsehair-worm')
CAMPAIGN=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
PREFIXES=('accounting-reads','scheduler-reads','quota-reads','held-reads','registered-snapshot-reads')
KINDS={'query','runner-error','response','verdict','command','exception','before','after','intent','result'}
MAX_FILES=100000;MAX_BYTES=10_000_000_000
PRIMARY_SUPPORT=ROOT/'maintenance/study-primary-external-census-policy-v1-20261023'

def require(value,message):
    if not value:raise ValueError(message)

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for raw in iter(lambda:f.read(1048576),b''):h.update(raw)
    return h.hexdigest()

def canonical(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()

def recovery_goal(free_files,verified_removed,minimum_files_removed):
    require(type(minimum_files_removed) is int and 0<=minimum_files_removed<=1000000,'Finite nonnegative project cleanup minimum required')
    return free_files>=300000 and verified_removed>=minimum_files_removed

def sync_directory(path):
    """Fail closed unless the native filesystem positively syncs the directory."""
    flags=os.O_RDONLY|getattr(os,'O_DIRECTORY',0)
    fd=os.open(path,flags)
    try:os.fsync(fd)
    finally:os.close(fd)

def sync_archive(path):
    # A write-capable descriptor is also required by Windows fixture fsync;
    # no archive bytes are changed through this descriptor.
    with Path(path).open('r+b') as f:f.flush();os.fsync(f.fileno())
    sync_directory(Path(path).parent)

def atomic(path,value):
    path=Path(path);stage=path.with_name(path.name+'.tmp')
    with stage.open('x') as f:json.dump(value,f,sort_keys=True);f.flush();os.fsync(f.fileno())
    os.replace(stage,path)
    sync_directory(path.parent)

def ordinary(path,root,*,directory=False):
    path=Path(path);root=Path(root)
    require(path.is_absolute() and path!=root and path.is_relative_to(root) and path.resolve()==path,'Ordinary confined path required')
    for p in [path,*path.parents]:
        if p==root:break
        require(not p.is_symlink(),'Symlink refused')
    info=path.lstat()
    require(stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode) and info.st_nlink==1,'Ordinary single-link file/directory required')
    if hasattr(os,'geteuid'):require(info.st_uid==os.geteuid(),'Foreign-owned path refused')
    return path

def observed(path,root):
    path=ordinary(path,root);s=path.stat();identity=(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
    digest=sha(path);after=path.stat()
    require(identity==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns),'Source changed during hashing')
    return {'sha256':digest,'bytes':s.st_size,'identity':list(identity)}

def reader_namespace(name):
    parts=PurePosixPath(name).parts
    return bool(parts and (any(parts[0]==p or parts[0].startswith(p+'-') for p in PREFIXES) or len(parts)>=4 and re.fullmatch('accounting-refresh-[0-9]{6}',parts[0]) and parts[1] in {'native-accounting','registered-reads'} and parts[2].startswith('read-')))

def allowed(name):
    parts=PurePosixPath(name).parts
    if len(parts)<2 or str(PurePosixPath(name))!=name or any(x in {'.','..',''} for x in parts):return False
    if not reader_namespace(name):return False
    if any(any(x in p.lower() for x in ('protocol','checkpoint','plan')) for p in parts):return False
    leaf=parts[-1];m=re.fullmatch(r'attempt-([1-5])-([a-z-]+)\.(json|txt)',leaf)
    return leaf in {'intent.json','result.json'} or bool(m and (m[3]=='json' and m[2] in KINDS or m[3]=='txt' and m[2] in {'stdout','stderr'}))

def referenced_paths(value,source):
    result=set()
    def visit(v):
        if isinstance(v,str) and Path(v).is_absolute() and Path(v).is_relative_to(source) and Path(v).suffix in {'.json','.gz','.txt'}:result.add(str(Path(v)))
        elif isinstance(v,dict):
            for key,x in v.items():visit(key);visit(x)
        elif isinstance(v,list):
            for x in v:visit(x)
    visit(value);return result

def bound_exclusions(source,documents,protected,*,root):
    """Read declared source-bound documents and their JSON locator closure."""
    excluded=set(map(str,protected));seen={};pending=list(documents)
    while pending:
        item=pending.pop();path=ordinary(item['path'],root);digest=sha(path)
        require(digest==item['sha256'],'Bound proof/source document changed')
        if str(path) in seen:require(seen[str(path)]==digest,'Conflicting bound document');continue
        seen[str(path)]=digest
        require(len(seen)<=10000 and path.stat().st_size<=512_000_000,'Finite bound document graph exceeded')
        raw=gzip.decompress(path.read_bytes()) if path.suffix=='.gz' else path.read_bytes()
        require(len(raw)<=512_000_000,'Finite decoded document exceeded')
        value=json.loads(raw)
        excluded.update(referenced_paths(value,source))
        def locators(v):
            if isinstance(v,dict):
                if isinstance(v.get('path'),str) and re.fullmatch('[0-9a-f]{64}',str(v.get('sha256',''))):
                    p=Path(v['path'])
                    if p.is_relative_to(root) and p.suffix in {'.json','.gz'}:pending.append({'path':str(p),'sha256':v['sha256']})
                for key,x in v.items():
                    q=Path(key) if isinstance(key,str) else Path('.')
                    if q.is_absolute() and q.is_relative_to(root) and q.suffix in {'.json','.gz'} and re.fullmatch('[0-9a-f]{64}',str(x)):pending.append({'path':str(q),'sha256':x})
                    locators(x)
            elif isinstance(v,list):
                for x in v:locators(x)
        locators(value)
    return excluded,seen

def roster(source,documents,protected,*,root):
    source=ordinary(source,root,directory=True)
    docs=list(documents)
    for directory,dirs,files in os.walk(source,followlinks=False):
        for name in dirs:require(not (Path(directory)/name).is_symlink(),'Symlink reader subtree refused')
        for name in files:
            path=Path(directory)/name;rel=path.relative_to(source).as_posix()
            if path.suffix in {'.json','.gz'} and not reader_namespace(rel):docs.append({'path':str(path),'sha256':sha(path)})
    excluded,seen=bound_exclusions(source,docs,protected,root=root)
    rows=[]
    for directory,dirs,files in os.walk(source,followlinks=False):
        for name in dirs:require(not (Path(directory)/name).is_symlink(),'Symlink reader subtree refused')
        for name in files:
            path=Path(directory)/name;rel=path.relative_to(source).as_posix()
            if not allowed(rel):continue
            if any(path==Path(x) or path.is_relative_to(Path(x)) for x in excluded):continue
            rows.append({'path':rel,**observed(path,root)})
    rows.sort(key=lambda r:r['path']);require(len(rows)<=MAX_FILES and sum(r['bytes'] for r in rows)<=MAX_BYTES,'Finite diagnostic roster required')
    return rows,seen

def validate_rows(source,rows,root):
    require(isinstance(rows,list) and rows and len(rows)<=MAX_FILES,'Finite explicit roster required')
    seen=set()
    for row in rows:
        require(isinstance(row,dict) and set(row)=={'path','sha256','bytes','identity'} and isinstance(row['path'],str) and allowed(row['path']) and row['path'] not in seen and re.fullmatch('[0-9a-f]{64}',str(row['sha256'])) and type(row['bytes']) is int and row['bytes']>=0 and isinstance(row['identity'],list) and len(row['identity'])==5,'Exact unique allowlisted raw-reader roster required')
        path=source/row['path'];require(path.is_relative_to(source) and path.resolve()==path,'Roster escaped source root');seen.add(row['path'])
    require(sum(r['bytes'] for r in rows)<=MAX_BYTES,'Finite diagnostic byte limit exceeded')

def binding_guard(bindings,*,root):
    pins={}
    for path,digest in bindings.items():
        value=observed(path,root);require(value['sha256']==digest,'Bound source/proof bytes changed');pins[path]=value['identity']
    def check():
        for path,identity in pins.items():
            p=ordinary(path,root);s=p.stat();require([s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns]==identity,'Bound source/proof changed during compaction')
    return check

def verify_tar(archive,rows,expected_sha,*,checkpoint=lambda:None):
    require(sha(archive)==expected_sha,'Archive SHA changed')
    expected={r['path']:r for r in rows};seen=set()
    # Roundtrip bytes are reopened and independently hashed on disk. Native
    # temporary members belong on the allocation's local disk, so verification
    # does not create and unlink a new Lustre inode for every archived leaf.
    scratch=os.environ.get('SLURM_TMPDIR')
    parent=Path(scratch) if scratch else Path(archive).parent
    require(parent.is_absolute() and parent.is_dir() and not parent.is_symlink() and parent.resolve()==parent,'Ordinary existing roundtrip scratch directory required')
    if hasattr(os,'geteuid'):require(parent.stat().st_uid==os.geteuid(),'Roundtrip scratch must be owned by this allocation user')
    with tempfile.TemporaryDirectory(prefix='controller-roundtrip-',dir=parent) as temp:
        restored=Path(temp)/'member'
        with tarfile.open(archive,'r:') as stream:
            for member in stream:
                checkpoint();require(member.isfile() and member.name in expected and member.name not in seen and member.size==expected[member.name]['bytes'],'Archive roster/type/size changed')
                seen.add(member.name)
                with stream.extractfile(member) as src,restored.open('xb') as dst:shutil.copyfileobj(src,dst,1048576)
                require(restored.stat().st_size==member.size and sha(restored)==expected[member.name]['sha256'],'Independent disk roundtrip SHA changed');restored.unlink()
    require(seen==set(expected),'Archive members absent')

def pack(source,destination,rows,authority,*,root,checkpoint=lambda:None):
    source=ordinary(source,root,directory=True);destination=Path(destination)
    validate_rows(source,rows,root)
    require(destination.is_relative_to(root) and destination.resolve()==destination and not destination.exists(),'Fresh archive output required');destination.mkdir()
    sync_directory(destination.parent)
    intent={'kind':'terminal-controller-raw-reader-evidence','source':str(source),'authority':authority,'members':rows,'scientific_result':False,'admission_authority':False}
    atomic(destination/'intent.json',intent);archive=destination/'evidence.tar'
    with tarfile.open(archive,'x:') as stream:
        for row in rows:
            checkpoint();path=source/row['path'];require(observed(path,root)=={k:row[k] for k in ('sha256','bytes','identity')},'Source changed before archival')
            stream.add(path,arcname=row['path'],recursive=False)
    sync_archive(archive)
    digest=sha(archive);verify_tar(archive,rows,digest,checkpoint=checkpoint)
    receipt={'status':'independent_roundtrip_verified_originals_retained','archive_sha256':digest,'intent_sha256':sha(destination/'intent.json'),'files':len(rows)}
    atomic(destination/'receipt.json',receipt);return receipt

def remove(source,destination,rows,receipt,*,root,checkpoint=lambda:None,after_unlink=lambda name:None,journal_interval=128):
    source=ordinary(source,root,directory=True);destination=ordinary(destination,root,directory=True)
    validate_rows(source,rows,root)
    require(type(journal_interval) is int and 1<=journal_interval<=128,'Finite recovery journal batch required')
    require(not (destination/'removal.json').exists(),'Prior removal requires explicit reconciliation; never blindly retry')
    require(json.loads((destination/'receipt.json').read_bytes())==receipt and receipt['status']=='independent_roundtrip_verified_originals_retained','Published independent roundtrip receipt changed')
    require(sha(destination/'intent.json')==receipt['intent_sha256'],'Archive intent changed')
    value=json.loads((destination/'intent.json').read_bytes());require(value['source']==str(source) and value['members']==rows,'Archive belongs to another source/roster')
    verify_tar(destination/'evidence.tar',rows,receipt['archive_sha256'],checkpoint=checkpoint)
    for row in rows:
        checkpoint();require(observed(source/row['path'],root)=={k:row[k] for k in ('sha256','bytes','identity')},'Original changed; all originals retained')
    journal={'status':'removal_intent','archive_sha256':receipt['archive_sha256'],'intent_sha256':receipt['intent_sha256'],'removed_count':0,'last_removed':None,'scientific_result':False}
    atomic(destination/'removal.json',journal)
    try:
        for row in rows:
            checkpoint();path=source/row['path'];require(observed(path,root)=={k:row[k] for k in ('sha256','bytes','identity')},'Original changed immediately before unlink')
            path.unlink();after_unlink(row['path']);journal['removed_count']+=1;journal['last_removed']=row['path'];journal['status']='removing'
            # The immutable durable archive and intent define every source
            # leaf. A crash can leave up to one batch unacknowledged, which
            # always requires explicit reconciliation before any retry.
            if journal['removed_count']%journal_interval==0:atomic(destination/'removal.json',journal)
        journal['status']='complete_terminal_diagnostic_compaction';atomic(destination/'removal.json',journal);return journal
    except BaseException as error:
        journal.update(status='interrupted_explicit_reconciliation_required',error_type=type(error).__name__);atomic(destination/'removal.json',journal);raise

def controller_identity(path,job,*,root=ROOT):
    path=ordinary(path,root,directory=True)
    af=path.parent==root/CAMPAIGN.relative_to(ROOT) and re.fullmatch(r'controller-(?:[0-3]-)?'+re.escape(job),path.name)
    primary=path.parent==root/PRIMARY_SUPPORT.relative_to(ROOT) and path.name=='native-'+job and job=='7046213'
    require(af or primary,'Exact source directory must identify its terminal native controller')
    return path

def terminal_owner(job,row,cost):
    require(row.get('job_id_raw')==job and row.get('user')=='pwa209' and row.get('account')=='def-ptewarie_cpu' and row.get('state') in {'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'} and row.get('cpus')==1 and row.get('elapsed_seconds',0)>0 and not row.get('unallocated_metadata'),'Positive owned terminal scalar controller required')
    require(all(row[k]==cost[k] for k in ('state','exit_code','cpus','elapsed_seconds')) and cost.get('gpus')==0 and 'gpu' not in row.get('alloc_tres','').lower(),'Owned controller and cost proof differ')
    return {'job_id':job,**row,'cost':cost}

def registration_binding(registration,job,*,root):
    require(isinstance(registration,dict) and registration.get('native_job_id',job)==job,'Original native controller registration required')
    values={}
    for key in ('plan','ledger','script'):
        item=registration[key];p=ordinary(item['path'],root);require(sha(p)==item['sha256'],'Original registered controller source changed');values[key]=p.read_bytes()
    plan=json.loads(values['plan']);ledger=json.loads(values['ledger']);name=registration['task_name'];tasks=[t for t in plan['tasks'] if t['name']==name]
    require(len(tasks)==1 and plan.get('root')==str(root) and ledger.get('plan_sha256')==canonical(plan) and ledger.get('source_sha256')==plan.get('release_identity',{}).get('sha256') and ledger.get('config_sha256')==plan.get('config_sha256')=='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35','Original plan/ledger/source/config identity differs')
    task=tasks[0];row=ledger['tasks'][name];text=values['script'].decode().replace('\r\n','\n')
    require(row.get('state')=='submitted' and row.get('job_id')==job and task['script']==registration['script']['path'] and task['script_sha256']==row['script_sha256']==canonical(text) and task['command'] in text and '#SBATCH --job-name=hhw-'+name in text.splitlines(),'Exact original owned job/script/task binding required')
    return {'registration':registration,'task':task}

def source_registration(source,job,*,root):
    source=controller_identity(source,job,root=root)
    if source.name=='native-'+job:
        document=source/'native-bindings.json';value=json.loads(ordinary(document,root).read_bytes());f=value['allocation']
        require(value.get('job')==job and f.get('JobId')==job and f.get('UserId')=='pwa209(3164768)' and f.get('Account')=='def-ptewarie_cpu' and f.get('NumCPUs')=='1','Original native controller allocation binding differs')
        registration=value['registration'];checked=registration_binding(registration,job,root=root)
        require(f.get('Command')==registration['script']['path'] and f.get('JobName')=='hhw-'+registration['task_name'],'Original controller script/name differs')
    else:
        document=source/'controller-start.json';value=json.loads(ordinary(document,root).read_bytes())
        binding=value['unique_allocation_bindings'][job];pairs=[x for x in value['registered_pair_hashes'].values() if x['ledger_path']==binding['ledger']]
        require(len(pairs)==1,'Exact original controller registration pair required');pair=pairs[0]
        planpath=ordinary(pair['plan_path'],root);ledgerpath=ordinary(pair['ledger_path'],root);plan=json.loads(planpath.read_bytes());ledger=json.loads(ledgerpath.read_bytes())
        require(canonical(plan)==pair['plan_sha256'] and canonical(ledger)==pair['ledger_sha256'],'Original controller admission plan/ledger changed')
        task=next(t for t in plan['tasks'] if t['name']==binding['task'])
        require(canonical(task['resources'])==binding['resource_sha256'],'Original controller resource binding changed')
        registration={'native_job_id':job,'task_name':binding['task'],'plan':{'path':str(planpath),'sha256':sha(planpath)},'ledger':{'path':str(ledgerpath),'sha256':sha(ledgerpath)},'script':{'path':task['script'],'sha256':sha(ordinary(task['script'],root))}}
        checked=registration_binding(registration,job,root=root)
    return {**checked,'source_document':{'path':str(document),'sha256':sha(document)}}

def main(argv=None):
    from datetime import datetime,timezone
    import horsehair
    from horsehair import budget,core,repair,slurm
    from horsehair.execution_resources import load_resource_policy
    p=argparse.ArgumentParser();p.add_argument('--request',type=Path,required=True);p.add_argument('--request-sha256',required=True);p.add_argument('--wrapper-sha256',required=True);a=p.parse_args(argv)
    require(sha(__file__)==a.wrapper_sha256 and sha(a.request)==a.request_sha256,'Frozen wrapper/request changed')
    require(os.geteuid()==3164768 and re.fullmatch('[1-9][0-9]*',os.environ.get('SLURM_JOB_ID','')),'Owned allocated native job required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval allocation required')
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    def run(cmd,**kw):return subprocess.run(cmd,**{**kw,'timeout':90})
    observed_native=run(['scontrol','show','job',os.environ['SLURM_JOB_ID'],'-o'],capture_output=True,text=True);observed_native.check_returncode();require(not observed_native.stderr,'Actual own allocation required')
    fields=dict(s.split('=',1) for s in shlex.split(observed_native.stdout) if '=' in s)
    require(fields.get('JobId')==os.environ['SLURM_JOB_ID'] and fields.get('UserId')=='pwa209(3164768)' and fields.get('JobState')=='RUNNING' and fields.get('Account')=='def-ptewarie_cpu' and fields.get('NumCPUs')=='1' and fields.get('NumNodes') in {'1','1-1'} and fields.get('MinMemoryNode') in {'16G','16384M'} and fields.get('TimeLimit') in {'04:00:00','4:00:00','0-04:00:00'} and fields.get('Command')==str(Path(__file__).with_name('controller-compact.sbatch')) and fields.get('JobName')=='hhw-terminal-controller-read-compaction-20261103' and 'ArrayJobId' not in fields and all('gpu' not in fields.get(k,'').lower() for k in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask')),'Exact owned scalar1CPU16G4h controller diagnostic recovery required')
    end=datetime.fromisoformat(fields['EndTime']).replace(tzinfo=timezone.utc).timestamp();env=float(os.environ['SLURM_JOB_END_TIME']);require(abs(end-env)<=2 and time.time()+600<end,'Actual finite maintenance end required')
    def checkpoint():require(time.time()+300<end,'Finite maintenance finalization margin exhausted')
    framework=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e';source='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
    require(Path(horsehair.__file__).resolve()==framework/'src/horsehair/__init__.py' and core.source_identity(framework)['sha256']==source,'Original complete qualified execution framework required')
    request=json.loads(a.request.read_bytes());case=Path(request['case']);require(case.is_relative_to(ROOT/'maintenance') and case.resolve()==case and not case.exists(),'Fresh recovery case required');case.mkdir();sync_directory(case.parent)
    require(request['target_free_files']==300000 and 1<=len(request['sources'])<=1000,'Finite exact target/controller set required')
    minimum=request['minimum_files_removed'];recovery_goal(0,0,minimum)
    policy=load_resource_policy(ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json','f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5',root=ROOT,config=core.read_json(framework/'configs/study.json'))
    with budget.resource_lease(ROOT,core.read_json(framework/'configs/study.json'),cpus=1,gpus=0,max_hours=4,job_id=os.environ['SLURM_JOB_ID'],resource_policy=policy),core.exclusive_file_lock(CAMPAIGN/'coordination-controller.mutex',timeout=60):
        state_sha=sha(CAMPAIGN/'state.json.gz');items=request['sources'];ids=[item['job'] for item in items];require(len(set(ids))==len(ids),'Duplicate terminal controller request')
        for item in items:require(re.fullmatch('[1-9][0-9]*',item['job']) and item['job']!=os.environ['SLURM_JOB_ID'],'Terminal scalar job required');controller_identity(item['path'],item['job'])
        budget.native_accounting(ROOT,ids,runner=run);native=repair._repair_native_states(ids,{},owner='pwa209',account='def-ptewarie_cpu',runner=run)
        completed=[]
        for item in items:
            checkpoint();require(sha(CAMPAIGN/'state.json.gz')==state_sha,'Canonical state changed during maintenance')
            quota=budget.require_storage_headroom(ROOT,required_files=30,reserve_files=5000,required_bytes=10_000_000_000,reserve_bytes=10_000_000_000,runner=run)
            if recovery_goal(quota['personal_available_files'],sum(row['files_removed'] for row in completed),minimum):break
            job=item['job'];completion=slurm.completed_native_dependency(ROOT,job,dependency_kind='afterany');require(completion is not None,'Actual final controller cost proof absent')
            owner=terminal_owner(job,native.get(job,{}),completion['native_records'][job]);source_path=Path(item['path']);registered=source_registration(source_path,job,root=ROOT)
            registration_docs=[registered['source_document']]+[registered['registration'][key] for key in ('plan','ledger')]
            docs=list(request['binding_documents'])+list(item.get('binding_documents',[]))+registration_docs;protected=list(request['protected_paths'])+list(item.get('protected_paths',[]))
            rows,bound=roster(source_path,docs,protected,root=ROOT)
            if not rows:print(json.dumps({'job':job,'status':'all_raw_leaves_bound_or_absent','files_removed':0}),flush=True);continue
            bound[registered['registration']['script']['path']]=registered['registration']['script']['sha256']
            require(str(CAMPAIGN/'state.json.gz') in bound,'Canonical campaign snapshot must be an explicitly pinned document')
            guard=binding_guard(bound,root=ROOT)
            def bound_checkpoint():checkpoint();guard()
            authority={'kind':'actual_terminal_controller_diagnostics','native':owner,'completion':completion,'original_registration':registered,'source':str(source_path),'bound_documents':bound,'request_sha256':a.request_sha256,'wrapper_sha256':a.wrapper_sha256,'canonical_state_sha256':state_sha}
            destination=case/('controller-'+job);receipt=pack(source_path,destination,rows,authority,root=ROOT,checkpoint=bound_checkpoint)
            for path,digest in bound.items():require(sha(path)==digest,'Bound proof/source changed before removal')
            journal=remove(source_path,destination,rows,receipt,root=ROOT,checkpoint=bound_checkpoint)
            completed.append({'job':job,'files_removed':journal['removed_count'],'archive':str(destination),'archive_sha256':receipt['archive_sha256']});atomic(case/'progress.json',{'completed':completed,'scientific_result':False});print(json.dumps(completed[-1]),flush=True)
        final=budget.require_storage_headroom(ROOT,required_files=0,runner=run);removed=sum(r['files_removed'] for r in completed);result={'status':'terminal_controller_diagnostic_recovery_complete','completed':completed,'files_removed':removed,'minimum_files_removed':minimum,'minimum_met':removed>=minimum,'free_files':final['personal_available_files'],'target_reached':final['personal_available_files']>=300000,'scientific_result':False,'admission_authority':False,'canonical_state_sha256':state_sha};atomic(case/'result.json',result);print(json.dumps(result),flush=True)

if __name__=='__main__':main()
