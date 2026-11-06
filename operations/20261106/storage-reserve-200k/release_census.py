"""One finite release of the already registered census chain after verified cleanup."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
import argparse,hashlib,json,os,re,runpy,shlex,stat,subprocess,sys,time

ROOT=Path('/scratch/pwa209/horsehair-worm')
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
MANIFEST='288af211cad1ad186105e6b2dafa08d4195920b132ab9efef367af98c0fbdf6e'
PROVIDER='3ed26e6a2ec4a7683b880f593dfb172e86ff9cf8529424ccb047e543cc334927'
ARCHIVE='1ef9c2985932b665860a5badf689a076857c3dbbfe311e74d1c2ddec1d6db558'
ORIGINAL_SOURCE='fd26774ad98b1a20703263cc0c8f468fbfe4b0877544dd6da4798e509e36a6eb'
ORIGINAL_REQUEST='25bf39cd70a09ee4321415e80aa8858e30add550a90e5d98ab8676d5a5103564'
PHASE='storage-reserve-200k-v2-census-20261106'
POLICY_MODULE='0e8691671a32e2b55cfcd626d461c2708d11d05b00c0491721c6c3be13107f7d'
POLICY='2e8e2bd2c1f70aa988a9414d2b44d9ffd0f768f8bc2e329747b7839b2855f2d9'
TERMINAL={'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}

def require(ok,message):
    if not ok:raise ValueError(message)

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def canonical(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()

def ordinary(path,root=ROOT,directory=False):
    p=Path(path);require(p.is_absolute() and p.is_relative_to(root) and p.resolve()==p,'Exact confined ordinary path required')
    for parent in [p,*p.parents]:
        if not parent.is_relative_to(root):break
        require(not parent.is_symlink(),'Redirected path refused')
    s=p.lstat();require(stat.S_ISDIR(s.st_mode) if directory else stat.S_ISREG(s.st_mode) and s.st_nlink==1,'Ordinary owned single-link path required')
    if hasattr(os,'geteuid'):require(s.st_uid==os.geteuid(),'Foreign-owned path refused')
    return p

def pin(binding,root=ROOT):
    require(type(binding) is dict and set(binding)=={'path','sha256'} and re.fullmatch('[0-9a-f]{64}',str(binding['sha256'])),'Exact immutable locator required')
    p=ordinary(binding['path'],root);require(sha(p)==binding['sha256'],'Pinned source/evidence changed: '+str(p));return p

def save(path,value):
    raw=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    with Path(path).open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())

def epoch(value):
    if type(value) in (int,float):return float(value)
    if isinstance(value,str):
        try:
            parsed=datetime.fromisoformat(value.replace('Z','+00:00'));return (parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed).timestamp()
        except ValueError:return None
    return None

def time_proof(fields,wanted):
    for key in ('EligibleTime','StartTime','BeginTime'):
        observed=epoch(fields.get(key))
        if observed is not None and abs(observed-wanted)<=1:return {'field':key,'value':fields[key],'epoch':observed}
    return None

def json_time_proof(value,job,wanted):
    rows=value.get('jobs',[]);require(type(rows) is list and len(rows)==1 and str(rows[0].get('job_id'))==job,'Exact single native JSON job required');found=[]
    def values(node,path):
        if isinstance(node,dict):
            if node.get('set') is False or node.get('infinite') is True:return
            for key,item in node.items():
                if key in {'number','value','timestamp','time'}:values(item,path+'.'+key)
        else:
            actual=epoch(node)
            if actual is not None and abs(actual-wanted)<=1:found.append({'field':path,'value':node,'epoch':actual})
    def visit(node,path):
        if isinstance(node,dict):
            for key,item in node.items():
                if re.sub('[^a-z]','',key.lower()) in {'eligible','eligibletime','eligibilitytime','start','starttime','begin','begintime'}:values(item,path+'.'+key)
                visit(item,path+'.'+key)
        elif isinstance(node,list):
            for index,item in enumerate(node):visit(item,path+'['+str(index)+']')
    visit(rows[0],'jobs[0]');return found[0] if found else None

def native_identity(fields,job,script,name,*,held=True,cpus='8',memory=('64G','65536M'),limit=('06:00:00','6:00:00','0-06:00:00')):
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)' and fields.get('Account')=='def-ptewarie_cpu'
        and fields.get('Command')==str(script) and fields.get('JobName')==name,'Owned source-bound native identity required')
    require(fields.get('NumCPUs')==cpus and fields.get('NumNodes') in {'1','1-1'} and fields.get('MinMemoryNode') in memory
        and fields.get('TimeLimit') in limit and fields.get('Requeue')=='0' and 'ArrayJobId' not in fields
        and all('gpu' not in fields.get(k,'').lower() for k in ('ReqTRES','AllocTRES','Gres','TresPerNode','TresPerTask')),'Exact scalar CPU-only resources required')
    if held:require(fields.get('JobState')=='PENDING' and fields.get('Reason')=='JobHeldUser' and fields.get('Priority')=='0','Exact original held job required')
    else:require(fields.get('JobState') in {'RUNNING','COMPLETING','COMPLETED'} or fields.get('JobState')=='PENDING' and fields.get('Reason') not in {'JobHeldUser','JobHeldAdmin'} and int(fields.get('Priority','0'))>0,'Positive owned native release acknowledgement required')

def verify_plan(census,root=ROOT):
    planpath=pin(census['plan'],root);ledgerpath=pin(census['ledger'],root);plan=json.loads(planpath.read_bytes());ledger=json.loads(ledgerpath.read_bytes())
    require(planpath==root/'plans'/PHASE/'plan.json' and ledgerpath==root/'state'/(PHASE+'-submission.json'),'Exact amended census plan namespace required')
    require(plan['root']==str(root) and plan['phase']==PHASE and plan['config_sha256']==ledger['config_sha256']==CONFIG
        and plan['release_identity']['sha256']==ledger['source_sha256']==SOURCE and ledger['plan_sha256']==canonical(plan),'Full unchanged configuration/source and registered plan required')
    require(census['helper']['sha256']==PROVIDER and census['manifest']['sha256']==MANIFEST,'Exact qualified amended provider source required')
    helper=pin(census['helper'],root);manifest=pin(census['manifest'],root);support=root/'maintenance/storage-reserve-200k-v2-20261106/census'
    require(helper==support/'run-fresh-storage-census-resilience-v1-20261024.py' and manifest==support/'source-manifest.json','Exact approved provider support namespace required')
    require(len(plan['tasks'])==64 and len(census['jobs'])==64 and len(set(census['jobs']))==64,'Exactly64 distinct existing census allocations required')
    require(set(ledger['tasks'])=={t['name'] for t in plan['tasks']},'Complete registered task membership required')
    out=[];pins=[census['plan'],census['ledger'],census['helper'],census['manifest']]
    for index,task in enumerate(plan['tasks'],1):
        name='rolling-storage-census-'+str(index).zfill(2);script=ordinary(task['script'],root);text=script.read_text();row=ledger['tasks'][name];job=row['job_id']
        require(task['name']==name and script==root/'plans'/PHASE/(name+'.sbatch') and task['command'] in text
            and task['script_sha256']==row['script_sha256']==canonical(text) and row['state']=='submitted' and job==census['jobs'][index-1]
            and re.fullmatch('[1-9][0-9]*',job),'Exact original registered scalar job/script required')
        require(task['resources']=={'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None}
            and task['depends']==([] if index==1 else ['rolling-storage-census-'+str(index-1).zfill(2)])
            and task['dependency_kind']==('afterok' if index==1 else 'afterany'),'Unchanged finite serial census resources/dependencies required')
        require(task['rolling_census_provider']=={'helper':census['helper'],'source_manifest':census['manifest'],'case_id':'rolling-'+str(index).zfill(2)},'Same qualified provider on every slot required')
        args=shlex.split(task['command']);require(args.count(str(helper))==1,'Exact provider invocation required');tail=args[args.index(str(helper))+1:]
        require(len(tail)%2==0 and len(set(tail[::2]))==len(tail)//2,'Unique explicit command bindings required');flags=dict(zip(tail[::2],tail[1::2]))
        require(flags['--source-manifest']==str(manifest) and flags['--source-manifest-sha256']==MANIFEST and flags['--native-plan']==str(planpath)
            and flags['--native-ledger']==str(ledgerpath) and flags['--native-script']==str(script) and flags['--native-task']==name,'Complete original source/native command binding required')
        begins=re.findall(r'^#SBATCH --begin=([^\r\n]+)$',text,re.M);require(len(begins)==(0 if index==1 else 1),'Original first-immediate/future-time script contract required')
        minimum=None if index==1 else epoch(begins[0]);require(index==1 or minimum is not None,'Original future earliest time must be exact')
        predecessor=flags['--kernel-qualification-job-id'] if index==1 else census['jobs'][index-2]
        resolved=set(row.get('issued_native_dependencies',[]))|{str(proof.get('job_id')) for proof in row.get('completed_native_dependencies',[])}
        require(row.get('dependency_kind')==task['dependency_kind'] and predecessor in resolved,'Original native dependency resolution is missing')
        out.append({'job':job,'index':index,'script':str(script),'name':'hhw-'+name,'dependency':predecessor,'kind':'afterok' if index==1 else 'afterany','minimum':minimum})
        pins.append({'path':str(script),'sha256':sha(script)})
    return out,pins

def dependency_guard(fields,task,final):
    dep=fields.get('Dependency','');wanted=task['kind']+':'+task['dependency']
    if re.fullmatch(re.escape(wanted)+r'(?:\((?:unfulfilled|fulfilled)\))?',dep):return
    require(dep in {'(null)','None',''},'Native chain was altered or contains an OR dependency')
    row=final(task['dependency']);require(row['job']==task['dependency'] and row['state'] in TERMINAL,'Positive final original predecessor required')
    if task['kind']=='afterok':require(row['state']=='COMPLETED' and row['exit']=='0:0','Original success dependency is not fulfilled')

def targets(tasks,anchor):
    require(isinstance(anchor,datetime) and anchor.tzinfo is not None,'Explicit readiness anchor required')
    return {t['job']:max((anchor+timedelta(hours=4*(t['index']-1))).timestamp(),t['minimum']) for t in tasks if t['index']>1}

def verify_cleanup(cleanup,row,root=ROOT):
    for key in ('source','request','registration','script'):pin(cleanup[key],root)
    require(row['job']==cleanup['job_id'] and row['state']=='COMPLETED' and row['exit']=='0:0' and row['cpus']==1 and 0<row['elapsed']<=43200,'Actual successful1CPU12h continuation required')
    reg=json.loads(Path(cleanup['registration']['path']).read_bytes())
    require(reg['job_id']==cleanup['job_id'] and reg['source_sha256']==cleanup['source']['sha256'] and reg['request_sha256']==cleanup['request']['sha256']
        and reg['script_sha256']==cleanup['script']['sha256'],'Original continuation registration source/request differs')
    native_identity(reg['actual_held_native'],cleanup['job_id'],cleanup['script']['path'],'hhw-orthology-archive-continuation-20261106',cpus='1',memory=('8G','8192M'),limit=('12:00:00','0-12:00:00'))
    script=Path(cleanup['script']['path']).read_text();require(cleanup['source']['path'] in script and cleanup['request']['path'] in script and cleanup['request']['sha256'] in script,'Actual continuation script must invoke the pinned source/request')
    resultpath=ordinary(cleanup['result_path'],root);result=json.loads(resultpath.read_bytes())
    require(result['status']=='complete_verified_orthology_sequence_copy_archive_continuation' and result['files_removed']==43145
        and result['native_job_id']==cleanup['job_id'] and result['source_sha256']==cleanup['source']['sha256'] and result['request_sha256']==cleanup['request']['sha256']
        and result['original_archive_sha256']==ARCHIVE and result['original_wrapper_sha256']==ORIGINAL_SOURCE and result['original_request_sha256']==ORIGINAL_REQUEST
        and result['original_inputs_preserved'] is True and result['no_repacking'] is True and result['scientific_result'] is False,'Complete original archive/removal lineage required')
    receiptpath=resultpath.with_name('continuation-receipt.json');journalpath=resultpath.with_name('removal.json')
    require(sha(ordinary(receiptpath,root))==result['continuation_receipt_sha256'] and sha(ordinary(journalpath,root))==result['removal_journal_sha256'],'Complete continuation receipt/journal bytes required')
    receipt=json.loads(receiptpath.read_bytes());journal=json.loads(journalpath.read_bytes())
    require(receipt['status']=='independent_existing_tar_roundtrip_verified_originals_retained' and receipt['files']==43145
        and receipt.get('original_archive_sha256',receipt.get('archive_sha256'))==ARCHIVE,'Independent existing-tar roundtrip proof required')
    require(journal['status']=='complete_verified_existing_orthology_archive_removal' and journal['removed_count']==43145
        and journal.get('original_archive_sha256',journal.get('archive_sha256'))==ARCHIVE,'Complete43145-file actual removal journal required')
    return result,{'path':str(resultpath),'sha256':sha(resultpath)}

def issue_once(path,command,runner=subprocess.run):
    save(path.with_suffix('.intent.json'),{'command':command,'automatic_retry_permitted':False})
    try:
        p=runner(command,capture_output=True,text=True,timeout=30);answer={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr,'timed_out':False}
    except subprocess.TimeoutExpired as e:answer={'returncode':None,'stdout':str(e.stdout),'stderr':str(e.stderr),'timed_out':True}
    save(path.with_suffix('.response.json'),answer);return answer

def main():
    p=argparse.ArgumentParser();p.add_argument('--request',type=Path,required=True);p.add_argument('--request-sha256',required=True);p.add_argument('--self-sha256',required=True);a=p.parse_args()
    require(sha(__file__)==a.self_sha256 and sha(ordinary(a.request))==a.request_sha256,'Exact release source/request required')
    require(os.geteuid()==3164768 and re.fullmatch('[1-9][0-9]*',os.environ.get('SLURM_JOB_ID','')),'Owned actual scalar native allocation required')
    require(subprocess.check_output(['hostname','-f'],text=True,timeout=10).strip().endswith('.narval.calcul.quebec'),'Narval allocation required')
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    request=json.loads(a.request.read_bytes());require(request['kind']=='finite-held-census-release-after-verified-orthology-continuation' and request['root']==str(ROOT),'Exact finite personal-root request required')
    script=pin(request['release_script']);reg=json.loads(ordinary(Path(__file__).with_name('release-census-registration.json')).read_bytes());job=os.environ['SLURM_JOB_ID']
    require(reg['job_id']==job and reg['request_sha256']==a.request_sha256 and reg['source_sha256']==a.self_sha256 and reg['script_sha256']==request['release_script']['sha256'],'Actual immutable handoff registration required')
    def query(cmd):
        x=subprocess.run(cmd,capture_output=True,text=True,timeout=30);x.check_returncode();require(not x.stderr,'Exact positive native observation required');return x.stdout
    def fields(ident):return dict(x.split('=',1) for x in shlex.split(query(['scontrol','show','job',ident,'-o'])) if '=' in x)
    own=fields(job);native_identity(own,job,script,'hhw-storage-reserve-200k-census-release',held=False,cpus='1',memory=('8G','8192M'),limit=('00:30:00','0-00:30:00'))
    require(own['JobState']=='RUNNING','Actual running release allocation required');end=epoch(own['EndTime']);require(end is not None and abs(end-float(os.environ['SLURM_JOB_END_TIME']))<=2,'Actual allocation end required')
    def deadline():require(time.time()+120<end,'Two-minute finite finalization reserve exhausted')
    def final(ident):
        raw=query(['sacct','-X','-n','-P','-j',ident,'--format=JobIDRaw,User,Account,State,ExitCode,AllocCPUS,ElapsedRaw,AllocTRES%256'])
        rows=[x.split('|') for x in raw.splitlines() if x.strip()];require(len(rows)==1 and len(rows[0])==8,'One actual scalar native record required');x=rows[0]
        require(x[0]==ident and x[1:3]==['pwa209','def-ptewarie_cpu'] and re.fullmatch('[0-9]+:[0-9]+',x[4]) and x[5].isdigit() and x[6].isdigit() and 'gpu' not in x[7].lower(),'Owned scalar CPU-only final record required')
        return {'job':x[0],'state':x[3].split()[0].split('+')[0],'exit':x[4],'cpus':int(x[5]),'elapsed':int(x[6]),'alloc_tres':x[7]}
    deadline();stop=min(time.monotonic()+600,time.monotonic()+end-time.time()-120)
    while True:
        try:
            cleanup_native=final(request['cleanup']['job_id'])
            if cleanup_native['state']=='COMPLETED' and cleanup_native['exit']=='0:0':break
            require(cleanup_native['state'] in {'PENDING','RUNNING','COMPLETING'},'Continuation prerequisite failed')
        except subprocess.TimeoutExpired:pass
        require(time.monotonic()+5<stop,'Continuation final accounting is still pending');time.sleep(5)
    cleanup_result,cleanup_binding=verify_cleanup(request['cleanup'],cleanup_native)
    policy_parent=ROOT/'maintenance/storage-reserve-200k-v2-20261106'
    require(request['policy_module']=={'path':str(policy_parent/'storage_policy.py'),'sha256':POLICY_MODULE}
        and request['policy']=={'path':str(policy_parent/'policy.json'),'sha256':POLICY},'Same explicitly approved shared200k policy required')
    module=pin(request['policy_module']);policy_path=pin(request['policy']);api=runpy.run_path(str(module),run_name='verified_finite_census_release_policy');policy=api['verify_policy'](policy_path,request['policy']['sha256'],root=ROOT)
    tasks,pins=verify_plan(request['census']);old=request['old_census'];require(old['job_id']=='6983060','Exact original remaining census must be retained held');pin(old['script'])
    resultpath=Path(cleanup_binding['path'])
    pins.extend([request['policy_module'],request['policy'],old['script'],request['release_script'],{'path':str(a.request),'sha256':a.request_sha256},cleanup_binding,
        {'path':str(resultpath.with_name('continuation-receipt.json')),'sha256':cleanup_result['continuation_receipt_sha256']},
        {'path':str(resultpath.with_name('removal.json')),'sha256':cleanup_result['removal_journal_sha256']}])
    pins.extend(request['cleanup'][key] for key in ('source','request','registration','script'))
    area=Path(__file__).with_name('census-release-evidence');area.mkdir(mode=0o700,exist_ok=True);ordinary(area,directory=True);case=area/('native-'+job);case.mkdir(mode=0o700,exist_ok=False)
    def guard():
        deadline()
        for loc in pins:pin(loc)
        native_identity(fields(old['job_id']),old['job_id'],old['script']['path'],'hhw-rolling-storage-census-64')
    def held(task,label):
        f=fields(task['job']);native_identity(f,task['job'],task['script'],task['name']);dependency_guard(f,task,final);save(case/(label+'.json'),f);return f
    import horsehair
    from horsehair import budget,core
    from horsehair.execution_resources import load_resource_policy
    framework=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e';require(Path(horsehair.__file__).resolve()==framework/'src/horsehair/__init__.py' and core.source_identity(framework)['sha256']==SOURCE,'Exact qualified execution framework required')
    config=core.read_json(framework/'configs/study.json');resource=load_resource_policy(ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json','f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5',root=ROOT,config=config)
    def quota():return budget.require_storage_headroom(ROOT,required_files=2000,required_bytes=1_000_000_000,reserve_files=api['reserve_for'](2000,policy),reserve_bytes=2_000_000_000_000)
    with budget.resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=.5,job_id=job,resource_policy=resource):
        guard();before={t['job']:held(t,'before-all-'+t['job']) for t in tasks};fresh=quota();anchor=datetime.now(timezone.utc).replace(microsecond=0);wanted=targets(tasks,anchor)
        save(case/'validated-intent.json',{'jobs':[t['job'] for t in tasks],'cleanup_native':cleanup_native,'cleanup_result':cleanup_binding,'fresh_quota':fresh,'readiness_anchor_utc':anchor.isoformat(),'desired_earliest_epochs':wanted,'new_jobs_submitted':0,'automatic_replay_permitted':False})
        for task in tasks[1:]:
            guard();pre=held(task,'before-update-'+task['job']);timestamp=datetime.fromtimestamp(wanted[task['job']],timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
            issue_once(case/('update-'+task['job']),['scontrol','update','JobId='+task['job'],'StartTime='+timestamp]);post=held(task,'after-update-'+task['job'])
            for key in ('JobId','UserId','Account','JobName','Command','NumCPUs','NumNodes','MinMemoryNode','TimeLimit','ReqTRES','WorkDir','StdOut','StdErr'):require(pre.get(key)==post.get(key),'Unrelated native identity/resource field changed: '+key)
            proof=time_proof(post,wanted[task['job']])
            if proof is None:proof=json_time_proof(json.loads(query(['scontrol','--json','show','job',task['job']])),task['job'],wanted[task['job']])
            require(proof is not None,'Native earliest time is not positively observable; do not release')
        guard();fresh=quota();released=[];save(case/'cadence-complete.json',{'jobs':[t['job'] for t in tasks],'fresh_quota':fresh,'earliest_epochs':wanted})
        for task in tasks:
            guard();held(task,'before-release-'+task['job']);issue_once(case/('release-'+task['job']),['scontrol','release',task['job']]);post=fields(task['job']);native_identity(post,task['job'],task['script'],task['name'],held=False)
            save(case/('released-'+task['job']+'.json'),post);released.append(task['job'])
        guard();result={'status':'existing64_censuses_released_after_verified_cleanup','released_jobs':released,'cleanup_native_job_id':request['cleanup']['job_id'],'verified_files_removed':cleanup_result['files_removed'],'storage_policy':policy.receipt(),'fresh_quota_before_release':fresh,'old_census_retained_held':'6983060','new_jobs_submitted':0,'automatic_replay_permitted':False,'scientific_result':False}
        save(case/'result.json',result);print(json.dumps(result),flush=True)

if __name__=='__main__':main()
