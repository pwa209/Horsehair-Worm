"""One-off terminal diagnostic compaction using the existing qualified archive helper.

Only canonical-complete worker reader evidence is eligible. Scientific inputs,
worker receipts, checkpoints and condition artifacts remain ordinary files.
"""
from pathlib import Path
import argparse,gzip,hashlib,importlib.util,json,os,re,shlex,subprocess,time
from datetime import datetime,timezone

ROOT=Path('/scratch/pwa209/horsehair-worm')
CAMPAIGN=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
HELPER_SHA='045d8650db62c0fa52d7f56bb7bdb2669729509c23ecfbbc847e73cbbff8d42a'
CANARY={'path':str(ROOT/'maintenance/study-af-evidence-archive-canary-v1-20261021/finalized-native-proof/canary.json'),'sha256':'921635cbeac2b954c7946c7a6ddd3c375a8ecd862630492d1aa0ee26b58ac14b'}

def require(value,message):
    if not value:raise ValueError(message)

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def canonical_jobs(state):
    jobs={}
    for ident,row in state['jobs'].items():
        if row['status']!='complete':continue
        job=row['job_id'];require(isinstance(job,str) and re.fullmatch('[1-9][0-9]*',job),'Completed job must be exact scalar ID')
        require(job not in jobs,'Duplicate canonical completed native job')
        require(row['intent']['kind'] in {'training','evaluation','assembly'},'Only scientific worker diagnostics may be archived')
        jobs[job]=ident
    return jobs

def selected_owner(job,native,cost):
    require(native.get('job_id_raw')==job and native.get('user')=='pwa209' and native.get('account') in {'def-ptewarie_cpu','def-ptewarie_gpu'},'Actual owned scalar allocation required')
    require(native.get('state')=='COMPLETED' and native.get('exit_code')=='0:0' and native.get('elapsed_seconds',0)>0 and native.get('cpus',0)>0 and not native.get('unallocated_metadata'),'Only positively completed allocated workers may be compacted')
    require(all(native[k]==cost[k] for k in ('state','exit_code','cpus','elapsed_seconds')),'Actual owner observation differs from cost proof')
    tres=dict(p.split('=',1) for p in native['alloc_tres'].split(',') if '=' in p)
    gpu=int(tres['gres/gpu']) if 'gres/gpu' in tres else sum(int(v) for k,v in tres.items() if k.startswith('gres/gpu:'))
    require(gpu==cost['gpus'],'Actual GPU allocation differs from cost proof')
    return {'job_id':job,'user':native['user'],'account':native['account'],**{k:cost[k] for k in ('state','exit_code','cpus','gpus','elapsed_seconds')}}

def protected_references(receipt,paths):
    """A receipt reference to an eligible leaf makes it ineligible for removal."""
    strings=[]
    def visit(value):
        if isinstance(value,str):strings.append(value)
        elif isinstance(value,dict):
            for item in value.values():visit(item)
        elif isinstance(value,list):
            for item in value:visit(item)
    visit(receipt)
    require(not any(str(path) in text or path.as_posix() in text for path in paths for text in strings),'Worker receipt directly references selected diagnostic leaf')

def allocation(fields,job,environment_end,now):
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)' and fields.get('JobState')=='RUNNING' and fields.get('Account')=='def-ptewarie_cpu' and fields.get('NumCPUs')=='1' and fields.get('NumNodes') in {'1','1-1'} and fields.get('MinMemoryNode') in {'16G','16384M'} and fields.get('TimeLimit') in {'04:00:00','4:00:00','0-04:00:00'} and 'ArrayJobId' not in fields and fields.get('Command')==str(Path(__file__).with_name('compact.sbatch')) and fields.get('JobName')=='hhw-terminal-diagnostic-compaction-20261103','Actual exact script/name owned scalar1CPU16G4h maintenance allocation required')
    require(all('gpu' not in fields.get(k,'').lower() for k in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask')),'Maintenance is CPU-only')
    end=datetime.fromisoformat(fields['EndTime']).replace(tzinfo=timezone.utc).timestamp()
    require(abs(end-environment_end)<=2 and now+600<min(end,environment_end),'Actual finite maintenance end required')
    return min(end,environment_end)

def candidates(helper,state,*,root=ROOT,campaign=CAMPAIGN):
    rows=[]
    for job,ident in canonical_jobs(state).items():
        area=campaign.parent/('native-'+job);receipt=area/'receipt.json'
        require(area.is_dir() and not area.is_symlink() and receipt.is_file(),'Canonical complete native area/receipt absent')
        value=json.loads(receipt.read_bytes())
        require(value.get('status')=='complete_native_work' and value.get('native_job_id')==job and value.get('source_sha256')==SOURCE and value.get('full_config_sha256')==CONFIG,'Original canonical worker receipt changed')
        names=[];total=0
        for directory,dirs,files in os.walk(area,followlinks=False):
            for name in dirs:require(not (Path(directory)/name).is_symlink(),'Reader symlink refused')
            for name in files:
                p=Path(directory)/name;rel=p.relative_to(area).as_posix();first=rel.split('/')[0]
                if not any(first==prefix or first.startswith(prefix+'-') for prefix in helper.READER_PREFIXES):continue
                helper.reader_evidence_name(rel);helper.confined(p,root,file=True);names.append(p);total+=p.stat().st_size
        if not names:continue
        protected_references(value,names)
        require(len(names)<=helper.MAX_FILES,'Worker diagnostic batch exceeds existing archive helper bound')
        rows.append({'job':job,'intent':ident,'source':str(area),'receipt':str(receipt),'receipt_sha256':sha(receipt),'files':len(names),'bytes':total})
    return sorted(rows,key=lambda x:(-x['files'],x['job']))

def execute_one(helper,row,authority,destination,*,root=ROOT,canary=CANARY,checkpoint=lambda:None):
    require(sha(row['receipt'])==row['receipt_sha256'],'Worker receipt changed after audit')
    receipt=helper.pack_terminal_readers(row['source'],destination,authority=authority,root=root,checkpoint=checkpoint)
    require(receipt['files']==row['files'],'Diagnostic roster changed after count')
    journal=helper.remove_verified_originals(destination,receipt['index_sha256'],receipt['archive_sha256'],removal_canary=canary,root=root,checkpoint=checkpoint)
    require(journal['status']=='complete_originals_compacted' and len(journal['removed'])==row['files'],'Diagnostic removal did not positively finish')
    require(sha(row['receipt'])==row['receipt_sha256'],'Original worker receipt changed during compaction')
    return {'job':row['job'],'archive':str(destination),'files_removed':len(journal['removed']),'index_sha256':receipt['index_sha256'],'archive_sha256':receipt['archive_sha256']}

def main(argv=None):
    p=argparse.ArgumentParser();p.add_argument('--helper',type=Path,required=True);p.add_argument('--wrapper-sha256',required=True);p.add_argument('--case',type=Path,required=True);p.add_argument('--mode',choices=['audit','compact'],required=True);p.add_argument('--target-free-files',type=int,default=300000);a=p.parse_args(argv)
    require(re.fullmatch('[0-9a-f]{64}',a.wrapper_sha256) and sha(__file__)==a.wrapper_sha256,'Frozen compaction wrapper source changed')
    import horsehair
    from horsehair import budget,core,repair,slurm
    from horsehair.execution_resources import load_resource_policy
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    require(os.geteuid()==3164768 and os.environ.get('SLURM_JOB_ID','').isdigit(),'Owned scheduled native allocation required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval allocation required')
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py' and core.source_identity(FRAMEWORK)['sha256']==SOURCE,'Original complete qualified framework changed')
    require(a.case.is_relative_to(ROOT/'maintenance') and a.case.resolve()==a.case and not a.case.exists(),'Fresh ordinary maintenance case required')
    require(sha(a.helper)==HELPER_SHA and sha(CANARY['path'])==CANARY['sha256'],'Existing qualified helper/canary changed')
    observed=subprocess.run(['scontrol','show','job',os.environ['SLURM_JOB_ID'],'-o'],capture_output=True,text=True,timeout=90);observed.check_returncode();require(not observed.stderr,'Scheduler errors refused')
    fields=dict(pair.split('=',1) for pair in shlex.split(observed.stdout) if '=' in pair)
    end=allocation(fields,os.environ['SLURM_JOB_ID'],float(os.environ['SLURM_JOB_END_TIME']),time.time())
    def checkpoint():require(time.time()+300<end,'Finite recovery finalization margin exhausted')
    def run(cmd,**kw):return subprocess.run(cmd,**{**kw,'timeout':90})
    spec=importlib.util.spec_from_file_location('qualified_evidence_archive',a.helper);helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
    a.case.mkdir();completed=[]
    policy=load_resource_policy(ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json','f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5',root=ROOT,config=core.read_json(FRAMEWORK/'configs/study.json'))
    with budget.resource_lease(ROOT,core.read_json(FRAMEWORK/'configs/study.json'),cpus=1,gpus=0,max_hours=4,job_id=os.environ['SLURM_JOB_ID'],resource_policy=policy), core.exclusive_file_lock(CAMPAIGN/'coordination-controller.mutex',timeout=60):
        with gzip.open(CAMPAIGN/'state.json.gz','rt') as f:state=json.load(f)
        state_sha=sha(CAMPAIGN/'state.json.gz');rows=candidates(helper,state)
        core.atomic_json(a.case/'audit.json',{'state_sha256':state_sha,'candidates':rows,'potential_files':sum(r['files'] for r in rows),'scientific_result':False})
        if a.mode=='audit':print(json.dumps({'status':'diagnostic_audit_complete','files':sum(r['files'] for r in rows),'workers':len(rows)}),flush=True);return
        canary=json.loads(Path(CANARY['path']).read_bytes());canary_job=canary['native_job_id'];ids=[r['job'] for r in rows]+[canary_job]
        budget.native_accounting(ROOT,ids,runner=run)
        native=repair._repair_native_states(ids,{},owner='pwa209',runner=run)
        helper._verify_canary(CANARY,ROOT)
        for row in rows:
            checkpoint();require(sha(CAMPAIGN/'state.json.gz')==state_sha,'Canonical state changed during recovery')
            quota=budget.require_storage_headroom(ROOT,required_files=row['files']+30,required_bytes=2*row['bytes']+100_000_000,reserve_files=5000,reserve_bytes=10_000_000_000,runner=run)
            if quota['personal_available_files']>=a.target_free_files:break
            job=row['job'];proof=slurm.completed_native_dependency(ROOT,job);require(proof is not None,'Actual complete worker cost proof absent')
            owner=selected_owner(job,native.get(job,{}),proof['native_records'][job])
            proof_path=a.case/(job+'-completion.json');owner_path=a.case/(job+'-owner.json');core.atomic_json(proof_path,proof);core.atomic_json(owner_path,owner)
            authority={'primary_source_sha256':PRIMARY,'full_config_sha256':CONFIG,'execution_source_sha256':SOURCE,'result_receipt':{'path':row['receipt'],'sha256':row['receipt_sha256']},'native':{'completion':{'path':str(proof_path),'sha256':sha(proof_path)},'owner':{'path':str(owner_path),'sha256':sha(owner_path)}}}
            result=execute_one(helper,row,authority,a.case/('worker-'+job),checkpoint=checkpoint);completed.append(result)
            core.atomic_json(a.case/'progress.json',{'status':'recovering_terminal_diagnostics','completed':completed,'state_sha256':state_sha,'scientific_result':False})
            print(json.dumps(result),flush=True)
        final=budget.require_storage_headroom(ROOT,required_files=0,runner=run)
        result={'status':'diagnostic_compaction_complete','completed':completed,'files_removed':sum(x['files_removed'] for x in completed),'free_files':final['personal_available_files'],'target_reached':final['personal_available_files']>=a.target_free_files,'state_sha256':state_sha,'scientific_result':False,'admission_authority':False}
        core.atomic_json(a.case/'result.json',result);print(json.dumps(result),flush=True)

if __name__=='__main__':main()
