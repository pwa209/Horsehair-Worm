"""Finite source-pinned metadata-only audits; no archive or scientific fit."""
from pathlib import Path
import argparse,hashlib,importlib.util,json,os,re,shutil,subprocess,sys,time,xml.etree.ElementTree as ET

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONTEXT=ROOT/'maintenance/study-af-vtl-structure-stage-v2-20261021/run-vtl-track-schema-audit-v2-20261021.py'
CONTEXT_SHA='284b082a04eccebe707378a6e8f26836a4467846e3b213c3e9ff9b4325a86ff1'
FROZEN_SUITES = None
FLAGS={'scientific_result':False,'scientific_fit':False,'admission_authority':False,'original_files_removed':False,'storage_credit_authorized':False}

def require(value,message):
    if not value:raise ValueError(message)

def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(4*1024**2),b''):h.update(block)
    return h.hexdigest()

def main():
    require(FROZEN_SUITES is not None,'Unfrozen metadata gate cannot execute')
    p=argparse.ArgumentParser();p.add_argument('--mode',choices=['stock','census'],required=True);p.add_argument('--native-script',type=Path,required=True);a=p.parse_args()
    binding=FROZEN_SUITES[a.mode];stage=Path(__file__).resolve().parent
    require(stage==ROOT/('maintenance/study-af-'+a.mode+'-metadata-stage-v1-20261021'),'Exact fresh metadata staging namespace required')
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    require(os.geteuid()==3164768 and os.environ.get('USER')=='pwa209' and os.environ.get('SLURM_CPUS_PER_TASK')=='1'
        and os.environ.get('HHW_GPU_COUNT')=='0' and os.environ.get('HHW_MAX_HOURS')=='1','Owned finite1CPU metadata allocation required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval required')
    import horsehair
    from horsehair.core import source_identity,read_json,canonical_hash,native_job_id
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,'Exact qualified metadata execution context required')
    own_sha=digest(Path(__file__));require(digest(CONTEXT)==CONTEXT_SHA,'Reviewed operational context changed')
    spec=importlib.util.spec_from_file_location('hhw_metadata_operational_context',CONTEXT);ctx=importlib.util.module_from_spec(spec);spec.loader.exec_module(ctx)
    ctx.CASE=ROOT/('maintenance/study-af-'+a.mode+'-metadata-audit-v1-20261021')
    ctx.SUPPORT=ROOT/('maintenance/study-af-'+a.mode+'-metadata-support-v1-20261021')
    require(not ctx.CASE.exists() and not ctx.SUPPORT.exists(),'Fresh metadata attempt required; no replay')
    config=read_json(PRIMARY/'configs/study.json');require(canonical_hash(config)==ctx.CONFIG and read_json(FRAMEWORK/'configs/study.json')==config,'Frozen full config required')
    helper=stage/binding['helper'];tests=stage/binding['tests']
    for path,sha in [(helper,binding['helper_sha256']),(tests,binding['tests_sha256']),(ctx.GUARD,ctx.GUARD_SHA),(ctx.SCHEDULER,ctx.SCHEDULER_SHA),(ctx.QUOTA,ctx.QUOTA_SHA),(ctx.QUALIFICATION,ctx.QUALIFICATION_SHA)]:ctx.ordinary_source(path,sha)
    policy=ctx.load_resource_policy(ctx.POLICY,ctx.POLICY_SHA,root=ROOT,config=config);ctx.SUPPORT.mkdir(mode=0o700)
    import runpy
    runpy.run_path(str(ctx.GUARD))['install'](ctx.SUPPORT/'accounting-reads')
    job=str(native_job_id() or '');require(re.fullmatch('[1-9][0-9]*',job),'Scalar native metadata job required')
    allocation=ctx.native_allocation(job,a.native_script,runpy.run_path(str(ctx.SCHEDULER))['run_scontrol_job']);end=allocation['allocation_end_unix']
    registry,installation=ctx.qualified_registry_read_guard(ctx.REGISTRY_Q,ctx.REGISTRY_Q_SHA,ctx.REGISTRY_Q_JOB,end)
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'Accounting guard must precede campaign binding')
    campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],ctx.SUPPORT,'metadata-baseline',resource_policy=policy,cpus=0,gpus=0,hours=1)
    qjob=str(read_json(ctx.QUALIFICATION)['slurm_job_id']);observed=repair._repair_native_states(sorted({qjob,registry['job_id']}),{},owner='pwa209',account='def-ptewarie_cpu')
    public_q=ctx.qualification_proof(ctx.QUALIFICATION,ctx.QUALIFICATION_SHA,qjob,observed);private_q=ctx.completed_registry_qualification(registry,observed)
    with ctx.resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        quota=runpy.run_path(str(ctx.QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,required_bytes=10_000_000_000,reserve_files=248000,reserve_bytes=2_000_000_000_000,evidence_dir=ctx.SUPPORT/'quota-reads',allocation_deadline_unix=end)
        try:storage=ctx.storage_proof(config,quota)
        except ctx.PendingStorage as error:
            ctx.exclusive(ctx.SUPPORT/'receipt.json',{'status':'pending_storage_no_tests_or_metadata_audit_started','reason':str(error),'native_job_id':job,**FLAGS});return 0
        temp=Path(os.environ['SLURM_TMPDIR']);require(temp.is_absolute() and temp.is_dir() and temp==temp.resolve() and temp.stat().st_uid==3164768 and shutil.disk_usage(temp).free>=3_000_000_000,'Owned allocation-local fixture capacity required')
        workspace=temp/('hhw-metadata-'+a.mode+'-'+job);workspace.mkdir(mode=0o700,exist_ok=False)
        env={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1','TZ':'UTC'}
        for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):env.pop(key,None)
        xml=ctx.SUPPORT/'private-tests.xml';command=[sys.executable,'-s','-m','pytest',str(tests),'-q','-p','no:cacheprovider','--basetemp',str(workspace/'fixtures'),'--junitxml',str(xml)]
        test_seconds=min(300,end-time.time()-binding['worker_seconds']-300)
        if test_seconds<60:
            ctx.exclusive(ctx.SUPPORT/'receipt.json',{'status':'pending_native_margin_no_tests_or_metadata_audit_started','native_job_id':job,**FLAGS});return 0
        ctx.exclusive(ctx.SUPPORT/'tests-intent.json',{'command':command,'frozen_suite':binding,'no_actual_audit_input_read_yet':True,**FLAGS})
        with (ctx.SUPPORT/'tests-stdout.txt').open('xb') as out,(ctx.SUPPORT/'tests-stderr.txt').open('xb') as err:
            result=subprocess.run(command,cwd=FRAMEWORK,env=env,stdout=out,stderr=err,timeout=test_seconds,check=False)
        require(type(result.returncode) is int and result.returncode==0,'Actual complete metadata private tests failed')
        raw=xml.read_bytes();require(len(raw)<=2_000_000 and b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Bounded plain private JUnit required')
        cases=list(ET.fromstring(raw).iter('testcase'));rows=sorted((x.get('classname','').split('.')[-1],x.get('name','')) for x in cases)
        require(len(cases)==len(set(rows))==binding['count'] and canonical_hash(rows)==binding['roster_sha256']
            and all(not any(x.find(k) is not None for k in ('failure','error','skipped')) for x in cases),'Actual complete native metadata test roster required')
        from horsehair.cli import _qualification_junit
        require(_qualification_junit(xml)=={'tests':binding['count'],'failures':0,'errors':0,'skipped':0},'Actual complete private JUnit suite summary required')
        ctx.exclusive(ctx.SUPPORT/'private-qualification.json',{'test_counts':{'tests':len(cases),'failures':0,'errors':0,'skipped':0},'test_source':{'path':str(tests),'sha256':binding['tests_sha256']},'helper':{'path':str(helper),'sha256':binding['helper_sha256']},'junit':{'path':str(xml),'sha256':digest(xml)},**FLAGS})
        if end-time.time()<=binding['worker_seconds']+300:
            ctx.exclusive(ctx.SUPPORT/'receipt.json',{'status':'pending_native_margin_tests_complete_no_metadata_audit_started','native_job_id':job,'actual_private_test_count':binding['count'],**FLAGS});return 0
        require(digest(helper)==binding['helper_sha256'] and digest(tests)==binding['tests_sha256'] and digest(CONTEXT)==CONTEXT_SHA
            and digest(Path(__file__))==own_sha and source_identity(FRAMEWORK)['sha256']==SOURCE,'Audit source changed before actual input read')
        policy=ctx.load_resource_policy(ctx.POLICY,ctx.POLICY_SHA,root=ROOT,config=config);ctx.CASE.mkdir(mode=0o700)
        output=ctx.CASE/'metadata-inventory.json';seconds=binding['worker_seconds'];command=[sys.executable,'-s','-u',str(helper),*binding['arguments'],'--output',str(output),'--max-seconds',str(seconds)]
        ctx.exclusive(ctx.SUPPORT/'input-bindings.json',{'source_sha256':SOURCE,'allocation':allocation,'public_qualification':public_q,'registry_qualification':private_q,'registry_installation':installation,'storage':storage,'resource_policy':policy.receipt(),'operational_context_sha256':CONTEXT_SHA,'wrapper_sha256':own_sha,'child_command':command,'maximum_child_seconds':seconds+180,'no_actual_audit_input_read_before_private_qualification':True,**FLAGS})
        with (ctx.CASE/'stdout.txt').open('xb') as out,(ctx.CASE/'stderr.txt').open('xb') as err:
            result=subprocess.run(command,cwd=FRAMEWORK,env=env,stdout=out,stderr=err,timeout=seconds+180,check=False)
        require(type(result.returncode) is int and result.returncode==0 and output.is_file() and 0<output.stat().st_size<=256*1024**2,'Complete bounded metadata child required')
        require(digest(helper)==binding['helper_sha256'] and digest(tests)==binding['tests_sha256'] and digest(Path(__file__))==own_sha and digest(CONTEXT)==CONTEXT_SHA and source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,'Metadata execution sources changed')
        policy=ctx.load_resource_policy(ctx.POLICY,ctx.POLICY_SHA,root=ROOT,config=config)
        receipt={'status':'complete_native_metadata_audit','mode':a.mode,'native_job_id':job,'allocation':allocation,'source_sha256':SOURCE,'resource_policy':policy.receipt(),'actual_private_test_count':binding['count'],'result':{'path':str(output),'sha256':digest(output),'bytes':output.stat().st_size},'private_qualification':{'path':str(ctx.SUPPORT/'private-qualification.json'),'sha256':digest(ctx.SUPPORT/'private-qualification.json')},**FLAGS}
        ctx.exclusive(ctx.SUPPORT/'receipt.json',receipt);print(json.dumps(receipt,sort_keys=True));return 0

if __name__=='__main__':raise SystemExit(main())
