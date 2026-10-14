"""Explicit stage dispatch; unavailable scientific evidence is never manufactured."""
from __future__ import annotations
import argparse
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from .core import atomic_json, canonical_hash, environment_record, read_json, source_identity, validate_config,digest,native_job_id


def _qualification_timeout():
    """Keep time for process termination, durable evidence and lease completion."""
    cleanup_margin=300.
    remaining=3600.
    if os.environ.get('HHW_MAX_HOURS'):
        hours=float(os.environ['HHW_MAX_HOURS'])
        if not math.isfinite(hours) or hours<=0:raise ValueError('Invalid qualification allocation duration')
        remaining=min(remaining,hours*3600)
    if os.environ.get('SLURM_JOB_END_TIME'):
        native_end=float(os.environ['SLURM_JOB_END_TIME'])
        if not math.isfinite(native_end):raise ValueError('Invalid qualification allocation end time')
        remaining=min(remaining,native_end-time.time())
    timeout=remaining-cleanup_margin
    if timeout<=0:raise RuntimeError('Insufficient allocation time for qualification and its cleanup margin')
    return timeout,cleanup_margin


def _qualification_workspace(root,job_id):
    if job_id:
        value=os.environ.get('SLURM_TMPDIR')
        if not value:raise RuntimeError('Native qualification requires SLURM_TMPDIR; no scratch-filesystem fixture fallback')
        base=Path(value).resolve()
        kind='SLURM_TMPDIR'
    else:
        base=(Path(root)/'tests').resolve()
        base.mkdir(parents=True,exist_ok=True)
        kind='local_unique_test_directory'
    if not base.is_dir():raise ValueError('Qualification workspace base must be an existing directory')
    workspace=Path(tempfile.mkdtemp(prefix='hhw-qualification-'+(job_id or 'local')+'-',dir=base)).resolve()
    if not workspace.is_relative_to(base) or workspace==base:raise ValueError('Qualification workspace escapes its designated base')
    return workspace,kind


def _stop_qualification(process):
    """Stop only the freshly launched pytest process and its owned group."""
    if process.poll() is not None:return
    if os.name=='posix':
        try:os.killpg(process.pid,signal.SIGTERM)
        except ProcessLookupError:return
    else:
        subprocess.run(['taskkill','/PID',str(process.pid),'/T','/F'],capture_output=True,timeout=10,
                       creationflags=subprocess.CREATE_NO_WINDOW)
    try:process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if os.name=='posix':
            try:os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError:pass
        else:process.kill()
        process.wait(timeout=5)


def _copy_qualification_file(source,destination):
    """One durable copy per finished artifact; never fsync every test message."""
    destination=Path(destination);temporary=destination.with_name(destination.name+'.partial')
    with Path(source).open('rb') as reader,temporary.open('xb') as writer:
        shutil.copyfileobj(reader,writer,length=1024*1024)
        writer.flush();os.fsync(writer.fileno())
    temporary.replace(destination)
    return {'path':str(destination.resolve()),'sha256':digest(destination),'bytes':destination.stat().st_size}


def _qualification_junit(path):
    tree=ET.parse(path)
    cases=tree.findall('.//testcase')
    suites=tree.findall('.//testsuite') if tree.getroot().tag!='testsuite' else [tree.getroot()]
    counts={key:sum(int(suite.get(key,'0')) for suite in suites) for key in ('tests','failures','errors','skipped')}
    if not cases or counts['tests']!=len(cases):raise ValueError('Qualification JUnit lacks a complete nonempty testcase roster')
    if sum(case.find('skipped') is not None for case in cases)!=counts['skipped']:
        raise ValueError('Qualification JUnit skip counts disagree with actual cases')
    return counts


def _preserve_qualification_failure(workspace,evidence,deadline):
    """Keep bounded fixture evidence as well as the complete log/JUnit."""
    archive=workspace/'failed-workspace.tar'
    total=0;files=0;limit=1_000_000_000;omitted=[]
    try:
        with tarfile.open(archive,'x') as output:
            for path in workspace.rglob('*'):
                if path==archive:continue
                if time.monotonic()>=deadline:raise TimeoutError('Failure-fixture preservation reached cleanup deadline')
                if path.is_symlink():
                    omitted.append({'path':path.relative_to(workspace).as_posix(),'target':os.readlink(path),
                                    'reason':'symlink targets are never followed or copied'})
                    continue
                if not path.is_file():continue
                size=path.stat().st_size
                if total+size>limit:raise ValueError('Failure-fixture preservation reached its one-GB bound')
                output.add(path,arcname=path.relative_to(workspace).as_posix(),recursive=False)
                total+=size;files+=1
        artifact=_copy_qualification_file(archive,evidence/'failed-workspace.tar')
        return {'status':'preserved','files':files,'uncompressed_bytes':total,'omitted_symlinks':omitted,**artifact}
    except (OSError,ValueError,TimeoutError) as error:
        return {'status':'bounded_incomplete','files':files,'uncompressed_bytes':total,'reason':str(error),
                'local_workspace_path':str(workspace),'omitted_symlinks':omitted,
                'complete_log_and_junit_preserved_separately':True}


def _cpu_qualification(config,root,source):
    root=Path(root).resolve();source=Path(source).resolve();job_id=native_job_id()
    allocation_clock=time.monotonic();timeout,cleanup_margin=_qualification_timeout()
    workspace,kind=_qualification_workspace(root,job_id)
    attempt=uuid.uuid4().hex
    evidence=root/'state/qualification-evidence'/((job_id or 'local')+'-'+attempt)
    evidence.mkdir(parents=True,exist_ok=False)
    state=root/'state/cpu-qualification.json'
    if state.exists():_copy_qualification_file(state,evidence/'previous-cpu-qualification.json')
    identity=source_identity(source);config_sha=canonical_hash(config)
    local_log=workspace/'pytest.log';local_junit=workspace/'junit.xml'
    command=[sys.executable,'-u','-m','pytest',str(source/'tests'),'-v','--color=no','--basetemp',str(workspace/'pytest'),
             '--junitxml',str(local_junit),'-o','cache_dir='+str(workspace/'cache'),'-o','faulthandler_timeout=120']
    environment=os.environ.copy()
    # Qualification always executes the complete immutable suite, regardless of
    # an interactive shell's optional pytest selectors or unrelated plugins.
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):environment.pop(key,None)
    environment.update(PYTHONUNBUFFERED='1',PYTHONDONTWRITEBYTECODE='1',PYTEST_DISABLE_PLUGIN_AUTOLOAD='1')
    started=time.monotonic();deadline=allocation_clock+timeout
    record={'schema_version':2,'status':'running','qualification_attempt':attempt,'slurm_job_id':job_id,
            'source_sha256':identity['sha256'],'config_sha256':config_sha,'started_unix':time.time(),
            'timeout_seconds':timeout,'cleanup_margin_seconds':cleanup_margin,'command':command,
            'workspace':str(workspace),'workspace_kind':kind,'local_live_log':str(local_log),
            'live_progress_destination':'inherited stdout; Slurm writes its existing batch log',
            'evidence_directory':str(evidence),
            'test_selection':'complete source/tests directory; no environment selectors or third-party plugin autoload',
            'evidence_class':'software qualification; not biological evidence'}
    atomic_json(evidence/'receipt.json',record);atomic_json(state,record)
    process=None;error=None;returncode=None;timed_out=False
    try:
        record['environment']=environment_record()
        with local_log.open('xb',buffering=0) as log:
            process=subprocess.Popen(command,cwd=source,env=environment,stdout=log,stderr=subprocess.STDOUT,
                start_new_session=os.name=='posix',creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
            with local_log.open('rb') as progress:
                def relay():
                    value=progress.read()
                    if value:sys.stdout.write(value.decode('utf-8',errors='replace'));sys.stdout.flush()
                while True:
                    remaining=deadline-time.monotonic()
                    if remaining<=0:
                        timed_out=True;_stop_qualification(process);break
                    try:returncode=process.wait(timeout=min(1.,remaining));break
                    except subprocess.TimeoutExpired:relay()
                relay()
            returncode=process.returncode
        counts=_qualification_junit(local_junit) if local_junit.is_file() else None
        record['test_counts']=counts
        if counts is None and not timed_out:raise ValueError('Qualification process did not produce a complete JUnit receipt')
        if returncode==0 and counts and any(counts[key] for key in ('failures','errors','skipped')):
            raise ValueError('Qualification cannot succeed with failed, errored or skipped tests')
        if source_identity(source)!=identity or canonical_hash(config)!=config_sha:
            raise ValueError('Qualification source/configuration changed during the test process')
    except BaseException as exc:
        error=exc
        if process is not None:_stop_qualification(process)
        returncode=process.returncode if process is not None else None
        record['error']={'type':type(exc).__name__,'message':str(exc)}
    record.update(status='timed_out' if timed_out else 'complete' if returncode==0 and error is None else 'failed',
                  returncode=returncode,elapsed_seconds=time.monotonic()-started,finished_unix=time.time())
    artifacts={}
    for label,path in [('log',local_log),('junit',local_junit)]:
        if path.is_file():artifacts[label]=_copy_qualification_file(path,evidence/path.name)
    record['artifacts']=artifacts
    record['source_verified_unchanged']=source_identity(source)==identity
    if not record['source_verified_unchanged']:
        record['status']='failed'
        if error is None:
            error=ValueError('Qualification source changed before evidence finalization')
            record['error']={'type':type(error).__name__,'message':str(error)}
    if record['status']!='complete':
        record['failed_fixture_evidence']=_preserve_qualification_failure(workspace,evidence,deadline+cleanup_margin-120.)
    record['workspace_deleted']=False
    record['receipt_sha256']=canonical_hash(record)
    atomic_json(evidence/'receipt.json',record);atomic_json(state,record)
    if error is not None:raise error
    if record['status']!='complete':raise RuntimeError('Native implementation qualification '+record['status']+'; inspect '+str(evidence/'receipt.json'))
    return record


def qualification(config,root,gpu=False):
    source=Path(__file__).resolve().parents[2]
    root=Path(root)
    root.mkdir(parents=True,exist_ok=True)
    if gpu:
        import torch
        torch.set_num_threads(int(os.environ.get('SLURM_CPUS_PER_TASK','2')))
        if not torch.cuda.is_available():raise RuntimeError('CUDA qualification requested but CUDA unavailable')
        torch.manual_seed(123)
        torch.cuda.reset_peak_memory_stats()
        import copy
        from .agents import make_policy
        comparisons={}
        for architecture in ['rnn','gru','modular','feedforward']:
            cpu=make_policy(architecture,100000,config['agents'].get('observation_window',32)).eval()
            cuda=copy.deepcopy(cpu).cuda().eval()
            a,b=cpu.initial_state(8),cuda.initial_state(8)
            maximum=0.
            with torch.no_grad():
                for _ in range(32):
                    observation=torch.randn(8,32)
                    x,_,a=cpu(observation,a)
                    y,_,b=cuda(observation.cuda(),b)
                    maximum=max(maximum,float((torch.softmax(x,-1)-torch.softmax(y.cpu(),-1)).abs().max()))
            if maximum>3e-5:raise ValueError('CPU/CUDA action-probability regression exceeds declared tolerance')
            comparisons[architecture]={'max_action_probability_difference':maximum,'tolerance':3e-5,'parameters':cpu.parameter_count}
        model=torch.nn.GRU(16,128,batch_first=True).cuda()
        optimizer=torch.optim.Adam(model.parameters(),lr=.001)
        values=torch.randn(16,32,16,device='cuda')
        started=time.monotonic()
        for _ in range(20):
            optimizer.zero_grad(set_to_none=True)
            y,_=model(values)
            loss=y.square().mean()
            loss.backward()
            optimizer.step()
        torch.cuda.synchronize()
        record={'status':'complete','evidence_class':'hardware qualification; not a scientific agent result',
                'device':torch.cuda.get_device_name(),'torch':torch.__version__,'cuda':torch.version.cuda,
                'peak_allocated_bytes':torch.cuda.max_memory_allocated(), 'iterations':20,
                'elapsed_seconds':time.monotonic()-started,'loss':float(loss.detach().cpu()),'cpu_cuda_regression':comparisons}
        atomic_json(root/'state/cuda-qualification.json',record)
    else:
        record=_cpu_qualification(config,root,source)
    print(json.dumps(record))
    return record


def source_audit(config,root):
    from . import acquire
    root=Path(root)
    manifest=read_json(root/'manifests/source_manifest.json')
    acquire.validate_manifest(manifest)
    extracted=[]
    for item in manifest['assets']:
        if item['source_id']!='fly':continue
        path=acquire.safe_destination(root,item['relative_path'])
        if item.get('eligible') is False:continue
        if not path.exists():raise FileNotFoundError(f'Mandatory source missing: {path}')
        if path.suffix.lower()=='.zip':
            destination=root/'data/extracted'/item['source_id']
            # Reuse only byte/CRC-validated members of the same checksum-pinned
            # original ZIP. This preserves the earlier complete extraction.
            import zipfile,zlib
            members=[];cached=True
            with zipfile.ZipFile(path) as archive:
                for member in archive.infolist():
                    if member.is_dir():continue
                    target=acquire.safe_destination(destination,member.filename)
                    if not target.is_file() or target.stat().st_size!=member.file_size:
                        cached=False;break
                    crc=0
                    with target.open('rb') as handle:
                        for chunk in iter(lambda:handle.read(8*1024**2),b''):crc=zlib.crc32(chunk,crc)
                    if crc & 0xffffffff != member.CRC:
                        raise ValueError('Previously extracted source member changed; preserve it for reviewed recovery')
                    members.append(str(target))
            extracted.extend(members if cached else acquire.safe_extract(path,destination,max_unpacked_bytes=400_000_000_000))
    inventory=acquire.inventory(root/'data/extracted',root/'manifests/source_inventory.json')
    from . import behaviour
    if hasattr(behaviour,'discover_fly_sources'):
        discovered=behaviour.discover_fly_sources(root/'data/extracted/fly',root,config['behaviour'])
        atomic_json(root/'manifests/fly_adapters.json',discovered)
        if discovered.get('fly_sources'):
            import copy
            active=copy.deepcopy(config)
            active['behaviour']['fly_sources']=discovered['fly_sources']
            behaviour.run(active,root,'ingest')
    formal=behaviour.formal_behavior_readiness(config,root)
    report={'status':'source_inventory_complete','source_manifest_hash':manifest['manifest_hash'],
            'extracted_files':len(extracted),'inventoried_files':len(inventory['files']),
            'ingestion_ready':(root/'data/fly_observations.parquet').exists(),
            'scientific_fit_ready':formal['ready'],'formal_exposure_readiness':formal,
            'source_sha256':source_identity(Path(__file__).resolve().parents[2])['sha256'],
            'config_sha256':canonical_hash(config),'evidence_class':'source audit'}
    atomic_json(root/'state/source-audit.json',report)
    from .interventions import intervention_readiness
    report['interventions']=intervention_readiness(config,root)
    atomic_json(root/'state/source-audit.json',report)
    from collections import Counter
    file_counts=Counter(x['source_id'] for x in manifest['assets'] if x.get('eligible') is not False)
    names=sorted(file_counts)
    byte_counts={name:sum(x.get('bytes') or 0 for x in manifest['assets'] if x['source_id']==name and x.get('eligible') is not False)/1e9 for name in names}
    paths=[root/'manifests/source_manifest.json',root/'manifests/source_inventory.json',root/'state/source-audit.json']
    crosswalk=manifest['sources'].get('ant_rna',{}).get('crosswalk',[])
    primary={x['biological_id'] for x in crosswalk if str(x.get('included')).lower() in ['true','1']}
    panels=[{'kind':'scatter','title':'Frozen source inventory','x':names,'y':[file_counts[n] for n in names],'xlabel':'Public source','ylabel':'Eligible deposited files'},
            {'kind':'scatter','title':'Deposited compressed byte counts','x':names,'y':[byte_counts[n] for n in names],'xlabel':'Public source','ylabel':'GB (decimal)'}]
    if primary:
        cohorts={k:{x['biological_id'] for x in crosswalk if x.get('cohort')==k and x['biological_id'] in primary} for k in ['discovery','challenge']}
        panels.append({'kind':'scatter','title':'Independent 2021 ant brains','x':['discovery','refusal challenge'],'y':[len(cohorts[k]) for k in ['discovery','challenge']],'xlabel':'Preassigned cohort','ylabel':'Unique brains'})
    atomic_json(root/'results/figure_inputs/01.json',{'evidence_class':'source_audit','config_sha256':canonical_hash(config),
        'source_sha256':source_identity(Path(__file__).resolve().parents[2])['sha256'],'source_result_paths':[str(p.resolve()) for p in paths],
        'source_result_hashes':{str(p.resolve()):digest(p) for p in paths},'panels':panels,
        'caption':'Frozen public metadata and verified fly source inventory. Deposited files and compressed bytes describe acquisition, not biological replication. Ant counts use unique, preassigned biological identities; refusal brains remain separate from score discovery. Other source groups are verified independently before their analyses.'})
    return report


def wait_transfer(config,root,group):
    path=Path(root)/'state'/('transfer-'+group+'.json')
    deadline=time.monotonic()+143*3600
    while time.monotonic()<deadline:
        if path.exists():
            record=read_json(path)
            if record['status']=='complete':
                # Operational runtime-path amendments do not change deposited
                # raw bytes. Consume an older transfer only against the exact
                # current frozen manifest, then independently verify its files.
                manifest=Path(root)/'manifests'/('tool_runtime_manifest.json' if group=='tools' else 'source_manifest.json')
                if record.get('source_manifest_sha256')!=canonical_hash(read_json(manifest)):
                    raise ValueError('Transfer belongs to a different frozen source manifest')
                return record
            if record['status']=='failed':raise RuntimeError('Source transfer failed; inspect preserved receipt')
        time.sleep(30)
    raise TimeoutError('Finite transfer wait expired; preserve ledger and transfer checkpoint')


def verify_sources(config,root,group):
    from . import acquire
    root=Path(root)
    manifest=read_json(root/'manifests'/('tool_runtime_manifest.json' if group=='tools' else 'source_manifest.json'))
    acquire.validate_manifest(manifest)
    records=[]
    groups={'fly':['fly'],'rna':['ant_rna','ant_reference','mantis_rna','fly_reference'],'rna-pilot':['ant_rna','ant_reference','mantis_rna','fly_reference'],'mantis':['polarization','mating'],'tools':['tool_runtime']}
    pilot_ids=set(acquire.rna_pilot_asset_ids(manifest)) if group=='rna-pilot' else None
    for item in manifest['assets']:
        if item['source_id'] not in groups[group]:continue
        if pilot_ids is not None and item['asset_id'] not in pilot_ids:continue
        if item.get('eligible') is False:continue
        records.append(acquire.verify_file(acquire.safe_destination(root,item['relative_path']),item))
    record={'status':'complete','verified_files':records,'manifest_sha256':canonical_hash(manifest),'evidence_class':'native source integrity verification'}
    atomic_json(root/'state'/('source-verification-'+group+'.json'),record)
    return record


def assess_pilot(config,root,advance=False):
    from .admission import assess_admission
    from .budget import native_accounting,storage_snapshot
    from .slurm import plan,submit
    root=Path(root)
    identifiers=[]
    for path in (root/'state').glob('*submission*.json'):
        record=read_json(path)
        if record.get('job_id'):identifiers.append(record['job_id'])
        identifiers.extend(v['job_id'] for v in record.get('tasks',{}).values() if v.get('state')=='submitted')
    if not os.environ.get('SLURM_JOB_ID'):raise RuntimeError('Measured assessment must execute under Slurm')
    native_accounting(root,identifiers)
    storage_snapshot(root)
    result=assess_admission(config,root)
    if advance:
        release=Path(__file__).resolve().parents[2]
        identity=read_json(release/'release.json')
        for phase,record in result.items():
            if record['status']!='admitted' or phase not in ['rna-full','agents-development','behaviour']:continue
            output=root/'plans'/(identity['release_id']+'-'+phase)
            ledger=root/'state'/(identity['release_id']+'-'+phase+'-submission.json')
            if ledger.exists():continue
            if output.exists():raise RuntimeError('Existing plan without submission ledger; inspect before advancing')
            plan(config,release,output,phase)
            submit(output/'plan.json',ledger,environment_job=os.environ['SLURM_JOB_ID'])
    return result


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',type=Path,default=Path('configs/study.json'))
    p.add_argument('--root',type=Path)
    sub=p.add_subparsers(dest='command',required=True)
    sub.add_parser('validate-config')
    sub.add_parser('environment')
    verify=sub.add_parser('verify-source');verify.add_argument('--identity',type=Path,required=True)
    q=sub.add_parser('qualify');q.add_argument('--gpu',action='store_true')
    sub.add_parser('source-audit')
    for command in ['wait-transfer','verify-sources']:
        s=sub.add_parser(command);s.add_argument('--group',required=True,choices=['fly','rna','rna-pilot','mantis','tools'])
    rna=sub.add_parser('rna-pilot');rna.add_argument('--prepare-only',action='store_true');rna.add_argument('--index',type=int)
    assessment=sub.add_parser('assess-pilot');assessment.add_argument('--advance',action='store_true')
    sub.add_parser('r-qualify')
    m=sub.add_parser('molecular');m.add_argument('--stage',required=True,choices=['runtime','prepare-rna-full','rna-run','ant','mantis']);m.add_argument('--task-index',type=int)
    sub.add_parser('agent-analysis')
    pipeline=sub.add_parser('molecular-pipeline');pipeline.add_argument('--stage',required=True,choices=['prepare','orthology','freeze','predict','mantis-orfs','mantis-orthology','benchmark-primary','benchmark-transfer'])
    accounting=sub.add_parser('accounting');accounting.add_argument('--job-ids',nargs='+',required=True)
    for name in ['behaviour','interventions']:
        s=sub.add_parser(name);s.add_argument('--stage',required=True)
    agents=sub.add_parser('agents');agents.add_argument('--mode',required=True);agents.add_argument('--task-index',type=int)
    planner=sub.add_parser('slurm-plan');planner.add_argument('--release',type=Path,required=True);planner.add_argument('--output',type=Path,required=True);planner.add_argument('--phase',default='initial')
    submit=sub.add_parser('submit');submit.add_argument('--plan',type=Path,required=True);submit.add_argument('--ledger',type=Path,required=True);submit.add_argument('--environment-job')
    sub.add_parser('report')
    sub.add_parser('archive')
    controller=sub.add_parser('pipeline');controller.add_argument('--segment',type=int,default=0);controller.add_argument('--max-seconds',type=float);controller.add_argument('--interval',type=int,default=60);controller.add_argument('--primary-release',type=Path)
    a=p.parse_args(argv)
    config=read_json(a.config)
    validate_config(config)
    root=a.root or Path(config['paths']['root'])
    numerical={'qualify','source-audit','assess-pilot','behaviour','interventions','agents','report','archive','wait-transfer','verify-sources','rna-pilot','r-qualify','molecular','agent-analysis','molecular-pipeline','pipeline'}
    if os.environ.get('SLURM_JOB_ID') and a.command in numerical:
        from .budget import resource_lease
        job_id=native_job_id()
        with resource_lease(root,config,cpus=int(os.environ.get('SLURM_CPUS_PER_TASK','1')),
                            gpus=int(os.environ.get('HHW_GPU_COUNT','0')),
                            max_hours=float(os.environ.get('HHW_MAX_HOURS','1')),job_id=job_id):
            return execute(a,config,root)
    return execute(a,config,root)


def execute(a,config,root):
    if a.command=='validate-config':result={'valid':True,'config_sha256':canonical_hash(config)}
    elif a.command=='environment':result=environment_record()
    elif a.command=='verify-source':
        identity=read_json(a.identity)
        observed=source_identity(a.identity.resolve().parent)
        if identity['source_identity']!=observed:raise ValueError('Immutable release changed; execution rejected')
        result={'source_verified':True,'sha256':observed['sha256']}
    elif a.command=='qualify':result=qualification(config,root,a.gpu)
    elif a.command=='source-audit':result=source_audit(config,root)
    elif a.command=='wait-transfer':result=wait_transfer(config,root,a.group)
    elif a.command=='verify-sources':result=verify_sources(config,root,a.group)
    elif a.command=='rna-pilot':
        from .molecular import run_rna_pilot
        result=run_rna_pilot(config,root,index=a.index,prepare_only=a.prepare_only)
    elif a.command=='assess-pilot':result=assess_pilot(config,root,a.advance)
    elif a.command=='r-qualify':
        result=subprocess.run(['Rscript','-e','library(DESeq2); library(jsonlite); cat(as.character(packageVersion("DESeq2")), "\\n"); sessionInfo()'],text=True,capture_output=True)
        record={'status':'complete' if result.returncode==0 else 'failed','stdout':result.stdout,'stderr':result.stderr,'job_id':os.environ.get('SLURM_JOB_ID')}
        atomic_json(Path(root)/'state/r-qualification.json',record)
        if result.returncode:raise RuntimeError('Native R/DESeq2 qualification failed')
        import runpy
        record['status']='modules_qualified; production fixtures pending'
        atomic_json(Path(root)/'state/r-qualification.json',record)
        try:fixture=runpy.run_path(str(Path(__file__).resolve().parents[2]/'scripts/qualify_r.py'))['run'](root)
        except BaseException as error:
            record.update(status='failed',fixture_error=str(error))
            atomic_json(Path(root)/'state/r-qualification.json',record)
            raise
        record['status']='complete'
        record['production_count_model_fixtures']=fixture
        atomic_json(Path(root)/'state/r-qualification.json',record)
        result=record
    elif a.command=='molecular':
        from . import molecular
        if a.stage=='runtime':
            from .molecular_pipeline import runtime_directory
            result=molecular.install_tool_runtime(root,runtime_directory(config,root),qualify=True,
                orthofinder_python=config.get('molecular',{}).get('orthofinder_python'))
        elif a.stage=='prepare-rna-full':result=molecular.prepare_full_rna(config,root)
        elif a.stage=='rna-run':
            if a.task_index is None:raise ValueError('Fixed biological task index required')
            result=molecular.run_rna_task(molecular.rna_directories(config,root)['full_plan']/'rna_plan.json',a.task_index)
            result.update(config_sha256=canonical_hash(config),full_config_sha256=canonical_hash(config))
            atomic_json(Path(root)/'state'/f'rna-full-{a.task_index}.json',result)
        elif a.stage=='ant':result=molecular.run_ant(root,config)
        else:result=molecular.run_mantis(root)
    elif a.command=='agent-analysis':
        from .agent_analysis import run
        result=run(config,root)
    elif a.command=='molecular-pipeline':
        from .molecular_pipeline import run
        result=run(config,root,a.stage)
    elif a.command=='accounting':
        from .budget import native_accounting
        result=native_accounting(root,a.job_ids)
    elif a.command in ['behaviour','interventions']:
        import importlib
        module=importlib.import_module('horsehair.'+a.command)
        result=module.run(config,root,a.stage)
    elif a.command=='agents':
        from .agents import run
        result=run(config,root,mode=a.mode,task_index=a.task_index)
    elif a.command=='pipeline':
        from .pipeline import run
        result=run(config,root,segment=a.segment,max_seconds=a.max_seconds,interval=a.interval,primary_release=a.primary_release)
    elif a.command=='slurm-plan':
        from .slurm import plan
        result=plan(config,a.release,a.output,a.phase)
    elif a.command=='submit':
        from .slurm import submit
        result=submit(a.plan,a.ledger,a.environment_job)
    elif a.command=='archive':
        from .archive import run
        result=run(config,root)
    else:
        from .report import build_report
        result=build_report(config,root)
    if result is not None:print(json.dumps(result,default=str))
    return 0


if __name__=='__main__':raise SystemExit(main())

