"""Finite, source-pinned native wrapper for the qualified pure F2 dispatcher.

No scheduler mutation is retried. Ambiguous submission/release evidence stops
the controller and remains available for explicit reconciliation. Four finite
96-hour segments are registered by the external deployment, never by this file.
"""
from pathlib import Path
import argparse
import gzip
import hashlib
import json
import math
import os
import re
import runpy
import shlex
import subprocess
import time
from datetime import datetime,timezone

from horsehair import agent_campaign as dispatch, agent_execution_v2 as evaluation, agents, slurm
from horsehair.core import atomic_json, canonical_hash, digest, exclusive_file_lock, native_job_id, read_json, source_identity, ReceiptLockTimeout
from horsehair.budget import resource_lease, storage_snapshot
from horsehair.admission import _storage, AdmissionError
from horsehair.execution_resources import load_resource_policy, BASE_CONFIG_SHA256, PRIMARY_SCIENTIFIC_SOURCE_SHA256

ROOT=Path('/scratch/pwa209/horsehair-worm')
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
FRAMEWORK_SHA='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
AGENTS_SHA='fbc98bc5765b2d1b2fc9f46b48b104da2cfba7a211e696f7b23e23745e67ac14'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-scheduler-read.py'
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
QUOTA=ROOT/'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'
QUOTA_SHA='5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045'


def require(value,message):
    if not value:raise ValueError(message)


def contained(path,namespace,*,exists=True):
    original=Path(path).absolute();value=original.resolve();namespace=Path(namespace).resolve()
    require(value.is_relative_to(namespace) and value!=namespace and not original.is_symlink(),'Personal ordinary path required')
    for parent in original.parents:
        if parent.resolve()==namespace:break
        require(not parent.is_symlink(),'Redirected personal ancestor refused')
    if exists:require(value.is_file(),'Required ordinary file missing')
    return value


def frozen_file(path,sha,namespace=None):
    path=contained(path,namespace or ROOT/'maintenance')
    require(isinstance(sha,str) and re.fullmatch('[0-9a-f]{64}',sha) and digest(path)==sha,'Immutable file SHA mismatch')
    return read_json(path)


def fields(response):
    response.check_returncode();lines=[s for s in response.stdout.splitlines() if s.strip()]
    require(len(lines)==1,'Exactly one native allocation required')
    pairs=re.findall(r'(?<!\S)(\w+)=([^\s]*)',lines[0]);result=dict(pairs)
    require(len(result)==len(pairs),'Duplicate native scheduler fields refused')
    return result


def seconds(text):
    match=re.fullmatch(r'(?:(\d+)-)?(\d+):(\d{2}):(\d{2})',text)
    require(match is not None,'Finite native TimeLimit required')
    d,h,m,s=map(lambda v:int(v or 0),match.groups());return d*86400+h*3600+m*60+s


def gpu_count(tres):
    if not tres:return 0
    values={}
    for item in tres.split(','):
        require(item.count('=')==1,'Malformed native TRES field')
        key,value=item.split('=',1)
        require(key and key not in values,'Duplicate native TRES field')
        values[key]=value
    gpu={key:value for key,value in values.items() if key.startswith('gres/gpu')}
    require(all((key=='gres/gpu' or re.fullmatch(r'gres/gpu:[a-zA-Z0-9_-]+',key))
            and re.fullmatch(r'[0-9]+',value) for key,value in gpu.items()),'Malformed or negative native GPU count')
    typed=sum(int(value) for key,value in gpu.items() if key!='gres/gpu')
    if 'gres/gpu' in gpu:
        total=int(gpu['gres/gpu'])
        require(len(gpu)==1 or total==typed,'Conflicting generic/typed native GPU counts')
        return total
    return typed


def guarded_modules(area):
    """Install before campaign imports bind native parser function references."""
    require(digest(GUARD)==GUARD_SHA,'Pinned native guard changed')
    identity=runpy.run_path(str(GUARD))['install'](area/'accounting-reads')
    from horsehair import repair
    from horsehair import campaign as campaign_module
    marker=getattr(repair._repair_native_states,'__hhw_native_accounting_read_guard__',None)
    require(marker is not None and marker=={k:v for k,v in identity.items() if k!='status'}
            and campaign_module._repair_native_states is repair._repair_native_states,
            'Native accounting modules were imported before the verified guard')
    return campaign_module,repair._repair_native_states


def normalized_native(ident,record):
    require(record['user']=='pwa209' and record['account'] in {'def-ptewarie_cpu','def-ptewarie_gpu'},'Owned native allocation required')
    return {'job_id':str(ident),'state':record['state'],'exit_code':record['exit_code'],
        'cpus':record['cpus'],'gpus':gpu_count(record['alloc_tres']),
        'elapsed_seconds':record['elapsed_seconds'],'end':record.get('end','Unknown'),
        'user':record['user'],'account':record['account']}


def worker_binding(descriptor):
    """Controller repairs retain one immutable F2 worker descriptor identity."""
    binding=descriptor['worker_descriptor']
    require(set(binding)=={'path','sha256'},'Exact stable worker descriptor binding required')
    worker=frozen_file(binding['path'],binding['sha256'])
    common=('case','framework_release','execution_source_sha256','worker_sha256','qualification','resource_policy',
            'frozen_protocol_sha256','gpu_qualification','agent_campaign_case','canary_request','canary_plan')
    require(all(worker.get(key)==descriptor.get(key) for key in common)
            and ('worker_path' not in worker or worker['worker_path']==descriptor['worker_path']),
            'Stable worker descriptor changed scientific/operational execution bindings')
    return Path(binding['path']).resolve(),binding['sha256'],worker


def load_context(path,sha,segment):
    d=frozen_file(path,sha);framework=contained(d['framework_release'],ROOT/'releases',exists=False)
    config=read_json(PRIMARY/'configs/study.json');job=str(native_job_id() or '')
    require(type(segment) is int and 0<=segment<4 and job.isdigit() and os.geteuid()==3164768
            and os.environ.get('USER')=='pwa209' and os.environ.get('SLURM_CPUS_PER_TASK')=='1'
            and os.environ.get('HHW_GPU_COUNT')=='0' and os.environ.get('HHW_MAX_HOURS')=='96',
            'One owned finite96-hour CPU controller allocation required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval context required')
    import horsehair
    require(framework.is_dir() and Path(horsehair.__file__).resolve()==framework/'src/horsehair/__init__.py'
            and source_identity(framework)['sha256']==d['execution_source_sha256']==FRAMEWORK_SHA
            and source_identity(PRIMARY)['sha256']==PRIMARY_SCIENTIFIC_SOURCE_SHA256
            and canonical_hash(config)==BASE_CONFIG_SHA256 and read_json(framework/'configs/study.json')==config,
            'Qualified execution/frozen science/configuration required')
    require(digest(Path(__file__))==d['controller_sha256'] and digest(Path(agents.__file__))==AGENTS_SHA,
            'Pinned controller or original agent implementation changed')
    worker=contained(d['worker_path'],ROOT/'maintenance');require(digest(worker)==d['worker_sha256'],'Pinned worker changed')
    worker_descriptor_path,worker_descriptor_sha,worker_descriptor=worker_binding(d)
    case=contained(d['agent_campaign_case'],ROOT/'maintenance',exists=False)
    require(case==Path(d['case']).resolve()/'agent-final-f64-v1','Exact finite campaign case required')
    protocol_path=agents._directory(ROOT,config['agents'])/'frozen_protocol.json'
    protocol=frozen_file(protocol_path,d['frozen_protocol_sha256'],ROOT)
    policy=load_resource_policy(Path(d['resource_policy']['path']),d['resource_policy']['sha256'],root=ROOT,config=config)
    q=frozen_file(d['qualification']['path'],d['qualification']['sha256'])
    counts=q['test_counts']
    require(q['status']=='complete' and q.get('returncode')==0 and q.get('config_sha256')==BASE_CONFIG_SHA256
            and q['source_sha256']==FRAMEWORK_SHA and q['source_verified_unchanged'] is True
            and counts==d['qualification']['test_counts'] and counts['tests']==1694
            and ('job_id' not in d['qualification'] or str(q['slurm_job_id'])==str(d['qualification']['job_id']))
            and counts['failures']==counts['errors']==counts['skipped']==0,'Exact positive1694-test native qualification required')
    case.mkdir(parents=True,exist_ok=True)
    area=case/f'controller-{segment}-{job}';require(not area.exists(),'Controller attempt cannot overwrite evidence');area.mkdir()
    for p,h in [(GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(QUOTA,QUOTA_SHA)]:require(digest(p)==h,'Pinned native reader changed')
    campaign_module,native_reader=guarded_modules(area)
    scheduler=runpy.run_path(str(SCHEDULER))['run_scontrol_job']
    allocation=fields(scheduler(job,area/'scheduler-reads'))
    require(allocation.get('JobId')==job and allocation.get('UserId')=='pwa209(3164768)'
            and allocation.get('JobState')=='RUNNING' and allocation.get('NumCPUs')=='1'
            and allocation.get('Account')==config['paths']['cpu_account'] and seconds(allocation.get('TimeLimit',''))==96*3600,
            'Actual native controller contract differs')
    require(all('gpu' not in allocation.get(k,'').lower() for k in ('TresPerNode','TresPerJob','TresPerTask','AllocTRES','ReqTRES','Gres')),
            'Controller must have no GPU')
    end=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    native_end=datetime.fromisoformat(allocation.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp()
    require(math.isfinite(end) and abs(end-native_end)<=2 and end>time.time()+180,'Native finalization margin required')
    context={'descriptor':d,'descriptor_path':Path(path).resolve(),'descriptor_sha256':sha,'framework':framework,'config':config,
        'job_id':job,'segment':segment,'case':case,'area':area,'policy':policy,'protocol':protocol,
        'protocol_path':protocol_path,'scheduler':scheduler,'allocation':allocation,'deadline':end-180,
        'primary_release_id':read_json(PRIMARY/'release.json')['release_id'],
        'campaign_module':campaign_module,'native_reader':native_reader,
        'worker_descriptor_path':worker_descriptor_path,'worker_descriptor_sha256':worker_descriptor_sha,
        'worker_descriptor':worker_descriptor}
    admission(context,'controller-start')
    require(slurm.completed_native_dependency(ROOT,str(q['slurm_job_id'])) is not None,'Native qualification has not completed0:0')
    binding=d['gpu_qualification'];gpu=frozen_file(binding['path'],binding['sha256'])
    regression=gpu.get('result',{});comparisons=regression.get('cpu_cuda_regression',{})
    require(gpu.get('status')=='complete_native_work' and gpu.get('mode')=='gpu-qualify'
            and gpu.get('native_job_id')==binding['job_id'] and gpu.get('descriptor_sha256')==binding['descriptor_sha256']
            and gpu.get('source_sha256')==FRAMEWORK_SHA and gpu.get('full_config_sha256')==BASE_CONFIG_SHA256
            and gpu.get('resource_policy')==policy.receipt() and slurm.completed_native_dependency(ROOT,binding['job_id']) is not None
            and regression.get('status')=='complete' and regression.get('iterations')==20
            and regression.get('device') and regression.get('cuda') and regression.get('torch')
            and math.isfinite(regression.get('elapsed_seconds',float('nan'))) and regression['elapsed_seconds']>0
            and math.isfinite(regression.get('loss',float('nan')))
            and set(comparisons)=={'rnn','gru','modular','feedforward'}
            and all(math.isfinite(v['max_action_probability_difference']) and 0<=v['max_action_probability_difference']<=3e-5
                    and v.get('tolerance')==3e-5 for v in comparisons.values()),'Current source-bound GPU worker qualification required')
    return context


def admission(c,label,**resources):
    return c['campaign_module'].combined_admission(ROOT,c['config'],c['primary_release_id'],c['area'],label,
        resource_policy=c['policy'],**resources)


def read_evidence(c,prefix):
    c['read_sequence']=c.get('read_sequence',0)+1
    path=c['area']/(prefix+'-'+str(c['read_sequence']).zfill(7))
    require(not path.exists(),'Reader evidence path cannot be reused')
    return path


def source_guard(c):
    d=frozen_file(c['descriptor_path'],c['descriptor_sha256'])
    require(d==c['descriptor'] and digest(Path(__file__))==d['controller_sha256']
            and digest(Path(d['worker_path']))==d['worker_sha256']
            and digest(c['protocol_path'])==d['frozen_protocol_sha256']
            and source_identity(c['framework'])['sha256']==FRAMEWORK_SHA
            and source_identity(PRIMARY)['sha256']==PRIMARY_SCIENTIFIC_SOURCE_SHA256,
            'Controller/worker/source/protocol changed before a new wave')
    frozen_file(d['qualification']['path'],d['qualification']['sha256'])
    frozen_file(d['gpu_qualification']['path'],d['gpu_qualification']['sha256'])
    path,sha,worker=worker_binding(d)
    require(path==c['worker_descriptor_path'] and sha==c['worker_descriptor_sha256'] and worker==c['worker_descriptor'],
            'Stable worker descriptor changed during controller execution')


class PendingStorage(ValueError):pass


def storage_reservation(state,extra_evaluations=(),census_start=None):
    """Whole-policy parts, pending native writers, and forward six-hour reads."""
    def counted(job):
        if census_start is None:return False
        proof=job.get('native_proof') or {}
        if proof.get('state') not in dispatch.TERMINAL_FAILURES|{'COMPLETED'}:return False
        try:
            end=datetime.fromisoformat(proof['end'].replace('Z','+00:00'))
            if end.tzinfo is None:end=end.replace(tzinfo=timezone.utc)
            return end.timestamp()<=census_start
        except (KeyError,TypeError,ValueError):return False
    pending={name for name,job in state['jobs'].items() if not counted(job)}
    training={v['intent']['cell_index'] for name,v in state['jobs'].items()
              if name in pending and v['intent']['kind']=='training'}
    evaluated={v['intent']['cell_index'] for v in state['jobs'].values() if v['intent']['kind']=='evaluation'}
    evaluated.update(v['index'] for v in state['cells'] if v['evaluation_plan'] is not None)
    evaluated.update(extra_evaluations)
    for cell in state['cells']:
        if cell['assembly_job'] is not None and counted(state['jobs'][cell['assembly_job']]):evaluated.discard(cell['index'])
    assembled={v['intent']['cell_index'] for name,v in state['jobs'].items()
               if name in pending and v['intent']['kind']=='assembly'}
    placeholders={index for index in evaluated if not any(v['intent']['kind']=='evaluation' and v['intent']['cell_index']==index
                  for v in state['jobs'].values())}
    maintenance=len(pending)+len(placeholders)
    # At most120 ticks before the next compulsory census.100files/tick covers
    # three17-file native reads, two12-file quota reads and status updates.
    #200/job separately covers all three combined-admission calls (6 reads),
    # two14-file scheduler reads,12-file quota and plan/ledger/lease receipts.
    return {'files':12000+30*len(training)+630*len(evaluated)+20*len(assembled)+200*maintenance,
        'bytes':5_000_000_000+2_000_000_000*len(training)+5_000_000_000*len(evaluated)
                +1_000_000_000*len(assembled)+100_000_000*maintenance,
        'training_policies':len(training),'evaluation_policies':len(evaluated),
        'pending_jobs':sorted(pending),'pending_training_indices':sorted(training),
        'open_evaluation_indices':sorted(evaluated),'pending_assembly_indices':sorted(assembled),
        'evaluation_plan_placeholders':sorted(placeholders),'controller_file_allowance':12000,
        'bound_basis':'30/training;600conditionparts+30plan/policy;20/assembly;200/pending native job;12000forward-six-hour reads; no compaction'}


def fresh_census(c,*,clock=time.time,sleeper=time.sleep,popen=subprocess.Popen):
    """Run slow original census in this allocation, with a finite child deadline."""
    path=ROOT/'state/storage-budget.json';now=clock()
    if path.is_file():
        record=read_json(path)
        measured=record.get('measured_unix');captured=datetime.fromisoformat(record.get('captured_utc','').replace('Z','+00:00'))
        if (type(measured) in (int,float) and math.isfinite(measured) and 0<=now-measured<6*3600
                and captured.tzinfo is not None and 0<=now-captured.timestamp()<6*3600):return record
    timeout=min(7200,c['deadline']-clock()-300)
    if timeout<4200:raise PendingStorage('Fresh slow census lacks a seventy-minute native margin; new work paused')
    command=['python','-s','-c','from horsehair.budget import storage_snapshot; storage_snapshot('+repr(str(ROOT))+')']
    with (c['area']/'storage-census.log').open('ab') as output:
        child=popen(command,stdout=output,stderr=subprocess.STDOUT,cwd=c['framework'],start_new_session=True)
        deadline=clock()+timeout
        while child.poll() is None:
            if clock()>=deadline:
                child.terminate()
                try:child.wait(timeout=10)
                except subprocess.TimeoutExpired:child.kill();child.wait(timeout=10)
                raise PendingStorage('Bounded original census did not finish; previous census cannot admit work')
            sleeper(min(30,deadline-clock()))
        if child.returncode:raise PendingStorage('Original census failed; no partial measurement admits new work')
    require(path.is_file(),'Census did not preserve a storage receipt')
    return read_json(path)


def storage_guard(c,state,*,extra_evaluations=()):
    record=fresh_census(c)
    census_start=datetime.fromisoformat(record['captured_utc'].replace('Z','+00:00')).timestamp()
    reservation=storage_reservation(state,extra_evaluations,census_start)
    entry=record.get('exact_entry_measurements')
    require(entry is not None,'Exact native study entry census required')
    entry_path=contained(entry['receipt_path'],ROOT/'state')
    with entry_path.open('rb') as stream:raw_detail=stream.read(512*1024*1024+1)
    if len(raw_detail)>512*1024*1024:raise PendingStorage('Census detail exceeds the bounded512MiB metadata read')
    if hashlib.sha256(raw_detail).hexdigest()!=entry['receipt_sha256']:
        raise PendingStorage('Mutable census detail/budget pair changed during a fresh read; no mixed pair admits work')
    detail=json.loads(raw_detail)
    require(detail.get('status')=='measured' and detail.get('count')==record['study_files']
            and detail.get('bytes')==record['study_bytes'] and len(detail['items'])==detail['count']
            and sum(item['bytes'] for item in detail['items'])==detail['bytes'],
            'Storage entry census differs from measured totals')
    buckets={}
    prefixes={}
    for cell in state['cells']:
        if cell['index'] in reservation['pending_training_indices']:
            original=evaluation.final_cells(c['config'])[cell['index']]
            training=agents._directory(ROOT,c['config']['agents'])/'final'/agents._cell_name(original['task'],original['architecture'],original['seed'])
            key='training-'+str(cell['index']);prefixes[str(training.relative_to(ROOT))+'/']=key
            buckets[key]={'files':30,'bytes':2_000_000_000,'seen_files':0,'seen_bytes':0}
        if cell['index'] in reservation['open_evaluation_indices']:
            key='evaluation-'+str(cell['index']);prefixes[str((c['case']/f'cell-{cell["index"]:04d}').relative_to(ROOT))+'/']=key
            assembling=cell['index'] in reservation['pending_assembly_indices']
            buckets[key]={'files':630+20*int(assembling),'bytes':5_000_000_000+1_000_000_000*int(assembling),
                'seen_files':0,'seen_bytes':0}
    for ident in reservation['pending_jobs']:
        job=state['jobs'][ident];key='job-'+ident
        buckets[key]={'files':200,'bytes':100_000_000,'seen_files':0,'seen_bytes':0}
        if job['job_id'] is not None:prefixes[str((Path(c['descriptor']['case'])/('native-'+job['job_id'])).relative_to(ROOT))+'/']=key
        prefixes['plans/af-f2-'+ident+'/']=key
        prefixes['logs/af-f2-'+ident+'-']=key
        prefixes['state/af-f2-'+ident+'-']=key
    for item in detail['items']:
        name=item['relative_path'];key=next((key for prefix,key in prefixes.items() if name.startswith(prefix)),None)
        if key is not None:
            buckets[key]['seen_files']+=1;buckets[key]['seen_bytes']+=item['bytes']
    captured_files=sum(min(v['files'],v['seen_files']) for v in buckets.values())
    captured_bytes=sum(min(v['bytes'],v['seen_bytes']) for v in buckets.values())
    exceeded={key:value for key,value in buckets.items() if value['seen_files']>value['files'] or value['seen_bytes']>value['bytes']}
    if exceeded:
        atomic_json(c['case']/'storage-reservation.json',{'status':'resource_pending_storage',
            'reason':'A measured output bucket exceeded its declared bound; no inferred future savings',
            'exceeded_buckets':exceeded,'scientific_result':False,'admission_authority':False})
        raise PendingStorage('Measured output exceeds a reservation bucket; bounded forecast review required')
    forecast_files=max(0,reservation['files']-captured_files);forecast_bytes=max(0,reservation['bytes']-captured_bytes)
    quota_module=runpy.run_path(str(QUOTA));quota_reader=quota_module['require_storage_headroom_with_wait']
    try:
        headroom=quota_reader(ROOT,required_files=forecast_files,required_bytes=forecast_bytes,
            reserve_files=max(0,250000-forecast_files),reserve_bytes=2_000_000_000_000,
            evidence_dir=read_evidence(c,'quota-reads'),allocation_deadline_unix=c['deadline']+180)
        future_bytes,future_files,paths=_storage(ROOT,c['config'],forecast_bytes,forecast_files,personal_headroom=headroom)
    except (AdmissionError,ValueError,TimeoutError) as error:
        if isinstance(error,TimeoutError):
            approved=(type(error) is ReceiptLockTimeout and Path(error.path)==(ROOT/'state/storage-headroom.mutex').resolve()
                or type(error) in [quota_module.get('QuotaWaitBudgetExceeded'),quota_module.get('NativeAllocationMarginError')])
            if not approved:raise
        atomic_json(c['case']/'storage-reservation.json',{'status':'resource_pending_storage','reason':str(error),
            'reservation':reservation,'captured_campaign_files':captured_files,'captured_campaign_bytes':captured_bytes,
            'forecast_files':forecast_files,'forecast_bytes':forecast_bytes,'scientific_result':False,'admission_authority':False})
        raise PendingStorage(str(error)) from error
    proof={'status':'ready','reservation':reservation,'captured_campaign_files':captured_files,
        'captured_campaign_bytes':captured_bytes,'future_study_files':future_files,'future_study_bytes':future_bytes,
        'storage_budget_sha256':digest(ROOT/'state/storage-budget.json'),'personal_quota':headroom,
        'full240_minimum_parts_and_training_file_forecast':record['study_files']+240*(600+30+30+20+3*200)+12000,
        'scientific_result':False,'admission_authority':False}
    atomic_json(c['case']/'storage-reservation.json',proof);return proof


def save_state(c,state):
    """One compressed state; immutable worker/condition evidence stays separate."""
    path=c['case']/'state.json.gz';temp=path.with_suffix('.partial')
    raw=json.dumps(state,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    with temp.open('wb') as stream:
        with gzip.GzipFile(fileobj=stream,mode='wb',mtime=0) as writer:writer.write(raw)
        stream.flush();os.fsync(stream.fileno())
    os.replace(temp,path)
    atomic_json(c['case']/'state-binding.json',{'state_file_sha256':digest(path),'state_sha256':state['state_sha256'],
        'campaign_plan_sha256':state['plan_sha256'],'revision':state['revision'],'scientific_result':False,'admission_authority':False})


def initialize(c):
    plan_path=c['case']/'campaign-plan.json';state_path=c['case']/'state.json.gz'
    if plan_path.exists():
        require(state_path.exists(),'Interrupted campaign initialization requires reconciliation')
        plan=read_json(plan_path);binding=read_json(c['case']/'state-binding.json')
        require(digest(state_path)==binding['state_file_sha256'],'Persisted state file changed')
        with gzip.open(state_path,'rt',encoding='utf8') as stream:state=json.load(stream)
        require(state['state_sha256']==binding['state_sha256'] and state['plan_sha256']==plan['plan_sha256'],'Persisted state identity changed')
        dispatch._state(plan,state)
        return plan,state
    require(not state_path.exists(),'Unbound campaign state refused')
    d=c['descriptor']
    plan=dispatch.plan_campaign(c['config'],c['protocol'],protocol_binding={'path':str(c['protocol_path']),'sha256':d['frozen_protocol_sha256']},
        source_binding={'primary_source_sha256':PRIMARY_SCIENTIFIC_SOURCE_SHA256,'primary_agents_sha256':AGENTS_SHA,
        'execution_source_sha256':FRAMEWORK_SHA,'evaluation_module_sha256':digest(Path(evaluation.__file__))},
        policy_binding=c['policy'].receipt(),case=str(c['case']))
    atomic_json(plan_path,plan);state=dispatch.initial_state(plan);save_state(c,state);return plan,state


def worker_receipt(c,job,mode,index=None,*,descriptor_sha=None):
    path=contained(Path(c['descriptor']['case'])/f'native-{job}'/'receipt.json',ROOT/'maintenance')
    value=read_json(path)
    require(value.get('status')=='complete_native_work' and value.get('mode')==mode and value.get('index')==index
            and value.get('native_job_id')==job and value.get('descriptor_sha256')==(descriptor_sha or c['worker_descriptor_sha256'])
            and value.get('source_sha256')==FRAMEWORK_SHA and value.get('full_config_sha256')==BASE_CONFIG_SHA256
            and value.get('resource_policy')==c['policy'].receipt()
            and value.get('scientific_result') is False and value.get('admission_authority') is False,
            'Completed qualified worker receipt binding differs')
    return path,value


def positive_native(native,job):
    proof=native.get(job)
    return bool(proof and proof['state']=='COMPLETED' and proof['exit_code']=='0:0' and proof['elapsed_seconds']>0 and proof['cpus']>0)


def precision_gate(c,native):
    """Require actual paired reproduction, finite f64 rows and positive assembly."""
    gate=c['descriptor']['precision_gate'];jobs=[str(gate[k]) for k in ('diagnostics_job_id','assembly_job_id')]
    if any(native.get(job,{}).get('state') in dispatch.TERMINAL_FAILURES for job in jobs):raise ValueError('Precision gate native job failed; explicit repair required')
    if not all(positive_native(native,j) for j in jobs):return False
    old=frozen_file(gate['descriptor_path'],gate['descriptor_sha256'])
    for key in ('framework_release','execution_source_sha256','worker_sha256','qualification','resource_policy','frozen_protocol_sha256','canary_request','canary_plan'):
        require(old[key]==c['descriptor'][key],'Precision worker descriptor changed approved source/request/plan')
    records=[]
    for mode,job,key in [('precision-diagnostics',jobs[0],'diagnostics_receipt_path'),('precision-assemble',jobs[1],'assembly_receipt_path')]:
        path,value=worker_receipt(c,job,mode,descriptor_sha=gate['descriptor_sha256'])
        require(path==Path(gate[key]).resolve(),'Precision receipt does not belong to declared native job')
        if key.replace('_path','_sha256') in gate:require(digest(path)==gate[key.replace('_path','_sha256')],'Precision worker receipt changed')
        records.append({'path':str(path),'sha256':digest(path),'value':value})
    request=frozen_file(old['canary_request']['path'],old['canary_request']['sha256']);case=Path(request['case'])
    binding=old['canary_plan']
    require(digest(case/'plan.json')==binding['plan_sha256'] and digest(case/'plan-binding.json')==binding['plan_binding_sha256'],'Pinned precision plan changed')
    plan=evaluation._load_plan(case,binding['plan_sha256'])
    require(plan['canary'] is True and len(plan['conditions'])==request['required_conditions']==52,'Exact52-condition canary required')
    diagnostics=records[0]['value']['result'];summary=records[1]['value']['result'];reports=diagnostics['reports']
    f64=[r for r in reports if r['dtype']=='torch.float64'];f32=[r for r in reports if r['dtype']=='torch.float32']
    require(len(reports)==104 and len(f64)==len(f32)==52
            and {r['condition_index'] for r in f64}=={r['condition_index'] for r in f32}==set(range(52))
            and all(r['status']=='available' and r.get('unavailable_rows',0)==0 for r in f64)
            and sum(r['rows'] for r in f64)==1664 and any(r['status']=='unavailable' for r in f32)
            and diagnostics['plan_sha256']==binding['plan_sha256'],'Paired f32 failure reproduction/f64 availability gate failed')
    require(summary['status']=='complete_precision_canary' and summary['canary'] is True
            and summary['conditions']==52 and summary['trial_rows']==1664 and summary['plan_sha256']==binding['plan_sha256'],
            'Exact complete assembled precision canary required')
    import numpy as np
    import pandas as pd
    artifact=contained(case/summary['artifact_name'],case)
    require(digest(artifact)==summary['trial_file_sha256'],'Assembled canary trial bytes changed')
    frame=pd.read_parquet(artifact)
    require(len(frame)==1664 and frame.numerical_status.eq('available').all() and frame.calibration_numerical_status.eq('available').all()
            and np.isfinite(frame[['success','redirection','motor_accuracy','clean_policy_kl']].to_numpy(float)).all(),
            'Actual f64 canary rows are incomplete or nonfinite')
    evaluation.assemble(case,expected_plan_sha256=binding['plan_sha256'])
    frozen={'status':'qualified_precision_gate','records':[{k:r[k] for k in ('path','sha256')} for r in records],
        'canary_plan_sha256':binding['plan_sha256'],'trial_file_sha256':summary['trial_file_sha256'],
        'native':{j:native[j] for j in jobs},'scientific_result':False,'admission_authority':False}
    path=c['case']/'precision-gate.json'
    if path.exists():require(read_json(path)==frozen,'Previously qualified precision gate changed')
    else:atomic_json(path,frozen)
    return True


def condition_snapshot(case,plan):
    committed=evaluation._committed(case,plan);result=[]
    for index in sorted(committed):
        record=committed[index];path=case/record['_receipt_name']
        require(digest(path)==record['_receipt_sha256'],'Committed receipt changed during verification')
        result.append({'sha256':record['_receipt_sha256'],'value':read_json(path)})
    return result


def collect_packets(c,plan,state,native):
    packets={};verified_cases={}
    for cell in state['cells']:
        binding=cell['evaluation_plan']
        if binding is None:continue
        case=Path(binding['case'])
        require(digest(case/'plan-binding.json')==binding['plan_binding_sha256'],'Evaluation plan activation changed')
        ep=evaluation._load_plan(case,binding['plan_sha256'])
        verified_cases[str(case)]=condition_snapshot(case,ep)
    for ident,job in state['jobs'].items():
        jid=job['job_id']
        if jid is None or not positive_native(native,jid):continue
        intent=job['intent'];kind=intent['kind'];mode={'training':'agent-train','evaluation':'agent-evaluation','assembly':'agent-assembly'}[kind]
        path,receipt=worker_receipt(c,jid,mode,intent['cell_index'])
        proof=native[jid];allocation=receipt['allocation']
        require(allocation.get('JobId')==jid and allocation.get('NumCPUs')==str(proof['cpus'])
                and allocation.get('UserId')=='pwa209(3164768)' and allocation.get('Account')==proof['account'],
                'Worker allocation receipt differs from final owned native proof')
        packet={'campaign_plan_sha256':plan['plan_sha256'],'intent_id':ident,'native_job_id':jid,
            'case':intent['case'],'sha256':digest(path)}
        if kind=='training':
            summary_path=contained(receipt['training_summary_path'],ROOT)
            require(summary_path==Path(intent['case'])/'summary.json'
                    and digest(summary_path)==receipt['training_summary_sha256'] and read_json(summary_path)==receipt['training_summary'],
                    'Original final training summary changed')
            checkpoint=summary_path.parent/'checkpoint.pt'
            require(not checkpoint.is_symlink() and digest(checkpoint)==receipt['training_summary']['checkpoint_sha256'],
                    'Original final checkpoint changed')
            packet['value']=receipt['training_summary']
        else:
            cell=state['cells'][intent['cell_index']];binding=cell['evaluation_plan'];case=Path(binding['case'])
            require(receipt.get('evaluation_case')==str(case) and receipt.get('evaluation_plan_sha256')==binding['plan_sha256']
                    and receipt.get('evaluation_binding_sha256')==binding['plan_binding_sha256'],'Native evaluation receipt changed its case/plan')
            if str(case) not in verified_cases:
                require(digest(case/'plan-binding.json')==binding['plan_binding_sha256'],'Evaluation plan activation changed')
                ep=evaluation._load_plan(case,binding['plan_sha256']);verified_cases[str(case)]=condition_snapshot(case,ep)
            snapshot=verified_cases[str(case)]
            # Verify all current condition files every tick, but do not replace a
            # historical chunk's smaller, already frozen completion snapshot.
            if job['status']=='complete':continue
            packet['value']=receipt['result']
            if kind=='evaluation':packet['condition_receipts']=snapshot
        if job['status']=='complete':continue
        packets[ident]=packet
    return packets


def bind_ready(c,plan,state,gate):
    if not gate:return state
    for cell in state['cells']:
        if cell['evaluation_plan'] is not None or cell['training_job'] is None:continue
        job=state['jobs'][cell['training_job']]
        if job['status']!='complete':continue
        original=plan['cells'][cell['index']];case=Path(original['evaluation_case'])
        try:storage_guard(c,state,extra_evaluations=[cell['index']])
        except PendingStorage as error:c['storage_pending']=str(error);break
        require(not case.exists(),'Interrupted/foreign evaluation preparation requires reconciliation')
        summary=job['receipt']['value'];checkpoint=Path(original['training_case'])/'checkpoint.pt'
        result=evaluation.prepare_plan(c['config'],c['protocol'],checkpoint,case,
            checkpoint_sha256=summary['checkpoint_sha256'],primary_agents_sha256=AGENTS_SHA,
            task=original['task'],architecture=original['architecture'],seed=original['seed'])
        binding={'case':str(case),'plan_sha256':digest(case/'plan.json'),'plan_binding_sha256':digest(case/'plan-binding.json'),'value':result}
        state=dispatch.bind_evaluation_plan(plan,state,cell['index'],binding,expected_revision=state['revision']);save_state(c,state)
    return state


def native_plan(c,intent):
    name='af-f2-'+intent['intent_id'];directory=ROOT/'plans'/name;ledger=ROOT/'state'/(name+'-submission.json')
    require(not directory.exists() and not ledger.exists(),'Existing native intention cannot be resubmitted')
    directory.mkdir(parents=True)
    kind=intent['kind'];mode={'training':'agent-train','evaluation':'agent-evaluation','assembly':'agent-assembly'}[kind]
    argv=['python',str(c['descriptor']['worker_path']),'--descriptor',str(c['worker_descriptor_path']),
        '--descriptor-sha256',c['worker_descriptor_sha256'],'--mode',mode,'--index',str(intent['cell_index'])]
    resources=intent['resources']
    for key,arg in [('cpus','--cpus'),('gpus','--gpus'),('hours','--hours')]:argv += [arg,str(resources[key])]
    if kind!='training':
        binding=c['state']['cells'][intent['cell_index']]['evaluation_plan']
        argv += ['--evaluation-case',binding['case'],'--evaluation-plan-sha256',binding['plan_sha256'],
                 '--evaluation-binding-sha256',binding['plan_binding_sha256']]
    command=shlex.join(argv);contract={'cpus':resources['cpus'],'gpu':bool(resources['gpus']),
        'hours':resources['hours'],'memory':'64G' if kind=='training' else '32G'}
    text=slurm._script(c['config'],c['framework'],name,command,**contract)
    text=text.replace('set -euo pipefail','#SBATCH --hold\nset -euo pipefail',1)
    script=directory/(name+'.sbatch');script.write_text(text,encoding='utf8',newline='\n')
    task={'name':name,'script':str(script),'script_sha256':canonical_hash(text),'depends':[],
        'external_dependencies':[],'dependency_kind':'afterok','resources':contract,'command':command}
    value={'schema_version':1,'root':str(ROOT),'phase':name,'config_sha256':BASE_CONFIG_SHA256,
        'release_identity':source_identity(c['framework']),'tasks':[task],
        'campaign_plan_sha256':intent['campaign_plan_sha256'],'intent_sha256':intent['intent_sha256'],
        'controller_descriptor_sha256':c['descriptor_sha256'],'worker_descriptor_sha256':c['worker_descriptor_sha256']}
    atomic_json(directory/'plan.json',value)
    return directory/'plan.json',ledger,task


def verify_held(c,job,task,intent):
    result=fields(c['scheduler'](job,read_evidence(c,'held-reads')))
    require(result.get('JobId')==job and result.get('UserId')=='pwa209(3164768)'
            and result.get('JobName')=='hhw-'+task['name'] and result.get('JobState')=='PENDING'
            and result.get('Reason')=='JobHeldUser' and result.get('NumCPUs')==str(intent['resources']['cpus'])
            and result.get('NumNodes') in {'1','1-1'} and result.get('MinMemoryNode')==task['resources']['memory']
            and result.get('Account')==c['config']['paths']['gpu_account' if intent['resources']['gpus'] else 'cpu_account']
            and Path(result.get('Command','')).resolve()==Path(task['script']).resolve()
            and seconds(result.get('TimeLimit',''))==intent['resources']['hours']*3600,
            'Held native owner/name/script/account/resources differ')
    require(gpu_count(result.get('ReqTRES',''))==intent['resources']['gpus'],'Held native GPU request differs')
    return result


def submit_intentions(c,plan,state,intents,*,runner=subprocess.run):
    """Caller holds registration mutex; each action is issued at most once."""
    for intent in intents:
        resources=intent['resources'];name=intent['intent_id']
        admission(c,'before-'+name,cpus=resources['cpus'],gpus=resources['gpus'],hours=resources['hours'])
        c['state']=state;path,ledger_path,task=native_plan(c,intent)
        journal=c['case']/('submission-'+name+'.json')
        require(not journal.exists(),'Existing submission/release journal requires reconciliation')
        record={'intent':intent,'plan_path':str(path),'plan_sha256':digest(path),'ledger_path':str(ledger_path),
            'status':'before_sbatch','scientific_result':False,'admission_authority':False}
        atomic_json(journal,record)
        def bounded_runner(command,**kwargs):
            kwargs.setdefault('timeout',45)
            return runner(command,**kwargs)
        ledger=slurm.submit(path,ledger_path,runner=bounded_runner);job=str(ledger['tasks'][task['name']]['job_id'])
        state=dispatch.record_submission(plan,state,name,job,expected_revision=state['revision']);save_state(c,state)
        record.update(status='registered_held',job_id=job);atomic_json(journal,record)
        record['held_allocation']=verify_held(c,job,task,intent);atomic_json(journal,record)
        admission(c,'registered-'+name,cpus=0,gpus=0,hours=1)
        record['status']='releasing';atomic_json(journal,record)
        response=runner(['scontrol','release',job],text=True,capture_output=True,timeout=45)
        response.check_returncode()
        record.update(status='released',released_unix=time.time());atomic_json(journal,record)
    return state


def tick(c,*,runner=subprocess.run,native_reader=None):
    with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex',timeout=30):
        source_guard(c)
        plan,state=initialize(c)
        # No automatic replay after a crash between intent persistence, sbatch,
        # confirmation, global registration, held verification or release.
        for ident,job in state['jobs'].items():
            journal=c['case']/('submission-'+ident+'.json')
            require(job['job_id'] is not None and journal.exists() and read_json(journal)['status']=='released',
                    'Ambiguous prior submission/release preserved; explicit reconciliation required')
        admission(c,'tick-'+str(state['revision']))
        gate=c['descriptor']['precision_gate'];ids={j['job_id'] for j in state['jobs'].values() if j['job_id'] is not None}
        ids.update(str(gate[k]) for k in ('diagnostics_job_id','assembly_job_id'))
        records=(native_reader or c['native_reader'])(sorted(ids),{},owner='pwa209',account=None)
        native={ident:normalized_native(ident,record) for ident,record in records.items()}
        packets=collect_packets(c,plan,state,native)
        candidate,intents=dispatch.dispatch_tick(plan,state,native,packets)
        if candidate['status']=='needs_repair':
            save_state(c,candidate)
            raise ValueError('Qualified dispatcher reports needs_repair; native evidence retained')
        storage_guard(c,candidate)
        state=candidate;save_state(c,state)
        qualified=precision_gate(c,native)
        state=bind_ready(c,plan,state,qualified)
        # Bind only creates inputs. Dispatch again to issue evaluation intents.
        candidate,extra=dispatch.dispatch_tick(plan,state,native,{})
        try:storage_guard(c,candidate)
        except PendingStorage as error:c['storage_pending']=str(error);extra=[]
        else:state=candidate;save_state(c,state)
        intents += extra
        state=submit_intentions(c,plan,state,intents,runner=runner)
        summary={'status':state['status'],'revision':state['revision'],'precision_gate':qualified,
            'native_controller_job':c['job_id'],'segment':c['segment'],'jobs':len(state['jobs']),
            'training_complete':sum(j['intent']['kind']=='training' and j['status']=='complete' for j in state['jobs'].values()),
            'policies_complete':sum(v['complete'] for v in state['cells']),'new_intentions':len(intents),
            'state_sha256':state['state_sha256'],'storage_pending':c.get('storage_pending'),
            'scientific_result':False,'admission_authority':False,
            'H4_inference_status':state['H4_inference_status']}
        atomic_json(c['case']/'status.json',summary);return summary


def run(c,*,sleeper=time.sleep,clock=time.time):
    with resource_lease(ROOT,c['config'],cpus=1,gpus=0,max_hours=96,job_id=c['job_id'],resource_policy=c['policy']):
        for index in range(1920):
            if clock()+180>=c['deadline']:break
            try:summary=tick(c)
            except PendingStorage as error:
                summary={'status':'resource_pending_storage','reason':str(error),'segment':c['segment'],
                    'native_controller_job':c['job_id'],'scientific_result':False,'admission_authority':False}
                atomic_json(c['case']/'status.json',summary)
            except BaseException as error:
                atomic_json(c['case']/'controller-failure.json',{'status':'needs_repair','job_id':c['job_id'],
                    'segment':c['segment'],'error_type':type(error).__name__,'error':str(error),
                    'scientific_result':False,'admission_authority':False})
                raise
            print('AF_AGENT_CONTROLLER '+json.dumps(summary,sort_keys=True),flush=True)
            if summary['status']=='computational_evaluations_complete_inference_pending':return summary
            until=min(c['deadline']-180,clock()+180)
            while clock()<until:sleeper(min(30,until-clock()))
    result={'status':'finite_segment_complete','segment':c['segment'],'native_job_id':c['job_id'],
        'scientific_result':False,'admission_authority':False}
    atomic_json(c['area']/'segment-complete.json',result);return result


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--descriptor',required=True,type=Path);parser.add_argument('--descriptor-sha256',required=True)
    parser.add_argument('--segment',required=True,type=int);args=parser.parse_args(argv)
    run(load_context(args.descriptor,args.descriptor_sha256,args.segment))


if __name__=='__main__':main()
