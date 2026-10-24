"""Bounded native, whole-record first-adjoint comparison; no scientific fit.

Only one recording component and one held-out recording are compared. The full
training-fold scaler, duration knots, nuisance dimensions and normalizer remain
unchanged. This is not a full-cohort objective/performance qualification.
"""
from dataclasses import asdict
from pathlib import Path
import argparse
import copy
import hashlib
import importlib.util
import json
import math
import os
import re
import signal
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-caebc9c1dc77-8495a3d98551'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
SOURCE='8495a3d985515bf71f476295da87e663a1e764864ddce47a06794e7e11d88f94'
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
CONTEXT_SHA='b90861f983c8318d7968b8e56ab0a368932f611369880a9f05e41f7556dfeb24'
PREFIX_HELPER_SHA='dec3ace9591b71759bbd7418b550e657ecb87a85823652e9d1e56547fe19ec60'
PREFIX_TEST_SHA='874cfce84a7814536e6442a00775d7ad3b652ef7ad12a087e81585edd0b88169'
PREFIX_CASE=ROOT/'maintenance/behaviour-full-cohort-prefix-v5-20261022'
PREFIX_SUPPORT=ROOT/'maintenance/behaviour-full-cohort-prefix-v5-support-20261022'
PREFIX_STAGE='full_fold_preprocessing_and_eligibility_census'
ADJOINT_SHA='5ff03ba506d4ad8dab66dd51def1c728aade96a8af77661be03de20e40a205f9'
PROFILE_SHA='e60561b425d500d08690675d98e4f86b2617044e446183433cdf3e4465602e59'
CASE=ROOT/'maintenance/behaviour-full-cohort-continuation-v2-20261024'
SUPPORT=ROOT/'maintenance/behaviour-full-cohort-continuation-support-v2-20261024'
MEMORY_LIMIT=48*1024**3
PRIVATE_TEST_COUNT=72
PRIVATE_TEST_ROSTER_SHA='78847872afe3396dbcbee0312a3c9a07af5f782e862d51b1d103618d57aba420'
FALSE_FLAGS={'scientific_result':False,'scientific_fit':False,'complete_declared_workload':False,
             'admission_authority':False,'original_H1_H2_H3_gates_cleared':False,
             'native_gpu_qualified':False,'second_derivatives_supported':False,
             'whole_cohort_objective_qualified':False,'optimizer_convergence_qualified':False,
             'MALA_acceptance_qualified':False}
STAGES=('full_fold_preprocessing_and_eligibility_census','primary_full_training_objective_and_all_gradients',
        'adjoint_full_training_objective_and_all_gradients','full_training_objective_and_gradient_comparison')


def require(value,message):
    if not value:raise ValueError(message)


def digest(path):
    value=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(8*1024**2),b''):value.update(block)
    return value.hexdigest()


def exclusive(path,value):
    raw=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    require(len(raw)<=20_000_000,'Bounded technical receipt required')
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def ordinary(path,*,fresh=False,maintenance=True):
    original=Path(path);resolved=original.resolve();root=ROOT.resolve()
    require(original.is_absolute() and resolved.is_relative_to(root) and resolved!=root,'Personal study path required')
    prefix=ROOT if original.is_relative_to(ROOT) else root
    require(original.is_relative_to(prefix),'Foreign redirected namespace refused')
    require(all(not p.is_symlink() for p in [original,*original.parents] if p!=prefix and p.is_relative_to(prefix)),
            'Subordinate source/output symlink refused')
    if maintenance:require(resolved.is_relative_to(root/'maintenance'),'Personal maintenance namespace required')
    require(not resolved.exists() if fresh else resolved.is_file(),'Fresh output required' if fresh else 'Ordinary source file required')
    return resolved


def load_exact(name,path,sha):
    require(digest(path)==sha,'Pinned private/qualified module bytes changed')
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    require(digest(path)==sha,'Module changed during load');return module


def scheduler_utc():
    """Native query display basis only; stored recording clocks are untouched."""
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()


def shared_admission(callback,allocation_deadline,*,evidence_dir,lock_factory=None,wall=time.time,clock=time.monotonic):
    """Wait finitely for the same kernel inode, then call original admission once.

    Only pre-acquisition exact ReceiptLockTimeout on this resolved mutex may
    retry. No callback, scientific operator, inode removal or owner substitution
    is retried. Diagnostics are durable and occur outside the acquired lock.
    """
    from horsehair.core import ReceiptLockTimeout
    if lock_factory is None:
        from horsehair.core import exclusive_file_lock
        lock_factory=exclusive_file_lock
    require(not isinstance(allocation_deadline,bool) and isinstance(allocation_deadline,(int,float))
        and math.isfinite(allocation_deadline),'Finite native admission deadline required')
    path=(ROOT/'state/af-campaign-registration.mutex').resolve()
    evidence=ordinary(evidence_dir,fresh=True);evidence.mkdir(mode=0o700,exist_ok=False)
    started=clock();wait_limit=min(900.,allocation_deadline-wall()-300.)
    require(math.isfinite(started),'Finite monotonic admission clock required')
    end=started+max(0.,wait_limit);attempts=[]
    def finish(status,**extra):
        value={'status':status,'path':str(path),'maximum_acquisition_seconds':max(0.,wait_limit),
            'maximum_per_attempt_seconds':30.,'maximum_attempts':31,'elapsed_seconds':clock()-started,
            'allocation_deadline_unix':allocation_deadline,'native_acquisition_margin_seconds':300,
            'attempts':attempts,'callback_retried':False,'guard_removed_or_replaced':False,
            'scope':'Unchanged qualified8495 combined_admission only','native_query_timezone':'UTC',
            'released_before_numerical_execution':True,
            'root_serializes_other_publishers_until_admission_returns':True,**extra,**FALSE_FLAGS}
        exclusive(evidence/'result.json',value);return value
    for number in range(1,32):
        remaining=min(end-clock(),allocation_deadline-wall()-300.)
        if remaining<.01:
            summary=finish('pending_registration_no_probe_started',reason='Finite registration acquisition envelope exhausted')
            raise DeadlineBound('Registration acquisition exhausted; durable diagnostics: '+str(evidence/'result.json'))
        timeout=min(30.,remaining)
        attempt={'attempt':number,'timeout_seconds':timeout,'status':'acquisition_intended'}
        exclusive(evidence/(f'attempt-{number:02d}-intent.json'),dict(attempt,guard_path=str(path),**FALSE_FLAGS))
        # A slow durable intent write must not consume the reserved native margin.
        timeout=min(timeout,end-clock(),allocation_deadline-wall()-300.)
        if timeout<.01:
            attempts.append(dict(attempt,status='not_attempted_margin_exhausted'))
            finish('pending_registration_no_probe_started',reason='Margin consumed before kernel acquisition')
            raise DeadlineBound('Registration margin exhausted after diagnostic persistence')
        attempt['timeout_seconds']=timeout;acquired=False;callback_started=False
        try:
            with lock_factory(path,timeout=timeout):
                acquired=True;acquired_after=clock()-started
                if clock()>=end or wall()+300>=allocation_deadline:
                    raise DeadlineBound('Registration margin exhausted at kernel acquisition; callback not started')
                callback_started=True;value=callback()
                if wall()+120>=allocation_deadline:
                    raise DeadlineBound('Native finalization margin exhausted during unchanged admission')
            attempts.append(dict(attempt,status='admission_complete',acquired_after_seconds=acquired_after))
            exclusive(evidence/(f'attempt-{number:02d}-result.json'),attempts[-1])
            return value,finish('complete_admission',acquired_after_seconds=acquired_after)
        except BaseException as error:
            retry=(not acquired and type(error) is ReceiptLockTimeout and Path(error.path)==path)
            attempts.append(dict(attempt,status='kernel_contention' if retry else 'strict_failure',
                error_type=type(error).__name__,reason=str(error),callback_started=callback_started,
                retry_exact_preacquisition_contention_only=retry))
            exclusive(evidence/(f'attempt-{number:02d}-result.json'),attempts[-1])
            if not retry:
                finish('strict_admission_failure',error_type=type(error).__name__,reason=str(error))
                raise
    finish('pending_registration_no_probe_started',reason='Finite acquisition attempt ceiling reached')
    raise DeadlineBound('Finite registration attempt ceiling reached; callback never started')


class MemoryBound(RuntimeError):pass
class DeadlineBound(RuntimeError):pass


def rss_bytes(pid=None):
    """Current resident bytes; native Linux /proc is mandatory for supervision."""
    target=os.getpid() if pid is None else pid
    path=Path('/proc')/str(target)/'status'
    if path.is_file():
        match=re.search(r'(?m)^VmRSS:\s*([0-9]+)\s+kB\s*$',path.read_text())
        if match is None and pid is not None:raise ProcessLookupError('Child no longer has resident process memory')
        require(match is not None,'Native current RSS evidence missing');return int(match[1])*1024
    if os.name=='nt':
        # Standalone local synthetic tests need no optional process package.
        import ctypes
        from ctypes import wintypes
        class Counters(ctypes.Structure):
            _fields_=[('cb',wintypes.DWORD),('PageFaultCount',wintypes.DWORD),
                *[(name,ctypes.c_size_t) for name in ('PeakWorkingSetSize','WorkingSetSize','QuotaPeakPagedPoolUsage',
                    'QuotaPagedPoolUsage','QuotaPeakNonPagedPoolUsage','QuotaNonPagedPoolUsage','PagefileUsage','PeakPagefileUsage')]]
        kernel=ctypes.WinDLL('kernel32',use_last_error=True);psapi=ctypes.WinDLL('psapi',use_last_error=True)
        kernel.OpenProcess.restype=wintypes.HANDLE;kernel.GetCurrentProcess.restype=wintypes.HANDLE
        kernel.CloseHandle.argtypes=[wintypes.HANDLE]
        psapi.GetProcessMemoryInfo.argtypes=[wintypes.HANDLE,ctypes.POINTER(Counters),wintypes.DWORD]
        handle=kernel.GetCurrentProcess() if target==os.getpid() else kernel.OpenProcess(0x410,False,target)
        require(bool(handle),'Windows test process RSS handle unavailable');value=Counters();value.cb=ctypes.sizeof(value)
        try:require(psapi.GetProcessMemoryInfo(handle,ctypes.byref(value),value.cb),'Windows test process RSS unavailable')
        finally:
            if target!=os.getpid():kernel.CloseHandle(handle)
        return int(value.WorkingSetSize)
    if pid is not None:raise ProcessLookupError('Child RSS process disappeared')
    raise ValueError('Native current RSS evidence missing')


class MemoryGuard:
    """Fail before an oversized primary saved graph; supervisor also checks RSS.

    Unique storage accounting can conservatively include a no-longer-live saved
    tensor. It is a rejection bound, never a forecast or proof of peak RSS.
    """
    def __init__(self,deadline,*,limit=MEMORY_LIMIT,rss=rss_bytes,clock=time.monotonic):
        require(type(limit) is int and limit>0 and math.isfinite(deadline),'Finite memory/deadline guard required')
        self.deadline,self.limit,self.rss,self.clock=deadline,limit,rss,clock
        self.storage={};self.saved_bytes=0;self.calls=0;self.peak_rss=0
    def check(self):
        if self.clock()>=self.deadline:raise DeadlineBound('Whole-clock probe deadline reached; comparison remains partial')
        current=self.rss();self.peak_rss=max(self.peak_rss,current)
        if current>=self.limit:raise MemoryBound('Current RSS reached the48GiB technical rejection ceiling')
    def pack(self,tensor):
        self.calls+=1
        storage=tensor.untyped_storage();key=(str(tensor.device),storage.data_ptr())
        size=storage.nbytes();previous=self.storage.get(key,0)
        self.saved_bytes+=max(0,size-previous);self.storage[key]=max(size,previous)
        if self.saved_bytes>=self.limit:raise MemoryBound('Unique primary saved storage reached the48GiB technical rejection ceiling')
        if self.calls%128==1:self.check()
        return tensor
    def __enter__(self):
        import torch
        self.check();self.hooks=torch.autograd.graph.saved_tensors_hooks(self.pack,lambda tensor:tensor)
        self.hooks.__enter__();return self
    def __exit__(self,*args):return self.hooks.__exit__(*args)
    def evidence(self):
        return {'saved_storage_rejection_bound_bytes':self.limit,'unique_saved_storage_bytes':self.saved_bytes,
                'saved_tensor_callbacks':self.calls,'observed_current_rss_peak_bytes':self.peak_rss,
                'saved_storage_accounting_is_conservative':True}


def select_clock(roster,identities,*,salt):
    allowed=set(identities);candidates=[row for row in roster if row['animal_id'] in allowed and row['observed_any']]
    require(len(candidates)==len(allowed) and bool(candidates),'Complete eligible source-clock selection roster required')
    longest=max(row['bins'] for row in candidates)
    selected=min((row for row in candidates if row['bins']==longest),
        key=lambda row:hashlib.sha256((salt+row['animal_id']).encode()).hexdigest())
    return dict(selected,selection='Longest whole source clock; SHA256 source-ID tie break; outcomes not inspected')


def compare_tensor(reference,candidate,*,atol=2e-9,rtol=2e-7):
    import torch
    require(reference.shape==candidate.shape and reference.dtype==candidate.dtype,'Paired tensor shape/dtype changed')
    require(bool(torch.isfinite(reference).all()) and bool(torch.isfinite(candidate).all()),'Nonfinite paired numeric evidence')
    a,b=reference.detach().to(torch.float64),candidate.detach().to(torch.float64)
    difference=(a-b).abs();scaled=difference/(atol+rtol*a.abs())
    evidence={'shape':list(a.shape),'elements':a.numel(),'max_absolute_difference':float(difference.max()) if a.numel() else 0.,
              'max_tolerance_ratio':float(scaled.max()) if a.numel() else 0.,'atol':atol,'rtol':rtol}
    require(evidence['max_tolerance_ratio']<=1,'Paired whole-record numerical equivalence failed: '+str(evidence))
    return evidence


def compare_gradients(reference,candidate):
    left,right=dict(reference.named_parameters()),dict(candidate.named_parameters());require(left.keys()==right.keys(),'Full parameter roster changed')
    output={}
    for name,p in left.items():
        q=right[name];require(p.shape==q.shape and p.requires_grad==q.requires_grad,'Original nuisance parameter dimensions changed')
        require((p.grad is None)==(q.grad is None),'Gradient presence changed for '+name)
        output[name]={'shape':list(p.shape),'requires_grad':p.requires_grad,'gradient_is_none':p.grad is None}
        if p.grad is not None:output[name].update(compare_tensor(p.grad,q.grad))
    return output


def marginal_scores(output,weights):
    import torch
    cumulative=output['log_scores'].cumsum(1)
    marginal=torch.logsumexp(cumulative+weights[:,None],0)
    return torch.cat([marginal[:1],marginal[1:]-marginal[:-1]])


def paired_stage(s,adjoint,initial,sequence,normalizer,settings,stage,deadline,*,guard_factory=MemoryGuard):
    """One whole recording, unchanged nuisance rows; not a whole-fold gradient."""
    import torch
    validation=stage.startswith('validation_');gradient='gradients' in stage;update='Adam_update' in stage
    models=[copy.deepcopy(initial),copy.deepcopy(initial)];outputs=[];evidence=[];artifacts=[]
    require(type(normalizer) is int and normalizer>=int(sequence.observed.sum())>0,'Full-fold positive normalizer required')
    for index,model in enumerate(models):
        require(type(model) is s.MovementModel and model.max_duration_bins is None,'Original uncapped primary class required')
        model.zero_grad();effects,weights=model.predictive_random_effects(sequence,3) if validation else (None,None)
        if validation:require(len(effects)==3**model.states,'Actual complete predictive context grid required')
        forward=(lambda:model(sequence,effects,return_filter=validation)) if index==0 else (lambda:adjoint.forward_sequence(model,sequence,effects))
        guard=guard_factory(deadline);started=time.perf_counter();cpu=time.process_time()
        if gradient:
            with guard:
                output=forward()
                if validation:
                    eligible=sequence.observed & (sequence.times-sequence.times[0]>=float(settings.get('warmup_seconds',600)))
                    require(bool(eligible.any()),'Original full-clock validation warm-up window is empty')
                    objective=-marginal_scores(output,weights)[model._tensor(eligible).bool()].mean()
                else:objective=(-output['log_prior']-output['log_likelihood'].sum())/normalizer
                require(bool(torch.isfinite(objective)),'Nonfinite original paired objective');objective.backward();guard.check()
        else:
            with torch.no_grad():output=forward()
            objective=output['log_likelihood'].sum();guard.check()
        outputs.append({'log_scores':output['log_scores'].detach().clone(),'log_likelihood':output['log_likelihood'].detach().clone(),
                        'log_prior':output['log_prior'].detach().clone(),'max_tail_probability':output['max_tail_probability'].detach().clone(),
                        'objective':objective.detach().clone()})
        if validation:outputs[-1]['marginal_scores']=marginal_scores(output,weights).detach().clone()
        artifacts.append({'gradients':{name:None if p.grad is None else p.grad.detach().clone() for name,p in model.named_parameters()}})
        evidence.append({'implementation':'primary0e2' if index==0 else 'qualified_first_adjoint',
            'wall_seconds':time.perf_counter()-started,'process_cpu_seconds':time.process_time()-cpu,
            'full_clock_bins':len(sequence.times),'contexts':len(effects) if validation else 1,'guard':guard.evidence()})
        del output
    comparisons={key:compare_tensor(outputs[0][key],outputs[1][key]) for key in outputs[0]}
    result={'stage':stage,'status':'paired_complete','measurements':evidence,'comparisons':comparisons,
            'one_record_component_only':True,'global_training_fold_normalizer':normalizer,
            'prior_in_training_objective_exactly_once':not validation,'validation_objective_has_no_prior':validation}
    if gradient:result['all_named_gradients']=compare_gradients(*models)
    if update:
        norms=[]
        for model in models:
            optimizer=torch.optim.Adam(model.parameters(),lr=float(settings.get('learning_rate',.001)))
            norms.append(torch.nn.utils.clip_grad_norm_(model.parameters(),float(settings.get('gradient_clip',5))))
            optimizer.step()
        result['gradient_norm_before_clip']=compare_tensor(norms[0],norms[1])
        result['clipped_all_named_gradients']=compare_gradients(*models)
        result['one_original_Adam_update']={name:compare_tensor(value,models[1].state_dict()[name]) for name,value in models[0].state_dict().items()}
        result['clip_calls_per_implementation']=result['Adam_update_calls_per_implementation']=1
    for index,model in enumerate(models):artifacts[index].update(outputs[index],model_state=model.state_dict())
    return result,artifacts


def source_bindings(primary,framework):
    from horsehair.core import source_identity,canonical_hash,read_json
    require(Path(primary).resolve()==PRIMARY.resolve() and Path(framework).resolve()==FRAMEWORK.resolve(),'Exact immutable primary/framework paths required')
    require(source_identity(primary)['sha256']==PRIMARY_SHA and source_identity(framework)['sha256']==SOURCE,'Exact qualified source identities required')
    config=read_json(PRIMARY/'configs/study.json')
    require(canonical_hash(config)==CONFIG and read_json(FRAMEWORK/'configs/study.json')==config,'Unchanged full scientific configuration required')
    require(digest(FRAMEWORK/'src/horsehair/duration_adjoint.py')==ADJOINT_SHA
        and digest(FRAMEWORK/'src/horsehair/cohort_profile.py')==PROFILE_SHA,'Exact newly qualified technical modules required')
    return config


class NoEligibleValidation(RuntimeError):pass


def bound_json(path,sha):
    path=ordinary(path)
    require(re.fullmatch('[0-9a-f]{64}',sha) and path.stat().st_size<=20_000_000,'Bounded explicit input SHA required')
    raw=path.read_bytes();require(hashlib.sha256(raw).hexdigest()==sha,'Immutable prefix input changed')
    def unique(pairs):
        value={}
        for key,item in pairs:
            require(key not in value,'Duplicate prefix metadata key');value[key]=item
        return value
    return json.loads(raw,object_pairs_hook=unique,parse_constant=lambda x:(_ for _ in ()).throw(ValueError('Nonfinite metadata refused')))


def prefix_evidence(path,sha,job,observed):
    """Accept independent positive verification while preserving original failure."""
    from horsehair.slurm import completed_native_dependency
    require(re.fullmatch('[1-9][0-9]*',job) and job!=ORIGINAL_JOB_ID
        and Path(path)==VERIFIER_OUTPUT/('native-'+job)/'receipt.json',
        'Exact independent verifier receipt/job required')
    value=bound_json(path,sha)
    expected=bound_json(Path(__file__).with_name('expected-original-manifest.json'),EXPECTED_MANIFEST_SHA)
    require(hashlib.sha256(json.dumps(expected,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()==EXPECTED_CANONICAL_SHA,
        'Exact original manifest identity required')
    record=observed.get(job,{})
    require(record.get('state')=='COMPLETED' and record.get('exit_code')=='0:0'
        and record.get('user')=='pwa209' and record.get('account')=='def-ptewarie_cpu'
        and record.get('job_id_raw')==job and record.get('unallocated_metadata') is False
        and type(record.get('cpus')) is int and record['cpus']==4
        and type(record.get('elapsed_seconds')) is int and 0<record['elapsed_seconds']<=7200
        and 'gpu' not in record.get('alloc_tres','').lower(),
        'Positive owned scalar4CPU/two-hour verifier completion required')
    completion=completed_native_dependency(ROOT,job)
    require(completion is not None,'Verifier native completion must be persisted positively')
    producer=observed.get(ORIGINAL_JOB_ID,{})
    old=expected['original_native']
    require(producer.get('state')==old['state']=='FAILED' and producer.get('exit_code')==old['exit_code']=='1:0'
        and producer.get('job_id_raw')==ORIGINAL_JOB_ID and producer.get('user')==old['owner']=='pwa209'
        and producer.get('account')==old['account']=='def-ptewarie_cpu'
        and producer.get('cpus')==old['cpus']==1 and producer.get('elapsed_seconds')==old['elapsed_seconds']==18500
        and producer.get('unallocated_metadata') is False and 'gpu' not in producer.get('alloc_tres','').lower(),
        'Original failed native producer must remain explicitly failed')
    require(value.get('status')=='technical_prefix_independently_verified' and value.get('native_job_id')==job
        and value.get('original_job_id')==ORIGINAL_JOB_ID and value.get('original_native')==old
        and value.get('expected_manifest_sha256')==EXPECTED_CANONICAL_SHA
        and value.get('original_helper_sha256')==PREFIX_HELPER_SHA
        and value.get('primary_source_sha256')==PRIMARY_SHA and value.get('framework_source_sha256')==SOURCE
        and value.get('full_config_sha256')==CONFIG and value.get('original_receipt_status')=='worker_failed'
        and value.get('worker_result_status')=='complete_full_cohort_first_update_technical_prefix'
        and value.get('consumer_requires_verifier_native_completed_zero') is True
        and all(value.get(k) is False for k in (*FALSE_FLAGS,'canonical_state_changed','numerical_worker_replayed',
            'artifacts_deserialized','original_native_history_rewritten')),
        'Exact source-bound independent technical certificate required')
    require(re.fullmatch('[0-9a-f]{64}',str(VERIFIER_SOURCE_MANIFEST_SHA))
        and value.get('source_manifest_sha256')==VERIFIER_SOURCE_MANIFEST_SHA,
        'Native plan must pin the exact verifier source bundle')
    bundle=bound_json(VERIFIER_SOURCE/'source-manifest.json',VERIFIER_SOURCE_MANIFEST_SHA)
    require(isinstance(bundle.get('files'),dict) and bundle['files'].get('expected-original-manifest.json')==EXPECTED_MANIFEST_SHA,
        'Verifier must bind the same frozen original manifest')
    for name,pin in bundle['files'].items():
        require(isinstance(name,str) and not Path(name).is_absolute() and '..' not in Path(name).parts,
            'Confined verifier source bundle required')
        require(digest(ordinary(VERIFIER_SOURCE/name))==pin,'Verifier source changed')
    registration=verifier_registration(job,VERIFIER_SOURCE_MANIFEST_SHA)
    files=original_file_bindings(expected)
    certificate_files=value.get('evidence_files',{})
    require(isinstance(certificate_files,dict) and set(files).issubset(certificate_files),
        'Independent certificate must bind every original file and artifact')
    for name,pin in files.items():
        require(isinstance(certificate_files[name],dict) and certificate_files[name].get('sha256')==pin,
            'Independent evidence file hash differs from frozen original')
    helper=ordinary(ROOT/expected['helper_path'])
    require(digest(helper)==PREFIX_HELPER_SHA,'Exact original v5 preprocessing helper required')
    prefix=load_exact('qualified_original_v5_prefix_ingress',helper,PREFIX_HELPER_SHA)
    for suite,name in [('prefix','private-tests.xml'),('context','context-private-tests.xml')]:
        prefix.verify_private_junit(PREFIX_SUPPORT/name,suite=suite)
    worker_path=PREFIX_CASE/'worker-result.json'
    worker=bound_json(worker_path,expected['pinned_files'][str(worker_path.relative_to(ROOT))])
    prefix.validate_worker_result(worker,PREFIX_CASE)
    require(worker.get('status')=='complete_full_cohort_first_update_technical_prefix'
        and isinstance(worker.get('torch_version'),str) and bool(worker['torch_version']),
        'Actual complete original v5 worker/runtime required')
    stages=worker['completed_stages']
    require(value.get('completed_stages')==[stage['stage'] for stage in stages]
        and stages and stages[0].get('stage')==PREFIX_STAGE,
        'Independent certificate must bind every completed original stage')
    stage=stages[0]
    plan_path=PREFIX_CASE/'plan.json';plan=bound_json(plan_path,stage['plan_sha256'])
    census_path=PREFIX_CASE/'clock-eligibility-census.json';census=bound_json(census_path,plan['census_sha256'])
    require(stage.get('census_sha256')==plan['census_sha256'] and census.get('all_original_roster_rows_retained') is True
        and plan.get('private_helper_sha256')==PREFIX_HELPER_SHA and plan.get('primary_source_sha256')==PRIMARY_SHA
        and plan.get('framework_source_sha256')==SOURCE and plan.get('full_config_sha256')==CONFIG,
        'Original full-roster/source/scaler plan provenance changed')
    artifact=ordinary(stage['artifacts_path'])
    require(artifact==PREFIX_CASE/(PREFIX_STAGE+'.pt') and digest(artifact)==stage['artifacts_sha256'],
        'Preserved original stage1 artifact changed')
    return {'receipt':{'path':str(path),'sha256':sha},'native_job_id':job,'native_observed':record,'completion':completion,
        'original_job_id':ORIGINAL_JOB_ID,'original_native_outcome_preserved':producer,
        'independent_verification_only':True,'verifier_source_manifest_sha256':VERIFIER_SOURCE_MANIFEST_SHA,
        'verifier_registration':registration,
        'runtime_torch_version':worker['torch_version'],
        'helper':{'path':str(helper),'sha256':PREFIX_HELPER_SHA},'plan':{'path':str(plan_path),'sha256':stage['plan_sha256']},
        'census':{'path':str(census_path),'sha256':plan['census_sha256']},
        'original_stage1_artifact':{'path':str(artifact),'sha256':stage['artifacts_sha256'],'loaded':False},
        'full_prefix_completion_required':False,'all_original_v5_stages_independently_verified':True,'only_complete_stage1_is_required':True,**FALSE_FLAGS}

def prepare(s,b,profile,config,root,case,deadline,emit,prefix_info=None):
    """Recompute the exact pinned full training fold; never select one recording."""
    import torch
    require(prefix_info is not None,'Completed source-bound prefix preprocessing evidence is required')
    require(str(torch.__version__)==prefix_info['runtime_torch_version'],'Original and recomputed Torch runtimes differ')
    prefix=load_exact('qualified_prefix_recomputation',ordinary(prefix_info['helper']['path']),PREFIX_HELPER_SHA)
    original=bound_json(prefix_info['plan']['path'],prefix_info['plan']['sha256'])
    original_census=bound_json(prefix_info['census']['path'],prefix_info['census']['sha256'])
    try:model,train,validation,scaler,n,settings,recomputed=prefix.prepare(s,b,profile,config,root,case,deadline,emit)
    except prefix.MemoryBound as error:raise MemoryBound(str(error)) from error
    except prefix.DeadlineBound as error:raise DeadlineBound(str(error)) from error
    require(recomputed==original and bound_json(case/'clock-eligibility-census.json',recomputed['census_sha256'])==original_census,
        'Recomputed full-fold scaler/knots/roles/normalizer or clock census differs from bound prefix')
    require([q.animal_id for q in train]==original['split']['training_ids']
        and len(train)==original['training_fold_recordings']==1107
        and sum(len(q.times) for q in train)==original['training_fold_full_bins']==7828183
        and n==original['global_observed_bin_normalizer']==sum(int(q.observed.sum()) for q in train)==2666463
        and len(set(q.animal_id for q in train))==len(train),
        'Exact complete ordered original training fold and global observed normalizer required')
    require(original['full_nuisance_dimensions']=={'animal_rows':1107,'experiment_rows':10}
        and original['original_validation_criterion_defined'] is False,
        'Original nuisance dimensions and unresolved validation criterion must remain unchanged')
    (case/'plan.json').rename(case/'recomputed-prefix-plan.json')
    plan=dict(recomputed,prefix_evidence=prefix_info,
        recomputed_prefix_plan_sha256=digest(case/'recomputed-prefix-plan.json'),
        preprocessing_recomputed_not_restored=True,original_stage1_tensor_artifact_loaded=False,
        byte_parity_with_original_initial_tensor_artifact_claimed=False,
        numerical_route={'primary':'cohort_profile.objective_gradient','candidate':'duration_adjoint.objective_gradient',
            'independent_batch_size':1,'block_size':None,'prior_applications_per_implementation':1,
            'optimizer_updates':0,'mid_operation_resume_supported':False},
        private_full_fold_helper_sha256=digest(Path(__file__)))
    exclusive(case/'plan.json',plan)
    return model,train,n,settings,plan


class ResidentDeadlineGuard(MemoryGuard):
    """Bound current resident memory across sequential released batch graphs."""
    def pack(self,tensor):
        self.calls+=1
        if self.calls%128==1:self.check()
        return tensor
    def evidence(self):
        return {'resident_rejection_bound_bytes':self.limit,'observed_current_rss_peak_bytes':self.peak_rss,
            'saved_tensor_callbacks':self.calls,'cumulative_saved_storage_rejection_applied':False,
            'tracking_scope':'Current RSS and deadline; released cohort-batch storage is not accumulated'}


def full_training_comparison(profile,adjoint,initial,training,normalizer,deadline,emit,commit,*,guard_factory=MemoryGuard,candidate_guard_factory=ResidentDeadlineGuard):
    """Route unchanged complete-fold APIs; this wrapper defines no likelihood."""
    require(training and len(training)==1107 and normalizer==2666463
        and normalizer==sum(int(q.observed.sum()) for q in training)
        and len({q.animal_id for q in training})==len(training),
        'The complete ordered training fold and its original positive normalizer are mandatory')
    models=[copy.deepcopy(initial),copy.deepcopy(initial)]
    require(models[0] is not models[1] and all(model is not initial for model in models),'Separate initial model copies required')
    left,right=models[0].state_dict(),models[1].state_dict()
    require(left.keys()==right.keys(),'Identical original initial parameter roster required')
    for name in left:require(compare_tensor(left[name],right[name])['max_absolute_difference']==0.,'Identical original initial model states required')
    class BoundedTimings(profile.Timings):
        def __init__(self):super().__init__(emit);self.memory={}
        def call(self,phase,callback,**unit):
            guard=guard_factory(deadline)
            with guard:value=super().call(phase,callback,**unit)
            guard.check();e=guard.evidence();old=self.memory.setdefault(phase,{'maximum_saved_storage_bytes':0,'maximum_current_rss_bytes':0})
            old['maximum_saved_storage_bytes']=max(old['maximum_saved_storage_bytes'],e['unique_saved_storage_bytes'])
            old['maximum_current_rss_bytes']=max(old['maximum_current_rss_bytes'],e['observed_current_rss_peak_bytes'])
            return value
    objectives=[];measurements=[]
    for index,model in enumerate(models):
        guard_factory(deadline).check();emit({'stage':STAGES[index+1],'state':'started'},True)
        started=time.perf_counter();cpu=time.process_time()
        if index==0:
            timer=BoundedTimings()
            objective=profile.objective_gradient(model,training,normalizer,timer)
            memory=timer.memory;timings=copy.deepcopy(timer.totals)
        else:
            guard=candidate_guard_factory(deadline)
            with guard:objective=adjoint.objective_gradient(model,training,normalizer,batch_size=1,block_size=None)
            guard.check();memory=guard.evidence();timings={}
        objectives.append(objective.detach().clone())
        measurement={'implementation':'primary0e2' if index==0 else 'qualified_first_adjoint',
            'wall_seconds':time.perf_counter()-started,'process_cpu_seconds':time.process_time()-cpu,
            'training_clocks':len(training),'training_bins':sum(len(q.times) for q in training),
            'global_observed_bin_normalizer':normalizer,'prior_applications':1,
            'independent_batch_size':None if index==0 else 1,'block_size':None,
            'timings':timings,'memory':memory}
        measurements.append(measurement)
        commit(STAGES[index+1],measurement,{'objective':objective.detach().clone(),
            'model_state':model.state_dict(),
            'named_gradients':{name:None if p.grad is None else p.grad.detach().clone() for name,p in model.named_parameters()}})
    comparison={'objective':compare_tensor(objectives[0],objectives[1]),
        'all_named_gradients':compare_gradients(*models),'measurements':measurements,
        'training_clocks':len(training),'training_bins':sum(len(q.times) for q in training),
        'global_observed_bin_normalizer':normalizer,'prior_applications_per_implementation':1,
        'separate_identical_initial_models':True,'optimizer_updates':0,
        'validation_calls':0,'mid_operation_resume_supported':False,
        'coverage_scope':'One complete original declared training fold at its frozen initialization; other workload cells remain unmeasured'}
    commit(STAGES[3],comparison,{'comparison':comparison})
    return comparison


def record_probe_evidence(observed):
    """Require the completed original one-record probe without loading tensors."""
    from horsehair.slurm import completed_native_dependency
    job='6943757';record=observed.get(job,{})
    require(record.get('job_id_raw')==job and record.get('state')=='COMPLETED' and record.get('exit_code')=='0:0'
        and record.get('user')=='pwa209' and record.get('account')=='def-ptewarie_cpu'
        and record.get('unallocated_metadata') is False and record.get('cpus')==1
        and type(record.get('elapsed_seconds')) is int and 0<record['elapsed_seconds']<=8*3600
        and 'gpu' not in record.get('alloc_tres','').lower(),'Positive owned completed one-record native probe required')
    completion=completed_native_dependency(ROOT,job);require(completion is not None,'Persisted positive one-record afterok proof required')
    old_area=ROOT/'maintenance/study-behaviour-adjoint-continuation-v1-20261024'
    old_support=ROOT/'maintenance/behaviour-full-record-adjoint-v5prefix-support-v1-20261024'
    old_case=ROOT/'maintenance/behaviour-full-record-adjoint-v5prefix-v1-20261024'
    manifest_sha='8a409623d93ede64b5f980dab2160bda0745ebd240a3255e70498d26cc34d794'
    helper_sha='459e0aa542eabd824dfe6f9deacf5e0e702b0108e5816760024c155823e1bb2f'
    receipt_sha='76126379b9f292d2df77779223af3e947d40e81222ed7bb24c18d4230d1315c7'
    bundle=bound_json(old_area/'source-manifest.json',manifest_sha)
    require(bundle.get('complete_native_qualification_tests')==131 and isinstance(bundle.get('files'),dict),
        'Exact previous native-qualified source bundle required')
    for name,pin in bundle['files'].items():
        require(Path(name).name==name and digest(ordinary(old_area/name))==pin,'Previous probe source bytes changed')
    receipt=bound_json(old_support/'receipt.json',receipt_sha)
    require(receipt.get('status')=='complete_one_record_first_order_probe' and receipt.get('native_job_id')==job
        and receipt.get('worker_returncode')==0 and receipt.get('source_sha256')==SOURCE
        and receipt.get('primary_source_sha256')==PRIMARY_SHA and receipt.get('full_config_sha256')==CONFIG
        and all(receipt.get(k) is False for k in FALSE_FLAGS),'Exact honest completed one-record receipt required')
    private=receipt.get('private_qualification',{})
    require(private.get('candidate_sha256')==helper_sha and private.get('total_tests')==131
        and private.get('all_numerical_definition_bytes_preserved') is True,'Actual131-case prior qualification required')
    allocation=receipt['allocation'];plan_path=Path(allocation['plan_path']);ledger_path=Path(allocation['ledger_path'])
    require(plan_path==ROOT/'plans/behaviour-adjoint-continuation-v1-20261024/plan.json'
        and ledger_path==ROOT/'state/behaviour-adjoint-continuation-v1-20261024-submission.json',
        'Exact previous probe registration namespace required')
    plan=registered_json(plan_path);ledger=registered_json(ledger_path)
    canonical=lambda v:hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
    require(canonical(plan)==allocation['plan_sha256']==ledger['plan_sha256']
        and digest(ledger_path)==allocation['ledger_sha256'] and plan.get('source_manifest_sha256')==manifest_sha
        and len(plan.get('tasks',[]))==1,'Exact source-bound previous native plan and ledger required')
    task=plan['tasks'][0];entry=ledger['tasks'].get(task['name'],{})
    script=ordinary(task['script'],maintenance=False)
    require(task['name']=='behaviour-adjoint-continuation-v1' and entry.get('job_id')==job
        and entry.get('state')=='submitted' and task.get('resources')=={'cpus':1,'memory':'64G','hours':8,'gpu':False,'array':None}
        and canonical(script.read_text())==task['script_sha256']==entry.get('script_sha256')
        and digest(script)==allocation['script_sha256']
        and '--source-manifest-sha256 '+manifest_sha in task['command'],
        'Previous native job must bind the reviewed exact script and source bundle')
    legacy=load_exact('completed_one_record_validator',old_area/'run-full-record-adjoint-v5prefix-v1-20261024.py',helper_sha)
    legacy.validate_worker_result(receipt['worker_result'],old_case)
    return {'job_id':job,'native_observed':record,'completion':completion,
        'receipt':{'path':str(old_support/'receipt.json'),'sha256':receipt_sha},
        'source_manifest':{'path':str(old_area/'source-manifest.json'),'sha256':manifest_sha},
        'helper_sha256':helper_sha,'native_qualified_cases':131,'artifacts_deserialized':False,**FALSE_FLAGS}


def census_qualification_bridge(schedule,qualification,end,*,process_factory=None,wall=time.time):
    """Run only the original census metadata validator in a separate 4f process."""
    evidence=ordinary(SUPPORT/'census-provider-verification',fresh=True);evidence.mkdir(mode=0o700)
    capsule=Path(__file__).resolve().parent;name='verify-census-provider-v1-20261024.py'
    require(isinstance(CAPSULE_MANIFEST,dict) and name in CAPSULE_MANIFEST['files'],'Pinned separate census bridge required')
    bridge=ordinary(capsule/name);bridge_sha=CAPSULE_MANIFEST['files'][name]
    require(digest(bridge)==bridge_sha,'Separate census bridge source changed')
    source=capsule/'source-manifest.json'
    request={'schema_version':1,'schedule':schedule,'qualification':qualification,
        'source_manifest':{'path':str(source),'sha256':digest(source)},
        'context_helper':{'path':str(capsule/'run-full-cohort-continuation-context-v2-20261024.py'),'sha256':CONTEXT_SHA},
        'provider_helper':schedule['helper']}
    exclusive(evidence/'request.json',request);request_sha=digest(evidence/'request.json')
    require(type(end) in (int,float) and math.isfinite(end),'Finite metadata bridge deadline required')
    seconds=min(300.,end-wall()-600.);require(math.isfinite(seconds) and seconds>0,'Metadata bridge cleanup margin exhausted')
    framework=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
    command=[sys.executable,'-s','-u',str(bridge),'--request',str(evidence/'request.json'),
        '--request-sha256',request_sha,'--response',str(evidence/'response.json'),'--self-sha256',bridge_sha,
        '--allocation-end',str(end)]
    environment=os.environ.copy();environment.update(PYTHONPATH=str(framework/'src'),PYTHONNOUSERSITE='1',
        PYTHONDONTWRITEBYTECODE='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
    exclusive(evidence/'intent.json',{'command':command,'maximum_seconds':seconds,'framework':str(framework),
        'isolated_package_environment':True,'additional_retries':0,**FALSE_FLAGS})
    factory=subprocess.Popen if process_factory is None else process_factory
    with (evidence/'stdout.txt').open('xb') as stdout,(evidence/'stderr.txt').open('xb') as stderr:
        process=factory(command,stdout=stdout,stderr=stderr,env=environment,cwd=str(framework),start_new_session=True,close_fds=True)
        try:code=process.wait(timeout=seconds)
        except BaseException as error:
            termination=stop(process)
            exclusive(evidence/'process-result.json',{'status':'metadata_bridge_failed_no_retry','error_type':type(error).__name__,
                'error':str(error),'termination':termination,**FALSE_FLAGS});raise
    exclusive(evidence/'process-result.json',{'status':'metadata_bridge_process_returned','returncode':code,
        'positive_reaped_exit':type(code) is int,'additional_retries':0,**FALSE_FLAGS})
    require(type(code) is int and code==0,'Separate original4f census metadata validator failed')
    require(all((evidence/name).stat().st_size<=2_000_000 for name in ('stdout.txt','stderr.txt','response.json')),
        'Bounded separate census metadata outputs required')
    response_sha=digest(evidence/'response.json');result=bound_json(evidence/'response.json',response_sha)
    require(result.get('schema_version')==1 and type(result['schema_version']) is int
        and result.get('status')=='complete_exact_4f_census_qualification_metadata_bridge'
        and result.get('request_sha256')==request_sha and result.get('bridge_sha256')==bridge_sha
        and result.get('source_manifest')==request['source_manifest'] and result.get('provider_helper')==schedule['helper']
        and result.get('source_identity',{}).get('package_path')==str(framework/'src/horsehair/__init__.py')
        and result['source_identity']['framework']['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
        and result['source_identity']['primary']['sha256']==PRIMARY_SHA and result['source_identity']['config_sha256']==CONFIG
        and all(result.get(flag) is False for flag in (*FALSE_FLAGS,'numerical_work_executed','canonical_state_changed','artifacts_deserialized')),
        'Exact independent4f origin/source/request-bound metadata proof required')
    require(isinstance(result.get('checked_paths'),dict)
        and result.get('qualification',{}).get('checked_paths')==result['checked_paths'],'Complete original validator proof required')
    require(all(digest(ordinary(path,maintenance=False))==pin for path,pin in result['checked_paths'].items()),
        'Separate census metadata evidence changed')
    require(digest(bridge)==bridge_sha and digest(source)==request['source_manifest']['sha256']
        and digest(evidence/'request.json')==request_sha,'Parent source or bridge request changed during verification')
    return {'checked_paths':result['checked_paths'],'separate_4f_bridge':result,
        'response':{'path':str(evidence/'response.json'),'sha256':response_sha},**FALSE_FLAGS}


def census_provider_evidence(schedule_path,schedule_sha,job,native_reader=None,*,receipt_sha=None,qualification_verifier=None):
    """Bind the new first producer; existing current-census admission is unchanged."""
    from horsehair.slurm import completed_native_dependency
    from horsehair.core import canonical_hash
    source_area=ROOT/'maintenance/study-census-resilience-support-v1-20261024'
    phase='study-census-resilience-v1-20261024'
    helper_name='run-fresh-storage-census-resilience-v1-20261024.py'
    helper_sha='94ac29d11da50412ba3c1f9269ed5762224f93635cd1fae505bfabc5f9b63a93'
    provider_manifest_sha='dbe6c589a7dda4e8d54cc041ddee1dfa77f7804d67e290efcd396c7108b6f152'
    require(re.fullmatch('[1-9][0-9]*',str(job)) and str(job) not in
        {'6865674','6942117','6689965','6943757','6975379'},'Explicit new first census provider identity required')
    schedule=bound_json(schedule_path,schedule_sha)
    require(set(schedule)=={'plan','ledger','helper','source_manifest','qualification','case_root'}
        and schedule['helper']=={'path':str(source_area/helper_name),'sha256':helper_sha}
        and schedule['source_manifest']=={'path':str(source_area/'source-manifest.json'),'sha256':provider_manifest_sha}
        and schedule['plan']['path']==str(ROOT/'plans'/phase/'plan.json')
        and schedule['ledger']['path']==str(ROOT/'state'/(phase+'-submission.json'))
        and schedule['case_root']==str(ROOT/'maintenance/study-census-resilience-v1-20261024'),
        'Exact source-bound new resilience schedule required')
    producer=load_exact('whole_cohort_completed_census_provider',ordinary(schedule['helper']['path']),schedule['helper']['sha256'])
    manifest,pins=producer.source_manifest(Path(schedule['source_manifest']['path']),schedule['source_manifest']['sha256'])
    require(manifest.get('kind')=='census-resilience-source-manifest-v1','Separately qualified new provider source closure required')
    require(all(digest(ordinary(schedule[key]['path'],maintenance=False))==schedule[key]['sha256']
        for key in ('plan','ledger')),'Pinned actual provider registration metadata required')
    plan=registered_json(schedule['plan']['path']);ledger=registered_json(schedule['ledger']['path'])
    require(len(plan.get('tasks',[]))==64 and ledger.get('plan_sha256')==canonical_hash(plan)
        and plan.get('config_sha256')==CONFIG and ledger.get('source_sha256')==producer.SOURCE,
        'Exact complete provider plan and confirmed source-bound ledger required')
    task=plan['tasks'][0]
    require(ledger.get('tasks',{}).get(task['name'],{}).get('job_id')==str(job),
        'Only the actually registered first new provider may gate this retry')
    registration,_=producer.registered_task(schedule['plan']['path'],schedule['ledger']['path'],task['name'],task['script'],str(job),
        hours=6,helper=Path(schedule['helper']['path']))
    case=Path(schedule['case_root'])/('native-'+str(job));receipt_path=ordinary(case/'receipt.json')
    receipt_sha=digest(receipt_path) if receipt_sha is None else receipt_sha
    receipt=bound_json(receipt_path,receipt_sha)
    require(receipt.get('status')=='complete_exact_size_sweep_with_original_conservative_freshness'
        and receipt.get('native_job_id')==str(job) and receipt.get('source_sha256')==producer.SOURCE
        and receipt.get('config_sha256')==CONFIG and receipt.get('source_manifest')==schedule['source_manifest']
        and receipt.get('qualification')==schedule['qualification'] and receipt.get('provider_helper')==schedule['helper']
        and receipt.get('wrapper_sha256')==schedule['helper']['sha256']
        and receipt.get('allocation',{}).get('native_registration')==registration
        and all(receipt.get(flag) is False for flag in producer.FALSE_FLAGS),
        'Positive exact completed first-provider receipt and original false gates required')
    sweep=receipt.get('sweep',{})
    require(sweep.get('path')==str(case/'sweep-result.json'),'Exact new provider sweep receipt required')
    bound_json(sweep['path'],sweep['sha256'])
    completion=completed_native_dependency(ROOT,str(job));row=(completion or {}).get('native_records',{}).get(str(job),{})
    require(row.get('state')=='COMPLETED' and row.get('exit_code')=='0:0'
        and type(row.get('cpus')) is int and row['cpus']==8 and row.get('gpus')==0
        and type(row.get('elapsed_seconds')) is int and 0<row['elapsed_seconds']<=6*3600,
        'Positive finalized actual first-provider eight-CPU completion required')
    checked=dict(pins);checked[str(Path(schedule_path))]=schedule_sha
    for key in ('plan','ledger','helper','source_manifest','qualification'):
        checked[schedule[key]['path']]=schedule[key]['sha256']
    checked[str(receipt_path)]=receipt_sha;checked[sweep['path']]=sweep['sha256']
    observed=None;qualification=None
    if native_reader is not None:
        require(callable(qualification_verifier),'Separate original4f qualification verifier required')
        qualification=qualification_verifier(schedule,schedule['qualification'])
        checked.update(qualification['checked_paths'])
        records=native_reader([str(job)],{},owner='pwa209',account='def-ptewarie_cpu');observed=records.get(str(job),{})
        require(set(records)=={str(job)} and observed.get('job_id_raw')==str(job)
            and observed.get('state')=='COMPLETED' and observed.get('exit_code')=='0:0'
            and observed.get('user')=='pwa209' and observed.get('account')=='def-ptewarie_cpu'
            and observed.get('unallocated_metadata') is False and observed.get('cpus')==8
            and observed.get('elapsed_seconds')==row['elapsed_seconds']
            and 'gpu' not in observed.get('alloc_tres','').lower(),
            'Fresh exact owned first-provider native completion required before fixtures or probe')
    require(all(digest(ordinary(path,maintenance=False))==pin for path,pin in checked.items()),
        'New provider source, registration or evidence changed during verification')
    return {'job_id':str(job),'schedule':{'path':str(Path(schedule_path)),'sha256':schedule_sha},
        'receipt':{'path':str(receipt_path),'sha256':receipt_sha},'sweep':sweep,
        'source_manifest':schedule['source_manifest'],'completion':completion,'native_observed':observed,
        'fresh_native_and_full_qualification_verified':native_reader is not None,
        'qualification':qualification,'first_provider_completion_only':True,
        'current_census_storage_admission_unmodified':True,'artifacts_deserialized':False,**FALSE_FLAGS}



def _worker(primary,framework,root,case,deadline,prefix_info_path,prefix_info_sha):
    import torch
    import horsehair
    from horsehair import semimarkov as s,behaviour as b
    from horsehair.core import atomic_json
    primary,framework,root,case=(Path(p).resolve() for p in (primary,framework,root,case))
    require(Path(horsehair.__file__).resolve()==primary/'src/horsehair/__init__.py'
        and all(Path(module.__file__).resolve().is_relative_to(primary/'src') for module in (s,b)), 'Frozen isolated primary interpreter required')
    config=source_bindings(primary,framework)
    require(root.resolve()==ROOT.resolve()==Path(config['paths']['root']).resolve() and case.resolve()==CASE.resolve()
        and case.is_dir() and not case.is_symlink(),'Exact personal native case/root required')
    require(os.environ.get('SLURM_CPUS_PER_TASK')=='1' and os.environ.get('HHW_GPU_COUNT')=='0','One CPU and no GPU required')
    torch.set_num_threads(1)
    adjoint=load_exact('horsehair._native_duration_adjoint',framework/'src/horsehair/duration_adjoint.py',ADJOINT_SHA)
    profile=load_exact('horsehair._native_cohort_profile',framework/'src/horsehair/cohort_profile.py',PROFILE_SHA)
    progress={'status':'preprocessing','torch_threads':1,'torch_version':str(torch.__version__),'full_config_sha256':CONFIG,
        'primary_source_sha256':PRIMARY_SHA,'framework_source_sha256':SOURCE,'completed_stages':[],**FALSE_FLAGS}
    last=[None]
    def emit(event,force=False):
        progress['last_event']=event;progress['current_rss_bytes']=rss_bytes();progress['updated_unix']=time.time()
        if force or last[0] is None or time.monotonic()-last[0]>=30:
            atomic_json(case/'progress.json',progress);last[0]=time.monotonic()
    def commit(stage,result,artifacts):
        require(stage==STAGES[len(progress['completed_stages'])],'Only complete sequential stage boundaries may be committed')
        checkpoint=case/(stage+'.pt');torch.save(artifacts,checkpoint)
        with checkpoint.open('rb') as stream:os.fsync(stream.fileno())
        value=dict(result,stage=stage,status='full_fold_technical_stage_complete',artifacts_path=str(checkpoint),
            artifacts_sha256=digest(checkpoint),plan_sha256=progress['plan_sha256'],
            exact_mid_operation_resume_supported=False,**FALSE_FLAGS)
        exclusive(case/(stage+'.json'),value);progress['completed_stages'].append(value)
        emit({'stage':stage,'state':'completed'},True)
    try:
        emit({'stage':'source_bound_worker_started'},True)
        prefix_info=bound_json(prefix_info_path,prefix_info_sha)
        require(prefix_info['helper']['sha256']==PREFIX_HELPER_SHA and prefix_info['original_stage1_artifact']['loaded'] is False
            and prefix_info['only_complete_stage1_is_required'] is True and all(prefix_info.get(k) is False for k in FALSE_FLAGS),
            'Exact parent-verified prefix preprocessing provenance required')
        bound_json(prefix_info['receipt']['path'],prefix_info['receipt']['sha256'])
        initial,training,normalizer,settings,plan=prepare(s,b,profile,config,root,case,deadline,emit,prefix_info)
        progress.update(status='full_training_fold_comparison_running',plan_sha256=digest(case/'plan.json'),
            training_clocks=len(training),training_bins=sum(len(q.times) for q in training),global_observed_bin_normalizer=normalizer)
        commit(STAGES[0],{'training_clocks':len(training),'training_bins':sum(len(q.times) for q in training),
            'global_observed_bin_normalizer':normalizer,'original_validation_criterion_defined':False,
            'all_original_roster_rows_retained':True,'original_stage1_tensor_artifact_loaded':False},
            {'initial_state':initial.state_dict(),'torch_rng_state':torch.get_rng_state(),'normalizer':normalizer})
        full_training_comparison(profile,adjoint,initial,training,normalizer,deadline,emit,commit)
        require(digest(Path(plan['source_bin_path']))==plan['source_bin_sha256']
            and digest(Path(plan['provenance_path']))==plan['provenance_sha256'],'Source bin/provenance changed during full-fold comparison')
        source_bindings(primary,framework);progress['status']='complete_full_training_fold_first_order_comparison'
    except (MemoryBound,DeadlineBound) as error:
        progress.update(status='memory_bound_not_compared' if isinstance(error,MemoryBound) else 'bounded_deadline_reached',
                        error_type=type(error).__name__,reason=str(error))
    except BaseException as error:
        progress.update(status='worker_failed',error_type=type(error).__name__,reason=str(error))
    progress.update(pending_stages=[stage for stage in STAGES if stage not in {r['stage'] for r in progress['completed_stages']}],
        process_cpu_seconds=time.process_time(),finished_unix=time.time(),memory_ceiling_bytes=MEMORY_LIMIT,
        exact_mid_operation_resume_supported=False,
        scope='Single frozen MX/two-state/10s training fold at initialization; no validation, optimization, inference or complete-workload proof')
    atomic_json(case/'worker-result.json',progress);emit({'stage':'worker_finished'},True)
    return 1 if progress['status']=='worker_failed' else 0


CHILD="""import importlib.util,pathlib,sys
primary,framework,helper,root,case,deadline,prefix_info,prefix_sha=sys.argv[1:]
sys.path.insert(0,str(pathlib.Path(primary)/'src'))
spec=importlib.util.spec_from_file_location('native_adjoint_probe',helper)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
raise SystemExit(module._worker(pathlib.Path(primary),pathlib.Path(framework),pathlib.Path(root),pathlib.Path(case),float(deadline),pathlib.Path(prefix_info),prefix_sha))
"""


def stop(process,*,term_seconds=10.,kill_seconds=20.,group_signal=None):
    """One finite teardown sequence; a signal request never proves termination."""
    require(all(type(x) in (int,float) and math.isfinite(x) and 0<x<=30 for x in (term_seconds,kill_seconds)),
        'Finite bounded teardown waits required')
    result={'signal_requests':[],'waits':[],'errors':[],'reaped':False,'returncode':None,
        'termination_confirmed':False,'additional_teardown_replay_permitted':False}
    def poll():
        try:return process.poll()
        except Exception as error:
            result['errors'].append({'operation':'poll','type':type(error).__name__,'error':str(error)});return None
    code=poll()
    if code is None:
        for name,timeout in [('SIGTERM',term_seconds),('SIGKILL',kill_seconds)]:
            try:
                if group_signal is not None:group_signal(process.pid,getattr(signal,name))
                elif os.name=='posix':os.killpg(process.pid,getattr(signal,name))
                elif name=='SIGTERM':process.terminate()
                else:process.kill()
                result['signal_requests'].append({'signal':name,'request_returned':True})
            except Exception as error:
                result['errors'].append({'operation':name,'type':type(error).__name__,'error':str(error)})
            try:
                code=process.wait(timeout=timeout);result['waits'].append({'maximum_seconds':timeout,'completed':True})
            except Exception as error:
                result['waits'].append({'maximum_seconds':timeout,'completed':False,'type':type(error).__name__,'error':str(error)})
                code=poll()
            if code is not None:break
    result.update(reaped=type(code) is int,returncode=code if type(code) is int else None,
        termination_confirmed=type(code) is int)
    return result


def validate_worker_result(result,case):
    require(result.get('status') in {'complete_full_training_fold_first_order_comparison','memory_bound_not_compared','bounded_deadline_reached','worker_failed'}
        and all(result.get(key) is False for key in FALSE_FLAGS)
        and result.get('primary_source_sha256')==PRIMARY_SHA and result.get('framework_source_sha256')==SOURCE
        and result.get('full_config_sha256')==CONFIG,'Valid source-bound honest technical worker receipt required')
    completed=result.get('completed_stages');require(isinstance(completed,list),'Explicit completed-stage roster required')
    names=[value.get('stage') for value in completed]
    require(names==list(STAGES[:len(names)]),'Partial completion must be an unambiguous original-stage prefix')
    if result['status']=='complete_full_training_fold_first_order_comparison':require(names==list(STAGES),'All four full-fold technical stages must actually complete')
    for value in completed:
        path=case/(value['stage']+'.json');artifact=case/(value['stage']+'.pt')
        require(value.get('status')=='full_fold_technical_stage_complete' and all(value.get(key) is False for key in FALSE_FLAGS)
            and value.get('training_clocks')==1107 and value.get('training_bins')==7828183
            and value.get('global_observed_bin_normalizer')==2666463
            and value.get('exact_mid_operation_resume_supported') is False
            and json.loads(path.read_text())==value and value.get('artifacts_path')==str(artifact)
            and digest(artifact)==value.get('artifacts_sha256') and digest(case/'plan.json')==value.get('plan_sha256'),
            'Actual completed full-fold stage bytes/coverage/plan/false gates must bind the worker result')


def supervise(command,environment,deadline,case,*,popen=subprocess.Popen,clock=time.monotonic,sleeper=time.sleep,rss=rss_bytes,stopper=None):
    result=None;reason=None;peak=0;supervisor_error=None;teardown=None
    with (case/'stdout.log').open('xb') as stdout,(case/'stderr.log').open('xb') as stderr:
        process=popen(command,env=environment,cwd=PRIMARY,stdout=stdout,stderr=stderr,start_new_session=os.name=='posix')
        try:
            while process.poll() is None:
                if clock()>=deadline:reason='bounded_deadline_reached';break
                try:used=rss(process.pid)
                except (FileNotFoundError,ProcessLookupError) as error:
                    if process.poll() is not None:break
                    # Linux may remove VmRSS before waitpid can reap an exiting child.
                    # Wait once for natural exit, bounded by the existing deadline.
                    remaining=deadline-clock()
                    if remaining<=0:reason='bounded_deadline_reached';break
                    try:
                        exit_code=process.wait(timeout=min(1.,remaining))
                    except subprocess.TimeoutExpired:
                        if clock()>=deadline:reason='bounded_deadline_reached';break
                        raise error
                    require(type(exit_code) is int,'Positive child reap evidence required after RSS disappeared')
                    break
                peak=max(peak,used)
                if used>=MEMORY_LIMIT:reason='memory_bound_not_compared';break
                sleeper(1)
        except Exception as error:
            reason='worker_failed';supervisor_error={'type':type(error).__name__,'error':str(error)}
        finally:
            teardown=(stop if stopper is None else stopper)(process)
        code=teardown['returncode']
    path=case/'worker-result.json'
    if path.is_file():result=json.loads(path.read_text());validate_worker_result(result,case)
    progress_path=case/'progress.json';progress=None
    if progress_path.is_file():
        body=json.loads(progress_path.read_text())
        if result is None and (reason in {'bounded_deadline_reached','memory_bound_not_compared'} or not teardown['reaped']):
            partial=dict(body,status=reason if reason in {'bounded_deadline_reached','memory_bound_not_compared','worker_failed'} else 'worker_failed')
            validate_worker_result(partial,case)
            # The original progress path may still change if a native child is
            # uninterruptible. Preserve a separate immutable observed value.
            snapshot=case/'supervisor-progress-snapshot.json';exclusive(snapshot,body)
            progress={'path':str(snapshot),'sha256':digest(snapshot),'value':body,
                'observed_worker_path':str(progress_path),'worker_path_may_continue_changing':not teardown['reaped']}
    if reason is None and (code!=0 or result is None):reason='worker_failed'
    if reason is None:
        reason=result['status']
    if not teardown['reaped']:reason='worker_termination_unconfirmed'
    return {'status':reason,'worker_returncode':code,'worker_result':result,'preserved_partial_progress':progress,
        'termination':teardown,'supervisor_error':supervisor_error,
        'requires_native_terminal_proof_before_consumption':not teardown['reaped'],'supervisor_peak_current_rss_bytes':peak,
        'maximum_child_deadline_monotonic':deadline,'memory_ceiling_bytes':MEMORY_LIMIT,**FALSE_FLAGS}


def verify_private_junit(path):
    from horsehair.core import canonical_hash
    from horsehair.cli import _qualification_junit
    raw=Path(path).read_bytes()
    require(len(raw)<=2_000_000 and b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Bounded plain private JUnit required')
    cases=list(ET.fromstring(raw).iter('testcase'))
    roster=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in cases)
    counts={'tests':PRIVATE_TEST_COUNT,'failures':0,'errors':0,'skipped':0}
    require(len(roster)==len(set(roster))==PRIVATE_TEST_COUNT and canonical_hash(roster)==PRIVATE_TEST_ROSTER_SHA
        and all(a=='test_full_record_adjoint_v3_20261021' and b for a,b in roster)
        and all(not any(c.find(k) is not None for k in ('failure','error','skipped')) for c in cases)
        and _qualification_junit(path)==counts,'Exact complete passing native adjoint v3 test roster required')
    return {'path':str(path),'sha256':digest(path),'test_counts':counts,'test_roster_sha256':PRIVATE_TEST_ROSTER_SHA}


def native_fixture_workspace():
    temp=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temp.is_absolute() and temp.is_dir() and temp==temp.resolve() and not temp.is_symlink()
        and temp.stat().st_uid==3164768 and shutil.disk_usage(temp).free>=3_000_000_000
        and os.statvfs(temp).f_favail>=5000,'Native local private fixture capacity required')
    workspace=temp/('hhw-adjoint-v3-tests-'+os.environ['SLURM_JOB_ID']);workspace.mkdir(mode=0o700,exist_ok=False)
    return workspace


def qualify_private(test_path,test_sha,allocation_end,*,runner=subprocess.run):
    """Qualify unchanged baseline plus operational derivative in separate suites."""
    require(CAPSULE_MANIFEST is not None,'Pinned candidate source capsule required')
    area=Path(__file__).resolve().parent
    legacy_path=ordinary(area/'run-full-record-adjoint-v3-20261021.py')
    legacy=load_exact('immutable_adjoint_v3_qualification',legacy_path,BASELINE_HELPER_SHA)
    require(digest(area/'run-full-cohort-prefix-v3-20261021.py')==BASELINE_PREFIX_SHA,
        'Immutable legacy fixture closure required')
    legacy.SUPPORT=SUPPORT
    baseline=legacy.qualify_private(test_path,test_sha,allocation_end,runner=runner)
    additional=[]
    for suite in CAPSULE_MANIFEST['operational_test_suites']:
        source=ordinary(area/suite['path'])
        require(digest(source)==CAPSULE_MANIFEST['files'][suite['path']], 'Pinned operational suite required')
        output=SUPPORT/suite['name'];output.mkdir(mode=0o700,exist_ok=False)
        workspace=Path(os.environ['SLURM_TMPDIR'])/('hhw-adjoint-'+suite['name']+'-'+os.environ['SLURM_JOB_ID'])
        workspace.mkdir(mode=0o700,exist_ok=False)
        environment={**os.environ,'HHW_TEST_WORKSPACE':str(workspace),'HHW_TEST_RESULT_DIR':str(output),
            'PYTHONDONTWRITEBYTECODE':'1','HHW_ADJOINT_BASELINE':str(legacy_path)}
        timeout=min(300.,allocation_end-time.time()-300.)
        require(timeout>=60,'Finite operational qualification margin required')
        command=[sys.executable,'-B',str(source)]
        exclusive(output/'intent.json',{'command':command,'source_sha256':digest(source),'maximum_seconds':timeout,**FALSE_FLAGS})
        with (output/'stdout.log').open('xb') as stdout,(output/'stderr.log').open('xb') as stderr:
            completed=runner(command,cwd=workspace,env=environment,stdout=stdout,stderr=stderr,timeout=timeout,check=False)
        result_path=output/'test-result.json'
        require(completed.returncode==0 and result_path.is_file(),'Actual complete operational native suite failed')
        result=bound_json(result_path,digest(result_path))
        require(result.get('tests')==suite['tests'] and result.get('failures')==0 and result.get('errors')==0
            and result.get('skipped')==0 and result.get('successful') is True
            and result.get('tests_sha256')==digest(source) and result.get('derivative_sha256')==digest(Path(__file__))
            and digest(source)==CAPSULE_MANIFEST['files'][suite['path']],
            'Exact complete operational native test roster required')
        additional.append({'suite':suite['name'],'path':str(result_path),'sha256':digest(result_path),
            'test_source_sha256':digest(source),'tests':suite['tests'],'failures':0,'errors':0,'skipped':0})
    for name,pin in CAPSULE_MANIFEST['files'].items():
        require(digest(area/name)==pin,'Candidate capsule changed during native qualification')
    result={'legacy_numerical_baseline':baseline,'operational_derivative_suites':additional,
        'candidate_sha256':digest(Path(__file__)),'all_numerical_definition_bytes_preserved':True,
        'total_tests':baseline['test_counts']['tests']+sum(x['tests'] for x in additional),**FALSE_FLAGS}
    exclusive(SUPPORT/'candidate-private-qualification.json',result)
    return result



ORIGINAL_JOB_ID='6865674'

EXPECTED_MANIFEST_SHA='5b5e276e8c394fda765d3d6183bc8dce47e4a626123f4607f722fa512fef0e9a'

EXPECTED_CANONICAL_SHA='b3be0c3340c80864407b10b34ab5a9b324330a0b7647d8ae6374ae64b180f7fd'

VERIFIER_SOURCE=ROOT/'maintenance/study-behaviour-supervisor-reconciliation-source-v1-20261024'

VERIFIER_OUTPUT=ROOT/'maintenance/study-behaviour-supervisor-reconciliation-v1-20261024'

VERIFIER_SOURCE_MANIFEST_SHA=None

CAPSULE_MANIFEST=None

BASELINE_HELPER_SHA='815292c4323a03eb1e9cd83499afd9ca3adb0eec8cdcecb56913fb05f79c3ea1'

BASELINE_PREFIX_SHA='b131d6317858eae3c43c2ae7337f46e3173ab64355693980af2e7e489af46d3d'

def adjoint_runtime_seconds(allocation_end,*,now=None):
    """Use only a verified24h allocation with ten minutes reserved for cleanup."""
    current=time.time() if now is None else now
    require(type(current) in (int,float) and math.isfinite(current),'Finite actual allocation clock required')
    require(type(allocation_end) in (str,int,float),'Finite native allocation end required')
    end=float(allocation_end)
    require(math.isfinite(end) and 630<=end-current<=24*3600+2,'Verified24h allocation and cleanup margin required')
    return min(85800.,end-current-600.)

def verify_capsule(sha):
    """Bind all local code and fixtures to the immutable native script argument."""
    area=Path(__file__).resolve().parent
    manifest=bound_json(area/'source-manifest.json',sha)
    require(manifest.get('schema_version')==1 and isinstance(manifest.get('files'),dict),
        'Explicit source bundle required')
    for name,pin in manifest['files'].items():
        require(isinstance(name,str) and Path(name).name==name and re.fullmatch('[0-9a-f]{64}',pin),
            'Flat pinned source bundle required')
        path=ordinary(area/name)
        require(path.parent==area and digest(path)==pin,'Source bundle changed: '+name)
    require(manifest['files'].get(Path(__file__).name)==digest(Path(__file__)),
        'Native script must bind this exact candidate source')
    return manifest

def original_file_bindings(expected):
    """Recheck frozen files and opaque artifact hashes without deserializing."""
    evidence={}
    for relative,pin in expected['pinned_files'].items():
        require(not Path(relative).is_absolute() and '..' not in Path(relative).parts,
            'Exact root-relative original file required')
        path=ordinary(ROOT/relative,maintenance=False)
        require(path.is_relative_to(ROOT.resolve()) and digest(path)==pin,
            'Original file changed: '+relative)
        evidence[relative]=pin
    for absolute,pin in expected['artifact_pins'].items():
        path=ordinary(absolute)
        require(path.is_relative_to(PREFIX_CASE.resolve()) and digest(path)==pin,
            'Original opaque artifact changed')
        evidence[str(path.relative_to(ROOT.resolve()))]=pin
    return evidence

def registered_json(path):
    """Read confined plan/state metadata with the original strict JSON rules."""
    path=ordinary(path,maintenance=False)
    raw=path.read_bytes()
    require(len(raw)<=20_000_000,'Bounded registered metadata required')
    def unique(pairs):
        value={}
        for key,item in pairs:
            require(key not in value,'Duplicate registration key');value[key]=item
        return value
    value=json.loads(raw,object_pairs_hook=unique,parse_constant=lambda x:(_ for _ in ()).throw(ValueError('Nonfinite metadata refused')))
    require(digest(path)==hashlib.sha256(raw).hexdigest(),'Registered metadata changed during read')
    return value

def verifier_registration(job,source_sha):
    """Link positive native completion to the frozen verifier launch itself."""
    plan_path=ROOT/'plans/behaviour-reconciliation-v1-20261024/plan.json'
    ledger_path=ROOT/'state/behaviour-reconciliation-v1-20261024-submission.json'
    started_path=VERIFIER_OUTPUT/('native-'+job)/'started.json'
    started=bound_json(started_path,digest(started_path))
    plan=registered_json(plan_path);ledger=registered_json(ledger_path)
    canonical=lambda value:hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
    require(len(plan.get('tasks',[]))==1,'Exact one-job verifier plan required')
    task=plan['tasks'][0];registered=ledger.get('tasks',{}).get('behaviour-reconciliation-v1',{})
    require(task.get('name')=='behaviour-reconciliation-v1' and registered.get('job_id')==job
        and registered.get('state')=='submitted' and ledger.get('plan_sha256')==canonical(plan)
        and plan.get('config_sha256')==CONFIG and plan.get('release_identity',{}).get('sha256')==SOURCE
        and ledger.get('source_sha256')==SOURCE and ledger.get('config_sha256')==CONFIG,
        'Verifier native completion must belong to its exact source-bound registered plan')
    script=ordinary(task.get('script',''),maintenance=False)
    require(script.parent==plan_path.parent and task.get('script_sha256')==canonical(script.read_text())
        and registered.get('script_sha256')==task['script_sha256'],
        'Verifier script changed after native registration')
    text=script.read_text()
    require(str(VERIFIER_SOURCE/'run_native_reconciliation.py') in text
        and re.search(r'(?:^|\s)--source-manifest-sha256\s+[\'\"]?'+source_sha+r'[\'\"]?(?:\s|$)',text),
        'Verifier native script must pin the exact reviewed source bundle')
    allocation=started.get('allocation',{});fields=allocation.get('fields',{})
    require(started.get('native_job_id')==job and started.get('source_manifest_sha256')==source_sha
        and allocation.get('plan_sha256')==digest(plan_path) and allocation.get('ledger_sha256')==digest(ledger_path)
        and fields.get('JobId')==job and fields.get('JobState')=='RUNNING'
        and fields.get('UserId')=='pwa209(3164768)' and fields.get('Account')=='def-ptewarie_cpu'
        and fields.get('NumCPUs')=='4' and fields.get('MinMemoryNode')=='16G'
        and fields.get('TimeLimit') in {'02:00:00','2:00:00','0-02:00:00'}
        and fields.get('Command')==str(script),
        'Verifier started evidence must bind its actual registered allocation')
    return {'started':{'path':str(started_path),'sha256':digest(started_path)},
        'plan':{'path':str(plan_path),'sha256':digest(plan_path)},
        'ledger':{'path':str(ledger_path),'sha256':digest(ledger_path)},
        'script':{'path':str(script),'sha256':digest(script)}}

def main():
    parser=argparse.ArgumentParser()
    for name in ('qualification','resource-policy','native-script','context-helper','prefix-receipt','tests','census-provider-schedule'):parser.add_argument('--'+name,type=Path,required=True)
    for name in ('qualification-sha256','qualification-job-id','resource-policy-sha256','context-helper-sha256','prefix-receipt-sha256','prefix-job-id','tests-sha256','census-provider-job-id','census-provider-schedule-sha256','census-provider-receipt-sha256'):parser.add_argument('--'+name,required=True)
    parser.add_argument('--source-manifest-sha256',required=True)
    parser.add_argument('--verifier-source-manifest-sha256',required=True)
    args=parser.parse_args()
    global CAPSULE_MANIFEST,VERIFIER_SOURCE_MANIFEST_SHA
    CAPSULE_MANIFEST=verify_capsule(args.source_manifest_sha256)
    VERIFIER_SOURCE_MANIFEST_SHA=args.verifier_source_manifest_sha256
    require(re.fullmatch('[0-9a-f]{64}',VERIFIER_SOURCE_MANIFEST_SHA),'Pinned verifier bundle SHA required')
    scheduler_utc()
    require(args.context_helper_sha256==CONTEXT_SHA,'Exact reviewed operational context helper required')
    ordinary(args.context_helper);context=load_exact('reviewed_profile_native_context',args.context_helper,CONTEXT_SHA)
    require(context.FRAMEWORK==FRAMEWORK and context.SOURCE==SOURCE and context.PRIMARY_SHA==PRIMARY_SHA,'Operational context source mismatch')
    # Only operational evidence/result namespaces change; the derivative context changes only owned24h allocation validation;
    # qualification, accounting, storage and numerical kernels retain their pins.
    context.CASE,context.SUPPORT=CASE,SUPPORT
    job=str(context.native_job_id() or '')
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='1' and os.environ.get('HHW_GPU_COUNT')=='0'
        and os.environ.get('HHW_MAX_HOURS')=='24','Owned finite1CPU/24-hour native full-fold context required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval context required')
    import horsehair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py','Qualified maintenance package required')
    config=source_bindings(PRIMARY,FRAMEWORK)
    require(args.resource_policy==context.POLICY and args.resource_policy_sha256==context.POLICY_SHA,'Exact explicit qualified source policy required')
    policy=context.load_resource_policy(args.resource_policy,args.resource_policy_sha256,root=ROOT,config=config)
    ordinary(CASE,fresh=True);ordinary(SUPPORT,fresh=True)
    for path,sha in [(context.GUARD,context.GUARD_SHA),(context.SCHEDULER,context.SCHEDULER_SHA),(context.QUOTA,context.QUOTA_SHA)]:
        require(digest(path)==sha,'Pinned native read helper changed')
    SUPPORT.mkdir(mode=0o700)
    import runpy
    runpy.run_path(str(context.GUARD))['install'](SUPPORT/'accounting-reads')
    reader=runpy.run_path(str(context.SCHEDULER))['run_scontrol_job']
    allocation=context.native_allocation(job,args.native_script,reader)
    from horsehair import repair
    repair._repair_native_states=context.bounded_native_reader(repair._repair_native_states,
        SUPPORT/'bounded-native-reads',allocation['allocation_end_unix'])
    from horsehair import campaign
    require(campaign._repair_native_states is repair._repair_native_states,'Bounded accounting must precede campaign import')
    from horsehair.core import ReceiptLockTimeout
    try:
        _,registration=shared_admission(lambda:campaign.combined_admission(ROOT,config,
            context.read_json(PRIMARY/'release.json')['release_id'],SUPPORT,'full-fold-first-order-technical',
            resource_policy=policy,cpus=0,gpus=0,hours=24),allocation['allocation_end_unix'],evidence_dir=SUPPORT/'registration-wait')
    except (ReceiptLockTimeout,DeadlineBound) as error:
        if isinstance(error,ReceiptLockTimeout):
            require(type(error) is ReceiptLockTimeout and Path(error.path)==(ROOT/'state/af-campaign-registration.mutex').resolve(),
                'Only exact shared registration-lock contention may remain pending')
        exclusive(SUPPORT/'receipt.json',{'status':'pending_registration_no_probe_started','reason':str(error),'native_job_id':job,
            'allocation':allocation,'resource_policy':policy.receipt(),'source_sha256':SOURCE,'full_config_sha256':CONFIG,
            'qualification_not_yet_validated_by_probe_wrapper':{'path':str(args.qualification),'sha256':args.qualification_sha256},
            'root_registered_native_accounting_remains_required':True,**FALSE_FLAGS})
        return 0
    observed=repair._repair_native_states(sorted({args.qualification_job_id,args.prefix_job_id,ORIGINAL_JOB_ID,'6943757',args.census_provider_job_id}),{},owner='pwa209',account='def-ptewarie_cpu')
    qualification=context.qualification_proof(args.qualification,args.qualification_sha256,args.qualification_job_id,observed)
    previous_probe=record_probe_evidence(observed)
    prefix_info=prefix_evidence(args.prefix_receipt,args.prefix_receipt_sha256,args.prefix_job_id,observed)
    provider_info=census_provider_evidence(args.census_provider_schedule,args.census_provider_schedule_sha256,
        args.census_provider_job_id,repair._repair_native_states,receipt_sha=args.census_provider_receipt_sha256,
        qualification_verifier=lambda schedule,qualification:census_qualification_bridge(schedule,qualification,allocation['allocation_end_unix']))
    with context.resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=24,job_id=job,resource_policy=policy):
        quota=runpy.run_path(str(context.QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,
            required_bytes=10_000_000_000,reserve_files=248000,reserve_bytes=2_000_000_000_000,
            evidence_dir=SUPPORT/'quota-reads',allocation_deadline_unix=allocation['allocation_end_unix'])
        try:storage=context.storage_proof(config,quota)
        except context.PendingStorage as error:
            exclusive(SUPPORT/'receipt.json',{'status':'pending_storage_no_probe_started','reason':str(error),'native_job_id':job,**FALSE_FLAGS});return 0
        private_qualification=qualify_private(args.tests,args.tests_sha256,allocation['allocation_end_unix'])
        exclusive(SUPPORT/'prefix-evidence.json',prefix_info)
        exclusive(SUPPORT/'input-bindings.json',{'allocation':allocation,'qualification':qualification,'storage':storage,'resource_policy':policy.receipt(),
            'prefix_evidence':prefix_info,'previous_one_record_probe':previous_probe,'new_census_provider':provider_info,
            'private_qualification':private_qualification,
            'registration_protection':registration,
            'context_helper':{'path':str(args.context_helper),'sha256':CONTEXT_SHA,'operational_rebinding':{'CASE':str(CASE),'SUPPORT':str(SUPPORT)}},
            'adjoint_sha256':ADJOINT_SHA,'profile_sha256':PROFILE_SHA,'helper_sha256':digest(Path(__file__)),**FALSE_FLAGS})
        from horsehair.rna_recovery import _primary_environment
        seconds=adjoint_runtime_seconds(os.environ['SLURM_JOB_END_TIME']);deadline=time.monotonic()+seconds
        CASE.mkdir(mode=0o700)
        environment=_primary_environment({'primary_release':str(PRIMARY),'maintenance_release':str(FRAMEWORK),'root':str(ROOT)})
        environment.update(OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
        command=[sys.executable,'-s','-u','-c',CHILD,str(PRIMARY),str(FRAMEWORK),str(Path(__file__).resolve()),str(ROOT),str(CASE),str(deadline),
            str(SUPPORT/'prefix-evidence.json'),digest(SUPPORT/'prefix-evidence.json')]
        exclusive(CASE/'started.json',{'command':command,'maximum_worker_seconds':seconds,'native_job_id':job,
            'primary_source_sha256':PRIMARY_SHA,'framework_source_sha256':SOURCE,'full_config_sha256':CONFIG,'helper_sha256':digest(Path(__file__)),**FALSE_FLAGS})
        result=supervise(command,environment,deadline,CASE)
        source_bindings(PRIMARY,FRAMEWORK)
        prefix_evidence(args.prefix_receipt,args.prefix_receipt_sha256,args.prefix_job_id,observed)
        require(digest(args.context_helper)==CONTEXT_SHA and digest(args.qualification)==args.qualification_sha256
            and digest(args.resource_policy)==args.resource_policy_sha256,'Prerequisite changed during paired probe')
        verify_capsule(args.source_manifest_sha256)
        result.update(native_job_id=job,case=str(CASE),qualification=qualification,allocation=allocation,
            prefix_evidence=prefix_info,previous_one_record_probe=previous_probe,new_census_provider=provider_info,
            private_qualification=private_qualification,
            preprocessing_recomputed_not_restored=True,numerical_scope='One complete frozen training fold at initialization',
            source_sha256=SOURCE,primary_source_sha256=PRIMARY_SHA,full_config_sha256=CONFIG,resource_policy=policy.receipt(),
            pending=['Native full-design timing/admission remains unqualified','Other models/folds/starts/rates/bin widths','Full optimizer convergence/LBFGS',
                     'Three-state27-context sensitivity','GPU/occupancy/PIT/Hessian/MALA/recovery/scientific admission'])
        exclusive(CASE/'receipt.json',result);exclusive(SUPPORT/'receipt.json',result)
        print(json.dumps({'status':result['status'],'native_job_id':job,'receipt':str(SUPPORT/'receipt.json'),**FALSE_FLAGS},sort_keys=True))
        return 1 if result['status'] in {'worker_failed','worker_termination_unconfirmed'} else 0


if __name__=='__main__':raise SystemExit(main())
