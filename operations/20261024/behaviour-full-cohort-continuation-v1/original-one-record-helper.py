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
CONTEXT_SHA='e055352de535d029c658723b69707c371ab5d89e451cace506d305c344a3fd0f'
PREFIX_HELPER_SHA='dec3ace9591b71759bbd7418b550e657ecb87a85823652e9d1e56547fe19ec60'
PREFIX_TEST_SHA='874cfce84a7814536e6442a00775d7ad3b652ef7ad12a087e81585edd0b88169'
PREFIX_CASE=ROOT/'maintenance/behaviour-full-cohort-prefix-v5-20261022'
PREFIX_SUPPORT=ROOT/'maintenance/behaviour-full-cohort-prefix-v5-support-20261022'
PREFIX_STAGE='full_fold_preprocessing_and_eligibility_census'
ADJOINT_SHA='5ff03ba506d4ad8dab66dd51def1c728aade96a8af77661be03de20e40a205f9'
PROFILE_SHA='e60561b425d500d08690675d98e4f86b2617044e446183433cdf3e4465602e59'
CASE=ROOT/'maintenance/behaviour-full-record-adjoint-v5prefix-v1-20261024'
SUPPORT=ROOT/'maintenance/behaviour-full-record-adjoint-v5prefix-support-v1-20261024'
MEMORY_LIMIT=48*1024**3
PRIVATE_TEST_COUNT=72
PRIVATE_TEST_ROSTER_SHA='78847872afe3396dbcbee0312a3c9a07af5f782e862d51b1d103618d57aba420'
FALSE_FLAGS={'scientific_result':False,'scientific_fit':False,'complete_declared_workload':False,
             'admission_authority':False,'original_H1_H2_H3_gates_cleared':False,
             'native_gpu_qualified':False,'second_derivatives_supported':False,
             'whole_cohort_objective_qualified':False,'optimizer_convergence_qualified':False,
             'MALA_acceptance_qualified':False}
STAGES=('training_full_clock_scores','validation_full_quadrature_scores',
        'training_component_all_gradients_and_one_Adam_update','validation_full_quadrature_all_gradients')


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
    import torch
    require(prefix_info is not None,'Completed source-bound prefix preprocessing evidence is required')
    require(str(torch.__version__)==prefix_info['runtime_torch_version'],'Original and recomputed Torch runtimes differ')
    prefix=load_exact('qualified_prefix_recomputation',ordinary(prefix_info['helper']['path']),PREFIX_HELPER_SHA)
    original=bound_json(prefix_info['plan']['path'],prefix_info['plan']['sha256'])
    original_census=bound_json(prefix_info['census']['path'],prefix_info['census']['sha256'])
    try:model,train,validation,scaler,n,settings,recomputed=prefix.prepare(s,b,profile,config,root,case,deadline,emit)
    except prefix.MemoryBound as error:raise MemoryBound(str(error)) from error
    except prefix.DeadlineBound as error:raise DeadlineBound(str(error)) from error
    # Every numerical and structural preprocessing field must match exactly.
    require(recomputed==original and bound_json(case/'clock-eligibility-census.json',recomputed['census_sha256'])==original_census,
        'Recomputed full-fold scaler/knots/roles/normalizer or clock census differs from bound prefix')
    selected_training=select_clock(recomputed['roster'],recomputed['split']['training_ids'],salt='adjoint-v1:training:')
    selection=prefix.prospective_validation_canary_v2(original_census['rows'],recomputed['split']['validation_ids'])
    (case/'plan.json').rename(case/'recomputed-prefix-plan.json')
    if selection['selected'] is None:raise NoEligibleValidation('No original validation clock has an observed post600s prediction bin')
    training=next(q for q in train if q.animal_id==selected_training['animal_id'])
    valid=next(q for q in validation if q.animal_id==selection['selected'])
    require(bool((valid.observed & (valid.times-valid.times[0]>=float(settings.get('warmup_seconds',600)))).any()),
        'Selected original full validation clock has no defined prediction window')
    selected_validation=next(row for row in recomputed['roster'] if row['animal_id']==valid.animal_id)
    plan=dict(recomputed,prefix_evidence=prefix_info,selected_training_clock=selected_training,
        selected_validation_clock=dict(selected_validation,selection=selection),
        recomputed_prefix_plan_sha256=digest(case/'recomputed-prefix-plan.json'),
        preprocessing_recomputed_not_restored=True,original_stage1_tensor_artifact_loaded=False,
        byte_parity_with_original_initial_tensor_artifact_claimed=False,selection_amendment='full-clock-observation-eligible-validation-canary-v2',
        predictive_contexts=9,quadrature_order=3,selected_validation_clock_has_defined_original_score=True,
        private_canary_helper_sha256=digest(Path(__file__)))
    exclusive(case/'plan.json',plan)
    torch.save({'initial_state':model.state_dict(),'torch_rng_state':torch.get_rng_state(),
        'parameter_shapes':recomputed['parameter_shapes'],'plan_sha256':digest(case/'plan.json'),**FALSE_FLAGS},case/'initial-state.pt')
    return model,training,scaler.transform(valid),n,settings,plan


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
    try:
        emit({'stage':'source_bound_worker_started'},True)
        prefix_info=bound_json(prefix_info_path,prefix_info_sha)
        require(prefix_info['helper']['sha256']==PREFIX_HELPER_SHA and prefix_info['original_stage1_artifact']['loaded'] is False
            and prefix_info['only_complete_stage1_is_required'] is True and all(prefix_info.get(k) is False for k in FALSE_FLAGS),
            'Exact parent-verified prefix preprocessing provenance required')
        bound_json(prefix_info['receipt']['path'],prefix_info['receipt']['sha256'])
        model,training,validation,normalizer,settings,plan=prepare(s,b,profile,config,root,case,deadline,emit,prefix_info)
        progress.update(status='whole_record_pair_running',plan_sha256=digest(case/'plan.json'),initial_state_sha256=digest(case/'initial-state.pt'))
        for stage in STAGES:
            MemoryGuard(deadline).check();emit({'stage':stage,'state':'started'},True)
            sequence=validation if stage.startswith('validation_') else training
            result,artifacts=paired_stage(s,adjoint,model,sequence,normalizer,settings,stage,deadline)
            checkpoint=case/(stage+'.pt');torch.save(artifacts,checkpoint)
            result.update(artifacts_path=str(checkpoint),artifacts_sha256=digest(checkpoint),plan_sha256=progress['plan_sha256'],**FALSE_FLAGS)
            exclusive(case/(stage+'.json'),result);progress['completed_stages'].append(result)
            emit({'stage':stage,'state':'completed'},True)
        require(digest(Path(plan['source_bin_path']))==plan['source_bin_sha256']
            and digest(Path(plan['provenance_path']))==plan['provenance_sha256'],'Source bin/provenance changed during paired probe')
        source_bindings(primary,framework);progress['status']='complete_one_record_first_order_probe'
    except (MemoryBound,DeadlineBound) as error:
        progress.update(status='memory_bound_not_compared' if isinstance(error,MemoryBound) else 'bounded_deadline_reached',
                        error_type=type(error).__name__,reason=str(error))
    except NoEligibleValidation as error:
        progress.update(status='unsupported_no_eligible_validation_clock',error_type=type(error).__name__,reason=str(error))
    except BaseException as error:
        progress.update(status='worker_failed',error_type=type(error).__name__,reason=str(error))
    progress.update(pending_stages=[stage for stage in STAGES if stage not in {r['stage'] for r in progress['completed_stages']}],
        process_cpu_seconds=time.process_time(),finished_unix=time.time(),memory_ceiling_bytes=MEMORY_LIMIT,
        scope='Whole recording component with full-fold preprocessing/nuisance rows; no whole-cohort objective/optimizer convergence proof')
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
    require(result.get('status') in {'complete_one_record_first_order_probe','memory_bound_not_compared','bounded_deadline_reached','worker_failed','unsupported_no_eligible_validation_clock'}
        and all(result.get(key) is False for key in FALSE_FLAGS)
        and result.get('primary_source_sha256')==PRIMARY_SHA and result.get('framework_source_sha256')==SOURCE
        and result.get('full_config_sha256')==CONFIG,'Valid source-bound honest technical worker receipt required')
    completed=result.get('completed_stages');require(isinstance(completed,list),'Explicit completed-stage roster required')
    names=[value.get('stage') for value in completed]
    require(names==list(STAGES[:len(names)]),'Partial completion must be an unambiguous original-stage prefix')
    if result['status']=='complete_one_record_first_order_probe':require(names==list(STAGES),'All four numerical stages must actually complete')
    for value in completed:
        path=case/(value['stage']+'.json');artifact=case/(value['stage']+'.pt')
        require(value.get('status')=='paired_complete' and all(value.get(key) is False for key in FALSE_FLAGS)
            and json.loads(path.read_text())==value and value.get('artifacts_path')==str(artifact)
            and digest(artifact)==value.get('artifacts_sha256') and digest(case/'plan.json')==value.get('plan_sha256'),
            'Actual completed-stage bytes/plan/false gates must bind the worker result')


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
    """Use the verified eight-hour allocation with an explicit cleanup reserve."""
    current=time.time() if now is None else now
    require(type(current) in (int,float) and math.isfinite(current),'Finite actual allocation clock required')
    require(type(allocation_end) in (str,int,float),'Finite native allocation end required')
    end=float(allocation_end)
    require(math.isfinite(end) and 330<=end-current<=8*3600+2,'Verified8h allocation and cleanup margin required')
    return min(27900.,end-current-300.)

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
    for name in ('qualification','resource-policy','native-script','context-helper','prefix-receipt','tests'):parser.add_argument('--'+name,type=Path,required=True)
    for name in ('qualification-sha256','qualification-job-id','resource-policy-sha256','context-helper-sha256','prefix-receipt-sha256','prefix-job-id','tests-sha256'):parser.add_argument('--'+name,required=True)
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
    # Only operational evidence/result namespaces change; no context numerical
    # function, qualification requirement, resource bound or kernel is changed.
    context.CASE,context.SUPPORT=CASE,SUPPORT
    job=str(context.native_job_id() or '')
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='1' and os.environ.get('HHW_GPU_COUNT')=='0'
        and os.environ.get('HHW_MAX_HOURS')=='8','Owned finite1CPU/eight-hour native adjoint context required')
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
            context.read_json(PRIMARY/'release.json')['release_id'],SUPPORT,'adjoint-baseline',
            resource_policy=policy,cpus=0,gpus=0,hours=8),allocation['allocation_end_unix'],evidence_dir=SUPPORT/'registration-wait')
    except (ReceiptLockTimeout,DeadlineBound) as error:
        if isinstance(error,ReceiptLockTimeout):
            require(type(error) is ReceiptLockTimeout and Path(error.path)==(ROOT/'state/af-campaign-registration.mutex').resolve(),
                'Only exact shared registration-lock contention may remain pending')
        exclusive(SUPPORT/'receipt.json',{'status':'pending_registration_no_probe_started','reason':str(error),'native_job_id':job,
            'allocation':allocation,'resource_policy':policy.receipt(),'source_sha256':SOURCE,'full_config_sha256':CONFIG,
            'qualification_not_yet_validated_by_probe_wrapper':{'path':str(args.qualification),'sha256':args.qualification_sha256},
            'root_registered_native_accounting_remains_required':True,**FALSE_FLAGS})
        return 0
    observed=repair._repair_native_states(sorted({args.qualification_job_id,args.prefix_job_id,ORIGINAL_JOB_ID}),{},owner='pwa209',account='def-ptewarie_cpu')
    qualification=context.qualification_proof(args.qualification,args.qualification_sha256,args.qualification_job_id,observed)
    prefix_info=prefix_evidence(args.prefix_receipt,args.prefix_receipt_sha256,args.prefix_job_id,observed)
    with context.resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=8,job_id=job,resource_policy=policy):
        quota=runpy.run_path(str(context.QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,
            required_bytes=10_000_000_000,reserve_files=248000,reserve_bytes=2_000_000_000_000,
            evidence_dir=SUPPORT/'quota-reads',allocation_deadline_unix=allocation['allocation_end_unix'])
        try:storage=context.storage_proof(config,quota)
        except context.PendingStorage as error:
            exclusive(SUPPORT/'receipt.json',{'status':'pending_storage_no_probe_started','reason':str(error),'native_job_id':job,**FALSE_FLAGS});return 0
        private_qualification=qualify_private(args.tests,args.tests_sha256,allocation['allocation_end_unix'])
        exclusive(SUPPORT/'prefix-evidence.json',prefix_info)
        exclusive(SUPPORT/'input-bindings.json',{'allocation':allocation,'qualification':qualification,'storage':storage,'resource_policy':policy.receipt(),
            'prefix_evidence':prefix_info,
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
            prefix_evidence=prefix_info,
            private_qualification=private_qualification,
            selection_amendment='full-clock-observation-eligible-validation-canary-v2',preprocessing_recomputed_not_restored=True,
            source_sha256=SOURCE,primary_source_sha256=PRIMARY_SHA,full_config_sha256=CONFIG,resource_policy=policy.receipt(),
            pending=['Whole-cohort objective/gradient/performance','Other models/folds/starts/rates/bin widths','Full optimizer convergence/LBFGS',
                     'Three-state27-context sensitivity','GPU/occupancy/PIT/Hessian/MALA/recovery/scientific admission'])
        exclusive(CASE/'receipt.json',result);exclusive(SUPPORT/'receipt.json',result)
        print(json.dumps({'status':result['status'],'native_job_id':job,'receipt':str(SUPPORT/'receipt.json'),**FALSE_FLAGS},sort_keys=True))
        return 1 if result['status'] in {'worker_failed','worker_termination_unconfirmed'} else 0


if __name__=='__main__':raise SystemExit(main())
