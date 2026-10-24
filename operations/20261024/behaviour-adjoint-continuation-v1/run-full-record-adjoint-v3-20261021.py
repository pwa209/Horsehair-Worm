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
CONTEXT_SHA='396e0ec6f9cf5936884fdd08d23e5e3d6e92f9bb06c654d747d04b2b27338787'
PREFIX_HELPER_SHA='b131d6317858eae3c43c2ae7337f46e3173ab64355693980af2e7e489af46d3d'
PREFIX_TEST_SHA='01b50175e001a44db682befdfd2728602d4ad467b5b8345092e5d3fa6d9ed80c'
PREFIX_CASE=ROOT/'maintenance/behaviour-full-cohort-prefix-v3-20261021'
PREFIX_SUPPORT=ROOT/'maintenance/behaviour-full-cohort-prefix-v3-support-20261021'
PREFIX_STAGE='full_fold_preprocessing_and_eligibility_census'
ADJOINT_SHA='5ff03ba506d4ad8dab66dd51def1c728aade96a8af77661be03de20e40a205f9'
PROFILE_SHA='e60561b425d500d08690675d98e4f86b2617044e446183433cdf3e4465602e59'
CASE=ROOT/'maintenance/behaviour-full-record-adjoint-v3-20261021'
SUPPORT=ROOT/'maintenance/behaviour-full-record-adjoint-v3-support-20261021'
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
    from horsehair.slurm import completed_native_dependency
    require(Path(path)==PREFIX_SUPPORT/'receipt.json' and re.fullmatch('[1-9][0-9]*',job),'Exact completed prefix receipt/job required')
    value=bound_json(path,sha);record=observed.get(job,{})
    require(record.get('state')=='COMPLETED' and record.get('exit_code')=='0:0' and record.get('user')=='pwa209'
        and record.get('account')=='def-ptewarie_cpu' and record.get('job_id_raw')==job and record.get('unallocated_metadata') is False
        and type(record.get('cpus')) is int and record['cpus']==1 and type(record.get('elapsed_seconds')) is int
        and 0<record['elapsed_seconds']<=7200 and 'gpu' not in record.get('alloc_tres','').lower(),
        'Positive owned scalar1CPU prefix completion required')
    completion=completed_native_dependency(ROOT,job);require(completion is not None,'Prefix native completion is not persisted positively')
    require(value.get('status') in {'complete_full_cohort_first_update_technical_prefix','bounded_deadline_reached','memory_bound_not_compared'}
        and value.get('native_job_id')==job and value.get('case')==str(PREFIX_CASE)
        and value.get('source_sha256')==SOURCE and value.get('primary_source_sha256')==PRIMARY_SHA
        and value.get('full_config_sha256')==CONFIG and all(value.get(k) is False for k in FALSE_FLAGS),
        'Source-bound complete or explicitly partial prefix required')
    q=value.get('private_qualification',{});test=q.get('test_source',{})
    require(q.get('helper_sha256')==PREFIX_HELPER_SHA and q.get('test_counts')=={'tests':32,'failures':0,'errors':0,'skipped':0}
        and q.get('test_roster_sha256')=='bf1907f95d35b44dcbee25c777f00e6fcd91df3bf692e1a50724546438e9d724'
        and test.get('sha256')==PREFIX_TEST_SHA and q.get('scientific_result') is False and q.get('admission_authority') is False,
        'Exact actual32-case prefix qualification required')
    test_path=ordinary(test['path']);require(digest(test_path)==PREFIX_TEST_SHA,'Prefix test source changed')
    helper=ordinary(test_path.with_name('run-full-cohort-prefix-v3-20261021.py'))
    require(digest(helper)==PREFIX_HELPER_SHA,'Prefix helper source changed')
    prefix=load_exact('qualified_prefix_ingress',helper,PREFIX_HELPER_SHA)
    require(prefix.verify_private_junit(q['path'])['sha256']==q['sha256'],'Actual full32 native JUnit changed')
    worker=value.get('worker_result') or (value.get('preserved_partial_progress') or {}).get('value',{})
    require(isinstance(worker.get('torch_version'),str) and bool(worker['torch_version']),'Actual prefix Torch runtime required')
    stages=worker.get('completed_stages',[])
    require(stages and stages[0].get('stage')==PREFIX_STAGE,'Prefix full preprocessing/census stage did not complete')
    stage=stages[0];stage_path=PREFIX_CASE/(PREFIX_STAGE+'.json')
    require(bound_json(stage_path,digest(stage_path))==stage and stage.get('status')=='operator_prefix_stage_complete'
        and all(stage.get(k) is False for k in FALSE_FLAGS),'Actual committed prefix stage1 required')
    plan_path=PREFIX_CASE/'plan.json';plan=bound_json(plan_path,stage['plan_sha256'])
    census_path=PREFIX_CASE/'clock-eligibility-census.json';census=bound_json(census_path,plan['census_sha256'])
    require(stage.get('census_sha256')==plan['census_sha256'] and census.get('all_original_roster_rows_retained') is True
        and plan.get('private_helper_sha256')==PREFIX_HELPER_SHA and plan.get('primary_source_sha256')==PRIMARY_SHA
        and plan.get('framework_source_sha256')==SOURCE and plan.get('full_config_sha256')==CONFIG,
        'Original full-roster/source/scaler plan provenance changed')
    artifact=ordinary(stage['artifacts_path']);require(artifact==PREFIX_CASE/(PREFIX_STAGE+'.pt')
        and digest(artifact)==stage['artifacts_sha256'],'Preserved original stage1 artifact changed')
    # The original tensor artifact remains byte-bound but is never unpickled.
    return {'receipt':{'path':str(path),'sha256':sha},'native_job_id':job,'native_observed':record,'completion':completion,
        'runtime_torch_version':worker['torch_version'],
        'helper':{'path':str(helper),'sha256':PREFIX_HELPER_SHA},'plan':{'path':str(plan_path),'sha256':stage['plan_sha256']},
        'census':{'path':str(census_path),'sha256':plan['census_sha256']},
        'original_stage1_artifact':{'path':str(artifact),'sha256':stage['artifacts_sha256'],'loaded':False},
        'full_prefix_completion_required':False,'only_complete_stage1_is_required':True,**FALSE_FLAGS}


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


def stop(process):
    if process.poll() is not None:return
    if os.name=='posix':os.killpg(process.pid,signal.SIGTERM)
    else:process.terminate()
    try:process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if os.name=='posix':os.killpg(process.pid,signal.SIGKILL)
        else:process.kill()
        process.wait(timeout=5)


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


def supervise(command,environment,deadline,case,*,popen=subprocess.Popen,clock=time.monotonic,sleeper=time.sleep,rss=rss_bytes):
    result=None;reason=None;peak=0
    with (case/'stdout.log').open('xb') as stdout,(case/'stderr.log').open('xb') as stderr:
        process=popen(command,env=environment,cwd=PRIMARY,stdout=stdout,stderr=stderr,start_new_session=os.name=='posix')
        try:
            while process.poll() is None:
                if clock()>=deadline:reason='bounded_deadline_reached';stop(process);break
                try:used=rss(process.pid)
                except (FileNotFoundError,ProcessLookupError):
                    if process.poll() is not None:break
                    raise
                peak=max(peak,used)
                if used>=MEMORY_LIMIT:reason='memory_bound_not_compared';stop(process);break
                sleeper(1)
        finally:
            if process.poll() is None:stop(process)
        code=process.returncode
    path=case/'worker-result.json'
    if path.is_file():result=json.loads(path.read_text());validate_worker_result(result,case)
    progress_path=case/'progress.json';progress=None
    if progress_path.is_file():
        body=json.loads(progress_path.read_text())
        if result is None and reason in {'bounded_deadline_reached','memory_bound_not_compared'}:
            partial=dict(body,status=reason);validate_worker_result(partial,case)
            progress={'path':str(progress_path),'sha256':digest(progress_path),'value':body}
    if reason is None and (code!=0 or result is None):reason='worker_failed'
    if reason is None:
        reason=result['status']
    return {'status':reason,'worker_returncode':code,'worker_result':result,'preserved_partial_progress':progress,'supervisor_peak_current_rss_bytes':peak,
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
    test_path=ordinary(test_path)
    require(test_path.name=='test_full_record_adjoint_v3_20261021.py' and test_path.parent==Path(__file__).resolve().parent
        and re.fullmatch('[0-9a-f]{64}',test_sha) and digest(test_path)==test_sha,'Exact colocated private adjoint v3 test source required')
    library=ordinary(test_path.with_name('run-full-cohort-prefix-v3-20261021.py'))
    require(digest(library)==PREFIX_HELPER_SHA,'Exact qualified prefix source fixture/library required')
    workspace=native_fixture_workspace()
    xml=SUPPORT/'private-tests.xml';helper_sha=digest(Path(__file__))
    command=[sys.executable,'-s','-m','pytest',str(test_path),'-q','-p','no:cacheprovider',
        '--basetemp',str(workspace/'fixtures'),'--junitxml',str(xml)]
    env={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1','TZ':'UTC'}
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):env.pop(key,None)
    timeout=min(600.,allocation_end-time.time()-300.);require(timeout>=60,'Finite native private test/finalization margin required')
    exclusive(SUPPORT/'private-tests-intent.json',{'command':command,'test_sha256':test_sha,'helper_sha256':helper_sha,**FALSE_FLAGS})
    with (SUPPORT/'private-tests-stdout.txt').open('xb') as out,(SUPPORT/'private-tests-stderr.txt').open('xb') as err:
        completed=runner(command,cwd=FRAMEWORK,env=env,stdout=out,stderr=err,timeout=timeout,check=False)
    require(completed.returncode==0,'Actual native full adjoint v3 private tests failed')
    result=verify_private_junit(xml)
    require(digest(test_path)==test_sha and digest(Path(__file__))==helper_sha and digest(library)==PREFIX_HELPER_SHA,
        'Private source changed during native qualification')
    result.update(test_source={'path':str(test_path),'sha256':test_sha},helper_sha256=helper_sha,
        prefix_library={'path':str(library),'sha256':PREFIX_HELPER_SHA},
        scientific_result=False,admission_authority=False)
    exclusive(SUPPORT/'private-qualification.json',result);return result


def main():
    parser=argparse.ArgumentParser()
    for name in ('qualification','resource-policy','native-script','context-helper','prefix-receipt','tests'):parser.add_argument('--'+name,type=Path,required=True)
    for name in ('qualification-sha256','qualification-job-id','resource-policy-sha256','context-helper-sha256','prefix-receipt-sha256','prefix-job-id','tests-sha256'):parser.add_argument('--'+name,required=True)
    args=parser.parse_args()
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
        and os.environ.get('HHW_MAX_HOURS')=='2','Owned finite1CPU native adjoint context required')
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
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'Guard must precede campaign reader binding')
    reader=runpy.run_path(str(context.SCHEDULER))['run_scontrol_job']
    allocation=context.native_allocation(job,args.native_script,reader)
    from horsehair.core import ReceiptLockTimeout
    try:
        _,registration=shared_admission(lambda:campaign.combined_admission(ROOT,config,
            context.read_json(PRIMARY/'release.json')['release_id'],SUPPORT,'adjoint-baseline',
            resource_policy=policy,cpus=0,gpus=0,hours=2),allocation['allocation_end_unix'],evidence_dir=SUPPORT/'registration-wait')
    except (ReceiptLockTimeout,DeadlineBound) as error:
        if isinstance(error,ReceiptLockTimeout):
            require(type(error) is ReceiptLockTimeout and Path(error.path)==(ROOT/'state/af-campaign-registration.mutex').resolve(),
                'Only exact shared registration-lock contention may remain pending')
        exclusive(SUPPORT/'receipt.json',{'status':'pending_registration_no_probe_started','reason':str(error),'native_job_id':job,
            'allocation':allocation,'resource_policy':policy.receipt(),'source_sha256':SOURCE,'full_config_sha256':CONFIG,
            'qualification_not_yet_validated_by_probe_wrapper':{'path':str(args.qualification),'sha256':args.qualification_sha256},
            'root_registered_native_accounting_remains_required':True,**FALSE_FLAGS})
        return 0
    observed=repair._repair_native_states(sorted({args.qualification_job_id,args.prefix_job_id}),{},owner='pwa209',account='def-ptewarie_cpu')
    qualification=context.qualification_proof(args.qualification,args.qualification_sha256,args.qualification_job_id,observed)
    prefix_info=prefix_evidence(args.prefix_receipt,args.prefix_receipt_sha256,args.prefix_job_id,observed)
    with context.resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=2,job_id=job,resource_policy=policy):
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
        from horsehair.behaviour_feasibility import runtime_limit
        seconds=runtime_limit(6300,allocation_end=os.environ['SLURM_JOB_END_TIME']);deadline=time.monotonic()+seconds
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
        result.update(native_job_id=job,case=str(CASE),qualification=qualification,allocation=allocation,
            prefix_evidence=prefix_info,
            private_qualification=private_qualification,
            selection_amendment='full-clock-observation-eligible-validation-canary-v2',preprocessing_recomputed_not_restored=True,
            source_sha256=SOURCE,primary_source_sha256=PRIMARY_SHA,full_config_sha256=CONFIG,resource_policy=policy.receipt(),
            pending=['Whole-cohort objective/gradient/performance','Other models/folds/starts/rates/bin widths','Full optimizer convergence/LBFGS',
                     'Three-state27-context sensitivity','GPU/occupancy/PIT/Hessian/MALA/recovery/scientific admission'])
        exclusive(CASE/'receipt.json',result);exclusive(SUPPORT/'receipt.json',result)
        print(json.dumps({'status':result['status'],'native_job_id':job,'receipt':str(SUPPORT/'receipt.json'),**FALSE_FLAGS},sort_keys=True))
        return 1 if result['status']=='worker_failed' else 0


if __name__=='__main__':raise SystemExit(main())
