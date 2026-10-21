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
import subprocess
import sys
import time

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-caebc9c1dc77-8495a3d98551'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
SOURCE='8495a3d985515bf71f476295da87e663a1e764864ddce47a06794e7e11d88f94'
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
CONTEXT_SHA='396e0ec6f9cf5936884fdd08d23e5e3d6e92f9bb06c654d747d04b2b27338787'
ADJOINT_SHA='5ff03ba506d4ad8dab66dd51def1c728aade96a8af77661be03de20e40a205f9'
PROFILE_SHA='e60561b425d500d08690675d98e4f86b2617044e446183433cdf3e4465602e59'
CASE=ROOT/'maintenance/behaviour-full-record-adjoint-v1-20261021'
SUPPORT=ROOT/'maintenance/behaviour-full-record-adjoint-v1-support-20261021'
MEMORY_LIMIT=48*1024**3
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


def shared_admission(callback,allocation_deadline,*,lock_factory=None,wall=time.time):
    """Hold the shared publisher mutex only for the unchanged admission read.

    Other publishers are serialized by the native deployment owner until this
    read returns. No numerical work, input preprocessing or child runs here.
    """
    if lock_factory is None:
        from horsehair.core import exclusive_file_lock
        lock_factory=exclusive_file_lock
    require(not isinstance(allocation_deadline,bool) and math.isfinite(allocation_deadline), 'Finite native admission deadline required')
    timeout=min(60.,allocation_deadline-wall()-150.)
    if timeout<1:raise DeadlineBound('No finite registration acquisition/finalization margin remains')
    started=time.monotonic();path=ROOT/'state/af-campaign-registration.mutex'
    with lock_factory(path,timeout=timeout):
        if wall()+120>=allocation_deadline:raise DeadlineBound('Native margin exhausted while acquiring registration lock')
        value=callback()
        if wall()+120>=allocation_deadline:raise DeadlineBound('Native margin exhausted during unchanged admission')
    return value,{'path':str(path),'maximum_acquisition_seconds':timeout,'elapsed_seconds':time.monotonic()-started,
        'scope':'Unchanged qualified8495 combined_admission only','released_before_numerical_execution':True,
        'native_query_timezone':'UTC','root_serializes_other_publishers_until_admission_returns':True}


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


def prepare(s,b,profile,config,root,case,deadline,emit):
    import numpy as np
    import pyarrow.parquet as pq
    import torch
    settings=dict(config['behaviour'],modulation=['elapsed_hours'],learning_rate=.001)
    require(float(settings.get('primary_bin_seconds',10))==10 and settings.get('max_duration_bins') is None
            and int(settings.get('states',2))==2 and .001 in settings['learning_rates'],'Original primary10s/two-state settings required')
    path=root/'results/behaviour/fly_bins_10s.parquet';provenance=root/'results/behaviour/streaming_bin_provenance.json'
    proof=json.loads(provenance.read_text());input_sha=digest(path)
    require(proof['bin_files'][str(path)]==input_sha,'Full original bin hash/provenance binding changed')
    parquet=pq.ParquetFile(path);roster=profile.recording_roster(parquet.metadata)
    split=profile.grouped_profile_split(roster,int(settings.get('inner_folds',3)))
    training=select_clock(roster,split['training_ids'],salt='adjoint-v1:training:')
    validation=select_clock(roster,split['validation_ids'],salt='adjoint-v1:validation:')
    sequences={};required=set(split['training_ids']+[validation['animal_id']])
    for row in roster:
        if row['animal_id'] not in required:continue
        if time.monotonic()>=deadline:raise DeadlineBound('Deadline during whole-fold source preprocessing')
        frame=parquet.read_row_group(row['row_group']).to_pandas();converted=b.to_sequences(frame,settings);del frame
        require(len(converted)==1 and converted[0].animal_id==row['animal_id'] and len(converted[0].times)==row['bins'],
                'A source clock was shortened or changed')
        sequences[row['animal_id']]=converted[0]
        emit({'stage':'whole_training_fold_input','loaded_recordings':len(sequences),'required_recordings':len(required)})
    train=[sequences[k] for k in split['training_ids']];valid=sequences[validation['animal_id']]
    s.assert_disjoint(train,[valid],experiment=True)
    scaler=s.TrainingScaler.fit(train);knots=s.training_duration_knots(train)
    transformed=[scaler.transform(q) for q in train];normalizer=sum(int(q.observed.sum()) for q in transformed)
    observed=np.concatenate([q.observations[q.observed] for q in train]);scale=np.maximum(np.std(observed,axis=0),1e-4)
    seed=int(settings.get('seed',0));torch.manual_seed(seed)
    constructor=dict(model='MX',n_covariates=len(scaler.mean),n_modulation=len(scaler.modulation_mean),dimension=train[0].observations.shape[1],
        knots=knots,animal_ids=[q.animal_id for q in train],experiment_ids=[q.experiment_id for q in train],states=2,max_duration_bins=None,bin_seconds=10.)
    model=s.MovementModel(**constructor)
    with torch.no_grad():
        model.log_rest_scale.copy_(model._tensor(np.log(scale/3)))
        for name,parameter in model.named_parameters():
            if parameter.requires_grad and name not in ('log_rest_scale','log_animal_sd','log_experiment_sd'):parameter.add_(torch.randn_like(parameter)*.05)
    effects,weights=model.predictive_random_effects(scaler.transform(valid),3);require(effects.shape==(9,2),'Exact two-state9-context predictive grid required')
    plan={'source_bin_path':str(path),'source_bin_sha256':input_sha,'provenance_path':str(provenance),'provenance_sha256':digest(provenance),
        'roster':roster,'split':split,'selected_training_clock':training,'selected_validation_clock':validation,
        'training_fold_recordings':len(train),'training_fold_full_bins':sum(len(q.times) for q in train),'global_observed_bin_normalizer':normalizer,
        'scaler':asdict(scaler),'duration_knots':knots.tolist(),'output_scale':scale.tolist(),'initialization_seed':seed,
        'parameter_shapes':{name:list(p.shape) for name,p in model.named_parameters()},'full_nuisance_dimensions':{'animal_rows':len(model.animal_ids),'experiment_rows':len(model.experiment_ids)},
        'settings':settings,'recording_time_only':True,'exposure_metadata_inferred':False,'predictive_contexts':9,
        'quadrature_order':3,'three_state27_context_sensitivity':'pending separate probe',**FALSE_FLAGS}
    exclusive(case/'plan.json',plan)
    torch.save({'constructor':constructor,'scaler':asdict(scaler),'initial_state':model.state_dict(),'torch_rng_state':torch.get_rng_state(),
                'plan_sha256':digest(case/'plan.json'),**FALSE_FLAGS},case/'initial-state.pt')
    return model,transformed[split['training_ids'].index(training['animal_id'])],scaler.transform(valid),normalizer,settings,plan


def _worker(primary,framework,root,case,deadline):
    import torch
    import horsehair
    from horsehair import semimarkov as s,behaviour as b
    from horsehair.core import atomic_json
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
        model,training,validation,normalizer,settings,plan=prepare(s,b,profile,config,root,case,deadline,emit)
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
    except BaseException as error:
        progress.update(status='worker_failed',error_type=type(error).__name__,reason=str(error))
    progress.update(pending_stages=[stage for stage in STAGES if stage not in {r['stage'] for r in progress['completed_stages']}],
        process_cpu_seconds=time.process_time(),finished_unix=time.time(),memory_ceiling_bytes=MEMORY_LIMIT,
        scope='Whole recording component with full-fold preprocessing/nuisance rows; no whole-cohort objective/optimizer convergence proof')
    atomic_json(case/'worker-result.json',progress);emit({'stage':'worker_finished'},True)
    return 1 if progress['status']=='worker_failed' else 0


CHILD="""import importlib.util,pathlib,sys
primary,framework,helper,root,case,deadline=sys.argv[1:]
sys.path.insert(0,str(pathlib.Path(primary)/'src'))
spec=importlib.util.spec_from_file_location('native_adjoint_probe',helper)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
raise SystemExit(module._worker(pathlib.Path(primary),pathlib.Path(framework),pathlib.Path(root),pathlib.Path(case),float(deadline)))
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
    require(result.get('status') in {'complete_one_record_first_order_probe','memory_bound_not_compared','bounded_deadline_reached','worker_failed'}
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


def main():
    parser=argparse.ArgumentParser()
    for name in ('qualification','resource-policy','native-script','context-helper'):parser.add_argument('--'+name,type=Path,required=True)
    for name in ('qualification-sha256','qualification-job-id','resource-policy-sha256','context-helper-sha256'):parser.add_argument('--'+name,required=True)
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
            resource_policy=policy,cpus=0,gpus=0,hours=2),allocation['allocation_end_unix'])
    except (ReceiptLockTimeout,DeadlineBound) as error:
        if isinstance(error,ReceiptLockTimeout):
            require(type(error) is ReceiptLockTimeout and Path(error.path)==(ROOT/'state/af-campaign-registration.mutex').resolve(),
                'Only exact shared registration-lock contention may remain pending')
        exclusive(SUPPORT/'receipt.json',{'status':'pending_registration_no_probe_started','reason':str(error),'native_job_id':job,
            'allocation':allocation,'resource_policy':policy.receipt(),'source_sha256':SOURCE,'full_config_sha256':CONFIG,
            'qualification_not_yet_validated_by_probe_wrapper':{'path':str(args.qualification),'sha256':args.qualification_sha256},
            'root_registered_native_accounting_remains_required':True,**FALSE_FLAGS})
        return 0
    observed=repair._repair_native_states([args.qualification_job_id],{},owner='pwa209',account='def-ptewarie_cpu')
    qualification=context.qualification_proof(args.qualification,args.qualification_sha256,args.qualification_job_id,observed)
    with context.resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=2,job_id=job,resource_policy=policy):
        quota=runpy.run_path(str(context.QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,
            required_bytes=10_000_000_000,reserve_files=248000,reserve_bytes=2_000_000_000_000,
            evidence_dir=SUPPORT/'quota-reads',allocation_deadline_unix=allocation['allocation_end_unix'])
        try:storage=context.storage_proof(config,quota)
        except context.PendingStorage as error:
            exclusive(SUPPORT/'receipt.json',{'status':'pending_storage_no_probe_started','reason':str(error),'native_job_id':job,**FALSE_FLAGS});return 0
        exclusive(SUPPORT/'input-bindings.json',{'allocation':allocation,'qualification':qualification,'storage':storage,'resource_policy':policy.receipt(),
            'registration_protection':registration,
            'context_helper':{'path':str(args.context_helper),'sha256':CONTEXT_SHA,'operational_rebinding':{'CASE':str(CASE),'SUPPORT':str(SUPPORT)}},
            'adjoint_sha256':ADJOINT_SHA,'profile_sha256':PROFILE_SHA,'helper_sha256':digest(Path(__file__)),**FALSE_FLAGS})
        from horsehair.rna_recovery import _primary_environment
        from horsehair.behaviour_feasibility import runtime_limit
        seconds=runtime_limit(6300,allocation_end=os.environ['SLURM_JOB_END_TIME']);deadline=time.monotonic()+seconds
        CASE.mkdir(mode=0o700)
        environment=_primary_environment({'primary_release':str(PRIMARY),'maintenance_release':str(FRAMEWORK),'root':str(ROOT)})
        environment.update(OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
        command=[sys.executable,'-s','-u','-c',CHILD,str(PRIMARY),str(FRAMEWORK),str(Path(__file__).resolve()),str(ROOT),str(CASE),str(deadline)]
        exclusive(CASE/'started.json',{'command':command,'maximum_worker_seconds':seconds,'native_job_id':job,
            'primary_source_sha256':PRIMARY_SHA,'framework_source_sha256':SOURCE,'full_config_sha256':CONFIG,'helper_sha256':digest(Path(__file__)),**FALSE_FLAGS})
        result=supervise(command,environment,deadline,CASE)
        source_bindings(PRIMARY,FRAMEWORK)
        require(digest(args.context_helper)==CONTEXT_SHA and digest(args.qualification)==args.qualification_sha256
            and digest(args.resource_policy)==args.resource_policy_sha256,'Prerequisite changed during paired probe')
        result.update(native_job_id=job,case=str(CASE),qualification=qualification,allocation=allocation,
            source_sha256=SOURCE,primary_source_sha256=PRIMARY_SHA,full_config_sha256=CONFIG,resource_policy=policy.receipt(),
            pending=['Whole-cohort objective/gradient/performance','Other models/folds/starts/rates/bin widths','Full optimizer convergence/LBFGS',
                     'Three-state27-context sensitivity','GPU/occupancy/PIT/Hessian/MALA/recovery/scientific admission'])
        exclusive(CASE/'receipt.json',result);exclusive(SUPPORT/'receipt.json',result)
        print(json.dumps({'status':result['status'],'native_job_id':job,'receipt':str(SUPPORT/'receipt.json'),**FALSE_FLAGS},sort_keys=True))
        return 1 if result['status']=='worker_failed' else 0


if __name__=='__main__':raise SystemExit(main())
