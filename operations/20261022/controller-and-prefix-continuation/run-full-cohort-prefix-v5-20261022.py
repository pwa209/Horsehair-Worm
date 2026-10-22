"""Bounded native, full-cohort first-update prefix; no scientific fit.

All source clocks and the frozen grouped fold are retained. This measures only
the exact first training update and original validation calls. Undefined scores
remain null; no optimizer convergence or alternative criterion is executed.
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
PROFILE_SHA='e60561b425d500d08690675d98e4f86b2617044e446183433cdf3e4465602e59'
CASE=ROOT/'maintenance/behaviour-full-cohort-prefix-v5-20261022'
SUPPORT=ROOT/'maintenance/behaviour-full-cohort-prefix-v5-support-20261022'
MEMORY_LIMIT=48*1024**3
PRIVATE_TEST_COUNT=57
PRIVATE_TEST_ROSTER_SHA='8af2796c2bcd624307b670dffddf8b3cb12691a609f9080a2ad5799e1fb9a0b9'
CONTEXT_TEST_COUNT=41
CONTEXT_TEST_ROSTER_SHA='3a16967be42683fcce40a7883d5b326991329c94cfb5d08a6a1ee88b3b8172a2'
FALSE_FLAGS={'scientific_result':False,'scientific_fit':False,'complete_declared_workload':False,
             'admission_authority':False,'original_H1_H2_H3_gates_cleared':False,
             'native_gpu_qualified':False,'second_derivatives_supported':False,
             'whole_cohort_objective_qualified':False,'optimizer_convergence_qualified':False,
             'MALA_acceptance_qualified':False}
STAGES=('full_fold_preprocessing_and_eligibility_census','full_training_objective_and_all_gradients',
        'single_original_clip_and_Adam_update','all_original_validation_calls_with_explicit_nulls')


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


def prefix_runtime_seconds(allocation_end, *, now=None):
    """Exact8h allocation already verified; bound this prefix independently.

    The preserved public finite-probe helper intentionally caps old probes at
    two hours. This new operational wrapper never changes that public helper.
    """
    current=time.time() if now is None else now
    require(type(current) in (int,float) and math.isfinite(current),'Finite actual allocation clock required')
    require(type(allocation_end) in (str,int,float),'Finite native allocation end required')
    end=float(allocation_end)
    require(math.isfinite(end) and 330<=end-current<=8*3600+2,'Verified8h native allocation and cleanup margin required')
    return min(27900.,end-current-300.)


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


def source_bindings(primary,framework):
    from horsehair.core import source_identity,canonical_hash,read_json
    require(Path(primary).resolve()==PRIMARY.resolve() and Path(framework).resolve()==FRAMEWORK.resolve(),'Exact immutable primary/framework paths required')
    require(source_identity(primary)['sha256']==PRIMARY_SHA and source_identity(framework)['sha256']==SOURCE,'Exact qualified source identities required')
    config=read_json(PRIMARY/'configs/study.json')
    require(canonical_hash(config)==CONFIG and read_json(FRAMEWORK/'configs/study.json')==config,'Unchanged full scientific configuration required')
    require(digest(FRAMEWORK/'src/horsehair/cohort_profile.py')==PROFILE_SHA,'Exact qualified full-cohort technical module required')
    return config



def clock_census(frame,row,warmup):
    """Use exactly to_sequences' effective observation mask, never effect labels."""
    import numpy as np
    require(type(warmup) in (int,float) and math.isfinite(warmup) and warmup>=0,'Finite original warm-up required')
    require(len(frame)==row['bins'] and set(map(str,frame.animal_id))=={row['animal_id']}
        and set(map(str,frame.experiment_id))=={row['experiment_id']},'Original whole-clock identity changed')
    group=frame.sort_values('record_time_s');times=group.record_time_s.to_numpy(float)
    require(bool(np.isfinite(times).all()) and bool((np.diff(times)>0).all()),'Finite strictly increasing original clock required')
    columns=['dx','dy'] if group.dx.notna().any() else ['dy']
    observed=group.observed.to_numpy(bool)&np.isfinite(group[columns].to_numpy(float)).all(1)
    eligible=observed & (times-times[0]>=warmup)
    return {'animal_id':row['animal_id'],'experiment_id':row['experiment_id'],'bins':len(times),
        'observed_bins':int(observed.sum()),'eligible_prediction_bins':int(eligible.sum()),
        'warmup_seconds':warmup,'recording_time_span_seconds':float(times[-1]-times[0]),
        'source_clock_sha256':hashlib.sha256(times.tobytes()).hexdigest(),
        'effective_observed_mask_sha256':hashlib.sha256(observed.tobytes()).hexdigest(),
        'prediction_window_status':'defined' if bool(eligible.any()) else 'undefined_no_observed_prediction_window',
        'prediction_log_score':None,'endpoint_magnitudes_used_for_selection':False,
        'finite_coordinate_mask_matches_primary':True}


def prospective_validation_canary_v2(census,validation_ids):
    """Unexecuted prospective canary selection; does not replace the frozen c5 probe."""
    allowed=set(validation_ids);rows=[r for r in census if r['animal_id'] in allowed]
    require(len(rows)==len(allowed) and len({r['animal_id'] for r in rows})==len(rows),'Complete validation census required')
    eligible=[r for r in rows if r['eligible_prediction_bins']>0]
    if not eligible:return {'status':'unsupported_no_defined_validation_clock','selected':None,'null_clock_ids':sorted(allowed)}
    longest=max(r['bins'] for r in eligible)
    row=min((r for r in eligible if r['bins']==longest),
        key=lambda r:hashlib.sha256(('adjoint-v2:validation:'+r['animal_id']).encode()).hexdigest())
    return {'status':'prospective_selection_only_not_executed','selected':row['animal_id'],
        'null_clock_ids':sorted(r['animal_id'] for r in rows if r['eligible_prediction_bins']==0),
        'selection':'Longest complete source clock with a defined original post-warmup observation mask; source-ID SHA tie break'}


def prepare(s,b,profile,config,root,case,deadline,emit):
    import numpy as np
    import pyarrow.parquet as pq
    import torch
    settings=dict(config['behaviour'],modulation=['elapsed_hours'],learning_rate=.001)
    require(float(settings.get('primary_bin_seconds',10))==10 and settings.get('max_duration_bins') is None
        and int(settings.get('states',2))==2 and .001 in settings['learning_rates'],'Original10s/two-state settings required')
    path=root/'results/behaviour/fly_bins_10s.parquet';provenance=root/'results/behaviour/streaming_bin_provenance.json'
    proof=json.loads(provenance.read_text());input_sha=digest(path)
    require(proof['bin_files'][str(path)]==input_sha,'Original source bin/provenance binding changed')
    parquet=pq.ParquetFile(path);roster=profile.recording_roster(parquet.metadata)
    split=profile.grouped_profile_split(roster,int(settings.get('inner_folds',3)))
    roles={k:role for role,field in [('training','training_ids'),('validation','validation_ids'),('outer_holdout','outer_holdout_ids'),
        ('original_all_missing_exclusion','excluded_all_missing_ids')] for k in split[field]}
    selected=set(split['training_ids']+split['validation_ids']);sequences={};census=[]
    for row in roster:
        MemoryGuard(deadline).check()
        frame=parquet.read_row_group(row['row_group']).to_pandas()
        item=clock_census(frame,row,float(settings.get('warmup_seconds',600)));item['frozen_fold_role']=roles[row['animal_id']];census.append(item)
        if row['animal_id'] in selected:
            converted=b.to_sequences(frame,settings)
            require(len(converted)==1 and converted[0].animal_id==row['animal_id'] and len(converted[0].times)==row['bins']
                and int(converted[0].observed.sum())==item['observed_bins'],'Original full-clock conversion or mask changed')
            sequences[row['animal_id']]=converted[0]
        del frame
        emit({'stage':'source_clock_census','inspected':len(census),'roster':len(roster),'converted':len(sequences)})
    train=[sequences[k] for k in split['training_ids']];validation=[sequences[k] for k in split['validation_ids']]
    s.assert_disjoint(train,validation,experiment=True)
    scaler=s.TrainingScaler.fit(train);knots=s.training_duration_knots(train)
    transformed=[scaler.transform(q) for q in train];normalizer=sum(int(q.observed.sum()) for q in transformed)
    require(normalizer>0,'Complete training fold must have observed bins')
    observed=np.concatenate([q.observations[q.observed] for q in train]);scale=np.maximum(np.std(observed,axis=0),1e-4)
    seed=int(settings.get('seed',0));torch.manual_seed(seed)
    constructor=dict(model='MX',n_covariates=len(scaler.mean),n_modulation=len(scaler.modulation_mean),dimension=train[0].observations.shape[1],
        knots=knots,animal_ids=[q.animal_id for q in train],experiment_ids=[q.experiment_id for q in train],states=2,max_duration_bins=None,bin_seconds=10.)
    model=s.MovementModel(**constructor)
    with torch.no_grad():
        model.log_rest_scale.copy_(model._tensor(np.log(scale/3)))
        for name,p in model.named_parameters():
            if p.requires_grad and name not in ('log_rest_scale','log_animal_sd','log_experiment_sd'):p.add_(torch.randn_like(p)*.05)
    exclusive(case/'clock-eligibility-census.json',{'rows':census,'source_bin_sha256':input_sha,
        'primary_source_sha256':PRIMARY_SHA,'full_config_sha256':CONFIG,'all_original_roster_rows_retained':True,**FALSE_FLAGS})
    nulls=[r['animal_id'] for r in census if r['frozen_fold_role']=='validation' and not r['eligible_prediction_bins']]
    plan={'source_bin_path':str(path),'source_bin_sha256':input_sha,'provenance_path':str(provenance),'provenance_sha256':digest(provenance),
        'roster':roster,'split':split,'census_sha256':digest(case/'clock-eligibility-census.json'),
        'primary_source_sha256':PRIMARY_SHA,'framework_source_sha256':SOURCE,'full_config_sha256':CONFIG,
        'private_helper_sha256':digest(Path(__file__)),
        'model_constructor':dict(constructor,knots=knots.tolist()),
        'settings':settings,'training_fold_recordings':len(train),'validation_fold_recordings':len(validation),
        'training_fold_full_bins':sum(len(q.times) for q in train),'global_observed_bin_normalizer':normalizer,
        'scaler':asdict(scaler),'duration_knots':knots.tolist(),'output_scale':scale.tolist(),'initialization_seed':seed,
        'parameter_shapes':{name:list(p.shape) for name,p in model.named_parameters()},
        'full_nuisance_dimensions':{'animal_rows':len(model.animal_ids),'experiment_rows':len(model.experiment_ids)},
        'original_validation_criterion_defined':not bool(nulls),'null_validation_clock_ids':nulls,
        'prospective_canary_v2_selection_not_executed':prospective_validation_canary_v2(census,split['validation_ids']),
        'recording_time_only':True,'exposure_metadata_inferred':False,'profile_module_sha256':PROFILE_SHA,**FALSE_FLAGS}
    exclusive(case/'plan.json',plan)
    return model,transformed,validation,scaler,normalizer,settings,plan


def parameter_evidence(model):
    import torch
    result={}
    for name,p in model.named_parameters():
        require(bool(torch.isfinite(p).all()) and (p.grad is None or bool(torch.isfinite(p.grad).all())),'Nonfinite full-cohort parameter or gradient')
        result[name]={'shape':list(p.shape),'gradient_is_none':p.grad is None,
            'gradient_sha256':None if p.grad is None else hashlib.sha256(p.grad.detach().cpu().contiguous().numpy().tobytes()).hexdigest()}
    return result


def validation_call_or_null(fit,sequence,warmup):
    count=int((sequence.observed & (sequence.times-sequence.times[0]>=warmup)).sum())
    try:
        value=fit.score(sequence,warmup)
    except ValueError as error:
        expected='No observed prediction window after warm-up: '+sequence.animal_id
        if count or str(error)!=expected:raise
        return {'animal_id':sequence.animal_id,'experiment_id':sequence.experiment_id,'status':'undefined_original_score',
            'n_scored':0,'log_score':None,'original_exception':str(error),'clock_bins':len(sequence.times)}
    require(count>0 and value['n_scored']==count and math.isfinite(value['log_score']),'Original validation score/mask mismatch')
    return {'animal_id':sequence.animal_id,'experiment_id':sequence.experiment_id,'status':'defined_original_score',
        'n_scored':count,'log_score':value['log_score'],'original_result':value,'clock_bins':len(sequence.times)}


def validation_summary(rows):
    import numpy as np
    require(rows and len({r['animal_id'] for r in rows})==len(rows),'All original validation rows must remain unique')
    groups={}
    for row in rows:groups.setdefault(row['experiment_id'],[]).append(row)
    defined=all(row['status']=='defined_original_score' for row in rows)
    return {'rows':rows,'original_validation_criterion_defined':defined,
        'original_validation_criterion':-float(np.mean([np.mean([r['log_score'] for r in values]) for values in groups.values()])) if defined else None,
        'experiment_denominators':{g:{'original_animals':len(values),'defined_animals':sum(r['status']=='defined_original_score' for r in values),
            'undefined_animals':sum(r['status']=='undefined_original_score' for r in values),'eligible_bins':sum(r['n_scored'] for r in values)} for g,values in groups.items()},
        'alternative_eligible_only_criterion_executed':False,'null_scores_replaced_by_zero':False}


def verify_private_junit(path,*,suite='prefix'):
    from horsehair.core import canonical_hash
    from horsehair.cli import _qualification_junit
    raw=Path(path).read_bytes()
    require(len(raw)<=2_000_000 and b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Bounded plain private JUnit required')
    cases=list(ET.fromstring(raw).iter('testcase'))
    roster=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in cases)
    require(suite in {'prefix','context'},'Exact private prefix/context suite required')
    count,roster_sha,stem=(PRIVATE_TEST_COUNT,PRIVATE_TEST_ROSTER_SHA,'test_full_cohort_prefix_v5_20261022') if suite=='prefix' else (
        CONTEXT_TEST_COUNT,CONTEXT_TEST_ROSTER_SHA,'test_full_cohort_prefix_context_v5_20261022')
    counts={'tests':count,'failures':0,'errors':0,'skipped':0}
    require(count>0 and len(roster)==len(set(roster))==count and canonical_hash(roster)==roster_sha
        and all(a==stem and b for a,b in roster)
        and all(not any(c.find(k) is not None for k in ('failure','error','skipped')) for c in cases)
        and _qualification_junit(path)==counts,'Exact complete passing native prefix test roster required')
    return {'path':str(path),'sha256':digest(path),'test_counts':counts,'test_roster_sha256':roster_sha,'suite':suite}


def native_fixture_workspace(suite='prefix'):
    temp=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temp.is_absolute() and temp.is_dir() and temp==temp.resolve() and not temp.is_symlink()
        and temp.stat().st_uid==3164768 and shutil.disk_usage(temp).free>=3_000_000_000
        and os.statvfs(temp).f_favail>=5000,'Native local private fixture capacity required')
    require(suite in {'prefix','context'},'Exact finite private fixture suite required')
    workspace=temp/('hhw-cohort-prefix-v5-'+suite+'-tests-'+os.environ['SLURM_JOB_ID']);workspace.mkdir(mode=0o700,exist_ok=False)
    return workspace


def qualify_private(test_path,test_sha,allocation_end,*,runner=subprocess.run,suite='prefix'):
    test_path=ordinary(test_path)
    require(suite in {'prefix','context'},'Exact complete private suite required')
    expected='test_full_cohort_prefix_v5_20261022.py' if suite=='prefix' else 'test_full_cohort_prefix_context_v5_20261022.py'
    require(test_path.name==expected and test_path.parent==Path(__file__).resolve().parent
        and re.fullmatch('[0-9a-f]{64}',test_sha) and digest(test_path)==test_sha,'Exact colocated private prefix test source required')
    workspace=native_fixture_workspace() if suite=='prefix' else native_fixture_workspace('context')
    prefix='private' if suite=='prefix' else 'context-private'
    xml=SUPPORT/(prefix+'-tests.xml');helper_sha=digest(Path(__file__))
    command=[sys.executable,'-s','-m','pytest',str(test_path),'-q','-p','no:cacheprovider',
        '--basetemp',str(workspace/'fixtures'),'--junitxml',str(xml)]
    env={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1','TZ':'UTC'}
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):env.pop(key,None)
    timeout=min(600.,allocation_end-time.time()-300.);require(timeout>=60,'Finite native private test/finalization margin required')
    exclusive(SUPPORT/(prefix+'-tests-intent.json'),{'command':command,'test_sha256':test_sha,'helper_sha256':helper_sha,**FALSE_FLAGS})
    with (SUPPORT/(prefix+'-tests-stdout.txt')).open('xb') as out,(SUPPORT/(prefix+'-tests-stderr.txt')).open('xb') as err:
        completed=runner(command,cwd=FRAMEWORK,env=env,stdout=out,stderr=err,timeout=timeout,check=False)
    require(completed.returncode==0,'Actual native full prefix private tests failed')
    result=verify_private_junit(xml,suite=suite)
    require(digest(test_path)==test_sha and digest(Path(__file__))==helper_sha,'Private source changed during native qualification')
    result.update(test_source={'path':str(test_path),'sha256':test_sha},helper_sha256=helper_sha,
        scientific_result=False,admission_authority=False)
    exclusive(SUPPORT/(prefix+'-qualification.json'),result);return result


def run_prefix(s,profile,model,train,validation,scaler,normalizer,settings,deadline,emit,commit):
    import torch
    class BoundedTimings(profile.Timings):
        def __init__(self):super().__init__(emit);self.memory={}
        def call(self,phase,callback,**unit):
            guard=MemoryGuard(deadline)
            with guard:value=super().call(phase,callback,**unit)
            guard.check();e=guard.evidence();old=self.memory.setdefault(phase,{'maximum_saved_storage_bytes':0,'maximum_current_rss_bytes':0})
            old['maximum_saved_storage_bytes']=max(old['maximum_saved_storage_bytes'],e['unique_saved_storage_bytes'])
            old['maximum_current_rss_bytes']=max(old['maximum_current_rss_bytes'],e['observed_current_rss_peak_bytes'])
            return value
    timer=BoundedTimings();optimizer=torch.optim.Adam(model.parameters(),lr=float(settings.get('learning_rate',.001)))
    objective=profile.objective_gradient(model,train,normalizer,timer)
    commit(STAGES[1],{'objective':float(objective.cpu()),'global_observed_bin_normalizer':normalizer,
        'all_named_parameters_and_gradients':parameter_evidence(model),'timings':copy.deepcopy(timer.totals),'memory':copy.deepcopy(timer.memory)},
        {'model_state':model.state_dict(),'named_gradients':{n:p.grad for n,p in model.named_parameters()},'optimizer_state':optimizer.state_dict()})
    norm=timer.call('gradient_clip_once',lambda:torch.nn.utils.clip_grad_norm_(model.parameters(),float(settings.get('gradient_clip',5))))
    timer.call('optimizer_update_once',optimizer.step)
    commit(STAGES[2],{'gradient_norm_before_clip':float(norm.cpu()),'clip_calls':1,'Adam_update_calls':1,
        'all_named_parameters_and_gradients':parameter_evidence(model),'timings':copy.deepcopy(timer.totals)},
        {'model_state':model.state_dict(),'clipped_named_gradients':{n:p.grad for n,p in model.named_parameters()},'optimizer_state':optimizer.state_dict()})
    fit=s.Fit(model,scaler,{'tail_tolerance':settings.get('tail_tolerance',1e-8)})
    rows=[timer.call('original_validation_full_clock_call',lambda:validation_call_or_null(fit,q,float(settings.get('warmup_seconds',600))),
                    source_id=q.animal_id,clock_bins=len(q.times)) for q in validation]
    result=validation_summary(rows)
    commit(STAGES[3],dict(result,timings=copy.deepcopy(timer.totals),memory=copy.deepcopy(timer.memory)),{'validation_rows':rows})
    return result


def _worker(primary,framework,root,case,deadline):
    import torch
    import horsehair
    from horsehair import semimarkov as s,behaviour as b
    from horsehair.core import atomic_json
    primary,framework,root,case=(Path(p).resolve() for p in (primary,framework,root,case))
    require(Path(horsehair.__file__).resolve()==primary/'src/horsehair/__init__.py'
        and all(Path(m.__file__).resolve().is_relative_to(primary/'src') for m in (s,b)),'Frozen isolated primary interpreter required')
    config=source_bindings(primary,framework)
    require(root.resolve()==ROOT.resolve()==Path(config['paths']['root']).resolve() and case.resolve()==CASE.resolve()
        and case.is_dir() and not case.is_symlink(),'Exact personal prefix case/root required')
    require(os.environ.get('SLURM_CPUS_PER_TASK')=='1' and os.environ.get('HHW_GPU_COUNT')=='0','One CPU and no GPU required')
    torch.set_num_threads(1)
    profile=load_exact('horsehair._native_cohort_profile',framework/'src/horsehair/cohort_profile.py',PROFILE_SHA)
    progress={'status':'preprocessing','torch_threads':1,'torch_version':str(torch.__version__),'full_config_sha256':CONFIG,
        'primary_source_sha256':PRIMARY_SHA,'framework_source_sha256':SOURCE,'completed_stages':[],**FALSE_FLAGS}
    last=[None]
    def emit(event,force=False):
        progress['last_event']=event;progress['current_rss_bytes']=rss_bytes();progress['updated_unix']=time.time()
        if event.get('event')=='completed':progress['timings']=copy.deepcopy(event.get('totals',{}))
        if force or last[0] is None or time.monotonic()-last[0]>=30:
            atomic_json(case/'progress.json',progress);last[0]=time.monotonic()
    def commit(stage,result,artifact):
        require(stage==STAGES[len(progress['completed_stages'])],'Unambiguous original prefix stage order required')
        path=case/(stage+'.pt');torch.save(dict(artifact,torch_rng_state=torch.get_rng_state(),**FALSE_FLAGS),path)
        value=dict(result,stage=stage,status='operator_prefix_stage_complete',artifacts_path=str(path),artifacts_sha256=digest(path),
            plan_sha256=digest(case/'plan.json'),exact_mid_operation_resume_supported=False,**FALSE_FLAGS)
        exclusive(case/(stage+'.json'),value);progress['completed_stages'].append(value);emit({'stage':stage,'state':'completed'},True)
    try:
        emit({'stage':'source_bound_prefix_worker_started'},True)
        model,train,validation,scaler,normalizer,settings,plan=prepare(s,b,profile,config,root,case,deadline,emit)
        progress.update(status='whole_cohort_prefix_running',plan_sha256=digest(case/'plan.json'))
        commit(STAGES[0],{'training_clocks':len(train),'validation_clocks':len(validation),
            'normalizer':normalizer,'census_sha256':plan['census_sha256'],
            'original_validation_criterion_defined':plan['original_validation_criterion_defined']},
            {'model_state':model.state_dict(),'scaler':asdict(scaler),'parameter_shapes':plan['parameter_shapes']})
        validation_result=run_prefix(s,profile,model,train,validation,scaler,normalizer,settings,deadline,emit,commit)
        require(digest(Path(plan['source_bin_path']))==plan['source_bin_sha256']
            and digest(Path(plan['provenance_path']))==plan['provenance_sha256'],'Source changed during complete prefix')
        source_bindings(primary,framework)
        progress.update(status='complete_full_cohort_first_update_technical_prefix',validation_summary=validation_result)
    except (MemoryBound,DeadlineBound) as error:
        progress.update(status='memory_bound_not_compared' if isinstance(error,MemoryBound) else 'bounded_deadline_reached',error_type=type(error).__name__,reason=str(error))
    except BaseException as error:progress.update(status='worker_failed',error_type=type(error).__name__,reason=str(error))
    progress.update(pending_stages=list(STAGES[len(progress['completed_stages']):]),process_cpu_seconds=time.process_time(),finished_unix=time.time(),
        memory_ceiling_bytes=MEMORY_LIMIT,scope='Exact first full-cohort training update plus original validation calls; undefined scores retained as null; no convergence claim')
    atomic_json(case/'worker-result.json',progress);emit({'stage':'worker_finished'},True)
    return 1 if progress['status']=='worker_failed' else 0
CHILD="""import importlib.util,pathlib,sys
primary,framework,helper,root,case,deadline=sys.argv[1:]
sys.path.insert(0,str(pathlib.Path(primary)/'src'))
spec=importlib.util.spec_from_file_location('native_adjoint_probe',helper)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
raise SystemExit(module._worker(pathlib.Path(primary),pathlib.Path(framework),pathlib.Path(root),pathlib.Path(case),float(deadline)))
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
    require(result.get('status') in {'complete_full_cohort_first_update_technical_prefix','memory_bound_not_compared','bounded_deadline_reached','worker_failed'}
        and all(result.get(key) is False for key in FALSE_FLAGS)
        and result.get('primary_source_sha256')==PRIMARY_SHA and result.get('framework_source_sha256')==SOURCE
        and result.get('full_config_sha256')==CONFIG,'Valid source-bound honest technical worker receipt required')
    completed=result.get('completed_stages');require(isinstance(completed,list),'Explicit completed-stage roster required')
    names=[value.get('stage') for value in completed]
    require(names==list(STAGES[:len(names)]),'Partial completion must be an unambiguous original-stage prefix')
    if result['status']=='complete_full_cohort_first_update_technical_prefix':require(names==list(STAGES),'All four numerical stages must actually complete')
    for value in completed:
        path=case/(value['stage']+'.json');artifact=case/(value['stage']+'.pt')
        require(value.get('status')=='operator_prefix_stage_complete' and all(value.get(key) is False for key in FALSE_FLAGS)
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
                except (FileNotFoundError,ProcessLookupError):
                    if process.poll() is not None:break
                    raise
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


def main():
    parser=argparse.ArgumentParser()
    for name in ('qualification','resource-policy','native-script','context-helper','tests','context-tests'):parser.add_argument('--'+name,type=Path,required=True)
    for name in ('qualification-sha256','qualification-job-id','resource-policy-sha256','context-helper-sha256','tests-sha256','context-tests-sha256'):parser.add_argument('--'+name,required=True)
    args=parser.parse_args()
    scheduler_utc()
    require(args.context_helper_sha256==CONTEXT_SHA,'Exact reviewed operational context helper required')
    ordinary(args.context_helper);context=load_exact('reviewed_profile_native_context',args.context_helper,CONTEXT_SHA)
    require(context.FRAMEWORK==FRAMEWORK and context.SOURCE==SOURCE and context.PRIMARY_SHA==PRIMARY_SHA,'Operational context source mismatch')
    require(context.CASE==CASE and context.SUPPORT==SUPPORT,
        'Exact fresh8h prefix context namespaces required')
    job=str(context.native_job_id() or '')
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='1' and os.environ.get('HHW_GPU_COUNT')=='0'
        and os.environ.get('HHW_MAX_HOURS')=='8','Owned finite1CPU native full-cohort prefix context required')
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
    from horsehair import repair
    reader=runpy.run_path(str(context.SCHEDULER))['run_scontrol_job']
    allocation=context.native_allocation(job,args.native_script,reader)
    repair._repair_native_states=context.bounded_native_reader(repair._repair_native_states,SUPPORT/'bounded-native-reads',
        allocation['allocation_end_unix'])
    from horsehair import campaign
    require(campaign._repair_native_states is repair._repair_native_states,'358 and finite read deadline must precede campaign binding')
    from horsehair.core import ReceiptLockTimeout
    try:
        _,registration=shared_admission(lambda:campaign.combined_admission(ROOT,config,
            context.read_json(PRIMARY/'release.json')['release_id'],SUPPORT,'cohort-prefix-baseline',
            resource_policy=policy,cpus=0,gpus=0,hours=8),allocation['allocation_end_unix'])
    except (ReceiptLockTimeout,DeadlineBound) as error:
        if isinstance(error,ReceiptLockTimeout):
            require(type(error) is ReceiptLockTimeout and Path(error.path)==(ROOT/'state/af-campaign-registration.mutex').resolve(),
                'Only exact shared registration-lock contention may remain pending')
        exclusive(SUPPORT/'receipt.json',{'status':'pending_registration_no_probe_started','reason':str(error),'native_job_id':job,
            'allocation':allocation,'resource_policy':policy.receipt(),'source_sha256':SOURCE,'full_config_sha256':CONFIG,
            'qualification_not_yet_validated_by_probe_wrapper':{'path':str(args.qualification),'sha256':args.qualification_sha256},
            'root_registered_native_accounting_remains_required':True,**FALSE_FLAGS})
        return 0
    except subprocess.TimeoutExpired as error:
        exclusive(SUPPORT/'receipt.json',{'status':'pending_native_read_no_probe_started','reason':str(error),
            'native_job_id':job,'allocation':allocation,'resource_policy':policy.receipt(),'source_sha256':SOURCE,
            'full_config_sha256':CONFIG,'native_query_retried_by_prefix':False,**FALSE_FLAGS});return 0
    observed=repair._repair_native_states([args.qualification_job_id],{},owner='pwa209',account='def-ptewarie_cpu')
    qualification=context.qualification_proof(args.qualification,args.qualification_sha256,args.qualification_job_id,observed)
    with context.resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=8,job_id=job,resource_policy=policy):
        quota=runpy.run_path(str(context.QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,
            required_bytes=10_000_000_000,reserve_files=248000,reserve_bytes=2_000_000_000_000,
            evidence_dir=SUPPORT/'quota-reads',allocation_deadline_unix=allocation['allocation_end_unix'])
        try:storage=context.storage_proof(config,quota)
        except context.PendingStorage as error:
            exclusive(SUPPORT/'receipt.json',{'status':'pending_storage_no_probe_started','reason':str(error),'native_job_id':job,**FALSE_FLAGS});return 0
        context_qualification=qualify_private(args.context_tests,args.context_tests_sha256,allocation['allocation_end_unix'],suite='context')
        private_qualification=qualify_private(args.tests,args.tests_sha256,allocation['allocation_end_unix'])
        exclusive(SUPPORT/'input-bindings.json',{'allocation':allocation,'qualification':qualification,'storage':storage,'resource_policy':policy.receipt(),
            'private_qualification':private_qualification,
            'context_private_qualification':context_qualification,
            'registration_protection':registration,
            'context_helper':{'path':str(args.context_helper),'sha256':CONTEXT_SHA,'operational_rebinding':{'CASE':str(CASE),'SUPPORT':str(SUPPORT)}},
            'profile_sha256':PROFILE_SHA,'helper_sha256':digest(Path(__file__)),**FALSE_FLAGS})
        from horsehair.rna_recovery import _primary_environment
        seconds=prefix_runtime_seconds(os.environ['SLURM_JOB_END_TIME']);deadline=time.monotonic()+seconds
        CASE.mkdir(mode=0o700)
        environment=_primary_environment({'primary_release':str(PRIMARY),'maintenance_release':str(FRAMEWORK),'root':str(ROOT)})
        environment.update(OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
        command=[sys.executable,'-s','-u','-c',CHILD,str(PRIMARY),str(FRAMEWORK),str(Path(__file__).resolve()),str(ROOT),str(CASE),str(deadline)]
        exclusive(CASE/'started.json',{'command':command,'maximum_worker_seconds':seconds,'native_job_id':job,
            'primary_source_sha256':PRIMARY_SHA,'framework_source_sha256':SOURCE,'full_config_sha256':CONFIG,'helper_sha256':digest(Path(__file__)),**FALSE_FLAGS})
        result=supervise(command,environment,deadline,CASE)
        source_bindings(PRIMARY,FRAMEWORK)
        require(digest(args.context_helper)==CONTEXT_SHA and digest(args.qualification)==args.qualification_sha256
            and digest(args.resource_policy)==args.resource_policy_sha256,'Prerequisite changed during full-cohort prefix')
        result.update(native_job_id=job,case=str(CASE),qualification=qualification,allocation=allocation,
            private_qualification=private_qualification,
            context_private_qualification=context_qualification,
            source_sha256=SOURCE,primary_source_sha256=PRIMARY_SHA,full_config_sha256=CONFIG,resource_policy=policy.receipt(),
            pending=['Complete optimizer convergence and valid original selection criterion','Other models/folds/starts/rates/bin widths','Full optimizer convergence/LBFGS',
                     'Three-state27-context sensitivity','GPU/occupancy/PIT/Hessian/MALA/recovery/scientific admission'])
        exclusive(CASE/'receipt.json',result);exclusive(SUPPORT/'receipt.json',result)
        print(json.dumps({'status':result['status'],'native_job_id':job,'receipt':str(SUPPORT/'receipt.json'),**FALSE_FLAGS},sort_keys=True))
        return 1 if result['status'] in {'worker_failed','worker_termination_unconfirmed'} else 0


if __name__=='__main__':raise SystemExit(main())
