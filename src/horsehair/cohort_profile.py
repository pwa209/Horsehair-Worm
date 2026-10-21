"""Bounded, full-clock grouped-fit profiling of the unchanged primary objective.

This is technical recording-time cost evidence. It does not supply exposure
metadata, choose a scientific model, or grant whole-study resource admission.
"""
from __future__ import annotations

import argparse
import copy
from dataclasses import asdict
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any

from .core import atomic_json, canonical_hash, digest, native_job_id, source_identity

PRIMARY_SHA = "0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630"
PERSONAL_ROOT = Path('/scratch/pwa209/horsehair-worm')
PENDING = ["Remaining grouped folds, models, learning rates and sensitivity widths",
           "Recovery refits and fitted intervals", "Posterior curvature and MALA",
           "Mantis validation/bootstrap and source-validated H2 calibration",
           "Complete workload runtime, output storage and native admission",
           "Cross-device whole-objective/gradient equivalence and GPU profiles"]


def require(condition: bool, message: str) -> None:
    if not condition: raise ValueError(message)


def _locations(config, root, output, *, fresh):
    """Bind resolved personal scratch and reject redirected output ancestors."""
    root, original = Path(root).resolve(), Path(output).absolute()
    require(root == PERSONAL_ROOT.resolve() == Path(config['paths']['root']).resolve()
            and root.is_dir(), 'Exact configured personal scratch root required')
    resolved = original.resolve()
    namespace = root/'maintenance'
    require(resolved.is_relative_to(namespace) and resolved != namespace,
            'Profile output must be a fresh personal maintenance case')
    require(not original.is_symlink(), 'Profile output must not be a symlink')
    reached_root = False
    for ancestor in original.parents:
        if ancestor.resolve() == root:
            reached_root = True
            break
        require(not ancestor.is_symlink(), 'Profile output has a redirected ancestor')
    require(reached_root, 'Profile output does not descend from the personal root')
    require(not resolved.exists() if fresh else resolved.is_dir(),
            'Fresh profile output required' if fresh else 'Existing bounded worker output required')
    return root, resolved


def grouped_profile_split(roster: list[dict[str, Any]], inner_folds: int = 3) -> dict[str, Any]:
    """Freeze an entire nested fold by source-key hashes, never outcomes."""
    require(bool(roster) and len({r["animal_id"] for r in roster}) == len(roster), "Unique complete recording roster required")
    require(all(type(r.get("observed_any")) is bool for r in roster), "Original observed-mask statistics required")
    retained = [r for r in roster if r["observed_any"]]
    experiments = sorted({r["experiment_id"] for r in retained})
    require(len(experiments) >= 3 and type(inner_folds) is int and inner_folds >= 2, "Three recording groups and grouped inner folds required")
    key = lambda value: hashlib.sha256(("full-cohort-profile-v1:" + value).encode()).hexdigest()
    outer = min(experiments, key=key)
    available = [g for g in experiments if g != outer]
    k = min(inner_folds, len(available))
    index = min(range(k), key=lambda value: key(outer+":inner:"+str(value)))
    validation = set(available[index::k])
    train = [r["animal_id"] for r in retained if r["experiment_id"] != outer and r["experiment_id"] not in validation]
    valid = [r["animal_id"] for r in retained if r["experiment_id"] in validation]
    return {"outer_experiment": outer, "inner_index": index, "inner_folds": k,
        "training_ids": train, "validation_ids": valid,
        "outer_holdout_ids": [r["animal_id"] for r in retained if r["experiment_id"] == outer],
        "excluded_all_missing_ids": [r["animal_id"] for r in roster if not r["observed_any"]],
        "selection": "Minimum SHA256 source-key order for outer group and complete inner fold; all eligible fold members retained",
        "biological_identity_claim": False}


def recording_roster(metadata) -> list[dict[str, Any]]:
    """Inspect original Parquet statistics without reading or selecting outcomes."""
    roster = []
    for index in range(metadata.num_row_groups):
        group, fields = metadata.row_group(index), {}
        for name in ('animal_id', 'experiment_id', 'analysis_role', 'observed'):
            columns = [group.column(j) for j in range(group.num_columns)
                       if group.column(j).path_in_schema == name]
            require(len(columns) == 1 and columns[0].statistics is not None,
                    'Complete native identity/mask statistics required')
            stats = columns[0].statistics
            require(stats.min is not None and stats.max is not None, 'Empty native identity/mask statistics')
            if name == 'observed':
                require(type(stats.max) is bool, 'Native observed-mask must be boolean')
                fields['observed_any'] = stats.max
            else:
                require(stats.min == stats.max, 'Mixed source identity row group')
                fields[name] = str(stats.min)
        if fields['analysis_role'] == 'behavioural_development':
            require(group.num_rows >= 2, 'Incomplete native source clock')
            roster.append(dict(fields, row_group=index, bins=group.num_rows))
    require(bool(roster), 'Native development source roster is empty')
    return roster


class Timings:
    """Synchronize devices and keep wall/CPU totals separate from allocated cost."""
    def __init__(self, emit=None, device="cpu"):
        self.totals: dict[str, dict[str, float]] = {}
        self.emit, self.device = emit, device

    def sync(self):
        if str(self.device).startswith("cuda"):
            import torch
            torch.cuda.synchronize(self.device)

    def call(self, phase, callback, **unit):
        if self.emit: self.emit({"event": "started", "phase": phase, **unit, "started_unix": time.time()})
        self.sync(); wall, cpu = time.perf_counter(), time.process_time()
        result = callback()
        self.sync(); elapsed, used = time.perf_counter()-wall, time.process_time()-cpu
        record = self.totals.setdefault(phase, {"calls": 0, "wall_seconds": 0., "cpu_seconds": 0.})
        record["calls"] += 1; record["wall_seconds"] += elapsed; record["cpu_seconds"] += used
        if self.emit: self.emit({"event": "completed", "phase": phase, **unit,
                                  "wall_seconds": elapsed, "cpu_seconds": used, "totals": self.totals})
        return result


def objective_gradient(model, sequences, normalizer: int, timings: Timings | None = None):
    """Accumulate a full cohort gradient, one prior and one global normalizer."""
    import torch
    timer = timings or Timings(device=next(model.parameters()).device)
    require(normalizer == sum(int(s.observed.sum()) for s in sequences) and normalizer > 0,
            "The global observed-bin normalizer must include the complete training fold")
    model.zero_grad()
    prior = timer.call("prior_forward", lambda: -model.log_prior()/normalizer)
    timer.call("prior_backward", prior.backward)
    value = prior.detach().clone()
    for sequence in sequences:
        loss = timer.call("likelihood_forward", lambda: -model(sequence)["log_likelihood"].sum()/normalizer,
                          source_id=sequence.animal_id, clock_bins=len(sequence.times))
        if not torch.isfinite(loss): raise FloatingPointError("Nonfinite original full-clock likelihood")
        timer.call("likelihood_backward", loss.backward, source_id=sequence.animal_id, clock_bins=len(sequence.times))
        value = value + loss.detach()
    require(all(p.grad is None or bool(torch.isfinite(p.grad).all()) for p in model.parameters()), "Nonfinite full-cohort gradient")
    return value


def adam_epoch(model, sequences, optimizer, normalizer, clip, timings=None):
    import torch
    timer = timings or Timings(device=next(model.parameters()).device)
    value = objective_gradient(model, sequences, normalizer, timer)
    norm = timer.call("gradient_clip_once", lambda: torch.nn.utils.clip_grad_norm_(model.parameters(), clip))
    timer.call("optimizer_update_once", optimizer.step)
    return value, float(norm.detach().cpu())


def profile_fit(train, validation, kind, settings, *, emit=None, checkpoint=None, device="cpu"):
    """Instrument the primary MAP optimizer without shortening any fit budget.

    Completed epoch checkpoints preserve Adam state and RNG. Interruption inside
    the original single LBFGS.step is reported as partial; its local search state
    is not misrepresented as an exact resumable optimizer checkpoint.
    """
    import numpy as np
    import torch
    from . import semimarkov as s
    require(device == 'cpu', 'GPU timing remains pending whole-objective cross-device qualification')
    require(bool(train), 'Complete nonempty training fold required')
    for sequence in [*train, *validation]: sequence.validate()
    s.assert_disjoint(train, validation, experiment=True)
    require(kind in {"M0", "MG", "MT", "MD", "MX", "HMM"}, "Initial profiler covers interpretable models and HMM only; AR/GRU remain declared pending")
    require(settings.get("max_duration_bins") is None, "Initial profile must retain every duration age")
    torch.set_num_threads(1)
    timer = Timings(emit, device)
    scaler = timer.call("training_scaler", lambda: s.TrainingScaler.fit(train))
    sequences = timer.call("training_transform", lambda: [scaler.transform(q) for q in train])
    knots = timer.call("training_knots", lambda: s.training_duration_knots(train))
    observed = timer.call("training_output_scale", lambda: np.concatenate([q.observations[q.observed] for q in train]))
    scale = timer.call('training_output_std', lambda: np.maximum(np.std(observed, axis=0), 1e-4))
    normalizer = sum(int(q.observed.sum()) for q in sequences)
    require(normalizer > 0 and len({q.animal_id for q in train}) == len(train), "Unique observed training clocks required")
    if emit: emit({'event': 'training_preprocessing_complete', 'scaler': asdict(scaler),
        'duration_knots': knots.tolist(), 'normalizer': normalizer, 'output_scale': scale.tolist(),
        'training_clocks': len(train), 'validation_clocks': len(validation),
        'training_bins': sum(len(q.times) for q in train), 'validation_bins': sum(len(q.times) for q in validation)})
    candidates, records = [], []
    for start in range(int(settings.get("starts", 5))):
        torch.manual_seed(int(settings.get("seed", 0))+start)
        model = s.MovementModel("MX" if kind == "HMM" else kind,
            len(scaler.mean), len(scaler.modulation_mean), train[0].observations.shape[1], knots,
            [q.animal_id for q in train], [q.experiment_id for q in train],
            states=int(settings.get("states", 2)), max_duration_bins=None,
            bin_seconds=train[0].bin_seconds).to(device)
        if kind == "HMM":
            with torch.no_grad(): model.duration_beta.zero_()
            model.duration_beta.requires_grad_(False)
        with torch.no_grad():
            model.log_rest_scale.copy_(model._tensor(np.log(scale/3)))
            for name, parameter in model.named_parameters():
                if parameter.requires_grad and name not in ("log_rest_scale", "log_animal_sd", "log_experiment_sd"):
                    parameter.add_(torch.randn_like(parameter)*.05)
        if emit: emit({'event': 'start_initialized', 'start': start, 'seed': int(settings.get('seed', 0))+start,
            'parameter_count': sum(p.numel() for p in model.parameters()),
            'trainable_parameter_count': sum(p.numel() for p in model.parameters() if p.requires_grad),
            'parameter_shapes': {n: list(p.shape) for n,p in model.named_parameters()},
            'dtype': str(next(model.parameters()).dtype), 'torch_threads': torch.get_num_threads()})
        optimizer = torch.optim.Adam(model.parameters(), lr=float(settings.get("learning_rate", .001)))
        best, stale, best_state = math.inf, 0, None
        history = []
        for epoch in range(int(settings.get("max_epochs", 500))):
            objective, gradient_norm = adam_epoch(model, sequences, optimizer, normalizer,
                float(settings.get("gradient_clip", 5)), timer)
            fit = s.Fit(model, scaler, {"tail_tolerance": settings.get("tail_tolerance", 1e-8)})
            grouped = {}
            for sequence in validation:
                score = timer.call("epoch_validation_full_clock", lambda: fit.score(sequence, settings.get("warmup_seconds", 600)),
                                   source_id=sequence.animal_id, clock_bins=len(sequence.times))["log_score"]
                grouped.setdefault(sequence.experiment_id, []).append(score)
            criterion = -float(np.mean([np.mean(v) for v in grouped.values()])) if validation else float(objective.cpu())
            require(math.isfinite(criterion), 'Nonfinite complete-fold validation criterion')
            history.append({"epoch": epoch, "objective": float(objective.cpu()), "criterion": criterion,
                            "finite_gradient": True, "gradient_norm_before_clip": gradient_norm})
            if criterion < best-float(settings.get("convergence_tolerance", 1e-7)):
                best, stale, best_state = criterion, 0, copy.deepcopy(model.state_dict())
            else: stale += 1
            if checkpoint:
                timer.call("checkpoint_io", lambda: checkpoint({"phase": "completed_adam_epoch", "start": start,
                    "next_epoch": epoch+1, "model_state": model.state_dict(), "optimizer_state": optimizer.state_dict(),
                    "best_state": best_state, "best_criterion": best, "stale": stale, "history": history,
                    "torch_rng_state": torch.get_rng_state(),
                    "exact_mid_lbfgs_resume_supported": False}))
            if emit: emit({"event": "epoch_complete", "start": start, **history[-1], "stale": stale})
            if stale >= int(settings.get("patience", 30)): break
        require(best_state is not None, "No finite primary optimizer solution")
        model.load_state_dict(best_state)
        iterations = int(settings.get("lbfgs_iterations", 200))
        if iterations:
            if checkpoint: checkpoint({"phase": "before_original_lbfgs_step", "start": start,
                "model_state": model.state_dict(), "torch_rng_state": torch.get_rng_state(),
                "exact_mid_lbfgs_resume_supported": False})
            refinement = torch.optim.LBFGS(model.parameters(), max_iter=iterations, line_search_fn="strong_wolfe")
            timer.call("original_lbfgs_step", lambda: refinement.step(lambda: objective_gradient(model, sequences, normalizer, timer)), start=start)
        with torch.no_grad():
            final_objective = timer.call("final_objective_full_cohort", lambda:
                float((-model.log_prior()-sum(model(q)["log_likelihood"].sum() for q in sequences)).cpu())/normalizer)
            tails = timer.call("final_tail_audit_full_cohort", lambda:
                [float(model(q)["max_tail_probability"].max().cpu()) for q in sequences])
        require(max(tails) <= float(settings.get("tail_tolerance", 1e-8)), "Full duration tail audit failed")
        records.append({"start": start, "epochs": len(history), "history": history,
            "objective": final_objective, "criterion": best, "max_tail_probability": max(tails)})
        candidates.append((final_objective, copy.deepcopy(model)))
        if emit: emit({"event": "start_complete", "record": records[-1]})
    candidates.sort(key=lambda value: value[0])
    return {"model": candidates[0][1], "scaler": scaler, "records": records, "timings": timer.totals,
            "normalizer": normalizer, "training_clocks": len(train), "validation_clocks": len(validation)}


def _worker(config_path: Path, root: Path, primary: Path, output: Path, kind: str, rate: float, device: str):
    import pyarrow.parquet as pq
    import torch
    import horsehair
    from . import behaviour as b, semimarkov as s
    from .behaviour import _process_peak_ram_bytes
    require(all(Path(m.__file__).resolve().is_relative_to(primary/'src') for m in (horsehair,b,s))
            and source_identity(primary)["sha256"] == PRIMARY_SHA, "Frozen primary numerical interpreter required")
    config=json.loads(config_path.read_text());root,output=_locations(config,root,output,fresh=False)
    settings=dict(config['behaviour'],modulation=['elapsed_hours'],learning_rate=rate)
    require(canonical_hash(config)==canonical_hash(json.loads((primary/'configs/study.json').read_text())), "Full scientific configuration changed")
    require(rate in settings.get('learning_rates',[.0003,.001,.003]) and float(settings.get('primary_bin_seconds',10))==10, "Declared primary10s rate required")
    require(device=='cpu' and os.environ.get('HHW_GPU_COUNT')=='0', "Initial native profile is CPU only")
    torch.set_num_threads(1)
    progress={"status":"loading_full_grouped_fold","primary_source_sha256":PRIMARY_SHA,
        "full_config_sha256":canonical_hash(config),"native_job_id":native_job_id(),"torch_threads":1,
        "allocated_cpus":int(os.environ['SLURM_CPUS_PER_TASK']),"device":device,"model":kind,"learning_rate":rate,
        'torch_version':str(torch.__version__),'python_version':sys.version,'frozen_optimizer_settings':settings,
        "scientific_result":False,"scientific_fit":False,"complete_declared_workload":False,"admission_authority":False,
        "pending":PENDING,"biological_time_interpretation":"Technical recording-relative time only; no exposure clock inferred"}
    def emit(event):
        progress['last_event']=event;progress['updated_unix']=time.time();progress['process_peak_ram_bytes']=_process_peak_ram_bytes()
        progress['process_cpu_seconds']=time.process_time()
        if 'totals' in event:progress['timings']=event['totals']
        if event['event']=='training_preprocessing_complete':progress['preprocessing']=event
        if event['event']=='start_initialized':progress['current_start']=event
        if event['event']=='epoch_complete':progress['last_completed_epoch']=event
        if event['event']=='start_complete':progress.setdefault('completed_starts',[]).append(event['record'])
        atomic_json(output/'progress.json',progress)
    timer=Timings(emit)
    path=root/'results/behaviour/fly_bins_10s.parquet';proof_path=root/'results/behaviour/streaming_bin_provenance.json'
    proof=json.loads(proof_path.read_text());input_sha=timer.call('input_hash',lambda:digest(path))
    require(proof['bin_files'][str(path)]==input_sha,"Full native bin provenance changed")
    pf=pq.ParquetFile(path);roster=recording_roster(pf.metadata)
    split=grouped_profile_split(roster,int(settings.get('inner_folds',3)))
    planned={'source_bin_path':str(path),'source_bin_sha256':input_sha,'provenance_path':str(proof_path),
        'provenance_sha256':digest(proof_path),'roster':roster,'split':split,'model':kind,'learning_rate':rate,
        'primary_source_sha256':PRIMARY_SHA,'full_config_sha256':canonical_hash(config),
        'profile_module_sha256':digest(Path(__file__)),'scientific_result':False,'admission_authority':False}
    atomic_json(output/'plan.json',planned);progress['plan_sha256']=digest(output/'plan.json');progress['split']=split
    selected=set(split['training_ids']+split['validation_ids']);sequences={}
    for row in roster:
        if row['animal_id'] not in selected:continue
        frame=timer.call('input_read',lambda:pf.read_row_group(row['row_group']).to_pandas(),source_id=row['animal_id'])
        seq=timer.call('input_sequence_conversion',lambda:b.to_sequences(frame,settings),source_id=row['animal_id'])
        require(len(seq)==1 and seq[0].animal_id==row['animal_id'] and len(seq[0].times)==row['bins'],"A whole native source clock was shortened or changed")
        sequences[row['animal_id']]=seq[0]
    train=[sequences[k] for k in split['training_ids']];validation=[sequences[k] for k in split['validation_ids']]
    require(all(bool((q.observed & (q.times-q.times[0]>=settings.get('warmup_seconds',600))).any()) for q in validation),
            'A complete declared validation clock lacks an observed prediction window after warm-up')
    progress['input_timings']=copy.deepcopy(timer.totals)
    progress.update(status='full_grouped_optimizer_running',training_clocks=len(train),validation_clocks=len(validation),
        training_bins=sum(len(q.times) for q in train),validation_bins=sum(len(q.times) for q in validation))
    def checkpoint(value):
        value.update(plan_sha256=progress['plan_sha256'],primary_source_sha256=PRIMARY_SHA,full_config_sha256=canonical_hash(config))
        temp=output/'checkpoint.pt.partial';torch.save(value,temp)
        with temp.open('rb') as stream:os.fsync(stream.fileno())
        os.replace(temp,output/'checkpoint.pt')
    result=profile_fit(train,validation,kind,settings,emit=emit,checkpoint=checkpoint,device=device)
    require(digest(path)==input_sha and digest(proof_path)==planned['provenance_sha256'],"Inputs changed during the full-cohort profile")
    require(digest(Path(__file__))==planned['profile_module_sha256'] and source_identity(primary)['sha256']==PRIMARY_SHA,
            'Profiling wrapper or frozen primary changed during execution')
    progress.update(status='declared_fold_profile_complete',timings=result['timings'],normalizer=result['normalizer'],
        parameter_count=sum(p.numel() for p in result['model'].parameters()),completed_unix=time.time(),
        process_cpu_seconds=time.process_time(),process_peak_ram_bytes=_process_peak_ram_bytes())
    atomic_json(output/'worker-result.json',progress);atomic_json(output/'progress.json',progress)


_CHILD="""import importlib.util,pathlib,sys
primary,wrapper,config,root,output,kind,rate,device=sys.argv[1:]
sys.path.insert(0,str(pathlib.Path(primary)/'src'))
import horsehair
if not pathlib.Path(horsehair.__file__).resolve().is_relative_to(pathlib.Path(primary).resolve()/'src'):raise RuntimeError('Primary package isolation failed')
spec=importlib.util.spec_from_file_location('horsehair._cohort_profile',wrapper)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
module._worker(pathlib.Path(config),pathlib.Path(root),pathlib.Path(primary),pathlib.Path(output),kind,float(rate),device)
"""


def run_profile(config_path: Path, root: Path, primary: Path, output: Path, *, kind="MX", rate=.001, device="cpu", max_seconds=6300):
    from .rna_recovery import _primary_environment
    from .behaviour_feasibility import runtime_limit
    job=native_job_id();primary=Path(primary).resolve()
    root,output=_locations(json.loads(Path(config_path).read_text()),root,output,fresh=True)
    require(device=='cpu' and os.environ.get('HHW_GPU_COUNT')=='0', 'Initial native full-cohort profiler is CPU only; GPU remains pending')
    require(job is not None and os.environ.get('SLURM_CPUS_PER_TASK')=='1', "Native full-cohort CPU profile requires exactly one CPU")
    require(not output.exists() and source_identity(primary)['sha256']==PRIMARY_SHA,"Fresh case and frozen primary source required")
    seconds=runtime_limit(max_seconds,allocation_end=os.environ.get('SLURM_JOB_END_TIME'))
    output.mkdir(parents=True,exist_ok=False)
    identity={'native_job_id':job,'framework_source_sha256':source_identity(Path(__file__).resolve().parents[2])['sha256'],
        'profile_module_sha256':digest(Path(__file__)),'primary_source_sha256':PRIMARY_SHA,
        'scientific_result':False,'scientific_fit':False,'complete_declared_workload':False,'admission_authority':False,
        'maximum_worker_seconds':seconds,'started_unix':time.time(),'device':device,'model':kind,'learning_rate':rate}
    atomic_json(output/'started.json',identity)
    env=_primary_environment({'primary_release':str(primary),'maintenance_release':str(Path(__file__).resolve().parents[2]),'root':str(root)})
    command=[sys.executable,'-s','-c',_CHILD,str(primary),str(Path(__file__).resolve()),str(Path(config_path).resolve()),str(Path(root).resolve()),str(output.resolve()),kind,str(rate),device]
    timed_out=False
    with (output/'stdout.log').open('w') as stdout,(output/'stderr.log').open('w') as stderr:
        process=subprocess.Popen(command,stdout=stdout,stderr=stderr,env=env,cwd=primary,start_new_session=os.name!='nt')
        try:code=process.wait(timeout=seconds)
        except subprocess.TimeoutExpired:
            timed_out=True
            if os.name=='nt':process.terminate()
            else:os.killpg(process.pid,signal.SIGTERM)
            try:code=process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                if os.name=='nt':process.kill()
                else:os.killpg(process.pid,signal.SIGKILL)
                code=process.wait(timeout=10)
    progress=json.loads((output/'progress.json').read_text()) if (output/'progress.json').exists() else None
    completed = code==0 and (output/'worker-result.json').exists() and progress is not None and progress.get('status')=='declared_fold_profile_complete'
    require(digest(Path(__file__))==identity['profile_module_sha256'] and source_identity(primary)['sha256']==PRIMARY_SHA,
            'Profiling wrapper or primary source changed during the bounded worker')
    receipt=dict(identity,status='bounded_deadline_reached' if timed_out else 'declared_fold_profile_complete' if completed else 'worker_failed',
        worker_returncode=code,progress=progress,completed_unix=time.time(),pending=PENDING,
        output_sha256={p.name:digest(p) for p in output.iterdir() if p.is_file() and p.name!='receipt.json'})
    atomic_json(output/'receipt.json',receipt)
    return receipt


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('config','root','primary-release','output-dir'):p.add_argument('--'+name,required=True,type=Path)
    p.add_argument('--model',default='MX');p.add_argument('--learning-rate',default=.001,type=float)
    p.add_argument('--device',choices=['cpu'],default='cpu');p.add_argument('--max-seconds',type=float,default=6300)
    a=p.parse_args(argv);result=run_profile(a.config,a.root,a.primary_release,a.output_dir,kind=a.model,rate=a.learning_rate,device=a.device,max_seconds=a.max_seconds)
    require(all(result.get(key) is False for key in ('scientific_result','scientific_fit','complete_declared_workload','admission_authority')),
            'A technical profile cannot claim scientific or admission authority')
    require(result.get('status') in {'worker_failed','bounded_deadline_reached','declared_fold_profile_complete'},
            'Unrecognized bounded worker receipt status')
    require((a.output_dir/'receipt.json').is_file() and json.loads((a.output_dir/'receipt.json').read_text())==result,
            'CLI requires the exact persisted bounded worker receipt')
    print(json.dumps({'status':result['status'],'technical_partial':result['status']=='bounded_deadline_reached',
        'scientific_result':False,'scientific_fit':False,'complete_declared_workload':False,'admission_authority':False},sort_keys=True))
    return 1 if result['status']=='worker_failed' else 0


if __name__=='__main__':raise SystemExit(main())
