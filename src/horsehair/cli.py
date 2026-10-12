"""Explicit stage dispatch; unavailable scientific evidence is never manufactured."""
from __future__ import annotations
import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from .core import atomic_json, canonical_hash, environment_record, read_json, source_identity, validate_config


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
                'elapsed_seconds':time.monotonic()-started,'loss':float(loss.detach().cpu())}
        atomic_json(root/'state/cuda-qualification.json',record)
    else:
        command=['python','-m','pytest',str(source/'tests'),'-q','--basetemp',str(root/'tests'/('qualification-'+os.environ.get('SLURM_JOB_ID','local')))]
        response=subprocess.run(command,cwd=source,text=True,capture_output=True)
        record={'status':'complete' if response.returncode==0 else 'failed','returncode':response.returncode,
                'stdout':response.stdout,'stderr':response.stderr,'environment':environment_record(),
                'evidence_class':'software qualification; not biological evidence'}
        atomic_json(root/'state/cpu-qualification.json',record)
        if response.returncode:raise RuntimeError('Native implementation qualification failed; inspect receipt')
    print(json.dumps(record))
    return record


def source_audit(config,root):
    from . import acquire
    root=Path(root)
    manifest=read_json(root/'manifests/source_manifest.json')
    acquire.validate_manifest(manifest)
    extracted=[]
    for item in manifest['assets']:
        path=acquire.safe_destination(root,item['relative_path'])
        if item.get('eligible') is False:continue
        if not path.exists():raise FileNotFoundError(f'Mandatory source missing: {path}')
        if path.suffix.lower()=='.zip':
            destination=root/'data/extracted'/item['source_id']
            extracted.extend(acquire.safe_extract(path,destination,max_unpacked_bytes=400_000_000_000))
    inventory=acquire.inventory(root/'data/extracted',root/'manifests/source_inventory.json')
    from . import behaviour
    if hasattr(behaviour,'discover_fly_sources'):
        discovered=behaviour.discover_fly_sources(root/'data/extracted/fly')
        atomic_json(root/'manifests/fly_adapters.json',discovered)
    report={'status':'source_inventory_complete','source_manifest_hash':manifest['manifest_hash'],
            'extracted_files':len(extracted),'inventoried_files':len(inventory['files']),
            'scientific_fit_ready':False,'evidence_class':'source audit'}
    atomic_json(root/'state/source-audit.json',report)
    return report


def assess_pilot(config,root):
    root=Path(root)
    # Do not promote a source inventory or shortened training run into a final fit.
    reports={}
    for key,path in [('cpu',root/'state/cpu-qualification.json'),('cuda',root/'state/cuda-qualification.json'),('source',root/'state/source-audit.json')]:
        reports[key]=read_json(path) if path.exists() else {'status':'pending'}
    agent_files=list((root/'results/agents/pilot').rglob('*.json')) if (root/'results/agents/pilot').exists() else []
    report={'status':'pilot_assessment','reports':reports,'agent_receipt_files':[str(p.relative_to(root)) for p in agent_files],
            'final_scientific_evidence_complete':False,
            'next':['two-ant-library RAM/runtime/storage benchmark','two-fly-experiment preprocessing/model benchmark','complete-agent throughput/competence benchmark','measured phase admission']}
    atomic_json(root/'state/pilot-assessment.json',report)
    return report


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
    sub.add_parser('assess-pilot')
    for name in ['behaviour','interventions']:
        s=sub.add_parser(name);s.add_argument('--stage',required=True)
    agents=sub.add_parser('agents');agents.add_argument('--mode',required=True);agents.add_argument('--task-index',type=int)
    planner=sub.add_parser('slurm-plan');planner.add_argument('--release',type=Path,required=True);planner.add_argument('--output',type=Path,required=True);planner.add_argument('--phase',default='initial')
    submit=sub.add_parser('submit');submit.add_argument('--plan',type=Path,required=True);submit.add_argument('--ledger',type=Path,required=True);submit.add_argument('--environment-job')
    sub.add_parser('report')
    a=p.parse_args(argv)
    config=read_json(a.config)
    validate_config(config)
    root=a.root or Path(config['paths']['root'])
    numerical={'qualify','source-audit','assess-pilot','behaviour','interventions','agents','report'}
    if os.environ.get('SLURM_JOB_ID') and a.command in numerical:
        from .budget import resource_lease
        job_id=os.environ['SLURM_JOB_ID']
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
    elif a.command=='assess-pilot':result=assess_pilot(config,root)
    elif a.command in ['behaviour','interventions']:
        import importlib
        module=importlib.import_module('horsehair.'+a.command)
        result=module.run(config,root,a.stage)
    elif a.command=='agents':
        from .agents import run
        result=run(config,root,mode=a.mode,task_index=a.task_index)
    elif a.command=='slurm-plan':
        from .slurm import plan
        result=plan(config,a.release,a.output,a.phase)
    elif a.command=='submit':
        from .slurm import submit
        result=submit(a.plan,a.ledger,a.environment_job)
    else:
        from .report import build_report
        result=build_report(config,root)
    if result is not None:print(json.dumps(result,default=str))
    return 0


if __name__=='__main__':raise SystemExit(main())

