"""Finite Slurm plans, measured admission, submission leases, and accounting."""
from __future__ import annotations
import json
import os
import re
import shlex
import subprocess
from pathlib import Path
from .core import atomic_json, canonical_hash, read_json, source_identity, validate_config


def _script(config, release, name, command, *, cpus=4, memory='16G', hours=2, gpu=False, array=None, network=False):
    root=config['paths']['root']
    account=config['paths']['gpu_account' if gpu else 'cpu_account']
    lines=['#!/bin/bash',f'#SBATCH --account={account}',f'#SBATCH --job-name=hhw-{name}',
           f'#SBATCH --cpus-per-task={cpus}',f'#SBATCH --mem={memory}',f'#SBATCH --time={hours}:00:00',
           f'#SBATCH --output={root}/logs/{name}-%A_%a.log']
    if gpu:lines += ['#SBATCH --gpus=a100:1']
    if array:lines += [f'#SBATCH --array={array}']
    lines += ['set -euo pipefail','test "$(id -un)" = pwa209',
              'module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1',
              f'source {shlex.quote(config["paths"]["environment"]+"/bin/activate")}',
              f'cd {shlex.quote(str(release))}',
              f'export PYTHONPATH={shlex.quote(str(Path(release)/"src"))}',
              'export MPLBACKEND=Agg', 'export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}',
              'export OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK}',
              'export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}',
              'export PYTHONUNBUFFERED=1',f'export HHW_GPU_COUNT={int(gpu)}',f'export HHW_MAX_HOURS={hours}']
    if network:lines += ['module load httpproxy/1.0']
    lines += [f'python -m horsehair.cli --config configs/study.json --root {shlex.quote(root)} verify-source --identity release.json',command]
    return '\n'.join(lines)+'\n'


def plan(config, release: Path, output: Path, phase='initial'):
    validate_config(config)
    output=Path(output)
    output.mkdir(parents=True,exist_ok=False)
    release=Path(release)
    root=config['paths']['root']
    prefix=f'python -m horsehair.cli --config configs/study.json --root {shlex.quote(root)}'
    tasks=[]
    def add(name,command,depends=(),**resources):
        filename=output/(name+'.sbatch')
        text=_script(config,release,name,command,**resources)
        filename.write_text(text,encoding='utf8',newline='\n')
        tasks.append({'name':name,'script':str(filename),'script_sha256':canonical_hash(text),'depends':list(depends),'resources':resources})
    if phase=='initial':
        add('qualification',prefix+' qualify',cpus=4,memory='16G',hours=1)
        add('discovery',f'python -m horsehair.acquire discover --output {root}/manifests/source_manifest.json',depends=['qualification'],cpus=2,memory='8G',hours=1,network=True)
        add('acquisition',f'python -m horsehair.acquire download --manifest {root}/manifests/source_manifest.json --root {root}/raw',depends=['discovery'],cpus=2,memory='8G',hours=24,network=True)
        add('source-audit',prefix+' source-audit',depends=['acquisition'],cpus=4,memory='32G',hours=4)
        add('gpu-qualification',prefix+' qualify --gpu',depends=['qualification'],cpus=4,memory='32G',hours=1,gpu=True)
        add('agent-pilot',prefix+' agents --mode pilot',depends=['gpu-qualification'],cpus=6,memory='64G',hours=2,gpu=True)
        add('pilot-assessment',prefix+' assess-pilot',depends=['source-audit','agent-pilot'],cpus=2,memory='8G',hours=1)
    elif phase=='agents-development':
        require_admission(config,Path(root),'agents-development')
        add('agent-development',prefix+' agents --mode development --task-index "$SLURM_ARRAY_TASK_ID"',array='0-44%1',cpus=6,memory='64G',hours=4,gpu=True)
        add('agent-freeze',prefix+' agents --mode freeze',depends=['agent-development'],cpus=2,memory='8G',hours=1)
    elif phase=='agents-final':
        require_admission(config,Path(root),'agents-final')
        add('agent-final',prefix+' agents --mode final --task-index "$SLURM_ARRAY_TASK_ID"',array='0-179%1',cpus=6,memory='64G',hours=2,gpu=True)
        add('agent-evaluation',prefix+' agents --mode evaluate',depends=['agent-final'],cpus=6,memory='64G',hours=24,gpu=True)
    elif phase=='behaviour':
        require_admission(config,Path(root),'behaviour')
        add('behaviour-recovery',prefix+' behaviour --stage recover',cpus=8,memory='32G',hours=24)
        add('behaviour-fit',prefix+' behaviour --stage fit',depends=['behaviour-recovery'],cpus=8,memory='64G',hours=48)
        add('intervention-challenge',prefix+' interventions --stage challenge',depends=['behaviour-fit'],cpus=8,memory='32G',hours=24)
    else:
        raise ValueError(f'Unknown finite phase {phase}')
    record={'schema_version':1,'phase':phase,'config_sha256':canonical_hash(config),'release_identity':source_identity(release),'tasks':tasks,
            'scope':'initial source/pilot execution' if phase=='initial' else 'admitted scientific work',
            'cpu_ceiling':config['resources']['cpu_ceiling'],'gpu_concurrency':1}
    atomic_json(output/'plan.json',record)
    return record


def require_admission(config,root,phase):
    path=Path(root)/'state'/f'admission-{phase}.json'
    if not path.exists():raise ValueError(f'Measured phase admission required: {path}')
    record=read_json(path)
    if record.get('config_sha256')!=canonical_hash(config) or record.get('status')!='admitted':
        raise ValueError('Invalid measured admission or changed scientific config')
    if record['projected_cpu_core_hours']>config['resources']['cpu_core_hours_cap'] or record['projected_gpu_hours']>config['resources']['gpu_hours_cap']:
        raise ValueError('Projected study workload exceeds budget')


def submit(plan_path,ledger_path,environment_job=None,runner=subprocess.run):
    p=read_json(plan_path)
    ledger_path=Path(ledger_path)
    lock=ledger_path.with_suffix('.lock')
    lock.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(lock,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    try:
        os.close(fd)
        ledger=read_json(ledger_path) if ledger_path.exists() else {'plan_sha256':canonical_hash(p),'tasks':{}}
        if ledger['plan_sha256']!=canonical_hash(p):raise ValueError('Submission ledger belongs to another plan')
        for task in p['tasks']:
            existing=ledger['tasks'].get(task['name'])
            if existing:
                if existing.get('state')=='submitted':continue
                raise RuntimeError('Ambiguous/rejected prior submission: inspect native records; do not replay')
            script=Path(task['script'])
            if canonical_hash(script.read_text(encoding='utf8'))!=task['script_sha256']:
                raise ValueError('Slurm script changed after plan freeze')
            dependencies=[ledger['tasks'][n]['job_id'] for n in task['depends']]
            if environment_job and not task['depends']:dependencies.append(str(environment_job))
            command=['sbatch','--parsable']
            if dependencies:command += ['--dependency=afterok:'+':'.join(dependencies)]
            command += [str(script)]
            ledger['tasks'][task['name']]={'state':'submitting','command':command}
            atomic_json(ledger_path,ledger)
            response=runner(command,text=True,capture_output=True)
            job_id=response.stdout.strip().split(';')[0]
            if response.returncode!=0 or not re.fullmatch(r'\d+',job_id):
                ledger['tasks'][task['name']].update(state='ambiguous_or_rejected',stdout=response.stdout,stderr=response.stderr)
                atomic_json(ledger_path,ledger)
                raise RuntimeError(f'Unconfirmed submission: {task["name"]}')
            ledger['tasks'][task['name']].update(state='submitted',job_id=job_id)
            atomic_json(ledger_path,ledger)
        return ledger
    finally:
        lock.unlink(missing_ok=True)

