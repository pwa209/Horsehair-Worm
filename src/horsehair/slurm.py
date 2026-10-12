"""Finite Slurm plans, measured admission, submission leases, and accounting."""
from __future__ import annotations
import json
import os
import re
import shlex
import subprocess
from pathlib import Path
from .core import atomic_json, canonical_hash, read_json, source_identity, validate_config


def _script(config, release, name, command, *, cpus=4, memory='16G', hours=2, gpu=False, array=None, network=False,modules=()):
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
              'export PYTHONUNBUFFERED=1','export CUBLAS_WORKSPACE_CONFIG=:4096:8',f'export HHW_GPU_COUNT={int(gpu)}',f'export HHW_MAX_HOURS={hours}']
    if network:lines += ['module load httpproxy/1.0']
    if modules:lines += ['module load '+' '.join(shlex.quote(x) for x in modules)]
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
    admission={}
    rna_modules=['star/2.7.11b','fastqc/0.12.1','trimmomatic/0.39','subread/2.0.6','samtools/1.20']
    def add(name,command,depends=(),external_deps=(),**resources):
        measured=admission.get('allocation_parameters',{}).get('rna-full' if name=='rna-counts' else name)
        if measured:
            measured=dict(measured);concurrency=measured.pop('concurrency')
            resources.update(measured)
            if resources.get('array'):
                original=int(resources['array'].split('%')[1])
                resources['array']=resources['array'].split('%')[0]+'%'+str(min(original,concurrency))
        filename=output/(name+'.sbatch')
        text=_script(config,release,name,command,**resources)
        filename.write_text(text,encoding='utf8',newline='\n')
        tasks.append({'name':name,'script':str(filename),'script_sha256':canonical_hash(text),'depends':list(depends),'external_dependencies':list(external_deps),'resources':resources})
    def initial_job(name):
        identity=read_json(release/'release.json')
        ledger=read_json(Path(root)/'state'/(identity['release_id']+'-initial-submission.json'))
        task=ledger['tasks'][name]
        if task['state']!='submitted':raise ValueError('Required initial stage has no confirmed native job')
        return task['job_id']
    if phase=='initial':
        add('qualification',prefix+' qualify',cpus=4,memory='16G',hours=1)
        add('r-qualification',prefix+' r-qualify',depends=['qualification'],cpus=2,memory='8G',hours=1,modules=['r/4.4.0','r-bundle-bioconductor/3.20'])
        add('transfer-wait-tools',prefix+' wait-transfer --group tools',cpus=1,memory='4G',hours=24)
        add('molecular-runtime',prefix+' molecular --stage runtime',depends=['transfer-wait-tools','r-qualification'],cpus=8,memory='32G',hours=2,modules=['perl/5.36.1','r/4.4.0','r-bundle-bioconductor/3.20'])
        for group in ['fly','rna']:
            add('transfer-wait-'+group,prefix+' wait-transfer --group '+group,cpus=1,memory='4G',hours=24)
            add('source-verification-'+group,prefix+' verify-sources --group '+group,depends=['transfer-wait-'+group,'qualification'],cpus=4,memory='16G',hours=8)
        add('source-audit',prefix+' source-audit',depends=['source-verification-fly'],cpus=4,memory='64G',hours=8)
        add('rna-reference',prefix+' rna-pilot --prepare-only',depends=['source-verification-rna'],cpus=8,memory='64G',hours=4,modules=rna_modules)
        add('molecular-pilot',prefix+' molecular-pipeline --stage benchmark-primary',depends=['rna-reference','molecular-runtime'],cpus=8,memory='64G',hours=8)
        for index in [0,1]:
            add('rna-pilot-'+str(index),prefix+' rna-pilot --index '+str(index),depends=['rna-reference'],cpus=8,memory='64G',hours=24,modules=rna_modules)
        add('fly-pilot',prefix+' behaviour --stage pilot',depends=['source-audit'],cpus=8,memory='64G',hours=4)
        add('gpu-qualification',prefix+' qualify --gpu',depends=['qualification'],cpus=4,memory='32G',hours=1,gpu=True)
        add('agent-pilot',prefix+' agents --mode pilot',depends=['gpu-qualification'],cpus=6,memory='64G',hours=2,gpu=True)
        add('agent-cpu-pilot',prefix+' agents --mode cpu-evaluation-pilot',depends=['agent-pilot'],cpus=4,memory='32G',hours=2)
        add('pilot-assessment',prefix+' assess-pilot --advance',depends=['fly-pilot','rna-pilot-0','rna-pilot-1','agent-cpu-pilot'],cpus=2,memory='8G',hours=1)
    elif phase=='agents-development':
        admission=require_admission(config,Path(root),'agents-development')
        cells=3*(3+int(config['agents'].get('feedforward_baseline',True)))*5
        add('agent-development',prefix+' agents --mode development --task-index "$SLURM_ARRAY_TASK_ID"',array=f'0-{cells-1}%1',cpus=6,memory='64G',hours=4,gpu=True)
        add('agent-freeze',prefix+' agents --mode freeze',depends=['agent-development'],cpus=2,memory='8G',hours=1)
    elif phase=='agents-final':
        admission=require_admission(config,Path(root),'agents-final')
        cells=3*(3+int(config['agents'].get('feedforward_baseline',True)))*20
        add('agent-final',prefix+' agents --mode final --task-index "$SLURM_ARRAY_TASK_ID"',array=f'0-{cells-1}%1',cpus=6,memory='64G',hours=2,gpu=True)
        add('agent-evaluation',prefix+' agents --mode evaluate --task-index "$SLURM_ARRAY_TASK_ID"',array=f'0-{cells-1}%8',depends=['agent-final'],cpus=4,memory='32G',hours=12)
        add('agent-analysis',prefix+' agent-analysis',depends=['agent-evaluation'],cpus=4,memory='32G',hours=12)
    elif phase=='rna-full':
        admission=require_admission(config,Path(root),'rna-full')
        add('rna-full-plan',prefix+' molecular --stage prepare-rna-full',cpus=2,memory='8G',hours=1)
        add('rna-counts',prefix+' molecular --stage rna-run --task-index "$SLURM_ARRAY_TASK_ID"',depends=['rna-full-plan'],array='0-59%2',cpus=8,memory='64G',hours=24,modules=rna_modules)
        add('ant-phase',prefix+' molecular --stage ant',depends=['rna-counts'],cpus=8,memory='32G',hours=8,modules=['r/4.4.0','r-bundle-bioconductor/3.20'])
        add('ant-score-freeze',prefix+' molecular-pipeline --stage freeze',depends=['ant-phase'],external_deps=[initial_job('molecular-pilot')],cpus=4,memory='32G',hours=4)
        add('mantis-phase',prefix+' molecular --stage mantis',cpus=8,memory='32G',hours=8,modules=['r/4.4.0','r-bundle-bioconductor/3.20'])
    elif phase=='behaviour':
        admission=require_admission(config,Path(root),'behaviour')
        add('behaviour-recovery',prefix+' behaviour --stage recover',cpus=8,memory='32G',hours=24)
        add('behaviour-fit',prefix+' behaviour --stage fit',depends=['behaviour-recovery'],cpus=8,memory='64G',hours=48)
        add('wp1-freeze',prefix+' behaviour --stage freeze-development',depends=['behaviour-fit'],cpus=8,memory='64G',hours=48)
        add('behaviour-inference',prefix+' behaviour --stage inference',depends=['behaviour-fit'],cpus=4,memory='32G',hours=4)
        add('intervention-challenge',prefix+' interventions --stage challenge',depends=['wp1-freeze'],cpus=8,memory='32G',hours=24)
        add('intervention-inference',prefix+' interventions --stage inference',depends=['intervention-challenge'],cpus=4,memory='32G',hours=4)
    elif phase=='molecular-transfer':
        require_admission(config,Path(root),'molecular')
        from .molecular import validate_freeze
        validate_freeze(Path(root)/'data/molecular/freeze/freeze.json')
        add('screen-gene-export',prefix+' interventions --stage screen',cpus=4,memory='32G',hours=4)
        add('molecular-transfer',prefix+' molecular-pipeline --stage predict',depends=['screen-gene-export'],cpus=8,memory='64G',hours=8)
    elif phase=='synthesis':
        add('evidence-report',prefix+' report',cpus=4,memory='32G',hours=4)
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
    if record.get('source_sha256')!=source_identity(Path(__file__).resolve().parents[2])['sha256']:raise ValueError('Measured admission uses different implementation')
    return record


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
            extra=task.get('external_dependencies',[])
            if any(not re.fullmatch(r'\d+',str(i)) for i in extra):raise ValueError('Invalid external native dependency')
            dependencies.extend(map(str,extra))
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

