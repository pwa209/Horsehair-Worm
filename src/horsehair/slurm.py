"""Finite Slurm plans, measured admission, submission leases, and accounting."""
from __future__ import annotations
import json
import math
import os
import re
import shlex
import subprocess
from pathlib import Path
from .core import atomic_json, canonical_hash, read_json, source_identity, validate_config, digest, exclusive_file_lock


def _array_indices(array):
    match = re.fullmatch(r'(\d+)(?:-(\d+))?(?:%([1-9]\d*))?', str(array))
    if not match:
        raise ValueError('Invalid finite Slurm array')
    first, last = int(match[1]), int(match[2] or match[1])
    if first > last or last-first > 10_000:
        raise ValueError('Slurm array range is empty or unbounded')
    return list(range(first, last+1))


def completed_native_dependency(root, job_id, array=None, *, dependency_kind='afterok'):
    """Positive persisted sacct proof; a missing native record proves nothing.

    The resource ledger changes as other jobs finish. Its selected final native
    records are immutable and separately hashed, so unrelated ledger updates do
    not invalidate a previously proven completed dependency.
    """
    job_id = str(job_id)
    if not re.fullmatch(r'\d+(?:_\d+)?', job_id):
        raise ValueError('Invalid native dependency identity')
    if dependency_kind not in {'afterok','afterany'} or (array and '_' in job_id):
        raise ValueError('Invalid native dependency kind or array parent')
    path = Path(root)/'state/resource-budget.json'
    if not path.is_file():
        return None
    native = read_json(path).get('native_jobs', {})
    identities = [job_id+'_'+str(index) for index in _array_indices(array)] if array else [job_id]
    records = {ident: native.get(ident) for ident in identities}
    terminal = {'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}
    failures = [ident for ident, record in records.items() if record and record.get('state') in terminal-{'COMPLETED'}]
    if failures and dependency_kind == 'afterok':
        raise ValueError('Required native dependency failed: '+', '.join(failures))
    if any(not record for record in records.values()):
        return None
    if dependency_kind == 'afterok' and any(record.get('exit_code') != '0:0' for record in records.values()):
        raise ValueError('Completed native dependency has a nonzero exit code')
    for record in records.values():
        seconds, cpus = float(record.get('elapsed_seconds', -1)), float(record.get('cpus', -1))
        if record.get('basis') != 'sacct final allocated wall time' or record.get('state') not in terminal or not re.fullmatch(r'\d+:\d+', str(record.get('exit_code', ''))) or not math.isfinite(seconds) or not math.isfinite(cpus) or seconds < 0 or cpus < 0 or (dependency_kind == 'afterok' and (seconds == 0 or cpus == 0)):
            return None
    proof = {'job_id': job_id, 'array': array, 'dependency_kind': dependency_kind, 'source_path': str(path.resolve()),
        'source_file_sha256_at_capture': digest(path), 'native_records': records,
        'native_records_sha256': canonical_hash(records),
        'basis': 'Positive persisted final sacct records for every declared task; require COMPLETED/0:0 for afterok'}
    proof['completion_proof_sha256'] = canonical_hash(proof)
    return proof


def _verify_completed_dependency(root, proof):
    expected_path = (Path(root)/'state/resource-budget.json').resolve()
    if Path(proof.get('source_path', '')).resolve() != expected_path:
        raise ValueError('Completed dependency proof comes from another study ledger')
    if proof.get('completion_proof_sha256') != canonical_hash({key:value for key,value in proof.items() if key != 'completion_proof_sha256'}):
        raise ValueError('Completed dependency proof changed')
    current = completed_native_dependency(root, proof['job_id'], proof.get('array'), dependency_kind=proof.get('dependency_kind','afterok'))
    if current is None or current['native_records_sha256'] != proof.get('native_records_sha256'):
        raise ValueError('Completed native dependency is missing or changed')


def _script(config, release, name, command, *, cpus=4, memory='16G', hours=2, gpu=False, array=None, network=False,modules=()):
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]*',name) or not isinstance(cpus,int) or isinstance(cpus,bool) or cpus < 1 or not isinstance(hours,int) or isinstance(hours,bool) or not 1 <= hours <= 168 or not re.fullmatch(r'[1-9]\d*[MG]',str(memory)):
        raise ValueError('Invalid bounded Slurm task resources or name')
    if array:
        _array_indices(array)
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
              f'export PYTHONPATH={shlex.quote(str(Path(release)/"src"))}:"${{PYTHONPATH:-}}"',
              'export MPLBACKEND=Agg', 'export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}',
              'export OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK}',
              'export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}',
              'export PYTHONUNBUFFERED=1','export CUBLAS_WORKSPACE_CONFIG=:4096:8',f'export HHW_GPU_COUNT={int(gpu)}',f'export HHW_MAX_HOURS={hours}']
    if network:lines += ['module load httpproxy/1.0']
    if modules:lines += ['module load '+' '.join(shlex.quote(x) for x in modules)]
    lines += [f'python -m horsehair.cli --config configs/study.json --root {shlex.quote(root)} verify-source --identity release.json',command]
    return '\n'.join(lines)+'\n'


def plan(config, release: Path, output: Path, phase='initial', *, prior_dependencies=None):
    validate_config(config)
    output, release = Path(output), Path(release)
    if output.exists():
        raise FileExistsError('Immutable Slurm plan directory already exists')
    root = config['paths']['root']
    prefix = f'python -m horsehair.cli --config configs/study.json --root {shlex.quote(root)}'
    tasks, scripts, admission = [], {}, {}
    prior_dependencies = prior_dependencies or {}
    rna_modules = ['star/2.7.11b','fastqc/0.12.1','trimmomatic/0.39','subread/2.0.6','samtools/1.20']
    r_modules = ['r/4.4.0','r-bundle-bioconductor/3.20']

    def phase_job(selected_phase, name):
        release_id = read_json(release/'release.json')['release_id']
        choices = ['managed-initial','initial'] if selected_phase == 'initial' else [selected_phase]
        for selected in choices:
            ledger_path = Path(root)/'state'/(release_id+'-'+selected+'-submission.json')
            if not ledger_path.is_file():
                continue
            ledger = read_json(ledger_path)
            native_plan = read_json(Path(root)/'plans'/(release_id+'-'+selected)/'plan.json')
            if ledger.get('plan_sha256') != canonical_hash(native_plan):
                raise ValueError('Required dependency ledger does not match its immutable plan')
            task = ledger.get('tasks', {}).get(name, {})
            if task.get('state') != 'submitted':
                raise ValueError('Required stage has no confirmed native submission: '+name)
            planned = next((value for value in native_plan['tasks'] if value['name'] == name), None)
            if planned is None:
                raise ValueError('Native dependency is absent from its immutable plan')
            return {'job_id':task['job_id'],'array':planned['resources'].get('array')}
        raise ValueError('Required prior phase is not registered: '+selected_phase)

    def add(name, command, depends=(), external_deps=(), dependency_kind='afterok', **resources):
        if dependency_kind not in {'afterok','afterany'}:
            raise ValueError('Unsupported native dependency kind')
        measured = admission.get('allocation_parameters', {}).get('rna-full' if name == 'rna-counts' else name)
        if measured:
            measured = dict(measured)
            concurrency = int(measured.pop('concurrency'))
            resources.update(measured)
            if resources.get('array'):
                original = int(resources['array'].split('%')[1])
                resources['array'] = resources['array'].split('%')[0]+'%'+str(min(original, concurrency))
        defaults = {'cpus':4,'memory':'16G','hours':2,'gpu':False}
        resources = dict(defaults, **resources)
        completed, pending, pending_specs = [], [], []
        for external in [*external_deps, *prior_dependencies.get(name, [])]:
            external = {'job_id':external} if not isinstance(external, dict) else external
            ident = str(external['job_id'])
            proof = completed_native_dependency(root, ident, external.get('array'), dependency_kind=dependency_kind)
            if proof is not None:
                completed.append(proof)
            else:
                pending.append(ident)
                pending_specs.append({'job_id':ident,'array':external.get('array')})
        filename = output/(name+'.sbatch')
        text = _script(config, release, name, command, **resources)
        scripts[filename] = text
        tasks.append({'name':name,'script':str(filename),'script_sha256':canonical_hash(text),
            'depends':list(depends),'dependency_kind':dependency_kind,
            'external_dependencies':list(dict.fromkeys(pending)),
            'external_dependency_specs':pending_specs,
            'completed_external_dependencies':completed,'resources':resources})

    if phase in {'initial','managed-initial'}:
        managed = phase == 'managed-initial'
        add('qualification', prefix+' qualify', cpus=4,memory='16G',hours=1)
        add('r-qualification', prefix+' r-qualify', depends=['qualification'],cpus=2,memory='8G',hours=1,modules=r_modules)
        add('gpu-qualification', prefix+' qualify --gpu', depends=['qualification'],cpus=4,memory='32G',hours=1,gpu=True)
        if managed:
            for group in ['fly','mantis','tools']:
                add('source-verification-'+group, prefix+' verify-sources --group '+group,
                    depends=['qualification'],cpus=4,memory='16G',hours=8)
            runtime_deps = ['source-verification-tools','r-qualification']
        else:
            add('transfer-wait-tools',prefix+' wait-transfer --group tools',cpus=1,memory='4G',hours=24)
            runtime_deps = ['transfer-wait-tools','r-qualification']
            add('transfer-wait-fly',prefix+' wait-transfer --group fly',cpus=1,memory='4G',hours=144)
            add('source-verification-fly',prefix+' verify-sources --group fly',depends=['transfer-wait-fly','qualification'],cpus=4,memory='16G',hours=8)
        add('molecular-runtime',prefix+' molecular --stage runtime',depends=runtime_deps,cpus=2,memory='8G',hours=1)
        add('transfer-wait-rna',prefix+' wait-transfer --group rna-pilot',cpus=1,memory='4G',hours=144)
        add('source-verification-rna',prefix+' verify-sources --group rna-pilot',depends=['transfer-wait-rna','qualification'],cpus=4,memory='16G',hours=8)
        add('source-audit',prefix+' source-audit',depends=['source-verification-fly'],cpus=8,memory='64G',hours=8)
        if managed:
            add('source-replication',prefix+' behaviour --stage source-replication',depends=['source-audit'],cpus=8,memory='64G',hours=8)
            add('mantis-behaviour',prefix+' behaviour --stage mantis',depends=['source-verification-mantis'],cpus=8,memory='32G',hours=4)
        add('rna-reference',prefix+' rna-pilot --prepare-only',depends=['source-verification-rna'],cpus=8,memory='64G',hours=24 if managed else 4,modules=rna_modules)
        add('molecular-pilot',prefix+' molecular-pipeline --stage benchmark-primary',depends=['rna-reference','molecular-runtime'],cpus=8,memory='64G',hours=24 if managed else 8)
        for index in [0,1]:
            add('rna-pilot-'+str(index),prefix+' rna-pilot --index '+str(index),depends=['rna-reference'],cpus=8,memory='64G',hours=24,modules=rna_modules)
        if managed:
            cells = len(config['agents']['envs'])*(len(config['agents']['architectures'])+int(config['agents'].get('feedforward_baseline',True)))
            add('agent-benchmark-training',prefix+' agents --mode benchmark-training --task-index "$SLURM_ARRAY_TASK_ID"',depends=['gpu-qualification'],array=f'0-{cells-1}%1',cpus=6,memory='64G',hours=12,gpu=True)
            add('agent-benchmark-evaluation',prefix+' agents --mode benchmark-cpu-evaluation --task-index "$SLURM_ARRAY_TASK_ID"',depends=['agent-benchmark-training'],array=f'0-{cells-1}%2',cpus=4,memory='32G',hours=24)
            add('agent-benchmark-aggregate',prefix+' agents --mode benchmark-aggregate',depends=['agent-benchmark-evaluation'],cpus=2,memory='8G',hours=1)
            for segment in range(4):
                previous = ['pipeline-controller-'+str(segment-1)] if segment else ['qualification']
                add('pipeline-controller-'+str(segment),prefix+' pipeline --segment '+str(segment),depends=previous,
                    dependency_kind='afterany' if segment else 'afterok',cpus=1,memory='8G',hours=144)
        else:
            add('fly-pilot',prefix+' behaviour --stage pilot',depends=['source-audit'],cpus=8,memory='64G',hours=4)
            add('agent-pilot',prefix+' agents --mode pilot',depends=['gpu-qualification'],cpus=6,memory='64G',hours=2,gpu=True)
            add('agent-cpu-pilot',prefix+' agents --mode cpu-evaluation-pilot',depends=['agent-pilot'],cpus=4,memory='32G',hours=2)
            add('pilot-assessment',prefix+' assess-pilot --advance',depends=['fly-pilot','rna-pilot-0','rna-pilot-1','agent-cpu-pilot'],cpus=2,memory='8G',hours=1)
    elif phase == 'agents-development':
        admission = require_admission(config,Path(root),'agents-development')
        cells = len(config['agents']['envs'])*(len(config['agents']['architectures'])+int(config['agents'].get('feedforward_baseline',True)))*len(config['agents']['development_seeds'])
        add('agent-development',prefix+' agents --mode development --task-index "$SLURM_ARRAY_TASK_ID"',array=f'0-{cells-1}%1',cpus=6,memory='64G',hours=4,gpu=True)
        add('agent-freeze',prefix+' agents --mode freeze',depends=['agent-development'],cpus=2,memory='8G',hours=1)
    elif phase == 'agents-final':
        admission = require_admission(config,Path(root),'agents-final')
        cells = len(config['agents']['envs'])*(len(config['agents']['architectures'])+int(config['agents'].get('feedforward_baseline',True)))*len(config['agents']['final_seeds'])
        add('agent-final',prefix+' agents --mode final --task-index "$SLURM_ARRAY_TASK_ID"',array=f'0-{cells-1}%1',cpus=6,memory='64G',hours=2,gpu=True)
        add('agent-evaluation',prefix+' agents --mode evaluate --task-index "$SLURM_ARRAY_TASK_ID"',array=f'0-{cells-1}%8',depends=['agent-final'],cpus=4,memory='32G',hours=12)
        add('agent-analysis',prefix+' agent-analysis',depends=['agent-evaluation'],cpus=4,memory='32G',hours=12)
    elif phase == 'rna-full':
        admission = require_admission(config,Path(root),'rna-full')
        add('rna-all-wait',prefix+' wait-transfer --group rna',cpus=1,memory='4G',hours=144)
        add('rna-all-verified',prefix+' verify-sources --group rna',depends=['rna-all-wait'],cpus=4,memory='16G',hours=8)
        add('rna-full-plan',prefix+' molecular --stage prepare-rna-full',external_deps=[phase_job('initial','rna-reference')],cpus=2,memory='8G',hours=1)
        add('rna-counts',prefix+' molecular --stage rna-run --task-index "$SLURM_ARRAY_TASK_ID"',depends=['rna-full-plan','rna-all-verified'],array='0-59%2',cpus=8,memory='64G',hours=24,modules=rna_modules)
        add('ant-phase',prefix+' molecular --stage ant',depends=['rna-counts'],cpus=8,memory='32G',hours=8,modules=r_modules)
        add('ant-score-freeze',prefix+' molecular-pipeline --stage freeze',depends=['ant-phase'],external_deps=[phase_job('initial','molecular-pilot')],cpus=4,memory='32G',hours=4)
        add('mantis-phase',prefix+' molecular --stage mantis',depends=['rna-all-verified'],cpus=8,memory='32G',hours=8,modules=r_modules)
    elif phase == 'behaviour-benchmark':
        add('behaviour-benchmark',prefix+' behaviour --stage benchmark-workload',cpus=8,memory='64G',hours=24)
    elif phase == 'behaviour':
        admission = require_admission(config,Path(root),'behaviour')
        add('behaviour-recovery',prefix+' behaviour --stage recover',cpus=8,memory='32G',hours=24)
        add('behaviour-identifiability',prefix+' behaviour --stage identifiability',depends=['behaviour-recovery'],cpus=4,memory='32G',hours=4)
        for suffix in ['', '-5s', '-20s']:
            add('behaviour-fit'+suffix,prefix+' behaviour --stage fit'+suffix,depends=['behaviour-recovery'],cpus=8,memory='64G',hours=48)
            add('behaviour-posterior-checks'+suffix,prefix+' behaviour --stage posterior-checks'+suffix,depends=['behaviour-fit'+suffix],cpus=8,memory='64G',hours=24)
        add('behaviour-sensitivity-checks',prefix+' behaviour --stage sensitivity-checks',depends=['behaviour-posterior-checks-5s','behaviour-posterior-checks-20s'],cpus=4,memory='32G',hours=4)
        add('behaviour-bayesian-check',prefix+' behaviour --stage bayesian-check',depends=['behaviour-fit'],cpus=8,memory='64G',hours=24)
        add('wp1-freeze',prefix+' behaviour --stage freeze-development',depends=['behaviour-fit'],cpus=8,memory='64G',hours=48)
        add('behaviour-inference',prefix+' behaviour --stage inference',depends=['behaviour-identifiability','behaviour-posterior-checks','behaviour-sensitivity-checks','behaviour-bayesian-check'],cpus=4,memory='32G',hours=4)
    elif phase == 'interventions':
        admission = require_admission(config,Path(root),'behaviour')
        add('intervention-challenge',prefix+' interventions --stage challenge',external_deps=[phase_job('behaviour','wp1-freeze')],cpus=8,memory='32G',hours=24)
        add('intervention-inference',prefix+' interventions --stage inference',depends=['intervention-challenge'],cpus=4,memory='32G',hours=4)
    elif phase == 'screen':
        admission = require_admission(config,Path(root),'behaviour')
        add('screen-gene-export',prefix+' interventions --stage screen',external_deps=[phase_job('rna-full','ant-score-freeze')],cpus=4,memory='32G',hours=4)
    elif phase == 'molecular-benchmark':
        add('molecular-benchmark',prefix+' molecular-pipeline --stage benchmark-transfer',cpus=8,memory='64G',hours=24)
    elif phase == 'molecular-transfer':
        admission = require_admission(config,Path(root),'molecular')
        from .molecular import validate_freeze
        validate_freeze(Path(root)/'data/molecular/freeze/freeze.json')
        add('molecular-transfer',prefix+' molecular-pipeline --stage predict',external_deps=[phase_job('molecular-benchmark','molecular-benchmark')],cpus=8,memory='64G',hours=24)
    elif phase == 'molecular-corroboration':
        add('mantis-orfs',prefix+' molecular-pipeline --stage mantis-orfs',cpus=8,memory='64G',hours=24)
        add('mantis-orthology',prefix+' molecular-pipeline --stage mantis-orthology',depends=['mantis-orfs'],cpus=8,memory='64G',hours=24)
    elif phase == 'synthesis':
        add('evidence-report',prefix+' report',cpus=4,memory='32G',hours=4)
        add('durable-archive',prefix+' archive',depends=['evidence-report'],cpus=1,memory='8G',hours=4)
    else:
        raise ValueError(f'Unknown finite phase {phase}')

    names = set()
    for task in tasks:
        if task['name'] in names or not set(task['depends']).issubset(names):
            raise ValueError('Slurm plan is not a finite topologically ordered DAG')
        names.add(task['name'])
    from .pipeline import prospective_capacity
    capacity = prospective_capacity({'candidate':{'tasks':tasks}}, {}, {})
    if capacity['cpus'] > config['resources']['cpu_ceiling'] or capacity['gpus'] > config['resources']['gpu_concurrency']:
        raise ValueError('Finite phase exceeds the study CPU/GPU concurrency cap')
    record = {'schema_version':2,'phase':phase,'config_sha256':canonical_hash(config),
        'release_identity':source_identity(release),'root':str(root),'tasks':tasks,
        'scope':'bounded qualification, source checks, resource benchmarks and finite study controllers' if phase in {'initial','managed-initial'} else 'bounded scientific benchmark' if phase.endswith('benchmark') else 'admitted scientific work' if admission else 'bounded supporting analysis',
        'cpu_ceiling':config['resources']['cpu_ceiling'],'gpu_concurrency':config['resources']['gpu_concurrency'],
        'maximum_simultaneous_resources':capacity,'admission_phase':admission.get('phase')}
    output.mkdir(parents=True,exist_ok=False)
    for filename,text in scripts.items():
        filename.write_text(text,encoding='utf8',newline='\n')
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
    if environment_job is not None and not re.fullmatch(r'\d+(?:_\d+)?',str(environment_job)):
        raise ValueError('Invalid environment dependency identity')
    known=set()
    for task in p['tasks']:
        if task['name'] in known or not set(task.get('depends',[])).issubset(known):
            raise ValueError('Submission plan is not a topologically ordered DAG')
        known.add(task['name'])
        if task.get('dependency_kind','afterok') not in {'afterok','afterany'}:
            raise ValueError('Invalid dependency kind in frozen submission plan')
        script=Path(task['script'])
        if canonical_hash(script.read_text(encoding='utf8'))!=task['script_sha256']:
            raise ValueError('Slurm script changed after plan freeze')
    ledger_path=Path(ledger_path)
    lock=ledger_path.with_suffix('.mutex')
    with exclusive_file_lock(lock):
        ledger=read_json(ledger_path) if ledger_path.exists() else {'schema_version':2,'plan_sha256':canonical_hash(p),'environment_job':environment_job,
            'source_sha256':p.get('release_identity',{}).get('sha256'),'config_sha256':p.get('config_sha256'),'tasks':{}}
        if ledger['plan_sha256']!=canonical_hash(p):raise ValueError('Submission ledger belongs to another plan')
        if 'environment_job' in ledger and ledger['environment_job']!=environment_job:
            raise ValueError('Submission environment dependency changed after its context was frozen')
        for task in p['tasks']:
            existing=ledger['tasks'].get(task['name'])
            if existing:
                if existing.get('state')=='submitted':continue
                raise RuntimeError('Ambiguous/rejected prior submission: inspect native records; do not replay')
            script=Path(task['script'])
            kind=task.get('dependency_kind','afterok')
            dependencies=[]
            completed=list(task.get('completed_external_dependencies',[]))
            root=p.get('root')
            for proof in completed:
                if not root:raise ValueError('Completed native dependency lacks its study root')
                if proof.get('dependency_kind','afterok')!=kind:raise ValueError('Completion proof uses another dependency kind')
                _verify_completed_dependency(root,proof)
            planned={value['name']:value for value in p['tasks']}
            for name in task.get('depends',[]):
                ident=ledger['tasks'][name]['job_id']
                array=planned[name].get('resources',{}).get('array')
                proof=completed_native_dependency(root,ident,array,dependency_kind=kind) if root else None
                if proof is None:dependencies.append(str(ident))
                else:completed.append(proof)
            extra=task.get('external_dependencies',[])
            if any(not re.fullmatch(r'\d+(?:_\d+)?',str(i)) for i in extra):raise ValueError('Invalid external native dependency')
            specs={str(value['job_id']):value for value in task.get('external_dependency_specs',[])}
            for ident in map(str,extra):
                proof=completed_native_dependency(root,ident,specs.get(ident,{}).get('array'),dependency_kind=kind) if root else None
                if proof is None:dependencies.append(ident)
                else:completed.append(proof)
            if environment_job and not task.get('depends'):
                proof=completed_native_dependency(root,environment_job,dependency_kind=kind) if root else None
                if proof is None:dependencies.append(str(environment_job))
                else:completed.append(proof)
            dependencies=list(dict.fromkeys(dependencies))
            command=['sbatch','--parsable']
            if dependencies:command += ['--dependency='+kind+':'+':'.join(dependencies)]
            command += [str(script)]
            ledger['tasks'][task['name']]={'state':'submitting','command':command,'script_sha256':task['script_sha256'],
                'dependency_kind':kind,'issued_native_dependencies':dependencies,'completed_native_dependencies':completed}
            ledger['tasks'][task['name']]['dependency_resolution_sha256']=canonical_hash({'kind':kind,'issued':dependencies,'completed':completed})
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

