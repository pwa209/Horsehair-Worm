"""Synthetic qualification/native evidence for the narrow auxiliary ORF adapter."""
import copy
import shlex
from pathlib import Path

import pytest

from horsehair import repair, mantis_recovery
from horsehair.core import atomic_json, canonical_hash, digest, read_json, source_identity
from horsehair.slurm import completed_native_dependency
from test_repair import fixture, register


def adapter_command(change):
    return ('python ' + shlex.quote(str(Path(change['framework_release']) / 'src/horsehair/mantis_recovery.py'))
        + ' run --manifest ' + shlex.quote(change['manifest_path'])
        + ' --manifest-sha256 ' + change['manifest_sha256']
        + ' --preparation ' + shlex.quote(change['preparation_path'])
        + ' --preparation-sha256 ' + change['preparation_sha256'])


def mantis_fixture(tmp_path, monkeypatch):
    f = fixture(tmp_path)
    framework = f['preprocessing']
    module = framework / 'src/horsehair/mantis_recovery.py'
    module.write_text('# Synthetic qualified input adapter, never executed.\n')
    framework_identity = source_identity(framework)
    atomic_json(framework / 'release.json', {'release_id': 'v8',
        'source_identity': framework_identity, 'config_sha256': canonical_hash(f['config'])})
    case = f['root'] / 'maintenance/mantis-reference-fixture'
    case.mkdir(parents=True)
    log, junit = case / 'pytest.log', case / 'junit.xml'
    log.write_text('Generated complete qualification fixture: 1 passed\n')
    junit.write_text('<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0">'
        '<testcase name="synthetic_fixture"/></testsuite></testsuites>')
    qualification = {'status': 'complete', 'returncode': 0, 'slurm_job_id': '990',
        'source_identity': framework_identity, 'source_sha256': framework_identity['sha256'],
        'primary_source_sha256': f['identity']['sha256'], 'config_sha256': canonical_hash(f['config']),
        'source_verified_unchanged': True,
        'test_selection': 'complete source/tests directory; no environment selectors or third-party plugin autoload',
        'test_counts': {'tests': 1, 'failures': 0, 'errors': 0, 'skipped': 0},
        'artifacts': {name: {'path': str(path), 'sha256': digest(path)}
                      for name, path in [('log', log), ('junit', junit)]}}
    qualification_path = case / 'qualification.json'
    atomic_json(qualification_path, qualification)
    manifest = {'root': str(f['root']), 'primary_release': str(f['primary']),
        'primary_source_identity': f['identity'], 'full_config_sha256': canonical_hash(f['config']),
        'maintenance_release': str(framework), 'maintenance_source_identity': framework_identity,
        'previous_job_ids': {'mantis-orfs': '31', 'mantis-orthology': '32'},
        'capture_slurm_job_id': '992', 'orf_recipe': copy.deepcopy(mantis_recovery.ORF_RECIPE)}
    preparation = {'schema_version': 1, 'status': 'prepared', 'slurm_job_id': '991'}
    manifest_path, preparation_path = case / 'manifest.json', case / 'preparation.json'
    atomic_json(manifest_path, manifest); atomic_json(preparation_path, preparation)
    # Full GFF/source/preservation validation has independent adapter tests.
    # Here decoded synthetic evidence exercises canonical registration itself.
    monkeypatch.setattr(mantis_recovery, 'validate_registration_manifest',
        lambda manifest, manifest_sha, preparation, preparation_sha:
            {'manifest': read_json(manifest), 'preparation': read_json(preparation)})
    change = {'framework_release': str(framework), 'qualification_path': str(qualification_path),
        'qualification_sha256': digest(qualification_path), 'qualification_job_id': '990',
        'manifest_path': str(manifest_path), 'manifest_sha256': digest(manifest_path),
        'preparation_path': str(preparation_path), 'preparation_sha256': digest(preparation_path)}
    native_jobs = {ident: {'state': 'COMPLETED', 'exit_code': '0:0', 'elapsed_seconds': 60,
        'cpus': 1, 'basis': 'sacct final allocated wall time'} for ident in ('990', '991', '992')}
    atomic_json(f['root'] / 'state/resource-budget.json', {'leases': {}, 'native_jobs': native_jobs})
    f['native'].update(copy.deepcopy(native_jobs))
    plan_path = f['root'] / 'plans/v7-molecular-corroboration/plan.json'
    old_tasks, new_tasks = [], []
    ledger, submitted = {'tasks': {}}, {'tasks': {}}
    for index, name in enumerate(('mantis-orfs', 'mantis-orthology')):
        command = ('python -m horsehair.cli --config configs/study.json --root '
            + shlex.quote(str(f['root'])) + ' molecular-pipeline --stage ' + name)
        text = ('#!/bin/bash\n#SBATCH --time=24:00:00\n#SBATCH --cpus-per-task=8\n#SBATCH --mem=64G\n'
            'set -euo pipefail\nexport HHW_MAX_HOURS=24\n'
            + 'module load ' + ('r/4.4.0' if index == 0 else 'StdEnv/2023') + '\n'
            + 'cd ' + shlex.quote(str(f['primary'])) + '\n'
            + 'export PYTHONPATH=' + shlex.quote(str(f['primary'] / 'src')) + ':"${PYTHONPATH:-}"\n'
            + 'python -m horsehair.cli --config configs/study.json verify-source --identity release.json\n'
            + command + '\n')
        path = plan_path.parent / (name + '.sbatch')
        path.parent.mkdir(parents=True, exist_ok=True); path.write_text(text)
        old = {'name': name, 'script': str(path), 'script_sha256': canonical_hash(text), 'command': command,
            'resources': {'cpus': 8, 'memory': '64G', 'hours': 24, 'gpu': False},
            'depends': [] if index == 0 else ['mantis-orfs']}
        old_tasks.append(old)
        ledger['tasks'][name] = {'state': 'submitted', 'job_id': str(31 + index), 'script_sha256': old['script_sha256']}
        replacement = copy.deepcopy(old)
        new_path = f['replacement_path'].parent / (name + '.sbatch')
        if index == 0:
            text = text.replace('set -euo pipefail', '#SBATCH --hold\nset -euo pipefail', 1)
            text = text.replace(command, adapter_command(change))
            replacement['command'] = adapter_command(change)
        new_path.write_text(text)
        replacement.update(script=str(new_path), script_sha256=canonical_hash(text))
        proof = {'state': 'submitted', 'job_id': str(101 + index), 'script_sha256': replacement['script_sha256'],
                 'dependency_kind': 'afterok', 'issued_native_dependencies': [] if index == 0 else ['101']}
        if index == 0:
            completed = [completed_native_dependency(f['root'], ident) for ident in ('990', '991')]
            replacement['completed_external_dependencies'] = completed
            proof['completed_native_dependencies'] = completed
        new_tasks.append(replacement); submitted['tasks'][name] = proof
        f['native'][str(31 + index)] = {'state': 'FAILED' if index == 0 else 'CANCELLED', 'exit_code': '1:0' if index == 0 else '0:0'}
        f['native'][str(101 + index)] = {'state': 'PENDING', 'exit_code': '0:0'}
    old = {'phase': 'molecular-corroboration', 'root': str(f['root']),
        'config_sha256': canonical_hash(f['config']), 'release_identity': f['identity'], 'tasks': old_tasks}
    replacement = {**old, 'tasks': new_tasks}
    for value in (ledger, submitted):
        value.update(source_sha256=f['identity']['sha256'], config_sha256=canonical_hash(f['config']))
    descriptor = {**f['descriptor'], 'phase': 'molecular-corroboration',
        'execution_releases': {name: str(f['primary']) for name in ('mantis-orfs', 'mantis-orthology')},
        'required_external_dependencies': {'mantis-orfs': ['990', '991']}, 'mantis_reference_recovery': change}
    f.update(old=old, ledger=ledger, replacement=replacement, submitted=submitted, descriptor=descriptor,
        plan_path=plan_path, framework=framework, framework_identity=framework_identity,
        qualification=qualification, qualification_path=qualification_path,
        manifest=manifest, manifest_path=manifest_path, preparation_path=preparation_path, change=change)
    refresh(f)
    return f


def refresh(f):
    f['ledger']['plan_sha256'] = canonical_hash(f['old'])
    atomic_json(f['plan_path'], f['old'])
    atomic_json(f['root'] / 'state/v7-molecular-corroboration-submission.json', f['ledger'])
    f['submitted']['plan_sha256'] = canonical_hash(f['replacement'])
    atomic_json(f['replacement_path'], f['replacement'])
    atomic_json(f['replacement_ledger'], f['submitted'])
    atomic_json(f['descriptor_path'], f['descriptor'])


def test_mantis_adapter_registration_keeps_primary_parent_and_orthology_and_records_qualified_guard(tmp_path, monkeypatch):
    f = mantis_fixture(tmp_path, monkeypatch)
    before = {path: path.read_bytes() for path in f['primary'].rglob('*') if path.is_file()}
    result = register(f)
    assert result['status'] == 'complete'
    assert result['amendment_id'] == f['descriptor']['amendment_id']
    plan = read_json(f['plan_path'])
    assert plan['release_identity'] == f['identity'] and plan['phase'] == 'molecular-corroboration'
    guard = plan['dag_amendments'][-1]['mantis_reference_recovery']
    assert guard['framework_source_identity'] == f['framework_identity']
    assert guard['primary_source_identity'] == f['identity'] and guard['orf_recipe'] == mantis_recovery.ORF_RECIPE
    assert guard['previous_job_ids'] == {'mantis-orfs': '31', 'mantis-orthology': '32'}
    for task, prior in zip(plan['tasks'], f['old']['tasks']):
        assert task['execution_release']['source_identity'] == f['identity']
        assert task['resources'] == prior['resources'] and task['depends'] == prior['depends']
        if task['name'] == 'mantis-orthology': assert Path(task['script']).read_text() == Path(prior['script']).read_text()
    assert all(path.read_bytes() == value for path, value in before.items())
    assert register(f) == result and len(f['calls']) == 1


@pytest.mark.parametrize('fault', ['missing-declaration', 'wrong-phase', 'partial-targets', 'mixed-rna-guard',
    'wrong-execution', 'framework-source', 'qualification-pin', 'qualification-skipped', 'manifest-pin', 'preparation-pin',
    'manifest-primary', 'manifest-framework', 'manifest-config', 'manifest-previous', 'manifest-recipe',
    'wrong-resources', 'wrong-named-dependency', 'wrong-external-dependency', 'wrong-native-parent',
    'missing-qualified-proof', 'missing-preparation-proof', 'failed-qualification', 'failed-preparation',
    'active-controller', 'completed-original', 'started-adapter', 'unheld-adapter', 'wrong-wrapper', 'wrapper-pin',
    'extra-wrapper-argument', 'duplicate-wrapper', 'wrong-primary-command', 'changed-orthology-command',
    'changed-modules', 'wrong-cd', 'wrong-pythonpath'])
def test_mantis_adapter_rejects_scope_source_recipe_native_or_scientific_contract_drift_before_mutation(tmp_path, monkeypatch, fault):
    f = mantis_fixture(tmp_path, monkeypatch)
    orfs, orthology = f['replacement']['tasks']; proof = f['submitted']['tasks']['mantis-orfs']
    if fault == 'missing-declaration': f['descriptor'].pop('mantis_reference_recovery')
    elif fault == 'wrong-phase': f['descriptor']['phase'] = 'rna-full'
    elif fault == 'partial-targets': f['descriptor']['execution_releases'].pop('mantis-orthology')
    elif fault == 'mixed-rna-guard': f['descriptor']['rna_recovery'] = copy.deepcopy(f['change'])
    elif fault == 'wrong-execution': f['descriptor']['execution_releases']['mantis-orfs'] = str(f['framework'])
    elif fault == 'framework-source': (f['framework'] / 'src/horsehair/mantis_recovery.py').write_text('# Changed adapter\n')
    elif fault == 'qualification-pin': f['change']['qualification_sha256'] = '0' * 64
    elif fault == 'qualification-skipped':
        f['qualification']['test_counts']['skipped'] = 1
        atomic_json(f['qualification_path'], f['qualification']); f['change']['qualification_sha256'] = digest(f['qualification_path'])
    elif fault in {'manifest-pin', 'preparation-pin'}: f['change'][fault.split('-')[0] + '_sha256'] = '0' * 64
    elif fault.startswith('manifest-'):
        key, value = {'manifest-primary': ('primary_source_identity', {'sha256': '0' * 64}),
            'manifest-framework': ('maintenance_source_identity', {'sha256': '0' * 64}),
            'manifest-config': ('full_config_sha256', '0' * 64),
            'manifest-previous': ('previous_job_ids', {'mantis-orfs': 'other', 'mantis-orthology': '32'}),
            'manifest-recipe': ('orf_recipe', {**mantis_recovery.ORF_RECIPE, 'minimum_peptide_aa': 101})}[fault]
        f['manifest'][key] = value; atomic_json(f['manifest_path'], f['manifest']); f['change']['manifest_sha256'] = digest(f['manifest_path'])
    elif fault == 'wrong-resources': orfs['resources']['hours'] = 25
    elif fault == 'wrong-named-dependency': orthology['depends'] = []
    elif fault == 'wrong-external-dependency': orthology['external_dependencies'] = ['999']
    elif fault == 'wrong-native-parent': f['submitted']['tasks']['mantis-orthology']['issued_native_dependencies'] = ['999']
    elif fault == 'missing-qualified-proof': proof['completed_native_dependencies'] = proof['completed_native_dependencies'][1:]
    elif fault == 'missing-preparation-proof': proof['completed_native_dependencies'] = proof['completed_native_dependencies'][:1]
    elif fault in {'failed-qualification', 'failed-preparation'}: f['native']['990' if fault == 'failed-qualification' else '991']['state'] = 'FAILED'
    elif fault == 'active-controller': f['native']['13']['state'] = 'RUNNING'
    elif fault == 'completed-original': f['native']['31'].update(state='COMPLETED', exit_code='0:0')
    elif fault == 'started-adapter': f['native']['101']['state'] = 'RUNNING'
    else:
        task = orthology if fault == 'changed-orthology-command' else orfs
        path = Path(task['script']); text = path.read_text()
        if fault == 'unheld-adapter': text = text.replace('#SBATCH --hold\n', '')
        elif fault == 'wrong-wrapper': text = text.replace('mantis_recovery.py', 'other.py')
        elif fault == 'wrapper-pin': text = text.replace('--manifest-sha256 ' + f['change']['manifest_sha256'], '--manifest-sha256 ' + '0' * 64)
        elif fault == 'extra-wrapper-argument': text = text.replace(' --preparation-sha256', ' --index 0 --preparation-sha256')
        elif fault == 'duplicate-wrapper': text += adapter_command(f['change']) + '\n'
        elif fault == 'wrong-primary-command':
            old = f['old']['tasks'][0]; old_path = Path(old['script'])
            old_text = old_path.read_text().replace('--stage mantis-orfs', '--stage predict')
            old_path.write_text(old_text); old['script_sha256'] = canonical_hash(old_text)
        elif fault == 'changed-orthology-command': text = text.replace('--stage mantis-orthology', '--stage predict')
        elif fault == 'changed-modules': text = text.replace('module load r/4.4.0', 'module load r/4.4.1')
        elif fault == 'wrong-cd': text = text.replace('cd ' + shlex.quote(str(f['primary'])), 'cd ' + shlex.quote(str(f['framework'])))
        elif fault == 'wrong-pythonpath': text = text.replace(shlex.quote(str(f['primary'] / 'src')), shlex.quote(str(f['framework'] / 'src')))
        path.write_text(text); task['script_sha256'] = canonical_hash(text)
        f['submitted']['tasks'][task['name']]['script_sha256'] = task['script_sha256']
    refresh(f)
    ledger_path = f['root'] / 'state/v7-molecular-corroboration-submission.json'
    before = f['plan_path'].read_bytes(), ledger_path.read_bytes()
    with pytest.raises((ValueError, KeyError, TypeError, OSError)):
        register(f)
    assert (f['plan_path'].read_bytes(), ledger_path.read_bytes()) == before and not f['calls']


def test_adapter_projection_refuses_application_to_orthology_or_another_guard_kind(tmp_path, monkeypatch):
    f = mantis_fixture(tmp_path, monkeypatch)
    orfs, orthology = f['replacement']['tasks']
    binding = {**f['change'], 'root': str(f['root'])}
    with pytest.raises(ValueError, match='limited'):
        repair._check_script(orthology, f['old']['tasks'][1], f['primary'], f['root'], mantis_recovery=binding)
    with pytest.raises(ValueError, match='limited'):
        repair._check_script(orfs, f['old']['tasks'][0], f['primary'], f['root'], controller_primary=f['primary'], mantis_recovery=binding)


@pytest.mark.parametrize('fault',['wrong-phase','partial-targets','extra-targets','mixed-rna-recovery','mixed-rna-followup','mixed-controller'])
def test_mantis_scope_refusal_precedes_qualification_or_manifest_access(tmp_path,monkeypatch,fault):
    names={'mantis-orfs','mantis-orthology'}
    change={key:'unread-fixture' for key in ('framework_release','qualification_path','qualification_sha256',
        'qualification_job_id','manifest_path','manifest_sha256','preparation_path','preparation_sha256')}
    descriptor={'phase':'molecular-corroboration','mantis_reference_recovery':change,
                'execution_releases':{name:str(tmp_path) for name in names}}
    if fault=='wrong-phase':descriptor['phase']='rna-full'
    elif fault=='partial-targets':names={'mantis-orfs'}
    elif fault=='extra-targets':names=names|{'source-audit'}
    else:descriptor[{'mixed-rna-recovery':'rna_recovery','mixed-rna-followup':'rna_followup','mixed-controller':'controller_maintenance'}[fault]]={}
    monkeypatch.setattr(repair,'_controller_maintenance',lambda *args:pytest.fail('Invalid scope must precede qualification I/O'))
    with pytest.raises(ValueError,match='exactly both auxiliary'):
        repair._mantis_recovery_binding(descriptor,names,tmp_path,{}, {},tmp_path,{}, {})


def test_mantis_evidence_is_revalidated_after_native_accounting_before_canonical_writes(tmp_path,monkeypatch):
    f=mantis_fixture(tmp_path,monkeypatch)
    ledger_path=f['root']/'state/v7-molecular-corroboration-submission.json'
    before=f['plan_path'].read_bytes(),ledger_path.read_bytes()
    def accountant(root,ids):
        f['manifest']['orf_recipe']['minimum_peptide_aa']=101
        atomic_json(f['manifest_path'],f['manifest'])
        return {'pending_native_records':[],'spent_cpu_core_hours':10.,'spent_gpu_hours':0.}
    with pytest.raises(ValueError,match='manifest is missing, relocated or changed'):
        repair.register_amendment(f['descriptor_path'],native=f['native'],accountant=accountant)
    assert (f['plan_path'].read_bytes(),ledger_path.read_bytes())==before
