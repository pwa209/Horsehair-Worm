"""Synthetic scheduler/qualification fixtures for the scoped count guard."""
import copy
import shlex
from pathlib import Path

import pytest

from horsehair import repair, rna_recovery
from horsehair.core import atomic_json, canonical_hash, digest, read_json, source_identity
from horsehair.slurm import completed_native_dependency
from test_repair import fixture, register


def recovery_fixture(tmp_path, monkeypatch):
    f = fixture(tmp_path, phase='rna-full')
    framework = f['preprocessing']
    module = framework / 'src/horsehair/rna_recovery.py'
    module.write_text('# Synthetic operational guard; never executed.\n')
    framework_identity = source_identity(framework)
    atomic_json(framework / 'release.json', {'release_id': 'v8',
        'source_identity': framework_identity, 'config_sha256': canonical_hash(f['config'])})
    case = f['root'] / 'maintenance/rna-recovery-fixture'
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
        'previous_array_job_id': f['ledger']['tasks']['rna-counts']['job_id'],
        'capture_slurm_job_id': '992', 'plan_sha256': 'a' * 64,
        'reuse_indices': [*range(18), 19, 20], 'rerun_indices': [18, *range(21, 60)]}
    preparation = {'slurm_job_id': '991'}
    manifest_path, preparation_path = case / 'manifest.json', case / 'preparation.json'
    atomic_json(manifest_path, manifest)
    atomic_json(preparation_path, preparation)
    # Manifest validation itself has independent real-cohort tests. This fixture
    # supplies explicit decoded synthetic evidence to exercise registration.
    monkeypatch.setattr(rna_recovery, 'validate_registration_manifest',
        lambda manifest, manifest_sha, preparation, preparation_sha:
            {'manifest': read_json(manifest), 'preparation': read_json(preparation)})
    change = {'framework_release': str(framework), 'qualification_path': str(qualification_path),
        'qualification_sha256': digest(qualification_path), 'qualification_job_id': '990',
        'manifest_path': str(manifest_path), 'manifest_sha256': digest(manifest_path),
        'preparation_path': str(preparation_path), 'preparation_sha256': digest(preparation_path)}
    f['descriptor']['rna_recovery'] = change
    f['descriptor']['required_external_dependencies']['rna-counts'] = ['990', '991']
    native_jobs = {ident: {'state': 'COMPLETED', 'exit_code': '0:0', 'elapsed_seconds': 60,
                        'cpus': 1, 'basis': 'sacct final allocated wall time'}
                   for ident in ('990', '991', '992')}
    atomic_json(f['root'] / 'state/resource-budget.json', {'leases': {}, 'native_jobs': native_jobs})
    f['native'].update(copy.deepcopy(native_jobs))
    for tasks, old in [(f['old']['tasks'], True), (f['replacement']['tasks'], False)]:
        count = next(task for task in tasks if task['name'] == 'rna-counts')
        script = Path(count['script'])
        text = script.read_text().replace('python -m horsehair.cli rna-counts',
            'python -m horsehair.cli --config configs/study.json --root ' + shlex.quote(str(f['root']))
            + ' molecular --stage rna-run --task-index "$SLURM_ARRAY_TASK_ID"')
        count['resources'] = {'cpus': 8, 'memory': '56G', 'hours': 2, 'gpu': False, 'array': '0-59%2'}
        base = f['ledger' if old else 'submitted']['tasks']['rna-counts']['job_id']
        for index in range(60):
            f['native'][base + '_' + str(index)] = {'state': ('COMPLETED' if old and index in manifest['reuse_indices']
                                                         else 'FAILED' if old else 'PENDING'),
                                                     'exit_code': '1:0' if old and index not in manifest['reuse_indices'] else '0:0'}
        if not old:
            text = text.replace('#!/bin/bash\n', '#!/bin/bash\n#SBATCH --hold\n')
            old_command = next(line for line in text.splitlines() if 'molecular --stage rna-run' in line)
            wrapper = ('python ' + shlex.quote(str(module)) + ' run --manifest ' + shlex.quote(str(manifest_path))
                + ' --index "$SLURM_ARRAY_TASK_ID" --manifest-sha256 ' + change['manifest_sha256']
                + ' --preparation ' + shlex.quote(str(preparation_path))
                + ' --preparation-sha256 ' + change['preparation_sha256'])
            text = text.replace(old_command, wrapper)
            proofs = [completed_native_dependency(f['root'], ident) for ident in ('990', '991')]
            count['completed_external_dependencies'] = proofs
            f['submitted']['tasks']['rna-counts'].update(completed_native_dependencies=proofs,
                issued_native_dependencies=[], dependency_kind='afterok')
        script.write_text(text)
        count['script_sha256'] = canonical_hash(text)
        f['ledger' if old else 'submitted']['tasks']['rna-counts']['script_sha256'] = count['script_sha256']
    f.update(framework=framework, framework_identity=framework_identity, qualification=qualification,
        qualification_path=qualification_path, manifest=manifest, manifest_path=manifest_path,
        preparation_path=preparation_path, change=change)
    refresh(f)
    return f


def refresh(f):
    f['ledger']['plan_sha256'] = canonical_hash(f['old'])
    atomic_json(f['plan_path'], f['old'])
    atomic_json(f['root'] / 'state/v7-rna-full-submission.json', f['ledger'])
    f['submitted']['plan_sha256'] = canonical_hash(f['replacement'])
    atomic_json(f['replacement_path'], f['replacement'])
    atomic_json(f['replacement_ledger'], f['submitted'])
    atomic_json(f['descriptor_path'], f['descriptor'])


def test_count_guard_registration_preserves_primary_and_successful_work(tmp_path, monkeypatch):
    f = recovery_fixture(tmp_path, monkeypatch)
    result = register(f)
    assert result['status'] == 'complete'
    plan = read_json(f['plan_path'])
    count = next(task for task in plan['tasks'] if task['name'] == 'rna-counts')
    assert count['execution_release']['source_identity'] == f['identity']
    assert count['resources'] == {'cpus': 8, 'memory': '56G', 'hours': 2, 'gpu': False, 'array': '0-59%2'}
    assert result['rna_recovery']['framework_source_identity'] == f['framework_identity']
    assert result['rna_recovery']['reuse_indices'] == [*range(18), 19, 20]
    ledger = read_json(f['root'] / 'state/v7-rna-full-submission.json')
    assert ledger['tasks']['rna-full-plan'] == f['ledger']['tasks']['rna-full-plan']
    assert register(f) == result and len(f['calls']) == 1


@pytest.mark.parametrize('fault', ['no-declaration', 'extra-scope', 'wrong-execution', 'framework-source',
    'qualification-pin', 'qualification-primary', 'qualification-skipped', 'manifest-pin', 'preparation-pin',
    'manifest-primary', 'manifest-framework', 'manifest-array', 'wrong-array', 'wrong-resources',
    'wrong-cd', 'wrong-pythonpath', 'wrong-primary-command', 'wrapper-extra-arg', 'wrapper-pin',
    'extra-command', 'changed-verification', 'unheld', 'started-child', 'wrong-dependency-kind',
    'missing-qualified-dependency', 'missing-preparation-proof', 'failed-qualification', 'failed-preparation',
    'changed-named-dependency'])
def test_count_guard_rejects_unqualified_or_changed_contract_before_mutation(tmp_path, monkeypatch, fault):
    f = recovery_fixture(tmp_path, monkeypatch)
    count = next(task for task in f['replacement']['tasks'] if task['name'] == 'rna-counts')
    proof = f['submitted']['tasks']['rna-counts']
    if fault == 'no-declaration': f['descriptor'].pop('rna_recovery')
    elif fault == 'extra-scope': f['descriptor']['phase'] = 'managed-initial'
    elif fault == 'wrong-execution': f['descriptor']['execution_releases']['rna-counts'] = str(f['framework'])
    elif fault == 'framework-source': (f['framework'] / 'src/horsehair/rna_recovery.py').write_text('# changed\n')
    elif fault == 'qualification-pin': f['change']['qualification_sha256'] = '0' * 64
    elif fault.startswith('qualification-'):
        if fault == 'qualification-primary': f['qualification']['primary_source_sha256'] = '0' * 64
        else: f['qualification']['test_counts']['skipped'] = 1
        atomic_json(f['qualification_path'], f['qualification'])
        f['change']['qualification_sha256'] = digest(f['qualification_path'])
    elif fault in {'manifest-pin', 'preparation-pin'}: f['change'][fault.split('-')[0] + '_sha256'] = '0' * 64
    elif fault.startswith('manifest-'):
        key = {'manifest-primary': 'primary_source_identity', 'manifest-framework': 'maintenance_source_identity',
               'manifest-array': 'previous_array_job_id'}[fault]
        f['manifest'][key] = 'changed'
        atomic_json(f['manifest_path'], f['manifest'])
        f['change']['manifest_sha256'] = digest(f['manifest_path'])
    elif fault == 'wrong-array': count['resources']['array'] = '0-58%2'
    elif fault == 'wrong-resources': count['resources']['hours'] = 3
    elif fault == 'wrong-dependency-kind': count['dependency_kind'] = 'afterany'
    elif fault == 'changed-named-dependency': count['depends'] = ['ant-phase']
    elif fault == 'missing-qualified-dependency': f['descriptor']['required_external_dependencies']['rna-counts'] = ['991']
    elif fault == 'missing-preparation-proof': proof['completed_native_dependencies'] = proof['completed_native_dependencies'][:1]
    elif fault in {'failed-qualification', 'failed-preparation'}:
        f['native']['990' if fault == 'failed-qualification' else '991']['state'] = 'FAILED'
    elif fault == 'started-child': f['native'][proof['job_id'] + '_30']['state'] = 'RUNNING'
    else:
        path = Path(count['script']); text = path.read_text()
        if fault == 'wrong-cd': text = text.replace('cd ' + shlex.quote(f['primary'].as_posix()), 'cd ' + shlex.quote(f['framework'].as_posix()))
        elif fault == 'wrong-pythonpath': text = text.replace((f['primary'] / 'src').as_posix(), (f['framework'] / 'src').as_posix())
        elif fault == 'wrong-primary-command':
            old_count = next(task for task in f['old']['tasks'] if task['name'] == 'rna-counts')
            old_path = Path(old_count['script'])
            old_text = old_path.read_text().replace('--stage rna-run', '--stage ant')
            old_path.write_text(old_text); old_count['script_sha256'] = canonical_hash(old_text)
        elif fault == 'wrapper-extra-arg': text += '# harmless\n'  # Additional environment/script bytes are forbidden.
        elif fault == 'wrapper-pin': text = text.replace('--manifest-sha256 ' + f['change']['manifest_sha256'], '--manifest-sha256 ' + '0' * 64)
        elif fault == 'extra-command': text += 'python -m horsehair.cli molecular --stage ant\n'
        elif fault == 'changed-verification': text += 'export OMP_NUM_THREADS=1\n'
        elif fault == 'unheld': text = text.replace('#SBATCH --hold\n', '')
        path.write_text(text); count['script_sha256'] = canonical_hash(text); proof['script_sha256'] = count['script_sha256']
    refresh(f)
    ledger_path = f['root'] / 'state/v7-rna-full-submission.json'
    before = f['plan_path'].read_bytes(), ledger_path.read_bytes()
    with pytest.raises((ValueError, KeyError, TypeError)):
        register(f)
    assert (f['plan_path'].read_bytes(), ledger_path.read_bytes()) == before and not f['calls']

