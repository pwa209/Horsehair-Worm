"""Placement is a pinned, task-local scheduler request for CPU maintenance."""
import copy
import os
from types import SimpleNamespace

import pytest

from horsehair.core import atomic_json, canonical_hash, read_json
from horsehair.slurm import submit


@pytest.fixture(autouse=True)
def baseline_submission_environment(monkeypatch):
    # These tests isolate task-local placement. A controller's legitimate
    # inherited exclusions are covered separately in test_slurm_exclusions.py.
    monkeypatch.delenv('SBATCH_EXCLUDE', raising=False)


def fixture(tmp_path, name='pipeline-controller-0', gpu=False):
    script = tmp_path / 'job.sbatch'
    text = '#!/bin/bash\n#SBATCH --cpus-per-task=1\n#SBATCH --hold\ntrue\n'
    script.write_text(text)
    task = {'name': name, 'script': str(script), 'script_sha256': canonical_hash(text), 'depends': [],
            'resources': {'cpus': 1, 'memory': '8G', 'hours': 1, 'gpu': gpu},
            'placement_nodes': ['nc10504']}
    plan_path, ledger_path = tmp_path / 'plan.json', tmp_path / 'ledger.json'
    plan = {'tasks': [task]}
    atomic_json(plan_path, plan)
    calls = []
    def runner(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout=str(100 + len(calls)), stderr='')
    return locals()


@pytest.mark.parametrize('name', ['pipeline-controller-0', 'pipeline-controller-3', 'recovery-stat-probe', 'recovery-443'])
def test_cpu_maintenance_placement_is_actual_command_and_preserves_script_resources(tmp_path, monkeypatch, name):
    f = fixture(tmp_path, name)
    original_script = f['script'].read_bytes()
    resources = copy.deepcopy(f['task']['resources'])
    monkeypatch.delenv('SBATCH_NODELIST', raising=False)
    ledger = submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert f['calls'] == [['sbatch', '--parsable', '--nodelist=nc10504', str(f['script'])]]
    assert ledger['tasks'][name]['command'] == f['calls'][0]
    assert f['script'].read_bytes() == original_script
    assert read_json(f['plan_path'])['tasks'][0]['resources'] == resources
    assert 'SBATCH_NODELIST' not in os.environ


@pytest.mark.parametrize('nodes', [None, '', 'nc10504', [], ['nc10504', 'nc10504'], ['nc10504', 'nc10908'],
    [''], [None], [True], [10504], [' nc10504'], ['nc10504 '], ['nc10504\n'], ['nc10504,nc10908'],
    ['nc[10504]'], ['nc10504;true'], ['--nodes=1'], ['gpu10504'], ['nc1234'], ['nc123456'], ['nc10504.example.org']])
def test_invalid_or_multiple_placement_nodes_are_rejected_before_submission(tmp_path, nodes):
    f = fixture(tmp_path)
    f['task']['placement_nodes'] = nodes
    atomic_json(f['plan_path'], f['plan'])
    with pytest.raises(ValueError, match='Placement requires'):
        submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert not f['calls'] and not f['ledger_path'].exists()


@pytest.mark.parametrize('name,gpu', [('rna-counts', False), ('agent-final', True),
    ('pipeline-controller-4', False), ('pipeline-controller-0', True), ('recovery-stat-probe', True),
    ('recovery-', False), ('recovery-Bad', False), ('recovery-injected;true', False)])
def test_scientific_gpu_and_unsafe_task_names_cannot_request_maintenance_placement(tmp_path, name, gpu):
    f = fixture(tmp_path, name, gpu)
    with pytest.raises(ValueError, match='Placement requires'):
        submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert not f['calls'] and not f['ledger_path'].exists()


def test_invalid_later_task_does_not_submit_an_earlier_valid_task(tmp_path):
    f = fixture(tmp_path)
    f['plan']['tasks'].append({**copy.deepcopy(f['task']), 'name': 'rna-counts'})
    atomic_json(f['plan_path'], f['plan'])
    with pytest.raises(ValueError, match='Placement requires'):
        submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert not f['calls'] and not f['ledger_path'].exists()


def test_placement_is_pinned_by_plan_and_replay_never_resubmits(tmp_path):
    f = fixture(tmp_path)
    first = submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert submit(f['plan_path'], f['ledger_path'], runner=f['runner']) == first
    assert len(f['calls']) == 1
    f['task']['placement_nodes'] = ['nc10908']
    atomic_json(f['plan_path'], f['plan'])
    with pytest.raises(ValueError, match='another plan'):
        submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert len(f['calls']) == 1


def test_controller_placement_never_leaks_to_a_later_gpu_task_or_dependency_policy(tmp_path, monkeypatch):
    f = fixture(tmp_path)
    gpu = copy.deepcopy(f['task'])
    gpu.update(name='agent-final', depends=['pipeline-controller-0'], dependency_kind='afterany')
    gpu['resources']['gpu'] = True
    gpu.pop('placement_nodes')
    f['plan']['tasks'].append(gpu)
    atomic_json(f['plan_path'], f['plan'])
    monkeypatch.delenv('SBATCH_NODELIST', raising=False)
    ledger = submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert '--nodelist=nc10504' in f['calls'][0]
    assert f['calls'][1] == ['sbatch', '--parsable', '--dependency=afterany:101', str(f['script'])]
    assert ledger['tasks']['agent-final']['dependency_kind'] == 'afterany'
    assert 'SBATCH_NODELIST' not in os.environ


def test_absent_placement_leaves_existing_submission_command_unchanged(tmp_path):
    f = fixture(tmp_path, name='any-existing-name')
    f['task'].pop('placement_nodes')
    f['task'].pop('resources')
    atomic_json(f['plan_path'], f['plan'])
    ledger = submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert ledger['tasks']['any-existing-name']['command'] == ['sbatch', '--parsable', str(f['script'])]

