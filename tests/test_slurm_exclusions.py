"""Controller exclusions must reach sbatch and remain fixed on partial replay."""
import copy
import os
from types import SimpleNamespace

import pytest

from horsehair.core import atomic_json, canonical_hash, read_json
from horsehair.slurm import submit


def fixture(tmp_path):
    script = tmp_path / 'job.sbatch'
    script.write_text('#!/bin/bash\n#SBATCH --cpus-per-task=1\ntrue\n')
    tasks = [{'name': name, 'script': str(script), 'script_sha256': canonical_hash(script.read_text()),
              'depends': [] if index == 0 else ['cpu'],
              'resources': {'cpus': 1, 'memory': '8G', 'hours': 2, 'gpu': index == 1}}
             for index, name in enumerate(['cpu', 'gpu'])]
    plan = {'tasks': tasks}
    plan_path, ledger_path = tmp_path/'plan.json', tmp_path/'ledger.json'
    atomic_json(plan_path, plan)
    calls = []
    def runner(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout=str(100+len(calls)), stderr='')
    return locals()


def test_controller_setting_becomes_explicit_exclusions_on_actual_cpu_and_gpu_commands(tmp_path, monkeypatch):
    f = fixture(tmp_path)
    monkeypatch.setenv('SBATCH_EXCLUDE', 'nc11012,nc10908')
    monkeypatch.delenv('SBATCH_GPUS', raising=False)
    original_script, original_plan = f['script'].read_bytes(), copy.deepcopy(f['plan'])
    ledger = submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert f['calls'] == [
        ['sbatch', '--parsable', '--exclude=nc10908,nc11012', str(f['script'])],
        ['sbatch', '--parsable', '--exclude=nc10908,nc11012', '--dependency=afterok:101', str(f['script'])]]
    assert ledger['excluded_nodes'] == ['nc10908', 'nc11012']
    assert all(task['command'] == command for task, command in zip(ledger['tasks'].values(), f['calls']))
    assert f['script'].read_bytes() == original_script and read_json(f['plan_path']) == original_plan
    assert os.environ['SBATCH_EXCLUDE'] == 'nc11012,nc10908' and 'SBATCH_GPUS' not in os.environ
    assert not any(option.startswith(('--nodes=', '--nodelist=', '--gpus=', '--partition=', '--constraint='))
                   for command in f['calls'] for option in command)


def test_absent_exclusions_preserve_the_original_submission_command(tmp_path, monkeypatch):
    f = fixture(tmp_path)
    monkeypatch.delenv('SBATCH_EXCLUDE', raising=False)
    ledger = submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert f['calls'][0] == ['sbatch', '--parsable', str(f['script'])]
    assert ledger['excluded_nodes'] == []


@pytest.mark.parametrize('value', ['', ' ', 'nc10908,', ',nc10908', 'nc10908,,nc11012',
    'nc10908,nc10908', ' nc10908', 'nc10908 ', 'nc10908\n', 'nc[10908-11012]',
    'nc10908;true', '--nodelist=nc10908', 'nc1090', 'nc109081', 'gpu10908', 'nc10908.example.org'])
def test_invalid_exclusions_fail_before_any_submission_or_ledger_write(tmp_path, monkeypatch, value):
    f = fixture(tmp_path)
    monkeypatch.setenv('SBATCH_EXCLUDE', value)
    with pytest.raises(ValueError, match='Exclusions require'):
        submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert not f['calls'] and not f['ledger_path'].exists()


def partial_ledger(f, excluded_nodes):
    ledger = {'schema_version': 2, 'plan_sha256': canonical_hash(f['plan']),
              'tasks': {'cpu': {'state': 'submitted', 'job_id': '90'}}}
    if excluded_nodes is not None:
        ledger['excluded_nodes'] = excluded_nodes
    atomic_json(f['ledger_path'], ledger)
    return ledger


@pytest.mark.parametrize('value', [None, 'nc10908', 'nc11012,nc10908'])
def test_partial_submission_refuses_changed_or_removed_exclusion_policy(tmp_path, monkeypatch, value):
    f = fixture(tmp_path)
    before = partial_ledger(f, ['nc11012'])
    if value is None:
        monkeypatch.delenv('SBATCH_EXCLUDE', raising=False)
    else:
        monkeypatch.setenv('SBATCH_EXCLUDE', value)
    with pytest.raises(ValueError, match='exclusions changed'):
        submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert not f['calls'] and read_json(f['ledger_path']) == before


def test_partial_submission_keeps_policy_when_only_environment_order_changes(tmp_path, monkeypatch):
    f = fixture(tmp_path)
    partial_ledger(f, ['nc10908', 'nc11012'])
    monkeypatch.setenv('SBATCH_EXCLUDE', 'nc11012,nc10908')
    ledger = submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert f['calls'] == [['sbatch', '--parsable', '--exclude=nc10908,nc11012',
                           '--dependency=afterok:90', str(f['script'])]]
    assert ledger['tasks']['cpu']['job_id'] == '90'


def test_partial_legacy_ledger_continues_only_without_new_exclusions(tmp_path, monkeypatch):
    f = fixture(tmp_path)
    before = partial_ledger(f, None)
    monkeypatch.setenv('SBATCH_EXCLUDE', 'nc11012')
    with pytest.raises(ValueError, match='Legacy submission ledger'):
        submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert not f['calls'] and read_json(f['ledger_path']) == before
    monkeypatch.delenv('SBATCH_EXCLUDE')
    ledger = submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert f['calls'] == [['sbatch', '--parsable', '--dependency=afterok:90', str(f['script'])]]
    assert 'excluded_nodes' not in ledger


@pytest.mark.parametrize('frozen', [None, '', ['nc11012', 'nc10908'], ['nc11012', 'nc11012'],
    ['gpu11012'], [True]])
def test_invalid_frozen_exclusions_do_not_allow_a_new_native_submission(tmp_path, monkeypatch, frozen):
    f = fixture(tmp_path)
    partial_ledger(f, [])
    ledger = read_json(f['ledger_path'])
    ledger['excluded_nodes'] = frozen
    atomic_json(f['ledger_path'], ledger)
    monkeypatch.delenv('SBATCH_EXCLUDE', raising=False)
    with pytest.raises(ValueError):
        submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert not f['calls']


@pytest.mark.parametrize('legacy', [False, True])
def test_completed_ledger_is_unchanged_and_never_reinterprets_current_environment(tmp_path, monkeypatch, legacy):
    f = fixture(tmp_path)
    monkeypatch.setenv('SBATCH_EXCLUDE', 'nc11012')
    ledger = submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    if legacy:
        ledger.pop('excluded_nodes')
        atomic_json(f['ledger_path'], ledger)
    before = f['ledger_path'].read_bytes()
    monkeypatch.setenv('SBATCH_EXCLUDE', 'invalid;current environment')
    assert submit(f['plan_path'], f['ledger_path'], runner=f['runner']) == ledger
    assert len(f['calls']) == 2 and f['ledger_path'].read_bytes() == before


def test_exclusion_cannot_conflict_with_frozen_maintenance_placement(tmp_path, monkeypatch):
    f = fixture(tmp_path)
    f['tasks'][0].update(name='recovery-check', placement_nodes=['nc11012'])
    f['tasks'][1]['depends'] = ['recovery-check']
    atomic_json(f['plan_path'], f['plan'])
    monkeypatch.setenv('SBATCH_EXCLUDE', 'nc11012')
    with pytest.raises(ValueError, match='placement host is excluded'):
        submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert not f['calls'] and not f['ledger_path'].exists()


def test_rejected_submission_retains_explicit_command_and_never_replays(tmp_path, monkeypatch):
    f = fixture(tmp_path)
    monkeypatch.setenv('SBATCH_EXCLUDE', 'nc11012')
    def rejected(command, **kwargs):
        f['calls'].append(command)
        return SimpleNamespace(returncode=1, stdout='', stderr='lost acknowledgement')
    with pytest.raises(RuntimeError, match='Unconfirmed submission'):
        submit(f['plan_path'], f['ledger_path'], runner=rejected)
    before = read_json(f['ledger_path'])
    assert before['excluded_nodes'] == ['nc11012']
    assert '--exclude=nc11012' in before['tasks']['cpu']['command']
    monkeypatch.delenv('SBATCH_EXCLUDE')
    with pytest.raises(RuntimeError, match='prior submission'):
        submit(f['plan_path'], f['ledger_path'], runner=f['runner'])
    assert len(f['calls']) == 1 and read_json(f['ledger_path']) == before
