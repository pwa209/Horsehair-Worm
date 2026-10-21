from pathlib import Path
from types import SimpleNamespace
import importlib.util
import json

import pytest

from horsehair import repair


@pytest.fixture
def shim(monkeypatch):
    spec = importlib.util.spec_from_file_location('private_native_read_guard', Path(__file__).with_name('native-accounting-read-guard-20261021.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    original = repair._repair_native_states
    monkeypatch.setattr(repair, '_repair_native_states', original)
    sleeps = []
    monkeypatch.setattr(module.time, 'sleep', sleeps.append)
    return module, sleeps


def row(job='100_0', raw='200', state='RUNNING', seconds='100', owner='pwa209', account='def-ptewarie_cpu'):
    return '|'.join((job, raw, state, '0:0', seconds, '4', 'cpu=4,mem=32G,node=1', 'nc10902',
                     '2026-10-21T10:00:00', '2026-10-21T10:05:00', owner, account, '10000K'))


def runner_for(outputs, calls):
    def runner(command, **kwargs):
        calls.append(command)
        index = min(len(calls) - 1, len(outputs) - 1)
        return SimpleNamespace(returncode=0, stdout=outputs[index], stderr='')
    return runner


def run_query(tmp_path, shim, outputs):
    module, sleeps = shim
    module.install(tmp_path)
    calls = []
    result = repair._repair_native_states(['100', '100_0'], {'100': {0, 1}},
        runner=runner_for(outputs, calls), owner='pwa209', account='def-ptewarie_cpu')
    return result, calls, sleeps


def test_ordinary_complete_read_is_returned_without_retry_and_raw_is_preserved(tmp_path, shim):
    stdout = row()
    result, calls, sleeps = run_query(tmp_path, shim, [stdout])
    assert len(calls) == 1 and sleeps == [] and result['100_0']['elapsed_seconds'] == 100
    evidence = next(tmp_path.iterdir())
    assert (evidence / 'attempt-1-stdout.txt').read_text() == stdout
    query = json.loads((evidence / 'attempt-1-query.json').read_text())
    assert query['command'] == calls[0] and calls[0][calls[0].index('-j') + 1] == '100'
    response = json.loads((evidence / 'attempt-1-response.json').read_text())
    assert response['captured_before_strict_parse'] is True


def test_live_async_duplicate_requires_fresh_complete_read_and_never_selects_one_row(tmp_path, shim):
    conflict = row(seconds='100') + '\n' + row(seconds='101')
    fresh = row(seconds='105')
    result, calls, sleeps = run_query(tmp_path, shim, [conflict, fresh])
    assert len(calls) == 2 and calls[0] == calls[1] and sleeps == [1]
    assert result['100_0']['elapsed_seconds'] == 105
    evidence = next(tmp_path.iterdir())
    assert (evidence / 'attempt-1-stdout.txt').read_text() == conflict
    assert (evidence / 'attempt-2-stdout.txt').read_text() == fresh
    assert json.loads((evidence / 'result.json').read_text())['complete_reads_attempted'] == 2


def test_all_three_reads_are_bounded_with_one_and_two_second_backoff(tmp_path, shim):
    a = row(seconds='100') + '\n' + row(seconds='101')
    b = row(seconds='102') + '\n' + row(seconds='103')
    result, calls, sleeps = run_query(tmp_path, shim, [a, b, row(seconds='110')])
    assert len(calls) == 3 and sleeps == [1, 2]
    assert result['100_0']['elapsed_seconds'] == 110


def test_persistent_conflict_is_preserved_and_raises_after_three_reads(tmp_path, shim):
    module, sleeps = shim
    module.install(tmp_path)
    calls = []
    stdout = row() + '\n' + row(seconds='101')
    with pytest.raises(ValueError, match='Conflicting repair native records'):
        repair._repair_native_states(['100'], {'100': {0}}, runner=runner_for([stdout], calls))
    assert len(calls) == 3 and sleeps == [1, 2]
    evidence = next(tmp_path.iterdir())
    assert len(list(evidence.glob('attempt-*-stdout.txt'))) == 3
    assert json.loads((evidence / 'result.json').read_text())['returned_evidence'] is False


@pytest.mark.parametrize('states', [('COMPLETED', 'COMPLETED'), ('FAILED', 'FAILED'),
    ('RUNNING', 'COMPLETED'), ('CANCELLED', 'RUNNING'), ('SUSPENDED', 'RUNNING')])
def test_terminal_or_unsupported_conflicts_are_never_retried(tmp_path, shim, states):
    module, sleeps = shim
    module.install(tmp_path)
    calls = []
    stdout = row(state=states[0]) + '\n' + row(state=states[1], seconds='101')
    with pytest.raises(ValueError, match='Conflicting repair native records'):
        repair._repair_native_states(['100'], {'100': {0}}, runner=runner_for([stdout], calls))
    assert len(calls) == 1 and sleeps == []


@pytest.mark.parametrize('extra', [row(job='100_1', raw='201', owner='other'),
    row(job='100_1', raw='201', account='def-other_cpu'), row(job='100_9', raw='209'),
    row(job='100_1', raw='200')])
def test_validation_of_all_rows_catches_bad_metadata_after_first_conflict(tmp_path, shim, extra):
    module, sleeps = shim
    module.install(tmp_path)
    calls = []
    stdout = row() + '\n' + row(seconds='101') + '\n' + extra
    with pytest.raises(ValueError, match='Conflicting repair native records'):
        repair._repair_native_states(['100'], {'100': {0, 1}}, runner=runner_for([stdout], calls),
                                    owner='pwa209', account='def-ptewarie_cpu')
    assert len(calls) == 1 and sleeps == []
    verdict = json.loads(next(tmp_path.glob('*/attempt-1-verdict.json')).read_text())
    assert verdict['classification']['classification'] == 'not_retryable'


def test_non_conflict_parser_and_native_command_failures_are_never_retried(tmp_path, shim):
    module, sleeps = shim
    module.install(tmp_path)
    calls = []
    with pytest.raises(ValueError, match='complete native allocation'):
        repair._repair_native_states(['100'], {'100': {0}}, runner=runner_for(['bad|row'], calls))
    assert len(calls) == 1 and sleeps == []


def test_owner_failure_on_first_row_is_not_retried(tmp_path, shim):
    module, sleeps = shim
    module.install(tmp_path)
    calls = []
    with pytest.raises(ValueError, match='another owner/account'):
        repair._repair_native_states(['100'], {'100': {0}}, runner=runner_for([row(owner='other')], calls), owner='pwa209')
    assert len(calls) == 1 and sleeps == []


def test_same_guard_installation_is_idempotent_and_another_guard_cannot_nest(tmp_path, shim):
    module, sleeps = shim
    assert module.install(tmp_path)['status'] == 'installed'
    original_wrapper = repair._repair_native_states
    assert module.install(tmp_path)['status'] == 'already_installed'
    assert repair._repair_native_states is original_wrapper
    with pytest.raises(ValueError, match='cannot be nested'):
        module.install(tmp_path / 'another')


def test_allowed_live_state_transition_can_retry_only_to_a_strict_complete_snapshot(tmp_path, shim):
    stdout = row(state='RUNNING') + '\n' + row(state='COMPLETING', seconds='101')
    result, calls, sleeps = run_query(tmp_path, shim, [stdout, row(state='COMPLETED', seconds='200')])
    assert len(calls) == 2 and sleeps == [1]
    assert result['100_0']['state'] == 'COMPLETED' and result['100_0']['elapsed_seconds'] == 200
