"""Local, self-contained checks for the one-off metadata retirement action."""
from pathlib import Path
import copy
import importlib.util
import json

import pytest


ACTION = Path(__file__).with_name('operator-actions') / 'deadline-precall-recovery.py'
spec = importlib.util.spec_from_file_location('deadline_recovery_test_runtime', ACTION)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


# Reconstructed from the preserved read-only 807 observation. This is deliberately
# independent of work/ so the tests can also run from a clean checkout.
ORIGINAL_LEDGER = {
    'config_sha256': '446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35',
    'environment_job': None,
    'excluded_nodes': [],
    'plan_sha256': 'e6eed0419dc348bd9159ad6d2e71222a8dbf8271b615ae0bdadc3fd80d3422d3',
    'schema_version': 2,
    'source_sha256': '4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1',
    'tasks': {'af-f2-assembly-0089': {
        'attempt_started_unix': 1792983061.834107,
        'command': ['sbatch', '--parsable',
            '/scratch/pwa209/horsehair-worm/plans/af-f2-assembly-0089/af-f2-assembly-0089.sbatch'],
        'completed_native_dependencies': [],
        'dependency_kind': 'afterok',
        'dependency_resolution_sha256': '0767d1cba473900bc70025593783067dd58b91e9706318e77cdab134efcf1242',
        'issued_native_dependencies': [],
        'script_sha256': 'f1c56959f06c3d77cd68082d6da53c6939e3115be2ae316f632de59ade8ce573',
        'state': 'submitting',
    }},
}


def encoded(value):
    return (json.dumps(value, sort_keys=True, indent=2) + '\n').encode()


@pytest.fixture
def proof(monkeypatch):
    # Minimal stand-ins exercise the semantic checks after provenance has been
    # verified; production pins are never changed by the action itself.
    source = ("def invoke(command):\n"
        "                    require(time.time()+300<c['deadline'],'Submission finalization margin exhausted')\n"
        "                    return runner(command,text=True,capture_output=True,timeout=180)\n").encode()
    log = ("Traceback (most recent call last):\n"
        '  File "coordination.py", line 183, in invoke\n'
        '    result=invoke(command)\n'
        'ValueError: Submission finalization margin exhausted\n').encode()
    script = b'#!/bin/bash\n#SBATCH --hold\nexit 0\n'
    ledger = copy.deepcopy(ORIGINAL_LEDGER)
    row = ledger['tasks'][m.NAME]
    plan = {'tasks': [{'name': m.NAME, 'script': row['command'][2],
        'script_sha256': m.canonical(script.decode())}]}
    ledger['plan_sha256'] = m.canonical(plan)
    row['script_sha256'] = plan['tasks'][0]['script_sha256']
    jobs = {ident: {'status': 'ambiguous', 'job_id': None,
        'native_proof': None, 'receipt': None,
        'intent': {'intent_id': ident}} for ident in (m.IDENT, *m.OTHER)}
    jobs['preserved-complete-job'] = {'status': 'complete', 'job_id': '123'}
    state = {'revision': 526, 'state_sha256': m.STATE_SHA, 'jobs': jobs}
    journal = {'status': 'before_sbatch', 'intent': copy.deepcopy(jobs[m.IDENT]['intent']),
        'plan_sha256': m.digest(encoded(plan))}
    failure = {'admission_authority': False,
        'error': 'Submission finalization margin exhausted', 'error_type': 'ValueError',
        'job_id': m.ACTORS[0], 'scientific_result': False, 'segment': 0, 'status': 'needs_repair'}
    monkeypatch.setattr(m, 'SOURCE_SHA', m.digest(source))
    monkeypatch.setattr(m, 'LOG_SHA', m.digest(log))
    monkeypatch.setattr(m, 'LEDGER_SHA', m.digest(encoded(ledger)))
    return {'ledger': ledger, 'source': source, 'log': log, 'failure': failure,
        'state': state, 'window': {'work_deadline_unix': 1792983138.0},
        'journal': journal, 'plan': plan, 'script': script}


def test_reconstructed_original_ledger_matches_frozen_byte_hash():
    assert m.digest(encoded(ORIGINAL_LEDGER)) == m.LEDGER_SHA


def test_only_proven_row_is_retired_and_all_inputs_and_headers_are_preserved(proof):
    before = copy.deepcopy(proof)
    result = m.recovered_ledger(**proof)
    assert proof == before
    assert result == {**before['ledger'], 'tasks': {}}
    assert encoded({k: v for k, v in result.items() if k != 'tasks'}) == encoded(
        {k: v for k, v in before['ledger'].items() if k != 'tasks'})
    # Returned containers do not alias the live input ledger.
    result['excluded_nodes'].append('unrelated')
    assert proof == before


@pytest.mark.parametrize('key', ['source', 'log'])
def test_changed_frozen_source_or_complete_log_is_refused(proof, key):
    proof[key] += b'changed\n'
    with pytest.raises(ValueError, match='Exact frozen source'):
        m.recovered_ledger(**proof)


def test_source_without_the_guard_before_runner_is_refused_even_if_repinned(proof, monkeypatch):
    proof['source'] = proof['source'].replace(b'                    return runner(', b'                    return other(')
    monkeypatch.setattr(m, 'SOURCE_SHA', m.digest(proof['source']))
    with pytest.raises(ValueError, match='guard must precede'):
        m.recovered_ledger(**proof)


@pytest.mark.parametrize('old,new', [
    (b'line 183, in invoke', b'line 184, in invoke'),
    (b'result=invoke(command)', b'result=runner(command)'),
    (b'ValueError: Submission finalization margin exhausted\n', b'TimeoutError: native timeout\n'),
])
def test_a_different_failure_stack_never_authorizes_retirement(proof, monkeypatch, old, new):
    proof['log'] = proof['log'].replace(old, new)
    monkeypatch.setattr(m, 'LOG_SHA', m.digest(proof['log']))
    with pytest.raises(ValueError, match='pre-call stack'):
        m.recovered_ledger(**proof)


@pytest.mark.parametrize('key,value', [
    ('job_id', 'different-actor'), ('segment', 1), ('status', 'complete'),
    ('error_type', 'TimeoutExpired'), ('error', 'another failure'),
])
def test_failure_must_belong_to_the_exact_actor_and_precall_error(proof, key, value):
    proof['failure'][key] = value
    with pytest.raises(ValueError, match='actor failure'):
        m.recovered_ledger(**proof)


@pytest.mark.parametrize('key,value', [('revision', 527), ('state_sha256', 'changed')])
def test_a_changed_canonical_state_is_refused(proof, key, value):
    proof['state'][key] = value
    with pytest.raises(ValueError, match='Original state'):
        m.recovered_ledger(**proof)


@pytest.mark.parametrize('key,value', [
    ('status', 'submitted'), ('job_id', '123'),
    ('native_proof', {'job_id': '123'}), ('receipt', {'complete': True}),
])
def test_any_accepted_or_reconciled_batch_member_stops_recovery(proof, key, value):
    proof['state']['jobs'][m.OTHER[-1]][key] = value
    with pytest.raises(ValueError, match='exact saved unacknowledged batch'):
        m.recovered_ledger(**proof)


@pytest.mark.parametrize('change', ['missing', 'extra'])
def test_the_pending_batch_membership_must_remain_exact(proof, change):
    if change == 'missing':
        del proof['state']['jobs'][m.OTHER[-1]]
    else:
        proof['state']['jobs']['unexpected'] = copy.deepcopy(proof['state']['jobs'][m.IDENT])
    with pytest.raises(ValueError, match='exact saved unacknowledged batch'):
        m.recovered_ledger(**proof)


@pytest.mark.parametrize('key,value', [
    ('environment_job', '123'), ('excluded_nodes', ['other-node']),
    ('source_sha256', 'changed'), ('config_sha256', 'changed'), ('schema_version', 3),
])
def test_changed_ledger_headers_are_refused(proof, key, value):
    proof['ledger'][key] = value
    with pytest.raises(ValueError, match='Exact original submitting ledger'):
        m.recovered_ledger(**proof)


@pytest.mark.parametrize('key,value', [
    ('job_id', '123'), ('client_response', {'returncode': 0, 'stdout': '123\n', 'stderr': ''}),
    ('client_timeout', {'seconds': 180}), ('acknowledgement_basis', 'actual_sbatch_stdout'),
    ('native_held_observation', {'JobId': '123'}), ('state', 'submitted'),
    ('attempt_started_unix', 1792983062.0), ('command', ['sbatch', '--parsable', 'other.sbatch']),
])
def test_no_accepted_timed_out_or_changed_attempt_can_be_cleared(proof, monkeypatch, key, value):
    proof['ledger']['tasks'][m.NAME][key] = value
    monkeypatch.setattr(m, 'LEDGER_SHA', m.digest(encoded(proof['ledger'])))
    with pytest.raises(ValueError, match='No accepted ID, timeout, response or altered attempt'):
        m.recovered_ledger(**proof)


def test_an_extra_task_row_is_never_discarded(proof, monkeypatch):
    proof['ledger']['tasks']['other-task'] = copy.deepcopy(proof['ledger']['tasks'][m.NAME])
    monkeypatch.setattr(m, 'LEDGER_SHA', m.digest(encoded(proof['ledger'])))
    with pytest.raises(ValueError, match='Only the proven first row'):
        m.recovered_ledger(**proof)


@pytest.mark.parametrize('key', ['script', 'plan'])
def test_changed_script_or_plan_is_refused(proof, key):
    if key == 'script':
        proof['script'] += b'echo altered\n'
    else:
        proof['plan']['tasks'][0]['script'] = '/other/script'
    with pytest.raises(ValueError, match='No accepted ID, timeout, response or altered attempt'):
        m.recovered_ledger(**proof)


@pytest.mark.parametrize('key,value', [
    ('status', 'registered_held_verified'), ('job_id', '123'),
    ('intent', {'intent_id': 'different'}), ('plan_sha256', 'changed'),
])
def test_changed_or_acknowledged_journal_is_refused(proof, key, value):
    proof['journal'][key] = value
    with pytest.raises(ValueError, match='untouched before-sbatch journal'):
        m.recovered_ledger(**proof)


@pytest.mark.parametrize('deadline', [1792983061.834107, 1792983362.0, 1792983139.0])
def test_a_different_deadline_cannot_explain_the_original_attempt(proof, deadline):
    proof['window']['work_deadline_unix'] = deadline
    with pytest.raises(ValueError, match='conservative cutoff'):
        m.recovered_ledger(**proof)
