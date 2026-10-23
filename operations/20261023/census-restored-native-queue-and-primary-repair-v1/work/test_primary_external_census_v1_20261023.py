"""Primary orchestration contracts; no native allocations or study mutations."""
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from datetime import datetime, timezone
import ast
import copy
import importlib.util
import json
import os
import runpy
import shlex
import subprocess
import sys

import pytest

SOURCE = Path(__file__).with_name('run-primary-external-census-v1-20261023.py')
spec = importlib.util.spec_from_file_location('primary_external_census_policy', SOURCE)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def test_real_primary_source_derivative_preserves_normal_body_and_all_original_objects():
    from horsehair import pipeline
    before = {k: v for k, v in vars(pipeline).items() if callable(v)}
    consumer = lambda root: None
    function, proof = m.derive_run(pipeline, consumer)
    assert function is not pipeline.run and function.__globals__ is not vars(pipeline)
    assert function.__globals__['tick'] is pipeline.tick and function.__globals__['__file__'] == pipeline.__file__
    assert proof['normal_branch_ast_preserved'] and proof['nested_storage_guard_ast_preserved']
    assert proof['pipeline_source_sha256'] == m.PIPELINE_SHA
    assert all(getattr(pipeline, key) is value for key, value in before.items())
    assert function.__globals__['__primary_consumer'] is consumer


def test_changed_primary_source_is_not_accepted_as_original(tmp_path):
    path = tmp_path/'pipeline.py'
    path.write_text('def run():\n pass\n')
    with pytest.raises(ValueError, match='Exact original'):
        m.derive_run(SimpleNamespace(__file__=str(path)), lambda root: None)


def test_original_quota_call_and_return_are_preserved_with_checks_on_both_sides():
    calls = []
    record = {'positive_actual_quota': True}
    def original(root, *args, **kwargs):
        calls.append(('quota', root, args, kwargs))
        return record
    result = m.quota_guard(original, lambda root: calls.append(('census', root)))('/synthetic/root', required_files=3, reserve_files=250000)
    assert result is record
    assert calls == [('census', '/synthetic/root'), ('quota', '/synthetic/root', (), {'required_files': 3, 'reserve_files': 250000}), ('census', '/synthetic/root')]


@pytest.mark.parametrize('location', ['before', 'after'])
def test_missing_or_expired_census_prevents_quota_or_use_of_its_return(location):
    calls = []
    def consumer(root):
        calls.append('census')
        if location == 'before' or len(calls) == 3:
            raise m.PendingExternalCensus('No finalized producer')
    def quota(root):
        calls.append('quota')
        return {'positive': True}
    with pytest.raises(m.PendingExternalCensus):
        m.quota_guard(quota, consumer)('/synthetic')
    assert calls == (['census'] if location == 'before' else ['census', 'quota', 'census'])


def test_only_the_exact_owned_thirty_second_quota_timeout_is_pending():
    def original(root):
        raise subprocess.TimeoutExpired(list(m.QUOTA_COMMAND), 30)
    with pytest.raises(m.PendingNativeQuota) as caught:
        m.quota_guard(original, lambda root: None)('/synthetic')
    assert isinstance(caught.value.__cause__, subprocess.TimeoutExpired)


@pytest.mark.parametrize('command,timeout', [(['other', 'command'], 30), (['lfs', 'quota', '-u', 'another', '/scratch/pwa209'], 30),
    (['lfs', 'quota', '-u', '3164768', '/scratch/shared'], 30), (m.QUOTA_COMMAND, 60), (tuple(m.QUOTA_COMMAND), 30)])
def test_other_timeout_commands_or_limits_are_not_hidden(command, timeout):
    error = subprocess.TimeoutExpired(command, timeout)
    def original(root):
        raise error
    with pytest.raises(subprocess.TimeoutExpired) as caught:
        m.quota_guard(original, lambda root: None)('/synthetic')
    assert caught.value is error


@pytest.mark.parametrize('error', [OSError('I/O'), ValueError('source changed'), RuntimeError('native failure')])
def test_arbitrary_quota_failures_are_never_relabelled_as_waiting(error):
    def original(root):
        raise error
    with pytest.raises(type(error)) as caught:
        m.quota_guard(original, lambda root: None)('/synthetic')
    assert caught.value is error


def test_pending_record_never_preserves_scientific_completion_from_previous_status():
    previous = {'scientific_results_complete': True, 'publication_ready': True, 'phases': {'existing': 'unchanged'}}
    value = m.pending_record(previous, {'source_identity': {'sha256': m.PRIMARY_SHA}}, {'source_identity': {'sha256': m.OLD_SOURCE}}, 2, m.PendingNativeQuota('timed out'))
    assert value['status'] == 'pending_native_quota' and value['scheduling_paused'] is True
    assert value['scientific_results_complete'] is False and value['publication_ready'] is False
    assert previous['scientific_results_complete'] is True and value['phases'] == previous['phases']


def test_module_environment_retains_cvmfs_but_never_another_horsehair_release(tmp_path, monkeypatch):
    old = tmp_path/'horsehair-worm/releases/old'
    new = tmp_path/'horsehair-worm/releases/new'
    monkeypatch.setattr(m, 'OLD_FRAMEWORK', old)
    monkeypatch.setattr(m, 'FRAMEWORK', new)
    entries = [str(new/'src'), str(old/'src'), '/cvmfs/python/site-packages', '/cvmfs/ipykernel/site-packages', str(tmp_path/'horsehair-worm/releases/foreign/src')]
    monkeypatch.setenv('PYTHONPATH', os.pathsep.join(entries))
    monkeypatch.setenv('PYTEST_ADDOPTS', '-k unsafe')
    monkeypatch.setenv('PYTEST_PLUGINS', 'foreign')
    env = m.environment(old)
    assert env['PYTHONPATH'].split(os.pathsep) == [str(old.resolve()/'src'), '/cvmfs/python/site-packages', '/cvmfs/ipykernel/site-packages']
    assert 'PYTEST_ADDOPTS' not in env and 'PYTEST_PLUGINS' not in env and env['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] == '1'


@pytest.fixture
def registered(tmp_path, monkeypatch):
    root = tmp_path/'study'
    directory = root/'plans/new-primary'
    directory.mkdir(parents=True)
    (root/'state').mkdir()
    monkeypatch.setattr(m, 'ROOT', root)
    descriptor = root/'policy.json'
    descriptor.write_text('{}')
    args = SimpleNamespace(native_plan=directory/'plan.json', native_ledger=root/'state/new-submission.json', native_script=directory/'task.sbatch',
        native_task='task', policy_descriptor=descriptor, policy_descriptor_sha256=m.digest(descriptor), segment=0)
    tail = ['--mode', 'run', '--policy-descriptor', str(descriptor), '--policy-descriptor-sha256', m.digest(descriptor), '--native-plan', str(args.native_plan),
        '--native-ledger', str(args.native_ledger), '--native-task', 'task', '--native-script', str(args.native_script), '--segment', '0']
    command = shlex.join([sys.executable, '-s', '-u', str(SOURCE.resolve()), *tail])
    args.native_script.write_bytes(('#!/bin/bash\n'+command+'\n').encode())
    task = {'name': 'task', 'script': str(args.native_script), 'command': command, 'script_sha256': m.canonical_hash(args.native_script.read_text()),
        'resources': {'cpus': 1, 'memory': '64G', 'hours': 72, 'gpu': False, 'array': None},
        'primary_external_census': {'helper': m.locator(SOURCE.resolve()), 'policy_descriptor': m.locator(descriptor), 'mode': 'run'}}
    plan = {'root': str(root), 'config_sha256': m.CONFIG, 'release_identity': {'sha256': m.OLD_SOURCE}, 'tasks': [task]}
    ledger = {'config_sha256': m.CONFIG, 'source_sha256': m.OLD_SOURCE, 'plan_sha256': m.canonical_hash(plan),
        'tasks': {'task': {'state': 'submitted', 'job_id': '999', 'script_sha256': task['script_sha256']}}}
    args.native_plan.write_text(json.dumps(plan))
    args.native_ledger.write_text(json.dumps(ledger))
    return SimpleNamespace(root=root, args=args, tail=tail, task=task, plan=plan, ledger=ledger)


def save_registered(f):
    f.args.native_plan.write_text(json.dumps(f.plan))
    f.ledger['plan_sha256'] = m.canonical_hash(f.plan)
    f.args.native_ledger.write_text(json.dumps(f.ledger))


def test_actual_registered_primary_helper_arguments_source_and_resources_join(registered):
    f = registered
    proof, task = m.own_registration(f.args, '999', f.tail, 'run', m.digest(f.args.policy_descriptor))
    assert proof['native_job_id'] == '999' and task == f.task


@pytest.mark.parametrize('change', ['source', 'config', 'job', 'script', 'cpu', 'memory', 'hours', 'array', 'intent', 'argv'])
def test_resealed_wrong_primary_native_registration_is_refused(registered, change):
    f = registered
    if change == 'source': f.plan['release_identity']['sha256'] = '0'*64
    elif change == 'config': f.ledger['config_sha256'] = '0'*64
    elif change == 'job': f.ledger['tasks']['task']['job_id'] = '998'
    elif change == 'script': f.args.native_script.write_text('changed')
    elif change == 'cpu': f.task['resources']['cpus'] = 4
    elif change == 'memory': f.task['resources']['memory'] = '8G'
    elif change == 'hours': f.task['resources']['hours'] = 144
    elif change == 'array': f.task['resources']['array'] = '1-4'
    elif change == 'intent': f.task['primary_external_census']['mode'] = 'qualify'
    else: f.tail[-1] = '1'
    save_registered(f)
    with pytest.raises(ValueError):
        m.own_registration(f.args, '999', f.tail, 'run', m.digest(f.args.policy_descriptor))


@pytest.fixture
def native(registered, monkeypatch):
    end = 100000.
    monkeypatch.setenv('SLURM_JOB_END_TIME', str(end))
    fields = {'JobId': '999', 'UserId': 'pwa209(3164768)', 'Account': 'def-ptewarie_cpu', 'JobState': 'RUNNING', 'JobName': 'hhw-task',
        'NumCPUs': '1', 'NumNodes': '1', 'MinMemoryNode': '64G', 'Command': str(registered.args.native_script), 'TimeLimit': '3-00:00:00',
        'ReqTRES': 'cpu=1,mem=64G,node=1,billing=1000', 'AllocTRES': 'cpu=1,mem=64G,node=1,billing=1000',
        'Socks/Node': '*', 'NtasksPerN:B:S:C': '0:0:*:*', 'EndTime': datetime.fromtimestamp(end, timezone.utc).isoformat().removesuffix('+00:00')}
    def reader(job, area):
        return subprocess.CompletedProcess([], 0, ' '.join(k+'='+v for k, v in fields.items()), '')
    return fields, reader


def test_owned_native_allocation_and_actual_slash_colon_fields(registered, native):
    fields, reader = native
    actual, end = m.native_allocation('999', registered.args, 'run', reader, registered.root, wall=lambda: 1000.)
    assert actual == fields and end == 100000.


@pytest.mark.parametrize('field,value', [('UserId', 'foreign(1)'), ('JobId', '998'), ('Command', '/foreign'), ('NumCPUs', '4'),
    ('MinMemoryNode', '8G'), ('TimeLimit', '4-00:00:00'), ('ReqTRES', 'cpu=1,mem=64G,node=1,gres/gpu=0'), ('ArrayJobId', '999')])
def test_wrong_native_allocation_is_never_accepted(registered, native, field, value):
    fields, reader = native
    fields[field] = value
    with pytest.raises(ValueError):
        m.native_allocation('999', registered.args, 'run', reader, registered.root, wall=lambda: 1000.)


def test_duplicate_native_scheduler_fields_are_refused():
    response = subprocess.CompletedProcess([], 0, 'JobId=999 JobId=998 Socks/Node=* NtasksPerN:B:S:C=0:0:*:*', '')
    with pytest.raises(ValueError, match='Duplicate'):
        m.native_fields(response)


@pytest.mark.parametrize('delta', [-1., 0., 1.])
def test_accepted_native_end_tolerance_is_conservative(registered, native, monkeypatch, delta):
    fields, reader = native
    monkeypatch.setenv('SLURM_JOB_END_TIME', str(100000.+delta))
    _, end = m.native_allocation('999', registered.args, 'run', reader, registered.root, wall=lambda: 1000.)
    assert end == min(100000., 100000.+delta)
    assert 0 <= 100000.+delta-end <= 2


@pytest.fixture
def bridge(registered, tmp_path, monkeypatch):
    monkeypatch.setattr(m, 'source_guard', lambda pins: None)
    temporary = tmp_path/'allocation'
    temporary.mkdir()
    c = {'args': registered.args, 'argv': registered.tail, 'parent_mode': 'run', 'job': '999', 'descriptor_sha256': m.digest(registered.args.policy_descriptor),
        'end': 30000., 'temporary': temporary, 'allocation': {}, 'registration': {}, 'sequence': 0, 'pins': {}}
    def positive(sequence):
        return {'status': 'verified_complete_fresh_external_census', 'job_id': '999', 'sequence': sequence,
            'policy_descriptor': {'path': str(c['args'].policy_descriptor), 'sha256': c['descriptor_sha256']},
            'measured_unix': 900., 'captured_utc': datetime.fromtimestamp(900., timezone.utc).isoformat(), 'pair': {}, 'provider': {}, 'actual_provider_completed': True, **m.FALSE_FLAGS}
    return SimpleNamespace(c=c, positive=positive, root=registered.root)


def test_verifier_request_uses_isolated_framework_and_full_parent_context(bridge):
    calls = []
    def runner(command, **kwargs):
        calls.append((command, kwargs))
        request = json.loads(Path(command[command.index('--request')+1]).read_text())
        assert request['argv'] == bridge.c['argv'] and request['job'] == '999'
        return subprocess.CompletedProcess(command, 0, json.dumps(bridge.positive(1)).encode(), b'')
    result = m.make_consumer(bridge.c, runner=runner, wall=lambda: 1000.)(bridge.root)
    assert result['status'] == 'verified_complete_fresh_external_census'
    assert calls[0][1]['cwd'] == m.FRAMEWORK and calls[0][1]['env']['PYTHONPATH'].split(os.pathsep)[0] == str(m.FRAMEWORK.resolve()/'src')


def test_verified_pending_child_remains_pending_in_parent(bridge):
    def runner(command, **kwargs):
        return subprocess.CompletedProcess(command, 75, json.dumps({'status': 'pending_external_storage_census', 'job_id': '999', 'reason': 'producer running', **m.FALSE_FLAGS}).encode(), b'')
    with pytest.raises(m.PendingExternalCensus, match='producer running'):
        m.make_consumer(bridge.c, runner=runner, wall=lambda: 1000.)(bridge.root)


@pytest.mark.parametrize('change', ['job', 'sequence', 'descriptor', 'authority', 'stale', 'future', 'stderr', 'returncode'])
def test_wrong_or_stale_verifier_response_never_admits_work(bridge, change):
    def runner(command, **kwargs):
        value = bridge.positive(1)
        if change == 'job': value['job_id'] = '998'
        elif change == 'sequence': value['sequence'] = 0
        elif change == 'descriptor': value['policy_descriptor']['sha256'] = '0'*64
        elif change == 'authority': value['admission_authority'] = True
        elif change == 'stale': value['measured_unix'] = -22000.
        elif change == 'future': value['measured_unix'] = 1001.
        return subprocess.CompletedProcess(command, 1 if change == 'returncode' else 0, json.dumps(value).encode(), b'failure' if change == 'stderr' else b'')
    with pytest.raises(ValueError):
        m.make_consumer(bridge.c, runner=runner, wall=lambda: 1000.)(bridge.root)


def test_verifier_does_not_consume_original_four_hour_finalization_margin(bridge):
    cutoff = bridge.c['end']-4*3600
    calls = []
    def runner(command, **kwargs):
        calls.append(kwargs['timeout'])
        return subprocess.CompletedProcess(command, 0, json.dumps(bridge.positive(1)).encode(), b'')
    with pytest.raises(m.PendingExternalCensus, match='termination allowance'):
        m.make_consumer(bridge.c, runner=runner, wall=lambda: cutoff-20)(bridge.root)
    assert calls == []
    times = iter([cutoff-60, cutoff-60, cutoff-60, cutoff+1])
    with pytest.raises(m.PendingExternalCensus, match='expired during isolated'):
        m.make_consumer(bridge.c, runner=runner, wall=lambda: next(times))(bridge.root)
    assert calls == [60.]


def test_valid_census_expiring_after_child_response_becomes_pending_not_fatal(bridge):
    bridge.c['end'] = 100000.
    def runner(command, **kwargs):
        value = bridge.positive(1)
        value['measured_unix'] = 0.
        value['captured_utc'] = datetime.fromtimestamp(0., timezone.utc).isoformat()
        return subprocess.CompletedProcess(command, 0, json.dumps(value).encode(), b'')
    times = iter([21599., 21599., 21599., 21599., 21600.])
    with pytest.raises(m.PendingExternalCensus, match='expired before use'):
        m.make_consumer(bridge.c, runner=runner, wall=lambda: next(times))(bridge.root)


def test_crossing_legitimate_provider_publication_becomes_pending():
    pairs = iter([({'measured': 1}, {'budget': 'a', 'entry_sizes': 'b'}), ({'measured': 2}, {'budget': 'c', 'entry_sizes': 'd'})])
    outer = SimpleNamespace(fresh_pair=lambda base: next(pairs), provider_proof=lambda base, context, pair: {'final_native_producer': True})
    with pytest.raises(m.PendingExternalCensus, match='changed during'):
        m.provider_pair_join(outer, object(), {})


def test_same_complete_provider_pair_retains_its_original_clocks_and_proof():
    original = {'measured_unix': 1000., 'captured_utc': '1970-01-01T00:16:40+00:00'}
    pair = {'budget': 'a', 'entry_sizes': 'b'}
    proof = {'final_native_producer': True}
    outer = SimpleNamespace(fresh_pair=lambda base: (original, pair), provider_proof=lambda base, context, actual: proof)
    current, joined, result = m.provider_pair_join(outer, object(), {})
    assert current is original and joined is pair and result is proof


def test_finite_verifier_allowance_pauses_without_more_requests_or_child_reads(bridge):
    bridge.c['sequence'] = m.MAX_CHECKS
    calls = []
    with pytest.raises(m.PendingExternalCensus, match='allowance exhausted'):
        m.make_consumer(bridge.c, runner=lambda *a, **k: calls.append(a), wall=lambda: 1000.)(bridge.root)
    assert calls == [] and not list(bridge.c['temporary'].iterdir())


@pytest.mark.parametrize('failure', [False, True])
def test_original_d89_lease_surrounds_startup_and_loop_and_blocks_all_work_on_refusal(bridge, monkeypatch, failure):
    events = []
    @contextmanager
    def lease(root, config, **kwargs):
        events.append(('lease-enter', kwargs))
        if failure:
            raise ValueError('Original budget refused')
        try:
            yield
        finally:
            events.append(('lease-exit',))
    def consumer(root):
        events.append(('consumer',))
    pipeline = SimpleNamespace(tick=object())
    def private(*args, **kwargs):
        events.append(('run',))
        return {'status': 'active'}
    monkeypatch.setattr(m, 'derive_run', lambda p, c: (private, {'original_callables': {'tick': pipeline.tick}}))
    monkeypatch.setattr(m, 'package_origins', lambda framework: {})
    bridge.c.update(config={'resources': {'cpu_core_hours_cap': 20000}}, area=bridge.c['temporary'])
    if failure:
        with pytest.raises(ValueError, match='Original budget'):
            m.execute_primary(bridge.c, consumer=consumer, pipeline=pipeline, lease_factory=lease, clock=lambda: 1000.)
        assert len(events) == 1
    else:
        assert m.execute_primary(bridge.c, consumer=consumer, pipeline=pipeline, lease_factory=lease, clock=lambda: 1000.)['status'] == 'active'
        assert [x[0] for x in events] == ['lease-enter', 'consumer', 'run', 'lease-exit']
    assert events[0][1] == {'cpus': 1, 'gpus': 0, 'max_hours': 72, 'job_id': '999'}


def test_pending_cli_exit_code_is_real_subprocess_exit_75():
    # Execute the actual entry guard and actual pending branch, with only the
    # read-only native dependencies replaced inside this disposable child.
    code = '''import ast,json,os,runpy,sys
source=sys.argv[1]
namespace=runpy.run_path(source,run_name='primary_policy_child_fixture')
g=namespace['main'].__globals__
g['os'].geteuid=lambda:3164768
g['os'].environ['USER']='pwa209'
g['request_context']=lambda args:{'job':'999'}
def pending(context):raise g['PendingExternalCensus']('producer not finalized')
g['verify_census']=pending
sys.argv=[source,'--mode','verify']
entry=ast.parse(open(source,encoding='utf8').read()).body[-1]
g['__name__']='__main__'
exec(compile(ast.Module(body=[entry],type_ignores=[]),source,'exec'),g)
'''
    response = subprocess.run([sys.executable, '-s', '-c', code, str(SOURCE.resolve())], capture_output=True, env=os.environ.copy(), timeout=30)
    assert response.returncode == 75, response.stderr.decode()
    assert json.loads(response.stdout)['status'] == 'pending_external_storage_census'


def test_candidate_never_contains_scheduler_retirement_or_global_census_assignment():
    text = SOURCE.read_text()
    tree = ast.parse(text, feature_version=(3, 11))
    assert not any(isinstance(n, ast.Attribute) and isinstance(getattr(n, 'ctx', None), ast.Store)
        and n.attr in {'run', 'tick', 'storage_snapshot', 'require_storage_headroom'} for n in ast.walk(tree))
    assert not any(word in text for word in ('scancel', 'scontrol release', 'scontrol hold'))
