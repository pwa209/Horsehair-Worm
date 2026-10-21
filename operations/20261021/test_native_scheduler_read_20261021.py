from pathlib import Path
import importlib.util
import json
import subprocess

import pytest


@pytest.fixture
def scheduler_reader():
    spec = importlib.util.spec_from_file_location('private_scheduler_reader',
        Path(__file__).with_name('native_scheduler_read_20261021.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def fake_runner(values, calls):
    def run(command, **kwargs):
        calls.append((list(command), kwargs))
        value = values[min(len(calls) - 1, len(values) - 1)]
        if isinstance(value, Exception):
            raise value
        return value
    return run


def test_two_timeouts_then_success_returns_only_complete_raw_response(tmp_path, scheduler_reader):
    command = ['scontrol', 'show', 'job', '12345_0', '-o']
    first = subprocess.TimeoutExpired(command, 60, output=b'partial first', stderr=b'first stderr')
    second = subprocess.TimeoutExpired(command, 60, output='partial second', stderr='second stderr')
    success = subprocess.CompletedProcess(command, 0, 'complete unchanged native output', '')
    calls, sleeps = [], []
    result = scheduler_reader.run_scontrol_job('12345_0', tmp_path / 'evidence',
        runner=fake_runner([first, second, success], calls), sleeper=sleeps.append)
    assert result is success and sleeps == [2, 4]
    assert len(calls) == 3 and all(call[0] == command for call in calls)
    assert all(call[1]['timeout'] == 60 and call[1]['check'] is False and call[1]['env']['TZ'] == 'UTC' for call in calls)
    assert (tmp_path / 'evidence/attempt-1-stdout.txt').read_bytes() == b'partial first'
    assert (tmp_path / 'evidence/attempt-2-stdout.txt').read_text() == 'partial second'
    assert (tmp_path / 'evidence/attempt-3-stdout.txt').read_text() == success.stdout
    result_proof = json.loads((tmp_path / 'evidence/result.json').read_text())
    assert result_proof['attempts'] == 3 and result_proof['records_merged_or_selected'] is False


def test_timeout_exhaustion_preserves_all_attempts_and_raises(tmp_path, scheduler_reader):
    calls, sleeps = [], []
    error = subprocess.TimeoutExpired(['scontrol'], 60, output=b'still waiting', stderr=b'')
    with pytest.raises(subprocess.TimeoutExpired):
        scheduler_reader.run_scontrol_job('12345', tmp_path / 'evidence',
            runner=fake_runner([error], calls), sleeper=sleeps.append)
    assert len(calls) == 3 and sleeps == [2, 4]
    proof = json.loads((tmp_path / 'evidence/result.json').read_text())
    assert proof['status'] == 'scheduler_read_timed_out' and proof['returned_response'] is False
    assert len(list((tmp_path / 'evidence').glob('attempt-*-exception.json'))) == 3


def test_nonzero_native_exit_is_preserved_and_never_retried(tmp_path, scheduler_reader):
    calls, sleeps = [], []
    response = subprocess.CompletedProcess(['scontrol'], 1, '', 'Invalid job id specified')
    with pytest.raises(subprocess.CalledProcessError) as failure:
        scheduler_reader.run_scontrol_job('12345', tmp_path / 'evidence',
            runner=fake_runner([response], calls), sleeper=sleeps.append)
    assert len(calls) == 1 and sleeps == [] and failure.value.stderr == response.stderr
    proof = json.loads((tmp_path / 'evidence/result.json').read_text())
    assert proof['status'] == 'scheduler_read_nonzero_exit'


@pytest.mark.parametrize('job', ['0', '0123', '-1', '123;scancel 456', '123 456', '123,456',
    '123_[0-3]', '123.batch', '123_01', '', 'all', None, True])
def test_invalid_or_broad_job_selection_is_rejected_before_read(tmp_path, scheduler_reader, job):
    with pytest.raises(ValueError, match='exact positive'):
        scheduler_reader.run_scontrol_job(job, tmp_path / 'evidence',
            runner=lambda *a, **k: pytest.fail('invalid ID reached native command'))
    assert not (tmp_path / 'evidence').exists()


def test_existing_evidence_cannot_be_overwritten_even_after_success(tmp_path, scheduler_reader):
    evidence = tmp_path / 'evidence'
    calls = []
    response = subprocess.CompletedProcess(['scontrol'], 0, 'original raw proof', '')
    scheduler_reader.run_scontrol_job('12345', evidence, runner=fake_runner([response], calls))
    original = (evidence / 'attempt-1-stdout.txt').read_bytes()
    with pytest.raises(FileExistsError):
        scheduler_reader.run_scontrol_job('12345', evidence,
            runner=lambda *a, **k: pytest.fail('replay reached native command'))
    assert (evidence / 'attempt-1-stdout.txt').read_bytes() == original


@pytest.mark.parametrize('error', [OSError('transport failed'), ValueError('invalid native response')])
def test_non_timeout_exception_is_not_retried(tmp_path, scheduler_reader, error):
    calls, sleeps = [], []
    with pytest.raises(type(error)):
        scheduler_reader.run_scontrol_job('12345', tmp_path / 'evidence',
            runner=fake_runner([error], calls), sleeper=sleeps.append)
    assert len(calls) == 1 and sleeps == []
    assert json.loads((tmp_path / 'evidence/attempt-1-exception.json').read_text())['fresh_read_retry'] is False
