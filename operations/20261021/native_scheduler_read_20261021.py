"""Bounded, evidenced reads of one exact native Slurm job; never mutations."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

MAX_ATTEMPTS = 3
ATTEMPT_TIMEOUT_SECONDS = 60
BACKOFF_SECONDS = (2, 4)


def _json_exclusive(path, value):
    with Path(path).open('xb') as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True, allow_nan=False).encode())
        stream.flush()
        os.fsync(stream.fileno())


def _raw_exclusive(path, value):
    if value is None:
        body = b''
    elif isinstance(value, bytes):
        body = value
    elif isinstance(value, str):
        body = value.encode()
    else:
        raise TypeError('Native scheduler output must be raw bytes, text or absent')
    with Path(path).open('xb') as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    return {'path': str(Path(path).resolve()), 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}


def run_scontrol_job(job, evidence_dir, *, runner=None, sleeper=None):
    """Return one successful raw response after at most three fresh reads.

    Only subprocess.TimeoutExpired is retried. A nonzero exit or any other
    failure is preserved and raised immediately. The caller retains its exact
    strict owner/account/resource checks; this function parses no job metadata.
    evidence_dir must be new, so an earlier native read is never overwritten.
    Optional injected runner/sleeper arguments exist for local deterministic
    tests and do not change the production command allowlist.
    """
    job = str(job)
    if not re.fullmatch(r'[1-9][0-9]*(?:_(?:0|[1-9][0-9]*))?', job):
        raise ValueError('One exact positive scalar or logical array job ID is required')
    evidence_dir = Path(evidence_dir)
    if evidence_dir.is_symlink():
        raise ValueError('Scheduler evidence directory cannot be a symlink')
    evidence_dir = evidence_dir.resolve()
    evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    command = ['scontrol', 'show', 'job', job, '-o']
    _json_exclusive(evidence_dir / 'intent.json', {
        'operation': 'read-only exact scontrol show job', 'command': command,
        'max_attempts': MAX_ATTEMPTS, 'timeout_per_attempt_seconds': ATTEMPT_TIMEOUT_SECONDS,
        'backoff_seconds': list(BACKOFF_SECONDS), 'metadata_parser_changed': False,
        'records_merged_or_selected': False, 'started_unix': time.time()})
    reader = subprocess.run if runner is None else runner
    sleep = time.sleep if sleeper is None else sleeper
    for attempt in range(1, MAX_ATTEMPTS + 1):
        prefix = evidence_dir / ('attempt-' + str(attempt))
        _json_exclusive(str(prefix) + '-command.json', {'attempt': attempt, 'command': command,
            'timeout_seconds': ATTEMPT_TIMEOUT_SECONDS, 'started_unix': time.time()})
        try:
            response = reader(command, text=True, capture_output=True, check=False,
                timeout=ATTEMPT_TIMEOUT_SECONDS, env=dict(os.environ, TZ='UTC'))
        except subprocess.TimeoutExpired as error:
            stdout = _raw_exclusive(str(prefix) + '-stdout.txt', getattr(error, 'stdout', None))
            stderr = _raw_exclusive(str(prefix) + '-stderr.txt', getattr(error, 'stderr', None))
            retry = attempt < MAX_ATTEMPTS
            _json_exclusive(str(prefix) + '-exception.json', {'type': type(error).__name__,
                'message': str(error), 'timeout_seconds': error.timeout, 'stdout': stdout, 'stderr': stderr,
                'fresh_read_retry': retry, 'finished_unix': time.time()})
            if retry:
                sleep(BACKOFF_SECONDS[attempt - 1])
                continue
            _json_exclusive(evidence_dir / 'result.json', {'status': 'scheduler_read_timed_out',
                'attempts': attempt, 'returned_response': False, 'finished_unix': time.time()})
            raise
        except Exception as error:
            stdout = _raw_exclusive(str(prefix) + '-stdout.txt', getattr(error, 'stdout', None))
            stderr = _raw_exclusive(str(prefix) + '-stderr.txt', getattr(error, 'stderr', None))
            _json_exclusive(str(prefix) + '-exception.json', {'type': type(error).__name__,
                'message': str(error), 'stdout': stdout, 'stderr': stderr, 'fresh_read_retry': False})
            _json_exclusive(evidence_dir / 'result.json', {'status': 'scheduler_read_failed',
                'attempts': attempt, 'returned_response': False, 'exception_type': type(error).__name__})
            raise
        stdout = _raw_exclusive(str(prefix) + '-stdout.txt', response.stdout)
        stderr = _raw_exclusive(str(prefix) + '-stderr.txt', response.stderr)
        _json_exclusive(str(prefix) + '-response.json', {'returncode': response.returncode,
            'stdout': stdout, 'stderr': stderr, 'finished_unix': time.time()})
        if response.returncode != 0:
            _json_exclusive(evidence_dir / 'result.json', {'status': 'scheduler_read_nonzero_exit',
                'attempts': attempt, 'returned_response': False, 'returncode': response.returncode})
            raise subprocess.CalledProcessError(response.returncode, command,
                output=response.stdout, stderr=response.stderr)
        _json_exclusive(evidence_dir / 'result.json', {'status': 'scheduler_read_complete',
            'attempts': attempt, 'returned_response': True, 'metadata_parsed': False,
            'records_merged_or_selected': False, 'finished_unix': time.time()})
        return response
    raise AssertionError('Bounded scheduler read exhausted without a result')
