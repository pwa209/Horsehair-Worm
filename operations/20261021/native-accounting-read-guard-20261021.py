"""Bounded fresh reads for strictly validated live sacct snapshot conflicts.

No returned record is selected or merged. Only a fresh, complete query which
passes the unchanged repair parser can return evidence to its caller.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

CONFLICT = 'Conflicting repair native records'
LIVE_STATES = frozenset({'PENDING', 'RUNNING', 'CONFIGURING', 'COMPLETING'})
MAX_READS = 3
BACKOFF = (1, 2)
MARKER = '__hhw_native_accounting_read_guard__'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def _exclusive_json(path, value):
    body = json.dumps(value, sort_keys=True, indent=2, allow_nan=False).encode()
    with Path(path).open('xb') as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())


def _exclusive_text(path, value):
    if isinstance(value, bytes):
        body = value
    else:
        body = (value or '').encode()
    with Path(path).open('xb') as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    return hashlib.sha256(body).hexdigest()


def classify_live_conflict(stdout, rosters, *, parser, owner=None, account=None):
    """Validate ALL rows before recognizing a possible live snapshot race."""
    rows, step_rows = [], {}
    for line in stdout.splitlines():
        # The original parser independently validates format, owner/account,
        # allocation identity and the exact frozen array roster of every row.
        parsed = parser(line, rosters, owner=owner, account=account)
        ident = line.split('|', 1)[0]
        if '.' in ident:
            step_rows.setdefault(ident.split('.', 1)[0], []).append(line)
        else:
            require(set(parsed) == {ident}, 'An allocation row has ambiguous identity')
            rows.append((ident, line, parsed[ident]))
    grouped, physical = {}, {}
    for ident, line, record in rows:
        # Test each candidate against its own step-memory evidence as well.
        # Unallocated metadata plus positive step RSS is never a retryable read.
        parser('\n'.join([line, *step_rows.get(ident, [])]), rosters, owner=owner, account=account)
        grouped.setdefault(ident, []).append(record)
        if not record['unallocated_metadata']:
            raw = record['job_id_raw']
            require(raw not in physical or physical[raw] == ident,
                    'Actual physical native allocation has conflicting logical identities')
            physical[raw] = ident
    conflicts = {ident: values for ident, values in grouped.items()
                 if len({json.dumps(value, sort_keys=True) for value in values}) > 1}
    require(conflicts, 'No validated conflicting allocation duplicates were found')
    require(all(value['state'] in LIVE_STATES for values in conflicts.values() for value in values),
            'Conflicting terminal or unsupported native state cannot be retried')
    return {'classification': 'strictly_validated_live_snapshot_conflict',
            'conflicting_job_ids': sorted(conflicts),
            'conflicting_states': {ident: sorted({value['state'] for value in values}) for ident, values in conflicts.items()},
            'validated_allocation_rows': len(rows), 'records_selected_or_merged': False}


def install(evidence_dir):
    """Install before importing a driver which binds _repair_native_states."""
    from horsehair import repair
    evidence_dir = Path(evidence_dir)
    require(not evidence_dir.is_symlink(), 'Native read evidence directory cannot be a symlink')
    evidence_dir = evidence_dir.resolve()
    evidence_dir.mkdir(parents=True, exist_ok=True)
    original = repair._repair_native_states
    identity = {'path': str(Path(__file__).resolve()),
                'sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                'evidence_dir': str(evidence_dir), 'max_complete_reads': MAX_READS, 'backoff_seconds': list(BACKOFF)}
    installed = getattr(original, MARKER, None)
    if installed is not None:
        require(installed == identity, 'Another native read guard is already installed; guards cannot be nested')
        return dict(identity, status='already_installed')
    parser = repair._parse_repair_native

    def guarded(ids, array_rosters, *, runner=None, owner=None, account=None):
        requested_ids = list(map(str, ids))
        rosters = copy.deepcopy(array_rosters)
        actual_runner = subprocess.run if runner is None else runner
        call_dir = evidence_dir / ('read-' + str(time.time_ns()) + '-' + str(os.getpid()) + '-' + uuid.uuid4().hex)
        call_dir.mkdir(mode=0o700, exist_ok=False)
        _exclusive_json(call_dir / 'intent.json', {**identity, 'requested_ids': requested_ids,
            'array_rosters': {str(key): sorted(value) for key, value in rosters.items()},
            'owner': owner, 'account': account, 'started_unix': time.time(),
            'operation': 'read-only complete sacct query', 'scientific_source_modified': False})
        for attempt in range(1, MAX_READS + 1):
            captured = {}
            prefix = call_dir / ('attempt-' + str(attempt))
            def auditing_runner(command, **kwargs):
                require(command and command[0] == 'sacct', 'Read guard can execute only sacct')
                _exclusive_json(str(prefix) + '-query.json', {'command': list(command),
                    'text': kwargs.get('text'), 'capture_output': kwargs.get('capture_output'),
                    'started_unix': time.time(), 'attempt': attempt})
                try:
                    response = actual_runner(command, **kwargs)
                except BaseException as error:
                    _exclusive_json(str(prefix) + '-runner-error.json', {'type': type(error).__name__, 'message': str(error)})
                    raise
                captured['stdout'] = response.stdout
                captured['stdout_sha256'] = _exclusive_text(str(prefix) + '-stdout.txt', response.stdout)
                captured['stderr_sha256'] = _exclusive_text(str(prefix) + '-stderr.txt', getattr(response, 'stderr', ''))
                _exclusive_json(str(prefix) + '-response.json', {'returncode': response.returncode,
                    'stdout_sha256': captured['stdout_sha256'], 'stderr_sha256': captured['stderr_sha256'],
                    'finished_unix': time.time(), 'captured_before_strict_parse': True})
                return response
            try:
                result = original(requested_ids, rosters, runner=auditing_runner, owner=owner, account=account)
            except Exception as error:
                reason, retry = None, False
                if type(error) is ValueError and str(error) == CONFLICT and 'stdout' in captured:
                    try:
                        reason = classify_live_conflict(captured['stdout'], rosters,
                            parser=parser, owner=owner, account=account)
                        retry = attempt < MAX_READS
                    except Exception as diagnostic_error:
                        reason = {'classification': 'not_retryable', 'type': type(diagnostic_error).__name__,
                                  'reason': str(diagnostic_error)}
                verdict = {'attempt': attempt, 'original_exception_type': type(error).__name__,
                    'original_exception': str(error), 'classification': reason, 'retry_read_only': retry}
                _exclusive_json(str(prefix) + '-verdict.json', verdict)
                if retry:
                    time.sleep(BACKOFF[attempt - 1])
                    continue
                _exclusive_json(call_dir / 'result.json', {'status': 'strict_read_failed',
                    'complete_reads_attempted': attempt, 'returned_evidence': False,
                    'original_exception_type': type(error).__name__, 'original_exception': str(error),
                    'last_classification': reason, 'finished_unix': time.time()})
                raise
            _exclusive_json(call_dir / 'result.json', {'status': 'strict_fresh_query_passed',
                'complete_reads_attempted': attempt, 'returned_records': len(result),
                'stdout_sha256': captured.get('stdout_sha256'), 'records_selected_or_merged': False,
                'finished_unix': time.time()})
            return result
        raise AssertionError('Bounded native read loop exhausted without a result')

    guarded.__name__ = original.__name__
    guarded.__doc__ = original.__doc__
    setattr(guarded, MARKER, identity)
    repair._repair_native_states = guarded
    return dict(identity, status='installed')
