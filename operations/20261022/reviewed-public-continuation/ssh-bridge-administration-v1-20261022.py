"""Interactive SSH connection; credentials stay in the user's console.

The request ledger prevents replay after interruption. This bridge only sends
short deployment/inspection commands, never runs research on the login node.
"""
from __future__ import annotations
import argparse
import base64
import json
import os
import re
import subprocess
import threading
import time
from pathlib import Path


class TransportError(RuntimeError):
    """A sent request may have reached SSH; it must never be replayed."""


def _positive_seconds(value):
    try:
        seconds = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError('Request timeout must be a positive integer number of seconds') from error
    if seconds < 1:
        raise argparse.ArgumentTypeError('Request timeout must be a positive integer number of seconds')
    return seconds


def _remote_shell(no_login_shell):
    return '/bin/bash --noprofile --norc' if no_login_shell else '/bin/bash -l'


def _save_json(path, record):
    path = Path(path)
    temporary = path.with_name(path.name + f'.tmp-{os.getpid()}-{threading.get_ident()}')
    with temporary.open('w', encoding='utf-8') as handle:
        json.dump(record, handle)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def _echo(line):
    """Console availability must not control SSH drainage or receipts."""
    try:
        print(line, end='', flush=True)
    except (OSError, UnicodeError):
        pass


def _ambiguous(root, ident, reason):
    path = root / 'sent' / (ident + '.transport.json')
    record = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {'id': ident}
    record.update(state='transmission_outcome_unknown', reason=reason, updated_unix=time.time())
    _save_json(path, record)


def transmit_request(process, root, path, *, request_timeout=180, no_login_shell=False):
    """Record before writing; refuse every previously sent/completed ID."""
    if isinstance(request_timeout, bool) or not isinstance(request_timeout, int) or request_timeout < 1:
        raise ValueError('Request timeout must be a positive integer number of seconds')
    ident = path.stem
    if not re.fullmatch('[A-Za-z0-9_-]+', ident):
        raise ValueError('Invalid command identifier')
    transmitted = root / 'sent' / path.name
    transport_path = root / 'sent' / (ident + '.transport.json')
    if transmitted.exists() or transport_path.exists() or (root / 'completed' / (ident + '.json')).exists():
        raise RuntimeError('Duplicate request; inspect prior sent/completed receipts')
    path.replace(transmitted)
    encoded = base64.b64encode(transmitted.read_bytes()).decode('ascii')
    record = {'id': ident, 'state': 'transmission_outcome_unknown', 'recorded_unix': time.time(),
              'request_timeout_seconds': request_timeout, 'login_shell': not no_login_shell}
    _save_json(transport_path, record)
    # Preserve GNU timeout's exit124 (deadline) and137 (KILL escalation),
    # alongside ordinary command failures; completion never permits replay.
    command = (f"printf '\\n__PCM_BEGIN__{ident}\\n'; "
               f"printf %s {encoded} | base64 --decode | "
               f"/usr/bin/timeout --signal=TERM --kill-after=10s {request_timeout}s {_remote_shell(no_login_shell)} 2>&1; "
               "pcm_exit=$?; "
               f"printf '\\n__PCM_END__{ident}:%s\\n' \"$pcm_exit\"\n")
    try:
        process.stdin.write(command)
        process.stdin.flush()
    except OSError as error:
        reason = f'{type(error).__name__}: {error}'
        _ambiguous(root, ident, reason)
        _save_json(root / 'DISCONNECTED.json', {'ssh_exit': process.poll(), 'reason': 'stdin_write_failed',
                   'ambiguous_request': ident, 'error': reason, 'observed_unix': time.time()})
        raise TransportError(f'SSH write failed for {ident}; inspect sent receipts before any retry') from error
    record.update(state='sent_completion_unconfirmed', transmitted_unix=time.time())
    _save_json(transport_path, record)
    return ident


def read_output(process, root, disconnected):
    """Persist SSH output even when the local console pipe has closed."""
    active, stream = None, None
    reason, error = 'stdout_eof', None
    try:
        for line in process.stdout:
            begin = re.fullmatch(r'__PCM_BEGIN__([A-Za-z0-9_-]+)\s*', line)
            end = re.fullmatch(r'__PCM_END__([A-Za-z0-9_-]+):([0-9]+)\s*', line)
            if begin:
                if stream:
                    raise RuntimeError('Unexpected nested SSH command marker')
                active = begin[1]
                stream = (root / 'outputs' / (active + '.txt')).open('w', encoding='utf-8')
            elif end and active == end[1]:
                stream.close()
                stream = None
                _save_json(root / 'completed' / (active + '.json'),
                           {'id': active, 'exit_code': int(end[2]), 'completed_unix': time.time()})
                active = None
            elif stream:
                stream.write(line)
                stream.flush()
            _echo(line)
    except Exception as failure:
        reason, error = 'reader_failed', f'{type(failure).__name__}: {failure}'
    finally:
        try:
            if stream:
                stream.close()
            if active:
                _ambiguous(root, active, error or 'SSH stdout closed before a completion marker')
            # Do not overwrite a more specific stdin failure recorded by the
            # sender when its pipe and SSH stdout close at the same time.
            path = root / 'DISCONNECTED.json'
            prior = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
            if prior.get('reason') != 'stdin_write_failed':
                _save_json(path, {'ssh_exit': process.poll(), 'reason': reason, 'error': error,
                                 'ambiguous_request': active, 'observed_unix': time.time()})
        finally:
            disconnected.set()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--host', default='narval.alliancecan.ca')
    parser.add_argument('--user', default='pwa209')
    parser.add_argument('--request-timeout', type=_positive_seconds, default=180,
                        help='Per-request deadline in seconds; TERM at deadline, KILL after ten more seconds (default:180)')
    parser.add_argument('--no-login-shell', action='store_true',
                        help='Skip profile/rc startup files in initial and per-request remote bash shells')
    args = parser.parse_args()
    root = args.root.resolve()
    for name in ('requests', 'sent', 'completed', 'outputs'):
        (root / name).mkdir(parents=True, exist_ok=True)
    process = subprocess.Popen(
        ['ssh', '-T', '-o', 'StrictHostKeyChecking=yes', '-o', 'ConnectTimeout=30',
         '-o', 'ServerAliveInterval=30', '-o', 'ServerAliveCountMax=4',
         f'{args.user}@{args.host}', _remote_shell(args.no_login_shell)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None,
        text=True, encoding='utf-8', errors='replace', bufsize=1)
    disconnected = threading.Event()
    reader = threading.Thread(target=read_output, args=(process, root, disconnected), daemon=True)
    reader.start()
    _echo('Authenticate directly at the SSH and Duo prompts. No password is saved.\n')
    try:
        while process.poll() is None and not disconnected.is_set():
            candidates = sorted((root / 'requests').glob('*.sh'))
            if candidates:
                ident = transmit_request(process, root, candidates[0], request_timeout=args.request_timeout,
                                         no_login_shell=args.no_login_shell)
                while process.poll() is None and not disconnected.is_set() and not (root / 'completed' / (ident + '.json')).exists():
                    time.sleep(.3)
            time.sleep(.3)
    except TransportError as error:
        _echo(str(error) + '\n')
        return 1
    finally:
        try:
            process.stdin.close()
        except OSError:
            pass
        reader.join(timeout=5)
    return 0 if process.poll() == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())
