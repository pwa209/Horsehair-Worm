"""Finite, explicit ENA FTP transport using unchanged primary source checksums.

Run --launch only on the failed worker's narval1 transfer endpoint after native
preservation. No extraction or scientific analysis runs here. FTP is a transport
route: the frozen canonical HTTPS URL and provider checksum remain unchanged.
"""
from __future__ import annotations
import argparse
import errno
import ftplib
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import signal
import socket
import subprocess
import sys
import time
from types import FunctionType, SimpleNamespace
import requests
from urllib.parse import unquote, urlsplit

SOURCES = ['ant_rna', 'ant_reference', 'mantis_rna', 'fly_reference']
MAX_SECONDS = 120 * 3600
BANDWIDTH = 64 * 1024**2
MAX_ATTEMPTS = 6
ATTEMPT_SECONDS = 6 * 3600
BACKOFF_SECONDS = (10, 20, 40, 60, 60)


class TransferError(RuntimeError):
    pass


class TransferDeadline(BaseException):
    """The global cutoff cannot be swallowed by ordinary network retries."""


class TransferAttemptTimeout(TimeoutError):
    """A finite attempt may reconnect while the unchanged global budget remains."""


def transient_transport_error(error):
    """Only network failures retry; integrity, local I/O and FTP 5xx stop."""
    if isinstance(error, (TimeoutError, ConnectionError, EOFError, ftplib.error_temp)):
        return True
    if isinstance(error, socket.gaierror):
        return error.errno == socket.EAI_AGAIN
    return isinstance(error, OSError) and error.errno in {
        errno.ECONNRESET, errno.ETIMEDOUT, errno.EHOSTUNREACH, errno.ENETUNREACH,
        errno.ECONNABORTED, errno.EPIPE, errno.ENETDOWN, errno.ENETRESET,
    }


def _network_operation(action):
    try:
        return action()
    except Exception as error:
        if transient_transport_error(error) and not getattr(error, '_ftp_callback_failure', False):
            error._ftp_transport_failure = True
        raise


def ftp_route(item):
    url = item['url']
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise TransferError('Invalid canonical ENA source URL') from error
    path = unquote(parsed.path)
    if (item.get('kind') != 'fastq' or parsed.scheme != 'https'
        or parsed.hostname != 'ftp.sra.ebi.ac.uk' or port is not None
        or parsed.username or parsed.password or parsed.query or parsed.fragment
        or any(c in url or c in path for c in ('\r', '\n', '\x00'))
        or not re.fullmatch(r'/vol1/fastq/[A-Za-z0-9_./-]+\.fastq\.gz', path)
        or '..' in PurePosixPath(path).parts):
        raise TransferError('Only canonical credential-free ENA /vol1/fastq HTTPS identities may route through FTP')
    if (not isinstance(item.get('bytes'), int) or isinstance(item['bytes'], bool) or item['bytes'] <= 0
        or not re.fullmatch(r'md5:[a-fA-F0-9]{32}', item.get('provider_checksum', ''))):
        raise TransferError('FTP FASTQs require an exact pinned byte count and provider MD5')
    return path, 'ftp://ftp.sra.ebi.ac.uk' + path


def _snapshot(path):
    s = path.stat()
    return {'device': s.st_dev, 'inode': s.st_ino, 'bytes': s.st_size, 'mtime_ns': s.st_mtime_ns}


def _ftp_snapshot(ftp, path, expected):
    ftp.voidcmd('TYPE I')
    size = ftp.size(path)
    stamp = ftp.sendcmd('MDTM ' + path)
    if size != expected or not re.fullmatch(r'213 [0-9]{14}(?:\.[0-9]+)?', stamp):
        raise TransferError('FTP binary SIZE/MDTM does not establish the pinned representation')
    return {'ftp_size': size, 'ftp_mdtm': stamp[4:]}


def _download_ftp_attempt(item, root, helpers, *, ftp_factory=ftplib.FTP, deadline=None,
                          attempt_deadline=None, bandwidth=BANDWIDTH, sleeper=time.sleep):
    """One transfer attempt; only self-bound FTP prefixes may resume.

    Stream digests are independently checked against file bytes before a
    no-overwrite promotion. Rejected bytes and their diagnostics remain intact.
    """
    if not math.isfinite(bandwidth) or not 0 < bandwidth <= BANDWIDTH:
        raise ValueError('FTP bandwidth must be positive and at most 64MiB/s')
    core, acquire = helpers.core, helpers.acquire
    path, route = ftp_route(item)
    root = Path(root).resolve()
    lexical = root / item['relative_path']
    if lexical.is_symlink():
        raise TransferError('A source target symlink cannot be adopted')
    target = acquire.safe_destination(root, item['relative_path'])
    if target.exists():
        result = acquire.verify_file(target, item)
        return {**result, 'transfer_route': 'existing verified file; no download', 'canonical_url': item['url']}
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(target.name + '.part')
    sidecar = partial.with_name(partial.name + '.json')
    diagnostic = partial.with_name(partial.name + '.integrity.json')
    promoted_sidecar = target.with_name(target.name + '.ftp.json')
    if promoted_sidecar.exists() or promoted_sidecar.is_symlink():
        raise TransferError('Preserve an existing FTP promotion history when its final source is absent')
    signature = acquire.canonical_hash(item)
    contexts = {'md5': hashlib.md5(), 'sha256': hashlib.sha256()}
    started = time.monotonic()
    ftp = None
    receipt, written = None, 0

    def check_time():
        if deadline is not None and time.monotonic() >= deadline:
            raise TransferDeadline('Finite FTP worker deadline reached; partial retained')
        if attempt_deadline is not None and time.monotonic() >= attempt_deadline:
            raise TransferAttemptTimeout('Finite 6h FTP attempt deadline reached; prefix retained')

    def persist_prefix(handle=None):
        if handle is not None:
            handle.flush(); os.fsync(handle.fileno())
        receipt.update(prefix_bytes=written, prefix_sha256=contexts['sha256'].hexdigest(),
                       prefix_md5=contexts['md5'].hexdigest(), status='partial')
        core.atomic_json(sidecar, receipt)

    try:
        check_time()
        ftp = ftp_factory()
        _network_operation(lambda: ftp.connect('ftp.sra.ebi.ac.uk', 21, timeout=60))
        _network_operation(ftp.login); _network_operation(lambda: ftp.set_pasv(True))
        remote = _network_operation(lambda: _ftp_snapshot(ftp, path, item['bytes']))
        existing = partial.exists() or sidecar.exists() or partial.is_symlink() or sidecar.is_symlink()
        if existing:
            if partial.is_symlink() or sidecar.is_symlink() or not partial.is_file() or not sidecar.is_file():
                raise TransferError('An existing foreign or incomplete sidecar binding must be preserved')
            old = core.read_json(sidecar)
            binding = {'asset_hash': signature, 'protocol': 'ftp', 'canonical_url': item['url'],
                       'route_url': route, **remote}
            if any(old.get(key) != value for key, value in binding.items()):
                raise TransferError('FTP partial asset/protocol/SIZE/MDTM changed; preserve it')
            before = _snapshot(partial)
            if before['bytes'] > item['bytes'] or old.get('prefix_bytes') != before['bytes']:
                raise TransferError('FTP partial length differs from its recorded prefix; preserve it')
            with partial.open('rb') as prefix:
                for chunk in iter(lambda: prefix.read(8 * 1024**2), b''):
                    check_time()
                    for context in contexts.values(): context.update(chunk)
            if (_snapshot(partial) != before or contexts['sha256'].hexdigest() != old.get('prefix_sha256')
                or contexts['md5'].hexdigest() != old.get('prefix_md5')):
                raise TransferError('FTP prefix bytes changed; never append to an unverified prefix')
            written = before['bytes']
        else:
            binding = {'asset_hash': signature, 'protocol': 'ftp', 'canonical_url': item['url'],
                       'route_url': route, **remote}
        receipt = {**binding, 'resumed_from_bytes': written, 'status': 'partial'}
        mode = 'ab' if existing else 'xb'
        if written == item['bytes']:
            # A complete prefix is never fetched again or silently accepted.
            # The same independent disk/pinned-checksum gate below decides it.
            persist_prefix()
        else:
            flags = os.O_WRONLY | (os.O_APPEND if existing else os.O_CREAT | os.O_EXCL)
            flags |= getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_BINARY', 0)
            with os.fdopen(os.open(partial, flags, 0o600), mode) as output:
                opened = os.fstat(output.fileno())
                if existing and {'device': opened.st_dev, 'inode': opened.st_ino,
                    'bytes': opened.st_size, 'mtime_ns': opened.st_mtime_ns} != before:
                    raise TransferError('FTP prefix identity changed before append; preserve it')
                persist_prefix(output)
                def receive_body(chunk):
                    nonlocal written
                    check_time()
                    if written + len(chunk) > item['bytes']:
                        raise TransferError('FTP body exceeds the pinned byte count; rejected chunk not written')
                    output.write(chunk)
                    for context in contexts.values(): context.update(chunk)
                    written += len(chunk)
                    delay = (written - receipt['resumed_from_bytes']) / bandwidth - (time.monotonic() - started)
                    if delay > 0: sleeper(delay)
                def receive(chunk):
                    try: receive_body(chunk)
                    except Exception as error:
                        error._ftp_callback_failure = True
                        raise
                try:
                    _network_operation(lambda: ftp.retrbinary('RETR ' + path, receive,
                        blocksize=8 * 1024**2, rest=written if written else None))
                except BaseException:
                    # A caught transport/deadline failure remains safely resumable.
                    # An uncatchable kill leaves a stale prefix binding and refuses
                    # reuse until independently inspected; no bytes are truncated.
                    try:
                        persist_prefix(output)
                    except BaseException as checkpoint_error:
                        print('Prefix checkpoint failed; preserve original transfer failure: '
                              + repr(checkpoint_error), file=sys.stderr, flush=True)
                    raise
                else:
                    persist_prefix(output)
        if written != item['bytes']:
            raise TransferError('FTP body is shorter than the pinned byte count; prefix retained')
        check_time()
        if _network_operation(lambda: _ftp_snapshot(ftp, path, item['bytes'])) != remote:
            raise TransferError('FTP SIZE/MDTM changed during transfer; do not promote')
        stream = {key: value.hexdigest() for key, value in contexts.items()}
        disk = acquire.file_digests(partial, {'md5', 'sha256'})
        core.atomic_json(diagnostic, {'asset_id': item['asset_id'], 'asset_hash': signature,
            'canonical_url': item['url'], 'transfer_route': route, 'bytes': written,
            'stream_digests': stream, 'independent_disk_digests': disk, 'provider_checksum': item['provider_checksum'],
            'stream_equals_disk': stream == disk, 'status': 'integrity_checked_before_promotion'})
        if stream != disk:
            raise TransferError('FTP stream/disk digests differ; failed bytes and diagnostics retained')
        result = acquire.verify_file(partial, item, computed_digests=disk)
        # Hard-link creation is atomic and fails if another actor created target.
        os.link(partial, target)
        partial.unlink()
        receipt.update(status='verified_promoted', stream_digests=stream, independent_disk_digests=disk)
        core.atomic_json(sidecar, receipt)
        # The data-worker guard serializes promotion; a history destination is
        # nevertheless created without overwriting any existing evidence.
        os.link(sidecar, promoted_sidecar)
        sidecar.unlink()
        return {**result, 'path': str(target), 'canonical_url': item['url'], 'transfer_route': route,
            'protocol': 'ftp', **remote, 'stream_digests': stream, 'independent_disk_digests': disk,
            'independent_disk_verified': True, 'resumed_from_bytes': receipt['resumed_from_bytes']}
    except BaseException as error:
        if receipt is not None:
            try:
                core.atomic_json(diagnostic, {'asset_id': item['asset_id'], 'asset_hash': signature,
                    'canonical_url': item['url'], 'transfer_route': route, 'bytes_written': written,
                    'provider_checksum': item['provider_checksum'], 'error_type': type(error).__name__, 'error': str(error),
                    'partial_retained': partial.exists(), 'source_definition_unchanged': True,
                    **({'stream_digests': stream, 'independent_disk_digests': disk} if 'disk' in locals() else {})})
            except BaseException as diagnostic_error:
                print('Integrity diagnostic failed; preserve original transfer failure: '
                      + repr(diagnostic_error), file=sys.stderr, flush=True)
        raise
    finally:
        if ftp is not None:
            try: ftp.close()
            except OSError: pass


def download_ftp_asset(item, root, helpers, *, ftp_factory=ftplib.FTP, deadline=None,
                       bandwidth=BANDWIDTH, sleeper=time.sleep, attempts=MAX_ATTEMPTS,
                       attempt_seconds=ATTEMPT_SECONDS):
    """Finite reconnects; every attempt independently verifies its resume prefix.

    The journal retains each failed attempt, including its integrity diagnostic.
    It never turns checksum failures or local storage errors into retry loops.
    """
    if (not isinstance(attempts, int) or isinstance(attempts, bool) or not 1 <= attempts <= MAX_ATTEMPTS
        or not isinstance(attempt_seconds, (int, float)) or isinstance(attempt_seconds, bool)
        or not math.isfinite(attempt_seconds) or not 0 < attempt_seconds <= ATTEMPT_SECONDS):
        raise ValueError('Finite FTP retries require 1..6 attempts of at most 6 hours')
    target = helpers.acquire.safe_destination(Path(root).resolve(), item['relative_path'])
    partial = target.with_name(target.name + '.part')
    history = target.with_name(target.name + '.ftp-retries.json')
    if history.is_symlink(): raise TransferError('Preserve a foreign retry-history symlink')
    journal = (helpers.core.read_json(history) if history.exists() else
        {'asset_id': item['asset_id'], 'asset_hash': helpers.acquire.canonical_hash(item),
         'canonical_url': item['url'], 'maximum_attempts_per_invocation': MAX_ATTEMPTS,
         'attempt_seconds': ATTEMPT_SECONDS, 'attempts': []})
    if (journal.get('asset_hash') != helpers.acquire.canonical_hash(item)
        or journal.get('canonical_url') != item['url'] or not isinstance(journal.get('attempts'), list)):
        raise TransferError('Existing retry history differs from the pinned source identity')
    for attempt in range(1, attempts + 1):
        if deadline is not None and time.monotonic() >= deadline:
            raise TransferDeadline('Finite FTP worker deadline reached before reconnect')
        started = time.monotonic()
        bound = started + attempt_seconds
        if deadline is not None: bound = min(bound, deadline)
        try:
            result = _download_ftp_attempt(item, root, helpers, ftp_factory=ftp_factory,
                deadline=deadline, attempt_deadline=bound, bandwidth=bandwidth, sleeper=sleeper)
        except BaseException as error:
            transient = transient_transport_error(error) and (
                getattr(error, '_ftp_transport_failure', False) or isinstance(error, TransferAttemptTimeout))
            retry = transient and attempt < attempts
            diagnostic = partial.with_name(partial.name + '.integrity.json')
            entry = {'attempt': attempt, 'created_unix': time.time(),
                'elapsed_seconds': time.monotonic() - started, 'status': 'failed',
                'error_type': type(error).__name__, 'error': str(error),
                'transient_transport': transient, 'retry_scheduled': retry,
                'partial_retained': partial.exists()}
            if diagnostic.is_file() and not diagnostic.is_symlink():
                entry['integrity_diagnostic_sha256'] = helpers.core.digest(diagnostic)
                entry['integrity_diagnostic'] = helpers.core.read_json(diagnostic)
            journal['attempts'].append(entry)
            try: helpers.core.atomic_json(history, journal)
            except BaseException as history_error:
                print('Retry journal failed; preserve original transfer failure: ' + repr(history_error),
                      file=sys.stderr, flush=True)
                raise error
            if not retry: raise
            delay = BACKOFF_SECONDS[attempt - 1]
            if deadline is not None and deadline - time.monotonic() <= delay:
                raise TransferDeadline('Finite FTP budget cannot admit the next reconnect; partial retained') from error
            sleeper(delay)
        else:
            if journal['attempts']:
                journal['attempts'].append({'attempt': attempt, 'created_unix': time.time(),
                    'elapsed_seconds': time.monotonic() - started, 'status': 'verified',
                    'resumed_from_bytes': result.get('resumed_from_bytes'), 'sha256': result['sha256']})
                helpers.core.atomic_json(history, journal)
            return result


def _https_transient(error):
    """Original acquisition wraps network exceptions; local I/O never retries."""
    if isinstance(error, requests.RequestException):
        cause = error
    elif isinstance(error, RuntimeError) and str(error).startswith('Transfer failed for '):
        cause = error.__cause__
    else:
        return False
    if isinstance(cause, (requests.ConnectionError, requests.Timeout,
                          requests.exceptions.ChunkedEncodingError)):
        return True
    return isinstance(cause, requests.HTTPError) and cause.response is not None and cause.response.status_code in {429, 500, 502, 503, 504}


def download_https_asset(item, root, helpers, *, session, deadline, sleeper=time.sleep,
                         attempts=MAX_ATTEMPTS):
    """Explicit quarantined case only, using the original HTTPS transfer code.

    Isolated function globals strengthen verification and atomic promotion: its
    stream digests must equal an independent disk read before original provider
    verification, and an existing final file cannot be overwritten. The
    immutable primary module is never patched.
    Every network retry rechecks its checkpointed prefix and original validators.
    """
    ftp_route(item)  # Identical canonical ENA FASTQ/size/MD5 whitelist.
    if type(attempts) is not int or not 1 <= attempts <= MAX_ATTEMPTS:
        raise ValueError('Finite HTTPS recovery requires 1..6 attempts')
    if session.trust_env is not False:
        raise TransferError('Canonical HTTPS recovery requires proxy-independent public transport')
    core, acquire = helpers.core, helpers.acquire
    root = Path(root).resolve()
    if (root/item['relative_path']).is_symlink():
        raise TransferError('A source target symlink cannot be adopted by HTTPS recovery')
    target = acquire.safe_destination(root, item['relative_path'])
    if target.exists(): return acquire.verify_file(target, item)
    partial = target.with_name(target.name+'.part'); sidecar = partial.with_name(partial.name+'.json')
    checkpoint = target.with_name(target.name+'.https-prefix.json')
    history_path = target.with_name(target.name+'.https-retries.json')
    promotion = target.with_name(target.name+'.https.json')
    diagnostic = partial.with_name(partial.name+'.https-integrity.json')
    signature = acquire.canonical_hash(item)
    for path in (target, partial, sidecar, checkpoint, history_path, promotion, diagnostic):
        if path.is_symlink(): raise TransferError('A foreign HTTPS recovery symlink must be preserved')
    if promotion.exists(): raise TransferError('Preserve existing HTTPS promotion history when final source is absent')
    history = core.read_json(history_path) if history_path.exists() else {
        'asset_hash': signature, 'canonical_url': item['url'], 'protocol': 'https', 'attempts': []}
    if (history.get('asset_hash') != signature or history.get('canonical_url') != item['url']
        or history.get('protocol') != 'https' or not isinstance(history.get('attempts'), list)):
        raise TransferError('HTTPS attempt history differs from the frozen canonical source')
    last_verification = {}

    def check_deadline():
        if time.monotonic() >= deadline:
            raise TransferDeadline('Finite HTTPS worker deadline reached; partial retained')

    def prefix_binding(*, verify=False):
        if not partial.exists() and not sidecar.exists():
            if checkpoint.exists(): raise TransferError('HTTPS prefix checkpoint exists without its original bytes')
            return
        if (partial.is_symlink() or sidecar.is_symlink() or not partial.is_file() or not sidecar.is_file()):
            raise TransferError('Ordinary HTTPS prefix and original transfer sidecar are required')
        meta = core.read_json(sidecar); before = _snapshot(partial); meta_hash = core.digest(sidecar)
        if (meta.get('asset_hash') != signature or meta.get('url') != item['url']
            or before['bytes'] > item['bytes'] or not (meta.get('etag') or meta.get('last_modified'))
            or meta.get('transfer_response', {}).get('content_encoding') != 'identity'):
            raise TransferError('HTTPS prefix lacks unchanged canonical asset/representation/validator evidence')
        check_deadline(); hashes = acquire.file_digests(partial, {'md5', 'sha256'}); check_deadline()
        record = {'asset_hash': signature, 'canonical_url': item['url'], 'protocol': 'https',
                  'snapshot': before, 'sidecar_sha256': meta_hash, 'prefix_digests': hashes}
        if _snapshot(partial) != before or core.digest(sidecar) != meta_hash:
            raise TransferError('HTTPS prefix changed during independent checkpoint verification')
        if verify:
            if not checkpoint.is_file() or core.read_json(checkpoint) != record:
                raise TransferError('HTTPS prefix/validators changed before resume; do not append')
        else:
            if before['bytes'] == item['bytes']:
                acquire.verify_file(partial, item, computed_digests=hashes)
            core.atomic_json(checkpoint, record)

    def independent_verify(path, source, *, computed_digests=None):
        check_deadline()
        disk = acquire.file_digests(path, {'md5', 'sha256'}); check_deadline()
        same = computed_digests is None or computed_digests == disk
        last_verification.update(asset_id=source['asset_id'], asset_hash=signature,
            canonical_url=item['url'], protocol='https', provider_checksum=item['provider_checksum'],
            bytes=Path(path).stat().st_size, stream_digests=computed_digests,
            independent_disk_digests=disk, stream_equals_disk=same,
            original_transfer_response=core.read_json(sidecar) if sidecar.is_file() else None)
        core.atomic_json(diagnostic, last_verification)
        if not same: raise TransferError('HTTPS stream/disk digests differ; preserve failed bytes')
        return acquire.verify_file(path, source, computed_digests=disk)

    original = acquire.download_asset
    class NoOverwritePath(type(Path())):
        def replace(self, destination):
            os.link(self, destination)
            self.unlink()
            return Path(destination)
    def atomic_destination(directory, relative):
        return NoOverwritePath(acquire.safe_destination(directory, relative))
    isolated = FunctionType(original.__code__, {**original.__globals__, 'verify_file': independent_verify,
                            'safe_destination': atomic_destination},
                            original.__name__, original.__defaults__, original.__closure__)
    isolated.__kwdefaults__ = dict(original.__kwdefaults__ or {})
    for attempt in range(1, attempts+1):
        check_deadline(); prefix_binding(verify=True)
        try:
            result = isolated(item, root, session=session, attempts=1,
                bandwidth_bytes_per_second=BANDWIDTH, max_seconds=min(ATTEMPT_SECONDS, deadline-time.monotonic()))
        except BaseException as error:
            finite_attempt = str(error) == 'Finite transfer runtime reached; partial transfer retained for resumption'
            transient = _https_transient(error) or finite_attempt
            retry = transient and attempt < attempts
            if retry:
                try: prefix_binding()
                except BaseException as checkpoint_error:
                    history['attempts'].append({'attempt': attempt, 'error_type': type(error).__name__,
                        'error': str(error), 'retry_scheduled': False,
                        'checkpoint_error_type': type(checkpoint_error).__name__, 'checkpoint_error': str(checkpoint_error)})
                    core.atomic_json(history_path, history)
                    raise checkpoint_error from error
            history['attempts'].append({'attempt': attempt, 'error_type': type(error).__name__,
                'error': str(error), 'transient_transport': transient, 'retry_scheduled': retry,
                'partial_retained': partial.exists(), 'verification': dict(last_verification)})
            core.atomic_json(history_path, history)
            if not retry: raise
            check_deadline(); delay = BACKOFF_SECONDS[attempt-1]
            if deadline-time.monotonic() <= delay:
                raise TransferDeadline('Finite HTTPS budget cannot admit another attempt') from error
            sleeper(delay)
        else:
            history['attempts'].append({'attempt': attempt, 'status': 'verified', 'verification': dict(last_verification)})
            core.atomic_json(history_path, history)
            promoted = {**result, **last_verification, 'path': str(target), 'independent_disk_verified': True,
                        'transfer_route': item['url'], 'primary_acquisition_code_unchanged': True}
            with promotion.open('x') as output:
                json.dump(promoted, output, indent=2, sort_keys=True); output.flush(); os.fsync(output.fileno())
            return promoted


def _module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module; spec.loader.exec_module(module)
    return module


def bootstrap(primary):
    """Import scientific input helpers from the verified primary release only."""
    primary = Path(primary).resolve()
    maintenance = Path(__file__).resolve().parents[1]
    core = _module('_hhw_ftp_primary_core', primary / 'src/horsehair/core.py')
    acquire = _module('_hhw_ftp_primary_acquire', primary / 'src/horsehair/acquire.py')
    config = core.read_json(primary / 'configs/study.json'); core.validate_config(config)
    identity = core.read_json(primary / 'release.json')
    other = core.read_json(maintenance / 'release.json')
    if (core.source_identity(primary) != identity['source_identity']
        or core.source_identity(maintenance) != other['source_identity']
        or identity['config_sha256'] != core.canonical_hash(config)
        or other['config_sha256'] != identity['config_sha256']
        or core.read_json(maintenance / 'configs/study.json') != config
        or other['source_identity']['files'].get('scripts/transfer_rna_ftp.py') != core.digest(Path(__file__))):
        raise TransferError('Primary/maintenance release or full scientific configuration changed')
    return SimpleNamespace(core=core, acquire=acquire, config=config, primary=primary,
        maintenance=maintenance, identity=identity, maintenance_identity=other,
        root=Path(config['paths']['root']).resolve())


def ordinary(path, root):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(Path(root).resolve()):
        raise TransferError('Ordinary contained source evidence required: ' + str(path))
    return path


def native_completed(job, runner=subprocess.run):
    if not re.fullmatch(r'[0-9]+', str(job)): raise TransferError('Invalid native preservation identity')
    result = runner(['sacct', '-j', str(job), '--noheader', '--parsable2',
                     '--format=JobIDRaw%64,State%32,ExitCode'], text=True, capture_output=True, check=True, timeout=30)
    rows = [line.split('|') for line in result.stdout.splitlines() if line.split('|')[0].strip() == str(job)]
    if len(rows) != 1 or rows[0][1].strip() != 'COMPLETED' or rows[0][2].strip() != '0:0':
        raise TransferError('Native preservation must be positively COMPLETED0:0')
    return result.stdout


def native_resume_job(job, *, completed=False, runner=subprocess.run):
    """Positive native ownership, allocation and terminal evidence for a resume."""
    if not re.fullmatch(r'[0-9]+', str(job)): raise TransferError('Invalid native resume job identity')
    result = runner(['sacct', '-j', str(job), '--noheader', '--parsable2',
        '--format=JobIDRaw%64,State%32,ExitCode,User%32,Account%64,Start,End,ElapsedRaw,AllocCPUS,NodeList%128'],
        text=True, capture_output=True, check=True, timeout=30)
    rows = [line.split('|') for line in result.stdout.splitlines() if line.split('|')[0].strip() == str(job)]
    if len(rows) != 1 or len(rows[0]) != 10: raise TransferError('Unambiguous native resume job row required')
    row = [part.strip() for part in rows[0]]
    terminal = {'COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT', 'OUT_OF_MEMORY', 'NODE_FAIL', 'PREEMPTED'}
    state = row[1].split()[0]
    if (state not in terminal or completed and (state != 'COMPLETED' or row[2] != '0:0')
        or row[3] != 'pwa209' or row[4] != 'def-ptewarie_cpu'
        or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', row[5])
        or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', row[6])
        or row[6] < row[5] or not row[7].isdigit() or not row[8].isdigit()
        or int(row[8]) < 1 or not re.fullmatch(r'n[cg][0-9]+', row[9])):
        raise TransferError('Resume requires owned, allocated, positively terminal native evidence')
    return result.stdout


def _prefix_evidence(context, asset, proof):
    """Small-file and inode checks only; the scheduled proof reads the bytes."""
    target = context.acquire.safe_destination(context.root, asset['relative_path'])
    partial = ordinary(target.with_name(target.name + '.part'), context.root)
    sidecar = ordinary(partial.with_name(partial.name + '.json'), context.root)
    if target.exists() or target.is_symlink(): raise TransferError('Resume cannot replace a verified/final source')
    old = context.core.read_json(sidecar)
    route = ftp_route(asset)[1]
    binding = {'asset_hash': context.acquire.canonical_hash(asset), 'protocol': 'ftp',
        'canonical_url': asset['url'], 'route_url': route, 'ftp_size': asset['bytes']}
    if (any(old.get(k) != v for k, v in binding.items())
        or not re.fullmatch(r'[0-9]{14}(?:\.[0-9]+)?', str(old.get('ftp_mdtm', '')))
        or old.get('status') != 'partial' or not isinstance(old.get('prefix_bytes'), int)
        or isinstance(old['prefix_bytes'], bool) or not 0 <= old['prefix_bytes'] < asset['bytes']
        or old['prefix_bytes'] != partial.stat().st_size
        or not re.fullmatch('[0-9a-f]{32}', old.get('prefix_md5', ''))
        or not re.fullmatch('[0-9a-f]{64}', old.get('prefix_sha256', ''))
        or proof.get('ftp_representation') != {'ftp_size': old['ftp_size'], 'ftp_mdtm': old['ftp_mdtm']}):
        raise TransferError('Interrupted FTP prefix or fresh provider SIZE/MDTM binding is ambiguous')
    return target, partial, sidecar, old


def _previous_transfer_release(context, proof, failed):
    previous = Path(proof.get('previous_maintenance_release', '')).resolve()
    if previous.parent != context.primary.resolve().parent or previous.is_symlink():
        raise TransferError('Previous transfer release is outside the retained immutable release namespace')
    identity = context.core.read_json(previous/'release.json')
    if (context.core.source_identity(previous) != identity.get('source_identity')
        or identity['source_identity']['sha256'] != failed.get('maintenance_source_sha256')
        or identity['source_identity']['files'].get('scripts/transfer_rna_ftp.py') != failed.get('transfer_script_sha256')
        or identity.get('config_sha256') != context.identity['config_sha256']
        or context.core.read_json(previous/'configs/study.json') != context.config):
        raise TransferError('Previous transfer source/script/full configuration differs from the failed worker')
    return str(previous)


def _quarantined_previous_transfer_release(context, proof, failed):
    """Bind an older immutable proof without rewriting its preserved bytes."""
    if proof.get('previous_maintenance_release'):
        return _previous_transfer_release(context, proof, failed)
    sha = failed.get('maintenance_source_sha256', '')
    if not re.fullmatch('[0-9a-f]{64}', sha):
        raise TransferError('Failed worker has no exact retained maintenance source identity')
    candidates = [path for path in context.primary.resolve().parent.glob('*-'+sha[:12])
                  if path.is_dir() and not path.is_symlink()]
    if len(candidates) != 1:
        raise TransferError('Exactly one retained failed-worker release is required; original proof remains unchanged')
    return _previous_transfer_release(context, {**proof, 'previous_maintenance_release': str(candidates[0])}, failed)


def prepare_resume_case(context, case_dir, worker_proof_path, terminal_wait_job, *, runner=subprocess.run):
    """Scheduled byte verification for a later network interruption; no quarantine.

    The prior worker/provider proof must have been captured on actual narval1.
    This compute job hashes the interrupted prefix and binds an exclusive ready
    receipt to its native job identity; launch later requires COMPLETED0:0.
    """
    core, acquire, root = context.core, context.acquire, context.root
    job = os.environ.get('SLURM_JOB_ID', '')
    if (not re.fullmatch('[0-9]+', job) or not re.fullmatch(r'n[cg][0-9]+', socket.gethostname().split('.')[0])):
        raise TransferError('Prefix byte hashing requires a native scheduled compute allocation')
    case_dir = Path(case_dir).resolve()
    if not case_dir.is_relative_to(root / 'maintenance'): raise TransferError('Case escaped personal maintenance namespace')
    case_dir.mkdir(parents=True, exist_ok=True)
    if (case_dir/'ready.json').exists() or (case_dir/'prefix-proof.json').exists():
        raise TransferError('Resume preparation already has an outcome; never overwrite/replay')
    proof_path = ordinary(worker_proof_path, root); proof = core.read_json(proof_path)
    failed_path = ordinary(root/'state/transfer-rna.json', root); failed = core.read_json(failed_path)
    manifest = core.read_json(ordinary(root/'manifests/source_manifest.json', root)); acquire.validate_manifest(manifest)
    asset = next((a for a in manifest['assets'] if a['asset_id'] == proof.get('asset', {}).get('asset_id')), None)
    if (asset is None or proof.get('asset') != asset or proof.get('asset_sha256') != acquire.canonical_hash(asset)
        or proof.get('manifest_hash') != manifest['manifest_hash']
        or proof.get('host') != 'narval1' or proof.get('worker_absent') is not True
        or proof.get('transfer_guard_absent') is not True or proof.get('worker_pid') != failed.get('pid')
        or proof.get('failed_transfer') != failed or proof.get('failed_transfer_sha256') != core.digest(failed_path)
        or failed.get('status') != 'failed' or failed.get('host') != 'narval1'
        or failed.get('source_identity') != context.identity['source_identity']['sha256']
        or failed.get('config_sha256') != context.identity['config_sha256']
        or not re.fullmatch('[0-9a-f]{64}', failed.get('maintenance_source_sha256', ''))
        or not re.fullmatch('[0-9a-f]{64}', failed.get('transfer_script_sha256', ''))
        or failed.get('error_type') not in {'TimeoutError', 'ConnectionResetError', 'ConnectionAbortedError',
                                          'BrokenPipeError', 'EOFError', 'error_temp', 'TransferAttemptTimeout'}
        or proof.get('provider_metadata', {}).get('matches_pinned_asset') is not True
        or proof.get('provider_metadata', {}).get('manifest_modified') is not False
        or (root/'state/.transfer-rna.lock').exists() or (root/'state/.transfer-rna.lock').is_symlink()):
        raise TransferError('Interrupted worker/source/provider identity lacks an unchanged network failure proof')
    terminal = native_resume_job(terminal_wait_job, runner=runner)
    previous = _previous_transfer_release(context, proof, failed)
    _, partial, sidecar, old = _prefix_evidence(context, asset, proof)
    before = _snapshot(partial); sidecar_sha = core.digest(sidecar)
    digests = acquire.file_digests(partial, {'md5', 'sha256'})
    if (_snapshot(partial) != before or core.digest(sidecar) != sidecar_sha
        or digests != {'md5': old['prefix_md5'], 'sha256': old['prefix_sha256']}
        or core.digest(failed_path) != proof['failed_transfer_sha256']):
        raise TransferError('Interrupted prefix/sidecar/failure changed during independent byte verification')
    record = {'status': 'verified_ftp_prefix', 'slurm_job_id': job, 'host': socket.gethostname(),
        'created_unix': time.time(), 'source_sha256': context.identity['source_identity']['sha256'],
        'maintenance_source_sha256': context.maintenance_identity['source_identity']['sha256'],
        'script_sha256': core.digest(Path(__file__)), 'config_sha256': context.identity['config_sha256'],
        'manifest_hash': manifest['manifest_hash'], 'asset_id': asset['asset_id'],
        'asset_hash': acquire.canonical_hash(asset), 'failed_transfer_sha256': proof['failed_transfer_sha256'],
        'previous_maintenance_source_sha256': failed['maintenance_source_sha256'],
        'previous_transfer_script_sha256': failed['transfer_script_sha256'],
        'previous_maintenance_release': previous,
        'partial_path': str(partial), 'partial_snapshot': before, 'sidecar_path': str(sidecar),
        'sidecar_sha256': sidecar_sha, 'prefix_digests': digests, 'ftp_representation': proof['ftp_representation'],
        'provider_worker_proof_path': str(proof_path), 'provider_proof_sha256': core.digest(proof_path),
        'terminal_wait_job_id': str(terminal_wait_job), 'terminal_wait_native_proof': terminal,
        'source_definition_unchanged': True, 'quarantine_performed': False}
    with (case_dir/'prefix-proof.json').open('x') as output:
        json.dump(record, output, indent=2, sort_keys=True); output.flush(); os.fsync(output.fileno())
    ready = {**record, 'status': 'ready_for_transfer', 'transfer_launched': False,
        'recovery_mode': 'verified_ftp_prefix_resume', 'old_worker_host': 'narval1',
        'old_worker_pid': failed['pid'], 'prefix_proof_job_id': job,
        'prefix_proof_path': str(case_dir/'prefix-proof.json'), 'prefix_proof_sha256': core.digest(case_dir/'prefix-proof.json')}
    with (case_dir/'ready.json').open('x') as output:
        json.dump(ready, output, indent=2, sort_keys=True); output.flush(); os.fsync(output.fileno())
    return ready


def preflight(context, case_dir, *, runner=subprocess.run):
    core, acquire, root = context.core, context.acquire, context.root
    if socket.gethostname().split('.')[0] != 'narval1': raise TransferError('One worker belongs on actual narval1')
    case_dir = Path(case_dir).resolve()
    if not case_dir.is_relative_to(root / 'maintenance'): raise TransferError('Case escaped personal maintenance namespace')
    ready_path = ordinary(case_dir / 'ready.json', root); ready = core.read_json(ready_path)
    if (ready.get('status') != 'ready_for_transfer' or ready.get('transfer_launched') is not False
        or ready.get('source_definition_unchanged') is not True
        or ready.get('source_sha256') != context.identity['source_identity']['sha256']
        or ready.get('config_sha256') != context.identity['config_sha256'] or ready.get('old_worker_host') != 'narval1'):
        raise TransferError('Preservation is not bound to unchanged primary source/configuration')
    resume = ready.get('recovery_mode') == 'verified_ftp_prefix_resume'
    case_transport = ready.get('case_transport', 'ftp')
    if case_transport not in {'ftp', 'canonical_https'} or resume and case_transport != 'ftp':
        raise TransferError('Explicit canonical HTTPS is limited to a quarantined case asset')
    native = (native_resume_job(ready['prefix_proof_job_id'], completed=True, runner=runner)
              if resume else native_completed(ready['slurm_job_id'], runner))
    old_pid = ready['old_worker_pid']
    if not isinstance(old_pid, int) or old_pid <= 1: raise TransferError('Invalid old worker PID')
    try: os.kill(old_pid, 0)
    except ProcessLookupError: pass
    else: raise TransferError('Old worker PID exists/reused; do not kill it or replay recovery')
    guard = root / 'state/.transfer-rna.lock'
    if guard.exists() or guard.is_symlink(): raise TransferError('Preserve an existing transfer guard')
    failed_path = ordinary(root / 'state/transfer-rna.json', root)
    if core.digest(failed_path) != ready['failed_transfer_sha256']: raise TransferError('Failed receipt changed after preservation')
    failed = core.read_json(failed_path)
    proof_path = ordinary(ready['provider_worker_proof_path'], root); proof = core.read_json(proof_path)
    if (core.digest(proof_path) != ready['provider_proof_sha256'] or proof.get('host') != 'narval1'
        or proof.get('worker_pid') != old_pid or proof.get('worker_absent') is not True
        or proof.get('transfer_guard_absent') is not True or proof.get('failed_transfer') != failed
        or proof.get('failed_transfer_sha256') != ready['failed_transfer_sha256']
        or proof.get('provider_metadata', {}).get('matches_pinned_asset') is not True
        or proof.get('provider_metadata', {}).get('manifest_modified') is not False):
        raise TransferError('Actual worker/provider proof changed or is ambiguous')
    manifest_path = ordinary(root / 'manifests/source_manifest.json', root)
    manifest = core.read_json(manifest_path); acquire.validate_manifest(manifest)
    if manifest['manifest_hash'] != ready['manifest_hash']: raise TransferError('Frozen manifest changed')
    selected = [a for a in manifest['assets'] if a['source_id'] in SOURCES and (a.get('kind') != 'fastq' or a.get('eligible'))]
    if (not selected or len(selected) > min(256, context.config['resources']['scratch_budget_files'])
        or any(a.get('bytes') is None for a in selected)
        or sum(a['bytes'] for a in selected) > context.config['resources']['scratch_budget_bytes']):
        raise TransferError('Finite fixed RNA roster exceeds its resource ceilings')
    first = [a for a in selected if a['asset_id'] == ready['asset_id']]
    if len(first) != 1 or first[0] != proof['asset']: raise TransferError('Preserved asset does not match frozen source')
    asset = first[0]; ftp_route(asset)
    if (proof.get('asset_sha256') != acquire.canonical_hash(asset)
        or proof.get('manifest_hash') != manifest['manifest_hash']):
        raise TransferError('Provider proof is not bound to the frozen asset/manifest')
    previous_case_release = None
    if case_transport == 'canonical_https':
        previous_case_release = _quarantined_previous_transfer_release(context, proof, failed)
        native_resume_job(ready['slurm_job_id'], completed=True, runner=runner)
        probe = proof.get('provider_metadata', {}).get('canonical_https_probe', {})
        span = min(65536, asset['bytes'])
        if (probe.get('url') != asset['url'] or probe.get('status_code') != 206
            or probe.get('content_range') != f"bytes 0-{span-1}/{asset['bytes']}"
            or type(probe.get('received_bytes')) is not int or probe['received_bytes'] != span
            or probe.get('content_encoding') != 'identity'
            or not re.fullmatch('[0-9a-f]{64}', probe.get('sha256', ''))):
            raise TransferError('Canonical HTTPS case lacks a pinned direct identity-encoded range probe')
    if resume:
        previous = _previous_transfer_release(context, proof, failed)
        prefix_path = ordinary(ready['prefix_proof_path'], root); prefix = core.read_json(prefix_path)
        _, partial, sidecar, old = _prefix_evidence(context, asset, proof)
        expected = {'status': 'verified_ftp_prefix', 'slurm_job_id': ready['prefix_proof_job_id'],
            'source_sha256': context.identity['source_identity']['sha256'],
            'maintenance_source_sha256': context.maintenance_identity['source_identity']['sha256'],
            'script_sha256': core.digest(Path(__file__)), 'config_sha256': context.identity['config_sha256'],
            'manifest_hash': manifest['manifest_hash'], 'asset_id': asset['asset_id'],
            'asset_hash': acquire.canonical_hash(asset), 'failed_transfer_sha256': ready['failed_transfer_sha256'],
            'previous_maintenance_source_sha256': failed.get('maintenance_source_sha256'),
            'previous_transfer_script_sha256': failed.get('transfer_script_sha256'),
            'previous_maintenance_release': previous,
            'partial_path': str(partial), 'partial_snapshot': _snapshot(partial), 'sidecar_path': str(sidecar),
            'sidecar_sha256': core.digest(sidecar), 'prefix_digests': {'md5': old['prefix_md5'], 'sha256': old['prefix_sha256']},
            'ftp_representation': proof['ftp_representation'], 'provider_worker_proof_path': str(proof_path),
            'provider_proof_sha256': ready['provider_proof_sha256'],
            'terminal_wait_job_id': ready['terminal_wait_job_id'],
            'source_definition_unchanged': True, 'quarantine_performed': False}
        if (core.digest(prefix_path) != ready['prefix_proof_sha256']
            or any(prefix.get(k) != v for k, v in expected.items())
            or not re.fullmatch(r'n[cg][0-9]+', prefix.get('host', '').split('.')[0])
            or any(ready.get(k) != v for k, v in expected.items() if k not in {'status', 'slurm_job_id'})):
            raise TransferError('Native resume prefix/source/code/sidecar proof changed')
        native_resume_job(ready['terminal_wait_job_id'], runner=runner)
        return SimpleNamespace(case_dir=case_dir, ready=ready, ready_path=ready_path,
            proof_path=proof_path, failed_path=failed_path, manifest=manifest, selected=selected, asset=asset, native=native)
    target = acquire.safe_destination(root, asset['relative_path'])
    if any(p.exists() or p.is_symlink() for p in [target, target.with_name(target.name+'.part'), target.with_name(target.name+'.part.json')]):
        raise TransferError('Case asset requires a fresh original destination after preservation')
    q = ready['quarantine']
    if (q.get('status') != 'quarantined_invalid_partial' or q.get('asset_hash') != acquire.canonical_hash(asset)
        or q.get('asset_id') != asset['asset_id'] or q.get('source_version') != asset['version']
        or q.get('provider_checksum') != asset['provider_checksum'] or q.get('bytes') != asset['bytes']
        or q.get('source_definition_unchanged') is not True
        or not re.fullmatch('[0-9a-f]{32}', q.get('observed_digests', {}).get('md5', ''))
        or not re.fullmatch('[0-9a-f]{64}', q.get('observed_digests', {}).get('sha256', ''))
        or q['observed_digests']['md5'] == asset['provider_checksum'].split(':', 1)[1].lower()):
        raise TransferError('Quarantine identity differs or does not prove a bad pinned MD5')
    preserved = ordinary(q['preserved_partial'], root); sidecar = ordinary(q['preserved_sidecar'], root)
    journal = ordinary(preserved.parent/'recovery.json', root)
    if (Path(q['original_partial']).resolve() != target.with_name(target.name+'.part')
        or Path(q['original_sidecar']).resolve() != target.with_name(target.name+'.part.json')
        or preserved.parent.resolve().parent != (target.parent/'.quarantine').resolve()
        or not re.fullmatch('[a-z0-9][a-z0-9-]{0,63}', preserved.parent.name)
        or preserved.name != target.name+'.part' or sidecar != preserved.with_name(preserved.name+'.json')
        or _snapshot(preserved) != proof.get('partial_snapshot')
        or preserved.stat().st_size != asset['bytes'] or core.digest(sidecar) != q['sidecar_sha256']
        or core.digest(sidecar) != proof.get('sidecar_sha256')
        or core.read_json(sidecar).get('asset_hash') != acquire.canonical_hash(asset)
        or core.read_json(journal) != q or preserved.stat().st_mtime_ns > journal.stat().st_mtime_ns):
        raise TransferError('Preserved failed bytes/sidecar/journal changed')
    return SimpleNamespace(case_dir=case_dir, ready=ready, ready_path=ready_path,
        proof_path=proof_path, failed_path=failed_path, manifest=manifest, selected=selected, asset=asset, native=native,
        previous_maintenance_release=previous_case_release)


def _owned_guard(path, owner, action):
    with path.open('xb') as output:
        output.write(owner); output.flush(); os.fsync(output.fileno())
    try: return action()
    finally:
        if path.is_file() and not path.is_symlink() and path.read_bytes() == owner: path.unlink()
        else: print('Transfer guard owner changed; preserve it', file=sys.stderr, flush=True)


def case_native_identity(checked):
    if checked.ready.get('recovery_mode') == 'verified_ftp_prefix_resume':
        return {'recovery_mode': 'verified_ftp_prefix_resume',
            'prefix_proof_job_id': checked.ready['prefix_proof_job_id'],
            'prefix_native_proof': checked.native,
            'previous_maintenance_source_sha256': checked.ready['previous_maintenance_source_sha256'],
            'previous_transfer_script_sha256': checked.ready['previous_transfer_script_sha256']}
    return {'recovery_mode': 'quarantined_invalid_partial',
        'case_asset_id': checked.asset['asset_id'], 'case_transport': checked.ready.get('case_transport', 'ftp'),
        'previous_maintenance_release': getattr(checked, 'previous_maintenance_release', None),
        'preservation_job_id': checked.ready['slurm_job_id'], 'preservation_native_proof': checked.native}


def launch(context, case_dir, restart_id, *, runner=subprocess.run, popen=subprocess.Popen):
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}', restart_id): raise TransferError('Invalid unique restart identity')
    checked = preflight(context, case_dir, runner=runner)
    core, root = context.core, context.root
    restart = checked.case_dir / ('restart-' + restart_id)
    if restart.exists(): raise TransferError('Restart outcome already exists; never replay ambiguity')
    ps = runner(['ps', '-u', str(root.parent.name), '-o', 'pid=,args='], text=True, capture_output=True, check=True, timeout=20)
    conflicts = [line for line in ps.stdout.splitlines() if ('scripts/transfer_sources.py' in line and '--group rna' in line)
        or 'scripts/start_transfers.py' in line or 'rna301-transfer-worker' in line and '--worker' in line
        or 'transfer_rna_ftp.py' in line and '--worker' in line]
    if conflicts: raise TransferError('Another RNA data worker or launcher exists')
    restart.mkdir(exist_ok=False)
    for name, source in [('old-failed-transfer.json', checked.failed_path), ('provider-worker-proof.json', checked.proof_path),
                         ('native-ready.json', checked.ready_path)]:
        with (restart/name).open('xb') as output:
            output.write(source.read_bytes()); output.flush(); os.fsync(output.fileno())
    pilot = root/'state/transfer-rna-pilot.json'
    if pilot.exists():
        with (restart/'old-pilot-transfer.json').open('xb') as output:
            output.write(pilot.read_bytes()); output.flush(); os.fsync(output.fileno())
    journal = restart/'launch.json'
    record = {'status':'starting','restart_id':restart_id,'primary_release':str(context.primary),
        'primary_source_sha256':context.identity['source_identity']['sha256'],
        'maintenance_release':str(context.maintenance),'maintenance_source_sha256':context.maintenance_identity['source_identity']['sha256'],
        'config_sha256':context.identity['config_sha256'],'script_sha256':core.digest(Path(__file__)),
        'ready_sha256':core.digest(checked.ready_path),**case_native_identity(checked),
        'created_unix':time.time(),'maximum_seconds':MAX_SECONDS,
        'maximum_data_workers':1,'bandwidth_bytes_per_second':BANDWIDTH,'automatic_quarantine_retries':0,
        'maximum_transport_attempts_per_asset':MAX_ATTEMPTS, 'maximum_attempt_seconds':ATTEMPT_SECONDS,
        'transport_backoff_seconds':list(BACKOFF_SECONDS)}
    with journal.open('x') as output:
        json.dump(record, output, indent=2, sort_keys=True); output.flush(); os.fsync(output.fileno())
    log = root/'logs'/('transfer-rna-ftp-'+restart_id+'.log')
    env = os.environ.copy()
    for key in list(env):
        if key.lower() in {'http_proxy','https_proxy','all_proxy'}: env.pop(key)
    env['PYTHONUNBUFFERED'] = '1'
    command = ['nohup',sys.executable,str(Path(__file__).resolve()),'--primary-release',str(context.primary),
               '--case-dir',str(checked.case_dir),'--restart-id',restart_id,'--worker']
    try:
        with log.open('x') as output:
            child = popen(command,cwd=context.primary,env=env,stdin=subprocess.DEVNULL,
                          stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
        record.update(status='started',pid=child.pid,command=command,log=str(log),started_unix=time.time())
        core.atomic_json(journal, record)
    except BaseException as error:
        record.update(status='launch_ambiguous' if 'child' in locals() else 'launch_failed',error_type=type(error).__name__,error=str(error))
        if 'child' in locals(): record['pid']=child.pid
        core.atomic_json(journal,record); raise
    return record


def worker(context, case_dir, restart_id):
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}', restart_id): raise TransferError('Invalid unique restart identity')
    checked = preflight(context, case_dir)
    core, acquire, root = context.core, context.acquire, context.root
    restart = checked.case_dir / ('restart-' + restart_id)
    journal = core.read_json(ordinary(restart/'launch.json',root))
    if (journal.get('status') not in {'starting','started'} or journal.get('script_sha256') != core.digest(Path(__file__))
        or journal.get('ready_sha256') != core.digest(checked.ready_path)
        or journal.get('primary_source_sha256') != context.identity['source_identity']['sha256']
        or journal.get('maintenance_source_sha256') != context.maintenance_identity['source_identity']['sha256']):
        raise TransferError('Worker launch source/receipt identity changed')
    owner = json.dumps({'pid':os.getpid(),'host':'narval1','restart_id':restart_id,'created_unix':time.time()},sort_keys=True).encode()
    guard = root/'state/.transfer-rna.lock'
    def transfer():
        started=time.monotonic(); deadline=started+MAX_SECONDS
        record={'status':'running','group':'rna','sources':SOURCES,'pid':os.getpid(),'host':'narval1','started_unix':time.time(),
            'config_sha256':context.identity['config_sha256'],'source_identity':context.identity['source_identity']['sha256'],
            'maintenance_source_sha256':context.maintenance_identity['source_identity']['sha256'],
            'transfer_script_sha256':core.digest(Path(__file__)),**case_native_identity(checked),
            'restart_id':restart_id,'maximum_seconds':MAX_SECONDS,'bandwidth_bytes_per_second':BANDWIDTH,
            'automatic_quarantine_retries':0,'maximum_transport_attempts_per_asset':MAX_ATTEMPTS,
            'maximum_attempt_seconds':ATTEMPT_SECONDS,'transport_backoff_seconds':list(BACKOFF_SECONDS),
            'execution_class':'finite public-data file transfer; canonical pins unchanged',
            'verified_files':0}
        state=root/'state/transfer-rna.json'
        report={'manifest_hash':checked.manifest['manifest_hash'],'selection_hash':core.canonical_hash([a['asset_id'] for a in checked.selected]),
            'sources':SOURCES,'files':[],'receipt_path':str(restart/'download-receipts.json')}
        def expire(*_): raise TransferDeadline('Finite 120h data worker deadline reached; preserve partial')
        old=signal.signal(signal.SIGALRM,expire);signal.setitimer(signal.ITIMER_REAL,MAX_SECONDS)
        session=acquire.http_session();session.trust_env=False
        session.headers.update({'Cache-Control':'no-cache','Pragma':'no-cache'})
        try:
            if core.digest(state)!=checked.ready['failed_transfer_sha256']:raise TransferError('Failure changed before owned guard claim')
            core.atomic_json(state,record)
            ordered=[checked.asset,*[a for a in checked.selected if a['asset_id']!=checked.asset['asset_id']]]
            pilots=acquire.rna_pilot_asset_ids(checked.manifest);pilot_written=False
            if not pilots or not set(pilots)<={a['asset_id'] for a in checked.selected}:
                raise TransferError('Fixed pilot IDs are not all in the finite frozen RNA roster')
            for item in ordered:
                if time.monotonic()>=deadline:raise TransferDeadline('Finite data deadline reached before next file')
                if item.get('kind')=='fastq':
                    if item['asset_id']==checked.asset['asset_id'] and checked.ready.get('case_transport')=='canonical_https':
                        result=download_https_asset(item,root,context,session=session,deadline=deadline)
                    else:
                        result=download_ftp_asset(item,root,context,deadline=deadline)
                else:
                    result=acquire.download_asset(item,root,session=session,attempts=1,bandwidth_bytes_per_second=BANDWIDTH,
                        max_seconds=deadline-time.monotonic())
                report['files'].append(result);core.atomic_json(restart/'download-receipts.json',report)
                record.update(verified_files=len(report['files']),elapsed_seconds=time.monotonic()-started)
                core.atomic_json(state,record)
                if not pilot_written and set(pilots)<={row['asset_id'] for row in report['files']}:
                    core.atomic_json(root/'state/transfer-rna-pilot.json',{**record,'group':'rna-pilot','status':'complete',
                        'asset_ids':pilots,'verified_files':len(pilots),'source_manifest_sha256':core.canonical_hash(checked.manifest)})
                    pilot_written=True
            if time.monotonic()>=deadline:raise TransferDeadline('Finite deadline elapsed before full verification')
            record.update(status='complete',elapsed_seconds=time.monotonic()-started,
                source_manifest_sha256=core.canonical_hash(checked.manifest),download_receipt_path=report['receipt_path'])
            core.atomic_json(state,record)
        except BaseException as error:
            record.update(status='failed',error_type=type(error).__name__,error=str(error),elapsed_seconds=time.monotonic()-started,
                partial_preserved=True,repair_repeated=False)
            core.atomic_json(state,record);raise
        finally:
            signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,old);session.close()
        return record
    return _owned_guard(guard,owner,transfer)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--primary-release',required=True,type=Path)
    p.add_argument('--case-dir',required=True,type=Path)
    p.add_argument('--restart-id',required=True)
    p.add_argument('--worker-proof',type=Path)
    p.add_argument('--terminal-wait-job')
    modes=p.add_mutually_exclusive_group(required=True);modes.add_argument('--launch',action='store_true');modes.add_argument('--worker',action='store_true')
    modes.add_argument('--prepare-resume',action='store_true')
    args=p.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}',args.restart_id):raise TransferError('Invalid restart identity')
    context=bootstrap(args.primary_release)
    if args.prepare_resume:
        if args.worker_proof is None or args.terminal_wait_job is None:
            p.error('--prepare-resume requires --worker-proof and --terminal-wait-job')
        result=prepare_resume_case(context,args.case_dir,args.worker_proof,args.terminal_wait_job)
    else:
        if args.worker_proof is not None or args.terminal_wait_job is not None:
            p.error('Preparation evidence arguments are only valid with --prepare-resume')
        result=launch(context,args.case_dir,args.restart_id) if args.launch else worker(context,args.case_dir,args.restart_id)
    print(json.dumps(result,sort_keys=True),flush=True)


if __name__=='__main__':main()
