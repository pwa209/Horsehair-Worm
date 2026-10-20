"""Explicit v3, host-bound system-curl transport for frozen ENA FASTQs.

Every range is written to a new private file and validated before it contributes
to a candidate. Network retries can copy a verified committed prefix; checksum
failures start a fresh candidate. Failed attempts remain immutable. A full
independent disk read must match stream digests and the frozen provider MD5
before exclusive final promotion. No Requests data transfer occurs here.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import stat
import subprocess
import time
from urllib.parse import urlsplit

POLICY_NAME = 'all-selected-fastq-host-bound-system-curl-https-v3'
CURL_PATH = Path('/usr/bin/curl')
SEGMENT_BYTES = 64 * 1024**2
MAX_ATTEMPTS = 6
ASSET_SECONDS = 6 * 3600
WORKER_SECONDS = 120 * 3600
MAX_BANDWIDTH = 64 * 1024**2
SEGMENT_SECONDS = 300
BACKOFF = (10, 20, 40, 60, 60)
EMBEDDED_ERROR_SIGNATURE = (b'<Error><Code>ConnectionClosedException</Code><Message>'
                            b'Premature end of Content-Length delimited message body (expected:')


class CurlTransferError(RuntimeError):
    pass


class CurlNetworkError(CurlTransferError):
    pass


class CurlIntegrityError(CurlTransferError):
    pass


def require(ok, message):
    if not ok:
        raise CurlTransferError(message)


def _snapshot(path):
    info = path if isinstance(path, os.stat_result) else Path(path).stat()
    return {'device': info.st_dev, 'inode': info.st_ino, 'bytes': info.st_size,
            'mtime_ns': info.st_mtime_ns}


def _ordinary(path, root, *, directory=False):
    path, root = Path(path), Path(root).resolve()
    require(path.is_absolute() and path.is_relative_to(root), 'Curl staging escaped personal source root')
    cursor = root
    for part in path.relative_to(root).parts:
        cursor /= part
        require(not stat.S_ISLNK(cursor.lstat().st_mode), 'Curl source/staging symlink refused')
    info = path.lstat()
    require((stat.S_ISDIR if directory else stat.S_ISREG)(info.st_mode)
            and path.resolve().is_relative_to(root), 'Ordinary curl source/staging path required')
    return path


def _exclusive_json(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    with os.fdopen(fd, 'w', encoding='utf8') as output:
        json.dump(value, output, indent=2, sort_keys=True, allow_nan=False)
        output.write('\n'); output.flush(); os.fsync(output.fileno())


def _new_file(path):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    info = os.fstat(fd); os.close(fd)
    return _snapshot(info)


def _check_time(deadline):
    if time.monotonic() >= deadline:
        raise CurlTransferError('Finite curl asset/worker deadline reached; owned staging retained')


def _validate_item(item, helpers):
    parsed = urlsplit(item.get('url', ''))
    require(item.get('kind') == 'fastq' and item.get('eligible') is True
            and bool(item.get('crosswalk_hash')) and parsed.scheme == 'https'
            and parsed.hostname == 'ftp.sra.ebi.ac.uk' and parsed.port is None
            and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment
            and parsed.path.startswith('/vol1/fastq/') and parsed.path.endswith('.fastq.gz')
            and type(item.get('bytes')) is int and item['bytes'] > 0
            and re.fullmatch(r'md5:[0-9a-f]{32}', str(item.get('provider_checksum', '')))
            and not any(c in item['url'] for c in ('\r', '\n', '\x00')),
            'V3 curl requires an eligible frozen canonical ENA FASTQ with bytes and provider MD5')
    require(helpers.acquire.canonical_hash(item) == helpers.core.canonical_hash(item), 'Source hash helpers disagree')


def validate_policy_runtime(policy, *, runner=subprocess.run):
    """Bind the actual host and system binary; this performs no data request."""
    require(policy.get('schema_version') == 3 and policy.get('name') == POLICY_NAME
            and policy.get('transport_backend') == 'system_curl'
            and policy.get('target_transfer_host') == 'narval3'
            and socket.gethostname().split('.')[0] == 'narval3', 'Explicit native Narval3 v3 curl policy required')
    require(policy.get('maximum_transport_attempts_per_asset') == MAX_ATTEMPTS
            and policy.get('maximum_integrity_attempts_per_asset') == MAX_ATTEMPTS
            and policy.get('maximum_asset_seconds') == ASSET_SECONDS
            and policy.get('maximum_worker_seconds') == WORKER_SECONDS
            and policy.get('curl_segment_bytes') == SEGMENT_BYTES, 'Finite v3 curl transport policy changed')
    pinned = policy.get('system_curl', {})
    require(pinned.get('path') == str(CURL_PATH) and CURL_PATH.is_file()
            and not CURL_PATH.is_symlink() and os.access(CURL_PATH, os.X_OK)
            and re.fullmatch('[0-9a-f]{64}', str(pinned.get('sha256', ''))), 'Pinned ordinary system curl unavailable')
    digest = hashlib.sha256(CURL_PATH.read_bytes()).hexdigest()
    require(digest == pinned['sha256'], 'System curl binary differs from native preparation')
    version = runner([str(CURL_PATH), '-q', '--version'], capture_output=True, text=True,
                     timeout=10, check=False)
    require(version.returncode == 0 and isinstance(version.stdout, str) and len(version.stdout) <= 16384
            and version.stdout == pinned.get('version'), 'System curl version differs from native preparation')
    return dict(pinned)


def staging_directory(item, root, helpers):
    target = helpers.acquire.safe_destination(Path(root).resolve(), item['relative_path'])
    return target.with_name(target.name + '.curl-v3')


def _binding(item, helpers, policy):
    return {'schema_version': 3, 'asset': item, 'asset_hash': helpers.core.canonical_hash(item),
            'policy_sha256': helpers.core.canonical_hash(policy), 'protocol': 'system_curl_https',
            'canonical_url': item['url'], 'expected_bytes': item['bytes'],
            'provider_checksum': item['provider_checksum']}


def preflight_destination(item, root, helpers, policy):
    """Metadata audit only; the worker independently verifies retained data."""
    _validate_item(item, helpers)
    root = Path(root).resolve(); target = helpers.acquire.safe_destination(root, item['relative_path'])
    for path in (root / item['relative_path'], target, target.with_name(target.name + '.part'),
                 target.with_name(target.name + '.part.json')):
        require(not path.is_symlink(), 'Foreign original source/partial symlink must be preserved')
    require(not target.with_name(target.name + '.part').exists()
            and not target.with_name(target.name + '.part.json').exists(),
            'V3 cannot adopt an older FTP/Requests partial; native preservation is required')
    if target.exists():
        _ordinary(target, root)
    area = staging_directory(item, root, helpers)
    if target.exists():
        # The actual worker will provider-verify this final. Completed staging
        # remains an immutable audit trail and is never reopened for writing.
        return {'target': target, 'staging': area}
    if area.exists() or area.is_symlink():
        _ordinary(area, root, directory=True)
        binding = _ordinary(area / 'binding.json', root)
        require(helpers.core.read_json(binding) == _binding(item, helpers, policy), 'Existing v3 staging source/policy differs')
        started = helpers.core.read_json(_ordinary(area / 'asset-started.json', root))
        require(isinstance(started.get('started_unix'), (int, float))
                and math.isfinite(started['started_unix']) and 0 < started['started_unix'] <= time.time(),
                'Retained curl asset deadline is missing or invalid')
        require(not (area / 'promotion-intent.json').exists(), 'Preserve an ambiguous curl promotion; never replay')
        attempts = area / 'attempts'; _ordinary(attempts, root, directory=True)
        entries = sorted(attempts.iterdir())
        require(len(entries) <= MAX_ATTEMPTS and all(p.is_dir() and not p.is_symlink()
                and p.name == f'{n:04d}' for n, p in enumerate(entries, 1)), 'Unbounded or ambiguous curl attempt history')
        for entry in entries:
            outcome = _ordinary(entry / 'result.json', root)
            record = helpers.core.read_json(outcome)
            require(record.get('asset_hash') == _binding(item, helpers, policy)['asset_hash']
                    and record.get('policy_sha256') == helpers.core.canonical_hash(policy)
                    and record.get('status') == 'failed', 'Incomplete or foreign curl attempt must be preserved')
    return {'target': target, 'staging': area}


def parse_response_headers(raw, *, start, end, total, validators=None):
    require(len(raw) <= 65536, 'Excessive curl HTTP headers')
    blocks = [x for x in re.split(r'\r?\n\r?\n', raw.decode('iso-8859-1')) if x.strip()]
    require(len(blocks) == 1, 'Multiple HTTP responses, proxy headers or redirects refused')
    lines = blocks[0].splitlines(); matched = re.fullmatch(r'HTTP/(?:1\.[01]|2|3) (\d{3})(?: .*)?', lines[0])
    require(matched is not None, 'Invalid curl HTTP status line')
    status = int(matched.group(1)); headers = {}
    for line in lines[1:]:
        require(':' in line and not line[:1].isspace(), 'Malformed curl HTTP header')
        key, value = line.split(':', 1); key = key.lower().strip()
        require(key not in headers, 'Duplicate curl HTTP header refused')
        headers[key] = value.strip()
    require(not 300 <= status <= 399 and 'location' not in headers, 'Curl HTTP redirect refused')
    if status in {408, 429, 500, 502, 503, 504}:
        raise CurlNetworkError('Transient curl HTTP status ' + str(status))
    require(status == 206, 'Curl requires an exact 206 range response')
    require(headers.get('content-range') == f'bytes {start}-{end}/{total}', 'Curl Content-Range differs from frozen source span')
    require(headers.get('content-encoding', '').lower() in {'', 'identity'}, 'Encoded curl response refused')
    require('content-length' not in headers or headers['content-length'] == str(end - start + 1), 'Curl Content-Length differs from range span')
    current = {key: headers.get(key) for key in ('etag', 'last-modified')}
    require(any(current.values()), 'Curl response lacks an entity validator')
    require(validators is None or current == validators, 'Curl entity validator changed during transfer')
    return {'status_code': status, 'headers': headers, 'validators': current,
            'content_range': headers['content-range'], 'content_encoding': 'identity'}


def _fetch_segment(item, directory, index, start, end, validators, policy, deadline, bandwidth, runner):
    _check_time(deadline)
    length = end - start + 1
    paths = {key: directory / f'segment-{index:06d}.{suffix}' for key, suffix in
             [('body', 'body.private'), ('headers', 'headers.private'),
              ('stderr', 'stderr.private'), ('stdout', 'stdout.private')]}
    before = {key: _new_file(path) for key, path in paths.items()}
    seconds = min(SEGMENT_SECONDS, deadline - time.monotonic())
    argv = [str(CURL_PATH), '-q', '--silent', '--show-error', '--noproxy', '*', '--proto', '=https',
            '--http1.1', '--connect-timeout', str(min(20, seconds)), '--max-time', str(seconds),
            '--limit-rate', str(bandwidth), '--max-filesize', str(length), '--max-redirs', '0',
            '--header', 'Accept-Encoding: identity', '--header', 'Cache-Control: no-cache',
            '--header', 'Pragma: no-cache', '--header', 'Connection: close',
            '--range', f'{start}-{end}', '--dump-header', str(paths['headers']),
            '--output', str(paths['body']), item['url']]
    if validators:
        argv[2:2] = ['--header', 'If-Range: ' + (validators['etag'] or validators['last-modified'])]
    record = {'segment_index': index, 'start': start, 'end': end, 'expected_bytes': length,
              'argv': argv, 'paths': {key: str(value) for key, value in paths.items()},
              'started_unix': time.time(), 'status': 'started'}
    def cap_output():
        import resource
        # Applies even when a server ignores Range and omits Content-Length.
        resource.setrlimit(resource.RLIMIT_FSIZE, (max(length, 65536), max(length, 65536)))
    try:
        kwargs = {'timeout': max(0.001, seconds), 'check': False}
        if os.name == 'posix': kwargs['preexec_fn'] = cap_output
        with paths['stdout'].open('wb') as stdout, paths['stderr'].open('wb') as stderr:
            completed = runner(argv, stdout=stdout, stderr=stderr, **kwargs)
        record['returncode'] = completed.returncode
        for key, path in paths.items():
            info = path.lstat()
            require(stat.S_ISREG(info.st_mode) and info.st_dev == before[key]['device']
                    and info.st_ino == before[key]['inode'], 'Curl private output identity changed')
        record['bytes'] = paths['body'].stat().st_size
        require(record['bytes'] <= length, 'Curl body exceeded finite range bound')
        if not paths['headers'].stat().st_size and completed.returncode in {5, 6, 7, 18, 28, 35, 52, 55, 56}:
            raise CurlNetworkError('Finite curl connection failure ' + str(completed.returncode))
        record['response'] = parse_response_headers(paths['headers'].read_bytes(), start=start, end=end,
                                                   total=item['bytes'], validators=validators)
        if completed.returncode in {5, 6, 7, 18, 28, 35, 52, 55, 56}:
            raise CurlNetworkError('Finite curl transport failure ' + str(completed.returncode))
        require(completed.returncode == 0, 'Nonrecoverable curl process failure ' + str(completed.returncode))
        if record['bytes'] != length:
            raise CurlNetworkError('Curl range body ended before its declared length')
        body = paths['body'].read_bytes()
        embedded = body.find(EMBEDDED_ERROR_SIGNATURE)
        if embedded >= 0:
            record['known_embedded_transport_error'] = {
                'signature': 'ConnectionClosedException/Premature end of Content-Length delimited message body',
                'absolute_offset': start + embedded, 'segment_offset': embedded,
                'matched_bytes': len(EMBEDDED_ERROR_SIGNATURE),
                'matched_signature_sha256': hashlib.sha256(EMBEDDED_ERROR_SIGNATURE).hexdigest()}
            raise CurlNetworkError('Known embedded ConnectionClosedException XML in curl range; reject before append')
        record.update(status='verified_segment', sha256=hashlib.sha256(body).hexdigest())
        return record
    except subprocess.TimeoutExpired as error:
        record.update(status='failed', error_type=type(error).__name__, error='Finite curl segment timeout')
        raise CurlNetworkError(record['error']) from error
    except BaseException as error:
        record.update(status='failed', error_type=type(error).__name__, error=str(error))
        raise
    finally:
        record.update(finished_unix=time.time(), observed_body_bytes=paths['body'].stat().st_size,
                      headers_sha256=hashlib.sha256(paths['headers'].read_bytes()).hexdigest(),
                      body_sha256=hashlib.sha256(paths['body'].read_bytes()).hexdigest())
        _exclusive_json(directory / f'segment-{index:06d}.json', record)


def _checkpoint(path, candidate, item, helpers, policy, contexts, validators):
    value = {'asset_hash': helpers.core.canonical_hash(item), 'policy_sha256': helpers.core.canonical_hash(policy),
             'candidate_snapshot': _snapshot(candidate), 'prefix_bytes': candidate.stat().st_size,
             'prefix_digests': {name: value.hexdigest() for name, value in contexts.items()},
             'validators': validators, 'candidate_path': str(candidate)}
    helpers.core.atomic_json(path, value)
    return value


def _copy_verified_prefix(previous, candidate, root, item, helpers, policy, deadline):
    old = helpers.core.read_json(_ordinary(previous, root)); source = _ordinary(Path(old['candidate_path']), root)
    require(old.get('asset_hash') == helpers.core.canonical_hash(item)
            and old.get('policy_sha256') == helpers.core.canonical_hash(policy)
            and old.get('candidate_snapshot') == _snapshot(source)
            and type(old.get('prefix_bytes')) is int and 0 <= old['prefix_bytes'] < item['bytes']
            and old['prefix_bytes'] % SEGMENT_BYTES == 0, 'Retained curl checkpoint differs from committed source prefix')
    contexts = {name: hashlib.new(name) for name in ('md5', 'sha256')}; before = _snapshot(source)
    with source.open('rb') as src, candidate.open('xb') as dst:
        for chunk in iter(lambda: src.read(8 * 1024**2), b''):
            _check_time(deadline); dst.write(chunk)
            for value in contexts.values(): value.update(chunk)
        dst.flush(); os.fsync(dst.fileno())
    require(_snapshot(source) == before == old['candidate_snapshot']
            and candidate.stat().st_size == old['prefix_bytes']
            and {name: value.hexdigest() for name, value in contexts.items()} == old['prefix_digests'],
            'Retained curl prefix failed independent verification; no request permitted')
    return contexts, old['validators'], old['prefix_bytes']


def download_asset(item, root, helpers, *, policy, deadline, attempts=MAX_ATTEMPTS,
                   attempt_seconds=ASSET_SECONDS, bandwidth=MAX_BANDWIDTH,
                   sleeper=time.sleep, runner=subprocess.run):
    """Finite transactional ranges with explicit bounded fresh integrity retries."""
    require(type(attempts) is int and 1 <= attempts <= MAX_ATTEMPTS, 'Curl attempts must be finite 1..6')
    require(isinstance(deadline, (int, float)) and not isinstance(deadline, bool) and math.isfinite(deadline), 'Finite curl worker deadline required')
    require(isinstance(attempt_seconds, (int, float)) and not isinstance(attempt_seconds, bool)
            and math.isfinite(attempt_seconds) and 0 < attempt_seconds <= ASSET_SECONDS, 'Finite six-hour curl asset ceiling required')
    require(type(bandwidth) is int and 1 <= bandwidth <= MAX_BANDWIDTH, 'Curl bandwidth exceeds fixed ceiling')
    asset_started = time.monotonic(); deadline = min(deadline, asset_started + attempt_seconds)
    _check_time(deadline); validate_policy_runtime(policy, runner=runner)
    checked = preflight_destination(item, root, helpers, policy)
    root = Path(root).resolve(); target, area = checked['target'], checked['staging']
    if target.exists(): return helpers.acquire.verify_file(target, item)
    target.parent.mkdir(parents=True, exist_ok=True); _ordinary(target.parent, root, directory=True)
    if not area.exists():
        area.mkdir(mode=0o700, exist_ok=False); (area / 'attempts').mkdir(mode=0o700)
        _exclusive_json(area / 'binding.json', _binding(item, helpers, policy))
        _exclusive_json(area / 'asset-started.json', {'started_unix': time.time(), 'maximum_asset_seconds': ASSET_SECONDS})
    asset_clock = helpers.core.read_json(area / 'asset-started.json')
    deadline = min(deadline, time.monotonic() + ASSET_SECONDS - (time.time() - asset_clock['started_unix']))
    _check_time(deadline)
    prior = sorted((area / 'attempts').iterdir()); previous = None
    if prior:
        last = helpers.core.read_json(prior[-1] / 'result.json')
        require(last.get('retryable') is True, 'Previous curl outcome is terminal; preserve without replay')
        previous = Path(last['checkpoint_path']) if last.get('failure_class') == 'network' else None
    for number in range(len(prior) + 1, attempts + 1):
        _check_time(deadline)
        directory = area / 'attempts' / f'{number:04d}'; directory.mkdir(mode=0o700, exist_ok=False)
        candidate = directory / 'candidate.private'; checkpoint_path = directory / 'checkpoint.json'
        record = {'status': 'started', 'attempt': number, 'asset_hash': helpers.core.canonical_hash(item),
                  'policy_sha256': helpers.core.canonical_hash(policy), 'canonical_url': item['url'],
                  'expected_bytes': item['bytes'], 'provider_checksum': item['provider_checksum'],
                  'candidate_path': str(candidate), 'checkpoint_path': str(checkpoint_path),
                  'started_unix': time.time(), 'system_curl': policy['system_curl'], 'provider_checksum_waived': False}
        _exclusive_json(directory / 'started.json', record)
        try:
            if previous is not None:
                contexts, validators, offset = _copy_verified_prefix(previous, candidate, root, item, helpers, policy, deadline)
            else:
                _new_file(candidate); contexts = {name: hashlib.new(name) for name in ('md5', 'sha256')}
                validators, offset = None, 0
            record['resumed_from_bytes'] = offset
            _checkpoint(checkpoint_path, candidate, item, helpers, policy, contexts, validators)
            segment_index = 0
            while offset < item['bytes']:
                _check_time(deadline); end = min(offset + SEGMENT_BYTES, item['bytes']) - 1
                segment = _fetch_segment(item, directory, segment_index, offset, end, validators,
                                         policy, deadline, bandwidth, runner)
                body = _ordinary(Path(segment['paths']['body']), root); before = _snapshot(body)
                require(before['bytes'] == end - offset + 1, 'Verified curl segment length changed')
                segment_digest = hashlib.sha256(); candidate_before = _snapshot(candidate)
                committed = helpers.core.read_json(checkpoint_path)
                require(committed['candidate_snapshot'] == candidate_before
                        and committed['prefix_bytes'] == offset
                        and committed['prefix_digests'] == {name: value.hexdigest() for name, value in contexts.items()},
                        'Candidate checkpoint changed before verified append')
                fd = os.open(candidate, os.O_WRONLY | os.O_APPEND | getattr(os, 'O_NOFOLLOW', 0))
                with os.fdopen(fd, 'ab') as dst, body.open('rb') as src:
                    require(_snapshot(os.fstat(dst.fileno())) == candidate_before
                            and candidate_before['bytes'] == offset, 'Candidate prefix changed before append')
                    for chunk in iter(lambda: src.read(8 * 1024**2), b''):
                        _check_time(deadline); dst.write(chunk); segment_digest.update(chunk)
                        for value in contexts.values(): value.update(chunk)
                    dst.flush(); os.fsync(dst.fileno())
                require(_snapshot(body) == before and segment_digest.hexdigest() == segment['sha256']
                        and candidate.stat().st_size == end + 1, 'Curl segment changed during verified append')
                validators = segment['response']['validators']; offset = end + 1; segment_index += 1
                _checkpoint(checkpoint_path, candidate, item, helpers, policy, contexts, validators)
            _check_time(deadline); before = _snapshot(candidate)
            stream = {name: value.hexdigest() for name, value in contexts.items()}
            disk = helpers.acquire.file_digests(candidate, {'md5', 'sha256'}); _check_time(deadline)
            require(_snapshot(candidate) == before and before['bytes'] == item['bytes'], 'Curl candidate changed during independent disk verification')
            record.update(stream_digests=stream, independent_disk_digests=disk, stream_equals_disk=stream == disk,
                          independent_disk_verified=True, candidate_snapshot=before)
            # A provider payload mismatch can be retried from a fresh download,
            # but disagreement with the bytes this process already appended is
            # local candidate corruption (or an unreliable independent read).
            # Metadata cannot prove immutability: a same-size overwrite may
            # retain mtime on a coarse filesystem clock or restore it explicitly.
            if stream != disk:
                raise CurlTransferError('Curl candidate changed during independent disk verification: stream/disk checksums differ')
            if disk['md5'] != item['provider_checksum'].split(':', 1)[1]:
                raise CurlIntegrityError('Curl candidate provider checksum mismatch')
            verified = helpers.acquire.verify_file(candidate, item, computed_digests=disk)
            require(not target.exists() and not target.is_symlink() and _snapshot(candidate) == before,
                    'Final source appeared or candidate changed before exclusive promotion')
            intent = {**record, 'status': 'verified_before_exclusive_promotion', 'target': str(target)}
            _exclusive_json(area / 'promotion-intent.json', intent)
            os.link(candidate, target)
            require(target.is_file() and not target.is_symlink(), 'Promoted curl source is not ordinary')
            record.update(status='verified_promoted', finished_unix=time.time(), target=str(target), retryable=False)
            _exclusive_json(directory / 'result.json', record)
            promoted = {**verified, **record, 'path': str(target), 'transfer_route': item['url'],
                        'status': 'verified', 'promotion_status': 'verified_promoted',
                        'transport_backend': 'system_curl', 'primary_acquisition_code_unchanged': True}
            _exclusive_json(area / 'promotion.json', promoted)
            return promoted
        except BaseException as error:
            kind = 'network' if isinstance(error, CurlNetworkError) else 'integrity' if isinstance(error, CurlIntegrityError) else 'terminal'
            retryable = kind in {'network', 'integrity'} and number < attempts and time.monotonic() + BACKOFF[number - 1] < deadline
            record.update(status='failed', error_type=type(error).__name__, error=str(error), failure_class=kind,
                          retryable=retryable, finished_unix=time.time(), failed_bytes_preserved=True)
            if not (directory / 'result.json').exists(): _exclusive_json(directory / 'result.json', record)
            if not retryable: raise
            previous = checkpoint_path if kind == 'network' else None
            sleeper(BACKOFF[number - 1])
    raise CurlTransferError('Finite curl asset attempt budget exhausted; all failed evidence retained')
