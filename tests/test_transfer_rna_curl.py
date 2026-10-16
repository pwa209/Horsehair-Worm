"""V3 curl integrity, transactional resume and bounded retry tests; no network."""
import hashlib
import importlib.util
import os
from pathlib import Path
import time
from types import SimpleNamespace

import pytest

from horsehair import acquire, core


SPEC = importlib.util.spec_from_file_location('tested_transfer_rna_curl',
    Path(__file__).resolve().parents[1] / 'scripts/transfer_rna_curl.py')
curl = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(curl)
HELPERS = SimpleNamespace(core=core, acquire=acquire)
VERSION = 'curl fixture 8.0\nProtocols: https\n'


@pytest.fixture
def setup(tmp_path, monkeypatch):
    binary = tmp_path / 'system-curl'; binary.write_bytes(b'pinned system binary fixture')
    binary.chmod(0o700)
    monkeypatch.setattr(curl, 'CURL_PATH', binary)
    monkeypatch.setattr(curl, 'SEGMENT_BYTES', 4)
    monkeypatch.setattr(curl.socket, 'gethostname', lambda: 'narval3')
    if os.name == 'nt':
        # Native qualification uses the real exclusive hard-link syscall.
        def exclusive_link(source, destination):
            with Path(destination).open('xb') as out:
                out.write(Path(source).read_bytes())
        monkeypatch.setattr(curl.os, 'link', exclusive_link)
    policy = {'schema_version': 3, 'name': curl.POLICY_NAME,
        'transport_backend': 'system_curl', 'target_transfer_host': 'narval3',
        'maximum_transport_attempts_per_asset': 6, 'maximum_integrity_attempts_per_asset': 6,
        'maximum_asset_seconds': 21600, 'maximum_worker_seconds': 432000,
        'curl_segment_bytes': 4, 'system_curl': {'path': str(binary),
            'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'version': VERSION}}
    return policy


def item(body=b'abcdefghij'):
    return acquire.asset(source='ant_rna', name='SRR123_1.fastq.gz',
        url='https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR123/001/SRR123/SRR123_1.fastq.gz',
        version='PRJNA-fixture', rights='INSDC public archive', size=len(body),
        checksum='md5:' + hashlib.md5(body).hexdigest(), kind='fastq', eligible=True,
        run_accession='SRR123', crosswalk_hash='c' * 64)


class Runner:
    def __init__(self, body, change=None):
        self.body, self.change, self.calls = body, change, []

    def __call__(self, argv, **kwargs):
        if '--version' in argv:
            return SimpleNamespace(returncode=0, stdout=VERSION, stderr='')
        start, end = map(int, argv[argv.index('--range') + 1].split('-'))
        body = self.body[start:end + 1]
        headers = (f'HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{end}/{len(self.body)}\r\n'
                   f'Content-Length: {end-start+1}\r\nETag: "unchanged"\r\nLast-Modified: fixture\r\n\r\n').encode()
        response = {'body': body, 'headers': headers, 'returncode': 0}
        self.calls.append({'argv': argv, 'start': start, 'end': end, 'kwargs': kwargs})
        if self.change:
            response.update(self.change(self.calls[-1], len(self.calls), response) or {})
        Path(argv[argv.index('--output') + 1]).write_bytes(response['body'])
        Path(argv[argv.index('--dump-header') + 1]).write_bytes(response['headers'])
        return SimpleNamespace(returncode=response['returncode'])


def run_download(root, policy, runner, source=None, **kwargs):
    return curl.download_asset(source or item(runner.body), root, HELPERS, policy=policy,
        deadline=time.monotonic() + 1000, sleeper=lambda _: None, runner=runner, **kwargs)


def target(root, source=None):
    return acquire.safe_destination(root, (source or item())['relative_path'])


def test_valid_ranges_verify_full_provider_md5_before_exclusive_promotion(tmp_path, setup, monkeypatch):
    runner = Runner(b'abcdefghij'); source = item(runner.body)
    before = core.canonical_hash(source); calls = []
    original = acquire.file_digests
    def independent(path, algorithms):
        calls.append(Path(path)); return original(path, algorithms)
    monkeypatch.setattr(acquire, 'file_digests', independent)
    result = run_download(tmp_path, setup, runner, source)
    assert target(tmp_path, source).read_bytes() == runner.body
    assert [(x['start'], x['end']) for x in runner.calls] == [(0, 3), (4, 7), (8, 9)]
    assert result['independent_disk_verified'] and result['stream_equals_disk']
    assert result['independent_disk_digests']['md5'] == hashlib.md5(runner.body).hexdigest()
    assert len(calls) == 1 and core.canonical_hash(source) == before
    assert result['status'] == 'verified' and result['promotion_status'] == 'verified_promoted' and result['provider_checksum_waived'] is False
    for call in runner.calls:
        argv = call['argv']
        assert argv[1] == '-q' and argv[argv.index('--noproxy') + 1] == '*'
        assert '--location' not in argv and '--retry' not in argv and '--continue-at' not in argv
        assert '--http1.1' in argv and 'Accept-Encoding: identity' in argv
        assert '--max-filesize' in argv and float(argv[argv.index('--max-time') + 1]) <= 300


def test_cached_final_is_provider_reverified_and_completed_staging_is_not_reopened(tmp_path, setup):
    runner = Runner(b'abcdefghij'); run_download(tmp_path, setup, runner)
    cached = Runner(runner.body); result = run_download(tmp_path, setup, cached)
    assert cached.calls == [] and result['status'] == 'verified'
    target(tmp_path).write_bytes(b'x' * 10)
    with pytest.raises(acquire.AcquisitionError, match='checksum'):
        run_download(tmp_path, setup, cached)
    assert cached.calls == []


def test_network_retry_copies_only_a_verified_committed_prefix(tmp_path, setup):
    def fail_second(call, count, response):
        if count == 2: return {'body': b'e', 'returncode': 18}
    runner = Runner(b'abcdefgh', fail_second); source = item(runner.body)
    result = run_download(tmp_path, setup, runner, source)
    assert [(x['start'], x['end']) for x in runner.calls] == [(0, 3), (4, 7), (4, 7)]
    assert result['resumed_from_bytes'] == 4
    area = curl.staging_directory(source, tmp_path, HELPERS)
    assert (area / 'attempts/0001/candidate.private').read_bytes() == b'abcd'
    assert (area / 'attempts/0001/segment-000001.body.private').read_bytes() == b'e'
    assert core.read_json(area / 'attempts/0001/result.json')['failure_class'] == 'network'
    assert target(tmp_path, source).read_bytes() == runner.body


def test_connection_failure_without_headers_retries_from_owned_empty_checkpoint(tmp_path, setup):
    runner = Runner(b'abcd', lambda call, count, response:
                    {'body': b'', 'headers': b'', 'returncode': 7} if count == 1 else None)
    result = run_download(tmp_path, setup, runner)
    assert len(runner.calls) == 2 and result['resumed_from_bytes'] == 0


def test_exact_embedded_upstream_error_in_valid_206_is_rejected_before_append_and_retried(tmp_path, setup, monkeypatch):
    monkeypatch.setattr(curl, 'SEGMENT_BYTES', 1024); setup['curl_segment_bytes'] = 1024
    body = b'original compressed representation bytes;' * 40
    marker = curl.EMBEDDED_ERROR_SIGNATURE + b' 1024; received: 20)</Message><ErrorMessage/><RequestId/></Error>'
    def inject(call, count, response):
        if count == 1:
            data = response['body']; return {'body': data[:40] + marker + data[40+len(marker):]}
    runner = Runner(body, inject); source = item(body)
    result = run_download(tmp_path, setup, runner, source)
    area = curl.staging_directory(source, tmp_path, HELPERS)
    failed_segment = core.read_json(area / 'attempts/0001/segment-000000.json')
    assert failed_segment['known_embedded_transport_error']['absolute_offset'] == 40
    assert failed_segment['status'] == 'failed' and failed_segment['bytes'] == 1024
    assert (area / 'attempts/0001/candidate.private').read_bytes() == b''
    assert marker in (area / 'attempts/0001/segment-000000.body.private').read_bytes()
    assert core.read_json(area / 'attempts/0001/result.json')['failure_class'] == 'network'
    assert result['status'] == 'verified' and target(tmp_path, source).read_bytes() == body


def test_repeated_embedded_upstream_errors_have_finite_retries_and_never_append(tmp_path, setup, monkeypatch):
    monkeypatch.setattr(curl, 'SEGMENT_BYTES', 1024); setup['curl_segment_bytes'] = 1024
    body = b'original compressed representation bytes;' * 20
    marker = curl.EMBEDDED_ERROR_SIGNATURE + b' 820; received: 0)</Message></Error>'
    def inject(call, count, response):
        return {'body': marker + response['body'][len(marker):]}
    runner = Runner(body, inject); source = item(body)
    with pytest.raises(curl.CurlNetworkError, match='embedded ConnectionClosedException'):
        run_download(tmp_path, setup, runner, source, attempts=6)
    area = curl.staging_directory(source, tmp_path, HELPERS)
    assert len(runner.calls) == 6 and not target(tmp_path, source).exists()
    assert all((p / 'candidate.private').read_bytes() == b'' for p in (area / 'attempts').iterdir())


def test_provider_mismatch_gets_a_bounded_fresh_candidate_and_preserves_failed_bytes(tmp_path, setup):
    runner = Runner(b'abcdefgh', lambda call, count, response:
                    {'body': b'x' * len(response['body'])} if count <= 2 else None)
    source = item(runner.body); result = run_download(tmp_path, setup, runner, source, attempts=2)
    area = curl.staging_directory(source, tmp_path, HELPERS)
    assert (area / 'attempts/0001/candidate.private').read_bytes() == b'xxxxxxxx'
    first = core.read_json(area / 'attempts/0001/result.json')
    assert first['failure_class'] == 'integrity' and first['retryable']
    assert first['independent_disk_digests']['md5'] != source['provider_checksum'].split(':')[1]
    assert result['resumed_from_bytes'] == 0 and target(tmp_path, source).read_bytes() == runner.body


def test_repeated_provider_mismatch_exhausts_fixed_budget_without_promotion_or_replay(tmp_path, setup):
    runner = Runner(b'abcd', lambda call, count, response: {'body': b'xxxx'})
    source = item(runner.body)
    with pytest.raises(curl.CurlIntegrityError, match='provider checksum'):
        run_download(tmp_path, setup, runner, source, attempts=2)
    assert len(runner.calls) == 2 and not target(tmp_path, source).exists()
    with pytest.raises(curl.CurlTransferError, match='terminal'):
        run_download(tmp_path, setup, runner, source, attempts=2)
    assert len(runner.calls) == 2


@pytest.mark.parametrize('change', ['range', 'total', 'length', 'encoding', 'redirect', 'multiple', 'validator'])
def test_invalid_response_never_appends_to_trusted_prefix_or_retries(tmp_path, setup, change):
    def wrong(call, count, response):
        if count != 2: return
        headers = response['headers']
        if change == 'range': headers = headers.replace(b'bytes 4-7/', b'bytes 3-7/')
        elif change == 'total': headers = headers.replace(b'/8\r\n', b'/9\r\n')
        elif change == 'length': headers = headers.replace(b'Content-Length: 4', b'Content-Length: 3')
        elif change == 'encoding': headers = headers.replace(b'ETag:', b'Content-Encoding: gzip\r\nETag:')
        elif change == 'redirect': headers = headers.replace(b'206 Partial Content', b'302 Found')
        elif change == 'multiple': headers += headers
        else: headers = headers.replace(b'"unchanged"', b'"changed"')
        return {'headers': headers}
    runner = Runner(b'abcdefgh', wrong); source = item(runner.body)
    with pytest.raises(curl.CurlTransferError): run_download(tmp_path, setup, runner, source)
    area = curl.staging_directory(source, tmp_path, HELPERS)
    assert len(runner.calls) == 2 and not target(tmp_path, source).exists()
    assert (area / 'attempts/0001/candidate.private').read_bytes() == b'abcd'


def test_short_success_response_is_network_failure_but_oversized_body_is_terminal(tmp_path, setup):
    runner = Runner(b'abcd', lambda call, count, response: {'body': b'a'} if count == 1 else None)
    assert run_download(tmp_path, setup, runner)['status'] == 'verified'
    other = tmp_path / 'other'; other.mkdir()
    bad = Runner(b'abcd', lambda call, count, response: {'body': b'abcde'})
    with pytest.raises(curl.CurlTransferError, match='exceeded'):
        run_download(other, setup, bad)
    assert len(bad.calls) == 1 and not target(other).exists()


def test_changed_checkpoint_bytes_cannot_resume_or_trigger_another_data_request(tmp_path, setup):
    def fail_and_corrupt(call, count, response):
        if count == 2:
            candidate = Path(call['argv'][call['argv'].index('--output') + 1]).parent / 'candidate.private'
            candidate.write_bytes(b'xxxx')
            return {'body': b'e', 'returncode': 18}
    runner = Runner(b'abcdefgh', fail_and_corrupt)
    with pytest.raises(curl.CurlTransferError, match='checkpoint'):
        run_download(tmp_path, setup, runner)
    assert len(runner.calls) == 2 and not target(tmp_path).exists()


def test_candidate_change_during_range_fetch_is_refused_before_append(tmp_path, setup):
    def mutate_prefix(call, count, response):
        if count == 2:
            candidate = Path(call['argv'][call['argv'].index('--output') + 1]).parent / 'candidate.private'
            candidate.write_bytes(b'xxxx')
    runner = Runner(b'abcdefgh', mutate_prefix)
    with pytest.raises(curl.CurlTransferError, match='checkpoint changed before verified append'):
        run_download(tmp_path, setup, runner)
    area = curl.staging_directory(item(runner.body), tmp_path, HELPERS)
    assert (area / 'attempts/0001/candidate.private').read_bytes() == b'xxxx'
    assert len(runner.calls) == 2 and not target(tmp_path).exists()


def test_an_existing_final_created_during_verification_is_never_overwritten(tmp_path, setup, monkeypatch):
    runner = Runner(b'abcd'); original = acquire.file_digests
    def appear(path, algorithms):
        result = original(path, algorithms)
        target(tmp_path).write_bytes(b'foreign bytes'); return result
    monkeypatch.setattr(acquire, 'file_digests', appear)
    with pytest.raises(curl.CurlTransferError, match='Final source appeared'):
        run_download(tmp_path, setup, runner)
    assert target(tmp_path).read_bytes() == b'foreign bytes'


def test_stream_disk_disagreement_cannot_promote(tmp_path, setup, monkeypatch):
    runner = Runner(b'abcd'); original = acquire.file_digests
    def corrupt(path, algorithms):
        value = original(path, algorithms); value['sha256'] = '0' * 64; return value
    monkeypatch.setattr(acquire, 'file_digests', corrupt)
    with pytest.raises(curl.CurlIntegrityError, match='stream/disk'):
        run_download(tmp_path, setup, runner, attempts=1)
    assert not target(tmp_path).exists()


def test_candidate_changed_during_disk_read_is_terminal_and_retained(tmp_path, setup, monkeypatch):
    runner = Runner(b'abcd'); original = acquire.file_digests
    def mutate(path, algorithms):
        Path(path).write_bytes(b'xxxx'); return original(path, algorithms)
    monkeypatch.setattr(acquire, 'file_digests', mutate)
    with pytest.raises(curl.CurlTransferError, match='changed during independent disk'):
        run_download(tmp_path, setup, runner)
    assert len(runner.calls) == 1 and not target(tmp_path).exists()


@pytest.mark.parametrize('change', ['host', 'binary', 'version', 'policy'])
def test_runtime_must_match_exact_native_host_binary_and_policy(tmp_path, setup, monkeypatch, change):
    runner = Runner(b'abcd')
    if change == 'host': monkeypatch.setattr(curl.socket, 'gethostname', lambda: 'narval1')
    elif change == 'binary': curl.CURL_PATH.write_bytes(b'changed executable')
    elif change == 'version': setup['system_curl']['version'] = 'other version'
    else: setup['name'] = 'all-selected-fastq-host-bound-https-v2'
    with pytest.raises(curl.CurlTransferError): run_download(tmp_path, setup, runner)
    assert not runner.calls and not target(tmp_path).exists()


def test_foreign_requests_partial_is_not_adopted(tmp_path, setup):
    path = target(tmp_path); path.parent.mkdir(parents=True)
    partial = path.with_name(path.name + '.part'); partial.write_bytes(b'ab')
    runner = Runner(b'abcd')
    with pytest.raises(curl.CurlTransferError, match='older FTP/Requests partial'):
        run_download(tmp_path, setup, runner)
    assert partial.read_bytes() == b'ab' and not runner.calls


def test_expired_global_deadline_makes_no_data_or_runtime_request(tmp_path, setup):
    runner = Runner(b'abcd')
    with pytest.raises(curl.CurlTransferError, match='deadline'):
        curl.download_asset(item(runner.body), tmp_path, HELPERS, policy=setup,
                            deadline=time.monotonic()-1, runner=runner)
    assert not runner.calls and not target(tmp_path).exists()
