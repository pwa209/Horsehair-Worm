"""FTP transport integrity and launch guards; no public data are downloaded."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import time
from types import SimpleNamespace

import pytest

from horsehair import acquire, core

SPEC = importlib.util.spec_from_file_location(
    'tested_transfer_rna_ftp', Path(__file__).resolve().parents[1] / 'scripts/transfer_rna_ftp.py')
transport = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(transport)
HELPERS = SimpleNamespace(core=core, acquire=acquire)


@pytest.fixture(autouse=True)
def workstation_hardlink_syscall(monkeypatch):
    # The Windows workspace sandbox denies CreateHardLink even inside its
    # writable root. Model its exclusive-creation semantics here; native Linux
    # qualification executes the actual os.link implementation unchanged.
    if os.name == 'nt':
        def exclusive_link(src, dst):
            with Path(dst).open('xb') as output:
                output.write(Path(src).read_bytes())
        monkeypatch.setattr(transport.os, 'link', exclusive_link)


def source(body=b'compressed archive fixture bytes', name='SRR123_1.fastq.gz'):
    return acquire.asset(source='ant_rna', name=name,
        url='https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR123/001/SRR123/' + name,
        version='PRJNA-fixture', rights='INSDC public archive', size=len(body),
        checksum='md5:' + hashlib.md5(body).hexdigest(), kind='fastq', eligible=True,
        run_accession='SRR123')


class FTP:
    def __init__(self, body, *, size=None, stamp='20261014090000', stop_after=None,
                 post_stamp=None, chunks=None):
        self.body, self.reported_size = body, len(body) if size is None else size
        self.stamp, self.post_stamp = stamp, post_stamp
        self.stop_after, self.chunks = stop_after, chunks
        self.calls, self.snapshots, self.closed = [], 0, False

    def connect(self, host, port, timeout):
        self.calls.append(('connect', host, port, timeout))

    def login(self):
        self.calls.append(('login',))

    def set_pasv(self, passive):
        self.calls.append(('passive', passive))

    def voidcmd(self, command):
        assert command == 'TYPE I'
        self.calls.append(('binary',))

    def size(self, path):
        return self.reported_size

    def sendcmd(self, command):
        assert command.startswith('MDTM /vol1/fastq/')
        self.snapshots += 1
        return '213 ' + (self.post_stamp if self.post_stamp and self.snapshots > 1 else self.stamp)

    def retrbinary(self, command, callback, blocksize, rest):
        self.calls.append(('retrieve', command, rest))
        data = self.body[rest or 0:]
        if self.stop_after is not None:
            if self.stop_after: callback(data[:self.stop_after])
            raise OSError('fixture transport interruption')
        for chunk in self.chunks or [data]:
            callback(chunk)

    def close(self):
        self.closed = True


def download(item, root, ftp, **kwargs):
    return transport.download_ftp_asset(item, root, HELPERS, ftp_factory=lambda: ftp,
                                        sleeper=lambda _: None, **kwargs)


def paths(root, item):
    target = acquire.safe_destination(root, item['relative_path'])
    return target, target.with_name(target.name + '.part'), target.with_name(target.name + '.part.json')


def test_valid_ftp_uses_binary_passive_and_independent_disk_digest_before_promotion(tmp_path, monkeypatch):
    body = b'actual received fixture bytes'
    item = source(body); before = core.canonical_hash(item); ftp = FTP(body)
    real_digests = acquire.file_digests
    disk_calls = []
    def independent(path, algorithms):
        disk_calls.append((Path(path), set(algorithms)))
        return real_digests(path, algorithms)
    monkeypatch.setattr(acquire, 'file_digests', independent)
    result = download(item, tmp_path, ftp)
    target, partial, sidecar = paths(tmp_path, item)
    assert target.read_bytes() == body and not partial.exists() and not sidecar.exists()
    assert disk_calls == [(partial, {'md5', 'sha256'})]
    assert result['independent_disk_verified'] is True
    assert result['stream_digests'] == result['independent_disk_digests']
    assert result['sha256'] == hashlib.sha256(body).hexdigest()
    assert result['canonical_url'] == item['url'] and result['transfer_route'].startswith('ftp://')
    assert core.canonical_hash(item) == before
    assert ('binary',) in ftp.calls and ('passive', True) in ftp.calls and ftp.closed
    proof = core.read_json(target.with_name(target.name + '.ftp.json'))
    assert proof['asset_hash'] == before and proof['status'] == 'verified_promoted'


def test_cached_source_is_reverified_without_another_network_connection(tmp_path):
    item = source(); target, _, _ = paths(tmp_path, item)
    target.parent.mkdir(parents=True); target.write_bytes(b'compressed archive fixture bytes')
    ftp = FTP(b'unused')
    result = download(item, tmp_path, ftp)
    assert result['status'] == 'verified' and ftp.calls == []
    target.write_bytes(b'x' * item['bytes'])
    with pytest.raises(acquire.AcquisitionError, match='checksum'):
        download(item, tmp_path, ftp)
    assert ftp.calls == [] and target.read_bytes() == b'x' * item['bytes']


@pytest.mark.parametrize('body,error', [(b'ab', 'shorter'), (b'123456', 'exceeds')])
def test_short_and_oversized_bodies_cannot_promote_and_retained_prefix_is_exact(tmp_path, body, error):
    item = source(b'12345'); ftp = FTP(body, size=5)
    with pytest.raises(transport.TransferError, match=error): download(item, tmp_path, ftp)
    target, partial, sidecar = paths(tmp_path, item)
    assert not target.exists() and partial.exists() and sidecar.exists()
    expected = body if len(body) < 5 else b''
    assert partial.read_bytes() == expected
    assert core.read_json(sidecar)['prefix_bytes'] == len(expected)
    assert core.read_json(partial.with_name(partial.name + '.integrity.json'))['partial_retained'] is True


def test_wrong_provider_checksum_retains_complete_bytes_and_refuses_a_second_fetch(tmp_path):
    item = source(b'12345'); bad = b'abcde'
    with pytest.raises(acquire.AcquisitionError, match='checksum'): download(item, tmp_path, FTP(bad))
    target, partial, sidecar = paths(tmp_path, item)
    before = partial.read_bytes(), sidecar.read_bytes()
    ftp = FTP(b'12345')
    with pytest.raises(acquire.AcquisitionError, match='checksum'): download(item, tmp_path, ftp)
    assert not any(call[0] == 'retrieve' for call in ftp.calls)
    assert not target.exists() and partial.read_bytes() == before[0]
    assert core.read_json(sidecar)['prefix_bytes'] == 5
    assert core.read_json(partial.with_name(partial.name + '.integrity.json'))['source_definition_unchanged'] is True


def test_stream_disk_disagreement_is_reported_before_promotion(tmp_path, monkeypatch):
    item = source(b'12345'); real_digests = acquire.file_digests
    def corrupt(path, algorithms):
        Path(path).write_bytes(b'abcde')
        return real_digests(path, algorithms)
    monkeypatch.setattr(acquire, 'file_digests', corrupt)
    with pytest.raises(transport.TransferError, match='stream/disk'):
        download(item, tmp_path, FTP(b'12345'))
    target, partial, _ = paths(tmp_path, item)
    proof = core.read_json(partial.with_name(partial.name + '.integrity.json'))
    assert not target.exists() and partial.read_bytes() == b'abcde'
    assert proof['stream_digests'] != proof['independent_disk_digests']


@pytest.mark.parametrize('prefix_length', [0, 3])
def test_only_a_digest_bound_ftp_prefix_can_resume_including_an_empty_prefix(tmp_path, prefix_length):
    body = b'123456789'; item = source(body)
    with pytest.raises(OSError, match='interruption'):
        download(item, tmp_path, FTP(body, stop_after=prefix_length))
    _, partial, sidecar = paths(tmp_path, item)
    assert partial.read_bytes() == body[:prefix_length]
    assert core.read_json(sidecar)['prefix_bytes'] == prefix_length
    ftp = FTP(body); result = download(item, tmp_path, ftp)
    assert result['resumed_from_bytes'] == prefix_length
    assert next(call for call in ftp.calls if call[0] == 'retrieve')[2] == (prefix_length or None)
    assert paths(tmp_path, item)[0].read_bytes() == body


@pytest.mark.parametrize('alteration', ['SIZE', 'MDTM', 'prefix', 'protocol', 'asset'])
def test_changed_representation_or_prefix_refuses_resume_without_changing_failed_bytes(tmp_path, alteration):
    body = b'123456789'; item = source(body)
    with pytest.raises(OSError): download(item, tmp_path, FTP(body, stop_after=3))
    _, partial, sidecar = paths(tmp_path, item)
    if alteration == 'prefix': partial.write_bytes(b'abc')
    if alteration in {'protocol', 'asset'}:
        record = core.read_json(sidecar)
        record['protocol' if alteration == 'protocol' else 'asset_hash'] = 'foreign'
        core.atomic_json(sidecar, record)
    before = partial.read_bytes(), sidecar.read_bytes()
    ftp = FTP(body, size=10 if alteration == 'SIZE' else None,
              stamp='20261014090001' if alteration == 'MDTM' else '20261014090000')
    with pytest.raises(transport.TransferError): download(item, tmp_path, ftp)
    assert not any(call[0] == 'retrieve' for call in ftp.calls)
    assert (partial.read_bytes(), sidecar.read_bytes()) == before


def test_remote_modification_during_transfer_retains_bytes(tmp_path):
    item = source(b'12345')
    with pytest.raises(transport.TransferError, match='during transfer'):
        download(item, tmp_path, FTP(b'12345', post_stamp='20261014090001'))
    target, partial, _ = paths(tmp_path, item)
    assert not target.exists() and partial.read_bytes() == b'12345'


def test_atomic_no_overwrite_promotion_preserves_an_unrelated_existing_final(tmp_path, monkeypatch):
    item = source(b'12345'); target, partial, _ = paths(tmp_path, item)
    original_link = transport.os.link
    def concurrent_link(src, dst):
        if Path(dst) == target: target.write_bytes(b'other owner')
        return original_link(src, dst)
    monkeypatch.setattr(transport.os, 'link', concurrent_link)
    with pytest.raises(FileExistsError): download(item, tmp_path, FTP(b'12345'))
    assert target.read_bytes() == b'other owner' and partial.read_bytes() == b'12345'


def test_checkpoint_io_failure_cannot_mask_the_original_transport_failure(tmp_path, monkeypatch):
    item = source(b'12345'); real_atomic = core.atomic_json; calls = []
    def failing_checkpoint(path, record, **kwargs):
        calls.append(Path(path))
        if len(calls) > 1: raise OSError('fixture checkpoint I/O failure')
        return real_atomic(path, record, **kwargs)
    monkeypatch.setattr(core, 'atomic_json', failing_checkpoint)
    with pytest.raises(OSError, match='fixture transport interruption'):
        download(item, tmp_path, FTP(b'12345', stop_after=3))
    assert paths(tmp_path, item)[1].read_bytes() == b'123'


@pytest.mark.parametrize('url', [
    'ftp://ftp.sra.ebi.ac.uk/vol1/fastq/input.fastq.gz',
    'https://other.example/vol1/fastq/input.fastq.gz',
    'https://user:password@ftp.sra.ebi.ac.uk/vol1/fastq/input.fastq.gz',
    'https://ftp.sra.ebi.ac.uk:21/vol1/fastq/input.fastq.gz',
    'https://ftp.sra.ebi.ac.uk/vol1/fastq/../input.fastq.gz',
    'https://ftp.sra.ebi.ac.uk/vol1/fastq/%2E%2E/input.fastq.gz',
    'https://ftp.sra.ebi.ac.uk/vol1/fastq/input.fastq.gz?route=other',
    'https://ftp.sra.ebi.ac.uk/vol1/fastq/input.fastq.gz#other',
    'https://ftp.sra.ebi.ac.uk/vol1/fastq/input%0d%0aOTHER.fastq.gz',
    'https://ftp.sra.ebi.ac.uk/other/input.fastq.gz',
])
def test_ftp_routing_rejects_unsupported_or_ambiguous_canonical_urls(url):
    item = source(); item['url'] = url
    with pytest.raises(transport.TransferError): transport.ftp_route(item)


def test_global_cutoff_prevents_a_new_connection(tmp_path):
    item = source(); ftp = FTP(b'compressed archive fixture bytes')
    with pytest.raises(transport.TransferDeadline):
        download(item, tmp_path, ftp, deadline=time.monotonic() - 1)
    assert ftp.calls == [] and not paths(tmp_path, item)[1].exists()


def test_guard_is_exclusive_and_never_removes_a_foreign_owner(tmp_path):
    guard = tmp_path / '.transfer-rna.lock'; guard.write_bytes(b'foreign owner')
    with pytest.raises(FileExistsError): transport._owned_guard(guard, b'new owner', lambda: None)
    assert guard.read_bytes() == b'foreign owner'
    guard.unlink()
    def reassign():
        guard.write_bytes(b'different owner')
        raise ValueError('science/source failure')
    with pytest.raises(ValueError, match='science/source failure'):
        transport._owned_guard(guard, b'new owner', reassign)
    assert guard.read_bytes() == b'different owner'


@pytest.fixture
def preserved_case(tmp_path, monkeypatch):
    root = tmp_path / 'scratch/pwa209/horsehair-worm'
    case = root / 'maintenance/rna343-fixture'; case.mkdir(parents=True)
    (root / 'state').mkdir(); (root / 'logs').mkdir(); (root / 'manifests').mkdir()
    item = source(b'12345'); target, partial, sidecar = paths(root, item)
    partial.parent.mkdir(parents=True); partial.write_bytes(b'abcde')
    core.atomic_json(sidecar, {'asset_hash': core.canonical_hash(item), 'etag': 'retained-original'})
    snapshot = transport._snapshot(partial); sidecar_hash = core.digest(sidecar)
    failed = {'status': 'failed', 'host': 'narval1', 'pid': 4136485,
              'error': 'Provider checksum mismatch', 'verified_files': 47}
    core.atomic_json(root / 'state/transfer-rna.json', failed)
    manifest = {'schema_version': 1, 'assets': [item], 'sources': {}}
    manifest['manifest_hash'] = core.canonical_hash(manifest)
    core.atomic_json(root / 'manifests/source_manifest.json', manifest)
    proof = {'host': 'narval1', 'worker_pid': 4136485, 'worker_absent': True,
        'transfer_guard_absent': True, 'failed_transfer': failed,
        'failed_transfer_sha256': core.digest(root / 'state/transfer-rna.json'),
        'provider_metadata': {'matches_pinned_asset': True, 'manifest_modified': False},
        'asset': item, 'asset_sha256': core.canonical_hash(item), 'manifest_hash': manifest['manifest_hash'],
        'partial_snapshot': snapshot, 'sidecar_sha256': sidecar_hash}
    core.atomic_json(case / 'provider-worker-proof.json', proof)
    quarantine = acquire.quarantine_invalid_complete_partial(item, root, 'rna343-fixture')
    identity = {'source_identity': {'sha256': 'primary-source', 'files': {}}, 'config_sha256': 'full-config'}
    ready = {'status': 'ready_for_transfer', 'transfer_launched': False, 'source_definition_unchanged': True,
        'source_sha256': 'primary-source', 'config_sha256': 'full-config', 'old_worker_host': 'narval1',
        'old_worker_pid': 4136485, 'slurm_job_id': '5524039', 'asset_id': item['asset_id'],
        'provider_worker_proof_path': str(case / 'provider-worker-proof.json'),
        'provider_proof_sha256': core.digest(case / 'provider-worker-proof.json'),
        'failed_transfer_sha256': proof['failed_transfer_sha256'], 'manifest_hash': manifest['manifest_hash'],
        'quarantine': quarantine}
    core.atomic_json(case / 'ready.json', ready)
    config = {'resources': {'scratch_budget_files': 1000, 'scratch_budget_bytes': 6000000000000}}
    context = SimpleNamespace(core=core, acquire=acquire, root=root.resolve(), config=config,
        identity=identity, maintenance_identity={'source_identity': {'sha256': 'maintenance-source'}},
        primary=tmp_path / 'primary', maintenance=tmp_path / 'maintenance-release')
    context.primary.mkdir()
    monkeypatch.setattr(transport.socket, 'gethostname', lambda: 'narval1.narval.calcul.quebec')
    def absent(pid, sig): raise ProcessLookupError()
    monkeypatch.setattr(transport.os, 'kill', absent)
    def runner(command, **kwargs):
        if command[0] == 'sacct': return SimpleNamespace(stdout='5524039|COMPLETED|0:0\n5524039.batch|COMPLETED|0:0\n')
        if command[0] == 'ps': return SimpleNamespace(stdout='123 current-launcher.py\n')
        raise AssertionError(command)
    return SimpleNamespace(context=context, root=root, case=case, item=item, ready=ready,
                           proof=proof, failed=failed, runner=runner)


def test_preflight_accepts_only_unchanged_preservation_and_reads_no_large_data(preserved_case, monkeypatch):
    case = preserved_case
    real_digest = core.digest
    def small_only(path):
        assert not str(path).endswith('.fastq.gz.part'), 'Launcher must not rehash preserved GB bytes'
        return real_digest(path)
    monkeypatch.setattr(core, 'digest', small_only)
    checked = transport.preflight(case.context, case.case, runner=case.runner)
    assert checked.asset == case.item and checked.ready['slurm_job_id'] == '5524039'
    assert core.read_json(case.root / 'state/transfer-rna.json') == case.failed
    assert Path(checked.ready['quarantine']['preserved_partial']).read_bytes() == b'abcde'


@pytest.mark.parametrize('fault', ['active', 'host', 'guard', 'state', 'native', 'config', 'quarantine'])
def test_preflight_rejects_active_wrong_host_changed_receipt_or_wrong_native_proof(preserved_case, monkeypatch, fault):
    case = preserved_case; runner = case.runner
    if fault == 'active': monkeypatch.setattr(transport.os, 'kill', lambda *_: None)
    if fault == 'host': monkeypatch.setattr(transport.socket, 'gethostname', lambda: 'narval2')
    if fault == 'guard': (case.root / 'state/.transfer-rna.lock').write_bytes(b'foreign')
    if fault == 'state': core.atomic_json(case.root / 'state/transfer-rna.json', {'status': 'changed'})
    if fault == 'native':
        runner = lambda *_args, **_kwargs: SimpleNamespace(stdout='5529999|COMPLETED|0:0\n5524039|FAILED|1:0\n')
    if fault == 'config':
        case.ready['config_sha256'] = 'different'; core.atomic_json(case.case / 'ready.json', case.ready)
    if fault == 'quarantine': Path(case.ready['quarantine']['preserved_sidecar']).write_text('{}')
    with pytest.raises(transport.TransferError): transport.preflight(case.context, case.case, runner=runner)
    assert not list(case.case.glob('restart-*'))
    assert Path(case.ready['quarantine']['preserved_partial']).read_bytes() == b'abcde'


def test_launcher_preserves_failed_receipt_and_refuses_replay_of_acknowledged_start(preserved_case):
    case = preserved_case; launches = []
    failed_bytes = (case.root / 'state/transfer-rna.json').read_bytes()
    def popen(command, **kwargs):
        launches.append((command, kwargs)); return SimpleNamespace(pid=777)
    result = transport.launch(case.context, case.case, 'rna343-ftp-fixture', runner=case.runner, popen=popen)
    restart = case.case / 'restart-rna343-ftp-fixture'
    assert result['status'] == 'started' and result['pid'] == 777
    assert launches[0][0][0] == 'nohup' and launches[0][1]['start_new_session'] is True
    assert result['maximum_seconds'] == 120 * 3600 and result['automatic_quarantine_retries'] == 0
    assert result['bandwidth_bytes_per_second'] == 64 * 1024**2
    assert (restart / 'old-failed-transfer.json').read_bytes() == failed_bytes
    assert (case.root / 'state/transfer-rna.json').read_bytes() == failed_bytes
    with pytest.raises(transport.TransferError, match='never replay'):
        transport.launch(case.context, case.case, 'rna343-ftp-fixture', runner=case.runner, popen=popen)
    assert len(launches) == 1


def test_launcher_checks_other_rna_workers_before_claiming_a_unique_journal(preserved_case):
    case = preserved_case
    def runner(command, **kwargs):
        if command[0] == 'ps': return SimpleNamespace(stdout='999 python scripts/transfer_sources.py --group rna\n')
        return case.runner(command, **kwargs)
    with pytest.raises(transport.TransferError, match='Another RNA'):
        transport.launch(case.context, case.case, 'rna343-ftp-fixture', runner=runner,
                         popen=lambda *_a, **_k: pytest.fail('Must not launch a competing worker'))
    assert not (case.case / 'restart-rna343-ftp-fixture').exists()


def test_failed_popen_is_journaled_and_never_replayed(preserved_case):
    case = preserved_case
    def unavailable(*_args, **_kwargs): raise OSError('fixture launch failure')
    with pytest.raises(OSError, match='launch failure'):
        transport.launch(case.context, case.case, 'failed-launch', runner=case.runner, popen=unavailable)
    journal = core.read_json(case.case / 'restart-failed-launch/launch.json')
    assert journal['status'] == 'launch_failed' and journal['error_type'] == 'OSError'
    with pytest.raises(transport.TransferError, match='never replay'):
        transport.launch(case.context, case.case, 'failed-launch', runner=case.runner, popen=unavailable)


def setup_worker(case, monkeypatch, *, pilot_ids, ftp_body, extra_assets=()):
    checked = transport.preflight(case.context, case.case, runner=case.runner)
    checked.selected = [case.item, *extra_assets]
    transport.launch(case.context, case.case, 'worker-fixture', runner=case.runner,
                     popen=lambda *_a, **_k: SimpleNamespace(pid=777))
    monkeypatch.setattr(transport, 'preflight', lambda *_a, **_k: checked)
    monkeypatch.setattr(acquire, 'rna_pilot_asset_ids', lambda _manifest: pilot_ids)
    timers = []
    monkeypatch.setattr(transport, 'signal', SimpleNamespace(SIGALRM=1, ITIMER_REAL=0,
        signal=lambda *_a: 'old handler', setitimer=lambda *args: timers.append(args)))
    session = SimpleNamespace(headers={}, trust_env=True, close=lambda: None)
    monkeypatch.setattr(acquire, 'http_session', lambda: session)
    real_download = transport.download_ftp_asset; ftp_calls = []
    def ftp_asset(item, root, helpers, **kwargs):
        ftp_calls.append(item['asset_id'])
        return real_download(item, root, helpers, ftp_factory=lambda: FTP(ftp_body, size=item['bytes']),
                             sleeper=lambda _: None, **kwargs)
    monkeypatch.setattr(transport, 'download_ftp_asset', ftp_asset)
    return SimpleNamespace(checked=checked, session=session, ftp_calls=ftp_calls, timers=timers)


def test_worker_first_case_failure_stops_remaining_roster_and_preserves_failed_history(preserved_case, monkeypatch):
    case = preserved_case
    second = source(b'54321', 'SRR123_2.fastq.gz')
    run = setup_worker(case, monkeypatch, pilot_ids=[case.item['asset_id'], second['asset_id']],
                       ftp_body=b'abcde', extra_assets=[second])
    old = (case.case / 'restart-worker-fixture/old-failed-transfer.json').read_bytes()
    with pytest.raises(acquire.AcquisitionError, match='checksum'):
        transport.worker(case.context, case.case, 'worker-fixture')
    receipt = core.read_json(case.root / 'state/transfer-rna.json')
    assert receipt['status'] == 'failed' and receipt['verified_files'] == 0
    assert receipt['automatic_quarantine_retries'] == 0 and receipt['repair_repeated'] is False
    assert run.ftp_calls == [case.item['asset_id']]
    assert not (case.root / 'state/transfer-rna-pilot.json').exists()
    assert paths(case.root, case.item)[1].read_bytes() == b'abcde'
    assert Path(case.ready['quarantine']['preserved_partial']).read_bytes() == b'abcde'
    assert (case.case / 'restart-worker-fixture/old-failed-transfer.json').read_bytes() == old
    assert not (case.root / 'state/.transfer-rna.lock').exists() and run.timers[-1] == (0, 0)


def test_worker_completes_only_after_whole_roster_and_uses_primary_https_helper_for_references(preserved_case, monkeypatch):
    case = preserved_case
    body = b'count table fixture'
    reference = acquire.asset(source='ant_reference', name='genome.fa.gz',
        url='https://ftp.ncbi.nlm.nih.gov/public/genome.fa.gz', version='frozen-GCF', rights='public NCBI',
        size=len(body), checksum='md5:' + hashlib.md5(body).hexdigest())
    run = setup_worker(case, monkeypatch, pilot_ids=[case.item['asset_id'], reference['asset_id']],
                       ftp_body=b'12345', extra_assets=[reference])
    observations = []
    def https_asset(item, root, **kwargs):
        observations.append(core.read_json(case.root / 'state/transfer-rna.json'))
        assert not (case.root / 'state/transfer-rna-pilot.json').exists()
        assert kwargs['session'] is run.session and kwargs['session'].trust_env is False
        assert kwargs['session'].headers['Cache-Control'] == 'no-cache'
        assert kwargs['attempts'] == 1 and 0 < kwargs['max_seconds'] <= 120 * 3600
        assert kwargs['bandwidth_bytes_per_second'] == 64 * 1024**2
        target = acquire.safe_destination(root, item['relative_path']); target.parent.mkdir(parents=True)
        target.write_bytes(body)
        return acquire.verify_file(target, item)
    monkeypatch.setattr(acquire, 'download_asset', https_asset)
    result = transport.worker(case.context, case.case, 'worker-fixture')
    assert result['status'] == 'complete' and result['verified_files'] == 2
    assert result['source_identity'] == 'primary-source' and result['config_sha256'] == 'full-config'
    assert observations[0]['status'] == 'running' and observations[0]['verified_files'] == 1
    pilot = core.read_json(case.root / 'state/transfer-rna-pilot.json')
    assert pilot['status'] == 'complete' and pilot['asset_ids'] == [case.item['asset_id'], reference['asset_id']]
    assert pilot['verified_files'] == 2 and not (case.root / 'state/.transfer-rna.lock').exists()
    files = core.read_json(case.case / 'restart-worker-fixture/download-receipts.json')['files']
    assert {row['asset_id'] for row in files} == {case.item['asset_id'], reference['asset_id']}


def test_worker_cannot_claim_pilot_completion_for_ids_outside_frozen_selection(preserved_case, monkeypatch):
    case = preserved_case
    run = setup_worker(case, monkeypatch, pilot_ids=['not-in-roster'], ftp_body=b'12345')
    with pytest.raises(transport.TransferError, match='pilot IDs'):
        transport.worker(case.context, case.case, 'worker-fixture')
    assert run.ftp_calls == [] and not (case.root / 'state/transfer-rna-pilot.json').exists()
    assert core.read_json(case.root / 'state/transfer-rna.json')['status'] == 'failed'
