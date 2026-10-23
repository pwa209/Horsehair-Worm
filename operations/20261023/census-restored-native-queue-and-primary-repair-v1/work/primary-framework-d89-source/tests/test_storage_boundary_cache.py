"""Complete logical-length censuses retain verified point-in-time boundaries."""
from datetime import datetime, timezone
import copy
import hashlib
import json
from pathlib import Path
import stat

import pytest

from horsehair import pipeline
from horsehair.core import digest


LIMIT = 6_000_000_000_000


def entry_fixture(root, relative, size, inode):
    prefix = min(size, 65536)
    checksum = hashlib.sha256(b'A' * prefix).hexdigest()
    probes = []
    if size >= 65536:
        # Independent synthetic observations for a file of length 65,537.
        assert size == 65537
        probes = [{'offset': 65536, 'bytes': 1}, {'offset': 131072, 'bytes': 0}]
        probes += [{'offset': offset, 'bytes': 0} for offset in
                   (98304, 81920, 73728, 69632, 67584, 66560, 66048, 65792,
                    65664, 65600, 65568, 65552, 65544, 65540, 65538, 65537)]
    if size:
        probes.append({'offset': size - 1, 'bytes': 1})
    probes.append({'offset': size, 'bytes': 0})
    item = {'relative_path': relative, 'schema_version': 1, 'status': 'measured',
            'method': 'owned_regular_length', 'path': str(root / relative), 'bytes': size,
            'max_bytes': LIMIT, 'prefix_cap_bytes': 65536, 'prefix_bytes': prefix,
            'prefix_sha256': checksum, 'eof_observed': True,
            'identity': {'inode': inode, 'uid': 3164768, 'mode': stat.S_IFREG | 0o600,
                         'nlink': 1, 'device_major': 1, 'device_minor': 2, 'ctime_ns': 0},
            'size_proof': {'kind': 'prefix_eof' if size < 65536 else 'pread_first_eof', 'probes': probes}}
    if size < 65536:
        item['content_sha256'] = checksum
    return item


def boundary_cache_fixture(root):
    root = root.resolve()
    state = root / 'state'
    state.mkdir()
    quota = state / 'quota-100000000000000.txt'
    quota.write_text('Generated personal quota evidence\n', encoding='utf8')
    items = [entry_fixture(root, 'data/molecular/orthology-repair-v7-20261013/of_run/Results_Oct13/Single_Copy_Orthologue_Sequences/OG0004073.fa', 3, 40),
             entry_fixture(root, 'raw/ant_rna/SRR123_1.fastq.gz.curl-v3/attempts/0001/segment-000001.stderr.private', 0, 41),
             entry_fixture(root, 'raw/ant_rna/SRR123_1.fastq.gz', 65537, 42)]
    evidence = {'schema_version': 1, 'status': 'measured', 'root': str(root),
                'method': 'owned_regular_length', 'count': 3, 'bytes': 65540, 'items': items}
    sidecar = state / 'storage-entry-sizes.json'
    sidecar.write_text(json.dumps(evidence), encoding='utf8')
    record = {'status': 'measured', 'root': str(root), 'measured_unix': 100000.,
              'captured_utc': datetime.fromtimestamp(100000., timezone.utc).isoformat(),
              'study_bytes': 65540, 'study_files': 3, 'personal_available_bytes': 5000,
              'personal_available_files': 100, 'personal_uid': 3164768,
              'native_source_paths': [str(quota)], 'native_source_hashes': {str(quota): digest(quota)},
              'exact_entry_measurements': {'schema_version': 1, 'method': 'owned_regular_length',
                  'receipt_path': str(sidecar), 'receipt_sha256': digest(sidecar), 'count': 3, 'bytes': 65540}}
    path = state / 'storage-budget.json'
    path.write_text(json.dumps(record), encoding='utf8')
    return path, sidecar, quota, record, evidence


def repin_changed(path, sidecar, record, evidence):
    sidecar.write_text(json.dumps(evidence), encoding='utf8')
    record['exact_entry_measurements']['receipt_sha256'] = digest(sidecar)
    path.write_text(json.dumps(record), encoding='utf8')


def test_general_entry_cache_validates_prefix_and_binary_eof_without_leaf_io(tmp_path):
    path, sidecar, _, record, evidence = boundary_cache_fixture(tmp_path)
    original = path.read_bytes(), sidecar.read_bytes()
    assert all(not (tmp_path / item['relative_path']).exists() for item in evidence['items'])
    result = pipeline._cached_storage_snapshot(tmp_path, now=100060.)
    assert result['exact_entry_measurements'] == record['exact_entry_measurements']
    assert result['receipt_sha256'] == digest(path)
    assert result['refresh_due_unix'] == 121600
    assert (path.read_bytes(), sidecar.read_bytes()) == original


def test_general_entry_cache_does_not_claim_full_hash_for_binary_measurements(tmp_path):
    _, _, _, _, evidence = boundary_cache_fixture(tmp_path)
    item = evidence['items'][-1]
    assert 'content_sha256' not in item and item['size_proof']['kind'] == 'pread_first_eof'
    assert pipeline._cached_storage_snapshot(tmp_path, now=100060.) is not None


@pytest.mark.parametrize('change', ['null', 'old-method', 'schema', 'boolean-count', 'nan-count',
    'float-bytes', 'negative-bytes', 'partial-count', 'partial-bytes', 'float-main-files',
    'float-main-bytes', 'wrong-path', 'outside-root', 'bad-hash', 'missing-owner'])
def test_general_descriptor_requires_complete_bound_integer_census(tmp_path, change):
    path, sidecar, _, record, _ = boundary_cache_fixture(tmp_path)
    descriptor = record['exact_entry_measurements']
    if change == 'null': record['exact_entry_measurements'] = None
    elif change == 'old-method': descriptor['method'] = 'owned_regular_eof'
    elif change == 'schema': descriptor['schema_version'] = True
    elif change == 'boolean-count': descriptor['count'] = True
    elif change == 'nan-count': descriptor['count'] = float('nan')
    elif change == 'float-bytes': descriptor['bytes'] = 65540.
    elif change == 'negative-bytes': descriptor['bytes'] = -1
    elif change == 'partial-count': descriptor['count'] = 2
    elif change == 'partial-bytes': descriptor['bytes'] = 65539
    elif change == 'float-main-files': record['study_files'] = 3.
    elif change == 'float-main-bytes': record['study_bytes'] = 65540.
    elif change == 'wrong-path': descriptor['receipt_path'] = str(sidecar.with_name('other.json'))
    elif change == 'outside-root': descriptor['receipt_path'] = str(tmp_path.parent / sidecar.name)
    elif change == 'bad-hash': descriptor['receipt_sha256'] = '0' * 63
    elif change == 'missing-owner': record.pop('personal_uid')
    path.write_text(json.dumps(record), encoding='utf8')
    assert pipeline._cached_storage_snapshot(tmp_path, now=100060.) is None


@pytest.mark.parametrize('change', ['root', 'method', 'schema', 'pending', 'missing-entry',
    'duplicate-path', 'wrong-sum', 'items-dict'])
def test_pinned_general_sidecar_cannot_change_roster_or_totals(tmp_path, change):
    path, sidecar, _, record, evidence = boundary_cache_fixture(tmp_path)
    if change == 'root': evidence['root'] = str(tmp_path.parent)
    elif change == 'method': evidence['method'] = 'owned_regular_eof'
    elif change == 'schema': evidence['schema_version'] = 2
    elif change == 'pending': evidence['status'] = 'pending'
    elif change == 'missing-entry': evidence['items'].pop()
    elif change == 'duplicate-path': evidence['items'][1] = copy.deepcopy(evidence['items'][0])
    elif change == 'wrong-sum': evidence['bytes'] += 1
    elif change == 'items-dict': evidence['items'] = {}
    repin_changed(path, sidecar, record, evidence)
    assert pipeline._cached_storage_snapshot(tmp_path, now=100060.) is None


@pytest.mark.parametrize('change', ['absolute', 'traversal', 'noncanonical', 'backslash',
    'wrong-row-path', 'missing-eof', 'wrong-owner', 'false-full-hash', 'missing-last-probe',
    'negative-last-byte', 'skipped-binary-probe', 'changed-prefix-hash'])
def test_complete_entry_cache_delegates_exact_boundary_and_identity_validation(tmp_path, change):
    path, sidecar, _, record, evidence = boundary_cache_fixture(tmp_path)
    item = evidence['items'][-1]
    if change == 'absolute': item['relative_path'] = '/outside.fastq.gz'
    elif change == 'traversal': item['relative_path'] = '../outside.fastq.gz'
    elif change == 'noncanonical': item['relative_path'] = 'raw//ant_rna/SRR123_1.fastq.gz'
    elif change == 'backslash': item['relative_path'] = 'raw\\ant_rna\\SRR123_1.fastq.gz'
    elif change == 'wrong-row-path': item['path'] = str(tmp_path / 'other.fastq.gz')
    elif change == 'missing-eof': item['eof_observed'] = False
    elif change == 'wrong-owner': item['identity']['uid'] += 1
    elif change == 'false-full-hash': item['content_sha256'] = item['prefix_sha256']
    elif change == 'missing-last-probe': item['size_proof']['probes'].pop()
    elif change == 'negative-last-byte': item['size_proof']['probes'][-2]['bytes'] = 0
    elif change == 'skipped-binary-probe': item['size_proof']['probes'].pop(2)
    elif change == 'changed-prefix-hash': evidence['items'][0]['content_sha256'] = '0' * 64
    repin_changed(path, sidecar, record, evidence)
    assert pipeline._cached_storage_snapshot(tmp_path, now=100060.) is None


@pytest.mark.parametrize('change', ['changed', 'truncated', 'missing', 'changed-quota', 'expired'])
def test_general_entry_cache_retains_original_integrity_and_freshness_checks(tmp_path, change):
    _, sidecar, quota, _, _ = boundary_cache_fixture(tmp_path)
    now = 100060.
    if change == 'changed': sidecar.write_text('{}')
    elif change == 'truncated': sidecar.write_bytes(sidecar.read_bytes()[:15])
    elif change == 'missing': sidecar.unlink()
    elif change == 'changed-quota': quota.write_text('Changed generated quota evidence\n')
    elif change == 'expired': now = 121600.
    assert pipeline._cached_storage_snapshot(tmp_path, now=now) is None


def test_general_entry_sidecar_symlink_is_refused(tmp_path, monkeypatch):
    _, sidecar, _, _, _ = boundary_cache_fixture(tmp_path)
    original = Path.is_symlink
    monkeypatch.setattr(Path, 'is_symlink', lambda path: path == sidecar or original(path))
    assert pipeline._cached_storage_snapshot(tmp_path, now=100060.) is None


def test_general_entry_sidecar_change_during_validation_is_refused(tmp_path, monkeypatch):
    _, sidecar, _, _, _ = boundary_cache_fixture(tmp_path)
    original = pipeline.digest
    monkeypatch.setattr(pipeline, 'digest', lambda path: '0' * 64 if path == sidecar else original(path))
    assert pipeline._cached_storage_snapshot(tmp_path, now=100060.) is None
