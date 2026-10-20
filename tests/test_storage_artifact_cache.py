"""Cached storage evidence must retain its exact bounded artifact measurements."""
from datetime import datetime, timezone
import copy
import hashlib
import json
from pathlib import Path
import stat

import pytest

from horsehair import pipeline
from horsehair.core import digest


def artifact_cache_fixture(root):
    root = root.resolve()
    state = root / 'state'
    state.mkdir()
    quota = state / 'quota-100000000000000.txt'
    quota.write_text('Generated personal quota evidence\n', encoding='utf8')
    items = []
    for index, payload in enumerate((b'abc', b'')):
        relative = f'tests/qualification-123/test_archive/generated-{index}.txt'
        items.append({'relative_path': relative, 'path': str(root / relative),
                      'status': 'measured', 'method': 'owned_regular_eof',
                      'bytes': len(payload), 'read_cap_bytes': 1024 * 1024,
                      'sha256': hashlib.sha256(payload).hexdigest(), 'eof_observed': True,
                      'identity': {'inode': 40 + index, 'uid': 3164768, 'mode': stat.S_IFREG | 0o600,
                                   'nlink': 1, 'device_major': 1, 'device_minor': 2, 'ctime_ns': 0}})
    evidence = {'schema_version': 1, 'status': 'measured', 'root': str(root),
                'method': 'owned_regular_eof', 'count': len(items), 'bytes': 3, 'items': items}
    sidecar = state / 'storage-artifacts.json'
    sidecar.write_text(json.dumps(evidence), encoding='utf8')
    record = {'status': 'measured', 'root': str(root), 'measured_unix': 100000.,
              'captured_utc': datetime.fromtimestamp(100000., timezone.utc).isoformat(),
              'study_bytes': 1000, 'study_files': 10, 'personal_available_bytes': 5000,
              'personal_available_files': 100, 'personal_uid': 3164768,
              'native_source_paths': [str(quota)], 'native_source_hashes': {str(quota): digest(quota)},
              'artifact_eof_measurements': {'schema_version': 1, 'method': 'owned_regular_eof',
                  'receipt_path': str(sidecar), 'receipt_sha256': digest(sidecar), 'count': 2, 'bytes': 3}}
    path = state / 'storage-budget.json'
    path.write_text(json.dumps(record), encoding='utf8')
    return path, sidecar, quota, record, evidence


def write_changed(path, sidecar, record, evidence):
    """Repin deliberately malformed fixtures so validation must inspect them."""
    sidecar.write_text(json.dumps(evidence), encoding='utf8')
    record['artifact_eof_measurements']['receipt_sha256'] = digest(sidecar)
    path.write_text(json.dumps(record), encoding='utf8')


def test_valid_artifact_cache_keeps_original_deadline_and_never_stats_payloads(tmp_path):
    path, sidecar, quota, record, evidence = artifact_cache_fixture(tmp_path)
    original = path.read_bytes(), sidecar.read_bytes()
    # No artifact leaf exists: cache validation uses lexical selectors and the
    # already recorded EOF evidence, never new stat/read calls on old payloads.
    assert not (tmp_path / evidence['items'][0]['relative_path']).exists()
    result = pipeline._cached_storage_snapshot(tmp_path, now=100060.)
    assert result['artifact_eof_measurements'] == record['artifact_eof_measurements']
    assert result['receipt_sha256'] == digest(path)
    assert result['refresh_due_unix'] == 121600 and result['age_seconds'] == 60
    assert result['native_source_hashes'] == {str(quota): digest(quota)}
    assert (path.read_bytes(), sidecar.read_bytes()) == original


def test_legacy_storage_cache_needs_no_artifact_sidecar(tmp_path):
    path, sidecar, _, record, _ = artifact_cache_fixture(tmp_path)
    record.pop('artifact_eof_measurements')
    path.write_text(json.dumps(record), encoding='utf8')
    sidecar.unlink()
    result = pipeline._cached_storage_snapshot(tmp_path, now=100060.)
    assert result['receipt_sha256'] == digest(path)
    assert 'artifact_eof_measurements' not in result


def test_empty_selected_artifacts_have_an_exact_zero_byte_total(tmp_path):
    path, sidecar, _, record, evidence = artifact_cache_fixture(tmp_path)
    for item in evidence['items']:
        item.update(bytes=0, sha256=hashlib.sha256(b'').hexdigest())
    evidence['bytes'] = record['artifact_eof_measurements']['bytes'] = 0
    write_changed(path, sidecar, record, evidence)
    result = pipeline._cached_storage_snapshot(tmp_path, now=100060.)
    assert result['artifact_eof_measurements']['bytes'] == 0
    assert result['artifact_eof_measurements']['count'] == 2


@pytest.mark.parametrize('change', ['null', 'method', 'schema', 'boolean-schema', 'zero-count',
    'fractional-count', 'nan-count', 'negative-bytes', 'infinite-bytes', 'boolean-bytes',
    'too-many-files', 'too-many-bytes', 'wrong-path', 'outside-root', 'bad-hash', 'missing-owner'])
def test_artifact_descriptor_cannot_substitute_unbound_or_impossible_totals(tmp_path, change):
    path, sidecar, _, record, evidence = artifact_cache_fixture(tmp_path)
    descriptor = record['artifact_eof_measurements']
    changes = {'method': ('method', 'lazy_size'), 'schema': ('schema_version', 2),
               'boolean-schema': ('schema_version', True), 'zero-count': ('count', 0),
               'fractional-count': ('count', 1.5), 'nan-count': ('count', float('nan')),
               'negative-bytes': ('bytes', -1), 'infinite-bytes': ('bytes', float('inf')),
               'boolean-bytes': ('bytes', True), 'too-many-files': ('count', 11),
               'too-many-bytes': ('bytes', 1001),
               'wrong-path': ('receipt_path', str(sidecar.with_name('other.json'))),
               'outside-root': ('receipt_path', str(tmp_path.parent / 'storage-artifacts.json')),
               'bad-hash': ('receipt_sha256', 'not-a-sha256')}
    if change == 'null':
        record['artifact_eof_measurements'] = None
    elif change == 'missing-owner':
        record.pop('personal_uid')
    else:
        key, value = changes[change]
        descriptor[key] = value
    path.write_text(json.dumps(record), encoding='utf8')
    assert pipeline._cached_storage_snapshot(tmp_path, now=100060.) is None


@pytest.mark.parametrize('change', ['schema', 'root', 'method', 'status', 'count', 'boolean-count',
    'bytes', 'non-list-items', 'missing-item', 'duplicate-path', 'sum', 'non-dict-item'])
def test_pinned_sidecar_must_describe_the_same_exact_measured_roster(tmp_path, change):
    path, sidecar, _, record, evidence = artifact_cache_fixture(tmp_path)
    if change == 'schema': evidence['schema_version'] = True
    elif change == 'root': evidence['root'] = str(tmp_path.parent)
    elif change == 'method': evidence['method'] = 'lazy_size'
    elif change == 'status': evidence['status'] = 'pending'
    elif change == 'count': evidence['count'] = 3
    elif change == 'boolean-count': evidence['count'] = True
    elif change == 'bytes': evidence['bytes'] = float('nan')
    elif change == 'non-list-items': evidence['items'] = {}
    elif change == 'missing-item': evidence['items'].pop()
    elif change == 'duplicate-path': evidence['items'][1] = copy.deepcopy(evidence['items'][0])
    elif change == 'sum': evidence['items'][0]['bytes'] = 2
    elif change == 'non-dict-item': evidence['items'][0] = None
    write_changed(path, sidecar, record, evidence)
    assert pipeline._cached_storage_snapshot(tmp_path, now=100060.) is None


@pytest.mark.parametrize('change', ['outside', 'parent', 'noncanonical', 'backslash', 'raw-input',
    'wrong-path', 'not-measured', 'wrong-method', 'missing-eof', 'oversize', 'wrong-cap',
    'bad-sha', 'directory', 'symlink', 'wrong-owner', 'zero-inode', 'zero-links',
    'negative-device', 'boolean-identity', 'incomplete-identity'])
def test_item_evidence_is_limited_to_owned_selected_regular_files(tmp_path, change):
    path, sidecar, _, record, evidence = artifact_cache_fixture(tmp_path)
    item = evidence['items'][0]
    if change == 'outside': item['relative_path'] = '/outside.txt'
    elif change == 'parent': item['relative_path'] = 'tests/qualification-123/../../outside.txt'
    elif change == 'noncanonical': item['relative_path'] = 'tests/qualification-123//generated.txt'
    elif change == 'backslash': item['relative_path'] = 'tests\\qualification-123\\generated.txt'
    elif change == 'raw-input':
        item['relative_path'] = 'raw/ant_rna/SRR22989345_1.fastq.gz'
        item['path'] = str(tmp_path / item['relative_path'])
    elif change == 'wrong-path': item['path'] = str(tmp_path / 'other.txt')
    elif change == 'not-measured': item['status'] = 'symlink'
    elif change == 'wrong-method': item['method'] = 'cached_stat'
    elif change == 'missing-eof': item['eof_observed'] = 1
    elif change == 'oversize': item['bytes'] = 1024 * 1024 + 1
    elif change == 'wrong-cap': item['read_cap_bytes'] = 64 * 1024 * 1024
    elif change == 'bad-sha': item['sha256'] = 'G' * 64
    elif change == 'directory': item['identity']['mode'] = stat.S_IFDIR | 0o700
    elif change == 'symlink': item['identity']['mode'] = stat.S_IFLNK | 0o777
    elif change == 'wrong-owner': item['identity']['uid'] += 1
    elif change == 'zero-inode': item['identity']['inode'] = 0
    elif change == 'zero-links': item['identity']['nlink'] = 0
    elif change == 'negative-device': item['identity']['device_major'] = -1
    elif change == 'boolean-identity': item['identity']['ctime_ns'] = False
    elif change == 'incomplete-identity': item['identity'].pop('nlink')
    write_changed(path, sidecar, record, evidence)
    assert pipeline._cached_storage_snapshot(tmp_path, now=100060.) is None


@pytest.mark.parametrize('change', ['changed', 'truncated', 'missing', 'changed-quota', 'expired'])
def test_artifact_cache_keeps_native_and_sidecar_integrity_checks(tmp_path, change):
    _, sidecar, quota, _, _ = artifact_cache_fixture(tmp_path)
    now = 100060.
    if change == 'changed': sidecar.write_text('{}')
    elif change == 'truncated': sidecar.write_bytes(sidecar.read_bytes()[:15])
    elif change == 'missing': sidecar.unlink()
    elif change == 'changed-quota': quota.write_text('Changed generated quota evidence\n')
    elif change == 'expired': now = 121600.
    assert pipeline._cached_storage_snapshot(tmp_path, now=now) is None


def test_artifact_sidecar_symlink_is_rejected_before_read(tmp_path, monkeypatch):
    _, sidecar, _, _, _ = artifact_cache_fixture(tmp_path)
    original = Path.is_symlink
    monkeypatch.setattr(Path, 'is_symlink', lambda path: path == sidecar or original(path))
    assert pipeline._cached_storage_snapshot(tmp_path, now=100060.) is None


def test_artifact_sidecar_changed_during_validation_invalidates_cache(tmp_path, monkeypatch):
    _, sidecar, _, _, _ = artifact_cache_fixture(tmp_path)
    original = pipeline.digest
    monkeypatch.setattr(pipeline, 'digest', lambda path: '0' * 64 if path == sidecar else original(path))
    assert pipeline._cached_storage_snapshot(tmp_path, now=100060.) is None


def test_artifact_sidecar_parent_cannot_redirect_evidence_outside_root(tmp_path, monkeypatch):
    _, sidecar, _, _, _ = artifact_cache_fixture(tmp_path)
    original = Path.resolve
    monkeypatch.setattr(Path, 'resolve', lambda path, *args, **kwargs:
        tmp_path.parent / 'external.json' if path == sidecar else original(path, *args, **kwargs))
    assert pipeline._cached_storage_snapshot(tmp_path, now=100060.) is None
