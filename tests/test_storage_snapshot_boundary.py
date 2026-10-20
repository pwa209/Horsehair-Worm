"""A full native census must avoid size getattr for every regular leaf."""
from pathlib import Path
from types import SimpleNamespace
import os

import pytest

from horsehair import budget, storage_artifacts
from horsehair.core import digest, read_json


def setup_census(tmp_path, monkeypatch):
    monkeypatch.setattr(budget, 'PERSONAL_STUDY_ROOT', tmp_path)
    monkeypatch.setattr(budget.os, 'getuid', lambda: 3164768, raising=False)
    (tmp_path / 'state').mkdir()
    quota = ('Filesystem kbytes quota limit grace files quota limit grace\n'
             '/scratch/pwa209 1024 20000000000 20000000000 - 1000 1000000 1000000 -\n')
    def runner(command, **kwargs):
        assert command == ['lfs', 'quota', '-u', '3164768', '/scratch/pwa209']
        return SimpleNamespace(returncode=0, stdout=quota, stderr='')
    def put(relative, data=b'x'):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path
    def fixture_entries(root, maximum, *, workers=1):
        assert 1 <= workers <= 16
        for directory, dirs, files in os.walk(root, followlinks=False):
            for filename in files:
                path = Path(directory) / filename
                yield path, storage_artifacts.measure_regular_length(path, maximum, root=root)
    # The native iterator has its own descriptor and unknown-d_type tests.
    # This portable seam exercises census accounting and failure publication.
    monkeypatch.setattr(storage_artifacts, 'iter_regular_lengths', fixture_entries, raising=False)
    return runner, put


def test_general_census_never_requests_leaf_size_or_link_getattr(tmp_path, monkeypatch):
    runner, put = setup_census(tmp_path, monkeypatch)
    fasta = put('data/molecular/orthology/of_run/Single_Copy_Orthologue_Sequences/OG0004073.fa')
    fastq = put('raw/ant_rna/SRR123_1.fastq.gz')
    other = put('unknown/artifact.anything')
    ignored = put('links/placeholder')
    measured_sizes = {fasta: 896, fastq: 4_000_000_000, other: 0}
    leaves = {*measured_sizes, ignored}; calls = []
    def measure(path, maximum, *, root):
        assert root == tmp_path and maximum == 6_000_000_000_000
        calls.append(path)
        if path == ignored:
            return {'status': 'symlink'}
        return {'schema_version': 1, 'status': 'measured', 'method': 'owned_regular_length',
                'path': str(path), 'bytes': measured_sizes[path], 'eof_observed': True}
    monkeypatch.setattr(storage_artifacts, 'measure_regular_length', measure)
    original_stat, original_link = Path.stat, Path.is_symlink
    def guarded_stat(path, *args, **kwargs):
        assert path not in leaves, 'General census requested a leaf size from Lustre'
        return original_stat(path, *args, **kwargs)
    def guarded_link(path):
        assert path not in leaves, 'General census requested link getattr before safe classification'
        return original_link(path)
    monkeypatch.setattr(Path, 'stat', guarded_stat)
    monkeypatch.setattr(Path, 'is_symlink', guarded_link)
    result = budget.storage_snapshot(tmp_path, runner=runner, exact_sizes=True)
    assert set(calls) == leaves
    assert result['study_files'] == 3 and result['study_bytes'] == 4_000_000_896
    descriptor = result['exact_entry_measurements']
    proof = read_json(descriptor['receipt_path'])
    assert descriptor['receipt_sha256'] == digest(descriptor['receipt_path'])
    assert descriptor['count'] == proof['count'] == result['study_files']
    assert descriptor['bytes'] == proof['bytes'] == result['study_bytes']
    assert proof['method'] == 'owned_regular_length'
    assert [row['relative_path'] for row in proof['items']] == sorted(
        path.relative_to(tmp_path).as_posix() for path in measured_sizes)
    assert 'artifact_eof_measurements' not in result


@pytest.mark.parametrize('status', ['directory', 'special', 'symlink'])
def test_nonregular_entries_never_become_zero_length_files(tmp_path, monkeypatch, status):
    runner, put = setup_census(tmp_path, monkeypatch)
    put('placeholder')
    monkeypatch.setattr(storage_artifacts, 'measure_regular_length', lambda *a, **k: {'status': status})
    result = budget.storage_snapshot(tmp_path, runner=runner, exact_sizes=True)
    assert result['study_files'] == result['study_bytes'] == 0
    assert read_json(result['exact_entry_measurements']['receipt_path'])['items'] == []


@pytest.mark.parametrize('failure', [OSError('read failed'), ValueError('cap exceeded')])
def test_boundary_failure_preserves_previous_census(tmp_path, monkeypatch, failure):
    runner, put = setup_census(tmp_path, monkeypatch)
    previous = put('state/storage-budget.json', b'{"previous":"retained"}')
    put('unclassified/OG0004073.fa')
    def refuse(*args, **kwargs):
        raise failure
    monkeypatch.setattr(storage_artifacts, 'measure_regular_length', refuse)
    with pytest.raises(type(failure), match=str(failure)):
        budget.storage_snapshot(tmp_path, runner=runner, exact_sizes=True)
    assert previous.read_bytes() == b'{"previous":"retained"}'
    assert not (tmp_path / 'state/storage-entry-sizes.json').exists()


@pytest.mark.parametrize('selection', ['false', 0, 1])
def test_measurement_mode_does_not_accept_truthy_nonbooleans(tmp_path, monkeypatch, selection):
    runner, put = setup_census(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match='boolean'):
        budget.storage_snapshot(tmp_path, runner=runner, exact_sizes=selection)
