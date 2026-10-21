"""Filesystem verification fixtures; these are not biological evidence."""
from pathlib import Path
import os

import pytest

from horsehair import rna_recovery as recovery
from horsehair.core import digest


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch):
    monkeypatch.setattr(recovery.time, "sleep", lambda seconds: None)


def test_stable_binding_retains_one_strict_read(tmp_path, monkeypatch):
    path = tmp_path / "counts.txt"
    path.write_bytes(b"stable original counts")
    reads = []
    def observed(p):
        reads.append(p)
        return digest(p)
    monkeypatch.setattr(recovery, "digest", observed)
    assert recovery._binding(path) == {"path": str(path.resolve()), "bytes": 22, "sha256": digest(path)}
    assert reads == [path]


def test_one_metadata_change_requires_two_matching_stable_rereads(tmp_path, monkeypatch):
    path = tmp_path / "counts.txt"
    path.write_bytes(b"stable original counts")
    reads = []
    def settle(p):
        value = digest(p)
        reads.append(value)
        if len(reads) == 1:
            stat = p.stat()
            os.utime(p, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
        return value
    monkeypatch.setattr(recovery, "digest", settle)
    assert recovery._binding(path)["sha256"] == digest(path)
    assert len(reads) == 3


def test_content_change_is_never_retried_into_acceptance(tmp_path, monkeypatch):
    path = tmp_path / "counts.txt"
    path.write_bytes(b"old counts")
    reads = []
    def changed(p):
        value = digest(p)
        reads.append(value)
        if len(reads) == 1:
            p.write_bytes(b"new counts")
        return value
    monkeypatch.setattr(recovery, "digest", changed)
    with pytest.raises(recovery.RNARecoveryError, match="content changed"):
        recovery._binding(path)
    assert len(reads) == 2


def test_repeated_metadata_changes_exhaust_a_finite_bound(tmp_path, monkeypatch):
    path = tmp_path / "counts.txt"
    path.write_bytes(b"same counts")
    reads = []
    def unstable(p):
        value = digest(p)
        reads.append(value)
        stat = p.stat()
        os.utime(p, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
        return value
    monkeypatch.setattr(recovery, "digest", unstable)
    with pytest.raises(recovery.RNARecoveryError, match="did not stabilize"):
        recovery._binding(path)
    assert len(reads) == recovery.BINDING_ATTEMPTS


def test_same_bytes_at_a_replacement_inode_are_rejected(tmp_path, monkeypatch):
    path = tmp_path / "counts.txt"
    path.write_bytes(b"same counts")
    def replaced(p):
        value = digest(p)
        p.rename(p.with_suffix(".old"))
        p.write_bytes(b"same counts")
        return value
    monkeypatch.setattr(recovery, "digest", replaced)
    with pytest.raises(recovery.RNARecoveryError, match="replaced"):
        recovery._binding(path)


def test_stable_rereads_must_have_the_same_metadata_identity(tmp_path, monkeypatch):
    path = tmp_path / "counts.txt"
    path.write_bytes(b"same counts")
    calls = []
    def unsettled_between_passes(p):
        value = digest(p)
        calls.append(value)
        if len(calls) in (1, 3):
            stat = p.stat()
            os.utime(p, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
        return value
    monkeypatch.setattr(recovery, "digest", unsettled_between_passes)
    with pytest.raises(recovery.RNARecoveryError, match="did not stabilize"):
        recovery._binding(path)
    assert len(calls) == recovery.BINDING_ATTEMPTS
