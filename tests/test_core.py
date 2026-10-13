import pytest
import pandas as pd
from horsehair.core import assert_disjoint, verify_unique_units, canonical_hash, atomic_json, read_json, stage_receipt


def test_independent_animals_not_time_bins():
    with pytest.raises(ValueError, match='leakage'):
        assert_disjoint(pd.DataFrame({'animal':['a','b']}), pd.DataFrame({'animal':['a']}), 'animal')


def test_duplicate_source_key_rejected():
    with pytest.raises(ValueError, match='duplicate'):
        verify_unique_units(pd.DataFrame({'animal':['a','a'], 'time':[1,1]}), ['animal','time'])


def test_config_hash_order_invariant():
    assert canonical_hash({'a':1,'b':2}) == canonical_hash({'b':2,'a':1})


def test_receipts_do_not_recompute_or_silently_change_config(tmp_path):
    with stage_receipt(tmp_path, {'seed':1}, 'fit'):
        atomic_json(tmp_path/'metrics.json', {'evidence_class':'synthetic'})
    assert read_json(tmp_path/'completed.json')['status'] == 'complete'
    with pytest.raises(FileExistsError):
        with stage_receipt(tmp_path, {'seed':1}, 'fit'):
            pass
    with pytest.raises(ValueError):
        with stage_receipt(tmp_path, {'seed':2}, 'fit'):
            pass

def test_atomic_receipt_retries_only_prepared_windows_replacement(tmp_path, monkeypatch):
    from horsehair import core
    path = tmp_path/'receipt.json'
    core.atomic_json(path, {'state':'submitting'})
    original = core.os.replace
    calls=[]
    def replace(source, destination):
        calls.append((source,destination))
        if len(calls)==1:
            raise PermissionError('temporary Windows reader')
        return original(source,destination)
    monkeypatch.setattr(core.sys,'platform','win32')
    monkeypatch.setattr(core.os,'replace',replace)
    core.atomic_json(path, {'state':'submitted','job_id':'123'})
    assert core.read_json(path)=={'state':'submitted','job_id':'123'}
    assert len(calls)==2 and calls[0]==calls[1]


def test_atomic_receipt_keeps_old_and_prepared_files_after_persistent_denial(tmp_path, monkeypatch):
    from horsehair import core
    import pytest
    path = tmp_path/'receipt.json'
    core.atomic_json(path, {'state':'submitting'})
    monkeypatch.setattr(core.sys,'platform','win32')
    clock=iter([0.,3.])
    monkeypatch.setattr(core.time,'monotonic',lambda:next(clock))
    def denied(*args):
        raise PermissionError('persistent reader or ACL')
    monkeypatch.setattr(core.os,'replace',denied)
    with pytest.raises(PermissionError):
        core.atomic_json(path, {'state':'submitted','job_id':'123'})
    assert core.read_json(path)=={'state':'submitting'}
    assert core.read_json(path.with_name('receipt.json.partial'))=={'state':'submitted','job_id':'123'}


def test_receipt_guard_contention_reports_holder_and_releases_without_unlink(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from horsehair.core import exclusive_file_lock, ReceiptLockTimeout
    guard = tmp_path / 'budget.mutex'
    acquired, release = Event(), Event()
    def holder():
        with exclusive_file_lock(guard):
            acquired.set()
            assert release.wait(5)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(holder)
        assert acquired.wait(5)
        inode = guard.stat().st_ino
        try:
            with pytest.raises(ReceiptLockTimeout) as raised:
                with exclusive_file_lock(guard, timeout=.1):
                    pytest.fail('A second writer acquired a live guard')
            diagnostics = raised.value.diagnostics
            assert diagnostics['guard_path'] == str(guard.resolve())
            assert diagnostics['waited_seconds'] >= .09
            assert diagnostics['last_known_holder']['state'] == 'held'
            assert diagnostics['owner_metadata_is_authoritative'] is False
        finally:
            release.set()
        future.result()
    with exclusive_file_lock(guard, timeout=.1):
        assert guard.stat().st_ino == inode
    assert read_json(guard.with_name(guard.name + '.owner.json'))['state'] == 'released'


def test_receipt_owner_diagnostic_failure_cannot_break_kernel_release(tmp_path, monkeypatch):
    from horsehair import core
    def failed_metadata(*args, **kwargs):
        raise OSError('diagnostic write unavailable')
    monkeypatch.setattr(core, 'atomic_json', failed_metadata)
    guard = tmp_path / 'guard.mutex'
    with pytest.raises(ValueError, match='transaction failed'):
        with core.exclusive_file_lock(guard):
            raise ValueError('transaction failed')
    with core.exclusive_file_lock(guard, timeout=0):
        assert guard.is_file()
