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

