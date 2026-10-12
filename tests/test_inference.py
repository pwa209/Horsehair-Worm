import numpy as np
import pandas as pd
import pytest
from horsehair.inference import holm, benjamini_hochberg, experiment_weighted_contrast, equivalence_tost


def test_holm_primary_three_tests():
    np.testing.assert_allclose(holm([.01,.04,.03]), [.03,.06,.06])


def test_bh_known_results():
    np.testing.assert_allclose(benjamini_hochberg([.01,.04,.03]), [.03,.04,.04])


def test_more_time_bins_do_not_change_experiment_weight():
    frame = pd.DataFrame({'experiment_id':['a','b','c'], 'animal_id':['1','2','3'], 'difference':[1.,-1.,0.]})
    original = experiment_weighted_contrast(frame, replicates=100, seed=1)
    repeated = pd.concat([frame, frame.loc[frame.experiment_id=='a']]*5)
    doubled = experiment_weighted_contrast(repeated, replicates=100, seed=1)
    assert original == doubled


def test_nonsignificance_does_not_establish_equivalence():
    result=equivalence_tost([-1., 0., 1.], margin=.2)
    assert not result['equivalent']

