"""Inference at the independent sampling unit with explicit multiplicity families."""
from __future__ import annotations
import numpy as np
from scipy import stats


def holm(pvalues):
    p = np.asarray(pvalues, dtype=float)
    if p.ndim != 1 or not np.all(np.isfinite(p)) or np.any((p < 0) | (p > 1)):
        raise ValueError('Invalid P values')
    order = np.argsort(p)
    out = np.empty(len(p))
    out[order] = np.minimum(1, np.maximum.accumulate(p[order] * (len(p) - np.arange(len(p)))))
    return out


def benjamini_hochberg(pvalues):
    p = np.asarray(pvalues, dtype=float)
    if not np.all(np.isfinite(p)) or np.any((p < 0) | (p > 1)):
        raise ValueError('Invalid P values')
    order = np.argsort(p)
    corrected = np.minimum.accumulate((p[order] * len(p) / np.arange(1,len(p)+1))[::-1])[::-1]
    out = np.empty(len(p))
    out[order] = np.minimum(1, corrected)
    return out


def experiment_weighted_contrast(frame, value='difference', experiment='experiment_id', animal='animal_id', replicates=10000, seed=20261012):
    """Equal experiment weight; resample experiments, then animals within each."""
    if frame[[value, experiment, animal]].isna().any().any():
        raise ValueError('Missing contrast or independent-unit key')
    units = frame.groupby([experiment, animal], observed=True)[value].mean().reset_index()
    groups = {str(k):g[value].to_numpy(float) for k,g in units.groupby(experiment, observed=True)}
    if len(groups) < 3:
        raise ValueError('Fewer than three experiments: population uncertainty unavailable')
    rng = np.random.default_rng(seed)
    keys = list(groups)
    estimate = float(np.mean([np.mean(v) for v in groups.values()]))
    draws = np.empty(replicates)
    for j in range(replicates):
        selected = rng.choice(keys, len(keys), replace=True)
        draws[j] = np.mean([rng.choice(groups[k], len(groups[k]), replace=True).mean() for k in selected])
    means = np.array([v.mean() for v in groups.values()])
    if len(means) <= 16:
        from itertools import product
        null = np.array([np.mean(means * np.array(s)) for s in product([-1,1], repeat=len(means))])
        p = float(np.mean(np.abs(null) >= abs(estimate) - 1e-12))
        p_method = 'exact experiment sign-flip under symmetric null'
    else:
        null = np.mean(rng.choice([-1,1], (replicates,len(means))) * means, axis=1)
        p = float((1 + np.sum(np.abs(null) >= abs(estimate))) / (replicates+1))
        p_method = 'Monte Carlo experiment sign-flip under symmetric null'
    return {'estimate':estimate, 'ci95':np.quantile(draws,[.025,.975]).tolist(), 'pvalue':p,
            'pvalue_method':p_method, 'experiments':len(groups), 'animals':len(units),
            'independent_unit':'experiment with nested animals', 'bootstrap_replicates':replicates}


def equivalence_tost(differences, margin=0.2, alpha=.05):
    x = np.asarray(differences, dtype=float)
    if len(x)<3 or margin <= 0 or not np.isfinite(x).all():
        raise ValueError('Equivalence requires finite independent contrasts and positive margin')
    mean = x.mean()
    se = stats.sem(x)
    if se <= 0:
        raise ValueError('Degenerate uncertainty cannot establish equivalence')
    p_lower = stats.t.sf((mean+margin)/se, len(x)-1)
    p_upper = stats.t.cdf((mean-margin)/se, len(x)-1)
    q = stats.t.ppf(1-alpha, len(x)-1) * se
    return {'mean':float(mean), 'ci90':[float(mean-q),float(mean+q)], 'margin':margin,
            'pvalue':float(max(p_lower,p_upper)), 'equivalent':bool(max(p_lower,p_upper)<alpha)}

