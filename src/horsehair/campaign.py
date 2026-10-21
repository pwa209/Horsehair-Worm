"""Explicit expanded-resource admission, retaining all original study costs."""
from __future__ import annotations

from datetime import datetime, timezone
import math
from pathlib import Path
import re
import runpy

from .core import atomic_json, canonical_hash, digest, exclusive_file_lock, read_json
from .execution_resources import resolve_resource_policy
from .budget import native_accounting
from .pipeline import prospective_capacity, prospective_registered_budget
from .repair import _repair_native_states, _repair_allocation_accounting_runner

REGISTERED_DRIVER = 'maintenance/sixth-repair-support-20261016/controller-primary-repair-sixth-20261016.py'
REGISTERED_DRIVER_SHA256 = '49815eeb509909d5eac2ebde2e4f4a3918cf40cefdc5f643f2bcaffead629eb1'


def _require(value, message):
    if not value:
        raise ValueError(message)


def assess_registered_snapshot(plans, ledgers, observed, budget, members, limits, *, cpus, gpus, hours):
    """Pure finite calculation; lifting total caps never discards reservations."""
    _require(type(cpus) is int and 0 <= cpus <= limits['cpu_ceiling']
             and type(gpus) is int and gpus in (0, 1)
             and not isinstance(hours, bool) and isinstance(hours, (int, float))
             and math.isfinite(hours) and 0 < hours <= limits['max_native_hours'],
             'Invalid finite requested allocation')
    _require(cpus > 0 or gpus == 0, 'GPU requests require a CPU allocation')
    totals = dict(prospective_registered_budget(plans, ledgers, observed, budget))
    capacity = dict(prospective_capacity(plans, ledgers, observed))
    outside = {}
    for ident, lease in budget.get('leases', {}).items():
        logical = str(budget.get('native_raw_aliases', {}).get(ident, ident))
        if lease.get('state') != 'running' or logical in members:
            continue
        _require(re.fullmatch(r'[0-9]+(?:_[0-9]+)?', logical) is not None
                 and logical not in budget.get('native_jobs', {}),
                 'An outside lease needs positive completed native reconciliation')
        bound, cpu, gpu = lease['max_hours'], lease['cpus'], lease['gpus']
        _require(type(cpu) is int and cpu > 0 and type(gpu) is int and gpu in (0, 1)
                 and not isinstance(bound, bool) and isinstance(bound, (int, float))
                 and math.isfinite(bound) and 0 < bound <= limits['max_native_hours'],
                 'Invalid outside native resource lease')
        outside[ident] = lease
        totals['cpu_core_hours'] += cpu * bound
        totals['gpu_hours'] += gpu * bound
        capacity['cpus'] += cpu
        capacity['gpus'] += gpu
    _require(all(isinstance(v, (int, float)) and not isinstance(v, bool)
                 and math.isfinite(v) and v >= 0 for v in
                 (totals['cpu_core_hours'], totals['gpu_hours'], capacity['cpus'], capacity['gpus'])),
             'Nonfinite or negative registered costs/capacity')
    reservation = {'cpu_core_hours': cpus * hours, 'gpu_hours': gpus * hours}
    for key, cap_key in (('cpu_core_hours', 'cpu_core_hours_cap'), ('gpu_hours', 'gpu_hours_cap')):
        cap = limits[cap_key]
        _require(cap is None or totals[key] + reservation[key] <= cap,
                 'Combined registered work exceeds the explicit total resource policy')
    _require(capacity['cpus'] + cpus <= limits['cpu_ceiling']
             and capacity['gpus'] + gpus <= limits['gpu_concurrency'],
             'Combined registered work exceeds the explicit concurrency envelope')
    return {'totals': totals, 'capacity': capacity, 'proposed_reservation': reservation,
            'proposed_capacity': {'cpus': cpus, 'gpus': gpus}, 'external_active_leases': outside}


def combined_admission(root, config, primary_release_id, area, label, *, resource_policy,
                       cpus=0, gpus=0, hours=1):
    """Fresh native accounting for all unique source-bound old and new jobs.

    Caller serializes registration/submission around this check. It is not a
    scheduler reservation or permission to overwrite the canonical science.
    The pinned registry reader supplies the same original source/plan checks.
    """
    root, area = Path(root).resolve(), Path(area).resolve()
    policy = resolve_resource_policy(resource_policy, root=root, config=config)
    _require(area.is_relative_to(root/'maintenance') and not area.is_symlink()
             and area.is_dir() and re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', label),
             'Admission receipt requires an explicit personal maintenance directory/name')
    driver = root/REGISTERED_DRIVER
    _require(driver.is_file() and not driver.is_symlink() and digest(driver) == REGISTERED_DRIVER_SHA256,
             'Original source-bound registry reader changed')
    helpers = runpy.run_path(str(driver), run_name='af_registered_accounting_only')
    plans, ledgers, ids, rosters, members, hashes, bindings, excluded = helpers['all_registered_work'](root, config, primary_release_id)
    before = read_json(root/'state/resource-budget.json')
    accounting_ids = set(ids)
    for ident, lease in before.get('leases', {}).items():
        if lease.get('state') in {'running', 'finished'}:
            logical = str(before.get('native_raw_aliases', {}).get(ident, ident))
            _require(re.fullmatch(r'[0-9]+(?:_[0-9]+)?', logical), 'Invalid native lease identity')
            accounting_ids.add(logical)
    observed = _repair_native_states(sorted(accounting_ids), rosters, owner='pwa209', account=None)
    accounting = native_accounting(root, sorted(accounting_ids),
        runner=_repair_allocation_accounting_runner(rosters, owner='pwa209', account=None))
    with exclusive_file_lock(root/'state/resource-budget.mutex'):
        policy = resolve_resource_policy(policy, root=root, config=config)
        for item in hashes.values():
            _require(canonical_hash(read_json(item['plan_path'])) == item['plan_sha256']
                     and canonical_hash(read_json(item['ledger_path'])) == item['ledger_sha256'],
                     'Registered plan/ledger changed during expanded-resource admission')
        budget = read_json(root/'state/resource-budget.json')
        assessed = assess_registered_snapshot(plans, ledgers, observed, budget, members,
            policy.receipt()['limits'], cpus=cpus, gpus=gpus, hours=hours)
        receipt = {'status': 'admitted', 'checked_utc': datetime.now(timezone.utc).isoformat(),
            'config_sha256': canonical_hash(config), 'resource_policy': policy.receipt(),
            **assessed, 'registered_pair_hashes': hashes, 'unique_allocation_bindings': bindings,
            'excluded_unbound_files': excluded, 'native_aliases': budget.get('native_raw_aliases', {}),
            'native_accounting': accounting, 'native_observed': observed,
            'is_scheduler_reservation': False, 'scientific_gates_changed': False,
            'rule': 'All unique registered native spent and unfinished bounds plus nonduplicated outside leases; explicit CPU/GPU total caps removed, measurement retained'}
        atomic_json(area/(label+'.json'), receipt)
    return receipt
