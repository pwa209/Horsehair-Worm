import copy
from contextlib import ExitStack, contextmanager
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from horsehair import budget, execution_resources as resources
from horsehair.core import atomic_json, canonical_hash, digest, read_json


@pytest.fixture
def policy_fixture(tmp_path, monkeypatch):
    config = read_json(Path(__file__).resolve().parents[1] / 'configs/study.json')
    assert canonical_hash(config) == resources.BASE_CONFIG_SHA256
    monkeypatch.setattr(resources, 'PERSONAL_STUDY_ROOT', tmp_path)
    monkeypatch.setattr(resources, 'source_identity', lambda _: {'sha256': 'a' * 64})
    monkeypatch.delenv('SLURM_JOB_END_TIME', raising=False)
    path = tmp_path / 'maintenance/execution-resource-policies/approved-20261021/policy.json'
    value = {'schema_version': 1, 'kind': 'approved-operational-resource-policy',
        'policy_id': 'approved-20261021', 'study_root': str(tmp_path.resolve()),
        'base_config_sha256': resources.BASE_CONFIG_SHA256,
        'primary_scientific_source_sha256': resources.PRIMARY_SCIENTIFIC_SOURCE_SHA256,
        'execution_source_sha256': 'a' * 64, 'limits': dict(resources.LIMITS),
        'approval': {'source': 'current_user', 'statement': 'Fixture of explicit current-user authorization to lift cumulative CPU/GPU restrictions.',
            'evidence_reference': 'synthetic current-user authority fixture; not a real user signature',
            'approved_at_utc': '2026-10-21T17:00:00+00:00',
            'authorizes_cpu_gpu_resource_amendment': True,
            'identity_verification': 'instruction_provenance_only'}, 'scientific_gates_changed': False}
    atomic_json(path, value)
    return tmp_path, config, path, value


def load(fixture):
    root, config, path, _ = fixture
    return resources.load_resource_policy(path, digest(path), root=root, config=config)


def test_policy_is_explicit_source_bound_and_keeps_frozen_configuration(policy_fixture):
    root, config, path, _ = policy_fixture
    before = canonical_hash(config)
    verified = load(policy_fixture)
    assert verified.path == path and verified.sha256 == digest(path)
    assert verified.receipt()['limits']['cpu_core_hours_cap'] is None
    assert verified.receipt()['limits']['gpu_hours_cap'] is None
    assert verified.receipt()['limits']['cpu_ceiling'] == 256
    assert verified.receipt()['limits']['gpu_concurrency'] == 4
    assert verified.receipt()['scientific_gates_changed'] is False
    assert 'not cryptographic' in verified.receipt()['authority_basis']
    assert canonical_hash(config) == before == resources.BASE_CONFIG_SHA256
    assert not (root / 'state/resource-budget.json').exists()


@pytest.mark.parametrize('field,value', [
    ('cpu_ceiling', True), ('cpu_ceiling', 0), ('cpu_ceiling', -1), ('cpu_ceiling', 257),
    ('cpu_ceiling', 256.0), ('gpu_concurrency', 0), ('gpu_concurrency', True),
    ('gpu_concurrency', float('inf')), ('cpu_core_hours_cap', 20000),
    ('gpu_hours_cap', 400), ('max_native_hours', 169),
    ('scratch_budget_bytes', 6_000_000_000_001), ('scratch_budget_files', 250001),
    ('scratch_reserve_bytes', 0),
])
def test_policy_rejects_unapproved_limits_and_nonfinite_counts(policy_fixture, field, value):
    root, config, path, record = policy_fixture
    record['limits'][field] = value
    path.write_text(json.dumps(record))
    with pytest.raises(ValueError):
        resources.load_resource_policy(path, digest(path), root=root, config=config)


@pytest.mark.parametrize('mutation', [
    lambda r: r.update(schema_version=True),
    lambda r: r.update(primary_scientific_source_sha256='b' * 64),
    lambda r: r.update(base_config_sha256='b' * 64),
    lambda r: r.update(execution_source_sha256='b' * 64),
    lambda r: r.update(study_root='/scratch/shared/horsehair-worm'),
    lambda r: r.update(scientific_gates_changed=True),
    lambda r: r['approval'].update(source='agent_inference'),
    lambda r: r['approval'].update(statement=' '),
    lambda r: r['approval'].update(evidence_reference=''),
    lambda r: r['approval'].update(authorizes_cpu_gpu_resource_amendment=False),
    lambda r: r['approval'].update(identity_verification='cryptographically_verified'),
    lambda r: r['approval'].update(approved_at_utc='2026-10-21T17:00:00'),
    lambda r: r.pop('approval'),
    lambda r: r.update(global_override=True),
])
def test_policy_rejects_missing_approval_foreign_identity_or_extra_authority(policy_fixture, mutation):
    root, config, path, record = policy_fixture
    mutation(record)
    path.write_text(json.dumps(record))
    with pytest.raises(ValueError):
        resources.load_resource_policy(path, digest(path), root=root, config=config)


def test_changed_config_and_changed_policy_fail_closed(policy_fixture):
    root, config, path, record = policy_fixture
    old_sha = digest(path)
    altered = copy.deepcopy(config)
    altered['agents']['final_seeds'].pop()
    with pytest.raises(ValueError, match='configuration mismatch'):
        resources.load_resource_policy(path, old_sha, root=root, config=altered)
    path.write_bytes(path.read_bytes() + b' ')
    with pytest.raises(ValueError, match='bytes changed'):
        resources.load_resource_policy(path, old_sha, root=root, config=config)


def test_policy_rejects_duplicate_keys_oversize_and_nan(policy_fixture):
    root, config, path, _ = policy_fixture
    for raw in [b'{"schema_version":1,"schema_version":1}', b' ' * 65537,
                b'{"schema_version": NaN}']:
        path.write_bytes(raw)
        with pytest.raises(ValueError):
            resources.load_resource_policy(path, digest(path), root=root, config=config)


@pytest.mark.parametrize('where', ['other-root', 'maintenance/policy.json',
    'maintenance/execution-resource-policies/nested/other/policy.json',
    'maintenance/execution-resource-policies/approved-20261021/another.json'])
def test_policy_rejects_unapproved_namespaces(policy_fixture, where):
    root, config, path, _ = policy_fixture
    other = root / where
    other.parent.mkdir(parents=True, exist_ok=True)
    other.write_bytes(path.read_bytes())
    with pytest.raises(ValueError, match='namespace'):
        resources.load_resource_policy(other, digest(other), root=root, config=config)


def test_unpinned_and_foreign_root_policies_are_refused(policy_fixture):
    root, config, path, _ = policy_fixture
    for sha in [None, True, '', 'A' * 64, '0' * 64]:
        with pytest.raises(ValueError):
            resources.load_resource_policy(path, sha, root=root, config=config)
    with pytest.raises(ValueError, match='exact personal'):
        resources.load_resource_policy(path, digest(path), root=root / 'other', config=config)
    for value in [None, {}, str(path), (path,), [path, digest(path)]]:
        with pytest.raises(ValueError, match='explicit'):
            resources.resolve_resource_policy(value, root=root, config=config)


def test_verified_locator_is_rechecked_instead_of_trusting_mutable_object(policy_fixture, monkeypatch):
    root, config, path, _ = policy_fixture
    locator = load(policy_fixture)
    locator.approval['statement'] = 'A caller cannot replace the on-disk authorization.'
    rechecked = resources.resolve_resource_policy(locator, root=root, config=config)
    assert rechecked.approval['statement'] != locator.approval['statement']
    monkeypatch.setattr(resources, 'source_identity', lambda _: {'sha256': 'b' * 64})
    with pytest.raises(ValueError, match='execution source mismatch'):
        resources.resolve_resource_policy(locator, root=root, config=config)


@pytest.mark.parametrize('redirected', ['file', 'policy-directory', 'namespace', 'maintenance'])
def test_policy_rejects_redirected_file_or_any_maintenance_parent(policy_fixture, monkeypatch, redirected):
    root, config, path, _ = policy_fixture
    selected = {'file': path, 'policy-directory': path.parent,
                'namespace': path.parent.parent, 'maintenance': path.parent.parent.parent}[redirected]
    original = Path.is_symlink
    monkeypatch.setattr(Path, 'is_symlink', lambda p: p == selected or original(p))
    with pytest.raises(ValueError, match='redirected parents'):
        resources.load_resource_policy(path, digest(path), root=root, config=config)


@pytest.mark.parametrize('spent_cpu,spent_gpu,match', [(20001, 0, 'CPU-hour'), (0, 401, 'GPU-hour')])
def test_legacy_caps_still_apply_without_explicit_policy(policy_fixture, spent_cpu, spent_gpu, match):
    root, config, _, _ = policy_fixture
    atomic_json(root / 'state/resource-budget.json', {'spent_cpu_core_hours': spent_cpu,
        'spent_gpu_hours': spent_gpu, 'leases': {}})
    with pytest.raises(ValueError, match=match):
        with budget.resource_lease(root, config, cpus=1, gpus=0, max_hours=1, job_id='10'):
            pytest.fail('Legacy limits must remain active')


def test_environment_cannot_enable_policy_and_legacy_gpu_ceiling_is_one(policy_fixture, monkeypatch):
    root, config, path, _ = policy_fixture
    monkeypatch.setenv('HHW_RESOURCE_POLICY', str(path))
    monkeypatch.setenv('HHW_CPU_CEILING', '256')
    with budget.resource_lease(root, config, cpus=1, gpus=1, max_hours=1, job_id='10'):
        with pytest.raises(ValueError, match='concurrency'):
            with budget.resource_lease(root, config, cpus=1, gpus=1, max_hours=1, job_id='11'):
                pytest.fail('No implicit GPU policy selection')
    with pytest.raises(ValueError, match='concurrency'):
        with budget.resource_lease(root, config, cpus=65, gpus=0, max_hours=1, job_id='12'):
            pytest.fail('No implicit CPU policy selection')


def test_explicit_policy_removes_only_cumulative_caps_and_preserves_receipts(policy_fixture):
    root, config, path, _ = policy_fixture
    historical = {'original': {'source_sha256': 'c' * 64, 'cpu_core_hours': 20001}}
    atomic_json(root / 'state/resource-budget.json', {'spent_cpu_core_hours': 20001.,
        'spent_gpu_hours': 401., 'leases': {}, 'native_jobs': historical})
    pair = (path, digest(path))
    with budget.resource_lease(root, config, cpus=100, gpus=1, max_hours=168, job_id='10', resource_policy=pair) as receipt:
        assert receipt['resource_policy']['sha256'] == pair[1]
        running = read_json(root / 'state/resource-budget.json')
        assert running['leases']['10']['resource_policy']['limits']['cpu_core_hours_cap'] is None
        assert running['spent_cpu_core_hours'] == 20001 and running['native_jobs'] == historical
    final = read_json(root / 'state/resource-budget.json')
    journal = read_json(root / 'state/resource-lease-finalizations/10.json')
    assert final['spent_cpu_core_hours'] >= 20001 and final['spent_gpu_hours'] >= 401
    assert final['native_jobs'] == historical
    assert final['leases']['10']['resource_policy'] == journal['resource_policy'] == receipt['resource_policy']
    assert journal['config_sha256'] == resources.BASE_CONFIG_SHA256
    assert journal['local_finalization'] == 'recorded'


def test_exactly_four_parallel_single_gpu_leases_and_256_cpus(policy_fixture):
    root, config, path, _ = policy_fixture
    locator = load(policy_fixture)
    with ExitStack() as stack:
        for index in range(4):
            stack.enter_context(budget.resource_lease(root, config, cpus=64, gpus=1,
                max_hours=1, job_id=str(10 + index), resource_policy=locator))
        value = read_json(root / 'state/resource-budget.json')
        assert sum(r['gpus'] for r in value['leases'].values()) == 4
        assert sum(r['cpus'] for r in value['leases'].values()) == 256
        with pytest.raises(ValueError, match='concurrency'):
            with budget.resource_lease(root, config, cpus=1, gpus=0, max_hours=1,
                job_id='15', resource_policy=(path, digest(path))):
                pytest.fail('CPU ceiling must remain finite')
    with ExitStack() as stack:
        for index in range(4):
            stack.enter_context(budget.resource_lease(root, config, cpus=1, gpus=1,
                max_hours=1, job_id=str(20 + index), resource_policy=locator))
        with pytest.raises(ValueError, match='concurrency'):
            with budget.resource_lease(root, config, cpus=1, gpus=1, max_hours=1,
                job_id='25', resource_policy=locator):
                pytest.fail('GPU ceiling must remain finite')


@pytest.mark.parametrize('kwargs', [
    {'cpus': True}, {'cpus': float('nan')}, {'cpus': float('inf')}, {'cpus': -1},
    {'cpus': 1.5}, {'gpus': True}, {'gpus': 2}, {'gpus': -1},
    {'max_hours': True}, {'max_hours': float('nan')}, {'max_hours': float('inf')},
    {'max_hours': -1}, {'max_hours': 169}, {'max_hours': '1'},
])
def test_policy_native_leases_reject_nonfinite_or_unapproved_job_resources(policy_fixture, kwargs):
    root, config, path, _ = policy_fixture
    args = dict(cpus=1, gpus=0, max_hours=1, job_id='10', resource_policy=(path, digest(path)))
    args.update(kwargs)
    with pytest.raises(ValueError):
        with budget.resource_lease(root, config, **args):
            pytest.fail('Malformed native resource request admitted')


def test_policy_changed_between_loading_and_budget_mutex_cannot_admit(policy_fixture, monkeypatch):
    root, config, path, _ = policy_fixture
    original = budget.exclusive_file_lock
    @contextmanager
    def replace_before_admission(*args, **kwargs):
        path.write_bytes(path.read_bytes() + b' ')
        with original(*args, **kwargs):
            yield
    monkeypatch.setattr(budget, 'exclusive_file_lock', replace_before_admission)
    with pytest.raises(ValueError, match='bytes changed'):
        with budget.resource_lease(root, config, cpus=1, gpus=0, max_hours=1,
            job_id='10', resource_policy=(path, digest(path))):
            pytest.fail('Policy must be revalidated inside admission')
    assert not (root / 'state/resource-budget.json').exists()


def test_policy_change_after_admission_preserves_original_accounting_snapshot(policy_fixture):
    root, config, path, _ = policy_fixture
    pair = (path, digest(path))
    with budget.resource_lease(root, config, cpus=2, gpus=0, max_hours=1, job_id='10', resource_policy=pair):
        path.write_bytes(path.read_bytes() + b' ')
    record = read_json(root / 'state/resource-budget.json')
    journal = read_json(root / 'state/resource-lease-finalizations/10.json')
    assert record['leases']['10']['state'] == 'finished'
    assert record['leases']['10']['resource_policy']['sha256'] == pair[1]
    assert journal['resource_policy']['sha256'] == pair[1]
    assert journal['local_finalization'] == 'recorded'
    with pytest.raises(ValueError, match='bytes changed'):
        with budget.resource_lease(root, config, cpus=1, gpus=0, max_hours=1, job_id='11', resource_policy=pair):
            pytest.fail('A changed descriptor cannot authorize another lease')


def test_native_accounting_reconciliation_retains_policy_and_historical_receipts(policy_fixture):
    root, config, path, _ = policy_fixture
    old = {'state': 'COMPLETED', 'exit_code': '0:0', 'elapsed_seconds': 401 * 3600,
           'cpus': 64, 'gpus': 1, 'cpu_core_hours': 25664., 'gpu_hours': 401.,
           'basis': 'sacct final allocated wall time'}
    atomic_json(root / 'state/resource-budget.json', {'spent_cpu_core_hours': 25664.,
        'spent_gpu_hours': 401., 'leases': {}, 'native_jobs': {'99': old}})
    with budget.resource_lease(root, config, cpus=2, gpus=1, max_hours=1,
        job_id='10', resource_policy=(path, digest(path))):
        pass
    before = read_json(root / 'state/resource-budget.json')['leases']['10']['resource_policy']
    journal_hash = digest(root / 'state/resource-lease-finalizations/10.json')
    runner = lambda *a, **k: SimpleNamespace(returncode=0,
        stdout='10|10|COMPLETED|60|2|cpu=2,gres/gpu=1|0:0|\n')
    budget.native_accounting(root, ['10'], runner)
    record = read_json(root / 'state/resource-budget.json')
    assert record['native_jobs']['99'] == old
    assert record['leases']['10']['state'] == 'native_reconciled'
    assert record['leases']['10']['resource_policy'] == before
    assert record['spent_cpu_core_hours'] == pytest.approx(25664 + 2 / 60)
    assert record['spent_gpu_hours'] == pytest.approx(401 + 1 / 60)
    assert digest(root / 'state/resource-lease-finalizations/10.json') == journal_hash
