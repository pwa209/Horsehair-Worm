"""Explicit operational resource policy, separate from the frozen science.

A descriptor records human instruction provenance, not a cryptographic proof
of identity. Its exact bytes and software/configuration bindings must be pinned
by the caller. Nothing here clears scientific gates or changes source data.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

from .core import canonical_hash, source_identity


PERSONAL_STUDY_ROOT = Path('/scratch/pwa209/horsehair-worm')
BASE_CONFIG_SHA256 = '446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
PRIMARY_SCIENTIFIC_SOURCE_SHA256 = '0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
EXECUTION_SOURCE_ROOT = Path(__file__).resolve().parents[2]
MAX_POLICY_BYTES = 65536
LIMITS = {'cpu_ceiling': 256, 'gpu_concurrency': 4,
          'cpu_core_hours_cap': None, 'gpu_hours_cap': None,
          'max_native_hours': 168, 'scratch_budget_bytes': 6_000_000_000_000,
          'scratch_budget_files': 250000, 'scratch_reserve_bytes': 2_000_000_000_000}


def _require(value, message):
    if not value:
        raise ValueError(message)


def _strict_pairs(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, 'Duplicate resource policy field')
        result[key] = value
    return result


def _invalid_number(value):
    raise ValueError('Resource policy numbers must be finite: ' + value)


@dataclass(frozen=True)
class VerifiedResourcePolicy:
    """Pinned locator; admission reopens the descriptor, including for objects."""

    path: Path
    sha256: str
    execution_source_sha256: str
    base_config_sha256: str
    primary_scientific_source_sha256: str
    policy_id: str
    approval: dict

    def receipt(self):
        return {'path': str(self.path), 'sha256': self.sha256,
                'policy_id': self.policy_id, 'execution_source_sha256': self.execution_source_sha256,
                'base_config_sha256': self.base_config_sha256,
                'primary_scientific_source_sha256': self.primary_scientific_source_sha256,
                'limits': dict(LIMITS), 'approval': json.loads(json.dumps(self.approval)),
                'scientific_gates_changed': False,
                'authority_basis': 'Recorded current-user instruction provenance; not cryptographic identity verification'}


def load_resource_policy(path, expected_sha256, *, root, config):
    """Validate one immutable, explicitly approved personal-study descriptor.

    No environment variable, global policy file or modified scientific config
    is consulted. The descriptor must bind the current execution source and the
    unchanged primary science/configuration. Provider/scheduler limits remain
    independent obligations of the native caller.
    """
    root = Path(root).resolve()
    _require(root == PERSONAL_STUDY_ROOT.resolve(), 'Resource policy requires the exact personal study root')
    original = Path(path)
    _require(original.is_absolute(), 'Resource policy path must be absolute')
    namespace = root / 'maintenance/execution-resource-policies'
    resolved = original.resolve()
    _require(resolved.is_relative_to(namespace) and resolved.name == 'policy.json'
             and len(resolved.relative_to(namespace).parts) == 2,
             'Resource policy is outside the exact personal maintenance namespace')
    policy_id = resolved.parent.name
    _require(re.fullmatch(r'[a-z0-9][a-z0-9-]{0,95}', policy_id) is not None,
             'Invalid finite resource policy identity')
    _require(not original.is_symlink() and original == resolved and resolved.is_file()
             and all(not parent.is_symlink() for parent in (resolved.parent, namespace, namespace.parent)),
             'Resource policy must be an immutable regular file without redirected parents')
    _require(isinstance(expected_sha256, str) and re.fullmatch(r'[0-9a-f]{64}', expected_sha256) is not None,
             'Exact expected resource policy SHA256 is required')
    with resolved.open('rb') as stream:
        raw = stream.read(MAX_POLICY_BYTES + 1)
    _require(len(raw) <= MAX_POLICY_BYTES and hashlib.sha256(raw).hexdigest() == expected_sha256,
             'Resource policy bytes changed or exceed their finite bound')
    record = json.loads(raw, object_pairs_hook=_strict_pairs, parse_constant=_invalid_number)
    keys = {'schema_version', 'kind', 'policy_id', 'study_root', 'base_config_sha256',
            'primary_scientific_source_sha256', 'execution_source_sha256', 'limits',
            'approval', 'scientific_gates_changed'}
    _require(isinstance(record, dict) and set(record) == keys
             and type(record['schema_version']) is int and record['schema_version'] == 1
             and record['kind'] == 'approved-operational-resource-policy'
             and record['policy_id'] == policy_id and record['study_root'] == str(root)
             and record['scientific_gates_changed'] is False,
             'Malformed resource policy schema, scope or study identity')
    _require(canonical_hash(config) == BASE_CONFIG_SHA256 == record['base_config_sha256']
             and record['primary_scientific_source_sha256'] == PRIMARY_SCIENTIFIC_SOURCE_SHA256,
             'Resource policy scientific source or base configuration mismatch')
    limits = record['limits']
    _require(isinstance(limits, dict) and set(limits) == set(LIMITS), 'Malformed resource policy limits')
    for key, expected in LIMITS.items():
        value = limits[key]
        _require(value is None if expected is None else type(value) is int and value == expected,
                 'Resource policy limit differs from the approved finite operating envelope: ' + key)
    for key in ('scratch_budget_bytes', 'scratch_budget_files', 'scratch_reserve_bytes'):
        _require(config['resources'][key] == limits[key] and type(config['resources'][key]) is int,
                 'Frozen storage limits cannot change in an operational resource policy')
    approval = record['approval']
    approval_keys = {'source', 'statement', 'evidence_reference', 'approved_at_utc',
                     'authorizes_cpu_gpu_resource_amendment', 'identity_verification'}
    _require(isinstance(approval, dict) and set(approval) == approval_keys
             and approval['source'] == 'current_user'
             and approval['authorizes_cpu_gpu_resource_amendment'] is True
             and approval['identity_verification'] == 'instruction_provenance_only'
             and all(isinstance(approval[k], str) and 0 < len(approval[k].strip()) <= 8192
                     for k in ('statement', 'evidence_reference', 'approved_at_utc')),
             'Explicit current-user resource authorization provenance is required')
    try:
        approved_at = datetime.fromisoformat(approval['approved_at_utc'].replace('Z', '+00:00'))
    except ValueError as error:
        raise ValueError('Resource approval timestamp must be an explicit UTC instant') from error
    _require(approved_at.tzinfo is not None and approved_at.utcoffset() == timezone.utc.utcoffset(approved_at),
             'Resource approval timestamp must be an explicit UTC instant')
    execution_sha = record['execution_source_sha256']
    _require(isinstance(execution_sha, str) and re.fullmatch(r'[0-9a-f]{64}', execution_sha) is not None
             and source_identity(EXECUTION_SOURCE_ROOT)['sha256'] == execution_sha,
             'Resource policy execution source mismatch')
    # An object returned here remains only a locator; the admission path reloads
    # it under the existing durable budget mutex before reserving resources.
    return VerifiedResourcePolicy(resolved, expected_sha256, execution_sha,
        record['base_config_sha256'], record['primary_scientific_source_sha256'], policy_id, approval)


def resolve_resource_policy(value, *, root, config):
    """Accept only an explicit pinned path pair or a freshly rechecked locator."""
    if isinstance(value, VerifiedResourcePolicy):
        return load_resource_policy(value.path, value.sha256, root=root, config=config)
    _require(isinstance(value, tuple) and len(value) == 2,
             'Resource policy must be an explicit (path, SHA256) pair or verified locator')
    return load_resource_policy(value[0], value[1], root=root, config=config)
