"""Explicit operational personal-inode reserve; frozen science remains unchanged."""
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import ast
import copy
import hashlib
import inspect
import json
import os
import re
import stat
from types import CodeType

ROOT = Path('/scratch/pwa209/horsehair-worm')
POLICY_ID = 'storage-reserve-200k-v2-20261106'
MINIMUM_FREE_FILES = 200000
RECEIPT_FILE_OVERHEAD = 5
SOURCE = '4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY_SOURCE = '0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG = '446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
MAX_POLICY_BYTES = 65536
MAX_SOURCE_BYTES = 2000000


def require(value, message):
    if not value:
        raise ValueError(message)


def _pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, 'Duplicate storage policy field')
        result[key] = value
    return result


def _invalid_number(value):
    raise ValueError('Nonfinite storage policy number: ' + value)


def _ordinary(path, root):
    path, root = Path(path), Path(root)
    require(path.is_absolute() and path.is_relative_to(root) and path.resolve() == path,
            'Storage policy requires its exact ordinary personal path')
    for parent in (path.parent, *path.parent.parents):
        if not parent.is_relative_to(root):
            break
        info = parent.lstat()
        require(stat.S_ISDIR(info.st_mode), 'Redirected storage policy parent refused')
        if hasattr(os, 'geteuid'):
            require(info.st_uid == os.geteuid(), 'Foreign-owned storage policy parent refused')
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1,
            'Owned single-link ordinary storage policy required')
    if hasattr(os, 'geteuid'):
        require(info.st_uid == os.geteuid(), 'Foreign-owned storage policy refused')
    return info


@dataclass(frozen=True)
class VerifiedStoragePolicy:
    path: Path
    sha256: str
    root: Path

    def receipt(self):
        return {'path': str(self.path), 'sha256': self.sha256, 'policy_id': POLICY_ID,
                'minimum_personal_free_files': MINIMUM_FREE_FILES,
                'receipt_file_overhead': RECEIPT_FILE_OVERHEAD,
                'execution_source_sha256': SOURCE, 'primary_scientific_source_sha256': PRIMARY_SOURCE,
                'base_config_sha256': CONFIG, 'study_file_cap_changed': False,
                'byte_limits_changed': False, 'scientific_gates_changed': False,
                'authority_basis': 'Recorded current-user instruction provenance'}


def verify_policy(path, expected_sha256, *, root=ROOT):
    """Verify the explicit pinned descriptor; no environment/default-file lookup."""
    path, root = Path(path), Path(root)
    require(root.is_absolute() and root.resolve() == root and root == ROOT,
            'Exact authorized personal study root required')
    require(path == root/'maintenance'/POLICY_ID/'policy.json',
            'Exact approved storage policy namespace required')
    require(isinstance(expected_sha256, str) and re.fullmatch('[0-9a-f]{64}', expected_sha256),
            'Exact expected storage policy SHA256 required')
    before = _ordinary(path, root)
    require(before.st_size <= MAX_POLICY_BYTES, 'Storage policy exceeds finite size bound')
    with path.open('rb') as stream:
        raw = stream.read(MAX_POLICY_BYTES + 1)
    after = _ordinary(path, root)
    identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    require(identity(before) == identity(after) and len(raw) <= MAX_POLICY_BYTES
            and hashlib.sha256(raw).hexdigest() == expected_sha256,
            'Storage policy bytes changed or differ from explicit pin')
    record = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_invalid_number)
    expected = {'schema_version': 1, 'kind': 'approved-personal-scratch-file-reserve',
                'policy_id': POLICY_ID, 'study_root': str(root),
                'minimum_personal_free_files': MINIMUM_FREE_FILES,
                'receipt_file_overhead': RECEIPT_FILE_OVERHEAD,
                'unchanged_study_file_cap': 250000,
                'unchanged_scratch_budget_bytes': 6000000000000,
                'unchanged_scratch_reserve_bytes': 2000000000000,
                'execution_source_sha256': SOURCE, 'primary_scientific_source_sha256': PRIMARY_SOURCE,
                'base_config_sha256': CONFIG, 'scientific_gates_changed': False}
    require(isinstance(record, dict) and set(record) == set(expected)|{'approval'},
            'Exact operational storage policy schema required')
    for key, value in expected.items():
        require(type(record[key]) is type(value) and record[key] == value,
                'Storage policy scope or unchanged limit mismatch: ' + key)
    approval = record['approval']
    keys = {'source', 'statement', 'evidence_reference', 'approved_at_utc',
            'authorizes_personal_file_reserve_amendment', 'identity_verification'}
    require(isinstance(approval, dict) and set(approval) == keys
            and approval['source'] == 'current_user'
            and approval['authorizes_personal_file_reserve_amendment'] is True
            and approval['identity_verification'] == 'instruction_provenance_only'
            and all(isinstance(approval[k], str) and 0 < len(approval[k].strip()) <= 8192
                    for k in ('statement', 'evidence_reference', 'approved_at_utc')),
            'Explicit current-user storage amendment provenance required')
    approved = datetime.fromisoformat(approval['approved_at_utc'].replace('Z', '+00:00'))
    require(approved.tzinfo is not None and approved.utcoffset() == timezone.utc.utcoffset(approved),
            'Storage approval requires an explicit UTC instant')
    return VerifiedStoragePolicy(path, expected_sha256, root)


def reserve_for(required_files, verified_policy):
    """Keep a 200000 total floor and every prospective inode plus receipt space."""
    require(type(required_files) is int and required_files >= 0,
            'Prospective file demand must be a nonnegative integer')
    require(type(verified_policy) is VerifiedStoragePolicy, 'Explicit verified storage policy required')
    verify_policy(verified_policy.path, verified_policy.sha256, root=verified_policy.root)
    return max(0, MINIMUM_FREE_FILES - required_files - RECEIPT_FILE_OVERHEAD)


def derive_reserved_function(function, verified_policy, *, expected_source_sha256,
                             expected_reserve_expression, required_files_expression,
                             expected_count=1):
    """Privately amend only an exact original quota reserve keyword expression.

    This accepts original, undecorated module functions. Already-derived code
    is rejected rather than silently discarding another operational amendment.
    """
    reserve_for(0, verified_policy)
    require(inspect.isfunction(function) and function.__closure__ is None,
            'Original module function without a closure required')
    require(type(expected_count) is int and expected_count > 0,
            'Explicit positive quota-boundary count required')
    require(isinstance(expected_source_sha256, str)
            and re.fullmatch('[0-9a-f]{64}', expected_source_sha256), 'Pinned original source required')
    path = Path(inspect.getsourcefile(function)).resolve()
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= MAX_SOURCE_BYTES,
            'Finite ordinary original source required')
    raw = path.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == expected_source_sha256, 'Original source bytes changed')
    tree = ast.parse(raw)
    matches = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == function.__name__]
    require(len(matches) == 1 and not matches[0].decorator_list,
            'One original undecorated function definition required')
    original = matches[0]
    namespace = dict(function.__globals__)
    code_path = function.__code__.co_filename
    compiled_module = compile(raw, code_path, 'exec', dont_inherit=True)
    original_codes = [item for item in compiled_module.co_consts if isinstance(item, CodeType)
                      and item.co_name == function.__name__ and item.co_firstlineno == original.lineno]
    require(len(original_codes) == 1 and function.__code__ == original_codes[0],
            'Already-derived function refused; preserve existing amendments at their explicit hook')
    old = ast.parse(expected_reserve_expression, mode='eval').body
    demand = ast.parse(required_files_expression, mode='eval').body
    dump = lambda node: ast.dump(node, include_attributes=False)
    derived = copy.deepcopy(original)
    count = 0
    for call in (node for node in ast.walk(derived) if isinstance(node, ast.Call)):
        keywords = {item.arg: item for item in call.keywords if item.arg is not None}
        if 'reserve_files' in keywords and dump(keywords['reserve_files'].value) == dump(old):
            require('required_files' in keywords and dump(keywords['required_files'].value) == dump(demand),
                    'Quota boundary prospective demand differs from explicit contract')
            keywords['reserve_files'].value = ast.Call(func=ast.Name(id='__verified_file_reserve', ctx=ast.Load()),
                                                     args=[copy.deepcopy(demand)], keywords=[])
            count += 1
    require(count == expected_count, 'Exact quota reserve amendment count required')
    restored = copy.deepcopy(derived)
    restored_count = 0
    for call in (node for node in ast.walk(restored) if isinstance(node, ast.Call)):
        for keyword in call.keywords:
            value = keyword.value
            if (keyword.arg == 'reserve_files' and isinstance(value, ast.Call)
                    and isinstance(value.func, ast.Name) and value.func.id == '__verified_file_reserve'):
                keyword.value = copy.deepcopy(old)
                restored_count += 1
    require(restored_count == expected_count and dump(restored) == dump(original),
            'Only the declared operational reserve expression may change')
    require('__verified_file_reserve' not in namespace, 'Private quota helper name collision')
    namespace['__verified_file_reserve'] = lambda required: reserve_for(required, verified_policy)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[derived], type_ignores=[])), code_path, 'exec'), namespace)
    private = namespace[function.__name__]
    private.__defaults__ = function.__defaults__
    private.__kwdefaults__ = function.__kwdefaults__
    return private, {'source_sha256': expected_source_sha256,
                     'storage_reserve_policy': verified_policy.receipt(),
                     'quota_reserve_expressions_changed': count,
                     'reverse_transform_ast_equal': True, 'original_module_unchanged': True,
                     'study_occupancy_and_byte_checks_preserved': True}
