"""Explicit H4 bridge for the complete evaluation-only float64 amendment.

The frozen training, endpoint validators and statistics are imported unchanged.
This module never rewrites a legacy protocol, trial, receipt or H4 result. Native
admission, storage headroom, leases and process deadlines belong to the launcher.
"""
from __future__ import annotations

import copy
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import re
import math
import subprocess
import time

from . import agents, agent_analysis as original, agent_execution_v2 as evaluation
from .core import canonical_hash, digest, exclusive_file_lock, read_json, source_identity
from .execution_resources import BASE_CONFIG_SHA256, PRIMARY_SCIENTIFIC_SOURCE_SHA256
from .slurm import completed_native_dependency


PERSONAL_ROOT = Path('/scratch/pwa209/horsehair-worm')
AGENTS_SHA256 = 'fbc98bc5765b2d1b2fc9f46b48b104da2cfba7a211e696f7b23e23745e67ac14'
STATISTICS_SHA256 = '25b21cf14abd9691279e770e12f7b31cbd18f0bd2abd6b394925dc287480af06'
EVALUATION_SHA256 = 'a83b89a987aad050afc09439d32aaf3398f61404e1b93c6784ae3a286a507bc0'
ANALYSIS_AMENDMENT = 'H4-complete-float64-evaluation-bridge-v1'
EVIDENCE_KEYS = {'qualification', 'precision_plan', 'precision_binding', 'precision_diagnostics', 'precision_assembly'}
MIN_FULL_QUALIFICATION_TESTS = 1737  # Qualified1694-test core plus initial43 bridge tests.
FULL_TEST_SELECTION = 'complete source/tests directory; no environment selectors or third-party plugin autoload'
ARCHIVE_HELPER_SHA256 = '045d8650db62c0fa52d7f56bb7bdb2669729509c23ecfbbc847e73cbbff8d42a'
ARCHIVE_REGISTRY_KIND = 'final_policy_condition_archive_registry_v1'
ARCHIVE_PACK_FRAMEWORK_SHA256 = '4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
ARCHIVE_PACK_QUALIFICATION_SHA256 = '29ed530f3783f3c35c3cf20343bdb663732ec2bf4bf5c3218f8b029510becc2f'
ARCHIVE_PACK_QUALIFICATION_PATH = 'maintenance/study-af-execution-support-v1-20261021/qualification.json'
ARCHIVE_PACK_QUALIFICATION_JOB = '6684484'
ARCHIVE_CELL_KEYS = {'evaluation_case', 'policy_inputs', 'archive_directory', 'index_sha256', 'archive_sha256',
                     'pack_intent', 'roundtrip', 'pack_receipt', 'pack_producer'}
ARCHIVE_NATIVE_REPAIR_SHA256 = 'f189d515acdcba105fda56dd8d52c08044db736c8de9176fa5488931d428e7aa'


def _require(value, message):
    if not value:
        raise ValueError(message)


def _sha(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def _sources():
    expected = {'agents': (agents, AGENTS_SHA256), 'statistics': (original, STATISTICS_SHA256),
                'evaluation': (evaluation, EVALUATION_SHA256)}
    _require(all(digest(Path(module.__file__)) == sha for module, sha in expected.values()),
             'Frozen training/statistics/evaluation implementation changed')
    identity = source_identity(Path(__file__).resolve().parents[2])
    tests = {name: sha for name, sha in identity['files'].items() if name.startswith('tests/')}
    return {'primary_scientific_source_sha256': PRIMARY_SCIENTIFIC_SOURCE_SHA256,
            **{name + '_module_sha256': sha for name, (_, sha) in expected.items()},
            'analysis_module_sha256': digest(Path(__file__)),
            'execution_source_sha256': identity['sha256'], 'test_files_sha256': canonical_hash(tests),
            'test_file_count': len(tests)}


def _root(config, root):
    root = Path(root).resolve()
    _require(root == PERSONAL_ROOT.resolve() == Path(config['paths']['root']).resolve()
             and canonical_hash(config) == BASE_CONFIG_SHA256, 'Exact original configuration and personal root required')
    return root


def _confined(root, path):
    path = Path(path)
    _require(path.is_absolute() and path.resolve().is_relative_to(root) and path.resolve() != root,
             'Evidence must remain inside personal study root')
    # Resolve the system-level scratch alias once, but never follow a subordinate
    # redirected file or parent directory into another evidence namespace.
    prefixes = [p for p in [Path(root), PERSONAL_ROOT] if p.resolve() == root and path.is_relative_to(p)]
    _require(prefixes, 'Evidence locator uses a foreign redirected namespace')
    prefix = max(prefixes, key=lambda p: len(p.parts))
    _require(all(not p.is_symlink() for p in [path, *path.parents] if p != prefix and p.is_relative_to(prefix)),
             'Subordinate evidence symlinks are forbidden')
    return path.resolve()


def _locator(root, path):
    path = _confined(root, path)
    _require(path.is_file(), 'Missing source-bound evidence file')
    return {'path': str(path), 'sha256': digest(path)}


def _bound(root, binding, *, json_file=True):
    _require(isinstance(binding, dict) and set(binding) == {'path', 'sha256'} and _sha(binding['sha256']),
             'Exact file locator and SHA256 required')
    path = _confined(root, binding['path'])
    _require(path.is_file() and digest(path) == binding['sha256'], 'Immutable evidence bytes changed or disappeared')
    if not json_file:
        return path
    _require(path.stat().st_size <= 10_000_000, 'Metadata file exceeds finite bound')
    return read_json(path)


def _exclusive_json(path, value):
    import os
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    _require(len(raw) <= 50_000_000, 'Analysis metadata exceeds finite bound')
    with Path(path).open('xb') as handle:
        handle.write(raw); handle.flush(); os.fsync(handle.fileno())


def _load_archive_helper(path):
    raw = Path(path).read_bytes()
    _require(hashlib.sha256(raw).hexdigest() == ARCHIVE_HELPER_SHA256, 'Archive reader source changed before import')
    spec = importlib.util.spec_from_file_location('_h4_verified_archive_' + ARCHIVE_HELPER_SHA256, path)
    _require(spec is not None and spec.loader is not None, 'Archive helper is not an importable source file')
    helper = importlib.util.module_from_spec(spec)
    # Execute the already-hashed source bytes, never a second unverified read.
    exec(compile(raw, str(path), 'exec'), helper.__dict__)
    return helper


def _archive_metadata(root, helper, binding):
    path = _bound(root, binding, json_file=False)
    _require(path.stat().st_size <= 10_000_000, 'Archive metadata exceeds finite bound')
    raw = path.read_bytes()
    _require(hashlib.sha256(raw).hexdigest() == binding['sha256'], 'Archive metadata changed while reading')
    return helper.strict_json(raw)


def _archive_context(root, binding, cells, *, native_deadline=None):
    """Load only an explicitly pinned operational reader; no data-supplied code."""
    if binding is None:
        return None
    registry = _bound(root, binding)
    _require(set(registry) == {'schema_version', 'kind', 'root', 'full_config_sha256',
        'primary_scientific_source_sha256', 'archive_helper', 'archive_canary', 'cells',
        'scientific_result', 'admission_authority'} and registry['schema_version'] == 1
        and registry['kind'] == ARCHIVE_REGISTRY_KIND and registry['root'] == str(root)
        and registry['full_config_sha256'] == BASE_CONFIG_SHA256
        and registry['primary_scientific_source_sha256'] == PRIMARY_SCIENTIFIC_SOURCE_SHA256
        and registry['scientific_result'] is False and registry['admission_authority'] is False,
        'Exact personal source-bound condition archive registry required')
    helper_path = _bound(root, registry['archive_helper'], json_file=False)
    _require(registry['archive_helper']['sha256'] == ARCHIVE_HELPER_SHA256,
             'Only the separately reviewed exact archive reader is supported')
    helper = _load_archive_helper(helper_path)
    # Parse a second time with the pinned strict parser: duplicate/nonfinite JSON
    # cannot become a different registry after ordinary metadata decoding.
    registry = _archive_metadata(root, helper, binding)
    helper._verify_canary(registry['archive_canary'], root)
    roster = registry['cells']
    _require(isinstance(roster, dict) and all(isinstance(k, str) and k in {str(i) for i in range(240)} for k in roster),
             'Archive cells must use exact canonical decimal policy indices')
    by_index = {cell['index']: cell for cell in cells}
    # One fresh terminal owner read for this complete in-memory context. Each
    # policy still checks the selected cost row and exact Q/JUnit bytes. This
    # object is never loaded from metadata, written, cached globally or reused
    # by another _archive_context call.
    pack_qualification = (_pack_qualification(root, {'path': str(root / ARCHIVE_PACK_QUALIFICATION_PATH),
        'sha256': ARCHIVE_PACK_QUALIFICATION_SHA256}, native_deadline=native_deadline) if roster else None)
    for key, descriptor in roster.items():
        _require(int(key) in by_index, 'Archive cell is absent from the original campaign')
        _archive_descriptor(root, helper, descriptor, by_index[int(key)], pack_qualification=pack_qualification)
    _bound(root, registry['archive_helper'], json_file=False); _bound(root, binding, json_file=False)
    return {'binding': copy.deepcopy(binding), 'registry': registry, 'helper': helper,
            'pack_qualification': pack_qualification}


def _archive_descriptor(root, helper, descriptor, cell, *, pack_qualification=None):
    """Metadata/native provenance, independently of endpoint reads."""
    _require(isinstance(descriptor, dict) and set(descriptor) == ARCHIVE_CELL_KEYS,
             'Exact immutable policy archive descriptor required')
    source = _confined(root, descriptor['evaluation_case'])
    directory = _confined(root, descriptor['archive_directory'])
    _require(source == Path(cell['evaluation_case']).resolve() and directory.is_relative_to(root / 'maintenance')
             and not directory.is_relative_to(source) and _sha(descriptor['index_sha256'])
             and _sha(descriptor['archive_sha256']), 'Archive belongs to another canonical policy case')
    inputs = descriptor['policy_inputs']
    _require(isinstance(inputs, dict) and set(inputs) == {'plan', 'binding', 'summary'}, 'Canonical archive inputs required')
    for key, name in [('plan', 'plan.json'), ('binding', 'plan-binding.json'), ('summary', 'evaluation_summary.json')]:
        _require(inputs[key] == _locator(root, source / name), 'Archive canonical policy input changed')
    values = {}
    for key, name in [('pack_intent', 'pack-intent.json'), ('roundtrip', 'roundtrip.json'), ('pack_receipt', 'pack-receipt.json')]:
        _require(Path(descriptor[key]['path']).resolve() == directory / name, 'Archive evidence moved to another directory')
        values[key] = _archive_metadata(root, helper, descriptor[key])
    index = _archive_metadata(root, helper, {'path': str(directory / 'index.json'), 'sha256': descriptor['index_sha256']})
    intent, roundtrip, receipt = (values[k] for k in ['pack_intent', 'roundtrip', 'pack_receipt'])
    _require(index.get('kind') == intent.get('kind') == 'completed-final-policy-conditions'
             and index.get('source') == intent.get('source') == str(source)
             and index.get('helper_sha256') == intent.get('helper_sha256') == ARCHIVE_HELPER_SHA256
             and index.get('authority') == intent.get('authority')
             and index.get('members') == intent.get('members') and len(index.get('members', [])) == 600
             and index.get('authority', {}).get('policy_inputs') == inputs
             and receipt.get('status') == 'verified_archive_originals_retained'
             and receipt.get('helper_sha256') == ARCHIVE_HELPER_SHA256
             and receipt.get('index_sha256') == descriptor['index_sha256']
             and receipt.get('archive_sha256') == descriptor['archive_sha256']
             and receipt.get('intent_sha256') == descriptor['pack_intent']['sha256']
             and receipt.get('roundtrip_sha256') == descriptor['roundtrip']['sha256']
             and receipt.get('files') == roundtrip.get('files') == 600
             and receipt.get('original_removal_authorized') is False
             and receipt.get('scientific_result') is False and receipt.get('admission_authority') is False
             and roundtrip.get('index_sha256') == descriptor['index_sha256']
             and roundtrip.get('archive_sha256') == descriptor['archive_sha256']
             and roundtrip.get('independent_extracted_bytes_verified') is True
             and roundtrip.get('member_roster_sha256') == index.get('member_roster_sha256')
             and roundtrip.get('bytes') == index.get('source_content_bytes'), 'Exact verified pack/roundtrip receipt required')
    producer = descriptor['pack_producer']
    _require(isinstance(producer, dict) and set(producer) == {'native', 'result', 'qualification'}, 'Separate pack-producer native proof and qualification required')
    _pack_qualification(root, producer['qualification'], observation=pack_qualification)
    job, _ = helper.verify_native(producer['native'], root, completed=True)
    result = _archive_metadata(root, helper, producer['result'])
    expected = {'schema_version': 1, 'status': 'complete_verified_policy_archive_pack', 'native_job_id': job,
        'archive_helper_sha256': ARCHIVE_HELPER_SHA256, 'evaluation_case': str(source), 'archive_directory': str(directory),
        'index_sha256': descriptor['index_sha256'], 'archive_sha256': descriptor['archive_sha256'],
        'pack_intent_sha256': descriptor['pack_intent']['sha256'], 'roundtrip_sha256': descriptor['roundtrip']['sha256'],
        'pack_receipt_sha256': descriptor['pack_receipt']['sha256'], 'full_config_sha256': BASE_CONFIG_SHA256,
        'primary_scientific_source_sha256': PRIMARY_SCIENTIFIC_SOURCE_SHA256, 'scientific_result': False, 'admission_authority': False}
    _require(all(result.get(k) == v for k, v in expected.items())
             and result.get('execution_source_sha256') == ARCHIVE_PACK_FRAMEWORK_SHA256,
             'Pack producer does not bind this exact archive and canonical policy')
    return index


@dataclass(frozen=True)
class _PackQualificationObservation:
    root: str
    job_id: str
    allocation_json: bytes


def _pack_qualification(root, locator, *, observation=None, native_deadline=None):
    """The initial pack transport supports only the already qualified4f frame.

    This does not claim the private pack wrapper belongs to the public1694-test
    suite. Its library has the separate exact39-case native canary above.
    """
    _require(locator == {'path': str(root / ARCHIVE_PACK_QUALIFICATION_PATH),
                         'sha256': ARCHIVE_PACK_QUALIFICATION_SHA256}, 'Exact independently qualified pack framework required')
    q = _bound(root, locator)
    counts = {'tests': 1694, 'failures': 0, 'errors': 0, 'skipped': 0}
    _require(q.get('status') == 'complete' and type(q.get('returncode')) is int and q['returncode'] == 0
             and q.get('source_sha256') == ARCHIVE_PACK_FRAMEWORK_SHA256 and q.get('config_sha256') == BASE_CONFIG_SHA256
             and q.get('source_verified_unchanged') is True and q.get('slurm_job_id') == ARCHIVE_PACK_QUALIFICATION_JOB
             and q.get('test_counts') == counts, 'Full pinned1694-test pack framework qualification required')
    if observation is None:
        joined = _owned_completed_cpu_qualification(root, ARCHIVE_PACK_QUALIFICATION_JOB, deadline=native_deadline)
        observation = _PackQualificationObservation(str(Path(root).resolve()), ARCHIVE_PACK_QUALIFICATION_JOB,
            json.dumps(joined, sort_keys=True, separators=(',', ':'), allow_nan=False).encode())
    else:
        _require(type(observation) is _PackQualificationObservation
                 and observation.root == str(Path(root).resolve()) and observation.job_id == ARCHIVE_PACK_QUALIFICATION_JOB,
                 'Only this context-scoped pack qualification observation is reusable')
        joined = json.loads(observation.allocation_json)
        current = completed_native_dependency(root, ARCHIVE_PACK_QUALIFICATION_JOB)
        _require(current is not None and current['native_records_sha256'] == joined['completion']['native_records_sha256'],
                 'Selected qualification allocation changed within this archive context')
    artifact = q.get('artifacts', {}).get('junit', {})
    junit = _bound(root, {'path': artifact.get('path'), 'sha256': artifact.get('sha256')}, json_file=False)
    from .cli import _qualification_junit
    _require(junit.is_relative_to(root / 'state/qualification-evidence') and junit.stat().st_size == artifact.get('bytes')
             and _qualification_junit(junit) == counts, 'Actual pinned1694-test native JUnit bytes required')
    return observation


def _owned_completed_cpu_qualification(root, job, *, deadline=None):
    """Join fresh strict owner metadata to the unchanged positive cost proof.

    Resource-budget native rows contain allocation costs, not owner/account or
    allocation-template fields. Those are independently read through the
    strict native parser, then joined on the complete scalar allocation.
    Neither row is enriched, fabricated, substituted or written here.
    """
    _require(type(job) is str and re.fullmatch('[1-9][0-9]*', job), 'Exact scalar native qualification job required')
    proof = completed_native_dependency(root, job)
    _require(proof is not None and set(proof.get('native_records', {})) == {job},
             'Positive persisted native qualification completion is missing')
    cost = proof['native_records'][job]
    _require(cost.get('state') == 'COMPLETED' and cost.get('exit_code') == '0:0'
             and cost.get('job_id') == cost.get('job_id_raw') == job
             and type(cost.get('cpus')) is int and cost['cpus'] == 4
             and type(cost.get('gpus')) is int and cost['gpus'] == 0
             and type(cost.get('elapsed_seconds')) is int and 0 < cost['elapsed_seconds'] <= 3600,
             'Positive scalar four-CPU cost-ledger qualification proof required')
    # Import at call time so the launcher's reviewed read guard remains in
    # force. No scheduler mutation or retry of a scientific operation occurs.
    from . import repair
    _require(digest(Path(repair.__file__)) == ARCHIVE_NATIVE_REPAIR_SHA256,
             'Qualification owner read requires the exact source-checked thirteen-field native parser')
    now = time.monotonic()
    deadline = now + 60 if deadline is None else deadline
    _require(type(deadline) in (int, float) and math.isfinite(deadline) and now < deadline,
             'Finite native qualification query deadline required')
    deadline = min(deadline, now + 60)
    def bounded_runner(command, **kwargs):
        expected = ['sacct', '--array', '-j', job, '--format=' + repair.REPAIR_NATIVE_FORMAT,
                    '--units=K', '--noheader', '--parsable2']
        _require(command == expected and kwargs == {'text': True, 'capture_output': True},
                 'Only the exact read-only thirteen-field native qualification query is supported')
        remaining = deadline - time.monotonic()
        _require(0 < remaining <= 60, 'Native qualification query deadline exhausted')
        response = subprocess.run(command, **kwargs, timeout=remaining)
        _require(type(response.returncode) is int and isinstance(response.stdout, str) and isinstance(response.stderr, str)
                 and len(response.stdout.encode()) <= 1_000_000 and len(response.stderr.encode()) <= 1_000_000,
                 'Bounded native qualification response required')
        return response
    records = repair._repair_native_states([job], {}, runner=bounded_runner, owner='pwa209', account='def-ptewarie_cpu')
    _require(time.monotonic() < deadline, 'Native qualification owner read exceeded its deadline')
    _require(digest(Path(repair.__file__)) == ARCHIVE_NATIVE_REPAIR_SHA256,
             'Strict qualification native parser changed during the read')
    _require(set(records) == {job}, 'Exactly one fresh owned native qualification allocation required')
    native = records[job]
    tres = native.get('alloc_tres', '')
    parts = tres.split(',') if type(tres) is str and tres else []
    _require(parts and all(part.count('=') == 1 and all(part.split('=', 1)) for part in parts),
             'Nonempty explicit native qualification allocation TRES required')
    resources = dict(part.split('=', 1) for part in parts)
    _require(len(resources) == len(parts) and resources.get('cpu') == str(cost['cpus'])
             and 'gpu' not in tres.lower(), 'Unambiguous CPU-only native qualification TRES must match its cost row')
    _require(native.get('user') == 'pwa209' and native.get('account') == 'def-ptewarie_cpu'
             and native.get('unallocated_metadata') is False
             and all(native.get(key) == cost[key] for key in ('state', 'exit_code', 'job_id_raw', 'cpus', 'elapsed_seconds'))
             and type(native.get('cpus')) is int and type(native.get('elapsed_seconds')) is int
             and native.get('node_list') not in {None, '', 'None assigned', 'None', 'Unknown'}
             and native.get('start') not in {None, '', 'None', 'Unknown'}
             and native.get('end') not in {None, '', 'None', 'Unknown'},
             'Fresh owned native qualification differs from its positive persisted allocation')
    current = completed_native_dependency(root, job)
    _require(current is not None and current['native_records_sha256'] == proof['native_records_sha256'],
             'Selected positive native qualification cost row changed during the owner query')
    return {'completion': proof, 'native': native}


@contextmanager
def _archived_conditions(root, context, descriptor, cell, value, assembly):
    """One verified reader, every original raw packet; never a partial fallback."""
    helper = context['helper']; index = _archive_descriptor(root, helper, descriptor, cell,
        pack_qualification=context['pack_qualification'])
    source = Path(cell['evaluation_case']).resolve()
    _, _, native_result = helper.verify_authority(index['authority'], source, root, policy=True)
    _require(native_result.get('result') == assembly
             and native_result.get('evaluation_plan_sha256') == descriptor['policy_inputs']['plan']['sha256']
             and native_result.get('evaluation_binding_sha256') == descriptor['policy_inputs']['binding']['sha256'],
             'Archive original native assembly authority differs')
    with helper.ArchiveReader(descriptor['archive_directory'], descriptor['index_sha256'], descriptor['archive_sha256']) as reader:
        _require(reader.index == index, 'Archive index changed while opening')
        bindings = assembly.get('condition_bindings', [])
        _require(len(bindings) == 300 and all(type(r.get('condition_index')) is int for r in bindings)
                 and [r['condition_index'] for r in bindings] == list(range(300)),
                 'All ordered original archive conditions required')
        expected, committed = {}, {}
        for i, binding in enumerate(bindings):
            receipt_name, artifact_name = binding['receipt_name'], binding['artifact_name']
            _require(re.fullmatch(r'condition-[0-9]{5}-[0-9a-f]{32}\.json', receipt_name)
                     and artifact_name == receipt_name[:-5] + '.parquet'
                     and receipt_name not in expected and artifact_name not in expected,
                     'Unknown or duplicate original archived part name')
            expected.update({receipt_name: binding['receipt_sha256'], artifact_name: binding['artifact_sha256']})
            raw = reader.read_member(receipt_name); receipt = helper.strict_json(raw)
            _require(hashlib.sha256(raw).hexdigest() == binding['receipt_sha256']
                     and type(receipt.get('condition_index')) is int and receipt['condition_index'] == i
                     and receipt.get('condition') == value['conditions'][i] and receipt.get('status') == 'complete'
                     and receipt.get('plan_sha256') == descriptor['policy_inputs']['plan']['sha256']
                     and receipt.get('original_weight_sha256') == value['original_weight_sha256']
                     and receipt.get('evaluation_weight_sha256') == assembly['evaluation_weight_sha256']
                     and receipt.get('artifact_name') == artifact_name and receipt.get('artifact_sha256') == binding['artifact_sha256'],
                     'Archived original condition provenance changed')
            evaluation._validate_restoration(receipt['restoration'], value['conditions'][i])
            committed[i] = dict(receipt, _receipt_name=receipt_name, _receipt_sha256=binding['receipt_sha256'])
        _require({name: member['sha256'] for name, member in reader.members.items()} == expected,
                 'Archive must contain exactly all600 authoritative condition files')
        yield reader, committed
    # Recheck the pathname too: a replaced archive must not hide behind an open fd.
    _bound(root, {'path': str(Path(descriptor['archive_directory']) / 'evidence.tar'), 'sha256': descriptor['archive_sha256']}, json_file=False)
    _archive_descriptor(root, helper, descriptor, cell, pack_qualification=context['pack_qualification'])


def _campaign(config, protocol, plan, root):
    raw = {k: v for k, v in plan.items() if k != 'plan_sha256'}
    _require(plan.get('plan_sha256') == canonical_hash(raw) and plan.get('config') == config
             and plan.get('protocol') == protocol and plan.get('protocol_sha256') == agents._digest(protocol),
             'Campaign plan/configuration/protocol identity changed')
    binding = plan.get('source_binding', {})
    _require(binding.get('primary_source_sha256') == PRIMARY_SCIENTIFIC_SOURCE_SHA256
             and binding.get('primary_agents_sha256') == AGENTS_SHA256
             and binding.get('evaluation_module_sha256') == EVALUATION_SHA256
             and _sha(binding.get('execution_source_sha256')), 'Campaign scientific/evaluation source binding changed')
    expected = evaluation.final_cells(config)
    settings = evaluation._settings(config)
    _require(len(expected) == 240 and settings['final_seeds'] == list(range(1000, 1020)),
             'Original240 policies and twenty final seeds required')
    campaign_case = _confined(root, plan['case'])
    _require(campaign_case.is_relative_to(root / 'maintenance'), 'Personal maintenance campaign required')
    for cell in expected:
        name = agents._cell_name(cell['task'], cell['architecture'], cell['seed'])
        cell.update(training_case=str(agents._directory(root, settings) / 'final' / name),
                    evaluation_case=str(campaign_case / f"cell-{cell['index']:04d}"))
    _require(plan.get('cells') == expected and plan.get('conditions') == evaluation._conditions(settings, protocol)
             and len(plan['conditions']) == 300 and plan.get('scientific_result') is False
             and plan.get('admission_authority') is False, 'Full immutable final policy/condition roster required')
    _require(protocol.get('config_sha256') == agents._digest(settings)
             and protocol.get('full_config_sha256') == BASE_CONFIG_SHA256
             and protocol.get('implementation_sha256') == AGENTS_SHA256
             and protocol.get('H4_inference_specification') == agents.h4_specification(config)
             and protocol.get('final_seeds') == settings['final_seeds']
             and protocol.get('learning_rate') in settings['lr_grid'] and protocol.get('biological_directions') is None,
             'Original separately frozen synthetic H4 specification required')
    return expected


def _native_receipt(root, binding, mode, source_sha):
    receipt = _bound(root, binding)
    _require(receipt.get('status') == 'complete_native_work' and receipt.get('mode') == mode
             and receipt.get('source_sha256') == source_sha and receipt.get('full_config_sha256') == BASE_CONFIG_SHA256
             and receipt.get('scientific_result') is False and receipt.get('admission_authority') is False
             and re.fullmatch('[1-9][0-9]*', str(receipt.get('native_job_id', '')))
             and completed_native_dependency(root, receipt['native_job_id']) is not None,
             'Positive source-bound native precision evidence required')
    return receipt


def _evidence(config, root, campaign, sources, evidence):
    _require(set(evidence) == EVIDENCE_KEYS, 'Complete independent qualification and precision evidence required')
    q = _bound(root, evidence['qualification']); counts = q.get('test_counts', {})
    _require(q.get('status') == 'complete' and q.get('returncode') == 0
             and q.get('source_sha256') == sources['execution_source_sha256']
             and q.get('config_sha256') == BASE_CONFIG_SHA256 and q.get('source_verified_unchanged') is True
             and type(counts.get('tests')) is int and counts['tests'] >= MIN_FULL_QUALIFICATION_TESTS
             and all(counts.get(k) == 0 for k in ['failures', 'errors', 'skipped'])
             and completed_native_dependency(root, str(q.get('slurm_job_id'))) is not None,
             'Complete current bridge-source native qualification required')
    from .cli import _qualification_junit
    source = Path(__file__).resolve().parents[2]
    command = q.get('command', [])
    workspace = Path(q.get('workspace', ''))
    _require(workspace.is_absolute() and isinstance(command, list) and command and isinstance(command[0], str),
             'Qualification must identify its original complete-suite command and workspace')
    expected_command = [command[0], '-u', '-m', 'pytest', str(source / 'tests'), '-v', '--color=no',
        '--basetemp', str(workspace / 'pytest'), '--junitxml', str(workspace / 'junit.xml'),
        '-o', 'cache_dir=' + str(workspace / 'cache'), '-o', 'faulthandler_timeout=120']
    _require(q.get('test_selection') == FULL_TEST_SELECTION and command == expected_command,
             'Qualification must execute the complete current immutable test directory')
    artifact = q.get('artifacts', {}).get('junit', {})
    junit = _bound(root, {key: artifact.get(key) for key in ['path', 'sha256']}, json_file=False)
    _require(_qualification_junit(junit) == counts, 'Actual qualification testcase roster differs from declared complete counts')
    _bound(root, evidence['precision_binding'])
    precision_path = Path(evidence['precision_plan']['path'])
    _bound(root, evidence['precision_plan'])
    _require(Path(evidence['precision_binding']['path']).resolve() == precision_path.parent.resolve() / 'plan-binding.json',
             'Independent precision binding belongs to another case')
    precision = evaluation._load_plan(precision_path.parent, evidence['precision_plan']['sha256'])
    kw = {'ranks': [1, 4], 'strengths': [0, .4], 'evaluation_episodes': 4, 'calibration_episodes': 32, 'kl_bisections': 10}
    _require(precision['config'] == config and precision['original_protocol'] == campaign['protocol']
             and precision['canary'] is True and precision['benchmark_directions'] is True
             and precision['canary_overrides'] == kw and precision['task'] == 'delayed_choice'
             and precision['architecture'] == 'gru' and precision['seed'] == 900 and len(precision['conditions']) == 52,
             'Exact original failed-policy paired precision canary required')
    source = campaign['source_binding']['execution_source_sha256']
    diagnostics = _native_receipt(root, evidence['precision_diagnostics'], 'precision-diagnostics', source)
    result = diagnostics['result']; reports = result.get('reports', [])
    _require(result.get('plan_sha256') == evidence['precision_plan']['sha256']
             and diagnostics.get('float32_failure_reproduced') is True
             and diagnostics.get('float64_all_canary_conditions_available') is True,
             'Paired precision gate does not bind this exact canary')
    for dtype in ['torch.float32', 'torch.float64']:
        rows = [r for r in reports if r.get('dtype') == dtype]
        _require(len(rows) == 52 and {r['condition_index'] for r in rows} == set(range(52)), 'Complete unique paired precision roster required')
        if dtype == 'torch.float32':
            _require(any(r['status'] == 'unavailable' for r in rows), 'Original float32 failure was not reproduced')
        else:
            _require(all(r['status'] == 'available' and r.get('unavailable_rows') == 0 for r in rows), 'Float64 canary remains numerically unavailable')
    _require(len(reports) == 104, 'Unexpected paired precision reports')
    assembly = _native_receipt(root, evidence['precision_assembly'], 'precision-assemble', source)['result']
    _require(assembly.get('status') == 'complete_precision_canary' and assembly.get('plan_sha256') == evidence['precision_plan']['sha256']
             and assembly.get('canary') is True and assembly.get('conditions') == 52 and assembly.get('trial_rows') == 1664
             and assembly.get('scientific_result') is False and assembly.get('admission_authority') is False,
             'Complete1664-row precision canary assembly required')


def freeze_plan(config, root, campaign_plan, case, *, campaign_plan_sha256,
                original_protocol, original_protocol_sha256, evidence):
    """Freeze analysis choices and actual native qualification before inference."""
    config = copy.deepcopy(config); root = _root(config, root); sources = _sources()
    campaign_binding = {'path': str(campaign_plan), 'sha256': campaign_plan_sha256}
    protocol_binding = {'path': str(original_protocol), 'sha256': original_protocol_sha256}
    campaign, protocol = _bound(root, campaign_binding), _bound(root, protocol_binding)
    _require(Path(original_protocol).resolve() == (agents._directory(root, evaluation._settings(config)) / 'frozen_protocol.json').resolve(),
             'Original protocol must remain at its canonical preserved location')
    cells = _campaign(config, protocol, campaign, root)
    _evidence(config, root, campaign, sources, evidence)
    case = _confined(root, case)
    _require(case.is_relative_to(root / 'results/amended-h4') and not case.exists(), 'Fresh separate amended H4 namespace required')
    case.mkdir(parents=True)
    plan = {'schema_version': 1, 'analysis_amendment': ANALYSIS_AMENDMENT, 'evaluation_amendment': evaluation.AMENDMENT,
            'numerical_difference': evaluation.DIFFERENCE, 'root': str(root), 'case': str(case), 'config': config,
            'full_config_sha256': BASE_CONFIG_SHA256, 'source_bindings': sources, 'campaign_binding': campaign_binding,
            'protocol_binding': protocol_binding, 'original_protocol': protocol, 'campaign': campaign, 'cells': cells,
            'evidence': copy.deepcopy(evidence), 'inference': protocol['H4_inference_specification'],
            'planned_policy_cells': 240, 'conditions_per_policy': 300, 'independent_final_seeds': 20,
            'trial_rows_per_policy': 153600, 'planned_trial_rows': 36864000, 'holm_family_tests': 2304,
            'scientific_result': False, 'admission_authority': False, 'created_unix': time.time()}
    _exclusive_json(case / 'analysis-plan.json', plan)
    _exclusive_json(case / 'analysis-plan-binding.json', {'sha256': digest(case / 'analysis-plan.json'), 'automatic_rewrite_permitted': False})
    return plan


def _load(case, expected_plan_sha256=None):
    case = Path(case).resolve(); binding = read_json(case / 'analysis-plan-binding.json')
    actual = digest(case / 'analysis-plan.json')
    _require(actual == binding['sha256'] and (expected_plan_sha256 is None or actual == expected_plan_sha256), 'Immutable analysis plan changed')
    plan = read_json(case / 'analysis-plan.json'); root = _root(plan['config'], plan['root'])
    _require(case == _confined(root, plan['case']) and case.is_relative_to(root / 'results/amended-h4')
             and plan['analysis_amendment'] == ANALYSIS_AMENDMENT and plan['evaluation_amendment'] == evaluation.AMENDMENT
             and plan['numerical_difference'] == evaluation.DIFFERENCE and plan['source_bindings'] == _sources()
             and plan['inference'] == plan['original_protocol']['H4_inference_specification'], 'Analysis amendment/source/specification changed')
    campaign, protocol = _bound(root, plan['campaign_binding']), _bound(root, plan['protocol_binding'])
    _require(campaign == plan['campaign'] and protocol == plan['original_protocol']
             and _campaign(plan['config'], protocol, campaign, root) == plan['cells'], 'Original campaign inputs changed')
    _evidence(plan['config'], root, campaign, plan['source_bindings'], plan['evidence'])
    return plan


def _entry(root, cell):
    training, case = Path(cell['training_case']), Path(cell['evaluation_case'])
    summary_binding = _locator(root, case / 'evaluation_summary.json'); summary = _bound(root, summary_binding)
    _require(summary.get('status') == 'complete_amended_evaluation' and summary.get('canary') is False
             and summary.get('conditions') == 300 and summary.get('trial_rows') == 153600
             and summary.get('scientific_result') is False and summary.get('admission_authority') is False,
             'Only complete final amended assemblies may enter the input manifest')
    artifact = summary.get('artifact_name')
    _require(isinstance(artifact, str) and Path(artifact).name == artifact, 'Exact assembled artifact basename required')
    return {'index': cell['index'], 'training_summary': _locator(root, training / 'summary.json'),
            'checkpoint': _locator(root, training / 'checkpoint.pt'), 'evaluation_plan': _locator(root, case / 'plan.json'),
            'evaluation_binding': _locator(root, case / 'plan-binding.json'), 'assembly_summary': summary_binding,
            'assembled_trials': _locator(root, case / artifact)}


def freeze_inputs(case, *, expected_plan_sha256=None, archive_registry=None):
    """Pin every complete policy manifest; endpoint values are not read here."""
    plan = _load(case, expected_plan_sha256); root = Path(plan['root']); case = Path(case)
    with exclusive_file_lock(case / 'analysis.lock', timeout=0):
        archives = _archive_context(root, archive_registry, plan['cells'])
        entries = [_entry(root, cell) for cell in plan['cells']]
        if archives is not None:
            for entry in entries:
                descriptor = archives['registry']['cells'].get(str(entry['index']))
                if descriptor is not None: entry['condition_archive'] = copy.deepcopy(descriptor)
        manifest = {'schema_version': 1, 'analysis_plan_sha256': digest(case / 'analysis-plan.json'), 'cells': entries,
                    'scientific_result': False, 'admission_authority': False, 'created_unix': time.time()}
        if archives is not None: manifest['archive_registry'] = copy.deepcopy(archive_registry)
        _exclusive_json(case / 'inputs.json', manifest)
        _exclusive_json(case / 'inputs-binding.json', {'sha256': digest(case / 'inputs.json'), 'automatic_rewrite_permitted': False})
    return manifest


def _final_plan(plan, cell, value):
    settings, protocol = evaluation._settings(plan['config']), plan['original_protocol']
    _require(value['config'] == plan['config'] and value['full_config_sha256'] == BASE_CONFIG_SHA256
             and value['original_protocol'] == protocol and value['original_protocol_sha256'] == agents._digest(protocol)
             and value['evaluation_settings'] == settings and value['canary'] is False and value['canary_overrides'] is None
             and value['benchmark_directions'] is False and value['evaluation_dtype'] == 'float64'
             and value['amendment'] == evaluation.AMENDMENT and value['numerical_difference'] == evaluation.DIFFERENCE
             and value['execution_module_sha256'] == EVALUATION_SHA256 and value['primary_agents_sha256'] == AGENTS_SHA256
             and value['conditions'] == plan['campaign']['conditions'] and len(value['conditions']) == 300
             and value['original_training_steps'] == settings['total_steps']
             and all(value[k] == cell[k] for k in ['task', 'architecture', 'seed'])
             and Path(value['checkpoint']).resolve() == (Path(cell['training_case']) / 'checkpoint.pt').resolve(),
             'Final amended plan changed frozen training, full grid or declared precision')


def _trial_summary(frame, settings, protocol, *, architecture, task, seed):
    """Original validators, estimands and independent-unit reductions, unchanged."""
    original.validate_trial_roster(frame, settings, protocol, architecture=architecture, task=task, seed=seed)
    summaries = original.summarize_trials(frame)
    controls = frame[~frame.restoration_control.eq('none')].groupby([*original.KEYS, 'restoration_control']).agg(
        success=('success', 'mean'), redirection=('redirection', 'mean'), nested_episodes=('episode_seed', 'size')).reset_index()
    controls['training_seed'] = seed
    return summaries, controls


def _validate_cell(plan, cell, entry, *, archives=None):
    import pandas as pd
    root, settings = Path(plan['root']), evaluation._settings(plan['config']); case = Path(cell['evaluation_case'])
    expected_entry = _entry(root, cell)
    descriptor = None if archives is None else archives['registry']['cells'].get(str(cell['index']))
    if descriptor is not None: expected_entry['condition_archive'] = descriptor
    _require(entry == expected_entry, 'Pinned policy input manifest changed')
    value = evaluation._load_plan(case, entry['evaluation_plan']['sha256']); _final_plan(plan, cell, value)
    summary = _bound(root, entry['training_summary'])
    expected = {'seed': cell['seed'], 'task': cell['task'], 'architecture': cell['architecture'],
                'config_sha256': agents._digest(settings), 'implementation_sha256': AGENTS_SHA256,
                'training_steps': settings['total_steps'], 'complete_training_budget': True,
                'learning_rate': float(plan['original_protocol']['learning_rate'])}
    _require(all(summary.get(k) == v for k, v in expected.items())
             and summary.get('checkpoint_sha256') == entry['checkpoint']['sha256'] == value['checkpoint_sha256']
             and summary.get('weight_sha256') == value['original_weight_sha256'], 'Original complete final-training provenance changed')
    base, saved = agents._load_policy(_bound(root, entry['checkpoint'], json_file=False), 'cpu', settings,
        seed=cell['seed'], architecture=cell['architecture'], task=cell['task'], learning_rate=expected['learning_rate'], complete=True)
    _require(saved['weight_sha256'] == summary['weight_sha256'] and agents.state_hash(base) == value['original_weight_sha256'],
             'Original float32 checkpoint weight identity changed')
    torch = agents._torch()
    _require(all(p.dtype == torch.float32 and torch.isfinite(p).all() for p in base.parameters()),
             'Original checkpoint contains nonfinite or non-float32 parameters')
    promoted_weight = agents.state_hash(evaluation._precision_policy(base, torch.float64))
    assembly = _bound(root, entry['assembly_summary'])
    expected_summary = {'status': 'complete_amended_evaluation', 'amendment': evaluation.AMENDMENT,
        'numerical_difference': evaluation.DIFFERENCE, 'plan_sha256': entry['evaluation_plan']['sha256'],
        'original_protocol_sha256': value['original_protocol_sha256'], 'evaluation_protocol_sha256': value['evaluation_protocol_sha256'],
        'trial_rows': 153600, 'conditions': 300, 'checkpoint_sha256': value['checkpoint_sha256'],
        'original_weight_sha256': value['original_weight_sha256'], 'evaluation_weight_sha256': promoted_weight,
        'canary': False, 'scientific_result': False, 'admission_authority': False}
    _require(all(assembly.get(k) == v for k, v in expected_summary.items())
             and assembly.get('trial_file_sha256') == entry['assembled_trials']['sha256'], 'Amended complete assembly identity changed')
    source = (_archived_conditions(root, archives, descriptor, cell, value, assembly) if descriptor is not None
              else nullcontext((None, evaluation._committed(case, value))))
    bindings, frames = [], []
    with source as (reader, committed):
        _require(set(committed) == set(range(300)), 'Every original condition must have exactly one committed receipt')
        for index, condition in enumerate(value['conditions']):
            receipt = committed[index]
            _require(receipt['evaluation_weight_sha256'] == promoted_weight, 'Promoted weights changed between conditions')
            if reader is None:
                _locator(root, case / receipt['_receipt_name'])
                part = pd.read_parquet(case / receipt['artifact_name'])
            else:
                # Real condition parts contain512 rows. Keep the reviewed finite
                # read_member bound; a larger member fails rather than truncates.
                part = pd.read_parquet(io.BytesIO(reader.read_member(receipt['artifact_name'])))
            frames.append(evaluation._validate_condition(part.to_dict('records'), value, condition))
            if reader is None:
                _require(digest(case / receipt['artifact_name']) == receipt['artifact_sha256']
                         and digest(case / receipt['_receipt_name']) == receipt['_receipt_sha256'],
                         'Condition source changed while its endpoints were read')
            bindings.append({'condition_index': index, 'receipt_name': receipt['_receipt_name'], 'receipt_sha256': receipt['_receipt_sha256'],
                             'artifact_name': receipt['artifact_name'], 'artifact_sha256': receipt['artifact_sha256']})
    _require(assembly.get('condition_bindings') == bindings, 'Assembly must bind all exact ordered condition receipts and parts')
    frame = pd.concat(frames, ignore_index=True); assembled = pd.read_parquet(_bound(root, entry['assembled_trials'], json_file=False))
    pd.testing.assert_frame_equal(frame, assembled, check_dtype=True, check_exact=True)
    _bound(root, entry['assembled_trials'], json_file=False)
    return _trial_summary(frame, settings, value['evaluation_protocol'], architecture=cell['architecture'], task=cell['task'], seed=cell['seed'])


def statistics(summaries, settings, inference):
    """Reusable numerical adapter; production roster checks surround this call."""
    return original.analyze_seed_summaries(summaries, settings, inference), original._paired_architecture_contrasts(summaries, settings, inference)


def _result(plan, summaries, curves):
    settings, inference = evaluation._settings(plan['config']), plan['inference']
    primary = [r for r in curves if r['primary_grid_point']]
    _require(len(primary) == 2304 and len(curves) == 3600 and all(r['independent_seeds'] == 20 for r in curves),
             'The complete original2304-test Holm family is required')
    qualifications = [agents.baseline_qualification(row['baseline_main_success'], {p: row[f'baseline_{p}'] for p in agents.PROBES}, settings)
                      for _, row in summaries.iterrows()]
    all_main = all(q['baseline_main_qualified'] for q in qualifications)
    all_probes = all(q['baseline_probes_qualified'] for q in qualifications); qualified = all_main and all_probes
    supported = [r for r in primary if r['operating_region_supported']]
    reference = inference['agent_reference_point']; selected = summaries
    for key, value in reference.items(): selected = selected[selected[key].eq(value)]
    _require(len(selected) == 240, 'Original reference point must include every final policy')
    blocked = selected.groupby('training_seed')[original.OUTCOMES].mean()
    claim = 'supported' if qualified and supported else 'not_supported' if qualified else 'pending'
    heldout = [r for r in supported if r['architecture'] == plan['original_protocol'].get('heldout_architecture')
               and r['task'] == plan['original_protocol'].get('heldout_task')]
    return {'claim_id': 'H4', 'analysis_amendment': ANALYSIS_AMENDMENT, 'evaluation_amendment': evaluation.AMENDMENT,
        'numerical_difference': evaluation.DIFFERENCE, 'status': 'complete_amended_computational_analysis', 'claim_status': claim,
        'evidence_class': 'computational_experiment', 'independent_unit': 'independently trained policy seed',
        'computational_analysis_complete': True, 'headline_claim_accepted': claim == 'supported',
        'computational_claim_accepted': claim == 'supported', 'biological_transfer_status': 'pending',
        'biological_constraints_available': False, 'biologically_constrained_supported_grid_points': 0,
        'estimate': float(blocked.redirection_difference.mean()),
        'ci95': original.bootstrap_ci(blocked.redirection_difference, repetitions=inference['bootstrap_replicates']),
        'pvalue': min(r['holm_pvalue'] for r in primary), 'reference_point': reference,
        'pvalue_estimand': 'omnibus existence of a multiplicity-adjusted joint operating point, including paired comparator advantages',
        'seed_inference': 'one-sided Student-t contrasts across independent seeds; intervals resample seed clusters',
        'reference_estimand': 'equal-task/architecture mean redirection change, blocked by training seed; not selected on evaluation',
        'reference_competence_losses': {p: float(blocked[f'loss_{p}'].mean()) for p in agents.PROBES},
        'all_baseline_competence_qualified': qualified, 'all_baseline_main_qualified': all_main, 'all_baseline_probes_qualified': all_probes,
        'supported_grid_points': len(supported), 'computational_dual_heldout_supported_grid_points': len(heldout),
        'computational_dual_heldout_transfer_status': 'supported' if heldout else 'not_supported' if qualified else 'pending',
        'planned_policy_cells': 240, 'independent_seed_count_per_cell': 20, 'trial_rows': 36864000,
        'baseline_main_success_min': inference['baseline_main_success_min'],
        'baseline_probe_accuracy_min': inference['baseline_probe_accuracy_min'],
        'correction': {'method': 'Holm', 'family': 'H4 prespecified computational grid', 'tests': 2304, 'alpha': inference['alpha'],
            'point_test': 'intersection-union maximum of redirection, three probe losses and two paired comparator advantages',
            'unavailable_points_retained_as_pvalue_one': sum(not r['available_for_H4'] for r in primary)},
        'original_protocol_sha256': agents._digest(plan['original_protocol']),
        'evaluation_protocol_sha256': agents._digest(evaluation._amended_protocol(plan['original_protocol'], settings, False)),
        'analysis_source_bindings': plan['source_bindings'], 'full_config_sha256': BASE_CONFIG_SHA256,
        'admission_authority': False,
        'limitations': ['The evaluation precision amendment may change calibration and sampled trajectories.',
                       'Computational causality does not validate an insect mechanism; biological transfer remains pending.',
                       *([] if qualified else ['At least one original baseline policy is unqualified; no headline H4 claim is accepted.'])]}


def run(case, *, inputs_sha256, expected_plan_sha256=None):
    """Validate the entire amended grid, then write only separate H4 outputs."""
    import pandas as pd
    plan = _load(case, expected_plan_sha256); case = Path(case).resolve(); root = Path(plan['root'])
    with exclusive_file_lock(case / 'analysis.lock', timeout=0):
        _require(digest(case / 'inputs.json') == read_json(case / 'inputs-binding.json')['sha256'] == inputs_sha256,
                 'Immutable complete input manifest changed')
        manifest = read_json(case / 'inputs.json')
        _require(manifest.get('analysis_plan_sha256') == digest(case / 'analysis-plan.json')
                 and len(manifest['cells']) == 240 and [r['index'] for r in manifest['cells']] == list(range(240)),
                 'Exact complete input manifest required')
        archives = _archive_context(root, manifest.get('archive_registry'), plan['cells'])
        output = case / 'analysis'; _require(not output.exists(), 'Completed or partial amended analysis may not be overwritten')
        summaries, controls, failures = [], [], []
        for cell, entry in zip(plan['cells'], manifest['cells']):
            try:
                summary, control = _validate_cell(plan, cell, entry, archives=archives)
                summaries.append(summary); controls.append(control)
            except (OSError, ValueError, RuntimeError, KeyError, AssertionError, TypeError) as error:
                failures.append({'index': cell['index'], 'task': cell['task'], 'architecture': cell['architecture'],
                                 'seed': cell['seed'], 'error_type': type(error).__name__, 'error': str(error)})
        audit = {'status': 'incomplete' if failures else 'complete_verified_amended_grid', 'verified_policy_cells': len(summaries),
                 'planned_policy_cells': 240, 'failures': failures, 'analysis_plan_sha256': digest(case / 'analysis-plan.json'),
                 'inputs_sha256': inputs_sha256, 'scientific_result': False, 'admission_authority': False}
        _exclusive_json(case / 'validation-audit.json', audit)
        _require(not failures, 'Amended H4 remains incomplete; inspect validation-audit.json')
        seeds, restoration = pd.concat(summaries, ignore_index=True), pd.concat(controls, ignore_index=True)
        _require(len(seeds) == 72000 and len(restoration) == 288000, 'Original seed/condition/control reductions incomplete')
        curves, paired = statistics(seeds, evaluation._settings(plan['config']), plan['inference'])
        result = _result(plan, seeds, curves)
        _require(_sources() == plan['source_bindings'], 'Analysis source changed during inference')
        _require(digest(case / 'inputs.json') == inputs_sha256, 'Input manifest changed during inference')
        # Recheck every top-level source file after reading, not just the manifest.
        for entry in manifest['cells']:
            for name, binding in entry.items():
                if name not in {'index', 'condition_archive'}: _bound(root, binding, json_file=False)
        if archives is not None:
            # Native selected rows, canary, helper, registry, canonical inputs and
            # producer receipts are rechecked after statistics, without caching.
            _archive_context(root, archives['binding'], plan['cells'])
            for descriptor in archives['registry']['cells'].values():
                _bound(root, {'path': str(Path(descriptor['archive_directory']) / 'evidence.tar'),
                             'sha256': descriptor['archive_sha256']}, json_file=False)
        output.mkdir()
        seed_path, control_path, curve_path = output / 'seed_summaries.parquet', output / 'restoration_controls.parquet', output / 'response_curves.json'
        seeds.to_parquet(seed_path, index=False); restoration.to_parquet(control_path, index=False)
        _exclusive_json(curve_path, curves); _exclusive_json(output / 'paired_architecture_contrasts.json', paired)
        result.update(analysis_plan_sha256=digest(case / 'analysis-plan.json'), inputs_sha256=inputs_sha256,
            source_result_hashes={str(path): digest(path) for path in [case / 'analysis-plan.json', case / 'inputs.json',
                case / 'validation-audit.json', seed_path, control_path, curve_path, output / 'paired_architecture_contrasts.json']},
            response_curves=str(curve_path), seed_summaries_path=str(seed_path))
        _exclusive_json(output / 'H4-amended.json', result)
        template = output / 'figure-template-original-statistics.json'
        original._figure_inputs(curves, [seed_path, control_path, curve_path], template, restoration,
            repetitions=plan['inference']['bootstrap_replicates'], config=plan['config'])
        figure = read_json(template)
        figure.update(analysis_amendment=ANALYSIS_AMENDMENT, evaluation_amendment=evaluation.AMENDMENT,
            numerical_difference=evaluation.DIFFERENCE, original_protocol_sha256=result['original_protocol_sha256'],
            evaluation_protocol_sha256=result['evaluation_protocol_sha256'], analysis_plan_sha256=result['analysis_plan_sha256'],
            inputs_sha256=inputs_sha256)
        figure['caption'] = 'Declared float64 evaluation amendment; original float32 training is preserved. ' + figure['caption']
        _exclusive_json(output / 'figure-inputs-05-amended.json', figure)
        return result


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['freeze-plan', 'freeze-inputs', 'analyze'])
    parser.add_argument('--case', type=Path, required=True)
    parser.add_argument('--plan-sha256'); parser.add_argument('--inputs-sha256')
    parser.add_argument('--config', type=Path); parser.add_argument('--root', type=Path)
    parser.add_argument('--campaign-plan', type=Path); parser.add_argument('--campaign-plan-sha256')
    parser.add_argument('--original-protocol', type=Path); parser.add_argument('--original-protocol-sha256')
    parser.add_argument('--evidence', type=Path); parser.add_argument('--evidence-sha256')
    parser.add_argument('--archive-registry', type=Path); parser.add_argument('--archive-registry-sha256')
    args = parser.parse_args(argv)
    if args.mode == 'freeze-plan':
        _require(all([args.config, args.root, args.campaign_plan, args.campaign_plan_sha256,
                      args.original_protocol, args.original_protocol_sha256, args.evidence, args.evidence_sha256]),
                 'Freeze requires explicit complete input locators and hashes')
        config = read_json(args.config); root = _root(config, args.root)
        evidence = _bound(root, {'path': str(args.evidence), 'sha256': args.evidence_sha256})
        freeze_plan(config, root, args.campaign_plan, args.case, campaign_plan_sha256=args.campaign_plan_sha256,
            original_protocol=args.original_protocol, original_protocol_sha256=args.original_protocol_sha256, evidence=evidence)
    else:
        _require(_sha(args.plan_sha256), 'An independent immutable analysis-plan SHA is required')
        if args.mode == 'freeze-inputs':
            _require(bool(args.archive_registry) == bool(args.archive_registry_sha256), 'Archive registry requires both path and independent SHA')
            archive = None if args.archive_registry is None else {'path': str(args.archive_registry), 'sha256': args.archive_registry_sha256}
            freeze_inputs(args.case, expected_plan_sha256=args.plan_sha256, archive_registry=archive)
        else:
            _require(_sha(args.inputs_sha256), 'An independent immutable input-manifest SHA is required')
            run(args.case, inputs_sha256=args.inputs_sha256, expected_plan_sha256=args.plan_sha256)
    result = {'mode': args.mode, 'case': str(args.case), 'admission_authority': False}
    for name in ['analysis-plan.json', 'inputs.json', 'analysis/H4-amended.json']:
        if (args.case / name).is_file(): result[name] = digest(args.case / name)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
