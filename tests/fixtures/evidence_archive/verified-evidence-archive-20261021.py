"""Private, byte-preserving evidence archives; no scientific admission.

The native wrapper supplies fresh owner/account proof, qualified result bindings,
storage admission, and positive native canary provenance. No scheduler commands
are executed here. Creating an archive never removes an original by default.
"""
from pathlib import Path, PurePosixPath
from contextlib import ExitStack
from datetime import datetime, timezone
import argparse
import hashlib
import io
import json
import math
import os
import re
import stat
import tarfile
import time
import uuid
import xml.etree.ElementTree as ET

from horsehair.core import exclusive_file_lock

ROOT = Path('/scratch/pwa209/horsehair-worm')
PRIMARY = '0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG = '446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
AGENTS = 'fbc98bc5765b2d1b2fc9f46b48b104da2cfba7a211e696f7b23e23745e67ac14'
MAX_FILES = 20000
MAX_TOTAL_BYTES = 10_000_000_000
MAX_ARCHIVE_BYTES = MAX_TOTAL_BYTES + 100_000_000
MAX_METADATA_BYTES = 128_000_000
MAX_MEMBER_BYTES = 5_000_000_000
MAX_SECONDS = 3000
EXPECTED_CANARY_TEST_SHA256 = 'ec4131d283d3f3a792a561c6ae6324de853a4301def9d275a03018a9801f5462'
EXPECTED_CANARY_TEST_NAMES = ("test_full_policy_roundtrip_retains_every_original_and_canonical_input",
    "test_deterministic_archive_and_index_independent_of_output_directory",
    "test_archive_aware_packets_preserve_original_raw_sha_and_json",
    "test_unassembled_policy_and_canary_are_refused_without_any_archive",
    "test_source_and_native_failures_preserve_originals[running]",
    "test_source_and_native_failures_preserve_originals[foreign_owner]",
    "test_source_and_native_failures_preserve_originals[changed_ledger]",
    "test_source_and_native_failures_preserve_originals[wrong_source]",
    "test_source_and_native_failures_preserve_originals[changed_checkpoint]",
    "test_source_and_native_failures_preserve_originals[missing_part]",
    "test_source_and_native_failures_preserve_originals[changed_part]",
    "test_unsafe_or_protected_paths_never_become_archive_members[../escape]",
    "test_unsafe_or_protected_paths_never_become_archive_members[/absolute]",
    "test_unsafe_or_protected_paths_never_become_archive_members[a/../b]",
    "test_unsafe_or_protected_paths_never_become_archive_members[a//b]",
    "test_unsafe_or_protected_paths_never_become_archive_members[a\\\\b]",
    "test_unsafe_or_protected_paths_never_become_archive_members[./a]",
    "test_unsafe_or_protected_paths_never_become_archive_members[a/plan.json]",
    "test_unsafe_or_protected_paths_never_become_archive_members[assembled-x.parquet]",
    "test_unsafe_or_protected_paths_never_become_archive_members[receipt.json]",
    "test_unsafe_or_protected_paths_never_become_archive_members[x/checkpoint.pt]",
    "test_symlink_and_hardlink_originals_refused",
    "test_archive_and_index_corruption_refused",
    "test_native_canary_is_mandatory_for_removal",
    "test_explicit_verified_removal_preserves_canonical_inputs_and_archive_packets",
    "test_changed_original_refuses_all_unlinks_even_when_archive_is_valid",
    "test_interruption_after_unlink_requires_exact_explicit_reconciliation",
    "test_published_archive_interruption_explicitly_resumes_without_repacking",
    "test_terminal_reader_pack_excludes_worker_receipt_and_unrelated_files",
    "test_current_native_job_never_compacted",
    "test_finite_input_deadline_and_exclusive_publication",
    "test_unpacked_destination_never_overwritten",
    "test_boolean_or_incomplete_native_canary_never_authorizes_unlink[one_test]",
    "test_boolean_or_incomplete_native_canary_never_authorizes_unlink[source_sha]",
    "test_boolean_or_incomplete_native_canary_never_authorizes_unlink[missing_case]",
    "test_boolean_or_incomplete_native_canary_never_authorizes_unlink[failed_case]",
    "test_boolean_or_incomplete_native_canary_never_authorizes_unlink[missing_junit]",
    "test_streaming_reader_does_not_require_member_in_memory",
    "test_each_reconciliation_records_its_latest_native_remover",)
TERMINAL = {'COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT', 'OUT_OF_MEMORY', 'NODE_FAIL',
            'PREEMPTED', 'BOOT_FAIL', 'DEADLINE', 'REVOKED'}
READER_PREFIXES = ('accounting-reads', 'scheduler-reads', 'quota-reads', 'held-reads')
PROTECTED_NAMES = {'plan.json', 'plan-binding.json', 'evaluation_summary.json', 'receipt.json',
    'summary.json', 'frozen_protocol.json', 'checkpoint.pt', 'training_metrics.json',
    'precision-gate.json', 'campaign-plan.json', 'state.json.gz', 'state-binding.json'}
READER_JSON_KINDS = {'query', 'runner-error', 'response', 'verdict', 'command', 'exception', 'before', 'after'}


def require(value, message):
    if not value:
        raise ValueError(message)


def canonical(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def helper_sha():
    return sha(Path(__file__).read_bytes())


def _bounded(checkpoint):
    deadline = time.monotonic() + MAX_SECONDS
    def check():
        if time.monotonic() >= deadline:
            raise TimeoutError('Finite archive-operation deadline reached')
        checkpoint()
    return check


def is_sha(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'Duplicate JSON field')
            result[key] = value
        return result
    def finite(value):
        number = float(value)
        require(math.isfinite(number), 'Nonfinite JSON value')
        return number
    return json.loads(raw, object_pairs_hook=pairs, parse_float=finite,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('Nonfinite JSON value')))


def relative_name(name):
    require(isinstance(name, str) and 0 < len(name.encode()) <= 1024
            and re.fullmatch(r'[A-Za-z0-9_.\-/]+', name) is not None,
            'Only exact generated ASCII relative paths are supported')
    path = PurePosixPath(name)
    require(not path.is_absolute() and str(path) == name and all(p not in {'.', '..', ''} for p in path.parts),
            'Unsafe or noncanonical archive path')
    require(not path.name.startswith('assembled-') and path.name not in PROTECTED_NAMES,
            'Canonical result, input, checkpoint or worker receipt must remain unpacked')
    return name


def reader_evidence_name(name):
    """Accept only leaf names actually produced by the pinned native readers."""
    relative_name(name)
    parts = PurePosixPath(name).parts
    require(len(parts) >= 2 and any(parts[0] == p or parts[0].startswith(p + '-') for p in READER_PREFIXES),
            'Only exact terminal reader evidence subtrees are supported')
    require(all(not any(word in p.lower() for word in ('protocol', 'checkpoint', 'plan'))
                and not p.lower().endswith(('.py', '.sh', '.sbatch', '.pt', '.pth')) for p in parts),
            'Source, protocol, plan and checkpoint files must remain unpacked')
    leaf = parts[-1]
    match = re.fullmatch(r'attempt-([1-5])-([a-z-]+)\.(json|txt)', leaf)
    require(leaf in {'intent.json', 'result.json'} or match is not None
            and (match[2] in READER_JSON_KINDS and match[3] == 'json'
                 or match[2] in {'stdout', 'stderr'} and match[3] == 'txt'),
            'Unknown terminal reader leaf is not eligible for compaction')
    return name


def confined(path, root, *, file=False, missing=False):
    root = Path(root).resolve(strict=True)
    original = Path(path).absolute()
    resolved = original.resolve(strict=not missing)
    require(resolved != root and resolved.is_relative_to(root), 'Path is outside the explicit study namespace')
    require(original == resolved and not original.is_symlink(), 'Symlink or noncanonical source path refused')
    for parent in original.parents:
        if parent == root:
            break
        require(not parent.is_symlink(), 'Symlink ancestor refused')
    if file:
        info = original.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, 'Only ordinary single-link files may be packed')
    return resolved


def observation(info):
    return {'device': info.st_dev, 'inode': info.st_ino, 'bytes': info.st_size,
            # Windows st_ctime is creation time and can settle after close;
            # it is not the Unix inode-change counter. Native Linux keeps it.
            'mtime_ns': info.st_mtime_ns, 'ctime_ns': None if os.name == 'nt' else info.st_ctime_ns,
            'mode': stat.S_IMODE(info.st_mode), 'links': info.st_nlink, 'uid': info.st_uid}


def file_hash(path, *, maximum=MAX_MEMBER_BYTES, checkpoint=lambda: None):
    path = Path(path)
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= maximum,
            'Ordinary bounded single-link original required')
    value, total = hashlib.sha256(), 0
    with path.open('rb') as stream:
        opened = observation(os.fstat(stream.fileno()))
        require(opened == observation(before), 'Original changed while opening: ' + path.name + ' ' +
                json.dumps({k: [observation(before)[k], opened[k]] for k in opened if observation(before)[k] != opened[k]}, sort_keys=True))
        while True:
            checkpoint(); chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            require(total <= maximum, 'Finite member byte bound exceeded')
            value.update(chunk)
        require(observation(os.fstat(stream.fileno())) == observation(before), 'Original changed while hashing')
    require(total == before.st_size and observation(path.lstat()) == observation(before), 'Original replaced during hashing')
    return {'bytes': total, 'sha256': value.hexdigest()}, observation(before)


def bound_bytes(locator, root, maximum=MAX_METADATA_BYTES):
    require(set(locator) == {'path', 'sha256'} and is_sha(locator['sha256']), 'Exact path/SHA locator required')
    path = confined(locator['path'], root, file=True)
    with path.open('rb') as stream:
        raw = stream.read(maximum + 1)
    require(len(raw) <= maximum and sha(raw) == locator['sha256'], 'Metadata bytes/SHA binding failed')
    return raw


def bound_json(locator, root):
    return strict_json(bound_bytes(locator, root))


def verify_native(binding, root, *, completed=False):
    """Check the wrapper's byte-bound native proof against selected final ledger rows.

    Owner proof is supplied by the scheduler-verifying wrapper, not invented or
    cryptographically authenticated by this file. The selected final budget row
    must still match; unrelated updates to the mutable ledger are permitted.
    """
    require(set(binding) == {'completion', 'owner'}, 'Completion and original owner/account proof required')
    proof = bound_json(binding['completion'], root)
    owner = bound_json(binding['owner'], root)
    require(proof.get('completion_proof_sha256') == canonical({k: v for k, v in proof.items() if k != 'completion_proof_sha256'})
            and proof.get('native_records_sha256') == canonical(proof.get('native_records'))
            and proof.get('array') is None, 'Native completion proof identity changed')
    job = str(proof.get('job_id', ''))
    require(re.fullmatch('[1-9][0-9]*', job) and set(proof['native_records']) == {job}, 'One exact terminal scalar job required')
    record = proof['native_records'][job]
    require(record.get('state') in TERMINAL and (not completed or record['state'] == 'COMPLETED' and record.get('exit_code') == '0:0')
            and record.get('basis') == 'sacct final allocated wall time'
            and type(record.get('cpus')) is int and record['cpus'] > 0
            and type(record.get('elapsed_seconds')) is int and record['elapsed_seconds'] > 0
            and type(record.get('gpus')) is int and 0 <= record['gpus'] <= 1,
            'Positive terminal allocated native evidence required')
    require(owner.get('job_id') == job and owner.get('user') == 'pwa209'
            and owner.get('account') in {'def-ptewarie_cpu', 'def-ptewarie_gpu'}
            and all(owner.get(k) == record[k] for k in ['state', 'exit_code', 'cpus', 'gpus', 'elapsed_seconds']),
            'Original native owner/account proof differs from final allocation')
    ledger_path = Path(root).resolve() / 'state/resource-budget.json'
    require(Path(proof.get('source_path', '')).resolve() == ledger_path, 'Native proof belongs to another study ledger')
    with ledger_path.open('rb') as stream:
        raw = stream.read(MAX_METADATA_BYTES + 1)
    require(len(raw) <= MAX_METADATA_BYTES and strict_json(raw).get('native_jobs', {}).get(job) == record,
            'Selected final native allocation is missing or changed')
    require(job != os.environ.get('SLURM_JOB_ID'), 'Current active job evidence cannot be compacted')
    return job, record


def verify_authority(authority, source, root, *, policy=False):
    require(authority.get('primary_source_sha256') == PRIMARY and authority.get('full_config_sha256') == CONFIG
            and is_sha(authority.get('execution_source_sha256')), 'Frozen science/config and qualified execution bindings required')
    job, native = verify_native(authority['native'], root, completed=policy)
    result = bound_json(authority['result_receipt'], root)
    require(result.get('native_job_id') == job and result.get('source_sha256') == authority['execution_source_sha256']
            and result.get('full_config_sha256') == CONFIG and result.get('scientific_result') is False
            and result.get('admission_authority') is False, 'Source-bound authoritative native result required')
    if policy:
        require(result.get('status') == 'complete_native_work' and result.get('mode') == 'agent-assembly'
                and result.get('evaluation_case') == str(source), 'Only positively assembled policy may be compacted')
    else:
        require(Path(source).name == 'native-' + job and result.get('status') in {'running', 'complete_native_work'},
                'Only terminal worker reader evidence is supported; current/active controller paths are forbidden')
    return job, native, result


def policy_roster(source, authority, root, checkpoint=lambda: None):
    source = confined(source, root)
    require(re.fullmatch('cell-[0-9]{4}', source.name) and 0 <= int(source.name[5:]) < 240, 'Exact final policy case required')
    job, native, result = verify_authority(authority, source, root, policy=True)
    locators = authority['policy_inputs']
    require(set(locators) == {'plan', 'binding', 'summary'}, 'Exact canonical plan, binding and result summary locators required')
    values = {}
    for key, name in [('plan', 'plan.json'), ('binding', 'plan-binding.json'), ('summary', 'evaluation_summary.json')]:
        require(Path(locators[key]['path']) == source / name, 'Canonical policy locator belongs to another case')
        values[key] = bound_json(locators[key], root)
    plan, binding, summary = (values[k] for k in ['plan', 'binding', 'summary'])
    require(plan.get('canary') is False and plan.get('benchmark_directions') is False
            and plan.get('full_config_sha256') == CONFIG and canonical(plan['config']) == CONFIG
            and plan.get('primary_agents_sha256') == AGENTS and plan.get('evaluation_dtype') == 'float64'
            and len(plan.get('conditions', [])) == 300 and plan.get('scientific_result') is False
            and plan.get('admission_authority') is False, 'Unchanged full final300-condition policy plan required')
    plan_sha = locators['plan']['sha256']
    require(binding.get('plan_sha256') == plan_sha and result.get('evaluation_plan_sha256') == plan_sha
            and result.get('evaluation_binding_sha256') == locators['binding']['sha256']
            and all(binding.get(k) == plan.get(k) for k in ['checkpoint_sha256', 'primary_agents_sha256',
                'execution_module_sha256', 'original_protocol_sha256']), 'Canonical plan/checkpoint/protocol binding changed')
    cp = confined(plan['checkpoint'], root, file=True)
    require(cp.name == 'checkpoint.pt' and file_hash(cp, checkpoint=checkpoint)[0]['sha256'] == plan['checkpoint_sha256'],
            'Original training checkpoint changed')
    require(summary == result.get('result') and summary.get('status') == 'complete_amended_evaluation'
            and summary.get('plan_sha256') == plan_sha and summary.get('conditions') == 300,
            'Authoritative complete assembled result differs')
    assembled = source / summary['artifact_name']
    require(assembled.name.startswith('assembled-') and assembled.suffix == '.parquet'
            and file_hash(confined(assembled, root, file=True), checkpoint=checkpoint)[0]['sha256'] == summary['trial_file_sha256'],
            'Canonical assembled trial artifact changed')
    roster, seen = [], set()
    for cell in summary.get('condition_bindings', []):
        checkpoint(); index = cell.get('condition_index')
        require(type(index) is int and 0 <= index < 300 and index not in seen, 'Unknown/duplicate committed condition')
        seen.add(index)
        receipt, artifact = cell['receipt_name'], cell['artifact_name']
        require(re.fullmatch('condition-[0-9]{5}-[0-9a-f]{32}\\.json', receipt)
                and artifact == receipt[:-5] + '.parquet', 'Exact original condition part naming required')
        value = bound_json({'path': str(source / receipt), 'sha256': cell['receipt_sha256']}, root)
        require(value.get('condition_index') == index and value.get('condition') == plan['conditions'][index]
                and value.get('status') == 'complete' and value.get('plan_sha256') == plan_sha
                and value.get('original_weight_sha256') == plan['original_weight_sha256']
                and value.get('evaluation_weight_sha256') == summary['evaluation_weight_sha256']
                and value.get('artifact_name') == artifact and value.get('artifact_sha256') == cell['artifact_sha256'],
                'Original committed condition provenance changed')
        roster.extend([{'path': receipt, 'sha256': cell['receipt_sha256']},
                       {'path': artifact, 'sha256': cell['artifact_sha256']}])
    require(seen == set(range(300)) and {p.name for p in source.glob('condition-*.json')} == {r['path'] for r in roster if r['path'].endswith('.json')},
            'Every original committed condition must be included exactly once')
    return source, roster


def reader_roster(source, authority, root, checkpoint=lambda: None):
    source = confined(source, root)
    verify_authority(authority, source, root)
    roster = []
    for directory, dirs, files in os.walk(source, followlinks=False):
        checkpoint()
        directory = Path(directory)
        for name in dirs:
            require(not (directory / name).is_symlink(), 'Reader subtree has a symlink')
        for name in files:
            relative = (directory / name).relative_to(source).as_posix()
            first = relative.split('/')[0]
            if not any(first == p or first.startswith(p + '-') for p in READER_PREFIXES):
                continue
            reader_evidence_name(relative)
            proof, observed = file_hash(confined(directory / name, root, file=True), checkpoint=checkpoint)
            roster.append({'path': relative, **proof})
            require(len(roster) <= MAX_FILES, 'Finite reader file roster bound exceeded')
    require(roster, 'No supported terminal reader evidence found')
    return source, roster


def _json_bytes(value):
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    require(len(raw) <= MAX_METADATA_BYTES, 'Finite metadata bound exceeded')
    return raw


def _exclusive_json(path, value):
    raw = _json_bytes(value)
    with Path(path).open('xb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    return sha(raw)


def _atomic_json(path, value):
    temporary = Path(path).with_name(Path(path).name + '.partial-' + uuid.uuid4().hex)
    _exclusive_json(temporary, value)
    os.replace(temporary, path)


def _publish_file(temporary, target):
    # Hard-link publication is exclusive on POSIX, unlike overwriting rename.
    # Both names are in the same freshly created archive directory.
    if os.name == 'nt':
        os.rename(temporary, target)
    else:
        os.link(temporary, target)
        Path(temporary).unlink()


class _HashingReader:
    def __init__(self, stream, checkpoint):
        self.stream, self.checkpoint, self.value, self.bytes = stream, checkpoint, hashlib.sha256(), 0
    def read(self, size=-1):
        self.checkpoint(); raw = self.stream.read(size)
        self.value.update(raw); self.bytes += len(raw)
        return raw


def _make_index(intent, archive_proof):
    return {'schema_version': 1, 'kind': intent['kind'], 'source': intent['source'],
        'helper_sha256': intent['helper_sha256'], 'authority': intent['authority'],
        'members': intent['members'], 'member_roster_sha256': canonical(intent['members']),
        'archive': {'name': 'evidence.tar', 'format': 'deterministic-pax-tar-v1', **archive_proof},
        'source_content_bytes': sum(r['bytes'] for r in intent['members']),
        'path_encoding': 'Exact generated ASCII POSIX relative paths, no normalization or inferred aliases',
        'scientific_result': False, 'admission_authority': False}


class ArchiveReader:
    """Verified, open-file archive view; member reads always recheck exact bytes."""
    def __init__(self, archive_dir, expected_index_sha256, expected_archive_sha256, *, checkpoint=lambda: None):
        checkpoint = _bounded(checkpoint)
        self.directory, self.checkpoint = Path(archive_dir), checkpoint
        require(is_sha(expected_index_sha256) and is_sha(expected_archive_sha256), 'Both immutable archive/index SHA256 values required')
        index_path = self.directory / 'index.json'
        require(not index_path.is_symlink(), 'Index symlink refused')
        with index_path.open('rb') as stream: raw = stream.read(MAX_METADATA_BYTES + 1)
        require(len(raw) <= MAX_METADATA_BYTES and sha(raw) == expected_index_sha256, 'Archive index changed')
        self._index = strict_json(raw)
        index = self._index
        require(index.get('schema_version') == 1 and index.get('scientific_result') is False
                and index.get('admission_authority') is False and index.get('archive', {}).get('name') == 'evidence.tar'
                and index['archive'].get('format') == 'deterministic-pax-tar-v1'
                and index['archive'].get('sha256') == expected_archive_sha256, 'Archive/index binding changed')
        members = index.get('members')
        require(isinstance(members, list) and 0 < len(members) <= MAX_FILES
                and canonical(members) == index.get('member_roster_sha256'), 'Immutable finite member roster required')
        names = [relative_name(r['path']) for r in members]
        require(names == sorted(names) and len(set(names)) == len(names)
                and all(type(r.get('bytes')) is int and 0 <= r['bytes'] <= MAX_MEMBER_BYTES and is_sha(r.get('sha256')) for r in members)
                and sum(r['bytes'] for r in members) == index.get('source_content_bytes') <= MAX_TOTAL_BYTES,
                'Malformed or excessive exact member inventory')
        self.members = {r['path']: dict(r) for r in members}
        path = self.directory / 'evidence.tar'
        require(not path.is_symlink() and path.is_file(), 'Ordinary archive required')
        self.stream = path.open('rb')
        try:
            before = os.fstat(self.stream.fileno())
            require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1, 'Archive must be ordinary single-link evidence')
            reader = _HashingReader(self.stream, checkpoint)
            while reader.read(1024 * 1024):
                require(reader.bytes <= MAX_ARCHIVE_BYTES, 'Finite archive bound exceeded')
            require(reader.value.hexdigest() == expected_archive_sha256 and reader.bytes == index['archive'].get('bytes')
                    and observation(os.fstat(self.stream.fileno())) == observation(before), 'Archive bytes changed')
            self.stream.seek(0)
            self.tar = tarfile.open(fileobj=self.stream, mode='r:')
            actual = {}
            for member in self.tar:
                checkpoint()
                require(member.name in self.members and member.name not in actual and member.type == tarfile.REGTYPE
                        and not member.issym() and not member.islnk() and member.size == self.members[member.name]['bytes']
                        and member.uid == member.gid == member.mtime == 0 and member.mode == 0o444
                        and not member.uname and not member.gname and set(member.pax_headers) <= {'path'},
                        'Unsafe, duplicate, unbound or nondeterministic tar member')
                actual[member.name] = member
                require(self._read_info(member, retain=False)[0] == self.members[member.name]['sha256'], 'Archived member SHA differs')
            require(set(actual) == set(self.members), 'Missing or extra archived members')
            last = list(actual.values())[-1]
            data_end = last.offset_data + ((last.size + 511) // 512) * 512
            self.stream.seek(data_end)
            trailer = self.stream.read(10241)
            require(1024 <= len(trailer) <= 10240 and not any(trailer)
                    and reader.bytes % 10240 == 0, 'Noncanonical or hidden trailing archive data')
            self.infos = actual
            require(observation(os.fstat(self.stream.fileno())) == observation(before), 'Archive changed during verification')
        except BaseException:
            self.stream.close()
            raise
    @property
    def index(self):
        return strict_json(_json_bytes(self._index))
    def _read_info(self, info, *, retain):
        value, total, chunks = hashlib.sha256(), 0, []
        with self.tar.extractfile(info) as stream:
            while True:
                self.checkpoint(); chunk = stream.read(1024 * 1024)
                if not chunk: break
                total += len(chunk); require(total <= info.size, 'Expanded member exceeds declared bound')
                value.update(chunk)
                if retain: chunks.append(chunk)
        require(total == info.size, 'Truncated archived member')
        return value.hexdigest(), b''.join(chunks) if retain else None
    def read_member(self, name):
        require(name in self.infos, 'Unknown exact archive member')
        require(self.infos[name].size <= MAX_METADATA_BYTES, 'Large member requires bounded streaming copy_member')
        value, raw = self._read_info(self.infos[name], retain=True)
        require(value == self.members[name]['sha256'], 'Member changed after archive verification')
        return raw
    def copy_member(self, name, output):
        require(name in self.infos, 'Unknown exact archive member')
        info = self.infos[name]; value, total = hashlib.sha256(), 0
        with self.tar.extractfile(info) as stream:
            while True:
                self.checkpoint(); chunk = stream.read(1024 * 1024)
                if not chunk: break
                total += len(chunk); require(total <= info.size, 'Expanded member exceeds declared bound')
                value.update(chunk); output.write(chunk)
        require(total == info.size and value.hexdigest() == self.members[name]['sha256'], 'Streamed member identity changed')
        return {'path': name, 'bytes': total, 'sha256': value.hexdigest()}
    def close(self):
        self.tar.close(); self.stream.close()
    def __enter__(self): return self
    def __exit__(self, *args): self.close()


def verify_archive(archive_dir, expected_index_sha256, expected_archive_sha256, *, checkpoint=lambda: None):
    with ArchiveReader(archive_dir, expected_index_sha256, expected_archive_sha256, checkpoint=checkpoint) as reader:
        return {'status': 'verified_exact_archive', 'index_sha256': expected_index_sha256,
            'archive_sha256': expected_archive_sha256, 'files': len(reader.members),
            'source_bytes': reader.index['source_content_bytes'], 'scientific_result': False, 'admission_authority': False}


def verify_policy_archive(archive_dir, expected_index_sha256, expected_archive_sha256, *, root=ROOT, checkpoint=lambda: None):
    """Bind the archive to live preserved canonical inputs and positive assembly."""
    with ArchiveReader(archive_dir, expected_index_sha256, expected_archive_sha256, checkpoint=checkpoint) as reader:
        index = reader.index
        require(index['kind'] == 'completed-final-policy-conditions', 'Completed policy archive required')
        source = confined(index['source'], root)
        authority = index['authority']
        _, _, result = verify_authority(authority, source, root, policy=True)
        values = {}
        for key, name in [('plan', 'plan.json'), ('binding', 'plan-binding.json'), ('summary', 'evaluation_summary.json')]:
            locator = authority['policy_inputs'][key]
            require(Path(locator['path']) == source / name, 'Canonical input moved to another case')
            values[key] = bound_json(locator, root)
        plan, binding, summary = (values[k] for k in ['plan', 'binding', 'summary'])
        require(plan.get('canary') is False and plan.get('benchmark_directions') is False
                and plan.get('full_config_sha256') == CONFIG and canonical(plan['config']) == CONFIG
                and plan.get('primary_agents_sha256') == AGENTS and plan.get('evaluation_dtype') == 'float64'
                and len(plan.get('conditions', [])) == 300
                and binding.get('plan_sha256') == authority['policy_inputs']['plan']['sha256']
                and summary == result.get('result') and summary.get('status') == 'complete_amended_evaluation'
                and summary.get('conditions') == 300, 'Preserved full policy source/result identity changed')
        for key in ['checkpoint_sha256', 'primary_agents_sha256', 'execution_module_sha256', 'original_protocol_sha256']:
            require(binding.get(key) == plan.get(key), 'Preserved protocol/checkpoint binding changed')
        checkpoint_path = confined(plan['checkpoint'], root, file=True)
        require(checkpoint_path.name == 'checkpoint.pt' and file_hash(checkpoint_path, checkpoint=checkpoint)[0]['sha256'] == plan['checkpoint_sha256'],
                'Preserved original training checkpoint changed')
        artifact = confined(source / summary['artifact_name'], root, file=True)
        require(artifact.parent == source and artifact.name.startswith('assembled-') and artifact.suffix == '.parquet'
                and file_hash(artifact, checkpoint=checkpoint)[0]['sha256'] == summary['trial_file_sha256'],
                'Preserved assembled trial artifact changed')
        expected = {}
        conditions = summary.get('condition_bindings', [])
        require(len(conditions) == 300 and {c['condition_index'] for c in conditions} == set(range(300)), 'Assembled condition roster changed')
        for condition in conditions:
            require(condition['receipt_name'] not in expected and condition['artifact_name'] not in expected,
                    'Duplicate authoritative source member')
            expected[condition['receipt_name']] = condition['receipt_sha256']
            expected[condition['artifact_name']] = condition['artifact_sha256']
        require({n: r['sha256'] for n, r in reader.members.items()} == expected, 'Archive differs from exact authoritative condition members')
        return {'status': 'verified_complete_policy_archive', 'source': str(source),
            'plan': plan, 'summary': summary, 'index_sha256': expected_index_sha256,
            'archive_sha256': expected_archive_sha256, 'scientific_result': False, 'admission_authority': False}


def unpack(archive_dir, destination, expected_index_sha256, expected_archive_sha256, *, checkpoint=lambda: None):
    destination = Path(destination).absolute()
    require(not destination.exists() and not destination.is_symlink()
            and destination.parent.resolve(strict=True) == destination.parent, 'Fresh ordinary extraction destination required')
    destination.mkdir(mode=0o700)
    result = []
    with ArchiveReader(archive_dir, expected_index_sha256, expected_archive_sha256, checkpoint=checkpoint) as reader:
        for name in sorted(reader.members):
            checkpoint(); target = destination / name
            require(target.resolve().is_relative_to(destination), 'Extraction path escapes its fresh destination')
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open('xb') as stream:
                reader.copy_member(name, stream); stream.flush(); os.fsync(stream.fileno())
            measured, _ = file_hash(target, checkpoint=checkpoint)
            require(measured == {k: reader.members[name][k] for k in ['bytes', 'sha256']}, 'Independent extracted-byte round trip failed')
            result.append({'path': name, **measured})
    return result


def _cleanup_roundtrip(destination, roster):
    # Only this pack's fresh scratch copies; no recursive delete or source paths.
    destination = Path(destination).resolve(strict=True)
    actual = {p.relative_to(destination).as_posix() for p in destination.rglob('*') if p.is_file() or p.is_symlink()}
    require(actual == {r['path'] for r in roster}, 'Unexpected round-trip scratch entry; preserve for reconciliation')
    for record in roster:
        target = confined(destination / record['path'], destination, file=True)
        require(file_hash(target)[0] == {k: record[k] for k in ['bytes', 'sha256']}, 'Round-trip scratch changed; preserve it')
    for record in roster: (destination / record['path']).unlink()
    for directory in sorted((p for p in destination.rglob('*') if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
        require(not directory.is_symlink() and directory.resolve().is_relative_to(destination), 'Scratch directory changed')
        directory.rmdir()
    destination.rmdir()


def _complete_pack(archive_dir, intent, archive_proof, *, checkpoint=lambda: None):
    archive_dir = Path(archive_dir)
    index = _make_index(intent, archive_proof)
    raw = _json_bytes(index); index_sha = sha(raw)
    index_path = archive_dir / 'index.json'
    if index_path.exists(): require(index_path.read_bytes() == raw, 'Existing index differs; no publication replay')
    else: _exclusive_json(index_path, index)
    verify_archive(archive_dir, index_sha, archive_proof['sha256'], checkpoint=checkpoint)
    roundtrip_path = archive_dir / 'roundtrip.json'
    if roundtrip_path.exists():
        roundtrip = strict_json(roundtrip_path.read_bytes())
        require(roundtrip.get('index_sha256') == index_sha and roundtrip.get('archive_sha256') == archive_proof['sha256']
                and roundtrip.get('member_roster_sha256') == canonical(intent['members'])
                and roundtrip.get('independent_extracted_bytes_verified') is True, 'Existing round-trip proof differs')
    else:
        scratch = archive_dir / ('roundtrip-' + uuid.uuid4().hex)
        restored = unpack(archive_dir, scratch, index_sha, archive_proof['sha256'], checkpoint=checkpoint)
        require(restored == intent['members'], 'Exact extracted source manifest differs')
        roundtrip = {'index_sha256': index_sha, 'archive_sha256': archive_proof['sha256'],
            'member_roster_sha256': canonical(restored), 'independent_extracted_bytes_verified': True,
            'files': len(restored), 'bytes': sum(r['bytes'] for r in restored)}
        _exclusive_json(roundtrip_path, roundtrip)
        _cleanup_roundtrip(scratch, restored)
    receipt = {'status': 'verified_archive_originals_retained', 'helper_sha256': intent['helper_sha256'],
        'index_sha256': index_sha, 'archive_sha256': archive_proof['sha256'],
        'intent_sha256': sha((archive_dir / 'pack-intent.json').read_bytes()),
        'roundtrip_sha256': sha(roundtrip_path.read_bytes()), 'files': len(intent['members']),
        'original_removal_authorized': False, 'scientific_result': False, 'admission_authority': False}
    path = archive_dir / 'pack-receipt.json'
    if path.exists(): require(strict_json(path.read_bytes()) == receipt, 'Existing pack receipt differs')
    else: _exclusive_json(path, receipt)
    return receipt


def _create_pack(source, roster, archive_dir, *, kind, authority, root, checkpoint=lambda: None):
    source, archive_dir = confined(source, root), confined(archive_dir, root, missing=True)
    require(not archive_dir.exists() and not archive_dir.is_relative_to(source), 'Fresh separate archive directory required')
    require(0 < len(roster) <= MAX_FILES, 'Finite nonempty exact source roster required')
    members, observations = [], {}
    for record in sorted(roster, key=lambda r: r['path']):
        name = relative_name(record['path']); path = confined(source / name, root, file=True)
        require(path.is_relative_to(source), 'Source member escapes its bound case')
        measured, observed = file_hash(path, checkpoint=checkpoint)
        require(is_sha(record.get('sha256')) and measured['sha256'] == record['sha256'], 'Frozen source member changed')
        members.append({'path': name, **measured}); observations[name] = observed
    require(len({r['path'] for r in members}) == len(members)
            and sum(r['bytes'] for r in members) <= MAX_TOTAL_BYTES, 'Duplicate/oversized source roster')
    intent = {'schema_version': 1, 'kind': kind, 'source': str(source), 'helper_sha256': helper_sha(),
        'authority': authority, 'members': members, 'source_observations': observations,
        'native_wrapper_storage_admission_required': True,
        'maximum_additional_peak_bytes': 2 * sum(r['bytes'] for r in members) + 2 * len(members) * 2048 + 3 * MAX_METADATA_BYTES,
        'maximum_additional_peak_files': len(members) + 12,
        'scientific_result': False, 'admission_authority': False}
    archive_dir.mkdir(mode=0o700)
    _exclusive_json(archive_dir / 'pack-intent.json', intent)
    temporary = archive_dir / 'evidence.tar.partial'
    with temporary.open('xb') as target:
        with tarfile.open(fileobj=target, mode='w', format=tarfile.PAX_FORMAT) as archive:
            for record in members:
                checkpoint(); path = confined(source / record['path'], root, file=True)
                require(observation(path.lstat()) == observations[record['path']], 'Source changed before archiving')
                info = tarfile.TarInfo(record['path']); info.size = record['bytes']
                info.uid = info.gid = info.mtime = 0; info.mode = 0o444; info.uname = info.gname = ''
                with path.open('rb') as stream:
                    require(observation(os.fstat(stream.fileno())) == observations[record['path']], 'Source changed while opening')
                    reader = _HashingReader(stream, checkpoint); archive.addfile(info, reader)
                    require(reader.bytes == record['bytes'] and reader.value.hexdigest() == record['sha256']
                            and stream.read(1) == b'' and observation(os.fstat(stream.fileno())) == observations[record['path']],
                            'Archived source bytes changed')
                require(observation(path.lstat()) == observations[record['path']], 'Source replaced after archiving')
        target.flush(); os.fsync(target.fileno())
    archive_proof, _ = file_hash(temporary, maximum=MAX_ARCHIVE_BYTES, checkpoint=checkpoint)
    _publish_file(temporary, archive_dir / 'evidence.tar')
    return _complete_pack(archive_dir, intent, archive_proof, checkpoint=checkpoint)


def plan_policy_pack(source, *, authority, root=ROOT, checkpoint=lambda: None):
    """Read-only frozen roster and peak demand for the wrapper's fresh quota gate."""
    checkpoint = _bounded(checkpoint)
    source = confined(source, root)
    with exclusive_file_lock(source / 'execution.lock', timeout=0):
        source, roster = policy_roster(source, authority, root, checkpoint)
        members = []
        for row in sorted(roster, key=lambda r: r['path']):
            proof, _ = file_hash(source / row['path'], checkpoint=checkpoint)
            members.append({'path': row['path'], **proof})
    total = sum(r['bytes'] for r in members)
    require(len(members) <= MAX_FILES and total <= MAX_TOTAL_BYTES, 'Finite complete archive plan required')
    return {'status': 'planned_not_admitted', 'helper_sha256': helper_sha(), 'source': str(source),
        'members': members, 'roster_sha256': canonical(members), 'source_bytes': total,
        'maximum_additional_peak_bytes': 2*total + 2*len(members)*2048 + 3*MAX_METADATA_BYTES,
        'maximum_additional_peak_regular_files': len(members)+12,
        'scientific_result': False, 'admission_authority': False}


def pack_policy(source, archive_dir, *, authority, root=ROOT, expected_roster_sha256=None, checkpoint=lambda: None):
    checkpoint = _bounded(checkpoint)
    source = confined(source, root)
    with exclusive_file_lock(source / 'execution.lock', timeout=0):
        source, roster = policy_roster(source, authority, root, checkpoint)
        if expected_roster_sha256 is not None:
            measured = [{'path': r['path'], **file_hash(source / r['path'], checkpoint=checkpoint)[0]}
                        for r in sorted(roster, key=lambda r: r['path'])]
            require(is_sha(expected_roster_sha256) and canonical(measured) == expected_roster_sha256,
                    'Source roster changed after native storage planning')
        return _create_pack(source, roster, archive_dir, kind='completed-final-policy-conditions', authority=authority, root=root, checkpoint=checkpoint)


def pack_terminal_readers(source, archive_dir, *, authority, root=ROOT, checkpoint=lambda: None):
    checkpoint = _bounded(checkpoint)
    source, roster = reader_roster(source, authority, root, checkpoint)
    return _create_pack(source, roster, archive_dir, kind='terminal-native-worker-reader-evidence', authority=authority, root=root, checkpoint=checkpoint)


def resume_published_pack(archive_dir, *, expected_intent_sha256, expected_archive_sha256,
                          explicit_reconcile=False, root=ROOT, checkpoint=lambda: None):
    require(explicit_reconcile is True, 'Interrupted pack requires explicit byte-bound reconciliation')
    archive_dir = confined(archive_dir, root)
    intent = bound_json({'path': str(archive_dir / 'pack-intent.json'), 'sha256': expected_intent_sha256}, root)
    require(intent['helper_sha256'] == helper_sha(), 'Interrupted archive belongs to another helper source')
    verify_authority(intent['authority'], Path(intent['source']), root, policy=intent['kind'] == 'completed-final-policy-conditions')
    archive_proof, _ = file_hash(archive_dir / 'evidence.tar', maximum=MAX_ARCHIVE_BYTES, checkpoint=checkpoint)
    require(archive_proof['sha256'] == expected_archive_sha256, 'Explicit published archive identity differs')
    return _complete_pack(archive_dir, intent, archive_proof, checkpoint=checkpoint)


def committed_conditions(archive_dir, expected_index_sha256, expected_archive_sha256, *, root=ROOT, checkpoint=lambda: None):
    """Original raw receipt SHA/value packets; canonical assembled trials stay live."""
    verified = verify_policy_archive(archive_dir, expected_index_sha256, expected_archive_sha256, root=root, checkpoint=checkpoint)
    with ArchiveReader(archive_dir, expected_index_sha256, expected_archive_sha256, checkpoint=checkpoint) as reader:
        index = reader.index
        require(index['kind'] == 'completed-final-policy-conditions', 'Policy condition archive required')
        summary, plan = verified['summary'], verified['plan']
        # The archive's complete authority is revalidated by native callers via
        # verify_policy_archive; this reader preserves exact raw packet identity.
        result = []
        for binding in sorted(summary['condition_bindings'], key=lambda r: r['condition_index']):
            raw = reader.read_member(binding['receipt_name'])
            require(sha(raw) == binding['receipt_sha256'], 'Summary/archived receipt identity differs')
            require(reader.members[binding['artifact_name']]['sha256'] == binding['artifact_sha256'], 'Summary/archived artifact differs')
            value = strict_json(raw)
            i = binding['condition_index']
            require(value.get('condition_index') == i and value.get('condition') == plan['conditions'][i]
                    and value.get('status') == 'complete'
                    and value.get('plan_sha256') == index['authority']['policy_inputs']['plan']['sha256']
                    and value.get('original_weight_sha256') == plan['original_weight_sha256']
                    and value.get('evaluation_weight_sha256') == summary['evaluation_weight_sha256']
                    and value.get('artifact_name') == binding['artifact_name']
                    and value.get('artifact_sha256') == binding['artifact_sha256'], 'Archived condition provenance differs')
            result.append({'sha256': sha(raw), 'value': value})
        require(len(result) == 300 and {r['value']['condition_index'] for r in result} == set(range(300)), 'Full300-condition roster required')
        return result


def _verify_canary(locator, root):
    canary = bound_json(locator, root)
    require(canary.get('status') == 'complete_verified_native_archive_canary'
            and canary.get('helper_sha256') == helper_sha() and canary.get('roundtrip_verified') is True
            and canary.get('unchanged_source_removal_guards_verified') is True
            and canary.get('scientific_result') is False and canary.get('admission_authority') is False,
            'Separate positive native pack/removal canary for this exact helper required')
    counts = canary.get('test_counts', {})
    require(EXPECTED_CANARY_TEST_NAMES and is_sha(EXPECTED_CANARY_TEST_SHA256)
            and type(counts.get('tests')) is int and counts['tests'] == len(EXPECTED_CANARY_TEST_NAMES)
            and all(type(counts.get(k)) is int and counts[k] == 0 for k in ['failures', 'errors', 'skipped']),
            'Positive exact native archive-canary test result required')
    source = canary.get('test_source', {})
    require(source.get('sha256') == EXPECTED_CANARY_TEST_SHA256
            and Path(source.get('path', '')).name == 'test_verified_evidence_archive_20261021.py',
            'Exact separately pinned archive-canary test source required')
    bound_bytes(source, root, 2_000_000)
    raw_junit = bound_bytes(canary['junit'], root, 8_000_000)
    require(b'<!DOCTYPE' not in raw_junit.upper() and b'<!ENTITY' not in raw_junit.upper(), 'JUnit entity declarations refused')
    junit = ET.fromstring(raw_junit)
    cases = list(junit.iter('testcase'))
    names = [case.get('name') for case in cases]
    require(len(cases) == len(EXPECTED_CANARY_TEST_NAMES) and len(set(names)) == len(names)
            and set(names) == set(EXPECTED_CANARY_TEST_NAMES)
            and all(case.get('classname', '').endswith('test_verified_evidence_archive_20261021')
                and not any(case.find(key) is not None for key in ['failure', 'error', 'skipped']) for case in cases)
            and canary.get('test_roster_sha256') == canonical(sorted(names)),
            'Actual full archive-canary JUnit test roster must pass without omitted/failed/skipped cases')
    job, record = verify_native(canary['native'], root, completed=True)
    require(canary.get('native_job_id') == job, 'Canary producer native identity differs')
    return canary


def remove_verified_originals(archive_dir, expected_index_sha256, expected_archive_sha256, *,
        removal_canary, root=ROOT, explicit_reconcile=False, expected_journal_sha256=None,
        checkpoint=lambda: None, after_unlink=None):
    """Explicit irreversible compaction, never automatic after an interruption.

    The canary and wrapper's native terminal proof are mandatory. A crash after
    unlink but before journal publication is reconciled only on an explicit new
    call carrying the journal's exact current byte SHA. The verified archive is
    always the surviving authoritative copy; no original pathname is reused.
    """
    checkpoint = _bounded(checkpoint)
    archive_dir = confined(archive_dir, root)
    with ExitStack() as guards:
        guards.enter_context(exclusive_file_lock(archive_dir / 'removal.mutex', timeout=0))
        _verify_canary(removal_canary, root)
        intent = bound_json({'path': str(archive_dir / 'pack-intent.json'),
            'sha256': strict_json((archive_dir / 'pack-receipt.json').read_bytes())['intent_sha256']}, root)
        require(intent['helper_sha256'] == helper_sha(), 'Removal must use this exact canary-qualified helper')
        source = confined(intent['source'], root)
        if intent['kind'] == 'completed-final-policy-conditions':
            guards.enter_context(exclusive_file_lock(source / 'execution.lock', timeout=0))
        verify_authority(intent['authority'], source, root, policy=intent['kind'] == 'completed-final-policy-conditions')
        verified = verify_archive(archive_dir, expected_index_sha256, expected_archive_sha256, checkpoint=checkpoint)
        if intent['kind'] == 'completed-final-policy-conditions':
            verify_policy_archive(archive_dir, expected_index_sha256, expected_archive_sha256, root=root, checkpoint=checkpoint)
        else:
            require(intent['kind'] == 'terminal-native-worker-reader-evidence', 'Unknown compaction domain refused')
        receipt = strict_json((archive_dir / 'pack-receipt.json').read_bytes())
        require(receipt.get('status') == 'verified_archive_originals_retained'
                and receipt.get('index_sha256') == expected_index_sha256
                and receipt.get('archive_sha256') == expected_archive_sha256
                and receipt.get('roundtrip_sha256') == sha((archive_dir / 'roundtrip.json').read_bytes()), 'Published round-trip pack proof changed')
        journal_path = archive_dir / 'removal-journal.json'
        if journal_path.exists():
            require(explicit_reconcile is True and is_sha(expected_journal_sha256), 'Prior removal is ambiguous; explicit reconciliation required')
            journal = bound_json({'path': str(journal_path), 'sha256': expected_journal_sha256}, root)
            require(journal['index_sha256'] == expected_index_sha256 and journal['archive_sha256'] == expected_archive_sha256
                    and journal['intent_sha256'] == receipt['intent_sha256'], 'Removal journal belongs to another archive')
            previous = journal.get('native_job_id')
            if previous is not None:
                ledger = strict_json((Path(root) / 'state/resource-budget.json').read_bytes())
                require(previous != os.environ.get('SLURM_JOB_ID') and ledger.get('native_jobs', {}).get(previous, {}).get('state') in TERMINAL,
                        'Previous native removal allocation must be terminal before reconciliation')
            journal.setdefault('reconciliation_attempts', []).append({
                'prior_journal_sha256': expected_journal_sha256,
                'previous_native_job_id': previous, 'native_job_id': os.environ.get('SLURM_JOB_ID'),
                'canary': removal_canary})
            journal['native_job_id'] = os.environ.get('SLURM_JOB_ID')
            journal['status'] = 'explicit_reconciliation_intent'; journal['revision'] += 1
            _atomic_json(journal_path, journal)
        else:
            require(not explicit_reconcile and expected_journal_sha256 is None, 'No existing removal journal to reconcile')
            journal = {'schema_version': 1, 'status': 'removal_intent', 'revision': 0,
                'index_sha256': expected_index_sha256, 'archive_sha256': expected_archive_sha256,
                'intent_sha256': receipt['intent_sha256'], 'native_job_id': os.environ.get('SLURM_JOB_ID'),
                'removed': [], 'reconciled_absent_after_interruption': [], 'canary': removal_canary,
                'scientific_result': False, 'admission_authority': False}
            _exclusive_json(journal_path, journal)
        expected = {r['path']: r for r in intent['members']}
        if intent['kind'] == 'terminal-native-worker-reader-evidence':
            for name in expected:
                reader_evidence_name(name)
        require(len(set(journal['removed'])) == len(journal['removed']) and set(journal['removed']) <= set(expected), 'Unbound removal progress')
        try:
            # Check EVERY still-present original before unlinking the first one.
            for name, record in expected.items():
                checkpoint(); path = source / relative_name(name)
                if not path.exists():
                    require(name in journal['removed'] or explicit_reconcile, 'Original absent before first removal; preserve evidence')
                    continue
                require(name not in journal['removed'], 'Removed original path was recreated; reconciliation required')
                path = confined(path, root, file=True)
                measured, observed = file_hash(path, checkpoint=checkpoint)
                require(measured == {k: record[k] for k in ['bytes', 'sha256']}
                        and observed == intent['source_observations'][name], 'Original changed since independent archive round trip')
            for name, record in expected.items():
                checkpoint()
                if name in journal['removed']: continue
                path = source / relative_name(name)
                if path.exists():
                    path = confined(path, root, file=True)
                    measured, observed = file_hash(path, checkpoint=checkpoint)
                    require(measured == {k: record[k] for k in ['bytes', 'sha256']}
                            and observed == intent['source_observations'][name], 'Original changed immediately before unlink')
                    path.unlink()
                    if after_unlink is not None: after_unlink(name)
                else:
                    require(explicit_reconcile is True, 'Unexpected absent original cannot be adopted automatically')
                    journal['reconciled_absent_after_interruption'].append(name)
                require(not path.exists() and not path.is_symlink(), 'Original unlink did not finish')
                journal['removed'].append(name); journal['revision'] += 1
                journal['status'] = 'removing_originals'; _atomic_json(journal_path, journal)
            journal['status'] = 'complete_originals_compacted'; journal['revision'] += 1
            _atomic_json(journal_path, journal)
            return journal
        except BaseException as error:
            journal.update(status='interrupted_explicit_reconciliation_required', error_type=type(error).__name__, error=str(error)[:500])
            journal['revision'] += 1; _atomic_json(journal_path, journal)
            raise
