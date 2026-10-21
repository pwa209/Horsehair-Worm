"""All originals are disposable local fixtures; no cluster calls or real deletion."""
from pathlib import Path
import copy
import hashlib
import importlib.util
import io
import json
import os
import tarfile
import xml.etree.ElementTree as ET

import pytest
from horsehair import agents


SPEC = importlib.util.spec_from_file_location('private_archive', Path(__file__).with_name('verified-evidence-archive-20261021.py'))
a = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(a)
EXECUTION = '4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'


def write(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(value, sort_keys=True, indent=2) + '\n').encode()
    path.write_bytes(raw)
    return {'path': str(path), 'sha256': a.sha(raw)}


def native(root, job='100', *, state='COMPLETED', current=None):
    record = {'state': state, 'exit_code': '0:0' if state == 'COMPLETED' else '1:0',
        'elapsed_seconds': 12, 'cpus': 1, 'gpus': 0, 'job_id': job, 'job_id_raw': job,
        'cpu_core_hours': 12/3600, 'gpu_hours': 0., 'basis': 'sacct final allocated wall time'}
    ledger_path = root / 'state/resource-budget.json'
    ledger = json.loads(ledger_path.read_bytes()) if ledger_path.exists() else {'native_jobs': {}}
    ledger['native_jobs'][job] = record; write(ledger_path, ledger)
    proof = {'job_id': job, 'array': None, 'dependency_kind': 'afterok' if state == 'COMPLETED' else 'afterany',
        'source_path': str(ledger_path), 'source_file_sha256_at_capture': a.sha(ledger_path.read_bytes()),
        'native_records': {job: record}, 'native_records_sha256': a.canonical({job: record}),
        'basis': 'Positive persisted final sacct records for every declared task; require COMPLETED/0:0 for afterok'}
    proof['completion_proof_sha256'] = a.canonical(proof)
    owner = {'job_id': job, 'state': state, 'exit_code': record['exit_code'], 'cpus': 1, 'gpus': 0,
             'elapsed_seconds': 12, 'user': 'pwa209', 'account': 'def-ptewarie_cpu'}
    return {'completion': write(root / 'maintenance/proofs' / (job + '-completion.json'), proof),
            'owner': write(root / 'maintenance/proofs' / (job + '-owner.json'), owner)}


@pytest.fixture
def policy(tmp_path, monkeypatch):
    monkeypatch.delenv('SLURM_JOB_ID', raising=False)
    root = tmp_path / 'study'; source = root / 'maintenance/campaign/agent-final-f64-v1/cell-0000'
    source.mkdir(parents=True)
    (source / 'execution.lock').write_bytes(b'\0')
    checkpoint = root / 'results/original/final/policy/checkpoint.pt'
    checkpoint.parent.mkdir(parents=True); checkpoint.write_bytes(b'original checkpoint bytes\x00\xff')
    config = json.loads((Path(agents.__file__).resolve().parents[2] / 'configs/study.json').read_bytes())
    plan = {'canary': False, 'benchmark_directions': False, 'config': config, 'full_config_sha256': a.CONFIG,
        'primary_agents_sha256': a.AGENTS, 'evaluation_dtype': 'float64', 'conditions': [{'index': i} for i in range(300)],
        'checkpoint': str(checkpoint), 'checkpoint_sha256': a.sha(checkpoint.read_bytes()),
        'execution_module_sha256': 'e'*64, 'original_protocol_sha256': 'f'*64,
        'original_weight_sha256': 'a'*64, 'scientific_result': False, 'admission_authority': False}
    plan_loc = write(source / 'plan.json', plan)
    binding = {k: plan[k] for k in ['checkpoint_sha256', 'primary_agents_sha256', 'execution_module_sha256', 'original_protocol_sha256']}
    binding['plan_sha256'] = plan_loc['sha256']; binding_loc = write(source / 'plan-binding.json', binding)
    conditions = []
    for i in range(300):
        stem = 'condition-%05d-%032x' % (i, i+1)
        artifact = source / (stem + '.parquet'); artifact.write_bytes(b'EXACT ORIGINAL PARQUET BYTES\x00' + str(i).encode())
        receipt = {'condition_index': i, 'condition': plan['conditions'][i], 'status': 'complete',
            'plan_sha256': plan_loc['sha256'], 'original_weight_sha256': plan['original_weight_sha256'],
            'evaluation_weight_sha256': 'b'*64, 'artifact_name': artifact.name,
            'artifact_sha256': a.sha(artifact.read_bytes()), 'restoration': {'original': 'literal'},
            'slurm_job_id': '99', 'rows': 32}
        receipt_loc = write(source / (stem + '.json'), receipt)
        conditions.append({'condition_index': i, 'receipt_name': Path(receipt_loc['path']).name,
            'receipt_sha256': receipt_loc['sha256'], 'artifact_name': artifact.name,
            'artifact_sha256': receipt['artifact_sha256']})
    assembled = source / 'assembled-original.parquet'; assembled.write_bytes(b'PRESERVED ORIGINAL ASSEMBLED TRIALS')
    summary = {'status': 'complete_amended_evaluation', 'plan_sha256': plan_loc['sha256'],
        'conditions': 300, 'artifact_name': assembled.name, 'trial_file_sha256': a.sha(assembled.read_bytes()),
        'condition_bindings': conditions, 'evaluation_weight_sha256': 'b'*64, 'trial_rows': 9600}
    summary_loc = write(source / 'evaluation_summary.json', summary)
    result = {'native_job_id': '100', 'source_sha256': EXECUTION, 'full_config_sha256': a.CONFIG,
        'scientific_result': False, 'admission_authority': False, 'status': 'complete_native_work',
        'mode': 'agent-assembly', 'evaluation_case': str(source), 'evaluation_plan_sha256': plan_loc['sha256'],
        'evaluation_binding_sha256': binding_loc['sha256'], 'result': summary}
    authority = {'primary_source_sha256': a.PRIMARY, 'full_config_sha256': a.CONFIG, 'execution_source_sha256': EXECUTION,
        'native': native(root), 'result_receipt': write(root / 'maintenance/campaign/native-100/receipt.json', result),
        'policy_inputs': {'plan': plan_loc, 'binding': binding_loc, 'summary': summary_loc}}
    return root, source, authority


def pack(policy, name='archive'):
    root, source, authority = policy
    destination = root / 'maintenance' / name
    receipt = a.pack_policy(source, destination, authority=authority, root=root)
    return destination, receipt


def canary(root):
    test_raw = Path(__file__).read_bytes()
    source = root / 'maintenance/proofs/test_verified_evidence_archive_20261021.py'
    source.parent.mkdir(parents=True, exist_ok=True); source.write_bytes(test_raw)
    suite = ET.Element('testsuite', tests=str(len(a.EXPECTED_CANARY_TEST_NAMES)), failures='0', errors='0', skipped='0')
    for name in a.EXPECTED_CANARY_TEST_NAMES:
        ET.SubElement(suite, 'testcase', classname='test_verified_evidence_archive_20261021', name=name)
    junit = root / 'maintenance/proofs/archive-canary.xml'; junit.write_bytes(ET.tostring(suite))
    return write(root / 'maintenance/proofs/canary.json', {
        'status': 'complete_verified_native_archive_canary', 'helper_sha256': a.helper_sha(),
        'roundtrip_verified': True, 'unchanged_source_removal_guards_verified': True,
        'scientific_result': False, 'admission_authority': False,
        'test_counts': {'tests': len(a.EXPECTED_CANARY_TEST_NAMES), 'failures': 0, 'errors': 0, 'skipped': 0},
        'test_source': {'path': str(source), 'sha256': a.sha(test_raw)},
        'junit': {'path': str(junit), 'sha256': a.sha(junit.read_bytes())},
        'test_roster_sha256': a.canonical(sorted(a.EXPECTED_CANARY_TEST_NAMES)),
        'native': native(root, '200'), 'native_job_id': '200'})


def test_full_policy_roundtrip_retains_every_original_and_canonical_input(policy):
    root, source, authority = policy
    before = {p.name: p.read_bytes() for p in source.iterdir() if not p.name.startswith('execution.lock')}
    destination, receipt = pack(policy)
    assert receipt['status'] == 'verified_archive_originals_retained'
    assert receipt['files'] == 600 and not receipt['original_removal_authorized']
    assert before == {p.name: p.read_bytes() for p in source.iterdir() if not p.name.startswith('execution.lock')}
    result = a.verify_policy_archive(destination, receipt['index_sha256'], receipt['archive_sha256'], root=root)
    assert result['status'] == 'verified_complete_policy_archive'
    assert not list(destination.glob('roundtrip-*'))
    assert (source / 'assembled-original.parquet').read_bytes() == b'PRESERVED ORIGINAL ASSEMBLED TRIALS'


def test_deterministic_archive_and_index_independent_of_output_directory(policy):
    first, r1 = pack(policy, 'archive-one'); second, r2 = pack(policy, 'archive-two')
    assert r1['archive_sha256'] == r2['archive_sha256'] and r1['index_sha256'] == r2['index_sha256']
    assert (first / 'evidence.tar').read_bytes() == (second / 'evidence.tar').read_bytes()


def test_archive_aware_packets_preserve_original_raw_sha_and_json(policy):
    root, source, authority = policy; destination, receipt = pack(policy)
    packets = a.committed_conditions(destination, receipt['index_sha256'], receipt['archive_sha256'], root=root)
    expected = json.loads((source / 'evaluation_summary.json').read_bytes())['condition_bindings']
    assert len(packets) == 300
    for binding, packet in zip(expected, packets):
        assert packet == {'sha256': binding['receipt_sha256'], 'value': json.loads((source / binding['receipt_name']).read_bytes())}


def test_unassembled_policy_and_canary_are_refused_without_any_archive(policy):
    root, source, authority = policy
    result_path = Path(authority['result_receipt']['path']); result = json.loads(result_path.read_bytes())
    result['mode'] = 'agent-evaluation'; authority['result_receipt'] = write(result_path, result)
    with pytest.raises(ValueError, match='positively assembled'): a.pack_policy(source, root / 'maintenance/archive', authority=authority, root=root)
    assert not (root / 'maintenance/archive').exists()


@pytest.mark.parametrize('failure', ['running', 'foreign_owner', 'changed_ledger', 'wrong_source', 'changed_checkpoint', 'missing_part', 'changed_part'])
def test_source_and_native_failures_preserve_originals(policy, failure):
    root, source, authority = policy
    if failure == 'running':
        proof = json.loads(Path(authority['native']['completion']['path']).read_bytes()); proof['native_records']['100']['state'] = 'RUNNING'
        proof['native_records_sha256'] = a.canonical(proof['native_records']); proof.pop('completion_proof_sha256'); proof['completion_proof_sha256'] = a.canonical(proof)
        authority['native']['completion'] = write(authority['native']['completion']['path'], proof)
    elif failure == 'foreign_owner':
        owner = json.loads(Path(authority['native']['owner']['path']).read_bytes()); owner['user'] = 'other'
        authority['native']['owner'] = write(authority['native']['owner']['path'], owner)
    elif failure == 'changed_ledger': write(root / 'state/resource-budget.json', {'native_jobs': {}})
    elif failure == 'wrong_source': authority['primary_source_sha256'] = '0'*64
    elif failure == 'changed_checkpoint':
        plan = json.loads((source / 'plan.json').read_bytes()); Path(plan['checkpoint']).write_bytes(b'changed')
    elif failure == 'missing_part': next(source.glob('condition-*.parquet')).unlink()
    elif failure == 'changed_part': next(source.glob('condition-*.parquet')).write_bytes(b'changed')
    with pytest.raises((ValueError, FileNotFoundError)): a.pack_policy(source, root / 'maintenance/archive', authority=authority, root=root)
    assert not (root / 'maintenance/archive').exists()


@pytest.mark.parametrize('name', ['../escape', '/absolute', 'a/../b', 'a//b', 'a\\b', './a', 'a/plan.json', 'assembled-x.parquet', 'receipt.json', 'x/checkpoint.pt'])
def test_unsafe_or_protected_paths_never_become_archive_members(name):
    with pytest.raises(ValueError): a.relative_name(name)


def test_symlink_and_hardlink_originals_refused(policy, monkeypatch):
    root, source, authority = policy; target = next(source.glob('condition-*.parquet'))
    twin = root / 'maintenance/linked-original'
    try: os.link(target, twin)
    except PermissionError:
        # Windows desktop sandbox denies hard-link creation. Inject only the
        # OS link-count observation; exercise the real confined/file guards.
        from types import SimpleNamespace
        original_lstat = Path.lstat
        def linked(path, *args, **kwargs):
            info = original_lstat(path, *args, **kwargs)
            if path != target: return info
            fields = {k: getattr(info, k) for k in ['st_mode','st_nlink','st_size','st_dev','st_ino','st_mtime_ns','st_ctime_ns','st_uid']}
            fields['st_nlink'] = 2
            return SimpleNamespace(**fields)
        with monkeypatch.context() as local:
            local.setattr(Path, 'lstat', linked)
            with pytest.raises(ValueError, match='single-link'): a.pack_policy(source, root / 'maintenance/archive', authority=authority, root=root)
    else:
        with pytest.raises(ValueError, match='single-link'): a.pack_policy(source, root / 'maintenance/archive', authority=authority, root=root)
        twin.unlink()
    other = root / 'maintenance/other'; other.write_bytes(target.read_bytes()); target.unlink()
    try: target.symlink_to(other)
    except OSError:
        target.write_bytes(other.read_bytes())
        original_symlink = Path.is_symlink
        with monkeypatch.context() as local:
            local.setattr(Path, 'is_symlink', lambda path: path == target or original_symlink(path))
            with pytest.raises(ValueError, match='Symlink'): a.pack_policy(source, root / 'maintenance/archive', authority=authority, root=root)
    else:
        with pytest.raises(ValueError, match='Symlink'): a.pack_policy(source, root / 'maintenance/archive', authority=authority, root=root)


def test_archive_and_index_corruption_refused(policy):
    root, source, authority = policy; destination, receipt = pack(policy)
    archive = destination / 'evidence.tar'; original = archive.read_bytes(); archive.write_bytes(b'X' + original[1:])
    with pytest.raises(ValueError, match='Archive bytes changed'): a.verify_archive(destination, receipt['index_sha256'], receipt['archive_sha256'])
    archive.write_bytes(original)
    index = destination / 'index.json'; original_index = index.read_bytes(); index.write_bytes(original_index + b' ')
    with pytest.raises(ValueError, match='index changed'): a.verify_archive(destination, receipt['index_sha256'], receipt['archive_sha256'])
    # A content-bound but non-regular tar type is still forbidden. Python's
    # isfile() also accepts CONTTYPE, so the exact REGTYPE check is required.
    rebuilt = io.BytesIO()
    with tarfile.open(fileobj=io.BytesIO(original), mode='r:') as old, tarfile.open(fileobj=rebuilt, mode='w', format=tarfile.PAX_FORMAT) as new:
        for i, member in enumerate(old):
            payload = old.extractfile(member).read()
            if i == 0: member.type = tarfile.CONTTYPE
            new.addfile(member, io.BytesIO(payload))
    altered = rebuilt.getvalue(); archive.write_bytes(altered)
    value = json.loads(original_index); value['archive'].update(sha256=a.sha(altered), bytes=len(altered))
    raw_index = (json.dumps(value, sort_keys=True, indent=2) + '\n').encode(); index.write_bytes(raw_index)
    with pytest.raises(ValueError, match='Unsafe'): a.verify_archive(destination, a.sha(raw_index), a.sha(altered))


def test_native_canary_is_mandatory_for_removal(policy):
    root, source, authority = policy; destination, receipt = pack(policy)
    wrong = write(root / 'maintenance/proofs/fake-canary.json', {'status': 'local_tests_only'})
    with pytest.raises(ValueError, match='native pack/removal canary'):
        a.remove_verified_originals(destination, receipt['index_sha256'], receipt['archive_sha256'], removal_canary=wrong, root=root)
    assert len(list(source.glob('condition-*'))) == 600


def test_explicit_verified_removal_preserves_canonical_inputs_and_archive_packets(policy):
    root, source, authority = policy; destination, receipt = pack(policy)
    preserved = {n: (source / n).read_bytes() for n in ['plan.json', 'plan-binding.json', 'evaluation_summary.json', 'assembled-original.parquet']}
    result = a.remove_verified_originals(destination, receipt['index_sha256'], receipt['archive_sha256'], removal_canary=canary(root), root=root)
    assert result['status'] == 'complete_originals_compacted' and len(result['removed']) == 600
    assert not list(source.glob('condition-*'))
    assert preserved == {n: (source / n).read_bytes() for n in preserved}
    assert Path(authority['result_receipt']['path']).is_file()
    packets = a.committed_conditions(destination, receipt['index_sha256'], receipt['archive_sha256'], root=root)
    assert len(packets) == 300


def test_changed_original_refuses_all_unlinks_even_when_archive_is_valid(policy):
    root, source, authority = policy; destination, receipt = pack(policy)
    original = next(source.glob('condition-*.parquet')); original.write_bytes(original.read_bytes() + b'CHANGED')
    with pytest.raises(ValueError, match='changed since independent'):
        a.remove_verified_originals(destination, receipt['index_sha256'], receipt['archive_sha256'], removal_canary=canary(root), root=root)
    assert len(list(source.glob('condition-*'))) == 600


def test_interruption_after_unlink_requires_exact_explicit_reconciliation(policy):
    root, source, authority = policy; destination, receipt = pack(policy); proof = canary(root)
    def interrupt(name): raise RuntimeError('Injected after unlink before journal commit')
    with pytest.raises(RuntimeError):
        a.remove_verified_originals(destination, receipt['index_sha256'], receipt['archive_sha256'], removal_canary=proof, root=root, after_unlink=interrupt)
    assert len(list(source.glob('condition-*'))) == 599
    journal_path = destination / 'removal-journal.json'; journal_sha = a.sha(journal_path.read_bytes())
    with pytest.raises(ValueError, match='explicit reconciliation'):
        a.remove_verified_originals(destination, receipt['index_sha256'], receipt['archive_sha256'], removal_canary=proof, root=root)
    with pytest.raises(ValueError, match='binding failed'):
        a.remove_verified_originals(destination, receipt['index_sha256'], receipt['archive_sha256'], removal_canary=proof, root=root,
            explicit_reconcile=True, expected_journal_sha256='0'*64)
    result = a.remove_verified_originals(destination, receipt['index_sha256'], receipt['archive_sha256'], removal_canary=proof, root=root,
        explicit_reconcile=True, expected_journal_sha256=journal_sha)
    assert len(result['removed']) == 600 and len(result['reconciled_absent_after_interruption']) == 1


def test_published_archive_interruption_explicitly_resumes_without_repacking(policy, monkeypatch):
    root, source, authority = policy; destination = root / 'maintenance/archive'
    complete = a._complete_pack
    def interrupt(*args, **kwargs): raise RuntimeError('Injected after archive publication')
    monkeypatch.setattr(a, '_complete_pack', interrupt)
    with pytest.raises(RuntimeError): a.pack_policy(source, destination, authority=authority, root=root)
    assert (destination / 'evidence.tar').is_file() and not (destination / 'index.json').exists()
    assert len(list(source.glob('condition-*'))) == 600
    intent_sha = a.sha((destination / 'pack-intent.json').read_bytes()); archive_sha = a.sha((destination / 'evidence.tar').read_bytes())
    monkeypatch.setattr(a, '_complete_pack', complete)
    with pytest.raises(ValueError, match='explicit byte-bound reconciliation'):
        a.resume_published_pack(destination, expected_intent_sha256=intent_sha, expected_archive_sha256=archive_sha, root=root)
    result = a.resume_published_pack(destination, expected_intent_sha256=intent_sha, expected_archive_sha256=archive_sha,
        explicit_reconcile=True, root=root)
    assert result['archive_sha256'] == archive_sha and result['files'] == 600


def test_terminal_reader_pack_excludes_worker_receipt_and_unrelated_files(policy):
    root, source, authority = policy
    native_area = Path(authority['result_receipt']['path']).parent
    for name, raw in [('accounting-reads/read-1/attempt-1-stdout.txt', b'raw native output'),
                      ('scheduler-reads/intent.json', b'{"exact":"read"}'),
                      ('quota-reads/result.json', b'{"quota":"fresh"}'),
                      ('unrelated.txt', b'MUST STAY')]:
        path = native_area / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(raw)
    destination = root / 'maintenance/reader-archive'
    receipt = a.pack_terminal_readers(native_area, destination, authority=authority, root=root)
    with a.ArchiveReader(destination, receipt['index_sha256'], receipt['archive_sha256']) as reader:
        assert len(reader.members) == 3 and all('/' in n for n in reader.members)
    assert (native_area / 'receipt.json').exists() and (native_area / 'unrelated.txt').exists()
    with pytest.raises(ValueError, match='Only terminal worker'):
        a.pack_terminal_readers(source, root / 'maintenance/wrong', authority=authority, root=root)
    for name in ['driver.py', 'protocol.json', 'training-plan.json', 'checkpoint.pt', 'unrecognized.json']:
        path = native_area / 'accounting-reads' / name
        path.write_text('MUST REMAIN UNPACKED')
        with pytest.raises(ValueError, match='must remain unpacked|not eligible'):
            a.pack_terminal_readers(native_area, root / 'maintenance' / ('refused-' + name), authority=authority, root=root)
        assert path.read_text() == 'MUST REMAIN UNPACKED'
        path.unlink()


def test_current_native_job_never_compacted(policy, monkeypatch):
    root, source, authority = policy; monkeypatch.setenv('SLURM_JOB_ID', '100')
    with pytest.raises(ValueError, match='Current active job'): a.pack_policy(source, root / 'maintenance/archive', authority=authority, root=root)


def test_finite_input_deadline_and_exclusive_publication(policy, monkeypatch):
    root, source, authority = policy
    monkeypatch.setattr(a, 'MAX_FILES', 1)
    with pytest.raises(ValueError, match='finite exact source roster|Finite nonempty'): a.pack_policy(source, root / 'maintenance/archive', authority=authority, root=root)
    monkeypatch.setattr(a, 'MAX_FILES', 20000)
    def expired(): raise TimeoutError('finite guard')
    with pytest.raises(TimeoutError): a.pack_policy(source, root / 'maintenance/archive', authority=authority, root=root, checkpoint=expired)
    destination, receipt = pack(policy)
    with pytest.raises(ValueError, match='Fresh separate archive'): a.pack_policy(source, destination, authority=authority, root=root)


def test_unpacked_destination_never_overwritten(policy):
    root, source, authority = policy; destination, receipt = pack(policy)
    target = root / 'maintenance/restored'
    restored = a.unpack(destination, target, receipt['index_sha256'], receipt['archive_sha256'])
    assert len(restored) == 600
    with pytest.raises(ValueError, match='Fresh ordinary extraction'): a.unpack(destination, target, receipt['index_sha256'], receipt['archive_sha256'])


@pytest.mark.parametrize('failure', ['one_test', 'source_sha', 'missing_case', 'failed_case', 'missing_junit'])
def test_boolean_or_incomplete_native_canary_never_authorizes_unlink(policy, failure):
    root, source, authority = policy; locator = canary(root)
    value = json.loads(Path(locator['path']).read_bytes())
    if failure == 'one_test': value['test_counts']['tests'] = 1
    elif failure == 'source_sha': value['test_source']['sha256'] = '0'*64
    elif failure == 'missing_junit': value.pop('junit')
    else:
        junit = ET.fromstring(Path(value['junit']['path']).read_bytes())
        if failure == 'missing_case': junit.remove(junit.find('testcase'))
        else: ET.SubElement(junit.find('testcase'), 'failure', message='failed')
        raw = ET.tostring(junit); Path(value['junit']['path']).write_bytes(raw); value['junit']['sha256'] = a.sha(raw)
    locator = write(locator['path'], value)
    with pytest.raises((ValueError, KeyError)): a._verify_canary(locator, root)
    assert len(list(source.glob('condition-*'))) == 600


def test_streaming_reader_does_not_require_member_in_memory(policy, monkeypatch):
    root, source, authority = policy; destination, receipt = pack(policy)
    with a.ArchiveReader(destination, receipt['index_sha256'], receipt['archive_sha256']) as reader:
        name = next(n for n in reader.members if n.endswith('.json'))
        monkeypatch.setattr(a, 'MAX_METADATA_BYTES', 1)
        with pytest.raises(ValueError, match='streaming'): reader.read_member(name)
        output = io.BytesIO(); result = reader.copy_member(name, output)
        assert result['sha256'] == a.sha(output.getvalue())


def test_each_reconciliation_records_its_latest_native_remover(policy, monkeypatch):
    root, source, authority = policy; destination, receipt = pack(policy); proof = canary(root)
    def interrupt(name): raise RuntimeError('Injected native removal interruption')
    monkeypatch.setenv('SLURM_JOB_ID', '300')
    with pytest.raises(RuntimeError):
        a.remove_verified_originals(destination, receipt['index_sha256'], receipt['archive_sha256'], removal_canary=proof, root=root, after_unlink=interrupt)
    journal_path = destination / 'removal-journal.json'
    assert json.loads(journal_path.read_bytes())['native_job_id'] == '300'
    native(root, '300'); monkeypatch.setenv('SLURM_JOB_ID', '301')
    journal_sha = a.sha(journal_path.read_bytes())
    with pytest.raises(RuntimeError):
        a.remove_verified_originals(destination, receipt['index_sha256'], receipt['archive_sha256'], removal_canary=proof, root=root,
            explicit_reconcile=True, expected_journal_sha256=journal_sha, after_unlink=interrupt)
    journal = json.loads(journal_path.read_bytes())
    assert journal['native_job_id'] == '301' and journal['reconciliation_attempts'][-1]['previous_native_job_id'] == '300'
    monkeypatch.setenv('SLURM_JOB_ID', '302'); native(root, '301', state='RUNNING')
    with pytest.raises(ValueError, match='must be terminal'):
        a.remove_verified_originals(destination, receipt['index_sha256'], receipt['archive_sha256'], removal_canary=proof, root=root,
            explicit_reconcile=True, expected_journal_sha256=a.sha(journal_path.read_bytes()))
    assert len(list(source.glob('condition-*'))) == 598
