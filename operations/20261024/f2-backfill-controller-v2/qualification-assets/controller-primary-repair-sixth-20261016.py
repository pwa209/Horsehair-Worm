"""Sixth scheduled migration of exactly four owned Horsehair controllers.

prepare-and-register freezes qualified replacements, stops the old roster and
commits its canonical amendment. Replacements remain held. The separate release
stage requires the restored canonical RNA ledger hash and a fresh combined
budget check. Its RNA root stays positively held until an independent complete
provider-verification coordinator releases it. There is no automatic replay.
"""
from pathlib import Path
from datetime import datetime, timezone
import argparse
import copy
import json
import math
import os
import re
import runpy
import shlex
import subprocess
import time

from horsehair.core import atomic_json, canonical_hash, digest, exclusive_file_lock, native_job_id, read_json, source_identity
from horsehair.slurm import submit, completed_native_dependency, _array_indices
from horsehair.repair import register_amendment, _repair_native_states, _repair_allocation_accounting_runner, _controller_maintenance, _check_script
from horsehair.budget import resource_lease, native_accounting
from horsehair.pipeline import NODES, prospective_capacity, prospective_registered_budget

ROOT = Path('/scratch/pwa209/horsehair-worm')
PRIMARY = Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
OLD = ['5740729', '5740731', '5740735', '5740739']
OLD_RNA_WAIT = '5739838'
RETAINED_RNA_PLAN = '5469489'
CASE_ID = 'controller-primary-sixth-20261016'
TRANSFER_RESTART = 'rna334-curl-20261016'
TRANSFER_CASE = 'rna334-curl-20261016'
NAMES = ['pipeline-controller-' + str(i) for i in range(4)]
TERMINAL = {'CANCELLED', 'COMPLETED', 'FAILED', 'TIMEOUT', 'NODE_FAIL', 'OUT_OF_MEMORY', 'PREEMPTED', 'BOOT_FAIL', 'DEADLINE', 'REVOKED'}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def now():
    return datetime.now(timezone.utc).isoformat()


def exclusive_bytes(path, body):
    with Path(path).open('xb') as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())


def native_control(job, name, script, *, held=False):
    result = subprocess.run(['scontrol', 'show', 'job', str(job), '-o'], text=True, capture_output=True, check=True, timeout=30)
    fields = dict(re.findall(r'(\w+)=([^ ]+)', result.stdout))
    require(fields.get('JobId') == str(job) and fields.get('JobName') == 'hhw-' + name
            and fields.get('UserId', '').startswith('pwa209(')
            and fields.get('Account') == 'def-ptewarie_cpu'
            and Path(fields.get('Command', '')).resolve() == Path(script).resolve(),
            'Native controller ownership/name/script does not match its canonical task: ' + str(job))
    if held:
        require(fields.get('JobState') == 'PENDING' and fields.get('Reason') == 'JobHeldUser',
                'Replacement controller must remain positively user-held: ' + str(job))
    return {'stdout': result.stdout, 'fields': fields}


def old_control(job, name, task):
    """Old terminal jobs need owned sacct proof; live jobs need exact scontrol."""
    require(name in NAMES and str(job) == OLD[NAMES.index(name)], 'Old controller is outside the exact canonical roster')
    script = Path(task['script'])
    require(script.is_file() and not script.is_symlink()
            and canonical_hash(script.read_text(encoding='utf8')) == task['script_sha256'], 'Old canonical script changed')
    facts = _repair_native_states([str(job)], {}, owner='pwa209', account='def-ptewarie_cpu')
    require(set(facts) == {str(job)}, 'Old controller lacks positive owned native state')
    proof = facts[str(job)]
    require(proof.get('user') == 'pwa209' and proof.get('account') == 'def-ptewarie_cpu', 'Old controller native proof belongs to another owner/account')
    if proof['state'] in TERMINAL:
        require(re.fullmatch(r'[0-9]+:[0-9]+', proof['exit_code']) is not None, 'Terminal old controller has no native exit evidence')
        return {'terminal': True, 'native': proof, 'script': str(script), 'script_sha256': task['script_sha256'],
                'basis': 'Positive owned final sacct evidence plus unchanged canonical script; no cancellation required'}
    control = native_control(job, name, script)
    require(control['fields'].get('JobState') in {'PENDING', 'RUNNING', 'CONFIGURING', 'COMPLETING', 'SUSPENDED'},
            'Old live controller state changed; inspect before cancellation')
    return {'terminal': False, 'native': proof, **control}


def script_contract(task, primary):
    """Inspect all original script fields before any scheduler mutation."""
    path = Path(task['script'])
    require(path.is_file() and not path.is_symlink(), 'Original script must be an ordinary file')
    text = path.read_text(encoding='utf8')
    require(canonical_hash(text) == task['script_sha256'], 'Original frozen script changed')
    lines = text.splitlines()
    cds = [line for line in lines if line.startswith('cd ')]
    exports = [line for line in lines if line.startswith('export PYTHONPATH=')]
    commands = [line for line in lines if line.startswith('python -m horsehair.cli ')
                and 'pipeline' in shlex.split(line)[4:]]
    require(len(cds) == len(exports) == len(commands) == 1, 'Original script has an ambiguous execution/command schema')
    execution = Path(task.get('execution_release', {}).get('path', str(primary))).resolve()
    require(Path(shlex.split(cds[0])[1]).resolve() == execution, 'Original cd differs from its bound execution release')
    exported = shlex.split(exports[0])[1].split('=', 1)[1]
    suffix = ':${PYTHONPATH:-}'
    require(exported.endswith(suffix) and Path(exported[:-len(suffix)]).resolve() == execution / 'src',
            'Original PYTHONPATH differs from its bound execution release')
    command = commands[0]
    require(task.get('command', command) == command, 'Original task command field differs from its actual script')
    argv = shlex.split(command)
    require(argv.count('--primary-release') == 1 and argv.index('--primary-release') == len(argv) - 2
            and Path(argv[-1]).resolve() == primary,
            'Repeat migration requires exactly one unchanged primary-release argument')
    old_release = read_json(execution / 'release.json')
    require(source_identity(execution) == old_release['source_identity'], 'Original controller execution release changed')
    return {'text': text, 'cd': cds[0], 'export': exports[0], 'command': command, 'execution': str(execution)}


def context(args):
    root, primary, framework = ROOT.resolve(), PRIMARY.resolve(), args.framework.resolve()
    require(args.case_id == CASE_ID, 'The sixth handoff requires its exact new controller case identity')
    job = native_job_id()
    require(re.fullmatch(r'[0-9]+', str(job or '')) is not None and os.environ.get('USER') == 'pwa209'
            and os.environ.get('SLURM_CPUS_PER_TASK') == '1', 'Management driver requires its own owned one-CPU Slurm allocation')
    require(digest(Path(__file__)) == args.helper_sha256, 'Management helper bytes changed')
    identity, other = read_json(primary / 'release.json'), read_json(framework / 'release.json')
    config = read_json(primary / 'configs/study.json')
    require(Path(config['paths']['root']).resolve() == root and source_identity(primary) == identity['source_identity']
            and source_identity(framework) == other['source_identity']
            and read_json(framework / 'configs/study.json') == config
            and identity['config_sha256'] == other['config_sha256'] == canonical_hash(config),
            'Immutable primary/framework/full scientific configuration changed')
    qualification = args.qualification.resolve()
    require(qualification.is_relative_to(root / 'maintenance') and qualification.is_file()
            and not qualification.is_symlink() and digest(qualification) == args.qualification_sha256,
            'Qualification path/hash is not its pinned personal maintenance receipt')
    return root, primary, framework, identity, other, config, str(job), qualification


def checked_pair(plan_path, ledger_path, root, config):
    plan, ledger = read_json(plan_path), read_json(ledger_path)
    require(Path(plan['root']).resolve() == root and plan['config_sha256'] == canonical_hash(config)
            and ledger['config_sha256'] == canonical_hash(config)
            and ledger['plan_sha256'] == canonical_hash(plan)
            and ledger['source_sha256'] == plan['release_identity']['sha256'],
            'Plan/ledger pair is not bound to this study root/full configuration/source')
    names = [task['name'] for task in plan['tasks']]
    require(len(names) == len(set(names)) and set(ledger.get('tasks', {})) <= set(names), 'Invalid plan/ledger task roster')
    return plan, ledger


def all_registered_work(root, config, rid):
    """Find real local plan/ledger pairs and deduplicate actual native jobs.

    Current canonical phases take precedence. Retained replacement/support
    ledgers add their unique jobs and failed costs, never a second reservation
    for an allocation already present in a canonical ledger.
    """
    indexed = {}
    for path in sorted((root / 'plans').rglob('plan.json')):
        require(not path.is_symlink(), 'A study plan is unexpectedly symlinked')
        value = read_json(path)
        if (Path(value.get('root', '')).resolve() == root
                and value.get('config_sha256') == canonical_hash(config)
                and isinstance(value.get('tasks'), list)):
            indexed.setdefault(canonical_hash(value), []).append(path)
    paths = []
    for phase in ['managed-initial', *[name for name, _, _ in NODES]]:
        ledger = root / 'state' / (rid + '-' + phase + '-submission.json')
        if ledger.exists():
            paths.append(ledger)
    paths.extend(path for path in sorted((root / 'state').glob('*-submission.json')) if path not in paths)
    plans, ledgers, ids, rosters, members, bindings, hashes, excluded = {}, {}, set(), {}, set(), {}, {}, []
    for ledger_path in paths:
        require(not ledger_path.is_symlink(), 'A submission ledger is unexpectedly symlinked')
        header = read_json(ledger_path)
        if header.get('config_sha256') != canonical_hash(config) or not header.get('plan_sha256') or not header.get('source_sha256'):
            excluded.append({'path': str(ledger_path), 'reason': 'Not a full source/configuration/plan-bound study ledger'})
            continue
        candidates = indexed.get(header['plan_sha256'], [])
        require(candidates, 'A source/configuration-bound submission ledger has no unchanged study plan: ' + str(ledger_path))
        plan_path = candidates[0]
        plan, ledger = checked_pair(plan_path, ledger_path, root, config)
        label = ledger_path.stem
        hashes[label] = {'plan_path': str(plan_path), 'ledger_path': str(ledger_path),
                         'plan_sha256': canonical_hash(plan), 'ledger_sha256': canonical_hash(ledger)}
        selected = []
        original_tasks = {task['name']: task for task in plan['tasks']}
        for task in plan['tasks']:
            record = ledger.get('tasks', {}).get(task['name'], {})
            if record.get('state') != 'submitted':
                require(not record.get('job_id'), 'Ambiguous allocation record requires explicit inspection')
                continue
            ident = str(record.get('job_id', ''))
            require(re.fullmatch(r'[0-9]+', ident) is not None, 'Invalid native study allocation identity')
            resource_hash = canonical_hash(task['resources'])
            if ident in bindings:
                require(bindings[ident]['resource_sha256'] == resource_hash, 'Duplicate native allocation has conflicting resource contracts')
                continue
            bindings[ident] = {'ledger': str(ledger_path), 'task': task['name'], 'resource_sha256': resource_hash}
            selected.append(copy.deepcopy(task))
            ids.add(ident)
            members.add(ident)
            if task['resources'].get('array'):
                rosters[ident] = _array_indices(task['resources']['array'])
                members.update(ident + '_' + str(index) for index in rosters[ident])
        selected_names = {task['name'] for task in selected}
        for task in selected:
            removed_parents = [name for name in task.get('depends', []) if name not in selected_names]
            task['depends'] = [name for name in task.get('depends', []) if name in selected_names]
            external = set(map(str, task.get('external_dependencies', [])))
            for name in removed_parents:
                require(name in original_tasks and name in ledger['tasks'] and ledger['tasks'][name].get('job_id'),
                        'Deduplicated dependency has no native allocation binding')
                external.add(str(ledger['tasks'][name]['job_id']))
            task['external_dependencies'] = sorted(external)
        if selected:
            plans[label] = {**plan, 'tasks': selected}
            ledgers[label] = {**ledger, 'tasks': {task['name']: ledger['tasks'][task['name']] for task in selected}}
    require(plans and ids, 'No positive source-bound study work was discovered')
    return plans, ledgers, ids, rosters, members, hashes, bindings, excluded


def combined_admission(root, config, rid, area, label):
    plans, ledgers, ids, rosters, members, hashes, bindings, excluded = all_registered_work(root, config, rid)
    before = read_json(root / 'state/resource-budget.json')
    aliases = before.get('native_raw_aliases', {})
    accounting_ids = set(ids)
    for ident, lease in before.get('leases', {}).items():
        logical = str(aliases.get(ident, ident))
        if lease.get('state') in {'running', 'finished'}:
            require(re.fullmatch(r'[0-9]+(?:_[0-9]+)?', logical) is not None, 'Invalid native lease identity')
            accounting_ids.add(logical)
    observed = _repair_native_states(sorted(accounting_ids), rosters, owner='pwa209', account=None)
    accounting = native_accounting(root, sorted(accounting_ids),
        runner=_repair_allocation_accounting_runner(rosters, owner='pwa209', account=None))
    with exclusive_file_lock(root / 'state/resource-budget.mutex'):
        # Re-read pair hashes under the guard: no capacity decision can use a
        # plan snapshot from before the separate RNA canonical amendment.
        for item in hashes.values():
            require(canonical_hash(read_json(item['plan_path'])) == item['plan_sha256']
                    and canonical_hash(read_json(item['ledger_path'])) == item['ledger_sha256'],
                    'A registered plan/ledger changed during native resource admission')
        budget = read_json(root / 'state/resource-budget.json')
        totals = prospective_registered_budget(plans, ledgers, observed, budget)
        capacity = prospective_capacity(plans, ledgers, observed)
        outside = {}
        for ident, lease in budget.get('leases', {}).items():
            logical = str(budget.get('native_raw_aliases', {}).get(ident, ident))
            if lease.get('state') != 'running' or logical in members:
                continue
            require(logical not in budget.get('native_jobs', {}), 'An outside lease lacks completed native reconciliation')
            hours = float(lease['max_hours'])
            cpus, gpus = int(lease['cpus']), int(lease['gpus'])
            require(math.isfinite(hours) and 0 < hours <= 168 and cpus > 0 and gpus in {0, 1}, 'Invalid outside native resource lease')
            outside[ident] = lease
            totals['cpu_core_hours'] += cpus * hours
            totals['gpu_hours'] += gpus * hours
            capacity['cpus'] += cpus
            capacity['gpus'] += gpus
        require(totals['cpu_core_hours'] <= config['resources']['cpu_core_hours_cap']
                and totals['gpu_hours'] <= config['resources']['gpu_hours_cap']
                and capacity['cpus'] <= config['resources']['cpu_ceiling']
                and capacity['gpus'] <= config['resources']['gpu_concurrency'], 'Combined registered/support workload exceeds fixed study limits')
        receipt = {'status': 'admitted', 'checked_utc': now(), 'config_sha256': canonical_hash(config),
                   'totals': totals, 'capacity': capacity, 'registered_pair_hashes': hashes,
                   'unique_allocation_bindings': bindings, 'excluded_unbound_files': excluded,
                   'external_active_leases': outside, 'native_aliases': budget.get('native_raw_aliases', {}),
                   'native_accounting': accounting, 'native_observed': observed,
                   'rule': 'Native spent plus all unique unfinished source-bound canonical/support allocations and nonduplicated outside leases; no scientific gate waived'}
        atomic_json(area / (label + '.json'), receipt)
    return receipt


def descriptor_for(args, root, primary, framework, qualification, qualjob, directory, ledger):
    return {'amendment_id': args.case_id, 'phase': 'managed-initial', 'root': str(root), 'primary_release': str(primary),
            'replacement_plan': str(directory / 'plan.json'), 'replacement_ledger': str(ledger),
            'execution_releases': {name: str(framework) for name in NAMES},
            'required_external_dependencies': {name: [qualjob] for name in NAMES},
            'controller_maintenance': {'framework_release': str(framework), 'primary_release': str(primary),
                'qualification_path': str(qualification), 'qualification_sha256': args.qualification_sha256, 'qualification_job_id': qualjob},
            'reason': 'Qualified primary-release admission/planning isolation; existing primary arguments, numeric release/configuration, completed development/freeze and all source evidence retained. Two-stage canonical handoff with restored RNA ledger and fresh combined budget before controller release.'}


def sixth_transfer_binding(root, primary, framework, identity, other, config):
    """Verify the exact new, whole-roster transport policy without data reads."""
    ready = read_json(root / 'maintenance' / TRANSFER_CASE / 'ready.json')
    require(ready.get('status') == 'ready_for_transfer' and ready.get('case_transport') == 'canonical_https'
            and ready.get('asset_id') == 'ant_rna:SRR22989334_1.fastq.gz',
            'The sixth handoff requires its exact native-preserved checksum failure case')
    transport = runpy.run_path(str(framework / 'scripts/transfer_rna_ftp.py'), run_name='hhw_policy_binding_only')
    transport_context = transport['bootstrap'](primary)
    manifest = read_json(root / 'manifests/source_manifest.json')
    require(ready.get('target_transfer_host') == 'narval3' and ready.get('transport_backend') == 'system_curl'
            and isinstance(ready.get('system_curl'), dict), 'The sixth recovery requires native-pinned Narval3 system curl')
    curl = ready['system_curl']
    require(set(curl) == {'path', 'sha256', 'version'} and curl['path'] == '/usr/bin/curl'
            and re.fullmatch('[0-9a-f]{64}', curl.get('sha256', '')) is not None
            and isinstance(curl.get('version'), str) and curl['version'].startswith('curl '), 'Native curl identity is incomplete')
    policy = transport['fastq_https_policy'](transport_context, manifest, preparation_job_id=ready['slurm_job_id'],
        target_transfer_host='narval3', transport_backend='system_curl', system_curl=curl)
    require(ready.get('fastq_transport_policy') == policy and policy.get('schema_version') == 3
            and policy.get('name') == 'all-selected-fastq-host-bound-system-curl-https-v3',
            'The whole-roster v3 curl policy differs from its qualified immutable transport source')
    restored = ready.get('restored_case_asset')
    require(isinstance(restored, dict) and set(restored) == {'proof_path', 'proof_sha256'}, 'Native-restored case asset lacks its exact proof binding')
    restored_path = Path(restored['proof_path'])
    require(restored_path.resolve().is_relative_to(root / 'maintenance') and restored_path.is_file()
            and not restored_path.is_symlink() and digest(restored_path) == restored['proof_sha256'], 'Native-restored asset proof changed or escaped maintenance')
    assets = [item for item in manifest['assets'] if item.get('asset_id') == ready['asset_id']]
    require(len(assets) == 1, 'Restored case asset is not unique in the frozen manifest')
    restored_record = transport['validate_restored_case_asset'](transport_context, ready, assets[0], manifest)
    require(isinstance(restored_record, dict) and restored_record.get('status') == 'verified_restored_case_asset',
            'The reviewed native restoration validator did not positively confirm this exact case')
    overlay = ready.get('transport_runtime_overlay')
    if overlay is not None:
        require(isinstance(overlay, dict) and set(overlay) == {'root', 'manifest_sha256'}
                and isinstance(overlay['root'], str) and Path(overlay['root']).is_absolute()
                and re.fullmatch('[0-9a-f]{64}', overlay['manifest_sha256']) is not None,
                'Transport runtime overlay has no exact native-prepared identity')
        overlay_path = Path(overlay['root'])
        overlay_root = overlay_path.resolve()
        require(not overlay_path.is_symlink() and overlay_root.is_dir()
                and any(overlay_root.is_relative_to(Path(base).resolve()) and overlay_root != Path(base).resolve()
                        for base in transport['RUNTIME_NAMESPACES']), 'Runtime overlay escaped its personal immutable namespace')
        overlay_manifest = overlay_root / 'runtime-manifest.json'
        require(overlay_manifest.is_file() and not overlay_manifest.is_symlink()
                and digest(overlay_manifest) == overlay['manifest_sha256'], 'The native-validated runtime overlay manifest changed')
    transfer = read_json(root / 'state/transfer-rna.json')
    expected = {'host': 'narval3', 'restart_id': TRANSFER_RESTART,
                'source_identity': identity['source_identity']['sha256'],
                'maintenance_source_sha256': other['source_identity']['sha256'],
                'transfer_script_sha256': other['source_identity']['files']['scripts/transfer_rna_ftp.py'],
                'config_sha256': canonical_hash(config), 'case_transport': 'canonical_https',
                'case_asset_id': ready['asset_id'], 'preservation_job_id': ready['slurm_job_id'],
                'fastq_transport_policy': policy, 'target_transfer_host': 'narval3',
                'transport_backend': 'system_curl', 'system_curl': curl, 'restored_case_asset': restored}
    require(transfer.get('transport_runtime_overlay') == overlay, 'The worker runtime overlay differs from native preparation')
    if overlay is not None:
        expected['transport_runtime_overlay'] = overlay
    require(transfer.get('status') in {'running', 'complete'} and not transfer.get('error')
            and all(transfer.get(key) == value for key, value in expected.items())
            and type(transfer.get('pid')) is int and transfer['pid'] > 1,
            'The sixth transfer is failed or is not the exact healthy qualified whole-roster HTTPS worker')
    if transfer['status'] == 'running':
        guardpath = root / 'state/.transfer-rna.lock'
        require(not guardpath.is_symlink(), 'A transfer guard symlink cannot establish ownership')
        guard = read_json(guardpath)
        require(guard.get('host') == 'narval3' and guard.get('pid') == transfer['pid']
                and guard.get('restart_id') == TRANSFER_RESTART, 'The sixth transfer guard differs from its worker')
    return {**expected, 'pid': transfer['pid']}


# Compatibility for root-owned RNA/completion helpers adapted from the fifth case.
fifth_transfer_binding = sixth_transfer_binding


def rna_preflight(args, root, primary, config, rid):
    """Freeze the independent RNA handoff before any controller mutation.

    RNA registration must precede controller registration: its guard requires
    the CURRENT canonical controller roster to be positively terminal. Newly
    registered held controllers would correctly fail that prerequisite.
    """
    require(args.rna_descriptor is not None and re.fullmatch('[0-9a-f]{64}', args.rna_descriptor_sha256 or '') is not None,
            'Prepare requires the precreated held RNA descriptor and exact file SHA256')
    descriptor_path = args.rna_descriptor.resolve()
    require(descriptor_path.is_relative_to(root / 'plans') and descriptor_path.is_file()
            and not descriptor_path.is_symlink() and digest(descriptor_path) == args.rna_descriptor_sha256,
            'RNA amendment descriptor path or pinned bytes changed')
    descriptor = read_json(descriptor_path)
    require(descriptor.get('phase') == 'rna-full' and Path(descriptor['root']).resolve() == root
            and Path(descriptor['primary_release']).resolve() == primary,
            'RNA amendment belongs to a different phase/root/primary release')
    original, oldledger = checked_pair(root / 'plans' / (rid + '-rna-full') / 'plan.json',
                                       root / 'state' / (rid + '-rna-full-submission.json'), root, config)
    require(str(oldledger['tasks']['rna-all-wait']['job_id']) == OLD_RNA_WAIT
            and str(oldledger['tasks']['rna-full-plan']['job_id']) == RETAINED_RNA_PLAN,
            'Current canonical RNA root or positively completed plan differs from the sixth repair roster')
    newplan_path, newledger_path = Path(descriptor['replacement_plan']).resolve(), Path(descriptor['replacement_ledger']).resolve()
    require(newplan_path.is_relative_to(root / 'plans') and newledger_path.is_relative_to(root / 'state'),
            'RNA replacement plan/ledger escaped the private study namespace')
    replacement, submitted = checked_pair(newplan_path, newledger_path, root, config)
    original_tasks = {task['name']: task for task in original['tasks']}
    candidates = {task['name']: task for task in replacement['tasks']}
    require(candidates.get('rna-full-plan') == original_tasks['rna-full-plan']
            and submitted['tasks'].get('rna-full-plan') == oldledger['tasks']['rna-full-plan'],
            'The completed RNA plan must be retained byte-for-byte as an unaffected task/acknowledgement')
    retained = _repair_native_states([RETAINED_RNA_PLAN], {}, owner='pwa209', account='def-ptewarie_cpu')[RETAINED_RNA_PLAN]
    require(retained['state'] == 'COMPLETED' and retained['exit_code'] == '0:0'
            and not retained['unallocated_metadata'] and retained['cpus'] > 0 and retained['elapsed_seconds'] > 0,
            'The retained RNA plan lacks its positive allocated native completion')
    targets = set(descriptor['execution_releases'])
    require(targets and targets <= set(original_tasks) and targets <= set(candidates)
            and 'rna-all-wait' in targets and 'rna-full-plan' not in targets,
            'RNA repair must preserve its completed plan and replace its failed wait chain')
    rosters, newids, oldids = {}, [], []
    for name in targets:
        task = candidates[name]
        require(task['resources'] == original_tasks[name]['resources']
                and Path(descriptor['execution_releases'][name]).resolve() == primary,
                'RNA repair changes frozen resources or numeric execution release')
        _check_script(task, original_tasks[name], primary, root)
        record = submitted['tasks'][name]
        require(record.get('state') == 'submitted' and record.get('script_sha256') == task['script_sha256'],
                'RNA replacement acknowledgement is not bound to its frozen script')
        new = str(record['job_id'])
        old = str(oldledger['tasks'][name]['job_id'])
        require(re.fullmatch('[0-9]+', new) is not None and new != old, 'RNA replacement aliases an old allocation')
        newids.append(new)
        oldids.append(old)
        if task['resources'].get('array'):
            rosters[new] = _array_indices(task['resources']['array'])
            rosters[old] = _array_indices(original_tasks[name]['resources']['array'])
    require(len(newids) == len(set(newids)) and not set(newids) & set(oldids), 'RNA replacement roster contains allocation aliases')
    native = _repair_native_states(newids + oldids, rosters, owner='pwa209', account='def-ptewarie_cpu')
    for name in targets:
        task = candidates[name]
        new = str(submitted['tasks'][name]['job_id'])
        old = str(oldledger['tasks'][name]['job_id'])
        indices = _array_indices(task['resources']['array']) if task['resources'].get('array') else None
        newmembers = [new + '_' + str(index) for index in indices] if indices is not None else [new]
        oldmembers = [old + '_' + str(index) for index in indices] if indices is not None else [old]
        require(all(ident in native and native[ident]['state'] == 'PENDING' for ident in newmembers),
                'RNA replacement chain must remain fully pending behind its held root')
        require(all(ident in native and native[ident]['state'] in TERMINAL for ident in oldmembers),
                'Replaced RNA allocation lacks positive terminal native proof')
    wait = candidates['rna-all-wait']
    require('#SBATCH --hold' in Path(wait['script']).read_text(encoding='utf8').splitlines(), 'RNA wait script is not explicitly held')
    native_control(str(submitted['tasks']['rna-all-wait']['job_id']), 'rna-all-wait', wait['script'], held=True)
    return {'descriptor_path': str(descriptor_path), 'descriptor_sha256': args.rna_descriptor_sha256,
            'replacement_plan_sha256': canonical_hash(replacement), 'replacement_ledger_sha256': canonical_hash(submitted),
            'old_plan_sha256': canonical_hash(original), 'old_ledger_sha256': canonical_hash(oldledger),
            'old_jobs': sorted(oldids), 'replacement_jobs': sorted(newids), 'native': native}


def prepare(args, ctx):
    root, primary, framework, identity, other, config, job, qualification = ctx
    rid = identity['release_id']
    canonical_plan = root / 'plans' / (rid + '-managed-initial') / 'plan.json'
    canonical_ledger = root / 'state' / (rid + '-managed-initial-submission.json')
    oldplan, oldledger = checked_pair(canonical_plan, canonical_ledger, root, config)
    original = {task['name']: task for task in oldplan['tasks']}
    require([oldledger['tasks'][name]['job_id'] for name in NAMES] == OLD, 'Canonical old controller roster changed; never cancel an inferred roster')
    contracts = {name: script_contract(original[name], primary) for name in NAMES}
    before = {name: old_control(ident, name, original[name]) for name, ident in zip(NAMES, OLD)}
    qualjob = str(read_json(qualification)['slurm_job_id'])
    native_accounting(root, [qualjob])
    qualnative = _repair_native_states([qualjob], {}, owner='pwa209', account='def-ptewarie_cpu')[qualjob]
    require(qualnative['state'] == 'COMPLETED' and qualnative['exit_code'] == '0:0'
            and not qualnative['unallocated_metadata'] and qualnative['elapsed_seconds'] > 0 and qualnative['cpus'] > 0,
            'Framework qualification lacks positive completed native allocation')
    area = root / 'maintenance' / args.case_id
    directory = root / 'plans' / args.case_id
    ledgerpath = root / 'state' / (args.case_id + '-submission.json')
    desc = descriptor_for(args, root, primary, framework, qualification, qualjob, directory, ledgerpath)
    _controller_maintenance(desc, set(NAMES), {name: original[name] for name in NAMES}, primary, identity['source_identity'], config, root)
    rna = rna_preflight(args, root, primary, config, rid)
    transfer_binding = sixth_transfer_binding(root, primary, framework, identity, other, config)
    # Fail on incompatible historical/support schemas before stopping a live
    # controller, rather than discovering an unbound pair at final admission.
    all_registered_work(root, config, rid)
    require(not area.exists() and not directory.exists() and not ledgerpath.exists(), 'Maintenance case already has an outcome; inspect rather than replay')
    area.mkdir(parents=True, exist_ok=False)
    directory.mkdir(parents=True, exist_ok=False)
    record = {'status': 'preflight_complete', 'stage': 'prepare-and-register', 'case_id': args.case_id,
              'job_id': job, 'old_jobs': OLD, 'created_utc': now(), 'helper_sha256': args.helper_sha256,
              'framework': str(framework), 'framework_source_sha256': other['source_identity']['sha256'],
              'primary_release': str(primary), 'primary_source_sha256': identity['source_identity']['sha256'],
              'config_sha256': canonical_hash(config), 'qualification_path': str(qualification),
              'qualification_sha256': args.qualification_sha256, 'rna_preflight': rna,
              'transfer_binding': transfer_binding, 'released': []}
    receipt = area / 'receipt.json'
    atomic_json(receipt, record)
    for name, path in [('old-plan.json', canonical_plan), ('old-ledger.json', canonical_ledger),
                       ('old-status.json', root / 'state/pipeline' / rid / 'status.json'),
                       ('old-lock.json', root / 'state/pipeline' / rid / 'controller.lock')]:
        if path.exists():
            exclusive_bytes(area / name, path.read_bytes())
    atomic_json(area / 'native-before.json', before)
    atomic_json(area / 'native-qualification.json', qualnative)
    atomic_json(area / 'rna-preflight.json', rna)
    try:
        tasks = []
        for name in NAMES:
            task, contract = copy.deepcopy(original[name]), contracts[name]
            text = contract['text'].replace(contract['cd'], 'cd ' + shlex.quote(str(framework)), 1)
            text = text.replace(contract['export'], 'export PYTHONPATH=' + shlex.quote(str(framework / 'src')) + ':${PYTHONPATH:-}', 1)
            if '#SBATCH --hold' not in text.splitlines():
                text = text.replace('set -euo pipefail', '#SBATCH --hold\nset -euo pipefail', 1)
            require('#SBATCH --hold' in text.splitlines(), 'Replacement root has no explicit hold')
            script = directory / (name + '.sbatch')
            exclusive_bytes(script, text.encode('utf8'))
            task.update(script=str(script), script_sha256=canonical_hash(text), command=contract['command'],
                        execution_release={'path': str(framework), 'release_id': other['release_id'],
                            'source_identity': other['source_identity'], 'config_sha256': canonical_hash(config)})
            missing = [parent for parent in task.get('depends', []) if parent not in NAMES]
            task['depends'] = [parent for parent in task.get('depends', []) if parent in NAMES]
            kind = task.get('dependency_kind', 'afterok')
            proof = completed_native_dependency(root, qualjob, dependency_kind=kind)
            require(proof is not None, 'Positive qualification dependency is absent')
            completed = task.setdefault('completed_external_dependencies', [])
            if proof not in completed:
                completed.append(proof)
            specs = {str(value['job_id']): copy.deepcopy(value) for value in task.get('external_dependency_specs', [])}
            for ident in task.get('external_dependencies', []):
                specs.setdefault(str(ident), {'job_id': str(ident), 'array': None})
            for parent in missing:
                ident = str(oldledger['tasks'][parent]['job_id'])
                specs[ident] = {'job_id': ident, 'array': original[parent]['resources'].get('array')}
            seen = {(str(value['job_id']), str(value.get('array')), value.get('dependency_kind', 'afterok')) for value in completed}
            pending = []
            for ident, spec in specs.items():
                if (ident, str(spec.get('array')), kind) in seen:
                    continue
                positive = completed_native_dependency(root, ident, spec.get('array'), dependency_kind=kind)
                if positive:
                    completed.append(positive)
                else:
                    pending.append(spec)
            task.update(external_dependencies=[str(value['job_id']) for value in pending], external_dependency_specs=pending)
            _check_script(task, original[name], framework, root, controller_primary=primary)
            tasks.append(task)
        plan = {'schema_version': 2, 'phase': 'managed-initial', 'root': str(root),
                'config_sha256': canonical_hash(config), 'release_identity': identity['source_identity'],
                'tasks': tasks, 'cpu_ceiling': config['resources']['cpu_ceiling'], 'gpu_concurrency': config['resources']['gpu_concurrency'],
                'admission_phase': oldplan.get('admission_phase'), 'maximum_simultaneous_resources': prospective_capacity({'controllers': {'tasks': tasks}}, {}, {}),
                'scope': 'Qualified framework migration only; unchanged primary numeric commands/configuration and dependency policies; two-stage held registration'}
        atomic_json(directory / 'plan.json', plan)
        atomic_json(directory / 'descriptor.json', desc)
        # One final ownership check on every old allocation precedes first submit.
        for name, ident in zip(NAMES, OLD):
            old_control(ident, name, original[name])
        record.update(status='submitting', replacement_plan_sha256=canonical_hash(plan))
        atomic_json(receipt, record)
        submitted = submit(directory / 'plan.json', ledgerpath)
        newids = [str(submitted['tasks'][name]['job_id']) for name in NAMES]
        require(len(set(newids)) == 4 and not set(newids) & set(OLD), 'New controller allocations alias old/native identities')
        held = {name: native_control(ident, name, task['script'], held=True) for name, ident, task in zip(NAMES, newids, tasks)}
        record.update(status='replacements_held', replacement_jobs=newids)
        atomic_json(receipt, record)
        atomic_json(area / 'native-new-held.json', held)
        for name, ident in zip(NAMES, OLD):
            old_control(ident, name, original[name])
        record['old_cancellation_acknowledgements'] = []
        for name, ident in reversed(list(zip(NAMES, OLD))):
            # Inspect each exact owned job immediately before cancellation.
            proof = old_control(ident, name, original[name])
            if proof['terminal']:
                record['old_cancellation_acknowledgements'].append({'job_id': ident, 'action': 'already_terminal', 'proof': proof})
                atomic_json(receipt, record)
                continue
            record['old_cancellation_intent'] = {'job_id': ident, 'native_ownership_proof': proof, 'created_utc': now()}
            atomic_json(receipt, record)
            subprocess.run(['scancel', ident], check=True, timeout=30)
            record['old_cancellation_acknowledgements'].append({'job_id': ident, 'action': 'scancel_acknowledged', 'checked_utc': now()})
            record.pop('old_cancellation_intent', None)
            atomic_json(receipt, record)
        deadline = time.monotonic() + 600
        while True:
            oldstates = _repair_native_states(OLD, {}, owner='pwa209', account='def-ptewarie_cpu')
            if set(oldstates) == set(OLD) and all(value['state'] in TERMINAL for value in oldstates.values()):
                break
            require(time.monotonic() < deadline, 'Old controllers not positively terminal; replacements remain held')
            time.sleep(10)
        atomic_json(area / 'old-native-terminal.json', oldstates)
        require(digest(Path(rna['descriptor_path'])) == rna['descriptor_sha256'], 'RNA descriptor changed after preflight')
        # This ordering is mandatory. RNA's native guard must still observe the
        # old positively terminal canonical controllers, not new held ones.
        rna_registration = register_amendment(Path(rna['descriptor_path']))
        require(rna_registration['status'] == 'complete', 'Canonical RNA registration is incomplete')
        atomic_json(area / 'rna-registration.json', rna_registration)
        registration = register_amendment(directory / 'descriptor.json')
        require(registration['status'] == 'complete', 'Canonical controller registration is incomplete')
        atomic_json(area / 'registration.json', registration)
        admission = combined_admission(root, config, rid, area, 'prepare-admission')
        require(canonical_hash(read_json(canonical_plan)) == registration['amended_plan_sha256']
                and canonical_hash(read_json(canonical_ledger)) == registration['amended_ledger_sha256'], 'Canonical controller pair changed after registration')
        for name, ident, task in zip(NAMES, newids, tasks):
            native_control(ident, name, task['script'], held=True)
        record.update(status='registered_held', registered_utc=now(), registration_sha256=digest(area / 'registration.json'),
                      rna_registration_sha256=digest(area / 'rna-registration.json'),
                      rna_ledger_sha256=rna_registration['amended_ledger_sha256'],
                      admission=admission, RNA_root_release_required_before_controller_release=False,
                      RNA_root_release_requires_all_sources_verified=True, RNA_root_expected_state='PENDING/JobHeldUser')
        atomic_json(receipt, record)
    except BaseException as error:
        record.update(status='failed', error_type=type(error).__name__, error=str(error), failed_utc=now(), automatic_replay_permitted=False)
        atomic_json(receipt, record)
        raise
    return record


def release(args, ctx):
    root, primary, framework, identity, other, config, job, qualification = ctx
    rid = identity['release_id']
    area = root / 'maintenance' / args.case_id
    record = read_json(area / 'receipt.json')
    require(record.get('status') == 'registered_held' and record.get('released') == []
            and record.get('framework_source_sha256') == other['source_identity']['sha256']
            and record.get('qualification_sha256') == args.qualification_sha256
            and record.get('helper_sha256') == args.helper_sha256, 'Release case is not exactly the untouched registered-held handoff')
    require(sixth_transfer_binding(root, primary, framework, identity, other, config) == record.get('transfer_binding'),
            'The sixth transfer worker or source-bound transport policy changed since preparation')
    require(re.fullmatch('[0-9a-f]{64}', args.rna_ledger_sha256 or '') is not None, 'Release requires the restored canonical RNA ledger hash')
    planpath = root / 'plans' / (rid + '-managed-initial') / 'plan.json'
    ledgerpath = root / 'state' / (rid + '-managed-initial-submission.json')
    plan, ledger = checked_pair(planpath, ledgerpath, root, config)
    registration = read_json(area / 'registration.json')
    require(digest(area / 'registration.json') == record['registration_sha256']
            and registration['status'] == 'complete' and canonical_hash(plan) == registration['amended_plan_sha256']
            and canonical_hash(ledger) == registration['amended_ledger_sha256'], 'Controller canonical pair/journal changed before release')
    tasks = {task['name']: task for task in plan['tasks']}
    newids = [str(ledger['tasks'][name]['job_id']) for name in NAMES]
    require(newids == record['replacement_jobs'], 'Registered replacement controller roster changed')
    for name, ident in zip(NAMES, newids):
        native_control(ident, name, tasks[name]['script'], held=True)
    rnaplan, rnaledger = checked_pair(root / 'plans' / (rid + '-rna-full') / 'plan.json',
                                     root / 'state' / (rid + '-rna-full-submission.json'), root, config)
    require(canonical_hash(rnaledger) == args.rna_ledger_sha256
            and args.rna_ledger_sha256 == record['rna_ledger_sha256']
            and str(rnaledger['tasks']['rna-all-wait']['job_id']) != OLD_RNA_WAIT, 'Restored RNA canonical ledger is missing or changed')
    rna_registration = read_json(area / 'rna-registration.json')
    require(digest(area / 'rna-registration.json') == record['rna_registration_sha256']
            and rna_registration['status'] == 'complete'
            and canonical_hash(rnaplan) == rna_registration['amended_plan_sha256']
            and canonical_hash(rnaledger) == rna_registration['amended_ledger_sha256'],
            'RNA canonical amendment journal changed before controller release')
    wait = str(rnaledger['tasks']['rna-all-wait']['job_id'])
    nativewait = _repair_native_states([wait], {}, owner='pwa209', account='def-ptewarie_cpu')[wait]
    require(nativewait['state'] == 'PENDING', 'Controller-only release requires the RNA root to remain pending')
    wait_task = next(task for task in rnaplan['tasks'] if task['name'] == 'rna-all-wait')
    held_root = native_control(wait, 'rna-all-wait', wait_task['script'], held=True)
    transfer = read_json(root / 'state/transfer-rna.json')
    require(transfer.get('status') in {'running', 'complete'} and transfer.get('host') == 'narval3'
            and transfer.get('source_identity') == identity['source_identity']['sha256']
            and transfer.get('config_sha256') == canonical_hash(config), 'RNA worker no longer has a healthy source-bound status')
    if transfer['status'] == 'running':
        guard = read_json(root / 'state/.transfer-rna.lock')
        require(guard.get('pid') == transfer.get('pid') and guard.get('restart_id') == transfer.get('restart_id')
                and guard.get('host') == 'narval3', 'RNA worker ownership guard does not match its current receipt')
    admission = combined_admission(root, config, rid, area, 'release-admission')
    # No scientific result/readiness flags are changed by this management step.
    require(canonical_hash(read_json(root / 'state' / (rid + '-rna-full-submission.json'))) == args.rna_ledger_sha256
            and canonical_hash(read_json(planpath)) == registration['amended_plan_sha256']
            and canonical_hash(read_json(ledgerpath)) == registration['amended_ledger_sha256'], 'Canonical DAG changed after final combined admission')
    transfer_after = read_json(root / 'state/transfer-rna.json')
    require(transfer_after.get('status') in {'running', 'complete'} and transfer_after.get('restart_id') == transfer.get('restart_id')
            and transfer_after.get('pid') == transfer.get('pid'), 'RNA transfer failed or changed during release admission')
    require(sixth_transfer_binding(root, primary, framework, identity, other, config) == record['transfer_binding'],
            'The exact whole-roster HTTPS worker/policy changed during final admission')
    held_root = native_control(wait, 'rna-all-wait', wait_task['script'], held=True)
    record.update(status='release_in_progress', release_job_id=job, release_started_utc=now(),
                  rna_ledger_sha256=args.rna_ledger_sha256, admission=admission,
                  RNA_root_released=False, RNA_root_held_at_controller_release=held_root,
                  RNA_release_rule='A separate source-bound completion coordinator must verify every selected provider checksum and re-admit the workload before releasing this held RNA root')
    atomic_json(area / 'receipt.json', record)
    try:
        for ident in reversed(newids):
            # Persist intent first. A lost acknowledgement is inspected, never
            # replayed by this driver, even though scontrol release is harmless.
            record['release_intent_job_id'] = ident
            atomic_json(area / 'receipt.json', record)
            subprocess.run(['scontrol', 'release', ident], check=True, timeout=30)
            record['released'].append(ident)
            record.pop('release_intent_job_id', None)
            atomic_json(area / 'receipt.json', record)
        record.update(status='complete', completed_utc=now())
        atomic_json(area / 'receipt.json', record)
    except BaseException as error:
        record.update(status='release_ambiguous', error_type=type(error).__name__, error=str(error), automatic_replay_permitted=False)
        atomic_json(area / 'receipt.json', record)
        raise
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', choices=['prepare-and-register', 'release'], required=True)
    parser.add_argument('--framework', type=Path, required=True)
    parser.add_argument('--qualification', type=Path, required=True)
    parser.add_argument('--qualification-sha256', required=True)
    parser.add_argument('--helper-sha256', required=True)
    parser.add_argument('--case-id', required=True)
    parser.add_argument('--rna-ledger-sha256')
    parser.add_argument('--rna-descriptor', type=Path)
    parser.add_argument('--rna-descriptor-sha256')
    args = parser.parse_args()
    ctx = context(args)
    root, _, _, _, _, config, job, _ = ctx
    with resource_lease(root, config, cpus=1, gpus=0, max_hours=1, job_id=job):
        result = prepare(args, ctx) if args.stage == 'prepare-and-register' else release(args, ctx)
    print('CONTROLLER_PRIMARY_REPAIR ' + json.dumps({key: result[key] for key in ['status', 'case_id', 'replacement_jobs', 'released']}, sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
