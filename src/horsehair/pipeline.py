"""Finite scheduled control of the complete study's admitted phase graph.

The controller runs in Slurm. It records missing scientific inputs explicitly,
never replays an ambiguous submission and never substitutes qualification for
scientific completion. Provider transfer workers remain independent.
"""
from __future__ import annotations

import json
import hashlib
import math
import os
import re
import stat
import subprocess
import sys
import time
import uuid
from pathlib import Path, PurePosixPath
from datetime import datetime, timezone

from .core import atomic_json, canonical_hash, digest, exclusive_file_lock, native_job_id, read_json, source_identity, ReceiptLockTimeout


NODES = (
    ('rna-full', ('initial:rna-pilot-0', 'initial:rna-pilot-1', 'initial:molecular-pilot'), 'rna-full'),
    ('agents-development', ('initial:agent-benchmark-aggregate',), 'agents-development'),
    ('agents-final', ('agents-development',), 'agents-final'),
    ('behaviour-benchmark', ('initial:source-audit',), None),
    ('behaviour', ('behaviour-benchmark',), 'behaviour'),
    ('interventions', ('behaviour',), 'behaviour'),
    ('screen', ('behaviour', 'rna-full'), 'behaviour'),
    ('molecular-benchmark', ('screen', 'initial:molecular-pilot'), None),
    ('molecular-transfer', ('molecular-benchmark',), 'molecular'),
    ('molecular-corroboration', ('rna-full', 'initial:molecular-pilot'), None),
    ('synthesis', ('behaviour', 'interventions', 'molecular-transfer', 'agents-final', 'molecular-corroboration'), None),
)
TERMINAL_FAILURE = {'FAILED', 'TIMEOUT', 'OUT_OF_MEMORY', 'CANCELLED', 'NODE_FAIL', 'BOOT_FAIL', 'PREEMPTED', 'DEADLINE', 'REVOKED'}
STORAGE_REFRESH_SECONDS = 6 * 3600
PRIMARY_READ_ATTEMPTS = 3
PRIMARY_READ_BACKOFF_SECONDS = (1, 2)
PRIMARY_TRANSIENT_READ_ERRNOS = {5, 107, 108, 110, 116}  # EIO, ENOTCONN, ESHUTDOWN, ETIMEDOUT, ESTALE


def _verified_storage_artifact_eof(root, record):
    """Bind cached artifact totals to the unchanged bounded-read evidence.

    Payloads were measured at the storage receipt's original timestamp. Cache
    validation reads only its fixed evidence sidecar; it never stats the old
    artifact leaves whose Lustre size queries may block.
    """
    from .storage_artifacts import artifact_read_cap

    descriptor = record['artifact_eof_measurements']
    expected_path = root / 'state/storage-artifacts.json'
    if (not isinstance(descriptor, dict) or type(descriptor.get('schema_version')) is not int
            or descriptor['schema_version'] != 1 or descriptor.get('method') != 'owned_regular_eof'
            or descriptor.get('receipt_path') != str(expected_path)
            or not isinstance(descriptor.get('receipt_sha256'), str)
            or re.fullmatch(r'[0-9a-f]{64}', descriptor['receipt_sha256']) is None
            or type(descriptor.get('count')) is not int or descriptor['count'] <= 0
            or type(descriptor.get('bytes')) is not int or descriptor['bytes'] < 0
            or descriptor['count'] > record['study_files'] or descriptor['bytes'] > record['study_bytes']
            or type(record.get('personal_uid')) is not int or record['personal_uid'] < 0
            or expected_path.is_symlink() or expected_path.resolve() != expected_path
            or not expected_path.is_file()):
        raise ValueError('Artifact storage cache descriptor is not bound to its measured study evidence')
    raw = expected_path.read_bytes()
    receipt_hash = hashlib.sha256(raw).hexdigest()
    if receipt_hash != descriptor['receipt_sha256']:
        raise ValueError('Artifact storage evidence changed after measurement')
    evidence = json.loads(raw)
    if (not isinstance(evidence, dict) or type(evidence.get('schema_version')) is not int
            or evidence['schema_version'] != 1 or evidence.get('status') != 'measured'
            or evidence.get('method') != 'owned_regular_eof' or evidence.get('root') != str(root)
            or type(evidence.get('count')) is not int or evidence['count'] != descriptor['count']
            or type(evidence.get('bytes')) is not int or evidence['bytes'] != descriptor['bytes']
            or not isinstance(evidence.get('items'), list) or len(evidence['items']) != descriptor['count']):
        raise ValueError('Artifact storage sidecar has another root, method or measured roster')
    seen, total = set(), 0
    identity_fields = {'inode', 'uid', 'mode', 'nlink', 'device_major', 'device_minor', 'ctime_ns'}
    for item in evidence['items']:
        if not isinstance(item, dict) or not isinstance(item.get('relative_path'), str):
            raise ValueError('Artifact storage item lacks its lexical relative path')
        relative = PurePosixPath(item['relative_path'])
        if (not item['relative_path'] or '\\' in item['relative_path'] or '\0' in item['relative_path']
                or relative.is_absolute() or '..' in relative.parts or relative.as_posix() != item['relative_path']
                or item['relative_path'] in seen):
            raise ValueError('Artifact storage item path is unsafe or duplicated')
        artifact_path = root.joinpath(*relative.parts)
        cap = artifact_read_cap(artifact_path, root)
        identity = item.get('identity')
        if (type(cap) is not int or cap <= 0 or item.get('path') != str(artifact_path)
                or item.get('status') != 'measured' or item.get('method') != 'owned_regular_eof'
                or item.get('eof_observed') is not True or type(item.get('bytes')) is not int
                or not 0 <= item['bytes'] <= cap or type(item.get('read_cap_bytes')) is not int
                or item['read_cap_bytes'] != cap or not isinstance(item.get('sha256'), str)
                or re.fullmatch(r'[0-9a-f]{64}', item['sha256']) is None
                or not isinstance(identity, dict) or set(identity) != identity_fields
                or any(type(identity[key]) is not int or identity[key] < 0 for key in identity_fields)
                or identity['inode'] == 0 or identity['nlink'] == 0
                or identity['uid'] != record['personal_uid'] or not stat.S_ISREG(identity['mode'])):
            raise ValueError('Artifact storage item is not a bounded owned regular-file EOF measurement')
        seen.add(item['relative_path'])
        total += item['bytes']
    if total != descriptor['bytes'] or digest(expected_path) != receipt_hash:
        raise ValueError('Artifact storage evidence totals or bytes changed during validation')
    return dict(descriptor)


def _cached_storage_snapshot(root, *, now=None):
    """Reuse only recent, root-bound measurements with unchanged quota evidence."""
    root = Path(root).resolve()
    path = root / 'state/storage-budget.json'
    try:
        if path.is_symlink() or not path.is_file():
            return None
        raw = path.read_bytes()
        record = json.loads(raw)
        current = time.time() if now is None else now
        measured = record.get('measured_unix')
        if (not isinstance(record, dict) or record.get('status') != 'measured'
                or not isinstance(record.get('root'), str) or Path(record['root']).resolve() != root
                or type(measured) not in (int, float) or not math.isfinite(measured)
                or type(current) not in (int, float) or not math.isfinite(current)
                or not 0 <= current - measured < STORAGE_REFRESH_SECONDS):
            return None
        captured = datetime.fromisoformat(record.get('captured_utc', '').replace('Z', '+00:00'))
        if (captured.tzinfo is None or captured.utcoffset() != timezone.utc.utcoffset(captured)
                or not 0 <= current - captured.timestamp() < STORAGE_REFRESH_SECONDS
                or abs(captured.timestamp() - measured) > 1):
            return None
        for key in ('study_bytes', 'study_files', 'personal_available_bytes', 'personal_available_files'):
            value = record.get(key)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                return None
            if key.endswith('files') and int(value) != value:
                return None
        sources, hashes = record.get('native_source_paths'), record.get('native_source_hashes')
        if (not isinstance(sources, list) or not sources or not all(isinstance(p, str) for p in sources)
                or len(sources) != len(set(sources)) or not isinstance(hashes, dict)
                or set(hashes) != set(sources)):
            return None
        for source in sources:
            native_path = Path(source)
            if (not native_path.is_absolute() or native_path.is_symlink()
                    or not native_path.resolve().is_relative_to(root / 'state')
                    or not native_path.is_file() or not re.fullmatch(r'quota-[0-9]+\.txt', native_path.name)
                    or not isinstance(hashes[source], str) or not re.fullmatch(r'[0-9a-f]{64}', hashes[source])
                    or digest(native_path) != hashes[source]):
                return None
        artifact_evidence = (_verified_storage_artifact_eof(root, record)
                             if 'artifact_eof_measurements' in record else None)
        receipt_hash = hashlib.sha256(raw).hexdigest()
        if digest(path) != receipt_hash:
            return None
        result = {'basis': 'Verified measured storage receipt reused until its original six-hour deadline',
                'receipt_path': str(path), 'receipt_sha256': receipt_hash,
                'measured_unix': measured, 'captured_utc': record['captured_utc'],
                'age_seconds': current - measured, 'refresh_due_unix': measured + STORAGE_REFRESH_SECONDS,
                'native_source_hashes': hashes}
        if artifact_evidence is not None:
            result['artifact_eof_measurements'] = artifact_evidence
        return result
    except (OSError, ValueError, TypeError, AttributeError, OverflowError):
        return None


def _recovery_guard(path):
    """Permanent inode with a process lock: native termination releases it."""
    return exclusive_file_lock(path, timeout=0)


def _native_states(ids, runner=subprocess.run):
    ids = list(dict.fromkeys(map(str, ids)))
    if not ids:
        return {}
    if any(not re.fullmatch(r'\d+(?:_\d+)?', value) for value in ids):
        raise ValueError('Invalid native study job identity')
    response = runner(['sacct', '-j', ','.join(ids), '--format=JobID%64,JobIDRaw%64,State,ExitCode', '--noheader', '--parsable2'], text=True, capture_output=True)
    if response.returncode:
        raise RuntimeError('Native job state is unavailable; no phase will be submitted')
    states = {}
    for line in response.stdout.splitlines():
        parts = line.strip().split('|')
        if len(parts) >= 4 and re.fullmatch(r'\d+(?:_\d+)?', parts[0]):
            if not re.fullmatch(r'\d+', parts[1]):
                raise ValueError('Invalid physical native allocation identity')
            item = {'state': parts[2].split()[0].split('+')[0], 'exit_code': parts[3], 'job_id_raw': parts[1]}
            if parts[0] in states and states[parts[0]] != item:
                raise ValueError('Conflicting native allocation records')
            if any(v['job_id_raw'] == parts[1] for k, v in states.items() if k != parts[0]):
                raise ValueError('Physical native allocation has conflicting logical identities')
            states[parts[0]] = item
    return states


def _array_indices(expression):
    roster = set()
    for term in expression.split('%', 1)[0].split(','):
        match = re.fullmatch(r'(\d+)(?:-(\d+)(?::(\d+))?)?', term)
        if not match:
            raise ValueError('Unsupported native array roster')
        start, stop, step = int(match[1]), int(match[2] or match[1]), int(match[3] or 1)
        if stop < start or step < 1:
            raise ValueError('Invalid native array roster')
        roster.update(range(start, stop + 1, step))
    return roster


def _task_status(task, native, plan_task=None):
    if task.get('state') != 'submitted' or not task.get('job_id'):
        return 'ambiguous_submission'
    ident = str(task['job_id'])
    array = (plan_task or {}).get('resources', {}).get('array') or task.get('resources', {}).get('array')
    expected = {ident + '_' + str(index) for index in _array_indices(array)} if array else {ident}
    records = [value for key, value in native.items() if key in expected or key == ident]
    if not records:
        return 'awaiting_native_accounting'
    if any(r['state'] in TERMINAL_FAILURE or (r['state'] == 'COMPLETED' and r['exit_code'] != '0:0') for r in records):
        return 'failed'
    if not expected <= set(native):
        return 'awaiting_native_accounting'
    if all(r['state'] == 'COMPLETED' and r['exit_code'] == '0:0' for r in records):
        return 'complete'
    return 'running' if any(r['state'] in {'RUNNING', 'COMPLETING'} for r in records) else 'queued'


def _phase_status(ledger, native, plan_record=None):
    planned = {task['name']: task for task in (plan_record or {}).get('tasks', [])}
    if planned and set(ledger.get('tasks', {})) != set(planned):
        return 'ambiguous_submission'
    statuses = [_task_status(task, native, planned.get(name)) for name, task in ledger.get('tasks', {}).items()]
    if not statuses:
        return 'ambiguous_submission'
    if 'ambiguous_submission' in statuses:
        return 'ambiguous_submission'
    if 'failed' in statuses:
        return 'failed'
    if all(value == 'complete' for value in statuses):
        return 'complete'
    return 'running' if 'running' in statuses else 'queued'


def _task_terminal(task, native, plan_task):
    ident = str(task.get('job_id', ''))
    array = plan_task.get('resources', {}).get('array')
    expected = {ident + '_' + str(index) for index in _array_indices(array)} if array else {ident}
    return bool(task.get('state') == 'submitted' and expected <= set(native)
                and all(native[key]['state'] in TERMINAL_FAILURE | {'COMPLETED'} for key in expected))


def _weighted_antichain(weights, edges):
    """Exact maximum simultaneous weight in a finite dependency partial order."""
    keys=list(weights);reachable={key:set() for key in keys}
    for left,right in edges:reachable[left].add(right)
    for middle in keys:
        for left in keys:
            if middle in reachable[left]:reachable[left].update(reachable[middle])
    if any(key in reachable[key] for key in keys):raise ValueError('Cyclic scientific job dependencies')
    total=sum(weights.values());size=2*len(keys)+2;source=size-2;sink=size-1
    graph=[[] for _ in range(size)]
    def edge(a,b,capacity):
        graph[a].append([b,capacity,len(graph[b])]);graph[b].append([a,0,len(graph[a])-1])
    indexes={key:index for index,key in enumerate(keys)}
    for key,index in indexes.items():
        edge(source,index,weights[key]);edge(index+len(keys),sink,weights[key])
        for right in reachable[key]:edge(index,indexes[right]+len(keys),total+1)
    flow=0
    while True:
        level=[-1]*size;level[source]=0;queue=[source]
        for node in queue:
            for target,capacity,_ in graph[node]:
                if capacity>0 and level[target]<0:level[target]=level[node]+1;queue.append(target)
        if level[sink]<0:break
        cursor=[0]*size
        def push(node,amount):
            if node==sink:return amount
            while cursor[node]<len(graph[node]):
                item=graph[node][cursor[node]];target,capacity,reverse=item
                if capacity>0 and level[target]==level[node]+1:
                    sent=push(target,min(amount,capacity))
                    if sent:item[1]-=sent;graph[target][reverse][1]+=sent;return sent
                cursor[node]+=1
            return 0
        while (amount:=push(source,total+1)):flow+=amount
    return total-flow


def prospective_capacity(plan_records, ledgers, native):
    weights_cpu={};weights_gpu={};edges=[];native_nodes={}
    for phase,plan_record in plan_records.items():
        for task in plan_record['tasks']:
            key=phase+':'+task['name'];record=ledgers.get(phase,{}).get('tasks',{}).get(task['name'])
            finished=record is not None and _task_terminal(record,native,task)
            resources=task['resources'];array=resources.get('array')
            concurrency=min(int(array.split('%')[1]),len(_array_indices(array))) if array and '%' in array else len(_array_indices(array)) if array else 1
            weights_cpu[key]=0 if finished else int(resources.get('cpus',4))*concurrency
            weights_gpu[key]=0 if finished else int(bool(resources.get('gpu',False)))*concurrency
            if record and record.get('job_id'):native_nodes[str(record['job_id'])]=key
    for phase,plan_record in plan_records.items():
        for task in plan_record['tasks']:
            key=phase+':'+task['name']
            edges.extend((phase+':'+parent,key) for parent in task.get('depends',[]))
            edges.extend((native_nodes[str(ident)],key) for ident in task.get('external_dependencies',[]) if str(ident) in native_nodes)
    return {'cpus':_weighted_antichain(weights_cpu,edges),'gpus':_weighted_antichain(weights_gpu,edges)}


def prospective_registered_budget(plan_records, ledgers, native, accounting):
    """Conservative allocation bounds for every unfinished registered task.

    Pending arrays reserve all remaining members. Active allocations reserve
    their entire declared walltime; the existing active-lease reservation is
    not added again. Native completed cost is already in the accounting total.
    """
    cpu=float(accounting.get('spent_cpu_core_hours',0))
    gpu=float(accounting.get('spent_gpu_hours',0))
    for phase,plan_record in plan_records.items():
        for task in plan_record['tasks']:
            resources=task['resources'];array=resources.get('array')
            record=ledgers.get(phase,{}).get('tasks',{}).get(task['name'],{})
            ident=str(record.get('job_id',''))
            identities=[ident+'_'+str(index) for index in _array_indices(array)] if array else [ident]
            remaining=sum(not (record.get('state')=='submitted' and key in native
                               and native[key]['state'] in TERMINAL_FAILURE|{'COMPLETED'}) for key in identities)
            hours=float(resources.get('hours',2))*remaining
            cpu+=hours*int(resources.get('cpus',4));gpu+=hours*int(bool(resources.get('gpu',False)))
    return {'cpu_core_hours':cpu,'gpu_hours':gpu,'basis':'native spent plus full allocated bounds for every unfinished registered job member'}


def _check_result(path, config, identity, *, status=None):
    if not path.is_file():
        return False, 'Completion artifact is not present: ' + path.name
    result = read_json(path)
    if result.get('source_sha256') != identity or result.get('config_sha256', result.get('full_config_sha256')) != canonical_hash(config):
        return False, 'Completion artifact belongs to a different source or configuration: ' + path.name
    if status is not None and result.get('status') not in status:
        return False, 'Scientific completion is pending: ' + path.name
    return True, None


def _validate_plan(record, config, identity):
    if record.get('config_sha256') != canonical_hash(config) or record.get('release_identity') != identity:
        raise ValueError('Frozen plan belongs to a different configuration or source release')


def _phase_outputs(phase, config, root, identity, release_id=None):
    expected = {
        'behaviour-benchmark': root/'state/behaviour-workload-benchmark.json',
        'behaviour': root/'results/inference/H1.json',
        'interventions': root/'results/inference/H2.json',
        'molecular-benchmark': root/'state/molecular-workload-benchmark.json',
        'molecular-transfer': root/'results/inference/H3.json',
        'agents-final': root/'results/inference/H4.json',
    }
    if phase in expected:
        valid, reason = _check_result(expected[phase], config, identity)
        if not valid:
            return valid, reason
        result = read_json(expected[phase])
        if phase.endswith('benchmark') and result.get('complete_declared_workload') is not True:
            return False, 'Full numerical workload coverage has not been measured'
        if result.get('status') in {'pending', 'incomplete', 'unavailable'} or result.get('analysis_complete') is False:
            return False, 'Scientific diagnostics remain pending'
        if phase in {'behaviour', 'interventions', 'molecular-transfer', 'agents-final'}:
            from .report import CLAIMS, _operational_gates, validate_result
            claim = {'behaviour':'H1', 'interventions':'H2', 'molecular-transfer':'H3', 'agents-final':'H4'}[phase]
            unit = next(unit for ident, _, unit in CLAIMS if ident == claim)
            try:
                validate_result(result, claim, unit, config, root)
                if claim=='H4' and result.get('claim_status') not in {'supported','not_supported'}:
                    return False,'H4 competence/operating-region qualification remains pending'
                if claim != 'H4' and _operational_gates(result, claim, config)['missing']:
                    return False, 'Required scientific diagnostics remain pending'
            except (ValueError, FileNotFoundError, KeyError) as error:
                return False, 'Scientific evidence validation is pending: ' + str(error)
    if phase == 'agents-development':
        from .agents import _directory
        path = _directory(root, config['agents'])/'frozen_protocol.json'
        if not path.is_file() or read_json(path).get('full_config_sha256') != canonical_hash(config):
            return False, 'Complete development-only protocol freeze is pending'
    if phase == 'rna-full':
        freeze = root/'data/molecular/freeze/freeze.json'
        valid, reason = _check_result(root/'state/molecular-freeze.json', config, identity)
        if not valid:
            return valid, reason
        from .molecular import validate_freeze
        from .report import verify_observed_inputs
        try:
            record, _ = validate_freeze(freeze)
            verify_observed_inputs(record['provenance'])
        except (ValueError, FileNotFoundError, KeyError) as error:
            return False, 'Actual ant molecular score freeze is pending: ' + str(error)
    if phase == 'screen':
        proof = root/'results/interventions/target_provenance.json'
        if not proof.is_file():
            return False, 'Audited unique-gene/shared-control target provenance is pending'
        from .report import verify_observed_inputs
        try:
            record=read_json(proof)
            verify_observed_inputs(record)
            required=['validated_gene_ids','shared_control_covariance_preserved','reagents_clustered_by_gene']
            if any(record.get(key) is not True for key in required):
                return False, 'Target identities and covariance remain pending'
            if digest(root/'results/interventions/gene_effects.tsv')!=record.get('target_table_sha256'):
                return False, 'Actual gene outcome table changed'
        except (ValueError, FileNotFoundError, KeyError) as error:
            return False, 'Actual target provenance validation is pending: ' + str(error)
    if phase == 'molecular-corroboration':
        valid, reason=_check_result(root/'state/molecular-mantis-orthology.json', config, identity,
                                  status={'native_three_species_corroboration_computed'})
        if not valid:
            return valid, reason
        if not (root/'data/molecular/orthology_mantis/gene_orthology.tsv').is_file():
            return False, 'Actual three-species orthology output is pending'
    if phase == 'synthesis':
        path=root/'results/report/report.json'
        valid, reason=_check_result(path,config,identity)
        if not valid:
            return valid, reason
        from .report import auxiliary_readiness
        auxiliary=auxiliary_readiness(config,root,source_sha=identity)
        if not auxiliary['ready']:
            return False,'Declared auxiliary assay science remains pending: '+ '; '.join(auxiliary['pending'])
        if read_json(path).get('analysis_complete') is not True:
            return False,'Actual completed claim decisions and multiplicity correction are pending'
        if not release_id:
            return False,'Durable preservation release identity is pending'
        receipt=root/'state'/('archive-'+release_id+'.json')
        valid, reason=_check_result(receipt,config,identity,status={'complete'})
        if not valid:
            return valid,reason
        from .archive import verify
        record=read_json(receipt)
        directory=Path(config['paths']['project'])/'artifacts'/release_id
        try:
            if record.get('archive_directory')!=str(directory) or record.get('manifest_sha256')!=digest(directory/'archive_manifest.json'):
                return False,'Durable archive receipt or destination changed'
            verify(directory,config,{'sha256':identity},release_id)
        except (ValueError,FileNotFoundError,KeyError) as error:
            return False,'Verified durable preservation remains pending: '+str(error)
    return True, None


def _readiness(phase, config, root):
    if phase=='synthesis':
        from .report import auxiliary_readiness
        result=auxiliary_readiness(config,root)
        if not result['ready']:return False,result['pending']
    if phase in {'behaviour-benchmark', 'behaviour'}:
        from .behaviour import formal_behavior_readiness
        record = formal_behavior_readiness(config, root)
        return bool(record.get('ingestion_ready' if phase == 'behaviour-benchmark' else 'ready')), record.get('pending', [])
    if phase in {'interventions', 'screen'}:
        from .interventions import intervention_readiness
        record = intervention_readiness(config, root)
        kind = 'challenge' if phase == 'interventions' else 'screen'
        return bool(record.get(kind + '_ready')), record.get(kind + '_pending', [])
    return True, []


def _initial_ledger(root, release_id):
    path = root/'state'/(release_id+'-managed-initial-submission.json')
    if not path.is_file():
        raise FileNotFoundError('Complete managed initial queue is not registered')
    return path, read_json(path)


def _primary_execution(config, root, release, action, phase, directory=None):
    """Run source-sensitive admission/planning in the immutable primary package.

    A maintenance controller has different source bytes. Passing only a primary
    hash into its admission code would still import framework benchmark helpers
    and its planner would validate against the framework again. An isolated
    interpreter imports the complete verified primary package instead. This
    operation never submits jobs or changes scientific inputs/results. Only its
    declared admission receipts or fresh plan outputs may be written. A failed
    child is retried only before its first attempted filesystem mutation.
    """
    root, release = Path(root).resolve(), Path(release).resolve()
    identity = read_json(release/'release.json')
    if (identity.get('source_identity') != source_identity(release)
            or identity.get('config_sha256') != canonical_hash(config)
            or read_json(release/'configs/study.json') != config
            or Path(config['paths']['root']).resolve() != root
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', identity.get('release_id', ''))):
        raise ValueError('Primary execution source/configuration/study namespace changed')
    if action not in {'admission', 'plan'} or not re.fullmatch(r'[a-z0-9][a-z0-9-]*', phase):
        raise ValueError('Unsupported primary execution operation/phase')
    if action == 'plan':
        directory = Path(directory).resolve()
        if not directory.is_relative_to(root/'plans') or directory.exists():
            raise ValueError('Primary plan output must be a fresh study plan directory')
    nonce = uuid.uuid4().hex
    request = {'release':str(release), 'root':str(root), 'source_identity':identity['source_identity'],
               'config':config, 'action':action, 'phase':phase, 'nonce':nonce,
               'directory':str(directory) if directory is not None else None}
    program = r'''
import importlib,json,os,platform,sys
from pathlib import Path
# Standard-library platform discovery may invoke Windows' read-only version
# query. Complete it before observing the isolated scientific operation.
platform.uname()
platform.processor()
request=json.load(sys.stdin)
release=Path(request['release']).resolve()
stage='import';mutation_attempted=False;process_attempted=False
def observe(event,args):
    global mutation_attempted,process_attempted
    if event in {'subprocess.Popen','os.system','os.fork','os.forkpty','os.exec','os.posix_spawn','os.spawn'}:
        process_attempted=True
        raise RuntimeError('Primary admission/planning context cannot execute subprocess or scheduler commands')
    if event=='open':
        mode,flags=args[1],args[2]
        if ((isinstance(mode,str) and any(value in mode for value in 'wax+'))
                or (isinstance(flags,int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))):
            mutation_attempted=True
    elif event in {'os.mkdir','os.remove','os.rmdir','os.rename','os.link','os.symlink','os.truncate','os.chmod','os.chown','os.utime'}:
        mutation_attempted=True
sys.addaudithook(observe)
try:
    sys.path.insert(0,str(release/'src'))
    import horsehair
    from horsehair import core
    if (Path(horsehair.__file__).resolve()!=release/'src/horsehair/__init__.py'
            or Path(core.__file__).resolve()!=release/'src/horsehair/core.py'):
        raise ValueError('Primary package/core imported from another release')
    def verify():
        record=core.read_json(release/'release.json')
        if (record['source_identity']!=core.source_identity(release) or record['source_identity']!=request['source_identity']
                or record['config_sha256']!=core.canonical_hash(request['config'])
                or core.read_json(release/'configs/study.json')!=request['config']
                or Path(request['config']['paths']['root']).resolve()!=Path(request['root']).resolve()):
            raise ValueError('Primary child source/configuration/study namespace changed')
    stage='verification';verify()
    stage='import'
    module_name={'admission':'admission','plan':'slurm'}[request['action']]
    module=importlib.import_module('horsehair.'+module_name)
    if Path(module.__file__).resolve()!=release/'src/horsehair'/(module_name+'.py'):
        raise ValueError('Primary operation imported from another release')
    stage='operation'
    if request['action']=='admission':
        result=module.assess_admission(request['config'],Path(request['root']),phase=request['phase'])
    else:
        result=module.plan(request['config'],release,Path(request['directory']),request['phase'])
    stage='verification';verify()
    print('HHW_PRIMARY_CONTEXT_RESULT '+json.dumps({'nonce':request['nonce'],'action':request['action'],
          'phase':request['phase'],'source_identity':request['source_identity'],
          'config_sha256':core.canonical_hash(request['config']),'module_path':str(Path(module.__file__).resolve()),
          'result':result},allow_nan=False),flush=True)
except OSError as error:
    print('HHW_PRIMARY_CONTEXT_IO_ERROR '+json.dumps({'nonce':request['nonce'],'action':request['action'],
          'phase':request['phase'],'source_identity':request['source_identity'],
          'config_sha256':request['config_sha256'],'error_type':type(error).__name__,
          'errno':error.errno,'filename':os.fsdecode(error.filename) if error.filename is not None else None,
          'message':str(error),'stage':stage,'mutation_attempted':mutation_attempted,
          'process_attempted':process_attempted},allow_nan=False),flush=True)
    raise
'''
    environment = dict(os.environ)
    # Retain Alliance dependency-module paths (for example Arrow), while the
    # child explicitly prepends/verifies the primary package and excludes user
    # site packages. Do not substitute framework helpers in this process.
    environment['PYTHONNOUSERSITE'] = '1'
    environment['PYTHONDONTWRITEBYTECODE'] = '1'
    request['config_sha256'] = canonical_hash(config)
    failures = []
    recovery_path = root/'state/pipeline'/identity['release_id']/'primary-context/read-recovery'/(phase+'-'+action+'-'+nonce+'.json')
    diagnostic = {'status':'not_started', 'nonce':nonce, 'operation':action, 'phase':phase,
                  'controller_job_id':native_job_id(), 'primary_release':str(release),
                  'source_sha256':identity['source_identity']['sha256'], 'config_sha256':canonical_hash(config),
                  'maximum_attempts':PRIMARY_READ_ATTEMPTS, 'maximum_seconds':900, 'attempts':failures,
                  'rule':'Fresh verified primary process; retry only a named transient filesystem read before any mutation; never retry scheduler actions or partial plan outputs'}
    child_input = json.dumps(request, allow_nan=False)
    deadline = time.monotonic() + 900
    for attempt in range(1, PRIMARY_READ_ATTEMPTS + 1):
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            if failures:
                failures[-1]['retry_scheduled'] = False
            diagnostic.update(status='deadline_exhausted', attempts=failures, deadline_exhausted_unix=time.time())
            atomic_json(recovery_path, diagnostic)
            raise RuntimeError('Verified primary '+action+' deadline exhausted before child launch; no framework fallback; preserved diagnostics: '+str(recovery_path))
        timed_out = False
        command = [sys.executable, '-s', '-c', program]
        try:
            response = subprocess.run(command, input=child_input,
                                      text=True, capture_output=True, cwd=release, env=environment,
                                      timeout=remaining)
        except subprocess.TimeoutExpired as error:
            # Timeout does not establish a read-only failure or authorize a new
            # child, even if it printed a structured error before hanging.
            timed_out = True
            output = error.stdout.decode(errors='replace') if isinstance(error.stdout, bytes) else error.stdout or ''
            stderr = error.stderr.decode(errors='replace') if isinstance(error.stderr, bytes) else error.stderr or ''
            response = subprocess.CompletedProcess(command, -1, output, 'Primary child deadline expired; '+stderr)
        if not response.returncode:
            break
        prefix = 'HHW_PRIMARY_CONTEXT_IO_ERROR '
        errors = [line[len(prefix):] for line in response.stdout.splitlines() if line.startswith(prefix)]
        error = None
        if len(errors) == 1:
            try:
                error = json.loads(errors[0])
            except (ValueError, TypeError):
                pass
        bound = (isinstance(error, dict) and all(error.get(key) == request[key]
                 for key in ('nonce', 'action', 'phase', 'source_identity', 'config_sha256')))
        filename = error.get('filename') if bound else None
        contained = False
        if isinstance(filename, str) and Path(filename).is_absolute():
            candidate = Path(filename).resolve()
            contained = candidate.is_relative_to(root) or candidate.is_relative_to(release)
        transient = (not timed_out and bound and type(error.get('errno')) is int and error['errno'] in PRIMARY_TRANSIENT_READ_ERRNOS
                     and error.get('mutation_attempted') is False and error.get('process_attempted') is False
                     and error.get('stage') in {'import', 'verification', 'operation'} and contained)
        # A plan directory is an immutable output. Its existence refuses replay
        # even if an incomplete/foreign child claimed that no mutation occurred.
        untouched = action != 'plan' or not directory.exists()
        delay = PRIMARY_READ_BACKOFF_SECONDS[attempt-1] if attempt < PRIMARY_READ_ATTEMPTS else 0
        retry = bool(transient and untouched and attempt < PRIMARY_READ_ATTEMPTS and deadline-time.monotonic() > delay)
        failures.append({'attempt':attempt, 'returncode':response.returncode, 'error':error,
                         'timed_out':timed_out,
                         'bound_transient_read_failure':bool(transient), 'retry_scheduled':retry,
                         'stderr_tail':response.stderr[-4000:], 'checked_unix':time.time()})
        diagnostic = {'status':'retrying' if retry else 'failed', 'nonce':nonce, 'operation':action, 'phase':phase,
                      'controller_job_id':native_job_id(), 'primary_release':str(release),
                      'source_sha256':identity['source_identity']['sha256'], 'config_sha256':canonical_hash(config),
                      'maximum_attempts':PRIMARY_READ_ATTEMPTS, 'maximum_seconds':900, 'attempts':failures,
                      'rule':'Fresh verified primary process; retry only a named transient filesystem read before any mutation; never retry scheduler actions or partial plan outputs'}
        try:
            atomic_json(recovery_path, diagnostic)
        except OSError as preservation_error:
            raise RuntimeError('Verified primary '+action+' failed; no framework fallback: '+response.stderr[-4000:]
                               + '; preserving read-failure diagnostics also failed: '+str(preservation_error)) from preservation_error
        if not retry:
            raise RuntimeError('Verified primary '+action+' failed; no framework fallback: '+response.stderr[-4000:]
                               + '; preserved diagnostics: '+str(recovery_path))
        # Revalidate unchanged primary identity before every fresh process.
        if (source_identity(release) != identity['source_identity'] or read_json(release/'release.json') != identity
                or read_json(release/'configs/study.json') != config):
            failures[-1]['retry_scheduled'] = False
            diagnostic.update(status='source_changed', attempts=failures)
            atomic_json(recovery_path, diagnostic)
            raise ValueError('Primary execution source/configuration changed during read recovery')
        time.sleep(delay)
        if time.monotonic() >= deadline:
            failures[-1]['retry_scheduled'] = False
            diagnostic.update(status='deadline_exhausted', attempts=failures, deadline_exhausted_unix=time.time())
            atomic_json(recovery_path, diagnostic)
            raise RuntimeError('Verified primary '+action+' deadline exhausted after read revalidation/backoff; no framework fallback; preserved diagnostics: '+str(recovery_path))
    if failures:
        diagnostic.update(status='awaiting_result_validation', successful_attempt=attempt)
        atomic_json(recovery_path, diagnostic)
    prefix = 'HHW_PRIMARY_CONTEXT_RESULT '
    envelopes = [json.loads(line[len(prefix):]) for line in response.stdout.splitlines() if line.startswith(prefix)]
    if len(envelopes) != 1:
        raise ValueError('Primary execution returned no unique bound result')
    envelope = envelopes[0]
    expected_module = release/'src/horsehair'/('admission.py' if action == 'admission' else 'slurm.py')
    if (envelope.get('nonce') != nonce or envelope.get('action') != action or envelope.get('phase') != phase
            or envelope.get('source_identity') != identity['source_identity']
            or envelope.get('config_sha256') != canonical_hash(config)
            or Path(envelope.get('module_path', '')).resolve() != expected_module
            or source_identity(release) != identity['source_identity'] or read_json(release/'release.json') != identity):
        raise ValueError('Primary execution result/source/configuration identity changed')
    result = envelope['result']
    path = root/'state'/('admission-'+phase+'.json') if action == 'admission' else directory/'plan.json'
    persisted = read_json(path)
    returned = result.get(phase) if action == 'admission' else result
    if (persisted != returned or persisted.get('config_sha256') != canonical_hash(config)
            or persisted.get('phase') != phase
            or (persisted.get('source_sha256') != identity['source_identity']['sha256'] if action == 'admission'
                else persisted.get('release_identity') != identity['source_identity'])):
        raise ValueError('Primary execution receipt differs from its returned source-bound result')
    if failures:
        diagnostic.update(status='recovered', successful_attempt=attempt, recovered_unix=time.time())
        atomic_json(recovery_path, diagnostic)
    atomic_json(root/'state/pipeline'/identity['release_id']/'primary-context'/(phase+'-'+action+'.json'), {
        'status':'complete', 'operation':action, 'phase':phase, 'checked_unix':time.time(),
        'controller_job_id':native_job_id(), 'primary_release':str(release),
        'source_sha256':identity['source_identity']['sha256'], 'config_sha256':canonical_hash(config),
        'module_path':str(expected_module), 'receipt_path':str(path), 'receipt_sha256':digest(path),
        'filesystem_read_recovery':({'path':str(recovery_path), 'sha256':digest(recovery_path), 'failed_attempts':len(failures)} if failures else None),
        'rule':'Original primary admission/planner and their own package helpers execute without scientific overrides; no jobs submitted by this operation'})
    return result


def tick(config, root, release, *, native=None, planner=None, submitter=None, assessor=None, readiness=None, storage_guard=None):
    from .admission import assess_admission
    from .slurm import plan, submit
    root, release = Path(root), Path(release)
    identity = read_json(release/'release.json')
    observed = source_identity(release)
    if identity['source_identity'] != observed or identity['config_sha256'] != canonical_hash(config):
        raise ValueError('Managed source/configuration changed; no submission is authorized')
    release_id, sha = identity['release_id'], observed['sha256']
    base = root/'state/pipeline'/release_id
    base.mkdir(parents=True, exist_ok=True)
    initial_path, initial = _initial_ledger(root, release_id)
    if initial.get('source_sha256', sha) != sha:
        raise ValueError('Initial queue belongs to a different source release')
    ledgers = {'initial': initial}
    initial_plan=read_json(root/'plans'/(release_id+'-managed-initial')/'plan.json')
    _validate_plan(initial_plan, config, observed)
    if initial.get('plan_sha256') != canonical_hash(initial_plan):
        raise ValueError('Initial native ledger differs from its frozen plan')
    plan_records={'initial':initial_plan}
    for name, _, _ in NODES:
        path = root/'state'/(release_id+'-'+name+'-submission.json')
        if path.is_file():
            ledgers[name] = read_json(path)
            plan_records[name]=read_json(root/'plans'/(release_id+'-'+name)/'plan.json')
            _validate_plan(plan_records[name], config, observed)
            if ledgers[name].get('plan_sha256') != canonical_hash(plan_records[name]):
                raise ValueError('Native phase ledger differs from its frozen plan')
    ids = [task['job_id'] for ledger in ledgers.values() for task in ledger.get('tasks', {}).values() if task.get('state') == 'submitted']
    states = _native_states(ids) if native is None else dict(native)
    accounting_path = root/'state/resource-budget.json'
    if accounting_path.is_file():
        for ident, proof in read_json(accounting_path).get('native_jobs', {}).items():
            if proof.get('basis') == 'sacct final allocated wall time' and proof.get('state') in TERMINAL_FAILURE | {'COMPLETED'}:
                states.setdefault(ident, {'state':proof['state'], 'exit_code':proof['exit_code']})
    initial_tasks = {task['name']: task for task in initial_plan['tasks']}
    status = {'initial:'+name: _task_status(initial['tasks'].get(name, {}), states, task) for name, task in initial_tasks.items() if not name.startswith('pipeline-controller-')}
    output_pending = {}
    for name, ledger in ledgers.items():
        if name == 'initial':
            continue
        status[name] = _phase_status(ledger, states, plan_records[name])
        if status[name] == 'complete':
            valid, reason = _phase_outputs(name, config, root, sha,release_id=release_id)
            if not valid:
                status[name] = 'pending_scientific_completion'
                output_pending[name] = reason
    previous = read_json(base/'status.json') if (base/'status.json').exists() else {}
    phases = {}
    primary_context = release.resolve() != Path(__file__).resolve().parents[2]
    if planner is None:
        planner = (lambda cfg, rel, directory, phase: _primary_execution(cfg, root, rel, 'plan', phase, directory)) if primary_context else plan
    if assessor is None:
        assessor = (lambda cfg, study, phase: _primary_execution(cfg, study, release, 'admission', phase)) if primary_context else assess_admission
    submitter = submitter or submit
    readiness = readiness or _readiness
    for name, depends, admission_phase in NODES:
        if name in ledgers:
            phases[name] = {'status': status[name], 'ledger': str(root/'state'/(release_id+'-'+name+'-submission.json'))}
            if name in output_pending:
                phases[name]['reason'] = output_pending[name]
            continue
        waiting = [dep for dep in depends if status.get(dep) != 'complete']
        if waiting:
            phases[name] = {'status': 'waiting_for_dependencies', 'dependencies': waiting}
            continue
        ready, missing = readiness(name, config, root)
        if not ready:
            phases[name] = {'status': 'waiting_for_scientific_inputs', 'pending': missing}
            continue
        if storage_guard is not None:
            storage_guard()
        phase_forecast=None
        if admission_phase:
            record = assessor(config, root, phase=admission_phase)[admission_phase]
            if record.get('status') != 'admitted':
                phases[name] = {'status': 'resource_' + record.get('status', 'pending'), 'reasons': record.get('reasons', [])}
                continue
            phase_forecast=record
            if storage_guard is not None:
                storage_guard(phase_forecast)
        directory = root/'plans'/(release_id+'-'+name)
        ledger_path = root/'state'/(release_id+'-'+name+'-submission.json')
        journal_path=base/(name+'-plan-journal.json')
        if directory.exists():
            journal=read_json(journal_path) if journal_path.exists() else {}
            candidate=read_json(directory/'plan.json') if (directory/'plan.json').is_file() else None
            if journal.get('state')!='plan_frozen' or journal.get('source_sha256') != sha or candidate is None or journal.get('plan_sha256')!=canonical_hash(candidate):
                phases[name] = {'status': 'requires_submission_review', 'reason': 'Plan/ledger outcome is not confirmed; never replay ambiguity'}
                continue
        else:
            atomic_json(journal_path,{'state':'planning','phase':name,'source_sha256':sha})
            planner(config, release, directory, name)
            candidate=read_json(directory/'plan.json')
            atomic_json(journal_path,{'state':'plan_frozen','phase':name,'source_sha256':sha,'plan_sha256':canonical_hash(candidate)})
        _validate_plan(candidate, config, observed)
        capacity=prospective_capacity(dict(plan_records,**{name:candidate}),ledgers,states)
        if capacity['cpus']>config['resources']['cpu_ceiling'] or capacity['gpus']>config['resources']['gpu_concurrency']:
            phases[name]={'status':'waiting_for_study_capacity','prospective_capacity':capacity}
            continue
        allocations=prospective_registered_budget(dict(plan_records,**{name:candidate}),ledgers,states,
                       read_json(accounting_path) if accounting_path.is_file() else {})
        if allocations['cpu_core_hours']>config['resources']['cpu_core_hours_cap'] or allocations['gpu_hours']>config['resources']['gpu_hours_cap']:
            phases[name]={'status':'waiting_for_registered_resource_budget','prospective_registered_budget':allocations}
            continue
        # Planning/primary admission may take several minutes. The scheduled
        # controller supplies a fresh check at the actual mutation boundary;
        # offline callers need no native quota command.
        if storage_guard is not None:
            storage_guard(phase_forecast) if phase_forecast is not None else storage_guard()
        atomic_json(journal_path,{'state':'submitting','phase':name,'source_sha256':sha,'plan_sha256':canonical_hash(candidate)})
        ledger = submitter(directory/'plan.json', ledger_path)
        atomic_json(journal_path,{'state':'submitted','phase':name,'source_sha256':sha,'plan_sha256':canonical_hash(candidate),'ledger':str(ledger_path)})
        ledgers[name]=ledger;plan_records[name]=candidate
        phases[name] = {'status': 'queued', 'ledger': str(ledger_path), 'job_ids': [task['job_id'] for task in ledger['tasks'].values()]}
        status[name] = 'queued'
    now = time.time()
    failed = [key for key, value in status.items() if value in {'failed', 'ambiguous_submission'}]
    phase_graph_complete = all(phases[name]['status'] == 'complete' for name, _, _ in NODES)
    primary_phases=('behaviour','interventions','molecular-transfer','agents-final')
    primary_complete=all(name in phases and phases[name]['status']=='complete' for name in primary_phases)
    from .report import auxiliary_readiness
    auxiliary=auxiliary_readiness(config,root,source_sha=sha)
    complete=phase_graph_complete and primary_complete and auxiliary['ready']
    overall_status=('complete' if complete else 'needs_repair' if failed else
                    'waiting_for_auxiliary_scientific_inputs' if primary_complete and not auxiliary['ready'] else
                    'waiting_for_primary_scientific_evidence' if phase_graph_complete else 'active')
    record = {'schema_version': 1, 'release_id': release_id, 'source_sha256': sha, 'config_sha256': canonical_hash(config),
              'checked_unix': now, 'controller_job_id': native_job_id(), 'status': overall_status,
              'initial_tasks': {key.split(':', 1)[1]: value for key, value in status.items() if key.startswith('initial:')},
              'phases': phases, 'failed_native_stages': failed, 'previous_checked_unix': previous.get('checked_unix'),
              'primary_analyses_complete':primary_complete,'phase_graph_complete':phase_graph_complete,
              'auxiliary_analyses_complete':auxiliary['ready'],'auxiliary_readiness':auxiliary,
              'scientific_results_complete': complete, 'publication_ready': False,
              'scope': 'All declared phase controllers are deployed; scientific jobs launch only after native dependency, source-readiness and measured resource admission checks'}
    atomic_json(base/'status.json', record)
    return record


def run(config, root, *, segment=0, max_seconds=None, interval=60, primary_release=None):
    from .budget import native_accounting,storage_snapshot,require_storage_headroom,StorageHeadroomError
    root = Path(root).resolve(); framework = Path(__file__).resolve().parents[2]
    release = framework if primary_release is None else Path(primary_release).resolve()
    if not native_job_id():
        raise RuntimeError('The sustained pipeline controller belongs in Slurm')
    if segment not in range(4) or interval < 30:
        raise ValueError('Invalid finite controller segment')
    limit = min(float(140*3600 if max_seconds is None else max_seconds), 140*3600)
    if not math.isfinite(limit) or limit < 30:
        raise ValueError('Controller lifetime must be at least30seconds and bounded')
    deadline = time.monotonic() + limit
    framework_identity = read_json(framework/'release.json')
    identity = read_json(release/'release.json')
    for candidate, record in ((framework, framework_identity), (release, identity)):
        if (record.get('source_identity') != source_identity(candidate)
                or record.get('config_sha256') != canonical_hash(config)
                or read_json(candidate/'configs/study.json') != config
                or Path(config['paths']['root']).resolve() != root):
            raise ValueError('Controller framework/primary release source or full configuration changed')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', identity.get('release_id', '')):
        raise ValueError('Unsafe controller primary release identity')
    directory = root/'state/pipeline'/identity['release_id'];directory.mkdir(parents=True, exist_ok=True)
    lock = directory/'controller.lock'
    recovery=directory/'controller-recovery.lock'
    with _recovery_guard(recovery):
        if lock.exists():
            original=lock.read_bytes();previous=json.loads(original);old=str(previous.get('job_id',''))
            proof=_native_states([old]).get(old)
            if not proof or proof['state'] not in TERMINAL_FAILURE|{'COMPLETED'}:
                raise RuntimeError('Existing controller lock has no native terminal proof; never start a duplicate')
            if not lock.exists() or lock.read_bytes()!=original:
                raise RuntimeError('Controller lock owner changed during native recovery; preserve live ownership')
            archived=directory/('controller-lock-'+old+'-'+str(time.time_ns())+'.json')
            atomic_json(archived,{'original':previous,'native_terminal_proof':proof})
            lock.unlink()
        with lock.open('x') as handle:
            json.dump({'job_id': native_job_id(), 'segment': segment, 'started_unix': time.time()}, handle)
    owner=lock.read_bytes()
    last_signature = None;last_storage = None;warning_count = 0;last_warning_signature = None
    storage_warning_signature=None
    def fresh_storage_guard(forecast=None):
        # The already configured total study file ceiling is a conservative
        # personal headroom requirement, separate from the six-hour census.
        # It changes no numerical method or declared scientific configuration.
        forecast=forecast or {}
        files=forecast.get('additional_files',0)
        return require_storage_headroom(root,required_files=files,required_bytes=forecast.get('additional_bytes',0),
            reserve_files=max(0,config['resources']['scratch_budget_files']-files),
            reserve_bytes=config['resources'].get('scratch_reserve_bytes',0))
    try:
        framework_record = {'controller_job_id': native_job_id(), 'segment': segment, 'started_unix': time.time(),
                            'framework_release': str(framework), 'framework_source_identity': framework_identity['source_identity'],
                            'primary_release': str(release), 'primary_source_identity': identity['source_identity'],
                            'config_sha256': canonical_hash(config),
                            'provenance_rule': 'Controller framework executes from its own verified source; primary plans, numeric jobs and scientific result validation retain the verified primary release'}
        atomic_json(directory / ('controller-framework-' + str(native_job_id()) + '.json'), framework_record)
        print(json.dumps({'controller_framework': framework_record}), flush=True)
        storage_cache = _cached_storage_snapshot(root)
        if storage_cache is not None:
            last_storage = time.monotonic() - storage_cache['age_seconds']
            cache_use = {**storage_cache, 'controller_job_id': native_job_id(), 'segment': segment,
                         'checked_unix': time.time(), 'config_sha256': canonical_hash(config),
                         'source_sha256': identity['source_identity']['sha256'],
                         'controller_framework_source_sha256': framework_identity['source_identity']['sha256']}
            atomic_json(directory / ('storage-cache-use-' + str(native_job_id()) + '.json'), cache_use)
            print(json.dumps({'storage_cache': cache_use}), flush=True)
        record=None
        while time.monotonic() < deadline:
            try:
                fresh_storage_guard()
                ids = []
                for path in (root/'state').glob('*submission*.json'):
                    ledger = read_json(path)
                    if ledger.get('job_id'):
                        ids.append(str(ledger['job_id']))
                    ids.extend(str(task['job_id']) for task in ledger.get('tasks', {}).values() if task.get('state') == 'submitted')
                if ids:
                    native_accounting(root, ids)
                if last_storage is None or time.monotonic() - last_storage >= STORAGE_REFRESH_SECONDS:
                    storage_snapshot(root);last_storage = time.monotonic()
                record = tick(config, root, release,storage_guard=fresh_storage_guard)
            except StorageHeadroomError as exc:
                # Keep the finite controller alive without scheduling or
                # repeatedly creating files while another project uses quota.
                record={**(record or {}),'status':'pending_personal_storage','checked_unix':time.time(),
                    'controller_job_id':native_job_id(),'segment':segment,'config_sha256':canonical_hash(config),
                    'source_sha256':identity['source_identity']['sha256'],
                    'phases':(record or {}).get('phases',{}),'initial_tasks':(record or {}).get('initial_tasks',{}),
                    'failed_native_stages':(record or {}).get('failed_native_stages',[]),
                    'scientific_results_complete':False,'publication_ready':False,'scheduling_paused':True,
                    'personal_storage_headroom':exc.record,'reasons':exc.record['reasons']}
                atomic_json(directory/'status.json',record)
                signature=canonical_hash(exc.record['reasons'])
                if signature!=storage_warning_signature:
                    print(json.dumps({'status':record['status'],'reasons':record['reasons'],
                        'personal_available_files':exc.record['personal_available_files']}),flush=True)
                    storage_warning_signature=signature
                time.sleep(min(interval,max(0,deadline-time.monotonic())))
                continue
            except ReceiptLockTimeout as exc:
                # A live receipt writer retains its kernel lock. Defer this tick
                # without altering owners, native costs or submission journals.
                warning_count += 1
                warning = {'status':'waiting_for_receipt_guard','warning_count':warning_count,'last_unix':time.time(),
                           'controller_job_id':native_job_id(),'segment':segment,'config_sha256':canonical_hash(config),
                           'source_sha256':identity['source_identity']['sha256'],
                           'controller_framework_source_sha256':framework_identity['source_identity']['sha256'],**exc.diagnostics}
                atomic_json(directory/'receipt-guard-warning.json',warning)
                signature = canonical_hash({'guard_path':exc.path,'holder':exc.last_known_holder})
                if signature != last_warning_signature:
                    print(json.dumps(warning),flush=True);last_warning_signature = signature
                time.sleep(min(interval,max(0,deadline-time.monotonic())))
                continue
            signature = canonical_hash({'phases': record['phases'], 'initial': record['initial_tasks'], 'status': record['status']})
            if signature != last_signature:
                print(json.dumps({'status': record['status'], 'phases': {key:value['status'] for key,value in record['phases'].items()}, 'failed_native_stages': record['failed_native_stages']}), flush=True)
                last_signature = signature
            if record['status'] == 'complete':
                return record
            time.sleep(min(interval, max(0, deadline-time.monotonic())))
        if record is None:
            record={'status':'waiting_for_receipt_guard','scientific_results_complete':False,'publication_ready':False,
                    'phases':{},'initial_tasks':{},'failed_native_stages':[], 'config_sha256':canonical_hash(config),
                    'source_sha256':identity['source_identity']['sha256']}
        record.update(segment=segment,receipt_guard_warning_count=warning_count,segment_status='finite_segment_finished', continuation='next prequeued controller segment' if segment < 3 else 'four-segment window exhausted; preserve state for reviewed continuation')
        atomic_json(directory/'status.json', record)
        return record
    finally:
        if lock.exists() and lock.read_bytes()==owner:
            lock.unlink()
