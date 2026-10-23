"""Finite primary orchestration using separately qualified external censuses.

The primary runs in its original d89 Python package. The 4f census verifier is
a separate process. No scientific function, global census function, canonical
plan, submission journal or old scheduler allocation is changed here.
"""
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from datetime import datetime, timezone
import argparse
import ast
import copy
import hashlib
import importlib.util
import json
import math
import os
import re
import runpy
import shlex
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

from horsehair.core import canonical_hash, digest, read_json, source_identity, native_job_id

ROOT = Path('/scratch/pwa209/horsehair-worm')
OLD_FRAMEWORK = Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261021-57e0eb8c4c6c-d89b31180d4c')
OLD_SOURCE = 'd89b31180d4c4ffe9c4023dc5534b96f2d157a3a8e3aecaff30849283d00a705'
PIPELINE_SHA = 'ef954fe249e376e0d911b64b67fae16290216b62f2efb93ecace25598fbcc575'
OLD_BUDGET_SHA = '59c0eabaac4e63077b3c567d2ec8f6895940a46305ea3d81f8ddf3ce48bf0745'
FRAMEWORK = ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE = '4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY = Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA = '0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG = '446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
SUPPORT = ROOT/'maintenance/study-primary-external-census-policy-v1-20261023'
SELF_NAME = 'run-primary-external-census-v1-20261023.py'
TEST_NAME = 'test_primary_external_census_v1_20261023.py'
CENSUS_SUPPORT = ROOT/'maintenance/study-af-rolling-census-support-v4-20261023'
CENSUS_MANIFEST_SHA = '1b21742f6ad5b7b48899a4890988da7ddba439798ba8b12a2bad57c7f1898e6e'
CENSUS_QUALIFICATION = ROOT/'maintenance/study-af-rolling-census-qualification-v5-20261023/qualification.json'
OUTER_NAME = 'run-af-agent-controller-census-policy-v1-20261023.py'
GUARD = ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA = '358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER = GUARD.with_name('native-scheduler-read.py')
SCHEDULER_SHA = '100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
REGISTRY = CENSUS_SUPPORT/'registered-snapshot-read-guard-v4-20261022.py'
REGISTRY_SHA = '5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
POLICY = ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA = 'f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
HOURS = 72
QUALIFICATION_HOURS = 1
MAX_CHECKS = 24000
QUOTA_COMMAND = ['lfs', 'quota', '-u', '3164768', '/scratch/pwa209']
FALSE_FLAGS = {'scientific_result': False, 'admission_authority': False,
               'scientific_gates_cleared': False, 'freshness_timestamp_rewritten': False,
               'canonical_science_changed': False}


def require(value, message):
    if not value:
        raise ValueError(message)


def strict(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value, 'Duplicate JSON key refused')
            value[key] = item
        return value
    def constant(value):
        raise ValueError('Nonfinite JSON refused')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def packet(path, sha=None, *, namespace=None, limit=8_000_000):
    namespace = ROOT if namespace is None else Path(namespace)
    p = Path(path)
    require(p.is_absolute() and p == p.resolve() and p != namespace and p.is_relative_to(namespace)
            and p.is_file() and all(not q.is_symlink() for q in [p, *p.parents] if q.is_relative_to(namespace)),
            'Ordinary confined immutable file required')
    with p.open('rb') as stream:
        raw = stream.read(limit+1)
    actual = hashlib.sha256(raw).hexdigest()
    require(len(raw) <= limit and (sha is None or actual == sha), 'Bounded immutable file changed')
    return raw, actual


def exclusive(path, value):
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False)+'\n').encode()
    with Path(path).open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def locator(path):
    return {'path': str(path), 'sha256': digest(path)}


def module(path, sha, name):
    packet(path, sha, limit=200_000)
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    require(digest(path) == sha, 'Verified module changed during import')
    return result


def environment(framework):
    first = str(Path(framework).resolve()/'src')
    known = {str(p/'src') for p in (FRAMEWORK, OLD_FRAMEWORK, PRIMARY, FRAMEWORK.resolve(), OLD_FRAMEWORK.resolve(), PRIMARY.resolve())}
    retained = []
    for value in os.environ.get('PYTHONPATH', '').split(os.pathsep):
        normalized = value.replace('\\', '/')
        other_release = '/horsehair-worm/releases/' in normalized and normalized.rstrip('/').endswith('/src')
        if value and value not in known and value != first and not other_release:
            retained.append(value)
    paths = [first, *retained]
    env = {**os.environ, 'PYTHONPATH': os.pathsep.join(paths), 'PYTHONDONTWRITEBYTECODE': '1',
           'PYTEST_DISABLE_PLUGIN_AUTOLOAD': '1', 'TZ': 'UTC'}
    for name in ('PYTEST_ADDOPTS', 'PYTEST_PLUGINS'):
        env.pop(name, None)
    return env


def package_origins(framework):
    expected = Path(framework).resolve()/'src/horsehair'
    result = {}
    for name, value in tuple(sys.modules.items()):
        if name == 'horsehair' or name.startswith('horsehair.'):
            path = getattr(value, '__file__', None)
            if path is not None:
                path = Path(path).resolve()
                require(path.is_relative_to(expected), 'Horsehair packages from different releases cannot mix: '+name)
                result[name] = str(path)
    require('horsehair' in result, 'Actual execution package origin required')
    return result


class PendingExternalCensus(ValueError):
    pass


class PendingNativeQuota(ValueError):
    pass


def quota_guard(original, consumer):
    """Original headroom calculation, with authority checked on both sides."""
    def checked(root, *args, **kwargs):
        consumer(root)
        try:
            result = original(root, *args, **kwargs)
        except subprocess.TimeoutExpired as error:
            if type(error) is not subprocess.TimeoutExpired or error.cmd != QUOTA_COMMAND or error.timeout != 30:
                raise
            raise PendingNativeQuota('Owned native quota read timed out after30seconds; no quota evidence admits work') from error
        consumer(root)
        return result
    return checked


def pending_record(previous, identity, framework_identity, segment, error):
    quota = type(error) is PendingNativeQuota
    return {**(previous or {}), 'status': 'pending_native_quota' if quota else 'pending_external_storage_census',
            'checked_unix': time.time(), 'controller_job_id': native_job_id(), 'segment': segment,
            'config_sha256': CONFIG, 'source_sha256': identity['source_identity']['sha256'],
            'controller_framework_source_sha256': framework_identity['source_identity']['sha256'],
            'phases': (previous or {}).get('phases', {}), 'initial_tasks': (previous or {}).get('initial_tasks', {}),
            'failed_native_stages': (previous or {}).get('failed_native_stages', []),
            'scientific_results_complete': False, 'publication_ready': False, 'scheduling_paused': True,
            'reasons': [str(error)], 'retry_after_seconds': 60, **FALSE_FLAGS}


def derive_run(pipeline, consumer, *, expected_sha=PIPELINE_SHA):
    """A private function; the original module and every original callable stay."""
    source = Path(pipeline.__file__)
    require(digest(source) == expected_sha, 'Exact original primary pipeline source required')
    tree = ast.parse(source.read_bytes(), feature_version=(3, 11))
    original = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'run')
    derived = copy.deepcopy(original)
    index = next(i for i, n in enumerate(derived.body) if isinstance(n, ast.ImportFrom) and n.module == 'budget')
    aliases = ast.parse('storage_snapshot=__primary_consumer\nrequire_storage_headroom=__primary_quota_guard(require_storage_headroom,__primary_consumer)\n').body
    derived.body[index+1:index+1] = aliases
    loops = [n for n in ast.walk(derived) if isinstance(n, ast.While)]
    require(len(loops) == 1, 'One original finite primary loop required')
    transaction = next(n for n in loops[0].body if isinstance(n, ast.Try))
    handler = ast.parse('try:\n pass\nexcept (__PrimaryPendingCensus,__PrimaryPendingQuota) as exc:\n record=__primary_pending_record(record,identity,framework_identity,segment,exc)\n atomic_json(directory/"status.json",record)\n time.sleep(min(interval,max(0,deadline-time.monotonic())))\n continue\n').body[0].handlers[0]
    transaction.handlers.insert(0, handler)
    restored = copy.deepcopy(derived)
    del restored.body[index+1:index+3]
    restored_loop = next(n for n in ast.walk(restored) if isinstance(n, ast.While))
    restored_try = next(n for n in restored_loop.body if isinstance(n, ast.Try))
    restored_try.handlers.pop(0)
    require(ast.dump(restored, include_attributes=False) == ast.dump(original, include_attributes=False),
            'Only the two local operational bindings and one pending handler may change')
    objects = {k: v for k, v in vars(pipeline).items() if callable(v)}
    namespace = dict(vars(pipeline))
    namespace.update(__primary_consumer=consumer, __primary_quota_guard=quota_guard,
                     __PrimaryPendingCensus=PendingExternalCensus, __PrimaryPendingQuota=PendingNativeQuota,
                     __primary_pending_record=pending_record)
    generated = ast.fix_missing_locations(ast.Module(body=[derived], type_ignores=[]))
    exec(compile(generated, str(Path(__file__)), 'exec'), namespace)
    require(all(getattr(pipeline, k) is v for k, v in objects.items()), 'Original primary functions changed')
    return namespace['run'], {'pipeline_source_sha256': expected_sha, 'normal_branch_ast_preserved': True,
                              'nested_storage_guard_ast_preserved': True, 'original_callables': objects}


def own_registration(args, job, argv, mode, descriptor_sha):
    plan = strict(packet(args.native_plan)[0])
    ledger = strict(packet(args.native_ledger)[0])
    expected_source = SOURCE if mode == 'qualify' else OLD_SOURCE
    require(plan.get('root') == str(ROOT) and plan.get('config_sha256') == ledger.get('config_sha256') == CONFIG
            and plan.get('release_identity', {}).get('sha256') == ledger.get('source_sha256') == expected_source
            and ledger.get('plan_sha256') == canonical_hash(plan), 'Actual own full source/config registered pair required')
    selected = [t for t in plan.get('tasks', []) if t.get('name') == args.native_task]
    require(len(selected) == 1, 'One actual own registered task required')
    task = selected[0]
    row = ledger.get('tasks', {}).get(args.native_task, {})
    text = packet(args.native_script)[0].decode()
    command = shlex.split(task.get('command', ''))
    cpus, hours = (4, 1) if mode == 'qualify' else (1, HOURS)
    require(command.count(str(Path(__file__))) == 1 and command[command.index(str(Path(__file__)))+1:] == argv
            and task.get('script') == str(args.native_script) and task['command'] in text
            and canonical_hash(text) == task.get('script_sha256') == row.get('script_sha256')
            and row.get('state') == 'submitted' and row.get('job_id') == job
            and task.get('resources') == {'cpus': cpus, 'memory': '64G', 'hours': hours, 'gpu': False, 'array': None}
            and task.get('primary_external_census') == {'helper': locator(Path(__file__)),
                'policy_descriptor': {'path': str(args.policy_descriptor), 'sha256': descriptor_sha}, 'mode': mode},
            'Every own helper/argument/script/native/resource/intent binding must match')
    return {'plan': locator(args.native_plan), 'ledger': locator(args.native_ledger),
            'script': locator(args.native_script), 'task_name': args.native_task, 'native_job_id': job}, task


def native_fields(response):
    response.check_returncode()
    lines = [x for x in response.stdout.splitlines() if x.strip()]
    require(len(lines) == 1, 'One native allocation row required')
    key = r'[A-Za-z][A-Za-z0-9_/:]*'
    pairs = re.findall(r'(?<!\S)('+key+r')=(.*?)(?=\s+'+key+r'=|$)', lines[0])
    fields = {k: v.strip() for k, v in pairs}
    require(len(fields) == len(pairs), 'Duplicate native scheduler fields refused')
    return fields


def native_allocation(job, args, mode, reader, area, *, wall=time.time):
    value = native_fields(reader(job, area/'native-own-read'))
    cpus, hours = (4, 1) if mode == 'qualify' else (1, HOURS)
    require(value.get('JobId') == job and value.get('UserId') == 'pwa209(3164768)'
            and value.get('Account') == 'def-ptewarie_cpu' and value.get('JobState') == 'RUNNING'
            and value.get('NumCPUs') == str(cpus) and value.get('NumNodes') in {'1', '1-1'}
            and value.get('MinMemoryNode') in {'64G', '65536M'} and value.get('Command') == str(args.native_script)
            and value.get('JobName') == 'hhw-'+args.native_task and 'ArrayJobId' not in value and 'ArrayTaskId' not in value,
            'Owned scalar exact primary/qualification allocation required')
    duration = re.fullmatch(r'(?:(\d+)-)?(\d+):(\d\d):(\d\d)', value.get('TimeLimit', ''))
    require(duration is not None, 'Finite actual allocation limit required')
    days, hh, mm, ss = (int(x or 0) for x in duration.groups())
    require(days*86400+hh*3600+mm*60+ss == hours*3600, 'Actual native duration differs')
    for name in ('ReqTRES', 'AllocTRES'):
        pairs = [x.partition('=') for x in value.get(name, '').split(',')]
        tres = {k: v for k, sep, v in pairs}
        require(len(tres) == len(pairs) and all(k and sep and v for k, sep, v in pairs)
                and tres.get('cpu') == str(cpus) and tres.get('node') == '1' and tres.get('mem') in {'64G', '65536M'}
                and not any('gpu' in k.lower() for k in tres), 'Exact actual CPU-only allocation TRES required')
    require(all('gpu' not in value.get(k, '').lower() for k in ('Gres', 'TresPerNode', 'TresPerJob', 'TresPerTask')), 'No native GPU allowed')
    end = datetime.fromisoformat(value.get('EndTime', '')).replace(tzinfo=timezone.utc).timestamp()
    env = float(os.environ.get('SLURM_JOB_END_TIME', 'nan'))
    require(math.isfinite(env) and abs(env-end) <= 2 and wall()+1200 < end, 'Finite actual native end/margin required')
    return value, min(end, env)


def policy_sources(path, sha):
    require(Path(path).is_relative_to(SUPPORT), 'Exact primary policy namespace required')
    policy = strict(packet(path, sha)[0])
    require(set(policy) == {'kind', 'source_manifest', 'census', 'qualification'}
            and policy['kind'] == 'primary-external-census-policy-v1', 'Exact separate primary policy required')
    binding = policy['source_manifest']
    require(set(binding) == {'path', 'sha256'} and Path(binding['path']) == SUPPORT/'source-manifest.json', 'Exact own source manifest required')
    manifest = strict(packet(binding['path'], binding['sha256'])[0])
    require(set(manifest) == {'kind', 'files', 'suites'} and manifest['kind'] == 'primary-external-census-source-manifest-v1'
            and set(manifest['files']) == {SELF_NAME, TEST_NAME}
            and set(manifest['suites']) == {'baseline_pipeline', 'baseline_budget', 'primary_policy'}, 'Complete separate source/test closure required')
    pins = {str(path): sha, binding['path']: binding['sha256']}
    for name, value in manifest['files'].items():
        packet(SUPPORT/name, value, limit=200_000)
        pins[str(SUPPORT/name)] = value
    require(Path(__file__) == SUPPORT/SELF_NAME and digest(Path(__file__)) == manifest['files'][SELF_NAME], 'Exact registered own source required')
    expected = {'baseline_pipeline': 'tests/test_pipeline.py', 'baseline_budget': 'tests/test_budget.py', 'primary_policy': TEST_NAME}
    for scope, relative in expected.items():
        contract = manifest['suites'][scope]
        require(set(contract) == {'source', 'source_sha256', 'tests', 'roster_sha256'} and contract['source'] == relative
                and type(contract['tests']) is int and contract['tests'] > 0
                and re.fullmatch('[0-9a-f]{64}', str(contract['roster_sha256'])), 'Exact complete suite contract required')
        source = OLD_FRAMEWORK.resolve()/relative if scope.startswith('baseline_') else SUPPORT/relative
        packet(source, contract['source_sha256'], namespace=OLD_FRAMEWORK.resolve() if scope.startswith('baseline_') else ROOT)
        pins[str(source)] = contract['source_sha256']
    census = policy['census']
    require(set(census) == {'qualification', 'provider_schedule', 'policy_source'}
            and Path(census['qualification']['path']) == CENSUS_QUALIFICATION
            and census['provider_schedule']['source_manifest'] == {'path': str(CENSUS_SUPPORT/'source-manifest.json'), 'sha256': CENSUS_MANIFEST_SHA},
            'Exact independently qualified rolling census closure required')
    packet(CENSUS_SUPPORT/'source-manifest.json', CENSUS_MANIFEST_SHA)
    return policy, manifest, pins


def source_guard(pins):
    for path, sha in pins.items():
        packet(path, sha, namespace=OLD_FRAMEWORK.resolve() if Path(path).is_relative_to(OLD_FRAMEWORK.resolve()) else ROOT, limit=64_000_000)
    require(source_identity(OLD_FRAMEWORK)['sha256'] == OLD_SOURCE
            and source_identity(FRAMEWORK)['sha256'] == SOURCE and source_identity(PRIMARY)['sha256'] == PRIMARY_SHA,
            'Old execution, verifier framework or original science source changed')
    packet(OLD_FRAMEWORK.resolve()/'src/horsehair/pipeline.py', PIPELINE_SHA, namespace=OLD_FRAMEWORK.resolve())
    packet(OLD_FRAMEWORK.resolve()/'src/horsehair/budget.py', OLD_BUDGET_SHA, namespace=OLD_FRAMEWORK.resolve())


def passing_junit(path, contract):
    raw, sha = packet(path)
    require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(), 'Unsafe JUnit refused')
    tree = ET.fromstring(raw)
    cases = list(tree.iter('testcase'))
    roster = sorted(x.get('classname', '').split('.')[-1]+'::'+x.get('name', '') for x in cases)
    leaves = [x for x in tree.iter('testsuite') if not x.findall('testsuite')]
    require(len(cases) == len(set(roster)) == contract['tests'] and canonical_hash(roster) == contract['roster_sha256']
            and all(x.get('classname') and x.get('name') for x in cases)
            and not any(list(x.iter(k)) for x in cases for k in ('failure', 'error', 'skipped'))
            and leaves and sum(int(x.get('tests', '-1')) for x in leaves) == contract['tests']
            and all(int(x.get(k, '0')) == 0 for x in tree.iter() if x.tag in {'testsuite', 'testsuites'} for k in ('failures', 'errors', 'skipped')),
            'Actual complete passing native suite roster required')
    return {'path': str(path), 'sha256': sha}


def finalized_primary_qualification(policy, manifest, slurm, refresh):
    binding = policy['qualification']
    require(type(binding) is dict and set(binding) == {'path', 'sha256', 'job_id'}
            and Path(binding['path']) == SUPPORT/'qualification.json'
            and re.fullmatch('[1-9][0-9]*', str(binding['job_id'])), 'Separate positive primary qualification locator required')
    proof = slurm.completed_native_dependency(ROOT, binding['job_id'])
    if proof is None:
        refresh('primary-qualification-completion')
        proof = slurm.completed_native_dependency(ROOT, binding['job_id'])
    if proof is None:
        raise PendingExternalCensus('Primary qualification native completion is not finalized')
    value = strict(packet(binding['path'], binding['sha256'])[0])
    require(value.get('status') == 'complete_primary_external_census_qualification'
            and value.get('native_job_id') == binding['job_id'] and value.get('source_manifest') == policy['source_manifest']
            and value.get('census') == policy['census'] and value.get('old_framework_source_sha256') == OLD_SOURCE
            and value.get('verifier_framework_source_sha256') == SOURCE and value.get('primary_source_sha256') == PRIMARY_SHA
            and value.get('config_sha256') == CONFIG and all(value.get(k) is False for k in FALSE_FLAGS),
            'Exact independently source-bound primary qualification required')
    require(value.get('d89_primary_lease_retained') is True and value.get('original_primary_cpu_core_hours_cap') == 20000
            and value.get('original_primary_gpu_hours_cap') == 400 and value.get('maximum_verifier_checks_per_segment') == MAX_CHECKS,
            'Original primary budget/finite verifier policy must remain explicit')
    registered = value['native_registration']
    plan = strict(packet(registered['plan']['path'], registered['plan']['sha256'])[0])
    tasks = [t for t in plan.get('tasks', []) if t.get('name') == registered['task_name']]
    require(len(tasks) == 1, 'One exact native qualification task required')
    tokens = shlex.split(tasks[0]['command'])
    require(tokens.count(str(Path(__file__))) == 1, 'Actual native qualification helper required')
    tail = tokens[tokens.index(str(Path(__file__)))+1:]
    qargs = parser().parse_args(tail)
    require(qargs.mode == 'qualify' and qargs.segment == 0, 'Actual qualification mode required')
    qsha = digest(qargs.policy_descriptor)
    qpolicy, qmanifest, _ = policy_sources(qargs.policy_descriptor, qsha)
    require(qpolicy['qualification'] is None and qpolicy['census'] == policy['census'] and qpolicy['source_manifest'] == policy['source_manifest']
            and qmanifest == manifest, 'Qualification descriptor/closure differs')
    actual, _ = own_registration(qargs, binding['job_id'], tail, 'qualify', qsha)
    require(actual == registered, 'Actual qualification plan/script/ledger/native identity differs')
    suites = value.get('suites', [])
    require(len(suites) == 3 and {x.get('scope') for x in suites} == set(manifest['suites']), 'Complete native primary test scope required')
    for suite in suites:
        contract = manifest['suites'][suite['scope']]
        require(suite.get('source_sha256') == contract['source_sha256'] and suite.get('junit') == passing_junit(suite['junit']['path'], contract), 'Native qualified test bytes changed')
        origins = strict(packet(suite['module_origins']['path'], suite['module_origins']['sha256'])[0])
        require(origins.get('mixed_release_modules') is False and origins.get('before') and origins.get('after')
                and all(Path(path).is_relative_to(OLD_FRAMEWORK.resolve()/'src/horsehair') for key in ('before', 'after') for path in origins[key].values()),
                'Actual native baseline must retain only the original package')
    row = proof['native_records'].get(binding['job_id'], {})
    require(row.get('state') == 'COMPLETED' and row.get('exit_code') == '0:0' and row.get('cpus') == 4 and row.get('gpus') == 0
            and type(row.get('elapsed_seconds')) is int and 0 < row['elapsed_seconds'] <= 3600, 'Positive owned four-CPU primary qualification completion required')
    return binding


def provider_pair_join(outer, base, bridge):
    value, pair = outer.fresh_pair(base)
    proof = outer.provider_proof(base, bridge, pair)
    current, checked = outer.fresh_pair(base)
    if pair != checked:
        raise PendingExternalCensus('Global census pair changed during finalized provider validation')
    return current, pair, proof


def verify_census(context):
    """Only called in a fresh interpreter whose horsehair package is 4f."""
    import horsehair
    from horsehair import slurm
    require(Path(horsehair.__file__).resolve() == FRAMEWORK/'src/horsehair/__init__.py', 'Verifier must import only its qualified 4f package')
    package_origins(FRAMEWORK)
    policy, manifest, pins = policy_sources(context['args'].policy_descriptor, context['descriptor_sha256'])
    source_guard(pins)
    closure = strict(packet(CENSUS_SUPPORT/'source-manifest.json', CENSUS_MANIFEST_SHA)[0])
    outer = module(CENSUS_SUPPORT/OUTER_NAME, closure['files'][OUTER_NAME], 'primary_qualified_external_census_reader')
    installed = context.setdefault('_verifier_install', {})
    @contextmanager
    def guarded(c, role):
        if not installed:
            for path, sha in ((GUARD, GUARD_SHA), (REGISTRY, REGISTRY_SHA), (POLICY, POLICY_SHA)):
                packet(path, sha)
            area = SUPPORT/('native-'+context['job'])/('accounting-refresh-'+str(context['sequence']).zfill(6))
            require(not area.exists(), 'Native refresh evidence cannot replay')
            area.mkdir(mode=0o700)
            runpy.run_path(str(GUARD))['install'](area/'native-accounting')
            registry = runpy.run_path(str(REGISTRY))
            registry['install'](area/'registered-reads', registration_mode='controller-already-locked', allocation_deadline_unix=context['end'])
            from horsehair import campaign, repair
            require(campaign._repair_native_states is repair._repair_native_states, 'Native guard must precede campaign binding')
            from horsehair.execution_resources import load_resource_policy
            operational_policy = load_resource_policy(POLICY, POLICY_SHA, root=ROOT, config=context['config'])
            installed.update(registry=registry, campaign=campaign, area=area, policy=operational_policy)
        with installed['registry']['registration_lock'](installed['area']/('lock-'+str(context.get('refreshes', 0))), context['end'], role=role):
            yield
    def admit(c, label, **resources):
        context['refreshes'] = context.get('refreshes', 0)+1
        return installed['campaign'].combined_admission(ROOT, context['config'], read_json(PRIMARY/'release.json')['release_id'],
            installed['area'], label+'-'+str(context['refreshes']).zfill(3), resource_policy=installed['policy'], **resources)
    def refresh(label):
        with guarded({}, 'controller-start'):
            return admit({}, label, cpus=0, gpus=0, hours=1)
    base = SimpleNamespace(PendingStorage=PendingExternalCensus, registration_guard=guarded, admission=admit)
    bridge = {'policy': policy['census'], 'base_context': {'config': context['config']}}
    if context['parent_mode'] == 'qualify':
        refresh('primary-qualification-readonly-admission')
    qjob = policy['census']['qualification']['job_id']
    if slurm.completed_native_dependency(ROOT, qjob) is None:
        refresh('external-census-qualification-completion')
    if slurm.completed_native_dependency(ROOT, qjob) is None:
        raise PendingExternalCensus('Census qualification native completion is not finalized; receipt not read')
    pins.update(outer.qualification(policy['census']))
    if context['parent_mode'] == 'run':
        finalized_primary_qualification(policy, manifest, slurm, refresh)
    try:
        current, pair, proof = provider_pair_join(outer, base, bridge)
    except PendingExternalCensus as error:
        if context['parent_mode'] != 'qualify':
            raise
        source_guard(pins)
        package_origins(FRAMEWORK)
        return {'status': 'qualified_operational_prerequisites_provider_integration_pending', 'reason': str(error),
                'actual_provider_completed': False, 'job_id': context['job'], **FALSE_FLAGS}
    source_guard(pins)
    package_origins(FRAMEWORK)
    return {'status': 'verified_complete_fresh_external_census', 'job_id': context['job'],
            'sequence': context['sequence'], 'policy_descriptor': {'path': str(context['args'].policy_descriptor), 'sha256': context['descriptor_sha256']},
            'pair': pair, 'measured_unix': current['measured_unix'], 'captured_utc': current['captured_utc'],
            'provider': proof, 'actual_provider_completed': True, **FALSE_FLAGS}


def bounded_child(command, *, cwd, env, timeout, runner=subprocess.Popen):
    require(timeout > 20, 'Finite verifier termination allowance required')
    child = runner(command, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try:
        out, err = child.communicate(timeout=timeout-10)
    except subprocess.TimeoutExpired:
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(child.pid, sig)
            except ProcessLookupError:
                pass
            try:
                child.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        raise
    require(len(out) <= 2_000_000 and len(err) <= 2_000_000, 'Finite verifier output required')
    return subprocess.CompletedProcess(command, child.returncode, out, err)


def make_consumer(context, *, runner=bounded_child, wall=time.time):
    def consume(root):
        require(Path(root).resolve() == ROOT, 'Owned primary root required')
        cutoff = context['end']-(4*3600 if context['parent_mode'] == 'run' else 180)
        if wall()+30 >= cutoff:
            raise PendingExternalCensus('Original finite operational window lacks verifier termination allowance')
        if context['sequence'] >= MAX_CHECKS:
            raise PendingExternalCensus('Finite per-segment verifier allowance exhausted; new authority checks paused')
        source_guard(context['pins'])
        if wall()+30 >= cutoff:
            raise PendingExternalCensus('Original finite operational window expired during source verification')
        context['sequence'] += 1
        request = {k: context[k] for k in ('job', 'argv', 'parent_mode', 'descriptor_sha256', 'end', 'allocation', 'registration', 'sequence')}
        path = context['temporary']/('verify-'+str(context['sequence']).zfill(6)+'.json')
        exclusive(path, request)
        command = [sys.executable, '-s', '-u', str(Path(__file__)), '--mode', 'verify', '--request', str(path), '--request-sha256', digest(path)]
        try:
            result = runner(command, cwd=FRAMEWORK, env=environment(FRAMEWORK), timeout=min(1200, cutoff-wall()))
        except subprocess.TimeoutExpired as error:
            raise PendingExternalCensus('The finite isolated census verifier did not establish current storage authority') from error
        if result.returncode == 75:
            pending = strict(result.stdout)
            require(pending.get('status') == 'pending_external_storage_census' and pending.get('job_id') == context['job']
                    and all(pending.get(k) is False for k in FALSE_FLAGS), 'Exact verifier pending response required')
            raise PendingExternalCensus(pending['reason'])
        require(result.returncode == 0 and not result.stderr, 'Isolated qualified census verifier failed: '+result.stderr.decode(errors='replace')[:2000])
        value = strict(result.stdout)
        if wall() >= cutoff:
            raise PendingExternalCensus('Original finite operational window expired during isolated verification')
        require(value.get('status') == 'verified_complete_fresh_external_census' and value.get('job_id') == context['job']
                and value.get('sequence') == context['sequence']
                and value.get('policy_descriptor') == {'path': str(context['args'].policy_descriptor), 'sha256': context['descriptor_sha256']}
                and value.get('actual_provider_completed') is True
                and all(value.get(k) is False for k in FALSE_FLAGS), 'Exact isolated verifier response/source context required')
        captured = datetime.fromisoformat(value['captured_utc'].replace('Z', '+00:00'))
        require(type(value['measured_unix']) in (int, float) and math.isfinite(value['measured_unix']) and captured.tzinfo is not None,
                'Malformed original census clocks refused')
        now = wall()
        ages = (now-value['measured_unix'], now-captured.timestamp())
        require(all(age >= 0 for age in ages), 'Future original census clocks refused')
        if any(age >= 21600 for age in ages):
            raise PendingExternalCensus('Original external census clocks expired before use')
        context['last_census_proof'] = value
        return value
    return consume


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=('run', 'qualify', 'verify'), required=True)
    for name in ('policy-descriptor', 'native-plan', 'native-ledger', 'native-script', 'request'):
        p.add_argument('--'+name, type=Path)
    for name in ('policy-descriptor-sha256', 'native-task', 'request-sha256'):
        p.add_argument('--'+name)
    p.add_argument('--segment', type=int, default=0)
    return p


def request_context(args):
    temporary = Path(os.environ.get('SLURM_TMPDIR', '')).resolve()
    require(temporary.is_absolute() and temporary.is_dir() and temporary.stat().st_uid == 3164768 and not temporary.is_relative_to(ROOT), 'Owned allocation-local verifier request required')
    value = strict(packet(args.request, args.request_sha256, namespace=temporary, limit=2_000_000)[0])
    require(value.get('job') == str(native_job_id() or '') and value.get('parent_mode') in {'qualify', 'run'}
            and type(value.get('sequence')) is int and 1 <= value['sequence'] <= MAX_CHECKS
            and math.isfinite(value.get('end', float('nan')))
            and 0 <= float(os.environ.get('SLURM_JOB_END_TIME', 'nan'))-value['end'] <= 2
            and time.time()+300 < value['end'], 'Exact actual parent allocation/request/deadline required')
    parent = parser().parse_args(value['argv'])
    require(parent.mode == value['parent_mode'], 'Actual registered parent mode required')
    registration, _ = own_registration(parent, value['job'], value['argv'], parent.mode, value['descriptor_sha256'])
    require(registration == value['registration'], 'Parent registered files changed after actual allocation verification')
    value.update(args=parent, config=read_json(PRIMARY/'configs/study.json'))
    return value


def qualify(context, policy, manifest):
    context['sequence'] += 1
    prerequisites = verify_census(context)
    suites = []
    for scope, contract in manifest['suites'].items():
        source = OLD_FRAMEWORK.resolve()/contract['source'] if scope.startswith('baseline_') else SUPPORT/contract['source']
        xml = context['area']/(scope+'.xml')
        proof_path = context['area']/(scope+'-module-origins.json')
        code = """import importlib.util,json,sys
from pathlib import Path
spec=importlib.util.spec_from_file_location('qualified_primary_policy_test_bootstrap',sys.argv[1]);helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
before=helper.package_origins(helper.OLD_FRAMEWORK)
import pytest
result=pytest.main(sys.argv[3:])
after=helper.package_origins(helper.OLD_FRAMEWORK)
helper.exclusive(Path(sys.argv[2]),{'before':before,'after':after,'mixed_release_modules':False})
raise SystemExit(result)
"""
        command = [sys.executable, '-s', '-c', code, str(Path(__file__)), str(proof_path), str(source), '-q', '-p', 'no:cacheprovider', '--basetemp', str(context['temporary']/scope), '--junitxml', str(xml)]
        with (context['area']/(scope+'-stdout.txt')).open('xb') as out, (context['area']/(scope+'-stderr.txt')).open('xb') as err:
            result = subprocess.run(command, cwd=OLD_FRAMEWORK, env=environment(OLD_FRAMEWORK), stdout=out, stderr=err,
                timeout=min(1800, context['end']-time.time()-300), check=False)
        require(result.returncode == 0, 'Actual native primary contract suite failed')
        suites.append({'scope': scope, 'source_sha256': contract['source_sha256'], 'junit': passing_junit(xml, contract), 'module_origins': locator(proof_path)})
    context['sequence'] += 1
    canary = verify_census(context)
    source_guard(context['pins'])
    receipt = {'status': 'complete_primary_external_census_qualification', 'native_job_id': context['job'],
        'native_registration': context['registration'], 'allocation': context['allocation'], 'source_manifest': policy['source_manifest'],
        'census': policy['census'], 'old_framework_source_sha256': OLD_SOURCE, 'verifier_framework_source_sha256': SOURCE,
        'primary_source_sha256': PRIMARY_SHA, 'config_sha256': CONFIG, 'suites': suites, 'native_census_prerequisites': prerequisites,
        'provider_integration': canary, 'provider_integration_required_before_run': True,
        'd89_primary_lease_retained': True, 'original_primary_cpu_core_hours_cap': context['config']['resources']['cpu_core_hours_cap'],
        'original_primary_gpu_hours_cap': context['config']['resources']['gpu_hours_cap'],
        'maximum_verifier_checks_per_segment': MAX_CHECKS,
        'verifier_exhaustion_pauses_without_more_reads': True,
        'native_completion_required': True, **FALSE_FLAGS}
    exclusive(SUPPORT/'qualification.json', receipt)
    return receipt


def execute_primary(context, *, consumer=None, pipeline=None, lease_factory=None, clock=time.time, sleeper=time.sleep):
    """Retain the original D89 CLI lease around startup and the complete loop."""
    from horsehair import pipeline as original_pipeline, budget
    pipeline = original_pipeline if pipeline is None else pipeline
    lease_factory = budget.resource_lease if lease_factory is None else lease_factory
    consumer = make_consumer(context) if consumer is None else consumer
    with lease_factory(ROOT, context['config'], cpus=1, gpus=0, max_hours=HOURS, job_id=context['job']):
        while clock()+4*3600+300 < context['end']:
            try:
                consumer(ROOT)
                break
            except PendingExternalCensus as error:
                from horsehair.core import atomic_json
                atomic_json(context['area']/'startup-pending.json', {'status': 'pending_external_storage_census', 'reason': str(error),
                    'native_job_id': context['job'], 'scientific_work_started': False, **FALSE_FLAGS})
                sleeper(min(60, max(0, context['end']-clock()-4*3600-300)))
        else:
            raise PendingExternalCensus('Finite primary startup window exhausted before finalized provider authority')
        private_run, preservation = derive_run(pipeline, consumer)
        exclusive(context['area']/'source-preservation.json', {k: v for k, v in preservation.items() if k != 'original_callables'})
        result = private_run(context['config'], ROOT, segment=context['args'].segment, interval=60, primary_release=PRIMARY.resolve())
        require(all(getattr(pipeline, k) is v for k, v in preservation['original_callables'].items()), 'Original primary callable changed during orchestration')
        source_guard(context['pins'])
        package_origins(OLD_FRAMEWORK)
        return result


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    args = parser().parse_args(argv)
    os.environ['TZ'] = 'UTC'
    if hasattr(time, 'tzset'):
        time.tzset()
    require(os.geteuid() == 3164768 and os.environ.get('USER') == 'pwa209', 'Owned native primary operational context required')
    if args.mode == 'verify':
        context = request_context(args)
        try:
            result = verify_census(context)
        except PendingExternalCensus as error:
            print(json.dumps({'status': 'pending_external_storage_census', 'job_id': context['job'], 'reason': str(error), **FALSE_FLAGS}, sort_keys=True))
            return 75
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 0
    require(all(getattr(args, name) is not None for name in ('policy_descriptor', 'policy_descriptor_sha256', 'native_plan', 'native_ledger', 'native_task', 'native_script'))
            and args.segment in range(4) and (args.mode != 'qualify' or args.segment == 0), 'Complete finite own invocation required')
    import horsehair
    expected = FRAMEWORK if args.mode == 'qualify' else OLD_FRAMEWORK.resolve()
    require(Path(horsehair.__file__).resolve() == expected/'src/horsehair/__init__.py', 'Primary and verifier packages must remain in separate interpreters')
    package_origins(expected)
    policy, manifest, pins = policy_sources(args.policy_descriptor, args.policy_descriptor_sha256)
    source_guard(pins)
    job = str(native_job_id() or '')
    require(re.fullmatch('[1-9][0-9]*', job) and os.environ.get('HHW_GPU_COUNT') == '0'
            and os.environ.get('SLURM_CPUS_PER_TASK') == ('4' if args.mode == 'qualify' else '1')
            and os.environ.get('HHW_MAX_HOURS') == ('1' if args.mode == 'qualify' else str(HOURS)), 'Exact native CPU/no-GPU/runtime environment required')
    require(subprocess.check_output(['hostname', '-f'], text=True).strip().endswith('.narval.calcul.quebec'), 'Native Narval allocation required')
    registration, task = own_registration(args, job, argv, args.mode, args.policy_descriptor_sha256)
    area = SUPPORT/('native-'+job)
    require(not area.exists() and not area.is_symlink(), 'Operational allocation cannot replay existing evidence')
    area.mkdir(mode=0o700)
    packet(SCHEDULER, SCHEDULER_SHA)
    allocation, end = native_allocation(job, args, args.mode, runpy.run_path(str(SCHEDULER))['run_scontrol_job'], area)
    temp = Path(os.environ.get('SLURM_TMPDIR', ''))
    require(temp.is_absolute() and temp == temp.resolve() and temp.is_dir() and not temp.is_symlink()
            and temp.stat().st_uid == 3164768 and not temp.is_relative_to(ROOT), 'Owned allocation-local operational requests required')
    temporary = temp/('hhw-primary-census-'+job)
    temporary.mkdir(mode=0o700)
    for item in registration.values():
        if type(item) is dict:
            pins[item['path']] = item['sha256']
    config = read_json(PRIMARY/'configs/study.json')
    require(canonical_hash(config) == CONFIG and read_json(OLD_FRAMEWORK/'configs/study.json') == config, 'Full original scientific configuration required')
    context = {'args': args, 'argv': argv, 'parent_mode': args.mode, 'job': job, 'descriptor_sha256': args.policy_descriptor_sha256,
        'area': area, 'end': end, 'temporary': temporary, 'allocation': allocation, 'registration': registration, 'sequence': 0, 'pins': pins, 'config': config}
    exclusive(area/'native-bindings.json', {k: context[k] for k in ('job', 'parent_mode', 'descriptor_sha256', 'end', 'allocation', 'registration')})
    if args.mode == 'qualify':
        require(policy['qualification'] is None, 'Qualification cannot authorize itself')
        from horsehair.budget import resource_lease
        from horsehair.execution_resources import load_resource_policy
        operational = load_resource_policy(POLICY, POLICY_SHA, root=ROOT, config=config)
        with resource_lease(ROOT, config, cpus=4, gpus=0, max_hours=1, job_id=job, resource_policy=operational):
            result = qualify(context, policy, manifest)
    else:
        require(type(policy['qualification']) is dict, 'Separately finalized primary qualification is mandatory')
        result = execute_primary(context)
    exclusive(area/'completion.json', {'status': 'finite_primary_operational_allocation_finished', 'native_job_id': job,
        'mode': args.mode, 'underlying_status': result.get('status'), 'native_completion_required': True, **FALSE_FLAGS})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
