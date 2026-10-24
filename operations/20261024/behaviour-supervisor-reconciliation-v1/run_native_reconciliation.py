"""Allocated verification of preserved output; never relabel the failed job."""
from pathlib import Path
from datetime import datetime, timezone
import argparse
import hashlib
import importlib.util
import json
import os
import re
import runpy
import shlex
import subprocess
import sys
import time

ROOT = Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK = ROOT/'releases/20261021-caebc9c1dc77-8495a3d98551'
PRIMARY = Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
CAPSULE = ROOT/'maintenance/study-behaviour-supervisor-reconciliation-source-v1-20261024'
OUTPUT = ROOT/'maintenance/study-behaviour-supervisor-reconciliation-v1-20261024'
PLAN = ROOT/'plans/behaviour-reconciliation-v1-20261024/plan.json'
LEDGER = ROOT/'state/behaviour-reconciliation-v1-20261024-submission.json'
POLICY = ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v2/policy.json'
POLICY_SHA = '5dac85400e709525458cc34a2edc2bb40b029eb743078672cd721ab444c17015'


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(8*1024**2), b''):
            h.update(block)
    return h.hexdigest()


def read_json(path):
    raw = Path(path).read_bytes()
    require(len(raw) < 20_000_000, 'Bounded JSON required')
    return json.loads(raw)


def exclusive(path, value):
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False)+'\n').encode()
    with Path(path).open('xb') as f:
        f.write(raw)
        f.flush()
        os.fsync(f.fileno())


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def command(args, timeout=45):
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    require(p.returncode == 0, 'Native read failed: '+str(args[:2]))
    return p.stdout


def duration(value):
    match = re.fullmatch(r'(?:(\d+)-)?(\d+):(\d+):(\d+)', value)
    require(match is not None, 'Finite native duration required')
    days, hours, minutes, seconds = (int(v or 0) for v in match.groups())
    return days*86400 + hours*3600 + minutes*60 + seconds


def own_allocation(job):
    raw = command(['scontrol', 'show', 'job', job, '-o'])
    fields = dict(t.split('=', 1) for t in shlex.split(raw) if '=' in t)
    plan, ledger = read_json(PLAN), read_json(LEDGER)
    from horsehair.core import canonical_hash
    task = plan['tasks'][0]
    require(len(plan['tasks']) == 1 and task['name'] == 'behaviour-reconciliation-v1', 'Exact one-job plan required')
    require(ledger['plan_sha256'] == canonical_hash(plan)
            and str(ledger['tasks'][task['name']]['job_id']) == job, 'Confirmed own plan registration required')
    require(fields.get('JobId') == job and fields.get('JobState') == 'RUNNING'
            and fields.get('UserId') == 'pwa209(3164768)'
            and fields.get('Account') == 'def-ptewarie_cpu'
            and fields.get('NumCPUs') == '4' and fields.get('MinMemoryNode') in {'16G', '16384M'}
            and duration(fields.get('TimeLimit', '')) == 7200
            and fields.get('Command') == task['script']
            and canonical_hash(Path(task['script']).read_text(encoding='utf-8')) == task['script_sha256'],
            'Owned unchanged 4CPU/16G/two-hour allocation required')
    end = datetime.strptime(fields['EndTime'], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).timestamp()
    require(abs(end-float(os.environ['SLURM_JOB_END_TIME'])) <= 2
            and 600 < end-time.time() <= 7202, 'Actual finite native end and cleanup margin required')
    return {'native_stdout': raw, 'fields': fields, 'end_unix': end,
            'plan_sha256': digest(PLAN), 'ledger_sha256': digest(LEDGER)}


def original_native(job):
    raw = command(['sacct', '-j', job, '--noheader', '--parsable2',
                   '--format=JobID%64,State,ExitCode,ElapsedRaw,AllocCPUS,User,Account,Start,End'])
    rows = [line.split('|') for line in raw.splitlines() if line.split('|')[0] == job]
    require(len(rows) == 1 and len(rows[0]) >= 9, 'Positive unique final native producer record required')
    row = rows[0]
    value = dict(zip(['job_id', 'state', 'exit_code', 'elapsed_seconds', 'cpus', 'owner', 'account', 'start', 'end'], row[:9]))
    value['elapsed_seconds'], value['cpus'] = int(value['elapsed_seconds']), int(value['cpus'])
    return value, raw


def run_tests(source, stem, output, workspace, environment, expected_count):
    env = dict(environment, HHW_TEST_WORKSPACE=str(workspace), HHW_TEST_RESULT_DIR=str(output))
    output.mkdir()
    workspace.mkdir()
    with (output/'stdout.log').open('xb') as stdout, (output/'stderr.log').open('xb') as stderr:
        p = subprocess.run([sys.executable, '-B', str(source)], cwd=workspace, env=env,
                           stdout=stdout, stderr=stderr, timeout=300)
    require(p.returncode == 0, 'Native regression suite failed: '+stem)
    result = read_json(output/'test-result.json')
    require(result['tests'] == expected_count and result['failures'] == 0 and result['errors'] == 0
            and result['skipped'] == 0 and result['successful'] is True, 'Exact complete native test roster required')
    return {'suite': stem, 'tests': result['tests'], 'failures': 0, 'errors': 0, 'skipped': 0,
            'test_source_sha256': digest(source), 'result_path': str(output/'test-result.json'),
            'result_sha256': digest(output/'test-result.json')}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-manifest-sha256', required=True)
    args = parser.parse_args()
    os.environ['TZ'] = 'UTC'
    if hasattr(time, 'tzset'):
        time.tzset()
    job = os.environ.get('SLURM_JOB_ID', '')
    require(re.fullmatch('[1-9][0-9]*', job) and os.geteuid() == 3164768
            and os.environ.get('USER') == 'pwa209'
            and os.environ.get('SLURM_CPUS_PER_TASK') == '4'
            and os.environ.get('HHW_GPU_COUNT') == '0' and os.environ.get('HHW_MAX_HOURS') == '2', 'Owned allocated finite verification required')
    require(command(['hostname', '-f']).strip().endswith('.narval.calcul.quebec'), 'Narval allocation required')
    require(Path(__file__).resolve() == CAPSULE/'run_native_reconciliation.py', 'Exact immutable verifier namespace required')
    manifest_path = CAPSULE/'source-manifest.json'
    require(digest(manifest_path) == args.source_manifest_sha256, 'Pinned source bundle required')
    bundle = read_json(manifest_path)
    for rel, sha in bundle['files'].items():
        p = CAPSULE/rel
        require(not p.is_symlink() and p.resolve().is_relative_to(CAPSULE)
                and digest(p) == sha, 'Source bundle changed: '+rel)
    import horsehair
    require(Path(horsehair.__file__).resolve() == FRAMEWORK/'src/horsehair/__init__.py', 'Qualified maintenance framework required')
    from horsehair.execution_resources import load_resource_policy
    from horsehair.budget import resource_lease
    from horsehair.core import exclusive_file_lock, read_json as framework_read_json
    require(digest(ROOT/bundle['original_helper_path']) == bundle['original_helper_sha256'], 'Frozen validator bytes changed')
    original = load(ROOT/bundle['original_helper_path'], 'frozen_prefix_artifact_validator')
    require(digest(ROOT/bundle['original_helper_path']) == bundle['original_helper_sha256'], 'Frozen validator changed during import')
    config = original.source_bindings(PRIMARY, FRAMEWORK)
    policy = load_resource_policy(POLICY, POLICY_SHA, root=ROOT, config=config)
    allocation = own_allocation(job)
    output = OUTPUT/('native-'+job)
    require(not output.exists() and not output.is_symlink(), 'Fresh verification output required')
    output.mkdir(parents=True, mode=0o700)
    exclusive(output/'started.json', {'native_job_id': job, 'allocation': allocation,
                                    'source_manifest_sha256': args.source_manifest_sha256,
                                    'operational_rebinding': {'context_SUPPORT': str(output)}})
    context = original.load_exact('frozen_prefix_context', ROOT/bundle['original_context_path'], original.CONTEXT_SHA)
    context.SUPPORT = output
    require(digest(context.GUARD) == context.GUARD_SHA, 'Pinned accounting adapter required')
    runpy.run_path(str(context.GUARD))['install'](output/'accounting-reads')
    from horsehair import repair
    repair._repair_native_states = context.bounded_native_reader(repair._repair_native_states,
        output/'bounded-native-reads', allocation['end_unix'])
    from horsehair import campaign
    require(campaign._repair_native_states is repair._repair_native_states, 'Bounded accounting must precede campaign import')
    with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex', timeout=60):
        admitted = campaign.combined_admission(ROOT, config, framework_read_json(PRIMARY/'release.json')['release_id'],
                                   output, 'verification-admission', resource_policy=policy, cpus=0, gpus=0, hours=2)
        require(admitted['unique_allocation_bindings'][job]['task'] == 'behaviour-reconciliation-v1',
                'Baseline admission must already include the actual verifier allocation exactly once')
    with resource_lease(ROOT, config, cpus=4, gpus=0, max_hours=2, job_id=job, resource_policy=policy):
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE='1',
                           HHW_LIFECYCLE_ORIGINAL=str(ROOT/bundle['original_helper_path']))
        temporary = Path(os.environ['SLURM_TMPDIR'])/('hhw-prefix-reconciliation-'+job)
        temporary.mkdir(mode=0o700)
        qualification = []
        for row in bundle['test_suites']:
            qualification.append(run_tests(CAPSULE/row['path'], row['name'], output/row['name'],
                                           temporary/row['name'], environment, row['tests']))
        frozen_tests = []
        for suite, name in [('prefix', 'private-tests.xml'), ('context', 'context-private-tests.xml')]:
            frozen_tests.append(original.verify_private_junit(ROOT/bundle['original_support_path']/name, suite=suite))
        expected = read_json(CAPSULE/'expected-original-manifest.json')
        native, raw_native = original_native(expected['original_job_id'])
        exclusive(output/'producer-native-before.json', {'value': native, 'stdout': raw_native})
        verifier = load(CAPSULE/'reconcile_completed_prefix.py', 'independent_prefix_reconciliation')
        certificate = verifier.verify(expected, root=ROOT, native=native, validator=original.validate_worker_result)
        # Preserve observed source and artifact bytes after the complete check as well.
        native_after, raw_after = original_native(expected['original_job_id'])
        require(native_after == native, 'Producer native terminal proof changed during verification')
        original.source_bindings(PRIMARY, FRAMEWORK)
        for rel, sha in bundle['files'].items():
            require(digest(CAPSULE/rel) == sha, 'Verifier bundle changed during execution')
        certificate.update(native_job_id=job, qualification=qualification,
                           original_native_private_qualification=frozen_tests,
                           source_manifest_sha256=args.source_manifest_sha256,
                           verified_utc=datetime.now(timezone.utc).isoformat(),
                           consumer_requires_verifier_native_completed_zero=True,
                           canonical_state_changed=False)
        exclusive(output/'producer-native-after.json', {'value': native_after, 'stdout': raw_after})
        exclusive(output/'receipt.json', certificate)
        exclusive(output/'continuation-gates.json', {
            'status': 'technical_prefix_reconciled_primary_coverage_still_pending',
            'technical_verification_receipt': str(output/'receipt.json'),
            'receipt_sha256': digest(output/'receipt.json'),
            'original_native_outcome_preserved': True,
            'remaining': ['Complete source-bound workload measurements', 'Valid original selection criterion',
                          'Other declared models, folds, starts, rates and bin widths',
                          'Scientific diagnostics and original metadata/provenance gates'],
            'automatic_scientific_admission': False, 'canonical_state_changed': False})
        print(json.dumps({'status': certificate['status'], 'native_job_id': job,
                          'receipt': str(output/'receipt.json'), 'receipt_sha256': digest(output/'receipt.json')}, sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
