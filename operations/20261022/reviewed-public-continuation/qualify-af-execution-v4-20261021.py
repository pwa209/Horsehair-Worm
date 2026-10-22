"""Fresh native software qualification; no scientific admission or workload retry."""
from datetime import datetime, timezone
from pathlib import Path
import argparse
import json
import math
import os
import re
import runpy
import subprocess
import time

from horsehair.core import read_json, source_identity, canonical_hash, digest, atomic_json, native_job_id, exclusive_file_lock
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy

ROOT = Path('/scratch/pwa209/horsehair-worm')
PRIMARY = Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf').resolve()
PRIMARY_SHA = '0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
RELEASE_FROZEN = True  # Actual immutable release and complete root-collected test count bound below.
FRAMEWORK_SHA = 'f047ce12b0a4a31312f2b4f5b6cff144342a12950f2e84fe1d444c8ea3a9b68c'
FRAMEWORK = ROOT / 'releases/20261021-e104ce63fa53-f047ce12b0a4'
POLICY = ROOT / 'maintenance/execution-resource-policies/af-expanded-20261021-v4/policy.json'
EXPECTED_TESTS = 1889  # Actual complete root collect; public105 plus metadata28 additions.
CONFIG_SHA = '446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
SUPPORT = ROOT / 'maintenance/study-af-execution-support-v4-20261021'
GUARD = ROOT / 'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA = '358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER = ROOT / 'maintenance/benchmark-preflight-stage-v4-20261021/native-scheduler-read.py'
SCHEDULER_SHA = '100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
QUOTA = ROOT / 'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'
QUOTA_SHA = '5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045'
DRIVER_SHA = '49815eeb509909d5eac2ebde2e4f4a3918cf40cefdc5f643f2bcaffead629eb1'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def approved_policy(path, expected_sha256, config):
    path = Path(path)
    require(path == POLICY and path.is_absolute() and not path.is_symlink()
            and re.fullmatch('[0-9a-f]{64}', expected_sha256) is not None,
            'Exact explicit new-source v4 resource policy path/SHA required')
    policy = load_resource_policy(path, expected_sha256, root=ROOT, config=config)
    require(policy.execution_source_sha256 == FRAMEWORK_SHA
            and policy.base_config_sha256 == CONFIG_SHA
            and policy.primary_scientific_source_sha256 == PRIMARY_SHA,
            'Resource policy must bind this new execution source and frozen science')
    return policy


FALSE_FLAGS={'scientific_result':False,'admission_authority':False}


class RegistrationWaitBudgetExceeded(TimeoutError):pass


def exclusive(path,value):
    raw=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    require(len(raw)<=2_000_000,'Finite private registration evidence required')
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def fresh_registration_evidence(path):
    path=Path(path)
    require(SUPPORT.is_dir() and not SUPPORT.is_symlink() and path.parent.resolve()==SUPPORT.resolve()
        and not path.exists() and not path.is_symlink(),'Fresh exact qualification registration evidence required')
    return path


def registration_operation(callback,allocation_deadline,*,evidence_dir,operation_kind='admission',lock_factory=None,wall=time.time,clock=time.monotonic):
    """Wait for the same kernel inode, then call one unchanged operation once.

    Only pre-acquisition exact ReceiptLockTimeout on this resolved mutex may
    retry. No callback, scientific operator, inode removal or owner substitution
    is retried. Diagnostics are durable and occur outside the acquired lock.
    """
    from horsehair.core import ReceiptLockTimeout
    require(operation_kind in {'admission','primary-plan-publication'},'Exact qualification registration operation required')
    if lock_factory is None:
        from horsehair.core import exclusive_file_lock
        lock_factory=exclusive_file_lock
    require(not isinstance(allocation_deadline,bool) and isinstance(allocation_deadline,(int,float))
        and math.isfinite(allocation_deadline),'Finite native admission deadline required')
    path=(ROOT/'state/af-campaign-registration.mutex').resolve()
    evidence=fresh_registration_evidence(evidence_dir);evidence.mkdir(mode=0o700,exist_ok=False)
    started=clock();wait_limit=min(900.,allocation_deadline-wall()-300.)
    require(math.isfinite(started),'Finite monotonic admission clock required')
    end=started+max(0.,wait_limit);attempts=[]
    def finish(status,**extra):
        value={'status':status,'operation_kind':operation_kind,'path':str(path),'maximum_acquisition_seconds':max(0.,wait_limit),
            'maximum_per_attempt_seconds':30.,'maximum_attempts':31,'elapsed_seconds':clock()-started,
            'allocation_deadline_unix':allocation_deadline,'native_acquisition_margin_seconds':300,
            'attempts':attempts,'callback_retried':False,'guard_removed_or_replaced':False,
            'scope':'One unchanged qualification admission or primary planning publication callback','native_query_timezone':'UTC',
            'released_before_numerical_execution':True,
            'all_publishers_must_use_this_same_permanent_kernel_mutex':True,**extra,**FALSE_FLAGS}
        exclusive(evidence/'result.json',value);return value
    for number in range(1,32):
        remaining=min(end-clock(),allocation_deadline-wall()-300.)
        if remaining<.01:
            summary=finish('pending_registration_no_qualification_operation_started',reason='Finite registration acquisition envelope exhausted')
            raise RegistrationWaitBudgetExceeded('Registration acquisition exhausted; durable diagnostics: '+str(evidence/'result.json'))
        timeout=min(30.,remaining)
        attempt={'attempt':number,'timeout_seconds':timeout,'status':'acquisition_intended'}
        exclusive(evidence/(f'attempt-{number:02d}-intent.json'),dict(attempt,guard_path=str(path),**FALSE_FLAGS))
        # A slow durable intent write must not consume the reserved native margin.
        timeout=min(timeout,end-clock(),allocation_deadline-wall()-300.)
        if timeout<.01:
            attempts.append(dict(attempt,status='not_attempted_margin_exhausted'))
            finish('pending_registration_no_qualification_operation_started',reason='Margin consumed before kernel acquisition')
            raise RegistrationWaitBudgetExceeded('Registration margin exhausted after diagnostic persistence')
        attempt['timeout_seconds']=timeout;acquired=False;callback_started=False
        try:
            with lock_factory(path,timeout=timeout):
                acquired=True;acquired_after=clock()-started
                if clock()>=end or wall()+300>=allocation_deadline:
                    raise RegistrationWaitBudgetExceeded('Registration margin exhausted at kernel acquisition; callback not started')
                callback_started=True;value=callback()
                if wall()+120>=allocation_deadline:
                    raise RegistrationWaitBudgetExceeded('Native finalization margin exhausted during unchanged admission')
            attempts.append(dict(attempt,status='admission_complete',acquired_after_seconds=acquired_after))
            exclusive(evidence/(f'attempt-{number:02d}-result.json'),attempts[-1])
            return value,finish('complete_admission',acquired_after_seconds=acquired_after)
        except BaseException as error:
            retry=(not acquired and type(error) is ReceiptLockTimeout and Path(error.path)==path)
            attempts.append(dict(attempt,status='kernel_contention' if retry else 'strict_failure',
                error_type=type(error).__name__,reason=str(error),callback_started=callback_started,
                retry_exact_preacquisition_contention_only=retry))
            exclusive(evidence/(f'attempt-{number:02d}-result.json'),attempts[-1])
            if not retry:
                finish('strict_admission_failure',error_type=type(error).__name__,reason=str(error))
                raise
    finish('pending_registration_no_qualification_operation_started',reason='Finite acquisition attempt ceiling reached')
    raise RegistrationWaitBudgetExceeded('Finite registration attempt ceiling reached; callback never started')


def expanded_baseline(config, science, policy, allocation_end, *, lock_factory=None, clock=time.time, monotonic=time.monotonic):
    runpy.run_path(str(GUARD))['install'](SUPPORT / 'native-accounting-reads')
    from horsehair import campaign, repair
    require(campaign._repair_native_states is repair._repair_native_states,
            'Pinned native guard must precede campaign reader binding')
    result,proof=registration_operation(lambda:campaign.combined_admission(ROOT,config,science['release_id'],SUPPORT,
        'native-qualification-baseline',resource_policy=policy,cpus=0,gpus=0,hours=1),allocation_end,
        evidence_dir=SUPPORT/'registration-admission-wait',lock_factory=lock_factory,wall=clock,clock=monotonic)
    atomic_json(SUPPORT/'registration-protection.json',dict(proof,
        released_before_tests_and_scientific_preprocessing=True))
    return result


def fresh_storage(config, quota, *, now=None):
    """Use the measured unchanged storage contract, with explicit six-hour age."""
    from horsehair.admission import _storage
    path = ROOT / 'state/storage-budget.json'; before = digest(path); value = read_json(path)
    measured = value.get('measured_unix'); now = time.time() if now is None else now
    require(type(measured) in (int, float) and math.isfinite(measured),
            'Finite actual storage census time required')
    captured = datetime.fromisoformat(value.get('captured_utc', '').replace('Z', '+00:00'))
    require(captured.tzinfo is not None and 0 <= now-measured < 21600
            and 0 <= now-captured.timestamp() < 21600,
            'Unchanged storage census must remain fresh within six hours')
    future_bytes, future_files, sources = _storage(ROOT, config, 5_000_000_000, 15000, personal_headroom=quota)
    require(digest(path) == before, 'Storage census changed during native qualification admission')
    result = {'census': {'path': str(path), 'sha256': before}, 'future_study_bytes': future_bytes,
        'future_study_files': future_files, 'source_paths': list(map(str, sources)),
        'maximum_census_age_seconds': 21600, 'scientific_result': False, 'admission_authority': False}
    atomic_json(SUPPORT / 'storage-proof.json', result)
    return result


def primary_smoke(config, allocation_end, *, clock=time.time, monotonic=time.monotonic, lock_factory=None):
    """Publish the unchanged fresh primary smoke once under the shared lock."""
    from horsehair.pipeline import _primary_execution
    path=ROOT/'plans/native-primary-context-study-af-execution-v4-20261021'
    def publish():
        require(not path.exists(),'Primary smoke plan must be fresh')
        return _primary_execution(config,ROOT,PRIMARY,'plan','behaviour-benchmark',path)
    result,proof=registration_operation(publish,allocation_end,evidence_dir=SUPPORT/'registration-primary-plan-wait',
        operation_kind='primary-plan-publication',lock_factory=lock_factory,wall=clock,clock=monotonic)
    atomic_json(SUPPORT/'primary-plan-registration-protection.json',proof)
    return result,path


def verify_suite(result, job):
    expected = {'tests': EXPECTED_TESTS, 'failures': 0, 'errors': 0, 'skipped': 0}
    require(result.get('status') == 'complete' and result.get('returncode') == 0
            and result.get('test_counts') == expected and result.get('slurm_job_id') == job
            and result.get('source_sha256') == FRAMEWORK_SHA and result.get('config_sha256') == CONFIG_SHA
            and result.get('source_verified_unchanged') is True,
            'Complete native 1889-test software suite with unchanged source/config did not pass')
    artifact = result.get('artifacts', {}).get('junit', {})
    path = Path(artifact.get('path', ''))
    require(path.is_absolute() and path.resolve().is_relative_to(ROOT/'state/qualification-evidence')
            and not path.is_symlink() and path.is_file()
            and type(artifact.get('bytes')) is int and path.stat().st_size == artifact['bytes']
            and re.fullmatch('[0-9a-f]{64}', artifact.get('sha256', '')) is not None
            and digest(path) == artifact['sha256'], 'Bound actual native JUnit bytes are required')
    from horsehair.cli import _qualification_junit
    require(_qualification_junit(path) == expected, 'Actual native JUnit full1889 roster differs')


def native_allocation(job):
    require(digest(SCHEDULER) == SCHEDULER_SHA, 'Pinned scheduler reader changed')
    reader = runpy.run_path(str(SCHEDULER))['run_scontrol_job']
    response = reader(job, SUPPORT / 'native-scheduler-reads')
    response.check_returncode()
    lines = [line for line in response.stdout.splitlines() if line.strip()]
    require(len(lines) == 1, 'Exactly one native allocation must own qualification')
    pairs = re.findall(r'(?<!\S)(\w+)=([^\s]*)', lines[0])
    fields = dict(pairs)
    require(len(fields) == len(pairs), 'Native allocation has duplicate fields')
    match = re.fullmatch(r'(?:(\d+)-)?(\d+):(\d{2}):(\d{2})', fields.get('TimeLimit', ''))
    require(match is not None, 'Unrecognized native allocation time limit')
    days, hours, minutes, seconds = match.groups()
    duration = int(days or 0) * 86400 + int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    require(fields.get('JobId') == job and fields.get('UserId') == 'pwa209(3164768)'
            and fields.get('Account') == 'def-ptewarie_cpu' and fields.get('JobState') == 'RUNNING'
            and fields.get('NumCPUs') == '4' and 0 < duration <= 3600,
            'Owned four-CPU bounded native allocation required')
    require(all('gpu' not in fields.get(key, '').lower() for key in
                ('TresPerNode', 'TresPerJob', 'TresPerTask', 'AllocTRES', 'ReqTRES', 'Gres')),
            'Qualification must use a CPU-only allocation')
    native_end = datetime.fromisoformat(fields['EndTime']).replace(tzinfo=timezone.utc).timestamp()
    environment_end = float(os.environ.get('SLURM_JOB_END_TIME', 'nan'))
    require(math.isfinite(environment_end) and abs(environment_end - native_end) <= 2
            and time.time() + 120 < native_end, 'Verified native allocation finalization margin required')
    require(digest(SCHEDULER) == SCHEDULER_SHA, 'Scheduler reader changed during use')
    return {'fields': fields, 'native_stdout': response.stdout, 'allocation_end_unix': native_end}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--framework', required=True, type=Path)
    parser.add_argument('--driver', required=True, type=Path)
    parser.add_argument('--expected-tests', required=True, type=int)
    parser.add_argument('--resource-policy', required=True, type=Path)
    parser.add_argument('--resource-policy-sha256', required=True)
    args = parser.parse_args()
    require(RELEASE_FROZEN is True and re.fullmatch('[0-9a-f]{64}',FRAMEWORK_SHA)
            and 'PENDING' not in str(FRAMEWORK) and type(EXPECTED_TESTS) is int and EXPECTED_TESTS>0,
            'Prospective qualification template has no frozen release/source/count')
    os.environ['TZ'] = 'UTC'
    if hasattr(time, 'tzset'): time.tzset()
    framework = args.framework.resolve()
    require(framework == FRAMEWORK, 'Exact new qualified-source candidate release required')
    identity, science = read_json(framework / 'release.json'), read_json(PRIMARY / 'release.json')
    config = read_json(PRIMARY / 'configs/study.json')
    job = str(native_job_id() or '')
    require(job.isdigit() and os.geteuid() == 3164768 and os.environ.get('SLURM_CPUS_PER_TASK') == '4',
            'Owned native four-CPU qualification required')
    require(subprocess.check_output(['id', '-un'], text=True).strip() == 'pwa209'
            and subprocess.check_output(['hostname', '-f'], text=True).strip().endswith('.narval.calcul.quebec'),
            'Personal Narval context required')
    import horsehair
    require(Path(horsehair.__file__).resolve() == framework / 'src/horsehair/__init__.py',
            'Qualification must execute the new framework package')
    require(source_identity(framework) == identity['source_identity']
            and identity['source_identity']['sha256'] == FRAMEWORK_SHA
            and source_identity(PRIMARY) == science['source_identity']
            and science['source_identity']['sha256'] == PRIMARY_SHA
            and canonical_hash(config) == CONFIG_SHA == identity['config_sha256'] == science['config_sha256']
            and read_json(framework / 'configs/study.json') == config,
            'Exact new framework, frozen primary and full configuration required')
    require(type(args.expected_tests) is int and args.expected_tests == EXPECTED_TESTS and not SUPPORT.exists(),
            'Exact fresh 1889-test qualification required')
    policy = approved_policy(args.resource_policy, args.resource_policy_sha256, config)
    require(digest(args.driver) == DRIVER_SHA and digest(GUARD) == GUARD_SHA and digest(QUOTA) == QUOTA_SHA,
            'Pinned private operational helper changed')
    SUPPORT.mkdir()
    allocation = native_allocation(job)
    expanded_baseline(config, science, policy, allocation['allocation_end_unix'])
    quota_read = runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait']
    with resource_lease(ROOT, config, cpus=4, gpus=0, max_hours=1, job_id=job, resource_policy=policy):
        quota = quota_read(ROOT, required_files=15000, required_bytes=5_000_000_000,
            reserve_files=config['resources']['scratch_budget_files'] - 15000,
            reserve_bytes=config['resources']['scratch_reserve_bytes'],
            evidence_dir=SUPPORT / 'qualification-quota-reads',
            allocation_deadline_unix=allocation['allocation_end_unix'])
        storage = fresh_storage(config, quota)
        from horsehair.cli import qualification
        result = qualification(config, ROOT)
        verify_suite(result, job)
        policy = approved_policy(args.resource_policy, args.resource_policy_sha256, config)
        smoke, smoke_path = primary_smoke(config, allocation['allocation_end_unix'])
        require(smoke['release_identity'] == science['source_identity']
                and smoke['config_sha256'] == CONFIG_SHA, 'Frozen primary smoke context differs')
        from horsehair.agent_cost_envelope import maximum_work, FULL_CONFIG_SHA256
        from horsehair.behaviour_feasibility import structural_cells
        require(canonical_hash(config) == FULL_CONFIG_SHA256 and len(structural_cells(config)) == 39
                and maximum_work(config)['grid_points'] == 300, 'Frozen complete workload structure changed')
        require(source_identity(framework) == identity['source_identity']
                and source_identity(PRIMARY) == science['source_identity']
                and digest(args.driver) == DRIVER_SHA and digest(GUARD) == GUARD_SHA
                and digest(QUOTA) == QUOTA_SHA, 'Source or operational helper changed during qualification')
        result.update(source_identity=identity['source_identity'], primary_source_sha256=PRIMARY_SHA,
            framework_release=str(framework), primary_release=str(PRIMARY), source_verified_unchanged=True,
            primary_context_plan_sha256=digest(smoke_path / 'plan.json'),
            operational_helper_sha256=digest(Path(__file__)), storage_headroom=quota,
            storage_proof=storage, registration_protection_sha256=digest(SUPPORT/'registration-protection.json'),
            resource_policy=policy.receipt(), expanded_baseline_sha256=digest(SUPPORT/'native-qualification-baseline.json'),
            scientific_result=False, admission_authority=False,
            native_allocation=allocation, private_read_helpers={'accounting': GUARD_SHA,
                'scheduler': SCHEDULER_SHA, 'quota': QUOTA_SHA},
            claim_scope='Complete software suite only; no scientific gate clearance')
        atomic_json(SUPPORT / 'qualification.json', result)
        print('AF_EXECUTION_QUALIFIED ' + json.dumps({'job_id': job, 'test_counts': result['test_counts'],
            'source_sha256': FRAMEWORK_SHA, 'qualification_sha256': digest(SUPPORT / 'qualification.json')},
            sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
