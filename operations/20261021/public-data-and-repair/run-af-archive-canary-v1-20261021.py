"""Full private archive tests in allocated scratch; never compact study data."""
from pathlib import Path
from datetime import datetime, timezone
import argparse
import json
import math
import os
import re
import runpy
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

from horsehair.core import read_json, digest, canonical_hash, source_identity, native_job_id, atomic_json
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy
from horsehair.slurm import completed_native_dependency

ROOT = Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK = ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
EXECUTION = '4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY = Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA = '0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG_SHA = '446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
POLICY = ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA = 'f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
HELPER_SHA = '045d8650db62c0fa52d7f56bb7bdb2669729509c23ecfbbc847e73cbbff8d42a'
TEST_SHA = 'ec4131d283d3f3a792a561c6ae6324de853a4301def9d275a03018a9801f5462'
GUARD = ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA = '358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER = ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-scheduler-read.py'
SCHEDULER_SHA = '100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
QUOTA = ROOT/'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'
QUOTA_SHA = '5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045'
CASE = ROOT/'maintenance/study-af-evidence-archive-canary-v1-20261021'


def require(value, message):
    if not value:
        raise ValueError(message)


def verify_junit(path, names):
    raw = Path(path).read_bytes()
    require(len(raw) < 8_000_000 and b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(), 'Bounded plain JUnit required')
    cases = list(ET.fromstring(raw).iter('testcase'))
    actual = [item.get('name') for item in cases]
    require(len(actual) == len(set(actual)) == len(names) == 39 and set(actual) == set(names)
        and all(item.get('classname', '').endswith('test_verified_evidence_archive_20261021')
            and not any(item.find(key) is not None for key in ['failure', 'error', 'skipped']) for item in cases),
        'Every exact archive testcase must pass natively without skips')
    from horsehair.cli import _qualification_junit
    require(_qualification_junit(path) == {'tests':39,'failures':0,'errors':0,'skipped':0}, 'JUnit counter/actual-case mismatch')
    return canonical_hash(sorted(actual))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--helper', type=Path, required=True)
    parser.add_argument('--tests', type=Path, required=True)
    args = parser.parse_args()
    config = read_json(PRIMARY/'configs/study.json')
    job = str(native_job_id() or '')
    require(job.isdigit() and os.geteuid() == 3164768 and os.environ.get('SLURM_CPUS_PER_TASK') == '4', 'Owned four-CPU native canary required')
    import horsehair
    require(Path(horsehair.__file__).resolve() == FRAMEWORK/'src/horsehair/__init__.py'
        and source_identity(FRAMEWORK)['sha256'] == EXECUTION and source_identity(PRIMARY)['sha256'] == PRIMARY_SHA
        and canonical_hash(config) == CONFIG_SHA, 'Exact qualified framework/frozen science required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'), 'Narval allocation required')
    for path, expected in [(args.helper,HELPER_SHA),(args.tests,TEST_SHA),(GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(QUOTA,QUOTA_SHA)]:
        require(path.resolve().is_relative_to(ROOT/'maintenance') and not path.is_symlink() and digest(path) == expected, 'Pinned private source/reader changed')
    require(args.helper.name == 'verified-evidence-archive-20261021.py' and args.tests.name == 'test_verified_evidence_archive_20261021.py'
        and args.helper.parent == args.tests.parent and not CASE.exists(), 'Fresh immutable canary/support pair required')
    CASE.mkdir(mode=0o700)
    response = runpy.run_path(str(SCHEDULER))['run_scontrol_job'](job,CASE/'scheduler-reads');response.check_returncode()
    lines = [line for line in response.stdout.splitlines() if line.strip()]
    require(len(lines)==1, 'One native scalar owner required')
    pairs = re.findall(r'(?<!\S)(\w+)=([^\s]*)',lines[0]);fields=dict(pairs)
    require(len(pairs)==len(fields) and fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
        and fields.get('JobState')=='RUNNING' and fields.get('Account')=='def-ptewarie_cpu'
        and fields.get('NumCPUs')=='4' and fields.get('NumNodes') in {'1','1-1'}, 'Native CPU owner/resource bound differs')
    match=re.fullmatch(r'(?:(\d+)-)?(\d+):(\d{2}):(\d{2})',fields.get('TimeLimit',''))
    require(match is not None, 'Finite native time limit required')
    day,hour,minute,second=match.groups()
    require(int(day or 0)*86400+int(hour)*3600+int(minute)*60+int(second)==3600, 'Exact one-hour canary required')
    require(all('gpu' not in fields.get(key,'').lower() for key in ['TresPerNode','TresPerJob','TresPerTask','AllocTRES','ReqTRES','Gres']), 'CPU-only canary required')
    end = datetime.fromisoformat(fields['EndTime']).replace(tzinfo=timezone.utc).timestamp()
    require(math.isfinite(float(os.environ.get('SLURM_JOB_END_TIME','nan')))
        and abs(float(os.environ['SLURM_JOB_END_TIME'])-end)<=2 and end>time.time()+900, 'Verified finite native end/margin required')
    policy = load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    runpy.run_path(str(GUARD))['install'](CASE/'accounting-reads')
    from horsehair import campaign, repair
    require(campaign._repair_native_states is repair._repair_native_states, 'Guard must precede campaign binding')
    campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],CASE,
        'native-start-admission',resource_policy=policy,cpus=0,gpus=0,hours=1)
    qpath=ROOT/'maintenance/study-af-execution-support-v1-20261021/qualification.json'
    require(digest(qpath)=='29ed530f3783f3c35c3cf20343bdb663732ec2bf4bf5c3218f8b029510becc2f', 'Native complete framework qualification changed')
    q=read_json(qpath)
    require(q['status']=='complete' and q['source_sha256']==EXECUTION
        and q['test_counts']=={'tests':1694,'failures':0,'errors':0,'skipped':0}
        and completed_native_dependency(ROOT,str(q['slurm_job_id'])) is not None, 'Positive framework native completion required')
    census=read_json(ROOT/'state/storage-budget.json');now=time.time()
    captured=datetime.fromisoformat(census['captured_utc'].replace('Z','+00:00')).timestamp()
    require(0<=now-captured<21600 and 0<=now-census['measured_unix']<21600
        and census['study_files']+2000<=250000 and census['study_bytes']+10_000_000_000<=6_000_000_000_000,
        'Fresh bounded study storage headroom required')
    temporary=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temporary.is_absolute() and temporary.is_dir() and not temporary.is_symlink()
        and temporary.stat().st_uid==3164768 and shutil.disk_usage(temporary).free>=2_000_000_000
        and os.statvfs(temporary).f_favail>=30000, 'Verified allocated temporary fixture capacity required')
    workspace=temporary/('hhw-archive-canary-'+job);workspace.mkdir(mode=0o700,exist_ok=False)
    lib=runpy.run_path(str(args.helper),run_name='native_archive_canary_library_only')
    require(lib['EXPECTED_CANARY_TEST_SHA256']==TEST_SHA and len(lib['EXPECTED_CANARY_TEST_NAMES'])==39, 'Exact complete private suite required')
    command=[sys.executable,'-s','-m','pytest',str(args.tests),'-q','-p','no:cacheprovider','--basetemp',str(workspace/'fixtures'),'--junitxml',str(CASE/'junit.xml')]
    environment={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1'}
    for key in ['PYTEST_ADDOPTS','PYTEST_PLUGINS']:
        environment.pop(key,None)
    atomic_json(CASE/'intent.json',{'command':command,'helper_sha256':HELPER_SHA,'test_sha256':TEST_SHA,
        'native_job_id':job,'allocation':fields,'fixture_workspace':str(workspace),'source_sha256':EXECUTION,
        'resource_policy':policy.receipt(),'production_compaction_permitted':False,'scientific_result':False})
    with resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        quota=runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,required_bytes=10_000_000_000,
            reserve_files=248000,reserve_bytes=2_000_000_000_000,evidence_dir=CASE/'quota-reads',allocation_deadline_unix=end)
        from horsehair.admission import _storage
        census_path=ROOT/'state/storage-budget.json';census_sha=digest(census_path)
        future_bytes,future_files,source_paths=_storage(ROOT,config,10_000_000_000,2000,personal_headroom=quota)
        require(digest(census_path)==census_sha, 'Study census changed during storage validation')
        atomic_json(CASE/'storage-proof.json',{'census_path':str(census_path),'census_sha256':census_sha,
            'future_study_bytes':future_bytes,'future_study_files':future_files,'source_paths':list(map(str,source_paths)),
            'fixture_files_are_allocation_local':True,'fixture_file_allowance':30000,'fixture_bytes_allowance':2_000_000_000})
        with (CASE/'pytest-stdout.txt').open('xb') as stdout,(CASE/'pytest-stderr.txt').open('xb') as stderr:
            result=subprocess.run(command,cwd=FRAMEWORK,stdout=stdout,stderr=stderr,timeout=min(3000,end-time.time()-180),check=False,
                env=environment)
        require(result.returncode==0, 'Complete native archive suite failed; originals stay unchanged')
        roster_sha=verify_junit(CASE/'junit.xml',lib['EXPECTED_CANARY_TEST_NAMES'])
        require(digest(args.helper)==HELPER_SHA and digest(args.tests)==TEST_SHA
            and source_identity(FRAMEWORK)['sha256']==EXECUTION and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,
            'Source changed during canary')
        receipt={'status':'native_archive_tests_passed_pending_final_native_completion','native_job_id':job,
            'helper_sha256':HELPER_SHA,'test_source':{'path':str(args.tests),'sha256':TEST_SHA},
            'junit':{'path':str(CASE/'junit.xml'),'sha256':digest(CASE/'junit.xml')},'test_roster_sha256':roster_sha,
            'test_counts':{'tests':39,'failures':0,'errors':0,'skipped':0},'roundtrip_verified':True,
            'unchanged_source_removal_guards_verified':True,'source_sha256':EXECUTION,'allocation':fields,
            'storage_headroom':quota,'resource_policy':policy.receipt(),'production_compaction_permitted':False,
            'original_study_files_removed':0,'scientific_result':False,'admission_authority':False,
            'next_requirement':'Separate final native producer proof and separately qualified archive-aware consumers before any compaction'}
        atomic_json(CASE/'receipt.json',receipt)
        print('AF_ARCHIVE_CANARY '+json.dumps(receipt,sort_keys=True),flush=True)


if __name__=='__main__':
    main()
