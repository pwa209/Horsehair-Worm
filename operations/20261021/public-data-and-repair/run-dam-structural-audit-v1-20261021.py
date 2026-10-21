"""One qualified source-bound structural DAM audit; no inference or retries."""
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
import sys
import shutil
import xml.etree.ElementTree as ET

from horsehair.core import read_json, source_identity, canonical_hash, digest, native_job_id
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy
from horsehair.slurm import completed_native_dependency

ROOT=Path('/scratch/pwa209/horsehair-worm')
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
CASE=ROOT/'maintenance/study-af-dam-structural-audit-v1-20261021'
SUPPORT=ROOT/'maintenance/study-af-dam-structural-support-v1-20261021'
QUALIFICATION=ROOT/'maintenance/study-af-execution-support-v1-20261021/qualification.json'
POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=GUARD.with_name('native-scheduler-read.py')
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
QUOTA=ROOT/'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'
QUOTA_SHA='5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045'
FALSE_FLAGS=('scientific_result','effects_calculated','effects_inspected','provider_code_executed','scientific_gates_cleared','admission_authority')


AUDITOR_SHA='76c6b4579ba8dbb5aff586cf2d68b290ea67cee6f93b7e0d6a98edbccfe4ea20'
TEST_SHA='36a25ef7c42773871b4b472112e18f6d773c7017a388408bf2a64ae78267b770'
MANIFEST=ROOT/'maintenance/study-af-dam-secondary-source-v1-20261021/source-manifest.json'
MANIFEST_SHA='c632eafc53b777dac5743730f98f66618334fbeec110b132e76b3e7835903410'
QUALIFICATION_SHA='29ed530f3783f3c35c3cf20343bdb663732ec2bf4bf5c3218f8b029510becc2f'
TEST_NAMES=('test_literal_mapping_is_source_bound_and_keeps_empty_unmapped_unknowns', 'test_code_metadata_drift_is_refused[SyntheticA=c(1,2)-SyntheticA=system("bad")-Executable]', 'test_code_metadata_drift_is_refused[SyntheticA=c(1,2)-SyntheticA=c(1,1)-unique channel]', 'test_code_metadata_drift_is_refused[SyntheticA=c(1,2)-SyntheticA=c(0,33)-unique channel]', 'test_code_metadata_drift_is_refused[monitor1=list(-monitor1=list(SyntheticA=c(5),-Duplicate]', 'test_code_metadata_drift_is_refused[exp1_channels <- list(-exp1_channels <- list()\\nexp1_channels <- list(-Repeated]', 'test_code_metadata_drift_is_refused["MT","CT"-"MT","Rt"-MT/CT]', 'test_code_metadata_drift_is_refused["Monitor_status"-"Changed_status"-column labels]', 'test_code_metadata_drift_is_refused["2025-01-01 00:02:00"-"2024-12-31 23:59:00"-window]', 'test_ambiguous_channel_mapping_is_reported_not_first_match_chosen', 'test_source_clock_parser_reports_assigned_utc[11 Apr 25-18:50:00-2025-04-11T18:50:00+00:00]', 'test_source_clock_parser_reports_assigned_utc[11Apr25-18:50:00-2025-04-11T18:50:00+00:00]', 'test_source_clock_parser_reports_assigned_utc[1 Jan 1969-00:00:00-1969-01-01T00:00:00+00:00]', 'test_invalid_source_clocks_are_not_coerced[31 Feb25-00:00:00]', 'test_invalid_source_clocks_are_not_coerced[1 Xxx25-00:00:00]', 'test_invalid_source_clocks_are_not_coerced[1 Jan25-25:00:00]', 'test_duplicate_identity_includes_monitor_tube_and_type', 'test_duplicate_conflict_out_of_order_and_offgrid_are_distinct', 'test_bad_status_zeros_missing_finite_and_shape_are_not_clean_activity', 'test_invalid_metadata_date_and_foreign_monitor_are_preserved', 'test_complete_synthetic_audit_independently_rehashes_all_inputs_and_has_no_inference', 'test_manifest_provider_or_transfer_tampering_never_yields_complete_audit[file_bytes]', 'test_manifest_provider_or_transfer_tampering_never_yields_complete_audit[manifest_hash]', 'test_manifest_provider_or_transfer_tampering_never_yields_complete_audit[record_hash]', 'test_manifest_provider_or_transfer_tampering_never_yields_complete_audit[provider_md5]', 'test_manifest_provider_or_transfer_tampering_never_yields_complete_audit[receipt_status]', 'test_manifest_provider_or_transfer_tampering_never_yields_complete_audit[receipt_source_url]', 'test_manifest_provider_or_transfer_tampering_never_yields_complete_audit[receipt_flag]', 'test_manifest_provider_or_transfer_tampering_never_yields_complete_audit[receipt_record]', 'test_manifest_provider_or_transfer_tampering_never_yields_complete_audit[receipt_path]', 'test_output_outside_personal_maintenance_is_refused', 'test_finite_deadline_is_required[0]', 'test_finite_deadline_is_required[59]', 'test_finite_deadline_is_required[5401]', 'test_finite_deadline_is_required[nan]', 'test_finite_deadline_is_required[True]', 'test_mid_read_failure_preserves_partial_evidence_and_raises')

def require(value,message):
    if not value:raise ValueError(message)


def exclusive(path,value):
    raw=(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def memory(value):
    match=re.fullmatch(r'([1-9][0-9]*)([MG])',value)
    require(match is not None,'Exact positive native memory units required')
    return int(match[1])*1024**(2 if match[2]=='M' else 3)


def native_allocation(job,script,reader):
    response=reader(job,SUPPORT/'own-scheduler-reads');response.check_returncode()
    lines=[line for line in response.stdout.splitlines() if line.strip()]
    require(len(lines)==1,'One actual native allocation required')
    pairs=re.findall(r'(?<!\S)(\w+)=([^\s]*)',lines[0]);fields=dict(pairs)
    require(len(fields)==len(pairs),'Duplicate native allocation fields refused')
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
        and fields.get('Account')=='def-ptewarie_cpu' and fields.get('JobState')=='RUNNING'
        and fields.get('NumCPUs')=='1' and fields.get('NumNodes') in {'1','1-1'}
        and fields.get('TimeLimit') in {'01:00:00','1:00:00','0-01:00:00'}
        and memory(fields.get('MinMemoryNode',''))==16*1024**3
        and fields.get('Command')==str(script),'Exact owned one-CPU16GB/one-hour audit allocation required')
    require(all('gpu' not in fields.get(key,'').lower() for key in
        ('TresPerNode','TresPerJob','TresPerTask','AllocTRES','ReqTRES','Gres')),'CPU-only audit allocation required')
    end=datetime.fromisoformat(fields.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp()
    environment=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(environment) and abs(environment-end)<=2 and time.time()+900<end,'Verified native end/margin required')
    script=Path(script)
    require(script.is_absolute() and script==script.resolve() and not script.is_symlink() and script.resolve().is_relative_to(ROOT/'plans'),
        'Exact immutable registered native script required')
    directory=script.parent;plan=read_json(directory/'plan.json')
    ledger_path=ROOT/'state'/(directory.name+'-submission.json');ledger=read_json(ledger_path)
    tasks=[task for task in plan['tasks'] if task['script']==str(script)]
    require(len(tasks)==1 and plan['config_sha256']==CONFIG and plan['release_identity']['sha256']==SOURCE
        and ledger['source_sha256']==SOURCE and ledger['plan_sha256']==canonical_hash(plan),
        'Source/config-bound registered audit plan required')
    task=tasks[0];registered=ledger['tasks'][task['name']];resources=task['resources']
    require(registered['job_id']==job and registered['state']=='submitted'
        and registered['script_sha256']==task['script_sha256']==canonical_hash(script.read_text())
        and fields.get('JobName')=='hhw-'+task['name']
        and resources=={'cpus':1,'memory':'16G','hours':1,'gpu':False,'array':None},
        'Native allocation does not match exact registered script/resources')
    return {'fields':fields,'native_stdout':response.stdout,'allocation_end_unix':end,
        'plan_path':str(directory/'plan.json'),'plan_sha256':canonical_hash(plan),
        'ledger_path':str(ledger_path),'ledger_sha256':digest(ledger_path),'script_sha256':digest(script)}


def qualification_proof(path,sha,job,observed):
    path=Path(path)
    require(path==QUALIFICATION and not path.is_symlink() and digest(path)==sha
        and re.fullmatch('[1-9][0-9]*',job) is not None,'Exact qualified4f qualification path/SHA/scalar job required')
    value=read_json(path);record=observed.get(job,{})
    require(value.get('status')=='complete' and type(value.get('returncode')) is int and value['returncode']==0
        and value.get('source_sha256')==SOURCE and value.get('config_sha256')==CONFIG
        and value.get('source_verified_unchanged') is True and value.get('slurm_job_id')==job
        and value.get('test_counts')=={'tests':1694,'failures':0,'errors':0,'skipped':0},
        'Positive actual full1694 qualified4f qualification required')
    require(record.get('state')=='COMPLETED' and record.get('exit_code')=='0:0'
        and record.get('user')=='pwa209' and record.get('account')=='def-ptewarie_cpu'
        and record.get('job_id_raw')==job and record.get('unallocated_metadata') is False
        and record.get('cpus')==4 and type(record.get('elapsed_seconds')) is int
        and 0<record['elapsed_seconds']<=3600 and 'gpu' not in record.get('alloc_tres','').lower(),
        'Positive owned scalar four-CPU qualification native completion required')
    proof=completed_native_dependency(ROOT,job)
    require(proof is not None,'Qualification final native accounting proof is pending')
    artifact=value.get('artifacts',{}).get('junit',{});junit=Path(artifact.get('path',''))
    require(junit.is_absolute() and junit.resolve().is_relative_to(ROOT/'state/qualification-evidence')
        and not junit.is_symlink() and digest(junit)==artifact.get('sha256')
        and junit.stat().st_size==artifact.get('bytes'),'Exact full qualification JUnit bytes required')
    from horsehair.cli import _qualification_junit
    require(_qualification_junit(junit)=={'tests':1694,'failures':0,'errors':0,'skipped':0},'Actual1694 native JUnit roster required')
    return {'path':str(path),'sha256':sha,'job_id':job,'native_observed':record,'completion':proof,
        'junit':artifact,'resource_policy':value.get('resource_policy')}


class PendingStorage(ValueError):pass


def storage_proof(config,quota):
    from horsehair.admission import _storage,AdmissionError
    path=ROOT/'state/storage-budget.json'
    if not path.is_file():raise PendingStorage('Exact existing study census is pending; no scan runs inside this audit')
    raw=path.read_bytes();value=json.loads(raw)
    captured=datetime.fromisoformat(value.get('captured_utc','').replace('Z','+00:00'))
    if captured.tzinfo is None or not 0<=(datetime.now(timezone.utc)-captured).total_seconds()<=21600:
        raise PendingStorage('Exact study census exceeds its six-hour freshness window')
    try:future_bytes,future_files,paths=_storage(ROOT,config,10_000_000_000,2000,personal_headroom=quota)
    except AdmissionError as error:raise PendingStorage(str(error)) from error
    import hashlib
    sha=hashlib.sha256(raw).hexdigest()
    if digest(path)!=sha:raise PendingStorage('Mutable study census changed during admission; preserve pending evidence')
    exclusive(SUPPORT/'study-census-snapshot.json',value)
    return {'path':str(path),'sha256':sha,'captured_utc':value['captured_utc'],
        'forecast_additional_bytes':10_000_000_000,'forecast_additional_files':2000,
        'projected_study_bytes':future_bytes,'projected_study_files':future_files,
        'native_source_paths':list(map(str,paths)),'headroom':quota}


def verify_private_junit(path):
    raw=Path(path).read_bytes()
    require(len(raw)<=8_000_000 and b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Bounded plain test JUnit required')
    cases=list(ET.fromstring(raw).iter('testcase'));names=[case.get('name') for case in cases]
    require(len(cases)==len(set(names))==37 and set(names)==set(TEST_NAMES)
        and all(case.get('classname','').endswith('test_audit_af_dam_secondary_v1_20261021')
            and not any(case.find(key) is not None for key in ['failure','error','skipped']) for case in cases),
        'Actual complete37 DAM test roster must pass without omissions/skips')
    from horsehair.cli import _qualification_junit
    require(_qualification_junit(path)=={'tests':37,'failures':0,'errors':0,'skipped':0},'Full DAM test counts differ')
    return canonical_hash(sorted(names))


def qualify_private(auditor,tests,job,end,*,runner=subprocess.run):
    require(digest(auditor)==AUDITOR_SHA and digest(tests)==TEST_SHA,'Pinned DAM source/tests changed')
    temporary=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temporary.is_absolute() and temporary.is_dir() and not temporary.is_symlink()
        and temporary.stat().st_uid==3164768 and shutil.disk_usage(temporary).free>=2_000_000_000
        and os.statvfs(temporary).f_favail>=2000,'Verified allocated node-local test capacity required')
    workspace=temporary/('hhw-dam-private-tests-'+job);workspace.mkdir(mode=0o700,exist_ok=False)
    command=[sys.executable,'-s','-m','pytest',str(tests),'-q','-p','no:cacheprovider',
        '--basetemp',str(workspace/'fixtures'),'--junitxml',str(SUPPORT/'private-tests-junit.xml')]
    environment=dict(os.environ,PYTEST_DISABLE_PLUGIN_AUTOLOAD='1',PYTHONDONTWRITEBYTECODE='1')
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):environment.pop(key,None)
    require(end-time.time()>2400,'Native margin insufficient for tests plus bounded1800-second source audit')
    exclusive(SUPPORT/'private-tests-intent.json',{'command':command,'auditor_sha256':AUDITOR_SHA,
        'tests_sha256':TEST_SHA,'native_job_id':job,'workspace':str(workspace),**{key:False for key in FALSE_FLAGS}})
    with (SUPPORT/'private-tests-stdout.txt').open('xb') as stdout,(SUPPORT/'private-tests-stderr.txt').open('xb') as stderr:
        result=runner(command,cwd=FRAMEWORK,stdout=stdout,stderr=stderr,env=environment,timeout=min(600,end-time.time()-2100),check=False)
    require(result.returncode==0,'DAM private tests failed before any source audit')
    roster=verify_private_junit(SUPPORT/'private-tests-junit.xml')
    require(digest(auditor)==AUDITOR_SHA and digest(tests)==TEST_SHA,'Private source/tests changed during qualification')
    value={'status':'complete_private_37_test_qualification','returncode':0,'native_job_id':job,
        'auditor_sha256':AUDITOR_SHA,'tests_sha256':TEST_SHA,
        'test_counts':{'tests':37,'failures':0,'errors':0,'skipped':0},
        'junit_sha256':digest(SUPPORT/'private-tests-junit.xml'),'test_roster_sha256':roster,
        'actual_provider_source_audit_performed':False,**{key:False for key in FALSE_FLAGS}}
    exclusive(SUPPORT/'private-test-qualification.json',value)
    return value


def run_source_audit(auditor,end,*,runner=subprocess.run):
    require(end-time.time()>1980 and not CASE.exists(),'Fresh result and full1800-second audit/finalization margin required')
    require(digest(auditor)==AUDITOR_SHA and digest(MANIFEST)==MANIFEST_SHA,'Bound source manifest/auditor changed')
    command=[sys.executable,'-s','-u',str(auditor),'--manifest',str(MANIFEST),
        '--manifest-sha256',MANIFEST_SHA,'--output',str(CASE),'--max-seconds','1800']
    exclusive(SUPPORT/'source-audit-intent.json',{'command':command,'source_manifest_sha256':MANIFEST_SHA,
        'auditor_sha256':AUDITOR_SHA,**{key:False for key in FALSE_FLAGS}})
    with (SUPPORT/'source-audit-stdout.txt').open('xb') as stdout,(SUPPORT/'source-audit-stderr.txt').open('xb') as stderr:
        result=runner(command,cwd=FRAMEWORK,stdout=stdout,stderr=stderr,
            timeout=min(1980,end-time.time()-180),check=False,
            env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'))
    require(result.returncode==0,'Actual DAM source audit failed; partial evidence remains separate from test qualification')
    value=read_json(CASE/'receipt.json')
    require(value.get('status')=='complete_read_only_structural_audit' and value.get('files_audited')==6
        and value.get('record_id')=='17175344' and value.get('manifest')=={'path':str(MANIFEST),'sha256':MANIFEST_SHA}
        and value.get('auditor_sha256')==AUDITOR_SHA and value.get('all_provider_identities_reverified') is True
        and all(value.get(key) is False for key in FALSE_FLAGS)
        and value.get('original_H1_H2_H3_gates_cleared') is False
        and value.get('analysis_admitted') is False and value.get('complete_declared_study') is False,
        'Complete structural source audit must preserve every inference/admission gate false')
    require(digest(auditor)==AUDITOR_SHA and digest(MANIFEST)==MANIFEST_SHA,'Auditor or manifest changed during source audit')
    return value


def main():
    parser=argparse.ArgumentParser()
    for name in ('auditor','tests','native-script'):parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args();job=str(native_job_id() or '')
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='1' and os.environ.get('HHW_GPU_COUNT')=='0'
        and os.environ.get('HHW_MAX_HOURS')=='1','Owned finite1CPU native DAM allocation required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval context required')
    import horsehair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,
        'Exact qualified4f framework and unchanged isolated primary required')
    config=read_json(PRIMARY/'configs/study.json')
    require(canonical_hash(config)==CONFIG and read_json(FRAMEWORK/'configs/study.json')==config,'Frozen full config required')
    require(not SUPPORT.exists() and not CASE.exists(),'Fresh immutable DAM support/results; no ambiguous replay')
    require(args.auditor.name=='audit-af-dam-secondary-v1-20261021.py'
        and args.tests.name=='test_audit_af_dam_secondary_v1_20261021.py' and args.auditor.parent==args.tests.parent,
        'Exact pinned colocated DAM helper/test pair required')
    for path,sha in [(args.auditor,AUDITOR_SHA),(args.tests,TEST_SHA),(MANIFEST,MANIFEST_SHA),
        (GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(QUOTA,QUOTA_SHA),(QUALIFICATION,QUALIFICATION_SHA)]:
        require(path.is_absolute() and path==path.resolve() and path.is_relative_to(ROOT/'maintenance')
            and not path.is_symlink() and digest(path)==sha,'Exact pinned ordinary native metadata/source required')
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    SUPPORT.mkdir(mode=0o700)
    runpy.run_path(str(GUARD))['install'](SUPPORT/'accounting-reads')
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'Guard must precede campaign reader binding')
    reader=runpy.run_path(str(SCHEDULER))['run_scontrol_job'];allocation=native_allocation(job,args.native_script,reader)
    campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],SUPPORT,
        'dam-baseline',resource_policy=policy,cpus=0,gpus=0,hours=1)
    q=read_json(QUALIFICATION);qjob=str(q.get('slurm_job_id',''))
    observed=repair._repair_native_states([qjob],{},owner='pwa209',account='def-ptewarie_cpu')
    qualified=qualification_proof(QUALIFICATION,QUALIFICATION_SHA,qjob,observed)
    with resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        quota=runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,
            required_bytes=10_000_000_000,reserve_files=248000,reserve_bytes=2_000_000_000_000,
            evidence_dir=SUPPORT/'quota-reads',allocation_deadline_unix=allocation['allocation_end_unix'])
        try:storage=storage_proof(config,quota)
        except PendingStorage as error:
            exclusive(SUPPORT/'receipt.json',{'status':'pending_storage_no_tests_or_audit_started','reason':str(error),
                'native_job_id':job,'allocation':allocation,**{key:False for key in FALSE_FLAGS}})
            return 0
        exclusive(SUPPORT/'native-bindings.json',{'allocation':allocation,'qualified_framework':qualified,
            'storage':storage,'resource_policy':policy.receipt(),'wrapper_sha256':digest(Path(__file__)),
            'source_sha256':SOURCE,'primary_source_sha256':PRIMARY_SHA,'full_config_sha256':CONFIG})
        tests=qualify_private(args.auditor,args.tests,job,allocation['allocation_end_unix'])
        result=run_source_audit(args.auditor,allocation['allocation_end_unix'])
        require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA
            and digest(POLICY)==POLICY_SHA,'Framework/primary/resource policy changed during audit')
        receipt={'status':'complete_native_read_only_dam_structural_audit','native_job_id':job,
            'source_sha256':SOURCE,'full_config_sha256':CONFIG,'resource_policy':policy.receipt(),
            'private_test_qualification':{'path':str(SUPPORT/'private-test-qualification.json'),
                'sha256':digest(SUPPORT/'private-test-qualification.json'),'test_counts':tests['test_counts']},
            'actual_source_audit':{'path':str(CASE/'receipt.json'),'sha256':digest(CASE/'receipt.json'),
                'status':result['status'],'files_audited':result['files_audited']},
            'source_manifest_sha256':MANIFEST_SHA,'allocation':allocation,
            'scope':'Structural metadata/raw-clock quality only; no effects, R execution or biological gate clearance',
            **{key:False for key in FALSE_FLAGS}}
        exclusive(SUPPORT/'receipt.json',receipt);print(json.dumps(receipt,sort_keys=True));return 0


if __name__=='__main__':raise SystemExit(main())

