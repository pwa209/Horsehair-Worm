"""Qualified read-only ethoscope SQLite inventory; separate57/39 native tests, no inference."""
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
CASE=ROOT/'maintenance/study-af-ethoscope-sqlite-audit-v2-20261021'
SUPPORT=ROOT/'maintenance/study-af-ethoscope-sqlite-support-v2-20261021'
QUALIFICATION=ROOT/'maintenance/study-af-execution-support-v1-20261021/qualification.json'
POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=GUARD.with_name('native-scheduler-read.py')
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
QUOTA=ROOT/'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'
QUOTA_SHA='5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045'
REGISTRY_SUPPORT=ROOT/'maintenance/study-af-registry-repair-support-v1-20261021'
REGISTRY_GUARD=REGISTRY_SUPPORT/'registered-snapshot-read-guard-20261021.py'
REGISTRY_GUARD_SHA='42fd488d27d1bf9aa75b46c113257525e296c526250e75ba12319d6139854db4'
REGISTRY_Q=REGISTRY_SUPPORT/'private-qualification.json'
REGISTRY_Q_HELPER=REGISTRY_SUPPORT/'qualify-f2-registry-repair-v1-20261021.py'
REGISTRY_Q_HELPER_SHA='fa655450d6d67a2ae517339965cc6befe44d1621c5751549b85f046a86c7e61c'
REGISTRY_CONTROLLER=REGISTRY_SUPPORT/'run-af-agent-controller-registry-v2-20261021.py'
REGISTRY_CONTROLLER_SHA='e1852d78a1f00e9ee8855413db1f6599fe31a73c4f4498d7b09158f4c4f61a1c'
FALSE_FLAGS=('scientific_result','effects_calculated','effects_inspected','provider_code_executed','scientific_gates_cleared','admission_authority')


QUALIFICATION_SHA="29ed530f3783f3c35c3cf20343bdb663732ec2bf4bf5c3218f8b029510becc2f"
AUDITOR_SHA='fec8396ee185d1751320761ef80666f73ea97ffe7f957faea24c51414d00de94'
TEST_SHA='92d19d1cc020d3160dae20834e07ab4195bbbc0b389d98c980b44d9a06242f5d'
RANGE_SHA="30a843a2771196a3eec1e202f8e695fd1d41e58b6423a73218ec74b6285ea17c"
RANGE_TEST_SHA="6a7d31cdcc08674aee39af0361cfe234d1eb669548634daf3458c4dd592f4ab3"
TEST_NAMES={'schema': ('test_schema_and_raw_clock_counts_are_exact_without_outcome_values', 'test_authorizer_refuses_writes_attachments_functions_and_outcome_reads[INSERT INTO ROI_1(t) VALUES(3)]', 'test_authorizer_refuses_writes_attachments_functions_and_outcome_reads[UPDATE ROI_1 SET t=2]', 'test_authorizer_refuses_writes_attachments_functions_and_outcome_reads[DELETE FROM ROI_1]', 'test_authorizer_refuses_writes_attachments_functions_and_outcome_reads[DROP TABLE ROI_1]', 'test_authorizer_refuses_writes_attachments_functions_and_outcome_reads[CREATE TABLE extra(x)]', 'test_authorizer_refuses_writes_attachments_functions_and_outcome_reads[ATTACH DATABASE ":memory:" AS other]', 'test_authorizer_refuses_writes_attachments_functions_and_outcome_reads[PRAGMA journal_mode=WAL]', 'test_authorizer_refuses_writes_attachments_functions_and_outcome_reads[PRAGMA query_only=OFF]', 'test_authorizer_refuses_writes_attachments_functions_and_outcome_reads[SELECT load_extension("bad")]', 'test_authorizer_refuses_writes_attachments_functions_and_outcome_reads[SELECT x FROM ROI_1]', 'test_authorizer_refuses_writes_attachments_functions_and_outcome_reads[SELECT y FROM ROI_1]', 'test_authorizer_refuses_writes_attachments_functions_and_outcome_reads[SELECT velocity FROM ROI_1]', 'test_authorizer_refuses_writes_attachments_functions_and_outcome_reads[SELECT max(x) FROM ROI_1]', 'test_extension_capability_report_is_truthful_without_weakening_sql_authorizer[False]', 'test_extension_capability_report_is_truthful_without_weakening_sql_authorizer[True]', 'test_compiled_no_extension_api_still_completes_exact_schema_clock_inventory', 'test_exposed_but_unusable_extension_api_fails_closed_and_closes_connection[disable_raises]', 'test_exposed_but_unusable_extension_api_fails_closed_and_closes_connection[noncallable]', 'test_views_without_rowid_and_unknown_time_schema_are_inventory_only', 'test_missing_unknown_metadata_and_duplicate_roi_ids_preserved', 'test_expired_deadline_interrupts_sql_and_finite_loop', 'test_sqlite_progress_handler_interrupts_large_builtin_query', 'test_untrusted_sql_identifier_never_interpolated[a"; DROP TABLE x;--]', 'test_untrusted_sql_identifier_never_interpolated[../x]', 'test_untrusted_sql_identifier_never_interpolated[a.b]', 'test_untrusted_sql_identifier_never_interpolated[a b]', 'test_untrusted_sql_identifier_never_interpolated[]', 'test_untrusted_sql_identifier_never_interpolated[xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx]', 'test_complete_source_bound_audit_stays_structural_and_preserves_all_inputs', 'test_source_or_receipt_corruption_cannot_produce_success[acquisition_sha]', 'test_source_or_receipt_corruption_cannot_produce_success[status]', 'test_source_or_receipt_corruption_cannot_produce_success[publication]', 'test_source_or_receipt_corruption_cannot_produce_success[helper]', 'test_source_or_receipt_corruption_cannot_produce_success[raw_sha]', 'test_source_or_receipt_corruption_cannot_produce_success[raw_md5]', 'test_source_or_receipt_corruption_cannot_produce_success[raw_bytes]', 'test_source_or_receipt_corruption_cannot_produce_success[raw_crc]', 'test_source_or_receipt_corruption_cannot_produce_success[database_bytes]', 'test_source_or_receipt_corruption_cannot_produce_success[record]', 'test_source_or_receipt_corruption_cannot_produce_success[checksum]', 'test_source_or_receipt_corruption_cannot_produce_success[index]', 'test_source_or_receipt_corruption_cannot_produce_success[documentation]', 'test_source_or_receipt_corruption_cannot_produce_success[paths]', 'test_source_or_receipt_corruption_cannot_produce_success[clearance]', 'test_mid_audit_error_keeps_start_and_failure_no_complete_receipt', 'test_source_changed_after_inventory_refused', 'test_native_cpu_contract_refuses_device_zero_and_all_gpu_assignments[0]', 'test_native_cpu_contract_refuses_device_zero_and_all_gpu_assignments[1]', 'test_native_cpu_contract_refuses_device_zero_and_all_gpu_assignments[0,1]', 'test_native_cpu_contract_refuses_device_zero_and_all_gpu_assignments[gpu:1]', 'test_valid_native_contract_records_only_cpu_and_no_admission', 'test_invalid_deadlines_refused_before_any_source_read[0]', 'test_invalid_deadlines_refused_before_any_source_read[59]', 'test_invalid_deadlines_refused_before_any_source_read[1801]', 'test_invalid_deadlines_refused_before_any_source_read[True]', 'test_invalid_deadlines_refused_before_any_source_read[nan]'), 'range': ('test_complete_verified_member_has_exact_bytes_and_false_scientific_flags', 'test_bad_response_headers_fail_before_body[delta0]', 'test_bad_response_headers_fail_before_body[delta1]', 'test_bad_response_headers_fail_before_body[delta2]', 'test_bad_response_headers_fail_before_body[delta3]', 'test_non206_is_not_read_or_retried[200]', 'test_non206_is_not_read_or_retried[302]', 'test_non206_is_not_read_or_retried[401]', 'test_non206_is_not_read_or_retried[403]', 'test_non206_is_not_read_or_retried[500]', 'test_partial_chunk_and_error_retained_no_promotion[short]', 'test_partial_chunk_and_error_retained_no_promotion[long]', 'test_partial_chunk_and_error_retained_no_promotion[timeout]', 'test_local_header_variants_refused[mutation0]', 'test_local_header_variants_refused[mutation1]', 'test_local_header_variants_refused[mutation2]', 'test_local_header_variants_refused[mutation3]', 'test_local_header_variants_refused[mutation4]', 'test_local_header_variants_refused[mutation5]', 'test_local_header_variants_refused[mutation6]', 'test_local_header_variants_refused[mutation7]', 'test_wrong_local_filename_refused', 'test_zip64_extra_refused_even_with_plain_sizes', 'test_successor_disagreement_prevents_hidden_descriptor', 'test_full_member_checks_refuse_corruption[corrupt_deflate]', 'test_full_member_checks_refuse_corruption[wrong_provider_md5]', 'test_full_member_checks_refuse_corruption[wrong_crc]', 'test_full_member_checks_refuse_corruption[raw_bound]', 'test_full_member_checks_refuse_corruption[compressed_trailing]', 'test_changed_metadata_source_refused_without_network[record]', 'test_changed_metadata_source_refused_without_network[checksum]', 'test_changed_metadata_source_refused_without_network[index]', 'test_fresh_case_required_no_ambiguity_replay', 'test_publication_collision_keeps_existing_and_verified_candidate', 'test_change_after_deflate_verification_refused_before_promotion[candidate]', 'test_change_after_deflate_verification_refused_before_promotion[source_metadata]', 'test_outside_personal_case_refused', 'test_subordinate_directory_symlink_refused', 'test_no_sqlite_import_or_query_in_gateway_source')}
ACQUISITION=ROOT/'maintenance/study-af-ethoscope-db-canary-v1-20261021/acquisition-result.json'
ACQUISITION_SHA='a696a19c549f40e4a14945ff8fff04fbcc5cbf5fafb6fe52471eab4cd3bffb54'
RAW_DB=ACQUISITION.with_name('2022-04-22_11-21-12_15517bc8ec75403fb282ef8e166d150a.db')
RAW_SHA='adede032bab1bda988072af5200983bc4037993b78aa807acb398c05e7fd9cc3'
DOCS=ROOT/'maintenance/study-af-ethoscope-documentation-v1-20261021'
INPUTS={
 'record':(DOCS/'6567039.json','2514b6e65c2892c69db797263ee545d546a76beee4ac931fc3b1257f45992cce'),
 'checksum':(DOCS/'db_files_with_checksum.txt','39d6f17957c394d95b3746df1aa4ff75ba15ae4722ffede293ed0c6de9c729c6'),
 'index':(ROOT/'maintenance/study-af-ethoscope-db-canary-support-v1-20261021/ethoscope-split-zip-index.json','ffc58aa960a5fdaec167107c4fece8fd7789fb5daad79675c9aa85980dee46b6'),
 'documentation':(DOCS/'html_notebooks.zip','499d00647a734caa528ee33cc4ab7e1f66217581355c3cdae59ac82f9fdfcee6')}
AUDIT_FALSE_FLAGS=('scientific_result','analysis_admitted','admission_authority','h1_cleared',
 'source_replication_passed','biological_transfer_supported','per_animal_exposure_time_verified',
 'physical_2d_units_qualified','activity_or_position_values_read','data_repaired_or_imputed')


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


def registry_qualification(path,sha,job):
    """Bind actual full84 source/JUnit evidence before installing its read guard."""
    path=Path(path)
    require(path==REGISTRY_Q and path==path.resolve() and not path.is_symlink()
        and re.fullmatch('[0-9a-f]{64}',sha) is not None and digest(path)==sha
        and re.fullmatch('[1-9][0-9]*',job) is not None,'Exact explicit private registry qualification path/SHA/job required')
    for source,expected in [(REGISTRY_Q_HELPER,REGISTRY_Q_HELPER_SHA),(REGISTRY_GUARD,REGISTRY_GUARD_SHA),
        (REGISTRY_CONTROLLER,REGISTRY_CONTROLLER_SHA)]:
        require(source.is_file() and source==source.resolve() and not source.is_symlink()
            and source.is_relative_to(ROOT/'maintenance') and digest(source)==expected,'Pinned qualified registry helper changed')
    q=read_json(path)
    require(q.get('status')=='complete_f2_registry_overlay_qualification' and type(q.get('returncode')) is int and q['returncode']==0
        and q.get('source_sha256')==SOURCE and q.get('config_sha256')==CONFIG and q.get('slurm_job_id')==job
        and q.get('qualification_helper_sha256')==REGISTRY_Q_HELPER_SHA
        and q.get('registry_guard_sha256')==REGISTRY_GUARD_SHA and q.get('controller_sha256')==REGISTRY_CONTROLLER_SHA
        and q.get('worker_shim_sha256')=='dd39905ce44f89f7f8bbdd325d1a0f1d5ec7950dd89d0e3bb267f6fdfe77a09a'
        and q.get('test_counts')=={'tests':84,'failures':0,'errors':0,'skipped':0},'Positive exact source-bound private84 registry qualification required')
    runpy.run_path(str(REGISTRY_Q_HELPER),run_name='sqlite_registry_q_source_validation')['verify_pins']()
    runpy.run_path(str(REGISTRY_CONTROLLER),run_name='sqlite_registry_q_roster_validation')['verify_operational_tests'](q)
    require(digest(path)==sha,'Private registry qualification changed during verification')
    return {'path':str(path),'sha256':sha,'job_id':job,'value':q,
        'guard':{'path':str(REGISTRY_GUARD),'sha256':REGISTRY_GUARD_SHA}}


def completed_registry_qualification(binding,observed):
    job=binding['job_id'];record=observed.get(job,{})
    require(record.get('state')=='COMPLETED' and record.get('exit_code')=='0:0' and record.get('user')=='pwa209'
        and record.get('account')=='def-ptewarie_cpu' and record.get('job_id_raw')==job
        and record.get('unallocated_metadata') is False and type(record.get('cpus')) is int and record['cpus']==4
        and type(record.get('elapsed_seconds')) is int and 0<record['elapsed_seconds']<=3600
        and 'gpu' not in record.get('alloc_tres','').lower(),'Positive owned scalar4CPU private registry native completion required')
    proof=completed_native_dependency(ROOT,job)
    require(proof is not None,'Private registry native qualification is not positively complete')
    registry_qualification(binding['path'],binding['sha256'],job)
    return {k:v for k,v in binding.items() if k!='value'}|{'native_observed':record,'completion':proof}


def qualified_registry_read_guard(path,sha,job,end):
    binding=registry_qualification(path,sha,job)
    installed=runpy.run_path(str(REGISTRY_GUARD))['install'](SUPPORT/'registered-snapshot-reads',
        registration_mode='worker-lock',allocation_deadline_unix=end)
    return binding,installed

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

def private_groups(auditor,tests,range_helper,range_tests):
    return {'schema':{'module':Path(auditor),'module_sha':AUDITOR_SHA,'tests':Path(tests),'tests_sha':TEST_SHA,
                      'test_basename':'test_audit_ethoscope_sqlite_schema_v2_20261021','count':57},
            'range':{'module':Path(range_helper),'module_sha':RANGE_SHA,'tests':Path(range_tests),'tests_sha':RANGE_TEST_SHA,
                     'test_basename':'test_acquire_ethoscope_db_canary_20261021','count':39}}


def verify_private_junit(path,kind):
    names_expected=TEST_NAMES[kind];raw=Path(path).read_bytes()
    require(len(raw)<=8_000_000 and b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Bounded plain private JUnit required')
    cases=list(ET.fromstring(raw).iter('testcase'));names=[case.get('name') for case in cases]
    basename={'schema':'test_audit_ethoscope_sqlite_schema_v2_20261021','range':'test_acquire_ethoscope_db_canary_20261021'}[kind]
    require(len(cases)==len(set(names))==len(names_expected) and set(names)==set(names_expected)
        and all(case.get('classname','').endswith(basename)
            and not any(case.find(key) is not None for key in ['failure','error','skipped']) for case in cases),
        'Actual complete private '+kind+' roster must pass without omissions/skips')
    from horsehair.cli import _qualification_junit
    require(_qualification_junit(path)=={'tests':len(names_expected),'failures':0,'errors':0,'skipped':0},'Full native private test counts differ')
    return canonical_hash(sorted(names))


def qualify_private(auditor,tests,range_helper,range_tests,job,end,*,runner=subprocess.run):
    groups=private_groups(auditor,tests,range_helper,range_tests)
    for group in groups.values():
        require(digest(group['module'])==group['module_sha'] and digest(group['tests'])==group['tests_sha'],'Pinned private source/tests changed')
    temporary=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temporary.is_absolute() and temporary.is_dir() and not temporary.is_symlink()
        and temporary.stat().st_uid==3164768 and shutil.disk_usage(temporary).free>=2_000_000_000
        and os.statvfs(temporary).f_favail>=2000,'Verified allocated node-local test capacity required')
    require(end-time.time()>1800,'Native margin insufficient for two bounded private suites and900-second source inventory')
    qualifications={}
    for kind,group in groups.items():
        workspace=temporary/('hhw-ethoscope-private-tests-'+job+'-'+kind);workspace.mkdir(mode=0o700,exist_ok=False)
        junit=SUPPORT/(kind+'-tests-junit.xml')
        command=[sys.executable,'-s','-m','pytest',str(group['tests']),'-q','-p','no:cacheprovider',
            '--basetemp',str(workspace/'fixtures'),'--junitxml',str(junit)]
        environment=dict(os.environ,PYTEST_DISABLE_PLUGIN_AUTOLOAD='1',PYTHONDONTWRITEBYTECODE='1')
        for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):environment.pop(key,None)
        require(end-time.time()>1080,'Native private qualification finalization margin exhausted')
        exclusive(SUPPORT/(kind+'-tests-intent.json'),{'command':command,'module_sha256':group['module_sha'],
            'tests_sha256':group['tests_sha'],'native_job_id':job,'workspace':str(workspace),**{key:False for key in FALSE_FLAGS}})
        with (SUPPORT/(kind+'-tests-stdout.txt')).open('xb') as stdout,(SUPPORT/(kind+'-tests-stderr.txt')).open('xb') as stderr:
            result=runner(command,cwd=FRAMEWORK,stdout=stdout,stderr=stderr,env=environment,
                timeout=min(300,end-time.time()-1080),check=False)
        require(result.returncode==0,'Private '+kind+' tests failed before any actual DB schema read')
        roster=verify_private_junit(junit,kind)
        require(digest(group['module'])==group['module_sha'] and digest(group['tests'])==group['tests_sha'],'Private bytes changed during qualification')
        value={'status':'complete_private_'+str(group['count'])+'_test_qualification','returncode':0,'native_job_id':job,
            'module_sha256':group['module_sha'],'tests_sha256':group['tests_sha'],
            'test_counts':{'tests':group['count'],'failures':0,'errors':0,'skipped':0},
            'junit':{'path':str(junit),'sha256':digest(junit)},'test_roster_sha256':roster,
            'actual_provider_database_schema_inspected':False,**{key:False for key in FALSE_FLAGS}}
        path=SUPPORT/(kind+'-test-qualification.json');exclusive(path,value)
        qualifications[kind]={'path':str(path),'sha256':digest(path),'value':value}
    return qualifications


def run_source_audit(auditor,end,*,runner=subprocess.run):
    require(end-time.time()>1080 and not CASE.exists(),'Fresh result and full900-second inventory/finalization margin required')
    require(digest(auditor)==AUDITOR_SHA and digest(ACQUISITION)==ACQUISITION_SHA and digest(RAW_DB)==RAW_SHA,'Bound canary/auditor changed')
    command=[sys.executable,'-s','-u',str(auditor),'--acquisition',str(ACQUISITION),
        '--acquisition-sha256',ACQUISITION_SHA]
    for key,(path,sha) in INPUTS.items():
        require(digest(path)==sha,'Bound original documentation/index changed')
        command.extend(['--'+key,str(path)])
    command.extend(['--output',str(CASE),'--max-seconds','900'])
    exclusive(SUPPORT/'source-audit-intent.json',{'command':command,'acquisition_sha256':ACQUISITION_SHA,
        'raw_db_sha256':RAW_SHA,'auditor_sha256':AUDITOR_SHA,**{key:False for key in FALSE_FLAGS}})
    with (SUPPORT/'source-audit-stdout.txt').open('xb') as stdout,(SUPPORT/'source-audit-stderr.txt').open('xb') as stderr:
        result=runner(command,cwd=FRAMEWORK,stdout=stdout,stderr=stderr,
            timeout=min(1080,end-time.time()-180),check=False,
            env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'))
    require(result.returncode==0,'Actual DB structural inventory failed; test qualifications remain separate evidence')
    value=read_json(CASE/'receipt.json')
    require(value.get('status')=='complete_read_only_structural_inventory'
        and value.get('auditor_sha256')==AUDITOR_SHA and value.get('acquisition')=={'path':str(ACQUISITION),'sha256':ACQUISITION_SHA}
        and value.get('database')=={'path':str(RAW_DB),'bytes':13594624,'md5':'e261acc31375cb122164c92ed957206e','sha256':RAW_SHA}
        and value.get('all_raw_and_documentation_identities_reverified') is True
        and value.get('unknown_schema_does_not_clear_a_scientific_gate') is True
        and value.get('source_metadata_exposure_timestamp_present') is False
        and all(value.get(key) is False for key in AUDIT_FALSE_FLAGS),'Complete source inventory must preserve all unknowns and false scientific gates')
    require({(p['role'],p['path'],p['sha256']) for p in value.get('source_pins',[])}==
        {(key,str(path),sha) for key,(path,sha) in INPUTS.items()},'Actual inventory documentation/source bindings differ')
    source_rows=value.get('source_animal_metadata_rows',[])
    require(len(source_rows)==20 and {r['region_id'] for r in source_rows}=={str(i) for i in range(1,21)}
        and all(r['date']=='2022-04-22' and r['machine_name']=='ETHOSCOPE_155' for r in source_rows),'Actual inventory source ROI metadata differs')
    inventory=value.get('inventory',{});path=Path(inventory.get('path',''))
    require(path==CASE/'schema-and-clock-inventory.json' and not path.is_symlink() and digest(path)==inventory.get('sha256'),'Exact structural inventory bytes required')
    detail=read_json(path)
    require(detail.get('outcome_values_read') is False and detail.get('provider_SQL_or_code_executed') is False
        and detail.get('clock_units_qualified') is False and detail.get('expected_grid_missingness_qualified') is False,
        'Schema presence cannot clear outcomes/clock/grid scientific qualifications')
    extension=detail.get('extension_loading_policy',{})
    require(set(extension)=={'python_enable_load_extension_api_present','explicit_disable_completed',
        'api_absence_does_not_establish_sqlite_compile_options','sql_load_extension','all_nonallowlisted_sql_functions'}
        and type(extension.get('python_enable_load_extension_api_present')) is bool
        and extension.get('explicit_disable_completed') is extension['python_enable_load_extension_api_present']
        and extension.get('api_absence_does_not_establish_sqlite_compile_options') is True
        and extension.get('sql_load_extension')=='denied_by_authorizer'
        and extension.get('all_nonallowlisted_sql_functions')=='denied_by_authorizer',
        'Truthful extension capability and unchanged SQL authorizer denial policy required')
    require(digest(auditor)==AUDITOR_SHA and digest(ACQUISITION)==ACQUISITION_SHA and digest(RAW_DB)==RAW_SHA
        and all(digest(path)==sha for path,sha in INPUTS.values()),'Canary/auditor/documentation changed during native inventory')
    return value


def main():
    parser=argparse.ArgumentParser()
    for name in ('auditor','tests','range-helper','range-tests','native-script'):parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--registry-qualification',type=Path,required=True)
    parser.add_argument('--registry-qualification-sha256',required=True)
    parser.add_argument('--registry-qualification-job-id',required=True)
    args=parser.parse_args();job=str(native_job_id() or '')
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='1' and os.environ.get('HHW_GPU_COUNT')=='0'
        and os.environ.get('HHW_MAX_HOURS')=='1','Owned finite1CPU native SQLite inventory allocation required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval context required')
    import horsehair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,
        'Exact qualified4f framework and unchanged isolated primary required')
    config=read_json(PRIMARY/'configs/study.json')
    require(canonical_hash(config)==CONFIG and read_json(FRAMEWORK/'configs/study.json')==config,'Frozen full config required')
    require(not SUPPORT.exists() and not CASE.exists(),'Fresh immutable support/results; no ambiguous replay')
    names=['audit-ethoscope-sqlite-schema-v2-20261021.py','test_audit_ethoscope_sqlite_schema_v2_20261021.py',
        'acquire-ethoscope-db-canary-20261021.py','test_acquire_ethoscope_db_canary_20261021.py']
    sources=[args.auditor,args.tests,args.range_helper,args.range_tests]
    require([p.name for p in sources]==names and len({p.parent for p in sources})==1,'Exact pinned colocated helper/test pairs required')
    pinned=[(args.auditor,AUDITOR_SHA),(args.tests,TEST_SHA),(args.range_helper,RANGE_SHA),(args.range_tests,RANGE_TEST_SHA),
        (ACQUISITION,ACQUISITION_SHA),(RAW_DB,RAW_SHA),*INPUTS.values(),
        (GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(QUOTA,QUOTA_SHA),(QUALIFICATION,QUALIFICATION_SHA)]
    for path,sha in pinned:
        require(path.is_absolute() and path==path.resolve() and path.is_relative_to(ROOT/'maintenance')
            and not path.is_symlink() and digest(path)==sha,'Exact pinned ordinary native source required')
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    SUPPORT.mkdir(mode=0o700)
    runpy.run_path(str(GUARD))['install'](SUPPORT/'accounting-reads')
    reader=runpy.run_path(str(SCHEDULER))['run_scontrol_job'];allocation=native_allocation(job,args.native_script,reader)
    registry,registry_installation=qualified_registry_read_guard(args.registry_qualification,args.registry_qualification_sha256,
        args.registry_qualification_job_id,allocation['allocation_end_unix'])
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'Guard must precede campaign reader binding')
    campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],SUPPORT,
        'ethoscope-sqlite-baseline',resource_policy=policy,cpus=0,gpus=0,hours=1)
    q=read_json(QUALIFICATION);qjob=str(q.get('slurm_job_id',''))
    observed=repair._repair_native_states(sorted({qjob,registry['job_id']}),{},owner='pwa209',account='def-ptewarie_cpu')
    qualified=qualification_proof(QUALIFICATION,QUALIFICATION_SHA,qjob,observed)
    registry_completed=completed_registry_qualification(registry,observed)
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
            'qualified_registry_overlay':registry_completed,'registry_installation':registry_installation,
            'storage':storage,'resource_policy':policy.receipt(),'wrapper_sha256':digest(Path(__file__)),
            'source_sha256':SOURCE,'primary_source_sha256':PRIMARY_SHA,'full_config_sha256':CONFIG})
        qualifications=qualify_private(args.auditor,args.tests,args.range_helper,args.range_tests,job,allocation['allocation_end_unix'])
        result=run_source_audit(args.auditor,allocation['allocation_end_unix'])
        require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA
            and digest(POLICY)==POLICY_SHA,'Framework/primary/resource policy changed during audit')
        receipt={'status':'complete_native_read_only_ethoscope_sqlite_inventory','native_job_id':job,
            'qualified_registry_overlay':registry_completed,'registry_installation':registry_installation,
            'source_sha256':SOURCE,'full_config_sha256':CONFIG,'resource_policy':policy.receipt(),
            'private_qualifications':{key:{k:v for k,v in item.items() if k!='value'} for key,item in qualifications.items()},
            'actual_source_inventory':{'path':str(CASE/'receipt.json'),'sha256':digest(CASE/'receipt.json'),
                'status':result['status'],'inventory':result['inventory']},
            'acquisition_sha256':ACQUISITION_SHA,'raw_db_sha256':RAW_SHA,'allocation':allocation,
            'scope':'Bounded read-only schema/ROI/raw-clock inventory only; unknown units/exposure retained',
            **{key:False for key in FALSE_FLAGS}}
        exclusive(SUPPORT/'receipt.json',receipt);print(json.dumps(receipt,sort_keys=True));return 0


if __name__=='__main__':raise SystemExit(main())
