"""Native bounded VTL structural audit; full68+40 private tests precede track bytes."""
from pathlib import Path
from datetime import datetime,timezone
import argparse
import hashlib
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
from horsehair.core import read_json,source_identity,canonical_hash,digest,native_job_id
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy
from horsehair.slurm import completed_native_dependency

ROOT=Path('/scratch/pwa209/horsehair-worm')

PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')

PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'

FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'

SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'

CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'

QUALIFICATION=ROOT/'maintenance/study-af-execution-support-v1-20261021/qualification.json'

POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'

POLICY_SHA='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'

GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'

GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'

SCHEDULER=GUARD.with_name('native-scheduler-read.py')

SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'

QUOTA=ROOT/'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'

QUOTA_SHA='5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045'

REGISTRY_SUPPORT=ROOT/'maintenance/study-af-registry-repair-support-v4-20261021'

REGISTRY_GUARD=REGISTRY_SUPPORT/'registered-snapshot-read-guard-v3-20261021.py'

REGISTRY_GUARD_SHA='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'

REGISTRY_Q=REGISTRY_SUPPORT/'private-qualification.json'
REGISTRY_Q_SHA='1f391caccd4629eb47150862f7bc2575e991e4ce5cb5aae99e955cdeae4510ca'
REGISTRY_Q_JOB='6699561'

REGISTRY_Q_HELPER=REGISTRY_SUPPORT/'qualify-f2-registry-repair-v4-20261021.py'

REGISTRY_Q_HELPER_SHA='7fd2348c3370811cb5ca2933357a007447b18c2ea57be1cef502fdd035fdda51'

REGISTRY_CONTROLLER=REGISTRY_SUPPORT/'run-af-agent-controller-registry-v4-20261021.py'

REGISTRY_CONTROLLER_SHA='475a170aab2d158cfd17adb250707e1d0767086d4c50b95516c360f2222560f8'

FALSE_FLAGS=('scientific_result','effects_calculated','effects_inspected','provider_code_executed','scientific_gates_cleared','admission_authority')

QUALIFICATION_SHA="29ed530f3783f3c35c3cf20343bdb663732ec2bf4bf5c3218f8b029510becc2f"

CASE=ROOT/'maintenance/study-af-vtl-track-structure-v2-20261021'

SUPPORT=ROOT/'maintenance/study-af-vtl-track-structure-support-v2-20261021'

class PendingStorage(ValueError):pass

AUDITOR_SHA='3da5b6a05436aa82d9ed94e9fcecdc9032ed04ecf890944313f384db3b572c1c'

TEST_SHA='63471f8706ccacd801de27314c37c30072d9806a8302df3f9cc7772807d0e930'

ACQ_HELPER_SHA='2a866bb7842111b7fdcb7a56e05cf77e8ce6a866277f4b808d7f761a6f7a8ae1'

ACQ_TEST_SHA='a1c38e14262a243ad5c90fe217e8a44a413c01f0b2cfdc73a6ba067a95cc1ca4'

TEST_NAMES={'schema': ('test_recognized_schema_quality_and_clock_counts_without_coordinate_values', 'test_source_bound_full_audit_copies_no_raw_values_and_executes_no_code', 'test_unknown_header_remains_uninterpreted_without_echoing_values[SECRET_OUTCOME\\t99\\n]', 'test_unknown_header_remains_uninterpreted_without_echoing_values[1\\tNaN\\t0:480\\t0:480\\t0\\t55\\t12\\n]', 'test_unknown_header_remains_uninterpreted_without_echoing_values[1\\t1e309\\t0:480\\t0:480\\t0\\t55\\t12\\n]', 'test_unknown_header_remains_uninterpreted_without_echoing_values[1\\t20\\tunknown\\t0:480\\t0\\t55\\t12\\n]', 'test_unknown_binary_header_is_inventory_only_not_unsafe_decode', 'test_malformed_missing_and_variable_rows_retained_without_repair[invalid_frame]', 'test_malformed_missing_and_variable_rows_retained_without_repair[invalid_count]', 'test_malformed_missing_and_variable_rows_retained_without_repair[variable_slots]', 'test_malformed_missing_and_variable_rows_retained_without_repair[empty_fields]', 'test_malformed_missing_and_variable_rows_retained_without_repair[bad_pair]', 'test_invalid_clock_row_does_not_create_false_adjacent_spacing_across_it', 'test_invalid_first_row_does_not_invent_initial_slot_roster', 'test_all_parser_resource_bounds_are_finite[line]', 'test_all_parser_resource_bounds_are_finite[bytes]', 'test_all_parser_resource_bounds_are_finite[rows]', 'test_all_parser_resource_bounds_are_finite[deadline]', 'test_changed_source_bytes_prevent_completed_audit[member]', 'test_changed_source_bytes_prevent_completed_audit[acquisition]', 'test_changed_source_bytes_prevent_completed_audit[index]', 'test_changed_source_bytes_prevent_completed_audit[mapping]', 'test_changed_source_bytes_prevent_completed_audit[writer]', 'test_changed_source_bytes_prevent_completed_audit[order]', 'test_dishonest_acquisition_provenance_refused[status]', 'test_dishonest_acquisition_provenance_refused[helper]', 'test_dishonest_acquisition_provenance_refused[provider_hash_claim]', 'test_dishonest_acquisition_provenance_refused[scientific_gate]', 'test_dishonest_acquisition_provenance_refused[selected]', 'test_dishonest_acquisition_provenance_refused[source_pins]', 'test_dishonest_acquisition_provenance_refused[selection]', 'test_mid_inspection_source_change_preserves_start_and_failure', 'test_matched_metadata_formulas_not_executed_or_exported_as_values', 'test_mapping_only_literal_name_relations_preserves_duplicates_no_gene_guess', 'test_existing_output_refused_without_overwrite', 'test_native_contract_refuses_all_gpu_assignments[0]', 'test_native_contract_refuses_all_gpu_assignments[1]', 'test_native_contract_refuses_all_gpu_assignments[0,1]', 'test_native_contract_refuses_all_gpu_assignments[gpu:1]', 'test_source_is_readonly_and_never_imports_or_executes_provider_code'), 'acquisition': ('test_complete_member_crc_and_first_acquisition_sha_truth', 'test_changed_or_unsupported_head_stops_before_any_get[delta0]', 'test_changed_or_unsupported_head_stops_before_any_get[delta1]', 'test_changed_or_unsupported_head_stops_before_any_get[delta2]', 'test_changed_or_unsupported_head_stops_before_any_get[delta3]', 'test_non206_body_not_read_or_retried[200]', 'test_non206_body_not_read_or_retried[302]', 'test_non206_body_not_read_or_retried[401]', 'test_non206_body_not_read_or_retried[403]', 'test_non206_body_not_read_or_retried[500]', 'test_changed_get_headers_stop_before_body[delta0]', 'test_changed_get_headers_stop_before_body[delta1]', 'test_changed_get_headers_stop_before_body[delta2]', 'test_changed_get_headers_stop_before_body[delta3]', 'test_redirected_url_refused_without_body', 'test_partial_range_and_error_preserved_no_promotion[short]', 'test_partial_range_and_error_preserved_no_promotion[long]', 'test_partial_range_and_error_preserved_no_promotion[timeout]', 'test_unsupported_or_disagreeing_local_header_refused[4-H-21]', 'test_unsupported_or_disagreeing_local_header_refused[6-H-1]', 'test_unsupported_or_disagreeing_local_header_refused[6-H-8]', 'test_unsupported_or_disagreeing_local_header_refused[6-H-2048]', 'test_unsupported_or_disagreeing_local_header_refused[8-H-0]', 'test_unsupported_or_disagreeing_local_header_refused[10-H-12]', 'test_unsupported_or_disagreeing_local_header_refused[12-H-23]', 'test_unsupported_or_disagreeing_local_header_refused[14-I-1]', 'test_unsupported_or_disagreeing_local_header_refused[18-I-1]', 'test_unsupported_or_disagreeing_local_header_refused[22-I-1]', 'test_unsupported_or_disagreeing_local_header_refused[26-H-0]', 'test_unsupported_or_disagreeing_local_header_refused[28-H-4097]', 'test_ntfs_extra_only_and_structural_bounds_refused[unknown]', 'test_ntfs_extra_only_and_structural_bounds_refused[zip64]', 'test_ntfs_extra_only_and_structural_bounds_refused[reserved]', 'test_ntfs_extra_only_and_structural_bounds_refused[tag]', 'test_ntfs_extra_only_and_structural_bounds_refused[length]', 'test_ntfs_extra_only_and_structural_bounds_refused[truncated]', 'test_local_filename_must_equal_frozen_selection', 'test_empty_local_extra_supported_without_inventing_central_local_equivalence', 'test_successor_magic_prevents_wrong_boundary_or_hidden_descriptor', 'test_no_supported_member_means_refusal_not_scope_or_size_fallback[flags-1]', 'test_no_supported_member_means_refusal_not_scope_or_size_fallback[flags-8]', 'test_no_supported_member_means_refusal_not_scope_or_size_fallback[flags-2048]', 'test_no_supported_member_means_refusal_not_scope_or_size_fallback[method-0]', 'test_no_supported_member_means_refusal_not_scope_or_size_fallback[external_attributes-2717843456]', 'test_no_supported_member_means_refusal_not_scope_or_size_fallback[external_attributes-293601280]', 'test_no_supported_member_means_refusal_not_scope_or_size_fallback[external_attributes-16]', 'test_no_supported_member_means_refusal_not_scope_or_size_fallback[external_attributes-8]', 'test_no_supported_member_means_refusal_not_scope_or_size_fallback[raw_bytes-67108865]', 'test_no_supported_member_means_refusal_not_scope_or_size_fallback[compressed_bytes-8388609]', 'test_no_supported_member_means_refusal_not_scope_or_size_fallback[extra_field_ids-value10]', 'test_index_integrity_contract_refuses_malformed_frozen_metadata[duplicate_name]', 'test_index_integrity_contract_refuses_malformed_frozen_metadata[duplicate_offset]', 'test_index_integrity_contract_refuses_malformed_frozen_metadata[roster]', 'test_index_integrity_contract_refuses_malformed_frozen_metadata[source_flag]', 'test_index_integrity_contract_refuses_malformed_frozen_metadata[mapping_path]', 'test_changed_source_bytes_refused_before_network[index]', 'test_changed_source_bytes_refused_before_network[mapping]', 'test_full_deflate_and_raw_bounds_cannot_be_weakened[trailing]', 'test_full_deflate_and_raw_bounds_cannot_be_weakened[incomplete]', 'test_full_deflate_and_raw_bounds_cannot_be_weakened[crc]', 'test_full_deflate_and_raw_bounds_cannot_be_weakened[raw_bound]', 'test_mid_acquisition_source_change_refused_before_promotion', 'test_candidate_change_after_verification_refused', 'test_existing_case_refused_no_ambiguity_replay', 'test_publication_collision_preserves_original_and_verified_candidate', 'test_outside_root_and_subordinate_symlink_refused', 'test_expired_inflation_deadline_refuses_without_parsing', 'test_gateway_source_has_no_track_parser_or_scientific_imports')}

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
    """Bind actual full100 source/JUnit evidence before installing its read guard."""
    path=Path(path)
    require(path==REGISTRY_Q and sha==REGISTRY_Q_SHA and job==REGISTRY_Q_JOB
        and path==path.resolve() and not path.is_symlink()
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
        and q.get('test_counts')=={'tests':100,'failures':0,'errors':0,'skipped':0},'Positive exact source-bound private100 registry qualification required')
    runpy.run_path(str(REGISTRY_Q_HELPER),run_name='vtl_registry_v4_q_source_validation')['verify_pins']()
    runpy.run_path(str(REGISTRY_CONTROLLER),run_name='vtl_registry_v4_q_roster_validation')['verify_operational_tests'](q)
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

def verify_private_junit(path,kind):
    names_expected=TEST_NAMES[kind];raw=Path(path).read_bytes()
    require(len(raw)<=8_000_000 and b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Bounded plain private JUnit required')
    cases=list(ET.fromstring(raw).iter('testcase'));names=[case.get('name') for case in cases]
    basename={'schema':'test_audit_vtl_track_schema_20261021','acquisition':'test_acquire_vtl_track_canary_20261021'}[kind]
    require(len(cases)==len(set(names))==len(names_expected) and set(names)==set(names_expected)
        and all(case.get('classname','').endswith(basename)
            and not any(case.find(key) is not None for key in ['failure','error','skipped']) for case in cases),
        'Actual complete private '+kind+' roster must pass without omissions/skips')
    from horsehair.cli import _qualification_junit
    require(_qualification_junit(path)=={'tests':len(names_expected),'failures':0,'errors':0,'skipped':0},'Full native private test counts differ')
    return canonical_hash(sorted(names))

def qualify_private(auditor,tests,acq_helper,acq_tests,job,end,*,runner=subprocess.run):
    groups=private_groups(auditor,tests,acq_helper,acq_tests)
    for group in groups.values():
        require(digest(group['module'])==group['module_sha'] and digest(group['tests'])==group['tests_sha'],'Pinned private source/tests changed')
    temporary=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temporary.is_absolute() and temporary.is_dir() and not temporary.is_symlink()
        and temporary.stat().st_uid==3164768 and shutil.disk_usage(temporary).free>=2_000_000_000
        and os.statvfs(temporary).f_favail>=2000,'Verified allocated node-local test capacity required')
    require(end-time.time()>1800,'Native margin insufficient for two bounded private suites and900-second source inventory')
    qualifications={}
    for kind,group in groups.items():
        workspace=temporary/('hhw-vtl-private-tests-'+job+'-'+kind);workspace.mkdir(mode=0o700,exist_ok=False)
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
        require(result.returncode==0,'Private '+kind+' tests failed before any actual track byte read')
        roster=verify_private_junit(junit,kind)
        require(digest(group['module'])==group['module_sha'] and digest(group['tests'])==group['tests_sha'],'Private bytes changed during qualification')
        value={'status':'complete_private_'+str(group['count'])+'_test_qualification','returncode':0,'native_job_id':job,
            'module_sha256':group['module_sha'],'tests_sha256':group['tests_sha'],
            'test_counts':{'tests':group['count'],'failures':0,'errors':0,'skipped':0},
            'junit':{'path':str(junit),'sha256':digest(junit)},'test_roster_sha256':roster,
            'actual_provider_track_bytes_read':False,**{key:False for key in FALSE_FLAGS}}
        path=SUPPORT/(kind+'-test-qualification.json');exclusive(path,value)
        qualifications[kind]={'path':str(path),'sha256':digest(path),'value':value}
    return qualifications


ACQUISITION=ROOT/'maintenance/study-af-vtl-whole-member-canary-v1-20261021/acquisition-result.json'
ACQUISITION_SHA='0c91c8a596402cc3ee7dc7abc4e8fe38d51e3453654fb3224b24e63b6d771808'
RAW_TRACK=ACQUISITION.with_name('2019-11-10_2_11-order.txt')
RAW_SHA='6587355772ae4d730bf9839ef2c1b60b9dbddbe852a4a7f24e611e39b0336996'
DOCUMENTATION={
    'index':(ROOT/'maintenance/study-af-vtl-directory-dataset01-v1-20261021/metadata-index.json','58209aca20832ec3897e30014393ace5dd25b186d4580c05d14921bb2f92a9e7'),
    'mapping':(ROOT/'maintenance/study-af-additional-public-metadata-v2-20261021/vtl_information.xlsx','b53bcc2242a6019f6556d217116c98d021878b6d4ed88c281f752d1eb555518d')}
CODE_PINS={'writer':('VTL-1.vtl.py','d83e7431c258977ba07e78b9f7430fe3e8a5dfb5b7569a5cfb9740e643f8d114'),
    'order':('VTL-2.dataorder.py','ff011406d399640da05b82c37256ea9d4a6f69f8b48e8c1ef93927ce43a3fe64')}
AUDIT_FALSE_FLAGS=('scientific_result','analysis_admitted','admission_authority','effects_inspected',
    'coordinate_values_exported','coordinate_numeric_values_aggregated','provider_code_executed',
    'physical_2d_units_qualified','absolute_or_exposure_clock_qualified','biological_identity_qualified',
    'per_slot_observation_mask_recoverable','ordered_track_imputation_qualified','data_repaired_or_imputed',
    'source_replication_passed','biological_transfer_supported','h1_h2_h3_gates_cleared',
    'representative_cohort_selected','gene_control_mapping_verified')


def private_groups(auditor,tests,acq_helper,acq_tests):
    return {'acquisition':{'module':Path(acq_helper),'module_sha':ACQ_HELPER_SHA,'tests':Path(acq_tests),'tests_sha':ACQ_TEST_SHA,
        'test_basename':'test_acquire_vtl_track_canary_20261021','count':68},
        'schema':{'module':Path(auditor),'module_sha':AUDITOR_SHA,'tests':Path(tests),'tests_sha':TEST_SHA,
        'test_basename':'test_audit_vtl_track_schema_20261021','count':40}}


def check_private_before_track_read(qualifications,job):
    require(set(qualifications)=={'acquisition','schema'},'Both full native suites must precede any track bytes')
    for kind,count,source,tests in [('acquisition',68,ACQ_HELPER_SHA,ACQ_TEST_SHA),('schema',40,AUDITOR_SHA,TEST_SHA)]:
        binding=qualifications[kind];path=SUPPORT/(kind+'-test-qualification.json')
        require(binding['path']==str(path) and digest(path)==binding['sha256'],'Exact durable native private qualification required')
        value=read_json(path)
        require(value==binding['value'] and value.get('status')=='complete_private_'+str(count)+'_test_qualification'
            and type(value.get('returncode')) is int and value.get('returncode')==0 and value.get('native_job_id')==job
            and value.get('module_sha256')==source and value.get('tests_sha256')==tests
            and value.get('test_counts')=={'tests':count,'failures':0,'errors':0,'skipped':0}
            and value.get('actual_provider_track_bytes_read') is False
            and all(value.get(key) is False for key in FALSE_FLAGS),'Exact complete positive native68/40 no-track-read qualification required')
        junit=SUPPORT/(kind+'-tests-junit.xml')
        require(value.get('junit')=={'path':str(junit),'sha256':digest(junit)}
            and verify_private_junit(junit,kind)==value.get('test_roster_sha256'),'Actual complete raw JUnit roster required before track read')


def run_source_audit(auditor,writer,order,end,qualifications,job,*,runner=subprocess.run):
    check_private_before_track_read(qualifications,job)
    require(end-time.time()>1080 and not CASE.exists(),'Fresh inventory and full finite audit/finalization margin required')
    # Every actual raw byte, including the first full hash, is read only inside
    # the hard-bounded child. Its pinned source verifies full size/SHA/CRC twice.
    require(digest(auditor)==AUDITOR_SHA and digest(ACQUISITION)==ACQUISITION_SHA,
        'Exact acquisition receipt/auditor source required')
    command=[sys.executable,'-s','-u',str(auditor),'--acquisition',str(ACQUISITION)]
    for key,(path,sha) in DOCUMENTATION.items():
        require(digest(path)==sha,'Verified directory/workbook changed');command.extend(['--'+key,str(path)])
    for key,path in [('writer',writer),('order',order)]:
        require(Path(path).name==CODE_PINS[key][0] and digest(path)==CODE_PINS[key][1],'Exact official code documentation required')
        command.extend(['--'+key,str(path)])
    command.extend(['--output',str(CASE),'--max-seconds','900'])
    exclusive(SUPPORT/'source-audit-intent.json',{'command':command,'acquisition_sha256':ACQUISITION_SHA,
        'raw_track_sha256':RAW_SHA,'first_acquisition_sha256_not_provider_checksum':True,
        'auditor_sha256':AUDITOR_SHA,'complete_private_suites_before_first_track_read':True,**{key:False for key in FALSE_FLAGS}})
    with (SUPPORT/'source-audit-stdout.txt').open('xb') as out,(SUPPORT/'source-audit-stderr.txt').open('xb') as err:
        completed=runner(command,cwd=FRAMEWORK,stdout=out,stderr=err,timeout=min(1080,end-time.time()-180),check=False,
            env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'))
    require(completed.returncode==0,'Actual structural audit failed; synthetic qualification stays separate')
    value=read_json(CASE/'receipt.json')
    require(value.get('status')=='complete_bounded_outcome_blind_vtl_schema_inventory'
        and value.get('auditor_sha256')==AUDITOR_SHA and value.get('acquisition')=={'path':str(ACQUISITION),'sha256':ACQUISITION_SHA}
        and value.get('member')=={'path':str(RAW_TRACK),'bytes':30121971,'crc32':'abefabb9','sha256':RAW_SHA}
        and value.get('all_source_bytes_reverified') is True and value.get('unknown_schema_does_not_clear_scientific_gates') is True
        and value.get('cryptographic_provider_member_checksum_available') is False
        and value.get('first_acquisition_sha256_not_provider_checksum') is True
        and all(value.get(key) is False for key in AUDIT_FALSE_FLAGS),'Actual full source-bound inventory must retain unknown scientific gates')
    require(value.get('native',{}).get('job_id')==job and value.get('native',{}).get('cpus')==1
        and value.get('native',{}).get('gpus')==0,'Actual auditor must bind this same CPU-only native allocation')
    expected_pins={'acquisition':(ACQUISITION,ACQUISITION_SHA),**DOCUMENTATION,
        'writer':(Path(writer),CODE_PINS['writer'][1]),'order':(Path(order),CODE_PINS['order'][1])}
    require({p['role']:(p['path'],p['sha256']) for p in value.get('source_pins',[])}==
        {key:(str(path),sha) for key,(path,sha) in expected_pins.items()},'Actual inventory must bind every exact documentation source')
    inventory=value.get('inventory',{});path=CASE/'schema-and-clock-inventory.json'
    require(inventory=={'path':str(path),'sha256':digest(path)},'Exact actual inventory bytes required')
    detail=read_json(path)
    require(detail.get('status') in {'recognized_writer_shape_inventory','unknown_header_schema_inventory'}
        and detail.get('bytes_read')==30121971 and all(detail.get(key) is False for key in AUDIT_FALSE_FLAGS)
        and detail.get('frame_index_metadata',{}).get('expected_grid_missingness_qualified') is False
        and detail.get('frame_index_metadata',{}).get('seconds_or_exposure_times_derived') is False,
        'Honest bounded complete structural/unknown-schema inventory required')
    return value


def ordinary_source(path,sha):
    path=Path(path)
    require(path.is_absolute() and path==path.resolve() and path.is_relative_to(ROOT/'maintenance')
        and all(not p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(ROOT))
        and path.is_file() and digest(path)==sha,'Exact pinned ordinary source path required')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('auditor','tests','acquisition-helper','acquisition-tests','writer','order','native-script','registry-qualification'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--registry-qualification-sha256',required=True);p.add_argument('--registry-qualification-job-id',required=True)
    args=p.parse_args();job=str(native_job_id() or '');own_sha=digest(Path(__file__));os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='1' and os.environ.get('HHW_GPU_COUNT')=='0'
        and os.environ.get('HHW_MAX_HOURS')=='1','Owned finite1CPU native context required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval required')
    import horsehair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,
        'Exact qualified4f and immutable primary required')
    config=read_json(PRIMARY/'configs/study.json')
    require(canonical_hash(config)==CONFIG and read_json(FRAMEWORK/'configs/study.json')==config,'Full unchanged config required')
    require(not SUPPORT.exists() and not CASE.exists(),'Fresh result/support; no replay')
    modules=[args.auditor,args.tests,args.acquisition_helper,args.acquisition_tests]
    require([path.name for path in modules]==['audit-vtl-track-schema-20261021.py','test_audit_vtl_track_schema_20261021.py',
        'acquire-vtl-track-canary-20261021.py','test_acquire_vtl_track_canary_20261021.py']
        and len({path.parent for path in modules})==1,'Complete four-file colocated native test roster required')
    for path,sha in [(args.auditor,AUDITOR_SHA),(args.tests,TEST_SHA),(args.acquisition_helper,ACQ_HELPER_SHA),(args.acquisition_tests,ACQ_TEST_SHA),
        (args.writer,CODE_PINS['writer'][1]),(args.order,CODE_PINS['order'][1]),(ACQUISITION,ACQUISITION_SHA),*DOCUMENTATION.values(),
        (GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(QUOTA,QUOTA_SHA),(QUALIFICATION,QUALIFICATION_SHA)]:ordinary_source(path,sha)
    # No raw-track read/hash belongs in this prequalification roster.
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config);SUPPORT.mkdir(mode=0o700)
    runpy.run_path(str(GUARD))['install'](SUPPORT/'accounting-reads')
    allocation=native_allocation(job,args.native_script,runpy.run_path(str(SCHEDULER))['run_scontrol_job']);end=allocation['allocation_end_unix']
    registry,installed=qualified_registry_read_guard(args.registry_qualification,args.registry_qualification_sha256,args.registry_qualification_job_id,end)
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'Guard358 must precede campaign native binding')
    campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],SUPPORT,'vtl-schema-baseline',resource_policy=policy,cpus=0,gpus=0,hours=1)
    qjob=str(read_json(QUALIFICATION)['slurm_job_id']);observed=repair._repair_native_states(sorted({qjob,registry['job_id']}),{},owner='pwa209',account='def-ptewarie_cpu')
    public_q=qualification_proof(QUALIFICATION,QUALIFICATION_SHA,qjob,observed);private_q=completed_registry_qualification(registry,observed)
    with resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        quota=runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,required_bytes=10_000_000_000,
            reserve_files=248000,reserve_bytes=2_000_000_000_000,evidence_dir=SUPPORT/'quota-reads',allocation_deadline_unix=end)
        try:storage=storage_proof(config,quota)
        except PendingStorage as error:
            exclusive(SUPPORT/'receipt.json',{'status':'pending_storage_no_tests_or_track_read_started','reason':str(error),
                'native_job_id':job,**{key:False for key in FALSE_FLAGS}});return 0
        exclusive(SUPPORT/'native-bindings.json',{'allocation':allocation,'public_qualification':public_q,'registry_qualification':private_q,
            'registry_installation':installed,'storage':storage,'source_sha256':SOURCE,'full_config_sha256':CONFIG,
            'resource_policy':policy.receipt(),'wrapper_sha256':own_sha,'raw_track_read_before_qualification':False})
        qualified=qualify_private(args.auditor,args.tests,args.acquisition_helper,args.acquisition_tests,job,end)
        result=run_source_audit(args.auditor,args.writer,args.order,end,qualified,job)
        require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA
            and digest(POLICY)==POLICY_SHA and digest(Path(__file__))==own_sha,'Source/policy/wrapper changed during audit')
        receipt={'status':'complete_native_bounded_vtl_structural_inventory','native_job_id':job,'allocation':allocation,
            'source_sha256':SOURCE,'full_config_sha256':CONFIG,'resource_policy':policy.receipt(),
            'private_qualifications':{key:{k:v for k,v in item.items() if k!='value'} for key,item in qualified.items()},
            'actual_source_inventory':{'path':str(CASE/'receipt.json'),'sha256':digest(CASE/'receipt.json'),'inventory':result['inventory']},
            **{key:False for key in FALSE_FLAGS}}
        exclusive(SUPPORT/'receipt.json',receipt);print(json.dumps(receipt,sort_keys=True));return 0


if __name__=='__main__':raise SystemExit(main())
