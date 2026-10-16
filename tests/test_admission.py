import json
import os
import shutil
from pathlib import Path
from datetime import datetime, timezone

import pytest

from horsehair.admission import AdmissionError, assess_admission, memory_bytes, parse_sacct
from horsehair.acquire import asset, atomic_json, canonical_hash as source_hash
from horsehair.core import canonical_hash, digest, source_identity


def test_native_sacct_joins_completed_allocation_and_batch_peak():
    jobs = parse_sacct("JobIDRaw|State|ElapsedRaw|AllocCPUS|MaxRSS|AllocTRES\n123|COMPLETED|120|6||cpu=6,gres/gpu=1,gres/gpu:a100=1\n123.batch|COMPLETED|119|6|2048M|cpu=6\n")
    assert jobs["123"]["gpus"] == 1
    assert jobs["123"]["peak_rss_bytes"] == 2048 * 1024**2
    assert memory_bytes("4G") == 4 * 1024**3
    with pytest.raises(AdmissionError):
        memory_bytes("")


def test_native_sacct_maps_array_allocation_and_step_to_logical_id():
    jobs=parse_sacct('JobID|JobIDRaw|State|ElapsedRaw|AllocCPUS|MaxRSS|AllocTRES|ExitCode\n800_0|901|COMPLETED|120|6||cpu=6,gres/gpu=1|0:0\n800_0.batch|901.batch|COMPLETED|119|6|2G|cpu=6|0:0\n800_1|800|FAILED|10|4|1G|cpu=4|1:0\n')
    assert set(jobs)=={'800_0','800_1'}
    assert jobs['800_0']['job_id_raw']=='901' and jobs['800_0']['peak_rss_bytes']==2*1024**3
    assert jobs['800_1']['state']=='FAILED'
    with pytest.raises(AdmissionError,match='conflicting logical'):
        parse_sacct('JobID|JobIDRaw|State|ElapsedRaw|AllocCPUS|MaxRSS|AllocTRES\n800_0|901|COMPLETED|120|6|2G|cpu=6\n800_1|901|COMPLETED|120|6|2G|cpu=6\n')


def config_for(root):
    return {"resources": {"cpu_ceiling": 64, "cpu_core_hours_cap": 20000, "gpu_hours_cap": 400, "scratch_budget_bytes": 100_000_000_000, "scratch_budget_files": 250000, "scratch_reserve_bytes": 2_000_000_000, "rna_cores": 8, "rna_concurrency": 2}, "molecular": {}, "paths": {"root": str(root)}}


def test_missing_native_measurements_remain_pending_without_numeric_forecast(tmp_path):
    result = assess_admission(config_for(tmp_path), tmp_path, "rna-full")
    assert result["rna-full"]["status"] == "pending"
    assert "projected_cpu_core_hours" not in result["rna-full"]
    assert "native sacct" in result["rna-full"]["reasons"][0]


def measured_rna_fixture(root):
    config = config_for(root)
    rows = [{"biological_id": f"brain{i}", "read_count": 10000+i*100, "included": True} for i in range(60)]
    manifest = {"schema_version": 1, "sources": {"ant_rna": {"crosswalk": rows}}, "assets": [asset(source="fixture", name="fixture", url="https://example.org/fixture", version="test", rights="synthetic")], "status": "synthetic_test_only"}
    manifest["manifest_hash"] = source_hash(manifest)
    atomic_json(root / "manifests" / "source_manifest.json", manifest)
    identity = source_identity(Path(__file__).resolve().parents[1])["sha256"]
    for index, brain in enumerate([0, 47]):
        atomic_json(root / "state" / f"rna-pilot-{index}.json", {"status": "pilot_processed", "config_sha256": canonical_hash(config), "source_sha256": identity, "slurm_job_id": str(index+1), "biological_id": f"brain{brain}", "read_count": rows[brain]["read_count"], "elapsed_seconds": 100+brain, "working_bytes": 1000000+brain*10000, "working_files": 12, "qc": {"status": "passed"}, "source_manifest_hash": manifest["manifest_hash"]})
    (root / "state" / "native-accounting.tsv").write_text("JobIDRaw|State|ElapsedRaw|AllocCPUS|MaxRSS|AllocTRES\n1|COMPLETED|100|8||cpu=8\n1.batch|COMPLETED|100|8|2048M|cpu=8\n2|COMPLETED|147|8||cpu=8\n2.batch|COMPLETED|147|8|3072M|cpu=8\n")
    atomic_json(root / "state" / "resource-budget.json", {"spent_cpu_core_hours": 1., "spent_gpu_hours": .2, "leases": {}})
    quota = root / "state" / "quota-native.txt"
    quota.write_text("Synthetic quota fixture; no actual cluster result\n")
    atomic_json(root / "state" / "storage-budget.json", {"status": "measured", "captured_utc": datetime.now(timezone.utc).isoformat(), "root": str(root), "study_bytes": 10000000, "study_files": 100, "personal_available_bytes": 100_000_000_000, "personal_available_files": 100000, "native_source_paths": [str(quota.resolve())], "native_source_hashes": {str(quota.resolve()): digest(quota)}})
    return config


def test_rna_forecast_uses_all60_depths_actual_wall_rss_and_quota(tmp_path):
    config = measured_rna_fixture(tmp_path)
    result = assess_admission(config, tmp_path, "rna-full")["rna-full"]
    assert result["status"] == "admitted"
    assert result["workload"]["biological_brains"] == 60
    assert result["workload"]["remaining_brains"] == 60
    assert result["projected_gpu_hours"] == .2
    assert result["allocation_parameters"]["rna-full"] == {"cpus": 8, "memory": "4G", "hours": 1, "gpu": False, "concurrency": 2}
    assert all(Path(path).is_file() and result["source_result_hashes"][path] == digest(Path(path)) for path in result["source_result_paths"])
    assert result["projected_scratch_bytes"] > 10_000_000


def test_measured_forecast_does_not_admit_over_budget_or_changed_qc(tmp_path):
    config = measured_rna_fixture(tmp_path)
    config["resources"]["cpu_core_hours_cap"] = .1
    for path in (tmp_path / "state").glob("rna-pilot-*.json"):
        record = json.loads(path.read_text())
        record["config_sha256"] = canonical_hash(config)
        atomic_json(path, record)
    result = assess_admission(config, tmp_path, "rna-full")["rna-full"]
    assert result["status"] == "rejected"
    path = tmp_path / "state" / "rna-pilot-1.json"
    record = json.loads(path.read_text())
    record["qc"]["status"] = "review_required"
    atomic_json(path, record)
    result = assess_admission(config, tmp_path, "rna-full")["rna-full"]
    assert result["status"] == "pending" and "QC-passing" in result["reasons"][0]


def primary_rna_release_fixture(tmp_path, runtime_dependency=False):
    """Copy actual source into a distinct release; all data/native facts are QA fixtures."""
    from horsehair import core
    root, release = tmp_path/'study', tmp_path/'primary'
    config = measured_rna_fixture(root)
    repository = Path(core.__file__).resolve().parents[2]
    study = json.loads((repository/'configs/study.json').read_text())
    config.update(schema_version=1, agents=study['agents'], inference=study['inference'])
    config['resources']['gpu_concurrency'] = 1
    config['paths'].update(cpu_account='fixture-cpu', gpu_account='fixture-gpu', environment='/fixture/env')
    atomic_json(release/'configs/study.json', config)
    package = release/'src/horsehair';package.mkdir(parents=True)
    for name in ('__init__.py','core.py','admission.py','acquire.py','slurm.py','agents.py','pipeline.py'):
        shutil.copyfile(repository/'src/horsehair'/name, package/name)
    if runtime_dependency:
        admission=package/'admission.py'
        admission.write_text(admission.read_text(encoding='utf8')+'\nimport hhw_runtime_dependency_fixture\n',encoding='utf8')
    identity = source_identity(release)
    atomic_json(release/'release.json', {'release_id':'qa-primary','source_identity':identity,'config_sha256':canonical_hash(config)})
    for path in (root/'state').glob('rna-pilot-*.json'):
        record=json.loads(path.read_text());record.update(config_sha256=canonical_hash(config),source_sha256=identity['sha256'])
        atomic_json(path,record)
    tasks=[{'name':name,'resources':{'cpus':1,'hours':1,'gpu':False}} for name in ('rna-reference','molecular-pilot')]
    plan={'phase':'managed-initial','config_sha256':canonical_hash(config),'release_identity':identity,'tasks':tasks}
    atomic_json(root/'plans/qa-primary-managed-initial/plan.json',plan)
    atomic_json(root/'state/qa-primary-managed-initial-submission.json', {'plan_sha256':canonical_hash(plan),
        'tasks':{task['name']:{'state':'submitted','job_id':str(70+index)} for index,task in enumerate(tasks)}})
    budget=json.loads((root/'state/resource-budget.json').read_text())
    budget['native_jobs']={str(index):{'state':'COMPLETED','exit_code':'0:0','elapsed_seconds':60,
        'cpus':1,'gpus':0,'basis':'sacct final allocated wall time'} for index in (70,71)}
    atomic_json(root/'state/resource-budget.json',budget)
    return config,root,release,identity


def test_maintenance_context_runs_actual_primary_admission_and_planner_without_source_override(tmp_path, monkeypatch):
    from horsehair import pipeline
    config,root,release,identity=primary_rna_release_fixture(tmp_path)
    # The framework's actual reader correctly refuses primary-bound measurements.
    framework=assess_admission(config,root,'rna-full')['rna-full']
    assert framework['status']=='pending' and 'immutable source release' in framework['reasons'][0]
    framework_path=str(Path(__file__).resolve().parents[1]/'src')
    monkeypatch.setenv('PYTHONPATH', os.pathsep.join(filter(None,[framework_path,os.environ.get('PYTHONPATH','')])))
    result=pipeline._primary_execution(config,root,release,'admission','rna-full')['rna-full']
    assert result['status']=='admitted' and result['source_sha256']==identity['sha256']
    destination=root/'plans/qa-primary-rna-full'
    planned=pipeline._primary_execution(config,root,release,'plan','rna-full',destination)
    assert planned['release_identity']==identity and planned['config_sha256']==canonical_hash(config)
    counts=next(task for task in planned['tasks'] if task['name']=='rna-counts')
    assert counts['resources']['array']=='0-59%2' and counts['resources']['hours']==1
    assert str(release) in Path(counts['script']).read_text()
    receipt=json.loads((root/'state/pipeline/qa-primary/primary-context/rna-full-plan.json').read_text())
    assert receipt['module_path']==str(release/'src/horsehair/slurm.py') and receipt['receipt_sha256']==digest(destination/'plan.json')


def test_primary_context_preserves_qualified_pythonpath_only_dependency(tmp_path,monkeypatch):
    from horsehair import pipeline
    config,root,release,identity=primary_rna_release_fixture(tmp_path,runtime_dependency=True)
    runtime=tmp_path/'qualified-runtime';runtime.mkdir()
    (runtime/'hhw_runtime_dependency_fixture.py').write_text('# Generated dependency-path QA fixture\n')
    framework=str(Path(__file__).resolve().parents[1]/'src')
    monkeypatch.setenv('PYTHONPATH',os.pathsep.join(filter(None,[framework,str(runtime),os.environ.get('PYTHONPATH','')])))
    result=pipeline._primary_execution(config,root,release,'admission','rna-full')['rna-full']
    assert result['status']=='admitted' and result['source_sha256']==identity['sha256']
    receipt=json.loads((root/'state/pipeline/qa-primary/primary-context/rna-full-admission.json').read_text())
    assert receipt['module_path']==str(release/'src/horsehair/admission.py')


def test_primary_context_preserves_real_agent_benchmark_and_hardware_guards(tmp_path):
    from horsehair import agents,pipeline
    config,root,release,identity=primary_rna_release_fixture(tmp_path)
    evidence=root/'state/fixture-input.json';atomic_json(evidence,{'evidence_class':'generated software fixture'})
    path=agents._directory(root,config['agents'])/'benchmark-training/foraging_rnn_900/training_benchmark.json'
    record={'benchmark_kind':'training','full_config_sha256':canonical_hash(config),'source_sha256':identity['sha256'],
        'benchmark_specification':agents.benchmark_specification(config),'task':'foraging','architecture':'rnn','seed':900,
        'slurm_job_id':'1','device':'cpu','source_result_paths':[str(evidence.resolve())],
        'source_result_hashes':{str(evidence.resolve()):digest(evidence)}}
    atomic_json(path,record)
    result=pipeline._primary_execution(config,root,release,'admission','agents-final')['agents-final']
    assert result['source_sha256']==identity['sha256'] and result['status']=='pending'
    assert 'single allocated CUDA device' in result['reasons'][0]
    # A changed scientific configuration/specification still cannot pass the
    # immutable primary checker merely because the controller knows its SHA.
    record['benchmark_specification']['training_steps']+=1;atomic_json(path,record)
    result=pipeline._primary_execution(config,root,release,'admission','agents-final')['agents-final']
    assert result['status']=='pending' and 'configuration/source/specification changed' in result['reasons'][0]


@pytest.mark.parametrize('fault',['source','configuration','root'])
def test_primary_context_rejects_drift_before_writing_any_admission(tmp_path,fault):
    from horsehair import pipeline
    config,root,release,_=primary_rna_release_fixture(tmp_path)
    if fault=='source':(release/'src/horsehair/agents.py').write_text('# Changed generated fixture\n')
    elif fault=='configuration':config['resources']['cpu_ceiling']-=1
    else:config['paths']['root']=str(root.parent)
    with pytest.raises(ValueError,match='Primary execution'):
        pipeline._primary_execution(config,root,release,'admission','rna-full')
    assert not (root/'state/admission-rna-full.json').exists()


def test_primary_child_failure_never_reuses_stale_positive_admission(tmp_path,monkeypatch):
    from types import SimpleNamespace
    from horsehair import pipeline
    config,root,release,identity=primary_rna_release_fixture(tmp_path)
    path=root/'state/admission-rna-full.json'
    atomic_json(path,{'phase':'rna-full','status':'admitted','config_sha256':canonical_hash(config),
                      'source_sha256':identity['sha256'],'evidence_class':'generated stale QA receipt'})
    original=path.read_bytes()
    def failed(command,**kwargs):
        assert command[:2]==[pipeline.sys.executable,'-s']
        assert kwargs['cwd']==release and 0 < kwargs['timeout'] <= 900
        assert kwargs['env']['PYTHONNOUSERSITE']=='1'
        return SimpleNamespace(returncode=1,stdout='',stderr='Generated child failure')
    monkeypatch.setattr(pipeline.subprocess,'run',failed)
    with pytest.raises(RuntimeError,match='no framework fallback'):
        pipeline._primary_execution(config,root,release,'admission','rna-full')
    assert path.read_bytes()==original
    assert not (root/'state/pipeline/qa-primary/primary-context/rna-full-admission.json').exists()


def primary_child_fault_injection(monkeypatch, injections):
    """Run actual isolated interpreters; inject faults only in the generated QA child."""
    import textwrap
    from horsehair import pipeline
    original = pipeline.subprocess.run
    children, sleeps = [], []
    def run(command, **kwargs):
        index = len(children)
        injection = injections[min(index, len(injections)-1)]
        altered = list(command)
        if injection:
            marker = "    stage='operation'\n"
            assert altered[-1].count(marker) == 1
            altered[-1] = altered[-1].replace(marker, marker+textwrap.indent(injection, '    ')+'\n', 1)
        result = original(altered, **kwargs)
        children.append(result)
        return result
    monkeypatch.setattr(pipeline.subprocess, 'run', run)
    monkeypatch.setattr(pipeline.time, 'sleep', sleeps.append)
    return children, sleeps


@pytest.mark.parametrize('number', [108, 116])
def test_primary_filesystem_read_recovers_in_fresh_primary_child_with_durable_evidence(tmp_path, monkeypatch, number):
    from horsehair import pipeline
    config, root, release, identity = primary_rna_release_fixture(tmp_path)
    fault = f"""original_read=module.read_json
def faulted_read(path):
    if Path(path).name=='resource-budget.json':
        raise OSError({number},'Generated transient filesystem read fixture',str(path))
    return original_read(path)
module.read_json=faulted_read"""
    children, sleeps = primary_child_fault_injection(monkeypatch, [fault, None])
    result = pipeline._primary_execution(config, root, release, 'admission', 'rna-full')['rna-full']
    assert result['status'] == 'admitted' and result['source_sha256'] == identity['sha256']
    assert len(children) == 2 and children[0].returncode != 0 and children[1].returncode == 0 and sleeps == [1]
    assert source_identity(release) == identity
    context = json.loads((root/'state/pipeline/qa-primary/primary-context/rna-full-admission.json').read_text())
    proof = context['filesystem_read_recovery']
    recovery = json.loads(Path(proof['path']).read_text())
    assert proof['sha256'] == digest(Path(proof['path'])) and proof['failed_attempts'] == 1
    assert recovery['status'] == 'recovered' and recovery['successful_attempt'] == 2
    error = recovery['attempts'][0]['error']
    assert error['errno'] == number and error['filename'] == str(root/'state/resource-budget.json')
    assert error['mutation_attempted'] is error['process_attempted'] is False


def test_primary_read_retry_exhaustion_preserves_stale_admission_and_last_failure(tmp_path, monkeypatch):
    from horsehair import pipeline
    config, root, release, identity = primary_rna_release_fixture(tmp_path)
    stale = root/'state/admission-rna-full.json'
    atomic_json(stale, {'status':'admitted', 'evidence_class':'generated stale QA receipt'})
    before = stale.read_bytes()
    fault = "raise OSError(108,'Generated persistent read outage',str(Path(request['root'])/'state/resource-budget.json'))"
    children, sleeps = primary_child_fault_injection(monkeypatch, [fault])
    with pytest.raises(RuntimeError, match='no framework fallback'):
        pipeline._primary_execution(config, root, release, 'admission', 'rna-full')
    assert len(children) == 3 and sleeps == [1, 2] and stale.read_bytes() == before
    reports = list((root/'state/pipeline/qa-primary/primary-context/read-recovery').glob('*.json'))
    report = json.loads(reports[0].read_text())
    assert len(reports) == 1 and report['status'] == 'failed' and len(report['attempts']) == 3
    assert report['attempts'][-1]['retry_scheduled'] is False
    assert report['attempts'][-1]['error']['errno'] == 108
    assert not (root/'state/pipeline/qa-primary/primary-context/rna-full-admission.json').exists()


@pytest.mark.parametrize('action', ['admission', 'plan'])
def test_primary_write_failure_or_partial_plan_is_never_replayed(tmp_path, monkeypatch, action):
    from horsehair import pipeline
    config, root, release, _ = primary_rna_release_fixture(tmp_path)
    directory = root/'plans/qa-primary-rna-full'
    if action == 'plan':
        mutation = "Path(request['directory']).mkdir()"
    else:
        mutation = "(Path(request['root'])/'state/generated-partial-receipt').write_text('retained QA partial')"
    fault = mutation+"\nraise OSError(108,'Generated read failure after mutation',str(Path(request['root'])/'state/resource-budget.json'))"
    children, sleeps = primary_child_fault_injection(monkeypatch, [fault])
    with pytest.raises(RuntimeError, match='no framework fallback'):
        pipeline._primary_execution(config, root, release, action, 'rna-full', directory if action == 'plan' else None)
    assert len(children) == 1 and not sleeps
    report = json.loads(next((root/'state/pipeline/qa-primary/primary-context/read-recovery').glob('*.json')).read_text())
    assert report['status'] == 'failed' and report['attempts'][0]['error']['mutation_attempted'] is True
    assert report['attempts'][0]['retry_scheduled'] is False
    assert directory.is_dir() if action == 'plan' else (root/'state/generated-partial-receipt').is_file()


def test_primary_planner_can_retry_read_before_creating_any_output(tmp_path, monkeypatch):
    from horsehair import pipeline
    config, root, release, identity = primary_rna_release_fixture(tmp_path)
    pipeline._primary_execution(config, root, release, 'admission', 'rna-full')
    destination = root/'plans/qa-primary-rna-full'
    fault = "raise OSError(108,'Generated planner read outage',str(Path(request['root'])/'state/resource-budget.json'))"
    children, sleeps = primary_child_fault_injection(monkeypatch, [fault, None])
    result = pipeline._primary_execution(config, root, release, 'plan', 'rna-full', destination)
    assert result['release_identity'] == identity and len(children) == 2 and sleeps == [1]
    assert (destination/'plan.json').is_file()
    assert not (root/'state/qa-primary-rna-full-submission.json').exists()


def test_primary_context_blocks_subprocess_before_any_scheduler_or_external_action(tmp_path, monkeypatch):
    from horsehair import pipeline
    config, root, release, _ = primary_rna_release_fixture(tmp_path)
    sentinel = root/'state/generated-subprocess-sentinel'
    code = 'from pathlib import Path;Path('+repr(str(sentinel))+').write_text("must never execute")'
    fault = 'import subprocess\nsubprocess.run([sys.executable,"-c",'+repr(code)+'],check=True)'
    children, sleeps = primary_child_fault_injection(monkeypatch, [fault])
    with pytest.raises(RuntimeError, match='cannot execute subprocess or scheduler commands'):
        pipeline._primary_execution(config, root, release, 'admission', 'rna-full')
    assert len(children) == 1 and not sleeps and not sentinel.exists()


def test_primary_permanent_read_error_does_not_retry(tmp_path, monkeypatch):
    from horsehair import pipeline
    config, root, release, _ = primary_rna_release_fixture(tmp_path)
    fault = "raise PermissionError(13,'Generated permanent permissions error',str(Path(request['root'])/'state/resource-budget.json'))"
    children, sleeps = primary_child_fault_injection(monkeypatch, [fault])
    with pytest.raises(RuntimeError, match='no framework fallback'):
        pipeline._primary_execution(config, root, release, 'admission', 'rna-full')
    assert len(children) == 1 and not sleeps


@pytest.mark.parametrize('fault', ['foreign_nonce', 'foreign_path', 'duplicate_envelope'])
def test_primary_read_recovery_rejects_unbound_or_ambiguous_error_evidence(tmp_path, monkeypatch, fault):
    from horsehair import pipeline
    config, root, release, _ = primary_rna_release_fixture(tmp_path)
    injection = "filename=str(Path(request['root'])/'state/resource-budget.json')\n"
    if fault == 'foreign_nonce':
        injection += "request['nonce']='foreign-fixture-context'\n"
    elif fault == 'foreign_path':
        injection += "filename=str(release.parent/'foreign-file.json')\n"
    else:
        injection += "print('HHW_PRIMARY_CONTEXT_IO_ERROR {}',flush=True)\n"
    injection += "raise OSError(108,'Generated unbound failure',filename)"
    children, sleeps = primary_child_fault_injection(monkeypatch, [injection])
    with pytest.raises(RuntimeError, match='no framework fallback'):
        pipeline._primary_execution(config, root, release, 'admission', 'rna-full')
    assert len(children) == 1 and not sleeps
    report = json.loads(next((root/'state/pipeline/qa-primary/primary-context/read-recovery').glob('*.json')).read_text())
    assert report['status'] == 'failed' and report['attempts'][0]['bound_transient_read_failure'] is False


def test_primary_read_retry_rechecks_primary_identity_before_new_process(tmp_path, monkeypatch):
    from horsehair import pipeline
    config, root, release, _ = primary_rna_release_fixture(tmp_path)
    fault = "raise OSError(108,'Generated read outage',str(Path(request['root'])/'state/resource-budget.json'))"
    children, sleeps = primary_child_fault_injection(monkeypatch, [fault])
    injected = pipeline.subprocess.run
    def drift(command, **kwargs):
        response = injected(command, **kwargs)
        path = release/'src/horsehair/agents.py'
        path.write_text(path.read_text()+'\n# Generated source-drift QA fixture\n')
        return response
    monkeypatch.setattr(pipeline.subprocess, 'run', drift)
    with pytest.raises(ValueError, match='source/configuration changed during read recovery'):
        pipeline._primary_execution(config, root, release, 'admission', 'rna-full')
    assert len(children) == 1 and not sleeps
    report = json.loads(next((root/'state/pipeline/qa-primary/primary-context/read-recovery').glob('*.json')).read_text())
    assert report['status'] == 'source_changed' and report['attempts'][0]['retry_scheduled'] is False


def test_primary_child_deadline_never_retries_even_with_structured_read_error(tmp_path, monkeypatch):
    from horsehair import pipeline
    config, root, release, _ = primary_rna_release_fixture(tmp_path)
    calls = []
    def timeout(command, **kwargs):
        calls.append(command)
        request = json.loads(kwargs['input'])
        error = {key:request[key] for key in ('nonce','action','phase','source_identity','config_sha256')}
        error.update(errno=108, filename=str(root/'state/resource-budget.json'), mutation_attempted=False,
                     process_attempted=False, stage='operation', error_type='OSError', message='Generated read error')
        output = ('HHW_PRIMARY_CONTEXT_IO_ERROR '+json.dumps(error)+'\n').encode()
        raise pipeline.subprocess.TimeoutExpired(command, 900, output=output, stderr=b'Generated timeout')
    monkeypatch.setattr(pipeline.subprocess, 'run', timeout)
    monkeypatch.setattr(pipeline.time, 'sleep', lambda _:pytest.fail('A child timeout must not retry'))
    with pytest.raises(RuntimeError, match='Primary child deadline expired'):
        pipeline._primary_execution(config, root, release, 'admission', 'rna-full')
    assert len(calls) == 1
    report = json.loads(next((root/'state/pipeline/qa-primary/primary-context/read-recovery').glob('*.json')).read_text())
    assert report['status'] == 'failed' and report['attempts'][0]['timed_out'] is True
    assert report['attempts'][0]['retry_scheduled'] is False


@pytest.mark.parametrize('exhaustion_point', ['revalidation', 'backoff', 'before_spawn'])
def test_primary_read_recovery_never_spawns_after_deadline(tmp_path, monkeypatch, exhaustion_point):
    from horsehair import pipeline
    config, root, release, _ = primary_rna_release_fixture(tmp_path)
    fault = "raise OSError(108,'Generated read outage',str(Path(request['root'])/'state/resource-budget.json'))"
    children, _ = primary_child_fault_injection(monkeypatch, [fault])
    clock = [0.0]
    monotonic_calls = [0]
    def monotonic():
        monotonic_calls[0] += 1
        if exhaustion_point == 'before_spawn' and monotonic_calls[0] == 2:
            clock[0] = 901.0
        return clock[0]
    monkeypatch.setattr(pipeline.time, 'monotonic', monotonic)
    original_identity = pipeline.source_identity
    def identity(path):
        result = original_identity(path)
        if exhaustion_point == 'revalidation' and children:
            clock[0] = 901.0
        return result
    monkeypatch.setattr(pipeline, 'source_identity', identity)
    def sleep(_):
        if exhaustion_point == 'backoff':
            clock[0] = 901.0
    monkeypatch.setattr(pipeline.time, 'sleep', sleep)
    with pytest.raises(RuntimeError, match='deadline exhausted'):
        pipeline._primary_execution(config, root, release, 'admission', 'rna-full')
    assert len(children) == (0 if exhaustion_point == 'before_spawn' else 1)
    report = json.loads(next((root/'state/pipeline/qa-primary/primary-context/read-recovery').glob('*.json')).read_text())
    assert report['status'] == 'deadline_exhausted'
    assert report['attempts'][-1]['retry_scheduled'] is False if children else not report['attempts']
