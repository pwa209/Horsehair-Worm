import json
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


def primary_rna_release_fixture(tmp_path):
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
    monkeypatch.setenv('PYTHONPATH', str(Path(__file__).resolve().parents[1]/'src'))
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
        assert kwargs['cwd']==release and kwargs['timeout']==900
        assert kwargs['env']['PYTHONNOUSERSITE']=='1'
        return SimpleNamespace(returncode=1,stdout='',stderr='Generated child failure')
    monkeypatch.setattr(pipeline.subprocess,'run',failed)
    with pytest.raises(RuntimeError,match='no framework fallback'):
        pipeline._primary_execution(config,root,release,'admission','rna-full')
    assert path.read_bytes()==original
    assert not (root/'state/pipeline/qa-primary/primary-context/rna-full-admission.json').exists()
