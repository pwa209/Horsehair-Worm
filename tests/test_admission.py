import json
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
