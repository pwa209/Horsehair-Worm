"""Generated preservation fixtures; no native Slurm/quota/scientific claim."""
import copy
from pathlib import Path

import pytest

from horsehair import archive
from horsehair.core import atomic_json, canonical_hash, digest, read_json, source_identity


@pytest.fixture
def archive_fixture(tmp_path,monkeypatch):
    root,project,release = tmp_path/"scratch-study",tmp_path/"personal-project",tmp_path/"release"
    project.mkdir()
    config = {"paths":{"root":str(root),"project":str(project)},"protocol_id":"generated-archive-fixture"}
    source = release/"src/horsehair/archive.py"
    source.parent.mkdir(parents=True)
    source.write_text("# Generated immutable-source fixture only\n")
    atomic_json(release/"configs/study.json",config)
    identity = source_identity(release)
    atomic_json(release/"release.json",{"release_id":"fixture-release","source_identity":identity,"config_sha256":canonical_hash(config)})
    figures = []
    for number in range(1,7):
        path = root/f"results/report/generated_figure_{number:02}.txt"
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text("Generated figure-integrity fixture, not biological evidence\n")
        figures.append({"id":f"{number:02}","paths":[str(path)],"rendered_file_hashes":{str(path):digest(path)}})
    report = {"generated_test_fixture":True,"source_sha256":identity["sha256"],"config_sha256":canonical_hash(config),
              "analysis_complete":True,"figures_complete":True,"publication_ready":False,
              "claims":[{"claim_id":claim,"status":"not_supported"} for claim in ("H1","H2","H3","H4")],"figures":figures}
    atomic_json(root/"results/report/report.json",report)
    for name,text in {"results/agents/final/checkpoint.pt":"generated checkpoint",
                      "results/.stage.lock":"generated volatile lock",
                      "results/unfinished.partial":"generated temporary file",
                      "data/molecular/ant/ant_counts.tsv":"gene\tbrain\na\t1\n",
                      "data/molecular/mantis/mantis_adapter_audit.json":"{}\n",
                      "data/molecular/freeze/frozen_scores.tsv":"gene\tscore\na\t.2\n",
                      "data/molecular/ant/working.bam":"generated excluded working BAM",
                      "data/molecular/orthology/proteome.faa":"generated excluded proteome",
                      "plans/example/plan.json":"{}\n"}.items():
        path = root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(text)
    atomic_json(root/"manifests/source_manifest.json",{"generated_test_fixture":True})
    atomic_json(root/"state/fixture-submission.json",{"generated_test_fixture":True,"tasks":{}})
    atomic_json(root/"state/resource-budget.json",{"generated_test_fixture":True,"spent_cpu_core_hours":1.})
    atomic_json(root/"state/pipeline/fixture-release/status.json",{"generated_test_fixture":True,"status":"active"})
    monkeypatch.setattr(archive,"__file__",str(source))
    monkeypatch.setattr(archive,"native_job_id",lambda:"999")
    quota_calls = []
    def mocked_quota(project,bytes_required,files_required):
        quota_calls.append((bytes_required,files_required))
        return {"generated_test_fixture":True,"user":"unlimited native-query fixture","project_group_available_bytes":10**12}
    monkeypatch.setattr(archive,"_native_quota",mocked_quota)
    return root,project,release,config,identity,quota_calls


def test_archive_copies_selected_real_fixture_bytes_hashes_and_reuses_verified_cache(archive_fixture):
    root,project,_,config,identity,quota_calls = archive_fixture
    result = archive.run(config,root)
    directory = project/"artifacts/fixture-release"
    record = read_json(directory/"archive_manifest.json")
    files = {item["relative_path"]:item for item in record["files"]}
    assert result["status"]=="complete" and not result["publication_ready"] and not result["scratch_deleted"]
    assert "results/agents/final/checkpoint.pt" in files
    assert "data/molecular/ant/ant_counts.tsv" in files and "source/release.json" in files
    assert "data/molecular/ant/working.bam" not in files and "results/.stage.lock" not in files
    assert "results/unfinished.partial" not in files
    assert "state/resource-budget.json" not in files
    snapshot = read_json(directory/"evidence_snapshots/state/resource-budget.json.json")
    assert snapshot["captured_payload"]["spent_cpu_core_hours"]==1. and snapshot["original_source_path"]==str((root/"state/resource-budget.json").resolve())
    for item in files.values():
        assert item["sha256"]==digest(directory/item["relative_path"])
        assert Path(item["source_path"]).is_absolute()
    assert result["manifest_sha256"]==digest(directory/"archive_manifest.json")
    payload = dict(record);self_hash = payload.pop("receipt_sha256")
    assert self_hash==canonical_hash(payload)
    assert archive.verify(directory,config,identity,"fixture-release")==record
    second = archive.run(config,root)
    assert second["reused_verified_archive"] and len(quota_calls)==1
    assert (root/"results/agents/final/checkpoint.pt").is_file()
    assert read_json(root/"state/archive-fixture-release.json")["manifest_sha256"]==result["manifest_sha256"]


@pytest.mark.parametrize("field,value",[("analysis_complete",False),("figures_complete",False),("source_sha256","0"*64),
                                       ("config_sha256","0"*64),("evidence_status","synthetic_fixture")])
def test_unqualified_foreign_or_synthetic_report_cannot_archive(archive_fixture,field,value):
    root,project,_,config,_,quota_calls = archive_fixture
    path = root/"results/report/report.json"
    report = read_json(path);report[field]=value;atomic_json(path,report)
    with pytest.raises(archive.ArchiveError):
        archive.run(config,root)
    assert not (project/"artifacts/fixture-release").exists() and not quota_calls


def test_changed_figure_is_detected_before_durable_copy(archive_fixture):
    root,project,_,config,_,_ = archive_fixture
    (root/"results/report/generated_figure_01.txt").write_text("tampered generated figure")
    with pytest.raises(archive.ArchiveError,match="figure changed"):
        archive.run(config,root)
    assert not (project/"artifacts/fixture-release").exists()


def test_partial_archive_is_preserved_and_not_replayed(archive_fixture):
    root,project,_,config,_,_ = archive_fixture
    directory = project/"artifacts/fixture-release"
    directory.mkdir(parents=True)
    (directory/"unconfirmed-copy.txt").write_text("generated ambiguous copy")
    with pytest.raises(archive.ArchiveError,match="lacks completion receipt"):
        archive.run(config,root)
    assert (directory/"unconfirmed-copy.txt").is_file()


def test_archive_cache_tampering_or_foreign_configuration_refuses_reuse(archive_fixture):
    root,project,_,config,identity,_ = archive_fixture
    archive.run(config,root)
    directory = project/"artifacts/fixture-release"
    changed = copy.deepcopy(config);changed["protocol_id"]="changed"
    with pytest.raises(archive.ArchiveError,match="another source/configuration"):
        archive.verify(directory,changed,identity,"fixture-release")
    (directory/"results/agents/final/checkpoint.pt").write_text("tampered checkpoint")
    with pytest.raises(archive.ArchiveError,match="bytes/hash changed"):
        archive.run(config,root)


def test_source_mutation_during_copy_withholds_completion_and_preserves_partial(archive_fixture,monkeypatch):
    root,project,_,config,_,_ = archive_fixture
    original = archive._copy_item
    def mutation(item,directory):
        original(item,directory)
        if item["relative_path"]=="data/molecular/ant/ant_counts.tsv":
            Path(item["source_path"]).write_text("changed generated source during copy")
    monkeypatch.setattr(archive,"_copy_item",mutation)
    with pytest.raises(archive.ArchiveError,match="Source changed after"):
        archive.run(config,root)
    directory = project/"artifacts/fixture-release"
    assert directory.is_dir() and not (directory/"archive_manifest.json").exists()


def test_budget_or_native_quota_refusal_occurs_before_destination_creation(archive_fixture,monkeypatch):
    root,project,_,config,_,_ = archive_fixture
    monkeypatch.setattr(archive,"MAX_BYTES",1)
    with pytest.raises(archive.ArchiveError,match="ceiling"):
        archive.run(config,root)
    assert not (project/"artifacts/fixture-release").exists()
    monkeypatch.setattr(archive,"MAX_BYTES",100_000_000_000)
    def unknown(*args):
        raise archive.ArchiveError("Native project group quota unavailable fixture")
    monkeypatch.setattr(archive,"_native_quota",unknown)
    with pytest.raises(archive.ArchiveError,match="unavailable"):
        archive.run(config,root)
    assert not (project/"artifacts/fixture-release").exists()


def test_quota_zero_user_limit_is_unlimited_but_group_limit_must_be_known():
    user = archive._quota_row("Filesystem kbytes quota limit grace files quota limit grace\n/project 1000 0 0 - 30 0 0 -\n")
    group = archive._quota_row("/project 1000 5000000 6000000 - 30 100000 120000 -\n")
    assert user["available_bytes"] is None and group["available_bytes"]==(5000000-1000)*1024
    archive._quota_capacity(user,group,1000,100)
    with pytest.raises(archive.ArchiveError,match="positive project/group"):
        archive._quota_capacity(user,user,1000,100)
    with pytest.raises(archive.ArchiveError,match="byte quota"):
        archive._quota_capacity(group,group,10**12,100)
    with pytest.raises(archive.ArchiveError,match="Exactly one"):
        archive._quota_row("unknown quota output")


def test_archive_requires_slurm_and_rejects_traversal(archive_fixture,monkeypatch):
    root,project,_,config,_,_ = archive_fixture
    monkeypatch.setattr(archive,"native_job_id",lambda:None)
    with pytest.raises(archive.ArchiveError,match="native Slurm"):
        archive.run(config,root)
    with pytest.raises(archive.ArchiveError,match="escapes|traversal"):
        archive._safe(project/"../outside",project)


def test_symlink_result_cannot_enter_archive(archive_fixture,monkeypatch):
    root,project,_,config,_,_ = archive_fixture
    source = root/"results/link.txt"
    try:
        source.symlink_to(root/"results/agents/final/checkpoint.pt")
    except OSError:
        # Windows accounts without symlink privileges still exercise the exact
        # rejection path; native POSIX tests use a genuine filesystem symlink.
        source.write_text("generated simulated link fixture")
        original = archive._linked
        monkeypatch.setattr(archive,"_linked",lambda path:path==source or original(path))
    with pytest.raises(archive.ArchiveError,match="symlinks"):
        archive.run(config,root)
    assert not (project/"artifacts/fixture-release").exists()
