"""Operational adapter fixtures; no biological evidence is manufactured."""
from contextlib import nullcontext
import csv
import hashlib
import inspect
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from horsehair import mantis_recovery as repair
from horsehair.core import canonical_hash, digest


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def table(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf8") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(fields); writer.writerows(rows)


def native(job, state="COMPLETED", exit_code="0:0", cpus=1, elapsed=20):
    return {"basis": "sacct final allocated wall time", "job_id": job, "state": state,
            "exit_code": exit_code, "cpus": cpus, "elapsed_seconds": elapsed}


@pytest.fixture
def deposited(tmp_path, monkeypatch):
    root = tmp_path / "scratch/study"
    framework, primary = tmp_path / "framework", tmp_path / "primary"
    config = {"paths": {"root": str(root)}, "resources": {"scratch_budget_files": 250000,
               "scratch_reserve_bytes": 2000000000000}}
    full_config = canonical_hash(config)
    monkeypatch.setattr(repair, "FULL_CONFIG_SHA256", full_config)
    monkeypatch.setattr(repair, "REFERENCE_GENES", 3)
    monkeypatch.setattr(repair, "MEASURED_GENES", 2)
    monkeypatch.setattr(repair, "PRIMARY_RELEASE", primary)
    current = {"source_identity": {"sha256": "f" * 64}, "config_sha256": full_config}
    primary_record = {"release_id": "primary-fixture", "source_identity": {"sha256": repair.PRIMARY_SOURCE_SHA256}, "config_sha256": full_config}
    monkeypatch.setattr(repair, "_context", lambda: (root, framework, current, primary_record, config))
    functions = {name: str(index) * 64 for index, name in enumerate(repair.KERNEL_FUNCTIONS, 1)}
    monkeypatch.setattr(repair, "_function_hashes", lambda path: functions)
    monkeypatch.setattr(repair, "resource_lease", lambda *args, **kwargs: nullcontext())
    monkeypatch.setattr(repair, "require_storage_headroom", lambda *args, **kwargs: {"ready": True})
    monkeypatch.setenv("SLURM_JOB_ID", "9000")
    monkeypatch.setenv("SLURM_CPUS_PER_TASK", "1")
    monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)
    monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
    write_json(primary / "configs/study.json", config)
    records = {"6626122": native("6626122"), "6625680": native("6625680", "FAILED", "1:0", 8, 11),
               "6625684": native("6625684", "CANCELLED", "0:0", 0, 0), "9000": native("9000"),
               "9001": native("9001", cpus=8), "9002": native("9002", cpus=8)}
    write_json(root / "state/resource-budget.json", {"native_jobs": records})
    output = root / "data/molecular/mantis_orfs"
    output.mkdir(parents=True)
    sequence = ">Cluster-1.0\nACGT\n>Cluster-2.0\nTGCA\n>Cluster-3.0\nNNNN\n"
    (output / "supertranscripts.fasta").write_text(sequence)
    (output / "failed-attempt.log").write_text("Keep this forensic original\n")
    raw = root / "raw/mantis_rna"
    raw.mkdir(parents=True)
    for name in ("source.fasta.gz", "source.gff.gz", "counts.txt.gz"):
        (raw / name).write_text("Fixture raw source retained\n")
    counts = root / "data/molecular/mantis/mantis_counts.tsv"
    counts.parent.mkdir(parents=True)
    counts.write_text("gene_id\tGSM\nCluster-1.0\t1\nCluster-2.0\t2\n")
    manifest_path = root / "manifests/source_manifest.json"
    write_json(manifest_path, {"fixture": "bound deposited manifest"})
    for name in ("freeze.json", "molecular_scores.tsv"):
        path = root / "data/molecular/freeze" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("Protected original ant-fly freeze\n")
    audit_case = root / "maintenance/native-audit"
    sequence_table = audit_case / "source-sequence-identities.tsv"
    table(sequence_table, ["transcript_id", "bases", "sequence_sha256"],
          [(name, 4, hashlib.sha256(bases.encode()).hexdigest()) for name, bases in
           (("Cluster-1.0", "ACGT"), ("Cluster-2.0", "TGCA"), ("Cluster-3.0", "NNNN"))])
    gff_table = audit_case / "gff-identifier-observations.tsv"
    table(gff_table, ["seqid", "explicit_gene_ids", "explicit_transcript_ids"],
          [(name, json.dumps([name]), json.dumps([original])) for name, original in
           (("Cluster-1.0", "TRINITY_DN1_c0_g1_i1"), ("Cluster-2.0", "Cluster-2.0"), ("Cluster-3.0", "TRINITY_DN3_c0_g1_i1"))])
    counted_table = audit_case / "counted-identifiers.tsv"
    table(counted_table, ["source", "row", "gene_id"], [(source, index, name) for source in ("deposited", "derived")
          for index, name in enumerate(("Cluster-1.0", "Cluster-2.0"))])
    audit_path = audit_case / "audit.json"
    audit = {"schema_version": 1, "status": "complete", "slurm_job_id": "6626122",
        "framework_source_sha256": repair.AUDIT_FRAMEWORK_SHA256, "primary_source_sha256": repair.PRIMARY_SOURCE_SHA256,
        "config_sha256": full_config, "scientific_recomputations": 0, "input_files_overwritten": False,
        "sequence_filtering_performed": False, "identifier_renaming_performed": False,
        "zero_imputation_performed": False, "mapping_created": False,
        "source_manifest": repair._binding(manifest_path),
        "input_bindings": {"fasta": repair._binding(raw / "source.fasta.gz"), "gff": repair._binding(raw / "source.gff.gz"),
             "counts": repair._binding(raw / "counts.txt.gz"), "derived_fasta": repair._binding(output / "supertranscripts.fasta"),
             "derived_counts": repair._binding(counts)},
        "tables": {path.name: repair._binding(path) for path in (sequence_table, gff_table, counted_table)},
        "observed": {"derived_fasta_exact_decompressed_bytes": True, "derived_fasta_exact_sequence_records": True,
                     "raw_derived_count_gene_sets_equal": True, "fasta_not_counted_count": 1, "counted_not_fasta_count": 0}}
    def refresh():
        for name, value in audit["tables"].items():
            audit["tables"][name] = repair._binding(Path(value["path"]))
        write_json(audit_path, audit)
        monkeypatch.setattr(repair, "AUDIT_SHA256", digest(audit_path))
        return digest(audit_path)
    sha = refresh()
    return SimpleNamespace(root=root, primary=primary, framework=framework, current=current, config=config,
        audit=audit, audit_path=audit_path, audit_sha=sha, refresh=refresh, output=output, records=records,
        case=root / "maintenance/explicit-reference-repair", gff_table=gff_table, sequence_table=sequence_table,
        functions=functions, monkeypatch=monkeypatch)


def prepare(d):
    result = repair.prepare_recovery(d.audit_path, digest(d.audit_path), d.case)
    return result, (d.case / "manifest.json", digest(d.case / "manifest.json"),
                    d.case / "preparation.json", digest(d.case / "preparation.json"))


def test_full_reference_mapping_preserves_unmeasured_genes_and_original_transcript_attributes(deposited):
    d = deposited
    before_raw = {str(p): digest(p) for p in (d.root / "raw").rglob("*") if p.is_file()}
    result, args = prepare(d)
    mapping = repair._table(result["manifest"]["full_reference_map"]["path"], {"transcript_id", "gene_id"})
    assert len(mapping) == 3 and [row["gene_id"] for row in mapping] == ["Cluster-1.0", "Cluster-2.0", "Cluster-3.0"]
    assert mapping[0]["transcript_id"] == "Cluster-1.0" and mapping[0]["original_assembly_transcript_id"] == "TRINITY_DN1_c0_g1_i1"
    assert mapping[2]["measured_in_count_matrix"] == "0"
    assert not d.output.exists()
    assert (d.case / "preserved/mantis_orfs/failed-attempt.log").read_text().startswith("Keep")
    assert result["manifest"]["reference_genes"] == 3 and result["manifest"]["measured_genes"] == 2
    assert before_raw == {str(p): digest(p) for p in (d.root / "raw").rglob("*") if p.is_file()}
    validated = repair.validate_registration_manifest(*args)
    assert validated["capture_native_record"]["job_id"] == "9000"
    assert validated["manifest"]["orf_recipe"] == repair.ORF_RECIPE


@pytest.mark.parametrize("mutation", ["ambiguous_gene", "renamed_gene", "missing_seqid", "duplicate_fasta", "missing_counted", "changed_raw", "bad_native", "allocated_cancel"])
def test_preparation_refuses_unproven_native_or_source_mapping_before_preservation(deposited, mutation):
    d = deposited
    if mutation in {"ambiguous_gene", "renamed_gene", "missing_seqid"}:
        rows = repair._table(d.gff_table, {"seqid"})
        if mutation == "ambiguous_gene":
            rows[0]["explicit_gene_ids"] = json.dumps(["Cluster-1.0", "OTHER"])
        elif mutation == "renamed_gene":
            rows[0]["explicit_gene_ids"] = json.dumps(["OTHER"])
        else:
            rows.pop()
        table(d.gff_table, ["seqid", "explicit_gene_ids", "explicit_transcript_ids"], [list(row.values()) for row in rows])
        d.refresh()
    elif mutation == "duplicate_fasta":
        rows = repair._table(d.sequence_table, {"transcript_id"})
        rows[1]["transcript_id"] = rows[0]["transcript_id"]
        table(d.sequence_table, ["transcript_id", "bases", "sequence_sha256"], [list(row.values()) for row in rows])
        d.refresh()
    elif mutation == "missing_counted":
        path = Path(d.audit["tables"]["counted-identifiers.tsv"]["path"])
        text = path.read_text().replace("Cluster-2.0", "NO_REFERENCE")
        path.write_text(text); d.refresh()
    elif mutation == "changed_raw":
        (d.root / "raw/mantis_rna/source.fasta.gz").write_text("Changed raw bytes")
    else:
        row = d.records["6625680" if mutation == "bad_native" else "6625684"]
        row["state" if mutation == "bad_native" else "cpus"] = "RUNNING" if mutation == "bad_native" else 8
        write_json(d.root / "state/resource-budget.json", {"native_jobs": d.records})
    with pytest.raises(ValueError):
        repair.prepare_recovery(d.audit_path, digest(d.audit_path), d.case)
    assert d.output.is_dir() and not d.case.exists()


@pytest.mark.parametrize("mutation", ["map", "freeze", "preserved_inode", "capture_not_complete", "existing_output", "existing_intent"])
def test_registration_refuses_changed_preservation_or_ambiguous_replay(deposited, mutation):
    d = deposited
    result, args = prepare(d)
    if mutation == "map":
        Path(result["manifest"]["full_reference_map"]["path"]).write_text("Filtered map")
    elif mutation == "freeze":
        (d.root / "data/molecular/freeze/molecular_scores.tsv").write_text("Changed ant-fly freeze")
    elif mutation == "preserved_inode":
        saved = d.case / "preserved/mantis_orfs"
        saved.rename(saved.with_name("retained-original")); saved.mkdir()
    elif mutation == "capture_not_complete":
        d.records["9000"]["state"] = "RUNNING"
        write_json(d.root / "state/resource-budget.json", {"native_jobs": d.records})
    elif mutation == "existing_output":
        d.output.mkdir()
    else:
        write_json(d.case / "run-intent.json", {"status": "ambiguous_earlier_execution"})
    with pytest.raises((ValueError, FileNotFoundError)):
        repair.validate_registration_manifest(*args)


def simulated_success(d):
    def run(command, **options):
        assert command[1:4] == ["-s", "-c", repair.PRIMARY_KERNEL_BOOTSTRAP]
        request = json.loads(command[4])
        assert request["primary"] == str(d.primary) and options["cwd"] == str(d.primary)
        assert request["reference_genes"] == 3 and request["function_sha256"] == d.functions
        assert options["env"]["PYTHONPATH"].split(__import__("os").pathsep)[0] == str(d.primary / "src")
        output = Path(request["output"])
        for name in ("mantis.fa", "protein_gene_map.tsv", "supertranscripts.fasta", "transdecoder/orf_plan.json", "transdecoder/transdecoder.sh", "transdecoder/mantis_supertranscripts.fasta.transdecoder.pep"):
            path = output / name
            path.parent.mkdir(parents=True, exist_ok=True); path.write_text("Original scientific kernel fixture output\n")
        kernel_plan = {"minimum_peptide_aa": 100, "single_best_per_transcript": True, "source_transcripts": 3,
                       "source_sha256": digest(output / "supertranscripts.fasta"), "transcript_map_sha256": digest(Path(request["map"]))}
        write_json(output / "transdecoder/orf_plan.json", kernel_plan)
        write_json(Path(request["result"]), {"status": "primary_scientific_kernel_completed", "slurm_job_id": "9001",
            "primary_scientific_kernel_source_sha256": repair.PRIMARY_SOURCE_SHA256, "full_config_sha256": repair.FULL_CONFIG_SHA256,
            "primary_kernel_function_sha256": d.functions, "orf_recipe": repair.ORF_RECIPE, "plan": kernel_plan, "all_reference_sequences_retained": True,
            "coverage": {"reference_genes": 3, "measured_count_genes": 2}, "primary_kernel_coverage": {"source_counted_genes": 3}})
        return SimpleNamespace(returncode=0)
    return run


def test_execution_uses_isolated_primary_kernel_and_truthful_adapter_receipt(deposited, monkeypatch):
    d = deposited
    _, args = prepare(d)
    monkeypatch.setenv("SLURM_JOB_ID", "9001"); monkeypatch.setenv("SLURM_CPUS_PER_TASK", "8")
    monkeypatch.setattr(repair.subprocess, "run", simulated_success(d))
    result = repair.execute_recovery_task(*args)
    assert result["source_sha256"] == "f" * 64
    assert result["primary_scientific_kernel_source_sha256"] == repair.PRIMARY_SOURCE_SHA256
    assert result["source_sha256"] != result["primary_scientific_kernel_source_sha256"]
    assert result["coverage"] == {"reference_genes": 3, "measured_count_genes": 2}
    assert read(d.root / "state/molecular-mantis-orfs.json") == read(d.case / "receipt.json")
    assert (d.case / "preserved/mantis_orfs/failed-attempt.log").is_file()
    with pytest.raises(ValueError, match="already started"):
        repair.execute_recovery_task(*args)


def read(path):
    return json.loads(path.read_text())


def test_failed_scientific_child_preserves_attempt_and_never_writes_success(deposited, monkeypatch):
    d = deposited
    _, args = prepare(d)
    monkeypatch.setenv("SLURM_JOB_ID", "9001"); monkeypatch.setenv("SLURM_CPUS_PER_TASK", "8")
    monkeypatch.setattr(repair.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=9))
    with pytest.raises(RuntimeError, match="kernel failed"):
        repair.execute_recovery_task(*args)
    assert read(d.case / "run-failure.json")["returncode"] == 9
    assert d.output.is_dir() and not (d.case / "receipt.json").exists()
    assert not (d.root / "state/molecular-mantis-orfs.json").exists()


@pytest.mark.parametrize("mutation", ["proteome", "orf_native", "orthology_native", "kernel_status", "kernel_source", "kernel_config", "kernel_functions", "kernel_recipe"])
def test_phase_completion_checks_exact_orthology_proteome_and_dual_native_provenance(deposited, monkeypatch, mutation):
    d = deposited
    _, args = prepare(d)
    monkeypatch.setenv("SLURM_JOB_ID", "9001"); monkeypatch.setenv("SLURM_CPUS_PER_TASK", "8")
    monkeypatch.setattr(repair.subprocess, "run", simulated_success(d))
    receipt = repair.execute_recovery_task(*args)
    proteomes = {"mantis": {"path": str(d.output / "mantis.fa"), "sha256": receipt["proteome_sha256"], "version": "fixture"}}
    plan = {"proteomes": proteomes, "plan_hash": canonical_hash(proteomes)}
    write_json(d.root / "data/molecular/orthology_mantis/orthology_plan.json", plan)
    orthology = {"status": "native_three_species_corroboration_computed", "source_sha256": repair.PRIMARY_SOURCE_SHA256,
                "config_sha256": repair.FULL_CONFIG_SHA256, "slurm_job_id": "9002", "plan": plan}
    write_json(d.root / "state/molecular-mantis-orthology.json", orthology)
    guard = dict(zip(("manifest_path", "manifest_sha256", "preparation_path", "preparation_sha256"), map(str, args)))
    canonical = {"config_sha256": repair.FULL_CONFIG_SHA256, "release_identity": {"sha256": repair.PRIMARY_SOURCE_SHA256},
                 "dag_amendments": [{"mantis_reference_recovery": guard}]}
    ledger = {"source_sha256": repair.PRIMARY_SOURCE_SHA256, "config_sha256": repair.FULL_CONFIG_SHA256,
              "plan_sha256": canonical_hash(canonical), "dag_amendments": canonical["dag_amendments"],
              "tasks": {"mantis-orfs": {"job_id": "9001"}, "mantis-orthology": {"job_id": "9002"}}}
    write_json(d.root / "plans/primary-fixture-molecular-corroboration/plan.json", canonical)
    write_json(d.root / "state/primary-fixture-molecular-corroboration-submission.json", ledger)
    proof = repair.validate_completed_phase(guard, d.config, d.root, {"sha256": repair.PRIMARY_SOURCE_SHA256})
    assert proof["status"] == "validated" and proof["orf_native_record"]["job_id"] == "9001"
    if mutation == "proteome":
        plan["proteomes"]["mantis"]["sha256"] = "0" * 64
        plan["plan_hash"] = canonical_hash(plan["proteomes"]); orthology["plan"] = plan
        write_json(d.root / "data/molecular/orthology_mantis/orthology_plan.json", plan)
        write_json(d.root / "state/molecular-mantis-orthology.json", orthology)
    elif mutation in {"orf_native", "orthology_native"}:
        d.records["9003"] = native("9003", cpus=8)
        write_json(d.root / "state/resource-budget.json", {"native_jobs": d.records})
        ledger["tasks"]["mantis-orfs" if mutation == "orf_native" else "mantis-orthology"]["job_id"] = "9003"
        write_json(d.root / "state/primary-fixture-molecular-corroboration-submission.json", ledger)
    else:
        path = Path(receipt["primary_kernel_result"]["path"])
        kernel = read(path)
        key, value = {"kernel_status": ("status", "another_operation"),
                      "kernel_source": ("primary_scientific_kernel_source_sha256", "0" * 64),
                      "kernel_config": ("full_config_sha256", "0" * 64),
                      "kernel_functions": ("primary_kernel_function_sha256", {}),
                      "kernel_recipe": ("orf_recipe", {**repair.ORF_RECIPE, "minimum_peptide_aa": 99})}[mutation]
        kernel[key] = value
        write_json(path, kernel)
        receipt["primary_kernel_result"] = repair._binding(path)
        write_json(d.case / "receipt.json", receipt)
        write_json(d.root / "state/molecular-mantis-orfs.json", receipt)
    with pytest.raises(ValueError):
        repair.validate_completed_phase(guard, d.config, d.root, {"sha256": repair.PRIMARY_SOURCE_SHA256})


def test_scientific_function_hashes_match_frozen_function_source():
    from horsehair import molecular
    path = Path(molecular.__file__).resolve().parents[2]
    expected = {name: hashlib.sha256((inspect.getsource(getattr(molecular, name)).strip() + "\n").encode()).hexdigest()
                for name in repair.KERNEL_FUNCTIONS}
    assert repair._function_hashes(path) == expected
