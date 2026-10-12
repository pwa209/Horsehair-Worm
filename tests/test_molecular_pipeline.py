import gzip

import pandas as pd
import pytest

from horsehair.molecular import MolecularError
from horsehair.molecular_pipeline import ant_protein_map, go_gene_annotations, parse_fly_catalog, parse_fly_relationships


def gz(path, content):
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(content)
    return path


def test_source_fly_relationships_and_primary_catalog_fields(tmp_path):
    relationship = gz(tmp_path / "relationships.gz", "#gene_ID\ttranscript_ID\tpolypeptide_ID\nFBgn0000001\tFBtr0000001\tFBpp0000001\nFBgn0000001\tFBtr0000002\tFBpp0000002\n")
    frame, audit = parse_fly_relationships(relationship)
    assert frame.gene_id.nunique() == 1 and audit["proteins"] == 2
    catalog = gz(tmp_path / "catalog.gz", "#gene_symbol\tprimary_FBgn\tsecondary_FBgn\tannotation_ID\nDmel\\geneA\tFBgn0000001\tFBgn0000002\tCG1\n")
    result = parse_fly_catalog(catalog)
    assert result.gene_id.item() == "FBgn0000001"
    assert result.secondary_gene_ids.item() == "FBgn0000002"
    assert result.gene_symbol.item() == "Dmel\\geneA"
    catalog = gz(tmp_path / "ambiguous.gz", "geneA\tFBgn0000001\tFBgn0000002\n")
    with pytest.raises(MolecularError, match="header is unresolved"):
        parse_fly_catalog(catalog)


def test_source_refseq_protein_ids_must_match_same_gtf_gene_namespace(tmp_path):
    gtf = tmp_path / "genes.gtf"
    gtf.write_text('NC_1\tRefSeq\tCDS\t1\t12\t.\t+\t0\tgene_id "LOC1"; protein_id "XP_1.1";\n')
    proteins = tmp_path / "proteins.fa"
    proteins.write_text(">XP_1.1 source annotation\nMKKK\n")
    frame = ant_protein_map(gtf, proteins)
    assert frame.gene_id.item() == "LOC1"
    proteins.write_text(">XP_1.2 changed release\nMKKK\n")
    with pytest.raises(MolecularError, match="exact protein mapping"):
        ant_protein_map(gtf, proteins)


def test_fixed_go_descendants_negation_taxon_and_source_version(tmp_path):
    obo = gz(tmp_path / "go.obo.gz", "format-version: 1.2\ndata-version: synthetic_fixture\n[Term]\nid: GO:0050877\n[Term]\nid: GO:0007623\n[Term]\nid: GO:0000001\nis_a: GO:0050877 ! fixed neuronal root\n")
    def row(gene, term, qualifier="", taxon="taxon:7227"):
        return "\t".join(["FB", gene, "gene", qualifier, term, "PMID:1", "IDA", "", "P", "gene", "", "protein", taxon, "20260101", "FlyBase", "", ""])+"\n"
    gaf = gz(tmp_path / "go.gaf.gz", "!gaf-version: 2.2\n"+row("FBgn0000001", "GO:0000001")+row("FBgn0000002", "GO:0007623", "NOT")+row("FBgn0000003", "GO:0007623", taxon="taxon:9606"))
    frame, audit = go_gene_annotations(obo, gaf, ["FBgn0000001", "FBgn0000002", "FBgn0000003"])
    assert frame.neuronal_circadian.tolist() == [1, 0, 0]
    assert audit["ontology_data_version"] == "synthetic_fixture"


def test_molecular_runtime_and_output_namespaces_stay_in_study_root(tmp_path):
    from horsehair.acquire import AcquisitionError
    from horsehair.molecular_pipeline import runtime_directory, primary_directory, prediction_directory
    config = {"molecular": {"tool_runtime_directory": "data/tool_runtime_v2", "orthology_directory": "data/orthology_v2", "prediction_directory": "results/transfer_v2"}}
    assert runtime_directory(config, tmp_path) == tmp_path / "data" / "tool_runtime_v2"
    assert primary_directory(config, tmp_path) == tmp_path / "data" / "orthology_v2"
    assert prediction_directory(config, tmp_path) == tmp_path / "results" / "transfer_v2"
    with pytest.raises(AcquisitionError):
        runtime_directory({"molecular": {"tool_runtime_directory": "../outside"}}, tmp_path)


def test_ambiguous_molecular_preparation_cannot_overwrite_historical_files(tmp_path):
    from horsehair.molecular_pipeline import prepare_native_proteomes
    directory = tmp_path / "data" / "molecular" / "orthology"
    directory.mkdir(parents=True)
    old = directory / "ant.fa"
    old.write_text(">historical\nMKKK\n")
    with pytest.raises(MolecularError, match="new prospective namespace"):
        prepare_native_proteomes({}, tmp_path)
    assert old.read_text() == ">historical\nMKKK\n"


def _benchmark_fixture(root, config, monkeypatch, *, full=True):
    import horsehair.molecular_pipeline as pipeline
    from horsehair.acquire import asset, atomic_json, canonical_hash
    directory = pipeline.primary_directory(config, root)
    directory.mkdir(parents=True)
    runtime = pipeline.runtime_directory(config, root)
    runtime.mkdir(parents=True)
    atomic_json(runtime / "runtime_qualification.json", {"status": "synthetic test proof only"})
    manifest = {"schema_version": 1, "assets": [asset(source="fixture", name="test.txt", url="https://example.org/test.txt", version="test", rights="synthetic test fixture")], "sources": {"fixture": "synthetic"}}
    manifest["manifest_hash"] = canonical_hash(manifest)
    atomic_json(root / "manifests" / "source_manifest.json", manifest)
    proteomes = {}
    for species in ["ant", "fly"]:
        path = directory / (species+".fa")
        path.write_text(f">{species}_fixture\nMKKK\n")
        proteomes[species] = {"path": str(path), "version": "synthetic"}
    atomic_json(directory / "proteomes.json", proteomes)
    atomic_json(directory / "reference_preparation.json", {"synthetic": True})
    for name in ["protein_gene_map.tsv", "gene_orthology.tsv", "fly_ant_identity.tsv", "fly_gene_catalog.tsv", "fly_target_families.tsv"]:
        (directory / name).write_text("synthetic test output\n")
    receipt = {"status": "native_primary_ant_fly_orthology_computed", "config_sha256": canonical_hash(config),
        "source_manifest_hash": manifest["manifest_hash"], "complete_primary_proteomes": full,
        "protein_counts": {"ant": 1, "fly": 1}, **pipeline._result_proof([directory / "ant.fa", directory / "fly.fa"])}
    pipeline._write_completed_stage(directory / "orthology_receipt.json", receipt)
    monkeypatch.setattr(pipeline, "source_identity", lambda _: {"sha256": "synthetic-release"})
    return directory


def test_full_orthology_benchmark_records_native_array_identity_and_rejects_changed_inputs(tmp_path, monkeypatch):
    import horsehair.molecular_pipeline as pipeline
    config = {"molecular": {"tool_runtime_directory": "data/tool_runtime_v2"}}
    def prepare(config, root):
        _benchmark_fixture(root, config, monkeypatch)
        return {"preparation": "synthetic fixture", "orthology": "synthetic fixture"}
    monkeypatch.setattr(pipeline, "prepare_primary", prepare)
    monkeypatch.setenv("SLURM_JOB_ID", "1234"); monkeypatch.setenv("SLURM_ARRAY_JOB_ID", "1200")
    monkeypatch.setenv("SLURM_ARRAY_TASK_ID", "7"); monkeypatch.setenv("SLURM_CPUS_PER_TASK", "8")
    result = pipeline.benchmark_primary(config, tmp_path)
    assert result["slurm_job_id"] == "1200_7" and result["allocated_cpus"] == 8
    assert result["benchmark_uses_full_primary_numerics"] is True
    assert result["complete_declared_workload"] is False
    assert str((tmp_path / "data" / "tool_runtime_v2" / "runtime_qualification.json").resolve()) in result["source_result_hashes"]
    (tmp_path / "data" / "molecular" / "orthology" / "gene_orthology.tsv").write_text("changed source output")
    with pytest.raises(MolecularError, match="proof is missing or changed"):
        pipeline.benchmark_primary(config, tmp_path)


def test_software_qualification_cannot_be_promoted_to_full_proteome_benchmark(tmp_path, monkeypatch):
    import horsehair.molecular_pipeline as pipeline
    config = {"molecular": {}}
    def prepare(config, root):
        _benchmark_fixture(root, config, monkeypatch, full=False)
        return {}
    monkeypatch.setattr(pipeline, "prepare_primary", prepare)
    with pytest.raises(MolecularError, match="abbreviated qualification"):
        pipeline.benchmark_primary(config, tmp_path)
    assert not (tmp_path / "state" / "molecular-primary-benchmark.json").exists()


def test_transfer_workload_refuses_shortened_inference_without_writing_admission_evidence(tmp_path):
    from horsehair.molecular_pipeline import benchmark_transfer
    config = {"molecular": {"gene_permutations": 20}, "inference": {"bootstrap_replicates": 20}}
    with pytest.raises(MolecularError, match="10,000-or-more"):
        benchmark_transfer(config, tmp_path)
    assert not (tmp_path / "state" / "molecular-workload-benchmark.json").exists()


def test_completed_transfer_cache_keeps_original_native_measurement_and_skips_numerical_replay(tmp_path, monkeypatch):
    import horsehair.molecular_pipeline as pipeline
    from horsehair.core import atomic_json, canonical_hash
    config = {"molecular": {"gene_permutations": 10_000}, "inference": {"bootstrap_replicates": 10_000}}
    _benchmark_fixture(tmp_path, config, monkeypatch)
    freeze = tmp_path / "data" / "molecular" / "freeze" / "freeze.json"
    target = tmp_path / "results" / "interventions" / "target_provenance.json"
    atomic_json(freeze, {"synthetic_cached_measurement_test": True})
    atomic_json(target, {"synthetic_cached_measurement_test": True})
    method = pipeline._transfer_method(config)
    prospective = {"method": method, "full_config_sha256": canonical_hash(config),
        "freeze_sha256": pipeline.file_hash(freeze), "target_provenance_sha256": pipeline.file_hash(target),
        "tasks": [{"name": "molecular-transfer", "planned_work_units": 1, "maximum_job_work_units": 1, "concurrency": 1}]}
    manifest = pipeline._manifest(tmp_path)
    receipt = {"status": "native_molecular_transfer_workload_completed", "config_sha256": canonical_hash(config),
        "source_sha256": "synthetic-release", "source_manifest_hash": manifest["manifest_hash"],
        "method": method, "prospective_workload_sha256": canonical_hash(prospective),
        "slurm_job_id": "1200_7", "tasks": [{"slurm_job_id": "1200_7", "elapsed_seconds": 123.4}],
        **pipeline._result_proof([freeze, target])}
    pipeline._write_completed_stage(tmp_path / "state" / "molecular-workload-benchmark.json", receipt)
    monkeypatch.setattr(pipeline, "predict_native", lambda *_: pytest.fail("Completed numerical result must not be recomputed"))
    monkeypatch.setenv("SLURM_JOB_ID", "9999")
    result = pipeline.run(config, tmp_path, "benchmark-transfer")
    assert result["slurm_job_id"] == "1200_7"
    assert result["tasks"][0]["elapsed_seconds"] == 123.4
    assert result["scientific_stage_executed"] is False
    freeze.write_text("changed frozen input")
    with pytest.raises(MolecularError, match="proof is missing or changed"):
        pipeline.benchmark_transfer(config, tmp_path)
