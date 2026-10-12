import json

import numpy as np
import pandas as pd
import pytest
from scipy.stats import norm

from horsehair.molecular import (
    BASELINE_COLUMNS, MolecularError, ant_crosswalk, collapse_technical_counts,
    external_prediction, freeze_scores, grouped_permutation_indices,
    parse_orthogroups, parse_strandedness, reversible_scores, rna_plan,
    select_representative_proteins, star_strandedness, transdecoder_gene_map,
    validate_counts,
)
from horsehair.molecular import run_external_prediction, tool_runtime_manifest, validate_freeze, write_h3, write_target_family_map
from horsehair.acquire import asset, atomic_json, canonical_hash, file_hash, validate_manifest


def ant_fixture():
    rows, samples = [], {}
    groups = [(phase, infection) for phase in ["10C", "25C", "wayup", "waydown"] for infection in ["inf", "Uninf"]] + [("refuseAttach", "inf"), ("refuseDetach", "inf")]
    for group_index, (phase, infection) in enumerate(groups):
        for rep in range(1, 7):
            sample = f"SAM{group_index}_{rep}"
            alias = f"2021-{phase}-{infection}-{rep}-{rep+1}"
            rows.append({"run_accession": f"RUN{group_index}_{rep}", "sample_accession": sample, "sample_alias": alias, "scientific_name": "Formica aserva", "library_name": f"LIB{group_index}_{rep}", "library_layout": "PAIRED", "fastq_ftp": f"ena/run{group_index}_{rep}_1.fastq.gz;ena/run{group_index}_{rep}_2.fastq.gz", "fastq_md5": "a"*32+";"+"b"*32, "fastq_bytes": "100;100"})
            samples[sample] = {"accession": sample, "alias": alias, "scientific_name": "Formica aserva", "attributes": {"tissue": "Brain", "infection_status": "Infected" if infection == "inf" else "Uninfected", "replicate_number": str(rep), "collection_date": "2021-06-07"}}
    return rows, samples


def test_ant_cohort_has_48_discovery_and_12_heldout_brains():
    rows, samples = ant_fixture()
    rows.append({"run_accession": "PARASITE", "sample_accession": "PARASITE_SAMPLE", "scientific_name": "Dicrocoelium dendriticum"})
    frame, audit = ant_crosswalk(rows, samples)
    assert audit["biological_brains"] == 60
    assert audit["discovery_brains"] == 48
    assert audit["challenge_brains"] == 12
    assert frame.loc[frame.run_id == "PARASITE", "included"].item() == False


def test_ant_missing_metadata_or_duplicate_replicates_fail():
    rows, samples = ant_fixture()
    samples.pop(rows[0]["sample_accession"])
    with pytest.raises(MolecularError, match="Missing biological"):
        ant_crosswalk(rows, samples)
    rows, samples = ant_fixture()
    samples[rows[0]["sample_accession"]]["attributes"]["infection_status"] = "Uninfected"
    with pytest.raises(MolecularError, match="disagreement"):
        ant_crosswalk(rows, samples)


def test_technical_runs_collapse_but_brains_never_merge_by_treatment():
    counts = pd.DataFrame({"r1": [1, 2], "r2": [3, 4], "r3": [5, 6]}, index=["g1", "g2"])
    samples = pd.DataFrame({"count_column": ["r1", "r2", "r3"], "biological_id": ["brain1", "brain1", "brain2"], "technical_library_id": ["lib1", "lib1", "lib2"], "group": ["same"]*3})
    merged, metadata = collapse_technical_counts(counts, samples)
    assert merged["brain1"].tolist() == [4, 6]
    assert merged["brain2"].tolist() == [5, 6]
    samples.loc[1, "technical_library_id"] = "different_preparation"
    with pytest.raises(MolecularError, match="collapse"):
        collapse_technical_counts(counts, samples)


@pytest.mark.parametrize("invalid", [[[-1, 2]], [[0.2, 2]], [[np.nan, 2]], [[0, 2]]])
def test_count_validation_rejects_tpm_negative_missing_and_empty_libraries(invalid):
    with pytest.raises(MolecularError):
        validate_counts(pd.DataFrame(invalid, index=["g"], columns=["a", "b"]))


def test_conjunction_score_does_not_assume_independent_contrasts():
    contrasts = pd.DataFrame({"gene_id": ["opposite", "same"], "on_log2fc": [3, 3], "on_se": [1, 1], "off_log2fc": [-2, 2], "off_se": [1, 1]})
    score = reversible_scores(contrasts)
    assert score.loc[0, "reversible_p"] == pytest.approx(2*norm.sf(2))
    assert score.loc[1, "reversible_p"] == 1
    assert score.loc[0, "reversible_score"] > score.loc[1, "reversible_score"]


def test_score_freeze_refuses_target_information(tmp_path):
    frame = pd.DataFrame({"gene_id": ["a"], "reversible_score": [1.0]})
    provenance = {key: "hash" for key in ["source_manifest_hash", "contrast_design_hash", "orthology_hash", "nuisance_descriptor_hash"]}
    with pytest.raises(MolecularError, match="outcomes"):
        freeze_scores(frame, provenance, tmp_path / "bad")
    provenance["fly_outcomes_inspected"] = False
    freeze_scores(frame, provenance, tmp_path / "good")
    with pytest.raises(MolecularError, match="exists"):
        freeze_scores(frame, provenance, tmp_path / "good")


def test_strand_inference_checks_directional_library(tmp_path):
    text = 'Fraction of reads explained by "1++,1--,2+-,2-+": 0.03\nFraction of reads explained by "1+-,1-+,2++,2--": 0.90'
    assert parse_strandedness(text)["featurecounts_code"] == 2
    with pytest.raises(MolecularError, match="conflicts"):
        parse_strandedness(text, expected="forward")
    path = tmp_path / "star.tsv"
    path.write_text("N_unmapped\t2\t2\t2\ng1\t100\t3\t97\ng2\t200\t6\t194\n")
    assert star_strandedness(path)["featurecounts_code"] == 2


def test_rna_pilot_plan_uses_actual_mates_and_preserves_source_crop(tmp_path):
    rows, samples = ant_fixture()
    crosswalk, _ = ant_crosswalk(rows, samples)
    ref = {"assembly": "GCA_003651465.1", "genome_fasta": "/refs/genome.fa", "gtf": "/refs/genes.gtf", "star_index": "/refs/index", "reference_manifest_hash": "frozen"}
    plan = rna_plan(crosswalk, ref, "/scratch/study", tmp_path, pilot=2, preprocessing="published")
    assert len(plan["jobs"]) == 2
    for job in plan["jobs"]:
        text = (tmp_path / ("rna_"+job["biological_id"]+".sh")).read_text()
        assert "CROP:97 HEADCROP:13" in text
        assert "--countReadPairs" in text
        assert "--star-counts" in text
        assert job["cohort"] == "discovery"


def test_representative_orf_and_orthology_coverage_require_source_ids(tmp_path):
    proteins = tmp_path / "protein.fa"
    proteins.write_text(">tx1.p1\nMAAA\n>tx1.p2\nMAAAAAA\n>tx2.p1\nMKKK\n")
    transcript_map = pd.DataFrame({"transcript_id": ["tx1", "tx2"], "gene_id": ["gene1", "gene2"]})
    mapping, coverage = transdecoder_gene_map(proteins, transcript_map)
    representatives = select_representative_proteins(proteins, mapping, tmp_path / "representatives.fa")
    assert representatives.loc[representatives.gene_id == "gene1", "protein_id"].item() == "tx1.p2"
    assert coverage["mapping_fraction"] == 1
    orth = tmp_path / "N0.tsv"
    orth.write_text("Orthogroup\tant\tmantis\tfly\nOG1\tA\ttx1.p2\tF\n")
    extra = pd.DataFrame({"species": ["ant", "fly"], "protein_id": ["A", "F"], "gene_id": ["antgene", "flygene"]})
    frame, report = parse_orthogroups(orth, pd.concat([representatives, extra], ignore_index=True))
    assert frame.one_to_one.all()
    assert report["coverage"]["mantis"]["mapped_genes"] == 1


def gene_frame(groups=24):
    rng = np.random.default_rng(11)
    frame = pd.DataFrame({"gene_id": [f"g{i}" for i in range(2*groups)], "orthogroup": np.repeat([f"OG{i}" for i in range(groups)], 2), "effect": rng.normal(size=2*groups), "effect_se": np.ones(2*groups), "reversible_score": rng.normal(size=2*groups), "average_expression": np.ones(2*groups), "detection_rate": np.ones(2*groups), "protein_length": rng.integers(100,500,size=2*groups), "orthology_confidence": np.ones(2*groups), "generic_infection_effect": rng.normal(size=2*groups), "neuronal_circadian": np.zeros(2*groups)})
    return frame


def test_permutation_moves_whole_related_gene_blocks():
    frame = gene_frame()
    indices, movable = grouped_permutation_indices(frame, np.random.default_rng(5))
    assert movable == len(frame)
    assert sorted(indices) == list(range(len(frame)))
    for group in frame.orthogroup.unique():
        recipient = frame.index[frame.orthogroup == group]
        assert frame.iloc[indices[recipient]].orthogroup.nunique() == 1


def test_external_prediction_includes_true_baseline_and_rejects_duplicate_gene():
    frame = gene_frame()
    frozen = {"status": "frozen_before_external_outcomes", "freeze_hash": "frozen"}
    report = external_prediction(frame, frozen, permutations=20, bootstrap_replicates=20)
    assert report["permutations"] == 20
    assert report["baseline_columns"] == BASELINE_COLUMNS
    assert 0 < report["permutation_p"] <= 1
    with pytest.raises(MolecularError, match="unique gene"):
        external_prediction(pd.concat([frame, frame.iloc[[0]]]), frozen, permutations=20)


def test_primary_ant_fly_transfer_does_not_require_mantis_orf(tmp_path):
    source = tmp_path / "N0.tsv"
    source.write_text("Orthogroup\tant\tmantis\tfly\nOG1\tA\t\tF\n")
    protein_map = pd.DataFrame({"species": ["ant", "fly"], "protein_id": ["A", "F"], "gene_id": ["ant_gene", "fly_gene"]})
    frame, coverage = parse_orthogroups(source, protein_map)
    assert frame.ant_fly_one_to_one.all()
    assert not frame.three_species_one_to_one.any()
    assert coverage["coverage"]["mantis"]["mapped_genes"] == 0
    report = write_target_family_map(frame, tmp_path / "families.tsv")
    assert report["fly_genes"] == 1 and report["fly_outcomes_inspected"] is False
    with pytest.raises(MolecularError, match="already exists"):
        write_target_family_map(frame, tmp_path / "families.tsv")


def test_pinned_software_manifest_is_a_valid_bounded_transfer_plan():
    manifest = tool_runtime_manifest()
    validate_manifest(manifest)
    assert sum(item["bytes"] for item in manifest["assets"]) < 32_000_000
    assert all(item["provider_checksum"].startswith("sha256:") for item in manifest["assets"])
    assert manifest["sources"]["tool_runtime"]["orthofinder"]["version"] == "2.5.5"


def test_h3_actual_computation_proof_and_changed_inputs_gate(tmp_path):
    root = tmp_path / "study"
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    frame = gene_frame()
    manifest = {"schema_version": 1, "assets": [asset(source="fixture", name="fixture.tsv", url="https://example.org/fixture.tsv", version="test", rights="synthetic fixture")], "sources": {"fixture": "synthetic test only"}}
    manifest["manifest_hash"] = canonical_hash(manifest)
    atomic_json(root / "manifests" / "source_manifest.json", manifest)
    orthology = pd.DataFrame([{"species": species, "gene_id": ("a" if species == "ant" else "g")+str(i), "orthogroup": "transfer"+str(i), "one_to_one": True} for i in range(len(frame)) for species in ["ant", "fly"]])
    descriptors = frame[["gene_id", *BASELINE_COLUMNS]].copy()
    contrasts = pd.DataFrame({"gene_id": ["a"+str(i) for i in range(len(frame))], "on_log2fc": np.abs(frame.reversible_score)+.1, "on_se": np.ones(len(frame)), "off_log2fc": -np.abs(frame.reversible_score)-.1, "off_se": np.ones(len(frame))})
    for name, table in [("genes", frame[["gene_id", "effect", "effect_se", "orthogroup"]]), ("orthology", orthology), ("descriptors", descriptors), ("contrasts", contrasts), ("families", frame[["gene_id", "orthogroup"]])]:
        table.to_csv(inputs / (name+".tsv"), sep="\t", index=False)
    source = inputs / "screen_source.tsv"
    source.write_text("synthetic shared-control test fixture\n")
    proof = {"validated_gene_ids": True, "shared_control_covariance_preserved": True, "reagents_clustered_by_gene": True, "target_table_sha256": file_hash(inputs / "genes.tsv"), "target_family_map_path": str(inputs / "families.tsv"), "target_family_map_sha256": file_hash(inputs / "families.tsv"), "source_result_paths": [str(source)]}
    atomic_json(inputs / "target_provenance.json", proof)
    covariance_path = inputs / "gene_covariance.npy"
    np.save(covariance_path, np.eye(len(frame))*.9 + np.ones((len(frame), len(frame)))*.1)
    order_path = inputs / "gene_covariance_order.json"
    atomic_json(order_path, {"gene_id": frame.gene_id.tolist()})
    proof.update(gene_covariance_path=str(covariance_path), gene_covariance_sha256=file_hash(covariance_path), gene_covariance_order_path=str(order_path), gene_covariance_order_sha256=file_hash(order_path))
    frame[["gene_id"]].to_csv(inputs / "catalog.tsv", sep="\t", index=False)
    provenance = {"fly_outcomes_inspected": False, "source_manifest_hash": manifest["manifest_hash"], "contrast_design_hash": "synthetic-design", "orthology_hash": canonical_hash(pd.read_csv(inputs / "orthology.tsv", sep="\t").fillna("").to_dict("records")), "nuisance_descriptor_hash": canonical_hash(pd.read_csv(inputs / "descriptors.tsv", sep="\t").fillna("").to_dict("records")), "phase_contrast_path": str(inputs / "contrasts.tsv"), "phase_contrast_sha256": file_hash(inputs / "contrasts.tsv"), "fly_gene_catalog_path": str(inputs / "catalog.tsv"), "fly_gene_catalog_sha256": file_hash(inputs / "catalog.tsv"), "target_family_map_sha256": file_hash(inputs / "families.tsv")}
    frozen = freeze_scores(reversible_scores(contrasts), provenance, inputs / "freeze")
    proof["molecular_freeze_hash"] = frozen["freeze_hash"]
    atomic_json(inputs / "target_provenance.json", proof)
    config = {"molecular": {"gene_permutations": 20}, "inference": {"bootstrap_replicates": 20, "alpha": .05}}
    output = root / "results" / "molecular"
    report = run_external_prediction(config, root, genes=inputs / "genes.tsv", orthology=inputs / "orthology.tsv", descriptors=inputs / "descriptors.tsv", freeze_path=inputs / "freeze" / "freeze.json", target_provenance=inputs / "target_provenance.json", output=output)
    assert report["status"] == "external_gene_prediction_computed"
    assert report["shared_control_covariance_propagated"] is True
    result = write_h3(config, root, output / "external_prediction.json")
    assert result["claim_id"] == "H3" and result["primary_family_pending"]
    figure = json.loads((root / "results" / "figure_inputs" / "04.json").read_text())
    assert figure["evidence_class"] == "biological_reanalysis" and len(figure["panels"]) == 2
    source.write_text("changed test source\n")
    with pytest.raises(MolecularError, match="missing or changed"):
        write_h3(config, root, output / "external_prediction.json")


def test_freeze_self_hash_cannot_be_relabelled(tmp_path):
    provenance = {key: "hash" for key in ["source_manifest_hash", "contrast_design_hash", "orthology_hash", "nuisance_descriptor_hash"]}
    provenance["fly_outcomes_inspected"] = False
    freeze_scores(pd.DataFrame({"gene_id": ["a"], "reversible_score": [1.]}), provenance, tmp_path)
    path = tmp_path / "freeze.json"
    record = json.loads(path.read_text())
    record["provenance"]["source_manifest_hash"] = "changed"
    atomic_json(path, record)
    with pytest.raises(MolecularError, match="provenance changed"):
        validate_freeze(path)


def test_h3_uncertainty_rejects_an_invalid_shared_control_covariance():
    frame = gene_frame()
    frozen = {"status": "frozen_before_external_outcomes", "freeze_hash": "fixture"}
    covariance = np.eye(len(frame))
    covariance[0, 1] = covariance[1, 0] = 2
    with pytest.raises(MolecularError, match="positive semidefinite"):
        external_prediction(frame, frozen, permutations=20, bootstrap_replicates=20, outcome_covariance=covariance)
