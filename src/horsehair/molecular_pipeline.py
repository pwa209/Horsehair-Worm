"""Finite native molecular stages from pinned public references to frozen H3.

All input downloads belong to the Narval transfer stage. This module validates
existing sources and executes explicit installed scientific utilities under
Slurm; no dataset text supplies executable commands or scientific identities.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

import numpy as np
import pandas as pd

from .acquire import file_hash, safe_destination, validate_manifest, verify_file
from .core import atomic_json, canonical_hash, read_json, source_identity
from .molecular import (MolecularError, freeze_scores, parse_orthogroups,
    prepare_reference, read_fasta, reversible_scores, run_external_prediction,
    select_representative_proteins, transdecoder_gene_map, write_h3,
    write_table, write_target_family_map)

GO_ROOTS = {"neuronal": "GO:0050877", "circadian": "GO:0007623"}


def _open(path):
    return gzip.open(path, "rt", encoding="utf-8") if str(path).endswith(".gz") else Path(path).open(encoding="utf-8")


def parse_fly_relationships(path):
    """Resolve source FBpp/FBgn IDs from the deposited relationship table."""
    rows, no_protein = [], 0
    with _open(path) as source:
        for line in source:
            if not line.strip() or line.startswith("#"):
                continue
            genes = set(re.findall(r"(?<![A-Za-z0-9])FBgn\d{7}(?!\d)", line))
            proteins = set(re.findall(r"(?<![A-Za-z0-9])FBpp\d{7}(?!\d)", line))
            if not genes and not proteins:
                continue  # an explicit text header, not an identity row
            if len(genes) != 1:
                raise MolecularError("FlyBase relationship row lacks one unambiguous source gene")
            if not proteins:
                no_protein += 1
                continue
            rows.extend({"species": "fly", "gene_id": next(iter(genes)), "protein_id": protein} for protein in sorted(proteins))
    table = pd.DataFrame(rows).drop_duplicates()
    if table.empty or table.protein_id.duplicated().any():
        raise MolecularError("FlyBase protein-to-gene relationships are empty or ambiguous")
    return table, {"source_sha256": file_hash(path), "proteins": len(table), "genes": table.gene_id.nunique(), "source_rows_without_protein": no_protein}


def parse_fly_catalog(path):
    """Use the source's declared primary-ID/symbol columns, never guessed symbols."""
    columns, rows = None, []
    with _open(path) as source:
        for line in source:
            if not line.strip():
                continue
            fields = line.rstrip("\n").lstrip("#").split("\t")
            normalized = [re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") for value in fields]
            primary = [i for i, value in enumerate(normalized) if value in {"primary_fbgn", "primary_fbgn_id", "flybase_gene_id", "gene_id"}]
            if len(primary) == 1:
                symbol = [i for i, value in enumerate(normalized) if value in {"gene_symbol", "symbol", "flybase_gene_symbol"}]
                secondary = [i for i, value in enumerate(normalized) if value in {"secondary_fbgn", "secondary_fbgn_ids"}]
                columns = (primary[0], symbol[0] if len(symbol) == 1 else None, secondary[0] if len(secondary) == 1 else None)
                continue
            if line.startswith("#"):
                continue
            if columns is None:
                raise MolecularError("FlyBase catalog primary-ID header is unresolved; record its native schema rather than guessing")
            gene = fields[columns[0]]
            if not re.fullmatch(r"FBgn\d{7}", gene):
                raise MolecularError("FlyBase primary gene ID is not a source FBgn identifier")
            rows.append({"gene_id": gene, "gene_symbol": fields[columns[1]] if columns[1] is not None else "", "secondary_gene_ids": fields[columns[2]] if columns[2] is not None else "", "catalog_release": "FB2026_03"})
    table = pd.DataFrame(rows)
    if table.empty or table.gene_id.duplicated().any():
        raise MolecularError("FlyBase catalog has missing or duplicated primary IDs")
    return table


def ant_protein_map(gtf, proteins):
    """Map exact RefSeq protein_id attributes to the same GTF gene_id counts."""
    rows = []
    with _open(gtf) as source:
        for line in source:
            if line.startswith("#") or not line.strip():
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) != 9:
                raise MolecularError("Invalid native GTF feature row")
            attributes = dict(re.findall(r'(\w+) "([^\"]*)";', fields[8]))
            if attributes.get("protein_id"):
                if not attributes.get("gene_id"):
                    raise MolecularError("RefSeq protein feature lacks a gene_id")
                rows.append({"species": "ant", "protein_id": attributes["protein_id"], "gene_id": attributes["gene_id"]})
    table = pd.DataFrame(rows).drop_duplicates()
    if table.empty or table.protein_id.duplicated().any():
        raise MolecularError("Matching NCBI GTF protein-to-counted-gene map is empty or ambiguous")
    sequences = {name for name, _, _ in read_fasta(proteins)}
    absent = sequences - set(table.protein_id)
    if absent:
        raise MolecularError(f"Matching RefSeq GTF has no exact protein mapping for {len(absent)} source peptides")
    return table[table.protein_id.isin(sequences)].copy()


def go_gene_annotations(obo_path, gaf_path, gene_ids):
    """Fixed is_a/part_of descendant annotation; NOT GAF records are excluded."""
    parents, aliases, version, term = {}, {}, None, None
    with _open(obo_path) as source:
        for line in source:
            line = line.rstrip("\n")
            if line.startswith("data-version:"):
                version = line.split(":", 1)[1].strip()
            if line.startswith("["):
                term = None
            elif line.startswith("id: GO:"):
                term = line.split()[1]; parents[term] = set()
            elif term and line.startswith("alt_id: GO:"):
                aliases[line.split()[1]] = term
            elif term and line.startswith("is_a: GO:"):
                parents[term].add(line.split()[1])
            elif term and line.startswith("relationship: part_of GO:"):
                parents[term].add(line.split()[2])
    if not version or not set(GO_ROOTS.values()).issubset(parents):
        raise MolecularError("Pinned ontology release/version or fixed neuronal/circadian roots are absent")
    cache = {}
    def descendants_flag(identifier, visiting=None):
        identifier = aliases.get(identifier, identifier)
        if identifier in cache:
            return cache[identifier]
        visiting = set() if visiting is None else visiting
        if identifier in visiting:
            raise MolecularError("GO is_a/part_of cycle encountered")
        visiting = visiting | {identifier}
        value = {key for key, root in GO_ROOTS.items() if identifier == root}
        for parent in parents.get(identifier, set()):
            value |= descendants_flag(parent, visiting)
        cache[identifier] = value
        return value
    positive, annotated, unknown, rows = {}, set(), set(), 0
    genes = set(gene_ids)
    with _open(gaf_path) as source:
        for line in source:
            if line.startswith("!") or not line.strip():
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) < 15:
                raise MolecularError("Pinned FlyBase GO source is not standard GAF")
            if "NOT" in fields[3].split("|") or fields[1] not in genes or "taxon:7227" not in fields[12].split("|"):
                continue
            rows += 1; annotated.add(fields[1])
            if fields[4] not in parents and fields[4] not in aliases:
                unknown.add(fields[4]); continue
            positive.setdefault(fields[1], set()).update(descendants_flag(fields[4]))
    if not rows:
        raise MolecularError("No native Drosophila GAF gene annotations match the versioned catalog")
    table = pd.DataFrame({"gene_id": sorted(genes)})
    table["neuronal_circadian"] = [int(bool(positive.get(gene))) for gene in table.gene_id]
    table["has_go_annotation"] = table.gene_id.isin(annotated)
    return table, {"ontology_data_version": version, "roots": GO_ROOTS, "relations": ["is_a", "part_of"], "unknown_go_terms": sorted(unknown), "annotated_genes": len(annotated), "positive_neuronal_circadian_genes": int(table.neuronal_circadian.sum()), "interpretation": "Binary presence of a non-NOT fixed-root/descendant GO annotation; zero does not establish biological absence"}


def _manifest(root):
    manifest = read_json(root / "manifests" / "source_manifest.json")
    validate_manifest(manifest)
    return manifest


def _assets(root, source):
    manifest = _manifest(root)
    assets = [item for item in manifest["assets"] if item["source_id"] == source]
    if not assets:
        raise MolecularError(f"Pinned required source is absent: {source}")
    paths = {}
    for item in assets:
        path = safe_destination(root, item["relative_path"])
        verify_file(path, item)
        paths[item.get("reference_role", path.name)] = path
    return paths


def _decompress(source, output):
    temporary = output.with_suffix(output.suffix + ".tmp")
    output.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(source, "rb") as handle, temporary.open("wb") as target:
        shutil.copyfileobj(handle, target, 8 * 1024**2)
    temporary.replace(output)
    return output


def prepare_native_proteomes(config, root):
    root = Path(root)
    directory = root / "data" / "molecular" / "orthology"
    directory.mkdir(parents=True, exist_ok=True)
    references = prepare_reference(_manifest(root), root, root / "data" / "references")
    sources = _assets(root, "fly_reference")
    fly = _decompress(sources["protein_fasta"], directory / "fly_all_proteins.fa")
    fly_map, map_report = parse_fly_relationships(sources["gene_protein_map"])
    fly_sequences = {name for name, _, _ in read_fasta(fly)}
    if fly_sequences - set(fly_map.protein_id):
        raise MolecularError("Pinned FlyBase translation FASTA contains peptides without exact source gene relationships")
    fly_map = fly_map[fly_map.protein_id.isin(fly_sequences)]
    catalog = parse_fly_catalog(sources["gene_catalog"])
    if not set(fly_map.gene_id).issubset(catalog.gene_id):
        raise MolecularError("Pinned FlyBase protein/gene catalog namespaces disagree")
    go_annotations, go_report = go_gene_annotations(sources["go_ontology"], sources["go_annotations"], catalog.gene_id)
    catalog = catalog.merge(go_annotations, on="gene_id", validate="one_to_one")
    write_table(catalog, directory / "fly_gene_catalog.tsv")
    ant_map = ant_protein_map(references["gtf"], references["protein_fasta"])
    representative_maps = []
    proteomes = {}
    for species, fasta, mapping, version in [("ant", references["protein_fasta"], ant_map, "GCF_003651465.1:NCBI-Annotation-Release-100"), ("fly", fly, fly_map, "FB2026_03:dmel-r6.69")]:
        output = directory / (species + ".fa")
        representative_maps.append(select_representative_proteins(fasta, mapping, output))
        proteomes[species] = {"path": str(output.resolve()), "sha256": file_hash(output), "version": version}
    write_table(pd.concat(representative_maps, ignore_index=True), directory / "protein_gene_map.tsv")
    report = {"status": "native_ant_fly_proteomes_prepared", "proteomes": proteomes, "fly_map": map_report, "go": go_report, "fly_gene_catalog": str((directory / "fly_gene_catalog.tsv").resolve()), "source_manifest_hash": _manifest(root)["manifest_hash"]}
    atomic_json(directory / "proteomes.json", proteomes)
    atomic_json(directory / "reference_preparation.json", report)
    return report


def run_native_orthology(config, root):
    root = Path(root)
    directory = root / "data" / "molecular" / "orthology"
    runtime = root / "data" / "tool_runtime"
    qualification = read_json(runtime / "runtime_qualification.json")
    if qualification.get("status") != "runtime_qualified":
        raise MolecularError("OrthoFinder/TransDecoder runtime has not passed its actual example qualification")
    proteomes = read_json(directory / "proteomes.json")
    for item in proteomes.values():
        if file_hash(item["path"]) != item["sha256"]:
            raise MolecularError("Prepared native proteome changed before orthology")
    env = os.environ.copy(); env["PATH"] = str(runtime / "bin") + os.pathsep + env.get("PATH", "")
    threads = int(config["resources"].get("molecular_cores", 8))
    input_directory = directory / "of_inputs"
    input_directory.mkdir(exist_ok=True)
    for species, item in proteomes.items():
        shutil.copyfile(item["path"], input_directory / (species+".fa"))
    run = directory / "of_run"
    if run.exists():
        raise MolecularError("Existing OrthoFinder run requires its completed receipt or a new prospective namespace; never replay an ambiguous stage")
    started = time.monotonic()
    subprocess.run([str(runtime / "bin" / "orthofinder"), "-f", str(input_directory), "-t", str(threads), "-a", str(min(threads, 8)), "-S", "diamond", "-o", str(run)], env=env, check=True)
    candidates = list(run.rglob("Phylogenetic_Hierarchical_Orthogroups/N0.tsv"))
    if len(candidates) != 1:
        raise MolecularError("Actual OrthoFinder run did not yield one unambiguous root HOG table")
    protein_map = pd.read_csv(directory / "protein_gene_map.tsv", sep="\t")
    orthology, coverage = parse_orthogroups(candidates[0], protein_map)
    write_table(orthology, directory / "gene_orthology.tsv")
    atomic_json(directory / "orthology_coverage.json", coverage)
    family = write_target_family_map(orthology, directory / "fly_target_families.tsv")
    # Actual sequence identity supplies a declared orthology nuisance descriptor;
    # a one-to-one flag is not turned into fabricated continuous confidence.
    diamond = runtime / "OrthoFinder_source" / "scripts_of" / "bin" / "diamond"
    db = directory / "ant_diamond"
    subprocess.run([str(diamond), "makedb", "--in", proteomes["ant"]["path"], "--db", str(db)], check=True)
    alignment = directory / "fly_ant_identity.tsv"
    subprocess.run([str(diamond), "blastp", "--query", proteomes["fly"]["path"], "--db", str(db), "--out", str(alignment), "--outfmt", "6", "qseqid", "sseqid", "pident", "bitscore", "--threads", str(threads), "--max-target-seqs", "0", "--max-hsps", "1", "--sensitive", "--evalue", "1e-5"], check=True)
    if not alignment.is_file() or not alignment.stat().st_size:
        raise MolecularError("No actual ant–fly protein alignments for orthology descriptors")
    return {"status": "native_primary_ant_fly_orthology_computed", "elapsed_seconds": time.monotonic()-started, "coverage": coverage, "target_family_map": family, "source_result_paths": [str(candidates[0]), str(directory / "gene_orthology.tsv"), str(alignment)]}


def build_native_descriptors(config, root):
    root = Path(root); directory = root / "data" / "molecular" / "orthology"
    orthology = pd.read_csv(directory / "gene_orthology.tsv", sep="\t")
    one = orthology[orthology.one_to_one.astype(str).str.lower().isin(["true", "1"])]
    ant = one[one.species == "ant"][["gene_id", "orthogroup"]].rename(columns={"gene_id": "ant_gene_id"})
    fly = one[one.species == "fly"][["gene_id", "orthogroup"]]
    pairs = ant.merge(fly, on="orthogroup", validate="one_to_one")
    proteins = pd.read_csv(directory / "protein_gene_map.tsv", sep="\t")
    pairs = pairs.merge(proteins[proteins.species == "ant"][["gene_id", "protein_id"]].rename(columns={"gene_id": "ant_gene_id", "protein_id": "ant_protein"}), on="ant_gene_id", validate="one_to_one")
    pairs = pairs.merge(proteins[proteins.species == "fly"][["gene_id", "protein_id", "protein_length"]].rename(columns={"protein_id": "fly_protein"}), on="gene_id", validate="one_to_one")
    wanted = {(row.fly_protein, row.ant_protein) for row in pairs.itertuples()}
    best = {}
    for chunk in pd.read_csv(directory / "fly_ant_identity.tsv", sep="\t", names=["query", "subject", "identity", "bitscore"], chunksize=100000):
        for row in chunk.itertuples():
            key = (row.query, row.subject)
            if key in wanted and (key not in best or row.bitscore > best[key][1]):
                best[key] = (float(row.identity), float(row.bitscore))
    pairs["sequence_identity"] = [best.get((row.fly_protein, row.ant_protein), (np.nan, 0))[0]/100 for row in pairs.itertuples()]
    pairs["orthology_confidence"] = pairs.sequence_identity
    expression_path = root / "data" / "molecular" / "ant" / "deseq2" / "expression_descriptors.tsv"
    expression = pd.read_csv(expression_path, sep="\t").rename(columns={"gene_id": "ant_gene_id"})
    columns = ["ant_gene_id", "average_expression", "detection_rate", "generic_infection_effect"]
    if not set(columns).issubset(expression):
        raise MolecularError("Actual discovery-only ant expression descriptors are incomplete")
    pairs = pairs.merge(expression[columns], on="ant_gene_id", validate="one_to_one")
    catalog = pd.read_csv(directory / "fly_gene_catalog.tsv", sep="\t")
    pairs = pairs.merge(catalog[["gene_id", "neuronal_circadian"]], on="gene_id", validate="one_to_one")
    numeric = ["average_expression", "detection_rate", "generic_infection_effect", "protein_length", "orthology_confidence", "sequence_identity", "neuronal_circadian"]
    invalid = ~np.isfinite(pairs[numeric].to_numpy(dtype=float)).all(axis=1)
    excluded = pairs.loc[invalid, "gene_id"].tolist()
    descriptors = pairs.loc[~invalid, ["gene_id", *numeric]].copy()
    if descriptors.empty:
        raise MolecularError("No one-to-one genes have actual discovery/sequence/GO descriptors")
    write_table(descriptors, directory / "fly_gene_descriptors.tsv")
    report = {"status": "native_outcome_independent_descriptors", "excluded_genes_without_sequence_identity_or_expression": excluded, "genes": len(descriptors), "expression_source": "Ant mapped orthologue: discovery-only average normalized count, detection and generic infection contrast; no fly outcome expression is used", "orthology_confidence_definition": "Actual ant–fly representative-peptide identity fraction from the fixed DIAMOND alignment; descriptive similarity, not a calibrated orthology posterior", "neuron_circadian_roots": GO_ROOTS, "nuisance_descriptor_hash": canonical_hash(descriptors.fillna("").to_dict("records"))}
    atomic_json(directory / "descriptor_provenance.json", report)
    return report


def freeze_native_scores(config, root):
    root = Path(root)
    if (root / "results" / "interventions" / "gene_effects.tsv").exists() or (root / "results" / "interventions" / "target_provenance.json").exists():
        raise MolecularError("External gene outcomes already exist; a new outcome-independent primary freeze cannot be claimed retrospectively")
    directory = root / "data" / "molecular" / "orthology"
    build_native_descriptors(config, root)
    contrast_path = root / "data" / "molecular" / "ant" / "deseq2" / "phase_contrasts.tsv"
    audit_path = contrast_path.parent / "rna_analysis_audit.json"
    audit = read_json(audit_path)
    if audit.get("mode") != "ant" or audit.get("discovery_brains") != 48 or audit.get("heldout_brains") != 12 or audit.get("independent_brains") != 60:
        raise MolecularError("Molecular freeze requires actual 48-discovery/12-challenge ant count-model audit")
    orthology = pd.read_csv(directory / "gene_orthology.tsv", sep="\t")
    descriptors = pd.read_csv(directory / "fly_gene_descriptors.tsv", sep="\t")
    provenance = {"source_manifest_hash": _manifest(root)["manifest_hash"], "contrast_design_hash": canonical_hash(audit), "orthology_hash": canonical_hash(orthology.fillna("").to_dict("records")), "nuisance_descriptor_hash": canonical_hash(descriptors.fillna("").to_dict("records")), "fly_outcomes_inspected": False, "phase_contrast_path": str(contrast_path.resolve()), "phase_contrast_sha256": file_hash(contrast_path), "target_outcome_artifacts_absent_at_freeze": True}
    provenance.update(fly_gene_catalog_path=str((directory / "fly_gene_catalog.tsv").resolve()), fly_gene_catalog_sha256=file_hash(directory / "fly_gene_catalog.tsv"), target_family_map_path=str((directory / "fly_target_families.tsv").resolve()), target_family_map_sha256=file_hash(directory / "fly_target_families.tsv"))
    inputs = [contrast_path, audit_path, audit_path.parent.parent / "ant_counts.tsv", audit_path.parent.parent / "ant_samples.tsv", directory / "gene_orthology.tsv", directory / "fly_gene_descriptors.tsv", directory / "descriptor_provenance.json", directory / "reference_preparation.json", directory / "fly_target_families.tsv"]
    if not all(path.is_file() for path in inputs):
        raise MolecularError("Native molecular freeze source results are incomplete")
    provenance.update(source_result_paths=[str(path.resolve()) for path in inputs], source_result_hashes={str(path.resolve()): file_hash(path) for path in inputs})
    return freeze_scores(reversible_scores(pd.read_csv(contrast_path, sep="\t")), provenance, root / "data" / "molecular" / "freeze")


def run_mantis_orfs(config, root):
    """Independent exploratory ORFs; never gate the primary ant–fly test."""
    from .molecular import orf_plan, prepare_mantis
    root = Path(root); directory = root / "data" / "molecular" / "mantis_orfs"
    directory.mkdir(parents=True, exist_ok=True)
    prepare_mantis(root)
    assets = _assets(root, "mantis_rna")
    source = next((path for name, path in assets.items() if "SuperTranscripts_tenodera.fasta" in name), None)
    if source is None:
        raise MolecularError("Native mantis SuperTranscript FASTA is absent")
    fasta = _decompress(source, directory / "supertranscripts.fasta")
    counted = pd.read_csv(root / "data" / "molecular" / "mantis" / "mantis_counts.tsv", sep="\t").gene_id.astype(str)
    names = [name for name, _, _ in read_fasta(fasta)]
    if not set(names).issubset(counted):
        raise MolecularError("Source SuperTranscript headers do not exactly identify counted mantis genes; explicit native mapping is required")
    mapping = pd.DataFrame({"transcript_id": names, "gene_id": names})
    write_table(mapping, directory / "transcript_gene_map.tsv")
    plan = orf_plan(fasta, directory / "transcript_gene_map.tsv", directory / "transdecoder")
    env = os.environ.copy(); env["PATH"] = str(root / "data" / "tool_runtime" / "bin") + os.pathsep + env.get("PATH", "")
    subprocess.run(["bash", plan["script"]], env=env, check=True)
    peptides = directory / "transdecoder" / "mantis_supertranscripts.fasta.transdecoder.pep"
    protein_map, coverage = transdecoder_gene_map(peptides, mapping)
    representative = select_representative_proteins(peptides, protein_map, directory / "mantis.fa")
    write_table(representative, directory / "protein_gene_map.tsv")
    return {"status": "native_mantis_orfs_computed", "coverage": coverage, "proteome_sha256": file_hash(directory / "mantis.fa"), "scope": "Independent mantis corroboration; primary ant–fly freeze is unchanged"}


def prepare_primary(config, root, *, include_freeze=False):
    """Execute the actual two-species source analysis, optionally then freeze."""
    preparation = prepare_native_proteomes(config, root)
    orthology = run_native_orthology(config, root)
    result = {"status": "native_primary_orthology_prepared", "preparation": preparation, "orthology": orthology, "inference_performed": False}
    if include_freeze:
        result["freeze"] = freeze_native_scores(config, root)
        result["status"] = "native_primary_molecular_score_frozen"
    return result


def benchmark_primary(config, root):
    """A bounded initial full-native benchmark; no score/outcome inference.

    The caller reserves its explicit Slurm CPU/RAM/walltime allocation. Source
    preparation and complete two-species OrthoFinder/DIAMOND runs are real
    reusable results if they finish. They do not claim H3 or invent a forecast.
    """
    root = Path(root); started = time.monotonic()
    result = prepare_primary(config, root)
    directory = root / "data" / "molecular" / "orthology"
    inputs = [root / "manifests" / "source_manifest.json", root / "data" / "tool_runtime" / "runtime_qualification.json", directory / "proteomes.json", directory / "protein_gene_map.tsv", directory / "gene_orthology.tsv", directory / "fly_ant_identity.tsv", directory / "fly_gene_catalog.tsv", directory / "fly_target_families.tsv"]
    proteomes = read_json(directory / "proteomes.json")
    protein_counts = {species: len(read_fasta(item["path"])) for species, item in proteomes.items()}
    result.update(status="native_primary_molecular_benchmark_completed", config_sha256=canonical_hash(config), source_sha256=source_identity(Path(__file__).resolve().parents[2])["sha256"], slurm_job_id=os.environ.get("SLURM_JOB_ID"), allocated_cpus=int(os.environ["SLURM_CPUS_PER_TASK"]) if os.environ.get("SLURM_CPUS_PER_TASK") else None, elapsed_seconds=time.monotonic()-started, working_bytes=sum(path.stat().st_size for path in directory.rglob("*") if path.is_file()), working_files=sum(path.is_file() for path in directory.rglob("*")), protein_counts=protein_counts, measured_work_units=1, work_unit_definition="Complete fixed-release ant/fly representative proteomes, source GO/catalog validation, full OrthoFinder and descriptive DIAMOND pair identities", benchmark_uses_full_primary_numerics=True, full_native_workload_completed=True, confirmatory_claim=False, score_frozen=False, external_outcomes_evaluated=False, source_result_paths=[str(path.resolve()) for path in inputs], source_result_hashes={str(path.resolve()): file_hash(path) for path in inputs})
    atomic_json(root / "state" / "molecular-primary-benchmark.json", result)
    return result


def run_mantis_orthology(config, root):
    """Three-species corroboration in a separate namespace, after actual ORFs."""
    from .molecular import orthology_plan
    root = Path(root)
    directory = root / "data" / "molecular" / "orthology_mantis"
    directory.mkdir(parents=True, exist_ok=True)
    primary = root / "data" / "molecular" / "orthology"
    mantis = root / "data" / "molecular" / "mantis_orfs"
    proteomes = read_json(primary / "proteomes.json")
    proteomes["mantis"] = {"path": str((mantis / "mantis.fa").resolve()), "sha256": file_hash(mantis / "mantis.fa"), "version": "GSE237182:TransDecoder-5.7.1:m100:longest-source-gene"}
    plan = orthology_plan(proteomes, directory, threads=int(config["resources"].get("molecular_cores", 8)))
    env = os.environ.copy(); env["PATH"] = str(root / "data" / "tool_runtime" / "bin") + os.pathsep + env.get("PATH", "")
    subprocess.run(["bash", str(directory / "orthofinder.sh")], env=env, check=True)
    candidates = list((directory / "proteomes").rglob("Phylogenetic_Hierarchical_Orthogroups/N0.tsv"))
    if len(candidates) != 1:
        raise MolecularError("Three-species corroboration did not yield one actual HOG table")
    mapping = pd.concat([pd.read_csv(primary / "protein_gene_map.tsv", sep="\t"), pd.read_csv(mantis / "protein_gene_map.tsv", sep="\t")], ignore_index=True)
    orthology, coverage = parse_orthogroups(candidates[0], mapping)
    write_table(orthology, directory / "gene_orthology.tsv")
    atomic_json(directory / "orthology_coverage.json", coverage)
    return {"status": "native_three_species_corroboration_computed", "coverage": coverage, "plan": plan, "scope": "Exploratory three-species mappings; frozen primary ant–fly analysis unchanged"}


def run(config, root, stage):
    root = Path(root); started = time.monotonic()
    if stage == "prepare":
        result = prepare_native_proteomes(config, root)
    elif stage == "orthology":
        result = run_native_orthology(config, root)
    elif stage == "freeze":
        result = freeze_native_scores(config, root)
    elif stage == "mantis-orfs":
        result = run_mantis_orfs(config, root)
    elif stage == "mantis-orthology":
        result = run_mantis_orthology(config, root)
    elif stage == "benchmark-primary":
        result = benchmark_primary(config, root)
    elif stage == "predict":
        directory = root / "data" / "molecular" / "orthology"
        output = root / "results" / "molecular" / "prediction"
        run_external_prediction(config, root, genes=root / "results" / "interventions" / "gene_effects.tsv", target_provenance=root / "results" / "interventions" / "target_provenance.json", orthology=directory / "gene_orthology.tsv", descriptors=directory / "fly_gene_descriptors.tsv", freeze_path=root / "data" / "molecular" / "freeze" / "freeze.json", output=output)
        result = write_h3(config, root, output / "external_prediction.json")
    else:
        raise MolecularError("Unknown finite molecular stage")
    result.update(config_sha256=canonical_hash(config), source_sha256=source_identity(Path(__file__).resolve().parents[2])["sha256"], stage=stage, stage_elapsed_seconds=time.monotonic()-started, slurm_job_id=os.environ.get("SLURM_JOB_ID"), scientific_stage_executed=True)
    atomic_json(root / "state" / f"molecular-{stage}.json", result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True); parser.add_argument("--root", required=True)
    parser.add_argument("--stage", required=True, choices=["prepare", "orthology", "freeze", "predict", "mantis-orfs", "mantis-orthology", "benchmark-primary"])
    args = parser.parse_args(argv)
    result = run(read_json(args.config), args.root, args.stage)
    print(json.dumps({"stage": args.stage, "status": result["status"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
