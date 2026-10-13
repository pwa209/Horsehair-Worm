"""Auditable molecular pipeline and guarded external gene prediction.

No biological labels are invented from treatment names or file order. Public ENA
aliases and BioSample attributes resolve the 2021 ant cohort; later scientific
adapters require explicit column/orthology maps. Functions expose the same stages
as the module CLI so scheduler orchestration need not execute arbitrary configs.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import math
import os
import posixpath
import re
import shlex
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import norm, rankdata, spearmanr

from .acquire import AcquisitionError, atomic_json, canonical_hash, ena_fastq_assets, fetch_ena_runs, file_hash, http_session, utc_now, validate_manifest
from .core import native_job_id, source_identity

BASELINE_COLUMNS = ["average_expression", "detection_rate", "protein_length", "orthology_confidence", "generic_infection_effect", "neuronal_circadian"]
PHASE_CODES = {"10C": "attached", "25C": "detached", "wayup": "pre", "waydown": "newly_detached", "refuseAttach": "refused_attach", "refuseDetach": "refused_detach"}
TOOL_RUNTIME = {
    "orthofinder": {"version": "2.5.5", "commit": "c85ab1df311fb4fe8d11c613382ad8914de8d896", "url": "https://github.com/davidemms/OrthoFinder/releases/download/2.5.5/OrthoFinder_source.tar.gz", "bytes": 15588113, "sha256": "43d034a66a13adba8872a0d4a76e32c25305a7fae638754adb61c37a3f957bd9", "directory": "OrthoFinder_source", "filename": "OrthoFinder_source_2.5.5.tar.gz"},
    "transdecoder": {"version": "5.7.1", "commit": "5791fde6a37903f803cc4f51f0882eca200ce437", "url": "https://codeload.github.com/TransDecoder/TransDecoder/tar.gz/5791fde6a37903f803cc4f51f0882eca200ce437", "bytes": 15752765, "sha256": "3f9bf74dd25e7573ab6cb072e399ab124c4d7e7929a8467c411e553a8a5a9964", "directory": "TransDecoder-5791fde6a37903f803cc4f51f0882eca200ce437", "filename": "TransDecoder_5.7.1.tar.gz"},
}
SITE_PERL = "/cvmfs/soft.computecanada.ca/easybuild/software/2020/Core/perl/5.30.2/bin/perl"
ORTHOFINDER_PYTHON_PACKAGES = {"numpy": "1.26.4", "scipy": "1.11.2"}
RUNTIME_QUALIFICATION_RECIPE = {
    "version": 3, "orthofinder_fixture_proteins": 8,
    "orthofinder_fixture_species": ["qualification_a", "qualification_b"],
    "orthofinder_fixture_definition": "Identical copies of eight distinct 80-400-aa proteins of different lengths from the first pinned official ExampleData FASTA, plus a known duplicate of the first protein in each species to exercise a four-gene orthogroup; software identity check, not a species experiment",
    "orthofinder_fixture_paralogs_per_species": 1,
    "orthofinder_seconds": 300, "transdecoder_longorfs_seconds": 300,
    "transdecoder_predict_seconds": 300, "binary_seconds": 30,
    "threads": 2, "library_threads": 1, "transdecoder_fixture": "sample_data/simple_transcriptome_target/Trinity.fasta.gz",
    "transdecoder_max_uncompressed_bytes": 10 * 1024**2,
    "fastme_numeric_fixture": "Four utility-only labels, distances AB=CD=1 and all cross-pair distances=3; require the AB|CD split",
    "biological_workload_measured": False,
    "worker_diagnostics": "Per-PID C-level faulthandler traces for the verified OrthoFinder launcher; process-group state snapshots at 15/45/120/240 seconds before the unchanged 300-second ceiling",
    "upstream_fixture_constraint": "Unmodified OrthoFinder 2.5.5 OGs() selects groups with at least four genes for dendroblast distances; an all-two-gene fixture leaves cleanup variable m undefined. The known four-gene utility group exercises this branch. This qualification does not replace the mandatory full ant/fly proteome benchmark.",
    "orthofinder_python_packages": ORTHOFINDER_PYTHON_PACKAGES,
}


class MolecularError(RuntimeError):
    pass


def write_table(frame: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    frame.to_csv(temporary, sep="\t", index=False)
    temporary.replace(path)


def parse_ena_sample(xml_text: str) -> dict:
    root = ET.fromstring(xml_text)
    sample = root.find(".//SAMPLE")
    if sample is None:
        raise MolecularError("ENA sample XML has no SAMPLE record")
    attrs = {element.findtext("TAG", ""): element.findtext("VALUE", "") for element in sample.findall(".//SAMPLE_ATTRIBUTE")}
    return {"accession": sample.attrib["accession"], "alias": sample.attrib.get("alias", ""), "scientific_name": sample.findtext(".//SCIENTIFIC_NAME", ""), "attributes": attrs}


def fetch_ant_sample_metadata(rows: list[dict], workers: int = 4) -> dict[str, dict]:
    accessions = sorted({row["sample_accession"] for row in rows if row.get("scientific_name") == "Formica aserva"})
    def fetch(accession: str) -> tuple[str, dict]:
        response = http_session().get(f"https://www.ebi.ac.uk/ena/browser/api/xml/{accession}", timeout=(20, 90))
        response.raise_for_status()
        value = parse_ena_sample(response.text)
        value["metadata_url"] = response.url
        value["retrieved_utc"] = utc_now()
        return accession, value
    with ThreadPoolExecutor(max_workers=workers) as executor:
        return dict(executor.map(fetch, accessions))


def ant_crosswalk(rows: list[dict], samples: dict[str, dict], *, expected_brains: int = 60) -> tuple[pd.DataFrame, dict]:
    """Resolve, count and retain every included/excluded project run.

    This depositor's complete alias is retained. Its final integer is never
    interpreted as a lane or stripped to manufacture duplicate brains.
    """
    records = []
    seen_runs = set()
    for row in rows:
        run, accession = row["run_accession"], row["sample_accession"]
        if run in seen_runs:
            raise MolecularError(f"Duplicate sequencing run: {run}")
        seen_runs.add(run)
        record = {"study": "PRJNA918668", "run_id": run, "experiment_id": row.get("experiment_accession", ""), "source_id": accession, "biological_id": accession, "technical_library_id": row.get("library_name", ""), "sample_alias": row.get("sample_alias", ""), "organism": row.get("scientific_name", ""), "read_count": row.get("read_count", ""), "base_count": row.get("base_count", ""), "fastq_urls": row.get("fastq_ftp", ""), "fastq_md5": row.get("fastq_md5", ""), "fastq_bytes": row.get("fastq_bytes", ""), "included": False, "exclusion_reason": "", "cohort": "excluded"}
        if record["organism"] != "Formica aserva":
            record["exclusion_reason"] = "nonhost_organism"
            records.append(record)
            continue
        if accession not in samples:
            raise MolecularError(f"Missing biological sample attributes for {accession}")
        sample = samples[accession]
        if sample["accession"] != accession or sample["scientific_name"] != "Formica aserva":
            raise MolecularError("ENA sample identity mismatch")
        alias = sample["alias"]
        if alias != record["sample_alias"]:
            raise MolecularError(f"Alias disagreement for {accession}")
        match = re.fullmatch(r"(2019|2021)-(10C|25C|wayup|waydown|refuseAttach|refuseDetach)-(Unif|Uninf|inf)-(\d+)-(\d+)", alias)
        if not match:
            raise MolecularError(f"Unrecognized deposited ant alias: {alias}; explicit mapping required")
        year, phase_code, infection_code, replicate, source_suffix = match.groups()
        attrs = sample["attributes"]
        tissue = attrs.get("tissue", "").strip().lower()
        if tissue != "brain":
            record["exclusion_reason"] = "nonbrain_tissue"
            records.append(record)
            continue
        infection = "infected" if infection_code == "inf" else "uninfected"
        attribute_infection = attrs.get("infection_status", "").strip().lower()
        if attribute_infection != infection or str(attrs.get("replicate_number", "")) != replicate:
            raise MolecularError(f"Alias/attribute infection or replicate disagreement for {accession}")
        collection_date = attrs.get("collection_date", "")
        if not collection_date.startswith(year):
            raise MolecularError(f"Alias/collection-year disagreement for {accession}")
        phase = PHASE_CODES[phase_code]
        if phase.startswith("refused") and infection != "infected":
            raise MolecularError("Refusal samples cannot be inferred for uninfected controls")
        record.update(tissue="brain", infection=infection, phase=phase, group=f"{infection}_{phase}", collection_year=year, collection_date=collection_date, batch=year, replicate_number=int(replicate), source_suffix=source_suffix, metadata_url=sample.get("metadata_url", ""))
        if year != "2021":
            record["exclusion_reason"] = "outside_published_2021_cohort"
        else:
            record.update(included=True, cohort="challenge" if phase.startswith("refused") else "discovery")
        if not record["technical_library_id"]:
            raise MolecularError(f"Missing library identifier for {run}")
        if row.get("library_layout") != "PAIRED":
            raise MolecularError(f"Unexpected single-end source run: {run}")
        records.append(record)
    frame = pd.DataFrame(records).sort_values("run_id").reset_index(drop=True)
    included = frame[frame["included"]]
    biology = included.drop_duplicates("biological_id")
    for key, group in included.groupby("biological_id"):
        for column in ["sample_alias", "infection", "phase", "collection_year", "technical_library_id"]:
            if group[column].nunique(dropna=False) != 1:
                raise MolecularError(f"Ambiguous biological identity or multiple libraries for {key}: {column}")
    if len(biology) != expected_brains:
        raise MolecularError(f"Published cohort requires {expected_brains} brains; unambiguous mapping yielded {len(biology)}")
    if biology.duplicated(["collection_year", "group", "replicate_number"]).any():
        raise MolecularError("Distinct BioSamples have duplicate primary biological replicate keys")
    group_sizes = biology.groupby("group").size().to_dict()
    required = {f"{infection}_{phase}" for infection in ["infected", "uninfected"] for phase in ["pre", "attached", "newly_detached", "detached"]} | {"infected_refused_attach", "infected_refused_detach"}
    if set(group_sizes) != required or any(value != 6 for value in group_sizes.values()):
        raise MolecularError(f"Expected ten six-brain groups, observed {group_sizes}")
    audit = {"status": "validated", "study": "PRJNA918668", "biological_brains": len(biology), "discovery_brains": int((biology.cohort == "discovery").sum()), "challenge_brains": int((biology.cohort == "challenge").sum()), "total_project_runs": len(frame), "excluded_runs": frame.loc[~frame.included, ["run_id", "exclusion_reason"]].to_dict("records"), "group_sizes": group_sizes, "crosswalk_hash": canonical_hash(frame.fillna("").to_dict("records"))}
    return frame, audit


def crosswalk_manifest(manifest: dict, frame: pd.DataFrame, audit: dict) -> dict:
    validate_manifest(manifest)
    if audit.get("status") != "validated":
        raise MolecularError("Cannot authorize raw reads from an unvalidated crosswalk")
    if canonical_hash(frame.fillna("").to_dict("records")) != audit["crosswalk_hash"]:
        raise MolecularError("Crosswalk hash mismatch")
    rows = manifest["sources"]["ant_rna"]["rows"]
    included_runs = set(frame.loc[frame.included, "run_id"])
    amended = {k: v for k, v in manifest.items() if k not in {"manifest_hash", "assets"}}
    amended.update(parent_hash=manifest["manifest_hash"], amendment="ENA/BioSample crosswalk resolves 60 published 2021 brains; excludes 2019 host and parasite records", created_utc=utc_now(), crosswalk_hash=audit["crosswalk_hash"])
    amended["assets"] = [item for item in manifest["assets"] if item["source_id"] != "ant_rna"] + ena_fastq_assets(rows, included_runs, audit["crosswalk_hash"])
    amended["manifest_hash"] = canonical_hash(amended)
    return amended


def validate_counts(counts: pd.DataFrame) -> pd.DataFrame:
    if counts.empty or counts.index.has_duplicates or counts.columns.has_duplicates:
        raise MolecularError("Counts must have unique gene and sample identifiers")
    values = counts.to_numpy(dtype=float)
    if not np.isfinite(values).all() or (values < 0).any() or not np.allclose(values, np.rint(values), atol=0, rtol=0):
        raise MolecularError("DESeq2 requires finite nonnegative integer counts, never TPM")
    if np.any(values.sum(axis=0) == 0):
        raise MolecularError("Empty RNA library")
    return counts.astype(np.int64)


def collapse_technical_counts(counts: pd.DataFrame, samples: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    required = {"count_column", "biological_id", "technical_library_id", "group"}
    if not required.issubset(samples.columns):
        raise MolecularError(f"Sample map missing {sorted(required - set(samples.columns))}")
    if samples.count_column.duplicated().any() or set(counts.columns) != set(samples.count_column):
        raise MolecularError("Every count column requires exactly one biological sample-map row")
    counts = validate_counts(counts)
    ordered = samples.set_index("count_column").loc[counts.columns]
    if ordered[list(required - {"count_column"})].isna().any().any():
        raise MolecularError("Missing biological/library identity")
    collapsed, metadata = {}, []
    stable = [c for c in ["group", "infection", "phase", "batch", "collection_year", "tissue", "technical_library_id"] if c in ordered]
    for brain, mapping in ordered.groupby("biological_id", sort=True):
        if any(mapping[col].nunique(dropna=False) != 1 for col in stable):
            raise MolecularError(f"Refuse collapse across conflicting biological/library metadata: {brain}")
        collapsed[brain] = counts.loc[:, mapping.index].sum(axis=1)
        record = mapping.iloc[0].to_dict()
        record.update(biological_id=brain, count_column=brain, technical_runs=";".join(mapping.index), n_technical_runs=len(mapping))
        metadata.append(record)
    return pd.DataFrame(collapsed), pd.DataFrame(metadata)


def adapt_mantis_counts(path: str | Path, column_map: str | Path, output: str | Path, *, gene_column: str = "Geneid") -> dict:
    """Explicitly map featureCounts BAM-named source columns to GEO brains."""
    source = pd.read_csv(path, sep="\t", comment="#", compression="infer")
    mapping = pd.read_csv(column_map, sep="\t", dtype=str)
    if gene_column not in source:
        raise MolecularError(f"Missing source gene identifier column {gene_column!r}")
    columns = mapping["count_column"].tolist()
    absent = set(columns) - set(source.columns)
    if absent:
        raise MolecularError(f"Count-column mapping does not match deposited headers: {sorted(absent)}")
    if "organism" in mapping and not mapping.organism.eq("Tenodera angustipennis").all():
        raise MolecularError("Host and parasite counts must be analyzed separately")
    counts = source.set_index(gene_column)[columns]
    collapsed, metadata = collapse_technical_counts(counts, mapping)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    write_table(collapsed.reset_index(names="gene_id"), output / "mantis_counts.tsv")
    write_table(metadata, output / "mantis_samples.tsv")
    report = {"source_sha256": file_hash(path), "map_sha256": file_hash(column_map), "biological_samples": len(metadata), "genes": len(collapsed), "organism": "Tenodera angustipennis", "status": "count_adapter_validated"}
    atomic_json(output / "mantis_count_audit.json", report)
    return report


def parse_geo_soft(path: str | Path) -> list[dict]:
    opener = gzip.open if str(path).endswith(".gz") else open
    samples, current = [], None
    with opener(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            line = line.rstrip("\n")
            if line.startswith("^SAMPLE = "):
                if current is not None:
                    samples.append(current)
                current = {"source_id": line.split(" = ", 1)[1], "characteristics": {}}
            elif line.startswith("^") and current is not None:
                samples.append(current)
                current = None
            elif current is not None and line.startswith("!Sample_") and " = " in line:
                key, value = line.split(" = ", 1)
                if key == "!Sample_characteristics_ch1":
                    label, separator, content = value.partition(":")
                    if not separator:
                        raise MolecularError("GEO characteristic lacks a field label")
                    label = " ".join(label.lower().split())
                    if label in current["characteristics"]:
                        raise MolecularError(f"Repeated GEO characteristic {label}")
                    current["characteristics"][label] = content.strip()
                elif key in {"!Sample_title", "!Sample_source_name_ch1", "!Sample_organism_ch1"}:
                    current[key.removeprefix("!Sample_")] = value
    if current is not None:
        samples.append(current)
    if not samples or len({sample["source_id"] for sample in samples}) != len(samples):
        raise MolecularError("GEO SOFT has no uniquely identified samples")
    return samples


def prepare_mantis(root: str | Path, output: str | Path | None = None) -> dict:
    """Adapt exact source titles and deposited integer gene-count rows."""
    root = Path(root)
    source = root / "raw" / "mantis_rna"
    counts_path = source / "GSE237182_count_tenodera.txt.gz"
    soft_path = source / "GSE237182_family.soft.gz"
    metadata = parse_geo_soft(soft_path)
    hosts = [sample for sample in metadata if sample.get("organism_ch1") == "Tenodera angustipennis" and sample.get("source_name_ch1") == "whole brain of a T. angustipennis"]
    if len(hosts) != 15:
        raise MolecularError(f"Expected fifteen explicitly annotated host brains; observed {len(hosts)}")
    source_counts = pd.read_csv(counts_path, sep="\t", index_col=0)
    if set(source_counts.columns) != {sample["title"] for sample in hosts}:
        raise MolecularError("Deposited count columns do not match GEO host sample titles exactly")
    rows = []
    for sample in hosts:
        characteristics = sample["characteristics"]
        phase = characteristics.get("manipulation status")
        if phase not in {"before", "during", "uninfected"}:
            raise MolecularError(f"Unrecognized mantis host phase {phase}")
        year = characteristics.get("sampling year")
        if year not in {"2017", "2018", "2019"}:
            raise MolecularError(f"Unrecognized source collection year {year}")
        infection = "uninfected" if phase == "uninfected" else "infected"
        rows.append({"study": "GSE237182", "source_id": sample["source_id"], "count_column": sample["title"], "biological_id": sample["source_id"], "technical_library_id": sample["title"], "group": "uninfected" if infection == "uninfected" else "infected_" + phase, "phase": phase, "infection": infection, "batch": year, "collection_year": year, "organism": "Tenodera angustipennis", "tissue": "brain", "sex": characteristics.get("sex", ""), "worm_sample_names": characteristics.get("c. fukuii sample name in the host", ""), "host_source_title": sample["title"]})
    mapping = pd.DataFrame(rows)
    counts, biology = collapse_technical_counts(source_counts, mapping)
    output = Path(output) if output else root / "data" / "molecular" / "mantis"
    write_table(counts.reset_index(names="gene_id"), output / "mantis_counts.tsv")
    write_table(biology, output / "mantis_samples.tsv")
    report = {"status": "mantis_counts_and_metadata_validated", "source_counts_sha256": file_hash(counts_path), "source_soft_sha256": file_hash(soft_path), "brains": len(biology), "groups": biology.groupby("group").size().to_dict(), "collection_year_by_group": pd.crosstab(biology.group, biology.collection_year).to_dict(), "source_gene_identifier": "deposited SuperTranscript/Corset cluster ID from the first count column", "worm_samples_not_pooled": len(metadata)-len(hosts), "output": str(output)}
    atomic_json(output / "mantis_adapter_audit.json", report)
    return report


def run_mantis(root: str | Path) -> dict:
    report = prepare_mantis(root)
    output = Path(report["output"])
    script = Path(__file__).resolve().parents[2] / "scripts" / "rna_counts.R"
    subprocess.run(["Rscript", str(script), "--mode", "mantis", "--counts", str(output / "mantis_counts.tsv"), "--samples", str(output / "mantis_samples.tsv"), "--output", str(output / "deseq2"), "--batch-column", "collection_year"], check=True)
    result = json.loads((output / "deseq2" / "rna_analysis_audit.json").read_text())
    atomic_json(Path(root) / "state" / "mantis-molecular.json", result)
    return result


def q(value: Any) -> str:
    return shlex.quote(str(value))


def rna_directories(config: dict, root: str | Path) -> dict[str, Path]:
    """Resolve prospective RNA namespaces inside the authorized study root."""
    from .acquire import safe_destination
    settings = config.get("molecular", {})
    pilot = settings.get("rna_plan_directory", "data/rna_plan")
    full = settings.get("rna_full_plan_directory", str(pilot).rstrip("/") + "/full" if "rna_plan_directory" in settings else "data/rna_full_plan")
    return {"output": safe_destination(root, settings.get("rna_output_directory", "data/rna")),
        "pilot_plan": safe_destination(root, pilot), "full_plan": safe_destination(root, full)}


def _rna_state_receipt(root: str | Path, name: str, report: dict) -> None:
    """Preserve the exact historical receipt before replacing its current pointer."""
    from .acquire import safe_destination
    path = safe_destination(root, "state/" + name)
    if path.is_file():
        prior = path.read_bytes()
        if json.loads(prior) != report:
            prior_hash = hashlib.sha256(prior).hexdigest()
            historical = safe_destination(root, "state/rna_receipt_history/" + path.stem + "-" + prior_hash + ".json")
            historical.parent.mkdir(parents=True, exist_ok=True)
            if historical.exists():
                if file_hash(historical) != prior_hash:
                    raise MolecularError("Historical RNA receipt changed; preserve both attempts")
            else:
                with historical.open("xb") as handle:
                    handle.write(prior); handle.flush(); os.fsync(handle.fileno())
    atomic_json(path, report)


def rna_plan(crosswalk: pd.DataFrame, references: dict, root: str | Path, output: str | Path, *, threads: int = 8, pilot: int | None = None, strandedness: str = "infer", preprocessing: str = "fastp", work_directory: str = "data/rna") -> dict:
    """Write executable per-brain jobs, with explicit protocol and QC gates."""
    if threads < 1 or preprocessing not in {"fastp", "published"} or strandedness not in {"infer", "reverse", "forward", "unstranded"}:
        raise MolecularError("Invalid RNA processing configuration")
    if references.get("assembly") != "GCA_003651465.1":
        raise MolecularError("Ant primary reference must be the publication-associated GCA_003651465.1")
    for key in ["genome_fasta", "gtf", "star_index"]:
        if not references.get(key):
            raise MolecularError(f"Reference configuration lacks {key}")
    if not references.get("reference_manifest_hash"):
        raise MolecularError("Reference files must have a version/checksum manifest")
    included = crosswalk[crosswalk["included"].astype(str).str.lower().isin(["true", "1"])].copy()
    brains = included.groupby("biological_id", sort=True)
    chosen = list(brains.groups)
    pilot_selection = "all eligible biological brains"
    if pilot is not None:
        discovery = included.loc[included.cohort == "discovery"].copy()
        reads = pd.to_numeric(discovery.get("read_count", pd.Series(index=discovery.index, dtype=float)), errors="coerce")
        if pilot == 2 and reads.notna().all() and (reads > 0).all():
            discovery["read_count_numeric"] = reads
            depth = discovery.groupby("biological_id").read_count_numeric.sum().sort_values(kind="stable")
            chosen = [depth.index[0], depth.index[-1]]
            pilot_selection = "lowest and highest deposited summed run read count among discovery brains; no outcome information"
        else:
            chosen = sorted(discovery.biological_id.unique())[:pilot]
            pilot_selection = "stable accession order among discovery brains; run read counts unavailable"
        if len(chosen) < pilot:
            raise MolecularError("Not enough discovery brains for the RNA pilot")
    from .acquire import safe_destination
    output, root = Path(output), Path(root)
    work_root = safe_destination(root, work_directory)
    output.mkdir(parents=True, exist_ok=True)
    jobs = []
    genome_script = "\n".join(["#!/bin/bash", "set -euo pipefail", f"mkdir -p {q(references['star_index'])}", f"STAR --runThreadN {threads} --runMode genomeGenerate --genomeDir {q(references['star_index'])} --genomeFastaFiles {q(references['genome_fasta'])} --sjdbGTFfile {q(references['gtf'])} --sjdbOverhang 83 --genomeSAindexNbases 13", ""])
    (output / "reference_index.sh").write_text(genome_script, encoding="utf-8", newline="\n")
    for brain in chosen:
        records = brains.get_group(brain)
        if records.technical_library_id.nunique() != 1:
            raise MolecularError(f"Cannot merge independently prepared libraries for {brain}")
        work = safe_destination(work_root, str(brain))
        r1, r2 = [], []
        for _, record in records.iterrows():
            addresses = str(record.fastq_urls).split(";")
            if len(addresses) != 2:
                raise MolecularError("Paired RNA input requires two FASTQ files")
            first, second = [root / "raw" / "ant_rna" / url.rsplit("/", 1)[-1] for url in addresses]
            if not first.name.endswith("_1.fastq.gz") or not second.name.endswith("_2.fastq.gz"):
                raise MolecularError("Mate order is not explicit in ENA FASTQ filenames")
            r1.append(first)
            r2.append(second)
        lines = ["#!/bin/bash", "set -euo pipefail", f"mkdir -p {q(work)}", f"cd {q(work)}", "rm -f .processing_complete", "for tool in STAR samtools featureCounts fastqc; do command -v \"$tool\" >/dev/null; done", f"printf '%s\\n' {q(references['reference_manifest_hash'])} > reference_manifest_hash.txt", "STAR --version > STAR.version.txt", "samtools --version > samtools.version.txt", "featureCounts -v 2> featureCounts.version.txt", "fastqc --version > fastqc.version.txt", "mkdir -p qc_raw qc_trimmed"]
        if preprocessing == "fastp":
            lines.extend(["command -v fastp >/dev/null", "fastp --version 2> fastp.version.txt"])
        else:
            lines.extend(["if command -v trimmomatic >/dev/null; then trimmer=(trimmomatic); else jars=(\"${EBROOTTRIMMOMATIC:?Trimmomatic module not loaded}\"/trimmomatic*.jar); test ${#jars[@]} -eq 1; test -f \"${jars[0]}\"; trimmer=(java -jar \"${jars[0]}\"); fi"])
        clean1, clean2 = [], []
        for index, (first, second) in enumerate(zip(r1, r2)):
            c1, c2 = f"clean_{index}_1.fastq.gz", f"clean_{index}_2.fastq.gz"
            clean1.append(c1)
            clean2.append(c2)
            lines.append(f"fastqc --threads {min(threads,2)} --outdir qc_raw {q(first)} {q(second)}")
            if preprocessing == "fastp":
                lines.append(f"fastp --in1 {q(first)} --in2 {q(second)} --out1 {q(c1)} --out2 {q(c2)} --thread {min(threads,16)} --trim_front1 13 --trim_front2 13 --max_len1 84 --max_len2 84 --detect_adapter_for_pe --qualified_quality_phred 15 --unqualified_percent_limit 40 --length_required 30 --json {q('fastp_'+str(index)+'.json')} --html {q('fastp_'+str(index)+'.html')}")
            else:
                lines.append(f"\"${{trimmer[@]}}\" PE -threads {threads} {q(first)} {q(second)} {q(c1)} unpaired_{index}_1.fastq.gz {q(c2)} unpaired_{index}_2.fastq.gz CROP:97 HEADCROP:13")
            lines.append(f"fastqc --threads {min(threads,2)} --outdir qc_trimmed {q(c1)} {q(c2)}")
        lines.append(f"STAR --runThreadN {threads} --genomeDir {q(references['star_index'])} --readFilesIn {q(','.join(clean1))} {q(','.join(clean2))} --readFilesCommand zcat --outSAMtype BAM SortedByCoordinate --quantMode GeneCounts --outFileNamePrefix aligned. --outSAMattributes NH HI AS nM --limitBAMsortRAM {int(references.get('sort_ram_bytes', 8*1024**3))}")
        lines.extend(["samtools index aligned.Aligned.sortedByCoord.out.bam", "samtools flagstat aligned.Aligned.sortedByCoord.out.bam > flagstat.txt"])
        if strandedness == "infer":
            lines.extend(["python -m horsehair.molecular strand-check --star-counts aligned.ReadsPerGene.out.tab --output strand.json --expected reverse", "strand_code=$(python -c 'import json; print(json.load(open(\"strand.json\"))[\"featurecounts_code\"])')"])
        else:
            code = {"unstranded": 0, "forward": 1, "reverse": 2}[strandedness]
            lines.append(f"strand_code={code}")
        lines.append(f"featureCounts -T {threads} -p --countReadPairs -s \"$strand_code\" -t exon -g gene_id -a {q(references['gtf'])} -o counts.txt aligned.Aligned.sortedByCoord.out.bam")
        if references.get("rrna_bed"):
            lines.extend([f"samtools view -c -L {q(references['rrna_bed'])} aligned.Aligned.sortedByCoord.out.bam > rrna_reads.txt", "samtools view -c aligned.Aligned.sortedByCoord.out.bam > aligned_reads.txt"])
        else:
            lines.append("printf '%s\\n' 'unavailable: no versioned rRNA intervals' > rrna_fraction.unavailable.txt")
        lines.append(f"python -m horsehair.molecular rna-qc --directory . --sample {q(brain)} --output qc.json")
        lines.extend(["test -s counts.txt", "printf '%s\\n' complete > .processing_complete", ""])
        script = output / f"rna_{brain}.sh"
        script.write_text("\n".join(lines), encoding="utf-8", newline="\n")
        jobs.append({"biological_id": brain, "cohort": records.iloc[0].cohort, "script": str(script), "runs": records.run_id.tolist(), "threads": threads, "output": str(work), "preprocessing": preprocessing})
    report = {"jobs": jobs, "reference_index_script": str(output / "reference_index.sh"), "reference_manifest_hash": references["reference_manifest_hash"], "crosswalk_hash": canonical_hash(crosswalk.fillna("").to_dict("records")), "status": "executable_plan", "preprocessing": preprocessing, "strand_mode": strandedness, "pilot_selection": pilot_selection, "work_directory": str(work_root)}
    atomic_json(output / "rna_plan.json", report)
    return report


def parse_strandedness(text: str, *, expected: str = "reverse", dominance: float = 0.8, min_explained: float = 0.5) -> dict:
    forward = re.search(r'Fraction of reads explained by "1\+\+,1--,2\+-,2-\+":\s*([\d.]+)', text)
    reverse = re.search(r'Fraction of reads explained by "1\+-,1-\+,2\+\+,2--":\s*([\d.]+)', text)
    if not forward or not reverse:
        raise MolecularError("Unrecognized RSeQC paired-end strand report")
    f, r = float(forward.group(1)), float(reverse.group(1))
    if f + r < min_explained or not 0 <= f <= 1 or not 0 <= r <= 1:
        raise MolecularError("Insufficient informative reads to infer strandedness")
    fraction = r / (f + r)
    inferred = "reverse" if fraction >= dominance else "forward" if fraction <= 1 - dominance else "unstranded" if 0.4 <= fraction <= 0.6 else "ambiguous"
    if inferred == "ambiguous" or inferred != expected:
        raise MolecularError(f"Observed library strand {inferred} conflicts with declared {expected}; review protocol before counts")
    return {"inferred": inferred, "reverse_fraction": fraction, "informative_fraction": f + r, "featurecounts_code": {"unstranded": 0, "forward": 1, "reverse": 2}[inferred], "status": "validated"}


def star_strandedness(path: str | Path, *, expected: str = "reverse", dominance: float = 0.8) -> dict:
    frame = pd.read_csv(path, sep="\t", header=None, names=["gene_id", "unstranded", "forward", "reverse"])
    frame = frame[~frame.gene_id.str.startswith("N_")]
    totals = frame[["unstranded", "forward", "reverse"]].sum().astype(float)
    if totals.unstranded <= 0 or totals.forward + totals.reverse < 0.5 * totals.unstranded:
        raise MolecularError("Insufficient gene-assigned reads to infer library strand")
    fraction = totals.reverse / (totals.forward + totals.reverse)
    inferred = "reverse" if fraction >= dominance else "forward" if fraction <= 1 - dominance else "unstranded" if 0.4 <= fraction <= 0.6 else "ambiguous"
    if inferred != expected:
        raise MolecularError(f"STAR count strand {inferred} conflicts with declared {expected}; review source protocol")
    return {"method": "STAR GeneCounts read1/read2 orientation", "inferred": inferred, "reverse_fraction": fraction, "counts": totals.to_dict(), "featurecounts_code": {"unstranded": 0, "forward": 1, "reverse": 2}[inferred], "status": "validated"}


def rna_qc(directory: str | Path, sample: str, *, min_mapping: float = 0.5, min_assignment: float = 0.25) -> dict:
    directory = Path(directory)
    log_path = directory / "aligned.Log.final.out"
    metrics = {}
    for line in log_path.read_text().splitlines():
        if "|" in line:
            key, value = line.split("|", 1)
            metrics[key.strip()] = value.strip()
    mapping = float(metrics.get("Uniquely mapped reads %", "nan").rstrip("%")) / 100
    summary = pd.read_csv(directory / "counts.txt.summary", sep="\t", index_col=0)
    if summary.shape[1] != 1:
        raise MolecularError("Per-biological-library featureCounts summary expected")
    total = float(summary.iloc[:, 0].sum())
    assignment = float(summary.loc["Assigned"].iloc[0]) / total if total else math.nan
    if not np.isfinite(mapping) or not np.isfinite(assignment):
        raise MolecularError("RNA QC metrics missing")
    report = {"biological_id": sample, "unique_mapping_fraction": mapping, "assigned_fragment_fraction": assignment, "star_metrics": metrics, "featurecounts_summary": summary.iloc[:, 0].to_dict(), "reference_mismatch_possible": True, "status": "passed" if mapping >= min_mapping and assignment >= min_assignment else "review_required", "criteria": {"min_unique_mapping": min_mapping, "min_assignment": min_assignment}}
    fastp = []
    for path in sorted(directory.glob("fastp_*.json")):
        value = json.loads(path.read_text())
        fastp.append({"file": path.name, "summary": value.get("summary"), "duplication_rate": value.get("duplication", {}).get("rate"), "adapter_cutting": value.get("adapter_cutting")})
    report["fastp"] = fastp
    if (directory / "rrna_reads.txt").exists():
        denominator = int((directory / "aligned_reads.txt").read_text())
        report["rrna_fraction"] = int((directory / "rrna_reads.txt").read_text()) / denominator if denominator else None
    else:
        report["rrna_fraction"] = None
        report["rrna_status"] = "unavailable_without_validated_intervals"
    if report["status"] != "passed":
        raise MolecularError(f"RNA QC review required for {sample}: mapping={mapping:.3f}, assignment={assignment:.3f}; do not silently exclude")
    return report


def combine_featurecounts(paths: list[str | Path], sample_ids: list[str], output: str | Path) -> pd.DataFrame:
    if len(paths) != len(sample_ids) or len(set(sample_ids)) != len(sample_ids):
        raise MolecularError("One unique biological identifier per count file required")
    columns = []
    for path, sample in zip(paths, sample_ids):
        frame = pd.read_csv(path, sep="\t", comment="#")
        metadata = {"Geneid", "Chr", "Start", "End", "Strand", "Length"}
        value_cols = [c for c in frame if c not in metadata]
        if len(value_cols) != 1 or "Geneid" not in frame:
            raise MolecularError("Expected one count column in per-library featureCounts output")
        column = frame.set_index("Geneid")[value_cols[0]].rename(sample)
        if column.index.has_duplicates:
            raise MolecularError("Duplicate count gene IDs")
        if columns and not column.index.equals(columns[0].index):
            raise MolecularError("Reference gene inventory differs across RNA samples")
        columns.append(column)
    counts = validate_counts(pd.concat(columns, axis=1))
    write_table(counts.reset_index(names="gene_id"), output)
    return counts


def prepare_reference(manifest: dict, root: str | Path, output: str | Path) -> dict:
    """Validate and decompress the matching reference pair, without downloading."""
    from .acquire import safe_destination, verify_file
    validate_manifest(manifest)
    root, output = Path(root), Path(output)
    output.mkdir(parents=True, exist_ok=True)
    assets = [item for item in manifest["assets"] if item["source_id"] == "ant_reference"]
    if not assets:
        raise MolecularError("Reference metadata absent from the acquisition manifest")
    receipts = []
    paths = {}
    for item in assets:
        source = safe_destination(root, item["relative_path"])
        receipt = verify_file(source, item)
        receipts.append(receipt)
        if source.suffix == ".gz":
            target = output / source.name.removesuffix(".gz")
            temporary = target.with_name(target.name + ".tmp")
            with gzip.open(source, "rb") as inp, temporary.open("wb") as out:
                import shutil
                shutil.copyfileobj(inp, out, length=8 * 1024 * 1024)
            temporary.replace(target)
        else:
            target = source
        paths[source.name] = str(target)
    pick = lambda suffix: next((value for key, value in paths.items() if key.endswith(suffix)), None)
    fasta, gtf, proteins = pick("_genomic.fna.gz"), pick("_genomic.gtf.gz"), pick("_protein.faa.gz")
    if not fasta or not gtf or not proteins:
        raise MolecularError("Matching genome/GTF/proteome are required")
    config = {"assembly": "GCA_003651465.1", "alignment_assembly": "GCF_003651465.1", "annotation": "NCBI Annotation Release 100", "genome_fasta": fasta, "gtf": gtf, "protein_fasta": proteins, "feature_table": pick("_feature_table.txt.gz"), "star_index": str(output / "star_index"), "reference_manifest_hash": canonical_hash({"assets": assets, "source_manifest_hash": manifest["manifest_hash"]}), "decompressed_sha256": {Path(value).name: file_hash(value) for value in paths.values()}, "original_annotation_reproduction_status": "unresolved_original_paper_annotation_not_distributed_in_current_GenBank_directory", "method_amendment": "Matching paired RefSeq genome and NCBI release100 annotation; not represented as identical to original gene models"}
    atomic_json(output / "references.json", config)
    atomic_json(output / "reference_receipts.json", receipts)
    return config


def run_rna_pilot(config: dict, root: str | Path, index: int | None = None, prepare_only: bool = False) -> dict:
    """Root scheduler API: index/reference preparation once, then two brain jobs.

    Prepared pilot membership is fixed before processing. No outcome, mapping
    percentage or expression result is used to pick a more favorable pilot.
    """
    from .core import canonical_hash as config_hash, source_identity
    import os
    root = Path(root)
    manifest = json.loads((root / "manifests" / "source_manifest.json").read_text())
    validate_manifest(manifest)
    source = manifest["sources"]["ant_rna"]
    if source.get("crosswalk_audit", {}).get("status") != "validated":
        raise MolecularError("Acquisition must resolve the biological crosswalk before RNA jobs")
    crosswalk = pd.DataFrame(source["crosswalk"])
    directories = rna_directories(config, root)
    plan_dir = directories["pilot_plan"]
    reference_dir = root / "data" / "references"
    preprocessing = config.get("molecular", {}).get("rna_preprocessing", "published")
    threads = int(config.get("resources", {}).get("rna_cores", 8))
    if prepare_only:
        references = prepare_reference(manifest, root, reference_dir)
        write_table(crosswalk, root / "manifests" / "sample_crosswalk.tsv")
        plan = rna_plan(crosswalk, references, root, plan_dir, threads=threads, pilot=2, preprocessing=preprocessing, work_directory=config.get("molecular", {}).get("rna_output_directory", "data/rna"))
        marker = reference_dir / "star_index" / "index_complete.json"
        if marker.exists():
            prior = json.loads(marker.read_text())
            if prior.get("reference_manifest_hash") != references["reference_manifest_hash"]:
                raise MolecularError("Existing STAR index belongs to a different reference")
        else:
            started = time.monotonic()
            subprocess.run(["bash", plan["reference_index_script"]], check=True)
            if not (reference_dir / "star_index" / "Genome").exists():
                raise MolecularError("STAR genomeGenerate did not produce a genome index")
            atomic_json(marker, {"reference_manifest_hash": references["reference_manifest_hash"], "elapsed_seconds": time.monotonic()-started, "status": "index_generated"})
        report = {"status": "reference_and_two_brain_plan_ready", "plan": plan, "references": str(reference_dir / "references.json"), "source_manifest_hash": manifest["manifest_hash"]}
        _rna_state_receipt(root, "rna-reference-ready.json", report)
        return report
    if index is None or index not in {0, 1}:
        raise MolecularError("RNA pilot requires fixed task index zero or one")
    plan = json.loads((plan_dir / "rna_plan.json").read_text())
    job = plan["jobs"][index]
    directory = Path(job["output"])
    started = time.monotonic()
    subprocess.run(["bash", job["script"]], check=True)
    report = {"status": "pilot_processed", "biological_id": job["biological_id"], "run_ids": job["runs"], "preprocessing": job["preprocessing"], "elapsed_seconds": time.monotonic()-started, "working_bytes": sum(path.stat().st_size for path in directory.rglob("*") if path.is_file()), "qc": json.loads((directory / "qc.json").read_text()), "source_manifest_hash": manifest["manifest_hash"], "scientific_evidence": "technical benchmark; no phase differential expression fitted"}
    report.update(config_sha256=config_hash(config), source_sha256=source_identity(Path(__file__).resolve().parents[2])["sha256"], slurm_job_id=native_job_id(), allocated_cpus=int(os.environ.get("SLURM_CPUS_PER_TASK", threads)), requested_memory_gb=config["resources"].get("rna_memory_gb"), read_count=int(pd.to_numeric(crosswalk.loc[crosswalk.biological_id == job["biological_id"], "read_count"], errors="raise").sum()), native_execution=native_job_id() is not None)
    report["working_files"] = sum(path.is_file() for path in directory.rglob("*"))
    report["output_directory"] = str(directory.resolve())
    _rna_state_receipt(root, f"rna-pilot-{index}.json", report)
    return report


def prepare_full_rna(config: dict, root: str | Path) -> dict:
    root = Path(root)
    manifest = json.loads((root / "manifests" / "source_manifest.json").read_text())
    crosswalk = pd.DataFrame(manifest["sources"]["ant_rna"]["crosswalk"])
    references = json.loads((root / "data" / "references" / "references.json").read_text())
    if not (Path(references["star_index"]) / "index_complete.json").exists():
        raise MolecularError("Full RNA processing requires the validated pilot reference index")
    return rna_plan(crosswalk, references, root, rna_directories(config, root)["full_plan"], threads=int(config.get("resources", {}).get("rna_cores", 8)), preprocessing=config.get("molecular", {}).get("rna_preprocessing", "published"), work_directory=config.get("molecular", {}).get("rna_output_directory", "data/rna"))


def run_rna_task(plan_path: str | Path, index: int) -> dict:
    plan = json.loads(Path(plan_path).read_text())
    if not 0 <= index < len(plan["jobs"]):
        raise MolecularError("RNA task index lies outside the fixed biological cohort")
    job = plan["jobs"][index]
    started = time.monotonic()
    subprocess.run(["bash", job["script"]], check=True)
    directory = Path(job["output"])
    from .core import source_identity
    return {"status": "processed", "biological_id": job["biological_id"], "cohort": job["cohort"], "elapsed_seconds": time.monotonic()-started, "qc": json.loads((directory / "qc.json").read_text()), "plan_hash": canonical_hash(plan), "slurm_job_id": native_job_id(), "source_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"], "native_execution": native_job_id() is not None, "allocated_cpus": int(os.environ["SLURM_CPUS_PER_TASK"]) if os.environ.get("SLURM_CPUS_PER_TASK") else None, "working_bytes": sum(path.stat().st_size for path in directory.rglob("*") if path.is_file()), "working_files": sum(path.is_file() for path in directory.rglob("*"))}


def run_ant(root: str | Path, config: dict | None = None) -> dict:
    root = Path(root)
    manifest = json.loads((root / "manifests" / "source_manifest.json").read_text())
    crosswalk = pd.DataFrame(manifest["sources"]["ant_rna"]["crosswalk"])
    included = crosswalk[crosswalk.included.astype(str).str.lower().isin(["true", "1"])].copy()
    biological = included.drop_duplicates("biological_id").sort_values("biological_id")
    if len(biological) != 60:
        raise MolecularError("Ant phase analysis requires the sixty uniquely mapped source brains")
    count_paths = []
    for brain in biological.biological_id:
        from .acquire import safe_destination
        directory = safe_destination(rna_directories(config or {}, root)["output"], str(brain))
        if not (directory / ".processing_complete").exists():
            raise MolecularError(f"RNA sample has not completed processing: {brain}")
        qc = json.loads((directory / "qc.json").read_text())
        if qc.get("status") != "passed":
            raise MolecularError(f"Unresolved RNA quality review: {brain}")
        count_paths.append(directory / "counts.txt")
    output = root / "data" / "molecular" / "ant"
    output.mkdir(parents=True, exist_ok=True)
    combine_featurecounts(count_paths, biological.biological_id.tolist(), output / "ant_counts.tsv")
    biological["count_column"] = biological.biological_id
    write_table(biological, output / "ant_samples.tsv")
    script = Path(__file__).resolve().parents[2] / "scripts" / "rna_counts.R"
    subprocess.run(["Rscript", str(script), "--mode", "ant", "--counts", str(output / "ant_counts.tsv"), "--samples", str(output / "ant_samples.tsv"), "--output", str(output / "deseq2"), "--batch-column", "batch"], check=True)
    report = json.loads((output / "deseq2" / "rna_analysis_audit.json").read_text())
    report["source_manifest_hash"] = manifest["manifest_hash"]
    atomic_json(root / "state" / "ant-molecular.json", report)
    return report


def read_fasta(path: str | Path) -> list[tuple[str, str, str]]:
    opener = gzip.open if str(path).endswith(".gz") else open
    records, name, description, sequence = [], None, "", []
    with opener(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line.startswith(">"):
                if name is not None:
                    records.append((name, description, "".join(sequence)))
                description = line[1:]
                name = description.split()[0]
                sequence = []
            elif line:
                if name is None:
                    raise MolecularError("Sequence before FASTA identifier")
                sequence.append(line.upper())
    if name is not None:
        records.append((name, description, "".join(sequence)))
    if not records or len({name for name, _, _ in records}) != len(records):
        raise MolecularError("FASTA requires nonempty unique source sequence identifiers")
    return records


def select_representative_proteins(protein_fasta: str | Path, protein_gene_map: pd.DataFrame, output_fasta: str | Path) -> pd.DataFrame:
    if not {"protein_id", "gene_id", "species"}.issubset(protein_gene_map) or protein_gene_map.duplicated(["species", "protein_id"]).any():
        raise MolecularError("Representative selection needs an explicit protein/gene map")
    records = {name: (description, sequence) for name, description, sequence in read_fasta(protein_fasta)}
    mapped = protein_gene_map.copy()
    if set(mapped.protein_id) - set(records):
        raise MolecularError("Protein mapping refers to absent FASTA sequences")
    mapped["protein_length"] = mapped.protein_id.map(lambda key: len(records[key][1].rstrip("*")))
    # Longest annotated isoform, lexicographic tie-break, without gene-name guesses.
    representatives = mapped.sort_values(["species", "gene_id", "protein_length", "protein_id"], ascending=[True, True, False, True]).drop_duplicates(["species", "gene_id"]).copy()
    output_fasta = Path(output_fasta)
    output_fasta.parent.mkdir(parents=True, exist_ok=True)
    with output_fasta.open("w", encoding="utf-8", newline="\n") as handle:
        for row in representatives.itertuples():
            sequence = records[row.protein_id][1].rstrip("*")
            if "*" in sequence:
                raise MolecularError(f"Internal protein stop codon: {row.protein_id}")
            handle.write(f">{row.protein_id}\n")
            handle.write("\n".join(sequence[i:i+80] for i in range(0, len(sequence), 80)) + "\n")
    return representatives


def orf_plan(supertranscripts: str | Path, transcript_gene_map: str | Path, output: str | Path) -> dict:
    """Execute an established ORF caller; require a measured-gene ID crosswalk."""
    mapping = pd.read_csv(transcript_gene_map, sep="\t", dtype=str)
    if not {"transcript_id", "gene_id"}.issubset(mapping) or mapping.transcript_id.duplicated().any():
        raise MolecularError("Unique supertranscript-to-counted-gene identifiers required")
    names = {name for name, _, _ in read_fasta(supertranscripts)}
    if set(mapping.transcript_id) != names:
        raise MolecularError("ORF map must cover each source supertranscript exactly once")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    fasta = output / "mantis_supertranscripts.fasta"
    lines = ["#!/bin/bash", "set -euo pipefail", f"mkdir -p {q(output)}", f"cd {q(output)}", f"cp {q(Path(supertranscripts).resolve())} {q(fasta)}", f"TransDecoder.LongOrfs -t {q(fasta)} -m 100", f"TransDecoder.Predict -t {q(fasta)} --single_best_only", f"python -m horsehair.molecular orf-map --peptides {q(str(fasta)+'.transdecoder.pep')} --transcript-map {q(Path(transcript_gene_map).resolve())} --output {q(output / 'protein_gene_map.tsv')}", ""]
    (output / "transdecoder.sh").write_text("\n".join(lines), encoding="utf-8", newline="\n")
    report = {"source_sha256": file_hash(supertranscripts), "transcript_map_sha256": file_hash(transcript_gene_map), "minimum_peptide_aa": 100, "single_best_per_transcript": True, "source_transcripts": len(names), "script": str(output / "transdecoder.sh"), "status": "executable_orf_plan"}
    atomic_json(output / "orf_plan.json", report)
    return report


def transdecoder_gene_map(peptides: str | Path, transcript_gene_map: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    if transcript_gene_map.transcript_id.duplicated().any():
        raise MolecularError("Ambiguous transcript-to-gene mapping")
    mapping = transcript_gene_map.set_index("transcript_id").gene_id.to_dict()
    rows = []
    for protein, description, sequence in read_fasta(peptides):
        # TransDecoder appends .pN to the exact input transcript ID. No other
        # trimming or biological inference is allowed here.
        match = re.fullmatch(r"(.+)\.p\d+", protein)
        transcript = match.group(1) if match else None
        if transcript not in mapping:
            raise MolecularError(f"TransDecoder ID absent from source map: {protein}")
        rows.append({"species": "mantis", "protein_id": protein, "transcript_id": transcript, "gene_id": mapping[transcript], "protein_length": len(sequence.rstrip("*")), "orf_description": description})
    frame = pd.DataFrame(rows)
    coverage = {"source_counted_genes": transcript_gene_map.gene_id.nunique(), "genes_with_orf": frame.gene_id.nunique(), "mapping_fraction": frame.gene_id.nunique() / transcript_gene_map.gene_id.nunique(), "ambiguous_multiple_orfs": frame.groupby("gene_id").size().loc[lambda series: series > 1].to_dict()}
    return frame, coverage


def join_frozen_gene_features(scores: pd.DataFrame, orthology: pd.DataFrame, targets: pd.DataFrame, descriptors: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    targets = targets.rename(columns={"orthogroup": "source_target_orthogroup"})
    if scores.gene_id.duplicated().any() or targets.gene_id.duplicated().any() or descriptors.gene_id.duplicated().any():
        raise MolecularError("Molecular score, gene target and nuisance descriptor rows must be unique")
    required = {"species", "gene_id", "orthogroup", "one_to_one"}
    if not required.issubset(orthology):
        raise MolecularError("Validated orthology table required")
    one = orthology[orthology.one_to_one.astype(str).str.lower().isin(["true", "1"])]
    ant = one[one.species == "ant"][["gene_id", "orthogroup"]]
    fly = one[one.species == "fly"][["gene_id", "orthogroup"]]
    if ant.orthogroup.duplicated().any() or fly.orthogroup.duplicated().any():
        raise MolecularError("One-to-one orthogroup flags disagree with mapping multiplicity")
    mapped = scores.merge(ant, on="gene_id", validate="one_to_one").rename(columns={"gene_id": "ant_gene_id"})
    mapped = mapped.merge(fly, on="orthogroup", validate="one_to_one")
    frame = targets.merge(mapped[["gene_id", "ant_gene_id", "orthogroup", "reversible_score"]], on="gene_id", validate="one_to_one")
    frame = frame.merge(descriptors, on="gene_id", validate="one_to_one")
    required_columns = ["effect", "effect_se", "reversible_score", *BASELINE_COLUMNS]
    absent = set(required_columns) - set(frame)
    if absent:
        raise MolecularError(f"Frozen descriptor set is incomplete: {sorted(absent)}")
    invalid = ~np.isfinite(frame[required_columns].to_numpy(dtype=float)).all(axis=1)
    excluded = frame.loc[invalid, "gene_id"].tolist()
    frame = frame.loc[~invalid].reset_index(drop=True)
    report = {"target_genes": len(targets), "mapped_measured_genes": len(frame), "unmapped_target_genes": sorted(set(targets.gene_id)-set(frame.gene_id)), "excluded_missing_descriptors": excluded, "mapped_fraction": len(frame)/len(targets) if len(targets) else 0, "orthology_hash": canonical_hash(orthology.fillna("").to_dict("records"))}
    return frame, report


def reversible_scores(contrasts: pd.DataFrame, cap: float = 10.0) -> pd.DataFrame:
    required = {"gene_id", "on_log2fc", "on_se", "off_log2fc", "off_se"}
    if not required.issubset(contrasts):
        raise MolecularError("Reversible score requires both phase contrasts and their standard errors")
    if contrasts.gene_id.duplicated().any():
        raise MolecularError("Repeated source genes in reversible score")
    result = contrasts.copy()
    valid = np.isfinite(result[["on_log2fc", "on_se", "off_log2fc", "off_se"]].to_numpy(dtype=float)).all(axis=1) & (result.on_se > 0) & (result.off_se > 0)
    z_on = result.on_log2fc.to_numpy() / result.on_se.to_numpy()
    z_off = result.off_log2fc.to_numpy() / result.off_se.to_numpy()
    positive = np.maximum(norm.sf(z_on), norm.cdf(z_off))
    negative = np.maximum(norm.cdf(z_on), norm.sf(z_off))
    p = np.minimum(1.0, 2 * np.minimum(positive, negative))
    p[~valid] = np.nan
    result["reversible_p"] = p
    result["reversible_score"] = np.minimum(cap, -np.log10(np.maximum(p, np.finfo(float).tiny)))
    result["initiation_direction"] = np.where(positive <= negative, 1, -1)
    result["adequately_measured"] = valid
    ordered = np.argsort(np.where(valid, p, np.inf))
    adjusted = np.full(len(result), np.nan)
    ranks = np.arange(1, valid.sum() + 1)
    if len(ranks):
        ordered_valid = ordered[:valid.sum()]
        adjusted[ordered_valid] = np.minimum(1, np.minimum.accumulate((p[ordered_valid] * valid.sum() / ranks)[::-1])[::-1])
    result["reversible_fdr"] = adjusted
    return result


def freeze_scores(scores: pd.DataFrame, provenance: dict, output: str | Path) -> dict:
    forbidden = {"effect", "effect_se", "fly_effect", "target", "heldout_outcome"}
    if forbidden & set(scores.columns) or provenance.get("fly_outcomes_inspected", True):
        raise MolecularError("Cannot freeze molecular discovery scores after target outcomes were inspected")
    for key in ["source_manifest_hash", "contrast_design_hash", "orthology_hash", "nuisance_descriptor_hash"]:
        if not provenance.get(key):
            raise MolecularError(f"Frozen score provenance requires {key}")
    output = Path(output)
    if (output / "freeze.json").exists():
        raise MolecularError("Molecular freeze already exists; create a dated amendment directory")
    output.mkdir(parents=True, exist_ok=True)
    write_table(scores, output / "molecular_scores.tsv")
    record = {"created_utc": utc_now(), "score_sha256": file_hash(output / "molecular_scores.tsv"), "provenance": provenance, "status": "frozen_before_external_outcomes"}
    record["freeze_hash"] = canonical_hash(record)
    atomic_json(output / "freeze.json", record)
    return record


def refusal_challenge(normalized_discovery: pd.DataFrame, discovery_samples: pd.DataFrame, refusal_counts: pd.DataFrame, refusal_samples: pd.DataFrame, calibration: pd.DataFrame, frozen_scores: pd.DataFrame, freeze: dict, *, bootstrap_replicates: int = 10_000, seed: int = 314159) -> tuple[pd.DataFrame, dict]:
    """Evaluate withheld natural refusals using discovery-only normalization.

    Projection weights and expression centering are fixed before the challenge.
    Brains, not genes, are resampled for challenge uncertainty.
    """
    if freeze.get("status") != "frozen_before_external_outcomes":
        raise MolecularError("Refusal challenge requires a frozen discovery score")
    if len(discovery_samples) != 48 or len(refusal_samples) != 12:
        raise MolecularError("Refusal evaluation requires 48 discovery and 12 challenge brains")
    if set(discovery_samples.biological_id) & set(refusal_samples.biological_id):
        raise MolecularError("Refusal brain leaked into discovery")
    if set(refusal_samples.group) != {"infected_refused_attach", "infected_refused_detach"} or any(refusal_samples.groupby("group").size() != 6):
        raise MolecularError("Refusal challenge requires six brains in each declared refusal group")
    required = {"gene_id", "training_mean", "training_sd", "size_factor_geometric_mean"}
    if not required.issubset(calibration) or calibration.gene_id.duplicated().any() or frozen_scores.gene_id.duplicated().any():
        raise MolecularError("Discovery-only expression calibration is incomplete")
    if not set(calibration.gene_id).issubset(normalized_discovery.index) or not set(calibration.gene_id).issubset(refusal_counts.index):
        raise MolecularError("Challenge reference gene IDs differ from discovery")
    if set(normalized_discovery.columns) != set(discovery_samples.biological_id) or set(refusal_counts.columns) != set(refusal_samples.biological_id):
        raise MolecularError("Every expression column requires a unique biological brain")
    refusal_counts = validate_counts(refusal_counts)
    table = calibration.merge(frozen_scores[["gene_id", "reversible_score", "initiation_direction"]], on="gene_id", validate="one_to_one")
    valid = np.isfinite(table[["training_mean", "training_sd", "reversible_score", "initiation_direction"]].to_numpy(dtype=float)).all(axis=1) & (table.training_sd > 1e-8)
    table = table[valid].set_index("gene_id")
    weights = table.reversible_score.to_numpy() * table.initiation_direction.to_numpy()
    denominator = np.abs(weights).sum()
    if len(table) < 20 or denominator <= 0:
        raise MolecularError("Too few measured/nonconstant genes for a frozen refusal signature")
    geo = calibration.set_index("gene_id").size_factor_geometric_mean
    eligible_geo = geo[(geo > 0) & np.isfinite(geo)]
    if len(eligible_geo) < 20:
        raise MolecularError("Insufficient discovery positive-count genes for frozen normalization")
    refusal_normalized = refusal_counts.astype(float).copy()
    size_factors = {}
    for brain in refusal_counts.columns:
        observed = refusal_counts.loc[eligible_geo.index, brain]
        usable = observed > 0
        if usable.sum() < 20:
            raise MolecularError(f"Insufficient measurable reference counts in {brain}")
        factor = float(np.exp(np.median(np.log(observed[usable])-np.log(eligible_geo[usable]))))
        size_factors[brain] = factor
        refusal_normalized[brain] /= factor
    combined = pd.concat([normalized_discovery, refusal_normalized], axis=1).loc[table.index]
    standardized = (np.log2(combined.to_numpy()+1) - table.training_mean.to_numpy()[:, None]) / table.training_sd.to_numpy()[:, None]
    projection = weights @ standardized / denominator
    samples = pd.concat([discovery_samples, refusal_samples], ignore_index=True).set_index("biological_id").loc[combined.columns]
    observations = samples.reset_index()[["biological_id", "group"]].copy()
    observations["frozen_signature_projection"] = projection
    rng = np.random.default_rng(seed)
    contrasts = []
    for refusal, comparator, expected_direction in [("infected_refused_attach", "infected_attached", -1), ("infected_refused_detach", "infected_detached", 1)]:
        challenge = observations.loc[observations.group == refusal, "frozen_signature_projection"].to_numpy()
        control = observations.loc[observations.group == comparator, "frozen_signature_projection"].to_numpy()
        if len(challenge) != 6 or len(control) != 6:
            raise MolecularError("Refusal phase comparator lacks its six source brains")
        draws = rng.choice(challenge, size=(bootstrap_replicates, 6), replace=True).mean(axis=1) - rng.choice(control, size=(bootstrap_replicates, 6), replace=True).mean(axis=1)
        estimate = float(challenge.mean()-control.mean())
        contrasts.append({"refusal_group": refusal, "matched_infected_phase": comparator, "expected_direction": expected_direction, "estimate": estimate, "bootstrap_ci95": np.quantile(draws, [.025, .975]).tolist(), "point_direction_matches": bool(estimate*expected_direction > 0), "brains_per_group": 6})
    return observations, {"status": "heldout_refusal_evaluated", "freeze_hash": freeze.get("freeze_hash"), "genes_in_signature": len(table), "challenge_size_factors_from_discovery_geomeans": size_factors, "contrasts": contrasts, "bootstrap_replicates": bootstrap_replicates, "seed": seed, "evidence_class": "cross-sectional observational challenge, not within-brain recovery or randomized intervention"}


def orthology_plan(proteomes: dict[str, dict], output: str | Path, *, threads: int = 16) -> dict:
    if not {"ant", "fly"}.issubset(proteomes) or set(proteomes) - {"ant", "mantis", "fly"}:
        raise MolecularError("Primary orthology requires ant and fly; mantis is an explicit optional corroboration")
    for species in proteomes:
        item = proteomes.get(species, {})
        if not item.get("path") or not item.get("sha256") or not item.get("version"):
            raise MolecularError(f"Versioned checksummed {species} proteome required")
        if Path(item["path"]).exists() and file_hash(item["path"]) != item["sha256"]:
            raise MolecularError(f"{species} proteome hash mismatch")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    protein_dir = output / "proteomes"
    lines = ["#!/bin/bash", "set -euo pipefail", f"mkdir -p {q(protein_dir)}"]
    for species, item in sorted(proteomes.items()):
        lines.extend([f"test -s {q(item['path'])}", f"printf '%s  %s\\n' {q(item['sha256'])} {q(item['path'])} | sha256sum -c -", f"cp {q(item['path'])} {q(protein_dir / (species+'.fa'))}"])
    lines.extend([f"orthofinder -f {q(protein_dir)} -t {threads} -a {min(threads,8)} -S diamond", ""])
    (output / "orthofinder.sh").write_text("\n".join(lines), encoding="utf-8", newline="\n")
    report = {"proteomes": proteomes, "command": lines[-2], "plan_hash": canonical_hash(proteomes), "one_to_one_primary": True}
    atomic_json(output / "orthology_plan.json", report)
    return report


def parse_orthogroups(path: str | Path, protein_map: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    required = {"species", "protein_id", "gene_id"}
    if not required.issubset(protein_map) or protein_map.duplicated(["species", "protein_id"]).any():
        raise MolecularError("Protein-to-gene map must be explicit and unique")
    source = pd.read_csv(path, sep="\t", dtype=str).fillna("")
    if "HOG" in source and "Orthogroup" not in source:
        source = source.rename(columns={"HOG": "Orthogroup"})
    if not {"Orthogroup", "ant", "fly"}.issubset(source):
        raise MolecularError("Expected OrthoFinder root HOG N0.tsv columns HOG (or legacy Orthogroup), ant and fly")
    if "mantis" not in source:
        source["mantis"] = ""
    mapping = protein_map.set_index(["species", "protein_id"])["gene_id"].to_dict()
    rows = []
    for _, record in source.iterrows():
        genes = {}
        for species in ["ant", "mantis", "fly"]:
            proteins = [p.strip() for p in record[species].split(",") if p.strip()]
            try:
                genes[species] = sorted({mapping[(species, p)] for p in proteins})
            except KeyError as error:
                raise MolecularError(f"OrthoFinder protein absent from source gene map: {error}") from error
        one_to_one = len(genes["ant"]) == 1 and len(genes["fly"]) == 1
        three_species = one_to_one and len(genes["mantis"]) == 1
        for species, items in genes.items():
            rows.extend({"orthogroup": record.Orthogroup, "species": species, "gene_id": gene, "one_to_one": one_to_one, "ant_fly_one_to_one": one_to_one, "three_species_one_to_one": three_species} for gene in items)
    frame = pd.DataFrame(rows)
    if frame.empty or frame.duplicated(["species", "gene_id"]).any():
        raise MolecularError("Empty orthology or source gene assigned to multiple orthogroups")
    coverage = {}
    for species in ["ant", "mantis", "fly"]:
        denominator = protein_map.loc[protein_map.species == species, "gene_id"].nunique()
        mapped = frame.loc[frame.species == species, "gene_id"].nunique()
        confidence = frame.three_species_one_to_one if species == "mantis" else frame.one_to_one
        one = frame.loc[(frame.species == species) & confidence, "gene_id"].nunique()
        coverage[species] = {"available_genes": denominator, "mapped_genes": mapped, "one_to_one_genes": one, "mapping_fraction": mapped / denominator if denominator else 0}
    return frame, {"coverage": coverage, "orthology_hash": canonical_hash(frame.to_dict("records")), "input_sha256": file_hash(path), "status": "validated"}


def _residual(values: np.ndarray, baseline: np.ndarray) -> np.ndarray:
    design = np.column_stack([np.ones(len(values)), baseline])
    return values - design @ np.linalg.lstsq(design, values, rcond=None)[0]


def incremental_rank_association(frame: pd.DataFrame) -> float:
    x = rankdata(frame.reversible_score.to_numpy())
    y = rankdata(np.abs(frame.effect.to_numpy()))
    baseline = np.column_stack([rankdata(frame[c].to_numpy()) for c in BASELINE_COLUMNS])
    rx, ry = _residual(x, baseline), _residual(y, baseline)
    if np.std(rx) < 1e-12 or np.std(ry) < 1e-12:
        raise MolecularError("No residual variance for molecular incremental association")
    return float(np.corrcoef(rx, ry)[0, 1])


def _permutation_blocks(frame: pd.DataFrame) -> list[list[np.ndarray]]:
    meta = frame.groupby("orthogroup", sort=True).agg(size=("gene_id", "size"), expression=("average_expression", "mean"), confidence=("orthology_confidence", "mean"), precision=("effect_se", "mean"))
    for col in ["expression", "confidence", "precision"]:
        meta[col + "_bin"] = np.minimum(1, (meta[col].rank(method="average", pct=True) * 2).astype(int))
    grouped = {key: frame.index[frame.orthogroup == key].to_numpy() for key in meta.index}
    return [[grouped[key] for key in stratum.index] for _, stratum in meta.groupby(["size", "expression_bin", "confidence_bin", "precision_bin"]) if len(stratum) >= 2]


def grouped_permutation_indices(frame: pd.DataFrame, rng: np.random.Generator) -> tuple[np.ndarray, int]:
    """Exchange whole gene-family blocks within matched nuisance bins.

    Equal block sizes keep all orthogroup genes together and permit an actual
    permutation rather than averaging/replicating target values. Singleton strata
    remain fixed, and an uninformative permutation scheme is rejected upstream.
    """
    indices = np.arange(len(frame))
    movable = 0
    for blocks in _permutation_blocks(frame):
        movable += sum(map(len, blocks))
        for receiver, donor in zip(blocks, rng.permutation(len(blocks))):
            indices[receiver] = blocks[donor]
    return indices, movable


def external_prediction(frame: pd.DataFrame, freeze: dict, *, permutations: int = 10_000, bootstrap_replicates: int = 10_000, seed: int = 271828, outcome_covariance: np.ndarray | None = None) -> dict:
    required = {"gene_id", "orthogroup", "effect", "effect_se", "reversible_score", *BASELINE_COLUMNS}
    if not required.issubset(frame):
        raise MolecularError(f"External prediction lacks baseline/outcome columns: {sorted(required - set(frame))}")
    if freeze.get("status") != "frozen_before_external_outcomes":
        raise MolecularError("An outcome-independent molecular freeze is required")
    if frame.gene_id.duplicated().any():
        raise MolecularError("External input requires shared-control-aware unique gene estimates; never duplicate reagent outcomes")
    frame = frame.reset_index(drop=True).copy()
    numeric = frame[["effect", "effect_se", "reversible_score", *BASELINE_COLUMNS]].to_numpy(dtype=float)
    if not np.isfinite(numeric).all() or (frame.effect_se <= 0).any():
        raise MolecularError("Missing/noisy-gene uncertainty or descriptors require explicit exclusions")
    observed = incremental_rank_association(frame)
    rng = np.random.default_rng(seed)
    blocks = _permutation_blocks(frame)
    movable = sum(len(block) for stratum in blocks for block in stratum)
    if movable < 10:
        raise MolecularError("Fewer than ten genes can exchange within the prespecified family/precision bins; permutation test is unidentified")
    if permutations < 1:
        raise MolecularError("Permutation count must be positive")
    null = np.empty(permutations)
    original = frame.effect.to_numpy().copy()
    noise_factor = None
    if outcome_covariance is not None:
        covariance = np.asarray(outcome_covariance, dtype=float)
        if covariance.shape != (len(frame), len(frame)) or not np.isfinite(covariance).all() or not np.allclose(covariance, covariance.T, atol=1e-10):
            raise MolecularError("Shared-control gene covariance has invalid dimensions, values or symmetry")
        if not np.allclose(np.diag(covariance), frame.effect_se.to_numpy()**2, rtol=1e-5, atol=1e-10):
            raise MolecularError("Shared-control gene covariance diagonal disagrees with target uncertainty")
        values, vectors = np.linalg.eigh(covariance)
        if values.min() < -1e-8 * max(1, values.max()):
            raise MolecularError("Shared-control gene covariance is not positive semidefinite")
        noise_factor = vectors * np.sqrt(np.maximum(0, values))[None, :]
    baseline_ranked = np.column_stack([np.ones(len(frame)), rankdata(frame[BASELINE_COLUMNS].to_numpy(), axis=0)])
    projector = np.linalg.pinv(baseline_ranked)
    residual_score = _residual(rankdata(frame.reversible_score.to_numpy()), baseline_ranked[:, 1:])
    target_ranked = rankdata(np.abs(original))
    for index in range(permutations):
        perm = np.arange(len(frame))
        for stratum in blocks:
            for receiver, donor in zip(stratum, rng.permutation(len(stratum))):
                perm[receiver] = stratum[donor]
        y = target_ranked[perm]
        residual_target = y - baseline_ranked @ (projector @ y)
        null[index] = np.corrcoef(residual_score, residual_target)[0,1]
    p = float((1 + (np.abs(null) >= abs(observed)).sum()) / (permutations + 1))
    cluster_rows = [group.index.to_numpy() for _, group in frame.groupby("orthogroup", sort=True)]
    bootstrap = []
    for _ in range(max(bootstrap_replicates + 10, math.ceil(1.2 * bootstrap_replicates))):
        sample_indices = np.concatenate([cluster_rows[i] for i in rng.integers(0, len(cluster_rows), size=len(cluster_rows))])
        sampled = frame.iloc[sample_indices]
        if noise_factor is not None:
            joint_effect = original + noise_factor @ rng.standard_normal(len(frame))
            sampled = sampled.copy()
            sampled["effect"] = joint_effect[sample_indices]
        try:
            bootstrap.append(incremental_rank_association(sampled))
        except MolecularError:
            pass
        if len(bootstrap) == bootstrap_replicates:
            break
    if len(bootstrap) != bootstrap_replicates or len(bootstrap) < 10:
        raise MolecularError("Cluster bootstrap is degenerate for the available mapped gene families")
    baseline = np.column_stack([np.ones(len(frame)), frame[BASELINE_COLUMNS].to_numpy(), frame.reversible_score.to_numpy()])
    weights = 1 / np.square(frame.effect_se.to_numpy())
    weighted = np.linalg.lstsq(baseline * np.sqrt(weights)[:, None], np.abs(original) * np.sqrt(weights), rcond=None)[0]
    return {"genes": len(frame), "orthogroups": frame.orthogroup.nunique(), "primary_incremental_rank_correlation": observed, "incremental_rank_ci95": np.quantile(bootstrap, [.025, .975]).tolist(), "bootstrap_unit": "orthogroup", "bootstrap_replicates": len(bootstrap), "shared_control_covariance_propagated": noise_factor is not None, "interval_method": "orthogroup bootstrap plus joint Gaussian estimation-error propagation from actual shared-control gene covariance" if noise_factor is not None else "orthogroup bootstrap conditional on observed gene estimates", "permutation_interpretation": "Conditional on observed gene estimates; assumes whole-family exchangeability within fixed nuisance bins; does not integrate estimation uncertainty into the permutation P value", "unadjusted_spearman": float(spearmanr(frame.reversible_score, np.abs(frame.effect)).statistic), "permutation_p": p, "permutations": permutations, "movable_genes": movable, "permutation_seed": seed, "null_quantiles": np.quantile(null, [0.025, 0.5, 0.975]).tolist(), "weighted_molecular_coefficient": float(weighted[-1]), "freeze_hash": freeze.get("freeze_hash"), "secondary_learned_predictor_eligible": len(frame) >= 40 and frame.orthogroup.nunique() >= 20, "baseline_columns": BASELINE_COLUMNS}


def nested_gene_prediction(frame: pd.DataFrame, *, embedding_columns: list[str] | None = None, seed: int = 271828) -> pd.DataFrame:
    """Nested orthogroup-held-out tuning with train-only transformations."""
    from sklearn.decomposition import PCA
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.linear_model import ElasticNet, Ridge
    from sklearn.model_selection import GridSearchCV, GroupKFold
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.compose import ColumnTransformer
    if len(frame) < 40 or frame.orthogroup.nunique() < 20 or frame.gene_id.duplicated().any():
        raise MolecularError("Supervised augmentation requires forty unique genes across twenty orthogroups")
    embedding_columns = embedding_columns or []
    baseline_columns = BASELINE_COLUMNS + (["sequence_identity"] if embedding_columns else [])
    columns = baseline_columns + ["reversible_score"] + embedding_columns
    if not set(columns).issubset(frame) or not np.isfinite(frame[columns + ["effect"]].to_numpy(dtype=float)).all():
        raise MolecularError("Predictor features must be complete and finite")
    groups, target = frame.orthogroup.to_numpy(), np.abs(frame.effect.to_numpy())
    outer = GroupKFold(n_splits=5)
    models = {
        "ridge": (Ridge(), {"model__alpha": np.logspace(-4, 4, 11)}),
        "elastic_net": (ElasticNet(max_iter=20_000, random_state=seed), {"model__alpha": np.logspace(-4, 4, 11), "model__l1_ratio": [0.1, 0.5, 0.9]}),
        "shallow_boost": (GradientBoostingRegressor(random_state=seed), {"model__max_depth": [1, 2, 3], "model__n_estimators": [50, 100, 200], "model__learning_rate": [0.03, 0.1]}),
    }
    rows = []
    for fold, (train, test) in enumerate(outer.split(frame, target, groups)):
        for feature_set in ["baseline", "molecular"] + (["molecular_embeddings"] if embedding_columns else []):
            selected = baseline_columns + ([] if feature_set == "baseline" else ["reversible_score"])
            if feature_set == "molecular_embeddings":
                selected += embedding_columns
            for name, (model, grid) in models.items():
                transforms = [("scalar", StandardScaler(), [c for c in selected if c not in embedding_columns])]
                if feature_set == "molecular_embeddings":
                    # PCA capacity cannot exceed the smallest inner training fold.
                    inner = GroupKFold(n_splits=3)
                    smallest = min(len(tr) for tr, _ in inner.split(frame.iloc[train], target[train], groups[train]))
                    transforms.append(("embedding", Pipeline([("scale", StandardScaler()), ("pca", PCA(n_components=min(16, len(embedding_columns), smallest)))]), embedding_columns))
                pipeline = Pipeline([("features", ColumnTransformer(transforms)), ("model", model)])
                search = GridSearchCV(pipeline, grid, scoring="neg_mean_squared_error", cv=GroupKFold(n_splits=3), n_jobs=1, error_score="raise")
                search.fit(frame.iloc[train][selected], target[train], groups=groups[train])
                prediction = search.predict(frame.iloc[test][selected])
                rows.extend({"gene_id": frame.iloc[index].gene_id, "orthogroup": frame.iloc[index].orthogroup, "outer_fold": fold, "model": name, "feature_set": feature_set, "observed_magnitude": target[index], "predicted_magnitude": float(value), "training_genes": len(train), "best_parameters": json.dumps(search.best_params_, sort_keys=True)} for index, value in zip(test, prediction))
    return pd.DataFrame(rows)


def tool_runtime_manifest() -> dict:
    """Two official pinned source archives; digests verified on 2026-10-12."""
    from .acquire import asset
    assets = [asset(source="tool_runtime", name=item["filename"], url=item["url"], version=item["version"], rights="Retain upstream LICENSE files; no source-data relicensing", size=item["bytes"], checksum="sha256:"+item["sha256"], kind="software_archive", tool=name, source_commit=item["commit"]) for name, item in TOOL_RUNTIME.items()]
    manifest = {"schema_version": 1, "created_utc": utc_now(), "assets": assets, "sources": {"tool_runtime": TOOL_RUNTIME}, "status": "pinned_software_plan", "parent_hash": None, "digest_provenance": "SHA-256 computed from official GitHub HTTPS archives; upstream old release metadata supplies no digest; exact bytes/version/commit retained"}
    manifest["manifest_hash"] = canonical_hash(manifest)
    return manifest


def orthofinder_launcher_text(official_script: str | Path) -> str:
    """Run unchanged vendor code with optional inherited C-level stack capture.

    Registering the C handler in every fork child makes blocked Python, native
    extension and multiprocessing workers observable without changing their
    numerical code, start method or work dispatch. No signal is sent unless its
    PID-specific registration file exists in this command's fresh namespace.
    """
    return """import faulthandler, os, runpy, signal, sys
from pathlib import Path
official = Path(%r)
trace_directory = os.environ.get('HHW_OF_TRACE_DIRECTORY')
trace = None
def register_trace():
    global trace
    trace = (Path(trace_directory) / (str(os.getpid()) + '.log')).open('xb', buffering=0)
    trace.write(('PID=' + str(os.getpid()) + ' PPID=' + str(os.getppid()) + '\\n').encode())
    faulthandler.enable(file=trace, all_threads=True)
    faulthandler.register(signal.SIGUSR2, file=trace, all_threads=True, chain=False)
if trace_directory and hasattr(signal, 'SIGUSR2') and hasattr(os, 'register_at_fork'):
    register_trace()
    os.register_at_fork(after_in_child=register_trace)
sys.path.insert(0, str(official.parent))
sys.argv[0] = str(official)
runpy.run_path(str(official), run_name='__main__')
""" % str(Path(official_script).resolve())


def tool_process_snapshot(process_group: int, trace_directory: str | Path, *, proc_root: str | Path = "/proc") -> list[dict]:
    """Inspect only this new process group; signal registered Python workers."""
    if os.name != "posix" or not hasattr(signal, "SIGUSR2"):
        return [{"status": "process_group_diagnostics_unavailable", "platform": os.name}]
    trace_directory, proc_root = Path(trace_directory), Path(proc_root)
    rows = []
    for item in proc_root.iterdir():
        if not item.name.isdigit():
            continue
        try:
            fields = (item / "stat").read_text().rsplit(")", 1)[1].split()
            if int(fields[2]) != process_group:
                continue
            row = {"pid": int(item.name), "parent_pid": int(fields[1]), "state": fields[0],
                "wait_channel": (item / "wchan").read_text().strip(), "trace_requested": False}
            # External binaries have no registration file and must not receive
            # SIGUSR2, whose default disposition would terminate them.
            if (trace_directory / (item.name + ".log")).is_file() and os.getpgid(int(item.name)) == process_group:
                os.kill(int(item.name), signal.SIGUSR2)
                row["trace_requested"] = True
            rows.append(row)
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        except (OSError, ValueError, IndexError) as error:
            rows.append({"pid": int(item.name), "diagnostic_error": type(error).__name__})
    return rows


def run_bounded_tool(command, log_path, *, env=None, cwd=None, timeout_seconds=300, python_worker_traces=False) -> dict:
    """Stream an external utility's output and bound its entire process group."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise MolecularError("External tool deadline must be finite and positive")
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    status = "completed"
    diagnostics = []
    if python_worker_traces:
        trace_directory = log_path.with_suffix(".traces")
        trace_directory.mkdir(exist_ok=False)
        env = (env or os.environ).copy()
        env["HHW_OF_TRACE_DIRECTORY"] = str(trace_directory.resolve())
    def snapshot(process):
        if python_worker_traces:
            diagnostics.append({"elapsed_seconds": time.monotonic()-started,
                "processes": tool_process_snapshot(process.pid, trace_directory)})
            atomic_json(log_path.with_suffix(".diagnostics.json"), {"snapshots": diagnostics,
                "trace_directory": str(trace_directory.resolve()), "status": "running"})
    with log_path.open("xb") as log:
        process = subprocess.Popen([str(value) for value in command], stdout=log,
            stderr=subprocess.STDOUT, env=env, cwd=cwd, start_new_session=os.name == "posix")
        try:
            deadlines = [value for value in [15, 45, 120, 240] if value < timeout_seconds] if python_worker_traces else []
            for deadline in [*deadlines, timeout_seconds]:
                try:
                    returncode = process.wait(timeout=max(.01, deadline-(time.monotonic()-started)))
                    break
                except subprocess.TimeoutExpired:
                    snapshot(process)
                    if deadline == timeout_seconds:
                        raise
        except subprocess.TimeoutExpired:
            status = "timed_out"
            if python_worker_traces:
                time.sleep(.1)  # the asynchronous C handlers flush into per-PID files
            if os.name == "posix":
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            else:
                process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
                process.wait(timeout=5)
            returncode = process.returncode
    report = {"command": [str(value) for value in command], "status": status,
        "returncode": returncode, "elapsed_seconds": time.monotonic()-started,
        "timeout_seconds": timeout_seconds, "log_path": str(log_path.resolve()),
        "log_sha256": file_hash(log_path)}
    if python_worker_traces:
        diagnostic_path = log_path.with_suffix(".diagnostics.json")
        atomic_json(diagnostic_path, {"snapshots": diagnostics, "trace_directory": str(trace_directory.resolve()),
            "trace_hashes": {str(path.resolve()): file_hash(path) for path in sorted(trace_directory.glob("*.log"))},
            "status": status, "native_job_id": native_job_id()})
        report.update(worker_diagnostics_path=str(diagnostic_path.resolve()), worker_diagnostics_sha256=file_hash(diagnostic_path))
    atomic_json(log_path.with_suffix(".receipt.json"), report)
    if status != "completed" or returncode != 0:
        raise MolecularError(f"External tool {status}, exit {returncode}; inspect {log_path}")
    return report


def qualification_proteomes(example, output) -> dict:
    """A small identity fixture from the exact verified utility archive."""
    example, output = Path(example), Path(output)
    candidates = sorted(path for path in example.iterdir() if path.suffix in {".fa", ".faa", ".fasta"})
    if not candidates:
        raise MolecularError("Pinned OrthoFinder archive has no official example proteome")
    source = candidates[0]
    selected, sequences, lengths = [], set(), set()
    for name, _, sequence in sorted(read_fasta(source)):
        if 80 <= len(sequence) <= 400 and set(sequence).issubset(set("ACDEFGHIKLMNPQRSTVWY")) and sequence not in sequences and len(sequence) not in lengths:
            selected.append((name, sequence)); sequences.add(sequence); lengths.add(len(sequence))
        if len(selected) == RUNTIME_QUALIFICATION_RECIPE["orthofinder_fixture_proteins"]:
            break
    if len(selected) != RUNTIME_QUALIFICATION_RECIPE["orthofinder_fixture_proteins"]:
        raise MolecularError("Official example lacks the required eight distinct bounded proteins")
    output.mkdir(parents=True, exist_ok=False)
    pairs = []
    for species in RUNTIME_QUALIFICATION_RECIPE["orthofinder_fixture_species"]:
        with (output / (species+".fa")).open("x", encoding="utf-8", newline="\n") as handle:
            for index, (_, sequence) in enumerate(selected):
                handle.write(f">{species}_p{index:03d}\n{sequence}\n")
            handle.write(f">{species}_p000_duplicate\n{selected[0][1]}\n")
    for index, (source_id, sequence) in enumerate(selected):
        pairs.append({"source_id": source_id, "sequence_sha256": hashlib.sha256(sequence.encode()).hexdigest(),
            "pair": [f"{species}_p{index:03d}" for species in RUNTIME_QUALIFICATION_RECIPE["orthofinder_fixture_species"]]})
    return {"source_path": str(source.resolve()), "source_sha256": file_hash(source),
        "input_hashes": {str(path.resolve()): file_hash(path) for path in output.glob("*.fa")},
        "expected_identical_pairs": pairs,
        "expected_paralog_group": [f"{species}_{suffix}" for species in RUNTIME_QUALIFICATION_RECIPE["orthofinder_fixture_species"] for suffix in ["p000", "p000_duplicate"]],
        "evidence_class": "software fixture; no biological species comparison"}


def validate_qualification_orthogroups(path, fixture) -> dict:
    frame = pd.read_csv(path, sep="\t", dtype=str).fillna("")
    species = RUNTIME_QUALIFICATION_RECIPE["orthofinder_fixture_species"]
    if not {"Orthogroup", *species}.issubset(frame):
        raise MolecularError("Qualification lacks the actual two-species orthogroup table")
    membership = {}
    for row in frame.to_dict("records"):
        for name in species:
            for protein in filter(None, (value.strip() for value in row[name].split(","))):
                if protein in membership:
                    raise MolecularError("Qualification protein appears in multiple orthogroups")
                membership[protein] = row["Orthogroup"]
    expected = {protein for item in fixture["expected_identical_pairs"] for protein in item["pair"]}
    expected.update(fixture.get("expected_paralog_group", []))
    if set(membership) != expected:
        raise MolecularError("Qualification did not assign all and only the known fixture proteins")
    for item in fixture["expected_identical_pairs"]:
        if len({membership[protein] for protein in item["pair"]}) != 1:
            raise MolecularError("An identical qualification protein pair did not share an orthogroup")
    paralogs = fixture.get("expected_paralog_group", [])
    if len(paralogs) != 4 or len({membership[protein] for protein in paralogs}) != 1:
        raise MolecularError("Qualification did not recover its known four-gene paralog group")
    return {"validated_identical_pairs": len(fixture["expected_identical_pairs"]),
        "validated_paralog_group_genes": len(paralogs),
        "shared_orthogroups": len(set(membership.values())), "orthogroups_path": str(Path(path).resolve()),
        "orthogroups_sha256": file_hash(path), "biological_accuracy_claim": False}


def validate_qualification_hogs(path, fixture) -> dict:
    """Require the actual full two-species root-HOG output, not only clustering."""
    frame = pd.read_csv(path, sep="\t", dtype=str).fillna("")
    species = RUNTIME_QUALIFICATION_RECIPE["orthofinder_fixture_species"]
    if "HOG" not in frame or not set(species).issubset(frame):
        raise MolecularError("Qualification lacks an actual full root-HOG table")
    membership = {}
    for row in frame.to_dict("records"):
        for name in species:
            for protein in filter(None, (value.strip() for value in row[name].split(","))):
                if protein in membership:
                    raise MolecularError("Qualification root HOG repeats a protein")
                membership[protein] = row["HOG"]
    pairs, paralogs = fixture["expected_identical_pairs"], fixture["expected_paralog_group"]
    expected = {protein for item in pairs for protein in item["pair"]} | set(paralogs)
    if set(membership) != expected:
        raise MolecularError("Full root HOGs must retain every known qualification protein")
    if any(len({membership[protein] for protein in item["pair"]}) != 1 for item in pairs) or len({membership[protein] for protein in paralogs}) != 1:
        raise MolecularError("Full root HOGs did not recover the known identity pairs and paralogs")
    return {"root_hog_path": str(Path(path).resolve()), "root_hog_sha256": file_hash(path),
        "validated_hog_proteins": len(expected), "validated_root_paralog_group_genes": len(paralogs)}


def validate_fastme_fixture(path) -> dict:
    tree = Path(path).read_text().strip()
    leaves = re.findall(r"[,(]\s*(qual_[abcd])\s*:", tree)
    if len(leaves) != 4 or set(leaves) != {"qual_a", "qual_b", "qual_c", "qual_d"}:
        raise MolecularError("FastME qualification lacks its four known utility fixture labels")
    splits = [re.search(r"\(\s*"+first+r"\s*:[^,():;]+,\s*"+second+r"\s*:[^,():;]+\)", tree)
        for first, second in [("qual_a", "qual_b"), ("qual_b", "qual_a"), ("qual_c", "qual_d"), ("qual_d", "qual_c")]]
    if not any(splits):
        raise MolecularError("FastME qualification did not recover the known AB|CD utility split")
    return {"tree_path": str(Path(path).resolve()), "tree_sha256": file_hash(path),
        "expected_split_recovered": True, "evidence_class": "synthetic numerical software fixture, not biological data"}


def numerical_runtime_proof() -> dict:
    """Freeze the actual Python package versions and native MINPACK implementation."""
    import importlib
    import scipy
    modules = [np, scipy, importlib.import_module("scipy.optimize._minpack")]
    try:
        modules.append(importlib.import_module("numpy._core._multiarray_umath"))
    except ModuleNotFoundError:
        modules.append(importlib.import_module("numpy.core._multiarray_umath"))
    return {"versions": {"numpy": np.__version__, "scipy": scipy.__version__},
        "module_hashes": {str(Path(module.__file__).resolve()): file_hash(module.__file__) for module in modules},
        "library_threads": 1, "algorithm_threads_control": "OrthoFinder -a remains explicitly supplied by each bounded job"}


def qualify_orthofinder_python(python: str | Path, log_path: str | Path) -> dict:
    """Check the isolated, pinned numerical stack using unchanged curve_fit."""
    script = """import hashlib, json, multiprocessing as mp, sys
from pathlib import Path
import numpy as np, scipy
from scipy.optimize import curve_fit, _minpack
def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def known_fit():
    x = np.asarray([80,100,150,200,250,300,350,400], dtype=float)**2
    def loglinear(x,a,b): return a*np.log10(x)+b
    target = loglinear(x,1.37,0.42)
    fit, _ = curve_fit(loglinear, x, target)
    if not np.allclose(fit,[1.37,0.42],rtol=1e-8,atol=1e-8):
        raise RuntimeError('Known log-linear MINPACK fit failed')
known_fit()
if sys.platform != 'win32':
    worker = mp.get_context('fork').Process(target=known_fit)
    worker.start(); worker.join(10)
    if worker.is_alive():
        worker.terminate(); worker.join(2)
        raise RuntimeError('Forked known log-linear fit timed out')
    if worker.exitcode != 0:
        raise RuntimeError('Forked known log-linear fit failed')
try:
    from numpy._core import _multiarray_umath
except ModuleNotFoundError:
    from numpy.core import _multiarray_umath
paths = [Path(module.__file__).resolve() for module in [np,scipy,_minpack,_multiarray_umath]]
venv_config = Path(sys.prefix)/'pyvenv.cfg'
if venv_config.is_file(): paths.append(venv_config)
record = {'versions':{'numpy':np.__version__,'scipy':scipy.__version__},
    'module_hashes':{str(path):sha(path) for path in paths},'python_executable':sys.executable,
    'python_prefix':sys.prefix,'python_version':sys.version,'library_threads':1,
    'algorithm_threads_control':'OrthoFinder -a remains explicitly supplied by each bounded job',
    'known_serial_fit_passed':True,'known_forked_fit_passed':sys.platform!='win32'}
print('HHW_OF_NUMERICAL_PROOF '+json.dumps(record,sort_keys=True))
"""
    python = Path(python)
    if not python.is_absolute() or not python.is_file():
        raise MolecularError("OrthoFinder requires an existing absolute isolated Python interpreter")
    env = os.environ.copy()
    for name in ["PYTHONPATH", "PYTHONHOME"]:
        env.pop(name, None)
    env.update(PYTHONNOUSERSITE="1", PYTHONDONTWRITEBYTECODE="1", OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1")
    run_bounded_tool([str(python), "-c", script], log_path, env=env, timeout_seconds=30)
    lines = [line[len("HHW_OF_NUMERICAL_PROOF "):] for line in Path(log_path).read_text().splitlines() if line.startswith("HHW_OF_NUMERICAL_PROOF ")]
    if len(lines) != 1:
        raise MolecularError("Isolated numerical check did not emit one unambiguous proof")
    record = json.loads(lines[0])
    if {name: version.split("+", 1)[0] for name, version in record["versions"].items()} != ORTHOFINDER_PYTHON_PACKAGES:
        raise MolecularError("OrthoFinder needs its isolated NumPy 1.26.4 / SciPy 1.11.2 environment; preserve the main study environment")
    return record


def validate_tool_runtime(destination: str | Path) -> dict:
    """Reuse only a completed, unchanged qualified runtime and its proof."""
    destination = Path(destination)
    receipt = destination / "runtime_qualification.json"
    if not receipt.is_file():
        raise MolecularError("Runtime destination is unqualified; preserve it and choose a new prospective directory")
    report = json.loads(receipt.read_text())
    if Path(report.get("bin", "")).resolve() != (destination / "bin").resolve():
        raise MolecularError("Runtime receipt belongs to a different prospective directory")
    if report.get("status") != "runtime_qualified" or report.get("recipe") != RUNTIME_QUALIFICATION_RECIPE or report.get("tools") != TOOL_RUNTIME:
        raise MolecularError("Runtime qualification is incomplete or uses a different qualification recipe")
    if report.get("qualification_hash") != canonical_hash({k: v for k, v in report.items() if k != "qualification_hash"}):
        raise MolecularError("Runtime qualification receipt changed")
    expected_archives = {item["sha256"]: item for item in TOOL_RUNTIME.values()}
    archives = report.get("archives", [])
    if len(archives) != len(expected_archives) or {item.get("sha256") for item in archives} != set(expected_archives):
        raise MolecularError("Runtime receipt lacks the complete pinned source archive proof")
    for archive in archives:
        expected = expected_archives[archive["sha256"]]
        path = Path(archive.get("path", ""))
        if archive.get("provider_checksum") != "sha256:"+expected["sha256"] or archive.get("bytes") != expected["bytes"] or not path.is_file() or file_hash(path) != expected["sha256"]:
            raise MolecularError("Pinned runtime source archive is missing or changed")
    proof = report.get("runtime_file_hashes", {})
    if any(not Path(path).resolve().is_relative_to(destination.resolve()) for path in proof):
        raise MolecularError("Runtime file proof leaves its qualified directory")
    if not proof or not all(Path(path).is_file() and file_hash(path) == digest for path, digest in proof.items()):
        raise MolecularError("Qualified runtime, wrapper, fixture, or output is missing or changed")
    for name in ["orthofinder", "TransDecoder.LongOrfs", "TransDecoder.Predict"]:
        wrapper = (destination / "bin" / name).resolve()
        if str(wrapper) not in proof or (os.name == "posix" and not os.access(wrapper, os.X_OK)):
            raise MolecularError("Qualified runtime command wrapper is missing or not executable")
    for interpreter in report.get("interpreters", {}).values():
        if not Path(interpreter["path"]).is_file() or file_hash(interpreter["path"]) != interpreter["sha256"]:
            raise MolecularError("Qualified runtime interpreter changed")
    if set(report.get("interpreters", {})) != {"python", "perl", "orthofinder_python"}:
        raise MolecularError("Qualified runtime lacks exact Python and isolated Perl provenance")
    numerical = report.get("numerical_runtime", {})
    if {name: version.split("+", 1)[0] for name, version in numerical.get("versions", {}).items()} != ORTHOFINDER_PYTHON_PACKAGES or not numerical.get("module_hashes") or any(not Path(path).is_file() or file_hash(path) != digest for path, digest in numerical.get("module_hashes", {}).items()):
        raise MolecularError("Qualified isolated Python numerical packages or native MINPACK implementation changed")
    orthofinder_check = report.get("checks", {}).get("orthofinder_example", {})
    if orthofinder_check.get("validated_identical_pairs") != 8 or orthofinder_check.get("validated_paralog_group_genes") != 4 or orthofinder_check.get("validated_hog_proteins") != 18 or report.get("checks", {}).get("transdecoder_example", {}).get("peptides", 0) <= 0 or report.get("checks", {}).get("fastme_numerical", {}).get("expected_split_recovered") is not True:
        raise MolecularError("Runtime receipt lacks actual orthogroup and peptide checks")
    return report


def restore_software_executable_modes(archive, destination):
    """After safe extraction, preserve executable bits of internal-file aliases."""
    import tarfile
    from .acquire import safe_destination
    with tarfile.open(archive, "r:gz") as handle:
        index = {posixpath.normpath(member.name): member for member in handle.getmembers()}
        for member in index.values():
            if member.isdir():
                continue
            payload, seen = member, set()
            while payload.issym() or payload.islnk():
                if payload.name in seen:
                    raise MolecularError("Cyclic software archive link")
                seen.add(payload.name)
                name = posixpath.normpath(posixpath.join(posixpath.dirname(payload.name), payload.linkname) if payload.issym() else payload.linkname)
                safe_destination(destination, name)
                if name not in index:
                    raise MolecularError("Software archive link target is absent")
                payload = index[name]
            if not payload.isfile():
                raise MolecularError("Software archive contains a non-file executable payload")
            if payload.mode & 0o111:
                safe_destination(destination, member.name).chmod(0o755)


def install_tool_runtime(root: str | Path, destination: str | Path, *, qualify: bool = True, perl_binary: str | Path = SITE_PERL, orthofinder_python: str | Path | None = None) -> dict:
    """Install from pretransferred verified archives; perform no network fetch.

    This job is numerical/software qualification and belongs in Slurm. The
    external tools are the explicit authorized analysis utilities, not commands
    copied from a dataset README. No downloaded installer is executed.
    """
    from .acquire import safe_destination, safe_extract, verify_file
    root, destination = Path(root).resolve(), Path(destination).resolve()
    if destination.exists() and any(destination.iterdir()):
        return validate_tool_runtime(destination)
    perl = Path(perl_binary)
    of_python = Path(orthofinder_python or os.environ.get("HHW_ORTHOFINDER_PYTHON", sys.executable))
    if not perl.is_absolute() or not perl.is_file():
        raise MolecularError("The verified isolated site Perl interpreter must be an existing absolute path")
    destination.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    atomic_json(destination / "runtime_attempt.json", {"status": "started", "created_utc": utc_now(),
        "recipe": RUNTIME_QUALIFICATION_RECIPE, "perl_binary": str(perl), "slurm_job_id": native_job_id()})
    manifest = tool_runtime_manifest()
    receipts = []
    for item in manifest["assets"]:
        archive = safe_destination(root, item["relative_path"])
        receipts.append(verify_file(archive, item))
        safe_extract(archive, destination, max_unpacked_bytes=250*1024**2, max_members=10_000,materialize_internal_links=True)
        # Restore only original executable bits for ordinary reviewed utility
        # members after the traversal/link gate; data extraction stays nonexec.
        restore_software_executable_modes(archive, destination)
    bin_directory = destination / "bin"
    bin_directory.mkdir(exist_ok=True)
    orthofinder = destination / TOOL_RUNTIME["orthofinder"]["directory"] / "orthofinder.py"
    launcher = bin_directory / "orthofinder_launcher.py"
    launcher.write_text(orthofinder_launcher_text(orthofinder), encoding="utf-8", newline="\n")
    td = destination / TOOL_RUNTIME["transdecoder"]["directory"]
    wrapper = bin_directory / "orthofinder"
    wrapper.write_text("#!/bin/sh\nunset PYTHONPATH PYTHONHOME\nexport PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1\nexport OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1\nexec " + q(of_python) + " " + q(launcher) + ' "$@"\n', encoding="utf-8", newline="\n")
    wrapper.chmod(0o755)
    for name in ["TransDecoder.LongOrfs", "TransDecoder.Predict"]:
        command = bin_directory / name
        command.write_text("#!/bin/sh\nunset PERL5LIB PERL_LOCAL_LIB_ROOT PERL_MB_OPT PERL_MM_OPT\nPATH=" + q(perl.parent) + ':"$PATH"; export PATH\nexec ' + q(perl) + " " + q(td / name) + ' "$@"\n', encoding="utf-8", newline="\n")
        command.chmod(0o755)
    env = os.environ.copy()
    env["PATH"] = str(bin_directory) + os.pathsep + env.get("PATH", "")
    for name in ["PERL5LIB", "PERL_LOCAL_LIB_ROOT", "PERL_MB_OPT", "PERL_MM_OPT"]:
        env.pop(name, None)
    env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1", OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1")
    qualification = destination / "qualification"
    qualification.mkdir()
    checks = {}
    numerical = qualify_orthofinder_python(of_python, qualification / "numerical-python.log")
    checks["perl_dependencies"] = run_bounded_tool([str(perl), "-MDB_File", "-MURI::Escape", "-e", "print qq(perl=$^V DB_File=$DB_File::VERSION URI=$URI::VERSION\\n)"], qualification / "perl-dependencies.log", env=env, timeout_seconds=30)
    if "perl=v5.30.2 DB_File=1.843 URI=" not in Path(checks["perl_dependencies"]["log_path"]).read_text():
        raise MolecularError("Isolated site Perl does not match the independently verified 5.30.2/DB_File 1.843 environment")
    for name, command in [("orthofinder", [str(wrapper), "-h"]), ("transdecoder", [str(bin_directory / "TransDecoder.Predict"), "--version"])]:
        checks[name] = run_bounded_tool(command, qualification / (name+"-version.log"), env=env, timeout_seconds=30)
        if TOOL_RUNTIME[name]["version"] not in Path(checks[name]["log_path"]).read_text():
            raise MolecularError(f"Pinned {name} did not report its declared version")
    # The pinned OrthoFinder source distribution includes mcl, diamond and fastme;
    # its normal distance-tree mode requires these, not an unpinned package solve.
    for name in ["mcl", "diamond", "fastme"]:
        binary = destination / "OrthoFinder_source" / "scripts_of" / "bin" / name
        if not binary.exists():
            raise MolecularError(f"Pinned OrthoFinder archive lacks {name}")
        options = ["version"] if name == "diamond" else ["--version"] if name == "mcl" else ["-h"]
        checks[name] = run_bounded_tool([str(binary), *options], qualification / (name+"-binary.log"), env=env, timeout_seconds=30)
    if qualify:
        matrix = qualification / "fastme-distances.phy"
        matrix.write_text("4\nqual_a 0 1 3 3\nqual_b 1 0 3 3\nqual_c 3 3 0 1\nqual_d 3 3 1 0\n", encoding="utf-8", newline="\n")
        tree = qualification / "fastme-tree.nwk"
        fastme = destination / "OrthoFinder_source" / "scripts_of" / "bin" / "fastme"
        command = run_bounded_tool([str(fastme), "-i", str(matrix), "-o", str(tree)], qualification / "fastme-numerical.log", env=env, cwd=qualification, timeout_seconds=30)
        checks["fastme_numerical"] = {**command, **validate_fastme_fixture(tree)}
        example = destination / "OrthoFinder_source" / "ExampleData"
        fixture = qualification_proteomes(example, qualification / "proteomes")
        atomic_json(qualification / "fixture.json", fixture)
        command = run_bounded_tool([str(wrapper), "-f", str(qualification / "proteomes"), "-t", "2", "-a", "2", "-S", "diamond", "-o", str(qualification / "orthofinder")], qualification / "orthofinder-example.log", env=env, timeout_seconds=RUNTIME_QUALIFICATION_RECIPE["orthofinder_seconds"], python_worker_traces=True)
        candidates = list((qualification / "orthofinder").rglob("Orthogroups/Orthogroups.tsv"))
        if len(candidates) != 1:
            raise MolecularError("Qualification lacks one unambiguous actual orthogroup output")
        checks["orthofinder_example"] = {**command, **validate_qualification_orthogroups(candidates[0], fixture)}
        root_hogs = list((qualification / "orthofinder").rglob("Phylogenetic_Hierarchical_Orthogroups/N0.tsv"))
        if len(root_hogs) != 1:
            raise MolecularError("Qualification lacks one unambiguous actual full root HOG output")
        checks["orthofinder_example"].update(validate_qualification_hogs(root_hogs[0], fixture))
        input_fasta = td / RUNTIME_QUALIFICATION_RECIPE["transdecoder_fixture"]
        if not input_fasta.exists():
            raise MolecularError("Pinned TransDecoder archive lacks its official example FASTA")
        copied = qualification / "Trinity.fasta"
        with gzip.open(input_fasta, "rb") as source, copied.open("wb") as target:
            size = 0
            while block := source.read(1024**2):
                size += len(block)
                if size > RUNTIME_QUALIFICATION_RECIPE["transdecoder_max_uncompressed_bytes"]:
                    raise MolecularError("Official TransDecoder qualification fixture exceeds its declared ceiling")
                target.write(block)
        checks["transdecoder_longorfs"] = run_bounded_tool([str(bin_directory / "TransDecoder.LongOrfs"), "-t", str(copied), "-m", "100"], qualification / "transdecoder-longorfs.log", cwd=qualification, env=env, timeout_seconds=RUNTIME_QUALIFICATION_RECIPE["transdecoder_longorfs_seconds"])
        command = run_bounded_tool([str(bin_directory / "TransDecoder.Predict"), "-t", str(copied), "--single_best_only"], qualification / "transdecoder-predict.log", cwd=qualification, env=env, timeout_seconds=RUNTIME_QUALIFICATION_RECIPE["transdecoder_predict_seconds"])
        peptides = Path(str(copied)+".transdecoder.pep")
        if not peptides.is_file() or not peptides.stat().st_size:
            raise MolecularError("TransDecoder qualification produced no peptides")
        transcripts = read_fasta(copied)
        mapping = pd.DataFrame({"transcript_id": [item[0] for item in transcripts], "gene_id": [item[0] for item in transcripts]})
        _, coverage = transdecoder_gene_map(peptides, mapping)
        checks["transdecoder_example"] = {**command, "peptide_sha256": file_hash(peptides), "peptides": len(read_fasta(peptides)), "input_transcripts": len(transcripts), "source_fixture_sha256": file_hash(input_fasta), "input_fasta_sha256": file_hash(copied), "coverage": coverage, "status": "official_example_executed"}
    report = {"schema_version": 2, "status": "runtime_qualified" if qualify else "runtime_installed_help_checked", "tools": TOOL_RUNTIME, "recipe": RUNTIME_QUALIFICATION_RECIPE, "archives": receipts, "checks": checks, "bin": str(bin_directory), "created_utc": utc_now(), "slurm_job_id": native_job_id(), "elapsed_seconds": time.monotonic()-started, "benchmark_uses_full_primary_numerics": False, "full_native_workload_completed": False, "evidence_class": "bounded software qualification; no biological result or full-proteome throughput measurement", "interpreters": {"python": {"path": str(Path(sys.executable).resolve()), "sha256": file_hash(sys.executable), "version": sys.version}, "perl": {"path": str(perl.resolve()), "sha256": file_hash(perl), "version_and_dependencies": Path(checks["perl_dependencies"]["log_path"]).read_text()[-2000:]}}, "runtime_file_hashes": {str(path.resolve()): file_hash(path) for path in sorted(destination.rglob("*")) if path.is_file() and path.name not in {"runtime_qualification.json", "runtime_attempt.json"}}}
    report.update(source_sha256=source_identity(Path(__file__).resolve().parents[2])["sha256"],
        allocated_cpus=int(os.environ["SLURM_CPUS_PER_TASK"]) if os.environ.get("SLURM_CPUS_PER_TASK") else None,
        wrapper_targets={"orthofinder": str(orthofinder), "orthofinder_diagnostic_launcher": str(launcher), "TransDecoder.LongOrfs": str(td / "TransDecoder.LongOrfs"), "TransDecoder.Predict": str(td / "TransDecoder.Predict")},
        perl_environment_policy="Wrappers clear PERL5LIB/PERL_LOCAL_LIB_ROOT/PERL_MB_OPT/PERL_MM_OPT and prepend only the isolated Perl bin; outer R/Python module environment is retained")
    report["numerical_runtime"] = numerical
    report["interpreters"]["orthofinder_python"] = {"path": str(of_python.absolute()), "sha256": file_hash(of_python), "prefix": numerical["python_prefix"], "version": numerical["python_version"]}
    report["qualification_hash"] = canonical_hash(report)
    atomic_json(destination / "runtime_qualification.json", report)
    return report


def validate_freeze(path: str | Path) -> tuple[dict, Path]:
    path = Path(path)
    freeze = json.loads(path.read_text())
    expected = freeze.get("freeze_hash")
    if not expected or canonical_hash({key: value for key, value in freeze.items() if key != "freeze_hash"}) != expected:
        raise MolecularError("Frozen molecular provenance changed")
    score = path.parent / "molecular_scores.tsv"
    if freeze.get("status") != "frozen_before_external_outcomes" or not score.is_file() or file_hash(score) != freeze.get("score_sha256"):
        raise MolecularError("Frozen molecular score file is missing or changed")
    if freeze.get("provenance", {}).get("fly_outcomes_inspected", True):
        raise MolecularError("Molecular freeze is not outcome independent")
    return freeze, score


def write_target_family_map(orthology: pd.DataFrame, output: str | Path) -> dict:
    """Freeze fly family assignments from source orthology, before screen values."""
    if not {"species", "gene_id", "orthogroup"}.issubset(orthology):
        raise MolecularError("Target-family map requires validated source orthology")
    if {"effect", "effect_se", "target", "fly_effect"} & set(orthology):
        raise MolecularError("Target-family source cannot include external outcomes")
    family = orthology.loc[orthology.species == "fly", ["gene_id", "orthogroup"]].drop_duplicates().sort_values("gene_id")
    if family.empty or family.isna().any().any() or family.gene_id.duplicated().any():
        raise MolecularError("Every fly gene requires exactly one source orthogroup")
    output = Path(output)
    if output.exists():
        raise MolecularError("Target-family map already exists; use a prospective amendment")
    write_table(family, output)
    report = {"status": "outcome_independent_family_map", "target_family_map_path": str(output.resolve()), "target_family_map_sha256": file_hash(output), "orthology_hash": canonical_hash(orthology.fillna("").to_dict("records")), "fly_genes": len(family), "fly_families": family.orthogroup.nunique(), "fly_outcomes_inspected": False}
    atomic_json(output.with_suffix(".provenance.json"), report)
    return report


def run_external_prediction(config: dict, root: str | Path, *, genes: str | Path, orthology: str | Path, descriptors: str | Path, freeze_path: str | Path, target_provenance: str | Path, output: str | Path, learned: bool = False) -> dict:
    """Join a validated freeze to source-proven unique gene targets and families."""
    from .core import canonical_hash as config_hash, source_identity
    root, output = Path(root), Path(output)
    manifest_path = root / "manifests" / "source_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    validate_manifest(manifest)
    freeze, score_path = validate_freeze(freeze_path)
    if freeze["provenance"]["source_manifest_hash"] != manifest["manifest_hash"]:
        raise MolecularError("The molecular freeze belongs to another biological manifest")
    target_provenance = Path(target_provenance)
    proof = json.loads(target_provenance.read_text())
    for flag in ["validated_gene_ids", "shared_control_covariance_preserved", "reagents_clustered_by_gene"]:
        if proof.get(flag) is not True:
            raise MolecularError(f"Actual external target provenance requires {flag}")
    genes = Path(genes)
    if proof.get("target_table_sha256") != file_hash(genes):
        raise MolecularError("External unique-gene target table changed or lacks source proof")
    if proof.get("molecular_freeze_hash") != freeze["freeze_hash"]:
        raise MolecularError("External gene aggregation did not verify the prior molecular freeze")
    catalog_path = Path(freeze["provenance"].get("fly_gene_catalog_path", ""))
    if not catalog_path.is_file() or freeze["provenance"].get("fly_gene_catalog_sha256") != file_hash(catalog_path):
        raise MolecularError("External target requires the unchanged outcome-blind source gene catalog")
    target_table = pd.read_csv(genes, sep="\t")
    catalog = pd.read_csv(catalog_path, sep="\t", dtype=str)
    if "gene_id" not in catalog or not set(target_table.gene_id).issubset(catalog.gene_id):
        raise MolecularError("External target IDs are absent from the prior source gene catalog")
    family_path = Path(proof.get("target_family_map_path", ""))
    if not family_path.is_file() or proof.get("target_family_map_sha256") != file_hash(family_path):
        raise MolecularError("External target-family source map is missing or changed")
    if freeze["provenance"].get("target_family_map_sha256") != file_hash(family_path):
        raise MolecularError("External target families changed after the outcome-blind freeze")
    source_paths = proof.get("source_result_paths", [])
    if not source_paths or not all(Path(path).is_file() for path in source_paths):
        raise MolecularError("External target requires actual source-result files")
    contrast_path = Path(freeze["provenance"].get("phase_contrast_path", ""))
    if not contrast_path.is_file() or freeze["provenance"].get("phase_contrast_sha256") != file_hash(contrast_path):
        raise MolecularError("Freeze requires an unchanged actual phase contrast table")
    frozen_source_paths = freeze["provenance"].get("source_result_paths", [str(contrast_path)])
    frozen_source_hashes = freeze["provenance"].get("source_result_hashes", {str(contrast_path): freeze["provenance"]["phase_contrast_sha256"]})
    if not frozen_source_paths or any(not Path(path).is_file() or frozen_source_hashes.get(path) != file_hash(path) for path in frozen_source_paths):
        raise MolecularError("Frozen molecular source result proof is missing or changed")
    orthology_table = pd.read_csv(orthology, sep="\t")
    descriptor_table = pd.read_csv(descriptors, sep="\t")
    for name, table in [("orthology_hash", orthology_table), ("nuisance_descriptor_hash", descriptor_table)]:
        if canonical_hash(table.fillna("").to_dict("records")) != freeze["provenance"][name]:
            raise MolecularError(f"Frozen {name} changed before external prediction")
    frame, mapping_report = join_frozen_gene_features(pd.read_csv(score_path, sep="\t"), orthology_table, target_table, descriptor_table)
    family = pd.read_csv(family_path, sep="\t", dtype=str)
    family_column = "family_id" if "family_id" in family else "orthogroup"
    if not {"gene_id", family_column}.issubset(family):
        raise MolecularError("Target-family map requires explicit gene_id and family_id/orthogroup columns")
    family = family[["gene_id", family_column]].drop_duplicates()
    if family.gene_id.duplicated().any() or family.isna().any().any() or not set(frame.gene_id).issubset(family.gene_id):
        raise MolecularError("Each mapped target gene must have exactly one explicit source family")
    frame = frame.rename(columns={"orthogroup": "mapped_orthogroup"}).merge(family.rename(columns={family_column: "orthogroup"}), on="gene_id", validate="one_to_one")
    if "source_target_orthogroup" in frame and not frame.source_target_orthogroup.eq(frame.orthogroup).all():
        raise MolecularError("Gene target family columns disagree with the independently frozen source family map")
    covariance_path = Path(proof.get("gene_covariance_path", ""))
    order_path = Path(proof.get("gene_covariance_order_path", ""))
    if not covariance_path.is_file() or not order_path.is_file() or proof.get("gene_covariance_sha256") != file_hash(covariance_path) or proof.get("gene_covariance_order_sha256") != file_hash(order_path):
        raise MolecularError("Actual shared-control gene covariance/order proof is missing or changed")
    order = json.loads(order_path.read_text()).get("gene_id", [])
    if len(order) != len(set(order)) or set(order) != set(target_table.gene_id):
        raise MolecularError("Shared-control covariance order does not match the unique source target genes")
    covariance = np.load(covariance_path, allow_pickle=False)
    if covariance.shape != (len(order), len(order)):
        raise MolecularError("Shared-control gene covariance dimension disagrees with source gene order")
    selected = [order.index(gene) for gene in frame.gene_id]
    covariance = covariance[np.ix_(selected, selected)]
    # Family clustering can be broader than strict one-to-one transfer groups;
    # neither related genes nor their reagents become independent permutations.
    output.mkdir(parents=True, exist_ok=True)
    report = external_prediction(frame, freeze, permutations=int(config["molecular"].get("gene_permutations", 10_000)), bootstrap_replicates=int(config["inference"].get("bootstrap_replicates", 10_000)), outcome_covariance=covariance)
    write_table(frame, output / "mapped_gene_features.tsv")
    atomic_json(output / "gene_mapping_coverage.json", mapping_report)
    inputs = [manifest_path, Path(freeze_path), score_path, genes, Path(orthology), Path(descriptors), target_provenance, catalog_path, family_path, covariance_path, order_path, contrast_path, output / "mapped_gene_features.tsv", output / "gene_mapping_coverage.json", *map(Path, source_paths), *map(Path, frozen_source_paths)]
    input_hashes = {str(path.resolve()): file_hash(path) for path in inputs}
    report.update(status="external_gene_prediction_computed", input_sha256=input_hashes,
        provenance={"config_sha256": config_hash(config), "source_identity_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"], "source_manifest_hash": manifest["manifest_hash"], "freeze_hash": freeze["freeze_hash"], "orthology_hash": freeze["provenance"]["orthology_hash"], "nuisance_descriptor_hash": freeze["provenance"]["nuisance_descriptor_hash"], "target_table_sha256": file_hash(genes), "target_family_map_sha256": file_hash(family_path), "phase_contrast_path": str(contrast_path.resolve()), "freeze_path": str(Path(freeze_path).resolve()), "target_provenance_path": str(target_provenance.resolve())})
    if learned and report["secondary_learned_predictor_eligible"]:
        secondary = nested_gene_prediction(frame)
        secondary_path = output / "nested_gene_predictions.tsv"
        write_table(secondary, secondary_path)
        report["input_sha256"][str(secondary_path.resolve())] = file_hash(secondary_path)
        report.update(secondary_learned_predictor_status="nested_orthogroup_validation_computed", secondary_models=sorted(secondary.model.unique().tolist()))
    else:
        report["secondary_learned_predictor_status"] = "ineligible_fewer_than_40_genes_or_20_orthogroups" if learned else "not_requested"
    atomic_json(output / "external_prediction.json", report)
    return report


def write_h3(config: dict, root: str | Path, external_prediction_json_path: str | Path) -> dict:
    """Export only an actually computed and provenance-validated H3 result."""
    from .core import canonical_hash as config_hash, source_identity
    root, path = Path(root), Path(external_prediction_json_path)
    result = json.loads(path.read_text())
    if result.get("status") != "external_gene_prediction_computed":
        raise MolecularError("H3 export requires a completed external gene prediction")
    if result.get("shared_control_covariance_propagated") is not True:
        raise MolecularError("H3 uncertainty must propagate the actual shared-control gene covariance")
    provenance = result.get("provenance", {})
    required = {"config_sha256", "source_identity_sha256", "source_manifest_hash", "freeze_hash", "orthology_hash", "nuisance_descriptor_hash", "target_table_sha256", "target_family_map_sha256", "freeze_path", "target_provenance_path"}
    if not required.issubset(provenance) or not all(provenance[key] for key in required):
        raise MolecularError("H3 external source/freeze/target-family proof is incomplete")
    if provenance["config_sha256"] != config_hash(config):
        raise MolecularError("H3 result belongs to a different study configuration")
    current_identity = source_identity(Path(__file__).resolve().parents[2])["sha256"]
    if provenance["source_identity_sha256"] != current_identity:
        raise MolecularError("H3 result belongs to another immutable source release")
    manifest = json.loads((root / "manifests" / "source_manifest.json").read_text())
    validate_manifest(manifest)
    if provenance["source_manifest_hash"] != manifest["manifest_hash"]:
        raise MolecularError("H3 biological source manifest changed")
    inputs = result.get("input_sha256", {})
    if not inputs or any(not Path(source).is_file() or file_hash(source) != expected for source, expected in inputs.items()):
        raise MolecularError("H3 source inputs are missing or changed")
    freeze, score_path = validate_freeze(provenance["freeze_path"])
    if freeze["freeze_hash"] != provenance["freeze_hash"] or str(score_path.resolve()) not in inputs:
        raise MolecularError("H3 freeze identity lacks matching source proof")
    if any(freeze["provenance"].get(key) != provenance[key] for key in ["source_manifest_hash", "orthology_hash", "nuisance_descriptor_hash"]):
        raise MolecularError("H3 freeze provenance and prediction differ")
    target_proof = json.loads(Path(provenance["target_provenance_path"]).read_text())
    if any(target_proof.get(flag) is not True for flag in ["validated_gene_ids", "shared_control_covariance_preserved", "reagents_clustered_by_gene"]):
        raise MolecularError("H3 external targets lack actual ID/covariance/reagent proof")
    if any(target_proof.get(key) != provenance[key] for key in ["target_table_sha256", "target_family_map_sha256"]):
        raise MolecularError("H3 target-family provenance and prediction differ")
    if target_proof.get("molecular_freeze_hash") != freeze["freeze_hash"]:
        raise MolecularError("H3 targets were not aggregated after verifying this prior freeze")
    if result.get("permutations", 0) < int(config["molecular"].get("gene_permutations", 10_000)):
        raise MolecularError("H3 permutation count is below the fixed primary configuration")
    if result.get("bootstrap_replicates", 0) < int(config["inference"].get("bootstrap_replicates", 10_000)):
        raise MolecularError("H3 bootstrap count is below the fixed primary configuration")
    estimate = float(result["primary_incremental_rank_correlation"])
    ci = [float(value) for value in result["incremental_rank_ci95"]]
    pvalue = float(result["permutation_p"])
    if len(ci) != 2 or not np.isfinite([estimate, *ci, pvalue]).all() or ci[0] > ci[1] or not -1 <= estimate <= 1 or not 0 <= pvalue <= 1:
        raise MolecularError("H3 estimate, uncertainty or permutation P value is invalid")
    minimum = float(config["molecular"].get("incremental_rank_effect_threshold", .2))
    record = {"claim_id": "H3", "hypothesis": "H3", "status": "computed_primary_family_pending", "claim_status": "pending_joint_Holm", "estimate": estimate, "ci95": ci, "pvalue": pvalue, "pvalue_method": "whole-orthogroup label permutation within fixed expression/orthology/precision bins", "independent_unit": "gene/orthogroup with clustered reagents", "evidence_class": "biological_reanalysis", "genes": result["genes"], "orthogroups": result["orthogroups"], "config_sha256": provenance["config_sha256"], "source_identity": provenance["source_identity_sha256"], "source_sha256": provenance["source_identity_sha256"], "source_manifest_hash": provenance["source_manifest_hash"], "data_manifest_sha256": file_hash(root / "manifests" / "source_manifest.json"), "source_result_paths": [str(path.resolve()), *inputs.keys()], "source_result_sha256": file_hash(path), "provenance": provenance, "primary_family_pending": True, "multiplicity_status": "H1/H2/H3 joint Holm adjustment pending", "multiplicity": "Joint Holm adjustment must use actual H1, H2 and H3 results; no stand-alone confirmatory decision", "operational_threshold": {"rank_effect": minimum, "point_threshold_met": estimate >= minimum, "interval_excludes_zero": ci[0] > 0 or ci[1] < 0, "unadjusted_p_below_alpha": pvalue < float(config["inference"].get("alpha", .05))}}
    record["source_result_hashes"] = {str(path.resolve()): file_hash(path), **inputs}
    record.update(interval_method=result["interval_method"], permutation_interpretation=result["permutation_interpretation"], shared_control_covariance_propagated=True)
    atomic_json(root / "results" / "inference" / "H3.json", record)
    panels = [{"kind": "interval", "x": [0], "y": [estimate], "lower": [ci[0]], "upper": [ci[1]], "title": "Frozen molecular score: incremental association", "xlabel": "H3 prespecified contrast", "ylabel": "Residualized rank correlation"}]
    coverage_path = path.parent / "gene_mapping_coverage.json"
    if coverage_path.is_file() and str(coverage_path.resolve()) in inputs:
        coverage = json.loads(coverage_path.read_text())
        panels.insert(0, {"kind": "scatter", "x": ["Source targets", "Mapped, measured"], "y": [coverage["target_genes"], coverage["mapped_measured_genes"]], "title": "Observed mapping coverage", "xlabel": "Gene cohort", "ylabel": "Unique genes"})
    panels = {"status": "actual_molecular_results_available", "evidence_class": "biological_reanalysis", "config_sha256": record["config_sha256"], "source_sha256": record["source_sha256"], "source_result_paths": record["source_result_paths"], "source_result_hashes": record["source_result_hashes"], "panels": panels, "caption": "Actual unique-gene mapping and frozen-score incremental rank association, with an orthogroup/family cluster bootstrap 95% interval. The molecular score is fixed before external outcomes; nuisance ranks are residualized. Primary H1/H2/H3 Holm adjustment remains pending.", "phase_contrast_path": provenance.get("phase_contrast_path")}
    atomic_json(root / "results" / "figure_inputs" / "figure4.json", panels)
    atomic_json(root / "results" / "figure_inputs" / "04.json", panels)
    return record


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    cross = sub.add_parser("ena-crosswalk")
    cross.add_argument("--manifest", required=True)
    cross.add_argument("--output", required=True)
    cross.add_argument("--workers", type=int, default=4)
    plan = sub.add_parser("rna-plan")
    plan.add_argument("--crosswalk", required=True)
    plan.add_argument("--references", required=True)
    plan.add_argument("--root", required=True)
    plan.add_argument("--output", required=True)
    plan.add_argument("--threads", type=int, default=8)
    plan.add_argument("--pilot", type=int)
    plan.add_argument("--strand", default="infer", choices=["infer", "reverse", "forward", "unstranded"])
    plan.add_argument("--preprocessing", default="fastp", choices=["fastp", "published"])
    reference = sub.add_parser("prepare-reference")
    reference.add_argument("--manifest", required=True)
    reference.add_argument("--root", required=True)
    reference.add_argument("--output", required=True)
    pilot_command = sub.add_parser("rna-pilot")
    pilot_command.add_argument("--config", required=True)
    pilot_command.add_argument("--root", required=True)
    pilot_command.add_argument("--index", type=int)
    pilot_command.add_argument("--prepare-only", action="store_true")
    full_rna = sub.add_parser("prepare-rna-full")
    full_rna.add_argument("--config", required=True)
    full_rna.add_argument("--root", required=True)
    task_rna = sub.add_parser("rna-run")
    task_rna.add_argument("--plan", required=True)
    task_rna.add_argument("--index", required=True, type=int)
    task_rna.add_argument("--output", required=True)
    ant_run = sub.add_parser("run-ant")
    ant_run.add_argument("--root", required=True)
    orfs = sub.add_parser("orf-plan")
    orfs.add_argument("--supertranscripts", required=True)
    orfs.add_argument("--transcript-map", required=True)
    orfs.add_argument("--output", required=True)
    orf_map = sub.add_parser("orf-map")
    orf_map.add_argument("--peptides", required=True)
    orf_map.add_argument("--transcript-map", required=True)
    orf_map.add_argument("--output", required=True)
    strand = sub.add_parser("strand-check")
    strand_source = strand.add_mutually_exclusive_group(required=True)
    strand_source.add_argument("--report")
    strand_source.add_argument("--star-counts")
    strand.add_argument("--output", required=True)
    strand.add_argument("--expected", default="reverse")
    qc = sub.add_parser("rna-qc")
    qc.add_argument("--directory", required=True)
    qc.add_argument("--sample", required=True)
    qc.add_argument("--output", required=True)
    adapt = sub.add_parser("adapt-counts")
    adapt.add_argument("--counts", required=True)
    adapt.add_argument("--column-map", required=True)
    adapt.add_argument("--gene-column", default="Geneid")
    adapt.add_argument("--output", required=True)
    mantis = sub.add_parser("prepare-mantis")
    mantis.add_argument("--root", required=True)
    mantis.add_argument("--output")
    mantis_run = sub.add_parser("run-mantis")
    mantis_run.add_argument("--root", required=True)
    orth = sub.add_parser("orthology-plan")
    orth.add_argument("--proteomes", required=True)
    orth.add_argument("--output", required=True)
    orth.add_argument("--threads", type=int, default=16)
    freeze = sub.add_parser("score-freeze")
    freeze.add_argument("--contrasts", required=True)
    freeze.add_argument("--provenance", required=True)
    freeze.add_argument("--output", required=True)
    predict = sub.add_parser("score-predict")
    predict.add_argument("--genes", required=True)
    predict.add_argument("--orthology", required=True)
    predict.add_argument("--descriptors", required=True)
    predict.add_argument("--freeze", required=True)
    predict.add_argument("--target-provenance", required=True)
    predict.add_argument("--config", required=True)
    predict.add_argument("--root", required=True)
    predict.add_argument("--output", required=True)
    predict.add_argument("--learned", action="store_true")
    target_family = sub.add_parser("target-family-map")
    target_family.add_argument("--orthology", required=True)
    target_family.add_argument("--output", required=True)
    runtime_manifest = sub.add_parser("runtime-manifest")
    runtime_manifest.add_argument("--output", required=True)
    install_runtime = sub.add_parser("install-runtime")
    install_runtime.add_argument("--root", required=True)
    install_runtime.add_argument("--destination", required=True)
    install_runtime.add_argument("--skip-qualification", action="store_true")
    install_runtime.add_argument("--perl-binary", default=SITE_PERL)
    install_runtime.add_argument("--orthofinder-python", default=None)
    analyze = sub.add_parser("analyze-primary")
    analyze.add_argument("--config", required=True)
    analyze.add_argument("--root", required=True)
    analyze.add_argument("--external-prediction", required=True)
    refusal = sub.add_parser("refusal-challenge")
    refusal.add_argument("--directory", required=True)
    refusal.add_argument("--samples", required=True)
    refusal.add_argument("--freeze", required=True)
    refusal.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    output = Path(args.output) if getattr(args, "output", None) else None
    if args.command == "ena-crosswalk":
        manifest = json.loads(Path(args.manifest).read_text())
        rows = manifest["sources"]["ant_rna"]["rows"]
        samples = fetch_ant_sample_metadata(rows, args.workers)
        frame, report = ant_crosswalk(rows, samples)
        output.mkdir(parents=True, exist_ok=True)
        write_table(frame, output / "sample_crosswalk.tsv")
        atomic_json(output / "sample_metadata.json", samples)
        atomic_json(output / "crosswalk_audit.json", report)
        atomic_json(output / "source_manifest_crosswalk.json", crosswalk_manifest(manifest, frame, report))
    elif args.command == "rna-plan":
        report = rna_plan(pd.read_csv(args.crosswalk, sep="\t", dtype={"collection_year": str}), json.loads(Path(args.references).read_text()), args.root, output, threads=args.threads, pilot=args.pilot, strandedness=args.strand, preprocessing=args.preprocessing)
    elif args.command == "prepare-reference":
        report = prepare_reference(json.loads(Path(args.manifest).read_text()), args.root, output)
    elif args.command == "rna-pilot":
        report = run_rna_pilot(json.loads(Path(args.config).read_text()), args.root, index=args.index, prepare_only=args.prepare_only)
    elif args.command == "prepare-rna-full":
        report = prepare_full_rna(json.loads(Path(args.config).read_text()), args.root)
    elif args.command == "rna-run":
        report = run_rna_task(args.plan, args.index)
        atomic_json(output, report)
    elif args.command == "run-ant":
        report = run_ant(args.root)
    elif args.command == "orf-plan":
        report = orf_plan(args.supertranscripts, args.transcript_map, output)
    elif args.command == "orf-map":
        frame, report = transdecoder_gene_map(args.peptides, pd.read_csv(args.transcript_map, sep="\t", dtype=str))
        write_table(frame, output)
        atomic_json(output.with_suffix(".coverage.json"), report)
    elif args.command == "strand-check":
        report = star_strandedness(args.star_counts, expected=args.expected) if args.star_counts else parse_strandedness(Path(args.report).read_text(), expected=args.expected)
        atomic_json(output, report)
    elif args.command == "rna-qc":
        report = rna_qc(args.directory, args.sample)
        atomic_json(output, report)
    elif args.command == "adapt-counts":
        report = adapt_mantis_counts(args.counts, args.column_map, output, gene_column=args.gene_column)
    elif args.command == "prepare-mantis":
        report = prepare_mantis(args.root, output)
    elif args.command == "run-mantis":
        report = run_mantis(args.root)
    elif args.command == "orthology-plan":
        report = orthology_plan(json.loads(Path(args.proteomes).read_text()), output, threads=args.threads)
    elif args.command == "score-freeze":
        scores = reversible_scores(pd.read_csv(args.contrasts, sep="\t"))
        provenance = json.loads(Path(args.provenance).read_text())
        provenance.update(phase_contrast_path=str(Path(args.contrasts).resolve()), phase_contrast_sha256=file_hash(args.contrasts))
        report = freeze_scores(scores, provenance, output)
    elif args.command == "runtime-manifest":
        if output.exists():
            raise MolecularError("Runtime manifest already exists; use a prospective amendment")
        report = tool_runtime_manifest()
        atomic_json(output, report)
    elif args.command == "install-runtime":
        report = install_tool_runtime(args.root, args.destination, qualify=not args.skip_qualification, perl_binary=args.perl_binary, orthofinder_python=args.orthofinder_python)
    elif args.command == "target-family-map":
        report = write_target_family_map(pd.read_csv(args.orthology, sep="\t"), output)
    elif args.command == "analyze-primary":
        report = write_h3(json.loads(Path(args.config).read_text()), args.root, args.external_prediction)
    elif args.command == "refusal-challenge":
        freeze, score_path = validate_freeze(args.freeze)
        directory = Path(args.directory)
        samples = pd.read_csv(args.samples, sep="\t")
        discovery = samples[samples.cohort == "discovery"]
        challenge = samples[samples.cohort == "challenge"]
        observations, report = refusal_challenge(pd.read_csv(directory / "normalized_discovery_counts.tsv", sep="\t", index_col=0), discovery, pd.read_csv(directory / "refusal_counts_heldout.tsv", sep="\t", index_col=0), challenge, pd.read_csv(directory / "refusal_training_calibration.tsv", sep="\t"), pd.read_csv(score_path, sep="\t"), freeze)
        output.mkdir(parents=True, exist_ok=True)
        write_table(observations, output / "refusal_projections.tsv")
        atomic_json(output / "refusal_challenge.json", report)
    else:
        report = run_external_prediction(json.loads(Path(args.config).read_text()), args.root, genes=args.genes, orthology=args.orthology, descriptors=args.descriptors, freeze_path=args.freeze, target_provenance=args.target_provenance, output=output, learned=args.learned)
    print(json.dumps({"command": args.command, "status": report.get("status", "completed")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
