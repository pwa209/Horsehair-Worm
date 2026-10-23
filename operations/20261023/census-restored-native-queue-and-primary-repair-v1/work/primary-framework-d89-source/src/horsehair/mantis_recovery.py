"""Explicit deposited reference-gene mapping around the frozen Mantis ORF kernel.

Every original reference sequence is retained. Count-matrix coverage is an
observation, never a filter or a source of invented gene identifiers.
"""
from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from horsehair.core import canonical_hash, native_job_id, read_json, source_identity
from horsehair.budget import resource_lease, require_storage_headroom
from horsehair.rna_recovery import (RNARecoveryError, _binding as _rna_binding, _unchanged as _rna_unchanged,
    _plain_file as _rna_plain_file, _exclusive_json, _native_record as _rna_native_record, _primary_environment)

PRIMARY_RELEASE = Path("/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf").resolve()
PRIMARY_SOURCE_SHA256 = "0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630"
FULL_CONFIG_SHA256 = "446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35"
AUDIT_SHA256 = "565ee3c681f54dba44cdb433c09212fc2f1c379961adfc5fba67f13e1833cf64"
AUDIT_FRAMEWORK_SHA256 = "eb7672ae84c232be94e2692daaac2b72dc23656112deea971d96937802f4f0ed"
PREVIOUS_JOB_IDS = {"mantis-orfs": "6625680", "mantis-orthology": "6625684"}
ORF_RECIPE = {"minimum_peptide_aa": 100, "single_best_per_transcript": True,
    "representative_selection": "longest peptide per explicit source gene; lexicographic protein ID tie-break",
    "source_fasta_scope": "all original reference supertranscripts; no count-based filtering"}
KERNEL_FUNCTIONS = ("orf_plan", "transdecoder_gene_map", "select_representative_proteins")
REFERENCE_GENES = 195386
MEASURED_GENES = 28499


def require(condition, message):
    if not condition:
        raise ValueError(message)


def _guard_call(function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except RNARecoveryError as error:
        # The controller's scientific-validation contract handles ValueError
        # as pending evidence. A changed input must not kill its native loop.
        raise ValueError(str(error)) from error


def _binding(path):
    return _guard_call(_rna_binding, path)


def _unchanged(value):
    return _guard_call(_rna_unchanged, value)


def _plain_file(path):
    return _guard_call(_rna_plain_file, path)


def _native_record(root, job, *, completed=False):
    return _guard_call(_rna_native_record, root, job, completed=completed)


def _context():
    framework = Path(__file__).resolve().parents[2]
    current, primary = read_json(framework / "release.json"), read_json(PRIMARY_RELEASE / "release.json")
    config = read_json(PRIMARY_RELEASE / "configs/study.json")
    root = Path(config["paths"]["root"]).resolve()
    require(source_identity(framework) == current["source_identity"] and
            source_identity(PRIMARY_RELEASE) == primary["source_identity"] and
            primary["source_identity"]["sha256"] == PRIMARY_SOURCE_SHA256 and
            read_json(framework / "configs/study.json") == config and
            current["config_sha256"] == primary["config_sha256"] == canonical_hash(config) == FULL_CONFIG_SHA256,
            "Mantis adapter framework, primary scientific source, or full configuration changed")
    return root, framework, current, primary, config


def _scalar_native(cpus=None):
    job = native_job_id()
    require(job is not None and re.fullmatch(r"\d+", job) is not None and
            (cpus is None or os.environ.get("SLURM_CPUS_PER_TASK") == str(cpus)),
            "Mantis repair requires its exact native scalar allocation")
    return job


def _prior_native(root):
    failed = _native_record(root, PREVIOUS_JOB_IDS["mantis-orfs"])
    require(failed["state"] == "FAILED" and failed["exit_code"] == "1:0" and failed["cpus"] == 8
            and failed["elapsed_seconds"] == 11, "Original ORF failure is not the reviewed allocated outcome")
    cancelled = read_json(root / "state/resource-budget.json").get("native_jobs", {}).get(PREVIOUS_JOB_IDS["mantis-orthology"], {})
    require(cancelled.get("basis") == "sacct final allocated wall time" and
            cancelled.get("job_id") == PREVIOUS_JOB_IDS["mantis-orthology"] and
            cancelled.get("state") == "CANCELLED" and cancelled.get("exit_code") == "0:0" and
            cancelled.get("cpus") == cancelled.get("elapsed_seconds") == 0,
            "Original orthology is not the reviewed never-allocated cancellation")
    return {"mantis-orfs": failed, "mantis-orthology": cancelled}


def _function_hashes(primary):
    path = Path(primary) / "src/horsehair/molecular.py"
    text = _plain_file(path).read_text(encoding="utf8")
    functions = {node.name: node for node in ast.parse(text).body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    require(all(name in functions for name in KERNEL_FUNCTIONS), "Frozen scientific kernel functions are missing")
    return {name: hashlib.sha256((ast.get_source_segment(text, functions[name]).strip() + "\n").encode()).hexdigest()
            for name in KERNEL_FUNCTIONS}


def _table(path, required):
    with _plain_file(Path(path)).open(encoding="utf8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        require(reader.fieldnames is not None and set(required) <= set(reader.fieldnames), "Native audit table schema changed")
        return list(reader)


def _audit(audit_path, audit_sha256, root, *, relocation=None):
    binding = _binding(Path(audit_path))
    require(binding["sha256"] == audit_sha256 == AUDIT_SHA256, "Native identifier audit bytes changed")
    audit = read_json(Path(audit_path))
    require(audit.get("schema_version") == 1 and audit.get("status") == "complete" and
            audit.get("slurm_job_id") == "6626122" and audit.get("framework_source_sha256") == AUDIT_FRAMEWORK_SHA256 and
            audit.get("primary_source_sha256") == PRIMARY_SOURCE_SHA256 and audit.get("config_sha256") == FULL_CONFIG_SHA256 and
            audit.get("scientific_recomputations") == 0 and audit.get("input_files_overwritten") is False and
            audit.get("sequence_filtering_performed") is False and audit.get("identifier_renaming_performed") is False and
            audit.get("zero_imputation_performed") is False and audit.get("mapping_created") is False,
            "Identifier audit is not the unchanged native full-reference observation")
    _native_record(root, "6626122", completed=True)
    _unchanged(audit["source_manifest"])
    for name, value in audit["input_bindings"].items():
        if name == "derived_fasta" and relocation is not None:
            require(value == relocation["original"], "Preserved derived FASTA has another original binding")
            _unchanged(relocation["preserved"])
            require(all(relocation["preserved"][key] == value[key] for key in ("bytes", "sha256")),
                    "Preserved derived FASTA bytes changed")
        else:
            _unchanged(value)
    for value in audit["tables"].values():
        _unchanged(value)
    tables = audit["tables"]
    sequences = _table(tables["source-sequence-identities.tsv"]["path"], {"transcript_id", "bases", "sequence_sha256"})
    source_ids = [row["transcript_id"] for row in sequences]
    require(len(source_ids) == len(set(source_ids)) == REFERENCE_GENES and
            all(row["transcript_id"] and int(row["bases"]) > 0 and re.fullmatch(r"[0-9a-f]{64}", row["sequence_sha256"]) for row in sequences),
            "Full reference FASTA identity/sequence inventory changed")
    annotations = _table(tables["gff-identifier-observations.tsv"]["path"], {"seqid", "explicit_gene_ids", "explicit_transcript_ids"})
    genes, original_transcripts = {}, {}
    for row in annotations:
        key = row["seqid"]
        values = json.loads(row["explicit_gene_ids"])
        transcripts = json.loads(row["explicit_transcript_ids"])
        require(key not in genes and isinstance(values, list) and len(values) == 1 and values[0] == key and
                isinstance(transcripts, list) and len(transcripts) == 1 and isinstance(transcripts[0], str) and bool(transcripts[0]),
                "Every FASTA seqid requires one explicit same-named deposited gene; transcript annotations are retained verbatim")
        genes[key], original_transcripts[key] = values[0], transcripts[0]
    require(set(genes) == set(source_ids), "GFF and full FASTA identifier universes differ")
    counted_rows = _table(tables["counted-identifiers.tsv"]["path"], {"source", "row", "gene_id"})
    counted = {name: [row["gene_id"] for row in counted_rows if row["source"] == name] for name in ("deposited", "derived")}
    require(all(row["source"] in counted for row in counted_rows) and counted["deposited"] == counted["derived"] and
            len(counted["deposited"]) == len(set(counted["deposited"])) == MEASURED_GENES and
            set(counted["deposited"]) <= set(source_ids), "Measured count identifiers changed or lack exact reference genes")
    observed = audit["observed"]
    require(observed.get("derived_fasta_exact_decompressed_bytes") is True and observed.get("derived_fasta_exact_sequence_records") is True
            and observed.get("raw_derived_count_gene_sets_equal") is True and observed.get("fasta_not_counted_count") == REFERENCE_GENES - MEASURED_GENES
            and observed.get("counted_not_fasta_count") == 0, "Native sequence/count coverage observations changed")
    return audit, sequences, genes, original_transcripts, set(counted["deposited"])


def _write_map(path, sequences, genes, original_transcripts, counted):
    with Path(path).open("x", encoding="utf8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(["transcript_id", "gene_id", "original_assembly_transcript_id", "measured_in_count_matrix", "sequence_bases", "sequence_sha256"])
        for row in sequences:
            key = row["transcript_id"]
            writer.writerow([key, genes[key], original_transcripts[key], int(genes[key] in counted), row["bases"], row["sequence_sha256"]])
        handle.flush(); os.fsync(handle.fileno())


def prepare_recovery(audit_path, audit_sha256, case_directory):
    job = _scalar_native()
    root, framework, current, primary, config = _context()
    case = Path(case_directory).resolve()
    require(case.is_relative_to(root / "maintenance") and not case.exists() and not case.is_symlink(), "Mantis repair case must be new personal maintenance storage")
    audit, sequences, genes, original_transcripts, counted = _audit(audit_path, audit_sha256, root)
    previous = _prior_native(root)
    output = root / "data/molecular/mantis_orfs"
    state = root / "state/molecular-mantis-orfs.json"
    require(output.is_dir() and not output.is_symlink() and not state.exists() and not state.is_symlink(),
            "Reviewed failed ORF output is absent or an existing scientific receipt requires separate review")
    orthology = root / "data/molecular/orthology_mantis"
    require(not orthology.exists() and not orthology.is_symlink(), "Unexpected prior orthology output requires preservation review")
    protected = [_binding(root / "data/molecular/freeze" / name) for name in ("freeze.json", "molecular_scores.tsv")]
    before = output.stat(); identity = {"device": before.st_dev, "inode": before.st_ino}
    case.mkdir(parents=True, exist_ok=False)
    _exclusive_json(case / "capture-intent.json", {"status": "preserving", "slurm_job_id": job,
        "previous_native_records": previous, "original_output": str(output), "original_output_identity": identity,
        "audit": _binding(Path(audit_path)), "maintenance_source_identity": current["source_identity"]})
    saved = case / "preserved/mantis_orfs"; saved.parent.mkdir()
    output.rename(saved)
    after = saved.stat()
    require({"device": after.st_dev, "inode": after.st_ino} == identity and not output.exists(), "Failed output preservation lost its original inode")
    relocation = {"original": audit["input_bindings"]["derived_fasta"], "preserved": _binding(saved / "supertranscripts.fasta")}
    _audit(audit_path, audit_sha256, root, relocation=relocation)
    mapping = case / "full_reference_transcript_gene_map.tsv"
    _write_map(mapping, sequences, genes, original_transcripts, counted)
    manifest = {"schema_version": 1, "status": "captured", "kind": "Mantis full-reference gene-map input adapter",
        "root": str(root), "primary_release": str(PRIMARY_RELEASE), "primary_source_identity": primary["source_identity"],
        "primary_release_id": primary["release_id"],
        "full_config_sha256": canonical_hash(config), "maintenance_release": str(framework),
        "maintenance_source_identity": current["source_identity"], "capture_slurm_job_id": job, "case_directory": str(case),
        "previous_job_ids": PREVIOUS_JOB_IDS, "previous_native_records": previous, "orf_recipe": ORF_RECIPE,
        "audit": _binding(Path(audit_path)), "derived_fasta_relocation": relocation, "full_reference_map": _binding(mapping),
        "reference_genes": len(genes), "measured_genes": len(counted), "unmeasured_reference_genes": len(genes) - len(counted),
        "primary_kernel_function_sha256": _function_hashes(PRIMARY_RELEASE), "protected_primary_freeze": protected,
        "output_directory": str(output), "preserved_output_directory": str(saved), "preserved_output_identity": identity,
        "scientific_parameters_changed": False, "source_sequences_filtered": False, "created_unix": time.time()}
    _exclusive_json(case / "manifest.json", manifest)
    preparation = {"schema_version": 1, "status": "prepared", "slurm_job_id": job,
        "manifest_sha256": _binding(case / "manifest.json")["sha256"], "primary_source_sha256": PRIMARY_SOURCE_SHA256,
        "full_config_sha256": canonical_hash(config), "maintenance_source_identity": current["source_identity"],
        "preserved_output_directory": str(saved), "preserved_output_identity": identity,
        "moved_raw_inputs": [], "primary_freeze_modified": False, "mapping": _binding(mapping)}
    _exclusive_json(case / "preparation.json", preparation)
    return {"manifest": manifest, "preparation": preparation}


def _load(manifest_path, manifest_sha256, preparation_path, preparation_sha256):
    root, framework, current, primary, config = _context()
    path, prep_path = Path(manifest_path).resolve(), Path(preparation_path).resolve()
    require(_binding(path)["sha256"] == manifest_sha256 and _binding(prep_path)["sha256"] == preparation_sha256, "Mantis repair manifest/preparation bytes changed")
    manifest, preparation = read_json(path), read_json(prep_path)
    case = Path(manifest["case_directory"])
    require(manifest.get("schema_version") == 1 and manifest.get("status") == "captured" and
            manifest.get("kind") == "Mantis full-reference gene-map input adapter" and case.is_relative_to(root / "maintenance")
            and path == case / "manifest.json" and prep_path == case / "preparation.json" and manifest.get("root") == str(root)
            and manifest.get("primary_release") == str(PRIMARY_RELEASE) and manifest.get("primary_source_identity") == primary["source_identity"]
            and manifest.get("primary_release_id") == primary["release_id"]
            and manifest.get("full_config_sha256") == canonical_hash(config) and manifest.get("maintenance_release") == str(framework)
            and manifest.get("maintenance_source_identity") == current["source_identity"] and manifest.get("previous_job_ids") == PREVIOUS_JOB_IDS
            and manifest.get("previous_native_records") == _prior_native(root) and manifest.get("orf_recipe") == ORF_RECIPE
            and manifest.get("primary_kernel_function_sha256") == _function_hashes(PRIMARY_RELEASE), "Mantis repair frozen provenance/recipe changed")
    require(preparation.get("schema_version") == 1 and preparation.get("status") == "prepared" and
            preparation.get("manifest_sha256") == manifest_sha256 and preparation.get("slurm_job_id") == manifest["capture_slurm_job_id"]
            and preparation.get("primary_source_sha256") == PRIMARY_SOURCE_SHA256 and preparation.get("full_config_sha256") == canonical_hash(config)
            and preparation.get("maintenance_source_identity") == current["source_identity"] and preparation.get("moved_raw_inputs") == []
            and preparation.get("primary_freeze_modified") is False and preparation.get("mapping") == manifest["full_reference_map"],
            "Mantis repair preservation proof changed")
    saved = Path(manifest["preserved_output_directory"])
    require(saved == case / "preserved/mantis_orfs" and saved.is_dir() and not saved.is_symlink(), "Preserved failed output is missing")
    stat = saved.stat()
    require({"device": stat.st_dev, "inode": stat.st_ino} == manifest["preserved_output_identity"] == preparation.get("preserved_output_identity")
            and preparation.get("preserved_output_directory") == str(saved), "Preserved failed output inode changed")
    _, sequences, genes, original_transcripts, counted = _audit(manifest["audit"]["path"], manifest["audit"]["sha256"], root,
                                                               relocation=manifest["derived_fasta_relocation"])
    _unchanged(manifest["full_reference_map"])
    mapped = _table(manifest["full_reference_map"]["path"], {"transcript_id", "gene_id", "original_assembly_transcript_id", "measured_in_count_matrix", "sequence_bases", "sequence_sha256"})
    expected = [{"transcript_id": row["transcript_id"], "gene_id": genes[row["transcript_id"]],
        "original_assembly_transcript_id": original_transcripts[row["transcript_id"]], "measured_in_count_matrix": str(int(genes[row["transcript_id"]] in counted)),
        "sequence_bases": row["bases"], "sequence_sha256": row["sequence_sha256"]} for row in sequences]
    require(mapped == expected and manifest.get("reference_genes") == len(genes) and manifest.get("measured_genes") == len(counted)
            and manifest.get("unmeasured_reference_genes") == len(genes) - len(counted), "Full-reference map was changed, filtered, or relabelled")
    for value in manifest["protected_primary_freeze"]:
        _unchanged(value)
    require(manifest["output_directory"] == str(root / "data/molecular/mantis_orfs"), "Mantis numerical output namespace changed")
    native = _native_record(root, str(manifest["capture_slurm_job_id"]), completed=True)
    return {"root": root, "primary_release": PRIMARY_RELEASE, "primary_source_identity": primary["source_identity"],
        "full_config_sha256": canonical_hash(config), "maintenance_source_identity": current["source_identity"],
        "manifest": manifest, "preparation": preparation, "capture_native_record": native, "preparation_native_record": native}


def validate_registration_manifest(manifest_path, manifest_sha256, preparation_path, preparation_sha256):
    result = _load(manifest_path, manifest_sha256, preparation_path, preparation_sha256)
    require(not Path(result["manifest"]["output_directory"]).exists() and
            not (Path(result["root"]) / "state/molecular-mantis-orfs.json").exists() and
            not (Path(result["manifest"]["case_directory"]) / "run-intent.json").exists(),
            "Mantis repair already started or produced output; never replay/adopt ambiguously")
    return result


PRIMARY_KERNEL_BOOTSTRAP = r'''
import hashlib,inspect,json,os,subprocess,sys
from pathlib import Path
request=json.loads(sys.argv[1]); primary=Path(request['primary']).resolve()
sys.path.insert(0,str(primary/'src'))
import horsehair.molecular as molecular
import horsehair.molecular_pipeline as pipeline
from horsehair.core import source_identity,canonical_hash,read_json,native_job_id
def require(ok,message):
    if not ok:raise ValueError(message)
require(Path(molecular.__file__).resolve()==primary/'src/horsehair/molecular.py' and Path(pipeline.__file__).resolve()==primary/'src/horsehair/molecular_pipeline.py','Scientific kernel imported from another release')
require(source_identity(primary)==request['primary_identity'] and canonical_hash(read_json(primary/'configs/study.json'))==request['config_sha256'],'Frozen scientific kernel changed')
functions={name:hashlib.sha256((inspect.getsource(getattr(molecular,name)).strip()+'\n').encode()).hexdigest() for name in request['function_sha256']}
require(functions==request['function_sha256'],'Scientific function source changed')
config=read_json(primary/'configs/study.json'); output=Path(request['output'])
fasta=pipeline._decompress(Path(request['source_fasta']),output/'supertranscripts.fasta')
require(molecular.file_hash(fasta)==request['decompressed_fasta_sha256'],'Full reference sequence bytes changed')
mapping=molecular.pd.read_csv(request['map'],sep='\t',dtype=str)
require(len(mapping)==request['reference_genes'] and not mapping.transcript_id.duplicated().any(),'Full source map was filtered or duplicated')
plan=molecular.orf_plan(fasta,request['map'],output/'transdecoder')
require(plan['minimum_peptide_aa']==100 and plan['single_best_per_transcript'] is True and plan['source_transcripts']==request['reference_genes'],'Original ORF recipe changed')
runtime=pipeline.runtime_directory(config,Path(request['root'])); molecular.validate_tool_runtime(runtime)
environment=os.environ.copy();environment['PATH']=str(runtime/'bin')+os.pathsep+environment.get('PATH','')
subprocess.run(['bash',plan['script']],env=environment,check=True)
peptides=output/'transdecoder/mantis_supertranscripts.fasta.transdecoder.pep'
protein_map,raw_coverage=molecular.transdecoder_gene_map(peptides,mapping)
representatives=molecular.select_representative_proteins(peptides,protein_map,output/'mantis.fa')
molecular.write_table(representatives,output/'protein_gene_map.tsv')
measured=set(mapping.loc[mapping.measured_in_count_matrix=='1','gene_id'])
with_orf=set(protein_map.gene_id)
result={'status':'primary_scientific_kernel_completed','slurm_job_id':native_job_id(),'primary_scientific_kernel_source_sha256':request['primary_identity']['sha256'],
 'full_config_sha256':request['config_sha256'],'primary_kernel_function_sha256':functions,'orf_recipe':request['orf_recipe'],'plan':plan,'primary_kernel_coverage':raw_coverage,
 'coverage':{'reference_genes':mapping.gene_id.nunique(),'measured_count_genes':len(measured),'unmeasured_reference_genes':mapping.gene_id.nunique()-len(measured),
  'reference_genes_with_orf':len(with_orf),'measured_genes_with_orf':len(with_orf&measured),'unmeasured_reference_genes_with_orf':len(with_orf-measured),
  'primary_source_counted_genes_field_semantics':'The frozen kernel field source_counted_genes is the full supplied reference-gene map denominator, including unmeasured reference genes'},
 'all_reference_sequences_retained':True,'count_absence_cause':'Not established; no zero counts or expression threshold is inferred'}
require(raw_coverage['source_counted_genes']==request['reference_genes'],'Primary reference denominator changed')
require(source_identity(primary)==request['primary_identity'],'Scientific source changed during numerical execution')
with Path(request['result']).open('x',encoding='utf8') as handle:
    json.dump(result,handle,sort_keys=True,indent=2);handle.write('\n');handle.flush();os.fsync(handle.fileno())
'''


def execute_recovery_task(manifest_path, manifest_sha256, preparation_path, preparation_sha256):
    current = _scalar_native(8)
    context = validate_registration_manifest(manifest_path, manifest_sha256, preparation_path, preparation_sha256)
    manifest, root = context["manifest"], Path(context["root"])
    require(current not in {*PREVIOUS_JOB_IDS.values(), str(manifest["capture_slurm_job_id"])},
            "Preservation and prior failed allocations cannot become the new ORF execution")
    config = read_json(PRIMARY_RELEASE / "configs/study.json")
    case, output = Path(manifest["case_directory"]), Path(manifest["output_directory"])
    kernel_result = case / ("primary-kernel-result-" + current + ".json")
    request = {"primary": str(PRIMARY_RELEASE), "primary_identity": manifest["primary_source_identity"],
        "config_sha256": FULL_CONFIG_SHA256, "function_sha256": manifest["primary_kernel_function_sha256"],
        "orf_recipe": ORF_RECIPE,
        "root": str(root), "output": str(output), "source_fasta": read_json(Path(manifest["audit"]["path"]))["input_bindings"]["fasta"]["path"],
        "decompressed_fasta_sha256": manifest["derived_fasta_relocation"]["preserved"]["sha256"],
        "map": manifest["full_reference_map"]["path"], "reference_genes": manifest["reference_genes"], "result": str(kernel_result)}
    command = [sys.executable, "-s", "-c", PRIMARY_KERNEL_BOOTSTRAP, json.dumps(request, sort_keys=True)]
    environment = _primary_environment({"primary_release": str(PRIMARY_RELEASE), "maintenance_release": manifest["maintenance_release"], "root": str(root)})
    started = time.time()
    _exclusive_json(case / "run-intent.json", {"status": "execution_intent", "slurm_job_id": current,
        "manifest_sha256": manifest_sha256, "preparation_sha256": preparation_sha256, "command": command,
        "source_sha256": manifest["maintenance_source_identity"]["sha256"], "primary_scientific_kernel_source_sha256": PRIMARY_SOURCE_SHA256,
        "full_config_sha256": FULL_CONFIG_SHA256, "orf_recipe": ORF_RECIPE, "started_unix": started})
    with resource_lease(root, config, cpus=8, gpus=0, max_hours=24, job_id=current):
        require_storage_headroom(root, required_files=10000, required_bytes=100_000_000_000,
            reserve_files=max(0, config["resources"]["scratch_budget_files"] - 10000), reserve_bytes=config["resources"]["scratch_reserve_bytes"])
        output.mkdir(parents=True, exist_ok=False)
        result = subprocess.run(command, cwd=str(PRIMARY_RELEASE), env=environment, check=False)
        if result.returncode:
            _exclusive_json(case / "run-failure.json", {"status": "primary_kernel_failed", "slurm_job_id": current,
                "returncode": result.returncode, "outputs_preserved": True, "automatic_replay_permitted": False})
            raise RuntimeError("Frozen primary ORF kernel failed; preserve outputs and native log")
        kernel = read_json(_plain_file(kernel_result))
        require(kernel.get("status") == "primary_scientific_kernel_completed" and kernel.get("slurm_job_id") == current and
                kernel.get("primary_scientific_kernel_source_sha256") == PRIMARY_SOURCE_SHA256 and kernel.get("full_config_sha256") == FULL_CONFIG_SHA256
                and kernel.get("primary_kernel_function_sha256") == manifest["primary_kernel_function_sha256"]
                and kernel.get("orf_recipe") == ORF_RECIPE
                and kernel.get("all_reference_sequences_retained") is True,
                "Scientific kernel result lacks exact source/recipe/native provenance")
        _load(manifest_path, manifest_sha256, preparation_path, preparation_sha256)
        outputs = {name: _binding(output / name) for name in ("mantis.fa", "protein_gene_map.tsv", "supertranscripts.fasta", "transdecoder/orf_plan.json", "transdecoder/transdecoder.sh", "transdecoder/mantis_supertranscripts.fasta.transdecoder.pep")}
        receipt = {"schema_version": 1, "status": "native_mantis_orfs_computed", "stage": "mantis-orfs",
            "source_sha256": manifest["maintenance_source_identity"]["sha256"], "primary_scientific_kernel_source_sha256": PRIMARY_SOURCE_SHA256,
            "config_sha256": FULL_CONFIG_SHA256, "full_config_sha256": FULL_CONFIG_SHA256, "slurm_job_id": current,
            "native_execution": True, "allocated_cpus": 8, "repair_manifest_sha256": manifest_sha256,
            "preparation_sha256": preparation_sha256, "audit": manifest["audit"], "orf_recipe": ORF_RECIPE,
            "primary_kernel_function_sha256": manifest["primary_kernel_function_sha256"], "primary_kernel_result": _binding(kernel_result),
            "coverage": kernel["coverage"], "primary_kernel_coverage": kernel["primary_kernel_coverage"], "outputs": outputs,
            "proteome_sha256": outputs["mantis.fa"]["sha256"], "all_reference_sequences_retained": True,
            "scientific_stage_executed": True, "primary_ant_fly_freeze_modified": False,
            "started_unix": started, "finished_unix": time.time(), "scope": "Explicit full-reference input adapter; frozen primary numerical functions; independent exploratory mantis corroboration"}
        _exclusive_json(case / "receipt.json", receipt)
        _exclusive_json(root / "state/molecular-mantis-orfs.json", receipt)
        return receipt


def validate_completed_phase(guard, config, root, primary_identity):
    """Validate dual ORF provenance and exact primary orthology input linkage."""
    root = Path(root).resolve()
    context = _load(guard["manifest_path"], guard["manifest_sha256"], guard["preparation_path"], guard["preparation_sha256"])
    manifest = context["manifest"]
    expected_primary = primary_identity.get("sha256") if isinstance(primary_identity, dict) else primary_identity
    require(context["root"] == root and canonical_hash(config) == FULL_CONFIG_SHA256 and expected_primary == PRIMARY_SOURCE_SHA256,
            "Completed mantis phase has another primary/configuration namespace")
    rid = manifest["primary_release_id"]
    canonical_plan = read_json(_plain_file(root / "plans" / (rid + "-molecular-corroboration") / "plan.json"))
    canonical_ledger = read_json(_plain_file(root / "state" / (rid + "-molecular-corroboration-submission.json")))
    amendments = [value["mantis_reference_recovery"] for value in canonical_plan.get("dag_amendments", []) if "mantis_reference_recovery" in value]
    require(canonical_plan.get("config_sha256") == FULL_CONFIG_SHA256 and canonical_plan.get("release_identity") == manifest["primary_source_identity"]
            and canonical_ledger.get("plan_sha256") == canonical_hash(canonical_plan)
            and canonical_ledger.get("source_sha256") == PRIMARY_SOURCE_SHA256 and canonical_ledger.get("config_sha256") == FULL_CONFIG_SHA256
            and amendments and amendments[-1] == guard and canonical_ledger.get("dag_amendments") == canonical_plan.get("dag_amendments")
            and set(canonical_ledger.get("tasks", {})) == {"mantis-orfs", "mantis-orthology"},
            "Completed mantis evidence is not bound to the current canonical guarded phase")
    state = root / "state/molecular-mantis-orfs.json"
    receipt_path = Path(manifest["case_directory"]) / "receipt.json"
    receipt = read_json(_plain_file(receipt_path))
    require(read_json(_plain_file(state)) == receipt and receipt.get("status") == "native_mantis_orfs_computed"
            and receipt.get("source_sha256") == manifest["maintenance_source_identity"]["sha256"]
            and receipt.get("primary_scientific_kernel_source_sha256") == PRIMARY_SOURCE_SHA256
            and receipt.get("config_sha256") == receipt.get("full_config_sha256") == FULL_CONFIG_SHA256
            and receipt.get("repair_manifest_sha256") == guard["manifest_sha256"] and receipt.get("preparation_sha256") == guard["preparation_sha256"]
            and receipt.get("audit") == manifest["audit"] and receipt.get("orf_recipe") == ORF_RECIPE
            and receipt.get("primary_kernel_function_sha256") == manifest["primary_kernel_function_sha256"]
            and receipt.get("native_execution") is True and receipt.get("allocated_cpus") == 8
            and receipt.get("all_reference_sequences_retained") is True and receipt.get("primary_ant_fly_freeze_modified") is False,
            "Completed ORFs do not prove the explicit adapter and unchanged primary kernel")
    require(str(receipt.get("slurm_job_id")) == str(canonical_ledger["tasks"]["mantis-orfs"].get("job_id")),
            "ORF scientific receipt belongs to another canonical native allocation")
    native = _native_record(root, str(receipt.get("slurm_job_id", "")), completed=True)
    require(native["cpus"] == 8, "Completed ORFs lack the original eight-CPU native allocation")
    output = root / "data/molecular/mantis_orfs"
    names = {"mantis.fa", "protein_gene_map.tsv", "supertranscripts.fasta", "transdecoder/orf_plan.json", "transdecoder/transdecoder.sh", "transdecoder/mantis_supertranscripts.fasta.transdecoder.pep"}
    require(set(receipt.get("outputs", {})) == names and all(receipt["outputs"][name].get("path") == str(output / name) for name in names),
            "Completed ORF output bindings refer to another scientific namespace")
    for value in [receipt["primary_kernel_result"], *receipt["outputs"].values()]:
        _unchanged(value)
    kernel = read_json(_plain_file(Path(receipt["primary_kernel_result"]["path"])))
    kernel_plan = read_json(_plain_file(output / "transdecoder/orf_plan.json"))
    require(kernel.get("status") == "primary_scientific_kernel_completed" and kernel.get("slurm_job_id") == receipt["slurm_job_id"]
            and kernel.get("primary_scientific_kernel_source_sha256") == PRIMARY_SOURCE_SHA256 and kernel.get("full_config_sha256") == FULL_CONFIG_SHA256
            and kernel.get("primary_kernel_function_sha256") == manifest["primary_kernel_function_sha256"] and kernel.get("orf_recipe") == ORF_RECIPE
            and kernel.get("all_reference_sequences_retained") is True and kernel.get("plan") == kernel_plan
            and kernel_plan.get("minimum_peptide_aa") == 100 and kernel_plan.get("single_best_per_transcript") is True
            and kernel_plan.get("source_transcripts") == manifest["reference_genes"]
            and kernel_plan.get("source_sha256") == receipt["outputs"]["supertranscripts.fasta"]["sha256"]
            and kernel_plan.get("transcript_map_sha256") == manifest["full_reference_map"]["sha256"]
            and kernel.get("coverage") == receipt.get("coverage") and kernel.get("primary_kernel_coverage") == receipt.get("primary_kernel_coverage")
            and kernel.get("primary_kernel_coverage", {}).get("source_counted_genes") == manifest["reference_genes"],
            "Bound scientific kernel result does not independently prove the complete original recipe and reference denominator")
    orthology = read_json(_plain_file(root / "state/molecular-mantis-orthology.json"))
    require(orthology.get("status") == "native_three_species_corroboration_computed" and orthology.get("source_sha256") == PRIMARY_SOURCE_SHA256
            and orthology.get("config_sha256") == FULL_CONFIG_SHA256
            and str(orthology.get("slurm_job_id")) == str(canonical_ledger["tasks"]["mantis-orthology"].get("job_id")),
            "Three-species orthology did not execute from the unchanged primary source and current canonical allocation")
    orthology_native = _native_record(root, str(orthology.get("slurm_job_id", "")), completed=True)
    require(orthology_native["cpus"] == 8, "Three-species orthology lacks its original eight-CPU successful allocation")
    plan = read_json(_plain_file(root / "data/molecular/orthology_mantis/orthology_plan.json"))
    mantis = plan.get("proteomes", {}).get("mantis", {})
    require(orthology.get("plan") == plan and mantis.get("path") == str(root / "data/molecular/mantis_orfs/mantis.fa")
            and mantis.get("sha256") == receipt["outputs"]["mantis.fa"]["sha256"] == receipt.get("proteome_sha256")
            and plan.get("plan_hash") == canonical_hash(plan["proteomes"]), "Primary orthology did not consume the exact repaired mantis proteome")
    return {"status": "validated", "orf_native_record": native, "orthology_native_record": orthology_native,
            "adapter_source_sha256": receipt["source_sha256"], "primary_scientific_kernel_source_sha256": PRIMARY_SOURCE_SHA256,
            "proteome_sha256": receipt["proteome_sha256"], "coverage": receipt["coverage"]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    prepare = commands.add_parser("prepare")
    for name in ("audit", "audit-sha256", "case"):
        prepare.add_argument("--" + name, required=True)
    run = commands.add_parser("run")
    for name in ("manifest", "manifest-sha256", "preparation", "preparation-sha256"):
        run.add_argument("--" + name, required=True)
    arguments = parser.parse_args(argv)
    if arguments.action == "prepare":
        root, _, _, _, config = _context()
        with resource_lease(root, config, cpus=int(os.environ.get("SLURM_CPUS_PER_TASK", "1")), gpus=0, max_hours=1, job_id=_scalar_native()):
            result = prepare_recovery(arguments.audit, arguments.audit_sha256, arguments.case)
        status = result["preparation"]["status"]
    else:
        result = execute_recovery_task(arguments.manifest, arguments.manifest_sha256, arguments.preparation, arguments.preparation_sha256)
        status = result["status"]
    print("MANTIS_REFERENCE_RECOVERY " + json.dumps({"status": status, "native_job_id": native_job_id()}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
