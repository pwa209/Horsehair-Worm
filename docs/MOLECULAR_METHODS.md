# Molecular analysis and executable interfaces

The molecular component tests stage-associated observational convergence. Ant,
mantis and fly experiments contain different subjects. No individual-level
mediation, causal molecular pathway, consciousness variable or biological
recovery claim is manufactured from their combination.

## Source-grounded RNA processing

The [ant source paper](https://pmc.ncbi.nlm.nih.gov/articles/PMC12530281/) reports
60 brains from ten groups of six, NEBNext Ultra II Directional library preparation,
2×100 bp NovaSeq reads, FastQC 0.11.9, Trimmomatic 0.39 with `CROP:97 HEADCROP:13`,
STAR 2.7.9a, featureCounts 2.0.3 and DESeq2 1.26.0. The source's effective pooled
colony sampling is not relabeled as independently replicated colonies.

The primary executable crop mode is `published`: raw/trimmed FastQC, the recorded
Trimmomatic crop, STAR alignment to the matching versioned genome/annotation,
and featureCounts paired-fragment gene counts. Currently available cluster
STAR/featureCounts/DESeq2 versions are recorded in each output; different versions
are explicit departures. A `fastp` sensitivity mode retains the same 13-base
front trim and 84-base length cap, with additional paired-adapter, quality and
NovaSeq poly-G processing. Those extra operations are not claimed to be the
source's exact preprocessing. The [fastp author's documentation](https://github.com/OpenGene/fastp)
defines the trim order and defaults used here.

The publication's GCA_003651465.1 GenBank folder currently lacks its original
gene annotation. NCBI's paired GCF_003651465.1 genome and Annotation Release 100
GTF/proteome provide an executable updated route. FASTA and GTF always come from
the same paired release; GenBank contig names are never silently combined with
RefSeq annotation. The original annotation comparison remains unresolved until
that exact source annotation can be recovered. The host/reference species
mismatch (F. aserva versus F. exsecta) remains a limitation.

Library strand is inferred from STAR GeneCounts columns: column 3 aligns read1
with RNA, and column 4 aligns read2 with RNA ([STAR manual](https://github.com/alexdobin/STAR/blob/master/extras/doc-latex/STARmanual.tex)).
The source directional protocol is expected to yield reverse orientation. At
least half of unstranded assigned counts must be informative, and the expected
orientation must explain at least 80% of directional counts. An incompatible or
ambiguous result stops before strand-specific featureCounts; `-s 2` is not
silently selected without the check. A validated RSeQC report can also be parsed.

QC records unique mapping, assigned-fragment fraction, FastQC files, source tool
versions and, for fastp, duplication/adapter summaries. Mapping below 50% or
assignment below 25% triggers technical review rather than automatic biological
exclusion. rRNA fraction is computed only with versioned rRNA intervals; absent
intervals are recorded as unavailable. Duplication rate is a diagnostic, not a
claim of measured library complexity. Competitive host/parasite cross-mapping
requires appropriate references and is not inferred from good host mapping.

The two RNA pilot brains are selected before expression analysis by the lowest
and highest deposited summed sequencing read counts among the 48 discovery
brains. With missing source read counts, stable accession selection is recorded
explicitly. Runtime and expanded storage are measured; the two pilot libraries
cannot establish phase effects or replace the complete cohort.

## Count analysis

`scripts/rna_counts.R` accepts integer gene counts and an explicit sample table.
It collapses only sequencing runs from the same prepared biological library,
checks all stable metadata, and rejects TPM, empty libraries and duplicated IDs.
The [DESeq2 documentation](https://www.bioconductor.org/packages/release/bioc/vignettes/DESeq2/inst/doc/DESeq2.html)
defines technical collapse and estimable designs.

Ant analysis fits a group-means DESeq2 model to the **48 discovery brains only**.
Its gene filter requires at least ten counts in at least six discovery brains.
The twelve refusal brains do not influence filtering, size factors, dispersions,
contrast selection or score construction. A technical batch is included only
when requested and estimable; a rank-deficient group/batch design fails.
Constant requested batch fields are recorded and omitted because they add no
independent information.

The declared log-expression contrasts are

```
C_on  = (I_attached - I_pre) - (U_attached - U_pre)
C_off = (I_detached - I_attached) - (U_detached - U_attached)
```

Newly detached brains remain a separate diagnostic. Generic infection is the
equal-weight mean of the four infected-versus-matched-uninfected contrasts.
DESeq2 writes unshrunk fold changes, standard errors, P values, FDR and normal
prior shrinkage outputs. Shared attached samples correlate contrast errors; the
reversible score uses two directional conjunction tests. For each direction it
takes the maximum of its two one-sided P values, then doubles the minimum across
the two directions. This conservative construction does not assume independence.
The continuous score is `min(10, -log10(p))`; adequately measured genes are retained
for rank prediction regardless of whether they individually pass FDR.

The mantis adapter joins **exact count-column titles** to GEO SOFT sample titles.
Live validation found 15 brains: four infected-before, five infected-during and
six uninfected, across 2017–2019. It keeps nineteen worm-body records separate.
The primary host contrast is during minus before with collection-year adjustment
when the full design is estimable. No host-after or recovery group is created.
Filtering uses the smallest valid biological group size. This small cohort does
not support an invented dense coexpression network.

## Orthology and prediction

Mantis SuperTranscript-to-counted-gene IDs must be explicitly mapped. The ORF
plan runs TransDecoder LongOrfs (minimum 100 aa) then Predict with one best ORF per
source transcript. Its `.pN` protein suffix is parsed according to the caller's
format, preserving the exact transcript ID. Within an explicitly annotated gene,
the longest protein is selected with a lexical tie-break. Missing and ambiguous
ORFs are counted. [TransDecoder documentation](https://github.com/TransDecoder/transdecoder.github.io/blob/master/index.asciidoc)
supports the procedure.

Versioned checksummed ant, mantis and fly proteomes run through
`orthofinder -f proteomes -t THREADS -a ANALYSIS_THREADS -S diamond`. The adapter
accepts the current root hierarchical orthogroup `N0.tsv` and explicitly labeled
legacy Orthogroups tables. [OrthoFinder documentation](https://github.com/davidemms/OrthoFinder)
describes root HOGs and species/gene-tree inference. Primary transfer uses strict
one-ant/one-fly-gene mappings, with measured/proteome denominators and unmapped
target lists. Missing mantis ORFs do not exclude an otherwise valid ant–fly
primary pair. Three-species one-to-one mappings are separately labeled for mantis
corroboration. Broader orthogroup aggregation is a declared sensitivity analysis.
An outcome-independent fly target-family table retains all source HOG members,
including related genes, for uncertainty and permutation clustering. A fly
reference and neuronal/circadian annotation must still be version-frozen from
actual public source IDs before a prediction can execute.

The executable runtime uses [OrthoFinder 2.5.5](https://github.com/davidemms/OrthoFinder/releases/tag/2.5.5)
at commit `c85ab1df311fb4fe8d11c613382ad8914de8d896` and
[TransDecoder 5.7.1](https://github.com/TransDecoder/TransDecoder/tree/5791fde6a37903f803cc4f51f0882eca200ce437).
Official HTTPS source archives were SHA-256 verified on 12 October 2026:

| Utility | Exact archive bytes | SHA-256 |
|---|---:|---|
| OrthoFinder source distribution | 15,588,113 | `43d034a66a13adba8872a0d4a76e32c25305a7fae638754adb61c37a3f957bd9` |
| TransDecoder pinned commit archive | 15,752,765 | `3f9bf74dd25e7573ab6cb072e399ab124c4d7e7929a8467c411e553a8a5a9964` |

These hashes were computed from official archive bytes; the old release metadata
does not publish a digest. The manifest retains exact source URLs, versions and
commits. The OrthoFinder distribution includes its required `diamond`, `mcl`
and `fastme` binaries. The prepared Python environment supplies NumPy/SciPy;
TransDecoder needs Perl `DB_File`, `URI::Escape` and Rscript. Installation checks
dependencies, runs utility version/help commands, then executes the official
OrthoFinder four-species example and TransDecoder transcript example. Missing
dependencies or a failed example stop qualification. No runtime installer or
instruction from a biological dataset README is executed.

Score freezing refuses fly outcome columns or a provenance flag indicating that
target outcomes were inspected. It requires source, contrast, orthology and
nuisance-descriptor hashes plus the actual phase-contrast path and SHA-256. A
changed frozen score or freeze metadata produces an error. The primary
external target is the magnitude of an uncertainty-aware, unique fly gene effect,
not a claimed signed prediction from expression to knockdown. Reagent-level
effects must be aggregated upstream with shared-control covariance preserved.
The predictor reads the actual cross-gene covariance matrix and explicit gene
order exported by that aggregation. It checks symmetry, positive semidefiniteness
and agreement between covariance diagonals and gene standard errors. Each
orthogroup bootstrap replicate receives one joint Gaussian estimation-error draw
over the full gene vector; repeated resampled genes retain the same draw. This
propagates correlated screen uncertainty into the interval. The primary label
permutation P value remains conditional on the observed effect estimates and
whole-family exchangeability within the fixed bins; it is not presented as
integrating that estimation uncertainty into the permutation test.

The true baseline contains average expression, detection rate, protein length,
orthology confidence, generic infection effect and a fixed neuronal/circadian
annotation. The primary statistic is molecular-score/absolute-effect rank
correlation after residualizing these baseline ranks. Weighted regression is a
sensitivity analysis. Ten thousand permutations exchange whole, equal-size
orthogroup blocks within fixed broad expression, orthology-confidence and
effect-precision bins. No reagent or related gene is individually permuted. Strata
without exchangeable blocks stay fixed; fewer than ten movable genes stops the
test rather than relaxing bins after seeing a target result.

Only at least forty unique genes across twenty orthogroups permit the secondary
learned predictors. Ridge (11 penalties from 1e-4 to 1e4), elastic net (mixing
0.1/0.5/0.9) and shallow boosting (depth 1–3, at most 200 trees) tune inside grouped
inner folds and predict outer orthogroup folds. Every model has a true
baseline-only fit. Optional fixed protein embeddings receive train-only scaling
and at most sixteen train-only PCA components; no foundation model is trained.
Performance is reported whether or not molecular augmentation wins.

The natural refusal challenge applies discovery-only gene filters, geometric
means, centering/scaling and frozen direction weights to the twelve withheld
brains. Brain bootstraps quantify the refusal contrasts against the fixed
attached/detached discovery references. This is a cross-sectional challenge;
different animals are not presented as recovered longitudinal trajectories.

External target export must prove source gene IDs, preserved shared-control
covariance, reagent clustering, the unique-gene table hash and the independently
frozen target-family map. H3 export rechecks every actual source file, the full
freeze identity, current code release/configuration, biological manifest,
permutation count and bootstrap count. It writes a rank-effect estimate, 95%
interval and permutation P value, leaving H1/H2/H3 joint Holm adjustment pending.
Figure 4 receives actual mapping counts and association uncertainty only when
those computations exist.

## Scheduler-facing entry points

```bash
# Shared validated reference and two-library plan/index; numerical work in Slurm.
python -m horsehair.molecular rna-pilot --config configs/study.json \
  --root "$STUDY" --prepare-only
# Two independent pilot jobs, index 0 or 1.
python -m horsehair.molecular rna-pilot --config configs/study.json \
  --root "$STUDY" --index 0
# After pilot admission, a fixed complete biological RNA cohort.
python -m horsehair.molecular prepare-rna-full --config configs/study.json --root "$STUDY"
python -m horsehair.molecular rna-run --plan "$STUDY/data/rna_full_plan/rna_plan.json" \
  --index 0 --output "$STUDY/state/rna-task-0.json"
python -m horsehair.molecular run-ant --root "$STUDY"
python -m horsehair.molecular run-mantis --root "$STUDY"
# Small software archive transfer runs directly on Narval's transfer endpoint.
python -m horsehair.molecular runtime-manifest \
  --output "$STUDY/manifests/tool_runtime_manifest.json"
python -m horsehair.acquire download \
  --manifest "$STUDY/manifests/tool_runtime_manifest.json" --root "$STUDY" \
  --max-files 2 --max-bytes 32000000 --bandwidth-mib 64 --max-seconds 1800
# Under Slurm with the prepared Python, Perl and R modules loaded.
python -m horsehair.molecular install-runtime --root "$STUDY" \
  --destination "$STUDY/data/tool_runtime"
export PATH="$STUDY/data/tool_runtime/bin:$PATH"
python -m horsehair.molecular target-family-map \
  --orthology "$STUDY/data/molecular/orthology/gene_orthology.tsv" \
  --output "$STUDY/data/molecular/orthology/fly_target_families.tsv"
python -m horsehair.molecular score-freeze --contrasts "$CONTRASTS" \
  --provenance "$FREEZE_PROVENANCE" --output "$FROZEN"
python -m horsehair.molecular score-predict --config configs/study.json \
  --root "$STUDY" --genes "$STUDY/results/interventions/gene_effects.tsv" \
  --target-provenance "$STUDY/results/interventions/target_provenance.json" \
  --orthology "$ORTHOLOGY" --descriptors "$DESCRIPTORS" \
  --freeze "$FROZEN/freeze.json" --output "$STUDY/results/molecular/prediction"
python -m horsehair.molecular analyze-primary --config configs/study.json \
  --root "$STUDY" \
  --external-prediction "$STUDY/results/molecular/prediction/external_prediction.json"
```

The Python root API is `run_rna_pilot(config, root, index=None,
prepare_only=False)`. `prepare_only=True` generates the reference/index and plan,
without processing brains; `index=0/1` executes the corresponding pilot. Full RNA
jobs, count-model fitting, ORFs, orthology and target prediction are separate
stages. A successful stage is not a completed mechanistic discovery.

## Finite native source orchestration

`horsehair.molecular_pipeline.run(config, root, stage)` provides `prepare`,
`orthology`, `freeze`, `predict`, `mantis-orfs` and `mantis-orthology`. The primary
reference preparation parses the declared FlyBase primary ID/symbol header and
exact FBpp/FBgn relationships. It maps RefSeq protein IDs to the same GTF gene IDs
used for ant counts. Unresolved schemas, namespace mismatches and absent source
IDs stop the stage. Longest annotated gene isoforms feed a **two-species ant/fly**
OrthoFinder run. A separate actual DIAMOND alignment supplies descriptive peptide
identity, rather than invented continuous orthology confidence. Frozen baseline
expression/detection/generic-infection descriptors come from the ant mapped
orthologue's discovery-only count analysis. Protein length and fixed-root GO
annotations come from the pinned fly reference. The neuronal/circadian indicator
uses GO:0050877 and GO:0007623 plus `is_a`/`part_of` descendants, excludes NOT GAF
records and records the embedded ontology version.

```bash
# Initial explicit bounded benchmark:8CPU/64GB/8h reserved by the orchestrator.
python -m horsehair.molecular_pipeline --config configs/study.json --root "$STUDY" \
  --stage benchmark-primary
# After complete ant counts and before screen-gene outcome aggregation:
python -m horsehair.molecular_pipeline --config configs/study.json --root "$STUDY" \
  --stage freeze
# After the independent screen exporter verifies that prior freeze:
python -m horsehair.molecular_pipeline --config configs/study.json --root "$STUDY" \
  --stage predict
```

`benchmark_primary(config, root)` runs actual full native reference preparation,
two-species OrthoFinder and DIAMOND without requiring ant expression counts. Its
receipt binds the code/configuration/Slurm job and records elapsed time, native
protein counts, working bytes/files and source hashes. Completing this source
benchmark supplies real reusable orthology, not a confirmatory H3 result. A later
freeze refuses existing screen gene-effect/provenance exports and records the
exact prior gene catalog and family map. Those independent inputs must precede
gene aggregation. Mantis ORFs and a separate three-species orthology run never
modify the frozen primary namespace. Biological agent mechanism directions are
provided by the validated training-only behavioural model; gene association
alone cannot manufacture their signed mechanism coefficients.

## Measured prospective resource admission

`horsehair.admission.assess_admission(config, root, phase=None,
safety_factor=1.5)` writes independent `admission-rna-full`,
`admission-agents-development`, `admission-agents-final`, `admission-behaviour`
and `admission-molecular` receipts. The1.5 factor is an explicit prospective
margin applied to actual measurements, not an observed runtime. Missing evidence
produces `pending` with reasons and no invented numeric forecast. Receipts bind
the immutable code/configuration and every benchmark/accounting/quota input hash.

Native accounting is `state/native-accounting.tsv`, generated with pipe-delimited
`sacct` fields `JobIDRaw,State,ElapsedRaw,AllocCPUS,MaxRSS,AllocTRES`. Allocation
rows provide elapsed time/resources; step rows provide peak RSS. Only completed
jobs with positive measured timing/RSS can support admission. The actual
`state/resource-budget.json` supplies spent and outstanding reserved hours.
`state/storage-budget.json` must contain status `measured`, the study root,
`captured_utc` within24hours, actual `study_bytes`, `study_files`,
`personal_available_bytes`, `personal_available_files`, `native_source_paths`
and matching `native_source_hashes`. Missing or changed native quota evidence
does not admit storage.

RNA forecasting uses the larger measured walltime/storage per source read from
the fixed low/high-depth pilot brains, sums all60 deposited biological depths
and counts the complete60-job plan conservatively. It derives memory, per-job
walltime and concurrency from actual RSS and measured allocation. Agent forecasts
include180 development learning-rate fits,240 configured final policies and240
CPU evaluations. They require architecture/task timing coverage and a real CPU
evaluation benchmark; a GPU timing cannot supply a CPU estimate. Full behaviour
and downstream molecular receipts accept explicit tasks with source-bound
`measured_work_units`, `planned_work_units`, `maximum_job_work_units`,
`elapsed_seconds`, `concurrency` and `benchmark_uses_full_primary_numerics=true`.
Full-tail numerical coverage and all predeclared recovery/fit work must be
represented. Admitted receipts expose dynamic `allocation_parameters`; exceeding
the reserved study caps or concurrency limits rejects a phase.
