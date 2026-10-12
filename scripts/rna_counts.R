#!/usr/bin/env Rscript
# Count-based source replication / phase inference. Refusal brains do not enter
# ant size-factor, dispersion, gene-filter or score construction.
suppressPackageStartupMessages(library(DESeq2))
suppressPackageStartupMessages(library(jsonlite))

parse_args <- function(x) {
  out <- list()
  if (length(x) %% 2 != 0) stop("Arguments must be --name value pairs")
  for (i in seq(1, length(x), by=2)) {
    if (!startsWith(x[i], "--")) stop("Invalid argument name")
    out[[substring(x[i], 3)]] <- x[i+1]
  }
  for (key in c("mode", "counts", "samples", "output")) {
    if (is.null(out[[key]])) stop(paste("Missing", key))
  }
  out
}

args <- parse_args(commandArgs(trailingOnly=TRUE))
if (!args$mode %in% c("ant", "mantis")) stop("mode must be ant or mantis")
dir.create(args$output, recursive=TRUE, showWarnings=FALSE)
write_tsv <- function(x, name) {
  write.table(x, file.path(args$output, name), sep="\t", quote=FALSE, row.names=FALSE, na="NA")
}

raw <- read.delim(args$counts, check.names=FALSE, stringsAsFactors=FALSE, comment.char="#")
samples <- read.delim(args$samples, check.names=FALSE, stringsAsFactors=FALSE)
if (!all(c("gene_id") %in% names(raw))) stop("Counts require first identifier column gene_id")
if (!all(c("count_column", "biological_id", "technical_library_id", "group", "infection", "phase") %in% names(samples))) stop("Samples lack explicit biological/library/group metadata")
if (anyDuplicated(raw$gene_id) || anyDuplicated(samples$count_column)) stop("Duplicate gene IDs or count-column sample assignments")
count_columns <- setdiff(names(raw), "gene_id")
if (!setequal(count_columns, samples$count_column)) stop("Every count column must map to one biological sample")
samples <- samples[match(count_columns, samples$count_column), , drop=FALSE]
mat <- as.matrix(raw[, count_columns, drop=FALSE])
storage.mode(mat) <- "double"
if (anyNA(mat) || any(!is.finite(mat)) || any(mat < 0) || any(mat != round(mat))) stop("DESeq2 requires nonnegative integer counts; TPM is prohibited")
if (any(colSums(mat) == 0)) stop("Empty RNA library")
rownames(mat) <- raw$gene_id

# Collapse only sequencing runs explicitly belonging to the same prepared
# biological library. Unrelated brains with matching group labels never merge.
ids <- unique(samples$biological_id)
collapsed <- matrix(0, nrow=nrow(mat), ncol=length(ids), dimnames=list(rownames(mat), ids))
bio <- samples[match(ids, samples$biological_id), , drop=FALSE]
stable <- intersect(c("technical_library_id", "group", "infection", "phase", "batch", "collection_year", "organism", "tissue", "cohort"), names(samples))
for (j in seq_along(ids)) {
  ix <- which(samples$biological_id == ids[j])
  for (field in stable) if (length(unique(samples[ix, field])) != 1) stop(paste("Conflicting technical collapse for", ids[j], field))
  collapsed[,j] <- rowSums(mat[,ix,drop=FALSE])
}
bio$count_column <- bio$biological_id
rownames(bio) <- bio$biological_id
write_tsv(data.frame(gene_id=rownames(collapsed), collapsed, check.names=FALSE), "collapsed_counts.tsv")
write_tsv(bio, "biological_samples.tsv")

if (args$mode == "ant") {
  if (!"cohort" %in% names(bio)) stop("Ant samples require discovery/challenge cohort designation")
  if (!"collection_year" %in% names(bio) || any(as.character(bio$collection_year) != "2021")) stop("Ant confirmatory input must contain the published 2021 cohort only")
  expected <- c(paste0(rep(c("infected", "uninfected"), each=4), "_", rep(c("pre", "attached", "newly_detached", "detached"), 2)), "infected_refused_attach", "infected_refused_detach")
  sizes <- table(bio$group)
  if (!setequal(names(sizes), expected) || any(sizes != 6)) stop("Ant design requires ten groups of six biological brains")
  training <- bio$cohort == "discovery"
  if (sum(training) != 48 || sum(bio$cohort == "challenge") != 12) stop("Ant refusal challenge must remain twelve held-out brains")
  minimum_samples <- 6L
} else {
  if ("organism" %in% names(bio) && any(bio$organism != "Tenodera angustipennis")) stop("Analyze mantis host brains separately from worm bodies")
  if ("tissue" %in% names(bio) && any(tolower(bio$tissue) != "brain")) stop("Mantis host tissue must be brain")
  if (any(bio$phase %in% c("after", "recovered"))) stop("No host-after recovery group exists in the deposited mantis study")
  training <- rep(TRUE, nrow(bio))
  minimum_samples <- min(table(bio$group))
}

train_samples <- bio[training, , drop=FALSE]
train_samples$group <- factor(train_samples$group)
design_terms <- "group"
batch_field <- args[["batch-column"]]
ignored_constant_batch <- NULL
if (!is.null(batch_field)) {
  if (!batch_field %in% names(train_samples)) stop("Requested batch covariate missing")
  if (anyNA(train_samples[[batch_field]])) stop("Missing batch assignments")
  if (length(unique(train_samples[[batch_field]])) > 1) {
    train_samples$technical_batch <- factor(train_samples[[batch_field]])
    design_terms <- c("group", "technical_batch")
  } else ignored_constant_batch <- batch_field
}
design <- as.formula(paste("~ 0 +", paste(design_terms, collapse=" + ")))
dm <- model.matrix(design, train_samples)
if (qr(dm)$rank != ncol(dm)) stop("RNA design is rank-deficient: group and batch are confounded; adjusted phase contrast is unidentified")
if (nrow(dm) <= ncol(dm)) stop("RNA design has no residual degrees of freedom")
train_counts <- collapsed[,training,drop=FALSE]
keep <- rowSums(train_counts >= 10) >= minimum_samples
if (sum(keep) < 20) stop("Too few adequately measured genes for DESeq2")
dds <- DESeqDataSetFromMatrix(countData=round(train_counts[keep,,drop=FALSE]), colData=train_samples, design=design)
dds <- DESeq(dds, betaPrior=FALSE, quiet=TRUE)
rn <- resultsNames(dds)
normalized <- counts(dds, normalized=TRUE)
write_tsv(data.frame(gene_id=rownames(normalized), normalized, check.names=FALSE), "normalized_discovery_counts.tsv")
descriptors <- data.frame(gene_id=rownames(dds), average_expression=rowMeans(normalized), detection_rate=rowMeans(counts(dds)>0))

contrast_vector <- function(weights) {
  v <- rep(0, length(rn))
  for (group_name in names(weights)) {
    ix <- which(rn == paste0("group", group_name))
    if (length(ix) != 1) stop(paste("Unmatched DESeq2 group coefficient", group_name, paste(rn, collapse=",")))
    v[ix] <- weights[[group_name]]
  }
  v
}
estimate <- function(weights, name) {
  contrast <- contrast_vector(weights)
  res <- results(dds, contrast=contrast, alpha=0.05, independentFiltering=FALSE)
  shrunk <- lfcShrink(dds, contrast=contrast, res=res, type="normal", quiet=TRUE)
  frame <- data.frame(gene_id=rownames(res), as.data.frame(res), shrink_log2fc=shrunk$log2FoldChange, shrink_se=shrunk$lfcSE)
  write_tsv(frame, paste0(name, ".tsv"))
  frame
}

if (args$mode == "ant") {
  on <- estimate(c(infected_attached=1, infected_pre=-1, uninfected_attached=-1, uninfected_pre=1), "initiation")
  off <- estimate(c(infected_detached=1, infected_attached=-1, uninfected_detached=-1, uninfected_attached=1), "termination")
  intermediate <- estimate(c(infected_newly_detached=1, infected_attached=-1, uninfected_newly_detached=-1, uninfected_attached=1), "newly_detached_diagnostic")
  generic <- estimate(c(infected_pre=.25, infected_attached=.25, infected_newly_detached=.25, infected_detached=.25, uninfected_pre=-.25, uninfected_attached=-.25, uninfected_newly_detached=-.25, uninfected_detached=-.25), "generic_infection")
  descriptors$generic_infection_effect <- generic$log2FoldChange
  contrast_table <- data.frame(gene_id=on$gene_id, on_log2fc=on$log2FoldChange, on_se=on$lfcSE, off_log2fc=off$log2FoldChange, off_se=off$lfcSE)
  write_tsv(contrast_table, "phase_contrasts.tsv")
  # Conjunction tests remain valid with correlated initiation/termination errors.
  z_on <- contrast_table$on_log2fc / contrast_table$on_se
  z_off <- contrast_table$off_log2fc / contrast_table$off_se
  positive <- pmax(pnorm(z_on, lower.tail=FALSE), pnorm(z_off))
  negative <- pmax(pnorm(z_on), pnorm(z_off, lower.tail=FALSE))
  conjunction <- pmin(1, 2*pmin(positive, negative))
  score <- pmin(10, -log10(pmax(conjunction, .Machine$double.xmin)))
  direction <- ifelse(positive <= negative, 1, -1)
  frozen <- data.frame(gene_id=rownames(dds), reversible_p=conjunction, reversible_fdr=p.adjust(conjunction, method="BH"), reversible_score=score, initiation_direction=direction)
  write_tsv(frozen, "reversible_scores_unfrozen.tsv")
  # Do not score refusal outcomes in this construction script. Save training-only
  # calibration parameters for a separate post-freeze challenge command.
  logtrain <- log2(normalized + 1)
  geometric <- exp(rowMeans(log(counts(dds))))
  calibration <- data.frame(gene_id=rownames(dds), training_mean=rowMeans(logtrain), training_sd=apply(logtrain, 1, sd), size_factor_geometric_mean=geometric)
  write_tsv(calibration, "refusal_training_calibration.tsv")
  write_tsv(data.frame(gene_id=rownames(collapsed), collapsed[,!training,drop=FALSE], check.names=FALSE), "refusal_counts_heldout.tsv")
} else {
  required_groups <- c("infected_before", "infected_during")
  if (!all(required_groups %in% levels(train_samples$group))) stop("Mantis comparison requires explicitly mapped infected_before / infected_during groups")
  stage <- estimate(c(infected_during=1, infected_before=-1), "mantis_stage")
  if ("uninfected" %in% levels(train_samples$group)) {
    generic <- estimate(c(infected_before=.5, infected_during=.5, uninfected=-1), "mantis_generic_infection")
    descriptors$generic_infection_effect <- generic$log2FoldChange
  }
}
write_tsv(descriptors, "expression_descriptors.tsv")
saveRDS(dds, file.path(args$output, "deseq2_fit.rds"))
writeLines(capture.output(sessionInfo()), file.path(args$output, "sessionInfo.txt"))
audit <- list(mode=args$mode, source_counts=args$counts, source_samples=args$samples, independent_brains=ncol(collapsed), discovery_brains=sum(training), heldout_brains=sum(!training), retained_genes=sum(keep), filter=list(count_threshold=10, minimum_biological_samples=minimum_samples), design=deparse(design), coefficients=rn, design_rank=qr(dm)$rank, constant_batch_ignored=ignored_constant_batch, status="count_model_fitted_not_external_validation", evidence_class="observational_cross_sectional")
write_json(audit, file.path(args$output, "rna_analysis_audit.json"), pretty=TRUE, auto_unbox=TRUE)
