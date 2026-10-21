# Prospective public-data analyses of activity dynamics

Status: source-only design, dated 21 October 2026. No count, coordinate or gene
effect has been estimated for this design. The comparison families below are
specified prospectively; execution still requires the numerical appendix,
assay checks, recovery tests, full source qualification and resource admission.
Earlier scientific source, failed attempts and unsupported findings remain
preserved. These are new questions with separate protocols and outputs. They do
not clear the original H1–H3, intervention or gene-transfer gates.

## Questions and available evidence

| Analysis | Prospective question | Unit and available support | Present limit |
|---|---|---|---|
| DAM activity counts | Do author-coded conditions improve prediction of minute activity counts in a held-out experiment, and can a count-specific model distinguish an observation gain from a change in latent transitions? | 131 coded nonempty recording channels in three author experiments, with two monitors per experiment | Labels do not verify individual infection, randomization, exposure onset or neural mechanism |
| Ethoscope host-response cohort | After assay and clock qualification, do author-coded treatments predict activity dynamics in a separate experiment? | Figure01 metadata lists five experiment labels and 36 date–machine databases; the acquired database belongs to FigureS09 | Raw coverage of the Figure01 cohort is unverified; the canary alone is not an external-validation cohort |
| VTL processing output | Which files, clocks, collection units and line labels can be linked reproducibly, and what later questions are defensible about the deposited processing output? | One whole ordered-output file has been acquired as a deterministic parser canary; filename-to-line/gene metadata is available | Ordered positions may be carried forward, observed/imputed flags and biological identities are absent, and stock/control edges remain unresolved |

The DAM source is the [Zenodo deposit17175344](https://zenodo.org/records/17175344).
Its channel labels and windows are taken from the deposited
[R documentation](https://zenodo.org/records/17175344/files/code_process_three_datasets.R?download=1),
parsed as literals without executing it. The manufacturer distinguishes CT
(CountsTotal) from MT (MovesTotal) in its
[DAM software data sheet](https://trikinetics.com/Downloads/DAMSystem3%20Software%20Data%20Sheet.pdf).
Neither stream is an XY trajectory or physical distance measurement.

The Ethoscope source is the
[bacterial host-response publication](https://journals.plos.org/plospathogens/article?id=10.1371/journal.ppat.1010826)
and [deposit6567039](https://zenodo.org/records/6567039). This biological setting
does not establish parasite manipulation. VTL documentation is the
[screen publication](https://www.nature.com/articles/s41467-024-48005-w) and
[original processing code](https://github.com/BioCUCKOO/VTL). The design does
not select genes using reported screen effects or the reported 86-gene subset.

## 1. DAM condition-associated count dynamics

### Source roster and experimental units

Every entry below is a recording channel assigned by the author's literal R
mapping. It is not a newly verified animal or infection. Each cell gives the
counts in monitor1 / monitor2; all five nonempty labels occur in each of the
three experiments.

| Author label | Exp1 | Exp2 | Exp3 | Unique channels |
|---|---:|---:|---:|---:|
| DAV | 4 / 4 | 5 / 4 | 4 / 4 | 25 |
| Bloomfield | 4 / 4 | 5 / 5 | 4 / 4 | 26 |
| DCV | 4 / 4 | 5 / 5 | 4 / 4 | 26 |
| Nora | 4 / 4 | 5 / 5 | 4 / 4 | 26 |
| Uninfected | 5 / 5 | 5 / 5 | 4 / 4 | 28 |

The full roster has192 channel slots:131 coded nonempty,11 explicitly Empty and
50 unmapped. Empty and unmapped slots remain in the audit and do not silently
join an analysis group. Four label-versus-Uninfected comparisons share the same
28 reference channels. The24 condition × experiment × monitor comparison strata
contain215 rows if controls are repeated; they still represent131 unique coded
nonempty channels. Repeated minutes, MT/CT streams, monitors and reused controls
do not create additional independent experiments.

The three literal windows begin at08:00 on12 April,16 April and30 April2025 and
end at08:00 three days later. Inclusive windows have4321 minute positions and
4320 minute intervals. The author's UTC parsing convention does not verify the
hardware timezone, light phase or an individual's exposure time. A filename
containing12dpe is retained as a filename label, not converted into an exact
injection or infection clock.

Native structural audit6690893 verified the six deposited files and parsed the
source windows. Its status is evidence about source integrity and format, not
evidence of a treatment effect. The initial audit found all4321 selected minute
positions in Exp1/Exp2, and4317/4318 in Exp3 monitors1/2, with four/three missing
positions and two off-grid rows per selected stream in each Exp3 monitor.
The canonical converter must reproduce those receipts before modelling.

### Observations, clocks and missingness

CT is the prospectively chosen primary stream; MT is a separate sensitivity
analysis. Preserve raw monitor number, tube number, datatype, source channel,
timestamp and file hash. The stream key includes monitor/tube/datatype; a
monitor-total row must not be merged with a different tube or beam stream.
Bind the manufacturer's manual bytes and qualify these definitions before
effects are read. The public inventory records that this manual binding remains
pending.

Preserve the complete declared minute clock. Valid observations require a
qualified monitor status, an on-grid source time, an unambiguous source row and
a finite nonnegative integer count. Missing and invalid observations have no
emission likelihood while the latent clock still propagates. Missing is never
zero. Do not round off-grid rows into the grid, interpolate counts, shorten
clocks, pool CT with MT or transform five zero counts into verified sleep.
Conflicting duplicate clock rows fail conversion. Any rule for byte-identical
duplicates must be frozen explicitly and preserve their original multiplicity.

Retain all mapped channels in the coverage ledger, including channels with no
scoreable observations. Do not remove channels because their activity is low,
their future survival is poor or their fitted result is inconvenient. Before
model fitting, freeze a coverage-only eligibility rule and publish all original,
eligible and excluded denominators with reasons. If a planned label is absent
from a training or test experiment after this rule, the corresponding outer
comparison is unsupported; it is not repaired with another experiment or a
different label. A valid zero count remains an observation.

### New count observation and transition models

This is a new assay-specific analysis. The original XY likelihood, directional
features, movement gain, displacement normalization and duration kernels cannot
be applied to counts by renaming columns. The primary model family is a
two-state negative-binomial hidden Markov model, with state-specific dispersion
and ordered lower/higher expected counts. These are statistical count states;
the analysis does not identify sleep, intent or a neural circuit.

For a valid minute observation, use NB2 with variance
`mu + mu**2 / dispersion`. Its log mean contains a state intercept, a common
predeclared24-hour basis in the author's recording clock, a hierarchical
zero-centred channel intercept and a monitor-within-experiment nuisance term.
The state means are ordered for identifiability. Circadian structure, nuisance
hierarchies, dispersion, initial-state convention, priors, fitting effort and
missingness treatment are identical across four model classes:

| Model | Condition terms |
|---|---|
| Common | None |
| Gain | One condition-specific multiplicative count gain, common to both state means |
| Transition | Condition-specific changes to the two transition logits |
| Combined | Both gain and transition terms |

Use Uninfected as the literal reference label. A free intercept for every
channel must not absorb an unconstrained condition gain; the nuisance hierarchy
and its centring are part of the frozen identifiable model. New-channel and
new-monitor nuisance effects in a held-out experiment are integrated over the
training-learned hierarchy. They are not fitted using held-out outcomes.
Initial-state handling must not introduce an extra condition effect in only
some classes. These HMMs imply geometric dwell times. An explicit-duration
extension would need a separately frozen comparison, recovery and cost plan.

The numerical appendix must fix the24-hour basis dimension, priors and scales,
training-only transformations, initialization/restart seeds, optimizer schedule,
gradient checks, finite-value refusal, convergence criteria and marginal
predictive-score computation before fitting the real counts. Its source and
configuration are separate from the original scientific protocol. Failure to
qualify any of these requirements stops the relevant comparison.

### Held-out prediction, uncertainty and multiplicity

If the coverage gates pass, use three outer folds, each holding out one entire
author experiment. Every minute, channel, monitor and both streams from that
experiment stay outside training and tuning. Any tuning uses only the remaining
experiments with grouped channels/monitors and must acknowledge that just two
author experiments remain. Do not create random minute-level train/test splits.

The primary score is the proper predictive log probability per valid CT minute,
averaged within channel, then equally across channels in each experiment, then
equally across the three experiments. All compared models use exactly the same
scoreable mask and denominators. Publish the three experiment-level score
differences and every denominator. The planned model-comparison family is Gain,
Transition and Combined versus Common, plus Transition versus Gain. CT defines
this primary family; MT cannot replace an unsuccessful CT result.

Only three independent author experiments support this outer comparison. Even
under an otherwise valid exchangeable sign-flip null, three paired signs have
only eight patterns: the smallest one-sided tail probability is1/8, and a
symmetric two-sided tail can be no smaller than1/4. Randomization is unverified,
so these are limits on possible evidence, not authorization for a causal
permutation test. High precision obtained by resampling minutes or channels
does not overcome the three-experiment limit. Model-based conditional intervals
must be labelled with their assumptions and must not be described as
design-based population treatment intervals.

The four label-versus-shared-reference contrasts form a single secondary family.
Use their shared-control covariance in any joint calculation; never analyse the
same control four times as independent evidence. Holm correction is permissible
only if the underlying individual tests have been justified. When that condition
fails, report descriptive contrasts and their model assumptions without
confirmatory significance labels. No unplanned endpoint, datatype or window
may become the primary result because it looks favourable.

### Simulation, recovery and negative controls

Before fitting real effects, implement and qualify a new NB2 count simulator
with the exact source experiment/channel layout, full clocks and observation
masks. A simulator must not borrow an original XY recovery receipt. Freeze a
seeded grid spanning state separation, count scale, overdispersion and dwell
times under null, gain-only, transition-only and combined mechanisms. Include
weakly distinguishable regimes rather than selecting easy examples. Add
missing-at-random and latent-state-dependent dropout challenges, extra
zero-inflation and nongeometric-duration misspecification. The latter cases test
failure detection; they do not establish a correction for unknown real dropout.

The numerical appendix must publish the full grid, repetitions, seed roster,
Monte Carlo precision, practical predictive-improvement threshold and numerical
acceptance bounds before real effects are revealed. Benchmarks on native Narval
must establish its complete cost first. At minimum assess full-objective and
gradient agreement, optimizer failure/convergence rates, gain-versus-transition
confusion, null false selections, predictive calibration and parameter recovery
where the model is identifiable. Keep failed and nonfinite runs. If gain and
transition cannot be distinguished at the source clocks and count regimes, the
real-data question is limited to predictive count association.

Negative controls include simulated null condition effects, byte-identical
relabelled copies as an invariance check, the absence of cross-fold nuisance
fitting and deliberate clock/mask corruption that must be rejected. Label
shuffling is a computational falsification only under explicitly stated
exchangeability assumptions. Empty channels may audit instrumentation; they
are not biological untreated controls.

## 2. Conditional Ethoscope extension

The acquired database is dated22 April2022, machineETHOSCOPE_155. Its metadata
has20 ROIs, genotypeCS, male, experimentF and FigureS09, with ten literal
`001Fnov` labels and ten PBS labels. Do not silently relabel001Fnov as a validated
species, dose or intervention. Native schema audit6699089 confirmed the20-ROI
roster and columns including `t`, `x`, `y` and `is_inferred`; it did not read
coordinate outcomes. Two coordinate fields do not establish free two-dimensional
locomotion in a tube arena. Raw clock values, physical units, expected sampling,
hardware timezone, ROI geometry, inference semantics and exact exposure clocks
remain unqualified. An epoch-like database metadata value is not an individual
injection timestamp. The publication's injection interval cannot supply missing
per-animal times.

The separately identified Figure01 metadata route has370 ROI entries across
36 databases: all labels are male/w1118;165 are Francisella,103 PBS and102
uninfected. Its literal support is:

| Source experiment | Francisella | PBS | uninfected | Databases |
|---|---:|---:|---:|---:|
| C | 44 | 22 | 21 | 8 |
| D | 40 | 20 | 20 | 8 |
| E | 41 | 21 | 21 | 8 |
| K | 20 | 20 | 20 | 6 |
| L | 20 | 20 | 20 | 6 |

Eight entries have other `obs` labels: one no_fly and seven ethoscope_killing;
362 are labelled ok. The roster retains all370 without automatically applying
an outcome-dependent exclusion. These metadata counts differ from an earlier
notebook/paper count and therefore do not establish exact published-cohort
replication. The canary database is not part of this Figure01 roster.

Across the complete metadata file,9842 figure entries correspond to9723 distinct
date–machine–ROI keys. All119 repeated keys have conflicting metadata between
Figure04D and FigureS07. They are neither proven independent animals nor clean
duplicate records. Do not pool those figures or reconcile their labels by
majority vote. Any future cross-figure use requires an explicit identity and
metadata resolution.

Before selecting the36 databases for scientific use, freeze the complete
metadata-based roster and verify each member against public provider checksums,
database identities and schema. Then qualify the exact producer version, time
units and discontinuities, physical units/ROI geometry, identity stability,
observed/inferred semantics and missingness. Retain declared exclusions and
future immobility without quietly adopting the publication's movement-based
death curation. Terminal immobility alone is not independently verified death.

Only after these gates could a separately registered, assay-specific model
compare author-label-associated dynamics in five leave-one-experiment-out folds.
The source experiment, date, machine, ROI and reused controls define grouping;
ROI minutes and databases on one date are not independent interventions. Choose
the observation model from qualified assay definitions, not from a favourable
effect. If only relative recording time is verified, restrict the question to
recording-relative prediction rather than a pre/post-injection causal change.
External consistency could support portability of a statistical model class,
not a common biological mechanism or shared gene effects.

## 3. VTL: processing-output qualification before intervention inference

The screen publication describes5588 genes and6885 RNAi/mutant lines in a male
elavGAL4 screening context. The separately available filename workbook has
23129 filenames,7435 line labels and5605 distinct nonempty gene labels. These
are different source universes, not interchangeable counts of independently
validated interventions. Some workbook gene labels contain multiple targets.
Stock, background, driver and matched-control edges require the official Source
Data5 stock mapping (MOESM8); direct public acquisition attempts have not yet
obtained a valid file. The screen's population mean is not a designated matched
control. Reported top hits and an LLM-derived gene list cannot define the
prospective gene universe or validation holdout.

The deposited ordering program carries a previous coordinate forward when it
cannot match a slot. Ordered output lacks a verified observed/imputed mask and
does not prove that a slot is the same fly across time. The original videos were
not retained, according to the publication. A nominal well may contain multiple
flies; the collection well, line, recording date and camera are the relevant
design units. Ordered slots must not be counted as independent biological
replicates or used as movement ground truth.

The current canary was chosen by a deterministic filename-hash rule among
bounded eligible whole members. It is a parser canary, not a representative
cohort. Immediate admissible questions concern syntax, source frame-index
continuity, nominal video-rate metadata, filename/workbook joins and missing
provenance. The audit exports no coordinate values or movement effects.

A later processing-output study could ask whether a predeclared well-level
feature of the deposited coordinate output is associated with source line
labels, conditional on date/camera coverage and the known ordering process.
Before bulk acquisition, it needs a complete metadata-only replicate ledger,
background/control mapping, independently supported holdout groups and a
frozen feature set with synthetic ordering-error challenges. Such a result would
describe an algorithm's deposited output under its censoring and carry-forward
rules. It would not validate true individual movement, a gene's causal motor
effect or the original E2 transfer claim. If these constraints leave no honest
gene comparison, the result is an unsupported gene stage, not an imputed one.

## Execution and evidence requirements

The new inventory script
[`scripts/prospective_secondary_design.py`](../scripts/prospective_secondary_design.py)
reads only the exact DAM R documentation and Ethoscope metadata bytes. It does
not execute deposited programs or read raw counts, coordinates or ordered
tracks. It records source spans, hashes, unique channel identities, shared
controls, complete label support, cross-figure conflicts and candidate grouped
splits. Every scientific-clearance flag is false. Its tests use synthetic
documentation; the generated inventory is evidence of design support only.

Documentation SHA256 bindings are:

| Source | SHA256 |
|---|---|
| DAM deposited R | `a78a423d650f56a9297526a762b2fdd84871eceb44cfd6fe05e770d6cc06275c` |
| Ethoscope METADATA | `83230109101cb01e57fb5f0d64b48b1965f07d9f45cf9c41db8105357c3d1616` |
| VTL filename workbook | `b53bcc2242a6019f6556d217116c98d021878b6d4ed88c281f752d1eb555518d` |

Native source-evidence locators retained for review include:

- DAM structural receipt:
  `/scratch/pwa209/horsehair-worm/maintenance/study-af-dam-structural-audit-v1-20261021/receipt.json`,
  SHA256 `f3bb101092821a8f1377343f897af51dcb1f45f6925c92165388ee85b1c015a7`.
- Ethoscope schema inventory:
  `/scratch/pwa209/horsehair-worm/maintenance/study-af-ethoscope-sqlite-audit-v2-20261021/schema-and-clock-inventory.json`,
  SHA256 `ce2ba0d16b1ca85380192c8ba2fa3fa680ec21535ffa94df29acae1200a4a021`.
- VTL canary acquisition result:
  `/scratch/pwa209/horsehair-worm/maintenance/study-af-vtl-whole-member-canary-v1-20261021/acquisition-result.json`,
  SHA256 `0c91c8a596402cc3ee7dc7abc4e8fe38d51e3453654fb3224b24e63b6d771808`.

Before execution, freeze a separate protocol ID, complete numerical appendix,
input/mask/split manifests, provenance and assay gates, primary/sensitivity
families, source and test identities, optimizer/recovery seeds, and rejection
rules. Qualify the complete resulting source on Narval, benchmark the complete
declared workload and obtain current storage/accounting admission. Keep original
receipts, all failed/nonfinite cases and explicit unsupported results. The user
has lifted total CPU/GPU caps; concurrency and storage limits still govern
dispatch. No analysis stage may manufacture eligibility by filling metadata,
discarding failed cells, redefining biological replicates or substituting a
negative/unsupported finding with another endpoint. No author contact is used.

Successful DAM simulation and held-out count prediction would complete a new
computational calibration and association study. A separately qualified
Ethoscope extension could test model-class portability. Neither establishes
the original parasite causal questions, and neither supplies the original E2
requirement for independent gene/reagent/control families.
