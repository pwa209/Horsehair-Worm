# Prospective recording-relative prediction eligibility

Status: a proposed analysis amendment, not an executed scientific analysis or a
clearance of the original H1, H2 or H3 gates. The original immutable scientific
source, full configuration and earlier technical receipts remain preserved.

The native whole-fold profiler6691343 reached the source data and stopped before
model fitting. Some validation recordings had no observed prediction bins after
the original600-second warm-up. The frozen primary `Fit.score` explicitly raises
for such a recording; its equal-experiment, then equal-animal validation criterion
therefore has no value for that complete fold. Replacing that value with zero,
discarding recordings silently or changing the clock would change the analysis.

The immediate private technical repair retains the same whole grouped fold,
recording clocks, observation masks, training scaler, duration knots, nuisance
rows and global training observed-bin normalizer. It first records a census for
every original source recording, including the original all-missing exclusions
and outer holdouts. It then measures the unchanged full prior and every training
likelihood/backward operation, one global gradient clip and one original Adam
update. It invokes the unchanged original `Fit.score` for every validation
recording on its complete clock. Only its exact known zero-window exception is
represented as an explicit null score; all other errors remain failures. The
original validation criterion remains null when any constituent is undefined.
No eligible-only alternative criterion or additional optimization epochs run in
this technical repair. A deadline or memory bound preserves honest partial
operator receipts rather than claiming a finished fit or resumable mid-operation
optimizer state.

For a separately qualified recording-relative analysis, the following source
coverage rule could be frozen before inspecting numerical effects:

1. Preserve the complete deposited recording roster and grouped split. Never
   shorten a clock, shorten warm-up or fill a missing observation to gain a score.
2. Define an eligible prediction bin using exactly the original effective mask:
   the recorded observed flag, finite required displacement coordinates and
   recording time at least600 seconds after that recording's first bin.
3. Preserve every recording with its full bin count, observed-bin count,
   eligible-bin count, mask/clock hashes and fold role. A recording with zero
   eligible bins has a null predictive score. An experiment containing no
   recordings with defined scores also has a null predictive score.
4. Define the amended criterion explicitly as the mean of recording means over
   defined recordings within each experiment, then the mean over experiments
   with defined recording means. Retain original and eligible recording counts
   and original and eligible experiment counts side by side. Never call these
   denominators the unchanged original complete-fold denominators. If no held-out
   experiment has a defined score, model selection remains unsupported.
5. Use identical coverage rules, clocks and missingness masks for every compared
   model, rate, start and simulation/recovery replicate. Keep the original full
   training-fold likelihood and its observed-bin normalizer unless a separate
   training-estimand amendment is explicitly constructed and qualified.
6. Report this as prediction conditional on the deposited observation coverage.
   Tracking loss may be informative; the rule alone does not establish that
   missingness is ignorable. Report coverage differences without imputing
   infection, exposure timing, treatment allocation or biological identities.

This proposal can support a useful analysis of recording-relative movement when
the available grouped coverage permits scoring, with a separately stated
conditional estimand. It cannot establish the original exposure-relative H1
question. It requires a new methods declaration, source-bound full numerical
tests and native qualification before scientific optimizer selection or final
inference uses the amended criterion. Parameter recovery must replay the same
complete clocks and masks rather than replace them with a cleaner cohort.

The earlier whole-record adjoint canary also needs a separate prospective
selection clarification. Its frozen `c5fcef7a` helper selects the longest
validation recording with any observed bin; this does not guarantee a bin after
warm-up. A new canary version may select the longest *complete* validation clock
with at least one originally eligible prediction bin, resolving ties solely by
source-ID SHA256. It must retain every ineligible recording as a null census row,
preserve full training-fold preprocessing and nuisance dimensions, and report
that it qualifies only the selected first-order operator. If no validation clock
is eligible, the canary remains unsupported. The earlier helper and all its
receipts must remain unchanged; computing a proposed selection in the census
does not execute or qualify that new canary.
