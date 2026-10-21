# H4 analysis for the float64 evaluation amendment

`horsehair.agent_analysis_v2` is a separate bridge for the complete
`evaluation-only-float64-v1` experiment. Original float32 checkpoints, the
development-frozen protocol and historical numerical failures remain intact.
The bridge does not change the training algorithm, perturbation grid, endpoint
definitions, final seeds, baseline requirements, statistical tests or reference
point. Float64 evaluation can change rounding, calibration and sampled
trajectories; its results are identified by the amended evaluation protocol.

The launcher must use a newly qualified immutable release containing this
bridge. The earlier 4f3903 execution release remains usable for its existing
qualified jobs, but its qualification cannot qualify this new analysis module.
Native scheduling, leases, accounting, storage headroom and hard deadlines remain
the launcher's responsibility. The bridge grants no admission authority.

The entry points are sequential:

1. `freeze_plan(config, root, campaign_plan, case, ...)` verifies the original
   campaign and protocol locators and independent native evidence, then writes
   an exclusive `analysis-plan.json` and binding. The evidence dictionary contains
   exact `{path, sha256}` locators for `qualification`, `precision_plan`,
   `precision_binding`, `precision_diagnostics` and `precision_assembly`.
   Qualification must cover the current complete source and test-file manifest,
   execute the exact complete test directory without selectors, contain at least
   the initial 1,737-test bridge baseline and match the actual hash-bound JUnit
   testcase roster. The old 1,694-test qualification cannot qualify the bridge.
   Precision evidence must
   show the original failed GRU pilot in both precisions, float32 failure
   reproduction, finite float64 results for all 52 declared conditions and a
   complete 1,664-row canary assembly. Positive completed native records must
   still exist in the study accounting ledger.
2. `freeze_inputs(case, expected_plan_sha256=...)` pins all 240 original training
   summaries/checkpoints, amended plans/bindings, assembly summaries and assembled
   trial files. It reads metadata and hashes bytes; it computes no endpoint
   effect. Incomplete policy assemblies cannot enter this manifest.
3. `run(case, inputs_sha256=..., expected_plan_sha256=...)` independently checks
   every checkpoint with the original loader and all 300 original conditions per
   policy, including each part's checksum, restoration, finite endpoints,
   calibration and exact episode roster. The assembled file must equal the
   verified ordered condition parts. Original and promoted weight hashes remain
   distinct. An unavailable, missing, duplicate, altered or corrupt record blocks
   inference; no row is filled, dropped or converted to a negative observation.
   Individual original part files remain required. An archive alone is not a
   substitute until a separately qualified archive-reading adapter is added.

The same entry points are available through `python -m
horsehair.agent_analysis_v2`: `freeze-plan`, `freeze-inputs` and `analyze`.
The latter two commands require an independently supplied analysis-plan SHA;
`analyze` also requires the independently supplied input-manifest SHA.

The complete workload contains 240 policies, 20 independent final seeds per
architecture/task block, 300 conditions and 153,600 trial rows per policy:
36,864,000 rows altogether. Reductions produce 72,000 seed/condition summaries
and 288,000 restoration-control summaries. The unchanged original statistics
evaluate 3,600 grid points, retaining all 2,304 primary points in the Holm family.
Matched comparators remain `random_subspace` and `output_gain`. Paired episodes
are nested within independently trained seeds, and architecture contrasts retain
seed pairing. The fixed reference point is not selected from the evaluation.

Outputs are confined to a fresh `results/amended-h4/<case>/analysis/` namespace.
They include `H4-amended.json`, complete response curves, seed and restoration
tables, paired architecture contrasts and a separate figure input. Legacy
`results/inference/H4.json`, original trial paths and original protocols are never
rewritten. A downstream report must explicitly select this amended result and
cite both protocol identities; this module does not silently switch consumers.

Valid negative results are reported as `not_supported`. Baseline-unqualified
results remain visible and receive `pending` claim status, with no accepted H4
headline. Unmatched calibration and unqualified operating points retain the
original conservative p-value of one in the full correction family. A supported
computational result still leaves biological transfer pending. Completing the
computational grid cannot establish an insect mechanism or clear a biological
metadata gate.
