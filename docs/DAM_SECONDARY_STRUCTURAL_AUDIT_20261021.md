# Prospective DAM secondary-data structural audit

This source is a candidate secondary activity dataset. It does not supply missing exposure timestamps, XY tracks or animal crosswalks for the original fly study. The audit produces source and recording-structure evidence, with every scientific gate and admission flag false. It computes no treatment effect, sleep bout, movement mean, endpoint or significance test and never executes the deposited R program.

## Source and literal metadata

The [official Zenodo17175344 record](https://zenodo.org/records/17175344) describes DAM5H monitoring of12-day-old mated female flies, at25°C and12:12 light/dark, with minute-resolution counts across three experiments. The deposited [R source](https://zenodo.org/records/17175344/files/code_process_three_datasets.R?download=1) defines monitor/channel groups named `DAV`, `Bloomfield`, `DCV`, `Nora`, `Uninfected` and `Empty`. Its model comment calls `Condition` a virus group and sets `Uninfected` as reference. These are author-coded group labels. The source does not by itself verify individual infection, randomization, genotype, dose or treatment onset. A filename containing12dpe is not an individual exposure timestamp.

The author-code collection windows are inclusive, assign UTC when parsing, and contain4321 expected minute positions each:

| Experiment | Provider filename prefix | Author-code window |
| --- | --- | --- |
| Exp1 |15Apr25 12dpe all 1 min res |2025-04-12 08:00 to2025-04-15 08:00 |
| Exp2 |23Apr25 12dpe part II 1 min res |2025-04-16 08:00 to2025-04-19 08:00 |
| Exp3 |06May25 12dpe part III 1 min res |2025-04-30 08:00 to2025-05-03 08:00 |

The audit parses each exact `expN_channels` literal and experiment call into32 channel entries per source monitor, preserving `Empty`, unmapped channels and overlaps. It joins the final parent-directory and monitor-file literals to the provider filename. Raw monitor fields remain separate from the monitor numbers assigned by R. The author-assigned timezone is not verified hardware timezone, and collection-window start is not infection onset.

## Independent source and structure checks

The command consumes the exact source manifest, its SHA256 and a fresh output directory below personal study maintenance. The caller must provide a bounded native allocation, current resource accounting and storage admission. The pure-stdlib auditor independently verifies all seven provider names, byte sizes, MD5s and SHA256s, the API-record bytes, transfer-receipt hashes and literal R bytes. Each data file is hashed while streamed once. Original partial-transfer references are retained as receipt evidence; they are not independently rehashed by this audit.

The audit records42-column shape, raw data types and monitor/status fields; it checks clocks separately by `(raw monitor, raw tube, data type)`. Multiple types sharing one timestamp are not automatically duplicates. It reports within-stream duplicates and conflicting duplicates, reverse clocks, unparseable dates, off-grid times, missing expected minute positions, and status coverage. For `MT`/`CT` rows with tube-number0 it reports per-channel missing, nonnumeric, nonfinite, negative and noninteger entries and unique valid status-one grid positions. Other stream types remain catalogued and are not converted into activity, trajectories or distance. No record is repaired or imputed.

The [manufacturer format documentation](https://trikinetics.com/Downloads/DAMSystem3%20Software%20Data%20Sheet.pdf) distinguishes total activity, beam/position and other stream types. The audit records this documentation URL with `manual_bytes_bound=false`; a future analysis must independently bind the relevant documentation bytes before relying on unit interpretation. Neither an infrared beam count nor a tube-position stream supplies a2D trajectory or a qualified physical-distance conversion.

Outputs are exclusive `literal-code-mapping.json`, one structural report per data file and a final source-bound receipt. Successful completion means that the structural audit finished and original provider identities matched. Reported data-quality problems are preserved; success does not mean the data pass a scientific analysis gate. A bounded failure preserves completed file reports and a failure receipt, and the command exits nonzero.

## Candidate prospective routes after audit

- **C:** Specify a separate count-activity observation model using actual clock coverage, source experiment/monitor/channel grouping and author-coded conditions. XY/directional emissions and inferred infection-onset alignment cannot be borrowed from the original design. Missingness, duplicate resolution, status handling, duration support, circadian structure, grouped validation and condition contrasts require a prospective specification before effects are inspected.
- **D:** Calibrate and perform recovery for that new count model using the measured recording lengths and missingness. This requires a count-model simulator and numerical qualification; original XY recovery is not evidence for this model.
- **E2:** Treat any later result as source-specific viral activity evidence. It does not automatically connect these flies to the worm/parasite RNA data or establish a common molecular mechanism.

The initial audit leaves these routes proposed, all original H1–H3 metadata gates unresolved, and scientific result, complete study and admission authority false.

## Actual native audit

Native job6690893 completed successfully after the exact37-test auditor suite passed with zero failures, errors or skips. The final six-file receipt is SHA256 `f3bb101092821a8f1377343f897af51dcb1f45f6925c92165388ee85b1c015a7` at personal scratch `maintenance/study-af-dam-structural-audit-v1-20261021/receipt.json`.

Within each author-defined window, Exp1 and Exp2 have4321 valid grid positions per monitor; Exp3 has4317 and4318. The latter preserve four/three missing grid positions and two off-grid rows per selected stream. Selected MT/CT streams have no duplicate/reverse clocks, and their retained valid grid positions have no missing, nonnumeric, nonfinite, negative or noninteger channel values. These checks neither compare condition activity nor identify biological sleep, infection or physical distance. All prospective scientific gates above remain pending.

## Native interface

```text
python audit-af-dam-secondary-v1-20261021.py \
  --manifest /scratch/pwa209/horsehair-worm/maintenance/study-af-dam-secondary-source-v1-20261021/source-manifest.json \
  --manifest-sha256 c632eafc53b777dac5743730f98f66618334fbeec110b132e76b3e7835903410 \
  --output /scratch/pwa209/horsehair-worm/maintenance/FRESH-STRUCTURAL-AUDIT-CASE \
  --max-seconds 5400
```

The illustrative output case must be replaced by a fresh immutable case. No network download, R interpreter or scheduler submission is performed by this command.
