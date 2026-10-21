# Native census renewal with unchanged conservative freshness

`run-fresh-storage-census-v1-20261021.py` performs one native exact-size census
using the unchanged qualified4f `horsehair.budget.storage_snapshot` function.
Its local contract tests use two explicitly synthetic inventory entries; no
heavy or recursive local scan is performed.

Frozen wrapper SHA256:
`084651bd0accb1127f1e41ce94bb763bbe308755819642d365c9eff9f78d02ce`.
Frozen tests SHA256:
`808d0521d586d6523bbcc5f184b96e406c5c681a3f052e9af56924917f18f2d4`.
All35 focused tests passed on the exact final source bytes.

The complete staged qualification roster is **three colocated files**:

1. `run-fresh-storage-census-v1-20261021.py` at the wrapper hash above.
2. `test_run_fresh_storage_census_v1_20261021.py` at the test hash above.
3. Reference-only `run-ethoscope-sqlite-audit-v3-20261021.py`, SHA256
   `81f4dcc425c9e7b242ba96223c20ecd3b3711360fc389827e81eaf13df8af274`.

The third file is parsed by one test to compare the original qualification
function ASTs. It is not imported, executed or used by the census runtime.
Native job6699504 correctly stopped during tests when this reference-only
sibling was absent:34 tests passed and one failed before the census wrapper
ran. Its failure is preserved; the next stage must include all three files.

The native allocation must be exactly one owned running4CPU/16G/4h/0GPU task
with matching registered4f plan, ledger and script. The caller imports the
qualified4f framework under its exact f444 resource policy. The wrapper requires
the original positive1694 public qualification and positive84 private registry
qualification6696233, installs guard358 before campaign imports, and uses the
unchanged42fd registry reader. It does not depend on the pending v3 registry
qualification. The common registration mutex is held at most for acquisition
plus the unchanged CPU/GPU/accounting baseline admission operation. The census
itself runs after that mutex is released. The baseline does not check census
age or modify storage freshness.

Native CLI:

```text
python run-fresh-storage-census-v1-20261021.py \
  --native-script /scratch/pwa209/horsehair-worm/plans/ACTUAL-REGISTERED-PLAN/ACTUAL-SCRIPT.sbatch \
  --registry-qualification /scratch/pwa209/horsehair-worm/maintenance/study-af-registry-repair-support-v1-20261021/private-qualification.json \
  --registry-qualification-sha256 ACTUAL-POSITIVE-84-CASE-QUALIFICATION-SHA256 \
  --registry-qualification-job-id 6696233
```

Fresh output is strictly:
`/scratch/pwa209/horsehair-worm/maintenance/study-af-fresh-storage-census-v1-20261021`.
An existing destination is refused. No automatic replay or cleanup is performed.
The4h native end is independently verified from the scheduler and environment.
A POSIX hard alarm preserves180 seconds for finalization. Fresh native personal
quota/headroom checks run separately before and after the sweep.

The qualified function first captures native personal quota, then sweeps the
directory. Its final dictionary currently overwrites the newly assigned sweep-
end `measured_unix` and `captured_utc` with the earlier quota-capture values.
These clocks are therefore a conservative beginning-of-sweep freshness basis.
This wrapper does not patch that function, rewrite either timestamp, relabel it
as an end time or extend admission freshness from its observed finish time.

The wrapper records separate wall/UTC/monotonic invocation start and finish
times, verifies that the original quota-capture clocks lie inside those bounds,
and states that a sweep is not an atomic point measurement. It requires the
returned census to match the published budget bytes; exact regular-length
detail hashes, EOF observations, counts and byte totals must all agree. Native
quota source bytes remain hash-bound. It preserves immutable copies of the
original budget and entry-size bytes, then rechecks the live pair to refuse a
mixed concurrent publication. Those immutable copies retain the original
conservative clocks. No measurement timestamp is synthesized.

The wrapper leaves all scientific, admission-authority and scientific-gate
flags false. A source/policy change, incomplete or concurrent evidence pair,
failed quota, scan error or deadline produces no completed wrapper receipt.
The original qualified function may already have atomically published a valid
measurement before a later wrapper check fails; that actual evidence is left
intact and must be interpreted from its own bytes. Root owns native execution
and the subsequent fresh-storage admission checks.
