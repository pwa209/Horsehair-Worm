# Census file-reserve amendment

This is the existing finite census provider with the current user's explicit 200,000-total-free-file reserve. Both before/after quota calls use the same shared immutable policy descriptor and module; 2,000 prospective files plus five receipt entries give a 197,995-file reserve. Byte demand/reserve, the separate 250,000-study-file ceiling, original full exact-length census primitives and original six-hour freshness clocks remain unchanged.

The complete 64-slot serial schedule and four-hour earliest cadence remain. This is replacement source for the existing census component, with no new persistent supervisor. Original provider source/receipts and original queued job IDs are preserved separately. Register and qualify these exact bytes before releasing new native provider slots. Passing generic tests is not a completed census or admission authority.

Remote support: `/scratch/pwa209/horsehair-worm/maintenance/storage-reserve-200k-v2-20261106/census`. Actual cases: `census/cases/native-<job>`. Qualification output: `census/qualification/qualification.json`. Qualification plan phase: `storage-reserve-200k-v2-census-qualification-20261106`; provider phase: `storage-reserve-200k-v2-census-20261106`.

Run `seal-source.py` only after the shared policy source and descriptor are final. `build-native-plans.py --mode qualification --source-manifest-sha256 <sha>` writes a new finite 4-CPU/one-hour qualification plan without submission. After actual successful native qualification, mode `provider` requires its exact report path/SHA/job ID and writes the complete 8-CPU/six-hour rolling schedule without submission. Existing source-bound native registration and positive-final accounting remain required.
