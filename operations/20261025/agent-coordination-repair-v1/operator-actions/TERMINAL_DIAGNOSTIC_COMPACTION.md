# Terminal diagnostic compaction

`compact-terminal-readers-20261103.py` is a one-off maintenance action for restoring personal-scratch inode headroom. It uses the existing qualified archive helper from [the original archive closure](../../../20261021/public-data-and-repair/verified-evidence-archive-20261021.py). It adds no scientific controller or scheduling layer.

Only workers already marked complete in the canonical agent campaign are eligible. The action independently joins their actual successful final Slurm allocations to the current cost ledger, preserves each original worker receipt, and archives only allowlisted accounting, scheduler, quota and held-job diagnostic leaves. A receipt reference to a selected leaf refuses removal. Every pack must pass the existing byte-for-byte archive roundtrip and native-canary checks before the existing removal API is called. Checkpoints, summaries, plans, condition artifacts, assembled trials and active-job evidence remain ordinary files.

The action requires the exact `compact.sbatch` sibling script and job name `hhw-terminal-diagnostic-compaction-20261103` in an owned scalar **1 CPU, 16 GiB, four-hour** Narval allocation. Its source hash is an explicit invocation argument. It holds the existing controller mutex and checks that canonical campaign-state bytes remain unchanged. Its measured recovery working-space check covers the current pack and a 5,000-file reserve; it does not admit scientific work or alter the scientific 250,005-file headroom requirement. It stops when 300,000 free file slots are observed, or reports that its eligible diagnostics were exhausted.

This is an irreversible removal of verified diagnostic originals after archival. An interrupted removal requires explicit byte-bound reconciliation through the existing archive helper; do not blindly replay a partly completed request. Preserve each archive, index, roundtrip receipt, native proof and removal journal.

Source SHA-256 pins:

| Source | SHA-256 |
| --- | --- |
| Compaction wrapper | `4a977c7b109d0e9df0a04308dedd7e85242aac00588e47ad30d5feb45ed4476b` |
| Repository wrapper tests | `7ce5d9609557bc443c6c916f3c3d8bb9c637b785b085a67cd03bc1bd1f09ab4f` |
| Existing archive helper | `045d8650db62c0fa52d7f56bb7bdb2669729509c23ecfbbc847e73cbbff8d42a` |
| Existing 39-case archive test source | `ec4131d283d3f3a792a561c6ae6324de853a4301def9d275a03018a9801f5462` |
| Original finalized native canary receipt | `921635cbeac2b954c7946c7a6ddd3c375a8ecd862630492d1aa0ee26b58ac14b` |

The repository wrapper suite passes **21 cases**, including a real archive roundtrip/removal fixture, preservation of all 600 policy-condition files, changed-source refusal, conflicting native ownership/accounting, referenced diagnostic leaves, and wrong allocation/script/name/deadline rejection. Repository tests load the existing archive closure in place. A flat native bundle can instead provide the unchanged helper and its test source as siblings. The frozen deployed test source has SHA-256 `9291d7c57043821f8d31c9125c876c64a86fbe0cbf22b6d72ee5dfd879b12ffe`; the repository version changes only this path lookup.

Passing fixture tests and queue registration do not establish actual reclaimed capacity. Require the real native result, verified removal journals and a fresh personal-quota observation before reporting file-count recovery. Scientific resumption additionally requires the unchanged fresh census and admission checks.

## Retired controller diagnostics and interrupted maintenance

The worker-only eligible set proved insufficient and its per-file Lustre roundtrip was slow. The replacement `compact-terminal-controller-reads-20261103.py` is another finite maintenance action in the same operation. It accepts a source-hashed allowlist of positively terminal old controller allocations, independently verifies their original plans/ledgers/scripts and final accounting, and retains all recursively referenced evidence. Only allowlisted unbound raw reader leaves are selected. Scientific data and models remain untouched.

Each pack has a byte-verified tar archive, an independent disk extraction on the validated allocation-local filesystem, and durable archive/receipt publication before removal. Archive bytes and directory entries must successfully `fsync`; native failure stops cleanup. Bound-document guards and unchanged original-byte/stat checks apply at each removal. Progress is journalled initially, every 128 removals, at completion and on exception. The positive native directory-sync probe is separate evidence from fixture tests. The 50-case suite has 48 passes and two link-permission skips on Windows; **all 50 must pass without skips on Narval before cleanup**.

`reconcile-stopped-worker-archives-20261103.py` first verifies actual final accounting of stopped worker maintenance and checks its published archives with the exact original qualified helper. An interrupted removal is resumed only through that helper's explicit reconciliation API with the original journal SHA. Unpublished packs retain all originals. Missing or conflicting evidence fails before the controller cleanup.

A maintenance result reports actual files removed, exhausted eligible scope and measured personal quota. It is not a scientific admission receipt. The separate finite quota handoff checks fresh unchanged headroom before releasing the existing census slots; a successful fresh census and both source-matched qualifications precede controller registration. The registration handoff executes the original primary and agent registration actions once, with inherited `SLURM_*` fields removed from the administrative child environment. Actual qualification IDs are bound from their immutable source-matched registration records. No persistent supervisor is added.

The dated registration scripts preserve exact original paths, IDs and source hashes as an execution record. Do not blindly replay or retarget a sent mutation, an existing immutable support directory or a partly completed archive transaction.
