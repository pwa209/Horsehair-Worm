# Exact condition archives for amended H4

This is a storage representation interface. The original240 policies,
300 conditions,20 seeds,153,600 trial rows per policy, statistics and declared
float64 evaluation amendment are unchanged. No scientific row is filled,
dropped, rounded, resampled or inferred from a missing file. A new immutable
public release containing this adapter and its tests needs complete native
qualification before use. The earlier8495 source qualification does not qualify
new local adapter code. A separate positive consumer integration canary is
required before any operational compaction; this module never unlinks files.

`freeze_inputs(..., archive_registry={"path": ..., "sha256": ...})` binds one
immutable registry in `inputs.json`. It copies each listed policy descriptor
into its matching input entry. Registry keys are exact decimal strings `"0"`
through `"239"`, without leading zeroes. An unlisted policy requires every
original JSON receipt and parquet part. A listed policy uses its whole verified
archive; partial fallback between original and archived parts is forbidden.

The exact registry fields are:

```json
{
  "schema_version": 1,
  "kind": "final_policy_condition_archive_registry_v1",
  "root": "/scratch/pwa209/horsehair-worm",
  "full_config_sha256": "<original full configuration SHA256>",
  "primary_scientific_source_sha256": "<original scientific source SHA256>",
  "archive_helper": {"path": "<explicit private helper.py>", "sha256": "045d8650db62c0fa52d7f56bb7bdb2669729509c23ecfbbc847e73cbbff8d42a"},
  "archive_canary": {"path": "<finalized native39 canary.json>", "sha256": "<SHA256>"},
  "cells": {"0": "<descriptor object below>"},
  "scientific_result": false,
  "admission_authority": false
}
```

Every path must be confined to the canonical personal study root, without a
subordinate symlink. All locators contain exactly `path` and `sha256`. The
production helper has no default import path: only the explicitly supplied
source bytes with the exact reviewed045d hash can be loaded. The test fixture
is never selected automatically. Registry and receipt JSON reject duplicate
keys and nonfinite numeric literals.

Each descriptor contains exactly:

```json
{
  "evaluation_case": "<canonical campaign/cell-0000 path>",
  "policy_inputs": {
    "plan": {"path": "<case>/plan.json", "sha256": "<SHA256>"},
    "binding": {"path": "<case>/plan-binding.json", "sha256": "<SHA256>"},
    "summary": {"path": "<case>/evaluation_summary.json", "sha256": "<SHA256>"}
  },
  "archive_directory": "<separate personal maintenance directory>",
  "index_sha256": "<index.json SHA256>",
  "archive_sha256": "<evidence.tar SHA256>",
  "pack_intent": {"path": "<archive>/pack-intent.json", "sha256": "<SHA256>"},
  "roundtrip": {"path": "<archive>/roundtrip.json", "sha256": "<SHA256>"},
  "pack_receipt": {"path": "<archive>/pack-receipt.json", "sha256": "<SHA256>"},
  "pack_producer": {
    "native": {"completion": {"path": "<proof>", "sha256": "<SHA256>"}, "owner": {"path": "<owner>", "sha256": "<SHA256>"}},
    "result": {"path": "<pack driver result.json>", "sha256": "<SHA256>"},
    "qualification": {"path": "<root>/maintenance/study-af-execution-support-v1-20261021/qualification.json", "sha256": "29ed530f3783f3c35c3cf20343bdb663732ec2bf4bf5c3218f8b029510becc2f"}
  }
}
```

The pack result binds `schema_version=1`,
`status=complete_verified_policy_archive_pack`, `native_job_id`,
`archive_helper_sha256`, `evaluation_case`, `archive_directory`,
`index_sha256`, `archive_sha256`, `pack_intent_sha256`, `roundtrip_sha256`,
`pack_receipt_sha256`, `execution_source_sha256`, `full_config_sha256`,
`primary_scientific_source_sha256`, and false `scientific_result` and
`admission_authority`. Its native proof must show the matching completed owned
scalar pack job and still match its selected final study accounting row.
The archive index/intent retain the independent original assembly authority:
that native proof is the assembly job, not the pack-producer job.

The initial transport supports only the fixed4f3903 framework, native job6684484
and its exact29ed530f qualification file. The1694-test actual JUnit roster and
owned four-CPU positive completion are independently checked. This qualifies
the public framework; it does not claim a private pack script was part of that
test suite. The private pack library instead requires its separately pinned
39-test source, actual full raw JUnit roster and finalized positive native
completion/owner proof. Other pack frameworks require a separately reviewed
interface amendment. Operational launchers remain responsible for the pack
wrapper's immutable script/native identity, storage admission and resource
lease. H4's own current full-source qualification is independent of both.

The consumer opens one reviewed `ArchiveReader` per cell. It verifies the full
uncompressed deterministic tar, every member checksum, exact600-member index,
canonical names, trailer and file constraints. It binds intent, roundtrip and
pack receipt, all three canonical policy inputs and the original completed
assembly result. All300 raw JSON receipts are parsed unchanged, with original
restoration and provenance checks. All300 parquet parts pass the unchanged
endpoint/calibration/episode validators. Their ordered concatenation must equal
the preserved assembled parquet exactly, including dtypes. The original
float32 checkpoint is loaded through the unchanged primary loader and promoted
weight identity is checked separately. Per-member reads and final archive
pathname/metadata checks detect changes after opening; the complete registry,
helper, canary, native rows, canonical files and archive hashes are rechecked
after statistics. No persistent validation cache is trusted.

Metadata has a finite10MB bound; the reviewed helper's finite128MB
`read_member` limit applies separately to each part. A larger part is refused,
not truncated. Normal full-condition parts contain512 rows. Original files may
coexist during qualification, but no original file is used as a fallback for a
listed archive. Checkpoints, plans, bindings, summaries, assembled trials and
native result receipts remain ordinary live files. Compaction journals and
all original receipt bytes remain preserved by the separate pack/removal
system. Native admission and file/byte quotas are never cleared by this module.

The synthetic test fixtures contain only exact reviewed helper/test source
code. Integration tests create a complete300-condition/600-member synthetic
policy archive, then validate all archived rows after their disposable
synthetic original parts are absent. No scientific dataset is bundled.
