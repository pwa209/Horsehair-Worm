# Reviewed A–F operational sources

These versioned sources reproduce the reviewed operational helpers used for the
21 October A–F execution amendment. `source-manifest.json` records their exact
bytes. Generated transport envelopes, authenticated bridge state, credentials,
downloaded data and private request drafts are excluded.

The native execution release remains the immutable `4f3903d7c26e…` source, qualified
by job 6684484 with 1,694 tests and no failures, errors or skips. These operational
files live outside that source identity and bind their own reviewed hashes.
They are not general commands to replay: immutable cases, plans, ledgers and
per-mutation journals deliberately refuse duplicate or ambiguous submissions.

The initial activation registered three jobs under hold. Its exact-string node
check rejected Narval's semantically single-node `1-1` representation. The first
reconciliation then encountered a completed job purged from `scontrol`'s live
cache. Both attempts and all held jobs were preserved. Reviewed reconciliation v2
uses strict terminal `sacct` evidence and exact original ledger joins. Job 6686657
completed and released the existing three jobs without resubmission.

The native worker has 56 mocked orchestration tests. The final controller has
48 and the continuation launcher has 24 in the original preserved snapshot.
Native continuation 6687007 failed two fixture checks because the mock assumed
filesystem listing order; production guards correctly refused the wrong mocked
job. The separate `continuation-v2/` snapshot keeps the production controller
unchanged, fixes the job-to-script test fixture, and adds an order-reversal
regression. Its complete 49-test controller suite and 33-test continuation suite
passed independently. Fresh continuation 6688784 uses allocation-local synthetic
fixtures and preserves the original 48-test, two-failure native evidence.
The continuation also requires actual
native execution of the complete controller suite before registering downstream
work. Its original GPU and plan prerequisites must be positively completed; a
queued job or successful shell exit alone never qualifies scientific data.

The standalone acquisition helper may finish with `partial_acquisition` after
network failures. Downstream consumers must require `all_provider_files_verified`
and independently validate each file. A native `COMPLETED/0:0` transfer job does
not establish that every requested payload was retrieved.

The user prohibits author data requests. Acquisition must use publicly available
sources and direct downloads. No author message was sent.

Source data, source facts and synthetic software fixtures remain distinct.
Original float32 checkpoints, numerical failures, primary scientific configuration,
completed ant discovery and agent development evidence are preserved. Storage
limits and scientific metadata gates remain enforced independently of the lifted
cumulative CPU/GPU caps.
