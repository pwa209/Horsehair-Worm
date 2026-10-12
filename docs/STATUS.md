# Study validation and source availability

12 October 2026: scientific source is synchronized to `pwa209/Horsehair-Worm`, and the Narval environment has passed Python, R and A100 qualification. Scientific source commit `e73f1c3e38da9883ce3ac7fa161b67b45d8ae54d` has SHA-256 `d6bcaaca072ba53ca3bd97b85ee96dc4913ac949809338dc1ff61c47e3ec03a4`; scientific configuration SHA-256 is `1bd9bc606ca563ce136e58234071db27070339dceb2b04a0d41ecfd8bcd29a61`. Documentation updates do not change the scientific source identity.

**162 tests passed on Narval in 220.65 seconds**. [GitHub CI](https://github.com/pwa209/Horsehair-Worm/actions/runs/38278350565) passed the same 162 tests. Native DESeq2 qualification and both production count-model fixtures passed. Actual A100-SXM4-40GB qualification passed: all four policy architectures agreed with CPU action probabilities within 1.2e-7, against a 3e-5 tolerance. These establish software and hardware qualification, not scientific findings.

Large biological dataset downloads run directly on Narval. All four required Dryad originals are now verified against the frozen provider byte counts and SHA-256 digests in the study's personal scratch. The previously recovered polarization workbook has 33,659 bytes and SHA-256 `1fda11643ae2691942a8b7edb8948a0c2e7e8abf8dec44bd5ddbb1c67ab4cb15`. The user approved a one-time small browser relay for the three remaining files, totalling 14,918 bytes; each was verified locally and again after transfer to Narval.

Direct scripted Dryad routes returned **403**, and the dataset API download returned **401 requiring a bearer token**. The public browser workflow ultimately delivered the original CSV and README files despite browser-tool timeouts. No Dryad API account, reconstructed data, alternate experiment or source-version substitution was used. Browser completion events were insufficient evidence: the actual files were accepted only after their size and published digest matched. The original failed transfer receipt is preserved, and the resolved files are registered against the unchanged source manifest for independent native verification.

| Verified original | Bytes | Provider SHA-256 |
|---|---:|---|
| Mating `mating_experiment.csv` | 9,119 | `bbad145de81d663eca132199248b487d9451b9f16eb437476ba17f8270ff9cce` |
| Mating `README.md` | 2,996 | `881cece1a78eab484cf85de64771cfef98c40fadf585370a75961068ed50ca15` |
| Polarization `README.md` | 2,803 | `ef74d0fd5aa0530495285ae2535345ebb0fc9b59da04310159cdb3b7eddac86d` |

Molecular software qualification found that Alliance Perl 5.36.1 lacks `DB_File`. The site's TransDecoder 5.7.1 environment supplies Perl 5.30.2 with DB_File 1.843; the isolated interpreter was independently checked under StdEnv/2023. A separately hashed environment amendment preserves the scientific source and configuration. Both external-tool examples must pass before molecular analysis can start. The original submission history, native execution receipts and environment adapter are retained locally and in the user's Narval project storage.

Further full phases require their measured admission receipts. Available quota and an A100 qualification alone do not establish the full experiment's runtime within the prespecified budget. No biological fit, final agent experiment, molecular contrast or headline scientific finding is complete. Test fixtures and shortened hardware pilots are never presented as biological evidence or qualified final agents.
