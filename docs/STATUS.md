# Study validation and source readiness

This page records public scientific and readiness facts. Source and native qualification milestones are summarized here; detailed execution receipts remain in ignored local artifacts and Narval research storage. The managed workflow code is available; successful deployment and queue registration of any revised release require its own verified receipts.

## Qualification and workflow

Reserve amendment checkpoint, **6 November 2026, 11:06 CET**: the user approved **200,000 total personal free-file slots** as sufficient. The explicit operational policy now uses `max(200000, prospective_files + 5)`, including the existing five receipt inodes. A fresh Narval call accepted that exact floor with **247,776 available** at 10:51 CET; the later 11:06 reading has **247,687 free slots**. This is point-in-time readiness, not a reservation against other projects. The 250,000 study-file cap, byte limits, source/configuration and scientific gates remain unchanged.

The revised census, primary consumer and agent coordinator passed **340 local cases**. Shared policy, archive continuation, finite release, activation, preparation and deployment fixtures add **163 local passes and two Windows link-permission skips**. Source-matched native qualifications **8630687 / 8630826 / 8630827** are released and awaiting scheduler priority; native qualification success is not yet claimed.

Finite preparation **8631848** is registered and released with `afterok` dependencies on those three qualifications. It will register the 64 held census slots, a **12-hour, one-CPU continuation of the existing verified archive**, a finite census release after successful cleanup, and a controller activation after the first successful new census. Child IDs will be assigned and journaled by that finite job; they are not yet registered at this checkpoint. There is no additional persistent supervisor. The original final census stays held during cleanup, and scientific worker IDs/checkpoints remain preserved.

The earlier archive 8510943 passed its 43,145-copy tar roundtrip but exhausted its three-hour allocation before removal; zero copies have been credited as reclaimed. Census 61 and 62 completed 0:0. Census 63 finished scanning but failed its final 250,005-free check at 249,976; its unfinalized receipt is not promoted. Both prior active agent controller segments completed 0:0. The six old pending controllers are held for controlled replacement. Canonical progress remains **126 completed trainings / 122 policies**, revision **699**; scientific analysis has not resumed yet. See the [reserve amendment and finite queue](../operations/20261106/storage-reserve-200k/README.md).

The following earlier checkpoints are historical.

Storage follow-up, **4 November 2026, 16:26 CET**: the completed directory cleanup removed **7,177 empty directories**, and personal quota currently has **250,678 free slots**. The 250,005-free guard and 250,000-study-file cap remain unchanged. A6 has retired the separate 60,000-deletion requirement; increasing the cap alone would also increase the cap-derived free-space requirement.

Read-only audit **8508393 completed 0:0 in 11 seconds**, counting **55,758 regular files** in the two completed orthology result trees. Source and reference review identified **43,145 generated FASTA copies** in four flat `Orthogroup_Sequences` and `Single_Copy_Orthologue_Sequences` directories. Canonical consumers use the retained tables, mappings, original proteomes and receipts. Gene trees, working directories and every other result remain in place.

Archive job **8510943** is running on `nc31149`. All **53 source-matched native tests passed** before it began independently checking every selected file's ownership, link/type, identity, size and complete SHA-256. A complete tar must pass independent on-disk byte roundtrip and durable publication before any original is removed. The tar, full index and proofs will live under `results/storage-archives/orthology-sequences-20261104`, which the existing final-preservation collector includes; persistent backup is not yet claimed. **No sequence copies have yet been removed** at this checkpoint.

To prevent a census scan racing those removals, the same pending census61 now has an additional native `afterany:8510943` wait condition. Its original predecessor is already terminal and its source-declared chain remains intact. All original census IDs, scripts and resource settings remain; B3 still waits for a successful fresh census61. This adds no persistent supervisor or repeated scientific job. Estimated reclaim is not credited as free space.

The earlier completed-recovery checkpoint follows:

Repair checkpoint, **4 November 2026, 15:20 CET**: personal scratch has **250,813 free slots** (749,187 used of 1,000,000), above the unchanged **250,005** admission guard. This is a small margin, not a reservation against future growth by other projects. Canonical campaign revision **699** remains **126 completed trainings and 122 completed policies**; the four completed trainings and three evaluations still await ordinary controller reconciliation. RNA-full and molecular-corroboration remain complete.

The overnight controller cleanup **8463111 completed 0:0 in 4,600 seconds**, after all **79 native cases** passed. It archived and removed **20,652 additional unbound diagnostic files**, giving **33,782 verified originals removed** across 30 transactions including the earlier pass. Two sources with stale historical references were preserved intact. Its truthful result remained `recovery_incomplete` because the self-imposed 60,000-deletion minimum was unmet; quota handoff **8463177 failed before any census action**.

Empty-directory cleanup **8507752 completed 0:0 in 97 seconds** on `nc31114`, after all **28 native cases** passed. It independently verified every archive, complete removal journal, original terminal allocation and absent original path across those 30 transactions before removing **7,177 empty derived directories**. It retained 690 nonempty directories, the controller roots and all files. The canonical state remained unchanged. The copied repository maintenance/handoff package passes **63 tests**, with one Windows symlink-permission skip.

Replacement finite handoff **8507797 completed 0:0 in 131 seconds**. It replaces the arbitrary requirement to delete 60,000 files with positive archive/pruning proof and fresh sufficient quota. The original incomplete report is preserved. The cluster quota, 250,000-file study occupancy cap, 250,005-free guard, byte limits and scientific settings are unchanged. Raising the occupancy cap alone would also raise the cap-derived personal-free requirement; it is not a useful relaxation of this admission check.

At the earlier 15:20 checkpoint, all four original census jobs **6983037, 6983056, 6983059 and 6983060** had been released without resubmission, and census61 awaited priority. The verified future earliest times are **19:17 CET and 23:17 CET on 4 November, then 03:17 CET on 5 November**; dependencies and scheduler availability still apply. Existing restart job **8423933** remains pending on successful fresh census61 and the already successful coordinator v9/primary consumer v6 qualifications (**195 + 103 native tests**). No additional persistent supervisor was introduced. Healthy replacement controllers and resumed scientific analysis are not yet established.

The subsequent read-only OrthoFinder audit and selected archive job are recorded above. Their projected reclaim is not credited capacity until verified removal and fresh quota establish it. The coordinated CPU/GPU envelope and joint storage-growth holds remain authoritative; full-study completion and scientific gates remain unresolved.

The following **27 October** checkpoint is historical:

At **27 October 2026, 07:41 CET**, canonical progress records **126 completed trainings and 122 completed policies**. All 11 jobs previously held after the release-acknowledgement failure completed 0:0, and all four prior v4 controller slots completed 0:0. A new sequential chain using the same qualified code, **7495474–7495477**, has been released: the first is pending in the scheduler with no reported reason, and the other three are waiting on dependencies. Four training jobs and the primary controller are waiting for resources reserved for maintenance. Narval lists an all-node reservation from 27 October 10:00 CET to 3 November 10:00 CET; scheduler reservations can change.

Census slot 16 failed with exit code 126 after an I/O error when starting Python on compute node **nc10901**; slot 17 is queued. Python works on the login node, but a compute-node fix has not been established. Storage admission and scientific gates remain unchanged. Scientific conclusions and the complete study remain pending.

The earlier baseline passed 162 tests on Narval and in [GitHub CI](https://github.com/pwa209/Horsehair-Worm/actions/runs/38278350565). Native DESeq2 and both production count-model fixtures passed. A100-SXM4-40GB qualification found agreement between CPU/GPU action probabilities for all four policy architectures within 1.2e-7, against a 3e-5 tolerance. These are historical software/hardware qualification results. Revised scientific source requires fresh source verification and source-matched native benchmark/admission receipts.

The managed implementation adds the complete phase controller graph, four finite sequential controller segments, full task/architecture agent timing coverage, CPU evaluation-grid aggregation, scientific readiness checks and measured phase admission. Local regression tests cover exact PPO resume, frozen-policy restoration, missing/tampered receipts, complete native array membership, prospective concurrency, idempotent submissions and native-proof crash recovery. Generated fixtures establish implementation behavior and do not contribute study observations or scientific replication.

The controller advances eligible finite phases automatically. Missing scientific metadata, incomplete benchmarks, failed native jobs, ambiguous submissions and forecasts exceeding the study limits remain explicit conditions requiring resolution. Complete phase code and controller registration do not establish completed experiments or scientific support.

The earlier release-acknowledgement repair passed **144 native tests** in qualification **7371890**, completed 0:0 in 25 seconds, and **225 local operational tests**. The existing coordination component resolves a lost release acknowledgement using positive final accounting when a successful job is no longer observable through the live-job interface. It verifies the original issued-release journal, exact job ID, held script/name/owner/resources and actual successful final allocation. It updates only the release acknowledgement; original scientific receipt validation, numerical routines, provider freshness and deadline reserves remain unchanged. Missing, conflicting or unsuccessful evidence cannot manufacture an acknowledgement or cause a repeated submission.

At **26 October 2026, 11:28 CET**, canonical revision **559** recognizes **101 completed trainings and 94 completed policies**. Completed assembly 7352510's release acknowledgement was recovered from its original identity bindings and actual final accounting, without rerunning it or editing canonical state. The recovered batch has **3 completed assembly jobs**, **1 completed CPU evaluation job**, **3 running CPU evaluation jobs** and **4 running GPU training jobs**. All 11 original jobs were released without resubmission.

At that checkpoint, controller **7373985 was RUNNING** with source-verified startup; **7373992/7374031/7374040** are released and awaiting sequential dependencies. The previous four controller slots were positively retired at a registration boundary, while worker jobs continued independently. Qualified source availability and queue registration do not establish successful production controller startup before its allocation and startup receipt exist. The primary pipeline remains pending maintenance availability; full scientific inference remains pending.

Five inactive public-source trees containing 11,043 regular files were previously archived and byte-verified before removing extracted copies. Source-only synchronization stores a complete committed archive, manifest and receipt per revision. Active scientific source, observations, checkpoints and original inference gates remain preserved.

Earlier, five repaired census sweeps and the complete-cohort behavioural objective/gradient comparison completed successfully. The latter verifies one full training fold at frozen initialization; it establishes no optimizer update or scientific fit. The controlled handoff has completed. At that earlier checkpoint the primary replacement was queued, with successors waiting on dependencies. Full inference and publication readiness remain pending.

## Public source availability

The required original Dryad polarization and mating files have been recovered and verified against the frozen provider byte counts and SHA-256 digests. Their source definitions were preserved. Direct scripted provider refusals and the integrity-verified recovery receipts remain in the research execution records.

| Verified original | Bytes | Provider SHA-256 |
|---|---:|---|
| Polarization `data_deposited.xlsx` | 33,659 | `1fda11643ae2691942a8b7edb8948a0c2e7e8abf8dec44bd5ddbb1c67ab4cb15` |
| Mating `mating_experiment.csv` | 9,119 | `bbad145de81d663eca132199248b487d9451b9f16eb437476ba17f8270ff9cce` |
| Mating `README.md` | 2,996 | `881cece1a78eab484cf85de64771cfef98c40fadf585370a75961068ed50ca15` |
| Polarization `README.md` | 2,803 | `ef74d0fd5aa0530495285ae2535345ebb0fc9b59da04310159cdb3b7eddac86d` |

Large fly and RNA sources are acquired directly on Narval and must be independently verified from their accession/checksum manifests before analysis. A scientific source-code update does not restart an existing transfer. Verified raw files and compatible partial downloads retain their frozen source provenance; changed source definitions require a versioned amendment.

Preserved RNA transfer failures contained application connection-error XML inside otherwise correctly sized compressed files. These copies failed both provider MD5 and gzip CRC validation. A separately staged reconstruction using independently matching official byte ranges passed the complete provider MD5, independent disk hashes and full gzip validation. Detailed immutable forensic, reconstruction and deployment receipts remain in the research records. The v3 transport detects this known error before committing a segment and keeps downstream RNA jobs held until complete acquisition verification; deployment of the revised executable still requires its own qualification and queue receipts.

## Scientific readiness and interpretation

The processed fly archive supports an author-defined uniform 3Hz recording clock. Primary exposure-modulated H1 additionally requires source-verified exposure offsets. Exposed survivors remain exposed; missing unexposed controls, intervention conditions or original animal/reagent/gene/family mappings remain pending. Independent intervention and molecular-transfer stages require validated shared-control uncertainty and unique biological/gene identities.

Formal behavioural admission requires complete numerical workload/recovery measurements and the declared diagnostics. Agent admission requires native timing for every task/architecture cell and the complete CPU perturbation-grid roster. Final H4 interpretation additionally requires actual policy competence, achieved matching, restoration and independent final-seed outcomes. The primary agent experiment supplies computational evidence; a biological bridge remains conditional on separately validated and frozen evidence.

No completed biological fit, final agent experiment, molecular contrast or headline scientific finding is established by these qualification and readiness records. Negative effects can be valid scientific outcomes when their required diagnostics are complete. Missing inputs and underidentified models remain explicit limitations. Publication readiness requires all six evidence-based figures and actual visual review.
