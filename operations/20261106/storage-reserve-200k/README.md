# Personal scratch reserve amendment

The user approved **200,000 total personal free-file slots** on 6 November 2026. The shared explicit policy accounts for the existing five receipt inodes and preserves larger prospective file demands:

```text
required_files + 5 + reserve_files = max(200000, required_files + 5)
reserve_files = max(0, 200000 - required_files - 5)
```

The 250,000 study-file cap, byte limits, frozen scientific configuration, freshness checks and accepted worker IDs remain unchanged. The personal-quota check is a current observation, not a capacity reservation against other projects.

At **11:44 CET**, preparation **8633285 completed 0:0 in 446 seconds**. It registered the actual recovery graph: cleanup **8634115** is pending Priority, release **8634118** waits on cleanup, and activation **8634125** waits on cleanup plus the first successful new census. The 64 new censuses beginning with **8633757** remain intentionally held until cleanup succeeds. Current personal quota has **247,053 free slots**. All 340 native qualification cases passed; new scientific work has not resumed. The actual IDs are preserved in `execution/preparation-complete.json`.

The following earlier checkpoint describes registration before preparation ran:

At **11:35 CET**, personal quota had **247,306 free slots**. All **340 native cases passed without skips**: census 8630687 (30), primary consumer 8632769 (112), agent coordinator 8632772 (198), each completed 0:0. Local operational fixtures total 504 passes and two Windows link-permission skips; native actions require their own complete passing tests before mutation.

Preparation **8633285** is registered and released, pending Priority. It will register the direct finite Slurm graph: the 64 held census slots, a 12-hour one-CPU continuation of the already verified orthology archive, a post-cleanup census release, and controller activation after the first successful fresh census. Future IDs will appear in `maintenance/storage-reserve-200k-v3-20261106/preparation/native-8633285/complete.json`. No extra persistent supervisor is introduced. Scientific analysis and full-study completion are not yet established.

The approved policy and successful census remain authoritative under `maintenance/storage-reserve-200k-v2-20261106`. Corrected controller and finite handoff capsules use `maintenance/storage-reserve-200k-v3-20261106`. V3 contains byte-identical census/shared-policy copies solely for isolated fixture loading; production explicitly uses the V2 census and policy locators. Controller qualification originally failed because test discovery assumed a local extracted repository. Native-first, accessible source lookup corrected those fixtures while retaining every source-hash assertion and behavioral check. The original failed qualifications and cancelled preparation remain preserved.

The first V3 preparation submission was explicitly rejected without an ID because a completed parent had expired from the scheduler's live table. Its original ledger and exact response were preserved; actual successful accounting was persisted and the same plan was submitted once to obtain 8633285. The source builder now refreshes final accounting before resolving dependency flags. Accepted or uncertain submissions must never be replayed.

The original 43,145-copy archive passed a complete byte roundtrip, but the three-hour allocation ended before removal. The finite continuation reuses that tar, independently verifies its full roster and original identities, and keeps all 65 declared census writers held until completion. Its 77 native tests precede any unlink. The separate study count plus the next-batch forecast still requires this cleanup; lowering the personal floor does not waive that occupancy check.

The finite release runs its 12 native cases, verifies successful complete removal, and releases the same 64 new census IDs with conservatively reanchored start times. The activation runs 62 native cases and retires only six exact held old controllers after eight replacements are proved held. Prior active agent controllers must have completed successfully; census 6983060 stays held. Full scientific forecasts remain enforced by the replacement controllers.

`execution/` records the accepted preparation, exact source capsules, certain-rejection recovery and native qualification checkpoint. The earlier V2 queue record is explicitly historical. These are generic operational results, not scientific findings.
