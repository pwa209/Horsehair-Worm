# Personal scratch reserve amendment

On 6 November 2026 the user instructed: “you can raise the bar, and make the free slots 200000 is sufficient”. This package changes the personal free-file admission floor to **200,000 total slots**, including the existing five receipt inodes. Each caller continues to require its full prospective file demand:

```text
required_files + receipt_overhead + reserve_files
    = max(200000, required_files + 5)
reserve_files = max(0, 200000 - required_files - 5)
```

The census, existing primary consumer and existing agent coordinator use the same explicit, source-pinned policy. The original scientific configuration, 250,000 study-file occupancy cap, byte limits, freshness clocks, numerical routines and accepted worker IDs remain preserved. The policy records instruction provenance; it does not claim cryptographic identity verification or reserve quota against another project.

The final runtime namespace is `maintenance/storage-reserve-200k-v2-20261106`. The first staged namespace and its qualification are retained as history after integration testing identified a compiler-context issue in the private function derivation. Immutable staged files are not overwritten. This amendment revises the existing components and adds no persistent supervisor.

The census closure retains its finite 64-slot, four-hour schedule. The original last census and pending old controller successors were held before deployment. A finite continuation of the already verified orthology archive is required as well: the observed 235,494 study files plus the 18,565-file forecast exceed the independent study occupancy cap. The continuation must reverify the existing archive, its full source roster and all retained inputs before removing any copy. A longer allocation is used; the earlier metadata microbenchmark did not establish that the whole operation would fit three hours.

Native qualification, queue registration, successful startup and scientific progress are distinct milestones. Actual job IDs and verification outcomes will be recorded after their native receipts exist. Old failed census and cleanup receipts are not promoted to successful results.

`build-native-deployment.py` carries every member of each sealed source manifest, including non-Python preservation records. It writes held qualification plans, records the actual owned native allocation, and releases the original accepted ID. A partially acknowledged submission must be reconciled from its journal before any retry.

At **11:06 CET on 6 November**, preparation **8631848** is registered and released after qualifications **8630687, 8630826 and 8630827**, which remain pending Priority. The accepted preparation and its exact source capsule are saved under `execution/`. Its later `preparation/native-8631848/complete.json` will record the actual child census, archive, release and activation IDs; no future child ID is invented here.

The fresh personal-floor observation accepted **200,000 required / 247,776 available**. At 11:06 the native quota has **247,687 free slots**. Native validation and scientific resumption remain pending. All 503 local passes and two Windows-only skips refer to generic software fixtures; they establish no scientific finding. Each native action requires its own complete source-matched tests before production mutation.
