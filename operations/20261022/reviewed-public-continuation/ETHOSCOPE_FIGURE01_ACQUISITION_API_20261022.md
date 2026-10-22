# Complete Figure01 acquisition design — source metadata only

This is a separate public host-response cohort identified prospectively in
`docs/PROSPECTIVE_SECONDARY_ANALYSES_20261021.md` (SHA256
`df47c02ef2e401b86b824618b36d58733679fd1be485c738169fdc4ac1a3e9a5`).
It does not recover the original fungal exposure/ROI crosswalks or clear original
H1–H3, D, E2, causal assignment, neural mechanism, or gene transfer. No author
contact is used. No new raw payload was acquired locally or remotely in preparing
these files. No SSH, scheduler operation, deposited-program execution, database
query, coordinate read, or analysis was performed.

## Frozen deliverables

The authoritative complete manifest is
`work/ethoscope-figure01-acquisition-manifest-v2-20261022.json`:

- 484,213 B; SHA256
  `92248b2b213fe7db425ee1fbe9c18c0699c7638943f7edd34bcf2081eeb91490`.
- Builder `work/build-ethoscope-figure01-manifest-20261022.py`: 24,112 B;
  SHA256 `d043f7c85e52c2e5ddb64f3c6eee3935a20fb24586870cf25a78288669902314`.
- Tests `work/test_build_ethoscope_figure01_manifest_20261022.py`: 7,809 B;
  SHA256 `f1ea6f7a8b3b80a10f2a05ae36cca904efec4e348e05c9eebd5e4402da9da72b`.
- Local metadata-only tests: **34 passed, 8.02 s**. JUnit
  `work/figure01-manifest-tests-20261022.xml`: 5,450 B; SHA256
  `ff5cbe28956b3850412583bb41609fcd3cefcfe70190b61e8abba7aec82dc113`.

The earlier unqualified metadata design
`work/ethoscope-figure01-acquisition-manifest-20261022.json` is retained as a
superseded draft; do not use it for admission. The v2 builder refuses to overwrite
the v2 output. Its pure `build(documents)` interface permits byte-for-byte
reconstruction without creating a transfer case or opening a network connection.
The 34 tests qualify this metadata design only. A download adapter has not yet
been written or qualified.

## Complete selection and joins

Select every metadata row whose literal `figure` is exactly `Figure01`. Form
date/machine keys, sorted lexicographically; resolve each key to exactly one
indexed `.db` pathname by the full machine-folder literal and the first ten
characters of the indexed recording timestamp. Refuse any missing or ambiguous
join. This resolves **all 36 sources**. The canary from FigureS09 is not included.

The manifest retains **all 370 original ROI rows**, every literal field, original
1-based source line and exact raw-line SHA256. It does not apply `obs`, `to_count`,
movement, death, survival or outcome filters. The complete flag universe is
362 `ok`, one `no_fly` and seven `ethoscope_killing`; the eight other entries are
also explicitly indexed in the summary. There are 165 Francisella, 103 PBS and
102 uninfected literal labels, in experiments C/D/E/K/L with 87/80/83/60/60 rows.
All are author labels, not verified infection or randomized assignment. Repeated
ROI minutes and databases on one date are not independent interventions.

For every DB, the manifest includes the exact source MD5, CRC32, compressed/raw
sizes, complete central entry, disk/local-header offset, independently indexed
successor, inferred data interval, ordered physical part segments, exact part URL,
part bytes and part provider MD5. The complete 59-part order is also retained.
Part MD5 values describe provider source identity: **range acquisition cannot
verify the whole-part MD5**. A computed raw SHA256 is not available until actual
acquisition and verification.

Seven existing documentation artifacts are SHA256-bound, including the provider
record, complete original metadata, deposited raw-member MD5 table, part checksum
table, indexed directory, raw directory and raw end-record tail. The builder also
verifies the provider MD5/size of both checksum documents, all part MD5 values
against both official records, and all 1,305 parsed binary central entries against
the frozen JSON index. It does not fetch or read any scientific database.

## Six split joins must be supported, never dropped

Every selected central entry is flags=0, method=8, version-needed=20, regular file,
without a per-member ZIP64 extra. The central extra is 24 B. The distance to its
independently indexed successor implies an **unverified** local header of 198 B:
30 fixed + 140 filename + 28 local extra. A local header is not assumed equal to
the central extra. Every inferred header fits inside its starting part. These
inferred values must be checked against actual native range bytes before any
payload request; they are not present-tense acquisition or schema evidence.

Six compressed streams cross into the next part. Physical ranges below use
inclusive starts and byte counts; the first segment ends at 2,097,151,999 and the
second begins at zero.

| Metadata key | First part / start / bytes | Next part / start / bytes |
|---|---|---|
| 2017-03-15 / ETHOSCOPE_012 | z20 / 1,943,921,697 / 153,230,303 | z21 / 0 / 33,130,792 |
| 2017-03-15 / ETHOSCOPE_035 | z57 / 1,967,848,806 / 129,303,194 | z58 / 0 / 68,858,590 |
| 2018-05-25 / ETHOSCOPE_001 | z45 / 2,074,645,025 / 22,506,975 | z46 / 0 / 183,258,845 |
| 2018-05-25 / ETHOSCOPE_057 | z07 / 2,078,127,862 / 19,024,138 | z08 / 0 / 153,647,320 |
| 2018-05-25 / ETHOSCOPE_063 | z02 / 2,010,330,909 / 86,821,091 | z03 / 0 / 162,079,467 |
| 2018-06-06 / ETHOSCOPE_054 | z12 / 1,920,555,309 / 176,596,691 | z13 / 0 / 13,743,007 |

In total, the cohort touches 24 provider parts. No complete part is needed. Read
segments consecutively as one deflate stream; do not scan for magic bytes,
restart the decompressor at a split boundary, skip a boundary marker, or discard
a source because the old canary cannot span parts. Any unsupported source leaves
the cohort incomplete with an explicit failure, never a 30-source success.

The archive has a separate **global ZIP64 end layout**: 223,898 B of central
directory on disk58 at offset2,022,980,783; a 56 B ZIP64 end record at
2,023,204,681; a 20 B locator; and a normal 22 B EOCD. The locator names 59 disks.
The metadata builder checks the exact global records against each other. Global
ZIP64 end records do not mean these 36 members need ZIP64 local extraction.
Runtime support remains deliberately limited to their ordinary indexed headers.

## Proposed narrow adapter boundary

Keep the already qualified canary unchanged:

- `work/acquire-ethoscope-db-canary-20261021.py`, SHA256
  `30a843a2771196a3eec1e202f8e695fd1d41e58b6423a73218ec74b6285ea17c`.
- Its 39-case test source, SHA256
  `6a7d31cdcc08674aee39af0361cfe234d1eb669548634daf3458c4dd592f4ab3`.
- Existing `work/ETHOSCOPE_RANGE_CANARY_API_20261021.md` remains historical canary
  documentation. A successful single canary is not full-cohort approval.

A new private adapter can retain the canary's exact response checks, confinement,
exclusive durable receipts, raw deflate/CRC/MD5/SHA256 checks and publication
semantics. It must replace module-global `TARGET`, `PART_URL`, `PART_BYTES` with
explicit immutable member/part arguments. Never mutate imported canary globals
or relabel its source hash/tests as the new implementation.

Separate two interfaces:

1. `acquire_one(manifest, source_id, fresh_transfer_case)` validates exact source
   bytes and complete manifest, parses the two bounded local-header reads, then
   fetches only that member's listed compressed segments into personal Narval
   scratch. It records all intended/actual chunk ranges, headers, partial bytes,
   hashes, errors and final closed transfer receipt. It does not import SQLite,
   inflate a full DB, analyze data, or dispatch scientific work.
2. `verify_one(manifest, source_id, closed_transfer_receipt, fresh_verification_case)`
   runs under a registered Slurm allocation. It validates every retained chunk
   and transfer proof, consumes exactly the frozen segment order through one
   raw-deflate state, enforces both byte bounds, verifies complete raw CRC32 and
   provider MD5, computes SHA256 and produces a durable provider-verified member
   receipt. Existing canary exclusive publication/collision checks apply; only
   its own temporary publication link may be removed after verified publication.
   No original source, range, failure or historical result is deleted.

All path inputs/outputs remain strictly under the declared personal scratch root;
no shared scratch, subordinate symlink, traversal or whole-archive extraction.
Recheck adapter, manifest and seven input SHA256 values before publication. Check
raw candidate and all chunk identity/content proofs again before final promotion.
Any ambiguous publication, timeout, missing input or collision preserves evidence
and forbids same-case replay. No retry, resume, alternative host or automatic
replacement of a failed source is implicit in this design.

## Proposed bounds and peak reservation

The full cohort has **7,970,156,585 compressed B + 15,499,844,608 raw B**. Preserve
all compressed ranges and verified raw DBs. The minimum retained source-byte
floor, including inferred local headers, is **23,470,008,321 B**. Reserve
**26,000,000,000 B and 5,000 additional files for one full attempt**, including
receipts, bootstrap fixtures/logs, small source copies, and partial/error evidence.
Reconcile this reservation with live study forecasts and personal quota before
dispatch; failed repairs need their own additional peak reservation.

The proposed new adapter uses at most **8 MiB per HTTP request**, streamed in
64 KiB buffers, with at most one request active. It makes exactly **1,045**
requests for a complete unchanged cohort, including 72 local-header requests.
Three per-request artifacts give 3,135 files; 20 fixed files ×36 give another720,
leaving margin within5,000. The old1 MiB bound would require7,694 requests and
23,082 range artifacts; it must not be combined with the new5,000-file estimate.
The largest selected member is328,694,564 B compressed /654,524,416 B raw, requiring
42 total requests under the proposed8 MiB bound. No request covers a whole part.

Bound each member's transfer at900 s, with15/90 s connect/read timeouts, no
redirects, identity encoding, exact206/Content-Range/Content-Length, and a minimum
one-second spacing between request starts. Header/part identity, index and source
MD5 disagreements fail before publication. An observed historical133-rate header
does not establish the current rate-limit period or guarantee eligibility;429,
401/403, other status codes or malformed range responses stop the case with
evidence, without automatically sleeping/retrying or reading a non-range body.

Use a separate 1 CPU /4 GiB native verification allocation, with a600-second hard
verification budget per member. Gateway work is bounded network streaming and
receipt writing; the full15.5 GB decompression/hashing work belongs under Slurm.
Whether compute nodes can reach the provider has not been verified. If a native
network canary later succeeds, transfer may also run under an admitted allocation;
do not assume that route while constructing the design.

These are proposed bounds, **not a throughput benchmark or completion promise**.
The worst-case sequential design budget is36×(900+600)=54,000 s (15 h), excluding
qualification/queue time. Live storage/accounting freshness must be renewed during
such a run; a once-captured6 h admission cannot authorize the whole15 h interval.
Root owns resource/source qualification, Slurm registration, strict ownership
proofs, fresh census and execution activation. This document grants none of them.

## Exact qualification additions before any payload

Port the complete39-case canary test set into a new adapter suite, preserving
failure assertions. Keep real provider traffic out of these tests. Add independent
synthetic fixtures and explicit expected outcomes for at least these contracts:

| Addition | Required challenge and outcome |
|---|---|
| Complete manifest | Recompute the frozen36/370 universe; omitted/duplicated source, dropped non-ok ROI, changed source row, ambiguous date-machine mapping or wrong manifest SHA must refuse before HTTP |
| Physical parts | Wrong disk→zNN mapping, part bytes, provider MD5, URL, part order, missing successor or out-of-range interval must refuse before payload |
| Split continuation | Deflate data divided inside a compressed block and at a1-byte part boundary must reproduce exact raw bytes through one decoder; swapped, duplicated, missing, skipped or altered join bytes must fail CRC/MD5/size/EOF without publication |
| Real frozen geometry | Verify the six exact joins above and all30 one-part intervals from metadata; total compressed bytes and1,045 request count must match; no whole-part request may be produced |
| Header expectations | Central extra24 B versus local extra28 B must be accepted only after actual bounded parsing; wrong actual extra length, local flags/version/name/CRC/size, ZIP64 local extra, descriptor, encryption or cross-part header must refuse |
| Global end records | A correct global ZIP64 end/locator with ordinary members must not trigger a blanket member refusal; changed locator/total-disk/central-offset bytes must fail metadata binding |
| New8 MiB bound | Exact8 MiB range accepted;8 MiB+1, part-end overflow, entire-part request, bool/noninteger/negative ranges refused; body overflow may preserve at most declared bytes+1 evidence |
| HTTP cross-part | Each request requires the exact URL-specific total and physical interval, including start0 of continuation parts; status200/302/401/403/429/500, changed encoding/length/range or a short body must fail before a positive receipt |
| No whole-body fallback | Non206 response body must never be consumed; redirects remain disabled; no general ZIP extractor or full-part endpoint fallback may run |
| Evidence preservation | First-part success then second-part timeout leaves all first/second intents, headers and partials, no promoted DB, no retry and no same-case replay |
| Hard deadlines | Native POSIX subprocess tests cover transfer900 s and verification600 s termination, including blocking network and blocked deflate read; timeout receipt remains durable and does not imply cohort completion |
| Source races | Mutate manifest, metadata, helper, chunk or verified candidate between validation and publication; reject without replacing existing evidence |
| Slurm boundary | Network phase must not inflate/query; verification must require positive owned native allocation/closure evidence and reject a changed/stale/unowned transfer receipt; neither phase may dispatch analysis |
| Exclusive publication | Existing final path, two-link crash ambiguity, partial publication and failed fsync keep prior bytes and error evidence; no automatic unlink/replay |
| Cohort completion | Exactly36 positive member receipts joined to the same manifest and all370 ROI rows required;35/36, duplicate/cross-manifest receipt or any unsupported header yields incomplete cohort; every science/assay flag remains false |
| Rate behaviour | Spacing/deadline controlled by a monotonic clock, one active request;429 and timeout never cause an unbounded wait or automatic retry; headers are recorded without inferring an undocumented period |

The 34 completed metadata tests are not substitutes for these future adapter tests.
Do not copy counts or hash identities from the old qualified canary onto the new
adapter. After source-only review, root must qualify and freeze the complete new
adapter, tests and native operational closure before acquiring any Figure01 DB.

## Receipt meaning and next scientific gates

A complete acquisition result requires all36 exact member receipts, every
compressed/raw count and deposited raw CRC/MD5 check positive, all computed raw
SHA256 values, retained range evidence and an unchanged370-row manifest. A process
exit alone, one successful canary,30 nonspanning sources, or a metadata-only join
is not a complete acquisition.

Acquisition then enables a separately reviewed read-only schema/identity/clock
audit. It does not establish source ROI presence, expected sampling, units,
geometry, missingness, inference semantics, terminal-immobility meaning, per-animal
exposure time, randomization or scientific analysis eligibility. Keep Figure01
separate from other figure records and their conflicting identities. Any later
model/holdout protocol must remain prospective and conditional on qualified assay
definitions; it must not be chosen from a favourable effect.
