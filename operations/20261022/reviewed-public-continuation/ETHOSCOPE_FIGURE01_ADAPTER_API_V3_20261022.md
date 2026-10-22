# Private Figure01 adapter v1 — bounded buffered-output revision handoff

Status: **local mocks passed; not peer-approved, natively qualified, deployed or
activated**. No real HTTP, scientific raw-data acquisition, SSH, Slurm operation,
database query, deposited-program execution or public source edit was performed.
The unchanged earlier canary does not qualify this new implementation.

## Exact source and test identities

| Artifact | Bytes | SHA256 |
|---|---:|---|
| `work/acquire-ethoscope-figure01-v1-20261022.py` | 46,893 | `0b2571bb650b95ae22f1988caebc8ef5c0371ba6b0ccea5bb8f5a88f332c1cbd` |
| `work/test_acquire_ethoscope_figure01_v1_20261022.py` | 37,663 | `24215ff1ec91d76e4b41e45f081c3253a9cd4a8f83f1765feaeaae7a16f98938` |
| `work/figure01-adapter-tests-v6-20261022.xml` | 20,753 | `e337c3d412e1c6fabc8981c63f7105650cb2ea9f79d458b4780bbff55aa35a99` |
| Complete frozen manifest v2 | 484,213 | `92248b2b213fe7db425ee1fbe9c18c0699c7638943f7edd34bcf2081eeb91490` |

The local run passed **123 tests in47.71 s, zero failures/errors/skips**. Test
roster SHA256 is
`94b983fa3aaee257fae79b2530225c6553cde169a7fb7adb094ab4ac0aa572cb`:
sort123 `[classname.split('.')[-1], testcase.name]` pairs and hash UTF8
`json.dumps(pairs,separators=(',',':'),ensure_ascii=True)`, without trailing LF.

Root replay, from the repository:

```text
.venv/Scripts/python.exe -m pytest -q work/test_acquire_ethoscope_figure01_v1_20261022.py --basetemp work/figure01-adapter-root-test-temp-20261022 --junitxml work/figure01-adapter-root-tests-20261022.xml
```

Windows needs an execution context that permits confined synthetic hard links.
Its `fstat/lstat` creation times can disagree; only the Windows metadata/mock
identity omits that field. Production is POSIX-only, retains complete ctime
identity, uses `O_NOFOLLOW`, checks actual owner UID3164768 and exact link counts.
Binary fd flags are explicit so Windows mock deflate bytes cannot be translated
or truncated at a text-mode control character. No production check is mocked by
the CLI.

## Complete qualification fixture closure

No old helper, builder, deposited R program or third-party source file is imported.
For native tests place the new adapter and tests together; beside them place
`ethoscope-figure01-acquisition-manifest-v2-20261022.json` and directory
`public-cohort-metadata-20261021/` with exactly these existing metadata artifacts:

| Basename | Bytes | SHA256 |
|---|---:|---|
| `zenodo-6567039.json` | 20,544 | `2514b6e65c2892c69db797263ee545d546a76beee4ac931fc3b1257f45992cce` |
| `ethoscope-METADATA.txt` | 1,030,376 | `83230109101cb01e57fb5f0d64b48b1965f07d9f45cf9c41db8105357c3d1616` |
| `db_files_with_checksum.txt` | 82,998 | `39d6f17957c394d95b3746df1aa4ff75ba15ae4722ffede293ed0c6de9c729c6` |
| `zip_files_checksums.csv` | 4,974 | `8cb455520bd4382d136010f6ef5fe0507cbcccaa0dd20b6ab60f2b01f6cbdff5` |
| `ethoscope-split-zip-index.json` | 596,027 | `ffc58aa960a5fdaec167112c4fece8fd7789fb5daad79675c9aa85980dee46b6` |
| `ethoscope-split-zip-directory.bin` | 223,898 | `c2a550fe715ab0de9bc70499b04f94c747da50845b9725cd96fa130420382528` |
| `ethoscope-split-zip-tail.bin` | 65,536 | `d80d5a2e9b380bfed81126a67cbdb29bb574a6c5fd298de79d4a2788b7d97385` |

These seven inputs total2,024,353 B. Manifest + adapter + tests + seven inputs
total2,593,122 B in10 files. All other test data are inline synthetic records,
random bytes and raw-deflate fixtures; HTTP is a local fake session. Tests never
read a scientific database or contact a provider. Actual frozen metadata tests
independently reject an omitted source/ROI, duplicate ROI, altered part order/URL/
MD5, changed segment join or a true scientific gate, even with a test-only
manifest-hash override.

The final local mock directory alone contained2,132 files /43,371,091 B. It was
not deleted. For native qualification reserve a separate **3,000 files /256 MB**,
then measure the actual footprint. Qualification fixtures are not silently
included in the later production5,000-file increment: include them in the fresh
baseline or add the separate reservation when admissions overlap.

## Interfaces and native contract

All paths must be absolute and below
`/scratch/pwa209/horsehair-worm`. New cases must be strictly under personal
`maintenance`, have an existing owned parent, and not exist. Every subordinate
symlink, wrong UID, traversal, unbounded input, multi-link source or conflicting
publication is refused. No recursive path creation or existing-case reuse.

The adapter reads the exact manifest and all seven SHA-bound metadata artifacts
before opening a fresh case. It validates every36 source, all370 original ROI
rows including the eight other `obs` flags, all59 ordered part bindings, provider
raw MD5s, central entries and exact physical intervals. No source or ROI filter
is applied. The manifest still has six two-part payloads; all local headers are
ordinary198 B headers. The frozen global ZIP64 end records do not enable ZIP64
local-member extraction. Headers with descriptor/encryption/member ZIP64,
unexpected version/name/CRC/sizes/extra length or successor mismatch fail.

Production transfer and verification must use the **same absolute manifest and
metadata paths**, as well as the same bytes: the complete path/hash closure is
recorded and rechecked across phases. The new helper itself can be staged at
another path only with exactly identical bytes. No old module globals are
modified; each request gets explicit member and part data.

### Transfer

```text
python acquire-ethoscope-figure01-v1-20261022.py transfer --manifest MANIFEST --metadata-dir METADATA_DIR --member ORDINAL --case FRESH_TRANSFER_CASE
```

`ORDINAL` is1..36 in the manifest's sorted date/machine order. Python interface:
`acquire_one(manifest_path,metadata_dir,ordinal,case,session=None)`.
The optional session is for injected mocks; the production CLI creates a session
with redirects disabled, `trust_env=False`, and zero HTTP retries. No proxy,
alternate host, whole-part fallback, resume or same-case replay is implemented.

The CLI installs a native POSIX900-second alarm covering the operation. API
callers also enter this alarm and must run on an isolated main-thread POSIX child
without an existing alarm. A monotonic900-second clock is checked during reads,
hashes and pacing. External root supervision remains necessary for any process
or filesystem failure beyond Python's signal handling.

It makes only the frozen exact206 requests, at most8,388,608 B each, one active
request and at least one second between starts, with15/90-second connect/read
timeouts. Exact Content-Range, part total, Content-Length and identity encoding
are required. Header/error text is bounded, positive encoded header proofs must
fit60,000 B before body consumption, and each error response artifact is capped
at262,144 B. A non206 body is never read. At most declared bytes+1 are retained
when a response exceeds its bound. Headers, intents, partial bodies and failures
remain in the fresh case;429/401/403 and other errors stop without retry.

Transfer reads/parses the actual30 B header and168 B name/extra continuation
before payload. Spanning payloads remain a single ordered compressed stream, with
exact ranges on each provider part; no marker scanning or split-byte skipping.
No decompressor or database query is called in this phase.

Positive output: `transfer-result.json`, status
`compressed_member_acquired_pending_native_verification`. It binds all range
intents, response and body hashes, actual local-header proof, complete source
closure and helper hash. Raw integrity and scientific flags are still false.
Failure output is `transfer-failure.json`, where possible; any incomplete or
ambiguous case is preserved and never automatically replayed. Root must retain
the exact successful transfer-result SHA256 for the next phase.

### Native verification

```text
python acquire-ethoscope-figure01-v1-20261022.py verify --manifest MANIFEST --metadata-dir METADATA_DIR --member ORDINAL --transfer-receipt CLOSED_TRANSFER_RESULT --transfer-sha256 EXACT_SHA256 --case FRESH_VERIFICATION_CASE
```

Python interface:
`verify_one(manifest_path,metadata_dir,ordinal,transfer_receipt,transfer_sha,case)`.
It enters a POSIX600-second alarm, requires a scalar Slurm job environment,
exactly one CPU per task and one task, and verifies real/effective UID where
available. Root must allocate and independently prove **1 CPU /4 GiB** and provide
source/Q/accounting/storage admission. Slurm environment variables alone are not
ownership or resource authorization; receipts explicitly retain that distinction.

It requires the external exact transfer-result hash, verifies every expected
range/proof/path/order/hash and actual saved header bytes, then feeds all payload
chunks through one bounded raw-deflate decoder. The decoder does not restart at
part boundaries. Complete compressed/raw lengths, stream EOF, raw CRC32 and
deposited raw MD5 must all verify. Raw SHA256 is computed. No SQLite import,
schema query, effect, clock interpretation or coordinate export occurs.

The source closure and compressed proofs are checked again; raw candidate hashes
are checked before exclusive hard-link publication. Collision or a two-link crash
ambiguity keeps all evidence. Only the exact owned temporary publication link is
removed after proving its identity with the verified final inode. Final bytes,
compressed proofs and all source hashes are checked again before the exclusive
positive receipt. Re-reading these proofs is included in600 seconds and has not
been throughput-benchmarked on this cohort.

Positive output: `verification-result.json`, status
`provider_verified_member_acquired`, plus a SHA-bound `verified-full-member.json`.
It records the exact native environment, source/member/helper/manifest hashes,
raw checksum proof and final path. Original compressed evidence stays retained.
All scientific flags and whole-part MD5 verification stay false. An incomplete
or negative verification has no positive member receipt and must not be counted.

### Complete cohort assembly

Create an independently source-bound JSON receipt index with
`manifest_sha256` and exactly36 `receipts` entries, each `{path,sha256}`. Then:

```text
python acquire-ethoscope-figure01-v1-20261022.py assemble --manifest MANIFEST --metadata-dir METADATA_DIR --receipt-index RECEIPT_INDEX --index-sha256 EXACT_INDEX_SHA256 --case FRESH_ASSEMBLY_CASE
```

Python interface:
`assemble_cohort(manifest_path,metadata_dir,receipt_index,index_sha,case)`.
It also requires the one-CPU scalar native environment and a600-second POSIX
alarm, as it rehashes all15.5 GB of verified raw DBs. It rejects35/36, duplicate
IDs, cross-manifest/helper proofs, changed DB bytes, missing or changed full
verification proofs, negative case sidecars and unresolved candidate links.

Positive output: `assembly-result.json`, status
`all36_provider_verified_members_present_native_accounting_join_pending`.
It preserves all370-row source metadata and eight other flags, and explicitly
requires root's actual owned native/Q/accounting joins. It does not claim a
completed scientific study, assay qualification, exposure clock or original
H1–H3/D/E2 clearance. No scientific stage is automatically dispatched.

## Exact reservation and bounded production footprint

The unchanged full-cohort payload is7,970,156,585 compressed B +15,499,844,608
raw B +7,128 header B =**23,470,008,321 B**. It requires exactly1,045 requests,
including72 header requests. The maximum member has328,694,564 compressed B and
654,524,416 raw B; no member requires more than42 total requests. No full2 GB
provider part is downloaded, and no whole-part MD5 is claimed from subranges.

The new code keeps three artifacts per request. Exact conservative ceilings:

- 3×1,045=3,135 range files. Their body bytes are already in the source floor;
  a failed request can add at most one excess evidence byte (at most36 across
  one attempt per member).
- Intents are at most4,096 B/request, error responses at most262,144 B/request:
  **278,220,800 B** total, including all response/error evidence.
- At most4 fixed transfer JSON files and5 fixed verification JSON files/member,
  each capped at2,000,000 B: **648,000,000 B**. This deliberately includes partial
  result plus failure sidecars, even though failed transfers cannot be verified.
- At most3 assembly JSON files plus one2 MB index: **8,000,000 B**.
- One complete10-file source/fixture-input copy: **2,593,122 B**. Additional root
  envelopes/logs/copies must fit their explicit allowance below.

The metadata/receipt ceiling is therefore936,813,922 B. Adding source bytes,
36 overflow bytes, and an extra simultaneous maximum compressed+raw member
allowance983,218,980 B yields **25,390,041,259 B**. The code does not create a
second raw copy for promotion: candidate/final are the same inode. The extra
member allowance is a conservative staging margin, not a measured duplicate.
With at most500 MB of additional production root envelopes/logs/copies the total
is**25,890,041,259 B**, within the provisional**26,000,000,000 B** reservation.
Larger/unbounded wrapper outputs require a revised reservation.

Conservative production path count:3,135 range files +36×(4 transfer files +
7 verification paths, counting candidate/final separately for a preserved crash)
+3 assembly +1 index +10 source inputs =**3,545 paths**. The5,000-file increment
leaves1,455 for bounded root envelopes/logs. Quota inodes and owned regular-path
census are different when a failed promotion leaves two links; both links are
preserved and receive no cleanup credit. No production data/evidence deletion
is authorized. The independent qualification footprint must be in the fresh
baseline or reserved separately as3,000 files/256 MB; overlapping increments
would therefore be**8,000 files /26.256 GB**, not5,000 files alone.

These are conservative source bounds, not a fresh quota observation, accepted
cost forecast or measured throughput. Every new repair attempt is an additive
fresh case requiring its own admitted peak. Source/Q/owned native evidence,
current personal/storage/accounting admission and any dispatch remain root's work.
Sequential processing limits are36×900 s of transfer +36×600 s of verification
+600 s of assembly =54,600 s (15 h10 min), excluding qualification, wrapper and
queue time. A single6 h storage/accounting snapshot cannot admit that whole
interval; root must renew admission at bounded phase boundaries. This timing
ceiling is not a measured forecast or an assertion that a job will finish.

## Mock coverage and remaining review

The112 tests cover positive one-part/two-part verification, one-byte split
boundaries, all six real metadata joins and exact1,045-range roster, native
CRC/MD5/length/EOF failures, swapped/duplicated/missing/trailing compressed bytes,
strict HTTP/error handling, actual8 MiB and8 MiB+1 bodies, encoded header caps,
no retry, saved partials, wrong source/receipt/helper/candidate, owner/path/link
guards, exclusive collision and two-link crash preservation, native scalar CPU
checks, monotonic timeout cases and complete/corrupt/partial36-member assembly.
JSON duplicate/nonfinite proofs are rejected. No real network is part of the suite.

On Windows, the hard-alarm test proves non-POSIX refusal. Its Linux branch must
still run under native qualification: an isolated child blocks in sleep and is
interrupted by a one-second POSIX alarm, bounded by the parent's five-second
subprocess timeout. The native adapter's full123-case suite, complete test roster,
Linux fd/path semantics and wrapper integration remain required before payload.

Old canary source remains SHA256
`30a843a2771196a3eec1e202f8e695fd1d41e58b6423a73218ec74b6285ea17c`;
old tests remain `6a7d31cdcc08674aee39af0361cfe234d1eb669548634daf3458c4dd592f4ab3`.
Provenance describes adapted guard/stream/publication logic; it does not claim
the new multipart graph is identical to, or qualified by, that older39-case run.


## Bounded empty-input deflate correction

Independent review demonstrated a valid65,537-zero-byte raw-deflate stream
(79 compressed bytes in the reviewed zlib build) whose first max-length decode
emits65,536 bytes with no unconsumed input, yet retains one output byte internally.
The prior pending-input-only loop incorrectly refused it as incomplete.

The sole new adapter execution change is in `_inflate`: after a nonempty output
with no remaining input and no EOF, call `decompress(b'',limit)` with the same
64 KiB /remaining-raw+1 bound. Continue only while output is produced, or while
nonempty pending input is consumed. An unchanged nonempty tail without output
fails immediately as no progress. Empty input with no output stops draining and
waits for the next compressed block. Final EOF remains mandatory. Raw overrun,
CRC/MD5/size, unused/trailing bytes and the600-second operation budget are unchanged.
No `flush()` or unbounded decoder output call is introduced.

Eleven new tests cover actual65,537/131,073/262,145 zero-byte streams in both
one-part and split cases; an explicit buffered-output model independent of zlib
version; buffered empty-input overrun; unchanged-tail no progress; empty/no-output
without EOF; and high-compression trailing-data refusal. Focused11 passed in
11.13 s; full123 passed in47.71 s, with no failures/errors/skips. The full suite
retains all strict finite-JSON regressions from the previous correction.

Preserved historical candidates:

- `work/acquire-ethoscope-figure01-v1-candidate-02a0cef1-20261022.py.txt`,
  SHA256 `02a0cef10b24e4d78a93d0d9f50de20f757bc92fef20bfb09be0b95637dd2abd`.
- `work/test_acquire_ethoscope_figure01_v1_candidate_3677e061_20261022.py.txt`,
  SHA256 `3677e0615ecde2f5f4dae0ce1724eae4db36203f49353a23c7dafb9bdc0158dc`.
- Earlier6bf/9c47 snapshots and V1/V2 APIs/handoffs remain unchanged. The current
  source/test pins above supersede them for prospective qualification only.

The complete36-source/370-row manifest remains SHA256
`92248b2b213fe7db425ee1fbe9c18c0699c7638943f7edd34bcf2081eeb91490`.
No source-selection, network, path, resource or publication behavior was changed
by the drain revision. Peer review and native qualification remain pending; no
actual network, scientific database, SSH or job operation occurred.
