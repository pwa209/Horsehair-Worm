# Streaming census repair qualification

This source checkpoint is current at 23 October 2026, 10:29 CEST (08:29 UTC).
Narval accepted new qualification **6859130** (request428); it was pending
scheduler priority at that check. Native qualification, a completed census,
resubmission of the five cancelled jobs and controller health are not claimed.

The original census failed its explicit deadline. A bounded private iterator
now consumes completed measurements immediately, allows up to64 I/O workers,
and limits pending work using actual file-descriptor headroom. Original
ownership, identity, race, EOF and measurement validation functions remain
unchanged. The dedicated census requests8CPU/64G/6h with a15-minute
finalization margin. Original quota clocks must remain strictly under6h.
An incomplete scan cannot admit work. Progress is kept outside the scanned
root, then preserved after the measurement context closes, including ordinary
failure. Native hard termination or uninterruptible I/O can prevent finalization.

The repeatable wrapper supports at most64 separately registered, sequential
allocations. The proposed finite reservation is3,072CPU core-hours; actual
DAG concurrency is8CPUs. Scheduled spacing and queue delays can leave
freshness gaps. The separately qualified controller policy waits during those
gaps rather than launching the older two-hour census. Its new controller
contract is1CPU/64G/96h; actual memory fit still requires native observation.

The source manifest binds14 private files and13 test suites. Twelve unchanged
suites passed686 local cases; the final revised controller policy passed71
separate cases. The frozen closure therefore contains757 cases. Native tests
and a real Linux filesystem canary are mandatory before use. Generated test
fixtures are software evidence only.

The first qualification attempt6855974 failed before collection because its
isolated Python path removed the Alliance-provided pygments dependency.
A read-only paired probe reproduced the failure and confirmed the installed
module succeeds in the normal module environment. The new qualifier preserves
those dependency paths with the qualified framework first, strips user pytest
plugins/options, and verifies actual child import paths and source hashes.
The failed attempt and all frozen earlier bytes remain preserved.

The frozen primary scientific configuration, source, prior results and storage
limits remain intact. CPU/GPU total caps use the existing explicit user-authorized
policy. No deletion, compaction credit, metadata recovery or scientific finding
is established by this operational qualification.
