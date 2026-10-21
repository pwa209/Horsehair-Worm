# Exact archive-reader test support

These are frozen source-code fixtures, not scientific data. Production never
loads them by default. Tests copy them into a disposable synthetic personal
scratch root and explicitly bind the copied locators.

* `verified-evidence-archive-20261021.py` is the reviewed private reader/packer,
  SHA256 `045d8650db62c0fa52d7f56bb7bdb2669729509c23ecfbbc847e73cbbff8d42a`.
* `test_verified_evidence_archive_20261021.py.txt` is the exact separate39-case
  test source, SHA256
  `ec4131d283d3f3a792a561c6ae6324de853a4301def9d275a03018a9801f5462`.
  The suffix prevents test collection. Its bytes are required by the reader's
  native-canary source binding; the H4 tests fabricate no real native proof.

The H4 integration test constructs all300 synthetic condition parts and their
300 raw receipts, packs and validates their real deterministic tar/index, and
compares archived rows against the preserved canonical assembled parquet.
Originals removed by that test are disposable test fixtures only. Positive
cluster archive and consumer qualifications remain separate deployment gates.
