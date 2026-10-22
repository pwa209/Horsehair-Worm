# Source-specific stock workbook schema audit v2

This private candidate fixes one identified format refusal. Native job 6704402 passed the original 47 synthetic cases, then refused the actual provider-verified workbook because its ZIP contains an opaque 220-byte printerSettings part. The failed native case, original 83ac auditor and ec4e test bytes remain preserved. No report, cell outcome or binary payload was exported by that failure.

Stage exactly these two files together:

- `audit-vtl-stock-workbook-schema-v2-20261022.py`, SHA256 `77af55c3cd2f1ef0b0ca4ed38252e86b39591d4104007812771da994fb92ab4f`.
- `test_audit_vtl_stock_workbook_schema_v2_20261022.py`, SHA256 `b5c33f212d7d256a27418ff19de657346af501bc1a60cfb73889a295dd414d42`.

The actual full test roster is 76 cases, SHA256 `e220c9312f6a795366854116c9b706f8ba8e06e019910469607631ef90f365cb`, calculated from sorted `(classname basename, testcase name)` pairs. Local 76 passed in 8.72s; a fresh two-file isolated bundle passed 76 in 7.71s. Native qualification remains pending and must pass all 76 actual cases with zero failures/errors/skips before the actual workbook phase. The local tests contain only synthetic OOXML/source-receipt fixtures.

Runtime uses the unchanged provider workbook 267375 B, SHA256 `f8fb6db82687d2dad58215df15283a8d8640acec6c722d0966d9f6f5ca183cb8`, provider MD5 `38192352700c09694811a8e8e43e8097`. The official publication metadata 91bde... and acquisition receipt 70da... retain their exact original paths and hashes in the module. No new acquisition or author contact is required.

The only permitted opaque binary is exact `xl/printerSettings/printerSettings1.bin`, exactly 220 declared uncompressed bytes. Its advertised role must have one exact `bin` Default MIME `application/vnd.openxmlformats-officedocument.spreadsheetml.printerSettings` and one exact internal relationship in `xl/worksheets/_rels/sheet1.xml.rels`: Id `rId1`, Type `http://schemas.openxmlformats.org/officeDocument/2006/relationships/printerSettings`, Target `../printerSettings/printerSettings1.bin`, with no extra attributes. Ambiguous or encoded declarations/targets, extra binaries, VBA, unknown binary names or different sizes are refused. The 32 MB total and 8 MB individual expansion bounds are unchanged.

The binary ZIP member is never opened, decompressed, parsed, executed or exported. Only its filename, declared sizes and central-directory CRC metadata are recorded, alongside hashes of the MIME/relationship XML. The CRC field is declared ZIP metadata rather than an independent payload verification. The advertised printer role is not independently established from its payload. Whole workbook MD5/SHA/size are still verified before and after the schema audit. All previous header-only information barriers, unknown body/formula withholding and scientific false flags remain.

The native entry point is unchanged in shape:

```text
python -s <source-stage>/audit-vtl-stock-workbook-schema-v2-20261022.py --output <fresh-result-case>/schema-inventory.json --max-seconds 900
```

It requires 1 CPU, `HHW_GPU_COUNT=0`, and a finite native end clock. The operational launcher must use a new result namespace, retain actual native/source/policy/plan/script checks, qualify 76 tests in allocated-node fixtures first, and place the entire actual-source phase (including initial/final hashing and XML scans) inside a hard 1080-second child limit. No parent-side workbook read may precede that bound. No original E2, control/background, genotype/reagent join, biological independence or original H1–H3 gate is cleared by this schema inventory.
