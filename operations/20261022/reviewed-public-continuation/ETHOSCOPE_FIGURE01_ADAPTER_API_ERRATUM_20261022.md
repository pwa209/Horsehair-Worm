# Figure01 adapter API documentation erratum

The SHA256 printed for `ethoscope-split-zip-index.json` in the metadata tables of
`ETHOSCOPE_FIGURE01_ADAPTER_API_V2_20261022.md` and
`ETHOSCOPE_FIGURE01_ADAPTER_API_V3_20261022.md` contains a transcription error.
A global documentation replacement of the test count107 with112 also changed
those three characters inside the printed digest. Those historical API files
remain byte-for-byte preserved; this erratum corrects their table only.

The authoritative unchanged split-index binding is:

- Filename: `ethoscope-split-zip-index.json`.
- Bytes:596,027.
- Correct SHA256:
  `ffc58aa960a5fdaec167107c4fece8fd7789fb5daad79675c9aa85980dee46b6`.
- Incorrect documentation-only string:
  `ffc58aa960a5fdaec167112c4fece8fd7789fb5daad79675c9aa85980dee46b6`.

The exact manifest, adapter `SOURCE_PINS`, all handoff JSON source bindings and
the actual metadata file have always used the correct digest. Their source or
metadata bytes were not changed. Current helper SHA256 remains
`0b2571bb650b95ae22f1988caebc8ef5c0371ba6b0ccea5bb8f5a88f332c1cbd`;
tests remain `24215ff1ec91d76e4b41e45f081c3253a9cd4a8f83f1765feaeaae7a16f98938`;
the complete36-source/370-row manifest remains
`92248b2b213fe7db425ee1fbe9c18c0699c7638943f7edd34bcf2081eeb91490`.

Independent local review approved this acquisition-only source after123 mock
tests passed. Native qualification, deployment/activation, actual acquisition
and all scientific eligibility gates remain pending. This erratum grants no
execution or resource authority and involves no network or scheduler action.
