# Controller binding repair, 7 November 2026

The finite activation job 8634125 stopped before submitting any replacement controller. Its primary descriptor inherited historical parent 6873101, while the separately qualified consumer and its plan builder both require 7046404. The AF descriptor was already consistent.

Activation now constructs both descriptors with that existing, source-pinned builder, preserving the scientific entry, original policy and flags. It also refreshes actual historical parent accounting before dependency resolution. Four regression cases extend the activation suite from 62 to 66 passing local cases; the same suite runs inside the replacement native activation allocation before mutation.

The original helper, tests and stale partial descriptor were preserved under `maintenance/storage-reserve-200k-v3-20261106/binding-repair-20261107/`. No accepted replacement registration existed, and all six old pending controllers were positively held before deployment. A single finite retry, **8709687**, was registered held, positively checked and released. It uses the existing handoff to register four primary and four agent segments, retire only the six old pending controllers, and release the replacements. No persistent supervisor was added. Scientific workers, configuration, storage caps and the 200,000 personal free-file minimum are unchanged.

At 14:05 CET the retry is pending scheduler priority. Replacement startup and scientific resumption have not yet been observed. Verified cleanup removed 43,145 generated sequence copies with original inputs preserved; the first two new censuses completed successfully and the third is running. Canonical results remain revision 699, 126 training runs and 122 policies.

`registered-deployment.sh` is the exact single-use administrative request already sent, and must not be replayed. `initial-receipt.json` records its actual accepted job and initial queue state. `build-deployment.py` reproduces the reviewed deployment from local source; it does not bypass the single-use native registration guards.
