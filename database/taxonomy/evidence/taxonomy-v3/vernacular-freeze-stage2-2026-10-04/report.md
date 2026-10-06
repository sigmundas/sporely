# Stage 2 — frozen compiler evidence archive

Implemented the freeze/archive boundary for the accepted Stage 1B candidate. No repository bundle replacement or production publication occurred. Changes remain uncommitted.

`freeze_release.py` prepares the complete publication set from already validated compiler/SQLite artifacts. `promote_desktop_bundle.py:freeze` validates the compiler inventory, source metadata/registry/red-list provenance and a receipt binding the compiler and SQLite hashes. The current recipe asserts 1,619 automatic + 17 reviewed additions (1,636 total, 766 concepts). No identity or vernacular projection is recomputed in this stage.

The frozen set contains deterministic SQLite gzip, full compiler/evidence archive, publication manifest, prepared compatibility bytes and `freeze.json`. The evidence archive includes all 13 compiler files, the accepted Stage 1B verification/recovery/preferred-name/removal audit, reviewed ledger/policies, pinned-source acquisition records, normalization reports/COL rejection evidence and previous publication/compatibility provenance. Every member is inventoried by SHA-256 and size. Raw source archives remain separately pinned; the compiler's per-name provenance remains durable after compact SQLite projection.

`promote_desktop_bundle.py:promote` requires the reviewed `freeze.json` hash. It validates every frozen file, archive member, compiler output binding, receipt, decompressed SQLite hash, registry and publication baseline before any writes. It copies only frozen bytes. It cannot compile, compress, create evidence or refresh metadata. Publication adds `compiler_evidence` with archive filename/hash/size and writes an immutable `<release>.freeze.json`. Earlier evidence archives/descriptors are retained; only the superseded SQLite gzip follows the existing cleanup rule.

`build_release.py` prepares and fingerprints the set after deterministic builds and registry checks, then optional promotion consumes that set. The standalone promotion CLI takes `--frozen-dir` and `--expect-freeze-sha256`. Future source updates must validate current projections and review decisions anew; archived previous evidence is explicit comparison input only, never a source of projected names. No database/cloud schema change is needed for this provenance archive.

Validation: 81 focused freeze/promotion/build/compiler/vernacular/SQLite tests pass. Tests cover archive determinism, raw evidence retention, changed baseline/registry, tampered gzip/archive/descriptor, missing compiler evidence, pinned-count mismatch, failure cleanup and promotion without compression. Syntax and diff checks pass.

Two independent real-data freezes are byte-identical. Archive/file/compiler-member/SQLite hashes verify. Scratch publication copies exact frozen bytes, retains previous evidence and leaves its frozen inputs unchanged; repeat publication is idempotent. Protected repository bundle/compatibility, identity ledgers, bridge policy and accepted SQLite fingerprints are unchanged. Details and exact publication fingerprints are in `verification.json`, `freeze.json` and `publication-manifest.json`.

Frozen accepted set: `/tmp/sporely-vernacular-stage2/finalA` (repeat: `finalB`). Evidence archive is 55,656,914 bytes; SQLite gzip is 73,283,038 bytes. These are local prepared candidates, not published releases. Stage 1B projection remains 96,144 rows with all 10,331 current COL recoveries and the 8,453 unsupported associations withheld; Norwegian decisions and identity contract remain unchanged.

Stop at Stage 2. Stage 3 remains cloud export/search validation and owner-directed publication of the reviewed frozen set. No cloud export/schema work or production operation in this stage.
