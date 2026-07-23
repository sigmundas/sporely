# Sporely taxonomy contracts

This directory owns the versioned policy inputs for taxonomy schema version 2.
It does not contain acquisition code, source archives, the stable-ID registry,
or compiled taxonomy-v2 databases.

Policy files use JSON-compatible YAML so clean environments can validate them
with Python's standard library. Their meaning is documented in `docs/`, while
architecture rationale lives in `docs/architecture/decisions/`.

Validate offline:

```text
./.venv/bin/python database/taxonomy/validate_policies.py
./.venv/bin/pytest -q database/taxonomy/tests/test_policy_validation.py
```

`TAXONOMY_SCHEMA_VERSION` is `2`. Content releases use
`tax-YYYY.MM.DD-NN`; schema, content, source, and application versions are
independent.

## COL XR acquisition boundary

COL releases must be explicitly pinned because Extended and Base releases are
different products, monthly versions have limited retention, and new releases
must not silently change a reproducible build. Discovery may list available
releases, but selection requires a request file containing a release label,
`Extended Release` type, integer ChecklistBank dataset key, issued date, DOI
when available, archive format, explicit scope, official delivery contract,
compiler-consumption policy, and expected license. There is no implicit or
permanent default key.

The request's canonical SHA-256 covers only deterministic selection fields.
Execution time, local paths, and tool commit are recorded separately. Persisted
requests reject credentials, authorization headers, tokens, and signed URLs.

Local fixture workflow:

```text
./.venv/bin/python database/taxonomy/scripts/refresh_col_xr.py validate-request request.json
./.venv/bin/python database/taxonomy/scripts/refresh_col_xr.py normalize request.json
./.venv/bin/python database/taxonomy/scripts/refresh_col_xr.py plan request.json
./.venv/bin/python database/taxonomy/scripts/refresh_col_xr.py validate-archive request.json fixture.zip
./.venv/bin/python database/taxonomy/scripts/refresh_col_xr.py status database/taxonomy/sources/col_xr/<release>
```

`plan` is a dry run and cannot make a network call. A managed release directory
contains `request.json` and `manifest.json`; future staged bytes remain under
`.staging/` until ZIP and metadata validation succeeds. Manifest states are
`planned`, `downloaded`, `validated`, and `failed`. Only validated bytes are
atomically promoted to `archive.zip`. An identical request is idempotent; a
different request cannot overwrite the same immutable release directory.

Fixture validation currently proves only ZIP integrity, safe member paths,
presence of ColDP `metadata.yaml`, a `NameUsage` or `Taxon` table, checksum and
request/fixture metadata agreement. The fixture metadata parser intentionally
supports a small flat YAML subset. It does not yet prove the exact production
export structure, complete ColDP metadata schema, requested field availability,
record counts, or extraction semantics. DwC-A and TextTree may be selected in a
request but their structural validators are not implemented in this subtask.
No real COL archive was downloaded.

### Metadata-only verification of the proposed 2026-07-17 XR

Stage 2A now has a read-only adapter for public ChecklistBank JSON metadata.
The adapter uses an injected `GET` transport, a 20-second timeout policy,
at most three redirects, official credential-free HTTPS hosts, a 512 KiB
response limit, JSON media-type enforcement, explicit HTML rejection, and an
SHA-256 of the exact response bytes. It compares normalized release label,
release type, dataset key, issued date, and DOI. Tests remain offline.

Official `GET` evidence captured on 2026-07-23 verifies this candidate:

| Field | Verified value |
|---|---|
| Release | `2026-07-17 XR` |
| Type | `Extended Release` (`origin: xrelease`) |
| ChecklistBank dataset | `315834` |
| Issued | `2026-07-17` |
| DOI | `10.48580/dgykv` |
| Exact accepted Fungi root | opaque usage ID `F` |
| Root rank/status/parent | `kingdom` / `accepted` / `CS5HF` (Eukaryota) |

The small `official-*` JSON fixtures in `tests/fixtures/col_xr/` are sanitized
subsets of public official responses. Their sidecars preserve endpoint,
capture time, original response byte count and SHA-256, and the sanitization
description. `synthetic-*` and `valid-request.json` remain explicitly
non-official test data.

The official download page distinguishes two delivery paths:

- Public prebuilt current-release archives use a pinned `GET`. The proposed
  full XR ColDP identity is
  `https://api.checklistbank.org/dataset/315834/export.zip?extended=true&format=ColDP`.
  It requires neither authentication nor export-job creation.
- Custom or partial exports use authenticated ChecklistBank job submission at
  `POST /dataset/{key}/export`. That interface is not part of the proposed
  first full-release acquisition.

The canonical source identity is the pinned dataset endpoint and its exact
`extended=true` and `format=ColDP` parameters. A transient prepared-archive URL
is never substituted for it. HEAD evidence captured without retrieving a body
shows one `302` redirect to the explicitly permitted official host
`download.checklistbank.org`, followed by `200 application/zip`,
`Content-Length: 1383646570`, ETag, Last-Modified, and byte-range support.
Redirects are limited to three; loops and any other host fail closed. Final
media types are limited to the declared ZIP/octet-stream allowlist.

The documentation supports a taxon-root filter but does not define whether a
filtered archive contains ancestor rows, only classification columns, or both.
It also does not document request-level field selection in the current
`ExportRequest`, a rate limit, polling cadence, archive expiry, or retry rules.
Those are hard verification items, not assumptions. `classification: true`
must not be treated as proof that ancestor usages are exported.

For the first compiler input, the proposal therefore recommends a full ColDP
XR archive. This avoids relying on undocumented ancestor-row behavior and
retains evidence needed to audit lineage and mappings. The archive may contain
more fields and entities than the compiler consumes. Acquisition preserves its
complete immutable bytes; compiler-required, audit-only, and ignored entities
are local consumption choices, not server-side export filters or claims that
ignored files are absent.

The official HEAD size supports a conservative proposed ceiling of 1.5 GiB
(`1610612736` bytes). A future approval must state its own maximum no greater
than that ceiling. Preflight must print the expected size (when declared) and
available disk space. Streaming must enforce the approved maximum, compare
received bytes with Content-Length, hash incrementally, write a temporary file,
validate ZIP structure without full extraction, and retain the original
archive bytes plus SHA-256. If HEAD later lacks Content-Length, preflight uses
the approved maximum for its disk-space requirement; it must not probe with an
unrestricted GET.

[`col-xr-source-selection.proposal.json`](col-xr-source-selection.proposal.json)
is deliberately marked `approval_status: proposed` and
`download_authorized: false`. The acquisition request loader rejects it. A
maintainer must separately generate
`col-xr-source-selection.approved.json`, binding the exact proposal hash,
request hash, release identity, canonical endpoint, redirect hosts, approved
maximum, and approval timestamp. No such artifact or real-download CLI exists
yet. The eventual command must require that artifact explicitly and cannot
infer approval interactively. No export job or signed URL was created. A later
authorized GET attempt opened a response but wrote zero bytes, as recorded
below.

### Failed attempt 1 and retry boundary

Stage 2A remains incomplete after two directory-lifecycle defects. The first
run stopped before GET because filesystem planning called `stat()` on a future
nonexistent parent. That was repaired with nearest-existing-ancestor,
symlink/type/escape, device, creation, and post-creation checks. The subsequent
authorized attempt created and validated `.staging`, opened the public GET
response, then incorrectly tried to create `.staging` again. It failed with
`FileExistsError` before reading or writing archive bytes.

The failed manifest preserves attempt 1 append-only: transport opening was
attempted, the response opened, expected bytes were 1,383,646,570, and zero
bytes were written. There is no partial or promoted archive. The original
failure fields remain intact.

The ownership contract is now explicit: the layout layer creates and validates
directories; the streaming layer requires the staging parent to exist, rejects
symlinks/device changes/existing destinations, and exclusively creates the
partial file before opening any response. Response-open and streaming failures
remove zero-length or partial files and preserve cleanup failures separately
from the original error.

Attempt 1 consumed the original one-transfer authorization. A future attempt 2
requires a separate retry-authorization JSON object containing:

- schema/status/authorization timestamp and non-empty reason;
- proposal, request, and original-approval SHA-256 values;
- failed attempt `1`, newly authorized attempt `2`, and maximum GET attempts
  `2`;
- the immutable endpoint and archive-size ceiling;
- acknowledgement that attempt 1 opened a response and wrote zero bytes.

No retry authorization currently exists. Inspect eligibility without mutation
or network access:

```text
./.venv/bin/python database/taxonomy/scripts/acquire_col_xr.py retry-status \
  database/taxonomy/sources/col_xr/2026-07-17-XR
```

All archive hashing uses bounded chunks. Acquisition code must not use
`Path.read_bytes()` or another whole-archive memory read.

### Attempt 2 policy gate and completed-byte quarantine

Authorized attempt 2 transferred exactly 1,383,646,570 bytes with streaming
SHA-256
`397d701c8eb269bf78d6ac7b03149915b0d9e2a2c18694be2c91445b807814f9`.
Structural validation stopped because the central directory contained more
than the configured 20,000-member ceiling. The old failure path deleted the
completed staging archive, so no archive exists locally and no quarantine is
fabricated retrospectively. Attempt 2 is consumed.

Member count alone is not evidence of corruption or a ZIP bomb. Policy version
2 makes 20,000 a warning threshold and 250,000 the hard emergency resource
ceiling. Crossing the warning must not bypass any other structural check. A future
inventory must also evaluate exact member count, central-directory bytes,
compressed/uncompressed totals and ratios, largest members, normalized path
collisions, traversal/absolute paths, symlinks and special files, encryption,
compression methods, filenames, and expected ColDP structure.

Future byte-complete downloads have distinct lifecycle outcomes:
`downloaded`, `quarantined`, `validated`, and `promoted`; interrupted transfers
are `transfer_failed`. Exact-size, fully hashed bytes that later hit a
structural rejection or policy limit move atomically to
`.quarantine/archive.zip`, never active `archive.zip`. Quarantine payload bytes
are Git-ignored while `quarantine.json`, checksums, and validation evidence stay
trackable. Existing or unrecognized quarantine payloads cannot be overwritten
or resumed. Promotion is allowed only after the exact quarantined size/hash
passes the approved validator.

The retained 2026-07-17 COL XR archive remains quarantined after an offline
bounded inspection. Its root `metadata.yaml` is 120,654,270 bytes uncompressed
(5,360,209 bytes deflated; SHA-256
`ae02692eaf1364d2928736435caac655271d28ea50177d57c197d0fd9e137771`),
which exceeds both the general 5 MiB limit and the mandatory maximum 64 MiB
source-specific limit. The archive has exactly 21,100 regular deflated members,
so it exceeds the 20,000 warning threshold but passes the 250,000 emergency
ceiling. No metadata-limit override was adopted and no archive was promoted.

A subsequent explicitly approved offline policy revision raises the exception to
256 MiB only for the exact pinned COL XR proposal, request, endpoint, release,
and inspected metadata hash. PyYAML's event parser validates the complete stream
without constructing a document object, using bounded reads and explicit byte,
depth, node, scalar, collection, anchor, alias, and duration limits. The real
document is a mapping with 21,085 source references; these reconcile exactly
with the numeric `source/*.yaml` members. Those members are delivery provenance,
while `NameUsage.tsv` is the compiler-required table.

The archive nevertheless remains quarantined: `NameUsage.tsv` uses namespaced
headers (`col:ID`, `col:parentID`, `col:status`, and similar), and the current
required-column validator accepts only unprefixed aliases. Namespace-aware
normalization requires a separate reviewed change; arbitrary prefixes must not
be silently stripped.

The pinned ChecklistBank header profile now resolves only exact canonical terms
or exact lowercase `col:` plus an entity-allowlisted term. It preserves original
and normalized tokens, rejects normalization collisions and deceptive prefixes,
and leaves cell values untouched. No archive descriptor declares this namespace;
it is documented only as an observed convention of this pinned export.

After header resolution passed, a complete bounded pass over the 2,929,163,002
uncompressed bytes of `NameUsage.tsv` reached EOF but reported 112 malformed
records under strict Python CSV quoting semantics. Validation stopped without
retrying or changing delimiter behavior, and the archive remains quarantined.

The authoritative ColDP TSV parser now uses physical tabs and record terminators
only; quotes are literal, and recognized backslash escapes are exposed solely in
a separate semantic view while raw values remain unchanged. The completed
NameUsage rescan persisted its evidence before policy evaluation. It accepted
111 former strict-CSV quote failures as valid 73-column TSV records, then
stopped on one genuine error: a UTF-8 BOM outside the first header token at line
1,853,650. The complete report contains 7,871,064 valid rows, zero duplicate
primary IDs, SHA-256
`5b7d7ec383ad69b7dc9c959dadd866a2769ea2433cbcbe1ae30f4b7d9359bdd0`,
and verified EOF/ZIP CRC.

Bounded inspection rejected a proposed fingerprint-specific correction. Line
1,853,650 contains two consecutive `EF BB BF` sequences at record-body byte
offsets 224 and 227. They occur five bytes into the `namePublishedInPage`
field—after `58, f`—rather than at record or field start. Because this is a
mid-scalar double occurrence, no compatibility normalization was added.

That rejection was subsequently superseded by an explicit maintainer decision
and machine-readable correction policy. Correction
`col-xr-2026-07-17-nameusage-5BK77-page-double-bom-v1` removes both U+FEFF
characters only from the semantic `namePublishedInPage` value for the exact
archive/member/row/field fingerprint. Raw archive, row, and field values remain
unchanged and auditable.

The final rescan accepted 7,871,065 NameUsage rows, applied exactly one
correction removing two semantic code points, found zero unapproved BOMs,
malformed rows, duplicate IDs, or self-parent references, and preserved the
member SHA-256. All 21,100 ZIP members passed streaming CRC validation. The
validated archive was atomically promoted to `archive.zip`; its SHA-256 remains
`397d701c8eb269bf78d6ac7b03149915b0d9e2a2c18694be2c91445b807814f9`.

An offline ZIP/ZIP64 central-directory parser now inventories bounded fixture
bytes without reading data members. It detects malformed records, sizes,
methods, flags, path hazards, duplicates, Unicode-normalization collisions,
Unix file types, aggregate ratios, largest members, and file-family groupings.

Generate the proposed remote audit plan offline:

```text
./.venv/bin/python database/taxonomy/scripts/remote_zip_audit.py \
  remote-zip-audit-plan \
  --endpoint 'https://api.checklistbank.org/dataset/315834/export.zip?extended=true&format=ColDP' \
  --archive-length 1383646570 \
  --etag '"5278c56a-65720b858a1cc"' \
  --last-modified 'Tue, 21 Jul 2026 15:31:43 GMT'
```

The plan authorizes nothing. Future execution requires separate approval and
is capped at 64 MiB total: one EOCD suffix response, an optional exact ZIP64
metadata response, and one exact central-directory response. It requires
`206`, valid `Content-Range`, unchanged validators/length, approved HTTPS
hosts/redirects, and aborts before body consumption on `200`. It permits no
data-member ranges, retry, resume, or full archive download.

Official references consulted on 2026-07-23:

- [COL releases](https://www.catalogueoflife.org/building/releases)
- [COL downloads and custom filtering](https://www.catalogueoflife.org/data/download)
- [COL API and ChecklistBank dataset keys](https://www.catalogueoflife.org/tools/api)
- [ColDP 1.2 specification](https://catalogueoflife.github.io/coldp/)
- [ChecklistBank OpenAPI](https://api.checklistbank.org/openapi.json)
