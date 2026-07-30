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

## NorTaxa Stage 2B fixture boundary

Stage 2A is complete: the pinned COL XR archive is promoted under
`sources/col_xr/2026-07-17-XR/archive.zip` with its tracked request, manifest,
checksum, corrections, and validation evidence. Stage 2B does not reopen,
extract, parse, or compile that archive.

Stage 2B is in progress and offline-only. The tracked
`nortaxa-source-selection.proposal.json` proposes Nortaxa (Artsnavnebasen)
version `1.284`, issued `2026-07-17`, as a versioned IPT Darwin Core Archive.
Its archive, EML, resource-page, dataset UUID, published counts, weekly update
frequency, expected CC-BY 4.0 license, allowed host, and 67,108,864-byte ceiling
are unverified network-derived proposal values. The proposal is explicitly
unauthorized. No NorTaxa archive or metadata has been downloaded and no
approved acquisition artifact exists.

The fixture request hash is
`38091edd85d40172539d3086732de2569a00102ff5564c66c55efb59360e7392`;
the canonical source-selection proposal hash is
`e025d53350422d1590836ddc6383f5ed93665ba82ec48db1b3708f2e337a67e3`.
The planned `sources/nortaxa/1.284/` release contains only a proposed
`request.json` and planned `manifest.json`. Raw archive, staging, quarantine,
and extracted bytes are ignored; validation evidence remains trackable.

The `nortaxa_dwca` profile resolves tables, delimiters, quoting, encodings,
line terminators, header counts, IDs, core IDs, and field indexes exclusively from the safe root
`meta.xml`. It keeps the DwC-A core row ID, `dwc:taxonID`,
`dwc:acceptedNameUsageID`, `dwc:parentNameUsageID`, extension `coreid`, and
namespaced scientific-name identifiers such as `NBIC:54995` distinct. None is
converted to an integer or treated as interchangeable. Acquisition preserves
raw bytes, terms, and provenance; final Sporely identity reconciliation is a
later compiler concern.

The profile registry is source-profile metadata, not a complete shared
acquisition framework. NorTaxa currently reuses the established COL canonical
JSON/SHA-256, secret rejection, immutable-release error, atomic JSON writing,
Git provenance, and safe ZIP-member helpers. Its DwC-A streaming and semantic
validation remain source-specific. Tables are iterated from ZIP members in
bounded chunks; complete tables and decoded strings are not materialized.
Declared columns are distinct from required row values: accepted roots and
higher taxa may omit usage, parent, family, genus, and epithet values, while
synonyms require an accepted target and species/genus ranks receive appropriate
semantic checks. Consistent unmapped physical columns are preserved.

Offline commands:

```text
./.venv/bin/python database/taxonomy/scripts/refresh_nortaxa.py validate-request REQUEST.json
./.venv/bin/python database/taxonomy/scripts/refresh_nortaxa.py normalize-request REQUEST.json
./.venv/bin/python database/taxonomy/scripts/refresh_nortaxa.py plan REQUEST.json
./.venv/bin/python database/taxonomy/scripts/refresh_nortaxa.py validate-fixture REQUEST.json FIXTURE.zip
./.venv/bin/python database/taxonomy/scripts/refresh_nortaxa.py status database/taxonomy/sources/nortaxa/1.284
```

There is deliberately no live-download command.

### Metadata-verification attempt 1

A narrowly authorized metadata-only attempt ran on 2026-07-23. It made one
GET to the exact versioned resource page, one GET to the exact versioned EML
endpoint, and one HEAD to the exact versioned archive endpoint. No archive GET,
Range request, retry, authentication, or external-link request occurred.

The attempt failed during offline resource-page parsing because the first
parser version treated a login form embedded in the normal IPT page as proof
that the entire response was a login page. Its orchestration had already
completed all three transports and retained response evidence only in process
memory, so the exception occurred before byte counts, response hashes,
redirects, sanitized fixtures, and archive headers were persisted. Those
values are unavailable and are not inferred.

The append-only failure record is
`sources/nortaxa/1.284/metadata-verification-attempt-1.json`, canonical
SHA-256
`9665bb1ed16958830304e753dfdb73829bc9383b45d67ee9bc4dc332c66e067a`.
The parser now distinguishes an embedded navigation login form from a
standalone login response, and verification now journals each response before
parsing it and stops the sequence immediately on a parsing failure. These
repairs are offline-tested only.

No source value was verified by attempt 1. The proposal remains unverified,
the manifest remains `planned`, and no approval exists. A future metadata
attempt requires separate explicit authorization; attempt 1 must not be
retried under the consumed authorization.

### Metadata-verification attempt 2

A second narrowly authorized metadata-only attempt ran on 2026-07-24 under a
separate authorization bound to the same proposal, canonical request, and
attempt-1 record SHA-256 values. It performed one GET to the versioned
resource page and one GET to the versioned EML endpoint. The versioned
archive HEAD was correctly not attempted because sequencing aborted before
the third operation. No archive GET, Range request, retry, authentication,
or external-link request occurred.

Each response was journaled atomically to the append-only attempt-2 record
before the parser ran. Both GETs returned `HTTP 200` from
`ipt.artsdatabanken.no` with no redirects: the resource page returned 196,861
bytes of `text/html` with body SHA-256
`35501bb5f85f42672357bdf28efcf6f91142245508f3b65a786be776fc4aa067`, and the
EML endpoint returned 5,755 bytes of `text/xml` with body SHA-256
`98dab203fdd38e13b8ec81a0d4d37129a56b90dc99ed69824d18851a53a0e6e9`. Combined
response body volume was 202,616 bytes, well within the 4 MiB metadata
ceiling.

The attempt failed with `AcquisitionError: unsafe EML declaration`. The
bounded pre-parse safety gate refuses `<!DOCTYPE`, `<!ENTITY`, `SYSTEM`, and
`PUBLIC` tokens anywhere in the EML body; the official response begins with
an `<!DOCTYPE eml:eml …>` declaration that is exactly the token the gate
rejects without a separately reviewed XML-safe-declaration policy. No
retry, fallback endpoint, or gate weakening was performed. Archive HEAD
values (Content-Length, ETag, Last-Modified, Accept-Ranges,
Content-Disposition, and content type), EML values (`title`,
`alternateIdentifier`/`packageId`, `pubDate`, `organizationName`,
`intellectualRights`, `edition`), and parsed resource-page values are
unavailable and are not reconstructed.

The append-only attempt-2 record is
`sources/nortaxa/1.284/metadata-verification-attempt-2.json`, canonical
SHA-256
`92ab2958c151eab417da4d29084682293002950bdc5a72b1c0caaf8a48c66ad9`. The
attempt-1 record, `request.json`, `manifest.json`, and
`nortaxa-source-selection.proposal.json` remain byte-identical. The manifest
remains `state: planned`, `approval_status: proposed`,
`download_authorized: false`, with empty `execution_attempts`, `download:
null`, and `validation: null`. No `metadata-verification.json`, sanitized
official fixture, approval, archive, staging, quarantine, or extracted
payload exists.

No source value was verified by attempt 2. The pinned NorTaxa 1.284
selection is not yet eligible for a separately reviewed archive-download
approval; archive acquisition remains unauthorized and Stage 2B is
incomplete. Attempt 2 must not be retried under the consumed authorization.
The next safe offline task is a separately reviewed XML-safe-declaration
policy that permits a plain `<!DOCTYPE eml:eml …>` prolog while continuing
to reject external-entity and external-resource declarations, proven with
offline fixtures, before any future metadata attempt is authorized.

### Metadata-verification attempt 3

A third narrowly authorized metadata-only attempt ran on 2026-07-24 under a
separate authorization bound to the proposal, canonical request, attempt-1,
and attempt-2 record SHA-256 values. It performed one GET to the versioned
resource page, one GET to the versioned EML endpoint, and one HEAD to the
versioned archive endpoint. No archive GET, Range request, retry,
authentication, or external-link request occurred.

Each response was journaled atomically to the append-only attempt-3 record
before the parser ran. Both GETs returned `HTTP 200` from
`ipt.artsdatabanken.no` with no redirects. The resource page returned
198,118 bytes of `text/html` with body SHA-256
`dc67f8470e1603dda06041e8e8e9601561eb7bb7520a0024b6eb7dd7f18d92c5`; the
EML endpoint returned 5,755 bytes of `text/xml` with body SHA-256
`98dab203fdd38e13b8ec81a0d4d37129a56b90dc99ed69824d18851a53a0e6e9` — byte-
identical to the attempt-2 EML body. Combined GET response volume was
203,873 bytes, well within the 4 MiB metadata ceiling.

Resource-page parsing succeeded and confirmed the observed dataset UUID,
title/version/resource-key markers, CC-BY 4.0 license text, weekly update
frequency, and the published VernacularName count. EML parsing succeeded:
the declaration-aware prolog validator observed no XML declaration, no
DOCTYPE, and root element `eml:eml`. Verified EML values include the
proposal title `Nortaxa (Artsnavnebasen)`, package ID
`a6c6cead-b5ce-4a4e-8cf5-1542ba708dec/v1.284`, publication date
`2026-07-17`, publisher `Artsdatabanken`, and CC-BY 4.0 rights text. No
`edition` value was exposed by the EML.

The attempt failed on the archive HEAD gate. The HEAD returned `HTTP 200`
with `content-type: application/zip` and no redirects, but the response
omitted a positive `Content-Length` header. The verifier raised
`AcquisitionError: archive HEAD lacks a positive Content-Length` in the
parse phase of the third operation. Declared archive size, ETag,
Last-Modified, Accept-Ranges, and Content-Disposition values are therefore
unavailable and are not reconstructed.

The append-only attempt-3 record is
`sources/nortaxa/1.284/metadata-verification-attempt-3.json`, canonical
SHA-256
`3c4e4c50815953af2feb1bf0f4f7346bfc0bd217c09b09efb65989b2f03fbcc0`. The
attempt-1 and attempt-2 records, `request.json`, `manifest.json`, and
`nortaxa-source-selection.proposal.json` remain byte-identical. The
manifest remains `state: planned`, `approval_status: proposed`,
`download_authorized: false`, with empty `execution_attempts`,
`download: null`, and `validation: null`. No
`metadata-verification.json`, sanitized official fixture, approval,
archive, staging, quarantine, or extracted payload exists.

Resource-page and EML values are verified. Archive HEAD values remain
unavailable because the third operation failed its transport/response
policy gate before parsing header evidence. The pinned NorTaxa 1.284
selection is not yet eligible for a separately reviewed archive-download
approval; archive acquisition remains unauthorized and Stage 2B is
incomplete. Attempt 3 must not be retried under the consumed
authorization. The next safe offline task is a separately reviewed
Content-Length policy that either accepts a documented HEAD without a
declared length while enforcing a bounded ceiling on the eventual GET, or
that shifts the size-declaration requirement to a bounded pre-GET probe,
proven with offline fixtures, before any future metadata attempt is
authorized.

### Offline repair — revised archive-size declaration policy

The archive HEAD gate in `nortaxa_metadata.py` no longer treats a missing
`Content-Length` header as a metadata-verification failure. The revised
policy is:

- `parse_content_length(header, ceiling)`: header ``None`` → ``None``
  (declared size unavailable). Empty, non-digit, comma-separated conflicting
  values, zero, negative, or over-ceiling values → hard `AcquisitionError`.
  Repeated identical values (`"100, 100"`) are accepted.
- `normalize_head(...)`: records `content_length` (`int` or `None`),
  `content_length_declared` (`bool`), `size_declaration_status`
  (`"declared"` or `"unavailable"`), and `size_declaration_reason`
  (`"header_absent"` or `None`). `ETag`, `Last-Modified`, `Accept-Ranges`,
  and `Content-Disposition` are optional evidence; their absence is not a
  failure. Content-Type still must match the ZIP allowlist (or the
  Content-Disposition must mention `.zip`), status must be 200, and
  redirects must stay within the approved host.
- No Range probe is introduced. A server may ignore ``Range`` and send the
  full body; policy relies on the streamed ceiling and on optional length
  consistency instead.

`evaluate_archive_get(...)` encodes the ceiling-enforced GET policy that
any later separately authorized archive download must satisfy:

- ``ceiling`` is a required positive integer; it is enforced independently
  of the response headers while the caller streams bytes.
- ``completed_bytes`` may never exceed ``ceiling`` (one-byte overflow is a
  hard failure whether or not a Content-Length was declared).
- If Content-Length is present, it must be positive, within the ceiling,
  and equal to ``completed_bytes``; the stream must reach EOF.
- If Content-Length is absent, only a clean EOF within the ceiling is
  accepted.
- Partial-file quarantine, incremental hashing, atomic promotion, and the
  no-extraction boundary are unchanged; the helper does not mutate the
  filesystem.

Under this revised policy the attempt-3 archive HEAD (HTTP 200,
`application/zip`, no redirects, no `Content-Length` header) would be
reclassified from a `parse_failed` gate rejection to a `parse_succeeded`
observation with `size_declaration_status: "unavailable"`. The canonical
attempt-3 evidence file
`sources/nortaxa/1.284/metadata-verification-attempt-3.json`
(SHA-256 `3c4e4c50815953af2feb1bf0f4f7346bfc0bd217c09b09efb65989b2f03fbcc0`)
is not modified: the terminology change is offline-only and applies to
future attempts and to interpretations of the existing record.

Because resource-page and EML values were verified in attempt 3 and the
archive HEAD would now record HTTP 200 with `application/zip`, redirect-free
delivery, and `size_declaration_status: "unavailable"`, the pinned NorTaxa
1.284 selection would, under the revised policy, be eligible for a
separately reviewed archive-acquisition approval. The eventual approval
artifact must bind an explicit compressed-byte ceiling and require the
above ``evaluate_archive_get`` checks at stream completion. No such
approval, and no archive GET, exists.

### Offline repair — declaration-aware EML XML safety policy

The old parser rejected `DOCTYPE`, `ENTITY`, `SYSTEM`, and `PUBLIC` as
plain substrings anywhere in an EML body. That was too coarse: it treated a
harmless `<!DOCTYPE eml:eml>` prolog as unsafe and could theoretically be
tripped by ordinary element text mentioning `SYSTEM` or `PUBLIC`.

The replacement is a bounded, declaration-aware inspection of the XML
prolog. It never resolves, loads, or fetches an external DTD, entity, or
resource. Raw response bytes and their SHA-256 remain untouched — the
policy only inspects a bounded window before the root element.

Accepted grammar (only these shapes are permitted, in this order):

1. an optional UTF-8 BOM;
2. an optional XML declaration `<?xml version="1.0"|"1.1" [encoding="UTF-8"] ?>`
   that must be the very first bytes if present, at most 256 bytes long, and
   contain no other characters;
3. zero or more well-formed XML comments (`<!-- ... -->`, up to 4 KiB each,
   no embedded `--`);
4. an optional single `<!DOCTYPE eml:eml>` with:
   - required whitespace after the keyword,
   - exact root name `eml:eml`,
   - no external identifier (`SYSTEM`, `PUBLIC`),
   - no internal subset (`[`, `]`),
   - at most 256 bytes,
   - only one occurrence, and only before the root element;
5. optional whitespace/comments between declarations;
6. exactly one root element start `<eml:eml …>` within the bounded prolog
   window (default 4 KiB).

Explicitly rejected: `SYSTEM` or `PUBLIC` external identifiers; internal
subsets introduced with `[`; `<!ENTITY`, `<!NOTATION`, `<!ATTLIST`,
`<!ELEMENT`, `<![CDATA[`, `<![INCLUDE[`, `<![IGNORE[`; general or parameter
entity declarations and expansions; multiple DOCTYPE declarations; a DOCTYPE
whose root name is not `eml:eml`; a DOCTYPE after the root element begins;
declarations hidden inside or after comments; processing instructions other
than the XML declaration; unterminated, malformed, or oversized declarations
or comments; UTF-16 / UTF-32 BOMs; NUL bytes in the prolog; XInclude
directives anywhere in the body; and any residual `<!ENTITY` or `<!DOCTYPE`
after the root element (belt-and-braces check).

External-resource resolution is prevented at three layers:

1. the prolog validator refuses every construct that would introduce an
   entity or external reference before parsing begins;
2. `parse_eml` runs `xml.etree.ElementTree.fromstring` — whose expat
   backend does not fetch external DTDs or resolve external general
   entities by default;
3. a post-prolog scan rejects `<!ENTITY`, `<!DOCTYPE`, and `xi:include`
   anywhere in the remainder of the body, so an attacker cannot smuggle
   them inside element content.

The policy has not yet been proven against the official ChecklistBank
response. Attempt 2 discarded that body; its exact DOCTYPE shape is
unavailable and is not fabricated. Only clearly labelled synthetic fixtures
are used for offline proof: `tests/fixtures/nortaxa/synthetic-eml.xml` and
inline byte strings in `tests/test_nortaxa_metadata.py`. Attempts 1 and 2
remain byte-identical; a future metadata attempt 3 still requires separate
explicit authorization.

### Offline repair — per-operation atomic parsed-result journaling

`verify()` now exposes an optional `journal_sink` callback that runs after
each state transition. The verification lifecycle is a three-operation
state machine (`resource_page` → `eml` → `archive_head`), where each
operation transitions through:

1. `pending`;
2. transport request performed and validated;
3. **transport evidence journaled** (method, URLs, redirect chain, status,
   content type, body byte count, body SHA-256, and HEAD headers when
   applicable);
4. parser invoked;
5. **sanitized parsed result journaled** on success, or `parse_failed`
   error record journaled on failure.

The next network operation only begins after the preceding parsed-result
journal transition has been emitted. When an operation fails, later
operations transition to `skipped` with an explicit reason, so absent
values are distinguishable from unavailable values. The verification-state
schema version is `2` and the operation-name tuple
`("resource_page", "eml", "archive_head")` is frozen.

Sanitization is unchanged: publisher emails, contact telephone numbers,
cookies, tokens, and complete official response bodies are not persisted
by any journal transition. Response bodies remain in process memory only
for the duration of a single operation; the journal records their byte
count and SHA-256 only.

A final `metadata-verification.json` may only be emitted when every
operation reaches `parse_succeeded` and cross-source consistency succeeds,
OR when a `parse_failed` operation satisfies the narrow resolved-success
contract enforced by `nortaxa_metadata.verify_closure_conditions`: (1) the
failure is exclusively an explicitly superseded policy decision registered in
`RESOLVABLE_PARSE_FAILURES`; (2) the resolution binds the attempt canonical
SHA-256 and the exact recorded transport evidence; (3) deterministic
re-evaluation under the identified replacement policy returns
`parse_succeeded`; (4) every sibling operation reached `parse_succeeded`;
(5) the new policy requires no evidence the attempt failed to preserve;
(6) the final artifact references the resolution by canonical SHA-256. Any
transport failure, unsafe content type, malformed response, unrelated parser
failure, missing required transport evidence, mismatched attempt hash, or
arbitrary error type is refused. Partial evidence sets a `"final": false`
flag on the state journal and cannot be mistaken for the completed artifact.

`replay_journal_state(state)` reconstructs a read-only summary from a
persisted state dictionary. It is deterministic, network-free, and does
not invoke parsers a second time; it exists so future audits can
distinguish `parse_succeeded`, `transport_failed`, `parse_failed`, and
`skipped` operations without touching the wire.

Attempt records remain append-only; attempts 1 and 2 are byte-identical
under this repair, and archive acquisition remains unauthorized. Stage 2B
remains incomplete.

### Offline closure — policy resolution and insufficient attempt-3 evidence

An offline corrective closure task ran on 2026-07-24 with no DNS, HEAD, GET,
Range, download, or extraction. The `parse_failed` attempt-3 archive-HEAD
outcome was audited against the formal resolved-success contract encoded in
`nortaxa_metadata.verify_closure_conditions`.

`sources/nortaxa/1.284/policy-resolution.json` (policy identifier
`nortaxa-archive-head-content-length-absent-v1`, schema v2) binds the proposal,
request, and attempts 1–3 canonical SHA-256 values; copies the attempt-3
transport observation verbatim (HTTP 200, `application/zip`, empty redirect
chain, zero-byte body, no body SHA-256); enumerates the closure contract's six
conditions; and lists the streaming-ceiling compensating controls that any
future GET must satisfy. It declares that no network request was performed.

The attempt-3 archive-HEAD record preserves only the transport summary
(`status_code`, `content_type`, `redirect_chain`, `body_bytes`, `body_sha256`,
`requested_url`, `final_url`, `method`, `operation`, and the parse-phase
error). It does NOT preserve a raw HEAD headers dict. Presence vs absence of
`Content-Length`, `ETag`, `Last-Modified`, `Accept-Ranges`, and
`Content-Disposition` is not deterministically recoverable — the revised
policy distinguishes an absent Content-Length (accepted as unavailable) from a
malformed/zero/negative/oversized value (hard failure), and attempt 3 lacks the
evidence to make that determination. `attempt_3_closure_result.status` is
`insufficient_transport_evidence` and all five HEAD headers are recorded as
`unavailable_due_to_parse_failure`, NOT `not_exposed`.

Consequently, no `metadata-verification.json` is emitted. The five conditions
that a future closure would need are documented in the artifact's
`closure_contract` block. The attempt-3 record is unmodified.

`nortaxa-acquisition.proposal.json` at the taxonomy-directory root binds the
release, proposal, request, attempts 1–3, and policy-resolution SHA-256 values,
and explicitly declares `unbound_evidence.metadata_verification_sha256: null`
with `prerequisites.metadata_verification_final_artifact_exists: false`. It
remains `approval_status: proposed`, `download_authorized: false`, cannot be
self-authorized, and cannot be created into an approval before a final
`metadata-verification.json` exists. Archive acquisition remains explicitly
unauthorized. Stage 2B remains incomplete pending a separately authorized
bounded HEAD retry that preserves the raw HEAD headers dict.

## NorTaxa Stage 2B archive acquisition

Manual invocation of `scripts/acquire_nortaxa.py --execute` is the acquisition
authorization. Each invocation opens at most one HTTP GET against the pinned
public endpoint `https://ipt.artsdatabanken.no/archive.do?r=artsnavnebase&v=1.284`.
There is no automatic retry, no redirect following, no Range/resume, no
authentication, no cookies, no conditional headers, and no automatic recovery.

The former approval/readiness/proposal framework was retired as
disproportionate for a public pinned archive. Historical `nortaxa-acquisition.proposal.json`
and `sources/nortaxa/1.284/executor-readiness.json` records are preserved
in Git history at commit `2ea3294d4269c043f98374f5bc2ad562971a53aa`. The
consumed Stage 2B attempt-1 approval and journal are preserved byte-identically
under unambiguous names in the release history:

- `sources/nortaxa/1.284/acquisition-attempt-1-approval.json`
  (SHA-256 `6de49d43812cbc6fe87bb89d329e72f3d24379d30f08b71b1c12d353124eb9e1`)
- `sources/nortaxa/1.284/acquisition-attempt-1-journal.json`
  (SHA-256 `026f3f4060d672deb307e17763ba02f41c8c7d3c22cb958709a3f511913b3d7a`)

Both are referenced from `manifest.json` and never used as active authorization.

The runtime executor is small and self-contained (approximately 500 lines):

```text
--execute is required
  -> refuse if archive.zip already exists
  -> exclusive nonblocking release-directory flock
  -> refuse if .staging/ contains any pre-existing payload
     (manual maintainer review required; no auto-recovery)
  -> destination-filesystem free space >= 67,108,864 bytes
  -> exactly one production GET with Accept-Encoding: identity,
     ambient proxies disabled, no redirect following, no auth/cookies
  -> reject non-200, changed final URL, unsupported Content-Type,
     non-identity Content-Encoding
  -> stream to a unique .staging/*.part file with incremental
     byte count + SHA-256 + ceiling enforcement + clean-EOF check +
     Content-Length consistency when exposed
  -> refresh_nortaxa.validate_fixture (non-extracting)
  -> os.link(temp, archive.zip, follow_symlinks=False) — no overwrite
  -> unlink temp, fsync release directory
  -> atomic manifest update: state=validated, download evidence,
     new execution_attempts entry
```

On any handled failure the executor deletes the temporary file, removes an
empty `.staging/` directory, appends a bounded typed failure entry to
`manifest.json`, records the phase and (if known) observed bytes / archive
SHA-256, and returns non-zero. It never promotes on failure and never retries.

A hard crash may leave a `.part` in `.staging/`. The next invocation refuses
and reports the path so a maintainer can inspect and remove it manually. That
refusal is intentional — the executor does not attempt automatic recovery.

The release-lock sentinel `.nortaxa-acquisition.lock` is intentionally
persistent and is ignored by `.gitignore`; it is not evidence and not
authorization.

The archive SHA-256 recorded in the manifest identifies the downloaded bytes.
It does NOT independently prove source authenticity — the NorTaxa endpoint
serves whatever bytes it serves at the time of the GET.

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

The COL `plan` command is a dry run and cannot make a network call. A managed release directory
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
