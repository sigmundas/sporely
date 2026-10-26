# Sporely Cloud Sync Contract and Repair Plan

## Shared reference contributions

A personal reference graph remains owner-private and owner-mutable. After an
authenticated sync stores an active observation-reference use whose observation
has an exact stable species identity, the server snapshots that source as an
immutable, attributed shared-contribution revision. Discovery is keyed only by
the exact `sporely_taxon_id`; DOI and other citation metadata are optional and
never identity, deduplication, or merge keys. Other users have read-only access
to the bounded public envelope and may copy it to fresh personal UUIDs.
Public envelopes replace all three owner-private graph UUIDs with stable public
contribution identities. Exact-taxon changes, use reassignment, soft detach,
and hard deletion withdraw an orphaned contribution; a service-owned taxonomy
resolution follows the same rule without requiring an end-user JWT subject.

Plotting a normalized library reference for an active desktop observation is
an explicit scientific use and must idempotently persist the existing frozen
observation-reference association, creating it with role `compared` only when
none exists. Plot visibility is display state and never attaches or detaches.
Desktop exact identity is forwarded only from a stored positive
`sporely_taxon_id` through the guarded selected-taxon RPC; names and citation
text are never taxonomy evidence. Historical contribution backfill is limited
to active owner-matched uses with a valid source graph and an already-stored
species-level exact taxon, and is replay-safe.

The `curated_*` names retained in older storage and client compatibility code
are legacy implementation terminology, not scientific review or approval.
Reviewer, publisher, and attestation policy are not prerequisites for sharing.
Moderation may change public visibility for abuse, privacy, or legal reasons,
but does not select a scientifically preferred contribution. Moderation state
is independent of owner withdrawal, so an owner cannot reshare around a hide.
Account deletion withdraws and anonymizes retained immutable history. Withdrawal
hides catalogue discovery while previously frozen observation and Compare
evidence continues to reproduce its exact revision.

A personal copy of a shared contribution stores frozen provenance without the
contributor: the served envelope minus `contributor` and the live
`relationship_roles`, with `source_sha256` over its canonical JSON
(sporely-web `20261002150000`, owner decision 2026-10-02). Copies made before
still carry the contributor and remain valid; a new copy of the same revision
recognises them. The desktop resolves the contributor for display live from
`get_public_reference_contribution_v2` (cached per revision), showing a
neutral label when unavailable; stop sharing, moderation and account deletion
never invalidate a copy.

Production shared-reference policy uses a one-minute fixed window, allowing a
short burst up to 60 requests per authenticated user or 30 requests per
anonymous trusted-edge IP/session. Unauthenticated requests without the trusted
edge identity share one fail-closed fallback bucket rather than trusting
caller-supplied forwarding/session headers. Throttled responses are HTTP `429` with `Retry-After`;
clients treat them as retryable and preserve local work. Catalogue pages
default to 25 rows and accept at most 100. Scientific contribution revisions
are immutable and retained indefinitely. Withdrawal, source deletion, and
account deletion remove contributions from new discovery/use while retained
history remains available to already-frozen observation and Compare evidence;
account deletion anonymizes that history. Rate-limit/request-abuse metadata is
retained for 30 days and operational policy logs for 90 days through a daily
database job and the service-only retention routine. Abuse, privacy, or legal takedowns may hide a
contribution immediately, with an initial-human-review target of five business
days. These controls do not introduce scientific approval or attestation.

Status: required behavior; Stage 1 landed; Stage 2 pending.

This is the shared sync specification for `sporely` (desktop) and `sporely-web` (web, Android, Supabase, and cloud media). Keep an identical copy at:

- `sporely/docs/supabase-sync-contract.md`
- `sporely-web/docs/supabase-sync-contract.md`

Any change to sync behavior must update both copies in the same work item.

This contract is the behavioral specification. For implementation
navigation — entry points, canonical function ownership, cloud-write and
destructive boundaries, and the staged refactoring plan — see
[`cloud-sync-architecture.md`](cloud-sync-architecture.md) (desktop
repository only).

## Purpose

Sporely is local-first on desktop and cloud-connected across desktop, web, and Android. Sync should copy shared observation and scientific data without silently destroying local files, cloud photos, or work from another device.

A regression introduced on 3 August 2026 connected publication-gallery selection to cloud image deletion. Photos omitted from an upload list could be treated as unwanted cloud images. This document defines the intended behavior and the plan to repair and recover affected data.

## Plain-English vocabulary

Plain English comes first; technical terms are in parentheses.

- **Local record** (`SQLite row`): a desktop database entry.
- **Cloud record** (`Supabase/Postgres row`): a shared database entry.
- **Cloud file** (`R2 object`): uploaded image bytes.
- **Local-to-cloud link** (`cloud_id`): cloud record ID stored locally.
- **Cloud-to-local link** (`desktop_id`): desktop record ID stored in the cloud. Retired as an identity; see "Per-record sync keys (`sync_key`)".
- **Sync key** (`sync_key`): a random per-record uuid minted locally before the first upload; the only recovery identity.
- **Known-good baseline** (`sync snapshot`): the last state both sides agreed on.
- **Removal marker** (`tombstone`): durable evidence of an explicit user deletion.
- **Recoverable deletion** (`soft delete`): keep the record but mark it deleted, usually with `deleted_at`.
- **Permanent deletion** (`hard delete`): remove the database record.
- **Measurement-only image record** (`metadata-only anchor`): an image row retained for measurement links without cloud image bytes.
- **Retry-safe operation** (`idempotent operation`): repeating it produces the same correct result without duplicates or extra deletion.
- **Three-way comparison** (`three-way merge`): compare local state, cloud state, and the known-good baseline.
- **Cloud file address** (`storage_path`): the key pointing to a cloud file.
- **Prepared upload list** (`prepared_items`): images whose bytes were prepared for this sync.
- **Protected cloud list** (`kept_cloud_ids`): existing cloud image rows that must remain.
- **Cloud-storage-desired excluded set** (`sporely_cloud_image_storage_excluded_ids_<obs>`): the local image ids for one observation whose bytes are *not* desired in Sporely Cloud image storage. Everything else is desired.

## Non-negotiable safety rules

1. **The gallery "Keep image in Sporely Cloud" checkbox is desired Sporely Cloud image-storage state.**
   - Checked = a cloud image copy is desired.
   - Unchecked, local-only image = do not upload bytes.
   - Unchecking an already-uploaded image = explicit cloud-copy deletion intent, which queues the tombstone/delete-pending lifecycle.
   - Rechecking before the tombstone syncs cancels the pending deletion and preserves the existing cloud copy.
2. **Internal omission is never deletion intent.** Filtering, failed preparation, missing files, metadata-only sync passes, `prepare_images_cb`=None fallbacks, `include_image_ids` subsets, or any other implementation detail must never independently create deletion intent. Deletion only comes from an explicit uncheck or an explicit context-menu removal.
3. **Deletion requires an explicit transition.** Store checkbox uncheck or "Delete cloud copy" intent as a removal marker (`tombstone`) before sync performs the remote deletion.
4. **Image bytes and metadata-only microscope anchors are different things.** Unchecking a measured microscope image removes its cloud image bytes but may still retain a metadata-only microscope anchor so spore measurements and the public spore mosaic keep working. Never confuse the two: the byte-storage predicate governs bytes only.
5. **Normal sync must not permanently delete image rows.** Use recoverable deletion (`soft delete`) first. Permanent deletion belongs to verified maintenance, account deletion, or retention cleanup.
6. **Store deletion intent before removing bytes.** Confirm the database deletion state first, then clean up cloud files as a retry-safe follow-up.
7. **Local originals remain authoritative.** A smaller cloud copy never overwrites a better local original. Downloaded files are recovery copies (`cloud_recovery_cache`) until explicitly promoted.
8. **Failures remain retryable.** Do not mark an observation fully synced if required image, measurement, calibration, summary, or deletion work failed.
9. **Conflicts block automatic writes.** When local and cloud both changed since the known-good baseline, stop and ask for a choice.
10. **Stable IDs define identity.** Match by IDs, not dates, filenames, file paths, species names, or sort order.
11. **All clients share the same state meanings.** Desktop, web, Android, cloud functions, and media workers must distinguish active, measurement-only, broken, and deleted images consistently.
12. **Identity repair is checkbox-independent.** A lost `cloud_id` is reconciled by `sync_key`; rows that predate keys use the one-time legacy recovery, which never writes to the matched row. Reconciliation happens regardless of the current gallery checkbox. Repair restores identity; it does not upload bytes. Recovery of identity for an unchecked image lets the byte gate correctly refuse re-uploads and lets a pending tombstone complete instead of creating a duplicate.
13. **The legacy `artsobs_publish_excluded_image_ids_<obs>` setting is publication-only.** Its values must never automatically tombstone or otherwise remove cloud media. They may surface as audit hints in a later stage but never independently drive cloud-side change.
14. **A partial remote collection is never authoritative remote state.** PostgREST (and any bounded API) can silently cap a response at the server row limit while returning HTTP 200. A truncated collection must never be used to conclude that rows are absent, to compute deletions, or to drive any diff against local state.
15. **Bounded or paginated remote reads must be exhausted successfully before absence may be interpreted as deletion.** If any page fails, the whole read fails — the client must raise, not return the pages fetched so far. Absence of a row is meaningful only after a complete, successful paginated read of the relevant scope.
16. **Incomplete remote data must never be stored as a sync snapshot.** The known-good baseline may only be persisted from a complete, successful remote read (and after all required child work succeeds). A snapshot recorded from truncated data poisons every future three-way comparison for that observation.
17. **PostgREST pagination must use deterministic ordering.** Every paginated bulk read must include an explicit `order=` clause with a unique tie-breaker (`id.asc`); offset-based paging over an unstable order can silently skip or duplicate rows across pages.
18. **Download from Cloud is a strict zero-cloud-write mode.** A pull-only run must complete with `cloud_writes_completed == 0` and an empty blocked-write list. Every write path — PATCH, POST, DELETE, RPC mutations, storage upload, storage removal, and identity write-backs — is out of scope for this mode.
19. **Pull-only write blocking is defense in depth, not expected control flow.** The fail-closed pull-only client wrapper exists to catch mistakes, not to be exercised. Pull-side code must gate its own writes at the source; any blocked write attempt recorded during a Download-from-Cloud run is a defect to fix at the source, not a handled event. New client writer methods must be added to the pull-only blocklist; new read methods join the allowlist only as an explicit, reviewed choice.
20. **The local `cloud_id` is the primary direct local→cloud identity.** Once it verifies against an existing same-owner cloud row, that row is the push target. `sync_key` is the only recovery identity. Integer local ids are never cloud identities.
21. **No client writes an identity onto an existing cloud row.** Pull links by cloud id or `sync_key`. (Replaces the former rule that a missing remote `desktop_id` is healed by a later sync.)
22. **Direct and reverse identity links resolving to different objects is an identity conflict, not permission to create a third object.** The same applies to an ambiguous reverse-link match (multiple rows carrying one `desktop_id`). The push must fail safely — no update of either candidate, no creation, no snapshot — and the local observation stays dirty/retryable for review.
23. **POST only for a row with a key and no `cloud_id`.** A 23505 on the table's `*_user_sync_key_uidx` means adopt: parent check, then the fetched row as base, then a conflict on any difference. A tombstone goes to the deletion flow. Any other 409 or 4xx is an error. "Lookup failed" never automatically means "create" while the local row carries a direct cloud identity.
24. **No no-op cloud writes on sync paths.** `observation_images.updated_at` is trigger-bumped on EVERY update, for every role, regardless of whether values changed — so a value-identical PATCH is a real child-change cursor event, not a harmless idempotent write. Before any cloud write in a sync path, check whether the target value already matches and skip the request if so. (Live incident 2026-08-24: unconditional `desktop_id` relink PATCHes during pull rewrote ~2 500 image rows per sync and created a self-sustaining full child re-pull echo loop.)
25. **The child-change cursor must commit the true `MAX(updated_at, id)` tuple over every inspected row, with numeric id ordering.** Row ids compare numerically, never as strings (`'10000' > '9999'`), and the strict filter and the advancement comparison must use identical ordering. The cursor advances only after `pull_all` succeeds; a failed pull leaves the cursor untouched so the changes are re-detected next sync.
26. **Superseded: imports and other forks mint fresh keys.** Portable import, legacy bundle import and image move give every created row a fresh `sync_key`, so the imported graph becomes new cloud rows and never adopts the source's. The former rule follows for the record only: while an observation has `portable_cloud_identity_pending=1`, push and pull reconciliation for the observation, its images (including metadata anchors), and measurements must ignore remote `desktop_id` recovery and must not write a `desktop_id`. Verified destination-side `cloud_id` links remain authoritative. The marker clears only after the complete graph has fresh cloud IDs and collision checks have safely established reverse IDs from the new destination integer IDs; any collision leaves the guard active.
27. **Observation reference uses preserve frozen evidence.** Sync must transmit and
    reconcile the stored use UUID, role, note, selected revision, and
    `snapshot_json` verbatim. It must never rebuild a snapshot from the current
    library during transport. A live use waits for a verified observation cloud
    ID and an acknowledged, converged measurement set. Deletes use durable local
    intent and positive CAS tokens; a confirmed parent-observation delete is an
    authoritative terminal acknowledgement for its child-use tombstones.

28. **A Sporely taxon identity may be asserted only with recorded proof of its
    producer.** `set_observation_selected_taxon_v2` accepts a Sporely-owned
    `sporely_taxon_id`; a positive integer is not evidence that a value IS one.
    Every client must carry the identity as
    `(state, proof, source_system, namespace, external_id, raw_external_id)`
    alongside the integer, and may emit the integer only when the proof is
    either "the value came from a compiled taxonomy-v2 artifact whose identity
    contract states its `taxon_id` is the Sporely ID" or "a namespaced external
    identifier resolved through an authoritative mapping to exactly one
    concept". A pre-provenance legacy integer is *unverified*, not proven, and
    is withheld until re-verified against the taxonomy artifact.

    The deployed RPC does validate active-release membership, so an arbitrary
    external integer is rejected server-side — but it cannot distinguish an
    external integer that numerically COLLIDES with a real Sporely ID in the
    active release, and no server-side check ever could. That residual case is
    why the proof standard is client-side.

    Refusing to emit is a skip, never a clear: an unproven or unresolved local
    identity is not evidence that the cloud's identity is wrong. Both the source
    evidence and any existing cloud selection survive. Equally, a failure to
    resolve an external identifier must not destroy the source prediction — an
    unresolved external identity is a STATE (`external_unresolved`) that is
    persisted with the observation, is still displayed by its name, and must
    never render as unidentified. "Unidentified" is reserved for an observation
    that carries no name at all. Identity-provenance columns are never pushed as
    ordinary observation fields; the guarded RPC stays the only identity channel.

    Namespace-lost integers are legacy/audit evidence only. Stripping a
    provider prefix (`NBIC:53482`) does not convert a scientific-name id into a
    taxon id: the raw provider value must be retained, and any namespace hop
    must be a bridge declared in `database/taxonomy/docs/identity-contract.md`.

    **A proven concept the cloud release lacks is preserved, not failed.** A
    client's installed release can hold concepts the scoped cloud release does
    not (every NorTaxa-only concept). When `set_observation_selected_taxon_v2`
    refuses a proven identity with `22023` "missing from the active
    taxonomy-v2 release", the client must not fail the observation's push.
    If the identity carries a non-Sporely source tuple, the client preserves
    it through `set_observation_identification_v2` as `external_unresolved`
    with no `selected_sporely_taxon_id` and `p_write_name = false`, unless the
    cloud row already holds that tuple. Otherwise the identity stays local.
    The local proven identity is kept either way. Any other refusal still
    fails the push. A later push of the row retries the guarded RPC, so the
    concept binds once the cloud release contains it; the refusal itself
    writes nothing.

    **A cloud identity received on pull is kept, never proven.** The server
    guarantees only that `selected_sporely_taxon_id` was in the active release
    when it was written; it records no producer and no release, and
    pre-Stage-2 desktops asserted unproven integers through the same RPC. A
    desktop therefore stores a pulled Sporely ID as `sporely_v2` with proof
    `cloud_selected_unverified` — only when its installed taxonomy artifact
    contains the concept, with the artifact's canonical name/rank as the
    snapshot and provenance naming the cloud and the local release. That
    proof is outside the proven set: the value is displayed, restored and
    preserved across saves, but never re-asserted through the RPC and never
    used by identity-gated lookups (Red List, reference attachment). An
    explicit picker selection replaces it with `taxonomy_v2_artifact`.

    **The persistence API accepts one complete typed identity transition.**
    The identity spans several columns but is a single value, and treating
    those columns as independently writable produces rows that contradict
    themselves: replacing a proven integer while supplying only one provenance
    field leaves the old proof attached to the new integer, and switching the
    state without naming the integer strands the old integer beside the new
    state. So a write that touches ANY identity field rewrites ALL of them —
    the supplied fields are the complete new identity and everything else is
    cleared. Nothing is inherited from the previous row, because inheritance is
    what makes a partial write look proven. The result is normalized through
    the same typed value readers use, so a write cannot produce a state a
    reader would reject.

    **An unqualified integer is refused at the write boundary, not merely
    gated downstream.** Storing a bare integer and relying on every consumer to
    check provenance requires every consumer to be correct forever; three
    separate consumers (cloud sync, normalized reference attachment, the Red
    List lookup) each had to be fixed for exactly this reason. A bare integer
    with no accompanying provenance is therefore dropped. Rows written before
    this rule keep their integers, are read as legacy-unverified, and are
    re-verified against the taxonomy artifact by the backfill — which is the
    only supported way an existing integer becomes proven.

    **The guarded RPC owns the id/state transition.** `taxon_identity_state`
    and `selected_sporely_taxon_id` must move in ONE statement, because the
    database constraint forbidding a bound id under `external_unresolved` is
    evaluated per statement. Splitting the write between client and RPC
    deadlocks every later binding attempt on an already-unresolved row —
    resolution, native-picker selection, and desktop-originated RPC writes
    alike. `set_observation_selected_taxon_v2` therefore sets the state
    alongside the id and leaves the preserved
    `(source_system, namespace, external_id, raw_external_id)` evidence intact,
    so a resolution stays auditable.

    **Selecting a new candidate is a change of identification.** When a client
    preserves an external identifier that does not resolve, it must also clear
    any previously bound concept — otherwise the row's name says one thing and
    its identity says another. Clearing first also keeps the constraint
    satisfied at every step. Manual free text remains the explicit way to clear
    an identification outright.

    **The identification is ONE write, and `set_observation_identification_v2`
    is its authoritative writer.** The bound concept, the identity provenance
    and the accepted `genus`/`species`/`common_name` are a single coupled
    change. Writing them in separate statements leaves a boundary at every join
    where the row can describe one taxon by name and another by identity, and
    no client ordering closes it: identity-first leaves a bound concept beside
    the old name when the name write fails, name-first leaves the new name
    beside the old concept when the guarded RPC rejects, and compensation is
    itself a write that can fail — narrowing the split rather than removing it,
    and potentially leaving it unreported. The RPC therefore writes all of them
    in one statement. `ai_selected_*` is provider history, is explicitly not an
    accepted identification, and stays outside the coupled set.

    **Deployment prerequisite, and fail-closed until it is met.** That function
    ships in its own migration, separate from the `taxon_identity_*` columns.
    A backend can have the columns without the function, and PostgREST's schema
    cache can lag a deployed function, so a client must infer NOTHING about
    column availability from the function's absence. When the function is
    unavailable a client must refuse the coupled change outright and leave the
    observation unmodified — it must not fall back to the narrower
    selected-taxon RPC, must not write the accepted name independently, and
    must not persist a reduced identity-only shape. Refusing is the only
    outcome that cannot corrupt the row.

    **A desktop's own abandoned identity is an explicit clear, never a
    silent skip, when the desktop can prove it deliberately changed the
    identification.** "No local identity" is not by itself an instruction to
    clear the cloud: legacy rows, unresolved identities, incomplete local
    state and older-client data all legitimately read as
    `no_identity_evidence` without the cloud ever being wrong. The push gate
    (`_sync_observation_selected_taxon` / `_maybe_clear_stale_cloud_identity`
    in `utils/cloud_sync.py`) issues an explicit clear through
    `set_observation_identification_v2` (never a bare PATCH of taxonomy
    columns) only when ALL of: (1) the current local committed identity is
    `no_identity_evidence`; (2) the stored sync baseline records that this
    same observation last synced with a non-empty *selected Sporely* identity
    (a baseline external/legacy value is not this desktop's RPC-asserted
    claim, so it is not this desktop's claim to withdraw); (3) the committed
    identification (`genus`/`species`) differs from that same baseline; (4)
    the cloud does not already agree (nothing to clear twice). Manual
    free-text renames and genus/species dropdown changes that invalidate a
    previously proven identity satisfy this; an unrelated field edit, a
    picker re-selection to a new proven concept, a legacy row with no prior
    baseline proof, and a preserved `external_unresolved` identity do not —
    the first two never change the committed identification key against the
    baseline, and the last two never establish condition (2). With no stored
    baseline at all, nothing is inferred and the identity is left alone (the
    existing fail-closed rule for an unknown baseline).

    **Condition (4) also fails closed the other way: a clear is withheld,
    never silently issued, when the cloud's current identity is a THIRD value
    the desktop never agreed to.** The clear may proceed only when the
    cloud's current selected identity is empty or still equals the baseline;
    a value that moved to something else since (another client, the web)
    means this desktop's baseline is stale, not that the newer cloud value is
    wrong. `_maybe_clear_stale_cloud_identity` logs and withholds in that
    case. In the ordinary desktop flow this is already unreachable — a
    genuine local identification change together with an independent remote
    identity change is a three-way `conflict` that blocks the whole
    observation push before this point — but the check is independent of
    that upstream block, not a substitute for it.

    **A non-claiming local row whose plain names already contradict a cloud
    row's bound identity, with no usable baseline at all, is a THIRD case
    ("Case F") distinct from both the explicit clear above and the
    proven-vs-proven no-baseline disagreement.** "No usable baseline" means
    no stored snapshot, or one that predates identity joining change
    detection. Unlike a proven/claimed local identity disagreeing with the
    cloud (which already fails closed via
    `_classify_identity_sync_change`'s `conflict` classification, with
    ordinary fields still permitted to push per the accepted partial-push
    policy — see `tests/test_cloud_identity_fail_closed.py::
    test_no_baseline_disagreement_stays_under_review_and_blocks_the_next_push`),
    a row with NO identity claim at all was previously adopted-by-default:
    nothing stopped its own genus/species from silently overwriting the
    cloud's committed name while the cloud's bound identity stayed attached,
    and the following pull silently bound that identity onto the desktop's
    unrelated names in turn. Both directions now fail closed together,
    through the same conflict-review mechanism (never a second one): when the
    cloud holds a non-empty identity, the local row makes no claim to any
    identity, and the committed identification differs between the two sides,
    the WHOLE observation is blocked — push before any cloud mutation,
    pull-apply before any local mutation — kept dirty, and surfaced as
    "needs review", exactly like other conflicts. Because the contradiction
    IS the identification itself, this is stricter than the claim-vs-claim
    case: no field on the observation partially goes through. It clears only
    through an explicit picker resolution (to the cloud's own concept, which
    matches and proceeds normally, or to a different proven concept, which
    then falls into the ordinary claim-vs-claim no-baseline review policy);
    a further manual free-text rename to yet another name does not clear it,
    since the explicit-clear condition above requires a known baseline that
    Case F lacks by definition.
29. **Children follow their parent's committed cloud id.** Children are
    uploaded only after their parent's `cloud_id` is committed locally. Media
    object keys are random reserved keys, reused on adoption, never derived
    from `sync_key`.
30. **Desktop write stop.** The desktop sends `x-sporely-sync-identity: 2` and
    `x-sporely-client-version`. A version listed in the server's write-block
    table is refused with `sporely_upgrade_required`.

### Temporary obsolete-desktop brake

<!-- obsolete-desktop-brake:begin -->
Every desktop released before per-record sync keys resolves its cloud writes
through the shared `desktop_id` reverse link, so two installations on one
account can silently overwrite each other's cloud observations. Until a
desktop that uses sync keys ships, the server refuses these desktops' cloud
writes.

- Who is refused: a request whose lower-cased `User-Agent` starts with
  `python-requests/` (the default every released desktop sends) and that has
  no `x-sporely-sync-identity: 2` header. In the database the request must
  also come through the Data API with an `authenticated` or `anon` JWT; the
  media worker's upload routes accept only `authenticated` JWTs. Web,
  Android, `service_role` and other server roles, migrations, cron and direct
  SQL sessions are not refused.
- Database: every INSERT, UPDATE and DELETE, including writes made inside
  RPCs, on `observations`, `observation_images`, `spore_measurements`,
  `spore_measurement_mosaics`, `spore_measurement_mosaic_tiles`,
  `observation_spore_summaries` and `observation_reference_uses` is refused
  with SQLSTATE `P0001` and HINT `sporely_upgrade_required`, which PostgREST
  returns as HTTP 400. Trigger: `trg_000_sync_brake`, function
  `private.sync_brake_guard()`, migration `20261015090000`.
- Media worker: every non-read method (PUT, DELETE and any other method
  except GET, HEAD and OPTIONS) under `/upload/` is refused with HTTP 426,
  header `X-Sporely-Error-Code: sporely_upgrade_required` and body
  `{"error":"sporely_upgrade_required", ...}`, before authentication, storage
  access or quota accounting. Its `/healthz` reports
  `obsoleteDesktopBrake: true` while the guard is deployed.
- Effect: reads are unaffected; refused desktops can still sign in, download
  and pull. A refused write changes nothing in the cloud. The desktop keeps
  the row local (new) or dirty (edited), keeps its photos and credentials,
  and uploads the pending work once it is updated to a desktop that is not
  refused.
- Removal: only by the sync-key cutover migration, in the same transaction
  that installs the permanent identity gate, with the worker guard removed in
  the matching worker release. Never as a rollback: removing either half
  early re-enables silent overwrites or media deletion ahead of a refused row
  delete.
- Not an authorization boundary: any caller can choose its User-Agent and
  headers. The brake holds back honest old clients; row-level security and
  the existing guards remain the authorization layer.
<!-- obsolete-desktop-brake:end -->

## Per-record sync keys (`sync_key`)

`sync_key` is a random uuid minted locally before the first upload. It is
immutable for every role, unique per owner, and recovers only the creating
database's own rows. `desktop_id` is retired as an identity: clients must not
send it, and the server refuses it on rows that have a key.

This section is the identity contract for `observations`,
`observation_images` and `spore_measurements`. It supersedes every
`desktop_id`-based identity rule elsewhere in this document: the reverse-link
lookups in rules 21–23, "First upload" finding a row by `desktop_id`, and the
`desktop_id` match in "Lost local `cloud_id` — identity repair". Rules 12, 20,
21, 23, 26, 29 and 30 above state its summary.

### Server

Migration `20261013210000_sync_key_identity.sql`:

- `sync_key uuid NULL` on the three tables, with no backfill. Rows created
  before sync keys keep NULL and can never be given a key.
- Per-owner partial unique indexes `observations_user_sync_key_uidx`,
  `observation_images_user_sync_key_uidx` and
  `spore_measurements_user_sync_key_uidx` on `(user_id, sync_key) WHERE
  sync_key IS NOT NULL`. Another owner may use the same key value; there is no
  cross-tenant 409.
- Guard `public._sync_identity_guard()` (BEFORE INSERT OR UPDATE, sorting last
  among BEFORE triggers):
  - all roles: `sync_key` never changes on UPDATE, in any direction;
  - every caller except `postgres`, `service_role` and `supabase_admin`: a row
    carrying both `sync_key` and `desktop_id` is refused on INSERT and UPDATE.
- `sync_key` never appears in non-owner views or public RPC results.

### Refusals

| Answer | Meaning | Client action |
| --- | --- | --- |
| `P0001`, hint `sporely_sync_identity_refused` | the write would change a key or put `desktop_id` on a keyed row | stop the upload phase (see the state machine) |
| `P0001`, hint `sporely_upgrade_required` | this client version is blocked from writing rows | stop the upload phase (see the state machine) |
| media worker HTTP `426` with `X-Sporely-Error-Code: sporely_upgrade_required` | this client version is blocked from media writes | stop the upload phase (see the state machine) |

Refusals are always `P0001` (HTTP 400), never `42501`/403: released desktops
v0.8.13 and v0.9.1 treat 403 as a sign-in failure. Refusal text is fixed. It
never contains row values, any auth word (`unauthorized`, `not logged in`,
`jwt`, `token`, `session expired`, `authentication failed`, `invalid_grant`,
`pgrst301`/`303`, `captcha_failed`) or any transient word (`timeout`,
`unavailable`, `schema cache`).

### Desktop write stop

The desktop sends `x-sporely-sync-identity: 2` and
`x-sporely-client-version: <app version>` on every Data API and media-worker
request. Migration `20261014120000_sync_client_write_block.sql` adds the
owner-only table `private.sync_client_write_block(client_version, reason,
created_at)`, created empty, and a BEFORE INSERT/UPDATE/DELETE row trigger on
`observations`, `observation_images`, `spore_measurements`,
`spore_measurement_mosaics`, `spore_measurement_mosaic_tiles`,
`observation_spore_summaries` and `observation_reference_uses`.

- When `x-sporely-sync-identity` is present and `x-sporely-client-version` is
  listed in the table, the write is refused with `P0001`, hint
  `sporely_upgrade_required`, and the message "This version of Sporely Desktop
  is blocked from uploading. Install the latest version from sporely.no.
  Nothing was changed in the cloud."
- The check keys only on the request headers, never on the database role.
  Requests without the identity header are never affected: web, Android,
  released desktops, Edge Functions, migrations and plain SQL sessions.
- With an empty table nothing is blocked. Writes made inside RPCs are covered
  too, because the request's headers stay visible inside the function.
- Limit (owner-accepted 2026-10-17; Codex security review of `29b22d3`:
  CHANGES REQUESTED): media-worker requests are **not** blocked by this table.
  A blocked build can still upload **and delete** media objects. Uploads cost
  orphan bytes at worst, because rows are written first. Deletes are worse:
  the desktop's observation delete removes the media first and then the rows,
  so a blocked build can delete the media, have its row deletion refused, and
  leave live cloud rows that reference missing media (broken images). The
  write stop is therefore **not** a safe complete stop on its own. Before a
  version is listed, every installation running it must be stopped or
  disconnected (runbook, "Emergency: activating the desktop write stop"). If
  that cannot be ensured, worker-side enforcement of the same block is
  required first; it is a planned later improvement, not part of this
  release.

Web and Android never send `desktop_id`, and never send the desktop identity
or version headers.

### Per-row state machine

For observations, images and measurements. A child waits for its parent's
committed `cloud_id`.

| State | Event | Action | Next |
| --- | --- | --- | --- |
| L0 (key, no `cloud_id`) | parent has no `cloud_id` | defer | L0 |
| L0 | POST with `sync_key` → 201 | commit `cloud_id` | L2 |
| L0 | `23505` naming the table's `*_user_sync_key_uidx` | GET by key and owner; parent check; tombstone → existing deletion/restore flow; content equal → link silently; content differs → sync conflict, never PATCH | L2 or conflict |
| L0 | `23505` on any other constraint, `23503`, `42501`, any other 4xx | plain error; row stays dirty | L0 |
| L0 | timeout, network error, crash | nothing; the retry repeats the same POST | L0 |
| L0 or L2 | `P0001` with hint `sporely_upgrade_required` or `sporely_sync_identity_refused`, or a media 426 with `X-Sporely-Error-Code: sporely_upgrade_required` | stop the upload phase for this run; keep rows dirty and downloads working; persistent banner "This version of Sporely can no longer upload. Please install the latest version."; log it as a server/client mismatch | unchanged |
| L2 (`cloud_id`) | edit | verify `cloud_id`; PATCH by id; never send `sync_key` or `desktop_id` | L2 |

No POST without a committed local key. A cloud-linked row with a NULL key is
never POSTed, only PATCHed by verified `cloud_id`.

### Adoption and ownership

- Adoption takes the fetched row as the base snapshot.
- Parent check: an image's `observation_id` must equal its local parent
  observation's `cloud_id`, and a measurement's `image_id` its local parent
  image's `cloud_id`. Otherwise it is an identity conflict: no re-parenting,
  and the row stays dirty.
- An adopted image reuses the adopted row's reserved `storage_path`. If its
  bytes were never confirmed, the retry uploads to that same key.
- The client matches the constraint name, not only SQLSTATE `23505`: the
  legacy `UNIQUE(desktop_id, user_id)` collision is also `23505`.
- An image moved to another observation is a new cloud row with a fresh key.
  The old cloud image is tombstoned, because cloud `observation_id` is
  immutable.
- Pull matches by cloud `id`, else by a `sync_key` equal to a local key, never
  by integer. Conflict detail pairs child rows by cloud id, then by key.

### Cloud not ready

Before uploading, the desktop probes `select=sync_key&limit=0`. If the column
is missing, it blocks uploads with "cloud not ready", and pull keeps working.

### One-time legacy recovery

An older desktop may have created a cloud row and crashed before storing its
`cloud_id`. Rows that existed before the upgrade and were never confirmed in
the cloud carry a one-time local marker. Before such a row's first POST, the
desktop makes one read-only lookup for same-owner rows with `sync_key IS NULL`
and `desktop_id` equal to the local id (for images and measurements, also
restricted to the already-linked parent):

- exactly one match whose uploaded content equals the local row: link it
  locally; no cloud write;
- any other outcome: clear the marker and POST as a new keyed row, reporting
  "possible duplicate of an earlier upload" in the sync summary.

A row found this way is never PATCHed and never deleted. The worst case is a
visible duplicate, never an overwrite.

### Rollback

No rollback ever restores `desktop_id` links.

## Normalized reference graph

The owner graph is ordered work → taxon treatment → measurement set →
observation reference use for live mutations and in reverse for tombstones.
All four owner feeds must be completely and successfully paginated and staged
before pull applies any row. Missing dependencies block the affected child;
feed absence alone never proves deletion. Pull advances table cursors only
after the complete staged graph is free of unresolved blocks and conflicts.

Every mutation uses the stable UUID and compare-and-set `row_version`. First
use creation alone uses `historical_import`; acknowledged updates, explicit
snapshot refresh, successor adoption, restores, and deletion use `current`.
Transport failure retries the identical UUID, payload, and expected token.
`created`, `updated`, and `no_change` persist the returned authoritative
baseline; CAS conflict persists review state and does not overwrite local
intent. An unknown create is resolved by a complete owner read before retry.
A library tombstone payload is `{id, deleted: true}` plus the row's unchanged
parent identity (`reference_work_id` for a treatment, `taxon_treatment_id`
for a measurement set): the server's sync RPCs check the named parent before
the tombstone branch and answer `invalid_parent` without it. Works have no
parent.
For new measurement-set mutations, absent `raw_points_json` is omitted from
the RPC payload rather than encoded as JSON `null`: the Stage 3 insert uses the
JSONB `->` operator and the table accepts only SQL `NULL` or a JSON array.
Genuine point arrays, including an empty array, are transmitted unchanged;
acknowledged updates retain explicit JSON `null` so an existing array can be
cleared through `jsonb_populate_record`.

Measurement sets carry the measurement-content extension
(`measurement_details_json`, `q_core_min`, `q_core_max`; contract
`docs/reference-data/measurement-content-contract.md`, section 9). Every
measurement-set mutation payload sends all three keys, JSON `null` included;
the adapter never strips them, because key presence is how a request
acknowledges the contract. On the server, a request that omits the keys may
still create a legacy row, retry unchanged, delete or restore, but any other
change to a row whose extension is non-NULL, and any successor of such a row,
is rejected with `invalid_payload`; a payload carrying some but not all keys is
rejected everywhere. Every content change is validated row-level (finite
positive values, ordered pairs, details structure and the 4096-byte limit; an
unknown future `schema_version` is stored opaquely). Owner reads and RPC rows
return the three columns, so a pre-extension server's rows are rejected by the
desktop as missing canonical fields, and a stored baseline that predates the
extension is read with the three keys as `null`. Pull reconciliation treats the
26 scientific-content fields of a measurement set as one conflict group:
concurrent edits inside the group conflict unless the complete resulting
content is identical, while `notes` keeps per-field merging.

Observation-reference snapshots have two supported versions (contract section
7). Version 2 is version 1 with `schema_version: 2`, `q_core_min`/`q_core_max`
inside the numeric-only `measurements` mapping (17 keys) and one new top-level
`measurement_details` key holding the decoded details object or `null`; the
whole snapshot stays within 65536 bytes and the details object within 4096
bytes of its canonical encoding. The emit rule follows the row, not a setting:
a legacy-only measurement set still produces its exact version-1 snapshot and
an enhanced one produces version 2. `private.reference_canonical_snapshot` and
the desktop builder apply the same rule, so the attachment RPC's equality with
the canonical snapshot keeps rejecting a version-1 projection of an enhanced
row. Readers ship before writers: the desktop use feed, the curated and
portable validators and `private.reference_snapshot_valid` accept both
versions through version-keyed exact key sets, and any other version is
refused loudly rather than read as version 1.
`private.public_reference_snapshot` preserves the extension instead of
rebuilding `measurements` without it, and the curated publication CHECK and
public curated reader accept `1` or `2`. Comparison is by version-aware
semantic projection: `schema_version` and `reference_revision` are dropped, a
missing extension equals an explicitly null one, real statistics are a genuine
difference, and an unsupported version is never projectable, so it is never
equal to anything. Nothing rewrites or enriches a historical snapshot;
replacement evidence goes through explicit refresh or successor adoption.
Enhanced content becoming frozen evidence is held behind the
minimum-supported-reader-version gate
(`references/measurement_content_gates.py` in `sporely-py`), which ships
closed: while it is closed the desktop refuses to attach, refresh onto, or
adopt an enhanced measurement set instead of freezing a lossy version-1
snapshot of it.

Observation-use pull imports the frozen `snapshot_json` exactly as stored.
Three-way reconciliation may automatically combine only disjoint role/note
edits. Identity, measurement-set, selected-time, revision, or snapshot
divergence remains an explicit conflict. Local observation deletion commits
child tombstones before requesting remote parent deletion. If that request
fails, tombstones remain retryable child-first; if it succeeds, the parent
acknowledgement resolves them without another child write.

Production orchestration runs normalized-reference reconciliation only after
the legacy observation pull has established observation cloud identities.
Normal sync reads the four complete owner feeds, reconciles them, then executes
the deterministic CAS plan. Download from Cloud passes the fail-closed
pull-only client into the same facade and returns immediately after the four
reads and local reconciliation; it must issue no reference or legacy writer
call, complete with `cloud_writes_completed == 0`, and leave
`blocked_write_attempts` empty.

Stage 6k adds immutable owner-private curated-fork provenance after the three
personal graph parents. The mapping key is the exact
`(curated_measurement_set_id, bundle_revision)` and records the explicit
positive taxonomy-v3 assignment plus the fresh personal work, treatment, and
measurement-set UUIDs and a SHA-256 fingerprint of the validated frozen public
envelope. Push waits for the personal graph; pull applies only after that graph
has reconciled and reconstructs the local frozen envelope through the exact
public revision read. Same-revision replay is a no-op, disagreement is a
conflict, a newer revision is a separate mapping, and neither path rewrites an
edited fork or an observation snapshot. Download from Cloud may read this
owner feed and the exact public bundle, but its provenance writer remains
blocked.

The owner feed is a complete, deterministically paginated collection. Desktop
reconciliation accepts up to 10,000 mappings and 64 MiB of encoded response
data per account; exceeding either bound fails the complete read without
returning a partial collection. There is no smaller undocumented 100-row
transport cap from which absence could be inferred.

The coordinator reports normalized-reference outcomes under `reference_sync`
with separate `pushed`, `pulled`, `errors`, `retryable_errors`,
`terminal_errors`, `conflicts`, and `blocked` fields. Existing top-level
observation/calibration counts do not absorb reference counts. Reference
errors, conflicts, and dependency blocks are also surfaced through the
existing top-level error channel. Typed errors and blocked outcomes retain the
RPC domain status (for example, `invalid_payload` or `invalid_parent`).

### Client capability (Stage M, sporely-web `20261002120000`)

Direct table reads of `reference_measurement_sets`,
`observation_reference_uses` and `reference_curated_forks` are the v1-only
legacy feed: the server withholds enhanced content and everything that
depends on it from them. The desktop therefore reads those three owner feeds
only through `list_reference_library_feed(p_entity, p_client_capabilities,
p_after_updated_at, p_after_id, p_limit)` with entities `measurement_set`,
`observation_use` and `curated_fork` (works and treatments keep their table
reads; they are never withheld). Every pull is a full pull: the first page
has no cursor, `next_cursor` pages only within that pull, and no persisted
cursor (including `reference_cloud_pull_cursors`) ever seeds `p_after_*`.
Rows are projected to the same column set the table select returned, so
reconciliation inputs are unchanged; deleted rows are part of the feed, so
tombstones still arrive. A non-`ok` status, a malformed page, a missing
column or a non-advancing cursor fails the whole pull (`rate_limited` as
retryable); a partial feed is never returned.

`p_client_capabilities` is
`{"reference_snapshot_versions":[1,2],"device_id":<uuid>,"client":"desktop_app","app_version":<APP_VERSION>}`.
Version 2 is declared only while the use-feed stager, the measurement-set
feed stager and the public envelope normalizer all accept it
(`declared_snapshot_versions`). Full measurement editing, attachment and
bundle export are active in the single-user desktop. The device id is a uuid4
generated once per profile and stored in the profile's `app_settings.json`
(`reference_client_device_id`); a missing, malformed or nil value is
replaced. Both `sync_reference_measurement_set` and
`sync_observation_reference_use` send it as the trailing parameter; other
reference writes take none. A bidirectional sync calls
`record_reference_client_capabilities` once per app session and account
before its first reference push; failure is logged and retried at the next
sync, never blocking. Download from Cloud never reports (the method is on the
pull-only block list; the feed RPC is on the read allowlist).

`requires_newer_client` retains the format-safety hold: the local change
stays pending and is never downgraded. The cloud migration
`20261026180718_activate_reference_reported_statistics` retires the
population-based `older_client_active` creation hold. Historical local
markers for that status no longer suppress a retry. Stale devices and the
30-day window are not prerequisites for this single-user app. Per-request
version validation, ownership, RLS, account binding and CAS remain enforced.

Public reads `search_public_reference_contributions_v2` and
`get_public_reference_contribution_v2` pass
`p_accept_snapshot_versions` = the declared versions. An item stamped
`measurement_details_omitted: true` (a v2 item projected to v1 for a
non-accepting caller) is tolerated and flagged on the bundle, shown with a
note and a disabled copy action, and never copied into the personal
library. Frozen provenance (cloud fork pull, portable import, bundle import)
rejects the marker. An unreadable or duplicate
item is skipped and logged rather than failing the whole page.

## Storage of desired cloud image-byte state

Local SQLite is authoritative:

- `images.cloud_id` — the current cloud identity link (if any).
- `image_tombstones` — durable explicit-deletion intent.
- `settings["sporely_cloud_image_storage_excluded_ids_<obs>"]` — the per-observation set of local image ids the user has excluded from Sporely Cloud image storage.
- `settings["sporely_cloud_image_storage_intent_ids_<obs>"]` — the per-observation storage-intent ledger. Only membership in this ledger proves an explicit decision (excluded or desired) exists for that image. Absence from the excluded set alone proves nothing — it may mean "explicitly desired" or "never decided." The observation-level init sentinel is retired; late-imported microscope frames must not inherit a stale "already initialized" flag.
- `settings["sporely_cloud_image_promotion_pending_<obs>_<img>"]` — a local pending marker written *before* the anchor-promotion reservation PATCH. Its presence together with a non-NULL remote `storage_path` marks the Worker key as UNCONFIRMED (reserved but not yet proven to hold bytes) and must not be trusted as proof of bytes.

The canonical byte-storage predicate is `cloud_image_bytes_desired(observation_id, image_id)`:

- returns `False` when `image_id` is in the excluded set for that observation;
- returns `True` otherwise;
- concerns bytes only — anchor lifecycle for measurements is separately governed by the metadata-only microscope anchor helper.

### Anchor promotion (metadata-only → byte-backed)

A linked metadata-only anchor (valid `cloud_id`, remote `storage_path` NULL) whose bytes become desired is promoted on its existing row rather than by creating a new row:

- The Worker media key is reserved via an owner-scoped conditional PATCH with filter `storage_path=is.null` — writing a candidate key only when the remote row is still NULL. The write is guarded by the local pending marker written *before* the PATCH.
- On successful reservation the upload proceeds; on confirmed upload the marker is cleared.
- On upload failure the partial R2 objects are removed and the key is released via a second owner-scoped conditional PATCH with filter `storage_path=eq.<exact reserved key>`, so a competing writer's key is never clobbered.
- A non-NULL `storage_path` combined with a live promotion-pending marker means UNCONFIRMED and must not be treated as bytes present. Once the marker is cleared, `storage_path` is authoritative again.

`SporelyCloudClient.upload_image_file` and `SporelyCloudClient.upload_original_image_file` refuse to send bytes when the predicate returns `False`, raising `CloudImageBytesNotDesiredError`. Recovery flows opt in by passing `recovery_authorized=True`.

## Ownership boundary

| Information | Desired ownership |
| --- | --- |
| Observation identity, taxonomy, notes, habitat, location, privacy, and draft state | Shared |
| Image metadata, type, order, microscope context, calibration link, crop, and scale | Shared |
| Desktop paths, import state, watch folders, caches, and UI state | Desktop/device only |
| High-quality local originals | Desktop-owned; optional companion upload |
| Web-friendly image copies | Cloud files (`R2 objects`) |
| Spore measurements and calibration identity/data | Shared |
| Social graph, social comments, reports, blocks, billing, moderation | Cloud/web only |
| Thumbnails, plots, mosaics, and crops | Rebuildable unless deliberately persisted |

## Keep image decisions explicit

The Observations and Measure gallery checkbox represents the desired Sporely Cloud state only:

- checked means the image should have a cloud copy;
- unchecked means it should not have a cloud copy;
- the cloud badge reports actual state: uploaded, delete pending, or absent.

External-publication inclusion (for example Artsobservasjoner or iNaturalist) is a separate workflow decision. It has its own persistence key (`artsobs_publish_excluded_image_ids_<obs>`) and does not participate in cloud storage sync in either direction. Ordinary upload filtering, a missing prepared file, or external-publication exclusion must never be reinterpreted as cloud deletion intent.

Canonical desktop image states are `NONE`, `UPLOADED`, `DELETE_PENDING`, and `DELETED`. A retained `cloud_id` with a synced tombstone is `DELETED`, not `UPLOADED`.

## Desired observation sync

### First upload

- Create the cloud observation with its `sync_key`, or adopt it after a `23505` on `observations_user_sync_key_uidx` (see "Per-record sync keys (`sync_key`)"). Never find it by `desktop_id`.
- Upload only images eligible for a new cloud copy (byte-storage desired) and whose bytes are actually available.
- Store local cloud links (`cloud_id`) only after confirmation.
- Save the known-good baseline (`sync snapshot`) after all required child work succeeds.
- Keep partial failures dirty and retryable.

### No changes

- Perform no writes, uploads, deletions, or unnecessary image encoding.
- Return a clear "nothing changed" result.

### One side changed

- Compare local, cloud, and baseline.
- Patch only shared fields that changed.
- Preserve cloud-only and local-only fields.
- A blank local text value is not an edit of a cloud NULL: an observation or image PATCH never turns NULL into `''`, and image change and conflict detection treat the two as equal. Clearing a cloud value to blank is still sent.
- A push (which runs before the pull) that keeps a cloud-only observation
  field edit also writes it to the local row, through
  `_apply_remote_observation_fields`, after the conflict preflight passes and
  before the cloud write; the outgoing payload carries the adopted local value.
  The post-push baseline records the cloud value, so a local row left at the
  old value would read as a local change and a later unrelated edit would push
  it back over the cloud edit — the pull cannot repair that, because baseline
  and cloud already agree. After a successful sync, local, cloud and baseline
  hold the same value for every adopted field. If the cloud write then fails,
  local and cloud already agree on it, which the next three-way comparison
  reads as shared. A blocked (conflicting) push adopts nothing. The virtual
  taxon identity field keeps its own adoption rule (see Taxonomy identity).
- Linked images follow the same three-way rule, with adoption. `push_image_metadata` enforces the rule for every PATCH of an existing image row, whichever path sends it (metadata drift, Refresh, upload, byte replacement, preparation, conflict resolutions): it shapes the body against the live cloud row, fetching that row when the caller has none and refusing the PATCH when it cannot, and leaves at the cloud value every field only the cloud changed since the stored baseline. Before the post-push baseline is stored, `_adopt_remote_image_edits_before_baseline` copies those same cloud-only edits into the local row without marking the observation dirty. Every column the desktop sends is classified once (`image_push._IMAGE_*_FIELDS`): one three-way list governs both protection and adoption (notes, objective, mount, stain, sample type and source, contrast, crop mode, measure colour, micro category, capture time, sort order, image type, scale, resample factor, GPS source, the AI crop box and the calibration, adopted as the local calibration link); `original_filename` is fill-only and never replaces a cloud value; storage key, upload metadata and the EXIF-safe flag belong to the uploader; observation, owner and deletion columns are structural. An adopted capture time keeps full precision (fractional seconds) and is recorded as cloud-held. A PATCH whose body equals the cloud row is not sent. A field both sides changed stays a conflict. The three-way check, in the push as in the preflight, compares local values normalized against the baseline (blank text over a NULL baseline, the pull-injected capture time, the `'cloud'` record and the legacy fallback). With no stored baseline row for an existing linked image whose local and cloud values differ, the push sends neither copy: the push preflight reports an image conflict and `push_image_metadata` refuses the PATCH (`ImageBaselineMissingError`), so the observation goes to conflict review and no baseline is stored. "Differ" means after the push's own normalization, in the push gate and in review alike (`image_payloads._no_baseline_image_values_equivalent`): the sample fields on both sides as the push sends them (`'Not_set'` is NULL, a legacy `Spore_print` condition is a source; an anchor is created with the raw local values), capture times as UTC instants, the float8 scale and resample factor compared at the 15 significant digits the cloud returns, and a NULL cloud capture time filled, not disputed, only on a cloud row without bytes whose local image is a microscope image with bytes not desired (the metadata-only anchor this desktop creates without a capture time). A row this desktop just created therefore equals its own payload and is linked in the same push. The float8 rule also applies to sync-key adoption and the no-op PATCH check, and the baselined three-way image comparisons (push and preflight) use both the float8 rule and the same sample normalization on the local, baseline and cloud sides. Only an explicit resolution writes; a later stored baseline restores the three-way rule. The conflict resolutions "Use this device", merge and the per-item plan store their baseline the same way.
- Known limitations (accepted by the plan owner, 2026-10-16, for the limited release): the no-baseline refusal is not yet honoured on two desktop paths — the prepared-upload loop can absorb the refusal and still store a baseline, and automatic conflict finalization can snapshot an unresolved image — so a stale local value can be pushed on a later sync. Other inherited desktop silent-overwrite risks (derived `species_guess`, pulls that ignore cloud clears, writers outside `push_image_metadata`, unconditional writes) are listed in sporely-py `docs/verification/2026-10-desktop-metadata-known-risks.md`. The whole-image conflict check can report false conflicts; those stop safely. A structural redesign (edit intent, conditional writes, atomic refusal state, reliable baselines, one enforcement point) is a separate future project.
- Refresh the baseline after success.

### Both sides changed

- Detect a conflict using a three-way comparison (`three-way merge`).
- Block automatic push and pull for that observation.
- Show affected categories: details, images, measurements, calibrations, or deletion.
- Offer "use this device," "use Sporely Cloud," or a future field-level merge.

## Desired image sync

### Existing cloud-backed image

When a local image still maps to an active cloud image:

- add it to the protected cloud list (`kept_cloud_ids`) whether or not bytes are prepared;
- keep the cloud row and file when omitted from `prepared_items` unless an explicit checkbox/context-menu tombstone is pending;
- patch metadata when necessary, without rewriting cloud values the user did not edit:
  - upload metadata (`upload_mode`, `source_*`, `stored_*`, `stored_bytes`) is sent only by the client that produced the bytes; a metadata patch never NULLs it;
  - the capture time a pull injected from the observation datetime (recorded at pull) never fills a `captured_at` the cloud row does not hold; a capture time the user sets is sent. For a copy pulled before that record existed, a local capture time equal to the observation datetime is withheld, and only while the cloud value is NULL;
  - for a recovery copy (`cloud_recovery_cache` / `cache`), the local file name never fills an `original_filename` the cloud row does not hold;
- replace bytes only when the trusted source changed or a missing cloud file is deliberately restored;
- never call it stale merely because it was filtered, skipped, or reordered.

### Lost local `cloud_id` — identity repair

Superseded by "Per-record sync keys (`sync_key`)": a lost `cloud_id` is reconciled by `sync_key`, and only the one-time legacy recovery still reads `desktop_id`. The former rule follows for the record.

When a local row has no `cloud_id` but a unique remote row shares the same `desktop_id`, matches owner scope, and matches image type:

- restore the local `cloud_id` from the remote row without uploading bytes;
- do this whether or not the image is currently desired (unchecked images repair too);
- if two or more remote rows share the `desktop_id`, do not auto-repair — log a warning and skip.

### New local image

- Upload when cloud upload is selected.
- Otherwise leave it local-only.
- Its local-only state must not affect existing cloud images.

### Existing microscope image

- Treat metadata, measurements, and bytes as separate concerns.
- Preserve existing cloud bytes during ordinary sync.
- External-publication selection must not clear `storage_path`, clear `original_storage_path`, or delete cloud files.
- Measurements may sync without bytes.

### Measurement-only image record

A measurement-only record (`metadata-only anchor`) is valid only when:

- bytes were never uploaded but measurements are shared; or
- the user explicitly chose "remove cloud image, keep measurements."

It must not be created from an external-publication choice. Owner-facing UI must distinguish deliberate measurement-only state from a broken or deleted image.

### Owner-sync and public-microscopy metadata parents

A metadata-only microscope parent (no bytes) exists for one of two separate
intents, and the desktop records which in `observation_images.metadata_purpose`
(sporely-web migration 20260925120000):

- `owner_sync` — carries the owner's own measurements of ANY type between the
  owner's devices when the image's bytes are deliberately excluded from cloud
  storage (`microscope_image_requires_owner_sync_anchor`: a microscope image
  with at least one measurement, independent of observation visibility and
  measurement type — the measurement pusher's own eligibility). Never public.
- `public_microscopy` — today's only owner consent to publish microscopy data
  (`microscope_image_requires_public_spore_anchor`: a spore measurement on an
  observation whose spore data is public).

The marker is intent, never authorization: the server exposes a no-byte
parent publicly only when it is marked `public_microscopy` AND it verifies
public child data itself (observation public and not a draft, spore data
public, a child measurement of a type in the single server-side
`public.is_public_microscopy_measurement_type` definition). NULL fails closed.
Rows with bytes follow ordinary image visibility. The desktop never infers
privacy from "not a spore": publishing cystidia later is a server type-set
change plus an owner-intent change of the marker.

- Owner-sync parents are created only when the server confirms the capability
  (`SporelyCloudClient._observation_images_support_metadata_purpose`: one
  user-scoped probe of the column, cached per client; a missing column is
  remembered as unsupported, any other error fails closed uncached); otherwise
  the desktop keeps the old public-spore-only behaviour, so an older server
  whose public RPCs would expose such a row never receives one. The probe and
  the targeted purpose read run only after local checks show an image needs
  a no-byte parent (owner-sync, or a public-spore parent being created or
  linked) or is a recorded retirement candidate, so observations with only
  byte-backed images issue no extra request. A public-spore parent always
  carries `public_microscopy` on a capable server: NULL fails closed on every
  marker-gated public surface, sporePoints and spore summaries included.
  Parents created by desktop builds older than this rule carry NULL and are
  not public until re-marked; the server backfill covers rows that existed
  when the migration ran.
- An image whose bytes are kept in the cloud gets its row from the ordinary
  upload, not a metadata-only parent.
- The purpose of an existing parent is corrected only when the remote value is
  known and differs (no no-op writes).
- An explicit cloud tombstone for an image whose owner measurements need the
  parent is cancelled, as for public spore anchors.
- Retirement: an `owner_sync` parent with no bytes, no byte-storage intent,
  no local measurements AND no cloud measurements is retired through a
  cloud-copy tombstone (`_retire_unneeded_owner_sync_parent`); the canonical
  tombstone push soft-deletes it on the next sync. A device that has not yet
  downloaded the measurements (zero local rows, cloud rows present) never
  retires it. Public-microscopy and legacy parents keep their existing
  lifecycle.
- Pull needs no special case: measurement import already attaches to any
  microscope parent regardless of type.

Known limitation, separate follow-up: deleting an individual measurement on
the desktop records no cloud deletion intent, so a deletion does not
propagate and retirement rarely becomes reachable until it does.

Regression: `tests/test_owner_sync_metadata_parents.py` (observation-917
shape: excluded image, three cheilocystidia, no spores).

### Missing cloud file

When an active row points to missing bytes:

- mark it broken or needing repair;
- do not silently delete the row;
- restore from a trusted local source where possible;
- otherwise show the owner a recoverable error;
- public clients may omit the broken photo, but owner diagnostics must expose the problem.

Changing image order (`sort_order`) is metadata only and cannot imply creation or deletion.

### Image-prep fast paths require upload completeness

> **Invariant.** Local media signatures describe local input/render state.
> They never prove remote upload completeness. Required cloud-media work is
> determined from per-image storage intent and cloud/link state.

A local media/render signature describes whether local *render inputs*
changed. It carries neither `cloud_id` nor cloud-storage intent, so it never
proves that the cloud identity or the bytes for the user's selected media
exist. Do not add either to the signature — that would make every cloud link
repair look like a local render change.

Consequence for `push_all` when `sync_images=True` and the observation already
exists in cloud: before any image-preparation fast path
(`image_render_unchanged` skip, tombstone-cleanup-only, metadata-only image
sync) may be taken, upload completeness is established separately:

1. `_ensure_cloud_image_storage_intent_initialized` seeds per-image storage
   intent — it must run *before* desiredness is read, because an unseeded
   ledger makes every row look uninitialized and an unseeded excluded set
   makes every row look desired;
2. `_pending_cloud_pushable_image_ids` computes the canonical pending set once
   for the observation;
3. a non-empty pending set vetoes all three fast paths and the existing full
   image-preparation/upload path runs instead;
4. if completeness cannot be established (any error), the sync fails closed
   into full image preparation.

There is exactly one pending-image predicate. Rows the upload path would skip
anyway — user-excluded, missing file, duplicate path, not-yet-initialized
intent — are not pending and therefore cannot cause a dirty loop.

`sync_images=False` (Refresh / background sync) never evaluates upload
completeness: it cannot upload bytes, so it must not be re-dirtied by them.

Without this gate an observation whose render inputs never changed but whose
selected images were never uploaded stays permanently stranded: dirty on every
sync, bytes never sent (`tests/test_cloud_sync_upload_completeness.py`).

The repaired run must converge along the whole chain, not merely send bytes:
one cloud image identity per local image (no duplicate row, no re-upload on
the next sync), measurement synchronization against that identity, and the
mosaic pusher receiving the resulting cloud-linked measurements
(`tests/test_cloud_media_measurement_mosaic_chain.py`).

Byte-storage state and measurement/mosaic participation stay independent, in
both directions. A microscope image the user excluded from cloud image storage
keeps no cloud bytes, yet its metadata-only anchor still carries its public
spore measurements to cloud and into the mosaic. A stranded-media incident is
therefore never repaired by requiring measured microscope source images to
upload their bytes — the byte-storage predicate governs bytes only.

### Recovering already-stranded observations

Installations stranded by an earlier defect are recovered through the existing
scanner, not a second one. `_mark_cloud_observations_dirty_for_pending_local_images`
is the only pending-image dirty scan, it only runs under `sync_images=True`,
and its cadence is owned by `_cloud_pending_image_repair_scan_due`: a versioned
repair generation (`cloud_pending_image_repair_version`) plus a 24-hour
watermark (`cloud_pending_image_repair_at`).

When a fix changes which observations the scan can actually rescue, bump
`_CLOUD_PENDING_IMAGE_REPAIR_VERSION`. An installation holding the previous
generation then performs exactly one rescan on its next explicit
`sync_images=True` synchronization, however fresh its watermark, and returns to
ordinary interval throttling afterwards. Do not recover stranded data by
widening the scan to Refresh/background sync, by removing the throttle, or by
adding a permanent broad scan.

Generation 2 covers the mosaic fix: an unchanged local render signature no
longer implies the selected media reached the cloud, so `synced` observations
holding desired, uploadable `cloud_id IS NULL` media become discoverable again
(`tests/test_cloud_sync_dirty_pending_images.py`,
`tests/test_cloud_sync_pending_image_repair.py`).

## Desired deletion flow

### Delete image everywhere

- Confirm when cloud data, measurements, annotations, or evidence are attached.
- Record explicit local deletion intent (`tombstone`).
- Mark the cloud row recoverably deleted (`deleted_at`).
- Keep identity and storage information for retry and audit.
- Remove cloud files only after the deletion state is confirmed.
- Do not hard-delete the row during routine sync.

### Remove only the cloud copy

This is separate from deleting the local image.

- Preserve local files and measurements.
- Keep a measurement-only record when required.
- Store explicit cloud-removal intent.
- Remove bytes after the new state is safely recorded.
- Show the resulting state to the owner.

The gallery checkbox and context-menu command share this lifecycle:

- `UPLOADED` + uncheck queues an unsynced tombstone and shows `DELETE_PENDING` immediately; the storage-excluded set gets the image id;
- recheck before sync cancels that tombstone, preserves the existing cloud ID, removes the image id from the storage-excluded set, and performs no upload or delete;
- after successful remote deletion, the checkbox remains unchecked and the synced tombstone yields `DELETED` with no cloud badge;
- recheck after `DELETED` preserves the historical tombstone, detaches the old identity, and explicitly restores exactly that image as a new cloud row.

### Remote deletion discovered on desktop

- Record it locally without erasing the local original.
- Ask whether to accept the deletion, restore cloud data from local files, or remain local-only.
- Do not repeatedly re-upload without a user decision.

## Child-change detection (`updated_at` cursor)

Child rows (`observation_images`, `spore_measurements`) can change in the
cloud without their parent observation changing, which the observation-level
fast path would otherwise miss. Detection works as follows:

- **Server side** (migration `20260824120000`): `observation_images.updated_at`
  is set to `now()` by a `BEFORE INSERT OR UPDATE` trigger unconditionally,
  for every role — service-role and admin writes advance it too. Historical
  values come only from the migration backfill. A covering index
  `(user_id, updated_at, id)` supports the keyset probe.
- **Desktop probe**: each sync reads rows with `updated_at >= cursor_ts`
  (paginated, ordered `updated_at.asc,id.asc`) and applies a strict
  client-side tuple filter: a row counts as changed only when
  `(updated_at, id) > (cursor_ts, cursor_id)`, with ids compared
  numerically. Measurements use the same scheme on their own leg.
- **Forced pulls**: parent observations of changed child rows are passed to
  `pull_all` as forced ids, bypassing the unchanged-observation pruning for
  exactly those observations — never a blanket `full_pull`.
- **Cursor persistence**: the per-leg `(ts, id)` cursor (v2) is stored in
  desktop app settings and advances to the maximum inspected tuple only
  after `pull_all` succeeds.
- **Consequence — rule 24**: because the trigger stamps every UPDATE, any
  no-op PATCH the desktop issues during pull becomes next sync's "change".
  Sync-path writes must be skipped when the remote value already matches
  (e.g. `desktop_id` relink is guarded by an equality check).
- Hard DELETEs leave no cursor trace; they are covered by the periodic full
  child safety scan, not by the probe.

## Retry-safe sequencing

Image upload should proceed as:

1. create or identify cloud image row;
2. upload or verify cloud file;
3. patch cloud file address (`storage_path`);
4. save local cloud link (`cloud_id`);
5. update known-good baseline (`sync snapshot`).

Deletion should proceed as:

1. store explicit intent;
2. soft-delete cloud row;
3. remove cloud files;
4. retain the removal marker long enough to prevent accidental recreation;
5. purge permanently only through deliberate maintenance.

An interruption after any step must be recoverable by repeating sync.

## Cloud → desktop taxonomy identity

The pull reads the cloud identity (`selected_sporely_taxon_id`, and
`taxon_identity_state` plus the preserved external tuple where the server has
the provenance columns; a server without them costs one retried read, once per
client) and maps it to ONE coherent local identity
(`_local_identity_columns_for_remote_claim`):

| Cloud row | Local identity written |
|---|---|
| selected Sporely ID present in the installed taxonomy-v2 artifact | `sporely_v2` / `cloud_selected_unverified`, artifact canonical name and rank as snapshot, provenance naming the cloud state and the local release |
| selected Sporely ID absent from the installed artifact (release mismatch, pre-v2 install) | `external_unresolved` `(sporely, sporely_taxon_id, <id>)` — preserved, never a bound integer |
| `external_unresolved` with a complete tuple | the same tuple as `external_unresolved` |
| no identity | no identity |
| identity columns absent from the row | nothing — local identity untouched |

Rules common to every write path:

- The local row already holding the same identity is never rewritten, so a
  picker-proven identity is not downgraded to the cloud token for the same
  concept.
- A cloud row without identity clears local identity only when the
  identification (genus/species) changed; absence is not evidence against
  local evidence for the same taxon.
- A bare integer is never stored (the persistence write boundary refuses it).

Creation (`_create_local_from_remote`) adopts the cloud identity. The
explicit "keep cloud" resolution adopts it too: the owner chose that side.
The automatic no-snapshot full apply does NOT overwrite a local claim (a
proven identity or a preserved non-Sporely tuple) that differs from a
non-empty cloud identity: nothing says which side changed, so it reports a
"taxon identity" review, keeps the row dirty, and stores the snapshot WITHOUT
the identity, leaving the identity baseline unknown so every later pull and
the push preflight keep classifying the disagreement as a conflict. The
three-way reconciliation of existing rows is described in the next section.

## Identity in change detection

The identity is the virtual observation field `taxon_identity`. It is never a
cloud column and never PATCHed: pushing it means the guarded RPC, which
accepts only a proven identity. All sides use one key vocabulary
(`sporely:<id>`, `external:<source>:<namespace>:<id>`, `""` for none):

- **remote** — `selected_sporely_taxon_id`, else a complete
  `external_unresolved` tuple, else `""`; a row without identity columns has
  no key and is ignored by every rule below;
- **baseline** — the key recorded in the stored snapshot; a snapshot stored
  before this rule has none (**unknown**, distinct from `""`);
- **local** — proven and `cloud_selected_unverified` IDs as `sporely:<id>`; an
  unconfirmable cloud Sporely ID preserved as `(sporely, sporely_taxon_id, id)`
  also as `sporely:<id>` (so holding it is no perpetual difference); other
  external tuples as `external:…`; a legacy integer as `legacy:<id>` (never
  equal to a cloud value).

A local **claim** is a proven identity or a preserved non-Sporely external
tuple; everything else (legacy, cloud-derived, manual text, none) is not the
desktop's own evidence. "Identification edited locally" means genus or species
is a local-only or conflicting field.

`_classify_identity_sync_change`, in order:

1. Local key equals remote key → nothing (`shared` if both moved from the
   baseline).
2. Unknown baseline:
   - local claim and a remote identity that differs → **conflict**;
   - proven local, nothing in the cloud → **local-only** (the RPC asserts it);
   - otherwise a non-claim local adopts a non-empty remote identity
     (**remote-only**) unless the identification is edited locally.
3. Known baseline: remote changed = remote ≠ baseline; local changed = local
   claim ≠ baseline.
   - remote changed and (local changed or identification edited locally) →
     **conflict**;
   - remote changed → **remote-only** (adopt, per the mapping above);
   - local changed and proven → **local-only**; an unpushable local claim is
     left alone, so it never keeps a row dirty.

Push (`push_all`, which runs before pull) obeys the same classification.
When the identity is remote-only the push ADOPTS the cloud identity locally
(as the pull would) before pushing — withholding alone is not enough, because
the post-push snapshot records the cloud value as baseline and a stale local
identity would then read as a local change for the next push to re-assert.
With no stored snapshot, a disagreement that would be a conflict is withheld,
reported, and left pending: the row stays dirty with the review marker and the
post-push snapshot is stored without identity, so the next preflight blocks.
A proven desktop pick the cloud lacks is still asserted.

A cloud row without identity clears local identity when three-way
reconciliation (or the owner's explicit choice) says the cloud cleared it
since the baseline; only the baseline-less full apply treats absence as "no
evidence" while genus/species are unchanged.

Consequences in the existing machinery: remote-only applies through
`_apply_remote_observation_fields(fields={'taxon_identity'})`; a conflict
blocks the snapshot advance, keeps the row dirty and blocks push until the
review (`get_conflict_detail` shows a "Taxon identity" row; "this device"
resolves through the RPC gate, "Sporely Cloud" through the pull mapping);
`_remote_snapshot_has_meaningful_changes` and the converge fast path treat an
identity-only cloud change as a change; `_local_has_real_changes_since_snapshot`
counts a proven identity the baseline does not record, so a desktop pick is
never cleared as "no real change" before the RPC runs. A pre-identity snapshot
reconciles once and is then recorded.

Regression: `tests/test_cloud_identity_change_detection.py`,
`tests/test_cloud_identity_pull.py`, `tests/test_cloud_identity_fail_closed.py`.

## Red List follows the identification it assesses

A Red List category is an assessment of one taxon. It must never become the
current value of an observation whose committed identification is a
different taxon.

- The observation dialog restores the Red List only from the persisted
  columns. `ai_state_json` is provider/UI history (the per-image prediction a
  user highlighted), not the committed identity, and its category is never
  borrowed. Regression: `tests/test_red_list_ai_history_not_promoted.py`.
- Before a push, `_merge_cloud_selected_ai_fields` gap-fills local NULL Red
  List columns from the cloud row (a row that never pulled them must not wipe
  the cloud value). It does so only while both rows name the same
  genus/species: after a desktop re-identification the local NULL is an
  explicit "no assessment for the new taxon", and filling it would copy the
  previous taxon's category onto the new identification and adopt it locally.
  Regression: `tests/test_red_list_push_merge_follows_identification.py`.

Found in the taxonomy-v2 closeout observation-917 integrity round-trip.

## Mosaic signature survives the sync's own working-file swap

The local mosaic signature (`_local_spore_mosaic_signature`) fingerprints each
source image by resolved path, size and mtime, so a genuine local file
replacement re-renders the mosaic. Pull-side materialization
(`_sync_existing_remote_image_to_local`) may itself replace a microscope
working file with the cloud copy — for example `P9150704.jpg` →
`P9150704.webp` on the same pixel grid. That swap is the sync's own
write-back, not a user edit, and must not make the next push re-render and
re-key an unchanged mosaic (the no-op fast-path contract).

- When the stored signature was **current immediately before** the swap, the
  swap re-stamps it from the post-swap inputs, exactly as the same function
  already re-stamps the image file signature.
- A signature that was already stale is left alone, so an out-of-date mosaic
  is still rebuilt. A file replaced outside sync still changes the signature.
- Float write-backs from the pull do not trigger rebuilds: the signature
  canonicalises floats to 6 decimal places.
- The pusher and `_current_local_mosaic_signature` share one eligibility query
  (`_load_spore_mosaic_eligible_rows`), so the rows a signature covers cannot
  drift from the rows the mosaic is rendered from.

Regression: `tests/test_cloud_spore_mosaic_signature.py`
(`test_sync_swap_of_microscope_working_file_carries_a_current_mosaic_signature_forward`
and its guards). Found in the taxonomy-v2 closeout observation-917 integrity
round-trip, where the swap was proven to be the sole trigger.

## Public spore mosaic after conflict resolution

Normal `push_all` and the legacy whole-observation `resolve_conflict_keep_local`
both call the canonical public-spore-mosaic helper
(`_push_spore_mosaic_for_observation`) right after a measurement push. The
per-item conflict resolver, `resolve_conflict_plan`, now does the same for a
**reviewed local-to-cloud measurement upload**, including a verified partial
retry where the upload already completed and only mosaic work remained:

- The mosaic step runs once per plan, after every measurement push/import op
  and before the snapshot → media-signature → synced finalization, whenever
  this call (or a verified prior attempt carried forward on retry) completed
  at least one `push_measurement`.
- **Conservative asymmetry guard.** The mosaic helper selects an
  observation-wide set of cloud-linked eligible measurements, so it cannot
  prove that a partial local upload matches the full cloud-approved state.
  `resolve_conflict_plan` therefore skips the mosaic step whenever either of
  two independent checks finds unproven local/cloud divergence:
  - the plan's merged accepted-asymmetry (new this round, carried from a
    prior attempt on retry, or inherited from an earlier snapshot) contains
    **any** retained local-only or cloud-only image or measurement entry —
    not only ones affecting microscope geometry. A kept-local field photo
    alongside an otherwise-clean upload therefore also skips the mosaic this
    stage; narrowing the guard to microscope-relevant asymmetry only is
    deferred.
  - a **matched** cloud-linked measurement (or its owning cloud-linked
    microscope image's render scale, or that image's `image_type`) still
    differs from cloud after the plan applies, even when no plan item named
    it. `finalize_sync_candidates` applies automatic items while manual
    conflicts remain open (`_build_plan_from_automatic_decisions`), so a
    genuine two-sided divergence reserved for the conflict dialog is never
    recorded as accepted asymmetry — it simply isn't in the plan's `items`
    at all. This check reuses the canonical measurement-match comparison
    (`_measurement_payloads_match`) against the reconciled local/remote
    state already read after the plan's writes, rather than a second
    renderer or a new persisted eligibility format. The `image_type` check
    exists because the mosaic SQL selects purely on the *local* image's
    type: a local image already reclassified to `microscope` while the
    cloud-approved counterpart still disagrees (or vice versa) must not let
    that measurement's tile render from a classification the cloud side has
    not agreed to.
- Mosaic failures follow the existing best-effort policy: an auth or
  temporarily-unavailable error aborts the resolution through the existing
  partial-plan-error path (so a retry can revisit mosaic work without
  replaying the completed measurement push); any other failure is logged and
  does not block finalization.
- Pure keep-local-only, field/image-only, and import-only (cloud-choice
  measurement) resolutions never push a measurement, so they never trigger
  mosaic work.
- Import-only mosaic generation and mixed-asymmetry atlas generation (partial
  local state safely merged into a public atlas) are out of scope for this
  stage and remain future work.

## Public, Android, and owner display

Public galleries should show only active rows with usable cloud files. That is correct for a deliberate measurement-only record, but not enough for diagnosis.

Owner/admin reads must distinguish:

- active image with valid bytes;
- deliberate measurement-only record;
- active row with unexpectedly missing bytes;
- recoverably deleted image;
- permanently purged image.

Public read functions (`RPCs`) may hide measurement-only and broken images, but must not turn accidental data loss into an invisible success.

## Incident: missing photos after 3 August 2026

### Root cause

The desktop began reading:

`artsobs_publish_excluded_image_ids_<observation_id>`

as both an external-publication choice and a Sporely Cloud media-storage choice.

### Field-photo failure

- Unchecked images were omitted from `prepared_items`.
- The sync loop protected only rows in prepared or explicitly protected sets (`kept_cloud_ids`).
- Other existing rows were classified as stale.
- Stale cleanup deleted cloud files, hard-deleted cloud image rows, and cleared local `cloud_id` values.

Local files remained, but Android/web had no cloud image row to display.

### Microscope-photo failure

Measured microscope rows were sometimes retained for foreign-key links, but unchecked images could be converted to metadata-only anchors:

- derivative and companion-original files were removed;
- `storage_path` and `original_storage_path` became null;
- `deleted_at` remained null.

Public image queries intentionally hide rows without `storage_path`, so the photo disappeared while measurements remained.

A follow-up prevented automatic re-upload after this conversion. It stopped an orphan-upload loop but did not restore removed bytes.

Reported examples include Mica cap, Boletales, and *Mycena haematopus*. These are audit starting points, not a complete list.

## Incident: truncated bulk reads, August 2026

A bulk image-metadata fetch issued one unpaginated PostgREST GET for the
image rows of many observations at once. The server silently capped the
response at its row limit (1000 rows) while returning HTTP 200, so the
client treated a truncated collection as the complete remote state. Effects:

- newly pulled observations whose image rows fell past the cap appeared to
  have zero images;
- the diff against local state produced false "cloud removed local image
  files" conflicts.

The repair made `_get_paginated` the canonical bulk reader: explicit
`limit`/`offset` paging until a short page, a mandatory deterministic
`order=` clause with `id.asc` tie-breaker, and hard failure (no partial
result) when any page errors. Batched `in.(…)` queries additionally
paginate each batch — batching bounds URL length and is not a substitute
for pagination. Safety rules 14–17 encode the lessons; regression tests
live in `tests/test_cloud_download_only.py` (desktop), including the guard
that a page failure can never yield a page-1-only snapshot.

## Incident: duplicate cloud observations after pull-only import, August 2026

Observations imported by Download from Cloud carry a valid local `cloud_id`
while the remote `desktop_id` remains NULL (pull-only performs zero cloud
writes, so the reverse link is deliberately not written back). The desktop
push path resolved identity only by the reverse link: when such an
observation later became dirty, the `desktop_id` lookup found nothing, the
client POSTed a new cloud observation, and the caller overwrote the correct
local `cloud_id` with the duplicate's id. The next pull then saw the original
cloud row as unlinked and imported it as a second local observation. Fourteen
duplicate cloud observations were created before the defect was found.

The repair established a canonical push-identity resolver: a verified local
`cloud_id` is the primary identity (PATCH, never POST); the reverse
`desktop_id` link is recovery-only and must match uniquely; disagreement or
ambiguity fails safely and keeps the observation retryable. Safety rules
20–23 encode the lessons; regression tests live in
`tests/test_observation_push_identity.py` (desktop).

## Repair plan

### Phase 0 — stop further damage (shipped)

Desktop (`sporely`):

- Disable stale-row deletion in ordinary image sync.
- Make omitted upload candidates harmless.
- Preserve all active linked cloud images in `kept_cloud_ids`.
- Stop publication checkboxes from converting existing images to metadata-only state.
- Stop metadata-anchor helpers from deleting cloud bytes.
- Keep explicit tombstone processing separate; pause it too if old markers cannot be trusted.

Operations:

- Avoid affected desktop sync builds on important data.
- Back up local SQLite before repair.
- Export affected cloud image rows.
- Preserve sync logs from 3–4 August 2026.

### Phase 1 — read-only evidence report

Report per observation/image:

- local and cloud observation IDs;
- local and cloud image IDs;
- image type and order;
- local file existence;
- `cloud_id` and local tombstone state;
- `deleted_at` and `purged_at`;
- `storage_path` and `original_storage_path`;
- actual cloud-file existence;
- measurement count;
- last sync time and source app version.

The audit must be dry-run and non-mutating. Run it first on reported observations, then all observations touched by affected desktop versions.

### Phase 2 — make cloud selection explicit (Stage 1 shipped)

Stage 1 implemented on desktop:

- The gallery "Keep image in Sporely Cloud" checkbox now persists to a dedicated per-observation setting `sporely_cloud_image_storage_excluded_ids_<obs>`, separate from `artsobs_publish_excluded_image_ids_<obs>`.
- A canonical predicate `cloud_image_bytes_desired(observation_id, image_id)` is the single source of truth for whether cloud image bytes should be sent for one local image.
- `SporelyCloudClient.upload_image_file` and `SporelyCloudClient.upload_original_image_file` refuse to send bytes for an image the user has unchecked, raising `CloudImageBytesNotDesiredError`. The gate fails closed on missing identity: normal-path callers must pass valid `observation_id` and `image_id`; recovery flows opt in by passing `recovery_authorized=True` (the only intentional exception, waiving the identity requirement for auditability).
- `_push_images_for_observation` guards the two byte-upload call sites and the `prepare_images_cb=None` fallback branch with the same predicate.
- Identity repair (`_associate_persisted_cloud_images`) is decoupled from the checkbox and works by unambiguous `desktop_id` match.
- A non-destructive initializer `_initialize_cloud_image_storage_desired_state_for_observation` seeds the storage-excluded set on the first sync path (invoked at the top of `_push_images_for_observation`, immediately after pending image tombstones are pushed, before any candidate filtering or byte-upload boundary) — not on gallery open — so background/headless sync cannot upload the full local microscope set for an observation the user has never viewed. It is now a thin alias of `_ensure_cloud_image_storage_intent_initialized`, the canonical per-image intent-ledger entry point (see "Storage of desired cloud image-byte state" above). UI still calls the same initializer for display consistency but is not required for correctness. Per-image seeding rules follow the ledger semantics: uploaded images stay desired; delete-pending / deleted images are excluded; local-only field images stay desired; each local-only microscope image is decided on its own — the retired group-freeze rule (per-magnification sparse default gated on whether any image in the group already had a cloud identity) is gone.
- Byte-upload ordering invariant: `cloud_image_bytes_desired` is absence-based and does not consult the ledger, so every byte-upload path MUST run `_ensure_cloud_image_storage_intent_initialized` before evaluating desiredness. Currently enforced by convention at push start (`_push_images_for_observation`) and at the dirty scan.
- Legacy Artsobs publication exclusions are not migrated into the new set.
- The reason enum split: `PENDING_REASON_EXCLUDED` remains for internal `excluded_ids` arguments; the new `PENDING_REASON_NOT_DESIRED_BY_USER` reports gallery-desired-state rejects.

Not yet:

- Fold gallery UI to expose the checkbox as a Sporely Cloud state control (already the case) with matching help text and audit surface.
- Move Artsobservasjoner/iNaturalist publication selection to its own, separately-scoped UI decision surface with its own default rules.
- Surface any legacy `artsobs_publish_excluded_image_ids_<obs>` values that correspond to visible cloud photos as audit hints without mutating cloud state.

### Phase 3 — repair desktop image algorithm (future work)

Desktop:

- Build `kept_cloud_ids` from every active cloud row that maps to an existing local image.
- Use `prepared_items` only for upload work.
- Remove "delete every remote row not kept" from ordinary sync.
- Process tombstones in a separate deletion phase.
- Use soft deletion before file cleanup.
- Never clear a valid `cloud_id` because the current run uploaded no bytes.
- Make metadata-only anchor creation explicit and non-destructive.
- Advance snapshots only after all required image operations succeed.

### Phase 4 — harden cloud and clients

Web/cloud (`sporely-web`):

- Keep public galleries limited to usable files.
- Add owner/admin diagnostics for measurement-only, broken, and deleted states.
- Add non-mutating file-existence verification.
- Make media deletion retry-safe and observable.
- Add state fields only if existing fields cannot distinguish intentional measurement-only state from broken or deleting state; do not add fields merely to hide an algorithm bug.

### Phase 5 — recover data

Only after the safety fix is deployed:

- **Hard-deleted field row:** recreate or relink by stable identity, re-upload from trusted local source, restore metadata/order, and stamp `cloud_id` after verification.
- **Pathless microscope anchor that should have bytes:** upload derivative, patch `storage_path`, preserve cloud image ID and measurement links, and restore `original_storage_path` only when policy allows.
- **Erroneous tombstone:** clear only when evidence proves it came from the regression; preserve genuine deletion.
- Verify cloud-file existence, identity, client visibility, and baseline updates for every recovered image.

### Phase 6 — regression tests

Desktop tests must prove:

- unchecked existing field images keep local row/bytes and queue only cloud deletion;
- unchecked existing microscope images keep local row/bytes, measurements, and annotations;
- rechecking an unsynced delete cancels it without upload or remote deletion;
- rechecking a completed delete uses image-specific explicit restore without touching live siblings;
- omission from `prepared_items` is not stale/deleted;
- every active linked image enters `kept_cloud_ids`;
- explicit tombstone deletion still works;
- soft deletion precedes cleanup;
- repeated unchanged sync performs no writes/uploads/deletions;
- interrupted upload or deletion recovers safely;
- missing cloud files are reported, not silently deleted;
- conflicts cover observation, image, measurement, and deletion changes;
- recovery copies never overwrite local originals;
- `cloud_image_bytes_desired` returns the correct answer for every state (uploaded, delete-pending, deleted, local-only field, local-only microscope with and without measurements);
- `SporelyCloudClient.upload_image_file` and `SporelyCloudClient.upload_original_image_file` raise `CloudImageBytesNotDesiredError` for undesired images and pass for desired ones;
- identity repair runs even for undesired images;
- legacy `artsobs_publish_excluded_image_ids_<obs>` values do not migrate into `sporely_cloud_image_storage_excluded_ids_<obs>`.

Web/cloud tests must prove:

- public functions hide deliberate measurement-only rows;
- owner/admin diagnostics expose deliberate and broken no-file states;
- restored images appear in Android/web queries;
- soft-deleted rows remain hidden;
- media deletion is retry-safe;
- storage keys remain owner-scoped.

### Phase 7 — release and monitor

- Ship the desktop safety hotfix before bulk recovery.
- Release read-only audit before write-capable recovery.
- Require dry-run reports and explicit approval for recovery batches.
- Record restored rows/files, intentional metadata-only rows, genuine deletions, missing local sources, and unresolved conflicts.
- Monitor hard deletes, file deletes, null `storage_path`, recovery counts, and source app versions.

## Definition of done

- Ordinary sync cannot delete a row or file without explicit intent.
- Publication selection and cloud deletion are separate.
- Existing field and microscope photos survive unchecked publication state.
- Measurement-only records are deliberate and diagnosable.
- Affected observations have been audited and recoverable media restored.
- Genuine deletions remain deleted.
- Repeated sync is retry-safe and causes no extra changes.
- Android and web show restored photos.
- Both repositories contain matching docs and regression tests.

## Repository responsibilities

### Desktop (`sporely`)

Owns local originals, SQLite state, three-way comparison, conflict blocking, upload preparation, ID links, tombstones, measurement/calibration sync, recovery from trusted local files, and desktop tests.

Primary areas:

- `utils/cloud_sync.py`
- `utils/cloud_media_recovery.py`
- `ui/observations_tab.py`
- `ui/main_window.py`
- `ui/image_gallery_widget.py`
- `database/models.py`
- `tests/test_cloud_sync_*.py`
- `tests/test_cloud_image_bytes_desired.py`
- `tests/test_cloud_storage_desired_initializer.py`
- `tests/test_observations_tab_cloud_sync.py`

### Web/cloud (`sporely-web`)

Owns schema and access rules (`RLS`), public/owner functions (`RPCs`), Android/web display, media worker behavior, diagnostics, and web/cloud tests.

Primary areas:

- `supabase/migrations/`
- `supabase/tests/`
- `src/images.js`
- `src/sync-queue.js`
- `supabase/functions/`
- observation gallery code.

## Destructive-change checklist

Every pull request that can remove a cloud row or file must state:

- What explicit user action created deletion intent?
- Where is that intent stored durably?
- Is the database change recoverable (`soft delete`)?
- What happens if cleanup fails or the app crashes after each step?
- Can retrying delete anything extra?
- Which test proves an omitted upload candidate is preserved?
- Which test proves publication selection is not deletion consent?
- How can the owner audit and recover the result?
- Were both copies of this contract updated?

## Private ingestion and location provenance (not deployed)

Migration `20261012105630_ingestion_location_provenance.sql` adds nullable
`observation_images.ingest_method` and observation `location_source` /
`location_status`. These are owner diagnostics, excluded from existing public
views/RPCs. Historical and unsupported-client values stay NULL; ingestion is
into Sporely and does not identify the photographing device. `gps_source`
continues to identify the contributing image separately. Client-reported
values are diagnostics, never authorization claims.

| Field | Values | NULL means |
| --- | --- | --- |
| `observation_images.ingest_method` | `sporely_camera`, `system_camera`, `web_camera`, `native_picker`, `web_file_picker`, `unknown` | historical row or client without provenance |
| `observations.location_source` | `device_gps`, `image_exif`, `manual`, `none`, `unknown` | no attribution (historical, legacy client, or invalidated by a legacy coordinate change) |
| `observations.location_status` | `acquired`, `disabled`, `denied`, `timeout`, `skipped`, `unavailable`, `unknown` | as above; always NULL together with `location_source` |

`unknown` means a provenance-aware client could not classify a new operation;
it is not a substitute for NULL.

### Server rules

The coordinate bundle is latitude, longitude, altitude and accuracy. Ordinary
UPDATEs changing any actual bundle value invalidate both location fields to
NULL, including direct writes resending unchanged enum labels. Unrelated edits
and value-identical coordinate resends preserve attribution. Complete pair and
coordinate compatibility checks apply to new provenance; legacy NULL pairs do
not impose new restrictions on historical coordinates:

- `acquired` requires valid latitude/longitude and a source of `device_gps`,
  `image_exif`, `manual` or `unknown`;
- every other status requires all four bundle values NULL and a source of
  `none` or `unknown`.

Aware clients must call owner/RLS-enforced `set_observation_location` with all
seven arguments (observation ID, four coordinate values, source, status). It
locks the owner row, updates coordinates with ordinary invalidation, then sets
the validated pair within the same transaction. Same-value retries make no
writes. A failing second write rolls back both. No bypass flag or definer
privileges are used. The same enum pair may legitimately be kept for new
coordinates (a new `device_gps`/`acquired` fix). The RPC does not collect GPS
or classify client actions. An INSERT may carry a compatible pair together
with its coordinates; INSERTs without the fields store NULL/NULL.

### Location outcome semantics (frozen transitions)

The pair describes the saved observation's current coordinates. Valid
selected coordinates use `acquired` and their actual source. Without
coordinates the status is the latest meaningful acquisition outcome of the
current session at save: continue-without-location preserves `denied`,
`timeout`, `unavailable` or `disabled`; `skipped` is used only when no such
outcome was observed, or when coordinates are deliberately cleared through
the aware operation (`none`/`skipped`). A successful selected fix supersedes
earlier failures; a failed attempt never replaces valid selected coordinates
or their source. Session state never borrows an outcome from an earlier
session, and stale asynchronous results are ignored. Status is not inferred
where acquisition was never observed. The full table is in
`docs/plans/active/2026-10-12-ingestion-location-provenance.md`.

### Web client behavior (Stage 3)

- **Session outcome.** `geo.js` records the classified outcome (permission
  denied → `denied`, preference disabled → `disabled`, timeout → `timeout`,
  position unavailable/unsupported/invalid fix → `unavailable`, other errors →
  `unknown`) in `state.captureSessionLocation.outcome`, scoped by a generation
  that changes only when a capture session starts or ends. Re-beginning the
  same session (preflight "continue" after a denial) keeps the outcome. Every
  acquisition (permission query, position request, watch, acquisition timer)
  belongs to the generation in which it started: a continuation that completes
  after the session changed may update the device-global permission but never
  the session's fix, outcome or acquisition state, and a request whose session
  ended during its permission check does not start. Within one session,
  overlapping attempts are ordered: each request, watch, standalone permission
  check or preference/unsupported observation takes an increasing attempt id,
  and only the newest attempt may record a failure outcome or reset acquisition
  state, so an older attempt completing after a retry cannot replace the
  retry's outcome. A superseded attempt whose permission check resolves late
  starts no acquisition and does not change acquisition status; only the
  device-global permission value is updated. A superseded attempt's successful fix still counts as a
  device fix. Global `state.location.error` is never used as provenance.
- **Save decision (review screen).** Selected coordinates → their source +
  `acquired`. Location preference disabled at save → `none`/`disabled`. The
  location sheet's "Continue without location" (or an earlier continue in the
  same session) → `none` + session outcome, else `skipped`. A successful
  "Try again" saves `device_gps`/`acquired`.
- **Coordinate sources.** `geo.js` tags each fix it accepts into the session
  as `device_gps`; a session fix and photo snapshots keep the source recorded
  with them (a restored pre-provenance fix stays `unknown`). GPS read from an
  original photo's EXIF (pickers, system camera — including the native New
  Find camera path, with late hydration that Save awaits) is `image_exif`. Sporely Cam writes GPS EXIF only from the location
  Sporely hands it, so its EXIF carries that location's source (`unknown` if
  none was handed over). In live review the selected coordinate (the more
  accurate of session fix and first photo with GPS) decides the source; a
  missing accuracy is unknown, never better than a measured one (ties keep the
  session fix), and an unmeasured accuracy or altitude is saved as NULL, not 0; in
  import groups it is the first photo with GPS. Late EXIF hydration changes the
  source only when its coordinates become the selected location.
- **Imports.** An imported review never observed acquisition: with coordinates
  it saves their source, without coordinates it saves a NULL pair. A
  multi-group import saves `none`/`skipped` only for groups the user explicitly
  confirmed saving without any location; a group with only a place name stays
  NULL.
- **Persistence.** Per-photo GPS source and the session outcome survive review
  drafts; per-photo source and the group source survive persisted import
  sessions. Drafts and sessions written before provenance restore with no
  outcome and an `unknown` source for existing coordinates.
- **Queue.** The pair travels on the queued observation payload and is
  inserted atomically with the coordinates in the single observation INSERT.
  An incomplete or incompatible pair is dropped (NULL/NULL) before the insert,
  so provenance can never block an upload. Legacy queue items carry no pair.
- **Manual location.** The find-detail map picker saves through
  `set_observation_location` as `manual`/`acquired` with NULL altitude and
  accuracy. The web app has no coordinate-clearing UI; an aware clear must use
  the RPC with `none`/`skipped`.
- **Adding photos** to an existing find writes image rows only (with their own
  `ingest_method`) and never touches the find's coordinates or provenance.
- **Pre-migration targets.** A queue INSERT rejected for missing provenance
  columns is retried once without the pair; a missing RPC falls back to a plain
  owner-filtered coordinate update. Any other error (CHECK, non-owner) is
  surfaced, never retried as a legacy write.

### Desktop requirements (Stage 4 implements against this)

The desktop client (`sporely`) must:

1. **Store** `location_source` and `location_status` on its local observation
   and `ingest_method` on its local image records, nullable, as text received
   from the cloud. Do not validate, remap or default them on pull.
2. **Pull** the three fields from owner base-table reads. Absent keys (a target
   or projection without the columns) leave local values unchanged; an
   explicit NULL is stored as NULL. Public views never carry them.
3. **Merge** `location_source`/`location_status` as members of the coordinate
   bundle (latitude, longitude, altitude, accuracy, source, status): the bundle
   moves as a unit in three-way comparison, never field by field. Comparisons
   are NULL-safe; NULL and an absent key are equal. A sync snapshot written
   before provenance (no provenance keys) treats the baseline pair as equal to
   the current cloud pair, so adopting the cloud pair is not a local change and
   produces no push.
4. **Preserve on unrelated edits.** Ordinary observation PATCHes never include
   `location_source` or `location_status`. Value-identical coordinate resends
   are allowed but should be avoided by comparing before writing.
5. **Coordinate changes.** A desktop coordinate change whose origin the desktop
   knows (manual entry → `manual`/`acquired`; image EXIF → `image_exif`/
   `acquired`; deliberate clear → `none`/`skipped`) must use
   `set_observation_location` with all seven arguments. Any other coordinate
   change uses the ordinary PATCH and accepts server invalidation to NULL/NULL;
   the desktop must not guess a pair. After either write the baseline records
   the resulting server pair (NULL/NULL after a legacy coordinate PATCH).
6. **First upload.** A new cloud observation INSERT may include a compatible
   pair with its coordinates when the desktop knows it; otherwise it omits both
   fields. Desktop-created images omit `ingest_method` (NULL) in the initial
   release.
7. **Images.** `ingest_method` is preserved cloud→desktop→cloud and never
   PATCHed by the desktop: reservation, re-upload, metadata-anchor promotion
   and identity repair keep the existing row value and do not resend it.
   `gps_source` remains the contributing-image designation and is unrelated to
   `ingest_method`.
8. **Zero-write paths.** Download-from-Cloud and no-op sync remain strictly
   zero-cloud-write; pulling or adopting provenance never causes a cloud write,
   media upload, media-key change or child media-version bump.
9. **Validation.** Any pair the desktop writes must satisfy the server rules
   above before it is sent; an invalid pair is not sent.

Older desktop clients without this support keep working: they preserve
provenance on unrelated edits, and the server invalidates it on their actual
coordinate changes. No image byte upload, media-key change or child
media-version bump is required merely to assign location provenance.

### Desktop implementation (sporely-py Stage 4)

How the desktop meets the requirements above, against sporely-web
`feature/photo-provenance` at `1029975`:

- **Storage.** SQLite `observations.location_source`/`location_status` and
  `images.ingest_method` are nullable TEXT, added by `database/schema.py`. The
  local pair is part of the coordinate bundle: `ObservationDB.update_observation`
  stores an explicitly passed pair (NULL included) and otherwise clears it
  whenever latitude or longitude actually changes (sync GPS tolerance), the
  local mirror of the server trigger. A stale pair therefore never describes
  new coordinates, and a writable local pair always means the desktop
  attributed the current coordinates.
- **Reads.** Observation reads select the pair, `gps_altitude` and
  `gps_accuracy`, and image reads select `ingest_method`, from the owner base
  tables; a target rejecting the provenance columns is re-read without that
  group once per client (as for identity provenance).
- **Snapshots and merge.** The baseline records the pair, altitude and
  accuracy only when the cloud row carries them. Comparison skips the pair
  entirely for a cloud row without the columns; a baseline without it uses the
  current cloud pair. The desktop stores no altitude or accuracy: a cloud
  change to them (against a baseline that recorded them) is a cloud bundle
  change, and their local absence is never a local change. Latitude,
  longitude, altitude, accuracy and the pair are one bundle: changed on both
  sides it is a conflict on the coordinates unless both sides hold the same
  latitude/longitude (shared; the cloud pair is taken). The local pair alone is never
  a local change: where local coordinates are not a pending change, a
  differing cloud pair is adopted by a local write only, including on the
  fast pull and in Download from Cloud. Applying cloud coordinates applies the
  cloud pair with them.
- **Attribution.** The Observation details dialog attributes a coordinate
  edit made in it: coordinates equal to those last applied from a photo's EXIF
  are `image_exif`/`acquired` — a new observation whose coordinates Prepare
  Images took from a photo imported in that session (the GPS the file's own
  EXIF held at import, not values typed over it in Prepare Images, of a file
  without a stored image row; stored or cloud-materialised images can carry
  Sporely-written GPS and never count) opens attributed to it, and that photo's
  EXIF stays the reference for later edits in the dialog; a deliberate clear of an existing
  observation is `none`/`skipped`; any other change (typed values, map link)
  is `manual`/`acquired`. A new observation whose coordinates were typed in
  Prepare Images (and are not that EXIF) opens `manual`/`acquired`.
  Unchanged coordinates keep the pair the opened
  record holds for them; a new observation saved without coordinates stays
  unattributed. An attribution survives an Edit Images detour (details dialog
  → Prepare Images → details dialog, for new and existing observations) while
  Prepare Images keeps those coordinates; if Prepare Images replaces them the
  change is of unknown origin, except that a new observation attributes them
  as above on reopening. Prepare Images itself and every other coordinate
  writer do not attribute.
- **Push.** An existing observation whose coordinates differ from the live
  cloud row and whose local pair is writable for them is written through
  `set_observation_location` (altitude and accuracy NULL — the desktop stores
  neither), and the following PATCH omits the coordinates. Otherwise the
  ordinary PATCH carries changed coordinates and the server invalidates the
  pair; coordinates equal to the live row are not resent. A missing RPC
  (PGRST202) falls back to the coordinate PATCH. A new observation INSERT
  carries a writable pair, retried once without it on a target lacking the
  columns. The baseline is refreshed from the cloud row after the push.
- **Conflict review.** The review classifies the coordinates with the same
  bundle rule (one-sided bundle changes are automatic decisions; a two-sided
  one lists latitude and longitude as one choice). A plan applies one side to
  both coordinates and refuses mixed sides. Taking the cloud side applies its
  pair with the coordinates; keeping this device's side pushes through the
  same aware path as ordinary sync (`set_observation_location` for an
  attributed location, coordinates equal to the cloud row not resent). The
  reviewed drift fingerprints include the local pair and the cloud pair,
  altitude and accuracy, so a change to any of them after review refuses the
  plan before any write. A completed coordinate operation records the bundle
  it left (the local pair after a pull; the cloud pair, altitude and accuracy
  read back after a push, whether from the RPC, server invalidation or the
  RPC fallback); a retry accepts exactly those effects, only on the side the
  operation wrote, and refuses any other change on either side.
- **Images.** `ingest_method` is adopted locally as received (absent key:
  unchanged) when an image is created or matched on pull, including
  metadata-only anchors. No image write payload (`_IMG_PUSH_COLS`, anchor
  payloads) contains it and it is outside the image snapshot and media
  signature, so it causes no upload, media-key change or version bump.
- **Download from Cloud.** `set_observation_location` is a blocked writer.
