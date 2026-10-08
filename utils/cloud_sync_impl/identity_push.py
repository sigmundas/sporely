"""Taxonomy identity push and explicit stale-identity clear, mixed into
``SporelyCloudClient``; the client class stays in the facade.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

from utils.taxon_identity import (
    STATE_NONE,
    TaxonIdentity,
)

from utils.cloud_sync_impl.errors import CloudSyncError
from utils.cloud_sync_impl.logs import logger
from utils.cloud_sync_impl.reconciliation.identity import (
    _baseline_identity_key,
    _identification_key,
    _IDENTITY_BASELINE_UNKNOWN,
    _remote_identity_claim,
)
from utils.cloud_sync_impl.reconciliation.values import _normalize_observation_int_value


class CloudSyncTaxonIdentityMixin:
    """Taxonomy identity push methods of ``SporelyCloudClient``."""

    def _sync_observation_selected_taxon(
        self,
        cloud_id: str,
        obs: dict,
        *,
        remote_obs: dict | None,
        baseline_obs: dict | None = None,
    ) -> None:
        """Forward an authoritative desktop selection through the guarded RPC.

        A missing local value is deliberately not inferred from genus/species
        text and does not erase cloud identity.  The RPC is skipped when the
        remote row already carries the same exact selection.

        Taxonomy-v2 closeout Stage 2: the proof standard is provenance, not
        sign. Previously any positive integer in ``sporely_taxon_id`` was
        asserted to the cloud as an owner-selected Sporely identity. The
        deployed RPC does validate active-release membership, so an arbitrary
        external integer is rejected server-side — but it cannot distinguish
        an external integer that *numerically collides* with a real Sporely ID
        in the active release, and no server-side check ever could. That
        residual case is closed here, by refusing to emit anything whose
        producer is not recorded as proof.

        Refusing is normally a skip, not a clear: an unproven or unresolved
        local identity is not by itself evidence that the cloud's identity is
        wrong, so the source evidence and any existing cloud selection both
        survive. Sync-integrity follow-up 4
        (docs/plans/active/2026-09-25-sync-integrity-follow-ups.md) adds one
        narrow exception: when ``baseline_obs`` proves the desktop itself last
        synced a proven Sporely identity for this observation, and the
        committed identification has since changed locally away from it, the
        absence of a current local identity is the desktop's own evidence that
        the user deliberately abandoned that identity — not merely unproven
        state. That case issues an explicit clear through the same atomic RPC
        the web uses for a coupled identity+name change, never a bare PATCH of
        taxonomy columns.
        """
        identity = TaxonIdentity.from_row(obs)
        if not identity.is_proven_sporely:
            if identity.state == STATE_NONE and self._maybe_clear_stale_cloud_identity(
                cloud_id, obs, remote_obs=remote_obs, baseline_obs=baseline_obs,
            ):
                return
            if identity.sporely_taxon_id is not None or identity.has_external_evidence:
                logger.info(
                    "cloud sync: skipping taxonomy identity for observation %s — "
                    "state=%s proof=%s source=%s namespace=%s external_id=%s; "
                    "only a proven Sporely identity may reach "
                    "set_observation_selected_taxon_v2",
                    obs.get('id'),
                    identity.state,
                    identity.identity_proof,
                    identity.source_system,
                    identity.namespace,
                    identity.external_id,
                )
            return
        taxon_id = identity.sporely_taxon_id
        remote_taxon_id = _normalize_observation_int_value(
            (remote_obs or {}).get('selected_sporely_taxon_id')
        )
        if remote_taxon_id == taxon_id:
            return
        self.set_observation_selected_taxon(cloud_id, taxon_id)

    def _maybe_clear_stale_cloud_identity(
        self,
        cloud_id: str,
        obs: dict,
        *,
        remote_obs: dict | None,
        baseline_obs: dict | None,
    ) -> bool:
        """Explicit-clear check for a local identity that reads as none.

        All of these must hold, mirroring
        docs/plans/active/2026-09-25-sync-integrity-follow-ups.md item 4:

        1. the current local committed identity is none (caller already
           checked ``identity.state == STATE_NONE``);
        2. the stored sync baseline shows the previously synchronized
           observation held a non-empty *selected Sporely* identity — not any
           identity: a baseline external/legacy value is not something this
           desktop ever asserted through the guarded RPC, so it is not this
           desktop's claim to withdraw;
        3. the committed identification (genus/species) has changed relative
           to that same baseline;
        4. that change is a real identification edit, not bookkeeping — (3)
           already establishes that by comparing the fields that name the
           taxon, not an unrelated field;
        5. the cloud's CURRENT selected identity is either empty or still
           equals that same baseline — never a third value. A cloud identity
           that moved to something else since the baseline is not this
           desktop's stale value to overwrite; the clear is withheld and the
           disagreement logged (see the fail-closed branch below).

        With no baseline (``baseline_obs`` is ``None``, or the stored snapshot
        predates identity joining change detection), nothing is inferred: a
        legacy/no-identity row with no proof of a prior selection is left
        alone, per the fail-closed rule.

        Returns ``True`` once the situation is handled (either a clear was
        issued, or the cloud already agrees), so the caller does not fall
        through to the ordinary "skip and log" path for what is actually a
        deliberate clear.
        """
        if baseline_obs is None:
            return False
        baseline_key = _baseline_identity_key(baseline_obs)
        if baseline_key is _IDENTITY_BASELINE_UNKNOWN or not baseline_key.startswith('sporely:'):
            return False
        if _identification_key(obs) == _identification_key(dict(baseline_obs or {})):
            # Nothing about the committed identification changed locally —
            # an unrelated edit (notes, location, habitat, …) must never
            # clear a cloud identity the user never touched.
            return False
        remote_claim = _remote_identity_claim(remote_obs)
        if remote_claim is not None and remote_claim.key == '':
            # The cloud already has no identity (e.g. a previous clear
            # already landed): nothing left to do, and definitely not a
            # repeated RPC call on every subsequent sync.
            return True
        if remote_claim is not None and remote_claim.key and remote_claim.key != baseline_key:
            # The cloud's CURRENT selected identity is neither empty nor the
            # baseline this desktop last synced — something else (another
            # client, the web) rebound the concept since. Clearing it would
            # wipe that other write's name/identity and withdraw ITS shared
            # reference contributions on the strength of a local rename this
            # desktop made against a now-stale baseline. Ordinarily this
            # exact case is already intercepted upstream: a genuine local
            # identification change together with a remote identity change
            # classifies as `_classify_identity_sync_change(...) ==
            # 'conflict'`, which blocks the whole observation push before
            # `push_observation` is ever reached (see push_all). This check
            # is a second, independent fail-closed gate in case that
            # upstream block is bypassed or this method is ever reached from
            # a different caller, so a stale local baseline can never
            # overwrite a cloud identity it never agreed with.
            logger.warning(
                "cloud sync: identity clear withheld for observation %s — "
                "cloud selected %s does not match the sync baseline %s; "
                "fail closed instead of overwriting a possibly newer remote "
                "selection (or its shared-reference contributions)",
                obs.get('id'), remote_claim.key, baseline_key,
            )
            return True
        genus = str(obs.get('genus') or '').strip() or None
        species = str(obs.get('species') or '').strip() or None
        common_name = str(obs.get('common_name') or '').strip() or None
        logger.info(
            "cloud sync: explicit identity clear for observation %s — "
            "baseline=%s committed identification changed locally; "
            "clearing via set_observation_identification_v2",
            obs.get('id'), baseline_key,
        )
        self.clear_observation_selected_taxon(
            cloud_id, genus=genus, species=species, common_name=common_name,
        )
        return True

    def _verify_identity_clear_landed(self, cloud_id: str) -> None:
        """Read back the row and confirm the clear actually took effect.

        Stage C review round 2, item 3: runtime-proven that
        ``observation_taxon_shared_reference_rate_row_trg``
        (sporely-web migration 20260830193144) can cancel THIS row's UPDATE
        of ``selected_sporely_taxon_id`` — a per-user, per-minute rate limit
        on the same trigger family the reference-library sync RPCs share —
        while the statement-level guard reports it correctly as HTTP 429.
        Reproduced end to end: a persistently rate-limited caller gets a real
        ``CloudTemporarilyUnavailableError`` (never a false success) and the
        observation stays locally dirty for retry, so the row-suppression
        itself was never observed to be silently reported as a success by
        the RPC call. This read-back is the narrow, desktop-side defence for
        the residual case regardless: if ``set_observation_identification_v2``
        ever returns without raising while the row's own
        ``selected_sporely_taxon_id`` still shows a value, that is
        indistinguishable from a silently dropped clear, so it must be
        treated exactly like one — fail closed (``CloudSyncError``, caught by
        the same per-observation handling every other push failure uses),
        never advance sync state or a baseline as if the clear had landed.

        Stage C review round 3, finding 3 broadens the same fail-closed
        standard to the read-back call itself, not just its result:

        - A transport failure while reading back (network error, non-2xx,
          decode failure) is indistinguishable from "the clear may or may
          not have landed" — it must fail closed exactly like a confirmed
          still-attached identity, not silently pass through.
        - A malformed or empty response for a row that MUST exist (the RPC
          just ran against this exact ``cloud_id`` without raising) is
          never treated as "confirmed cleared" — only a well-formed row
          that explicitly reports ``selected_sporely_taxon_id`` as empty
          counts as a confirmed clear. A response missing that key
          entirely (or not shaped like a row at all) fails closed too,
          the same as a genuine concurrency loss (e.g. the row was
          deleted between the RPC and this read-back).
        """
        try:
            verified = self.get_observation(cloud_id)
        except Exception as exc:
            raise CloudSyncError(
                f'cloud {cloud_id}: identity clear did not take effect — could '
                f'not verify the cloud row after set_observation_identification_v2 '
                f'({exc}). Refusing to advance sync state as if it succeeded.'
            ) from exc
        if not isinstance(verified, dict) or 'selected_sporely_taxon_id' not in verified:
            raise CloudSyncError(
                f'cloud {cloud_id}: identity clear did not take effect — the '
                f'read-back after set_observation_identification_v2 returned an '
                f'empty or malformed row ({verified!r}). Refusing to advance '
                f'sync state as if the clear succeeded.'
            )
        still_selected = _normalize_observation_int_value(verified.get('selected_sporely_taxon_id'))
        if still_selected is not None:
            raise CloudSyncError(
                f'cloud {cloud_id}: identity clear did not take effect — '
                f'the cloud row still reports selected_sporely_taxon_id='
                f'{still_selected!r} after set_observation_identification_v2 '
                f'returned. Refusing to advance sync state as if it succeeded '
                f'(a rate-limit or similar row-suppression trigger may have '
                f'cancelled the write).'
            )
