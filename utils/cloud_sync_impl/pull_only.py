"""Pull-only (Download from Cloud) client boundary: method registries and the fail-closed wrapper.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S3 of the cloud-sync extraction.
"""

from __future__ import annotations


from utils.cloud_sync_impl.errors import PullOnlyModeError


_PULL_ONLY_BLOCKED_CLIENT_METHODS = frozenset({
    '_patch', '_post', '_delete', '_storage_remove',
    'push_observation', 'push_image_metadata', 'push_measurement',
    'upload_image_file', 'upload_original_image_file',
    'set_image_storage_path', 'set_image_desktop_id', 'set_desktop_id',
    'set_observation_selected_taxon', 'clear_observation_selected_taxon',
    'set_measurement_desktop_id', 'set_image_original_storage_path',
    'reserve_image_storage_path_for_promotion',
    'release_image_storage_path_reservation',
    'soft_delete_image', 'delete_cloud_observation',
    'delete_cloud_measurements_for_image',
    'push_calibration_reference_image', 'push_calibration_metadata',
    'sync_reference_work', 'sync_reference_taxon_treatment',
    'sync_reference_measurement_set', 'sync_observation_reference_use',
    'submit_private_reference_for_curation', 'share_reference_contribution',
    'withdraw_reference_contribution', 'sync_reference_curated_fork',
    # Stage M device report: refreshes the owner's device record.
    'record_reference_client_capabilities',
    # Default-on reference sharing (owner set list, stop, share again).
    # The two writes are owner writes; the list is owner-only, rate-limited
    # and never needed by a download, so it is blocked too rather than added
    # to the read allowlist.
    'list_my_reference_sharing',
    'stop_sharing_reference_set',
    'share_reference_set_again',
})


# Callable methods on the wrapped client that Download-from-Cloud is
# permitted to invoke. Anything not on this allowlist is treated as
# potentially write-touching and blocked at the wrapper. Adding a new
# read method here is an explicit choice; a new writer method is never
# safe to add.
_PULL_ONLY_ALLOWED_READ_METHODS = frozenset({
    # Session / identity — read side of auth, never mutate cloud state.
    'fetch_current_user_id',
    'fetch_cloud_plan_profile',
    'save_credentials',            # writes local settings, no cloud write
    '_refresh_session_if_possible', # refresh token, no cloud data mutation
    # Observation reads
    'list_remote_observations',
    'get_observation',
    # Calibration reads
    'list_remote_calibrations',
    'find_remote_calibration',
    'list_reference_works',
    'list_reference_taxon_treatments',
    'list_reference_measurement_sets',
    'list_observation_reference_uses',
    'search_public_curated_reference_sets',
    'get_public_curated_reference_set',
    'search_public_reference_contributions_v2',
    'get_public_reference_contribution_v2',
    'list_reference_curated_forks',
    'list_reference_client_devices',
    '_list_reference_library_feed',
    # Image / measurement metadata reads
    'pull_bulk_image_metadata',
    'pull_image_metadata',
    'pull_measurements_for_images',
    'pull_observation_identifications',
    # Media byte reads
    'download_image_file',
    'download_image_file_read_only',
    # Low-level GET helpers. RPC calls are gated by function name below.
    '_get',
    'get_read_only',
    # Client-internal probes and path builders — pure read/compute.
    '_find_cloud_image',
    '_get_media_worker',
    '_build_original_storage_path',
    '_observation_images_support_ai_crop',
    '_observation_images_support_ai_crop_custom',
    '_observation_images_support_sample_source',
    '_observation_images_support_upload_metadata',
    '_using_default_r',
    'list_image_changes_since',
    'list_measurement_changes_since',
})


_PULL_ONLY_ALLOWED_RPC_NAMES = frozenset({
    'search_community_spore_datasets',
    'get_community_spore_dataset',
    'community_spore_taxon_summary',
    'search_public_reference_values',
    'get_public_observation',
    'search_public_curated_reference_sets',
    'get_public_curated_reference_set',
    'search_public_reference_contributions_v2',
    'get_public_reference_contribution_v2',
    # Stage M owner feed: read-only, does not refresh the device record.
    'list_reference_library_feed',
})


class PullOnlyCloudClient:
    """Fail-closed proxy over ``SporelyCloudClient`` for Download from Cloud.

    Delegation rules:

    * Non-callable attributes on the wrapped client (``user_id``,
      ``access_token``, …) forward verbatim.
    * Callable methods on the read allowlist forward verbatim.
    * Every named writer method raises :class:`PullOnlyModeError` and
      records the attempt on ``write_attempts``.
    * **Any other callable** on the wrapped client — including methods
      not yet named on either list — also raises. Under a plain
      denylist, a future or unrecognized method whose internals call
      ``self._patch`` would execute on the wrapped client and bypass
      the wrapper; the allowlist prevents that class of leak entirely.
    """

    is_pull_only = True

    def __init__(self, wrapped) -> None:
        self._wrapped = wrapped
        self.write_attempts: list[str] = []

    def _block(self, name: str, reason: str):
        def _blocked(*_args, **_kwargs):
            self.write_attempts.append(name)
            raise PullOnlyModeError(
                f"{reason} '{name}' is not allowed during Download from Cloud"
            )
        return _blocked

    def _rpc(self, function_name: str, payload: dict | None = None):
        rpc_name = str(function_name or '').strip()
        if rpc_name not in _PULL_ONLY_ALLOWED_RPC_NAMES:
            attempt = f'_rpc:{rpc_name or "<missing>"}'
            self.write_attempts.append(attempt)
            raise PullOnlyModeError(
                f"Unrecognized or write-capable RPC '{rpc_name}' is not allowed "
                "during Download from Cloud"
            )
        return self._wrapped._rpc(rpc_name, payload)

    def __getattr__(self, name: str):
        # __getattr__ only fires for attributes not found on ``self``; the
        # explicit ``is_pull_only`` / ``_wrapped`` / ``write_attempts``
        # attributes shadow this path.
        if name in _PULL_ONLY_BLOCKED_CLIENT_METHODS:
            return self._block(name, "Cloud write")
        attr = getattr(self._wrapped, name)
        if not callable(attr):
            return attr
        if name in _PULL_ONLY_ALLOWED_READ_METHODS:
            return attr
        return self._block(name, "Unrecognized client method")
