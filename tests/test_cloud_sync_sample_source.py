"""Cloud sync round-trip for the sample_source image field (Stage 2B).

Locks the invariants that keep sample_type / sample_source correctly split
across the desktop ↔ cloud boundary now that sporely-web's Stage 2A
migration has added `observation_images.sample_source`:

  * `_IMG_PUSH_COLS` and `_SNAPSHOT_IMG_FIELDS` include sample_source.
  * `_split_legacy_sample_type_into_source` promotes stale
    `sample_type='Spore_print'` rows to `sample_source='Spore_print'`
    without duplicating the value on the condition column.
  * `_apply_image_sample_fields_to_push_payload` sends whatever the local
    has AND does a null-safe merge — an empty local value can never wipe a
    non-empty cloud value during an unrelated metadata patch.
  * The pull-side `_remote_image_payload` carries sample_source into the
    snapshot / update path.
  * `sample_type` never contains `Spore_print` after normalization —
    condition stays Fresh/Dried/None.
"""

from __future__ import annotations

import pytest

from utils import cloud_sync
from utils.cloud_sync import (
    _IMG_PUSH_COLS,
    _SNAPSHOT_IMG_FIELDS,
    _apply_image_sample_fields_to_push_payload,
    _cloud_to_desktop_sample_source,
    _desktop_to_cloud_sample_source,
    _remote_image_payload,
    _split_legacy_sample_type_into_source,
)


# ---------------------------------------------------------------------------
# Field lists
# ---------------------------------------------------------------------------


def test_image_push_cols_include_sample_source():
    assert "sample_source" in _IMG_PUSH_COLS
    # And sample_type is still there (condition-only now).
    assert "sample_type" in _IMG_PUSH_COLS


def test_snapshot_img_fields_include_sample_source():
    assert "sample_source" in _SNAPSHOT_IMG_FIELDS
    assert "sample_type" in _SNAPSHOT_IMG_FIELDS


def test_image_metadata_only_fields_include_sample_source():
    """sample_source is a descriptive tag — it must patch via metadata-only
    sync, not force a byte re-upload."""
    from utils.cloud_sync import _IMAGE_METADATA_ONLY_FIELDS
    assert "sample_source" in _IMAGE_METADATA_ONLY_FIELDS
    assert "sample_type" in _IMAGE_METADATA_ONLY_FIELDS


# ---------------------------------------------------------------------------
# Legacy split helper
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["Spore_print", "spore_print", "spore print", "Print", "print"])
def test_legacy_sample_type_promoted_to_sample_source(raw):
    condition, source = _split_legacy_sample_type_into_source(raw, None)
    assert condition is None, (
        f"Legacy sample_type={raw!r} must be cleared from the condition column"
    )
    assert source == "Spore_print", (
        f"Legacy sample_type={raw!r} must land under sample_source; got {source!r}"
    )


def test_split_prefers_explicit_source_over_legacy_condition():
    condition, source = _split_legacy_sample_type_into_source("Spore_print", "Hymenium")
    # Legacy sample_type is dropped; the explicit source wins.
    assert condition is None
    assert source == "Hymenium"


def test_split_leaves_valid_condition_alone():
    condition, source = _split_legacy_sample_type_into_source("Fresh", "Stipe")
    assert condition == "Fresh"
    assert source == "Stipe"


def test_split_treats_not_set_as_none():
    condition, source = _split_legacy_sample_type_into_source("Not_set", "Not_set")
    assert condition is None
    assert source is None


def test_split_null_inputs_return_none():
    condition, source = _split_legacy_sample_type_into_source(None, None)
    assert condition is None
    assert source is None


def test_split_never_puts_spore_print_back_on_condition():
    """Even if a caller passes Spore_print via `sample_source`, condition must
    never end up as Spore_print again."""
    for raw_type in ("Spore_print", None, "", "Fresh"):
        condition, source = _split_legacy_sample_type_into_source(raw_type, "Spore_print")
        assert condition != "Spore_print"
        assert condition in {None, "Fresh"}
        assert source == "Spore_print"


# ---------------------------------------------------------------------------
# Push payload normalizer — null-safe merge + capability gate
# ---------------------------------------------------------------------------


class _FakeCloudClient:
    def __init__(self, *, supports_sample_source: bool, remote_sample_source: str | None = None):
        self._supports_sample_source = supports_sample_source
        self._remote_sample_source = remote_sample_source
        self.user_id = "user-test"
        self.get_calls: list[str] = []

    def _observation_images_support_sample_source(self):
        return self._supports_sample_source

    def _get(self, path):
        self.get_calls.append(path)
        if self._remote_sample_source is None:
            return []
        return [{"sample_source": self._remote_sample_source}]


def test_push_payload_forwards_local_sample_source():
    """Local desktop stores Title_Case (`Hymenium`); cloud canonical is
    lowercase snake_case (`hymenium`). Push must translate at the boundary."""
    payload = {"sample_type": "Fresh", "sample_source": "Hymenium"}
    image_row = {"id": 42, "sample_type": "Fresh", "sample_source": "Hymenium"}
    client = _FakeCloudClient(supports_sample_source=True)
    _apply_image_sample_fields_to_push_payload(
        payload, image_row, client=client, obs_cloud_id="cloud-obs"
    )
    assert payload["sample_type"] == "Fresh"
    assert payload["sample_source"] == "hymenium", (
        f"Push must send the lowercase cloud canonical; got {payload['sample_source']!r}"
    )
    # No remote lookup needed when local has an explicit value.
    assert client.get_calls == []


def test_push_payload_null_local_omits_sample_source_field():
    """The null-safe merge: when local is empty, omit `sample_source` from the
    payload entirely. On PATCH, PostgREST leaves the existing cloud value
    alone; on POST (new row), the column defaults to NULL. Either way, no
    unrelated metadata push can wipe the cloud sample_source, and no extra
    HTTP round-trip is needed to check the remote first."""
    payload = {"sample_type": None, "sample_source": None}
    image_row = {"id": 42, "sample_type": None, "sample_source": None}
    client = _FakeCloudClient(supports_sample_source=True)
    _apply_image_sample_fields_to_push_payload(
        payload, image_row, client=client, obs_cloud_id="cloud-obs"
    )
    assert "sample_source" not in payload, (
        "Local NULL sample_source must be omitted from the payload so "
        "PATCH leaves the cloud value untouched"
    )
    # No remote lookup happens — the omit-on-null strategy avoids the HTTP GET.
    assert client.get_calls == []


def test_push_payload_null_local_when_column_missing_still_omits():
    payload = {"sample_type": None, "sample_source": None}
    image_row = {"id": 42, "sample_type": None, "sample_source": None}
    client = _FakeCloudClient(supports_sample_source=False)
    _apply_image_sample_fields_to_push_payload(
        payload, image_row, client=client, obs_cloud_id="cloud-obs"
    )
    assert "sample_source" not in payload


def test_push_payload_legacy_spore_print_migrates_to_source():
    """Local desktop with stale sample_type='Spore_print' pushes as source,
    using the lowercase cloud canonical."""
    payload = {"sample_type": "Spore_print", "sample_source": None}
    image_row = {"id": 42, "sample_type": "Spore_print", "sample_source": None}
    client = _FakeCloudClient(supports_sample_source=True)
    _apply_image_sample_fields_to_push_payload(
        payload, image_row, client=client, obs_cloud_id="cloud-obs"
    )
    assert payload["sample_type"] is None, (
        "Legacy Spore_print must be cleared from sample_type before push"
    )
    # The legacy value was promoted to sample_source AND translated to the
    # lowercase cloud canonical.
    assert payload["sample_source"] == "spore_print"


def test_push_payload_drops_sample_source_when_cloud_lacks_column():
    """Older cloud deployment without the column: don't send sample_source."""
    payload = {"sample_type": "Fresh", "sample_source": "Hymenium"}
    image_row = {"id": 42, "sample_type": "Fresh", "sample_source": "Hymenium"}
    client = _FakeCloudClient(supports_sample_source=False)
    _apply_image_sample_fields_to_push_payload(
        payload, image_row, client=client, obs_cloud_id="cloud-obs"
    )
    assert "sample_source" not in payload, (
        "Cloud that hasn't migrated must not receive sample_source in the payload"
    )
    assert payload["sample_type"] == "Fresh"


# ---------------------------------------------------------------------------
# Pull-side snapshot payload
# ---------------------------------------------------------------------------


def test_remote_image_payload_normalizes_cloud_lowercase_to_desktop_titlecase():
    """Cloud sends lowercase (`spore_print`); the desktop-facing payload
    must present the desktop canonical (`Spore_print`) so downstream diff
    comparisons don't see a false mismatch."""
    remote = {
        "id": "cloud-img-1",
        "desktop_id": 42,
        "image_type": "microscope",
        "sample_type": None,
        "sample_source": "spore_print",  # cloud canonical
    }
    payload = _remote_image_payload(remote)
    assert payload["sample_source"] == "Spore_print"
    assert payload["sample_type"] is None


def test_remote_image_payload_accepts_legacy_titlecase_from_cloud():
    """Older cloud clients that emitted Title_Case still round-trip correctly."""
    remote = {
        "id": "cloud-img-3",
        "desktop_id": 44,
        "image_type": "microscope",
        "sample_type": None,
        "sample_source": "Spore_print",  # legacy Title_Case
    }
    payload = _remote_image_payload(remote)
    assert payload["sample_source"] == "Spore_print"


def test_remote_image_payload_preserves_condition_and_source_together():
    remote = {
        "id": "cloud-img-2",
        "desktop_id": 43,
        "image_type": "microscope",
        "sample_type": "Fresh",
        "sample_source": "hymenium",  # cloud lowercase
    }
    payload = _remote_image_payload(remote)
    assert payload["sample_type"] == "Fresh"
    assert payload["sample_source"] == "Hymenium"


# ---------------------------------------------------------------------------
# Never reintroduce Spore_print as a condition
# ---------------------------------------------------------------------------


def test_sample_type_options_do_not_include_spore_print():
    """Guardrail: even after this cloud-sync change lands, the local tag list
    for the specimen-condition dropdown must not list Spore_print."""
    from database.database_tags import DatabaseTerms
    assert "Spore_print" not in DatabaseTerms.SAMPLE_TYPES
    assert "Spore_print" in DatabaseTerms.SAMPLE_SOURCES


# ---------------------------------------------------------------------------
# Boundary converters — canonical cloud = lowercase snake_case
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "desktop_value,expected_cloud",
    [
        ("Spore_print", "spore_print"),
        ("Hymenium", "hymenium"),
        ("Stipe", "stipe"),
        ("Pileus", "pileus"),
        ("Context", "context"),
        ("Other", "other"),
    ],
)
def test_desktop_to_cloud_lowercases_canonical(desktop_value, expected_cloud):
    assert _desktop_to_cloud_sample_source(desktop_value) == expected_cloud


@pytest.mark.parametrize(
    "raw,expected_cloud",
    [
        ("Print", "spore_print"),         # compact pill label
        ("spore print", "spore_print"),   # display form
        ("sporeprint", "spore_print"),    # smashed
        ("spore_print", "spore_print"),   # already canonical cloud form
    ],
)
def test_desktop_to_cloud_maps_legacy_spore_print_variants(raw, expected_cloud):
    """The compact-pill 'Print' label, older display strings, and the
    already-lowercase form all map to the canonical cloud value."""
    assert _desktop_to_cloud_sample_source(raw) == expected_cloud


@pytest.mark.parametrize("raw", [None, "", "  ", "Not_set", "gibberish"])
def test_desktop_to_cloud_returns_none_for_empty_or_unknown(raw):
    assert _desktop_to_cloud_sample_source(raw) is None


@pytest.mark.parametrize(
    "cloud_value,expected_desktop",
    [
        ("spore_print", "Spore_print"),
        ("hymenium", "Hymenium"),
        ("stipe", "Stipe"),
        ("pileus", "Pileus"),
        ("context", "Context"),
        ("other", "Other"),
    ],
)
def test_cloud_to_desktop_titlecases_canonical(cloud_value, expected_desktop):
    """Cloud → local translation: whatever case the cloud sent, the local
    row ends up in desktop canonical Title_Case."""
    assert _cloud_to_desktop_sample_source(cloud_value) == expected_desktop


@pytest.mark.parametrize(
    "legacy",
    ["Spore_print", "Hymenium", "STIPE", "spore print", "spore_print"],
)
def test_cloud_to_desktop_accepts_both_case_variants(legacy):
    """Pull is tolerant: whether cloud sent lowercase or legacy Title_Case,
    the local row lands in a canonical desktop value."""
    result = _cloud_to_desktop_sample_source(legacy)
    assert result is not None
    from database.database_tags import DatabaseTerms
    assert result in DatabaseTerms.SAMPLE_SOURCES


@pytest.mark.parametrize("raw", [None, "", "  ", "Not_set"])
def test_cloud_to_desktop_returns_none_for_empty(raw):
    assert _cloud_to_desktop_sample_source(raw) is None


# ---------------------------------------------------------------------------
# End-to-end round-trip (both case variants)
# ---------------------------------------------------------------------------


def test_pull_then_push_round_trip_from_lowercase_cloud():
    """Round-trip: cloud → desktop → cloud. The value stays canonical
    lowercase on cloud after a pull-then-push cycle."""
    # Cloud sends lowercase.
    desktop_value = _cloud_to_desktop_sample_source("spore_print")
    assert desktop_value == "Spore_print"

    # Desktop UI displays Title_Case; push sends lowercase back.
    payload = {"sample_type": None}
    image_row = {"id": 1, "sample_type": None, "sample_source": desktop_value}
    client = _FakeCloudClient(supports_sample_source=True)
    _apply_image_sample_fields_to_push_payload(
        payload, image_row, client=client, obs_cloud_id="cloud-obs"
    )
    assert payload["sample_source"] == "spore_print"


def test_pull_then_push_round_trip_from_titlecase_cloud():
    """Same round-trip when the cloud row happens to have the legacy
    Title_Case form — desktop still lands on Title_Case locally, and the
    push re-normalizes to lowercase cloud canonical."""
    desktop_value = _cloud_to_desktop_sample_source("Spore_print")
    assert desktop_value == "Spore_print"

    payload = {"sample_type": None}
    image_row = {"id": 1, "sample_type": None, "sample_source": desktop_value}
    client = _FakeCloudClient(supports_sample_source=True)
    _apply_image_sample_fields_to_push_payload(
        payload, image_row, client=client, obs_cloud_id="cloud-obs"
    )
    assert payload["sample_source"] == "spore_print"
