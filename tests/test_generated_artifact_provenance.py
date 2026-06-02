import pytest

from utils.generated_artifact_provenance import build_generated_artifact_provenance


def test_thumbnail_descriptor_uses_generated_artifact_role():
    descriptor = build_generated_artifact_provenance(
        file_purpose="thumbnail",
        render_preset="gallery-small",
    )

    assert descriptor["source_role"] == "generated_artifact"
    assert descriptor["file_purpose"] == "thumbnail"
    assert descriptor["render_preset"] == "gallery-small"


def test_spore_crop_descriptor_preserves_audit_geometry():
    descriptor = build_generated_artifact_provenance(
        file_purpose="spore_crop",
        source_image_id=12,
        measurement_id=34,
        annotation_id=56,
        source_width=2048,
        source_height=1536,
        crop_bbox=(10, 20, 110, 220),
        rotation_angle=45,
        render_preset="evidence",
    )

    assert descriptor["source_role"] == "generated_artifact"
    assert descriptor["file_purpose"] == "spore_crop"
    assert descriptor["source_image_id"] == 12
    assert descriptor["measurement_id"] == 34
    assert descriptor["annotation_id"] == 56
    assert descriptor["source_width"] == 2048
    assert descriptor["source_height"] == 1536
    assert descriptor["crop_bbox"] == (10.0, 20.0, 110.0, 220.0)
    assert descriptor["rotation_angle"] == 45.0
    assert descriptor["render_preset"] == "evidence"


def test_plot_descriptor_can_omit_source_image():
    descriptor = build_generated_artifact_provenance(
        file_purpose="plot",
        measurement_id=99,
        render_preset="analysis",
    )

    assert descriptor["source_role"] == "generated_artifact"
    assert descriptor["file_purpose"] == "plot"
    assert descriptor["source_image_id"] is None
    assert descriptor["measurement_id"] == 99
    assert descriptor["annotation_id"] is None


def test_reference_descriptor_normalizes_common_variants():
    descriptor = build_generated_artifact_provenance(
        file_purpose="  Reference  ",
    )

    assert descriptor["file_purpose"] == "reference"


def test_invalid_file_purpose_is_rejected():
    with pytest.raises(ValueError, match="Unknown generated artifact file_purpose"):
        build_generated_artifact_provenance(file_purpose="not-a-real-purpose")


def test_helper_is_pure_and_does_not_mutate_metadata(tmp_path):
    metadata = {"origin": "ui", "nested": {"kind": "export"}}
    descriptor = build_generated_artifact_provenance(
        file_purpose="reference",
        metadata=metadata,
    )

    assert metadata == {"origin": "ui", "nested": {"kind": "export"}}
    assert descriptor["metadata"] == metadata
    assert descriptor["metadata"] is not metadata
    assert list(tmp_path.iterdir()) == []
