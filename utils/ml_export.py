"""ML export utilities for generating training datasets."""
import json
import shutil
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional
from PIL import Image
from database.schema import get_connection
from app_identity import APP_NAME


def export_coco_format(
    output_dir: str,
    include_thumbnails: bool = False,
    thumbnail_size: str = '512x512'
) -> dict:
    """Export annotations in COCO format for ML training.

    Args:
        output_dir: Directory to save the exported dataset
        include_thumbnails: If True, use thumbnails instead of original images
        thumbnail_size: Size preset if using thumbnails

    Returns:
        Dictionary with export statistics
    """
    output_path = Path(output_dir)
    images_dir = output_path / "images"

    # Create output directories
    output_path.mkdir(parents=True, exist_ok=True)
    images_dir.mkdir(parents=True, exist_ok=True)

    # Get all images with annotations
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get unique images that have annotations
    cursor.execute('''
        SELECT DISTINCT i.id, i.filepath, i.observation_id, i.image_type,
               i.scale_microns_per_pixel
        FROM images i
        INNER JOIN spore_annotations sa ON i.id = sa.image_id
        ORDER BY i.id
    ''')
    images_data = cursor.fetchall()

    # Get all annotations
    cursor.execute('''
        SELECT sa.*, i.filepath, i.scale_microns_per_pixel
        FROM spore_annotations sa
        INNER JOIN images i ON sa.image_id = i.id
        ORDER BY sa.image_id, sa.spore_number
    ''')
    annotations_data = cursor.fetchall()
    conn.close()

    # Build COCO format structure
    coco_data = {
        "info": {
            "description": "Mushroom Spore Dataset",
            "version": "1.0",
            "year": datetime.now().year,
            "contributor": APP_NAME,
            "date_created": datetime.now().isoformat()
        },
        "licenses": [
            {
                "id": 1,
                "name": "Unknown",
                "url": ""
            }
        ],
        "categories": [
            {
                "id": 1,
                "name": "spore",
                "supercategory": "fungi"
            }
        ],
        "images": [],
        "annotations": []
    }

    # Track statistics
    stats = {
        "images_exported": 0,
        "annotations_exported": 0,
        "images_skipped": 0,
        "errors": []
    }

    # Map old image IDs to new sequential IDs
    image_id_map = {}
    new_image_id = 1

    # Process images
    for img_row in images_data:
        img_id = img_row['id']
        filepath = img_row['filepath']

        # Determine source file
        if include_thumbnails:
            source_path = _get_thumbnail_path(img_id, thumbnail_size)
            if not source_path:
                source_path = filepath
        else:
            source_path = filepath

        source_path = Path(source_path)

        if not source_path.exists():
            stats["images_skipped"] += 1
            stats["errors"].append(f"Image not found: {source_path}")
            continue

        # Get image dimensions
        try:
            with Image.open(source_path) as img:
                width, height = img.size
        except Exception as e:
            stats["images_skipped"] += 1
            stats["errors"].append(f"Could not read image {source_path}: {e}")
            continue

        # Copy image to output directory
        new_filename = f"image_{new_image_id:05d}{source_path.suffix}"
        dest_path = images_dir / new_filename

        try:
            shutil.copy2(source_path, dest_path)
        except Exception as e:
            stats["images_skipped"] += 1
            stats["errors"].append(f"Could not copy image: {e}")
            continue

        # Add to COCO images
        coco_data["images"].append({
            "id": new_image_id,
            "file_name": new_filename,
            "width": width,
            "height": height,
            "license": 1,
            "date_captured": datetime.now().isoformat()
        })

        image_id_map[img_id] = new_image_id
        new_image_id += 1
        stats["images_exported"] += 1

    # Process annotations
    annotation_id = 1
    for ann_row in annotations_data:
        old_image_id = ann_row['image_id']

        # Skip if image was not exported
        if old_image_id not in image_id_map:
            continue

        new_img_id = image_id_map[old_image_id]

        # COCO bbox format: [x, y, width, height]
        bbox = [
            ann_row['bbox_x'],
            ann_row['bbox_y'],
            ann_row['bbox_width'],
            ann_row['bbox_height']
        ]
        area = bbox[2] * bbox[3]

        coco_data["annotations"].append({
            "id": annotation_id,
            "image_id": new_img_id,
            "category_id": 1,  # spore
            "bbox": bbox,
            "area": area,
            "iscrowd": 0,
            # Extra fields for spore-specific data
            "attributes": {
                "center_x": ann_row['center_x'],
                "center_y": ann_row['center_y'],
                "length_um": ann_row['length_um'],
                "width_um": ann_row['width_um'],
                "rotation_angle": ann_row['rotation_angle'],
                "spore_number": ann_row['spore_number'],
                "annotation_source": ann_row['annotation_source']
            }
        })

        annotation_id += 1
        stats["annotations_exported"] += 1

    # Save COCO JSON
    annotations_file = output_path / "annotations.json"
    with open(annotations_file, 'w', encoding='utf-8') as f:
        json.dump(coco_data, f, indent=2)

    # Save statistics
    stats_file = output_path / "export_stats.json"
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)

    return stats


def _get_thumbnail_path(image_id: int, size_preset: str) -> Optional[str]:
    """Get thumbnail path from database."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('''
        SELECT filepath FROM thumbnails
        WHERE image_id = ? AND size_preset = ?
    ''', (image_id, size_preset))

    row = cursor.fetchone()
    conn.close()

    if row:
        return row['filepath']
    return None


def export_yolo_format(output_dir: str) -> dict:
    """Export annotations in YOLO format for training.

    YOLO format: <class_id> <x_center> <y_center> <width> <height>
    All values normalized to [0, 1]

    Args:
        output_dir: Directory to save the exported dataset

    Returns:
        Dictionary with export statistics
    """
    output_path = Path(output_dir)
    images_dir = output_path / "images"
    labels_dir = output_path / "labels"

    # Create directories
    output_path.mkdir(parents=True, exist_ok=True)
    images_dir.mkdir(parents=True, exist_ok=True)
    labels_dir.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get images with annotations
    cursor.execute('''
        SELECT DISTINCT i.id, i.filepath
        FROM images i
        INNER JOIN spore_annotations sa ON i.id = sa.image_id
    ''')
    images_data = cursor.fetchall()

    stats = {
        "images_exported": 0,
        "annotations_exported": 0,
        "errors": []
    }

    for img_row in images_data:
        img_id = img_row['id']
        filepath = Path(img_row['filepath'])

        if not filepath.exists():
            stats["errors"].append(f"Image not found: {filepath}")
            continue

        # Get image dimensions
        try:
            with Image.open(filepath) as img:
                img_width, img_height = img.size
        except Exception as e:
            stats["errors"].append(f"Could not read image: {e}")
            continue

        # Copy image
        new_filename = f"image_{img_id:05d}{filepath.suffix}"
        shutil.copy2(filepath, images_dir / new_filename)

        # Get annotations for this image
        cursor.execute('''
            SELECT bbox_x, bbox_y, bbox_width, bbox_height
            FROM spore_annotations
            WHERE image_id = ?
        ''', (img_id,))
        annotations = cursor.fetchall()

        # Write YOLO label file
        label_filename = f"image_{img_id:05d}.txt"
        with open(labels_dir / label_filename, 'w') as f:
            for ann in annotations:
                # Convert to YOLO format (normalized center + dimensions)
                x_center = (ann['bbox_x'] + ann['bbox_width'] / 2) / img_width
                y_center = (ann['bbox_y'] + ann['bbox_height'] / 2) / img_height
                width = ann['bbox_width'] / img_width
                height = ann['bbox_height'] / img_height

                # Class 0 = spore
                f.write(f"0 {x_center:.6f} {y_center:.6f} {width:.6f} {height:.6f}\n")
                stats["annotations_exported"] += 1

        stats["images_exported"] += 1

    conn.close()

    # Write classes file
    with open(output_path / "classes.txt", 'w') as f:
        f.write("spore\n")

    # Write dataset.yaml for YOLO training
    yaml_content = f"""# Spore Detection Dataset
path: {output_path.absolute()}
train: images
val: images

names:
  0: spore
"""
    with open(output_path / "dataset.yaml", 'w') as f:
        f.write(yaml_content)

    return stats


def get_export_summary() -> dict:
    """Get summary of available data for export.

    Returns:
        Dictionary with counts of images and annotations
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Count images with annotations
    cursor.execute('''
        SELECT COUNT(DISTINCT image_id) FROM spore_annotations
    ''')
    images_with_annotations = cursor.fetchone()[0]

    # Count total annotations
    cursor.execute('SELECT COUNT(*) FROM spore_annotations')
    total_annotations = cursor.fetchone()[0]

    # Count total images
    cursor.execute('SELECT COUNT(*) FROM images')
    total_images = cursor.fetchone()[0]

    # Count thumbnails
    cursor.execute('SELECT COUNT(*) FROM thumbnails')
    total_thumbnails = cursor.fetchone()[0]

    conn.close()

    return {
        "total_images": total_images,
        "images_with_annotations": images_with_annotations,
        "total_annotations": total_annotations,
        "total_thumbnails": total_thumbnails
    }
