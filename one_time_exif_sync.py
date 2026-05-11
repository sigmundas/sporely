"""One-time sync script to copy cloud GPS, date, and camera metadata back into local image files."""
import sys
from pathlib import Path

from database.schema import get_connection
from database.models import ImageDB
from utils.cloud_sync import SporelyCloudClient, _inject_obs_exif_into_field_image, _load_obs_exif_fallback
from utils.temporal_matcher import _parse_timestamp

def run_sync():
    print("Connecting to Sporely Cloud...")
    client = SporelyCloudClient.from_stored_credentials()
    if not client:
        print("Error: Could not connect to Sporely Cloud. Please ensure you are logged in via the desktop app.")
        sys.exit(1)

    print("Connected successfully. Fetching local images linked to cloud...")
    conn = get_connection()
    try:
        conn.row_factory = __import__('sqlite3').Row
        rows = conn.execute(
            '''
            SELECT i.id, i.observation_id, i.filepath, i.cloud_id, o.cloud_id AS obs_cloud_id
            FROM images i
            JOIN observations o ON i.observation_id = o.id
            WHERE i.cloud_id IS NOT NULL
              AND o.cloud_id IS NOT NULL
              AND i.image_type != 'microscope'
              AND i.filepath IS NOT NULL
            '''
        ).fetchall()
        local_images = [dict(row) for row in rows]
    finally:
        conn.close()

    if not local_images:
        print("No synced local field images found.")
        return

    obs_cloud_ids = list(set(str(img['obs_cloud_id']) for img in local_images if img.get('obs_cloud_id')))
    if not obs_cloud_ids:
        print("No valid cloud observation IDs found.")
        return

    print(f"Fetching cloud metadata for images across {len(obs_cloud_ids)} observations...")
    
    remote_images = client.pull_bulk_image_metadata(obs_cloud_ids)
    remote_map = {str(img.get('id')): img for img in remote_images if img.get('id')}
    
    processed = 0
    skipped_not_in_cloud = 0
    skipped_file_missing = 0
    skipped_no_metadata = 0

    for img in local_images:
        cloud_id = str(img['cloud_id'])
        if cloud_id not in remote_map:
            skipped_not_in_cloud += 1
            continue
            
        remote_img = remote_map[cloud_id]
        filepath = img['filepath']
        p = Path(filepath)
        
        if not p.exists() or p.suffix.lower() not in {'.jpg', '.jpeg', '.webp'}:
            skipped_file_missing += 1
            continue
            
        obs_id = img['observation_id']
        obs_lat, obs_lon, obs_alt, obs_acc, obs_date = _load_obs_exif_fallback(obs_id)

        lat = remote_img.get('gps_latitude') if remote_img.get('gps_latitude') is not None else obs_lat
        lon = remote_img.get('gps_longitude') if remote_img.get('gps_longitude') is not None else obs_lon
        altitude = remote_img.get('gps_altitude') if remote_img.get('gps_altitude') is not None else obs_alt
        accuracy = remote_img.get('gps_accuracy') if remote_img.get('gps_accuracy') is not None else obs_acc
        date_str = remote_img.get('captured_at') or obs_date
        
        camera_model = remote_img.get('camera_model')
        iso = remote_img.get('iso')
        exposure_time = remote_img.get('exposure_time')
        f_number = remote_img.get('f_number')
        
        if lat is None and lon is None and not date_str and not camera_model:
            skipped_no_metadata += 1
            continue
            
        try:
            _inject_obs_exif_into_field_image(
                p,
                lat,
                lon,
                altitude,
                date_str,
                camera_model=camera_model,
                iso=iso,
                exposure_time=exposure_time,
                f_number=f_number,
                gps_accuracy=accuracy
            )
            
            if date_str:
                dt = _parse_timestamp(date_str)
                if dt:
                    try:
                        ImageDB.set_image_captured_at(img['id'], dt)
                    except Exception as e:
                        print(f"  -> Failed to update database captured_at for {p.name}: {e}")

            processed += 1
        except Exception as e:
            print(f"Failed to update {p.name}: {e}")

    print(f"\nDone! Processed {processed} images.")
    if skipped_not_in_cloud or skipped_file_missing or skipped_no_metadata:
        print("Skipped:")
        if skipped_not_in_cloud:
            print(f"  - {skipped_not_in_cloud} images missing from cloud database")
        if skipped_file_missing:
            print(f"  - {skipped_file_missing} image files were missing locally or unsupported (e.g. not jpg/webp)")
        if skipped_no_metadata:
            print(f"  - {skipped_no_metadata} images had no GPS, date, or camera data to write")

if __name__ == "__main__":
    run_sync()