# Prepare Images dialog

Use this dialog to review and tag images before saving them into an observation.

## Main areas

- **Left column** — Add/remove images, set image type (Field / Micro), and configure per-image settings (scale, microscopy metadata, image note).
- **Center preview** — Inspect the selected image, crop it, rotate it, and check scale-related overlays.
- **Right column** — "Current image" shows EXIF read from the file. "Time and GPS" lets you set observation date/time and GPS, with "Set from current image" to copy EXIF values. "Image resize" controls optimal downsampling for microscope images.
- **Bottom gallery** — Reorder and review all images in the current import batch.

## Typical workflow

1. Add images (drag-and-drop or "Add Images…" button).
2. Set **Image type** for each image (Field / Micro). Keyboard shortcuts: `F` for field, `M` for micro.
3. For **field images**: if the image has EXIF date/GPS, click **Set from current image** to pre-fill the observation's time and location.
4. For **microscope images**: assign objective and microscopy metadata (contrast, mount, stain, sample type). Use the scalebar tool or objective dropdown to set the scale.
5. Crop or rotate images where needed.
6. Continue to the observation dialog.

## "Set from current image" button

Reads EXIF GPS and `DateTimeOriginal` from the currently selected image file and copies those values into the observation's Date/time, Lat, and Lon fields.

- **Enabled** when: exactly one field image is selected AND the image file has GPS or datetime EXIF.
- **Disabled** when: microscope image selected, multiple images selected, or no EXIF found.
- **Source highlight**: when this image is already the active GPS/time source for the observation, the button is shown with a mint background.

### Cloud-synced images and EXIF

Images synced from the web app (app.sporely.no) may have no EXIF because the web app's free-tier 2 MP conversion uses the Canvas API, which strips all EXIF. The desktop app mitigates this by writing the observation's stored GPS and date back into the JPEG EXIF when downloading cloud images. This means the button will work for cloud images as long as the observation itself has GPS/date recorded.

If the button is still disabled for a cloud-synced field image, trigger a cloud sync — the backfill runs automatically on each sync pass.

## Notes

- The bottom gallery can be resized; thumbnail size follows the gallery height.
- Scale-bar calibration here is meant for imported images that already contain a visible scale bar.
- Images imported on desktop are stored at full resolution locally. The cloud copy may be 2 MP (free tier) — the desktop always keeps the larger local file.

## Keyboard shortcuts (within the dialog)

| Action | Key |
|---|---|
| Set image type: Field | `F` |
| Set image type: Micro | `M` |
| Toggle resize preview | `R` |
| Toggle crop mode | `C` |
| Scalebar mode | `S` |
| Next image | `N` or `→` |
| Previous image | `P` or `←` |
| Delete selected | `Del` or `Cmd+D` |
