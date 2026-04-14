# Field Photography

## Purpose

Field photos are for documenting context, habitat, and macroscopic features. They can also be used for length/size annotations when a scale is available: either as a ruler in the focus plane, or if you shoot with a macro lens with a known magnification.

Field photos are also used for AI species recognition, using [Artsorakelet](https://orakel.artsdatabanken.no/).

## Importing Field Photos

1. In **Prepare Images**, select images.
2. Set **Image type** to **Field**.
3. If the image has EXIF date/GPS, use **Set from current image** to populate observation metadata.
4. To get more precise AI identification, use the crop tool to select a tight crop of the mushroom.

> **Note on cloud-synced images:** Images uploaded from the web app (app.sporely.no) on free accounts may not carry EXIF because the 2 MP conversion strips metadata. The desktop restores GPS and date from the observation record automatically on sync, so "Set from current image" should work as long as the observation has GPS or date stored. If the button is still disabled, run a cloud sync to trigger the backfill.

## Field photos for AI identification

1. Select one or multiple (ctrl + click) field photos in the **Edit Observation** dialog
   You can open this dialog from the **Observations** table with **Alt+E** (Windows/Linux) or **Command+E** (macOS).
2.  Press **Guess** and the table will populate
3. Press **Copy** to copy the species over to the species name fields for this observation.

![Taxonomy](images/taxonomy.png)

## Measuring Lengths in Field Images

- Select **Category: Field** in the Measure tab.
- Use the **Line** tool (two clicks) to annotate length.
- Measurement overlays show **2 significant figures** with no unit label. The calibration defines the scale — if you calibrate on a 10 cm ruler, values are in cm; on a 1 m stick, in meters.
- If no scale is set, measurements are relative.

### Macro Lens Calibration

If you shoot with a macro lens, set up a profile under **Settings → Calibration → New Objective**, selecting profile type **Macro**. Enter:
- Magnification as a 1:X ratio divisor (e.g. 1 for 1:1 macro, 2 for 1:2, 4 for 1:4)
- Sensor width (mm) and image width (px)

This calculates a provisional scale. For best accuracy, calibrate against a ruler in the frame instead.

## Keyboard Shortcuts

| Action | Windows / Linux | macOS |
|--------|----------------|-------|
| Open Edit Observation | Alt+E | Option+E |
| Go to Observations tab | Alt+O | Option+O |
| Go to Measure tab | Alt+M | Option+M |
| Go to Analysis tab | Alt+A | Option+A |

## Tips

- Use a consistent camera or include a scale reference when possible.
- Capture multiple angles of the mushroom and include the habitat.

## See also

- [Microscopy workflow](./microscopy-workflow.md)
- [Spore measurements](./spore-measurements.md)
- [Taxonomy integration](./taxonomy-integration.md)
- [Database structure](./database-structure.md)
