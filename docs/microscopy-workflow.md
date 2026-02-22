# Microscopy Workflow

## Objectives

Objectives are defined by:
- Magnification (integer)
- Numerical Aperture (NA)
- Objective name/camera setup

You can add a note about your setup, what camera or phone adapters you're using etc.

Display name is built as `<Magnification>X/<NA> <Objective name>` in the UI.

## Calibration of image scales

MycoLog supports manual and auto calibration.

- **Manual**: click two points on a known scale.
- **Auto**: detect calibration slide lines, then review the result.

Calibration history stores:
- Camera model
- Megapixels used
- Confidence interval and residuals (when available)

You can export the calibration image with overlays for documentation.

Ideal resolution only appears after the currently loaded calibration image has
an auto result or manual measurements. It is based on the *current image*, not
the previously active calibration.
Calibration images are stored at full resolution; resampling is applied to
imported microscope images and the scale is adjusted by the resample factor.

## Sampling Assessment

Sampling status is shown in the Calibration dialog and Prepare Images panel. This checks if your pixel sampling is undersampled or oversampled based on NA.Typically, images taken with a 100X objective are oversampled, and your local database can be shrunk quite a lot if you work with spores.

There is a resize preview feature you can use to check if important details are lost: press **P** to toggle original resolution vs ideal resolution.

### Nyquist Sampling (Basics)

MycoLog uses a Nyquist-based ideal pixel size:

$$
p_{\mathrm{ideal}} = \frac{\lambda}{4\,\mathrm{NA}}
$$

where $\lambda$ is the illumination wavelength in $\mathrm{\mu m}$ and $\mathrm{NA}$ is the numerical aperture.

### Downsampling and Scale Propagation

If an image is resampled by a uniform factor $f$, the scale
and megapixels adjust as follows:

$$
p_{\mathrm{target}} = \frac{p_{\mathrm{full}}}{f}, \qquad
M_{\mathrm{target}} = M_{\mathrm{full}} \cdot f^{2}
$$

where $f$ is the linear resampling factor ($0 < f \le 1$), $p_{\mathrm{full}}$ is the
original scale in $\mathrm{\mu m}/\mathrm{px}$, $p_{\mathrm{target}}$ is the resampled scale in $\mathrm{\mu m}/\mathrm{px}$,
$M_{\mathrm{full}}$ is the original megapixels, and $M_{\mathrm{target}}$ is the resampled megapixels.

MycoLog uses this relationship instead of requiring a second calibration on the
downsampled image.

## Resolution Mismatch Warning

If a microscope image resolution differs significantly from the calibration image, a warning is shown in:
- **Measure** tab (Scale group)
- **Prepare Images** (Scale group)

This is expected for cropped images; the warning includes a tooltip with calibration vs image MP.
The comparison uses the calibration's stored resolution and the image's effective resolution
(taking resampling into account).

## Working with Scale

- Select an objective in the Scale dropdown.
- Use **Set scale...** for custom scale bars.
- For microscope images, ensure the correct objective is applied before measuring.

## Scale Bar Calibration (Manual)

If you need a custom scale (field images or slides without an objective profile):

1. Choose **Scale bar** in the Scale dropdown.
2. Click **Set scale...** and enter the real-world length.
3. Click two points on the scale bar in the image.

You can also trigger this dialog from the **No Scale Set** prompt when you start measuring.

## See also

- [Field photography](./field-photography.md)
- [Spore measurements](./spore-measurements.md)
- [Taxonomy integration](./taxonomy-integration.md)
- [Database structure](./database-structure.md)
