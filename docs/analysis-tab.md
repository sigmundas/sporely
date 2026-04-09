# Analysis Tab

The Analysis tab (Alt+A) is the primary tool for visualizing and exploring spore measurements. It combines an interactive scatter/histogram plot with a thumbnail gallery, reference data overlay, and statistics export.

Figure placeholder: overview of the Analysis tab.

---

## Layout

The tab is split into two main areas:

- **Left panel (fixed width):** Controls for the plot, reference data, gallery, and sharing.
- **Right panel:** Collapsible splitter with the plot canvas on top and the thumbnail gallery below.

---

## Plot Canvas

### Scatter Plot

Plots spore Length (x-axis) vs Width (y-axis). Each point represents one measured spore. Supports zoom (scroll wheel) and pan (left-drag). Points can be clicked to filter the gallery.

### Histograms

Three stacked histograms below the scatter: Length, Width, and Q (Length/Width). Histogram bars are clickable and filter the gallery to the selected bin range. Bin count is adjustable (3–50). Histograms can be hidden via the Histogram checkbox.

---

## Plot Styles

Three mutually exclusive modes controlled by radio buttons in the Plot Settings section.

### Ellipse

Draws a confidence ellipse around the spore cloud. Coverage is configurable from 50–99% via a slider. The ellipse is labeled with its coverage percentage. Default mode.

### Kernel Density (KDE)

Draws filled contour bands representing probability density. Controls:

- **Bandwidth:** 0.5–1.5× scaling of the Gaussian kernel.
- **Contours:** 1–10 rings.
- **Coverage:** 50–99% for the outermost contour.

### Mean Range

Parmasto-style view. Shows the mean point with a cross marker, the expected mean range as a rectangle, and an average Q line. Designed for comparison against reference data using Parmasto biometrics.

---

## Plot Options

Available in the Plot Settings section (collapsed by default):

| Option | Description |
|---|---|
| Histogram | Toggle histogram visibility |
| Bins | Number of histogram bins (3–50) |
| Image color | Color scatter points by source image |
| Plot Avg Q | Overlay the mean Q line |
| Plot Q 90% range | Overlay 5th–95th percentile Q lines |
| Plot Q min/max | Overlay absolute min/max Q lines |
| Axis equal | Force square aspect ratio on scatter plot |

---

## Filtering & Selection

### Click to Filter

- **Scatter plot click:** Selects the clicked point(s) and filters the gallery to show only those spores.
- **Histogram bar click:** Filters the gallery to spores within the clicked bin's metric range (Length, Width, or Q).

The active filter is shown in a label below the gallery controls (e.g., "Length: 10.50 – 15.30").

### Multi-Select

Hold Cmd (macOS) or Ctrl (Windows/Linux) while clicking to add points or histogram bins to the current selection rather than replacing it. See [Analysis Development Plan](analysis-development-plan.md).

### Clear Filter

The "Clear filter" button resets all active selections and restores the full gallery.

### Category Filter

A dropdown at the top of the left panel filters measurements by category (Spores, other measurement types, or All except spores).

---

## Reference Data

The Reference Values section (expanded by default) allows overlaying published or custom spore reference data on the scatter plot.

### Loading Reference Data

1. Search by common name, genus, or species.
2. Select a source from the dropdown, or use "Cloud..." to search community data.
3. Click "Plot" to overlay the reference on the scatter plot.

### Reference Series Table

All loaded reference datasets are listed in a table with columns: Plot (visibility toggle), Icon, Label, and Color. Each series can be shown or hidden independently.

### Reference Shape Options

| Option | Description |
|---|---|
| Ellipse | Draw reference bounds as an ellipse |
| Square | Draw reference bounds as a rectangle |
| Min/Max | Show min/max boundary lines in addition to the main shape |

### Adding/Editing Reference Data

- **Add:** Opens the Add/Edit Reference Data dialog. See [Reference Data Dialogs](reference-data-dialog.md).
- **Edit:** Opens the dialog for the currently selected reference record.

---

## Gallery

Thumbnail images of each measured spore are shown below the plot. The gallery updates live as filters change.

### Gallery Controls

| Control | Description |
|---|---|
| Orient | Rotate thumbnails so the long axis is vertical |
| Uniform scale | Scale all thumbnails to the same apparent spore length |
| Sort | Order by: Images, Width, Length, or Q |

---

## Statistics

A plain-text stats preview shows per-measurement details for the active selection. The "Include details" checkbox adds columns for Image, Contrast, Mount, Stain, Sample, and Objective.

### Export Options

| Button | Output |
|---|---|
| Export Plot | Saves the scatter/histogram chart as SVG, PNG, or JPEG |
| Export gallery | Saves the thumbnail grid as SVG, PNG, or JPEG |
| Export statistics | Copies formatted stats to clipboard |
| Save statistics | Saves stats to a .txt file |

---

## Spore Data Sharing

Controls who can find your measurements when others search community data:

- Public (share with everyone)
- Friends only
- Private (keep to myself)

---

## Settings Persistence

All plot and gallery settings are saved per observation and restored automatically when switching back to the Analysis tab. This includes plot style, coverage values, reference series, sort order, filter state, and splitter positions.
