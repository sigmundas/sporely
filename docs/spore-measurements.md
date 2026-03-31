# Spore Measurements

## Measurement Modes

- **Rectangle**: four clicks to define length and width for spores.
- **Line**: two clicks to measure length only.

## Categories

Common categories include **Spores** and **Field**. Categories determine how measurements are grouped and plotted.

## Reviewing Measurements

- Select a row in the measurements table to preview.
- Adjust lines and apply changes in the preview pane.
- Delete unwanted measurements from the table.

Drag the lines you want to adjust - the spore dimensions are updated instantly 

![Spore-preiew](images/spore-preview.png)

## Analysis

- Scatter plot shows length vs width.
- Histograms (optional) show distributions.
- Three plot modes are available: **Ellipse**, **Kernel density**, and **Mean range**.

### Ellipse

This is the classic spore-cloud view. It shows the measured points in length-width space, with an optional confidence ellipse around the specimen or around reference point datasets.

### 95% Confidence Ellipse

For bivariate measurements (length `x` and width `y`), the 95% confidence ellipse
is defined from the sample mean and covariance matrix.

Let

$$
\mathbf{z} =
\begin{bmatrix}
x \\
y
\end{bmatrix}
$$

and the mean

$$
\boldsymbol{\mu} =
\begin{bmatrix}
\bar{x} \\
\bar{y}
\end{bmatrix}
$$

and the covariance matrix

$$
\mathbf{\Sigma} =
\begin{bmatrix}
s_{x}^{2} & s_{xy} \\
s_{xy} & s_{y}^{2}
\end{bmatrix}
$$

Then the ellipse is the set of points satisfying

$$
(\mathbf{z} - \boldsymbol{\mu})^{\mathsf{T}} \mathbf{\Sigma}^{-1}
(\mathbf{z} - \boldsymbol{\mu}) = \chi^{2}_{2,\,0.95}
$$

where

$$
\chi^{2}_{2,\,0.95} \approx 5.991
$$

is the 95th percentile of the
chi-square distribution with 2 degrees of freedom.

Equivalently, if the eigenvalues of \(\mathbf{\Sigma}\) are

$$
\lambda_{1}, \lambda_{2}
$$

and the corresponding eigenvectors are

$$
\mathbf{v}_{1}, \mathbf{v}_{2}
$$

the ellipse axes are

$$
a = \sqrt{\chi^{2}_{2,\,0.95} \, \lambda_{1}}, \qquad
b = \sqrt{\chi^{2}_{2,\,0.95} \, \lambda_{2}}
$$

with the ellipse rotated by the eigenvectors

$$
\mathbf{v}_{1}, \mathbf{v}_{2}.
$$

### Kernel density

Kernel density shows the spore cloud as a smooth probability surface instead of a single outline.

The app uses a Gaussian kernel density estimate (KDE). For measured spores
\((x_i, y_i)\), the estimated density at a point \((x, y)\) is

$$
\hat{f}(x, y)
=
\frac{1}{n h_x h_y}
\sum_{i=1}^{n}
K\!\left(\frac{x-x_i}{h_x}, \frac{y-y_i}{h_y}\right)
$$

where \(K\) is a Gaussian kernel and \(h_x, h_y\) are bandwidth terms controlling smoothing.

In practice:

- **Bandwidth** controls how smooth or sharp the density becomes.
- **Contours** controls how many filled density bands are drawn.
- **Coverage** controls how much of the estimated density mass is included.

This view is useful when the spore cloud is clearly non-elliptical or when you want to see multiple dense regions.

### Mean range (Parmasto)

Mean range is a Parmasto-style summary view. Instead of emphasizing the full spore cloud, it compares specimen means against expected species-level ranges.

The specimen contributes:

- mean length \(L_m\)
- mean width \(W_m\)
- mean quotient \(Q_m\)

If Parmasto-style reference biometrics are available, the expected range is estimated from the reference mean and inter-specimen coefficient of variation:

$$
\text{expected range}
=
\bar{x} \pm 2 \left(\frac{CV}{100}\right)\bar{x}
$$

where \(\bar{x}\) is the species mean and \(CV\) is the Parmasto inter-specimen variation for that variable.

In the plot, this means:

- the specimen mean is shown as a point
- the expected length and width range form the main comparison region
- the expected mean `Q` range can be shown as guideline lines

This mode is useful for comparing one specimen against stored species-level reference means, rather than comparing full point clouds.

## Reference Data

- Add multiple reference datasets and toggle them in the plot table.
- Legend labels include genus initial and source, e.g. `G. marginata (Pedersen)`.
- Use **Add** and **Edit** to manage reference datasets.

## Export

### Observation Export

- **Export plot**: saves the current analysis plot (scatter + optional histograms).
- **Export gallery**: creates an annotated thumbnail mosaic for the active observation.
- **Copy stats / Save stats**: exports summary statistics plus a tab-separated table of raw measurements.

These actions live in the **Output** section of the Analysis tab and use the currently selected observation and category.

## See also

- [Field photography](./field-photography.md)
- [Microscopy workflow](./microscopy-workflow.md)
- [Taxonomy integration](./taxonomy-integration.md)
- [Database structure](./database-structure.md)
