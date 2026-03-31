"""
slide_calibration.py
────────────────────
Automatic calibration from microscope calibration slide images.
Zero dependencies beyond numpy + Pillow + matplotlib (all already required).

API designed for UI integration in Sporely.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Literal, Optional, Tuple
from collections import Counter
import numpy as np
from PIL import Image

# Import primitives from calibration_primitives.py
# Assumes it's in the same directory or utils/
try:
    from .calibration_primitives import (
        gauss_smooth, find_peaks, rotate_image, rotation_matrix,
        load_gray, half_max_edges, parabola_refine, filter_consistent_peaks
    )
except ImportError:
    # Fallback if running standalone - copy primitives inline
    raise ImportError("calibration_primitives.py must be in the same directory or utils/")


Axis = Literal["horizontal", "vertical"]


def _call_progress(progress_cb: Optional[Callable[[str, float], None]], step: str, frac: float) -> None:
    if progress_cb is None:
        return
    try:
        progress_cb(step, frac)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# ORIENTATION DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def detect_orientation(gray: np.ndarray) -> Axis:
    """Return 'horizontal' or 'vertical' — the dominant line orientation."""
    h, w = gray.shape
    
    # Average across bands (more robust than single slice)
    # Horizontal profile → vertical lines
    prof_h = gray[int(h*0.4):int(h*0.6), :].mean(axis=0)
    n_vert_lines = len(find_peaks(gauss_smooth(prof_h.max() - prof_h, 2.5),
                                   min_height=10, min_distance=15, min_prominence=5))
    
    # Vertical profile → horizontal lines  
    prof_v = gray[:, int(w*0.4):int(w*0.6)].mean(axis=1)
    n_horiz_lines = len(find_peaks(gauss_smooth(prof_v.max() - prof_v, 2.5),
                                    min_height=10, min_distance=15, min_prominence=5))
    
    return "vertical" if n_vert_lines >= n_horiz_lines else "horizontal"


# ══════════════════════════════════════════════════════════════════════════════
# TARGET COUNT HELPER (mode among counts ≥ max/2)
# ══════════════════════════════════════════════════════════════════════════════

def _find_target_count(counts: dict) -> int:
    """Return the most-common peak-count among positions where count ≥ max/2.
    Falls back to max if nothing qualifies."""
    if not counts:
        return 0
    max_n = max(counts.values())
    threshold = max(max_n * 0.5, 3)
    significant = [n for n in counts.values() if n >= threshold]
    return Counter(significant).most_common(1)[0][0] if significant else max_n


# ══════════════════════════════════════════════════════════════════════════════
# MEASUREMENT BAND DETECTION (on unrotated image)
# ══════════════════════════════════════════════════════════════════════════════

def _scan_peaks(gray: np.ndarray, axis: Axis, step: int = 8):
    """Return dict position → (n_peaks, spacing_std) along the scan axis."""
    h, w = gray.shape
    positions = range(0, h if axis == 'vertical' else w, step)
    result = {}
    
    for pos in positions:
        prof = gray[pos, :] if axis == 'vertical' else gray[:, pos]
        inv = prof.max() - prof
        inv_sm = gauss_smooth(inv, 2.5)
        
        peaks = find_peaks(inv_sm, min_height=inv_sm.max()*0.25,
                          min_distance=15, min_prominence=inv_sm.max()*0.15)
        
        if len(peaks) < 2:
            result[pos] = (len(peaks), 0.0)
        else:
            spacings = np.diff(peaks.astype(np.float64))
            result[pos] = (len(peaks), float(np.std(spacings)))
    
    return result


def find_measurement_band(gray: np.ndarray, axis: Axis) -> tuple[int, int]:
    """Find the region along the measurement axis where lines are most consistent.
    Returns (start, end) indices for the good band."""
    h, w = gray.shape
    scan_data = _scan_peaks(gray, axis, step=8)
    
    # Target = mode among counts ≥ max/2
    counts_only = {pos: n for pos, (n, _) in scan_data.items() if n > 0}
    if not counts_only:
        return (0, h if axis == 'vertical' else w)
    
    target_n = _find_target_count(counts_only)
    
    # Median spacing std among target positions
    target_stds = [s for pos, (n, s) in scan_data.items() if n == target_n]
    med_std = float(np.median(target_stds)) if target_stds else 0.0
    std_thresh = max(med_std * 5, 3.0)
    
    # Good positions: count == target AND spacing is consistent
    good = sorted([pos for pos, (n, s) in scan_data.items()
                   if n == target_n and s <= std_thresh])
    
    if not good:
        # Fallback: any position with target count
        good = sorted([pos for pos, (n, _) in scan_data.items() if n == target_n])
    
    if not good:
        return (0, h if axis == 'vertical' else w)
    
    # Expand to continuous range
    return (good[0], good[-1] + 8)


# ══════════════════════════════════════════════════════════════════════════════
# ANGLE REFINEMENT (multi-band tracking within good band on unrotated image)
# ══════════════════════════════════════════════════════════════════════════════

def _get_subpix_centers(prof: np.ndarray, expected_n: int) -> Optional[np.ndarray]:
    """Get sub-pixel centers, return None if count doesn't match."""
    inv = prof.max() - prof
    inv_sm = gauss_smooth(inv, 2.5)
    
    peaks = find_peaks(inv_sm, min_height=inv_sm.max()*0.25,
                      min_distance=15, min_prominence=inv_sm.max()*0.15)
    
    if len(peaks) != expected_n:
        return None
    
    return np.array([parabola_refine(prof, int(p)) for p in peaks])


def refine_angle(gray: np.ndarray, axis: Axis, band: tuple[int, int]) -> float:
    """Refine rotation angle via multi-band sub-pixel line tracking.
    
    Samples cross-sections within band on UNROTATED image, gets sub-pixel
    centers at each, fits position vs. band to get per-line slopes.
    Median slope gives the tilt angle.
    
    Returns angle in degrees.
    """
    h, w = gray.shape
    lo, hi = band
    
    # Sample within band
    sample_pos = np.arange(lo + 10, hi - 10, 18)
    if len(sample_pos) < 3:
        return 0.0
    
    # First pass: find target count
    counts = {}
    for pos in sample_pos:
        prof = gray[pos, :] if axis == 'vertical' else gray[:, pos]
        inv = prof.max() - prof
        inv_sm = gauss_smooth(inv, 2.5)
        peaks = find_peaks(inv_sm, min_height=inv_sm.max()*0.25,
                          min_distance=15, min_prominence=inv_sm.max()*0.15)
        counts[int(pos)] = len(peaks)
    
    target_n = _find_target_count(counts)
    
    # Second pass: get centers at qualifying positions
    band_data = {}
    for pos in sample_pos:
        prof = gray[pos, :] if axis == 'vertical' else gray[:, pos]
        centers = _get_subpix_centers(prof, target_n)
        if centers is not None:
            band_data[int(pos)] = centers
    
    if len(band_data) < 3:
        return 0.0
    
    # Fit slopes
    positions = np.array(sorted(band_data.keys()), dtype=np.float64)
    all_centers = np.array([band_data[int(p)] for p in positions])
    
    slopes = []
    for i in range(target_n):
        s = np.polyfit(positions, all_centers[:, i], 1)[0]
        if abs(s) < 1.0:  # Reject mismatched peaks
            slopes.append(s)
    
    if not slopes:
        return 0.0
    
    med_slope = np.median(slopes)
    
    # For vertical lines: tilt = dx/dy (angle from vertical)
    # For horizontal lines: tilt = dy/dx (angle from horizontal)
    return float(np.degrees(np.arctan(med_slope)))


# ══════════════════════════════════════════════════════════════════════════════
# MEASUREMENT (parabola + edge methods)
# ══════════════════════════════════════════════════════════════════════════════

def measure(rot_gray: np.ndarray, axis: Axis, band: tuple[int, int]) -> dict:
    """Measure line centers and spacings using both parabola and edge methods.
    
    Returns dict with:
        - centers (parabola method)
        - centers_edges (edge-midpoint method)
        - diffs_parab, diffs_edges (inter-line spacings)
        - widths (line widths)
        - other stats
    """
    h, w = rot_gray.shape
    
    # Average profile within inner 60% of band
    lo_m = band[0] + int((band[1] - band[0]) * 0.2)
    hi_m = band[1] - int((band[1] - band[0]) * 0.2)
    
    if axis == 'horizontal':
        prof = rot_gray[:, lo_m:hi_m].mean(axis=1)
    else:
        prof = rot_gray[lo_m:hi_m, :].mean(axis=0)
    
    # Parabola detection
    inv_sm = gauss_smooth(prof.max() - prof, 2.5)
    peaks = find_peaks(inv_sm, min_height=inv_sm.max()*0.25,
                      min_distance=15, min_prominence=inv_sm.max()*0.15)

    spacing_est = None
    if len(peaks) >= 2:
        diffs = np.diff(peaks.astype(np.float64))
        if diffs.size:
            spacing_est = float(np.median(diffs))
    edge_search = 25
    if spacing_est and np.isfinite(spacing_est) and spacing_est > 0:
        edge_search = max(25, int(0.45 * spacing_est))
        limit = int(0.49 * spacing_est)
        if limit > 0:
            edge_search = min(edge_search, limit)
    
    centers_raw = np.array([parabola_refine(prof, int(p)) for p in peaks])
    
    # Filter outliers
    kept = filter_consistent_peaks(centers_raw, tol=0.30)
    centers = centers_raw[kept]
    
    if len(centers) < 2:
        raise ValueError(f"Only {len(centers)} lines detected after filtering")
    
    # Edge-midpoint detection (using parabola as guide)
    edge_mids = []
    edges = []
    for cx in centers:
        top, bot = half_max_edges(prof, cx, search=edge_search)
        if top is not None and bot is not None:
            edge_mids.append((top + bot) / 2.0)
            edges.append((float(top), float(bot)))
        else:
            edge_mids.append(cx)  # Fallback to parabola
            edges.append((float(cx), float(cx)))
    
    edge_mids = np.array(edge_mids)
    
    # Line widths
    widths = []
    for cx in centers:
        top, bot = half_max_edges(prof, cx, search=edge_search)
        if top is not None and bot is not None:
            widths.append(abs(bot - top))
    
    widths = np.array(widths) if widths else np.array([0.0])
    
    # Spacings
    diffs_parab = np.diff(centers)
    diffs_edges = np.diff(edge_mids)
    
    return {
        'centers': centers,
        'centers_edges': edge_mids,
        'diffs_parab': diffs_parab,
        'diffs_edges': diffs_edges,
        'widths': widths,
        'edges': np.array(edges, dtype=np.float64),
        'profile': prof,
        'smoothed': inv_sm,
        'band_inner': (lo_m, hi_m),
    }


# ══════════════════════════════════════════════════════════════════════════════
# RESIDUAL VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def measure_residual_slope(rot_gray: np.ndarray, axis: Axis, band: tuple[int, int]) -> float:
    """Measure residual tilt within the measurement band (post-rotation).
    Uses the same multi-slice tracking logic as refine_angle()."""
    return refine_angle(rot_gray, axis, band)


# ══════════════════════════════════════════════════════════════════════════════
# API FOR UI INTEGRATION
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class CalibrationResult:
    """Result of automatic calibration (UI-compatible)."""
    axis: Axis
    angle_deg: float
    centers_px: np.ndarray
    centers_edges_px: np.ndarray
    edges_px: np.ndarray
    spacing_median_px: float
    spacing_median_edges_px: float
    nm_per_px: float
    nm_per_px_edges: float
    agreement_pct: float
    rel_scatter_mad_pct: float
    rel_scatter_iqr_pct: float
    drift_slope: float
    residual_slope_deg: float
    # Extra metadata retained for newer pipelines
    tilt_deg: float = 0.0
    n_lines: int = 0
    band: Optional[Tuple[int, int]] = None
    warning: Optional[str] = None

    @property
    def centers_parabola(self) -> np.ndarray:
        return self.centers_px

    @property
    def centers_edges(self) -> np.ndarray:
        return self.centers_edges_px

    @property
    def spacing_px_parabola(self) -> float:
        return self.spacing_median_px

    @property
    def spacing_px_edges(self) -> float:
        return self.spacing_median_edges_px

    @property
    def nm_per_px_parabola(self) -> float:
        return self.nm_per_px

    @property
    def scatter_mad_pct(self) -> float:
        return self.rel_scatter_mad_pct

    @property
    def scatter_iqr_pct(self) -> float:
        return self.rel_scatter_iqr_pct

    @property
    def residual_tilt_deg(self) -> float:
        return self.residual_slope_deg


def _drift_slope_from_centers(centers: np.ndarray) -> float:
    centers = np.sort(centers.astype(np.float64, copy=False))
    if len(centers) < 3:
        return float("nan")
    diffs = np.diff(centers)
    mids = (centers[:-1] + centers[1:]) / 2.0
    if len(mids) < 2:
        return float("nan")
    slope, _ = np.polyfit(mids, diffs, 1)
    return float(slope)


def calibrate_from_image(
    image_or_path: str | Image.Image,
    division_um: float,
    axis_override: Optional[Axis] = None,
    use_large_angles: bool = False,
    use_edges: bool = True,
    progress_cb: Optional[Callable[[str, float], None]] = None,
) -> CalibrationResult:
    """
    Calibrate from a microscope calibration slide image.
    
    Args:
        image_path: Path to calibration slide image
        division_um: Physical spacing between divisions (e.g., 10.0 for 0.01 mm)
        axis_override: Force 'horizontal' or 'vertical', or None for auto-detect
    
    Returns:
        CalibrationResult with measurements and quality metrics
    
    Raises:
        ValueError: If fewer than 2 lines detected or other failure
    """
    
    # Load
    _call_progress(progress_cb, "Loading image", 0.05)
    if isinstance(image_or_path, Image.Image):
        pil_src = image_or_path.convert("L")
        gray = np.array(pil_src, dtype=np.float64)
    else:
        pil_src = Image.open(image_or_path).convert("L")
        gray = load_gray(image_or_path)
    h, w = gray.shape
    
    # 1) Orientation
    _call_progress(progress_cb, "Detecting orientation", 0.15)
    axis = axis_override if axis_override else detect_orientation(gray)
    
    # 2) Find measurement band on unrotated image
    _call_progress(progress_cb, "Finding measurement band", 0.30)
    band = find_measurement_band(gray, axis)
    
    # 3) Refine angle (restricted to good band)
    _call_progress(progress_cb, "Refining angle", 0.45)
    tilt = refine_angle(gray, axis, band)
    
    # Rotation sign convention:
    # - Vertical lines: tilt = dx/dy. Positive tilt (lean right) needs CW rotation (negative).
    # - Horizontal lines: tilt = dy/dx. Positive tilt (slope down-right) needs CCW rotation (positive).
    rot_angle = -tilt if axis == 'vertical' else tilt
    
    # 4) Rotate
    _call_progress(progress_cb, "Rotating image", 0.60)
    rot_pil = rotate_image(pil_src, rot_angle)
    rot_gray = np.array(rot_pil, dtype=np.float64)
    
    # 5) Find final band on rotated image
    _call_progress(progress_cb, "Finding rotated band", 0.70)
    band_rot = find_measurement_band(rot_gray, axis)
    
    # 6) Measure with both methods
    _call_progress(progress_cb, "Measuring lines", 0.80)
    res = measure(rot_gray, axis, band_rot)
    
    # 7) Residual validation
    _call_progress(progress_cb, "Checking residual tilt", 0.88)
    residual_deg = measure_residual_slope(rot_gray, axis, band_rot)
    
    # 8) Statistics
    diffs_p = res['diffs_parab']
    diffs_e = res['diffs_edges']
    
    med_p = float(np.median(diffs_p))
    med_e = float(np.median(diffs_e))
    
    nm_per_px_p = (division_um * 1000.0) / med_p
    nm_per_px_e = (division_um * 1000.0) / med_e
    
    agreement = 100.0 * abs(nm_per_px_p - nm_per_px_e) / ((nm_per_px_p + nm_per_px_e) / 2.0)
    
    # MAD and IQR for parabola
    mad_p = float(np.median(np.abs(diffs_p - med_p)))
    q25, q75 = np.percentile(diffs_p, [25, 75])
    iqr_p = float(q75 - q25)
    
    scatter_mad = 100.0 * mad_p / med_p
    scatter_iqr = 100.0 * iqr_p / med_p
    
    drift_slope = _drift_slope_from_centers(res["centers"])

    # Warning status
    warning = "OK"
    if agreement > 2.0:
        warning = "POOR_AGREEMENT"
    elif abs(residual_deg) > 1.0:
        warning = "HIGH_RESIDUAL"
    elif scatter_mad > 5.0:
        warning = "HIGH_SCATTER"
    
    _call_progress(progress_cb, "Calibration complete", 1.0)

    return CalibrationResult(
        axis=axis,
        angle_deg=rot_angle,
        centers_px=res["centers"],
        centers_edges_px=res["centers_edges"],
        edges_px=res["edges"],
        spacing_median_px=med_p,
        spacing_median_edges_px=med_e,
        nm_per_px=nm_per_px_p,
        nm_per_px_edges=nm_per_px_e,
        agreement_pct=agreement,
        rel_scatter_mad_pct=scatter_mad,
        rel_scatter_iqr_pct=scatter_iqr,
        drift_slope=drift_slope,
        residual_slope_deg=residual_deg,
        tilt_deg=tilt,
        n_lines=len(res["centers"]),
        band=band_rot,
        warning=warning,
    )


def calibrate_image(
    img_or_path: str | Image.Image,
    spacing_um: float,
    axis_hint: Optional[Axis] = None,
    use_large_angles: bool = False,
    use_edges: bool = True,
    band_frac: Tuple[float, float] = (0.25, 0.75),
    smooth_sigma: float = 3.2,
    min_distance_px: Optional[int] = None,
    progress_cb: Optional[Callable[[str, float], None]] = None,
) -> CalibrationResult:
    """UI-compatible wrapper (extra args are accepted but ignored)."""
    _ = band_frac
    _ = smooth_sigma
    _ = min_distance_px
    return calibrate_from_image(
        img_or_path,
        division_um=spacing_um,
        axis_override=axis_hint,
        use_large_angles=use_large_angles,
        use_edges=use_edges,
        progress_cb=progress_cb,
    )


def build_overlay_lines(
    result: CalibrationResult,
    image_size: Tuple[int, int],
    use_edges: bool = False,
    origin_offset: Tuple[float, float] = (0.0, 0.0),
) -> list[list[float]]:
    """Return line coordinates in original image space for overlay rendering."""
    w, h = image_size
    ox, oy = origin_offset
    M_fwd = rotation_matrix(result.angle_deg, (w / 2.0, h / 2.0))
    M_back = np.linalg.inv(np.vstack([M_fwd, [0.0, 0.0, 1.0]]))[:2, :]

    centers = result.centers_edges_px if use_edges else result.centers_px
    band = result.band
    if band is None:
        band = (0, h if result.axis == "vertical" else w)

    lines: list[list[float]] = []
    for c in centers:
        if result.axis == "vertical":
            pts_rot = np.array([[c, float(band[0]), 1.0], [c, float(band[1]), 1.0]])
        else:
            pts_rot = np.array([[float(band[0]), c, 1.0], [float(band[1]), c, 1.0]])
        pts_orig = (M_back @ pts_rot.T).T
        lines.append([
            float(pts_orig[0, 0] + ox),
            float(pts_orig[0, 1] + oy),
            float(pts_orig[1, 0] + ox),
            float(pts_orig[1, 1] + oy),
        ])
    return lines


def build_overlay_edge_lines(
    result: CalibrationResult,
    image_size: Tuple[int, int],
    origin_offset: Tuple[float, float] = (0.0, 0.0),
) -> list[list[float]]:
    """Return line coordinates for 50% intensity edges for overlay rendering."""
    edges = result.edges_px if hasattr(result, "edges_px") else None
    if edges is None or len(edges) == 0:
        return []

    w, h = image_size
    ox, oy = origin_offset
    M_fwd = rotation_matrix(result.angle_deg, (w / 2.0, h / 2.0))
    M_back = np.linalg.inv(np.vstack([M_fwd, [0.0, 0.0, 1.0]]))[:2, :]

    band = result.band
    if band is None:
        band = (0, h if result.axis == "vertical" else w)

    lines: list[list[float]] = []
    for edge_pair in edges:
        if len(edge_pair) < 2:
            continue
        for c in (edge_pair[0], edge_pair[1]):
            if result.axis == "vertical":
                pts_rot = np.array([[c, float(band[0]), 1.0], [c, float(band[1]), 1.0]])
            else:
                pts_rot = np.array([[float(band[0]), c, 1.0], [float(band[1]), c, 1.0]])
            pts_orig = (M_back @ pts_rot.T).T
            lines.append([
                float(pts_orig[0, 0] + ox),
                float(pts_orig[0, 1] + oy),
                float(pts_orig[1, 0] + ox),
                float(pts_orig[1, 1] + oy),
            ])
    return lines


def create_overlay_image(
    image_path: str | Image.Image,
    result: CalibrationResult,
    use_edges: bool = False,
) -> Image.Image:
    """
    Create overlay visualization with detected lines.
    
    Args:
        image_path: Original calibration image
        result: CalibrationResult from calibrate_from_image()
        use_edges: If True, use edge centers; otherwise parabola centers
    
    Returns:
        PIL Image with red overlay lines
    """
    from PIL import ImageDraw
    
    img = Image.open(image_path).convert("RGB") if isinstance(image_path, str) else image_path.convert("RGB")
    w, h = img.size
    
    # Rotation matrix for back-projection
    M_fwd = rotation_matrix(result.angle_deg, (w/2, h/2))
    M_back = np.linalg.inv(np.vstack([M_fwd, [0, 0, 1]]))[:2]
    
    centers = result.centers_edges_px if use_edges else result.centers_px
    band = result.band or (0, h if result.axis == "vertical" else w)
    
    # Draw overlay
    overlay = img.convert('RGBA')
    draw = ImageDraw.Draw(overlay, 'RGBA')
    
    for cx in centers:
        # Build endpoints in rotated frame
        if result.axis == 'vertical':
            pts_rot = np.array([[cx, float(band[0]), 1],
                               [cx, float(band[1]), 1]])
        else:
            pts_rot = np.array([[float(band[0]), cx, 1],
                               [float(band[1]), cx, 1]])
        
        # Back-project to original
        pts_orig = (M_back @ pts_rot.T).T
        
        # Draw line
        draw.line(
            [(pts_orig[0, 0], pts_orig[0, 1]),
             (pts_orig[1, 0], pts_orig[1, 1])],
            fill=(255, 0, 0, 180),
            width=2
        )
    
    return overlay.convert('RGB')


# ══════════════════════════════════════════════════════════════════════════════
# COMMAND-LINE TEST
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python slide_calibration.py <image_path> <division_um>")
        print("Example: python slide_calibration.py slide.jpg 10.0")
        sys.exit(1)
    
    path = sys.argv[1]
    div_um = float(sys.argv[2])
    
    print(f"\nCalibrating from {path}")
    print(f"Division spacing: {div_um} µm\n")
    
    result = calibrate_from_image(path, div_um)
    
    print(f"Axis: {result.axis}")
    print(f"Angle: {result.angle_deg:.3f}°")
    print(f"Tilt: {result.tilt_deg:.3f}°")
    print(f"Lines detected: {result.n_lines}")
    print(f"\nParabola method: {result.nm_per_px_parabola:.2f} nm/px")
    print(f"Edge method:     {result.nm_per_px_edges:.2f} nm/px")
    print(f"Agreement:       {result.agreement_pct:.2f}%")
    print(f"\nScatter MAD:     {result.scatter_mad_pct:.2f}%")
    print(f"Scatter IQR:     {result.scatter_iqr_pct:.2f}%")
    print(f"Residual tilt:   {result.residual_tilt_deg:.3f}°")
    print(f"\nStatus: {result.warning}")
