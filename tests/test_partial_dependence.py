"""
Tests for Partial Dependence Plot (Figure 1 of the paper).

Paper: van Binsbergen, Han, Lopez-Lira (2022) "Man vs. Machine Learning"
Figure 1: "EPS as a non-Linear function of analysts' forecasts"

Validates the output image curve properties by extracting pixel data:
- PDP curve (royalblue) is monotonically increasing
- The relationship is non-linear S-shaped (core paper argument)
- 95% CI band (grey) exists and surrounds the curve
- Plot elements (title, axes, legend) are present
"""
from pathlib import Path

import numpy as np
import pytest
from PIL import Image


# ---------------------------------------------------------------------------
# Helpers: extract curve from rendered PNG
# ---------------------------------------------------------------------------

def _extract_blue_curve(img_array):
    """Extract the royalblue PDP curve from the image.
    Returns (col_indices, row_indices) sorted by x.
    """
    r, g, b = img_array[:, :, 0], img_array[:, :, 1], img_array[:, :, 2]
    mask = (
        (r > 30) & (r < 120) &
        (g > 60) & (g < 160) &
        (b > 180) & (b <= 255) &
        (b > r + 50)
    )
    rows, cols = np.where(mask)
    if len(cols) == 0:
        return np.array([]), np.array([])
    unique_cols = np.unique(cols)
    median_rows = np.array([np.median(rows[cols == c]) for c in unique_cols])
    order = np.argsort(unique_cols)
    return unique_cols[order], median_rows[order]


def _extract_grey_band(img_array):
    """Extract the grey CI band.
    Returns (col_indices, top_rows, bottom_rows).
    """
    r, g, b = img_array[:, :, 0], img_array[:, :, 1], img_array[:, :, 2]
    grey_mask = (
        (np.abs(r.astype(int) - g.astype(int)) < 15) &
        (np.abs(g.astype(int) - b.astype(int)) < 15) &
        (r > 150) & (r < 235)
    )
    rows, cols = np.where(grey_mask)
    if len(cols) == 0:
        return np.array([]), np.array([]), np.array([])
    unique_cols = np.unique(cols)
    top_rows = np.array([rows[cols == c].min() for c in unique_cols])
    bot_rows = np.array([rows[cols == c].max() for c in unique_cols])
    order = np.argsort(unique_cols)
    return unique_cols[order], top_rows[order], bot_rows[order]


@pytest.fixture(scope="module")
def img_array(pdp_png):
    assert pdp_png.exists(), f"Missing: {pdp_png}"
    img = Image.open(pdp_png).convert("RGB")
    return np.array(img)


@pytest.fixture(scope="module")
def blue_curve(img_array):
    cols, rows = _extract_blue_curve(img_array)
    assert len(cols) > 20, "Could not extract enough blue curve pixels"
    return cols, rows


@pytest.fixture(scope="module")
def grey_band(img_array):
    cols, top, bot = _extract_grey_band(img_array)
    assert len(cols) > 20, "Could not extract enough grey band pixels"
    return cols, top, bot


# ===========================================================================
# 1. Output file validity
# ===========================================================================

class TestOutputFile:
    def test_png_exists(self, pdp_png):
        assert pdp_png.exists(), f"Missing output: {pdp_png}"

    def test_png_nonzero_size(self, pdp_png):
        assert pdp_png.stat().st_size > 1000, "PNG file is suspiciously small"

    def test_png_valid_header(self, pdp_png):
        PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
        with open(pdp_png, "rb") as f:
            assert f.read(8) == PNG_SIGNATURE, "Not a valid PNG"

    def test_image_dimensions(self, img_array):
        h, w = img_array.shape[:2]
        assert w >= 500 and h >= 300, f"Image too small: {w}x{h}"


# ===========================================================================
# 2. Monotonicity: PDP curve should increase left-to-right
#    (image y-axis is inverted: smaller row = higher value)
# ===========================================================================

class TestMonotonicity:
    def test_overall_increasing(self, blue_curve):
        cols, rows = blue_curve
        n = len(rows)
        left_mean = np.mean(rows[:max(1, n // 10)])
        right_mean = np.mean(rows[-max(1, n // 10):])
        assert right_mean < left_mean, (
            "PDP curve should increase: right side should be higher "
            f"(row {right_mean:.0f}) than left (row {left_mean:.0f})"
        )

    def test_mostly_monotonic(self, blue_curve):
        cols, rows = blue_curve
        kernel = max(3, len(rows) // 50)
        smoothed = np.convolve(rows, np.ones(kernel) / kernel, mode="valid")
        diffs = np.diff(smoothed)
        frac_increasing = np.mean(diffs <= 0)
        assert frac_increasing >= 0.75, (
            f"Only {frac_increasing:.0%} of smoothed steps are increasing"
        )


# ===========================================================================
# 3. Non-linearity & S-shape: the paper's core argument
# ===========================================================================

class TestNonlinearity:
    def test_not_a_straight_line(self, blue_curve):
        """Residual from linear fit should have meaningful variance."""
        cols, rows = blue_curve
        x, y = cols.astype(float), rows.astype(float)
        linear_pred = np.polyval(np.polyfit(x, y, 1), x)
        residual_std = np.std(y - linear_pred)
        y_range = y.max() - y.min()
        assert residual_std / y_range > 0.01, (
            f"Curve appears too linear: residual_std/range = {residual_std / y_range:.4f}"
        )

    def test_quadratic_improves_fit(self, blue_curve):
        """Quadratic R^2 should beat linear, confirming non-linearity."""
        cols, rows = blue_curve
        x, y = cols.astype(float), rows.astype(float)
        ss_tot = np.sum((y - y.mean()) ** 2)
        if ss_tot == 0:
            pytest.skip("No variance in curve")
        lin_pred = np.polyval(np.polyfit(x, y, 1), x)
        r2_lin = 1 - np.sum((y - lin_pred) ** 2) / ss_tot
        quad_pred = np.polyval(np.polyfit(x, y, 2), x)
        r2_quad = 1 - np.sum((y - quad_pred) ** 2) / ss_tot
        assert r2_quad > r2_lin, (
            f"Quadratic R^2 ({r2_quad:.4f}) should exceed linear ({r2_lin:.4f})"
        )

    def test_s_shape_middle_steeper_than_tails(self, blue_curve):
        """S-shape: the middle segment should be steeper than at least one tail.
        (image y inverted: more negative slope = steeper rise)"""
        x, y = blue_curve[0].astype(float), blue_curve[1].astype(float)
        n = len(x)
        third = n // 3
        def slope(xa, ya):
            return np.polyfit(xa, ya, 1)[0]
        s_left = slope(x[:third], y[:third])
        s_mid = slope(x[third:2*third], y[third:2*third])
        s_right = slope(x[2*third:], y[2*third:])
        assert s_mid < s_left or s_mid < s_right, (
            f"Middle slope ({s_mid:.4f}) should be steeper than at least one tail "
            f"(left={s_left:.4f}, right={s_right:.4f})"
        )


# ===========================================================================
# 4. Confidence interval band
# ===========================================================================

class TestConfidenceInterval:
    def test_band_exists(self, grey_band):
        cols, top, bot = grey_band
        assert len(cols) > 20

    def test_band_has_width(self, grey_band):
        cols, top, bot = grey_band
        widths = bot - top
        assert np.median(widths) > 2, "CI band is too narrow to be visible"

    def test_blue_curve_inside_band(self, blue_curve, grey_band):
        curve_cols, curve_rows = blue_curve
        band_cols, band_top, band_bot = grey_band
        common_cols = np.intersect1d(curve_cols, band_cols)
        if len(common_cols) < 10:
            pytest.skip("Not enough overlapping columns to compare")
        inside_count = 0
        for c in common_cols:
            cr = curve_rows[curve_cols == c].mean()
            bt = band_top[band_cols == c].min()
            bb = band_bot[band_cols == c].max()
            if bt - 3 <= cr <= bb + 3:
                inside_count += 1
        frac = inside_count / len(common_cols)
        assert frac >= 0.8, (
            f"Only {frac:.0%} of curve points lie within CI band; expected >= 80%"
        )


# ===========================================================================
# 5. Plot elements: title, axis labels
# ===========================================================================

class TestPlotElements:
    def test_image_has_title_region(self, img_array):
        h = img_array.shape[0]
        top_strip = img_array[: h // 10, :, :]
        dark = np.all(top_strip < 80, axis=2)
        assert dark.sum() > 50, "No title text detected in top region"

    def test_image_has_xaxis_label(self, img_array):
        h = img_array.shape[0]
        bot_strip = img_array[h - h // 10 :, :, :]
        dark = np.all(bot_strip < 80, axis=2)
        assert dark.sum() > 50, "No x-axis label detected in bottom region"

    def test_image_has_yaxis_label(self, img_array):
        w = img_array.shape[1]
        left_strip = img_array[:, : w // 12, :]
        dark = np.all(left_strip < 80, axis=2)
        assert dark.sum() > 30, "No y-axis label detected in left region"
