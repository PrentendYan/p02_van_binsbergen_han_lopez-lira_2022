"""
Sanity checks for sample balance — period, time, and firm imbalance.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

# Thresholds (tunable)
MAX_PERIOD_IMBALANCE_RATIO = 10.0   # max(n_obs)/min(n_obs) across periods
MIN_OBS_FRACTION = 0.05             # no period should have < 5% of total obs
MAX_FIRM_SHARE = 0.10               # no single permno should have > 10% of period obs
MIN_OBS_PER_DATE = 1                # at least 1 obs per date (or skip if too strict)


def test_period_imbalance_when_eda_summary_exists():
    """When eda_forecast_summary.csv exists: no extreme period imbalance (n_obs)."""
    try:
        from settings import config
        out_path = config("OUTPUT_DIR") / "eda_forecast_summary.csv"
    except Exception:
        pytest.skip("settings not available")
    if not out_path.exists():
        pytest.skip("Run pipeline_eda first")
    summary = pd.read_csv(out_path)
    n_obs = summary["n_obs"].values
    total = n_obs.sum()
    assert total >= 1, "Total n_obs should be >= 1"
    # No period has < MIN_OBS_FRACTION of total
    for i, n in enumerate(n_obs):
        assert n >= MIN_OBS_FRACTION * total, (
            f"Period {summary.iloc[i]['period']} has n_obs={n} < {MIN_OBS_FRACTION*total:.0f} ({(n/total)*100:.1f}% of total)"
        )
    # max/min ratio across periods not too large
    n_obs_nonzero = n_obs[n_obs > 0]
    if len(n_obs_nonzero) < 2:
        return
    ratio = n_obs_nonzero.max() / n_obs_nonzero.min()
    assert ratio <= MAX_PERIOD_IMBALANCE_RATIO, (
        f"Period n_obs imbalance ratio {ratio:.1f} > {MAX_PERIOD_IMBALANCE_RATIO}"
    )


def test_time_balance_when_processed_data_exists():
    """When processed_data/*.csv exist: no date with zero obs (or very few dates empty)."""
    try:
        from settings import config
        processed_dir = config("PROCESSED_DIR")
        periods = list(config("FORECAST_PERIODS"))
    except Exception:
        pytest.skip("settings not available")
    if not processed_dir.exists():
        pytest.skip("PROCESSED_DIR not available")
    date_col = None
    found = False
    for period in periods:
        path = processed_dir / f"{period}.csv"
        if not path.exists():
            continue
        df = pd.read_csv(path)
        if date_col is None:
            date_col = "Date" if "Date" in df.columns else ("rankdate" if "rankdate" in df.columns else None)
        if date_col not in df.columns:
            continue
        found = True
        by_date = df.groupby(date_col).size()
        zeros = (by_date < MIN_OBS_PER_DATE).sum()
        assert zeros == 0, (
            f"{period}: {zeros} date(s) have < {MIN_OBS_PER_DATE} obs"
        )
        break
    if not found:
        pytest.skip("No processed_data/*.csv found")


def test_firm_balance_when_processed_data_exists():
    """When processed_data/*.csv exist: no single firm (permno) dominates (> MAX_FIRM_SHARE of obs)."""
    try:
        from settings import config
        processed_dir = config("PROCESSED_DIR")
        periods = list(config("FORECAST_PERIODS"))
    except Exception:
        pytest.skip("settings not available")
    if not processed_dir.exists():
        pytest.skip("PROCESSED_DIR not available")
    found = False
    for period in periods:
        path = processed_dir / f"{period}.csv"
        if not path.exists():
            continue
        df = pd.read_csv(path)
        if "permno" not in df.columns:
            continue
        found = True
        n_total = len(df)
        by_firm = df.groupby("permno").size()
        max_share = by_firm.max() / n_total
        assert max_share <= MAX_FIRM_SHARE, (
            f"{period}: one permno has {max_share*100:.1f}% of obs (max {MAX_FIRM_SHARE*100:.0f}% allowed)"
        )
        break
    if not found:
        pytest.skip("No processed_data/*.csv found")


def test_eda_summary_n_permno_balance():
    """When eda_forecast_summary exists: n_permno across periods not extremely skewed."""
    try:
        from settings import config
        out_path = config("OUTPUT_DIR") / "eda_forecast_summary.csv"
    except Exception:
        pytest.skip("settings not available")
    if not out_path.exists():
        pytest.skip("Run pipeline_eda first")
    summary = pd.read_csv(out_path)
    n_permno = summary["n_permno"].values
    n_permno = n_permno[n_permno > 0]
    if len(n_permno) < 2:
        return
    ratio = n_permno.max() / n_permno.min()
    assert ratio <= MAX_PERIOD_IMBALANCE_RATIO, (
        f"Period n_permno imbalance ratio {ratio:.1f} > {MAX_PERIOD_IMBALANCE_RATIO}"
    )
