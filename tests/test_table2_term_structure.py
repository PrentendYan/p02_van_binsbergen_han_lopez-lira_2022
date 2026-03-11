"""
Sanity checks for Table 2 term structure — replication formulas and result data.
"""
import numpy as np
import pandas as pd
import pytest
from pathlib import Path

from table2_term_structure import (
    HORIZON_LABELS,
    NW_LAGS,
    _newey_west_tstat,
    compute_table2_row,
)


def _make_table2_df(n_dates=20, n_firms_per_date=10, rf_mean=0.2, ae_mean=0.2, af_mean=0.25, bias_af_ml=-0.01):
    rows = []
    for d in range(n_dates):
        date_str = f"1990-{(d % 12) + 1:02d}"
        for _ in range(n_firms_per_date):
            rows.append({
                "Date": date_str,
                "predicted_adj_actual": rf_mean + np.random.randn() * 0.05,
                "meanest": af_mean + np.random.randn() * 0.05,
                "adj_actual": ae_mean + np.random.randn() * 0.05,
                "bias_AF_ML": bias_af_ml + np.random.randn() * 0.005,
            })
    return pd.DataFrame(rows)


def test_newey_west_constant_returns_nan():
    """Constant series → zero variance → t-stat NaN (formula sanity)."""
    s = pd.Series([1.0] * 30)
    assert np.isnan(_newey_west_tstat(s, maxlags=3))


def test_compute_table2_row_sanity():
    """Row has paper columns and N = number of observations."""
    df = _make_table2_df(n_dates=5, n_firms_per_date=4)
    row = compute_table2_row("Q1", df)
    required = ["Horizon", "RF", "AF", "AE", "(RF-AE)", "(AF-AE)", "(RF-AE)^2", "(AF-AE)^2", "(AF-RF)/P", "N",
                "t(RF-AE)", "t(AF-AE)", "t((AF-RF)/P)"]
    for k in required:
        assert k in row, f"Row missing: {k}"
    assert row["N"] == len(df)
    assert row["Horizon"] == HORIZON_LABELS["Q1"]
    assert NW_LAGS["Q1"] == 3 and NW_LAGS["A1"] == 12


def test_rf_csv_sanity_when_exists():
    """When results/*_rf.csv exist: required columns and key numeric not all NaN."""
    try:
        from settings import config
        results_dir = Path(config("RESULTS_DIR")) if isinstance(config("RESULTS_DIR"), str) else config("RESULTS_DIR")
        periods = list(config("FORECAST_PERIODS"))
    except Exception:
        pytest.skip("settings not available")
    if not results_dir.exists():
        pytest.skip("RESULTS_DIR not available")
    required = {"Date", "predicted_adj_actual", "meanest", "adj_actual", "bias_AF_ML"}
    found = False
    for period in periods:
        path = results_dir / f"{period}_rf.csv"
        if not path.exists():
            continue
        found = True
        df = pd.read_csv(path)
        assert required <= set(df.columns), f"{period}_rf.csv missing: {required - set(df.columns)}"
        for col in ["predicted_adj_actual", "meanest", "adj_actual", "bias_AF_ML"]:
            assert df[col].notna().sum() >= 1, f"{period}_rf.csv {col} all NaN"
    if not found:
        pytest.skip("No results/*_rf.csv found")
