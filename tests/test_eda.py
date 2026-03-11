"""
Sanity checks for EDA — summary structure required by replication.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def test_eda_summary_sanity_when_exists():
    """When eda_forecast_summary.csv exists, it has period, n_obs, n_permno and valid counts."""
    try:
        from settings import config
        out_path = config("OUTPUT_DIR") / "eda_forecast_summary.csv"
    except Exception:
        pytest.skip("settings not available")
    if not out_path.exists():
        pytest.skip("Run pipeline_eda first")
    summary = pd.read_csv(out_path)
    assert "period" in summary.columns and "n_obs" in summary.columns and "n_permno" in summary.columns
    assert len(summary) >= 1 and (summary["n_obs"] >= 0).all()
