"""
Sanity checks for bias_analysis — RF result CSV has required columns for plots.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def test_rf_csv_sanity_when_exists():
    """When results/Q1_rf.csv exists, it has columns needed for bias plots and valid Date."""
    try:
        from settings import config
        results_dir = config("RESULTS_DIR")
        q1_path = Path(results_dir) / "Q1_rf.csv" if isinstance(results_dir, str) else results_dir / "Q1_rf.csv"
    except Exception:
        pytest.skip("settings not available")
    if not q1_path.exists():
        pytest.skip("Run pipeline_train_rf first to generate results/Q1_rf.csv")
    df = pd.read_csv(q1_path)
    required = {"Date", "meanest", "predicted_adj_actual", "adj_actual"}
    assert required <= set(df.columns), f"Missing: {required - set(df.columns)}"
    assert len(df) >= 1 and df["Date"].notna().all()
