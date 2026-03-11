"""
Sanity checks for functions.py — macro extraction feeds RF features.
"""
import pandas as pd
import pytest
from functions import PrepareMacro


def test_prepare_macro_sanity():
    """Output has Dates + variable column and numeric values (required for merge)."""
    df = pd.DataFrame({
        "ROUTPUT65M11": [100.0, 101.0],
        "ROUTPUT65M12": [101.0, 102.0],
    })
    out = PrepareMacro(df, Begin_Year=65, Begin_Month=11, Name_col="ROUTPUT", Name_Var="GDP")
    assert "Dates" in out.columns and "GDP" in out.columns
    assert out["GDP"].notna().all() and pd.api.types.is_numeric_dtype(out["GDP"])
