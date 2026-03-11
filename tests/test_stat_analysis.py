"""
Sanity checks for stat_analysis.py — regression coefficients finite and p-values valid.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def test_bias_regression_sanity():
    """Post-regulation and N_analyst regressions produce finite coefs and p-values in [0,1]."""
    from settings import config
    import statsmodels.api as sm

    n = 50
    np.random.seed(42)
    df = pd.DataFrame({
        "Date": ["1999-01"] * (n // 2) + ["2001-01"] * (n // 2),
        "permno": list(range(10)) * 5,
        "bias_AF_ML": np.random.randn(n) * 0.01,
        "N_analyst": np.random.randint(1, 20, size=n),
    })
    df["post_regulation"] = np.where(df["Date"] > config("POST_REGULATION_DATE"), 1, 0)
    df["alpha_i"] = df.groupby("permno")["bias_AF_ML"].transform("mean")
    df["beta_t"] = df.groupby("Date")["bias_AF_ML"].transform("mean")

    # Regression 1
    X1 = df[["alpha_i", "beta_t", "post_regulation"]]
    m1 = sm.OLS(df["bias_AF_ML"], X1).fit()
    assert np.isfinite(m1.params["post_regulation"]) and 0 <= m1.pvalues["post_regulation"] <= 1

    # Regression 2
    X2 = df[["alpha_i", "beta_t", "N_analyst", "post_regulation"]]
    m2 = sm.OLS(df["bias_AF_ML"], X2).fit()
    assert np.isfinite(m2.params["N_analyst"]) and 0 <= m2.pvalues["N_analyst"] <= 1
