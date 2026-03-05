"""
Statistical analysis: regression to explain bias (post_regulation, N_analyst).
Replicates notebooks/stat_analysis_results.ipynb: Cell 6 (post_regulation only) and Cell 8 (+ N_analyst).
Outputs: OUTPUT_DIR/stat_analysis_regulation.txt (and printed summaries).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from settings import config

import numpy as np
import pandas as pd
import statsmodels.api as sm

OUTPUT_DIR = Path(config("OUTPUT_DIR"))
RESULTS_DIR = Path(config("RESULTS_DIR"))


def run_stat_analysis():
    periods = config("FORECAST_PERIODS")
    data = {}
    for period in periods:
        path = RESULTS_DIR / f"{period}_rf.csv"
        if not path.exists():
            print("Missing", path)
            return
        data[period] = pd.read_csv(path)

    lines = []
    for period in periods:
        data[period].rename(columns={'numest': 'N_analyst'}, inplace=True)
        data[period]['post_regulation'] = np.where(data[period]['Date'] > config("POST_REGULATION_DATE"), 1, 0)
        data[period]['alpha_i'] = data[period].groupby('permno')['bias_AF_ML'].transform('mean')
        data[period]['beta_t'] = data[period].groupby('Date')['bias_AF_ML'].transform('mean')

        # Regression 1: post_regulation only (notebook Cell 6)
        X1 = data[period][['alpha_i', 'beta_t', 'post_regulation']]
        y = data[period]['bias_AF_ML']
        model1 = sm.OLS(y, X1).fit()

        lines.append(f"Summary for {period} (post_regulation only):")
        lines.append(str(model1.summary()))
        gamma_coef = model1.params['post_regulation']
        gamma_pval = model1.pvalues['post_regulation']
        lines.append(f"\nHypothesis tests for {period}:")
        lines.append(f"gamma (post_regulation) coefficient: {gamma_coef}, p-value: {gamma_pval}")
        lines.append(f"Is gamma significant? {'Yes' if gamma_pval < 0.05 else 'No'}\n")

        # Regression 2: + N_analyst (notebook Cell 8)
        X2 = data[period][['alpha_i', 'beta_t', 'N_analyst', 'post_regulation']]
        model2 = sm.OLS(y, X2).fit()
        lines.append(f"Summary for {period} (+ N_analyst):")
        lines.append(str(model2.summary()))
        gamma_pval2 = model2.pvalues['post_regulation']
        lambda_pval = model2.pvalues['N_analyst']
        lines.append(f"gamma (post_regulation) p-value: {gamma_pval2}, lambda (N_analyst) p-value: {lambda_pval}")
        lines.append(f"Is gamma significant? {'Yes' if gamma_pval2 < 0.05 else 'No'}")
        lines.append(f"Is lambda significant? {'Yes' if lambda_pval < 0.05 else 'No'}\n")

    out = OUTPUT_DIR / "stat_analysis_regulation.txt"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(out, 'w') as f:
        f.write("\n".join(lines))
    print("Stat analysis saved to", out)
    for line in lines:
        print(line)
    return data


if __name__ == "__main__":
    run_stat_analysis()
