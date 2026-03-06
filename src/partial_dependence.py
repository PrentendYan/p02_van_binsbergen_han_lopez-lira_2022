"""
Partial Dependence Plot: Realized EPS vs Analysts' Forecast (meanest).
Depends on: data_engineering (processed_data/macro_data.csv, Q1.csv).
Outputs: OUTPUT_DIR/images/partial_dependence_meanest.png
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from settings import config
from functions import read_merge_prepare_data

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
from sklearn.inspection import partial_dependence
from sklearn import preprocessing
from scipy.stats.mstats import winsorize

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
IMAGES_DIR = Path(config("IMAGES_DIR"))
IMAGES_DIR.mkdir(parents=True, exist_ok=True)


def run_partial_dependence(period=None):
    from settings import config
    if period is None:
        period = config("PDP_DEFAULT_PERIOD")
    macro_path = Path(config("PROCESSED_DIR")) / "macro_data.csv"
    if not macro_path.exists():
        print("Missing", macro_path)
        return
    Macro_Data = pd.read_csv(macro_path)
    df = read_merge_prepare_data(period, Macro_Data, data_dir=DATA_DIR)
    if df is None or len(df) == 0:
        return

    X = df.drop(columns=['adj_actual', 'Date', 'permno', 'numest']).copy()
    y = df['adj_actual'].copy()
    for col in X.columns:
        try:
            limits = config("WINSORIZE_LIMITS")
            X[col] = winsorize(X[col], limits=list(limits))
        except Exception:
            pass
    scaler = preprocessing.StandardScaler().fit(X)
    X_scaled = scaler.transform(X)
    X_scaled = pd.DataFrame(X_scaled, columns=X.columns)

    meanest_idx = list(X.columns).index('meanest') if 'meanest' in X.columns else 0
    rf_model = RandomForestRegressor(
        n_estimators=config("RF_N_ESTIMATORS"),
        max_depth=config("RF_MAX_DEPTH"),
        max_samples=config("RF_MAX_SAMPLES"),
        min_samples_leaf=1,
        max_features='sqrt',
        n_jobs=config("RF_N_JOBS"),
        random_state=42,
    )
    rf_model.fit(X_scaled, y)

    pdp_results = partial_dependence(
        rf_model, X_scaled, [meanest_idx],
        kind='average', grid_resolution=config("PDP_GRID_RESOLUTION"), percentiles=(0, 1)
    )
    x_vals = pdp_results['grid_values'][0]
    y_vals = pdp_results['average'][0]

    # ICE for 95% confidence interval (notebook style)
    ice_results = partial_dependence(
        rf_model, X_scaled, [meanest_idx],
        kind='individual', grid_resolution=config("PDP_GRID_RESOLUTION"), percentiles=(0, 1)
    )
    ice_lines = ice_results['individual'][0]
    n_samples = ice_lines.shape[0]
    std_dev = np.std(ice_lines, axis=0)
    sem = std_dev / np.sqrt(n_samples)
    margin_of_error = 1.96 * sem
    upper = y_vals + margin_of_error
    lower = y_vals - margin_of_error

    # Plot: paper style (Figure 1)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.fill_between(x_vals, lower, upper, color='grey', alpha=0.3, label='95% Confidence Interval')
    ax.plot(x_vals, y_vals, color='royalblue', linewidth=2, label='Partial Dependence')
    ax.axhline(0, color='black', linewidth=0.5)
    ax.set_xlim(-2, 5)
    ax.set_title("Figure 1: EPS as a non-Linear function of analysts' forecasts", fontsize=14)
    ax.set_xlabel("Analysts' Forecasts (Standardized)", fontsize=12)
    ax.set_ylabel("Realized EPS", fontsize=12)
    ax.legend(loc='upper left')
    ax.grid(True, linestyle='--', alpha=0.5)
    ax.set_facecolor('#f0f0f0')
    plt.tight_layout()
    out = IMAGES_DIR / "partial_dependence_meanest.png"
    plt.savefig(out, dpi=config("OUTPUT_DPI"), format='png')
    plt.close()
    print("Partial dependence plot saved to", out)


if __name__ == "__main__":
    run_partial_dependence()
