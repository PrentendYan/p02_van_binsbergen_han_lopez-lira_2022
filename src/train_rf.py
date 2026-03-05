"""
Train Random Forest (and OLS) rolling-window models for Man vs Machine.
Depends on: data_engineering (processed_data/macro_data.csv, A1..Q3.csv).
Outputs: OUTPUT_DIR/results/{Q1,Q2,Q3,A1,A2}_rf.csv
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from settings import config

from functions import read_merge_prepare_data, train_test_rolling

import pandas as pd

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
RESULTS_DIR = Path(config("RESULTS_DIR"))
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def run_train_rf():
    periods = config("FORECAST_PERIODS")
    macro_path = Path(config("PROCESSED_DIR")) / "macro_data.csv"
    if not macro_path.exists():
        print("Missing", macro_path)
        return
    Macro_Data = pd.read_csv(macro_path)

    forecast_data = {}
    for forecast in periods:
        forecast_data[forecast] = read_merge_prepare_data(forecast, Macro_Data, data_dir=DATA_DIR)

    results_rolling = {}
    for forecast, df in forecast_data.items():
        print(forecast)
        out = RESULTS_DIR / f"{forecast}_rf.csv"
        if out.exists():
            print(f"Results for {forecast} already exist, skipping")
            continue
        results_rolling[forecast] = train_test_rolling(forecast, df)
        results_rolling[forecast].to_csv(out, index=False)
        print(f"Results for {forecast} saved to {out}")
    print("Pipeline train_rf done.")
    return results_rolling


if __name__ == "__main__":
    run_train_rf()
