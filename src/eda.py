"""
EDA for Man vs Machine: load processed forecast data and produce summary.
Depends on: data_engineering (processed_data/*.csv).
Outputs: OUTPUT_DIR/eda_forecast_summary.csv (optional).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from settings import config

import pandas as pd

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
PROCESSED_DIR = Path(config("PROCESSED_DIR"))


def run_eda():
    periods = config("FORECAST_PERIODS")
    forecast_data = {}
    for forecast in periods:
        path = PROCESSED_DIR / f"{forecast}.csv"
        if not path.exists():
            print("EDA: missing", path)
            return
        forecast_data[forecast] = pd.read_csv(path)
        if forecast_data[forecast].index.name is not None or 'Unnamed: 0' in forecast_data[forecast].columns:
            if 'Unnamed: 0' in forecast_data[forecast].columns:
                forecast_data[forecast] = forecast_data[forecast].drop(columns=['Unnamed: 0'], errors='ignore')
        forecast_data[forecast].reset_index(inplace=True, drop=True)

    summaries = []
    for name, df in forecast_data.items():
        summaries.append({
            'period': name,
            'n_obs': len(df),
            'n_permno': df['permno'].nunique(),
            'cols': len(df.columns),
        })
    summary_df = pd.DataFrame(summaries)
    out = OUTPUT_DIR / "eda_forecast_summary.csv"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(out, index=False)
    print("EDA summary saved to", out)
    return forecast_data


if __name__ == "__main__":
    run_eda()
