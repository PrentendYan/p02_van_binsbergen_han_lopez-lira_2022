"""
Bias analysis: Analyst vs RF vs Actual time series and bias plots.
Depends on: pipeline_train_rf (results/*_rf.csv).
Outputs: OUTPUT_DIR/images/{period}_RF_forecast_and_analyst_vs_actual.pdf
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from settings import config

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from scipy import stats
from matplotlib.dates import YearLocator

OUTPUT_DIR = Path(config("OUTPUT_DIR"))
RESULTS_DIR = Path(config("RESULTS_DIR"))
IMAGES_DIR = Path(config("IMAGES_DIR"))
IMAGES_DIR.mkdir(parents=True, exist_ok=True)


def run_bias_analysis():
    """Produce bias comparison plots: analyst forecast vs RF prediction vs actual.

    For each forecast period, loads the corresponding *_rf.csv from RESULTS_DIR,
    aggregates by date using trimmed means, and plots three series (analyst,
    RF, actual). Saves one PDF per period to IMAGES_DIR.

    Skips periods whose results file is missing. No return value.
    """
    periods = config("FORECAST_PERIODS")
    locator = YearLocator(config("BIAS_PLOT_YEAR_LOCATOR"))

    for period in periods:
        path = RESULTS_DIR / f"{period}_rf.csv"
        if not path.exists():
            print("Missing", path)
            continue
        df = pd.read_csv(path)
        df['Date'] = df['Date'].astype(str)
        g = df.groupby('Date')
        dates = sorted(g.groups.keys())

        plt.figure(figsize=config("BIAS_FIGSIZE"), dpi=config("BIAS_DPI"))
        plt.plot(
            dates,
            g['meanest'].apply(lambda x: stats.trim_mean(x, 0.01)).reindex(dates).values,
            label='analyst forecast'
        )
        plt.plot(
            dates,
            g['predicted_adj_actual'].apply(lambda x: stats.trim_mean(x, 0.01)).reindex(dates).values,
            label='RF prediction'
        )
        plt.plot(
            dates,
            g['adj_actual'].apply(lambda x: stats.trim_mean(x, 0.01)).reindex(dates).values,
            label='actual value'
        )
        plt.gca().xaxis.set_major_locator(locator)
        plt.gcf().autofmt_xdate()
        plt.legend()
        plt.title(f'{period}: Analyst Forecast vs RF Prediction vs Actual Value')
        out = IMAGES_DIR / f"{period}_RF_forecast_and_analyst_vs_actual.pdf"
        plt.savefig(out, dpi=config("OUTPUT_DPI"), format='pdf')
        plt.close()
        print("Saved", out)
    print("Bias analysis done.")


if __name__ == "__main__":
    run_bias_analysis()
