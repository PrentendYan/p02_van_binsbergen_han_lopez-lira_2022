"""
Extended pipeline: same analysis with data through 2026-02.
Outputs to _output_extended/, data to _data_extended/.
Existing _data/ and _output/ are not modified.

Usage:
    python src/run_extended.py

Requires: WRDS_USERNAME (and WRDS_PASSWORD) in .env for data download.
"""
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SRC_DIR))

# Override settings defaults BEFORE importing pipeline modules
from settings import defaults

BASE_DIR = defaults["BASE_DIR"]

# Separate data and output dirs
defaults["DATA_DIR"] = (BASE_DIR / "_data_extended").resolve()
defaults["PROCESSED_DIR"] = defaults["DATA_DIR"] / "processed_data"
defaults["OUTPUT_DIR"] = (BASE_DIR / "_output_extended").resolve()
defaults["RESULTS_DIR"] = defaults["OUTPUT_DIR"] / "results"
defaults["IMAGES_DIR"] = defaults["OUTPUT_DIR"] / "images"

# Extended time range
defaults["ROLLING_END_YEAR"] = 2026
defaults["ROLLING_N_LOOPS"] = 482      # test from 1986-01 to 2026-02
defaults["ROLLING_N_LOOPS_A2"] = 470   # test from 1987-01 to 2026-02

# Now import pipeline functions (they read config at import time)
from settings import config
from load_data import main as run_load_data
from data_engineering import run_data_engineering
from train_rf import run_train_rf
from partial_dependence import run_partial_dependence
from table2_term_structure import run_table2
from stat_analysis import run_stat_analysis
from bias_analysis import run_bias_analysis


def main():
    config("DATA_DIR").mkdir(parents=True, exist_ok=True)
    config("OUTPUT_DIR").mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Extended Pipeline (data through 2026-02)")
    print(f"  Data dir:   {config('DATA_DIR')}")
    print(f"  Output dir: {config('OUTPUT_DIR')}")
    print(f"  End year:   {config('ROLLING_END_YEAR')}")
    print("=" * 60)

    print("\n[1/7] Loading data from WRDS + Philadelphia Fed...")
    run_load_data()

    print("\n[2/7] Data engineering...")
    run_data_engineering()

    print("\n[3/7] Training RF models (rolling window)...")
    run_train_rf()

    print("\n[4/7] Partial dependence plot...")
    run_partial_dependence()

    print("\n[5/7] Table 2 (term structure)...")
    run_table2()

    print("\n[6/7] Statistical analysis...")
    run_stat_analysis()

    print("\n[7/7] Bias analysis plots...")
    run_bias_analysis()

    print("\n" + "=" * 60)
    print("Extended pipeline complete.")
    print(f"Results in: {config('OUTPUT_DIR')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
