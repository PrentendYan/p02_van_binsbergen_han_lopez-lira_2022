"""Run or update the project. This file uses the `doit` Python package. It works
like a Makefile, but is Python-based

"""

#######################################
## Configuration and Helpers for PyDoit
#######################################
## Make sure the src folder is in the path
import sys

sys.path.insert(1, "./src/")

from os import environ, getcwd, path
from pathlib import Path

from colorama import Fore, Style, init

## Custom reporter: Print PyDoit Text in Green
# This is helpful because some tasks write to sterr and pollute the output in
# the console. I don't want to mute this output, because this can sometimes
# cause issues when, for example, LaTeX hangs on an error and requires
# presses on the keyboard before continuing. However, I want to be able
# to easily see the task lines printed by PyDoit. I want them to stand out
# from among all the other lines printed to the console.

from settings import config

DATA_DIR = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")

##################################
## Man vs Machine pipeline (DAG)
##################################
## DAG: config -> load_data -> data_engineering -> [eda, train_rf, partial_dependence]
##      train_rf -> [stat_analysis, bias_analysis, table2]


def task_pipeline_load_data():
    """Pipeline step 1: Load raw data (WRDS CRSP/IBES/finratio + Philadelphia FED)."""
    raw_targets = [
        DATA_DIR / "crsp.csv",
        DATA_DIR / "ibes_summary.csv",
        DATA_DIR / "finratio.csv",
        DATA_DIR / "real_GDP_FED.csv",
        DATA_DIR / "IPT_FED.csv",
        DATA_DIR / "real_personal_consumption_FED.csv",
        DATA_DIR / "Unemployment_FED.csv",
    ]
    return {
        "actions": [
            "ipython ./src/settings.py",
            "python ./src/load_data.py",
        ],
        "targets": raw_targets,
        "file_dep": ["./src/settings.py", "./src/load_data.py"],
        "clean": [],
    }


def task_pipeline_data_engineering():
    """Pipeline step 2: IBES-CRSP link, macro, merge finratio -> processed_data/*.csv."""
    processed = DATA_DIR / "processed_data"
    targets = [
        DATA_DIR / "ibes_crsp.csv",
        processed / "macro_data.csv",
        processed / "A1.csv",
        processed / "A2.csv",
        processed / "Q1.csv",
        processed / "Q2.csv",
        processed / "Q3.csv",
    ]
    return {
        "actions": [
            "ipython ./src/settings.py",
            "python ./src/data_engineering.py",
        ],
        "targets": targets,
        "file_dep": [
            "./src/settings.py",
            "./src/functions.py",
            "./src/data_engineering.py",
            str(DATA_DIR / "crsp.csv"),
            str(DATA_DIR / "ibes_summary.csv"),
            str(DATA_DIR / "finratio.csv"),
            str(DATA_DIR / "real_GDP_FED.csv"),
            str(DATA_DIR / "IPT_FED.csv"),
            str(DATA_DIR / "real_personal_consumption_FED.csv"),
            str(DATA_DIR / "Unemployment_FED.csv"),
        ],
        "clean": [],
    }


def task_pipeline_eda():
    """Pipeline step 3: EDA on processed forecast data."""
    return {
        "actions": [
            "ipython ./src/settings.py",
            "python ./src/eda.py",
        ],
        "targets": [OUTPUT_DIR / "eda_forecast_summary.csv"],
        "file_dep": [
            "./src/settings.py",
            "./src/eda.py",
            str(DATA_DIR / "processed_data" / "A1.csv"),
            str(DATA_DIR / "processed_data" / "Q1.csv"),
        ],
        "clean": [],
    }


def task_pipeline_train_rf():
    """Pipeline step 4: Rolling-window RF (and OLS) training -> results/*_rf.csv."""
    results_dir = OUTPUT_DIR / "results"
    targets = [results_dir / f"{p}_rf.csv" for p in ["Q1", "Q2", "Q3", "A1", "A2"]]
    processed_dep = [
        str(DATA_DIR / "processed_data" / "macro_data.csv"),
        str(DATA_DIR / "processed_data" / "A1.csv"),
        str(DATA_DIR / "processed_data" / "A2.csv"),
        str(DATA_DIR / "processed_data" / "Q1.csv"),
        str(DATA_DIR / "processed_data" / "Q2.csv"),
        str(DATA_DIR / "processed_data" / "Q3.csv"),
    ]
    return {
        "actions": [
            "ipython ./src/settings.py",
            "python ./src/train_rf.py",
        ],
        "targets": targets,
        "file_dep": [
            "./src/settings.py",
            "./src/functions.py",
            "./src/train_rf.py",
        ] + processed_dep,
        "clean": [],
    }


def task_pipeline_stat_analysis():
    """Pipeline step 5: Regression (bias ~ alpha_i, beta_t, post_regulation)."""
    return {
        "actions": [
            "ipython ./src/settings.py",
            "python ./src/stat_analysis.py",
        ],
        "targets": [OUTPUT_DIR / "stat_analysis_regulation.txt"],
        "file_dep": [
            "./src/settings.py",
            "./src/stat_analysis.py",
            str(OUTPUT_DIR / "results" / "Q1_rf.csv"),
            str(OUTPUT_DIR / "results" / "A2_rf.csv"),
        ],
        "clean": [],
    }


def task_pipeline_partial_dependence():
    """Pipeline: Partial dependence plot (meanest -> realized EPS)."""
    return {
        "actions": [
            "ipython ./src/settings.py",
            "python ./src/partial_dependence.py",
        ],
        "targets": [OUTPUT_DIR / "images" / "partial_dependence_meanest.png"],
        "file_dep": [
            "./src/settings.py",
            "./src/functions.py",
            "./src/partial_dependence.py",
            str(DATA_DIR / "processed_data" / "macro_data.csv"),
            str(DATA_DIR / "processed_data" / "Q1.csv"),
        ],
        "clean": [],
    }


def task_pipeline_bias_analysis():
    """Pipeline: Bias analysis plots (analyst vs RF vs actual)."""
    images_dir = OUTPUT_DIR / "images"
    targets = [
        images_dir / f"{p}_RF_forecast_and_analyst_vs_actual.pdf"
        for p in ["Q1", "Q2", "Q3", "A1", "A2"]
    ]
    return {
        "actions": [
            "ipython ./src/settings.py",
            "python ./src/bias_analysis.py",
        ],
        "targets": targets,
        "file_dep": [
            "./src/settings.py",
            "./src/bias_analysis.py",
            str(OUTPUT_DIR / "results" / "Q1_rf.csv"),
            str(OUTPUT_DIR / "results" / "A2_rf.csv"),
        ],
        "clean": [],
    }


def task_pipeline_table2():
    """Pipeline: Table 2 term structure (RF, AF, AE, differences, Newey-West t-stats)."""
    return {
        "actions": [
            "ipython ./src/settings.py",
            "python ./src/table2_term_structure.py",
        ],
        "targets": [
            OUTPUT_DIR / "table2_term_structure.csv",
            OUTPUT_DIR / "table2_term_structure.txt",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/table2_term_structure.py",
            str(OUTPUT_DIR / "results" / "Q1_rf.csv"),
            str(OUTPUT_DIR / "results" / "Q2_rf.csv"),
            str(OUTPUT_DIR / "results" / "Q3_rf.csv"),
            str(OUTPUT_DIR / "results" / "A1_rf.csv"),
            str(OUTPUT_DIR / "results" / "A2_rf.csv"),
        ],
        "clean": [],
    }