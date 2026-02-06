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
from doit.reporter import ConsoleReporter

from settings import config

try:
    in_slurm = environ["SLURM_JOB_ID"] is not None
except:
    in_slurm = False


class GreenReporter(ConsoleReporter):
    def write(self, stuff, **kwargs):
        doit_mark = stuff.split(" ")[0].ljust(2)
        task = " ".join(stuff.split(" ")[1:]).strip() + "\n"
        output = (
            Fore.GREEN
            + doit_mark
            + f" {path.basename(getcwd())}: "
            + task
            + Style.RESET_ALL
        )
        self.outstream.write(output)


if not in_slurm:
    DOIT_CONFIG = {
        "reporter": GreenReporter,
        # other config here...
        # "cleanforget": True, # Doit will forget about tasks that have been cleaned.
        "backend": "sqlite3",
        "dep_file": "./.doit-db.sqlite",
    }
else:
    DOIT_CONFIG = {"backend": "sqlite3", "dep_file": "./.doit-db.sqlite"}
init(autoreset=True)


BASE_DIR = config("BASE_DIR")
DATA_DIR = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")
OS_TYPE = config("OS_TYPE")
USER = config("USER")


##################################
## Begin rest of PyDoit tasks here
##################################


def task_config():
    """Create empty directories for data and output if they don't exist"""
    return {
        "actions": ["ipython ./src/settings.py"],
        "targets": [DATA_DIR, OUTPUT_DIR],
        "file_dep": ["./src/settings.py"],
        "clean": [],
    }


def task_pull():
    """Pull WRDS data only"""
    # 1) CRSP → crsp_m.parquet（under DATA_DIR/WRDS ）
    yield {
        "name": "crsp_stock",
        "doc": "Pull CRSP stock data from WRDS",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_CRSP_stock.py",
        ],
        "targets": [DATA_DIR / "WRDS" / "crsp_m.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_CRSP_stock.py"],
        "clean": [],
    }

    # 2) Compustat + ccm → compa, comp_quarterly, ccm
    yield {
        "name": "crsp_compustat",
        "doc": "Pull CRSP-Compustat link and Compustat annual/quarterly from WRDS",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_CRSP_Compustat.py",
        ],
        "targets": [
            DATA_DIR / "compa.parquet",
            DATA_DIR / "comp_quarterly.parquet",
            DATA_DIR / "ccm.parquet",
        ],
        "file_dep": ["./src/settings.py", "./src/pull_CRSP_Compustat.py"],
        "clean": [],
    }

    # 3) IBES EPS forecast/actual + CRSP-IBES link (dependent on crsp_m)
    yield {
        "name": "pull_eps",
        "doc": "Pull IBES EPS forecast/actual and CRSP-IBES link from WRDS",
        "actions": ["ipython ./src/settings.py", "ipython ./src/pull_eps.py"],
        "targets": [
            DATA_DIR / "Forecast_EPS_summary_unadjusted_1986_2019.parquet",
            DATA_DIR / "Actual_EPS_summary_unadjusted_1986_2019.parquet",
            DATA_DIR / "crsp_ibes_link.parquet",
            DATA_DIR / "crsp_ibes_linked.parquet",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/pull_eps.py",
            str(DATA_DIR / "WRDS" / "crsp_m.parquet"),
        ],
        "clean": [],
    }

    # 4) Fama-French factors → FF_FACTORS.parquet
    yield {
        "name": "pull_ff",
        "doc": "Pull Fama-French factors from WRDS",
        "actions": ["ipython ./src/settings.py", "ipython ./src/pull_ff.py"],
        "targets": [DATA_DIR / "FF_FACTORS.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_ff.py"],
        "clean": [],
    }
    # 5) fred data → fred.parquet
    yield {
        "name": "pull_fred",
        "doc": "Pull fred data from FRED",
        "actions": ["ipython ./src/settings.py", "ipython ./src/pull_fred.py"],
        "targets": [DATA_DIR / "fred.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_fred.py"],
        "clean": [],
    }


def task_generate_charts():
    """Generate exploratory HTML charts from data"""
    yield {
        "name": "generate_charts",
        "doc": "Generate exploratory HTML charts (CRSP, Compustat, IBES, FF, FRED) from data",
        "actions": ["ipython ./src/generate_chart.py"],
        "targets": [
            OUTPUT_DIR / "explore_crsp.html",
            OUTPUT_DIR / "explore_compustat.html",
            OUTPUT_DIR / "explore_ibes.html",
            OUTPUT_DIR / "explore_ff.html",
            OUTPUT_DIR / "explore_fred.html",
        ],
        "file_dep": [
            "./src/generate_chart.py",
            str(DATA_DIR / "WRDS" / "crsp_m.parquet"),
            str(DATA_DIR / "compa.parquet"),
            str(DATA_DIR / "Forecast_EPS_summary_unadjusted_1986_2019.parquet"),
            str(DATA_DIR / "FF_FACTORS.parquet"),
            str(DATA_DIR / "fred.parquet"),
        ],
        "clean": True,
    }