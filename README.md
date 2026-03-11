# Man vs. Machine Learning — Replication

Replication of **van Binsbergen, Han & Lopez-Lira (2022)**, *Man vs. Machine Learning:
The Term Structure of Earnings Expectations and Conditional Biases*, Review of Financial Studies.

**Core finding:** Analyst consensus forecasts (AF) are *systematically biased upward* relative
to a Random Forest trained solely on public data (RF). Although analysts achieve lower raw RMSE
(they have access to private information), the price-scaled bias `(AF − RF) / P` is negative and
statistically highly significant at all five forecast horizons, revealing a conditional optimism
that persists throughout the term structure.

---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Source Code (`src/`)](#source-code-src)
3. [Pipeline](#pipeline)
4. [Notebooks (`notebooks/`)](#notebooks)
5. [Output Format (`_output/`)](#output-format-_output)
6. [Configuration](#configuration)
7. [Quick Start](#quick-start)
8. [Dependencies](#dependencies)

---

## Project Structure

```
.
├── dodo.py                      # doit task runner (pipeline DAG)
├── requirements.txt             # Python dependencies
├── .env.example                 # Template for .env (WRDS credentials, path overrides)
├── .env                         # local path overrides + WRDS credentials (not tracked)
├── .cruft.json                  # cookiecutter chartbook template metadata
├── chartbook.toml               # chartbook/docs configuration
├── PIPELINE.md                  # Pipeline documentation (DAG, steps, configuration)
├── REPLICATION_CHECKLIST.md     # Grading checklist for the replication project
│
├── src/                         # all pipeline scripts
│   ├── settings.py              # centralised config (paths, dates, RF params)
│   ├── functions.py             # shared utilities (macro prep, rolling-window RF)
│   ├── load_data.py             # Step 1 — pull raw data from WRDS + Philadelphia FED
│   ├── data_engineering.py      # Step 2 — IBES-CRSP link, EPS adjustment, merges
│   ├── eda.py                   # Step 3 — basic EDA on processed panel
│   ├── train_rf.py              # Step 4 — rolling-window Random Forest
│   ├── stat_analysis.py         # Step 5 — regression: bias ~ firm FE + time FE + regulation
│   ├── bias_analysis.py         # Plots: analyst vs RF vs actual EPS time series
│   ├── partial_dependence.py    # Partial dependence plot (meanest -> realized EPS)
│   ├── table2_term_structure.py # Table 2: RF, AF, AE means and Newey-West t-stats
│   ├── summary_stats.py         # Summary tables + figures for the replication report
│   ├── run_extended.py          # Extended-sample variant runner
│   └── generate_replication_latex.ipynb  # Notebook to generate LaTeX report programmatically
│
├── notebooks/
│   ├── code_walkthrough.ipynb   # Main walkthrough notebook (data + analysis)
│   ├── EDA.ipynb                # Exploratory data analysis
│   ├── RF.ipynb                 # Random Forest training experiments
│   ├── data_engineering.ipynb   # Data engineering steps
│   ├── stat_analysis_results.ipynb  # Statistical analysis results
│   └── partial_dependence_plot.ipynb # Partial dependence visualisation
│
├── tests/                       # Unit tests
│   ├── conftest.py              # Shared fixtures (project_root, images_dir, pdp_png)
│   └── test_partial_dependence.py  # Tests for Figure 1 (PDP curve properties)
│
├── _data/                       # Raw and processed data (gitignored, reproducible)
│   ├── crsp.csv                 # CRSP monthly returns + prices
│   ├── ibes_summary.csv         # IBES consensus forecasts
│   ├── ibes_crsp.csv            # Merged IBES-CRSP panel (post-link)
│   ├── finratio.csv             # Compustat financial ratios
│   ├── real_GDP_FED.csv         # Philadelphia FED real-time GDP vintages
│   ├── IPT_FED.csv              # Philadelphia FED industrial production vintages
│   ├── real_personal_consumption_FED.csv
│   ├── Unemployment_FED.csv
│   └── processed_data/
│       ├── macro_data.csv       # Merged macroeconomic variables
│       └── {Q1,Q2,Q3,A1,A2}.csv  # Per-horizon firm-month panels
│
├── _output/                     # All generated outputs (reproducible)
│   ├── eda_forecast_summary.csv
│   ├── stat_analysis_regulation.txt
│   ├── table2_term_structure.csv
│   ├── table2_term_structure.txt
│   ├── summary_stats_table.tex  # LaTeX: descriptive stats by horizon
│   ├── summary_stats_coverage.tex  # LaTeX: sample coverage by horizon
│   ├── results/
│   │   └── {Q1,Q2,Q3,A1,A2}_rf.csv  # RF predictions + panel variables
│   └── images/
│       ├── fig_bias_distribution.png
│       ├── fig_sample_coverage.png
│       ├── fig_rmse_comparison.png
│       ├── partial_dependence_meanest.png
│       └── {Q1,Q2,Q3,A1,A2}_RF_forecast_and_analyst_vs_actual.pdf
│
├── _output_extended/            # Same structure for extended-sample run
│
├── reports/
│   ├── replication_report_generated.tex  # Auto-generated LaTeX replication report
│   └── replication_report.pdf   # Compiled report (26 pages, gitignored)
│
├── assets/
│   ├── figure1.png              # Original Figure 1 from the paper (for comparison)
│   └── table2.png               # Original Table 2 from the paper (for comparison)
│
└── docs/                        # Built Sphinx/jupyter-book documentation (GitHub Pages)
```

---

## Source Code (`src/`)

### `settings.py` — Centralised Configuration

All paths, dates, and hyperparameters are defined here and consumed by every other script
via `from settings import config`. Variables can be overridden at three levels (highest priority first):

| Priority | Method | Example |
|----------|--------|---------|
| 1 | CLI argument | `python train_rf.py --OUTPUT_DIR=/my/path` |
| 2 | Environment variable / `.env` | `OUTPUT_DIR=/my/path` in `.env` |
| 3 | `settings.py` defaults | hardcoded defaults in `defaults` dict |

Key configuration parameters:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `_data/` | Raw and processed data |
| `OUTPUT_DIR` | `_output/` | All generated outputs |
| `PROCESSED_DIR` | `_data/processed_data/` | Per-horizon CSV panels |
| `RESULTS_DIR` | `_output/results/` | RF prediction CSVs |
| `IMAGES_DIR` | `_output/images/` | Generated figures |
| `FORECAST_PERIODS` | `[Q1,Q2,Q3,A1,A2]` | Five forecast horizons |
| `ROLLING_TRAIN_LENGTH` | `11` (years) | Rolling window for Q1–A1 |
| `ROLLING_TRAIN_LENGTH_A2` | `23` (years) | Rolling window for A2 |
| `ROLLING_N_LOOPS` | `408` | Monthly out-of-sample windows |
| `RF_N_ESTIMATORS` | `2000` | Trees per forest |
| `RF_MAX_DEPTH` | `7` | Maximum tree depth |
| `RF_MAX_SAMPLES` | `0.01` | 1% row subsample per tree |
| `RF_MIN_SAMPLES_LEAF` | `5` | Minimum leaf size |
| `POST_REGULATION_DATE` | `2000-10` | Regulation FD cutoff |

---

### `load_data.py` — Step 1: Raw Data Acquisition

Pulls four data sources and saves them to `_data/`:

- **CRSP** (`crsp.csv`): Monthly stock returns, prices, and cumulative adjustment factors
  (`cfacshr`) via WRDS. Used for price scaling and split-adjustment.
- **IBES Summary** (`ibes_summary.csv`): Consensus mean analyst forecasts (`meanest`),
  actual EPS, number of estimates, fiscal period end dates.
- **Compustat Financial Ratios** (`finratio.csv`): ~60 firm-level accounting ratios
  (leverage, profitability, liquidity, valuation) used as RF features.
- **Philadelphia FED Real-Time Vintages**: Four macro series downloaded as CSV —
  real GDP, industrial production, real personal consumption, unemployment.

> **Requires:** `WRDS_USERNAME` and `WRDS_PASSWORD` in `.env`.

---

### `data_engineering.py` — Step 2: Panel Construction

Three major operations:

1. **IBES–CRSP Link via CUSIP.** Matches IBES tickers to CRSP PERMNOs through 8-digit CUSIP,
   enforcing a date-range overlap filter to avoid stale links after mergers or ticker reuse.

2. **Split-Adjusted EPS.** When a stock split occurs between the analyst estimate date and the
   earnings announcement date, raw IBES EPS figures are on different per-share bases. The
   adjustment is:

   ```
   adj_actual = actual × (cfacshr_estimate_date / cfacshr_announcement_date)
   ```

3. **Macro Merge (no look-ahead).** Philadelphia FED real-time vintages are merged with
   `merge_asof(..., direction='backward')` on the estimate date, so only data released
   *before* the forecast date is used.

4. **Three-Pass Financial Ratio Imputation.**
   - Pass 1: Fill missing values with same-month industry median (Fama-French 49-industry).
   - Pass 2: Forward/backward fill within firm.
   - Pass 3: Remaining gaps filled with industry median again.
   After three passes, zero missing values remain in the financial ratio columns.

**Outputs:** `_data/ibes_crsp.csv`, `_data/processed_data/macro_data.csv`,
`_data/processed_data/{Q1,Q2,Q3,A1,A2}.csv`

---

### `functions.py` — Shared Utilities

Contains functions used by multiple pipeline scripts:

- **`PrepareMacro()`**: Loads Philadelphia FED CSV files, computes log returns for GDP /
  industrial production / consumption, and returns a single macro DataFrame.
- **`read_merge_prepare_data(period)`**: Loads the processed panel for one horizon,
  merges macro data via backward `merge_asof`, winsorises extreme EPS values at the 10th
  percentile, and returns a cleaned DataFrame ready for the rolling-window loop.
- **`train_test_rolling(df, ...)`**: Implements the rolling-window evaluation loop.
  For each month `t`, trains a `RandomForestRegressor` on `[t−W, t−1]` and predicts
  only month `t`. A `StandardScaler` is fit on training data only (no leakage).

---

### `train_rf.py` — Step 4: Rolling-Window Random Forest

Calls `read_merge_prepare_data` and `train_test_rolling` for each of the five horizons.
For each horizon, the RF is retrained 408 times (one per out-of-sample month).

**Feature matrix:** consensus analyst forecast (`meanest`), lagged EPS (`adj_past_eps`),
stock price, monthly return, four macro variables (GDP growth, consumption growth,
industrial production growth, unemployment level), and ~60 Compustat financial ratios.

**RF hyperparameters** match the paper: `n_estimators=2000`, `max_depth=7`,
`max_samples=0.01` (1% subsample per tree, preventing large-cap dominance),
`min_samples_leaf=5`.

**Output columns** added to the panel:
- `predicted_adj_actual`: RF out-of-sample prediction
- `bias_AF_ML`: `(meanest − predicted_adj_actual) / abs(price)`

**Outputs:** `_output/results/{Q1,Q2,Q3,A1,A2}_rf.csv`

---

### `stat_analysis.py` — Step 5: Regression Analysis

Estimates the panel regression:

```
bias_AF_ML(i,t) = alpha_i + beta_t + gamma * post_regulation_t + epsilon(i,t)
```

where `alpha_i` are firm fixed effects, `beta_t` are time fixed effects, and
`post_regulation_t` is a dummy equal to 1 after Regulation FD (October 2000).
Standard errors are clustered by time. Results saved to `_output/stat_analysis_regulation.txt`.

---

### `table2_term_structure.py` — Table 2 Replication

Reproduces Table 2 of the paper: for each horizon, computes time-series means of cross-sectional
means of RF, AF, AE, and their differences. Reports Newey-West standard errors (3 lags for
quarterly horizons, 12 lags for annual horizons).

**Outputs:** `_output/table2_term_structure.csv`, `_output/table2_term_structure.txt`

---

### `bias_analysis.py` — Time-Series Bias Plots

For each horizon, plots the 12-month rolling average of analyst consensus, RF prediction,
and realised EPS over time. Saves one PDF per horizon to `_output/images/`.

---

### `partial_dependence.py` — Partial Dependence Plot

Trains the RF on the full Q1 sample and computes partial dependence of the prediction on
`meanest` (analyst consensus). Reveals whether and how the RF discounts analyst forecasts.

**Output:** `_output/images/partial_dependence_meanest.png`

---

### `summary_stats.py` — Report Tables and Figures

Reads `_output/results/*_rf.csv` and generates all summary materials for the replication report:

| Output | Description |
|--------|-------------|
| `_output/summary_stats_table.tex` | Descriptive statistics (mean, std, percentiles) for 6 key variables × 5 horizons |
| `_output/summary_stats_coverage.tex` | Sample coverage (obs, firms, date range, avg firms/month, avg analysts) |
| `_output/images/fig_bias_distribution.png` | KDE of `(AF−RF)/P` by horizon |
| `_output/images/fig_sample_coverage.png` | Unique firms per year by horizon |
| `_output/images/fig_rmse_comparison.png` | RF vs analyst RMSE and MAE bar chart |

---

## Pipeline

The pipeline is managed by `doit` (analogous to `make`). Tasks are defined in `dodo.py`
and form a directed acyclic graph (DAG):

```
pipeline_load_data
        |
        v
pipeline_data_engineering
        |
   -----+-----+---------------------+
   |         |                     |
   v         v                     v
pipeline_eda  pipeline_train_rf  pipeline_partial_dependence
                   |
          ---------+---------+
          |                  |
          v                  v
pipeline_stat_analysis  pipeline_bias_analysis
          |
          v
pipeline_table2
```

### Task Summary

| Task | Script | Key Inputs | Key Outputs |
|------|--------|-----------|-------------|
| `pipeline_load_data` | `load_data.py` | WRDS, Philadelphia FED | `_data/*.csv` |
| `pipeline_data_engineering` | `data_engineering.py` | `_data/*.csv` | `_data/processed_data/*.csv` |
| `pipeline_eda` | `eda.py` | `processed_data/*.csv` | `eda_forecast_summary.csv` |
| `pipeline_train_rf` | `train_rf.py` | `processed_data/*.csv` | `results/*_rf.csv` |
| `pipeline_stat_analysis` | `stat_analysis.py` | `results/*_rf.csv` | `stat_analysis_regulation.txt` |
| `pipeline_partial_dependence` | `partial_dependence.py` | `processed_data/Q1.csv` | `partial_dependence_meanest.png` |
| `pipeline_bias_analysis` | `bias_analysis.py` | `results/*_rf.csv` | `*_RF_forecast_and_analyst_vs_actual.pdf` |
| `pipeline_table2` | `table2_term_structure.py` | `results/*_rf.csv` | `table2_term_structure.{csv,txt}` |

---

## Notebooks

| Notebook | Purpose |
|----------|---------|
| [`notebooks/code_walkthrough.ipynb`](notebooks/code_walkthrough.ipynb) | **Main walkthrough.** Introduces the three pipeline stages with code snippets, live data loading, and all key result plots. Covers data engineering, rolling-window RF design, RMSE/bias comparison across horizons, and the KDE of `(AF−RF)/P`. Intended as the primary entry point for readers new to the codebase. |
| [`notebooks/EDA.ipynb`](notebooks/EDA.ipynb) | Exploratory analysis of the processed firm-month panel (distribution of EPS, analyst coverage, sample composition). |
| [`notebooks/data_engineering.ipynb`](notebooks/data_engineering.ipynb) | Step-through of the IBES-CRSP link, split-adjustment logic, and macro merge. |
| [`notebooks/RF.ipynb`](notebooks/RF.ipynb) | Experiments with Random Forest hyperparameters and feature importance. |
| [`notebooks/stat_analysis_results.ipynb`](notebooks/stat_analysis_results.ipynb) | Interactive display of regression output and Table 2 results. |
| [`notebooks/partial_dependence_plot.ipynb`](notebooks/partial_dependence_plot.ipynb) | Generates and annotates the partial dependence plot of the RF w.r.t. analyst consensus. |

---

## Output Format (`_output/`)

### `results/{period}_rf.csv`

One file per forecast horizon. Each row is a firm-month observation in the out-of-sample
prediction window. Key columns:

| Column | Description |
|--------|-------------|
| `permno` | CRSP permanent security identifier |
| `Date` | Forecast month (YYYY-MM-DD) |
| `meanest` | Consensus mean analyst forecast (AF), $/share |
| `adj_actual` | Split-adjusted realised EPS (AE), $/share |
| `predicted_adj_actual` | RF out-of-sample prediction, $/share |
| `bias_AF_ML` | `(meanest − predicted_adj_actual) / abs(price)` |
| `numest` | Number of contributing analysts |
| `price` | CRSP closing price (absolute value), $ |
| `adj_past_eps` | Prior-period split-adjusted EPS |
| `ret` | Monthly stock return |
| `GDP_log_return`, `Cons_log_return`, `IPT_log_return` | Macro growth rates (as-of) |
| `Unempl` | Unemployment level (as-of) |
| ~60 Compustat columns | Financial ratios (leverage, profitability, liquidity, …) |

### `summary_stats_table.tex` / `summary_stats_coverage.tex`

LaTeX `tabular` fragments (no `\begin{table}` wrapper) intended for `\input{}` inside the
report. All numbers are plain ASCII — compatible with `pdflatex` and `siunitx` S-columns.

### Figure files

| File | Format | Content |
|------|--------|---------|
| `fig_bias_distribution.png` | PNG 120 dpi | KDE of `(AF−RF)/P` for all 5 horizons |
| `fig_sample_coverage.png` | PNG 120 dpi | Unique firms per year by horizon |
| `fig_rmse_comparison.png` | PNG 120 dpi | RMSE and MAE bar chart (RF vs analyst) |
| `partial_dependence_meanest.png` | PNG 100 dpi | PDP of RF prediction w.r.t. `meanest` |
| `{period}_RF_forecast_and_analyst_vs_actual.pdf` | PDF | Rolling 12-month average time series |

---

## Configuration

All scripts import configuration exclusively through `src/settings.py`:

```python
from settings import config
DATA_DIR    = Path(config("DATA_DIR"))
OUTPUT_DIR  = Path(config("OUTPUT_DIR"))
RF_N_ESTIMATORS = config("RF_N_ESTIMATORS")
```

To customise paths without modifying source code, create or edit `.env` in the project root:

```ini
# .env (not tracked by git)
WRDS_USERNAME=yourname
WRDS_PASSWORD=yourpassword
DATA_DIR=/custom/path/to/data
OUTPUT_DIR=/custom/path/to/output
```

Alternatively, pass any variable on the command line:

```bash
python src/train_rf.py --OUTPUT_DIR=/custom/output
```

---

## Quick Start

### Prerequisites

- Python environment with all dependencies installed (see below)
- WRDS account for raw data access
- MacTeX / TeX Live for compiling the replication report

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

> Note: The project uses `python3` from the active conda/venv environment.
> Do not use the `.venv` folder if it only contains pip+setuptools.

### 2. Configure credentials

```bash
# Copy and edit the environment file
cp .env.example .env   # or create .env manually
# Set WRDS_USERNAME, WRDS_PASSWORD, and any path overrides
```

### 3. Run the full pipeline

```bash
doit
```

`doit` will execute all tasks in dependency order, skipping tasks whose outputs are
already up-to-date (similar to `make`).

### 4. Run individual steps

```bash
doit pipeline_load_data
doit pipeline_data_engineering
doit pipeline_train_rf
doit pipeline_stat_analysis
doit pipeline_bias_analysis
doit pipeline_table2
```

### 5. Generate summary statistics and figures

```bash
python3 src/summary_stats.py
```

### 6. Compile the replication report

```bash
cd reports
/usr/local/texlive/2025/bin/universal-darwin/pdflatex replication_report_generated.tex
/usr/local/texlive/2025/bin/universal-darwin/pdflatex replication_report_generated.tex
```

Output: `reports/replication_report_generated.pdf`.

### 7. Run tests

```bash
pytest --doctest-modules
```

### 8. Code formatting

```bash
ruff format . && ruff check --select I --fix . && ruff check --fix .
```

---

## Dependencies

### Python packages

| Package | Role |
|---------|------|
| `pandas`, `numpy` | Data manipulation |
| `scikit-learn` | Random Forest, StandardScaler |
| `statsmodels` | Panel regression, Newey-West SE |
| `scipy` | KDE for bias distribution plots |
| `matplotlib` | All figures |
| `wrds` | WRDS database connection |
| `python-decouple` | Configuration loading from `.env` |
| `doit` | Pipeline task runner |
| `tqdm` | Progress bars in rolling-window loop |
| `ruff`, `black` | Code formatting and linting |
| `pytest` | Unit and doc tests |
| `jupyter`, `jupyterlab` | Notebook execution |

Install all with:

```bash
pip install -r requirements.txt
```

### LaTeX

MacTeX 2025 (or TeX Live equivalent) with packages: `booktabs`, `threeparttable`,
`siunitx`, `caption`, `float`, `hyperref`, `biblatex`/`bibtex`.

### Data access

WRDS subscription required for: CRSP (`crsp.msf`, `crsp.stocknames`),
IBES (`ibes.statsumu`, `ibes.id`), Compustat (`comp.funda`, financial ratios).

Philadelphia FED real-time data is downloaded from public URLs (no credentials needed).
