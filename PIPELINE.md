# Man vs Machine Pipeline (van Binsbergen, Han, Lopez-Lira 2022)

End-to-end pipeline run as a DAG via `doit`. Config and paths are centralized in `src/settings.py`.

## Project structure (pipeline-relevant)

```
├── dodo.py                 # doit tasks (DAG definition)
├── PIPELINE.md              # this file
├── _data/                   # DATA_DIR (raw + processed)
│   ├── crsp.csv, ibes_summary.csv, finratio.csv
│   ├── real_GDP_FED.csv, IPT_FED.csv, real_personal_consumption_FED.csv, Unemployment_FED.csv
│   ├── ibes_crsp.csv
│   └── processed_data/
│       ├── macro_data.csv
│       └── A1.csv, A2.csv, Q1.csv, Q2.csv, Q3.csv
├── _output/                 # OUTPUT_DIR
│   ├── eda_forecast_summary.csv
│   ├── results/            # RESULTS_DIR
│   │   └── Q1_rf.csv, Q2_rf.csv, Q3_rf.csv, A1_rf.csv, A2_rf.csv
│   ├── images/             # IMAGES_DIR
│   │   ├── partial_dependence_meanest.png
│   │   └── {Q1,Q2,Q3,A1,A2}_RF_forecast_and_analyst_vs_actual.pdf
│   └── stat_analysis_regulation.txt
└── src/
    ├── settings.py          # config: paths, dates, RF params, etc.
    ├── functions.py         # shared: PrepareMacro, read_merge_prepare_data, train_test_rolling
    ├── load_data.py         # Step 1: WRDS + Philadelphia FED
    ├── data_engineering.py  # Step 2: IBES-CRSP link, macro, merge finratio
    ├── eda.py               # Step 3: EDA on processed data
    ├── train_rf.py          # Step 4: Rolling-window RF + OLS
    ├── stat_analysis.py     # Step 5: bias ~ alpha_i, beta_t, post_regulation
    ├── partial_dependence.py# PDP: meanest → realized EPS
    └── bias_analysis.py     # Plots: analyst vs RF vs actual
```

## DAG structure

```
settings (creates _data, _output)
    │
    ▼
pipeline_load_data     → _data/crsp.csv, ibes_summary.csv, finratio.csv, *FED*.csv
    │
    ▼
pipeline_data_engineering → _data/ibes_crsp.csv, _data/processed_data/macro_data.csv, A1..Q3.csv
    │
    ├──────────────────────────────────┬─────────────────────────────┐
    ▼                                  ▼                             ▼
pipeline_eda                    pipeline_train_rf            pipeline_partial_dependence
    │                                  │                             │
    │  _output/eda_forecast_summary    │  _output/results/*_rf.csv   │  _output/images/partial_dependence_meanest.png
    │                                  │                             │
    │                                  ├─────────────────┬───────────┘
    │                                  ▼                 ▼
    │                         pipeline_stat_analysis   pipeline_bias_analysis
    │                                  │                 │
    │                                  │  stat_analysis_regulation.txt
    │                                  │  _output/images/*_RF_forecast_and_analyst_vs_actual.pdf
```

## How to run

```bash
# Run full pipeline (doit will run settings and create dirs as needed)
doit
```

Run specific steps:

```bash
doit pipeline_load_data
doit pipeline_data_engineering
doit pipeline_eda pipeline_train_rf pipeline_stat_analysis pipeline_partial_dependence pipeline_bias_analysis
```

## Step overview

| Step | Doit task | Script | Inputs | Outputs |
|------|-----------|--------|--------|---------|
| 1 | `pipeline_load_data` | `src/load_data.py` | WRDS (CRSP, IBES, finratio) + Philadelphia FED URLs | `_data/crsp.csv`, `ibes_summary.csv`, `finratio.csv`, `*FED*.csv` |
| 2 | `pipeline_data_engineering` | `src/data_engineering.py` | Step 1 outputs + WRDS link table | `_data/ibes_crsp.csv`, `processed_data/macro_data.csv`, `A1..Q3.csv` |
| 3 | `pipeline_eda` | `src/eda.py` | `processed_data/*.csv` | `_output/eda_forecast_summary.csv` |
| 4 | `pipeline_train_rf` | `src/train_rf.py` | `processed_data/macro_data.csv`, `A1..Q3.csv` | `_output/results/{Q1,Q2,Q3,A1,A2}_rf.csv` |
| 5 | `pipeline_stat_analysis` | `src/stat_analysis.py` | `results/*_rf.csv` | `_output/stat_analysis_regulation.txt` |
| — | `pipeline_partial_dependence` | `src/partial_dependence.py` | `processed_data/macro_data.csv`, `Q1.csv` | `_output/images/partial_dependence_meanest.png` |
| — | `pipeline_bias_analysis` | `src/bias_analysis.py` | `results/*_rf.csv` | `_output/images/{period}_RF_forecast_and_analyst_vs_actual.pdf` |

Shared logic (RF, rolling window, data prep) lives in `src/functions.py`; it is used by `train_rf.py`, `partial_dependence.py`, and `data_engineering.py` (PrepareMacro).

## Configuration

- **Paths:** `DATA_DIR`, `OUTPUT_DIR`, `PROCESSED_DIR`, `RESULTS_DIR`, `IMAGES_DIR` — defined in `src/settings.py` (defaults: `_data`, `_output`, and subdirs).
- **Override:** `.env` or CLI, e.g. `--DATA_DIR=...` / `--OUTPUT_DIR=...`
- **Pipeline params:** `FORECAST_PERIODS`, `DATA_START_DATE`, rolling window lengths, RF hyperparameters, `POST_REGULATION_DATE`, figure DPI, etc. are also in `settings.py` and can be overridden the same way.

## Dependencies

- **WRDS:** set `WRDS_USERNAME` and `WRDS_PASSWORD` in `.env` for `load_data` and `data_engineering`.
- **Python:** pandas, numpy, wrds, scikit-learn, statsmodels, scipy, matplotlib, tqdm, python-dotenv, decouple.
