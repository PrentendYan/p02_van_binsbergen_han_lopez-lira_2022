# Unit tests (research replication)

Tests ensure **code runs correctly** and **each test has a clear purpose**; there are no redundant or meaningless tests.

## Scope

| File | Purpose |
|------|--------|
| `test_table2_term_structure.py` | Table 2 formulas: Newey-West t-stat (H0: mean=0), (RF-AE), (AF-AE), (RF-AE)², (AF-AE)², (AF-RF)/P, N; paper layout of value/t-stat rows. |
| `test_functions.py` | `PrepareMacro`: Fed-style column names → (Dates, Var) time series, datetime parsing. |
| `test_data_engineering.py` | `group_fpi`: FPI 6,7,8→'678'; 1,2→'12' for merge. |
| `test_eda.py` | EDA summary CSV has `period`, `n_obs`, `n_permno`, `cols` when pipeline has run. |
| `test_stat_analysis.py` | Bias regressions: gamma (post_regulation) and lambda (N_analyst) coefficients and p-values finite and in [0,1]. |
| `test_bias_analysis.py` | `run_bias_analysis` produces PDF when `results/*_rf.csv` exist. |
| `test_partial_dependence.py` | Figure 1 PDP: curve monotonic, non-linear, 95% CI band, plot elements (existing). |

## Running

From project root:

```bash
python -m pytest tests/ -v
```

Or via doit:

```bash
doit test
```

Some tests are **conditional**: they skip when required pipeline outputs are missing (e.g. processed_data, results), so run the pipeline first for full coverage.
