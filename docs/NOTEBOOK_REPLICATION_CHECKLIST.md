# Notebook vs src 复现核对清单

本文档对照 `notebooks/` 与 `src/` 的实现，确认 pipeline 是否按 notebook 逻辑严格复现。

---

## 1. notebooks/functions.py ↔ src/functions.py

| 项目 | Notebook | src | 一致 |
|------|----------|-----|------|
| PrepareMacro(Macro_Data, Begin_Year, Begin_Month, Name_col, Name_Var) | 同左 | 同左 | ✓ |
| read_merge_prepare_data: 日期 1985–2019 | ✓ | ✓ (config 可覆盖) | ✓ |
| columns_to_drop | 固定列表 | config("COLS_TO_DROP_PREP") 同列表 | ✓ |
| trim_value=10, list_vars_to_trim | ✓ | config("TRIM_VALUE"), config("VARS_TO_TRIM") | ✓ |
| train_test_rolling: 1985-01–2019-12 | ✓ | ✓ | ✓ |
| length_train=11, n_loops=408; A2: 23, 396 | ✓ | config 同值 | ✓ |
| RandomForestRegressor(2000, max_depth=7, max_samples=0.01, min_samples_leaf=5, n_jobs=-1) | ✓ | config 同值 | ✓ |
| result_df 日期: 非 A2 为 1986-01–2019-12, A2 为 1987-01–2019-12 | ✓ | ✓ | ✓ |
| bias_AF_ML = (meanest - predicted_adj_actual) / price | ✓ | ✓ | ✓ |

---

## 2. notebooks/data_engineering.ipynb ↔ src/data_engineering.py

| 项目 | Notebook | src | 一致 |
|------|----------|-----|------|
| WRDS 连接 | `wrds.Connection(yautoconnect=True)` | 若未设 WRDS_USERNAME 则 `yautoconnect=True`，否则用 config | ✓ |
| IBES-CRSP link + CRSP/IBES 本地读入 + merge 逻辑 | 同左 | 同左 | ✓ |
| PrepareMacro(GDP_Raw, 65, 11, 'ROUTPUT', 'GDP') 等 | ✓ | ✓ | ✓ |
| IPT skiprows=range(1,620), drop 1:121 | ✓ | ✓ | ✓ |
| Unempl skiprows=range(1,225) | ✓ | ✓ | ✓ |
| finratio 列删除、winsorize/ffill 逻辑 | ✓ | ✓ | ✓ |
| A1/A2/Q1–Q3 按 fpi 切分并保存 processed_data/*.csv | ✓ | ✓ | ✓ |

---

## 3. notebooks/EDA.ipynb ↔ src/eda.py

| 项目 | Notebook | src | 一致 |
|------|----------|-----|------|
| periods | ['A1','A2','Q1','Q2','Q3'] | config("FORECAST_PERIODS") 默认同序 | ✓ |
| 读 processed_data/{period}.csv | index_col=0 再 reset_index | read_csv 后若有 'Unnamed: 0' 则 drop | ✓ (均兼容无 index 的 CSV) |
| 汇总 n_obs, n_permno, cols | 有类似分析 | 写入 eda_forecast_summary.csv | ✓ |

---

## 4. notebooks/RF.ipynb ↔ src/train_rf.py + src/functions.py

| 项目 | Notebook | src | 一致 |
|------|----------|-----|------|
| 读 macro_data + 各 period 的 read_merge_prepare_data | ✓ | train_rf 调 functions.read_merge_prepare_data | ✓ |
| train_test_rolling 参数与 RF/OLS 设定 | 见 functions 对照 | 同上 | ✓ |
| 输出 results/{Q1,Q2,Q3,A1,A2}_rf.csv | ✓ | ✓ | ✓ |

---

## 5. notebooks/stat_analysis_results.ipynb ↔ src/stat_analysis.py

| 项目 | Notebook | src | 一致 |
|------|----------|-----|------|
| 读 results/{period}_rf.csv | ✓ | ✓ | ✓ |
| numest → N_analyst, post_regulation = (Date > '2000-10') | ✓ | config("POST_REGULATION_DATE") 默认 '2000-10' | ✓ |
| alpha_i, beta_t 来自 groupby permno/Date 的 bias_AF_ML mean | ✓ | ✓ | ✓ |
| 回归 1: OLS(bias_AF_ML, [alpha_i, beta_t, post_regulation]) | Cell 6 | ✓ | ✓ |
| 回归 2: + N_analyst | Cell 8 | ✓（已加入） | ✓ |

---

## 6. notebooks/partial_dependence_plot.ipynb ↔ src/partial_dependence.py

| 项目 | Notebook | src | 一致 |
|------|----------|-----|------|
| read_merge_prepare_data(period, Macro_Data) | ✓ | ✓ | ✓ |
| winsorize 1% | limits=[0.01, 0.01] | config("WINSORIZE_LIMITS")=(0.01,0.01) | ✓ |
| StandardScaler 后训练 RF | ✓ | ✓ | ✓ |
| RF 超参 2000, 7, 0.01, 5, n_jobs=-1 | ✓ | config | ✓ |
| partial_dependence(..., grid_resolution=100, percentiles=(0,1)) | ✓ | ✓ | ✓ |
| figsize=(10,6), 保存 meanest PDP 图 | ✓ | ✓ | ✓ |

---

## 7. Bias 图（notebook 中可能分散在 RF 或单独）↔ src/bias_analysis.py

| 项目 | Notebook | src | 一致 |
|------|----------|-----|------|
| 按 Date groupby，meanest / predicted_adj_actual / adj_actual | ✓ | ✓ | ✓ |
| trim_mean(0.01) 做时间序列 | ✓ | ✓ | ✓ |
| YearLocator(2) | ✓ | config("BIAS_PLOT_YEAR_LOCATOR")=2 | ✓ |
| 输出 PDF 到 images/{period}_RF_forecast_and_analyst_vs_actual.pdf | ✓ | ✓ | ✓ |

---

## 8. Table 2（论文 Table 2，notebook 中可能无对应 cell）↔ src/table2_term_structure.py

| 项目 | 论文 / 说明 | src | 一致 |
|------|-------------|-----|------|
| 数据来源 | results/*_rf.csv | ✓ | ✓ |
| RF, AF, AE 时间序列平均（先按 Date 截面平均再对时间平均） | 表注 | ✓ | ✓ |
| (RF-AE), (AF-AE), 平方项, (AF-RF)/P | 表结构 | ✓ | ✓ |
| Newey-West t 统计量：季度 3 lags，年度 12 lags | 表注 | ✓ | ✓ |

---

## 已修正的差异

1. **data_engineering WRDS 连接**  
   - 原：仅使用 `config("WRDS_USERNAME")`。  
   - 现：与 notebook 一致；未设置 WRDS_USERNAME 时使用 `wrds.Connection(yautoconnect=True)`，否则使用 config 中的用户名。

2. **stat_analysis 第二种回归**  
   - 原：只做 post_regulation（对应 Cell 6）。  
   - 现：增加带 N_analyst 的回归（对应 Cell 8），并写入同一输出文件。

---

## 小结

- **functions / data_engineering / eda / train_rf / partial_dependence / bias_analysis**：逻辑与对应 notebook 一致，仅路径与部分常数改为从 `settings` 读取。  
- **stat_analysis**：已包含 notebook 中两种回归（仅 post_regulation；+ N_analyst）。  
- **table2_term_structure**：按论文 Table 2 与表注实现，使用与 notebook 一致的 results 数据。  

若之后修改 notebook，请同步更新本清单和相应 `src` 脚本。
