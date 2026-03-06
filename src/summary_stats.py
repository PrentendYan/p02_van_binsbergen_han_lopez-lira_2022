"""
Generate summary statistics tables and figures for the replication report.
Reads from OUTPUT_DIR/results/*_rf.csv (post-rolling-window results, so they
include RF predictions alongside the raw panel variables).

Outputs (saved to OUTPUT_DIR):
  summary_stats_table.tex          -- LaTeX table: per-horizon descriptive stats
  summary_stats_coverage.tex       -- LaTeX table: sample coverage by horizon
  fig_bias_distribution.png        -- KDE of (AF-RF)/P by horizon
  fig_sample_coverage.png          -- Unique firms per year for each horizon
  fig_rmse_comparison.png          -- RF vs AF RMSE / MAE bar chart
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from settings import config

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde

RESULTS_DIR = Path(config("RESULTS_DIR"))
OUTPUT_DIR  = Path(config("OUTPUT_DIR"))
IMAGES_DIR  = Path(config("IMAGES_DIR"))
IMAGES_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PERIODS = config("FORECAST_PERIODS")
HORIZON_LABELS = {
    "Q1": "Q1 (1-qtr)",
    "Q2": "Q2 (2-qtr)",
    "Q3": "Q3 (3-qtr)",
    "A1": "A1 (1-yr)",
    "A2": "A2 (2-yr)",
}

# ── Load all result files ──────────────────────────────────────────────────────
frames = {}
for p in PERIODS:
    path = RESULTS_DIR / f"{p}_rf.csv"
    if path.exists():
        df = pd.read_csv(path)
        df["horizon"] = p
        frames[p] = df

if not frames:
    print("No result files found – run pipeline first.")
    sys.exit(1)

# ── 1. Descriptive-statistics LaTeX table ─────────────────────────────────────
KEY_VARS = {
    "meanest":              "Analyst Forecast (AF)",
    "adj_actual":           "Realized EPS (AE)",
    "predicted_adj_actual": "RF Forecast (RF)",
    "bias_AF_ML":           "$(AF-RF)/P$ (Bias)",
    "numest":               "Number of Analysts",
    "price":                "Stock Price (\\$)",
}

rows = []
for p in PERIODS:
    if p not in frames:
        continue
    df = frames[p]
    for col, label in KEY_VARS.items():
        if col not in df.columns:
            continue
        s = df[col].dropna()
        rows.append({
            "Horizon":  HORIZON_LABELS[p],
            "Variable": label,
            "N":        f"{len(s):,}",
            "Mean":     f"{s.mean():.3f}",
            "Std":      f"{s.std():.3f}",
            "p5":       f"{s.quantile(0.05):.3f}",
            "p25":      f"{s.quantile(0.25):.3f}",
            "Median":   f"{s.median():.3f}",
            "p75":      f"{s.quantile(0.75):.3f}",
            "p95":      f"{s.quantile(0.95):.3f}",
        })

tbl = pd.DataFrame(rows)

# Write LaTeX
col_fmt = "ll" + "r" * (len(tbl.columns) - 2)
header  = (
    "\\begin{tabular}{" + col_fmt + "}\n"
    "\\toprule\n"
    "Horizon & Variable & N & Mean & Std & p5 & p25 & Median & p75 & p95 \\\\\n"
    "\\midrule\n"
)
body_lines = []
prev_horizon = None
for _, row in tbl.iterrows():
    if row["Horizon"] != prev_horizon:
        if prev_horizon is not None:
            body_lines.append("\\addlinespace\n")
        prev_horizon = row["Horizon"]
    vals = " & ".join(str(row[c]) for c in tbl.columns)
    body_lines.append(vals + " \\\\\n")

footer = "\\bottomrule\n\\end{tabular}\n"
latex_body = header + "".join(body_lines) + footer

out_tbl = OUTPUT_DIR / "summary_stats_table.tex"
out_tbl.write_text(latex_body, encoding="utf-8")
print("Saved", out_tbl)


# ── 2. Sample-coverage LaTeX table ────────────────────────────────────────────
cov_rows = []
for p in PERIODS:
    if p not in frames:
        continue
    df = frames[p]
    df2 = df.copy()
    df2["year"] = df2["Date"].astype(str).str[:4]
    cov_rows.append({
        "Horizon":       HORIZON_LABELS[p],
        "Obs":           f"{len(df2):,}",
        "Unique Firms":  f"{df2['permno'].nunique():,}",
        "Years":         f"{df2['year'].min()}--{df2['year'].max()}",
        "Avg Firms/Mo":  f"{df2.groupby('Date')['permno'].nunique().mean():.0f}",
        "Avg Analysts":  f"{df2['numest'].mean():.1f}",
    })

cov_tbl = pd.DataFrame(cov_rows)
cov_latex = (
    "\\begin{tabular}{lrrrr" + "r" * (len(cov_tbl.columns) - 5) + "}\n"
    "\\toprule\n"
    "Horizon & Observations & Unique Firms & Sample Period & Avg.~Firms/Mo & Avg.~Analysts \\\\\n"
    "\\midrule\n"
)
for _, row in cov_tbl.iterrows():
    cov_latex += " & ".join(str(row[c]) for c in cov_tbl.columns) + " \\\\\n"
cov_latex += "\\bottomrule\n\\end{tabular}\n"

out_cov = OUTPUT_DIR / "summary_stats_coverage.tex"
out_cov.write_text(cov_latex, encoding="utf-8")
print("Saved", out_cov)


# ── 3. Figure: KDE of (AF-RF)/P by horizon ────────────────────────────────────
fig, ax = plt.subplots(figsize=(9, 5))
colors = ["royalblue", "darkorange", "forestgreen", "firebrick", "purple"]
for (p, color) in zip(PERIODS, colors):
    if p not in frames:
        continue
    s = frames[p]["bias_AF_ML"].dropna()
    # winsorise for plotting only
    lo, hi = s.quantile(0.005), s.quantile(0.995)
    s = s.clip(lo, hi)
    kde = gaussian_kde(s, bw_method=0.3)
    xs  = np.linspace(lo, hi, 400)
    ax.plot(xs, kde(xs), label=HORIZON_LABELS[p], color=color, linewidth=1.8)

ax.axvline(0, color="black", linewidth=0.8, linestyle="--", label="Zero bias")
ax.set_xlabel("$(AF - RF) / P$", fontsize=12)
ax.set_ylabel("Density", fontsize=12)
ax.set_title("Distribution of Analyst--Machine Bias $(AF-RF)/P$ by Forecast Horizon", fontsize=12)
ax.legend(fontsize=9)
ax.grid(True, linestyle="--", alpha=0.4)
ax.set_facecolor("#f9f9f9")
plt.tight_layout()
out_kde = IMAGES_DIR / "fig_bias_distribution.png"
fig.savefig(out_kde, dpi=120)
plt.close(fig)
print("Saved", out_kde)


# ── 4. Figure: unique firms per year ──────────────────────────────────────────
fig, ax = plt.subplots(figsize=(10, 5))
colors = ["royalblue", "darkorange", "forestgreen", "firebrick", "purple"]
for (p, color) in zip(PERIODS, colors):
    if p not in frames:
        continue
    df2 = frames[p].copy()
    df2["year"] = df2["Date"].astype(str).str[:4].astype(int)
    by_year = df2.groupby("year")["permno"].nunique()
    ax.plot(by_year.index, by_year.values, label=HORIZON_LABELS[p],
            color=color, linewidth=1.8, marker="o", markersize=3)

ax.set_xlabel("Year", fontsize=12)
ax.set_ylabel("Number of Unique Firms", fontsize=12)
ax.set_title("Sample Coverage: Unique Firms per Year by Forecast Horizon", fontsize=12)
ax.legend(fontsize=9)
ax.grid(True, linestyle="--", alpha=0.4)
ax.set_facecolor("#f9f9f9")
plt.tight_layout()
out_cov_fig = IMAGES_DIR / "fig_sample_coverage.png"
fig.savefig(out_cov_fig, dpi=120)
plt.close(fig)
print("Saved", out_cov_fig)


# ── 5. Figure: RF vs AF RMSE and MAE per horizon ──────────────────────────────
metrics = {"RMSE": {}, "MAE": {}}
for p in PERIODS:
    if p not in frames:
        continue
    df = frames[p]
    rf_err = df["predicted_adj_actual"] - df["adj_actual"]
    af_err = df["meanest"]              - df["adj_actual"]
    metrics["RMSE"][p] = {"RF": np.sqrt((rf_err**2).mean()),
                          "AF": np.sqrt((af_err**2).mean())}
    metrics["MAE"][p]  = {"RF": rf_err.abs().mean(),
                          "AF": af_err.abs().mean()}

x     = np.arange(len(PERIODS))
width = 0.3
fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=False)

for ax, metric in zip(axes, ["RMSE", "MAE"]):
    rf_vals = [metrics[metric][p]["RF"] for p in PERIODS if p in metrics[metric]]
    af_vals = [metrics[metric][p]["AF"] for p in PERIODS if p in metrics[metric]]
    labels  = [HORIZON_LABELS[p]        for p in PERIODS if p in metrics[metric]]
    xi = np.arange(len(labels))
    b1 = ax.bar(xi - width/2, rf_vals, width, label="RF",     color="royalblue", alpha=0.85)
    b2 = ax.bar(xi + width/2, af_vals, width, label="Analyst", color="darkorange", alpha=0.85)
    ax.set_xticks(xi)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylabel(metric, fontsize=11)
    ax.set_title(f"{metric} by Forecast Horizon", fontsize=11)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.set_facecolor("#f9f9f9")

plt.suptitle("Forecast Accuracy: Random Forest vs.\ Analyst Consensus", fontsize=12, y=1.01)
plt.tight_layout()
out_rmse = IMAGES_DIR / "fig_rmse_comparison.png"
fig.savefig(out_rmse, dpi=120, bbox_inches="tight")
plt.close(fig)
print("Saved", out_rmse)

print("summary_stats.py done.")
