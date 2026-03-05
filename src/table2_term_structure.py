"""
Replicate Table 2: The term structure of earnings forecasts via machine learning.

Uses results/*_rf.csv: for each forecast horizon, computes time-series averages of
RF (ML forecast), AF (analyst forecast), AE (actual), their differences, squared
differences, (AF-RF)/P, and Newey-West t-statistics (3 lags for quarterly, 12 for annual).

Outputs: OUTPUT_DIR/table2_term_structure.csv (and optional LaTeX).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    from settings import config
    RESULTS_DIR = Path(config("RESULTS_DIR"))
    OUTPUT_DIR = Path(config("OUTPUT_DIR"))
    FORECAST_PERIODS = config("FORECAST_PERIODS")
except Exception:
    RESULTS_DIR = Path(__file__).resolve().parent.parent / "_output" / "results"
    OUTPUT_DIR = Path(__file__).resolve().parent.parent / "_output"
    FORECAST_PERIODS = ["Q1", "Q2", "Q3", "A1", "A2"]

import numpy as np
import pandas as pd
import statsmodels.api as sm

# Horizon labels for Table 2 (paper order)
HORIZON_LABELS = {
    "Q1": "One-quarter-ahead",
    "Q2": "Two-quarters-ahead",
    "Q3": "Three-quarters-ahead",
    "A1": "One-year-ahead",
    "A2": "Two-years-ahead",
}
# Newey-West lags: 3 for quarterly, 12 for annual (paper note)
NW_LAGS = {"Q1": 3, "Q2": 3, "Q3": 3, "A1": 12, "A2": 12}


def _newey_west_tstat(series: pd.Series, maxlags: int) -> float:
    """T-statistic for H0: mean = 0 using Newey-West SE."""
    y = series.dropna()
    if len(y) < 2 or y.std() == 0:
        return np.nan
    X = np.ones((len(y), 1))
    model = sm.OLS(y, X)
    res = model.fit(cov_type="HAC", cov_kwds={"maxlags": maxlags})
    return float(res.tvalues[0])


def compute_table2_row(period: str, df: pd.DataFrame) -> dict:
    """Compute one row of Table 2 for a given forecast horizon."""
    df = df.copy()
    df["_rf"] = df["predicted_adj_actual"]
    df["_af"] = df["meanest"]
    df["_ae"] = df["adj_actual"]
    df["_rf_ae"] = df["_rf"] - df["_ae"]
    df["_af_ae"] = df["_af"] - df["_ae"]
    df["_rf_ae_sq"] = df["_rf_ae"] ** 2
    df["_af_ae_sq"] = df["_af_ae"] ** 2

    # Time-series average: cross-sectional mean per Date, then average over dates
    by_date = df.groupby("Date").agg(
        RF=("_rf", "mean"),
        AF=("_af", "mean"),
        AE=("_ae", "mean"),
        RF_AE=("_rf_ae", "mean"),
        AF_AE=("_af_ae", "mean"),
        RF_AE_sq=("_rf_ae_sq", "mean"),
        AF_AE_sq=("_af_ae_sq", "mean"),
        AF_RF_P=("bias_AF_ML", "mean"),
    )

    row = {
        "Horizon": HORIZON_LABELS[period],
        "RF": by_date["RF"].mean(),
        "AF": by_date["AF"].mean(),
        "AE": by_date["AE"].mean(),
        "(RF-AE)": by_date["RF_AE"].mean(),
        "(AF-AE)": by_date["AF_AE"].mean(),
        "(RF-AE)^2": by_date["RF_AE_sq"].mean(),
        "(AF-AE)^2": by_date["AF_AE_sq"].mean(),
        "(AF-RF)/P": by_date["AF_RF_P"].mean(),
        "N": len(df),
    }

    # Newey-West t-stats on the time series (cross-sectional mean per date)
    maxlags = NW_LAGS[period]
    row["t(RF-AE)"] = _newey_west_tstat(by_date["RF_AE"], maxlags)
    row["t(AF-AE)"] = _newey_west_tstat(by_date["AF_AE"], maxlags)
    row["t((AF-RF)/P)"] = _newey_west_tstat(by_date["AF_RF_P"], maxlags)

    return row


def run_table2():
    """Load results, compute Table 2, save CSV in paper layout (value row + t-stat row per horizon)."""
    rows = []
    for period in FORECAST_PERIODS:
        path = RESULTS_DIR / f"{period}_rf.csv"
        if not path.exists():
            print(f"Missing {path}, skipping {period}")
            continue
        df = pd.read_csv(path)
        df["Date"] = df["Date"].astype(str)
        row = compute_table2_row(period, df)
        rows.append(row)

    # Build table in exact paper layout: each horizon = 2 rows (values, then t-stat)
    # Columns: Horizon, RF, AF, AE, (RF-AE), (AF-AE), (RF-AE)^2, (AF-AE)^2, (AF-RF)/P, N
    # t-stat row: only (RF-AE), (AF-AE), (AF-RF)/P filled; others blank
    COL_ORDER = ["RF", "AF", "AE", "(RF-AE)", "(AF-AE)", "(RF-AE)^2", "(AF-AE)^2", "(AF-RF)/P", "N"]
    out_rows = []
    for r in rows:
        horizon = r["Horizon"]
        # Value row
        out_rows.append({
            "Horizon": horizon,
            "RF": round(r["RF"], 3),
            "AF": round(r["AF"], 3),
            "AE": round(r["AE"], 3),
            "(RF-AE)": round(r["(RF-AE)"], 3),
            "(AF-AE)": round(r["(AF-AE)"], 3),
            "(RF-AE)^2": round(r["(RF-AE)^2"], 3),
            "(AF-AE)^2": round(r["(AF-AE)^2"], 3),
            "(AF-RF)/P": round(r["(AF-RF)/P"], 3),
            "N": int(r["N"]),
        })
        # t-stat row (only 3 columns filled)
        out_rows.append({
            "Horizon": "t-stat",
            "RF": "",
            "AF": "",
            "AE": "",
            "(RF-AE)": round(r["t(RF-AE)"], 2),
            "(AF-AE)": round(r["t(AF-AE)"], 2),
            "(RF-AE)^2": "",
            "(AF-AE)^2": "",
            "(AF-RF)/P": round(r["t((AF-RF)/P)"], 2),
            "N": "",
        })

    table_out = pd.DataFrame(out_rows)

    out_csv = OUTPUT_DIR / "table2_term_structure.csv"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    table_out.to_csv(out_csv, index=False)
    print("Table 2 (term structure) saved to", out_csv)

    # Also write a formatted text table matching the paper exactly (separator lines, alignment)
    out_txt = OUTPUT_DIR / "table2_term_structure.txt"
    _write_paper_format_table(out_rows, out_txt)
    print("Table 2 (paper format) saved to", out_txt)

    # Print to console
    print("\nTable 2: The term structure of earnings forecasts via machine learning\n")
    with open(out_txt) as f:
        print(f.read())

    return table_out


def _write_paper_format_table(rows: list, path: Path) -> None:
    """Write table in paper layout: header, then for each horizon value row + t-stat row, separators."""
    col_names = ["", "RF", "AF", "AE", "(RF-AE)", "(AF-AE)", "(RF-AE)^2", "(AF-AE)^2", "(AF-RF)/P", "N"]
    col_widths = [22, 7, 7, 7, 9, 9, 10, 10, 10, 12]

    def fmt_num(v, decimals=3):
        if v == "" or (isinstance(v, float) and np.isnan(v)):
            return ""
        if isinstance(v, (int, np.integer)):
            return f"{v:,}" if v >= 1000 else str(v)
        return f"{v:.{decimals}f}"

    lines = []
    header = "".join(n.ljust(w) for n, w in zip(col_names, col_widths))
    lines.append(header)
    lines.append("-" * len(header))

    for r in rows:
        is_tstat = r["Horizon"] == "t-stat"
        row_label = r["Horizon"]
        line = row_label.ljust(col_widths[0])
        if is_tstat:
            line += "".ljust(col_widths[1])
            line += "".ljust(col_widths[2])
            line += "".ljust(col_widths[3])
            line += fmt_num(r["(RF-AE)"], 2).rjust(col_widths[4])
            line += fmt_num(r["(AF-AE)"], 2).rjust(col_widths[5])
            line += "".ljust(col_widths[6])
            line += "".ljust(col_widths[7])
            line += fmt_num(r["(AF-RF)/P"], 2).rjust(col_widths[8])
            line += "".ljust(col_widths[9])
        else:
            line += fmt_num(r["RF"]).rjust(col_widths[1])
            line += fmt_num(r["AF"]).rjust(col_widths[2])
            line += fmt_num(r["AE"]).rjust(col_widths[3])
            line += fmt_num(r["(RF-AE)"]).rjust(col_widths[4])
            line += fmt_num(r["(AF-AE)"]).rjust(col_widths[5])
            line += fmt_num(r["(RF-AE)^2"]).rjust(col_widths[6])
            line += fmt_num(r["(AF-AE)^2"]).rjust(col_widths[7])
            line += fmt_num(r["(AF-RF)/P"]).rjust(col_widths[8])
            nstr = fmt_num(r["N"]) if r["N"] != "" else ""
            line += nstr.rjust(col_widths[9])
        lines.append(line)

    path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    run_table2()
