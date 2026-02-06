# src/generate_chart.py
"""Exploratory charts for each data source. Saves interactive HTML to _output/."""
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

def chart_crsp():
    df = pd.read_parquet(DATA_DIR / "crsp_m.parquet")
    # e.g. average monthly return by date
    s = df.groupby("date")["ret"].mean()
    fig = px.line(x=s.index, y=s.values, title="CRSP: Avg monthly return")
    fig.write_html(OUTPUT_DIR / "explore_crsp.html")

def chart_compustat():
    df = pd.read_parquet(DATA_DIR / "compa.parquet")
    # e.g. total assets over time (aggregate or median)
    s = df.groupby("datadate")["at"].median()
    fig = px.line(x=s.index, y=s.values, title="Compustat: Median total assets")
    fig.write_html(OUTPUT_DIR / "explore_compustat.html")

def chart_ibes():
    df = pd.read_parquet(DATA_DIR / "Forecast_EPS_summary_unadjusted_1986_2019.parquet")
    # e.g. mean EPS forecast by statpers
    s = df.groupby("statpers")["highest"].mean()
    fig = px.line(x=s.index, y=s.values, title="IBES: Mean EPS forecast")
    fig.write_html(OUTPUT_DIR / "explore_ibes.html")

def chart_ff():
    df = pd.read_parquet(DATA_DIR / "FF_FACTORS.parquet")
    # e.g. Mkt-RF time series (column name may be mktrf or Mkt-RF)
    col = [c for c in df.columns if "mktrf" in c.lower() or "mkt" in c.lower()]
    col = col[0] if col else df.columns[1]
    fig = px.line(df, x=df.index if df.index.name else df.iloc[:, 0], y=col, title="Fama-French: Mkt-RF")
    fig.write_html(OUTPUT_DIR / "explore_ff.html")
    
def chart_fred():
    df = pd.read_parquet(DATA_DIR / "fred.parquet")
    plot_df = df[["consumption_growth"]].dropna().reset_index()
    x_col = plot_df.columns[0]
    fig = px.line(plot_df, x=x_col, y="consumption_growth", title="FRED: Consumption growth")
    fig.write_html(OUTPUT_DIR / "explore_fred.html")

if __name__ == "__main__":
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    chart_crsp()
    chart_compustat()
    chart_ibes()
    chart_ff()
    chart_fred()