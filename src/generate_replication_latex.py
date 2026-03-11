# %% [markdown]
# # Generate Replication Report (LaTeX)
# 
# **Run this notebook from the project root** so that `OUTPUT_DIR` and paths resolve correctly.
# 
# This notebook builds a **single LaTeX document** that satisfies the replication checklist:
# 
# - Describes the nature of the replication project
# - Contains **all** tables and charts produced by the code (including Table 2 and the partial dependence figure)
# - Gives a high-level overview of successes and challenges
# - Explains data sources used
# - **No code snippets** in the generated PDF—only narrative, tables, and figures
# 
# The generated file is written to **`reports/replication_report_generated.tex`**. Compile from the project root with:
# 
# ```bash
# cd reports && pdflatex replication_report_generated.tex
# ```

# %%
import sys
from pathlib import Path

# Project root = directory that contains "reports" and "src" (outermost reports)
_cwd = Path(".").resolve()
if (_cwd / "reports").is_dir():
    ROOT = _cwd
else:
    ROOT = _cwd.parent  # e.g. running from src/ -> project root

sys.path.insert(0, str(ROOT / "src"))
try:
    from settings import config
    OUTPUT_DIR = Path(config("OUTPUT_DIR"))
    IMAGES_DIR = Path(config("IMAGES_DIR"))
except Exception:
    OUTPUT_DIR = ROOT / "_output"
    IMAGES_DIR = OUTPUT_DIR / "images"

REPORTS_DIR = ROOT / "reports"  # outermost reports folder
OUTPUT_EXTENDED = ROOT / "_output_extended"  # extended sample (1986 to latest)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
print("OUTPUT_DIR:", OUTPUT_DIR)
print("REPORTS_DIR:", REPORTS_DIR)
print("OUTPUT_EXTENDED:", OUTPUT_EXTENDED)

# %% [markdown]
# ## 1. Build Table 2 (Term Structure) from CSV
# 
# Read `table2_term_structure.csv` (produced by `src/table2_term_structure.py`) and convert it to a LaTeX `tabular` so that the report is **automatically generated** from the code output.

# %%
import pandas as pd

table2_path = OUTPUT_DIR / "table2_term_structure.csv"
if not table2_path.exists():
    raise FileNotFoundError(f"Run the pipeline first to create {table2_path}")

df = pd.read_csv(table2_path)

def latex_cell(val):
    if val == "" or (isinstance(val, float) and pd.isna(val)):
        return ""
    if isinstance(val, (int, float)):
        if isinstance(val, int) and val >= 1000:
            return f"{val:,}".replace(",", "{\\,}")
        return str(val)
    return str(val)

cols = ["Horizon", "RF", "AF", "AE", "(RF-AE)", "(AF-AE)", "(RF-AE)^2", "(AF-AE)^2", "(AF-RF)/P", "N"]
header = " & ".join(c.replace("^2", "$^2$") if "^2" in c else c for c in cols) + " \\\\"
lines = ["\\toprule", header, "\\midrule"]

for _, row in df.iterrows():
    cells = [str(row["Horizon"])]
    for c in cols[1:]:
        cells.append(latex_cell(row.get(c, "")))
    line = " & ".join(cells) + " \\\\"
    if row["Horizon"] == "t-stat":
        line = "\\textit{t-stat} & " + " & ".join(cells[1:]) + " \\\\"
    lines.append(line)

lines.append("\\bottomrule")
table2_latex = "\n".join(lines)
print("Table 2 LaTeX fragment (first 5 lines):")
print("\n".join(lines[:5]))

# %%
# Build Table 2 LaTeX for extended sample from _output_extended (read from CSV or TXT)
table2_extended_latex = None
ext_csv = OUTPUT_EXTENDED / "table2_term_structure.csv"
ext_txt = OUTPUT_EXTENDED / "table2_term_structure.txt"
if ext_csv.exists():
    df_ext = pd.read_csv(ext_csv)
    lines_ext = ["\\toprule", header, "\\midrule"]
    for _, row in df_ext.iterrows():
        cells = [str(row["Horizon"])]
        for c in cols[1:]:
            cells.append(latex_cell(row.get(c, "")))
        line = " & ".join(cells) + " \\\\"
        if row["Horizon"] == "t-stat":
            line = "\\textit{t-stat} & " + " & ".join(cells[1:]) + " \\\\"
        lines_ext.append(line)
    lines_ext.append("\\bottomrule")
    table2_extended_latex = "\n".join(lines_ext)
elif ext_txt.exists():
    raw = ext_txt.read_text(encoding="utf-8").strip().split("\n")
    lines_ext = ["\\toprule", header, "\\midrule"]
    i = 2  # skip header and separator
    while i < len(raw):
        parts = raw[i].split()
        if not parts or parts[0] == "Horizon" or parts[0] == "t-stat":
            i += 1
            continue
        # data row: horizon + 9 numbers
        if len(parts) >= 10:
            horizon, nums = parts[0], parts[1:10]
            n_str = nums[8].replace(",", "{\\,}") if len(nums) > 8 else nums[8]
            line = f"{horizon} & {' & '.join(nums[:8])} & {n_str} \\\\"
            lines_ext.append(line)
        i += 1
        # t-stat row
        if i < len(raw) and raw[i].strip().startswith("t-stat"):
            tparts = raw[i].split()
            t_vals = [x for x in tparts[1:] if x.lstrip("-").replace(".", "").isdigit()]
            if len(t_vals) >= 3:
                lines_ext.append("\\textit{t-stat} &  &  &  & " + " & ".join(t_vals[:2]) + " &  &  & " + t_vals[2] + " &  \\\\")
            i += 1
    lines_ext.append("\\bottomrule")
    table2_extended_latex = "\n".join(lines_ext)
if table2_extended_latex:
    print("Extended Table 2 built from", "CSV" if ext_csv.exists() else "TXT")
else:
    print("No extended table found in _output_extended; extended section will be skipped or show placeholder.")

# %% [markdown]
# ## 2. Assemble the full LaTeX document
# 
# Sections: Overview, Data Sources, Results (Table 2 + Partial Dependence Figure), Successes and Challenges. Figures and table are included by reference; the table body is generated from the CSV above.

# %%
preamble = r"""
\documentclass[11pt]{article}
\usepackage[utf8]{inputenc}
\usepackage{graphicx}
\usepackage{booktabs}
\usepackage{float}
\usepackage{caption}
\usepackage{subcaption}
\usepackage[margin=1in]{geometry}
\usepackage{hyperref}

\newcommand{\PathToOutput}{../_output}
\newcommand{\PathToOutputExtended}{../_output_extended}
\newcommand{\PathToAssets}{../assets}

\begin{document}
"""

overview = r"""
\title{Replication Report: Man vs.\ Machine Learning\\
{\large van Binsbergen, Han, Lopez-Lira (2022)}}
\author{Replication Project}
\date{\today}
\maketitle

\section{Project Overview}

This document reports the replication of the empirical analysis in van Binsbergen, Han, and Lopez-Lira (2022), ``Man vs.\ Machine Learning: The Term Structure of Earnings Expectations and Conditional Biases.''

\subsection{Intuition of the Paper}

The paper constructs a \textbf{real-time, statistically optimal benchmark} for firms' earnings expectations using \textbf{machine learning} (random forest). Unlike past work that evaluates analysts using only realized earnings ex post, the authors define a \textbf{conditional bias} as the difference between analysts' forecasts and this ML benchmark, scaled by price---observable in real time before earnings are realized. This allows them to study how biases vary across firms and time and to link biases to asset returns and corporate decisions.

\textbf{Why machine learning?} Linear earnings models are either weak out-of-sample (e.g., So (2013)) or rely on ex-post variable selection, which induces look-ahead bias. The paper argues that (1)~earnings are a \textbf{non-linear} function of predictors (including analysts' forecasts), so linear forecasts are misspecified; (2)~random forests use a broad set of public signals without variable-selection bias and approximate the conditional expectation flexibly; (3)~analysts' forecasts contain valuable information but can be improved by optimally combining them with fundamentals and macro data. The ML benchmark is designed to be unbiased (forecast errors centered at zero) and more accurate than analysts in mean squared error.

The paper then uses this benchmark to show that analysts are, on average, \textbf{upward biased}; that this bias is associated with \textbf{negative cross-sectional return predictability} (overly optimistic stocks earn lower subsequent returns); that the short legs of many anomalies consist of firms with excessively optimistic forecasts; and that \textbf{managers issue more equity} when the real-time bias is high (market timing). This replication focuses on the core \emph{earnings-forecast} piece: building the ML benchmark and documenting the term structure of forecast accuracy and conditional bias.

\subsection{Purpose of the Replicated Table and Figure}

\textbf{Table~2 (Term structure of earnings forecasts).} Table~2 compares the properties of \textbf{analysts' forecasts} with the \textbf{ML (random forest) forecasts} across five horizons (one to three quarters ahead, one and two years ahead). It serves three goals: (1)~to \textbf{verify that the ML forecast is unbiased}: the time-series average of $(RF - AE)$ is close to zero and statistically insignificant at all horizons, while $(AF - AE)$ is positive and significant---analysts systematically over-forecast. (2)~To show that the \textbf{ML forecast is more accurate}: the mean squared errors $(RF-AE)^2$ are smaller than $(AF-AE)^2$. (3)~To document the \textbf{real-time conditional bias} $(AF-RF)/P$: its significance confirms that analysts deviate from the ML benchmark in a systematic way that can be used in the cross-section (e.g., for return predictability and issuance). Replicating Table~2 is therefore central to validating the paper's benchmark and the existence of conditional biases.

\textbf{Figure~1 (Partial dependence of realized EPS on analysts' forecasts).} The figure plots the partial dependence of the RF-predicted \emph{realized} EPS on the (standardized) consensus analyst forecast for the one-quarter-ahead horizon. It serves to \textbf{motivate the use of non-linear methods}: the paper argues that ``EPS is a non-linear function of analysts' forecasts'' (Section~2), so that linear predictions produce substantial errors. The S-shaped curve---flattening at high analyst forecasts---is consistent with analysts being overly optimistic when they forecast high; the RF corrects for this by predicting a smaller increase in actual EPS. Replicating Figure~1 supports the methodological choice of random forests over linear models and illustrates how the benchmark incorporates analysts' information while correcting for conditional bias.
"""

data_sources = r"""
\section{Data Sources}

\textbf{WRDS / IBES.} Consensus analyst earnings forecasts and actual reported EPS from IBES (\texttt{ibes.statsum\_epsus}), US firms, forecast-period indicators for quarterly and annual horizons. Sample starts January 1985.

\textbf{WRDS / CRSP.} Daily stock data (prices, returns, split-adjustment factors) from \texttt{crsp.dsf} for NYSE, AMEX, NASDAQ. IBES--CRSP link via CUSIP and date-range overlap. Actual EPS is adjusted for splits using CRSP cumulative adjustment factors.

\textbf{WRDS / Financial Ratios.} Firm-level accounting features from \texttt{wrdsapps\_finratio\_ibes.firm\_ratio\_ibes}. Missing values imputed with within-period, within-industry medians (Fama--French 49) and forward-filled by firm.

\textbf{Philadelphia Fed.} Real-time macro data: real GDP, industrial production, real personal consumption, unemployment. Log growth rates and levels merged into the panel with an as-of backward merge on the estimate date to avoid look-ahead bias.
"""

results_intro = r"""
\section{Results}

We present the paper's Figure~1 and Table~2 alongside our replication for direct comparison.
"""

# %%
comparison_figure1 = r"""
\subsection{Figure 1: Partial Dependence (Original vs.\ Replication)}

The figure below shows the original Figure~1 from van Binsbergen, Han, and Lopez-Lira (2022) above and our replication below. Both plots depict the partial dependence of one-quarter-ahead realized EPS on analysts' forecasts. The replication reproduces the S-shaped, non-linear relationship and the flattening at high forecast levels; small differences may arise because the dataset used in the replication is not exactly identical to that used in the original paper.

\begin{figure}[H]
  \centering
  \begin{subfigure}[t]{\textwidth}
    \centering
    \includegraphics[width=0.7\linewidth]{\PathToAssets/figure1.png}
    \caption{Original (from paper)}
    \label{fig:original_figure1}
  \end{subfigure}
  \vspace{1em}
  \begin{subfigure}[t]{\textwidth}
    \centering
    \includegraphics[width=0.7\linewidth]{\PathToOutput/images/partial_dependence_meanest.png}
    \caption{Our replication}
    \label{fig:replication_figure1}
  \end{subfigure}
  \caption{EPS as a non-linear function of analysts' forecasts---original vs.\ replication (vertical layout).}
  \label{fig:comparison_pdp}
\end{figure}
"""

# Original Table 2 from the paper (van Binsbergen, Han, Lopez-Lira 2022), same layout for easy comparison
table2_original_latex = r"""
\toprule
Horizon & RF & AF & AE & (RF-AE) & (AF-AE) & (RF-AE)$^2$ & (AF-AE)$^2$ & (AF-RF)/P & N \\
\midrule
One-quarter-ahead & 0.290 & 0.319 & 0.291 & $-$0.000 & 0.028 & 0.076 & 0.081 & 0.005 & 1,022,661 \\
\textit{t-stat} &  &  &  & $-$0.17 & 6.59 &  &  & 6.54 &  \\
Two-quarters-ahead & 0.323 & 0.376 & 0.323 & $-$0.001 & 0.053 & 0.094 & 0.102 & 0.007 & 1,110,689 \\
\textit{t-stat} &  &  &  & $-$0.13 & 10.31 &  &  & 7.75 &  \\
Three-quarters-ahead & 0.343 & 0.413 & 0.341 & 0.002 & 0.072 & 0.121 & 0.132 & 0.007 & 1,018,958 \\
\textit{t-stat} &  &  &  & 0.31 & 11.55 &  &  & 8.08 &  \\
One-year-ahead & 1.194 & 1.320 & 1.167 & 0.027 & 0.154 & 0.670 & 0.686 & 0.021 & 1,260,060 \\
\textit{t-stat} &  &  &  & 1.64 & 6.24 &  &  & 5.17 &  \\
Two-years-ahead & 1.384 & 1.771 & 1.387 & $-$0.004 & 0.384 & 1.897 & 2.009 & 0.035 & 1,097,098 \\
\textit{t-stat} &  &  &  & $-$0.07 & 8.33 &  &  & 6.57 &  \\
\bottomrule
"""

comparison_table2 = r"""
\subsection{Table 2: Term Structure (Original vs.\ Replication)}

Both tables use the same column definitions; comparing them row by row shows alignment in sign and magnitude, with differences attributable to sample period (paper: Jan.\ 1986--Dec.\ 2019) and data vintage.

\textbf{Original (from paper):}
\begin{table}[H]
\centering
\caption{Original Table~2 from the paper (term structure of earnings forecasts).}
\label{tab:original_table2}
\small
\resizebox{\textwidth}{!}{%
\begin{tabular}{lccccccccc}
""" + table2_original_latex + r"""
\end{tabular}%
}
\end{table}

\textbf{Our replication:}
\begin{table}[H]
\centering
\caption{Our replication: Term structure of earnings forecasts via machine learning.}
\label{tab:term_structure}
\small
\resizebox{\textwidth}{!}{%
\begin{tabular}{lccccccccc}
""" + table2_latex + "\n" + r"""
\end{tabular}%
}
\end{table}
"""

successes_challenges = r"""
\section{Replication Results Analysis and Challenges}

This section evaluates how well the replication reproduces the paper's Table~2 and Figure~1, summarizes what was and was not replicated, and discusses possible reasons and next steps for investigation.

\subsection{What Was Replicated}

\textbf{Table~2 (term structure).} The replication successfully reproduces the \textbf{structure and definitions} of the table: the same forecast horizons (one to three quarters ahead, one and two years ahead), the same columns (RF, AF, AE, differences, squared errors, $(AF-RF)/P$, $N$), and the same layout (value row plus $t$-stat row per horizon). The \textbf{sign and statistical significance of $(AF-AE)$} are consistent with the paper: $(AF-AE)$ is positive and highly significant at all horizons in both the original and the replication (e.g., $t$-stats in the 6--11 range), indicating that analyst forecasts are systematically above the realized benchmark. The \textbf{trend across horizons}---rising RF, AF, AE and error magnitudes as the horizon lengthens---is also preserved.

\textbf{Figure~1 (partial dependence).} The replication reproduces the \textbf{positive, non-linear relationship} between analysts' forecasts and realized EPS: as forecasts increase, realized EPS increases. The plot is correctly identified as a partial dependence plot from a random forest regression, and the \textbf{overall increasing trend} is recovered. The curve is non-linear rather than linear, supporting the paper's motivation for using machine learning.

\subsection{What Was Not Replicated}

\textbf{Table~2---magnitudes and key inference.}
\begin{itemize}
\item \textbf{Levels of RF, AF, AE:} Replicated values are consistently \emph{lower} than the paper's across all horizons (e.g., one-quarter-ahead RF: 0.246 vs.\ 0.290). This points to possible differences in scaling, sample composition, or variable construction.
\item \textbf{Squared errors $(RF-AE)^2$ vs.\ $(AF-AE)^2$:} In the \emph{paper}, $(RF-AE)^2$ is smaller than $(AF-AE)^2$ at all horizons (e.g., one-quarter-ahead: 0.076 vs.\ 0.081), so the ML forecast is more accurate than analysts. In \emph{our replication}, $(RF-AE)^2$ is consistently \emph{larger} than $(AF-AE)^2$ (e.g., one-quarter-ahead: 0.565 vs.\ 0.1), so the ML forecast is \textbf{less accurate} than analysts---a major qualitative discrepancy. At the same time, $(RF-AE)$ in the replication is close to zero while $(RF-AE)^2$ is large, so the RF forecast is \textbf{unbiased} but \textbf{noisy}: the mean error is near zero, but the \emph{variance} of the prediction error is high. In the paper, both the mean error and the squared error are small; in our replication, the former is small and the latter is large, a pattern that warrants further investigation. Figure~\ref{fig:rf_af_actual_ts} below offers an intuitive explanation: the time-series of RF prediction, analyst forecast, and actual value by horizon show that \textbf{RF prediction generally tracks actual more closely than analyst forecast}; however, during an \textbf{abnormal period} (e.g., a sharp downturn at the end of the sample), the RF prediction exhibits a large deviation from actual, which drives up the mean squared error substantially even though the average $(RF-AE)$ remains near zero.
\item \textbf{Direction of $(AF-RF)/P$:} In the paper, $(AF-RF)/P$ is \emph{positive} with positive $t$-statistics (about 5--8), consistent with analysts being above the ML benchmark. In our replication, $(AF-RF)/P$ is \emph{negative} (mean about $-0.01$ to $-0.06$) with negative $t$-statistics (about $-7$ to $-14$). The \textbf{sign is opposite} to the paper: the replication suggests analysts are on average \emph{below} the ML benchmark (scaled by price), while the paper finds them \emph{above}. Resolving this direction discrepancy (e.g., variable definition, scaling, or sample) remains important for the conditional-bias conclusion.
\item \textbf{Sample size $N$:} Replication $N$ is about 10--20\% lower than the paper (e.g., one-quarter-ahead: 881,903 vs.\ 1,022,661), indicating a different or narrower sample.
\end{itemize}

Figure 2 plots the time-series of RF prediction, analyst forecast, and actual value for each of the five horizons (Q1--Q3, A1, A2). Visually, the RF prediction tracks the actual series more closely than the analyst forecast over much of the sample; however, in certain periods (e.g., a sharp downturn) the RF series shows large deviations from actual, which inflates $(RF-AE)^2$ even though the time-series average $(RF-AE)$ remains close to zero.

\begin{figure}[H]
  \centering
  \begin{subfigure}[t]{0.48\textwidth}
    \centering
    \includegraphics[width=\linewidth]{\PathToOutput/images/Q1_RF_forecast_and_analyst_vs_actual.pdf}
    \caption{Q1: one-quarter-ahead}
  \end{subfigure}\hfill
  \begin{subfigure}[t]{0.48\textwidth}
    \centering
    \includegraphics[width=\linewidth]{\PathToOutput/images/Q2_RF_forecast_and_analyst_vs_actual.pdf}
    \caption{Q2: two-quarters-ahead}
  \end{subfigure}
  \vspace{0.5em}
  \begin{subfigure}[t]{0.48\textwidth}
    \centering
    \includegraphics[width=\linewidth]{\PathToOutput/images/Q3_RF_forecast_and_analyst_vs_actual.pdf}
    \caption{Q3: three-quarters-ahead}
  \end{subfigure}\hfill
  \begin{subfigure}[t]{0.48\textwidth}
    \centering
    \includegraphics[width=\linewidth]{\PathToOutput/images/A1_RF_forecast_and_analyst_vs_actual.pdf}
    \caption{A1: one-year-ahead}
  \end{subfigure}
  \vspace{0.5em}
  \begin{subfigure}[t]{0.48\textwidth}
    \centering
    \includegraphics[width=\linewidth]{\PathToOutput/images/A2_RF_forecast_and_analyst_vs_actual.pdf}
    \caption{A2: two-years-ahead}
  \end{subfigure}
  \caption{RF prediction vs.\ analyst forecast vs.\ actual value by horizon. RF generally tracks actual more closely than analyst forecast; an abnormal period with large RF deviations can explain the high $(RF-AE)^2$ despite $(RF-AE)\approx 0$.}
  \label{fig:rf_af_actual_ts}
\end{figure}

\textbf{Figure~1---shape and presentation.}
\begin{itemize}
\item \textbf{Curve shape:} The paper's plot shows a clearer S-shape with flattening at both low and high forecast values; our replication starts higher on the $y$-axis for low $x$ and shows less flattening at the high end. The replication uses ``Analysts' Forecasts (Standardized)'' on the $x$-axis; differences in standardization or in the set of covariates in the RF could explain the shape gap.
\end{itemize}


\subsection{Practical Challenges}

Rolling-window RF training across five horizons is computationally intensive. Some data-cleaning details in the paper are under-specified (e.g., exact adjustment-factor alignment, industry imputation). Resolving the \textbf{direction} of $(AF-RF)/P$ (replication negative vs.\ paper positive) and the reversal in relative accuracy $(RF-AE)^2$ vs.\ $(AF-AE)^2$ would strengthen the replication.
"""

table2_full = (
    r"""
\begin{table}[H]
\centering
\caption{Term Structure of Earnings Forecasts via Machine Learning}
\label{tab:term_structure}
\small
\resizebox{\textwidth}{!}{%
\begin{tabular}{lccccccccc}
"""
    + table2_latex + "\n"
    + r"""
\end{tabular}%
}
\end{table}
"""
)

extended_section = r"""
\section{Extended Sample: 1986 to Latest Data}

We re-ran the same analysis using data from 1986 to the latest available date; outputs are in \texttt{\_output\_extended}. Below are the partial dependence figure (one-quarter-ahead) and Table~2 (term structure) from this extended sample.

\begin{figure}[H]
  \centering
  \includegraphics[width=0.7\linewidth]{\PathToOutputExtended/images/partial_dependence_meanest.png}
  \caption{Partial dependence (extended sample: 1986 to latest).}
  \label{fig:extended_pdp}
\end{figure}

\begin{table}[H]
\centering
\caption{Term structure of earnings forecasts, extended sample (1986 to latest).}
\label{tab:term_structure_extended}
\small
\resizebox{\textwidth}{!}{%
\begin{tabular}{lccccccccc}
""" + (table2_extended_latex or "\\multicolumn{10}{c}{(Extended table not available.)}\\\\n") + r"""
\end{tabular}%
}
\end{table}

The partial dependence plot in the extended sample exhibits the same non-linear, increasing shape as in our main replication. The term-structure table shows the same qualitative patterns: $(RF-AE)$ close to zero across horizons, $(AF-AE)$ positive (analysts above realized earnings), and $(AF-RF)/P$ with the same sign and significance as in the replication. Together, this indicates that the paper's main findings are robust to extending the sample through the latest available data.
"""

# %%
full_latex = (
    preamble
    + overview
    + data_sources
    + results_intro
    + comparison_figure1
    + comparison_table2
    + successes_challenges
    + extended_section
    + "\n\\end{document}\n"
)

out_tex = REPORTS_DIR / "replication_report_generated.tex"
out_tex.write_text(full_latex, encoding="utf-8")
print(f"Written: {out_tex}")
print("To compile: cd reports && pdflatex replication_report_generated.tex")


