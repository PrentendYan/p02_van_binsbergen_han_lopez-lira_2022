import numpy as np
import pandas as pd
from misc_tools import write_parquet
from settings import config
from pathlib import Path
from pull_CRSP_Compustat import load_compustat_annual

DATA_DIR = Path(config("DATA_DIR"))
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")


def cal_financial_ratios(comp):

    # Manual financial ratios derived from Compustat (fallback when WRDS ratios are not accessible)
    def _safe_div(numerator, denominator):
        denom = denominator.replace(0, np.nan)
        return numerator / denom

    financial_ratios = comp[
        [
            "gvkey",
            "datadate",
            "fyear",
            "at",
            "lt",
            "act",
            "lct",
            "che",
            "invt",
            "sale",
            "ib",
            "be",
            "prcc_f",
            "csho",
        ]
    ].copy()

    financial_ratios["current_ratio"] = _safe_div(financial_ratios["act"], financial_ratios["lct"])
    financial_ratios["quick_ratio"] = _safe_div(
        financial_ratios["act"] - financial_ratios["invt"],
        financial_ratios["lct"],
    )
    financial_ratios["cash_ratio"] = _safe_div(financial_ratios["che"], financial_ratios["lct"])
    financial_ratios["invt_turn"] = _safe_div(financial_ratios["sale"], financial_ratios["invt"])
    financial_ratios["asset_turn"] = _safe_div(financial_ratios["sale"], financial_ratios["at"])
    financial_ratios["prof_margin"] = _safe_div(financial_ratios["ib"], financial_ratios["sale"])
    financial_ratios["roe"] = _safe_div(financial_ratios["ib"], financial_ratios["be"])
    financial_ratios["roa"] = _safe_div(financial_ratios["ib"], financial_ratios["at"])
    financial_ratios["de_ratio"] = _safe_div(financial_ratios["lt"], financial_ratios["be"])
    financial_ratios["debt_assets"] = _safe_div(financial_ratios["lt"], financial_ratios["at"])

    financial_ratios["mktcap"] = financial_ratios["prcc_f"] * financial_ratios["csho"]
    financial_ratios["bm"] = _safe_div(financial_ratios["be"], financial_ratios["mktcap"])

    return financial_ratios


if __name__ == "__main__":
    compa = load_compustat_annual()
    financial_ratios = cal_financial_ratios(compa)
    write_parquet(financial_ratios, DATA_DIR / "financial_ratio.parquet")