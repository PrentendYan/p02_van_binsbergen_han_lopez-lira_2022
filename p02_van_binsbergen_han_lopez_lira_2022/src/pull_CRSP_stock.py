"""
Functions to pull and calculate the value and equal weighted CRSP indices.

 - Data for indices: https://wrds-www.wharton.upenn.edu/data-dictionary/crsp_a_indexes/
 - Data for raw stock data: https://wrds-www.wharton.upenn.edu/pages/get-data/center-research-security-prices-crsp/annual-update/stock-security-files/monthly-stock-file/
 - Why we can't perfectly replicate them: https://wrds-www.wharton.upenn.edu/pages/support/support-articles/crsp/index-and-deciles/constructing-value-weighted-return-series-matches-vwretd-crsp-monthly-value-weighted-returns-includes-distributions/
 - Methodology used: https://wrds-www.wharton.upenn.edu/documents/396/CRSP_US_Stock_Indices_Data_Descriptions.pdf
 - Useful link: https://www.tidy-finance.org/python/wrds-crsp-and-compustat.html

Thank you to Tobias Rodriguez del Pozo for his assistance in writing this
code.

Note: This code is based on the old CRSP SIZ format. Information
about the new CIZ format can be found here:

 - Transition FAQ: https://wrds-www.wharton.upenn.edu/pages/support/manuals-and-overviews/crsp/stocks-and-indices/crsp-stock-and-indexes-version-2/crsp-ciz-faq/
 - CRSP Metadata Guide: https://wrds-www.wharton.upenn.edu/documents/1941/CRSP_METADATA_GUIDE_STOCK_INDEXES_FLAT_FILE_FORMAT_2_0_CIZ_09232022v.pdf

"""

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.tseries.offsets import MonthEnd
from misc_tools import write_parquet
import wrds
from dateutil.relativedelta import relativedelta

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

def pull_CRSP_monthly_file(wrds_username=WRDS_USERNAME, start_date=START_DATE, end_date=END_DATE):
    
    conn = wrds.Connection(wrds_username=wrds_username)
    
    # CRSP Block
    crsp_m = conn.raw_sql(f"""
        select a.permno, a.permco, a.date, a.ret, a.retx, a.shrout, a.prc, a.cfacshr,
            b.shrcd, b.exchcd, b.siccd, b.ncusip,
            c.dlstcd, c.dlret
        from crsp.msf as a
        left join crsp.msenames as b
        on a.permno=b.permno and b.namedt<=a.date and a.date<=b.nameendt
        left join crsp.msedelist as c
        on a.permno=c.permno AND date_trunc('month', a.date) = date_trunc('month', c.dlstdt)
        where a.date between '2020-01-01' and '2025-12-31'
        and b.exchcd between 1 and 3
        and b.shrcd between 10 and 11
    """, date_cols=["date"])

    crsp_m[["permco", "permno", "shrcd", "exchcd"]] = crsp_m[["permco", "permno", "shrcd", "exchcd"]].astype(int)
    crsp_m["YearMonth"] = crsp_m["date"] + MonthEnd(0)

    crsp_m["dlret"] = np.where((~crsp_m["dlstcd"].isna()) & (crsp_m["dlret"].isna()), -0.5, crsp_m["dlret"])
    crsp_m["dlret"] = crsp_m["dlret"].fillna(0)
    crsp_m["retadj"] = (1 + crsp_m["ret"]) * (1 + crsp_m["dlret"]) - 1
    crsp_m["retadj"] = np.where((crsp_m["ret"].isna()) & (crsp_m["dlret"] != 0), crsp_m["dlret"], crsp_m["ret"])

    crsp_m = crsp_m.sort_values(by=["permno", "YearMonth"]).reset_index(drop=True)

    return crsp_m


def apply_delisting_returns(df):
    """
    Use instructions for handling delisting returns from: Chapter 7 of
    Bali, Engle, Murray --
    Empirical asset pricing-the cross section of stock returns (2016)

    First change dlret column.
    If dlret is NA and dlstcd is not NA, then:
    if dlstcd is 500, 520, 551-574, 580, or 584, then dlret = -0.3
    if dlret is NA but dlstcd is not one of the above, then dlret = -1
    """
    df["dlret"] = np.select(
        [
            df["dlstcd"].isin([500, 520, 580, 584] + list(range(551, 575)))
            & df["dlret"].isna(),
            df["dlret"].isna() & df["dlstcd"].notna() & df["dlstcd"] >= 200,
            True,
        ],
        [-0.3, -1, df["dlret"]],
        default=df["dlret"],
    )

    df["dlretx"] = np.select(
        [
            df["dlstcd"].isin([500, 520, 580, 584] + list(range(551, 575)))
            & df["dlretx"].isna(),
            df["dlretx"].isna() & df["dlstcd"].notna() & df["dlstcd"] >= 200,
            True,
        ],
        [-0.3, -1, df["dlretx"]],
        default=df["dlretx"],
    )

    # Replace the inplace operations with direct assignments
    df["ret"] = df["ret"].fillna(df["dlret"])
    df["retx"] = df["retx"].fillna(df["dlretx"])
    return df


def apply_delisting_returns_alt(df):
    df["dlret"] = df["dlret"].fillna(0)
    df["ret"] = df["ret"] + df["dlret"]
    df["ret"] = np.where(
        (df["ret"].isna()) & (df["dlret"] != 0), df["dlret"], df["ret"]
    )
    return df


def pull_CRSP_index_files(
    start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME
):
    """
    Pulls the CRSP index files from crsp_a_indexes.msix:
    (Monthly)NYSE/AMEX/NASDAQ Capitalization Deciles, Annual Rebalanced (msix)
    """
    # Pull index files
    query = f"""
        SELECT * 
        FROM crsp_a_indexes.msix
        WHERE caldt BETWEEN '{start_date}' AND '{end_date}'
    """
    # with wrds.Connection(wrds_username=wrds_username) as db:
    #     df = db.raw_sql(query, date_cols=["month", "caldt"])
    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(query, date_cols=["caldt"])
    db.close()
    return df


def load_CRSP_monthly_file(data_dir=DATA_DIR):
    path = Path(data_dir) / "crsp_m.parquet"
    df = pd.read_parquet(path)
    return df


if __name__ == "__main__":
    df_msf = pull_CRSP_monthly_file(start_date=START_DATE, end_date=END_DATE)
    write_parquet(df_msf, DATA_DIR / "crsp_m.parquet")
