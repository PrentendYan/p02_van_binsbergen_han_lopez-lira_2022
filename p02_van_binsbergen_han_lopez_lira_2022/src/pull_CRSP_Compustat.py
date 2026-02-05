"""
This module pulls and saves data on fundamentals from CRSP and Compustat.
It pulls fundamentals data from Compustat needed to calculate
book equity, and the data needed from CRSP to calculate market equity.

Note: This code uses the new CRSP CIZ format. Information
about the differences between the SIZ and CIZ format can be found here:

 - Transition FAQ: https://wrds-www.wharton.upenn.edu/pages/support/manuals-and-overviews/crsp/stocks-and-indices/crsp-stock-and-indexes-version-2/crsp-ciz-faq/
 - CRSP Metadata Guide: https://wrds-www.wharton.upenn.edu/documents/1941/CRSP_METADATA_GUIDE_STOCK_INDEXES_FLAT_FILE_FORMAT_2_0_CIZ_09232022v.pdf

For information about Compustat variables, see:
https://wrds-www.wharton.upenn.edu/documents/1583/Compustat_Data_Guide.pdf

For more information about variables in CRSP, see:
https://wrds-www.wharton.upenn.edu/documents/396/CRSP_US_Stock_Indices_Data_Descriptions.pdf
I don't think this is updated for the new CIZ format, though.

Here is some information about the old SIZ CRSP format:
https://wrds-www.wharton.upenn.edu/documents/1095/CRSP_Flat_File_formats_and_notes.pdf


The following is an outdated programmer's guide to CRSP:
https://wrds-www.wharton.upenn.edu/documents/400/CRSP_Programmers_Guide.pdf


"""

from pathlib import Path

import pandas as pd
import wrds
from pandas.tseries.offsets import MonthEnd
import numpy as np
from misc_tools import write_parquet
from settings import config

OUTPUT_DIR = Path(config("OUTPUT_DIR"))
DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")


description_compustat = {
    "gvkey": "Global Company Key",
    "datadate": "Data Date",
    "at": "Assets - Total",
    "sale": "Sales/Revenue",
    "cogs": "Cost of Goods Sold",
    "xsga": "Selling, General and Administrative Expense",
    "xint": "Interest Expense, Net",
    "pstkl": "Preferred Stock - Liquidating Value",
    "txditc": "Deferred Taxes and Investment Tax Credit",
    "pstkrv": "Preferred Stock - Redemption Value",
    # This item represents the total dollar value of the net number of
    # preferred shares outstanding multiplied by the voluntary
    # liquidation or redemption value per share.
    "seq": "Stockholders' Equity - Parent",
    "pstk": "Preferred/Preference Stock (Capital) - Total",
    "indfmt": "Industry Format",
    "datafmt": "Data Format",
    "popsrc": "Population Source",
    "consol": "Consolidation",
}


def pull_compustat(wrds_username=WRDS_USERNAME, start_date=START_DATE, end_date=END_DATE):
    conn = wrds.Connection(wrds_username=wrds_username)
    
    comp = conn.raw_sql(f"""
        select a.gvkey, a.datadate, a.fyear, a.csho, a.at, a.pstkl, a.txditc,
            a.pstkrv, a.seq, a.pstk, a.ppegt, a.invt, a.lt, a.sich, a.ib, a.oancf,
            a.act, a.dlc, a.che, a.lct, a.dvc, a.epspi, a.epspx,
            a.ajex,
            a.sale, a.ao, a.prcc_f
        from comp.funda as a
        where indfmt='INDL'
        and datafmt='STD'
        and popsrc='D'
        and consol='C'
        and curcd = 'USD'
        and datadate between '2020-01-01' and '2025-12-31'
    """, date_cols=["datadate"])

    comp["ps"] = np.where(comp["pstkrv"].isnull(), comp["pstkl"], comp["pstkrv"])
    comp["ps"] = np.where(comp["ps"].isnull(), comp["pstk"], comp["ps"])
    comp["ps"] = np.where(comp["ps"].isnull(), 0, comp["ps"])

    comp["txditc"] = comp["txditc"].fillna(0)
    comp["be"] = comp["seq"] + comp["txditc"] - comp["ps"]

    comp["act"] = comp["act"].fillna(0)
    comp["dlc"] = comp["dlc"].fillna(0)
    comp["che"] = comp["che"].fillna(0)
    comp["lct"] = comp["lct"].fillna(0)
    comp.sort_values(by=["gvkey", "datadate"], inplace=True)
    comp[["act_ch", "dlc_ch", "che_ch", "lct_ch"]] = comp.groupby("gvkey")[["act", "dlc", "che", "lct"]].diff()
    comp["acc"] = comp["act_ch"] + comp["dlc_ch"] - comp["che_ch"] - comp["lct_ch"]

    comp["at_l1"] = comp.groupby("gvkey")["at"].shift(1)
    comp["at_avg"] = comp[["at", "at_l1"]].mean(axis=1)
    comp["ag"] = comp.groupby("gvkey")["at"].pct_change(fill_method=None)
    comp["ppegt_diff"] = comp.groupby("gvkey")["ppegt"].diff()
    comp["ao_diff"] = comp.groupby("gvkey")["ao"].diff()
    comp["sale_l1"] = comp.groupby("gvkey")["sale"].shift(1)
    comp["sale_l3"] = comp.groupby("gvkey")["sale"].shift(3)
    comp["sale_l5"] = comp.groupby("gvkey")["sale"].shift(5)
    comp["sg_1y"] = comp["sale"] / comp["sale_l1"] - 1
    comp["sg_3y"] = (comp["sale"] / comp["sale_l3"]) ** (1 / 3) - 1
    comp["sg_5y"] = (comp["sale"] / comp["sale_l5"]) ** (1 / 5) - 1
    comp["adj_csho"] = comp["csho"] * comp["ajex"]
    comp["adj_csho_l1"] = comp.groupby("gvkey")["adj_csho"].shift(1)
    comp["nsi"] = np.log(comp["adj_csho"] / comp["adj_csho_l1"])

    return comp


description_crsp = {
    "permno": "Permanent Number - A unique identifier assigned by CRSP to each security.",
    "permco": "Permanent Company - A unique company identifier assigned by CRSP that remains constant over time for a given company.",
    "mthcaldt": "Calendar Date - The date for the monthly data observation.",
    "issuertype": "Issuer Type - Classification of the issuer, such as corporate or government.",
    "securitytype": "Security Type - General classification of the security, e.g., stock or bond.",
    "securitysubtype": "Security Subtype - More specific classification of the security within its type.",
    "sharetype": "Share Type - Classification of the equity share type, e.g., common stock, preferred stock.",
    "usincflg": "U.S. Incorporation Flag - Indicator of whether the company is incorporated in the U.S.",
    "primaryexch": "Primary Exchange - The primary stock exchange where the security is listed.",
    "conditionaltype": "Conditional Type - Indicator of any conditional issues related to the security.",
    "tradingstatusflg": "Trading Status Flag - Indicator of the trading status of the security, e.g., active, suspended.",
    "mthret": "Monthly Return - The total return of the security for the month, including dividends.",
    "mthretx": "Monthly Return Excluding Dividends - The return of the security for the month, excluding dividends.",
    "shrout": "Shares Outstanding - The number of outstanding shares of the security.",
    "mthprc": "Monthly Price - The price of the security at the end of the month.",
}


def get_crsp_columns(wrds_username=WRDS_USERNAME):
    """Get all column names from CRSP monthly stock file (CIZ format)."""
    sql_query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'crsp'
        AND table_name = 'msf_v2'
        ORDER BY ordinal_position;
    """

    db = wrds.Connection(wrds_username=wrds_username)
    columns = db.raw_sql(sql_query)
    db.close()

    return columns

def pull_compustat_quarterly(wrds_username=WRDS_USERNAME, start_date=START_DATE, end_date=END_DATE):
    conn = wrds.Connection(wrds_username=wrds_username)
    
    comp_q = conn.raw_sql("""
        select gvkey, datadate, fyearq, fqtr, 
            atq, ltq, niq, saleq, ceqq, 
            cshprq, epspxq, ajexq, rdq
        from comp.fundq
        where indfmt='INDL'
        and datafmt='STD'
        and popsrc='D'
        and consol='C'
        and curcdq = 'USD'
        and datadate between '2020-01-01' and '2025-12-31'
    """, date_cols=["datadate", "rdq"])

    # 基础处理
    comp_q.sort_values(by=["gvkey", "datadate"], inplace=True)
    comp_q["adj_cshprq"] = comp_q["cshprq"] * comp_q["ajexq"]
    comp_q["net_margin_q"] = comp_q["niq"] / comp_q["saleq"].replace(0, np.nan)
    comp_q["roe_q"] = comp_q["niq"] / comp_q["ceqq"].replace(0, np.nan)


    return comp_q


description_crsp_comp_link = {
    "gvkey": "Global Company Key - A unique identifier for companies in the Compustat database.",
    "permno": "Permanent Number - A unique stock identifier assigned by CRSP to each security.",
    "linktype": "Link Type - Indicates the type of linkage between CRSP and Compustat records. 'L' types refer to links considered official by CRSP.",
    "linkprim": "Primary Link Indicator - Specifies whether the link is a primary ('P') or secondary ('C') connection between the databases. Primary links are direct matches between CRSP and Compustat entities, while secondary links may represent subsidiary relationships or other less direct connections.",
    "linkdt": "Link Date Start - The starting date for which the linkage between CRSP and Compustat data is considered valid.",
    "linkenddt": "Link Date End - The ending date for which the linkage is considered valid. A blank or high value (e.g., '2099-12-31') indicates that the link is still valid as of the last update.",
}


def pull_CRSP_Comp_Link_Table(wrds_username=WRDS_USERNAME):
    sql_query = """
        SELECT 
            gvkey, lpermno AS permno, linktype, linkprim, linkdt, linkenddt
        FROM 
            crsp.ccmxpf_linktable
        WHERE 
            substr(linktype,1,1)='L' AND 
            (linkprim ='C' OR linkprim='P')
        """
    db = wrds.Connection(wrds_username=wrds_username)
    ccm = db.raw_sql(sql_query, date_cols=["linkdt", "linkenddt"])
    db.close()
    return ccm


def pull_Fama_French_factors(wrds_username=WRDS_USERNAME):
    conn = wrds.Connection(wrds_username=wrds_username)
    ff = conn.get_table(library="ff", table="factors_monthly")
    conn.close()
    ff[["smb", "hml"]] = ff[["smb", "hml"]].astype(float)

    ff["date"] = pd.to_datetime(ff["date"])
    ff["date"] = ff["date"] + MonthEnd(0)

    return ff


def load_compustat_annual(data_dir=DATA_DIR):
    path = Path(data_dir) / "compa.parquet"
    comp = pd.read_parquet(path)
    return comp

def load_compustat_quarterly(data_dir=DATA_DIR):
    path = Path(data_dir) / "comp_quarterly.parquet"
    comp_q = pd.read_parquet(path)
    return comp_q


def load_CRSP_Comp_Link_Table(data_dir=DATA_DIR):
    path = Path(data_dir) / "ccm.parquet"
    ccm = pd.read_parquet(path)
    return ccm


def load_Fama_French_factors(data_dir=DATA_DIR):
    path = Path(data_dir) / "FF_FACTORS.parquet"
    ff = pd.read_parquet(path)
    return ff




if __name__ == "__main__":
    comp = pull_compustat(wrds_username=WRDS_USERNAME)
    write_parquet(comp, DATA_DIR / "compa.parquet")

    comp_q = pull_compustat_quarterly(wrds_username=WRDS_USERNAME)
    write_parquet(comp_q, DATA_DIR / "comp_quarterly.parquet")
    
    ccm = pull_CRSP_Comp_Link_Table(wrds_username=WRDS_USERNAME)
    write_parquet(ccm, DATA_DIR / "ccm.parquet")
