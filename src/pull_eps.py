from datetime import datetime
import pandas as pd
from settings import config
from pathlib import Path
import wrds
from misc_tools import write_parquet
from pull_CRSP_stock import load_CRSP_monthly_file

START_DATE = config("START_DATE")
END_DATE = config("END_DATE")
DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")

def pull_eps_forecast(wrds_username=WRDS_USERNAME, start_date=START_DATE, end_date=END_DATE):
    conn = wrds.Connection(wrds_username=wrds_username)
    
    # Table candidates: 
    # 'statsumu' is the Unadjusted table (preferred for the paper).
    # 'statsum' is the Adjusted table (backup).
    summary_candidates = [
        ("ibes.statsumu_epsus", "statpers"),
        ("ibes.statsum_epsus", "statpers"),
    ]

    ibes_summary = None
    summary_exc = None

    for table, date_col in summary_candidates:
        try:
            print(f"Attempting to fetch data from {table}...")
            
            # SQL logic:
            # 1. measure='EPS': Filters for Earnings Per Share only.
            # 2. fpi='6': Filters for the 'Current Fiscal Quarter' forecast. 
            #    This is crucial to avoid duplicate rows for different forecast horizons.
            # 3. {date_col}: Filters by the statistical period date.
            sql = f"""
                SELECT * FROM {table} 
                WHERE measure = 'EPS' 
                AND fpi = '6' 
                AND {date_col} BETWEEN '2020-01-01' AND '2025-12-31'
            """
            
            # Execute query
            ibes_summary = conn.raw_sql(sql, date_cols=[date_col])
            
            if ibes_summary is not None and not ibes_summary.empty:
                print(f"Success! Retrieved {len(ibes_summary)} rows from {table}.")
                break
                
        except Exception as exc:
            print(f"Failed to query {table}. Error: {exc}")
            summary_exc = exc
            # We skip the "select * without date" fallback to prevent Out-of-Memory (OOM) errors
            continue

    conn.close()
    
    # Final Check and Save
    if ibes_summary is None or ibes_summary.empty:
        print(
            "Error: IBES summary tables are not accessible or returned no rows under the specified filters."
        )
    else:
        return ibes_summary
    
def pull_eps_actual(wrds_username=WRDS_USERNAME, start_date=START_DATE, end_date=END_DATE):
    
    conn = wrds.Connection(wrds_username=wrds_username)
    
    def _get_table_columns(table):
        schema, tbl = table.split(".")
        cols_df = conn.raw_sql(
            f"""
            select column_name
            from information_schema.columns
            where table_schema = '{schema}' and table_name = '{tbl}'
            order by ordinal_position
            """
        )
        return [c.lower() for c in cols_df["column_name"].tolist()]

    def _build_ibes_query(table, columns):
        select_cols = "*"
        filters = []
        if "measure" in columns:
            filters.append("measure='EPS'")
        if "pdicity" in columns:
            filters.append("pdicity in ('ANN','QTR')")
        date_col = None
        for candidate in ["anndats", "actdats", "statpers", "fpedats"]:
            if candidate in columns:
                date_col = candidate
                break
        if date_col:
            filters.append(f"{date_col} between '2020-01-01' and '2025-12-31'")
        where_clause = ""
        if filters:
            where_clause = "where " + " and ".join(filters)
        sql = f"""
            select {select_cols}
            from {table}
            {where_clause}
        """
        return sql, [date_col] if date_col else []

    summary_actual_candidates = [
        "ibes.actu_epsus",
        "ibes.act_epsus",
    ]

    ibes_summary_actual = None
    summary_actual_exc = None
    for table in summary_actual_candidates:
        try:
            cols = _get_table_columns(table)
            sql, date_cols = _build_ibes_query(table, cols)
            ibes_summary_actual = conn.raw_sql(sql, date_cols=date_cols)
            break
        except Exception as exc:
            summary_actual_exc = exc

    if ibes_summary_actual is None:
        try:
            available_tables = conn.raw_sql(
                """
                select table_name
                from information_schema.tables
                where table_schema = 'ibes'
                order by table_name
                """
            )
            tables_list = ", ".join(available_tables["table_name"].tolist())
        except Exception:
            tables_list = "<not visible>"
        print(
            "IBES summary actual tables are not accessible in this account. "
            "Skipping IBES summary actual. Available tables: ",
            tables_list,
        )
    else:
        return ibes_summary_actual
    
    
def pull_crsp_ibes_link(wrds_username=WRDS_USERNAME):
    
    conn = wrds.Connection(wrds_username=wrds_username)
    
    def _find_first(columns, candidates):
        for name in candidates:
            if name in columns:
                return name
        return None

    def _load_iclink_from_table(table):
        schema, tbl = table.split(".")
        cols_df = conn.raw_sql(
            f"""
            select column_name
            from information_schema.columns
            where table_schema = '{schema}' and table_name = '{tbl}'
            order by ordinal_position
            """
        )
        columns = [c.lower() for c in cols_df["column_name"].tolist()]

        permno_col = _find_first(columns, ["permno", "lpermno", "permno_crsp", "permno_crspn"])
        if permno_col is None:
            return None

        ticker_col = _find_first(columns, ["ticker", "ibtic", "tic", "ibes_ticker"])
        sdate_col = _find_first(
            columns,
            ["sdate", "startdate", "linkdt", "begdate", "start_date", "link_start"],
        )
        edate_col = _find_first(
            columns,
            ["edate", "enddate", "linkenddt", "end_date", "link_end"],
        )
        score_col = _find_first(columns, ["score", "linkscore", "quality"])

        select_cols = [f"{permno_col} as permno"]
        if ticker_col:
            select_cols.append(f"{ticker_col} as ticker")
        else:
            select_cols.append("NULL::text as ticker")
        if sdate_col:
            select_cols.append(f"{sdate_col} as sdate")
        else:
            select_cols.append("NULL::date as sdate")
        if edate_col:
            select_cols.append(f"{edate_col} as edate")
        else:
            select_cols.append("NULL::date as edate")
        if score_col:
            select_cols.append(f"{score_col} as score")
        else:
            select_cols.append("NULL::numeric as score")

        where_clause = ""
        if score_col:
            where_clause = "where score <= 1"

        sql = f"""
            select {', '.join(select_cols)}
            from {table}
            {where_clause}
        """
        return conn.raw_sql(sql, date_cols=["sdate", "edate"])

    iclink_candidates = [
        "wrdsapps.id_ibes",
        "wrdsapps.id_ibes_ccm",
        "wrdsapps.ibcrsphist",
        "wrdsapps.opcrsphist",
        "wrdsapps.firm_ratio_ibes",
        "wrdsapps.firm_ratio_ibes_ccm",
    ]

    iclink = None
    last_exc = None
    for table in iclink_candidates:
        try:
            iclink = _load_iclink_from_table(table)
            if iclink is not None and not iclink.empty:
                break
        except Exception as exc:
            last_exc = exc

    conn.close()
    if iclink is None:
        print(
            "No IBES-CRSP link table with permno found in wrdsapps. "
            "Skipping ICLINK.",
        )
    else:

        return iclink

        
def merge_crsp_ibes(crsp_m, iclink):
    
    crsp_ibes = crsp_m.merge(iclink, on="permno", how="left")
    if "sdate" in crsp_ibes.columns and "edate" in crsp_ibes.columns:
        crsp_ibes = crsp_ibes[
            (crsp_ibes["YearMonth"] >= crsp_ibes["sdate"])
            & (crsp_ibes["YearMonth"] <= crsp_ibes["edate"])
        ].copy()
    
    return crsp_ibes
    
def load_eps_forecast(data_dir=DATA_DIR):
    path = Path(data_dir) / "Forecast_EPS_summary_unadjusted_1986_2019.parquet"
    return pd.read_parquet(path)

def load_eps_actual(data_dir=DATA_DIR):
    path = Path(data_dir) / "Actual_EPS_summary_unadjusted_1986_2019.parquet"
    return pd.read_parquet(path)
    
    
if __name__ == "__main__":
    ibes_summary_forecast = pull_eps_forecast()
    write_parquet(ibes_summary_forecast, DATA_DIR / "Forecast_EPS_summary_unadjusted_1986_2019.parquet")
    
    ibes_summary_actual = pull_eps_actual()
    write_parquet(ibes_summary_actual, DATA_DIR / "Actual_EPS_summary_unadjusted_1986_2019.parquet")

    iclink = pull_crsp_ibes_link()
    write_parquet(iclink, DATA_DIR / "crsp_ibes_link.parquet")

    crsp_m = load_CRSP_monthly_file()
    crsp_ibes = merge_crsp_ibes(crsp_m, iclink)
    write_parquet(crsp_ibes, DATA_DIR / "crsp_ibes_linked.parquet")

    # Data Validation: Ensure uniqueness
    # In a clean IBES summary pull, (ticker + statpers) should be a unique key when fpi is fixed.
    max_dups = ibes_summary_forecast.groupby(['ticker', 'statpers']).size().max()
    if max_dups > 1:
        print(f"Warning: Detected {max_dups} duplicate entries for the same ticker/date. Check FPI settings.")
    else:
        print("Validation Passed: Data is unique per ticker and statistical period.")
    