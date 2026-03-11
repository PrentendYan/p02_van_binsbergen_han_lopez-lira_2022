"""
Data engineering for Man vs Machine: IBES-CRSP link, macro processing, merge with finratio.

Depends on: pipeline_load_data outputs (crsp, ibes_summary, finratio, FED CSVs).
Outputs: DATA_DIR/ibes_crsp.csv, DATA_DIR/processed_data/macro_data.csv, A1,A2,Q1,Q2,Q3.csv

Pipeline steps (data cleaning and alignment):
────────────────────────────────────────────

1) IBES–CRSP link table (WRDS)
   - IBES: from ibes.id (usfirm=1, cusip non-empty), per (ticker, cusip) take latest sdates → fdate/ldate.
   - CRSP: from crsp.stocknames (ncusip non-empty), per (permno, ncusip) take min(namedt), max(nameenddt).
   - Join IBES and CRSP on cusip = ncusip; keep (ticker, permno) with max(ldate) to get one permno per IBES identity.
   - Result: link_table [permno, ncusip] for matching IBES to CRSP.

2) IBES–CRSP panel and adjustment
   - Load local ibes_summary.csv and crsp.csv; normalize dates (statpers, rankdate as period 'M').
   - Align IBES to CRSP: merge IBES with link_table on cusip, then with CRSP on (permno, date=statpers) to get
     price, ret, cfacshr at estimate date → cfacshr_estdate.
   - Align to report date: merge again with CRSP on (permno, date=announcement_actual_eps) for cfacshr at report date
     → cfacshr_reportdate. Compute adjust_factor = cfacshr_estdate / cfacshr_reportdate, adj_actual = actual * adjust_factor.
   - Past EPS: group by (ticker, fpi_group), merge_asof on announcement_actual_eps (backward) to attach adj_past_eps
     and announcement_past_ep. fpi_group: 6,7,8→'678', 1,2→'12'.
   - Column cleanup: drop actual, cfacshr_estdate/reportdate; save as ibes_crsp.csv.

3) Macro data
   - Load FED CSVs: real_GDP_FED, IPT_FED (skip/filter rows), real_personal_consumption_FED, Unemployment (skip rows).
   - PrepareMacro(): for each series, walk (Begin_Year, Begin_Month) forward, build Dates + value per month; handle NaNs.
   - Unemployment: take first non-NaN per row from raw array → Unempl_Data with Dates (normalized to YYYY-MM).
   - Log returns: GDP, Cons, IPT → log(x_t / x_{t-1}), dropna.
   - Outer merge all macro series on Dates → macro_data.csv (GDP_log_return, Cons_log_return, IPT_log_return, Unempl, Dates).

4) Finratio and final forecast panels
   - Load finratio.csv; drop selected columns (peg*, pe_op*, price, ret_crsp, identifiers/industry text).
   - public_date to datetime; drop gvkey, adate, qdate, ticker, cusip, ff* and sector columns.
   - Fill NaNs: by (public_date, ffi49) fill with group median; by permno ffill/bfill; again by (public_date, ffi49) median.
   - Align to IBES: merge_asof(IBES_CRSP by statpers, finratio by public_date, by=permno, direction='backward')
     so each forecast row gets the latest available finratio as of statpers.
   - Split by fpi: A1 (fpi=1), A2 (fpi=2), Q1 (6), Q2 (7), Q3 (8). Drop rows with missing adj_actual, meanest, adj_past_eps.
   - Sort by (permno, rankdate), save A1.csv … Q3.csv to processed_data/.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from settings import config
from functions import PrepareMacro

import pandas as pd
import numpy as np

DATA_DIR = Path(config("DATA_DIR"))
PROCESSED_DIR = Path(config("PROCESSED_DIR"))
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def group_fpi(fpi):
    if fpi in [6, 7, 8]:
        return '678'
    elif fpi in [1, 2]:
        return '12'
    return fpi


def run_data_engineering(use_wrds=True):
    if use_wrds:
        try:
            import wrds
            # Match notebook: notebook uses wrds.Connection(yautoconnect=True).
            # If WRDS_USERNAME is set (e.g. in .env), use it for non-interactive runs.
            try:
                username = config("WRDS_USERNAME")
                db = wrds.Connection(wrds_username=username)
            except Exception:
                db = wrds.Connection(yautoconnect=True)
        except Exception as e:
            print("WRDS connection failed:", e)
            return
    else:
        db = None

    # ---- 1) IBES-CRSP link table from WRDS ----
    #(permno, ncusip)
    if db is not None:
        _ibes1 = db.raw_sql("""
            select ticker, cusip, cname, sdates
            from ibes.id
            where usfirm='1' and cusip != ''
        """, date_cols=['sdates'])
        _ibes1_date = _ibes1.groupby(['ticker', 'cusip']).sdates.agg(['min', 'max']).reset_index().rename(
            columns={'min': 'fdate', 'max': 'ldate'})
        _ibes2 = pd.merge(_ibes1, _ibes1_date, how='left', on=['ticker', 'cusip'])
        _ibes2 = _ibes2.sort_values(by=['ticker', 'cusip', 'sdates'])
        _ibes2 = _ibes2.loc[_ibes2.sdates == _ibes2.ldate].drop(['sdates'], axis=1)

        _crsp1 = db.raw_sql("""
            select permno, ncusip, comnam, namedt, nameenddt
            from crsp.stocknames where ncusip != ''
        """, date_cols=['namedt', 'nameenddt'])
        _crsp1_fnamedt = _crsp1.groupby(['permno', 'ncusip']).namedt.min().reset_index()
        _crsp1_lnameenddt = _crsp1.groupby(['permno', 'ncusip']).nameenddt.max().reset_index()
        _crsp1_dtrange = pd.merge(_crsp1_fnamedt, _crsp1_lnameenddt, on=['permno', 'ncusip'], how='inner')
        _crsp1 = _crsp1.drop(['namedt'], axis=1).rename(columns={'nameenddt': 'enddt'})
        _crsp2 = pd.merge(_crsp1, _crsp1_dtrange, on=['permno', 'ncusip'], how='inner')
        _crsp2 = _crsp2.loc[_crsp2.enddt == _crsp2.nameenddt].drop(['enddt'], axis=1)

        _link1_1 = pd.merge(_ibes2, _crsp2, how='inner', left_on='cusip', right_on='ncusip').sort_values(
            ['ticker', 'permno', 'ldate'])
        _link1_1_tmp = _link1_1.groupby(['ticker', 'permno']).ldate.max().reset_index()
        _link1_2 = pd.merge(_link1_1, _link1_1_tmp, how='inner', on=['ticker', 'permno', 'ldate'])
        link_table = _link1_2[['permno', 'ncusip']]
    else:
        print("Skipping WRDS link table; need existing ibes_crsp or run with WRDS.")
        return

    # ---- 2) Load local CRSP, IBES ----
    IBES = pd.read_csv(DATA_DIR / "ibes_summary.csv")
    if 'anndats_act' in IBES.columns:
        IBES.rename(columns={'anndats_act': 'announcement_actual_eps'}, inplace=True)
    CRSP = pd.read_csv(DATA_DIR / "crsp.csv")

    CRSP['rankdate'] = pd.to_datetime(CRSP['date'])
    CRSP['date'] = pd.to_datetime(CRSP['date'])
    CRSP['rankdate'] = CRSP['rankdate'].dt.to_period('M')
    CRSP['ret'] = pd.to_numeric(CRSP['ret'], errors='coerce')
    CRSP = CRSP.sort_values(by=['permno', 'rankdate'], ascending=True)

    IBES_link = pd.merge(IBES, link_table, how='inner', left_on=['cusip'], right_on=['ncusip']).drop('ncusip', axis=1)
    IBES_link['statpers'] = pd.to_datetime(IBES_link['statpers'])
    IBES_link['rankdate'] = pd.to_datetime(IBES_link['statpers']).dt.to_period('M')
    IBES_link['permno'] = IBES_link['permno'].astype('int')

    IBES_CRSP1 = pd.merge(
        CRSP[['permno', 'price', 'ret', 'cfacshr', 'date']],
        IBES_link,
        how='inner',
        left_on=['permno', 'date'],
        right_on=['permno', 'statpers']
    )
    IBES_CRSP1.rename(columns={'cfacshr': 'cfacshr_estdate'}, inplace=True)
    IBES_CRSP1.drop(columns=['date'], axis=1, inplace=True)

    IBES_CRSP1['announcement_actual_eps'] = pd.to_datetime(IBES_CRSP1['announcement_actual_eps'])
    IBES_CRSP2 = pd.merge(
        CRSP[['permno', 'cfacshr', 'date']],
        IBES_CRSP1,
        how='inner',
        left_on=['permno', 'date'],
        right_on=['permno', 'announcement_actual_eps']
    )
    IBES_CRSP2.rename(columns={'cfacshr': 'cfacshr_reportdate'}, inplace=True)
    IBES_CRSP2.drop(columns=['date'], axis=1, inplace=True)

    IBES_CRSP2['adjust_factor'] = IBES_CRSP2['cfacshr_estdate'] / IBES_CRSP2['cfacshr_reportdate']
    IBES_CRSP2['adj_actual'] = IBES_CRSP2['actual'] * IBES_CRSP2['adjust_factor']
    IBES_CRSP2.drop(columns=['actual', 'cfacshr_estdate', 'cfacshr_reportdate'], axis=1, inplace=True)

    IBES_CRSP2 = IBES_CRSP2.sort_values(by=['ticker', 'statpers'], ascending=True)
    IBES_adj_actual = IBES_CRSP2[['announcement_actual_eps', 'ticker', 'adj_actual', 'fpi']].sort_values(
        by=['ticker', 'announcement_actual_eps'], ascending=True)
    IBES_adj_actual.dropna(subset=['announcement_actual_eps'], inplace=True)
    IBES_adj_actual['announcement_actual_eps'] = pd.to_datetime(IBES_adj_actual['announcement_actual_eps'])
    IBES_CRSP2['fpi_group'] = IBES_CRSP2['fpi'].apply(group_fpi)
    IBES_adj_actual['fpi_group'] = IBES_adj_actual['fpi'].apply(group_fpi)

    IBES_CRSP = pd.merge_asof(
        IBES_CRSP2.set_index('statpers').sort_index(),
        IBES_adj_actual.set_index('announcement_actual_eps', drop=False).sort_index(),
        left_index=True,
        right_index=True,
        by=['ticker', 'fpi_group'],
        direction='backward'
    )
    IBES_CRSP.rename(columns={
        'adj_actual_x': 'adj_actual', 'adj_actual_y': 'adj_past_eps',
        'announcement_actual_eps_x': 'announcement_actual_eps',
        'announcement_actual_eps_y': 'announcement_past_ep', 'fpi_x': 'fpi'
    }, inplace=True)
    IBES_CRSP = IBES_CRSP.drop(columns=['fpi_group', 'fpi_y'])
    IBES_CRSP = IBES_CRSP.reset_index()

    IBES_CRSP.to_csv(DATA_DIR / "ibes_crsp.csv", index=False)
    print("Saved", DATA_DIR / "ibes_crsp.csv")

    # ---- 3) Macro data ----
    GDP_Raw = pd.read_csv(DATA_DIR / "real_GDP_FED.csv", index_col=0)
    IPT_Raw = pd.read_csv(DATA_DIR / "IPT_FED.csv", skiprows=range(1, 620), index_col=0)
    IPT_Raw.drop(IPT_Raw.columns[1:121], axis=1, inplace=True)
    IPT_Raw.reset_index(inplace=True, drop=True)
    Cons_Raw = pd.read_csv(DATA_DIR / "real_personal_consumption_FED.csv", index_col=0)
    Unempl_Raw = pd.read_csv(DATA_DIR / "Unemployment_FED.csv", skiprows=range(1, 225), index_col=0)

    GDP_Data = PrepareMacro(GDP_Raw, config("MACRO_GDP_START_YEAR"), config("MACRO_GDP_START_MONTH"), 'ROUTPUT', 'GDP')
    IPT_Data = PrepareMacro(IPT_Raw, config("MACRO_IPT_START_YEAR"), config("MACRO_IPT_START_MONTH"), 'IPT', 'IPT')
    Cons_Data = PrepareMacro(Cons_Raw, config("MACRO_CONS_START_YEAR"), config("MACRO_CONS_START_MONTH"), 'RCON', 'Cons')

    Unempl_Arr = Unempl_Raw.to_numpy()
    N = Unempl_Arr.shape[0]
    values = []
    for i in range(N):
        x = Unempl_Arr[i, 1:]
        first_non_nan = next((v for v in x if not np.isnan(v)), None)
        values.append(first_non_nan)
    Unempl_Data = pd.DataFrame({'Dates': Unempl_Raw['DATE'], 'Unempl': values})
    Unempl_Data['Dates'] = Unempl_Data['Dates'].str.replace(':', '-')
    Unempl_Data['Dates'] = pd.to_datetime(Unempl_Data['Dates'], format='%Y-%m')

    for df, name in zip([GDP_Data, Cons_Data, IPT_Data], ['GDP', 'Cons', 'IPT']):
        df[name + '_log_return'] = np.log(df[name] / df[name].shift(1))
        df.dropna(inplace=True)

    merged_macro = Unempl_Data
    for df in [GDP_Data, Cons_Data, IPT_Data]:
        merged_macro = pd.merge(merged_macro, df, on=['Dates'], how='outer')
    merged_macro.to_csv(PROCESSED_DIR / "macro_data.csv", index=False)
    print("Saved", PROCESSED_DIR / "macro_data.csv")

    # ---- 4) Finratio and merge ----
    finratio = pd.read_csv(DATA_DIR / "finratio.csv", index_col=0)
    finratio.drop(
        ['peg_1yrforward', 'peg_ltgforward', 'pe_op_basic', 'pe_op_dil', 'price', 'ret_crsp'],
        axis=1, inplace=True
    )
    finratio['public_date'] = pd.to_datetime(finratio['public_date'].astype('str'))
    finratio.drop([
        'gvkey', 'adate', 'qdate', 'ticker', 'cusip', 'ffi5_desc', 'ffi5', 'ffi10_desc', 'ffi10',
        'ffi12_desc', 'ffi12', 'ffi17_desc', 'ffi17', 'ffi30_desc', 'ffi30', 'ffi38_desc', 'ffi38',
        'ffi48_desc', 'ffi48', 'ffi49_desc', 'gsector', 'gicdesc'
    ], axis=1, inplace=True)

    vars_winsorize = list(finratio.drop(['permno'], axis=1).columns)
    finratio = finratio.dropna(axis=0, subset=['ffi49'])
    finratio.loc[:, vars_winsorize] = finratio.groupby(['public_date', 'ffi49'])[vars_winsorize].transform(
        lambda x: x.fillna(x.median(skipna=True)))
    finratio.loc[:, vars_winsorize] = finratio.groupby('permno')[vars_winsorize].transform(lambda x: x.ffill().bfill())
    finratio.loc[:, vars_winsorize] = finratio.groupby(['public_date', 'ffi49'])[vars_winsorize].transform(
        lambda x: x.fillna(x.median(skipna=True)))

    IBES_CRSP = IBES_CRSP.sort_values(by=['permno', 'statpers'], ascending=True)
    IBES_CRSP['statpers'] = pd.to_datetime(IBES_CRSP['statpers'])
    finratio = finratio.sort_values(by=['permno', 'public_date'], ascending=True)
    IBES_CRSP['permno'] = IBES_CRSP['permno'].astype(int)
    finratio['permno'] = finratio['permno'].astype(int)

    data = pd.merge_asof(
        IBES_CRSP.set_index('statpers').sort_index(),
        finratio.set_index('public_date', drop=False).sort_index(),
        left_index=True,
        right_index=True,
        by='permno',
        direction='backward'
    )
    data = data.reset_index()
    data = data.sort_values(by=['permno', 'rankdate'])

    if 'Unnamed: 0' in data.columns:
        data.drop(columns=['Unnamed: 0'], axis=1, inplace=True)

    A1 = data[data['fpi'] == 1].reset_index(drop=True)
    A2 = data[data['fpi'] == 2].reset_index(drop=True)
    Q1 = data[data['fpi'] == 6].reset_index(drop=True)
    Q2 = data[data['fpi'] == 7].reset_index(drop=True)
    Q3 = data[data['fpi'] == 8].reset_index(drop=True)

    for name, Forecast in [('A1', A1), ('A2', A2), ('Q1', Q1), ('Q2', Q2), ('Q3', Q3)]:
        Forecast.dropna(subset=['adj_actual', 'meanest', 'adj_past_eps'], inplace=True)
        Forecast.sort_values(by=['permno', 'rankdate'], ascending=True, inplace=True)
        Forecast.reset_index(drop=True, inplace=True)
        out = PROCESSED_DIR / f"{name}.csv"
        Forecast.to_csv(out, index=False)
        print("Saved", out)
    print("Data engineering done.")


if __name__ == "__main__":
    run_data_engineering(use_wrds=True)
