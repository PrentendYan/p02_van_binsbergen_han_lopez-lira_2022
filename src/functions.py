"""
Shared functions for Man vs Machine pipeline (van Binsbergen, Han, Lopez-Lira 2022).
Paths use project config (DATA_DIR, OUTPUT_DIR) when run via dodo; can be overridden.
"""
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import statsmodels.api as sm
import numpy as np
import matplotlib.pyplot as plt
from sklearn import preprocessing
from tqdm.auto import tqdm
from pathlib import Path


def _data_dir():
    try:
        from settings import config
        return Path(config("DATA_DIR"))
    except Exception:
        return Path("_data")


def _output_dir():
    try:
        from settings import config
        return Path(config("OUTPUT_DIR"))
    except Exception:
        return Path("_output")


def PrepareMacro(Macro_Data, Begin_Year, Begin_Month, Name_col, Name_Var):
    """
    Prepare macroeconomic data by extracting values for specified years and months.
    """
    month = Begin_Month
    dates = []
    values = []
    shape = Macro_Data.shape
    n_columns = shape[1]
    col = 1

    for i in range(0, n_columns + 1):
        year = (Begin_Year + i) % 100
        while month <= 12:
            if col == n_columns:
                break
            else:
                year_string = str("{:02d}".format(year))
                month_string = str(month)
                col_name = Name_col + year_string + 'M' + month_string
                A = Macro_Data[col_name]
                B = pd.Series(A.isna().values).value_counts()
                if A.count() == shape[0]:
                    values.append(A.iloc[-1])
                else:
                    values.append(A[B.iloc[1] - 1])
                if year >= Begin_Year:
                    dates.append('19' + year_string + '-' + month_string)
                else:
                    dates.append('20' + year_string + '-' + month_string)
                month += 1
                col += 1
        month = 1

    d = {'Dates': dates, Name_Var: values}
    y = pd.DataFrame(data=d)
    y['Dates'] = pd.to_datetime(y['Dates'], format='%Y-%m')
    return y


def read_merge_prepare_data(forecast_period, Macro_Data, data_dir=None):
    """
    Read, merge, and prepare data from CSV files.
    """
    if data_dir is None:
        data_dir = _data_dir()
    data_dir = Path(data_dir)
    processed = data_dir / "processed_data"
    forecast_file_path = processed / f"{forecast_period}.csv"
    df = pd.read_csv(forecast_file_path)

    df = df.sort_values(by=['permno', 'statpers'], ascending=True)
    df.statpers = pd.to_datetime(df.statpers)
    Macro_Data = Macro_Data[['GDP_log_return', 'Cons_log_return', 'IPT_log_return', 'Unempl', 'Dates']]
    Macro_Data = Macro_Data.sort_values(by=['Dates'], ascending=True)
    Macro_Data.Dates = pd.to_datetime(Macro_Data.Dates)

    Merged_Data = pd.merge_asof(
        df.set_index('statpers').sort_index(),
        Macro_Data.set_index('Dates', drop=False).sort_index(),
        left_index=True,
        right_index=True,
        direction='backward'
    ).drop(columns=['Dates'])

    Merged_Data = Merged_Data.reset_index()
    Merged_Data.sort_values(by=['permno', 'rankdate'], ascending=True)
    Merged_Data['Date'] = pd.to_datetime(Merged_Data['rankdate'], format='%Y-%m').dt.to_period('M')
    Merged_Data = Merged_Data[(Merged_Data['Date'].dt.year >= 1985) & (Merged_Data['Date'].dt.year <= 2019)].drop(['rankdate'], axis=1)
    Merged_Data.sort_values(by='Date', ascending=True, inplace=True)

    from settings import config
    columns_to_drop = config("COLS_TO_DROP_PREP")
    Merged_Data.drop(columns=columns_to_drop, axis=1, inplace=True)

    missing_values = Merged_Data.isna().sum()
    if (missing_values > 0).any():
        print(f"{forecast_period} Missing Values:")
        print(missing_values[missing_values > 0])
    Merged_Data.dropna(axis=0, inplace=True)

    trim_value = config("TRIM_VALUE")
    list_vars_to_trim = config("VARS_TO_TRIM")
    mask = pd.Series([True] * len(Merged_Data), index=Merged_Data.index)
    for column in list_vars_to_trim:
        lower_bound = -trim_value
        upper_bound = trim_value
        mask &= (Merged_Data[column] > lower_bound) & (Merged_Data[column] < upper_bound)
    Merged_Data = Merged_Data[mask]
    return Merged_Data


def train_test_rolling(period, data_frame):
    """
    Rolling-window training and testing for RF and OLS.
    """
    from settings import config
    data_frame = data_frame[(data_frame['Date'] >= '1985-01') & (data_frame['Date'] <= '2019-12')]
    start_train = pd.to_datetime('1985-01', format='%Y-%m').to_period('M')
    print(f"Length total df: {len(data_frame)}")

    y_hat_test_RF = pd.Series(dtype=float)
    y_hat_test_LR = pd.Series(dtype=float)

    length_train = config("ROLLING_TRAIN_LENGTH")
    n_loops = config("ROLLING_N_LOOPS")
    if period == 'A2':
        length_train = config("ROLLING_TRAIN_LENGTH_A2")
        n_loops = config("ROLLING_N_LOOPS_A2")

    for i in tqdm(range(0, n_loops)):
        train_start_date = (start_train.to_timestamp() + pd.DateOffset(months=i)).to_period('M')
        train_end_date = (start_train.to_timestamp() + pd.DateOffset(months=length_train + i)).to_period('M')
        train_data = data_frame[(data_frame['Date'] >= train_start_date) & (data_frame['Date'] <= train_end_date)]
        test_date = (start_train.to_timestamp() + pd.DateOffset(months=length_train + 1 + i)).to_period('M')
        test_data = data_frame[data_frame['Date'] == test_date]

        if len(test_data) != 0:
            y_train = train_data['adj_actual']
            X_train_full = train_data.loc[:, ~train_data.columns.isin(['adj_actual'])]
            X_test_full = test_data.loc[:, ~test_data.columns.isin(['adj_actual'])]
            X_train = X_train_full.drop(['Date', 'permno', 'numest'], axis=1)
            X_test = X_test_full.drop(['Date', 'permno', 'numest'], axis=1)

            scaler = preprocessing.StandardScaler().fit(X_train)
            X_train = scaler.transform(X_train)
            X_test = scaler.transform(X_test)

            forest_model_rf = RandomForestRegressor(
                n_estimators=config("RF_N_ESTIMATORS"),
                max_depth=config("RF_MAX_DEPTH"),
                max_samples=config("RF_MAX_SAMPLES"),
                min_samples_leaf=config("RF_MIN_SAMPLES_LEAF"),
                n_jobs=config("RF_N_JOBS"),
            )
            forest_model_rf.fit(X_train, y_train)
            pred_rf = forest_model_rf.predict(X_test)
            y_hat_test_RF = pd.concat([y_hat_test_RF, pd.Series(pred_rf)], ignore_index=True)

            X_train_LR = sm.add_constant(X_train)
            model_LR = sm.OLS(y_train, X_train_LR)
            olsres = model_LR.fit()
            X_test_LR = sm.add_constant(X_test, has_constant='add')
            y_hat_LR_temp = pd.Series(olsres.predict(X_test_LR))
            y_hat_test_LR = pd.concat([y_hat_test_LR, y_hat_LR_temp], ignore_index=True)

    if period == 'A2':
        result_df = data_frame[(data_frame['Date'] >= '1987-01') & (data_frame['Date'] <= '2019-12')].copy()
    else:
        result_df = data_frame[(data_frame['Date'] >= '1986-01') & (data_frame['Date'] <= '2019-12')].copy()
    result_df = result_df.sort_values('Date').reset_index(drop=True)
    n_test = len(y_hat_test_RF)
    result_df = result_df.head(n_test).copy()
    result_df['predicted_adj_actual'] = y_hat_test_RF.values
    result_df['predicted_adj_actual_LR'] = y_hat_test_LR.values
    result_df['bias_AF_ML'] = (result_df.meanest - result_df.predicted_adj_actual) / result_df.price
    return result_df
