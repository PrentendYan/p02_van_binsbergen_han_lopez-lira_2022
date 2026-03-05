import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import statsmodels.api as sm
import numpy as np
import matplotlib.pyplot as plt
from sklearn import preprocessing
from tqdm.auto import tqdm

# Function to prepare data, except for Unemployment data
def PrepareMacro(Macro_Data,Begin_Year,Begin_Month,Name_col,Name_Var):
    """
    Prepare macroeconomic data by extracting values for specified years and months.
    
    Parameters:
        Macro_Data (DataFrame): DataFrame containing macroeconomic data.
        Begin_Year (int): Starting year for data extraction.
        Begin_Month (int): Starting month for data extraction.
        Name_col (str): Prefix for column names in the DataFrame.
        Name_Var (str): Name of the variable to be extracted.
    
    Returns:
        DataFrame: DataFrame containing dates and values of the specified variable.
    """
    
    #Initilising the data
    month = Begin_Month
    dates = []  #list to store dates
    values = [] #list to store values
    shape = Macro_Data.shape
    n_columns = shape[1]
    col = 1

    #Loop to extract the data
    for i in range(0,n_columns+1):
        year = (Begin_Year + i) % 100
    
        
        while month <= 12:
            if col == n_columns:
                break
            else:
                year_string = str("{:02d}".format(year))
                month_string = str(month)


                col_name = Name_col + year_string + 'M' + month_string
                A = Macro_Data[col_name]
                B = pd.value_counts(A.isna().values)
            
                if A.count() == shape[0]: #Check for when no NaNs
                    values.append(A.iloc[-1])
                else:
                    values.append(A[B.iloc[1]-1])

                if year >= Begin_Year:
                    dates.append('19'+year_string+'-'+month_string)
                else:
                    dates.append('20'+year_string+'-'+month_string)

                month += 1
                col += 1
        month = 1

    #Saving everything in a dataframe
    d = {'Dates':dates, Name_Var:values}
    y = pd.DataFrame(data=d)
    y['Dates'] = pd.to_datetime(y['Dates'], format='%Y-%m')
    
    return y


def read_merge_prepare_data(forecast_period, Macro_Data):
    """
    Read, merge, and prepare data from CSV files.
    
    Parameters:
        forecast_period (str): Forecast period identifier.
        Macro_Data (DataFrame): DataFrame containing macroeconomic data.
    
    Returns:
        DataFrame: Merged and prepared DataFrame.
    """

    # Read CSV files
    forecast_file_path = f"data/processed_data/{forecast_period}.csv"
    df = pd.read_csv(forecast_file_path)


    # Merge DataFrames on Dates and drop unnecessary columns
    df = df.sort_values(by=['permno','statpers'], ascending=True)
    df.statpers = pd.to_datetime(df.statpers)

    Macro_Data = Macro_Data[['GDP_log_return', 'Cons_log_return', 'IPT_log_return', 'Unempl', 'Dates']] 
    Macro_Data= Macro_Data.sort_values(by=['Dates'], ascending=True)
    Macro_Data.Dates = pd.to_datetime(Macro_Data.Dates)


    Merged_Data = pd.merge_asof( df.set_index('statpers').sort_index(),
                        Macro_Data.set_index('Dates',drop=False).sort_index(),
                        left_index=True, 
                        right_index=True,
                        direction='backward').drop(columns=['Dates'])

    Merged_Data = Merged_Data.reset_index()
    Merged_Data.sort_values(by=['permno','rankdate'], ascending=True)

    Merged_Data['Date'] = pd.to_datetime(Merged_Data['rankdate'], format='%Y-%m').dt.to_period('M')
    Merged_Data = Merged_Data[(Merged_Data['Date'].dt.year >= 1985) & (Merged_Data['Date'].dt.year <= 2019)].drop(['rankdate'], axis=1)

    # Preparing the datasets
    Merged_Data.sort_values(by='Date', ascending=True, inplace=True)

    # Columns to drop
    columns_to_drop = ['adjust_factor',  'ticker', 'cusip', 'cname', 'fpedats', 'statpers', 'announcement_actual_eps', 'announcement_past_ep', 'public_date', 'fpi']
    Merged_Data.drop(columns=columns_to_drop, axis=1, inplace=True)

    # Missing values per column
    missing_values = Merged_Data.isna().sum()
    print(f"{forecast_period} Missing Values:")
    print(missing_values[missing_values > 0])

    # Drop rows with missing values 
    Merged_Data.dropna(axis=0, inplace=True)


   

    # Trim outliers from multiple columns, removing rows with outliers in any column
    trim_value = 10
    list_vars_to_trim = ['adj_actual', 'meanest', 'adj_past_eps']

    # Initialize a mask with all True values
    mask = pd.Series([True] * len(Merged_Data),index=Merged_Data.index)

    for column in list_vars_to_trim:
        lower_bound = -trim_value
        upper_bound = trim_value
        # Update the mask to exclude rows where the current column has outliers
        mask &= (Merged_Data[column] > lower_bound) & (Merged_Data[column] < upper_bound)

    # Apply the mask to filter the DataFrame
    Merged_Data = Merged_Data[mask]

    return Merged_Data


def train_test_rolling(period, data_frame):
    """
    Perform training and testing for Random Forest Regressor and Linear Regression models on rolling windows of data.
    
    Parameters:
        period (str): Period identifier.
        data_frame (DataFrame): DataFrame containing data for training and testing.
    
    Returns:
        DataFrame: DataFrame containing predicted values along with real values.
    """

    # Filter data for training and testing based on date (train on 1988, test after; except A2, train on 2 years)
    data_frame = data_frame[(data_frame['Date']>= '1985-01') & (data_frame['Date']<= '2019-12' )]
    start_train = pd.to_datetime('1985-01', format='%Y-%m').to_period('M')

    print(f"Length total df: {len(data_frame)}" )
 
    y_hat_test_RF = pd.Series()
    y_hat_test_LR = pd.Series()

    length_train = 11 # 12 months, hence add 11 to first month 
    n_loops = 408
    if period == 'A2':
        length_train = 23 # 24 months 
        n_loops = 396

    if False:
        df_std = pd.DataFrame()
        df_importances = pd.DataFrame()
    
    #n_loops = 10


    for i in tqdm(range(0, n_loops)): # till 12-2019 420 months; last for loop 420-12= 408, for A2 = 420-24=396
        train_start_date = (start_train.to_timestamp() + pd.DateOffset(months=i)).to_period('M')
        train_end_date = (start_train.to_timestamp() + pd.DateOffset(months=length_train+i)).to_period('M')
        train_data = data_frame[(data_frame['Date'] >= train_start_date) & (data_frame['Date'] <= train_end_date)]

        test_date = (start_train.to_timestamp() + pd.DateOffset(months=length_train + 1 + i)).to_period('M')
        test_data = data_frame[data_frame['Date'] == test_date]
        #print(f'{period} test on {test_date}')

        if len(test_data)!=0:
            
            # Separate predictors and target variable
            y_train = train_data['adj_actual']
            X_train_full = train_data.loc[:, ~train_data.columns.isin(['adj_actual'])]

            X_test_full = test_data.loc[:, ~test_data.columns.isin(['adj_actual'])]

            X_train = X_train_full.drop(['Date', 'permno', 'numest'], axis=1)
            X_test = X_test_full.drop(['Date', 'permno', 'numest'], axis=1)

            #Standardization
            scaler = preprocessing.StandardScaler().fit(X_train)
            feature_names = X_train.columns.tolist()        #for feature importance
            X_train = scaler.transform(X_train)
            X_test = scaler.transform(X_test)
            
            ##########################
            # RandomForestRegressor 
            ##########################
            forest_model_rf = RandomForestRegressor(n_estimators=2000, max_depth=7, max_samples=0.01,  min_samples_leaf=5,  n_jobs=-1) #max_samples=sample_fractions[period]
            #print("Training Random Forest")
            forest_model_rf.fit(X_train, y_train)
            #print("Random Forest training completed")

            y_hat_test_RF = pd.concat([y_hat_test_RF, pd.Series(forest_model_rf.predict(X_test))])
            #print(f' len prediciton RF {len(y_hat_test_RF.values)}')

   
            ##########################
            #Feature Importance -RF
            ##########################
            if False:
                df_importances[i] = forest_model_rf.feature_importances_
                df_std[i] = np.std([tree.feature_importances_ for tree in forest_model_rf.estimators_], axis=0)
            
                if i == n_loops-1:
                
                    #Calculate importances and standard deviation
                    importances = df_importances.mean(axis=1).values
                    df_std = df_std.map(lambda x: x**2)
                    std = np.sqrt(df_std.mean(axis=1).values)
        
                    # Get the indices that would sort the ndarray in descending order
                    sorted_indices = np.argsort(importances)[::-1]
        
                    # Sort the importances array in descending order
                    importances = importances[sorted_indices]
                    std = std[sorted_indices]
        
                    # Apply the same ordering to the list of features
                    #feature_names = X_test_full.columns.tolist()
                    sorted_list_feature_names = [feature_names[j] for j in sorted_indices]
        
                    # Define the number of bars to show
                    bars_to_show = 10
        
                    # Slice the forest_importances Series to include only the first 10 values
                    forest_importances = pd.Series(importances[:bars_to_show], index=sorted_list_feature_names[:bars_to_show])
        
                    # Plotting the histogram with only the first 10 bars
                    fig, ax = plt.subplots()
                    forest_importances.plot.bar(yerr=std[:bars_to_show], ax=ax)  # Remove range(1, bars_to_show) since it starts from 0 by default
                    ax.set_ylabel("Mean decrease in impurity")
                    fig.tight_layout()

                    # save feature importance graph
                    plt.savefig(f'images/{period}_feature_importance.png', dpi=100, format= 'png')  

            
            ##########################
            # Linear Regression
            ##########################  
            X_train_LR = sm.add_constant(X_train)
            model_LR = sm.OLS(y_train, X_train_LR)

           # print("Training Linear Regression")
            olsres = model_LR.fit()
            #print("Linear Regression training completed")

            X_test_LR = sm.add_constant(X_test, has_constant='add')
            y_hat_LR_temp = pd.Series(olsres.predict(X_test_LR))
            y_hat_test_LR = pd.concat([y_hat_test_LR, y_hat_LR_temp ])
           # print(f' len prediciton LR {len(y_hat_test_LR.values)}')
            
    # Dataframe with permno, date, predictor, real value and predicted value
    result_df = pd.DataFrame(data_frame[(data_frame['Date']>= '1986-01') & (data_frame['Date']<= '2019-12') ]) 
    if period == 'A2':
        result_df = pd.DataFrame(data_frame[(data_frame['Date']>= '1987-01') & (data_frame['Date']<= '2019-12') ])
    result_df['predicted_adj_actual'] = y_hat_test_RF.values 
    result_df['predicted_adj_actual_LR'] = y_hat_test_LR.values
    result_df['bias_AF_ML'] = (result_df.meanest - result_df.predicted_adj_actual) / result_df.price 

    return result_df