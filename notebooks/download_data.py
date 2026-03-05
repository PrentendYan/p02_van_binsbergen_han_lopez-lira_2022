
import wrds
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Connect to WRDS
print("Connecting to WRDS...")
wrds_username = os.getenv("WRDS_USERNAME")
# WRDS uses psycopg2, which respects PGPASSWORD environment variable
if os.getenv("WRDS_PASSWORD"):
    os.environ['PGPASSWORD'] = os.getenv("WRDS_PASSWORD")

db = wrds.Connection(wrds_username=wrds_username)
print("Connected to WRDS.")

# Function to fetch CRSP data: returns and prices of stocks
def fetch_crsp_data():
    print("Fetching CRSP data...")

    """
    Fetches stock returns and prices from CRSP.
    
    Data includes:
    - Common stocks (share code 10 and 11) in NYSE, AMEX, and NASDAQ (exchange code 1, 2, and 3)
    - Adjusts prices to remove negative signs indicating bid/ask averages.
    
    Returns:
        pd.DataFrame: Dataframe containing the CRSP data.
    """
    crsp = db.raw_sql("""
        SELECT a.permno, a.cusip, a.date, a.cfacshr, ABS(a.prc) AS price, b.shrcd, b.exchcd, a.ret
        FROM crsp.dsf AS a
        LEFT JOIN crsp.msenames AS b
        ON a.permno = b.permno
        AND b.namedt <= a.date
        AND a.date <= b.nameendt
        WHERE a.cusip != ''
        AND a.date >= '1985-01-01'
        AND (b.exchcd IN ('1', '2', '3'))
        AND (b.shrcd IN ('10', '11'))
    """)
    crsp.to_csv('data/crsp.csv')
    print("CRSP data saved to data/crsp.csv")
    return crsp

# Function to fetch IBES summary data: analysts' average estimates and actual values
def fetch_ibes_summary():
    print("Fetching IBES summary data...")
    """
    Fetches analyst estimates and actual values from IBES.
    
    Data includes:
    - Ticker, CUSIP, company name, forecast period end dates, statistical periods, mean estimates, forecast period indicators,
      number of estimates, actual values, and announcement dates of actual values.
    
    Returns:
        pd.DataFrame: Dataframe containing the IBES summary data.
    """
    ibes_summary = db.raw_sql("""
        SELECT ticker, cusip, cname, fpedats, statpers, meanest, fpi, numest, actual, anndats_act
        FROM ibes.statsum_epsus
        WHERE cusip != ''
        AND usfirm = '1'
        AND fpedats >= '1985-01-01'
        AND (fpi IN ('1', '2', '6', '7', '8'))
    """)
    ibes_summary.to_csv('data/ibes_summary.csv')
    print("IBES summary data saved to data/ibes_summary.csv")
    return ibes_summary

# Function to fetch financial ratios data
def fetch_financial_ratios():
    print("Fetching financial ratios...")
    """
    Fetches financial ratios from WRDS.
    
    Data includes:
    - Various financial ratios for firms, filtered by CUSIP and public date.
    
    Returns:
        pd.DataFrame: Dataframe containing the financial ratios data.
    """
    finratio = db.raw_sql("""
        SELECT *
        FROM wrdsapps_finratio_ibes.firm_ratio_ibes
        WHERE cusip != ''
        AND public_date >= '1985-01-01'
    """)
    finratio.to_csv('data/finratio.csv')
    print("Financial ratios saved to data/finratio.csv")
    return finratio

# Function to fetch data from Federal Reserve Bank of Philadelphia
def fetch_fed_data(url, filename):
    print(f"Fetching Fed data from {url}...")
    """
    Fetches economic data from the Federal Reserve Bank of Philadelphia.
    
    Parameters:
        url (str): URL of the Excel file to download.
        filename (str): Local filename to save the CSV data.
    
    Returns:
        pd.DataFrame: Dataframe containing the downloaded data.
    """
    df = pd.read_excel(url)
    df.to_csv(f'data/{filename}')
    print(f"Data saved to data/{filename}")
    return df

# Run the functions and save the data to CSV files
crsp_data = fetch_crsp_data()
ibes_summary_data = fetch_ibes_summary()
financial_ratios_data = fetch_financial_ratios()

# Fetch and save Real GDP data
real_gdp_url = "https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/real-time-data/data-files/xlsx/routputmvqd.xlsx?la=en&hash=403C8B9FD72B33F83C1EE5C59D015C86"
fetch_fed_data(real_gdp_url, 'real_GDP_FED.csv')

# Fetch and save IPT data
ipt_url = "https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/real-time-data/data-files/xlsx/iptmvmd.xlsx?la=en&hash=E53F4C735866E2366E50511D5C9CCADE"
fetch_fed_data(ipt_url, 'IPT_FED.csv')

# Fetch and save Real Personal Consumption data
personal_consumption_url = "https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/real-time-data/data-files/xlsx/rconmvqd.xlsx?la=en&hash=9F7B44DB227E6A620629495229CD93BB"
fetch_fed_data(personal_consumption_url, 'real_personal_consumption_FED.csv')

# Fetch and save Unemployment rate data
unemployment_url = "https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/real-time-data/data-files/xlsx/rucqvmd.xlsx?la=en&hash=FF1D4C67E144D916C1986A8EEDC4B42A"
fetch_fed_data(unemployment_url, 'Unemployment_FED.csv')
