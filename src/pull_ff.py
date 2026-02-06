import pandas as pd
import requests
import zipfile
import io
from io import StringIO
from settings import config
from pathlib import Path
from misc_tools import write_parquet

START_DATE = config("START_DATE")
END_DATE = config("END_DATE")
DATA_DIR = Path(config("DATA_DIR"))
from pathlib import Path


def pull_ff(
    n_factors: int = 3,
    start_date: str | None = None,
    end_date: str | None = None,
    as_decimal: bool = True,
) -> pd.DataFrame:

    if n_factors == 3:
        url = (
            "https://mba.tuck.dartmouth.edu/pages/faculty/"
            "ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip"
        )
        csv_name = "F-F_Research_Data_Factors_daily.csv"
        skiprows = 4

    elif n_factors == 5:
        url = (
            "https://mba.tuck.dartmouth.edu/pages/faculty/"
            "ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip"
        )
        csv_name = "F-F_Research_Data_5_Factors_2x3_daily.csv"
        skiprows = 3

    else:
        raise ValueError("n_factors must be 3 or 5")

    r = requests.get(url)
    r.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv_data = z.open(csv_name).read().decode("utf-8")

    df = pd.read_csv(StringIO(csv_data), skiprows=skiprows)

    df.rename(columns={df.columns[0]: "Date"}, inplace=True)

    df = df[df["Date"].astype(str).str.len() == 8]

    df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d")
    df.set_index("Date", inplace=True)
    df.sort_index(inplace=True)

    if as_decimal:
        df = df / 100.0

    if start_date is not None:
        df = df.loc[pd.to_datetime(start_date):]

    if end_date is not None:
        df = df.loc[:pd.to_datetime(end_date)]

    return df

def load_ff(data_dir=DATA_DIR):
    path = Path(data_dir) / "FF_FACTORS.parquet"
    return pd.read_parquet(path)

if __name__ == "__main__":
    ff = pull_ff(start_date=START_DATE, end_date=END_DATE)
    write_parquet(ff, DATA_DIR / "FF_FACTORS.parquet")