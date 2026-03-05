"""
Data loading for Man vs Machine pipeline: WRDS (CRSP, IBES, finratio) and Philadelphia Fed.
Outputs go to DATA_DIR. Run after settings (config) so DATA_DIR exists.
"""
import os
import sys
from pathlib import Path

# ensure src is on path and config available
sys.path.insert(0, str(Path(__file__).resolve().parent))
from settings import config

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(config("DATA_DIR"))


def _fetch_fed_data(url: str, filename: str) -> pd.DataFrame:
    df = pd.read_excel(url)
    out = DATA_DIR / filename
    df.to_csv(out)
    print(f"Data saved to {out}")
    return df


def fetch_crsp_data(db):
    """Fetch CRSP stock returns and prices from WRDS."""
    from settings import config
    data_start = config("DATA_START_DATE")
    print("Fetching CRSP data...")
    crsp = db.raw_sql(f"""
        SELECT a.permno, a.cusip, a.date, a.cfacshr, ABS(a.prc) AS price, b.shrcd, b.exchcd, a.ret
        FROM crsp.dsf AS a
        LEFT JOIN crsp.msenames AS b
        ON a.permno = b.permno
        AND b.namedt <= a.date
        AND a.date <= b.nameendt
        WHERE a.cusip != ''
        AND a.date >= '{data_start}'
        AND (b.exchcd IN ('1', '2', '3'))
        AND (b.shrcd IN ('10', '11'))
    """)
    out = DATA_DIR / "crsp.csv"
    crsp.to_csv(out)
    print(f"CRSP data saved to {out}")
    return crsp


def fetch_ibes_summary(db):
    """Fetch IBES summary (analyst estimates and actuals)."""
    from settings import config
    data_start = config("DATA_START_DATE")
    print("Fetching IBES summary data...")
    ibes_summary = db.raw_sql(f"""
        SELECT ticker, cusip, cname, fpedats, statpers, meanest, fpi, numest, actual, anndats_act
        FROM ibes.statsum_epsus
        WHERE cusip != ''
        AND usfirm = '1'
        AND fpedats >= '{data_start}'
        AND (fpi IN ('1', '2', '6', '7', '8'))
    """)
    out = DATA_DIR / "ibes_summary.csv"
    ibes_summary.to_csv(out)
    print(f"IBES summary saved to {out}")
    return ibes_summary


def fetch_financial_ratios(db):
    """Fetch financial ratios from WRDS."""
    from settings import config
    data_start = config("DATA_START_DATE")
    print("Fetching financial ratios...")
    finratio = db.raw_sql(f"""
        SELECT *
        FROM wrdsapps_finratio_ibes.firm_ratio_ibes
        WHERE cusip != ''
        AND public_date >= '{data_start}'
    """)
    out = DATA_DIR / "finratio.csv"
    finratio.to_csv(out)
    print(f"Financial ratios saved to {out}")
    return finratio


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        import wrds
    except ImportError:
        print("WRDS not installed; skipping WRDS pulls. Install with: pip install wrds")
        return

    if os.getenv("WRDS_PASSWORD"):
        os.environ["PGPASSWORD"] = os.getenv("WRDS_PASSWORD")
    wrds_username = os.getenv("WRDS_USERNAME")
    if not wrds_username:
        print("Set WRDS_USERNAME (and WRDS_PASSWORD) in .env to pull WRDS data.")
        return

    print("Connecting to WRDS...")
    db = wrds.Connection(wrds_username=wrds_username)
    print("Connected to WRDS.")

    fetch_crsp_data(db)
    fetch_ibes_summary(db)
    fetch_financial_ratios(db)

    real_gdp_url = "https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/real-time-data/data-files/xlsx/routputmvqd.xlsx?la=en&hash=403C8B9FD72B33F83C1EE5C59D015C86"
    _fetch_fed_data(real_gdp_url, "real_GDP_FED.csv")

    ipt_url = "https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/real-time-data/data-files/xlsx/iptmvmd.xlsx?la=en&hash=E53F4C735866E2366E50511D5C9CCADE"
    _fetch_fed_data(ipt_url, "IPT_FED.csv")

    personal_consumption_url = "https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/real-time-data/data-files/xlsx/rconmvqd.xlsx?la=en&hash=9F7B44DB227E6A620629495229CD93BB"
    _fetch_fed_data(personal_consumption_url, "real_personal_consumption_FED.csv")

    unemployment_url = "https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/real-time-data/data-files/xlsx/rucqvmd.xlsx?la=en&hash=FF1D4C67E144D916C1986A8EEDC4B42A"
    _fetch_fed_data(unemployment_url, "Unemployment_FED.csv")

    print("Pipeline load_data done.")


if __name__ == "__main__":
    main()
