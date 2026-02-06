# Dataframe: `P0:ibes_forecast` - IBES EPS Forecast

# IBES EPS Forecast

This dataset contains unadjusted EPS forecasts from IBES.



## DataFrame Glimpse

```
Rows: 321115
Columns: 20
$ ticker            <str> 'ZYNX'
$ cusip             <str> '98986M10'
$ oftic             <str> 'ZYXI'
$ cname             <str> 'ZYNEX INC'
$ statpers <datetime[ns]> 2025-12-18 00:00:00
$ measure           <str> 'EPS'
$ fiscalp           <str> 'QTR'
$ fpi               <str> '6'
$ estflag           <str> 'P'
$ curcode           <str> 'USD'
$ numest            <f64> 2.0
$ numup             <f64> 0.0
$ numdown           <f64> 0.0
$ medest            <f64> -0.14
$ meanest           <f64> -0.14
$ stdev             <f64> 0.05
$ highest           <f64> -0.11
$ lowest            <f64> -0.18
$ usfirm            <i64> 1
$ fpedats           <str> '2025-12-31'


```

## Dataframe Manifest

| Dataframe Name                 | IBES EPS Forecast                                                   |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [ibes_forecast](../dataframes/P0/ibes_forecast.md)                                       |
| Data Sources                   | WRDS                                        |
| Data Providers                 | IBES                                      |
| Links to Providers             | https://wrds-www.wharton.upenn.edu/                             |
| Topic Tags                     | Forecasts, Analyst                                          |
| Type of Data Access            | S,u,b,s,c,r,i,p,t,i,o,n                                  |
| How is data pulled?            | WRDS API via Python                                                    |
| Data available up to (min)     | 2025-12-18 00:00:00                                                             |
| Data available up to (max)     | 2025-12-18 00:00:00                                                             |
| Dataframe Path                 | /Users/yandong/Documents/GitHub/p02_van_binsbergen_han_lopez-lira_2022/p02_van_binsbergen_han_lopez_lira_2022/_data/Forecast_EPS_summary_unadjusted_1986_2019.parquet                                                   |


**Linked Charts:**


- [P0:explore_ibes](../../charts/P0.explore_ibes.md)



## Pipeline Manifest

| Pipeline Name                   | Man vs. Machine Learning                       |
|---------------------------------|--------------------------------------------------------|
| Pipeline ID                     | [P0](../index.md)              |
| Lead Pipeline Developer         | Dong Yan, Yilong Lin             |
| Contributors                    | Dong Yan, Yilong Lin           |
| Git Repo URL                    |                         |
| Pipeline Web Page               | <a href="file:///Users/yandong/Documents/GitHub/p02_van_binsbergen_han_lopez-lira_2022/p02_van_binsbergen_han_lopez_lira_2022/docs/index.html">Pipeline Web Page      |
| Date of Last Code Update        | 2026-02-06 15:11:11           |
| OS Compatibility                |  |
| Linked Dataframes               |  [P0:crsp_m](../dataframes/P0/crsp_m.md)<br>  [P0:compa](../dataframes/P0/compa.md)<br>  [P0:ibes_forecast](../dataframes/P0/ibes_forecast.md)<br>  [P0:ff_factors](../dataframes/P0/ff_factors.md)<br>  [P0:fred](../dataframes/P0/fred.md)<br>  |


