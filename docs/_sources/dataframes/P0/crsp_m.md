# Dataframe: `P0:crsp_m` - CRSP Monthly Stock Data

# CRSP Monthly Stock Data

This dataset contains monthly stock returns and other data from CRSP.



## DataFrame Glimpse

```
Rows: 2131025
Columns: 16
$ permno             <i64> 93436
$ permco             <i64> 53453
$ date      <datetime[ns]> 2019-12-31 00:00:00
$ ret                <f64> 0.267897
$ retx               <f64> 0.267897
$ shrout             <f64> 181062.0
$ prc                <f64> 418.32999
$ cfacshr            <f64> 15.0
$ shrcd              <i64> 11
$ exchcd             <i64> 3
$ siccd              <i64> 9999
$ ncusip             <str> '88160R10'
$ dlstcd             <i64> null
$ dlret              <f64> 0.0
$ YearMonth <datetime[ns]> 2019-12-31 00:00:00
$ retadj             <f64> 0.267897


```

## Dataframe Manifest

| Dataframe Name                 | CRSP Monthly Stock Data                                                   |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [crsp_m](../dataframes/P0/crsp_m.md)                                       |
| Data Sources                   | WRDS                                        |
| Data Providers                 | CRSP                                      |
| Links to Providers             | https://wrds-www.wharton.upenn.edu/                             |
| Topic Tags                     | Stock, Returns                                          |
| Type of Data Access            | S,u,b,s,c,r,i,p,t,i,o,n                                  |
| How is data pulled?            | WRDS API via Python                                                    |
| Data available up to (min)     | 2019-12-31 00:00:00                                                             |
| Data available up to (max)     | 2019-12-31 00:00:00                                                             |
| Dataframe Path                 | /Users/yandong/Documents/GitHub/p02_van_binsbergen_han_lopez-lira_2022/p02_van_binsbergen_han_lopez_lira_2022/_data/WRDS/crsp_m.parquet                                                   |


**Linked Charts:**


- [P0:explore_crsp](../../charts/P0.explore_crsp.md)



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


