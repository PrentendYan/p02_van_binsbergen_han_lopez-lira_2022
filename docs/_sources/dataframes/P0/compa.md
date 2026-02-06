# Dataframe: `P0:compa` - Compustat Annual

# Compustat Annual

This dataset contains annual financial statement data from Compustat.



## DataFrame Glimpse

```
Rows: 58709
Columns: 48
$ gvkey                <str> '369350'
$ datadate    <datetime[ns]> 2024-12-31 00:00:00
$ fyear                <i64> 2024
$ csho                 <f64> null
$ at                   <f64> 5715.961
$ pstkl                <f64> 0.0
$ txditc               <f64> 308.523
$ pstkrv               <f64> 0.0
$ seq                  <f64> 2876.098
$ pstk                 <f64> 0.0
$ ppegt                <f64> 5678.69
$ invt                 <f64> 952.488
$ lt                   <f64> 2816.05
$ sich                 <i64> 2024
$ ib                   <f64> 599.446
$ oancf                <f64> 1152.303
$ act                  <f64> 1686.524
$ dlc                  <f64> 88.002
$ che                  <f64> 72.472
$ lct                  <f64> 2100.649
$ dvc                  <f64> 0.0
$ epspi                <f64> 0.98
$ epspx                <f64> 0.98
$ ajex                 <f64> 1.0
$ sale                 <f64> 8227.629
$ ao                   <f64> 137.697
$ prcc_f               <f64> null
$ ps                   <f64> 0.0
$ be                   <f64> 3184.621
$ act_ch               <f64> 14.135999999999967
$ dlc_ch               <f64> 7.311999999999998
$ che_ch               <f64> 14.993999999999993
$ lct_ch               <f64> -61.40700000000015
$ acc                  <f64> 67.86100000000013
$ at_l1                <f64> 5828.486
$ at_avg               <f64> 5772.2235
$ ag                   <f64> -0.019306042769940523
$ ppegt_diff           <f64> -68.0010000000002
$ ao_diff              <f64> -32.52600000000001
$ sale_l1              <f64> 8420.521
$ sale_l3              <f64> null
$ sale_l5              <f64> null
$ sg_1y                <f64> -0.022907371170976187
$ sg_3y                <f64> null
$ sg_5y                <f64> null
$ adj_csho             <f64> null
$ adj_csho_l1          <f64> null
$ nsi                  <f64> null


```

## Dataframe Manifest

| Dataframe Name                 | Compustat Annual                                                   |
|--------------------------------|--------------------------------------------------------------------------------------|
| Dataframe ID                   | [compa](../dataframes/P0/compa.md)                                       |
| Data Sources                   | WRDS                                        |
| Data Providers                 | Compustat                                      |
| Links to Providers             | https://wrds-www.wharton.upenn.edu/                             |
| Topic Tags                     | Fundamentals, Accounting                                          |
| Type of Data Access            | S,u,b,s,c,r,i,p,t,i,o,n                                  |
| How is data pulled?            | WRDS API via Python                                                    |
| Data available up to (min)     | 2025-12-31 00:00:00                                                             |
| Data available up to (max)     | 2025-12-31 00:00:00                                                             |
| Dataframe Path                 | /Users/yandong/Documents/GitHub/p02_van_binsbergen_han_lopez-lira_2022/p02_van_binsbergen_han_lopez_lira_2022/_data/compa.parquet                                                   |


**Linked Charts:**


- [P0:explore_compustat](../../charts/P0.explore_compustat.md)



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


