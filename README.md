# Module 4: Analytics Engineering - Homework  

## Question 1: Understanding dbt model resolution  
Based on the sources.yaml file:  
```yaml
version: 2

sources:
  - name: raw_nyc_tripdata
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"
    schema:   "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"
    tables:
      - name: ext_green_taxi
      - name: ext_yellow_taxi
```
this .sql model:  
```dbt
select * 
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}
```
compiles to:  
- `select * from dtc_zoomcamp_2025.raw_nyc_tripdata.ext_green_taxi`
<img src="https://github.com/VMynenko/DE-Zoomcamp-Homework-4/blob/main/dbt_142321.png" alt="green_taxi" width="400" />

## Question 2: dbt Variables & Dynamic Models   
```dbt
select *
from {{ ref('fact_taxi_trips') }}
where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY
```
What would you change to accomplish that in a such way that command line arguments takes precedence over ENV_VARs, which takes precedence over DEFAULT value?  
**Answer**: The following option first checks the command line arguments for the presence of “days_back”. If it is not found, the environment variable “DAYS_BACK” is checked. If it is not found, the value “30” is used.  
- `Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`
