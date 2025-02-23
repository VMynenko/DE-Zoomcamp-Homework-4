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

## Question 3: dbt Data Lineage and Execution
Considering the data lineage below and that taxi_zone_lookup is the only materialization build (from a .csv seed file):  
<img src="https://github.com/VMynenko/DE-Zoomcamp-Homework-4/blob/main/homework_q2.png" alt="green_taxi" width="500" />  
This option is NOT applicable for materializing fct_taxi_monthly_zone_revenue:
- `dbt run --select models/staging/+`

## Question 4: dbt Macros and Jinja  
Correct statements:  
- `Setting a value for DBT_BIGQUERY_TARGET_DATASET env var is mandatory, or it'll fail to compile`
- `When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET`
- `When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET`
- `When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET`

## Question 5: Taxi Quarterly Revenue Growth  
Considering the YoY Growth in 2020, which were the yearly quarters with the best (or less worse) and worst results for green, and yellow?  
```dbt
WITH cte AS (
    SELECT 
        CONCAT(
            EXTRACT(YEAR FROM pickup_datetime),
            '-Q',
            EXTRACT(QUARTER FROM pickup_datetime)
        ) AS year_quarter,
        service_type,
        SUM(total_amount) AS quarterly_revenue
    FROM {{ ref('fact_trips') }}
    GROUP BY 1, 2
),
cte_with_lag AS (
    SELECT 
        year_quarter,
        service_type,
        quarterly_revenue,
        LAG(quarterly_revenue) OVER (
            PARTITION BY service_type 
            ORDER BY year_quarter
        ) AS prev_year_revenue
    FROM cte
)
SELECT 
    year_quarter,
    service_type,
    quarterly_revenue,
    prev_year_revenue,
    CASE 
        WHEN prev_year_revenue = 0 OR prev_year_revenue IS NULL THEN NULL 
        ELSE ROUND(
            (quarterly_revenue - prev_year_revenue) / prev_year_revenue * 100, 
            2
        )
    END AS yoy_growth_percentage
FROM cte_with_lag
```
- `green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}`

## Question 6: P97/P95/P90 Taxi Monthly Fare  
Now, what are the values of p97, p95, p90 for Green Taxi and Yellow Taxi, in April 2020?    
```dbt
WITH filtered_trips AS (
    SELECT 
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount
    FROM {{ ref('fact_trips') }}
    WHERE 
        fare_amount > 0 
        AND trip_distance > 0 
        AND payment_type_description IN ('Cash', 'Credit Card')
),
percentile_calculation AS (
    SELECT 
        service_type,
        CONCAT(
            year,
            "-",
            month
        ) AS year_month,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(97)] AS fare_p97,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(95)] AS fare_p95,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(90)] AS fare_p90
    FROM filtered_trips
    GROUP BY service_type, year, month
)
SELECT 
    service_type,
    year_month,
    fare_p97,
    fare_p95,
    fare_p90
FROM percentile_calculation
```
- `green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}`

## Question 7: Top #Nth longest P90 travel time Location for FHV  
For the Trips that respectively started from Newark Airport, SoHo, and Yorkville East, in November 2019, what are dropoff_zones with the 2nd longest p90 trip_duration?    
```dbt
WITH trip_durations AS (
    SELECT 
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        pickup_borough,
        pickup_zone,
        dropoff_borough,
        dropoff_zone,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref('dim_fhv_trips') }}
    WHERE pickup_datetime IS NOT NULL AND dropoff_datetime IS NOT NULL
),
p90_calculation AS (
    SELECT 
        year,
        month,
        pickup_borough,
        pickup_zone,
        dropoff_borough,
        dropoff_zone,
        APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] AS trip_duration_p90
    FROM trip_durations
    GROUP BY year, month, pickup_borough, pickup_zone, dropoff_borough, dropoff_zone
)
SELECT 
    year,
    month,
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    trip_duration_p90
FROM p90_calculation
```
```sql
WITH sample AS (
  SELECT * 
  FROM `de_zoomcamp.fct_fhv_monthly_zone_traveltime_p90` 
  WHERE pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
    AND year = 2019
    AND month = 11
)

SELECT 
  pickup_zone,
  dropoff_zone, 
  trip_duration_p90
FROM sample
QUALIFY ROW_NUMBER() OVER(PARTITION BY pickup_zone ORDER BY trip_duration_p90 DESC) = 2
```
- `LaGuardia Airport, Chinatown, Garment District`
