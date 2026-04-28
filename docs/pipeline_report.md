# Flight Price Analysis Pipeline Report

## 1. Project Overview

This project builds a data pipeline for the Bangladesh flight price dataset. The pipeline loads raw flight price data from a CSV file, stores it first in MySQL, moves it to PostgreSQL, transforms it with dbt, checks data quality, and publishes final reporting tables.

Apache Airflow is used to run the pipeline from start to finish. PostgreSQL is used as the data warehouse because dbt transformation is done there.

## 2. Pipeline Architecture

The pipeline has these main parts:

- CSV file: the raw flight price dataset.
- Airflow: the orchestration tool that controls all steps.
- MySQL: the raw landing database.
- PostgreSQL: the data warehouse used for dbt transformations and final reporting tables.
- dbt: the transformation and testing tool.
- Reporting schema: the final place for clean tables and KPI tables.

### Mermaid Architecture Code

![Flight price pipeline architecture](assets/pipeline_architecture.png)


## 3. Execution Flow

The main DAG is `flight_price_analysis_pipeline`. It is manually triggered because `schedule=None`. The pipeline does not run historical backfills because `catchup=False`.

Execution order:

1. Validate the CSV file.
2. Create the raw MySQL table.
3. Load the CSV data into MySQL.
4. Validate that MySQL has the same row count as the CSV file.
5. Copy the raw data from MySQL into PostgreSQL.
6. Run dbt seed to load airport reference data.
7. Run dbt models to clean and transform the data.
8. Run dbt tests.
9. Publish the transformed tables to the PostgreSQL reporting schema.

## 4. Airflow DAG and Task Description

### `flight_price_analysis_pipeline`

This is the main pipeline DAG. It controls the full ELT process for flight price analysis.

### `validate_csv_file`

This task checks that the CSV file exists and that it has the expected columns. It also counts the number of data rows. If the file is missing, empty, or has wrong columns, the pipeline stops.

### `create_mysql_raw_table`

This task creates the MySQL raw table named `raw_flight_prices`. It drops the old table first, then creates a fresh table with columns that match the CSV file.

### `load_csv_to_mysql`

This task loads the CSV file into MySQL using `LOAD DATA LOCAL INFILE`. The data is loaded into the raw table without business transformation.

### `validate_mysql_load`

This task compares the number of CSV rows with the number of rows loaded into MySQL. If the counts are different, the pipeline stops.

### `load_data_to_postgres_from_mysql`

This task reads rows from MySQL and inserts them into PostgreSQL table `public.raw_flight_prices`. It also changes column names into simple snake_case names. The data is copied in batches of 5,000 rows.

### `run_dbt_seed`

This task runs `dbt seed`. It loads the airport reference file `bd_airport.csv` into PostgreSQL. This reference data is used to check if source and destination airport codes are valid.

### `run_dbt_models`

This task runs `dbt run`. It builds the staging model, data quality audit model, clean fact table, and KPI tables in the PostgreSQL `analytics` schema.

### `run_dbt_test`

This task runs `dbt test`. It checks important source fields such as airline, source, destination, base fare, tax surcharge, and total fare for null values.

### `publish_transformed_data_to_postgres`

This task publishes dbt output tables from the `analytics` schema to the `reporting` schema. It first creates temporary load tables, checks that the clean flight price table is not empty, and then replaces the final reporting tables.

### `welcome_dag`

This is a simple sample DAG. It only prints `Welcome to Airflow!`. It is not part of the flight price pipeline.

## 5. dbt Model Description

### `stg_flight_prices`

This staging model reads from `public.raw_flight_prices`. It cleans the raw data by:

- Trimming text fields.
- Converting airport codes to uppercase.
- Renaming `class` to `flight_class`.
- Creating a `raw_row_number`.
- Calculating `calculated_total_fare_bdt` as base fare plus tax and surcharge.
- Creating `is_peak_season` for Eid and winter holidays.
- Replacing incorrect source total fare values with the calculated total fare.

### `bd_flight_fare_row_audit`

This quality model finds bad or corrected rows. It checks for:

- Missing airline, source, destination, or fare values.
- Negative fare values.
- Invalid source or destination airport codes.
- Total fare values that do not match base fare plus tax and surcharge.

### `flight_prices`

This is the clean fact table. It removes rows found in the audit table and keeps only clean flight price records. It also creates a route field using `source || ' -> ' || destination`.

### KPI Models

The mart models create final KPI tables for reporting:

- `average_fare_by_airline`
- `booking_count_by_airline`
- `seasonal_fare_variation`
- `most_popular_routes`

## 6. KPI Definitions and Computation Logic

### Average Fare by Airline

Table: `average_fare_by_airline`

Definition: shows the average total fare for each airline.

Logic:

```sql
round(avg(total_fare_bdt), 2) as average_total_fare_bdt
```

The table also includes booking count for each airline.

### Booking Count by Airline

Table: `booking_count_by_airline`

Definition: shows how many flight price records exist for each airline.

Logic:

```sql
count(*) as booking_count
```

The results are grouped by airline and ordered from the highest booking count to the lowest.

### Seasonal Fare Variation

Table: `seasonal_fare_variation`

Definition: compares average fare between peak season and non-peak season.

Logic:

```sql
case
    when is_peak_season then 'Peak'
    else 'Non-Peak'
end as season_group
```

Then the model calculates:

```sql
round(avg(total_fare_bdt), 2) as average_total_fare_bdt,
count(*) as booking_count
```

Peak season means the seasonality value is `Eid` or `Winter Holidays`.

### Most Popular Routes

Table: `most_popular_routes`

Definition: shows the routes with the highest number of records.

Logic:

```sql
source || ' -> ' || destination as route,
count(*) as booking_count,
round(avg(total_fare_bdt), 2) as average_total_fare_bdt
```

The results are grouped by source and destination, then ordered by booking count.

## 7. Challenge Encountered and Resolution

The main challenge was that there was no suitable dbt connector setup for transforming data directly in MySQL in this project. Because of this, MySQL is used only as the raw landing database.

To solve the issue, the pipeline copies raw data from MySQL into PostgreSQL. PostgreSQL is then used as the data warehouse. dbt connects to PostgreSQL and runs all transformations there. This makes the pipeline easier to manage because PostgreSQL works well with dbt models, seeds, and tests.

Final approach:

- MySQL stores the raw CSV load.
- Airflow moves raw data from MySQL to PostgreSQL.
- PostgreSQL acts as the data warehouse.
- dbt transforms and tests the data in PostgreSQL.
- Final tables are published to the PostgreSQL `reporting` schema.

## 8. Final Output Tables

The pipeline publishes these tables to the `reporting` schema:

- `flight_prices`
- `average_fare_by_airline`
- `booking_count_by_airline`
- `seasonal_fare_variation`
- `most_popular_routes`
- `bd_flight_fare_row_audit`

These tables can be used for reporting, dashboarding, and flight fare analysis.
