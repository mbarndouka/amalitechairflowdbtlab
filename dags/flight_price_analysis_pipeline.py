from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

CSV_PATH = "/opt/airflow/dags/data/Flight_Price_Dataset_of_Bangladesh.csv"
MYSQL_CONN_ID = "mysql_raw"
POSTGRES_CONN_ID = "postgres_target"

MYSQL_DB = "airflow"
MYSQL_RAW_TABLE = "raw_flight_prices"
POSTGRES_SCHEMA = "public"
POSTGRES_TARGET_TABLE = "raw_flight_prices"
LOAD_BATCH_SIZE = 5_000


@dag(
    dag_id="flight_price_analysis_pipeline",
    start_date=datetime(2026, 4, 26),
    schedule=None,
    catchup=False,
    tags=["flight-price", "mysql"]
)
def flight_price_analysis_pipeline():
    @task
    def validate_csv_file() -> int:
        path = Path(CSV_PATH)

        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")

        expect_columns = [
            "Airline",
            "Source",
            "Source Name",
            "Destination",
            "Destination Name",
            "Departure Date & Time",
            "Arrival Date & Time",
            "Duration (hrs)",
            "Stopovers",
            "Aircraft Type",
            "Class",
            "Booking Source",
            "Base Fare (BDT)",
            "Tax & Surcharge (BDT)",
            "Total Fare (BDT)",
            "Seasonality",
            "Days Before Departure",
        ]

        with path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.reader(file)
            header = next(reader)

            if header != expect_columns:
                raise ValueError(f"CSV file has unexpected columns. Expected: {expect_columns}, Found: {header}")

            row_count = sum(1 for _ in reader)
            if row_count == 0:
                raise ValueError("CSV file has not data rows")

            return row_count

    @task
    def create_mysql_raw_table() -> None:
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

        hook.run(
            f"""
            DROP TABLE IF EXISTS {MYSQL_DB}.{MYSQL_RAW_TABLE};
            CREATE TABLE {MYSQL_DB}.{MYSQL_RAW_TABLE} (
                `Airline` VARCHAR(255),
                `Source` VARCHAR(10),
                `Source Name` VARCHAR(255),
                `Destination` VARCHAR(10),
                `Destination Name` VARCHAR(255),
                `Departure Date & Time` DATETIME,
                `Arrival Date & Time` DATETIME,
                `Duration (hrs)` DECIMAL(12,6),
                `Stopovers` VARCHAR(50),
                `Aircraft Type` VARCHAR(100),
                `Class` VARCHAR(100),
                `Booking Source` VARCHAR(100),
                `Base Fare (BDT)` DECIMAL(14,6),
                `Tax & Surcharge (BDT)` DECIMAL(14,6),
                `Total Fare (BDT)` DECIMAL(14,6),
                `Seasonality` VARCHAR(100),
                `Days Before Departure` INT
            );
            """
        )

    @task
    def load_csv_to_mysql() -> None:
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID, local_infile=True)

        hook.run(
            f"""
            TRUNCATE TABLE {MYSQL_DB}.{MYSQL_RAW_TABLE};
            """
        )

        hook.run(
            f"""
            LOAD DATA LOCAL INFILE '{CSV_PATH}'
            INTO TABLE {MYSQL_DB}.{MYSQL_RAW_TABLE}
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 ROWS;
            """
        )

    @task
    def validate_mysql_load(expected_count: int) -> None:
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

        result = hook.get_first(
            f"""
            SELECT COUNT(*) FROM {MYSQL_DB}.{MYSQL_RAW_TABLE};
            """
        )

        actual_count = result[0] if result else 0

        if actual_count != expected_count:
            raise ValueError(
                f"Data load validation failed. Expected {expected_count} rows, "
                f"but found {actual_count} rows in MySQL."
            )

    @task
    def load_data_to_postgres_from_mysql() -> None:
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        postgres_hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {POSTGRES_SCHEMA}.{POSTGRES_TARGET_TABLE} (
                airline VARCHAR(255),
                source VARCHAR(10),
                source_name VARCHAR(255),
                destination VARCHAR(10),
                destination_name VARCHAR(255),
                departure_date_time TIMESTAMP,
                arrival_date_time TIMESTAMP,
                duration_hrs NUMERIC(12,6),
                stopovers VARCHAR(50),
                aircraft_type VARCHAR(100),
                class VARCHAR(100),
                booking_source VARCHAR(100),
                base_fare_bdt NUMERIC(14,6),
                tax_surcharge_bdt NUMERIC(14,6),
                total_fare_bdt NUMERIC(14,6),
                seasonality VARCHAR(100),
                days_before_departure INTEGER
            );
            TRUNCATE TABLE {POSTGRES_SCHEMA}.{POSTGRES_TARGET_TABLE};
            """
        )

        source_sql = f"""
            SELECT
                `Airline`,
                `Source`,
                `Source Name`,
                `Destination`,
                `Destination Name`,
                `Departure Date & Time`,
                `Arrival Date & Time`,
                `Duration (hrs)`,
                `Stopovers`,
                `Aircraft Type`,
                `Class`,
                `Booking Source`,
                `Base Fare (BDT)`,
                `Tax & Surcharge (BDT)`,
                `Total Fare (BDT)`,
                `Seasonality`,
                `Days Before Departure`
            FROM {MYSQL_DB}.{MYSQL_RAW_TABLE};
        """

        target_fields = [
            "airline",
            "source",
            "source_name",
            "destination",
            "destination_name",
            "departure_date_time",
            "arrival_date_time",
            "duration_hrs",
            "stopovers",
            "aircraft_type",
            "class",
            "booking_source",
            "base_fare_bdt",
            "tax_surcharge_bdt",
            "total_fare_bdt",
            "seasonality",
            "days_before_departure",
        ]

        mysql_conn = mysql_hook.get_conn()
        cursor = mysql_conn.cursor()
        try:
            cursor.execute(source_sql)
            while rows := cursor.fetchmany(LOAD_BATCH_SIZE):
                postgres_hook.insert_rows(
                    table=f"{POSTGRES_SCHEMA}.{POSTGRES_TARGET_TABLE}",
                    rows=rows,
                    target_fields=target_fields,
                    commit_every=LOAD_BATCH_SIZE,
                )
        finally:
            cursor.close()
            mysql_conn.close()

    csv_count = validate_csv_file()
    create_raw = create_mysql_raw_table()
    load_raw = load_csv_to_mysql()
    validate_raw = validate_mysql_load(csv_count)
    load_postgres = load_data_to_postgres_from_mysql()

    csv_count >> create_raw >> load_raw >> validate_raw >> load_postgres

flight_price_analysis_pipeline()
