from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

@dag(
    dag_id="load_flight_data_to_mysql",
    start_date=datetime(2026, 4, 26),
    schedule=None,
    catchup=False,
    tags=["mysql","csv","raw"],
)
def load_flight_data_to_mysql():
    @task
    def create_table():
        hook = MySqlHook(mysql_conn_id="mysql_raw")

        hook.run(
            f"""
                CREATE TABLE IF NOT EXISTS airflow.raw_flight_prices(
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
    def load_csv():
        hook = MySqlHook(mysql_conn_id="mysql_raw", local_infile=True)
        hook.run(
             f"""
                 LOAD DATA LOCAL INFILE '/opt/airflow/dags/data/Flight_Price_Dataset_of_Bangladesh.csv'
                 INTO TABLE airflow.raw_flight_prices
                 FIELDS TERMINATED BY ',' 
                 ENCLOSED BY '"'
                 LINES TERMINATED BY '\n'
                 IGNORE 1 ROWS;
             """
        )

    create_table() >> load_csv()

load_flight_data_to_mysql()