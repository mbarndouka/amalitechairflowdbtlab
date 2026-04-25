# Flight Price ELT Pipeline

This project is an ELT pipeline for the Bangladesh flight price dataset. The intended flow is:

1. Load the raw CSV data into MySQL.
2. Clean and transform the raw data with dbt.
3. Load the cleaned models into PostgreSQL.
4. Orchestrate the workflow with Apache Airflow.

## Project Structure

```text
.
├── Flight_Price_Dataset_of_Bangladesh.csv  # Raw flight price dataset
├── dags/                                   # Airflow DAGs
├── docker-compose.yaml                     # Airflow, MySQL, PostgreSQL, and Redis services
├── .env.example                            # Environment variable template
└── README.md
```

## Services

The Docker Compose setup includes:

- `mysql`: raw landing database for the CSV data
- `postgres`: target database for Airflow metadata and cleaned output tables
- `redis`: Celery broker for Airflow
- `airflow-apiserver`: Airflow web API and UI service
- `airflow-scheduler`: schedules DAG runs
- `airflow-worker`: executes Airflow tasks
- `airflow-dag-processor`: parses DAG files
- `airflow-triggerer`: handles deferred Airflow tasks
- `flower`: optional Celery monitoring service

## Prerequisites

- Docker
- Docker Compose
- Python, if you want to run dbt locally outside Docker
- dbt with MySQL and PostgreSQL adapters, if running transformations locally

Example local dbt installation:

```bash
pip install dbt-core dbt-mysql dbt-postgres
```

## Environment Setup

Create a local `.env` file from the example:

```bash
cp .env.example .env
```

Update the placeholder passwords and secrets in `.env`, especially:

- `POSTGRES_PASSWORD`
- `MYSQL_ROOT_PASSWORD`
- `MYSQL_PASSWORD`
- `AIRFLOW__CORE__FERNET_KEY`
- `AIRFLOW__API_AUTH__JWT_SECRET`
- `_AIRFLOW_WWW_USER_PASSWORD`

You can generate a Fernet key with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Start the Stack

Initialize and start the services:

```bash
docker compose up airflow-init
docker compose up -d
```

Airflow will be available at:

```text
http://localhost:8080
```

The default username comes from `_AIRFLOW_WWW_USER_USERNAME` in `.env`.

## Data Pipeline

### 1. Load Raw Data to MySQL

The raw source file is:

```text
Flight_Price_Dataset_of_Bangladesh.csv
```

Recommended raw table name:

```text
raw_flight_prices
```

The dataset includes columns such as:

- `Airline`
- `Source`
- `Source Name`
- `Destination`
- `Destination Name`
- `Departure Date & Time`
- `Arrival Date & Time`
- `Duration (hrs)`
- `Stopovers`
- `Aircraft Type`
- `Class`
- `Booking Source`
- `Base Fare (BDT)`
- `Tax & Surcharge (BDT)`
- `Total Fare (BDT)`
- `Seasonality`
- `Days Before Departure`

The MySQL service is exposed on:

```text
localhost:3306
```

Connection values are configured in `.env`:

- database: `MYSQL_DATABASE`
- user: `MYSQL_USER`
- password: `MYSQL_PASSWORD`

### 2. Clean Data with dbt

dbt should read from the MySQL raw table and build cleaned models. Typical cleaning steps include:

- Rename columns to snake_case.
- Cast date and time fields to timestamps.
- Cast fare and duration fields to numeric types.
- Standardize text values such as airline, class, stopovers, and booking source.
- Validate required fields.
- Remove duplicate records where appropriate.

Suggested model layers:

```text
dbt/
├── models/
│   ├── staging/
│   │   └── stg_flight_prices.sql
│   └── marts/
│       └── flight_prices_cleaned.sql
└── profiles.yml
```

### 3. Load Cleaned Data to PostgreSQL

The cleaned dbt models should be materialized into PostgreSQL. The PostgreSQL service is exposed on:

```text
localhost:5432
```

Connection values are configured in `.env`:

- database: `POSTGRES_DB`
- user: `POSTGRES_USER`
- password: `POSTGRES_PASSWORD`

Recommended target table name:

```text
flight_prices_cleaned
```

### 4. Orchestrate with Airflow

Airflow should orchestrate the full workflow:

1. Check that MySQL and PostgreSQL are available.
2. Load the CSV file into MySQL.
3. Run dbt transformations.
4. Validate the cleaned PostgreSQL tables.

The current DAG in `dags/welcome.py` is a starter DAG. Replace or extend it with a DAG for the flight price pipeline.

## Useful Commands

Start all services:

```bash
docker compose up -d
```

Stop all services:

```bash
docker compose down
```

View running containers:

```bash
docker compose ps
```

View Airflow logs:

```bash
docker compose logs -f airflow-scheduler
```

Open a MySQL shell:

```bash
docker compose exec mysql mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE"
```

Open a PostgreSQL shell:

```bash
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

## Next Steps

- Add a CSV-to-MySQL load script or Airflow task.
- Add a dbt project for staging and cleaned models.
- Add Airflow tasks to run the raw load and dbt commands.
- Add dbt tests for required fields, accepted values, and uniqueness checks.
