from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_welcome():
    print("Welcome to Airflow!")
    return "Airflow is working successfully!"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1), # Start date in the past
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'welcome_dag',
    default_args=default_args,
    description='A simple welcome DAG',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example', 'welcome'],
) as dag:

    # Task 1: Print a welcome message using Python
    print_welcome_task = PythonOperator(
        task_id='print_welcome',
        python_callable=print_welcome,
    )

    # Task 2: Print the current date using Bash
    print_date_task = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # Set task dependencies
    print_welcome_task >> print_date_task