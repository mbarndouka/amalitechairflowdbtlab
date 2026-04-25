from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="welcome_dag",
    description="A simple welcome DAG",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["welcome"],
)
def welcome_dag():
    @task
    def show_welcome() -> str:
        message = "Welcome to Airflow!"
        print(message)
        return message

    show_welcome()


welcome_dag()
