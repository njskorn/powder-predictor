from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta  # Add this import

# global args
# if any task fails, try after two minutes
default_args = {
    'owner': 'nettle',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='hello_airflow',
    default_args=default_args,  # Don't forget to use them!
    description='My first Airflow DAG',
    schedule=None,
    start_date=datetime(2025, 1, 10),
    catchup=False,
    tags=['tutorial'],
) as dag:

    def print_hello():
        print("Hello Airflow!")

    def print_goodbye():
        print("Goodbye Airflow!")

    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
    )

    system_info_task = BashOperator(
        task_id='system_info_task',
        bash_command='uname -a',
    )

    goodbye_task = PythonOperator(
        task_id='goodbye_task',
        python_callable=print_goodbye,
    )

    hello_task >> system_info_task >> goodbye_task