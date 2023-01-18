from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default Arguments for the DAG
default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create a DAG instance
dag = DAG(
    'hello_world',
    default_args=default_args,
    schedule=timedelta(minutes=10),
)

# Define a task using PythonOperator
def say_hello():
    print("Hello World!")

task = PythonOperator(
    task_id='hello_world_task',
    python_callable=say_hello,
    dag=dag
)