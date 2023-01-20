from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Default Arguments for the DAG
default_args = {
    'owner': 'guillaume.larue',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create a DAG instance
dag = DAG(
    'hello_world',
    default_args=default_args,
    schedule=timedelta(minutes=2),
)


# Define a task using PythonOperator
def say_hello():
    print("Hello World!")


task = PythonOperator(
    task_id='hello_world_task',
    python_callable=say_hello,
    dag=dag
)
