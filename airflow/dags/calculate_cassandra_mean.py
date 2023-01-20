import datetime
from datetime import timedelta
from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.python import PythonOperator
from cassandra.cluster import Cluster

default_args = {
    'owner': 'guillaume.lrue',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'calculate_cassandra_mean',
    default_args=default_args,
    schedule=timedelta(hours=1),
)


def calculate_mean(column_name):
    # Connect to Cassandra using the Cassandra library
    cluster = Cluster()
    session = cluster.connect()

    # Execute a SELECT query to get all values in the specified column
    rows = session.execute(f"SELECT {column_name} FROM mlops.logs")

    # Calculate the mean of the column values
    sum = 0
    count = 0
    for row in rows:
        sum += int(row.value)
        count += 1
    mean = sum / count
    print("sum", sum)
    print("count", count)
    return mean


# @apply_defaults
def print_mean(**kwargs):
    mean = calculate_mean('value')
    print(f'La moyenne de la colonne Value est {mean}')


mean_task = PythonOperator(
    task_id='calculate_mean',
    python_callable=print_mean,
    dag=dag,
)
