from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.utils.dates import days_ago
from airflow.utils import timezone 

# now = timezone.utcnow()
a_date = timezone.datetime(2020, 11, 8)

default_args = {
    'owner': 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2020, 11, 8),
    # 'start_date' : days_ago(1),
    'email' : ['osman.fernandez@outlook.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

dag = DAG(
    'segundo_dag',
    default_args = default_args,
    description = 'Our first DAG with ETL process',
    schedule_interval = timedelta(days=1),
    catchup = False,
)

def just_a_function():
    print("Hello World Here in Airflow :-)")

run_etl = PythonOperator(
    task_id = 'whole_spotify_etl',
    # python_callable = run_spotify_etl,
    python_callable = just_a_function,
    dag = dag,
)

run_etl