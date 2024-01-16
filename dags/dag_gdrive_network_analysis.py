import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def _check_file_exists():
    import os.path
    return os.path.exists("token.json")

    ...
    if exist:
        return 'get_data'
    else:
        return 'end'
    
def _get_data(**context):
    pass

DAG = DAG(
    dag_id='dag_gdrive_network_analysis',
    start_date=datetime.datetime(2021, 10, 1),
    schedule_interval='@daily',
    max_active_runs=1,
    retries=3,
    depent_on_past=False,
    catchup=False
)

start = EmptyOperator(task_id='start', dag=DAG)
end = EmptyOperator(task_id='end', dag=DAG)

check_file_exists = PythonOperator(
    task_id='check_file_exists',
    python_callable=_check_file_exists,
    retries=3,
    retry_delay=datetime.timedelta(seconds=30),
    dag=DAG
)

get_data = PythonOperator(
    task_id='get_data',
    python_callable=_get_data,
    execution_timeout=datetime.timedelta(minutes=20),
    retries=2,
    retry_delay=datetime.timedelta(seconds=5),
    dag=DAG
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=_transform_data,
    dag=DAG
)

ingest_to_postgres = PythonOperator(
    task_id='ingest_to_postgres',
    python_callable=_ingest_to_postgres,
    dag=DAG
)

start >> check_file_exists >> [end, get_data]
get_data >> transform_data >> ingest_to_postgres >> end