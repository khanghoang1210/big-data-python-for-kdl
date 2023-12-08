from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_ingest_etl',
    default_args=default_args,
    description='A simple DAG to schedule Spark Ingest and ETL jobs',
    schedule_interval=timedelta(days=1),
)

ingest_task = BashOperator(
    task_id='ingest_task',
    bash_command='spark-submit --class com.example.IngestJob /path/to/your/ingest-job.jar',
    dag=dag,
)

etl_task = BashOperator(
    task_id='etl_task',
    bash_command='spark-submit --class com.example.ETLJob /path/to/your/etl-job.jar',
    dag=dag,
)

ingest_task >> etl_task
