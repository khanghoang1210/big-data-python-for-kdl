# Import libraries
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from airflow import DAG
from crawl_data import boxOfficeMojo


default_args = {
    'owner' : 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

# Define Dag
with DAG (
    default_args=default_args,
    dag_id='crawl_data',
    description='crawler data from box office and imdb',
    start_date=datetime(2023, 6, 10),
    end_date=datetime(2023, 6, 13),
    schedule_interval='@daily'  
    
) as dag:

    # Crawl fact data task
    crawl_fact_data = PythonOperator(
        task_id = 'crawl_fact_data',
        python_callable=boxOfficeMojo,
        op_kwargs={'date': '{{ ds }}'},
        provide_context = True,
        do_xcom_push=True
    )
