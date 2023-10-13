# Import libraries
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta, date
from airflow import DAG
from crawl_data import boxOfficeMojo, crawl_imdb_data


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

    crawl_dim_data = PythonOperator(
        task_id = 'crawl_dim_data',
        python_callable=crawl_imdb_data,
        provide_context = True,
        do_xcom_push=True
    )

     # Create fact table task
    create_fact_table = PostgresOperator(
        task_id='create_fact_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS movie_revenue (
            rank integer,
            revenue text,
            crawled_date date,
            id text,
            primary key(crawled_date, id)
        )
        """
    )
crawl_fact_data >> crawl_dim_data