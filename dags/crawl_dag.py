# Import libraries
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
from airflow import DAG
from crawl_data import crawl_box_office_data, crawl_imdb_data


# Insert fact data to PostgreSQL
def read_and_insert_fact_data(**kwargs):
    ti = kwargs['ti']

    crawled_data = ti.xcom_pull(task_ids='crawl_fact_data')
    data = f"""{crawled_data}"""
    data_clean = data.replace("'", '"')
    json_fact_data = json.loads(data_clean)

    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')

    for item in json_fact_data:
        sql = """
        INSERT INTO movie_revenue (id, rank, revenue, gross_change_per_day, gross_change_per_week, crawled_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        pg_hook.run(sql, parameters=(
            item['id'],
            item['rank'],
            item['revenue'],
            item['gross_change_per_day'],
            item['gross_change_per_week'],
            item['crawled_date']))


# Insert dim data to PostgreSQL
def read_and_insert_dim_data(**kwargs):
    ti = kwargs['ti']

    crawled_data = ti.xcom_pull(task_ids='crawl_dim_data')
    data = f"""{crawled_data}"""
    data_clean = data.replace("'", '"')
    json_fact_data = json.loads(data_clean)


    # Connect to PostgreSQL using a PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    
    # Iterate over the JSON data and insert it into the 'movies' table
    for item in json_fact_data:
        sql = """
            insert into movies_detail (id, title, duration, rating, director, budget, worldwide_gross, genre)
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) 
            DO UPDATE
            SET crawled_date = EXCLUDED.crawled_date
            """
        pg_hook.run(sql, parameters=(
            item['id'],
            item['title'],
            item['duration'],
            item['rating'],
            item['director'] ,
            item['budget'],
            item['worldwide_gross'],
            item['genre'],
           # item['created_at']
        ))

default_args = {
    'owner' : 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# Define Dag
with DAG (
    default_args=default_args,
    dag_id='crawl_and_insert_data_into_db',
    description='crawler data from box office and imdb',
    start_date=datetime(2023, 6, 8),
    end_date=datetime(2023, 6, 10),
    schedule_interval='@daily'  
    
) as dag:

    # Crawl fact data task
    crawl_fact_data = PythonOperator(
        task_id = 'crawl_fact_data',
        python_callable=crawl_box_office_data,
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
            id text,
            rank integer,
            revenue text,
            gross_change_per_day text,
            gross_change_per_week text,
            crawled_date date,
            primary key(crawled_date, id)
        )
        """
    )

    create_dim_table = PostgresOperator(
        task_id='create_dim_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS movies_detail (
            id text,
            title text,
            duration text,
            rating text,
            director text,
            budget text,
            worldwide_gross text,
            genre text,
            crawled_date timestamp DEFAULT now(),
            primary key(id)
        )
        """
    )
     # Insert fact data task
    insert_fact_data_to_postgres = PythonOperator(
    task_id='insert_fact_data_to_postgres',
    python_callable=read_and_insert_fact_data,
    provide_context=True,
    op_kwargs={} 
    )

    # Insert dim data task
    insert_dim_data_to_postgres = PythonOperator(
        task_id = 'insert_dim_data_to_postgres',
        python_callable=read_and_insert_dim_data,
        provide_context = True,
        op_kwargs={}
    )

crawl_fact_data >> [insert_fact_data_to_postgres, crawl_dim_data] >> insert_dim_data_to_postgres
[create_fact_table, create_dim_table] 