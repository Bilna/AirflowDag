from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperatorimport airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

args={'owner': 'airflow'}

default_args = {
    'owner': 'airflow',    
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag_psql = DAG(
    dag_id = "postgresoperator_demo",
    default_args=args,
    # schedule_interval='0 0 * * *',
    schedule_interval='None',	
    dagrun_timeout=timedelta(minutes=60),
    description='use case of psql operator in airflow',
    start_date = airflow.utils.dates.days_ago(1)
)
create_table_sql_query = """CREATE TABLE employee (id INT NOT NULL, name VARCHAR(250) NOT NULL, dept VARCHAR(250) NOT NULL);"""
insert_data_sql_query = """insert into employee (id, name, dept) values(1, 'vamshi','bigdata'),(2, 'divya','bigdata'),(3, 'binny','projectmanager'),(4, 'omair','projectmanager') ;"""

create_table = PostgresOperator(
    sql = create_table_sql_query,
    task_id = "create_table_task",
    postgres_conn_id = "postgres_local",
    dag = dag_psql
    )

insert_data = PostgresOperator(
    sql = insert_data_sql_query,
    task_id = "insert_data_task",
    postgres_conn_id = "postgres_local",
    dag = dag_psql
    )

create_table >> insert_data

    if __name__ == "__main__":
        dag_psql.cli()
