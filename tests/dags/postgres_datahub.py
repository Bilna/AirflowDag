from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

try:
    from airflow.operators.python import PythonOperator
except ModuleNotFoundError:
    from airflow.operators.python_operator import PythonOperator

from datahub.ingestion.run.pipeline import Pipeline

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["bilna@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=120),
}


def ingest_postgresql(j):
    pipeline = Pipeline.create({
        "source": {
            "type": "postgres",
            "config": {
                "username": "postgres",
                "password": "password",
                "database": "airflow",
                "host_port": "100.64.25.120:5432",
                # "stateful_ingestion": {
                #     "enabled": True,
                #     "state_provider": {
                #         "type": "datahub",
                #         "config": {
                #             "datahub_api": {
                #                 "server": 'http://10.118.241.229:8080',
                #                 "token": "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMSIsImV4cCI6MTY2MjY5NjUzMiwianRpIjoiMmY5ZTc0ZmYtNjk4YS00YzQxLWIwNTYtZGEwYTdkYjQ2ZGIwIiwic3ViIjoiZGF0YWh1YiIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.QOWGIAiyaFRy2wC5pVMYD8HarrQex9Vy2MSCMEOgnP4"
                #             }
                #         }
                #     }
                # }
            },
        },
        "pipeline_name": "datahub-local-postgres_db",
        "sink": {
            "type": "datahub-kafka",
            "config": {
                "connection": {
                    "bootstrap": "http://datahub.odc-data-mgmt-01-drm.k8s.cec.lab.emc.com:31092",
                    "schema_registry_url": "http://datahub.odc-data-mgmt-01-drm.k8s.cec.lab.emc.com:8081"
                }
            }
        }
    })
    pipeline.run()
    pipeline.raise_from_status()


with DAG(
        "datahub_localtestrun_postgres_db",
        default_args=default_args,
        description="An example DAG which ingests metadata from PostgreSQL to DataHub",
        schedule_interval=timedelta(days=1),
        start_date=days_ago(2),
        catchup=False,
) as dag:
    #for j in range(1, 101):
    ingest_task = PythonOperator(
    task_id='ingest_postgresql',
    python_callable=ingest_postgresql,
	#    op_args={j},
    )
