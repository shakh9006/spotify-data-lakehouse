import logging
from datetime import timedelta

from airflow.sdk import dag, task
from plugins.dbt_operator import DbtCoreOperator

@dag(
    dag_id="hello",
    schedule=None,
    catchup=False,
    description="Hello World DAG",
    tags=["example"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def hello_world():
    logger = logging.getLogger(__name__)
    logger.info("Hello World")

    @task
    def dbt_ods_layer():
        logger.info("Hello World from print_hello")

    dbt_ods_layer()

hello_world()