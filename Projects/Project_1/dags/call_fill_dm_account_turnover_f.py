from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable

PATH = Variable.get("my_path")

default_args = {
    "owner": "dmitry",
    "start_date": datetime(2025, 1, 18),
    "retries": 2
}
with DAG(
    "fill_dm_account_turnover_f",
    default_args=default_args,
    description="Заполнение витрины dm_account_turnover_f",
    catchup=False,
    template_searchpath=[PATH],
    schedule="0 0 * * *"
) as dag:
    start = DummyOperator(
        task_id="start"
    )

    wait_for_insert_data = ExternalTaskSensor(
        task_id='wait_for_insert_data',
        external_dag_id='insert_data',
        external_task_id='end',
        mode='poke',
        timeout=3600,
        allowed_states=['success'],
        failed_states=['failed', 'skipped']
    )

    call_fill_dm_account_turnover_f = SQLExecuteQueryOperator(
        task_id="call_fill_dm_account_turnover_f",
        conn_id="postgres-db",
        sql="sql/fill_dm_account_turnover_f.sql"
    )

    end = DummyOperator(
        task_id="end"
    )

    (
        start
        >>wait_for_insert_data
        >>call_fill_dm_account_turnover_f
        >> end
    )