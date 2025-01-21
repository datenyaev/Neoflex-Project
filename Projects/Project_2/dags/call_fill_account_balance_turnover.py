from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime

PATH = Variable.get("my_path")

default_args = {
    "owner": "dmitry",
    "start_date": datetime(2025, 1, 18),
    "retries": 2
}
with DAG(
    "fill_account_balance_turnover",
    default_args=default_args,
    description="Исправление счетов",
    catchup=False,
    template_searchpath=[PATH],
    schedule="0 0 * * *"
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    wait_for_fix_tables = ExternalTaskSensor(
        task_id='wait_for_fix_tables',
        external_dag_id='fix_tables',
        external_task_id='end',
        mode='poke',
        timeout=3600,
        allowed_states=['success'],
        failed_states=['failed', 'skipped']
    )

    call_fill_account_balance_turnover = SQLExecuteQueryOperator(
        task_id="call_fill_account_balance_turnover",
        conn_id="project_two",
        sql="sql/fill_account_balance_turnover.sql"
    )

    end = DummyOperator(
        task_id="end"
    )

    (
        start
        >>wait_for_fix_tables
        >> call_fill_account_balance_turnover
        >> end
    )