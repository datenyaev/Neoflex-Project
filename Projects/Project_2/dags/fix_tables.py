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
    "fix_tables",
    default_args=default_args,
    description="Исправление счетов",
    catchup=False,
    template_searchpath=[PATH],
    schedule="0 0 * * *"
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    wait_for_fill_loan_holiday_info = ExternalTaskSensor(
        task_id='wait_for_fill_loan_holiday_info',
        external_dag_id='fill_loan_holiday_info',
        external_task_id='end',
        mode='poke',
        timeout=3600,
        allowed_states=['success'],
        failed_states=['failed', 'skipped']
    )

    fix_in_out_sum = SQLExecuteQueryOperator(
        task_id="fix_in_out_sum",
        conn_id="project_two",
        sql="sql/fix_in_out_sum.sql"
    )

    fix_in_out_sum_2 = SQLExecuteQueryOperator(
        task_id="fix_in_out_sum_2",
        conn_id="project_two",
        sql="sql/fix_in_out_sum_2.sql"
    )

    end = DummyOperator(
        task_id="end"
    )

    (
        start
        >>wait_for_fill_loan_holiday_info
        >> fix_in_out_sum
        >> fix_in_out_sum_2
        >> end
    )