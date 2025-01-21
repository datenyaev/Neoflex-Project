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
    "insert_data",
    default_args=default_args,
    description="Вставка данных",
    catchup=False,
    template_searchpath=[PATH],
    schedule="0 0 * * *"
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    wait_for_load_data = ExternalTaskSensor(
        task_id='wait_for_load_data',
        external_dag_id='load_data',
        external_task_id='end',
        mode='poke',
        timeout=3600,
        allowed_states=['success'],
        failed_states=['failed', 'skipped']
    )

    insert_deal_info = SQLExecuteQueryOperator(
        task_id="insert_deal_info",
        conn_id="project_two",
        sql="sql/insert_deal_info.sql"
    )

    insert_product_info = SQLExecuteQueryOperator(
        task_id="insert_product_info",
        conn_id="project_two",
        sql="sql/insert_product_info.sql"
    )

    insert_dict_currency = SQLExecuteQueryOperator(
        task_id="insert_dict_currency",
        conn_id="project_two",
        sql="sql/insert_dict_currency.sql"
    )

    end = DummyOperator(
        task_id="end"
    )

    (
        start
        >>wait_for_load_data
        >>[insert_deal_info, insert_product_info, insert_dict_currency]
        >> end
    )