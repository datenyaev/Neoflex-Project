import time
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import pandas as pd
from datetime import datetime

PATH = Variable.get("my_path")

def insert_data(table_name):
    try:
        # Пробуем загрузить CSV с кодировкой 'cp1251'
        df = pd.read_csv(PATH + f"{table_name}.csv", delimiter=",", encoding='cp1251')
    except UnicodeDecodeError:
        # Если возникла ошибка кодировки, пробуем без кодировки
        df = pd.read_csv(PATH + f"{table_name}.csv", delimiter=",")
    postgres_hook = PostgresHook("project_two")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="stage", if_exists="replace", index=False)

default_args = {
    "owner": "dmitry",
    "start_date": datetime(2025, 1, 18),
    "retries": 2
}
with DAG(
    "load_data",
    default_args=default_args,
    description="Загрузка данных",
    catchup=False,
    template_searchpath=[PATH],
    schedule="0 0 * * *"
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    load_deal_info = PythonOperator(
        task_id="load_deal_info",
        python_callable=insert_data,
        op_kwargs={"table_name": "deal_info"}
    )

    load_product_info = PythonOperator(
        task_id="load_product_info",
        python_callable=insert_data,
        op_kwargs={"table_name": "product_info"}
    )

    load_dict_currency = PythonOperator(
        task_id="load_dict_currency",
        python_callable=insert_data,
        op_kwargs={"table_name": "dict_currency"}
    )

    end = DummyOperator(
        task_id="end"
    )

    (
        start
        >>[load_deal_info, load_product_info, load_dict_currency]
        >> end
    )