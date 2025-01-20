from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.configuration import conf
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
    df.to_sql(table_name, engine, schema="load", if_exists="replace", index=False)

default_args = {
    "owner": "dmitry",
    "start_date": datetime(2024, 2, 25),
    "retries": 2
}
with DAG(
    "project_2",
    default_args=default_args,
    description="Загрузка данных в stage",
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

    split = DummyOperator(
        task_id="split"
    )

    insert_deal_info = SQLExecuteQueryOperator(
        task_id="insert_deal_info",
        conn_id="project_two",
        sql="sql_2/insert_deal_info.sql"
    )

    insert_product_info = SQLExecuteQueryOperator(
        task_id="insert_product_info",
        conn_id="project_two",
        sql="sql_2/insert_product_info.sql"
    )

    insert_dict_currency = SQLExecuteQueryOperator(
        task_id="insert_dict_currency",
        conn_id="project_two",
        sql="sql_2/insert_dict_currency.sql"
    )

    call_fill_loan_holiday_info = SQLExecuteQueryOperator(
        task_id="call_fill_loan_holiday_info",
        conn_id="project_two",
        sql="sql_2/fill_loan_holiday_info.sql"
    )

    fix_in_out_sum = SQLExecuteQueryOperator(
        task_id="fix_in_out_sum",
        conn_id="project_two",
        sql="sql_2/fix_in_out_sum.sql"
    )

    fix_in_out_sum_2 = SQLExecuteQueryOperator(
        task_id="fix_in_out_sum_2",
        conn_id="project_two",
        sql="sql_2/fix_in_out_sum_2.sql"
    )

    call_fill_account_balance_turnover = SQLExecuteQueryOperator(
        task_id="call_fill_account_balance_turnover",
        conn_id="project_two",
        sql="sql_2/fill_account_balance_turnover.sql"
    )

    end = DummyOperator(
        task_id="end"
    )

    (
        start
        >>[load_deal_info, load_product_info]
        >>split
        >>[insert_deal_info, insert_product_info, insert_dict_currency]
        >>call_fill_loan_holiday_info
        >>fix_in_out_sum
        >>fix_in_out_sum_2
        >>call_fill_account_balance_turnover
        >> end
    )