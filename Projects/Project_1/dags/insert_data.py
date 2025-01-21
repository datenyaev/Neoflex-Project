import time
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import pandas as pd
from datetime import datetime

PATH = Variable.get("my_path")

def insert_data(table_name):
    start_time = datetime.now()
    try:
        df = pd.read_csv(PATH + f"{table_name}.csv", delimiter=";", encoding='cp1251')
    except UnicodeDecodeError:
        df = pd.read_csv(PATH + f"{table_name}.csv", delimiter=";")

    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()

    temp_table = f"tmp_{table_name}"
    df.to_sql(temp_table, engine, schema='ds', if_exists='replace', index=False)

    upsert_sql = f"""
    SELECT ds.upsert_table('{table_name}', '{temp_table}');
    """

    rows_affected = postgres_hook.get_first(upsert_sql)
    if rows_affected:
        rows_affected = rows_affected[0]
    else:
        rows_affected = 0

    # Логирование
    time.sleep(5)
    end_time = datetime.now()
    message = f"{rows_affected} rows affected."

    postgres_hook.run(
        """
        INSERT INTO logs.data_load_log (process_name, start_time, end_time, status, rows_loaded, message)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        parameters=(f"Load process for {table_name}", start_time, end_time, "Completed", rows_affected, message)
    )

    # Удаление временной таблицы после использования
    postgres_hook.run(f"DROP TABLE IF EXISTS ds.{temp_table};")

default_args = {
    "owner": "dmitry",
    "start_date": datetime(2025, 1, 18),
    "retries": 2
}
with DAG(
    "insert_data",
    default_args=default_args,
    description="Загрузка данных в ds",
    catchup=False,
    template_searchpath=[PATH],
    schedule="0 0 * * *"
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    load_ft_balance_f = PythonOperator(
        task_id="load_ft_balance_f",
        python_callable=insert_data,
        op_kwargs={"table_name": "ft_balance_f"}
    )

    load_ft_posting_f = PythonOperator(
        task_id="load_ft_posting_f",
        python_callable=insert_data,
        op_kwargs={"table_name": "ft_posting_f"}
    )

    load_md_account_d = PythonOperator(
        task_id="load_md_account_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_account_d"}
    )

    load_md_currency_d = PythonOperator(
        task_id="load_md_currency_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_currency_d"}
    )

    load_md_exchange_rate_d = PythonOperator(
        task_id="load_md_exchange_rate_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_exchange_rate_d"}
    )

    load_md_ledger_account_s = PythonOperator(
        task_id="load_md_ledger_account_s",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_ledger_account_s"}
    )

    end = DummyOperator(
        task_id="end"
    )

    (
        start
        >>[load_ft_balance_f, load_ft_posting_f, load_md_account_d, load_md_currency_d, load_md_exchange_rate_d, load_md_ledger_account_s]
        >> end
    )