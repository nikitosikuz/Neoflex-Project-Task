from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.configuration import conf
from airflow.models import Variable

import pandas as pd
import time
from datetime import datetime

PATH = Variable.get("my_path")
conf.set("core", "template_searchpath", PATH)
POSTGRES_CONN_ID = "postgres-db"

def start_log(process_name):
     postgres_hook = PostgresHook(POSTGRES_CONN_ID)

     sql = """
        INSERT INTO logs.etl_process_log
            (process_name, start_time)
        VALUES
            (%s, now())
        RETURNING log_id;
    """
     
     return postgres_hook.get_first(sql, parameters=(process_name,))[0]

def finish_log(log_id, error_mess=None):
    postgres_hook = PostgresHook(POSTGRES_CONN_ID)
    sql = """
        UPDATE logs.etl_process_log
        SET
            end_time = now(),
            error_mess = %s
        WHERE log_id = %s;
    """
    postgres_hook.run(sql, parameters=(error_mess, log_id))

def read_csv_file(file_path):
    try:
        return pd.read_csv(file_path, delimiter=";", encoding="utf-8")

    except UnicodeDecodeError:
        return pd.read_csv(file_path, delimiter=";", encoding="cp1252")

def insert_data(table_name):
    process_name = f"load_{table_name}"
    log_id = start_log(process_name)

    time.sleep(5)

    try:
        df = read_csv_file(PATH + f"{table_name}.csv")
        df.columns = df.columns.str.lower()
        
        postgres_hook = PostgresHook(POSTGRES_CONN_ID)
        postgres_hook.run(f"TRUNCATE TABLE ds.{table_name}")
        engine = postgres_hook.get_sqlalchemy_engine()

        if table_name == "md_currency_d":
            df["currency_code"] = df["currency_code"].fillna(0).astype(int).astype(str)
            
        date_columns = [col for col in df.columns if "date" in col.lower()]
        for col in date_columns:
            if table_name == "ft_posting_f" and col == "oper_date":
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
        else:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        if table_name == "md_exchange_rate_d":
            df = df.drop_duplicates(subset=["data_actual_date", "currency_rk"], keep="first")

        df.to_sql(table_name, engine, schema="ds", if_exists="append", index=False)
        finish_log(log_id)

    except Exception as e:
        finish_log(log_id, error_mess=str(e))
        raise

default_args = {
    "owner": "nkuznetcov",
    "start_date": datetime(2026, 5, 5),
    "retries": 2
}

with DAG(
    dag_id="insert_data",
    template_searchpath=[PATH],
    default_args=default_args,
    description="Загрузка данных в схему DS",
    catchup=False,
    schedule=None
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",
        python_callable=insert_data,
        op_kwargs={"table_name": "ft_balance_f"}
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data,
        op_kwargs={"table_name": "ft_posting_f"}
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_account_d"}
    )

    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_currency_d"}
    )

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_exchange_rate_d"}
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_ledger_account_s"}
    )

    end = EmptyOperator(
        task_id="end"
    )

    (
        start
        >> [ft_balance_f,
            ft_posting_f,
            md_account_d,
            md_currency_d,
            md_exchange_rate_d,
            md_ledger_account_s]
        >> end
    )