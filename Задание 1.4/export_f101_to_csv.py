from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.configuration import conf

import pandas as pd
import os
from datetime import datetime

POSTGRES_CONN_ID = "postgres-db"
PATH = Variable.get("my_path")
conf.set("core", "template_searchpath", PATH)
CSV_FILE = "/opt/airflow/data/dm_f101_round_f.csv"

def start_log(process_name):
    hook = PostgresHook(POSTGRES_CONN_ID)

    sql = """
        INSERT INTO logs.etl_process_log (process_name, start_time)
        VALUES (%s, now())
        RETURNING log_id;
    """
    return hook.get_first(sql, parameters=(process_name,))[0]

def finish_log(log_id, error_mess=None):
    hook = PostgresHook(POSTGRES_CONN_ID)

    sql = """
        UPDATE logs.etl_process_log
        SET end_time = now(), error_mess = %s
        WHERE log_id = %s;
    """
    hook.run(sql, parameters=(error_mess, log_id))

def export_f101_to_csv():
    log_id = start_log("export_dm_f101_round_f_to_csv")

    try:
        hook = PostgresHook(POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        df = pd.read_sql(
            """
            SELECT *
            FROM dm.dm_f101_round_f
            ORDER BY from_date, to_date, ledger_account, characteristic
            """,
            engine
        )
        os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
        df.to_csv(CSV_FILE, sep=";", index=False, encoding="utf-8")
        finish_log(log_id)

    except Exception as e:
        finish_log(log_id, error_mess=str(e))
        raise

default_args = {
    "owner": "nkuznetcov",
    "start_date": datetime(2024, 1, 1),
    "retries": 2
}

with DAG(
    dag_id="export_f101_to_csv",
    default_args=default_args,
    description="Выгрузка 101 формы из DM в CSV",
    catchup=False,
    schedule=None
) as dag:

    start = EmptyOperator(
        task_id="start"
    )
    
    export_task = PythonOperator(
        task_id="export_dm_f101_round_f_to_csv",
        python_callable=export_f101_to_csv
    )

    end = EmptyOperator(
        task_id="end"
    )

    (
        start >> export_task >> end
    )