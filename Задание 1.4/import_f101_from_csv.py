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

def create_table_v2():
    hook = PostgresHook(POSTGRES_CONN_ID)

    sql = """
        DROP TABLE IF EXISTS dm.dm_f101_round_f_v2;

        CREATE TABLE dm.dm_f101_round_f_v2 AS
        SELECT *
        FROM dm.dm_f101_round_f
        WHERE 1 = 0;
    """
    hook.run(sql)


def import_f101_from_csv():
    log_id = start_log("import_csv_to_dm_f101_round_f_v2")

    try:
        if not os.path.exists(CSV_FILE):
            raise Exception(f"Файл не найден: {CSV_FILE}")

        create_table_v2()

        hook = PostgresHook(POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        df = pd.read_csv(CSV_FILE, sep=";", encoding="utf-8")
        df.to_sql("dm_f101_round_f_v2", engine, schema="dm", if_exists="append", index=False)
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
    dag_id="import_f101_from_csv",
    default_args=default_args,
    description="Импорт CSV файла в dm.dm_f101_round_f_v2",
    catchup=False,
    schedule=None
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    import_task = PythonOperator(
        task_id="import_csv_to_dm_f101_round_f_v2",
        python_callable=import_f101_from_csv
    )

    end = EmptyOperator(
        task_id="end"
    )

    (
        start >> import_task >> end
    )