import pandas as pd
import logging

from airflow.models import DAG
from airflow.utils import dates
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'start_date': dates.days_ago(1)
}

def obtener_pandas():
    conn = PostgresHook('redshift_produccion')
    df = conn.get_pandas_df("SELECT * FROM TABLE")
    logging.info("Datos obtenidos de la query")
    df.to_csv('s3://bucket/key.csv', index=False)
    logging.info("Guardado en S3")

with DAG(
    dag_id = 'dag_hooks',
    default_args=default_args,
    schedule_interval = '@daily') as dag:

    start = DummyOperator(task_id = 'start')

    obtener_pandas_operator = PythonOperator(
        task_id = 'obtener_pandas_operator',
        python_callable = obtener_pandas
    )

start >> obtener_pandas_operator >> fin