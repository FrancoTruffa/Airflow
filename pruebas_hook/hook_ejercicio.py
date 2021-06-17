import pandas as pd

from airflow.models import DAG
from airflow.utils import dates
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'start_date': dates.days_ago(1)
}

def getconnection():
    conn = PostgresHook('la_virgen_cuantos_datos')
    print("conexion exitosa")

def leer_datos():
    conn = PostgresHook('la_virgen_cuantos_datos')
    datos = conn.get_pandas_df("SELECT * FROM experiments.experimientos_100tifikos")
    datos.to_csv('datosExperimentos.csv', index = False)

with DAG(
    dag_id = 'dag_hooks_prueba',
    default_args=default_args,
    schedule_interval = '@daily') as dag:

    operador_de_conn = PythonOperator(
            task_id = 'operador_de_conn',
            python_callable = getconnection
    )

    operator_leer_datos = PythonOperator(
    task_id = 'operator_leer_datos',
    python_callable = leer_datos
    )

    copiar_datos = PostgresOperator(
        task_id = 'copiar_datos',
        sql = """
            INSERT INTO comprobar_trayectorias
            SELECT TOP 1 coordenadas
            FROM trajectories.precision_desplazamiento
        """
    )

operador_de_conn >> operator_leer_datos >> copiar_datos