from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'franco_truffa_macros',
    'start_date': datetime(2021, 3, 15, 15, 0 , 0)
}

def hello_world_loop(**context):
    for palabra in ['hello', 'world']:
        print(palabra)
    
    task_instance = context['task_instance']
    task_instance.xcom_push(key='clave_de_prueba', value = 'valor_de_prueba')
    
    return 'Retornando Datos'

def prueba_pull(**context):
    ti = context['task_instance']
    valor_capturado = ti.xcom_pull(task_ids='prueba_python')

    print(valor_capturado)

with DAG(dag_id='dag_prueba_macros',
        default_args= default_args,
        schedule_interval= '@once') as dag:

        start = DummyOperator(
            task_id = 'start'
        )

        prueba_python = PythonOperator(
            task_id = 'prueba_python',
            python_callable= hello_world_loop,
            do_xcom_push = True,
            provide_context = True

        )

        prueba_pull = PythonOperator(
            task_id = 'prueba_pull',
            python_callable= prueba_pull,
            do_xcom_push = True,
            provide_context = True
        )

        prueba_bash = BashOperator(
            task_id = 'prueba_bash',
            bash_command= 'echo {{ ds }}'
        )

        prueba_bash_macro = BashOperator(
            task_id = 'prueba_bash_macro',
            bash_command= 'echo {{ ti.xcom_pull(task_ids="prueba_python") }}'
        )

start >> prueba_python >> prueba_pull >> prueba_bash >> prueba_bash_macro