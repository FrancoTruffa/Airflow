from datetime import datetime

from airflow.models import DAG
#from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Homer_Simpson',
    'start_date': datetime(2021, 5, 20, 11, 0 , 0)
}

def titulo():
    print('El uso de Airflow en la universidad de Springfield')

def introduccion():
    print('El otro dia mi hija me dijo que Airflow no se utilizaba en la universidad de Springfield, y yo le dije: que no Lisa? que no?')

def relleno():
    for i in range(150):
        print('Pudrete Flanders ')


with DAG(dag_id='dag_homer_simpson',
        default_args= default_args,
        schedule_interval= '@once') as dag:

        primera_parte = PythonOperator(
            task_id = 'primera_parte',
            python_callable= titulo
        )

        segunda_parte = PythonOperator(
            task_id = 'segunda_parte',
            python_callable = introduccion
        )

        tercera_parte = PythonOperator(
            task_id = 'tercera_parte',
            python_callable= relleno
        )


primera_parte >> segunda_parte >> tercera_parte
