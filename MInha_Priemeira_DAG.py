from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import requests


def acessa_youtube():
    url = 'https://www.youtube.com/'
    response = requests.get(url)
    check = response.status_code
    return check


def teste_status(ti):
    check = ti.xcom_pull(task_ids = 'acessa_youtube')
    if (check == 200):
        return 'esta_ok'
    return 'nao_esta_ok'


with DAG('Minha_Primeira_DAG', start_date = datetime(2022,11,1),
          schedule_interval = '30 * * * *', catchup = False) as dag:
    
    acessa_youtube = PythonOperator(
        task_id = 'acessa_youtube',
        python_callable = acessa_youtube
    )

    teste_status = BranchPythonOperator(
        task_id = 'teste_status',
        python_callable = teste_status
    )


    esta_ok = BashOperator(
        task_id = 'esta_ok',
        bash_command = "echo 'ok' > teste.txt"
    )

    nao_esta_ok = BashOperator(
        task_id = 'nao_esta_ok',
        bash_command = "echo 'Não ok'"
    )


    acessa_youtube >> teste_status >> [esta_ok, nao_esta_ok]

