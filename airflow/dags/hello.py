from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def print_hello_world():
    print("Hello, World from Airflow!")

with dag:
    t1 = PythonOperator(
        task_id='print_hello_world',
        python_callable=print_hello_world,
    )

# Trigger the next run of the DAG
trigger_dag = TriggerDagRunOperator(
    task_id='trigger_next_run',
    trigger_dag_id='hello_world_dag',
    dag=dag,
)

t1 >> trigger_dag
