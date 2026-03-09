from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import subprocess

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 4, 16, 10),##指定 2025-08-04 16:10
    'retries': 0,
}

def run_population_scrapy():
    result = subprocess.run(
        ["python3", "Py_Popu_Migr_Scrapy_V6.py"],
        cwd="/opt/airflow/dags",
        capture_output=True,
        text=True
    )
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    if result.returncode != 0:
        raise Exception("Population Migration Script Failed")

with DAG(
    dag_id='d_001_popu_migr_scrapy_dag_H',
    default_args=default_args,
    schedule_interval='@once',  ##只跑一次
    is_paused_upon_creation=True,##預設改成不跑
    catchup=False,              ##不補跑
    tags=['history', 'population']
) as dag:

    run_popu_task = PythonOperator(
        task_id='run_population_migration_scrapy',
        python_callable=run_population_scrapy
    )

    trigger_re_scrapy = TriggerDagRunOperator(
        task_id='trigger_re_scrapy_dag',
        trigger_dag_id='d_002_re_scrapy_playwright_dag_H',
        wait_for_completion=False
    )

    run_popu_task >> trigger_re_scrapy