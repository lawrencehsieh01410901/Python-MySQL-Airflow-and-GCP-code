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

def run_popu_migr_daily_scrapy():
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
    dag_id='d_005_popu_migr_daily',
    default_args=default_args,
    schedule_interval='0 15 * * *',##每天下午15:00執行
    is_paused_upon_creation=True,##預設改成不跑
    catchup=False,              ##不補跑
    tags=['daily', 'population']
) as dag:

    run_popu_task = PythonOperator(
        task_id='run_popu_migr_daily_scrapy',
        python_callable=run_popu_migr_daily_scrapy
    )