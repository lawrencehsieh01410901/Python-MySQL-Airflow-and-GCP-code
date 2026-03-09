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

def run_log_folder():
    result = subprocess.run(
        ["python3", "Py_Log_Folder.py"],
        cwd="/opt/airflow/dags",
        capture_output=True,
        text=True
    )
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    if result.returncode != 0:
        raise Exception("Run Log Folder Error")

with DAG(
    dag_id='d_006_log_folder_daily',
    default_args=default_args,
    schedule_interval='0 18 * * *',##每天下午18:00執行
    is_paused_upon_creation=True,##預設改成不跑
    catchup=False,              ##不補跑
    tags=['daily', 'log collect']
) as dag:

    run_log_task = PythonOperator(
        task_id='run_log_folder',
        python_callable=run_log_folder
    )