from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 4),
    'retries': 999,                     ##交給Airflow重試
    'retry_delay': timedelta(minutes=10),##每10分鐘重試一次
}

def run_re_scrapy_once():
    result = subprocess.run(
        ["python3", "Py_RE_Scrapy_Playwright_T_5Y_Zero_Retry_V4.py"],
        cwd="/opt/airflow/dags",
        capture_output=True,
        text=True
    )
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    if result.returncode != 0:
        # 讓 Airflow 知道失敗，好觸發 retries
        raise AirflowFailException(f"Script failed with code {result.returncode}")

with DAG(
    dag_id='d_002_re_scrapy_playwright_dag_H',
    default_args=default_args,
    schedule_interval=None,##或你要的 cron；若只想跑一次，用外部觸發就好
    catchup=False,
    max_active_runs=1,        ##保證不會並發多個 run
    tags=['history', 'real_estate']
) as dag:

    run_re_task = PythonOperator(
        task_id='run_real_estate_scrapy',
        python_callable=run_re_scrapy_once
    )