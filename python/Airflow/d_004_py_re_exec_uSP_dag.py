# dags/d_004_py_re_exec_uSP_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 1),
    'retries': 5,
    'retry_delay': timedelta(minutes=20),
}

def run_exec_uSP():
    """執行 uSP_TRANSACTION_INTEGRATE 整合腳本"""
    result = subprocess.run(
        ["python3", "Py_RE_EXEC_uSP_TRANSACTION_INTEGRATE.py"],
        cwd="/opt/airflow/dags",
        capture_output=True,
        text=True
    )
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    if result.returncode != 0:
        raise AirflowFailException(
            f"Exec uSP Script Failed rc={result.returncode}\n{result.stderr[:500]}"
        )

with DAG(
    dag_id='d_004_py_re_exec_uSP_dag',
    default_args=default_args,
    schedule_interval=None,  ##僅由 d_003 觸發
    catchup=False,
    max_active_runs=1,
    tags=['monthly', 'mysql', 'integration', 'Always Triggered By d_003']
) as dag:

    run_exec_task = PythonOperator(
        task_id='run_exec_uSP',
        python_callable=run_exec_uSP
    )